#!/usr/bin/env python3
"""
Benchmark runner that orchestrates:
1. Starting Valkey infrastructure
2. Running benchmark application
3. Collecting system metrics (perf, CPU, disk I/O, network)
4. Outputting results to JSON

Reads configuration from external JSON files.
Phase completion is determined by workload config (duration or requests).
"""

import argparse
import csv
import json
import os
import random
import re
import shutil
import signal
import string
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple


def generate_job_id():
    now = datetime.now(timezone.utc)
    random_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"bench-{now.strftime('%Y%m%d-%H%M%S')}-{random_suffix}"


def get_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json_config(filepath: Path) -> dict:
    """Load JSON configuration file"""
    with open(filepath) as f:
        return json.load(f)


class VarianceControl:
    """Manages system settings for benchmark stability"""

    def __init__(self):
        self.turbo_boost_path = None
        self.original_turbo_state = None
        self.tc_configured = False
        self.nmi_watchdog_original = None

    def setup(self, network_delay_ms: int = 1):
        print("Setting up variance control...")
        self._disable_turbo_boost()
        self._setup_network_delay(network_delay_ms)
        self._disable_nmi_watchdog()
        self._set_perf_permissions()
        print("Variance control setup complete")

    def teardown(self):
        print("Restoring system settings...")
        self._restore_turbo_boost()
        self._remove_network_delay()
        self._restore_nmi_watchdog()
        print("System settings restored")

    def _disable_turbo_boost(self):
        intel_path = Path("/sys/devices/system/cpu/intel_pstate/no_turbo")
        amd_path = Path("/sys/devices/system/cpu/cpufreq/boost")

        try:
            if intel_path.exists():
                self.turbo_boost_path = intel_path
                self.original_turbo_state = intel_path.read_text().strip()
                subprocess.run(["sudo", "tee", str(intel_path)], input=b"1", capture_output=True)
                print("  ✓ Intel Turbo Boost disabled")
            elif amd_path.exists():
                self.turbo_boost_path = amd_path
                self.original_turbo_state = amd_path.read_text().strip()
                subprocess.run(["sudo", "tee", str(amd_path)], input=b"0", capture_output=True)
                print("  ✓ AMD Boost disabled")
            else:
                print("  ⚠ No turbo boost control found")
        except Exception as e:
            print(f"  ⚠ Could not disable turbo boost: {e}")

    def _restore_turbo_boost(self):
        if self.turbo_boost_path and self.original_turbo_state:
            try:
                subprocess.run(
                    ["sudo", "tee", str(self.turbo_boost_path)],
                    input=self.original_turbo_state.encode(),
                    capture_output=True
                )
                print("  ✓ Turbo boost restored")
            except Exception as e:
                print(f"  ⚠ Could not restore turbo boost: {e}")

    def _setup_network_delay(self, delay_ms: int):
        try:
            subprocess.run(["sudo", "tc", "qdisc", "del", "dev", "lo", "root"], capture_output=True)
            result = subprocess.run(
                ["sudo", "tc", "qdisc", "add", "dev", "lo", "root", "netem", "delay", f"{delay_ms}ms"],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                self.tc_configured = True
                print(f"  ✓ Network delay configured: {delay_ms}ms on loopback")
            else:
                print(f"  ⚠ Could not configure network delay: {result.stderr}")
        except Exception as e:
            print(f"  ⚠ Network delay setup failed: {e}")

    def _remove_network_delay(self):
        if self.tc_configured:
            try:
                subprocess.run(["sudo", "tc", "qdisc", "del", "dev", "lo", "root"], capture_output=True)
                print("  ✓ Network delay removed")
            except Exception as e:
                print(f"  ⚠ Could not remove network delay: {e}")

    def _disable_nmi_watchdog(self):
        try:
            nmi_path = Path("/proc/sys/kernel/nmi_watchdog")
            if nmi_path.exists():
                self.nmi_watchdog_original = nmi_path.read_text().strip()
                subprocess.run(["sudo", "tee", str(nmi_path)], input=b"0", capture_output=True)
                print("  ✓ NMI watchdog disabled")
        except Exception as e:
            print(f"  ⚠ Could not disable NMI watchdog: {e}")

    def _restore_nmi_watchdog(self):
        if self.nmi_watchdog_original:
            try:
                subprocess.run(
                    ["sudo", "tee", "/proc/sys/kernel/nmi_watchdog"],
                    input=self.nmi_watchdog_original.encode(),
                    capture_output=True
                )
                print("  ✓ NMI watchdog restored")
            except Exception as e:
                print(f"  ⚠ Could not restore NMI watchdog: {e}")

    def _set_perf_permissions(self):
        try:
            subprocess.run(["sudo", "sysctl", "-w", "kernel.perf_event_paranoid=-1"], capture_output=True)
            subprocess.run(["sudo", "sysctl", "-w", "kernel.kptr_restrict=0"], capture_output=True)
            print("  ✓ Perf permissions configured")
        except Exception as e:
            print(f"  ⚠ Could not set perf permissions: {e}")


class CSVWatcher:
    """Watches a CSV file for phase transitions using tail -f"""

    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.warmup_done_event = threading.Event()
        self.steady_done_event = threading.Event()
        self.tail_process: Optional[subprocess.Popen] = None
        self.watcher_thread: Optional[threading.Thread] = None

    def start(self):
        """Start watching the CSV file using tail -f"""
        # Wait for file to exist (no timeout - just wait)
        print(f"Waiting for CSV file: {self.csv_path}")
        while not self.csv_path.exists():
            time.sleep(0.1)

        self.tail_process = subprocess.Popen(
            ["tail", "-f", "-n", "+1", str(self.csv_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True
        )

        self.watcher_thread = threading.Thread(target=self._read_loop, daemon=True)
        self.watcher_thread.start()
        print(f"Started watching CSV (tail -f): {self.csv_path}")

    def stop(self):
        """Stop watching"""
        if self.tail_process:
            self.tail_process.terminate()
            try:
                self.tail_process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.tail_process.kill()
            self.tail_process = None

        if self.watcher_thread:
            self.watcher_thread.join(timeout=2)

        print("CSV watcher stopped")

    def wait_for_warmup_done(self) -> None:
        """Block until warmup phase is done (no timeout)"""
        self.warmup_done_event.wait()

    def wait_for_steady_done(self) -> None:
        """Block until steady phase is done (no timeout)"""
        self.steady_done_event.wait()

    def _read_loop(self):
        """Read lines from tail -f stdout"""
        if not self.tail_process:
            return

        for line in self.tail_process.stdout:
            line = line.strip()
            if not line or line.startswith("phase,"):
                continue

            self._process_line(line)

            if self.warmup_done_event.is_set() and self.steady_done_event.is_set():
                break

    def _process_line(self, line: str):
        """Process a single CSV line"""
        try:
            parts = line.split(",", 4)
            if len(parts) >= 2:
                phase = parts[0]
                status = parts[1]

                print(f"  [CSV] Phase: {phase}, Status: {status}")

                if phase == "WARMUP" and status == "done":
                    if not self.warmup_done_event.is_set():
                        print("  → WARMUP phase complete, starting monitoring")
                        self.warmup_done_event.set()
                elif phase == "STEADY" and status == "done":
                    if not self.steady_done_event.is_set():
                        print("  → STEADY phase complete, stopping monitoring")
                        self.steady_done_event.set()
        except Exception as e:
            print(f"  [CSV] Parse error: {e}")


class MonitoringManager:
    """Manages background monitoring processes"""

    def __init__(self, work_dir: Path):
        self.work_dir = work_dir
        self.processes = {}
        self.output_files = {}
        self._file_handles = {}
        self.hardware_perf_available = False

    def _check_hardware_perf_events(self) -> bool:
        try:
            result = subprocess.run(
                ["perf", "stat", "-e", "cycles", "--", "sleep", "0.1"],
                capture_output=True,
                text=True,
                timeout=5
            )
            available = "<not supported>" not in result.stderr and "<not counted>" not in result.stderr
            print(f"Hardware perf events: {'AVAILABLE' if available else 'NOT AVAILABLE'}")
            return available
        except Exception as e:
            print(f"Hardware perf check failed: {e}")
            return False

    def start_mpstat(self):
        output_file = self.work_dir / "mpstat.log"
        self.output_files["mpstat"] = output_file
        fh = open(output_file, "w")
        self._file_handles["mpstat"] = fh
        proc = subprocess.Popen(
            ["mpstat", "1"],
            stdout=fh,
            stderr=subprocess.DEVNULL,
            preexec_fn=os.setsid
        )
        self.processes["mpstat"] = proc

    def start_iostat(self):
        output_file = self.work_dir / "iostat.log"
        self.output_files["iostat"] = output_file
        fh = open(output_file, "w")
        self._file_handles["iostat"] = fh
        proc = subprocess.Popen(
            ["iostat", "-x", "1"],
            stdout=fh,
            stderr=subprocess.DEVNULL,
            preexec_fn=os.setsid
        )
        self.processes["iostat"] = proc

    def start_sar_network(self):
        output_file = self.work_dir / "sar_network.log"
        self.output_files["sar_network"] = output_file
        fh = open(output_file, "w")
        self._file_handles["sar_network"] = fh
        proc = subprocess.Popen(
            ["sar", "-n", "DEV", "1"],
            stdout=fh,
            stderr=subprocess.DEVNULL,
            preexec_fn=os.setsid
        )
        self.processes["sar_network"] = proc

    def start_perf_stat(self, pid: int):
        output_file = self.work_dir / "perf_stat.log"
        self.output_files["perf_stat"] = output_file
        fh = open(output_file, "w")
        self._file_handles["perf_stat"] = fh

        self.hardware_perf_available = self._check_hardware_perf_events()

        if self.hardware_perf_available:
            events = "cycles,instructions,cache-references,cache-misses,branch-instructions,branch-misses,context-switches,cpu-migrations,page-faults"
        else:
            events = "context-switches,cpu-migrations,page-faults"

        proc = subprocess.Popen(
            ["perf", "stat", "-e", events, "-p", str(pid)],
            stdout=subprocess.DEVNULL,
            stderr=fh
        )
        self.processes["perf_stat"] = proc
        print(f"Started perf stat on PID {pid}")

    def start_all(self, benchmark_pid: int):
        self.start_mpstat()
        self.start_iostat()
        self.start_sar_network()
        self.start_perf_stat(benchmark_pid)
        print("All monitoring processes started")

    def stop_all(self):
        if "perf_stat" in self.processes:
            perf_proc = self.processes.pop("perf_stat")
            try:
                if perf_proc.poll() is None:
                    perf_proc.send_signal(signal.SIGINT)
                perf_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                perf_proc.kill()
                perf_proc.wait(timeout=2)
            except Exception as e:
                print(f"Warning stopping perf: {e}")

        for name, proc in self.processes.items():
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                proc.wait(timeout=5)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
            except Exception as e:
                print(f"Warning stopping {name}: {e}")

        for fh in self._file_handles.values():
            try:
                fh.close()
            except Exception:
                pass

        print("All monitoring processes stopped")


def parse_mpstat(filepath: Path) -> dict:
    result = {
        "user_percent_avg": 0.0,
        "user_percent_max": 0.0,
        "system_percent_avg": 0.0,
        "system_percent_max": 0.0,
        "idle_percent_avg": 0.0,
        "idle_percent_min": 100.0,
        "iowait_percent_avg": 0.0,
        "steal_percent_avg": 0.0
    }

    if not filepath.exists():
        return result

    user_values, system_values, idle_values, iowait_values, steal_values = [], [], [], [], []

    with open(filepath) as f:
        for line in f:
            if "all" in line and not line.startswith("Average"):
                parts = line.split()
                if len(parts) >= 12:
                    try:
                        user_values.append(float(parts[2]))
                        system_values.append(float(parts[4]))
                        iowait_values.append(float(parts[5]))
                        steal_values.append(float(parts[8]))
                        idle_values.append(float(parts[11]))
                    except (ValueError, IndexError):
                        continue

    if user_values:
        result["user_percent_avg"] = round(sum(user_values) / len(user_values), 1)
        result["user_percent_max"] = round(max(user_values), 1)
    if system_values:
        result["system_percent_avg"] = round(sum(system_values) / len(system_values), 1)
        result["system_percent_max"] = round(max(system_values), 1)
    if idle_values:
        result["idle_percent_avg"] = round(sum(idle_values) / len(idle_values), 1)
        result["idle_percent_min"] = round(min(idle_values), 1)
    if iowait_values:
        result["iowait_percent_avg"] = round(sum(iowait_values) / len(iowait_values), 1)
    if steal_values:
        result["steal_percent_avg"] = round(sum(steal_values) / len(steal_values), 1)

    return result


def parse_iostat(filepath: Path) -> dict:
    result = {"read_bytes": 0, "write_bytes": 0, "read_iops": 0, "write_iops": 0}

    if not filepath.exists():
        return result

    read_kb, write_kb, read_iops, write_iops = [], [], [], []

    with open(filepath) as f:
        in_device_section = False
        for line in f:
            if line.startswith("Device"):
                in_device_section = True
                continue
            if in_device_section and line.strip():
                parts = line.split()
                if len(parts) >= 6 and not parts[0].startswith("loop"):
                    try:
                        read_iops.append(float(parts[1]))
                        write_iops.append(float(parts[2]))
                        read_kb.append(float(parts[3]))
                        write_kb.append(float(parts[4]))
                    except (ValueError, IndexError):
                        continue
            elif in_device_section and not line.strip():
                in_device_section = False

    if read_kb:
        result["read_bytes"] = int(sum(read_kb) * 1024)
        result["read_iops"] = int(sum(read_iops) / len(read_iops))
    if write_kb:
        result["write_bytes"] = int(sum(write_kb) * 1024)
        result["write_iops"] = int(sum(write_iops) / len(write_iops))

    return result


def parse_sar_network(filepath: Path) -> dict:
    result = {"bytes_sent": 0, "bytes_recv": 0, "packets_sent": 0, "packets_recv": 0}

    if not filepath.exists():
        return result

    rx_bytes, tx_bytes, rx_packets, tx_packets = [], [], [], []

    with open(filepath) as f:
        for line in f:
            if "lo" in line or "IFACE" in line or "Average" in line:
                continue
            parts = line.split()
            if len(parts) >= 9:
                try:
                    rx_packets.append(float(parts[2]))
                    tx_packets.append(float(parts[3]))
                    rx_bytes.append(float(parts[4]) * 1024)
                    tx_bytes.append(float(parts[5]) * 1024)
                except (ValueError, IndexError):
                    continue

    if rx_bytes:
        result["bytes_recv"] = int(sum(rx_bytes))
        result["bytes_sent"] = int(sum(tx_bytes))
        result["packets_recv"] = int(sum(rx_packets))
        result["packets_sent"] = int(sum(tx_packets))

    return result


def parse_perf_stat(filepath: Path, hardware_available: bool) -> dict:
    """Parse perf stat output"""
    result = {
        "cpu_cycles": None,
        "instructions": None,
        "ipc": None,
        "cache_references": None,
        "cache_misses": None,
        "cache_miss_rate": None,
        "branch_instructions": None,
        "branch_misses": None,
        "branch_miss_rate": None,
        "context_switches": 0,
        "cpu_migrations": 0,
        "page_faults": 0
    }

    if not filepath.exists():
        return result

    content = filepath.read_text()

    # Software events (always available)
    for key, pattern in [
        ("context_switches", r"([\d,]+)\s+context-switches"),
        ("cpu_migrations", r"([\d,]+)\s+cpu-migrations"),
        ("page_faults", r"([\d,]+)\s+page-faults")
    ]:
        match = re.search(pattern, content)
        if match:
            result[key] = int(match.group(1).replace(",", ""))

    # Hardware events (only on bare metal)
    if hardware_available:
        for key, pattern in [
            ("cpu_cycles", r"([\d,]+)\s+cycles"),
            ("instructions", r"([\d,]+)\s+instructions"),
            ("cache_references", r"([\d,]+)\s+cache-references"),
            ("cache_misses", r"([\d,]+)\s+cache-misses"),
            ("branch_instructions", r"([\d,]+)\s+branch(?:es|-instructions)"),
            ("branch_misses", r"([\d,]+)\s+branch-misses"),
        ]:
            match = re.search(pattern, content)
            if match:
                result[key] = int(match.group(1).replace(",", ""))

        # Calculate derived metrics
        if result["cpu_cycles"] and result["instructions"]:
            result["ipc"] = round(result["instructions"] / result["cpu_cycles"], 2)
        if result["cache_references"] and result["cache_references"] > 0:
            result["cache_miss_rate"] = round(
                100.0 * (result["cache_misses"] or 0) / result["cache_references"], 2
            )
        if result["branch_instructions"] and result["branch_instructions"] > 0:
            result["branch_miss_rate"] = round(
                100.0 * (result["branch_misses"] or 0) / result["branch_instructions"], 2
            )

    return result


def parse_benchmark_csv(filepath: Path) -> Tuple[dict, float]:
    """Parse benchmark CSV - extract STEADY phase done results and elapsed time"""
    operations = {}
    elapsed_seconds = 0.0

    if not filepath.exists():
        return operations, elapsed_seconds

    with open(filepath) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["phase"] == "STEADY" and row["status"] == "done":
                cmd_name = row["command_name"]
                operations[cmd_name] = {
                    "total_requests": int(row["num_requests"]),
                    "successful_requests": int(row["successful_requests"]),
                    "failed_requests": int(row["failed_requests"]),
                    "latency_min_us": int(row["latency_min_us"]),
                    "latency_max_us": int(row["latency_max_us"]),
                    "histogram_buckets": json.loads(row["histogram_json"])
                }
                elapsed_seconds = float(row["time_elapsed"])

    return operations, elapsed_seconds


class BenchmarkRunner:
    """Main benchmark orchestration class"""

    INFRA_CORES = "0-3"

    def __init__(
        self,
        project_dir: Path,
        output_file: Path,
        workload_config: dict,
        client_config: dict,
        skip_infra: bool = False,
        network_delay_ms: int = 1
    ):
        self.project_dir = project_dir
        self.output_file = output_file
        self.workload_config = workload_config
        self.client_config = client_config
        self.skip_infra = skip_infra
        self.network_delay_ms = network_delay_ms
        self.job_id = generate_job_id()
        self.timestamp = get_timestamp()
        self.variance_control = VarianceControl()

        cpu_count = os.cpu_count() or 8
        if cpu_count > 4:
            self.benchmark_cores = f"4-{cpu_count - 1}"
        else:
            self.benchmark_cores = "0-3"
            print(f"WARNING: Only {cpu_count} cores, benchmark shares cores with server")

        print(f"Core allocation: Server={self.INFRA_CORES}, Benchmark={self.benchmark_cores}")

    def start_infrastructure(self):
        if self.skip_infra:
            print("Skipping infrastructure setup")
            return

        print(f"Starting Valkey infrastructure on cores {self.INFRA_CORES}...")

        subprocess.run(["make", "cluster-stop"], cwd=self.project_dir, capture_output=True)
        subprocess.run(["pkill", "-f", "valkey-server"], capture_output=True)
        time.sleep(1)

        work_dir = self.project_dir / "work"
        if work_dir.exists():
            shutil.rmtree(work_dir)

        result = subprocess.run(
            ["taskset", "-c", self.INFRA_CORES, "make", "cluster-init"],
            cwd=self.project_dir,
            timeout=600
        )

        if result.returncode != 0:
            raise RuntimeError(f"Failed to start infrastructure (exit code {result.returncode})")

        time.sleep(2)

        valkey_cli = self.project_dir / "work/valkey/bin/valkey-cli"
        result = subprocess.run([str(valkey_cli), "-p", "7379", "ping"], capture_output=True, text=True)
        if "PONG" not in result.stdout:
            raise RuntimeError("Valkey verification failed")

        print("Valkey infrastructure started and verified")

    def stop_infrastructure(self):
        if self.skip_infra:
            return
        print("Stopping Valkey infrastructure...")
        subprocess.run(["make", "cluster-stop"], cwd=self.project_dir, capture_output=True)
        subprocess.run(["make", "clean"], cwd=self.project_dir, capture_output=True)
        print("Valkey infrastructure stopped")

    def run_benchmark(self, output_csv: Path, workload_config_path: Path) -> subprocess.Popen:
        benchmark_script = self.project_dir / ".github/scripts/mock_benchmark.py"

        cmd = [
            "taskset", "-c", self.benchmark_cores,
            "python3", str(benchmark_script),
            "--workload-config", str(workload_config_path),
            "--output", str(output_csv)
        ]
        print(f"Starting benchmark on cores {self.benchmark_cores}")

        return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def run(self, workload_config_path: Path):
        """Execute the full benchmark workflow"""
        self.variance_control.setup(network_delay_ms=self.network_delay_ms)

        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            benchmark_csv = work_dir / "benchmark_results.csv"

            try:
                self.start_infrastructure()

                print("Starting benchmark...")
                benchmark_proc = self.run_benchmark(benchmark_csv, workload_config_path)

                csv_watcher = CSVWatcher(benchmark_csv)
                csv_watcher.start()

                print("Waiting for WARMUP phase to complete...")
                csv_watcher.wait_for_warmup_done()

                print("Starting monitoring for STEADY phase...")
                monitor = MonitoringManager(work_dir)
                monitor.start_all(benchmark_proc.pid)

                print("Waiting for STEADY phase to complete...")
                csv_watcher.wait_for_steady_done()

                monitor.stop_all()
                csv_watcher.stop()

                stdout, stderr = benchmark_proc.communicate(timeout=10)
                print(f"Benchmark stderr:\n{stderr.decode()}")

                # Parse results
                operations, elapsed_seconds = parse_benchmark_csv(benchmark_csv)
                perf_counters = parse_perf_stat(
                    monitor.output_files.get("perf_stat", Path()),
                    monitor.hardware_perf_available
                )
                cpu_stats = parse_mpstat(monitor.output_files.get("mpstat", Path()))
                disk_stats = parse_iostat(monitor.output_files.get("iostat", Path()))
                network_stats = parse_sar_network(monitor.output_files.get("sar_network", Path()))

                # Copy CSV to output directory
                output_csv_path = self.output_file.with_suffix('.csv')
                if benchmark_csv.exists():
                    shutil.copy(benchmark_csv, output_csv_path)
                    print(f"Benchmark CSV saved to {output_csv_path}")

                # Build result
                result = {
                    "job_id": self.job_id,
                    "timestamp": self.timestamp,
                    "benchmark_config": self.workload_config,
                    "client_config": self.client_config,
                    "elapsed_seconds": int(elapsed_seconds),
                    "operations": operations,
                    "perf": {
                        "counters": perf_counters,
                        "flame_graph_url": f"s3://benchmark-artifacts/{self.job_id}/cpu-flamegraph.svg"
                    },
                    "cpu": cpu_stats,
                    "io": {
                        "disk": disk_stats,
                        "network": network_stats
                    }
                }

                with open(self.output_file, "w") as f:
                    json.dump(result, f, indent=2)

                print(f"\nResults written to {self.output_file}")

            finally:
                self.stop_infrastructure()
                self.variance_control.teardown()


def main():
    parser = argparse.ArgumentParser(description="Benchmark runner")
    parser.add_argument("--output", type=str, default="benchmark_results.json")
    parser.add_argument("--workload-config", type=str, required=True, help="Path to workload JSON config")
    parser.add_argument("--client-config", type=str, required=True, help="Path to client JSON config")
    parser.add_argument("--project-dir", type=str, default=None)
    parser.add_argument("--skip-infra", action="store_true")
    parser.add_argument("--network-delay", type=int, default=1)
    args = parser.parse_args()

    project_dir = Path(args.project_dir) if args.project_dir else Path.cwd()
    output_file = Path(args.output)
    workload_config_path = Path(args.workload_config)
    client_config_path = Path(args.client_config)

    # Load configurations
    workload_config = load_json_config(workload_config_path)
    client_config = load_json_config(client_config_path)

    print(f"Loaded workload config: {workload_config['benchmark-profile']['name']}")
    print(f"Loaded client config: {client_config['client_name']}")

    runner = BenchmarkRunner(
        project_dir=project_dir,
        output_file=output_file,
        workload_config=workload_config,
        client_config=client_config,
        skip_infra=args.skip_infra,
        network_delay_ms=args.network_delay
    )
    runner.run(workload_config_path)


if __name__ == "__main__":
    main()