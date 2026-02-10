#!/usr/bin/env python3
"""
Mock benchmark application that generates fake benchmark results.
Reads workload configuration and generates aligned results.
Outputs one JSON object per line (JSONL) to the metrics file.

Each line has the structure:
{
  "phase": {"id": "STEADY", "status": "COMPLETED", ...},
  "totals": {"requests": 2000000, "errors": 50},
  "metrics": {
    "SET": {
      "requests": 800000,
      "errors": 20,
      "latency": {
        "unit": "us",
        "count": 799980,
        "summary": {"min": 55, "p50": 140, "p95": 260, "p99": 400, "p999": 900, "max": 12000},
        "histogram": {"format": "hdr", "lowest": 1, "highest": 60000000, "sigfig": 3, "payload_b64": "...", "buckets": [[100, 16000], ...]}
      }
    }
  }
}
"""

import argparse
import base64
import json
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def get_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def cpu_work(iterations: int = 1000):
    result = 0.0
    for i in range(iterations):
        result += math.sin(i) * math.cos(i)
    return result


def load_workload_config(filepath: Path) -> dict:
    with open(filepath) as f:
        return json.load(f)


def get_phase_config(workload_config: dict, phase_id: str) -> dict:
    for phase in workload_config.get("phases", []):
        if phase["id"] == phase_id:
            return phase
    return {}


def get_phase_duration(phase_config: dict, default_duration: int = 60) -> int:
    completion = phase_config.get("completion", {})
    if completion.get("type") == "duration":
        return completion.get("seconds", default_duration)
    else:
        total_requests = completion.get("requests", 1000000)
        rps_limit = phase_config.get("rps_limit", -1)
        if rps_limit > 0:
            return max(1, total_requests // rps_limit)
        return default_duration


def get_phase_requests(phase_config: dict) -> int:
    completion = phase_config.get("completion", {})
    if completion.get("type") == "requests":
        return completion.get("requests", 1000000)
    else:
        duration = completion.get("seconds", 60)
        rps_limit = phase_config.get("rps_limit", -1)
        if rps_limit > 0:
            return duration * rps_limit
        else:
            return phase_config.get("keyspace", {}).get("keys_count", 100000)


def generate_fake_hdr_payload() -> str:
    """Generate a fake base64-encoded HDR histogram payload.
    In the real Java app this would be an actual HdrHistogram serialization."""
    fake_data = b"MOCK_HDR_HISTOGRAM_V2_ENC"
    return base64.b64encode(fake_data).decode("ascii")


def generate_buckets(latency_profile: list, count: int) -> list:
    """Generate histogram buckets as [[upper_bound_us, count], ...] pairs."""
    buckets = []
    remaining = count
    for i, (upper_bound, ratio) in enumerate(latency_profile):
        if i == len(latency_profile) - 1:
            bucket_count = remaining
        else:
            bucket_count = int(count * ratio)
            remaining -= bucket_count
        buckets.append([upper_bound, max(0, bucket_count)])
    return buckets


def compute_percentile(latency_profile: list, percentile: float) -> int:
    """Approximate a percentile value from [(upper_bound, ratio), ...] distribution."""
    cumulative = 0.0
    prev_bound = 0
    for upper_bound, ratio in latency_profile:
        cumulative += ratio
        if cumulative >= percentile:
            if ratio > 0:
                fraction = (percentile - (cumulative - ratio)) / ratio
            else:
                fraction = 0.5
            return int(prev_bound + fraction * (upper_bound - prev_bound))
        prev_bound = upper_bound
    return int(latency_profile[-1][0])


LATENCY_PROFILES = {
    "SET": [
        (100, 0.02), (200, 0.35), (300, 0.40), (500, 0.15),
        (1000, 0.05), (2000, 0.02), (5000, 0.01)
    ],
    "GET": [
        (100, 0.03), (200, 0.45), (300, 0.35), (500, 0.10),
        (1000, 0.04), (2000, 0.02), (5000, 0.01)
    ]
}

MIN_LATENCY = {"SET": 55, "GET": 45}
MAX_LATENCY = {"SET": 12000, "GET": 9000}


def generate_command_metrics(command: str, total_requests: int) -> dict:
    """Generate full metrics for a single command."""
    errors = max(0, int(total_requests * 0.00001))
    successful = total_requests - errors

    profile = LATENCY_PROFILES.get(command, LATENCY_PROFILES["GET"])
    min_lat = MIN_LATENCY.get(command, 45)
    max_lat = MAX_LATENCY.get(command, 9000)

    summary = {
        "min": min_lat,
        "p50": compute_percentile(profile, 0.50),
        "p95": compute_percentile(profile, 0.95),
        "p99": compute_percentile(profile, 0.99),
        "p999": compute_percentile(profile, 0.999),
        "max": max_lat
    }

    buckets = generate_buckets(profile, successful)

    return {
        "requests": total_requests,
        "errors": errors,
        "latency": {
            "unit": "us",
            "count": successful,
            "summary": summary,
            "histogram": {
                "format": "hdr",
                "lowest": 1,
                "highest": 60000000,
                "sigfig": 3,
                "payload_b64": generate_fake_hdr_payload(),
                "buckets": buckets
            }
        }
    }


def generate_phase_metrics(phase_config: dict, total_requests: int) -> dict:
    """Generate metrics dict keyed by command name."""
    commands = phase_config.get("commands", [])
    total_weight = sum(cmd.get("weight", 1.0) for cmd in commands)
    metrics = {}

    for cmd in commands:
        command = cmd.get("command", "").upper()
        weight = cmd.get("weight", 1.0)
        cmd_requests = int(total_requests * (weight / total_weight))
        metrics[command] = generate_command_metrics(command, cmd_requests)

    return metrics


def write_record(file_handle, phase_id: str, status: str, start_timestamp: str,
                 duration_ms: int, connections: int, metrics: dict):
    """Write a single JSONL record to the metrics file."""
    total_requests = sum(m["requests"] for m in metrics.values())
    total_errors = sum(m["errors"] for m in metrics.values())

    record = {
        "phase": {
            "id": phase_id,
            "status": status,
            "start_timestamp": start_timestamp,
            "duration_ms": duration_ms,
            "connections": connections
        },
        "totals": {
            "requests": total_requests,
            "errors": total_errors
        },
        "metrics": metrics
    }

    file_handle.write(json.dumps(record, separators=(",", ":")) + "\n")
    file_handle.flush()


def main():
    parser = argparse.ArgumentParser(description="Mock benchmark application")
    parser.add_argument("--workload-config", type=str, required=True,
                        help="Path to workload JSON config")
    parser.add_argument("--output", type=str, required=True,
                        help="Output JSONL metrics file path")
    args = parser.parse_args()

    workload_config = load_workload_config(Path(args.workload_config))
    profile_name = workload_config.get("benchmark_profile", {}).get("name", "Unknown")

    warmup_config = get_phase_config(workload_config, "WARMUP")
    steady_config = get_phase_config(workload_config, "STEADY")

    warmup_duration = get_phase_duration(warmup_config, default_duration=10)
    steady_duration = get_phase_duration(steady_config, default_duration=60)

    warmup_connections = warmup_config.get("connections", 50)
    steady_connections = steady_config.get("connections", 500)

    print(f"Mock benchmark starting", file=sys.stderr)
    print(f"  Workload: {profile_name}", file=sys.stderr)
    print(f"  WARMUP: {warmup_config.get('completion', {})} ({warmup_duration}s)", file=sys.stderr)
    print(f"  STEADY: {steady_config.get('completion', {})} ({steady_duration}s)", file=sys.stderr)
    print(f"  Output: {args.output}", file=sys.stderr)

    with open(args.output, "w") as f:

        # ========== WARMUP PHASE ==========
        print(f"Starting WARMUP phase ({warmup_duration}s)...", file=sys.stderr)
        warmup_start = get_timestamp()
        warmup_start_time = time.time()

        write_record(f, "WARMUP", "RUNNING", warmup_start, 0, warmup_connections, {})

        warmup_end = time.time() + warmup_duration
        warmup_iterations = 0
        while time.time() < warmup_end:
            cpu_work(500)
            warmup_iterations += 1

        warmup_elapsed_ms = int((time.time() - warmup_start_time) * 1000)
        warmup_total_requests = get_phase_requests(warmup_config)
        warmup_metrics = generate_phase_metrics(warmup_config, warmup_total_requests)

        write_record(f, "WARMUP", "COMPLETED", warmup_start, warmup_elapsed_ms,
                     warmup_connections, warmup_metrics)
        print(f"WARMUP phase complete ({warmup_iterations} iterations)", file=sys.stderr)

        time.sleep(0.5)

        # ========== STEADY STATE PHASE ==========
        print(f"Starting STEADY phase ({steady_duration}s)...", file=sys.stderr)
        steady_start = get_timestamp()
        steady_start_time = time.time()

        write_record(f, "STEADY", "RUNNING", steady_start, 0, steady_connections, {})

        steady_end = time.time() + steady_duration
        steady_iterations = 0
        while time.time() < steady_end:
            cpu_work(1000)
            steady_iterations += 1

        steady_elapsed_ms = int((time.time() - steady_start_time) * 1000)
        steady_total_requests = get_phase_requests(steady_config)
        steady_metrics = generate_phase_metrics(steady_config, steady_total_requests)

        write_record(f, "STEADY", "COMPLETED", steady_start, steady_elapsed_ms,
                     steady_connections, steady_metrics)
        print(f"STEADY phase complete ({steady_iterations} iterations)", file=sys.stderr)

    total_time = round(time.time() - warmup_start_time, 2)
    print(f"Mock benchmark complete. Total time: {total_time}s", file=sys.stderr)


if __name__ == "__main__":
    main()