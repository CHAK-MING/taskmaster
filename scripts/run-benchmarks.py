#!/usr/bin/env python3
"""Run DAGForge benchmarks with repeated measurements and summarize noise."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import platform
import statistics
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


TIME_TO_NS = {
    "ns": 1.0,
    "us": 1_000.0,
    "ms": 1_000_000.0,
    "s": 1_000_000_000.0,
}


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path(".git/benchmarks"))
    parser.add_argument("--filter", default=".*")
    parser.add_argument("--repetitions", type=int, default=7)
    parser.add_argument("--warmup-seconds", type=float, default=0.5)
    parser.add_argument("--min-time-seconds", type=float, default=0.2)
    parser.add_argument(
        "--cpu-set",
        default=os.environ.get("DAGFORGE_BENCH_CPUSET", ""),
        help="Optional taskset CPU list, for example 0-7",
    )
    parser.add_argument(
        "--build-label",
        default=os.environ.get("DAGFORGE_BENCH_BUILD_LABEL", "unspecified"),
        help="Build description recorded with the result, for example O3-native",
    )
    return parser.parse_args()


def capture(command: list[str]) -> str:
    try:
        completed = subprocess.run(
            command,
            cwd=REPOSITORY_ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError:
        return ""
    return completed.stdout.strip() if completed.returncode == 0 else ""


def cpu_model() -> str:
    for line in capture(["lscpu"]).splitlines():
        if line.startswith("Model name:"):
            return line.split(":", 1)[1].strip()
    return platform.processor() or "unknown"


def cpu_governor() -> str:
    governor = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    try:
        return governor.read_text(encoding="utf-8").strip()
    except OSError:
        return "unknown"


def main() -> int:
    args = parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(f"benchmark binary does not exist: {binary}")
    if args.repetitions < 3:
        raise SystemExit("at least 3 repetitions are required")

    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    raw_path = output / "raw.json"
    summary_path = output / "summary.md"
    environment_path = output / "environment.json"

    command = [
        str(binary),
        f"--benchmark_filter={args.filter}",
        f"--benchmark_repetitions={args.repetitions}",
        f"--benchmark_min_warmup_time={args.warmup_seconds}",
        f"--benchmark_min_time={args.min_time_seconds}s",
        "--benchmark_enable_random_interleaving=true",
        "--benchmark_report_aggregates_only=false",
        "--benchmark_out_format=json",
        f"--benchmark_out={raw_path}",
    ]
    if args.cpu_set:
        command = ["taskset", "-c", args.cpu_set, *command]

    completed = subprocess.run(
        command, cwd=REPOSITORY_ROOT, text=True, check=False
    )
    if completed.returncode != 0:
        return completed.returncode

    payload: dict[str, Any] = json.loads(raw_path.read_text(encoding="utf-8"))
    git_commit = capture(["git", "rev-parse", "HEAD"])
    environment = {
        "recorded_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "git_commit": git_commit,
        "git_dirty": bool(capture(["git", "status", "--porcelain"])),
        "binary": str(binary),
        "build_label": args.build_label,
        "command": command,
        "cpu_set": args.cpu_set or "unrestricted",
        "cpu_model": cpu_model(),
        "cpu_governor": cpu_governor(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "benchmark_context": payload.get("context", {}),
    }
    environment_path.write_text(
        json.dumps(environment, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in payload.get("benchmarks", []):
        if entry.get("run_type", "iteration") != "iteration":
            continue
        if "aggregate_name" in entry:
            continue
        groups[entry["name"]].append(entry)

    rows: list[
        tuple[str, float, float, float, float, float, float | None]
    ] = []
    for name, entries in sorted(groups.items()):
        samples_ns = [
            float(entry["real_time"]) * TIME_TO_NS[entry["time_unit"]]
            for entry in entries
        ]
        median_ns = statistics.median(samples_ns)
        p95_ns = percentile(samples_ns, 0.95)
        p99_ns = percentile(samples_ns, 0.99)
        stddev_ns = statistics.stdev(samples_ns) if len(samples_ns) > 1 else 0.0
        cv = stddev_ns / statistics.mean(samples_ns) if samples_ns else 0.0
        throughput_samples = [
            float(entry["items_per_second"])
            for entry in entries
            if "items_per_second" in entry
        ]
        throughput = (
            statistics.median(throughput_samples) if throughput_samples else None
        )
        rows.append(
            (name, median_ns, p95_ns, p99_ns, stddev_ns, cv, throughput)
        )

    with summary_path.open("w", encoding="utf-8") as stream:
        stream.write("# DAGForge benchmark summary\n\n")
        commit_label = git_commit[:12] if git_commit else "unknown"
        dirty_label = " (dirty)" if environment["git_dirty"] else ""
        benchmark_context = environment["benchmark_context"]
        stream.write(
            f"Commit: `{commit_label}`{dirty_label}; build: "
            f"`{args.build_label}`; CPU: {environment['cpu_model']}; "
            f"CPU set: `{environment['cpu_set']}`; governor: "
            f"`{environment['cpu_governor']}`; benchmark library build: "
            f"`{benchmark_context.get('library_build_type', 'unknown')}`.\n\n"
        )
        stream.write(
            f"Repetitions: {args.repetitions}; warmup: {args.warmup_seconds}s; "
            f"minimum measured time: {args.min_time_seconds}s.\n\n"
        )
        stream.write(
            "| Scenario | Median | p95 | p99 | Stddev | CV | Median throughput |\n"
        )
        stream.write("| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
        for name, median_ns, p95_ns, p99_ns, stddev_ns, cv, throughput in rows:
            throughput_text = (
                f"{throughput:,.0f} items/s" if throughput is not None else "—"
            )
            stream.write(
                f"| `{name}` | {median_ns / 1_000:.2f} µs | "
                f"{p95_ns / 1_000:.2f} µs | {p99_ns / 1_000:.2f} µs | "
                f"{stddev_ns / 1_000:.2f} µs | {cv * 100:.2f}% | "
                f"{throughput_text} |\n"
            )

    print(summary_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
