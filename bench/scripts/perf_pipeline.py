#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from benchlib.artifacts import compact_json, write_json


@dataclass(frozen=True)
class BenchSpec:
    config_id: int
    label: str
    benchmark_filter: str
    description: str


BENCH_SPECS: dict[int, BenchSpec] = {
    1: BenchSpec(
        config_id=1,
        label="full_path_fake_executor",
        benchmark_filter="BM_FullPathFakeExecutorThroughput/8/10",
        description="End-to-end scheduler + executor handoff",
    ),
    2: BenchSpec(
        config_id=2,
        label="execution_dispatch_storm",
        benchmark_filter="BM_ExecutionDispatchStorm/1/50/20/2",
        description="Execution dispatch pressure under bursty completions",
    ),
    3: BenchSpec(
        config_id=3,
        label="cron_production_path",
        benchmark_filter="BM_CronProductionPathThroughput/8/10",
        description="Scheduler trigger to run handoff path",
    ),
    5: BenchSpec(
        config_id=5,
        label="scheduler_catchup_schedule_execute",
        benchmark_filter="BM_SchedulerCatchupScheduleAndExecute/1/1000/0",
        description="Scheduler catchup plus execution handoff",
    ),
    6: BenchSpec(
        config_id=6,
        label="runtime_cross_shard_contention",
        benchmark_filter="BM_RuntimeCrossShardContention/4/1000",
        description="Cross-shard runtime contention path",
    ),
}

DEFAULT_PERF_EVENTS = [
    "cycles",
    "instructions",
    "cache-misses",
    "branch-misses",
    "context-switches",
    "cpu-migrations",
    "page-faults",
]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def bench_binary() -> Path:
    return Path(
        os.environ.get(
            "DAGFORGE_PERF_PIPELINE_BENCH_BIN",
            str(repo_root() / "build" / "bin" / "dagforge_benchmarks"),
        )
    )


def results_root() -> Path:
    return Path(
        os.environ.get(
            "DAGFORGE_PERF_PIPELINE_RESULTS_DIR",
            str(repo_root() / "bench_results" / "perf_pipeline"),
        )
    )


def run_tag() -> str:
    override = os.environ.get("DAGFORGE_PERF_PIPELINE_RUN_TAG")
    if override:
        return override
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"perf_pipeline_{now}_{os.getpid()}"


def selected_events() -> list[str]:
    raw = os.environ.get("DAGFORGE_PERF_PIPELINE_EVENTS", "")
    if not raw.strip() or raw.strip().lower() == "default":
        return list(DEFAULT_PERF_EVENTS)
    events = [item.strip() for item in raw.split(",")]
    return [item for item in events if item]


def benchmark_min_time() -> str:
    return os.environ.get("DAGFORGE_PERF_PIPELINE_BENCH_MIN_TIME", "0.05s")


def benchmark_repetitions() -> str:
    return os.environ.get("DAGFORGE_PERF_PIPELINE_BENCH_REPETITIONS", "1")


def spec_for_config(config_id: int) -> BenchSpec:
    try:
        return BENCH_SPECS[config_id]
    except KeyError as exc:
        raise SystemExit(f"unknown config_id: {config_id}") from exc


def parse_perf_value(raw: str) -> float | None:
    text = raw.strip()
    if not text or text.startswith("<"):
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def parse_perf_summary(perf_csv: Path) -> dict[str, Any]:
    counters: dict[str, dict[str, Any]] = {}
    with perf_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter=",")
        for row in reader:
            if not row or row[0].startswith("#"):
                continue
            event = row[2].strip() if len(row) > 2 else ""
            if not event:
                continue
            counters[event] = {
                "count": parse_perf_value(row[0]) if row else None,
                "unit": row[1].strip() if len(row) > 1 else "",
                "runtime": parse_perf_value(row[3]) if len(row) > 3 else None,
                "percent": parse_perf_value(row[4]) if len(row) > 4 else None,
                "scale": parse_perf_value(row[5]) if len(row) > 5 else None,
                "descriptor": row[6].strip() if len(row) > 6 else "",
            }
    return counters


def load_benchmark_summary(bench_json: Path) -> dict[str, Any]:
    payload = json.loads(bench_json.read_text(encoding="utf-8"))
    benchmarks = payload.get("benchmarks", [])
    if not benchmarks:
        raise RuntimeError(f"benchmark JSON missing benchmarks: {bench_json}")

    entry = next(
        (item for item in benchmarks if item.get("run_type") == "iteration"),
        benchmarks[0],
    )
    time_unit = entry.get("time_unit") or "ns"
    scale_to_us = {
        "ns": 1e-3,
        "us": 1.0,
        "ms": 1e3,
        "s": 1e6,
    }.get(time_unit, 1.0)
    counters = entry.get("counters", {})
    counter_map: dict[str, float] = {}
    for name, value in counters.items():
        if isinstance(value, dict) and "value" in value:
            try:
                counter_map[name] = float(value["value"])
            except (TypeError, ValueError):
                continue
        elif isinstance(value, (int, float)):
            counter_map[name] = float(value)

    return {
        "name": entry.get("name"),
        "run_name": entry.get("run_name"),
        "run_type": entry.get("run_type"),
        "iterations": int(entry.get("iterations") or 0),
        "repetitions": int(entry.get("repetitions") or 0),
        "real_time_us": float(entry.get("real_time") or 0.0) * scale_to_us,
        "cpu_time_us": float(entry.get("cpu_time") or 0.0) * scale_to_us,
        "time_unit": time_unit,
        "items_per_second": float(entry.get("items_per_second") or 0.0),
        "benchmark_counters": counter_map,
    }


def emit_prepare() -> int:
    payload = {
        "run_tag": run_tag(),
        "config_ids": sorted(BENCH_SPECS),
        "benchmarks": {
            str(spec.config_id): {
                "label": spec.label,
                "benchmark_filter": spec.benchmark_filter,
                "description": spec.description,
            }
            for spec in BENCH_SPECS.values()
        },
    }
    sys.stdout.write(compact_json(payload) + "\n")
    return 0


def run_benchmark(config_id: int, run_tag_value: str) -> dict[str, Any]:
    spec = spec_for_config(config_id)
    root = repo_root()
    bench_bin = bench_binary()
    if not bench_bin.exists():
        raise RuntimeError(f"benchmark binary not found: {bench_bin}")

    artifact_root = results_root() / run_tag_value / f"config_{config_id}_{spec.label}"
    artifact_root.mkdir(parents=True, exist_ok=True)
    bench_json = artifact_root / "benchmark.json"
    perf_csv = artifact_root / "perf.csv"
    stdout_log = artifact_root / "benchmark.stdout.log"
    stderr_log = artifact_root / "benchmark.stderr.log"

    events = ",".join(selected_events())
    cmd = [
        "perf",
        "stat",
        "-x,",
        "-e",
        events,
        "-o",
        str(perf_csv),
        "--",
        str(bench_bin),
        f"--benchmark_filter={spec.benchmark_filter}",
        f"--benchmark_min_time={benchmark_min_time()}",
        f"--benchmark_repetitions={benchmark_repetitions()}",
        "--benchmark_out_format=json",
        f"--benchmark_out={bench_json}",
    ]

    proc = subprocess.run(
        cmd,
        cwd=str(root),
        check=False,
        text=True,
        capture_output=True,
    )
    stdout_log.write_text(proc.stdout, encoding="utf-8")
    stderr_log.write_text(proc.stderr, encoding="utf-8")
    if proc.returncode != 0:
        raise RuntimeError(
            "perf benchmark failed "
            f"(config_id={config_id}, exit_code={proc.returncode})\n"
            f"stderr:\n{proc.stderr}"
        )
    if not bench_json.exists():
        raise RuntimeError(f"benchmark JSON not produced: {bench_json}")
    if not perf_csv.exists():
        raise RuntimeError(f"perf CSV not produced: {perf_csv}")

    benchmark = load_benchmark_summary(bench_json)
    perf = parse_perf_summary(perf_csv)
    payload = {
        "run_tag": run_tag_value,
        "config_id": config_id,
        "label": spec.label,
        "description": spec.description,
        "benchmark_filter": spec.benchmark_filter,
        "artifacts": {
            "dir": str(artifact_root),
            "benchmark_json": str(bench_json),
            "perf_csv": str(perf_csv),
            "stdout_log": str(stdout_log),
            "stderr_log": str(stderr_log),
        },
        "benchmark": benchmark,
        "perf": perf,
    }
    write_json(artifact_root / "summary.json", payload)
    return payload


def analyze_payload(payload: dict[str, Any], run_tag_value: str) -> dict[str, Any]:
    benchmark = payload["benchmark"]
    perf = payload["perf"]

    cycles = perf.get("cycles", {}).get("count")
    instructions = perf.get("instructions", {}).get("count")
    cache_misses = perf.get("cache-misses", {}).get("count")
    branch_misses = perf.get("branch-misses", {}).get("count")

    ipc = None
    if isinstance(cycles, (int, float)) and cycles > 0 and isinstance(
        instructions, (int, float)
    ):
        ipc = float(instructions) / float(cycles)

    cache_miss_rate = None
    if isinstance(cache_misses, (int, float)) and isinstance(instructions, (int, float)):
        if instructions > 0:
            cache_miss_rate = float(cache_misses) / float(instructions)

    branch_miss_rate = None
    if isinstance(branch_misses, (int, float)) and isinstance(instructions, (int, float)):
        if instructions > 0:
            branch_miss_rate = float(branch_misses) / float(instructions)

    notes: list[str] = []
    if ipc is not None:
        if ipc < 1.0:
            notes.append("stall-heavy")
        elif ipc > 2.0:
            notes.append("compute-efficient")
    if cache_miss_rate is not None and cache_miss_rate > 0.01:
        notes.append("cache-sensitive")
    if branch_miss_rate is not None and branch_miss_rate > 0.01:
        notes.append("branch-sensitive")
    if not notes:
        notes.append("steady")

    analysis = {
        "run_tag": run_tag_value,
        "config_id": payload["config_id"],
        "label": payload["label"],
        "description": payload["description"],
        "benchmark_filter": payload["benchmark_filter"],
        "artifact_dir": payload["artifacts"]["dir"],
        "benchmark_name": benchmark["name"],
        "real_time_us": benchmark["real_time_us"],
        "cpu_time_us": benchmark["cpu_time_us"],
        "items_per_second": benchmark["items_per_second"],
        "iterations": benchmark["iterations"],
        "benchmark_counters": benchmark["benchmark_counters"],
        "perf": {
            "cycles": cycles,
            "instructions": instructions,
            "cache_misses": cache_misses,
            "branch_misses": branch_misses,
            "context_switches": perf.get("context-switches", {}).get("count"),
            "cpu_migrations": perf.get("cpu-migrations", {}).get("count"),
            "page_faults": perf.get("page-faults", {}).get("count"),
        },
        "derived": {
            "ipc": ipc,
            "cache_miss_rate": cache_miss_rate,
            "branch_miss_rate": branch_miss_rate,
        },
        "notes": notes,
        "status": "ok",
    }
    artifact_dir = Path(payload["artifacts"]["dir"])
    write_json(artifact_dir / "analysis.json", analysis)
    return analysis


def fmt_number(value: Any, precision: int = 2) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        if abs(value) >= 1000:
            return f"{value:,.{precision}f}"
        return f"{value:.{precision}f}"
    return str(value)


def render_markdown(rows: list[dict[str, Any]], run_tag_value: str) -> str:
    headers = [
        "Config",
        "Benchmark",
        "Real time (us)",
        "Items/s",
        "IPC",
        "Cache miss %",
        "Branch miss %",
        "Notes",
    ]
    lines = [
        f"# Perf Pipeline Report",
        "",
        f"Run tag: `{run_tag_value}`",
        "",
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["config_id"]),
                    row["benchmark_name"],
                    fmt_number(row["real_time_us"]),
                    fmt_number(row["items_per_second"], 2),
                    fmt_number(row["derived"]["ipc"], 3),
                    fmt_number(row["derived"]["cache_miss_rate"], 4),
                    fmt_number(row["derived"]["branch_miss_rate"], 4),
                    ", ".join(row["notes"]),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "Artifacts are under the per-run `bench_results/perf_pipeline/<run_tag>/` tree.",
            "",
        ]
    )
    return "\n".join(lines)


def emit_report(analysis_jsons: list[str], run_tag_value: str) -> int:
    rows = [json.loads(item) for item in analysis_jsons]
    rows.sort(key=lambda row: row["config_id"])
    if not rows:
        raise SystemExit("report expects at least one analysis payload")

    markdown = render_markdown(rows, run_tag_value)
    report_dir = results_root() / run_tag_value
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "report.md"
    report_path.write_text(markdown + "\n", encoding="utf-8")

    fastest = min(rows, key=lambda row: row["real_time_us"])
    slowest = max(rows, key=lambda row: row["real_time_us"])
    payload = {
        "run_tag": run_tag_value,
        "status": "ok",
        "report_path": str(report_path),
        "rows": rows,
        "summary": {
            "fastest_config_id": fastest["config_id"],
            "slowest_config_id": slowest["config_id"],
            "benchmark_count": len(rows),
        },
        "markdown": markdown,
    }
    write_json(report_dir / "report.json", payload)
    sys.stdout.write(compact_json(payload) + "\n")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Perf-backed pipeline helper")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("prepare", help="Emit the pipeline spec as compact JSON")

    bench = sub.add_parser("benchmark", help="Run a benchmark under perf stat")
    bench.add_argument("--config-id", type=int, required=True)
    bench.add_argument("--run-tag", default="")

    analyze = sub.add_parser("analyze", help="Analyze a benchmark payload")
    analyze.add_argument("--payload", required=True)
    analyze.add_argument("--run-tag", default="")

    report = sub.add_parser("report", help="Render a final report")
    report.add_argument("--analysis-json", action="append", default=[])
    report.add_argument("--run-tag", default="")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.cmd == "prepare":
        return emit_prepare()

    tag = args.run_tag or os.environ.get("DAGFORGE_PERF_PIPELINE_RUN_TAG") or run_tag()

    if args.cmd == "benchmark":
        payload = run_benchmark(args.config_id, tag)
        sys.stdout.write(compact_json(payload) + "\n")
        return 0

    if args.cmd == "analyze":
        payload = json.loads(args.payload)
        analysis = analyze_payload(payload, tag)
        sys.stdout.write(compact_json(analysis) + "\n")
        return 0

    if args.cmd == "report":
        return emit_report(args.analysis_json, tag)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
