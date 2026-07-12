#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path

from benchlib.scenarios import reset_dir


REPO_ROOT = Path(__file__).resolve().parents[2]
BENCH_ROOT = REPO_ROOT / "bench" / "airflow_dags"


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def emit_header(
    name: str,
    dag_name: str,
    description: str,
    extra: list[str] | None = None,
) -> list[str]:
    lines = [
        f'id = "{dag_name}"',
        f'name = "{name}"',
        f'description = "{description}"',
        "max_active_runs = 1",
    ]
    if extra:
        lines.extend(extra)
    lines.append("")
    return lines


def emit_task(
    task_id: str,
    *,
    name: str | None = None,
    command: str | None = None,
    dependencies: list[str] | None = None,
    trigger_rule: str | None = None,
) -> list[str]:
    lines = ["[[tasks]]", f'id = "{task_id}"']
    if name:
        lines.append(f'name = "{name}"')
    if command is not None:
        lines.append(f"command = '{command}'")
    if dependencies:
        deps = ", ".join(f'"{dep}"' for dep in dependencies)
        lines.append(f"dependencies = [{deps}]")
    if trigger_rule:
        lines.append(f'trigger_rule = "{trigger_rule}"')
    lines.append("")
    return lines


def emit_cron_scene(bench_root: Path) -> None:
    scenario = bench_root / "scene16_cron_autoschedule_6x5"
    reset_dir(scenario)

    for i in range(6):
        dag_id = f"scene16_cron_autoschedule_6x5_dag_{i}"
        lines: list[str] = emit_header(
            f"Cron Autoschedule Bench {i}",
            dag_id,
            "Cron auto-schedule bench with five tasks and a cleanup trigger-rule tail",
            extra=['cron = "*/1 * * * *"', "start_date = 2026-03-21", "catchup = false"],
        )
        lines.extend(
            emit_task(
                "extract",
                name="Extract",
                command='printf "extract {{dag_id}} {{ds}} {{run_id}}\\n"',
            )
        )
        lines.extend(
            emit_task(
                "validate",
                name="Validate",
                command='printf "validate {{dag_id}} {{run_id}}\\n"',
                dependencies=["extract"],
            )
        )
        lines.extend(
            emit_task(
                "transform",
                name="Transform",
                command='printf "transform {{dag_id}} {{run_id}}\\n"',
                dependencies=["validate"],
            )
        )
        lines.extend(
            emit_task(
                "publish",
                name="Publish",
                command='printf "publish {{dag_id}} {{run_id}}\\n"',
                dependencies=["transform"],
            )
        )
        lines.extend(
            emit_task(
                "cleanup",
                name="Cleanup",
                command='printf "cleanup {{dag_id}} {{run_id}}\\n"',
                dependencies=["extract", "validate", "transform", "publish"],
                trigger_rule="all_done",
            )
        )
        write(scenario / f"{dag_id}.toml", "\n".join(lines).rstrip() + "\n")

    meta = {
        "mode": "auto_schedule",
        "expected_runs_per_dag": 1,
        "description": "Wait for scheduler-created runs instead of manual trigger",
    }
    write(scenario / "bench.meta.json", json.dumps(meta, indent=2, sort_keys=True) + "\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate realistic benchmark DAG scenarios")
    parser.add_argument(
        "--bench-root",
        default=str(BENCH_ROOT),
        help="Output root for generated realistic benchmark DAGs",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    emit_cron_scene(Path(args.bench_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
