#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

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
    executor: str | None = None,
    command: str | None = None,
    dependencies: list[str] | None = None,
    trigger_rule: str | None = None,
    docker_image: str | None = None,
    pull_policy: str | None = None,
    sensor_type: str | None = None,
    target: str | None = None,
    xcom_pull: list[dict[str, Any]] | None = None,
    xcom_push: list[dict[str, str]] | None = None,
) -> list[str]:
    lines = ["[[tasks]]", f'id = "{task_id}"']
    if name:
        lines.append(f'name = "{name}"')
    if executor:
        lines.append(f'executor = "{executor}"')
    if command is not None:
        lines.append(f"command = '{command}'")
    if dependencies:
        deps = ", ".join(f'"{dep}"' for dep in dependencies)
        lines.append(f"dependencies = [{deps}]")
    if trigger_rule:
        lines.append(f'trigger_rule = "{trigger_rule}"')
    if docker_image:
        lines.append(f'docker_image = "{docker_image}"')
    if pull_policy:
        lines.append(f'pull_policy = "{pull_policy}"')
    if sensor_type:
        lines.append(f'sensor_type = "{sensor_type}"')
    if target:
        lines.append(f'target = "{target}"')
    lines.append("")
    if xcom_pull:
        for pull in xcom_pull:
            lines.extend(
                [
                    "[[tasks.xcom_pull]]",
                    f'key = "{pull["key"]}"',
                    f'from = "{pull["from"]}"',
                    f'env = "{pull["env"]}"',
                ]
            )
            if pull.get("required"):
                lines.append("required = true")
            if "default_value" in pull:
                lines.append(f'default_value = {pull["default_value"]}')
            lines.append("")
    if xcom_push:
        for push in xcom_push:
            lines.extend(
                [
                    "[[tasks.xcom_push]]",
                    f'key = "{push["key"]}"',
                    f'source = "{push["source"]}"',
                ]
            )
            if "json_pointer" in push:
                lines.append(f'json_pointer = "{push["json_pointer"]}"')
            if "regex" in push:
                lines.append(f'regex = "{push["regex"]}"')
            lines.append("")
    return lines


def emit_docker_scene(bench_root: Path) -> None:
    scenario = bench_root / "scene15_docker_mixed_1x20"
    reset_dir(scenario)

    dag_id = "scene15_docker_mixed_1x20_dag_0"
    lines: list[str] = emit_header(
        "Docker Mixed Bench 1x20",
        dag_id,
        "Docker executor bench with 20 tasks spread across three stages",
    )
    lines.extend(
        emit_task(
            "prepare",
            name="Prepare Docker Bench",
            command='printf "scene15-run={{ds}}\\n"',
            xcom_push=[{"key": "run_tag", "source": "stdout"}],
        )
    )

    docker_image = "alpine:3.20"
    for stage, previous_stage in (("a", None), ("b", "a"), ("c", "b")):
        for idx in range(6):
            task_id = f"docker_{stage}_{idx}"
            deps = ["prepare"] if previous_stage is None else [f"docker_{previous_stage}_{idx}"]
            prev_ref = (
                "{{xcom.prepare.run_tag}}"
                if previous_stage is None
                else f"{{{{xcom.docker_{previous_stage}_{idx}.result}}}}"
            )
            command = (
                'printf "stage={stage} idx={idx} dag=__DAG_ID__ run=__RUN_ID__ '
                'prev={prev} task=__TASK_ID__\\n"'
            ).format(stage=stage, idx=idx, prev=prev_ref)
            command = (
                command.replace("__DAG_ID__", "{{dag_id}}")
                .replace("__RUN_ID__", "{{run_id}}")
                .replace("__TASK_ID__", "{{task_id}}")
            )
            lines.extend(
                emit_task(
                    task_id,
                    name=f"Docker {stage.upper()} {idx}",
                    executor="docker",
                    command=command,
                    dependencies=deps,
                    docker_image=docker_image,
                    pull_policy="IfNotPresent",
                    xcom_pull=[
                        {
                            "key": "run_tag" if previous_stage is None else "result",
                            "from": "prepare" if previous_stage is None else f"docker_{previous_stage}_{idx}",
                            "env": "RUN_TAG" if previous_stage is None else "UPSTREAM_RESULT",
                            "required": True,
                        }
                    ],
                    xcom_push=[{"key": "result", "source": "stdout"}],
                )
            )

    lines.extend(
        emit_task(
            "report",
            name="Render Docker Bench Report",
            command='printf "docker bench report run={{xcom.prepare.run_tag}} last={{xcom.docker_c_5.result}}\\n"',
            dependencies=[f"docker_c_{idx}" for idx in range(6)],
            trigger_rule="all_done",
            xcom_pull=[
                {"key": "run_tag", "from": "prepare", "env": "RUN_TAG", "required": True},
                {"key": "result", "from": "docker_c_5", "env": "LAST_RESULT", "required": True},
            ],
            xcom_push=[{"key": "report", "source": "stdout"}],
        )
    )

    write(scenario / f"{dag_id}.toml", "\n".join(lines).rstrip() + "\n")


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
                xcom_push=[{"key": "marker", "source": "stdout"}],
            )
        )
        lines.extend(
            emit_task(
                "validate",
                name="Validate",
                command='printf "validate {{xcom.extract.marker}}\\n"',
                dependencies=["extract"],
                xcom_pull=[{"key": "marker", "from": "extract", "env": "EXTRACT_MARKER", "required": True}],
                xcom_push=[{"key": "marker", "source": "stdout"}],
            )
        )
        lines.extend(
            emit_task(
                "transform",
                name="Transform",
                command='printf "transform {{xcom.validate.marker}}\\n"',
                dependencies=["validate"],
                xcom_pull=[{"key": "marker", "from": "validate", "env": "VALIDATE_MARKER", "required": True}],
                xcom_push=[{"key": "marker", "source": "stdout"}],
            )
        )
        lines.extend(
            emit_task(
                "publish",
                name="Publish",
                command='printf "publish {{dag_id}} {{xcom.transform.marker}}\\n"',
                dependencies=["transform"],
                xcom_pull=[{"key": "marker", "from": "transform", "env": "TRANSFORM_MARKER", "required": True}],
                xcom_push=[{"key": "marker", "source": "stdout"}],
            )
        )
        lines.extend(
            emit_task(
                "cleanup",
                name="Cleanup",
                command='printf "cleanup {{dag_id}} {{run_id}}\\n"',
                dependencies=["extract", "validate", "transform", "publish"],
                trigger_rule="all_done",
                xcom_push=[{"key": "marker", "source": "stdout"}],
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
    bench_root = Path(args.bench_root)
    emit_docker_scene(bench_root)
    emit_cron_scene(bench_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
