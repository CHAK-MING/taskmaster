from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence


def compact_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def write_json(path: Path, data: Any, *, pretty: bool = False) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if pretty:
        payload = json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False)
    else:
        payload = compact_json(data)
    path.write_text(payload + "\n", encoding="utf-8")
    return path


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_benchmark_result(
    output_dir: str | Path,
    scenario_name: str,
    run_ids: Sequence[str],
    expected_tasks: int,
    results: Mapping[str, Any],
) -> str:
    path = Path(output_dir) / f"{scenario_name}.latest.json"
    payload = {
        "scenario": scenario_name,
        "run_ids": list(run_ids),
        "expected_tasks": expected_tasks,
        "results": dict(results),
    }
    write_json(path, payload, pretty=True)
    return str(path)

