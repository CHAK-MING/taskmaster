from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shutil

from benchlib.subprocess_utils import run_command


OFFICIAL_TOTAL_LAG_S: dict[str, dict[str, float]] = {
    "scene1_linear_100x10": {"airflow_2_0_beta": 11.6, "airflow_1_10_10": 200.0},
    "scene2_linear_10x100": {"airflow_2_0_beta": 14.3, "airflow_1_10_10": 144.0},
    "scene3_tree_100x10": {"airflow_2_0_beta": 12.0, "airflow_1_10_10": 200.0},
}

SCENE_CLASSIFICATION: dict[str, str] = {
    "scene1_linear_100x10": "宽 DAG / 并行释放敏感",
    "scene2_linear_10x100": "深 DAG / 长关键路径敏感",
    "scene3_tree_100x10": "树型 DAG / 多后继传播敏感",
    "scene4_burst_ready_1x100": "超宽层 burst-ready / ready 洪峰释放敏感",
    "scene5_burst_ready_1x500": "超宽层 burst-ready / ready 洪峰释放敏感",
    "scene6_burst_ready_1x1000": "超宽层 burst-ready / ready 洪峰释放敏感",
    "scene7_diamond_100x10": "菱形 DAG / fan-out 后 fan-in 敏感",
    "scene8_fanout_100x10": "扇出 DAG / 单点释放敏感",
    "scene9_fanin_100x10": "扇入 DAG / 多前驱汇聚敏感",
    "scene10_mesh_100x10": "网状 DAG / 密集依赖传播敏感",
    "scene12_perf_pipeline_1x3": "真实场景 / perf pipeline stability",
    "scene13_perf_pipeline_mixed_1x21": "真实场景 / perf + XCom + sensor + trigger rules",
}

BURST_SWEEP_SCENARIOS = [
    "scene4_burst_ready_1x100",
    "scene5_burst_ready_1x500",
    "scene6_burst_ready_1x1000",
]

REALISTIC_SWEEP_SCENARIOS = [
    "scene7_diamond_100x10",
    "scene8_fanout_100x10",
    "scene9_fanin_100x10",
    "scene10_mesh_100x10",
]


@dataclass(frozen=True)
class ScenarioMeta:
    mode: str = "manual"
    expected_runs_per_dag: int = 1
    description: str = ""


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def airflow_dags_root() -> Path:
    return repo_root() / "bench" / "airflow_dags"


def ensure_generated_scenarios(scenario_dir: Path) -> None:
    if scenario_dir.exists():
        return
    root = repo_root()
    run_command(["python3", "bench/scripts/gen_airflow_bench_dags.py"], cwd=root)
    run_command(["python3", "bench/scripts/gen_realistic_bench_dags.py"], cwd=root)


def scenario_dag_ids(scenario_dir: str | Path) -> list[str]:
    scenario_path = Path(scenario_dir)
    return sorted(path.stem for path in scenario_path.glob("*.toml"))


def load_scenario_meta(scenario_dir: str | Path) -> ScenarioMeta:
    meta_path = Path(scenario_dir) / "bench.meta.json"
    if not meta_path.exists():
        return ScenarioMeta()
    payload = json.loads(meta_path.read_text(encoding="utf-8"))
    return ScenarioMeta(
        mode=str(payload.get("mode", "manual")),
        expected_runs_per_dag=int(payload.get("expected_runs_per_dag", 1)),
        description=str(payload.get("description", "")),
    )


def count_expected_tasks(scenario_dir: str | Path) -> int:
    total = 0
    for path in Path(scenario_dir).glob("*.toml"):
        total += path.read_text(encoding="utf-8").count("[[tasks]]")
    return total


def expected_dag_ids(root: Path, scenario_name: str) -> set[str]:
    scenario_dir = root / "bench" / "airflow_dags" / scenario_name
    ensure_generated_scenarios(scenario_dir)
    if not scenario_dir.exists():
        raise RuntimeError(f"scenario directory not found: {scenario_dir}")
    return {path.stem for path in scenario_dir.glob("*.toml")}


def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)

