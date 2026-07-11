from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
import time
from typing import Any, Awaitable, Callable, Iterable


@dataclass(frozen=True)
class BenchApiConfig:
    host: str
    port: int

    @classmethod
    def from_env(cls) -> "BenchApiConfig":
        return cls(
            host=os.environ.get("DAGFORGE_BENCH_API_HOST", "127.0.0.1"),
            port=int(os.environ.get("DAGFORGE_BENCH_API_PORT", "8888")),
        )

    @property
    def dags_base(self) -> str:
        return f"http://{self.host}:{self.port}/api/dags"

    @property
    def history_base(self) -> str:
        return f"http://{self.host}:{self.port}/api/history"


async def trigger_dag(session: Any, api_config: BenchApiConfig, dag_id: str) -> str:
    url = f"{api_config.dags_base}/{dag_id}/trigger"
    async with session.post(url, json={}) as resp:
        if resp.status >= 400:
            body = await resp.text()
            raise RuntimeError(
                f"trigger failed for {dag_id}: HTTP {resp.status}: {body}"
            )
        payload = await resp.json()
        dag_run_id = payload.get("dag_run_id")
        if not dag_run_id:
            raise RuntimeError(f"trigger response missing dag_run_id for {dag_id}")
        return str(dag_run_id)


async def get_run_state(session: Any, api_config: BenchApiConfig, run_id: str) -> str | None:
    url = f"{api_config.history_base}/{run_id}"
    async with session.get(url) as resp:
        if resp.status == 200:
            payload = await resp.json()
            state = payload.get("state")
            return str(state) if state is not None else None
        return None


async def get_dag_history(session: Any, api_config: BenchApiConfig, dag_id: str) -> Any:
    url = f"{api_config.dags_base}/{dag_id}/history"
    async with session.get(url) as resp:
        if resp.status >= 400:
            body = await resp.text()
            raise RuntimeError(
                f"failed to fetch dag history for {dag_id}: HTTP {resp.status}: {body}"
            )
        return await resp.json()


def history_run_ids(payload: Any) -> list[str]:
    if isinstance(payload, list):
        runs = payload
    elif isinstance(payload, dict):
        runs = payload.get("runs", [])
    else:
        return []

    run_ids: list[str] = []
    for entry in runs:
        if not isinstance(entry, dict):
            continue
        run_id = entry.get("dag_run_id") or entry.get("run_id")
        if isinstance(run_id, str) and run_id:
            run_ids.append(run_id)
    return run_ids


async def fetch_loaded_dag_ids(session: Any, api_config: BenchApiConfig) -> set[str]:
    async with session.get(api_config.dags_base) as resp:
        if resp.status >= 400:
            body = await resp.text()
            raise RuntimeError(f"failed to list dags: HTTP {resp.status}: {body}")
        payload = await resp.json()
        dags = payload.get("dags", [])
        return {dag.get("dag_id") for dag in dags if dag.get("dag_id")}


async def ensure_scenario_loaded(
    session: Any, api_config: BenchApiConfig, dag_ids: Iterable[str]
) -> None:
    loaded_dags = await fetch_loaded_dag_ids(session, api_config)
    expected = list(dag_ids)
    missing = [dag_id for dag_id in expected if dag_id not in loaded_dags]
    if missing:
        preview = ", ".join(missing[:5])
        raise RuntimeError(
            "scenario DAGs are not loaded in the running server; "
            f"missing={len(missing)} first_missing=[{preview}]"
        )


async def wait_for_runs(
    run_ids: Iterable[str],
    get_state: Callable[[str], Awaitable[str | None]],
    *,
    sleep_fn: Callable[[float], Awaitable[None]] = asyncio.sleep,
    timeout_s: float = 120.0,
    progress_interval_s: float = 5.0,
) -> None:
    run_ids_list = list(run_ids)
    print(f"Waiting for {len(run_ids_list)} DAG runs to complete...")
    pending = set(run_ids_list)
    start_wait = time.monotonic()
    last_progress = start_wait
    while pending:
        states = await asyncio.gather(*(get_state(run_id) for run_id in pending))
        for run_id, state in zip(list(pending), states, strict=False):
            if state and state.lower() in ("success", "failed"):
                pending.remove(run_id)
        if pending:
            now = time.monotonic()
            if now - last_progress >= progress_interval_s:
                preview = ", ".join(sorted(pending)[:3])
                print(
                    f"Still waiting on {len(pending)} DAG runs "
                    f"after {now - start_wait:.1f}s; sample=[{preview}]"
                )
                last_progress = now
            await sleep_fn(0.1)
        if time.monotonic() - start_wait > timeout_s:
            preview = ", ".join(sorted(pending)[:5])
            raise TimeoutError(
                "timed out waiting for DAG runs to complete; "
                f"remaining={len(pending)} sample=[{preview}]"
            )


async def wait_for_auto_scheduled_runs(
    dag_ids: Iterable[str],
    fetch_history: Callable[[str], Awaitable[Any]],
    *,
    expected_runs_per_dag: int = 1,
    sleep_fn: Callable[[float], Awaitable[None]] = asyncio.sleep,
    timeout_s: float = 120.0,
    progress_interval_s: float = 5.0,
) -> list[str]:
    dag_id_list = list(dag_ids)
    print(
        "Waiting for scheduler-created DAG runs to appear "
        f"({expected_runs_per_dag} per DAG)..."
    )
    pending = set(dag_id_list)
    observed: dict[str, list[str]] = {}
    start_wait = time.monotonic()
    last_progress = start_wait
    while pending:
        payloads = await asyncio.gather(*(fetch_history(dag_id) for dag_id in pending))
        for dag_id, payload in zip(list(pending), payloads, strict=False):
            run_ids = history_run_ids(payload)
            if len(run_ids) >= expected_runs_per_dag:
                observed[dag_id] = run_ids[:expected_runs_per_dag]
                pending.remove(dag_id)
        if pending:
            now = time.monotonic()
            if now - last_progress >= progress_interval_s:
                preview = ", ".join(sorted(pending)[:3])
                print(
                    f"Still waiting for {len(pending)} DAG histories "
                    f"after {now - start_wait:.1f}s; sample=[{preview}]"
                )
                last_progress = now
            await sleep_fn(0.5)
        if time.monotonic() - start_wait > timeout_s:
            preview = ", ".join(sorted(pending)[:5])
            raise TimeoutError(
                "timed out waiting for scheduler-created DAG runs; "
                f"remaining={len(pending)} sample=[{preview}]"
            )

    flattened = [run_id for ids in observed.values() for run_id in ids]
    print(f"Observed {len(flattened)} scheduler-created DAG runs.")
    return flattened

