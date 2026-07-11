from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Mapping


def first_present(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value not in (None, ""):
            return value
    return default


@dataclass(frozen=True)
class BenchDbConfig:
    host: str
    port: int
    user: str
    password: str | None
    database: str

    @classmethod
    def from_env(cls, *, require_password: bool = False) -> "BenchDbConfig":
        password = first_present("DAGFORGE_BENCH_DB_PASS", "DAGFORGE_DB_PASSWORD")
        if require_password and password is None:
            raise RuntimeError(
                "database password is not configured; export DAGFORGE_BENCH_DB_PASS "
                "or DAGFORGE_DB_PASSWORD before running bench scripts"
            )
        return cls(
            host=first_present("DAGFORGE_BENCH_DB_HOST", "DAGFORGE_DB_HOST", default="127.0.0.1")
            or "127.0.0.1",
            port=int(
                first_present("DAGFORGE_BENCH_DB_PORT", "DAGFORGE_DB_PORT", default="3306")
                or "3306"
            ),
            user=first_present("DAGFORGE_BENCH_DB_USER", "DAGFORGE_DB_USERNAME", default="root")
            or "root",
            password=password,
            database=first_present(
                "DAGFORGE_BENCH_DB_NAME",
                "DAGFORGE_DB_DATABASE",
                default="dagforge_perf16",
            )
            or "dagforge_perf16",
        )


def get_bench_db_host() -> str:
    return BenchDbConfig.from_env().host


def get_bench_db_port() -> int:
    return BenchDbConfig.from_env().port


def get_bench_db_user() -> str:
    return BenchDbConfig.from_env().user


def get_bench_db_password() -> str:
    cfg = BenchDbConfig.from_env(require_password=True)
    assert cfg.password is not None
    return cfg.password


def get_bench_db_name() -> str:
    return BenchDbConfig.from_env().database


def open_bench_db_connection(pymysql_module: Any):
    cfg = BenchDbConfig.from_env(require_password=True)
    assert cfg.password is not None
    return pymysql_module.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
    )


def apply_dagforge_db_env(env: Mapping[str, str]) -> dict[str, str]:
    cfg = BenchDbConfig.from_env(require_password=True)
    assert cfg.password is not None
    merged = dict(env)

    def set_if_missing(name: str, value: str) -> None:
        if not merged.get(name):
            merged[name] = value

    set_if_missing("DAGFORGE_DB_HOST", cfg.host)
    set_if_missing("DAGFORGE_DB_PORT", str(cfg.port))
    set_if_missing("DAGFORGE_DB_USERNAME", cfg.user)
    set_if_missing("DAGFORGE_DB_PASSWORD", cfg.password)
    set_if_missing("DAGFORGE_DB_DATABASE", cfg.database)
    return merged
