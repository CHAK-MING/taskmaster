from __future__ import annotations

from typing import Sequence


def sql_quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def build_run_ids_clause(run_ids: Sequence[str]) -> str:
    if not run_ids:
        raise ValueError("run_ids must not be empty")
    return ",".join(sql_quote(run_id) for run_id in run_ids)


def build_latest_attempts_cte(
    run_ids: Sequence[str], *, include_attempt: bool, include_state: bool
) -> str:
    ranked_columns = [
        "            ti.run_rowid,",
        "            ti.task_rowid,",
    ]
    latest_columns = [
        "            run_rowid,",
        "            task_rowid,",
    ]
    if include_attempt:
        ranked_columns.append("            ti.attempt,")
        latest_columns.append("            attempt,")
    if include_state:
        ranked_columns.append("            ti.state,")
        latest_columns.append("            state,")
    ranked_columns.extend(
        [
            "            ti.started_at,",
            "            ti.finished_at,",
            "            ROW_NUMBER() OVER (",
            "                PARTITION BY ti.run_rowid, ti.task_rowid",
            "                ORDER BY ti.attempt DESC",
            "            ) AS attempt_rank",
        ]
    )
    latest_columns.extend(
        [
            "            started_at,",
            "            finished_at",
        ]
    )

    run_ids_clause = build_run_ids_clause(run_ids)
    return "\n".join(
        [
            "    WITH RankedAttempts AS (",
            "        SELECT",
            *ranked_columns,
            "        FROM task_instances ti",
            "        JOIN dag_runs r ON r.run_rowid = ti.run_rowid",
            f"        WHERE r.dag_run_id IN ({run_ids_clause}) AND ti.attempt > 0",
            "    ),",
            "    LatestTaskInstances AS (",
            "        SELECT",
            *latest_columns,
            "        FROM RankedAttempts",
            "        WHERE attempt_rank = 1",
            "    )",
        ]
    ) + "\n"


def build_task_lag_query(run_ids: Sequence[str]) -> str:
    return (
        build_latest_attempts_cte(run_ids, include_attempt=True, include_state=True)
        + """
    ,
    TaskDeps AS (
        SELECT
            ti.run_rowid,
            ti.task_rowid,
            ti.started_at AS task_started_at,
            COUNT(td.dep_rowid) AS dep_count,
            MAX(up_ti.finished_at) AS max_upstream_finished_at
        FROM LatestTaskInstances ti
        JOIN dag_runs r ON r.run_rowid = ti.run_rowid
        LEFT JOIN task_dependencies td
            ON td.dag_rowid = r.dag_rowid
            AND td.task_rowid = ti.task_rowid
        LEFT JOIN LatestTaskInstances up_ti
            ON up_ti.run_rowid = ti.run_rowid
            AND up_ti.task_rowid = td.depends_on_task_rowid
        GROUP BY ti.run_rowid, ti.task_rowid, ti.started_at
    ),
    LagCalc AS (
        SELECT
            td.run_rowid,
            td.task_rowid,
            td.task_started_at,
            COALESCE(td.max_upstream_finished_at, r.started_at) AS dependency_met_at,
            CASE
                WHEN td.task_started_at > 0 AND (
                    td.dep_count = 0 OR COALESCE(td.max_upstream_finished_at, 0) > 0
                ) THEN GREATEST(
                    td.task_started_at - COALESCE(td.max_upstream_finished_at, r.started_at),
                    0
                )
                ELSE NULL
            END AS lag_ms
        FROM TaskDeps td
        JOIN dag_runs r ON r.run_rowid = td.run_rowid
    )
    SELECT
        COUNT(*) AS total_tasks,
        COALESCE(SUM(CASE WHEN lag_ms IS NOT NULL THEN 1 ELSE 0 END), 0) AS lag_tasks,
        COALESCE(SUM(lag_ms), 0) AS total_lag_ms,
        COALESCE(AVG(lag_ms), 0) AS avg_lag_ms,
        COALESCE(MAX(lag_ms), 0) AS max_lag_ms
    FROM LagCalc;
    """
    )


def build_task_persistence_query(run_ids: Sequence[str]) -> str:
    return (
        build_latest_attempts_cte(run_ids, include_attempt=True, include_state=True)
        + """
    SELECT
        COUNT(*) AS total_tasks,
        COALESCE(
            SUM(CASE WHEN state NOT IN (0, 1, 5) THEN 1 ELSE 0 END),
            0
        ) AS finished_tasks
    FROM LatestTaskInstances;
    """
    )


def build_dependency_summary_query(run_ids: Sequence[str]) -> str:
    return f"""
    SELECT COUNT(*) AS dependency_edges
    FROM task_dependencies td
    JOIN dag_runs r ON r.dag_rowid = td.dag_rowid
    WHERE r.dag_run_id IN ({build_run_ids_clause(run_ids)});
    """


def build_lag_breakdown_query(run_ids: Sequence[str]) -> str:
    return (
        build_latest_attempts_cte(run_ids, include_attempt=True, include_state=False)
        + """
    ,
    TaskDeps AS (
        SELECT
            ti.run_rowid,
            ti.task_rowid,
            ti.started_at AS task_started_at,
            COUNT(td.dep_rowid) AS dep_count,
            MAX(up_ti.finished_at) AS max_upstream_finished_at
        FROM LatestTaskInstances ti
        JOIN dag_runs r ON r.run_rowid = ti.run_rowid
        LEFT JOIN task_dependencies td
            ON td.dag_rowid = r.dag_rowid
            AND td.task_rowid = ti.task_rowid
        LEFT JOIN LatestTaskInstances up_ti
            ON up_ti.run_rowid = ti.run_rowid
            AND up_ti.task_rowid = td.depends_on_task_rowid
        GROUP BY ti.run_rowid, ti.task_rowid, ti.started_at
    ),
    LagCalc AS (
        SELECT
            CASE WHEN dep_count = 0 THEN 'root' ELSE 'downstream' END AS task_kind,
            CASE
                WHEN task_started_at > 0 AND (
                    dep_count = 0 OR COALESCE(max_upstream_finished_at, 0) > 0
                ) THEN GREATEST(
                    task_started_at - COALESCE(max_upstream_finished_at, r.started_at),
                    0
                )
                ELSE NULL
            END AS lag_ms
        FROM TaskDeps td
        JOIN dag_runs r ON r.run_rowid = td.run_rowid
    )
    SELECT
        task_kind,
        COUNT(lag_ms) AS tasks,
        AVG(lag_ms) AS avg_lag_ms,
        SUM(lag_ms) AS total_lag_ms,
        MAX(lag_ms) AS max_lag_ms
    FROM LagCalc
    GROUP BY task_kind
    """
    )


def build_task_lag_distribution_query(run_ids: Sequence[str]) -> str:
    return (
        build_latest_attempts_cte(run_ids, include_attempt=True, include_state=False)
        + """
    ,
    TaskDeps AS (
        SELECT
            ti.run_rowid,
            ti.task_rowid,
            ti.started_at AS task_started_at,
            COUNT(td.dep_rowid) AS dep_count,
            MAX(up_ti.finished_at) AS max_upstream_finished_at
        FROM LatestTaskInstances ti
        JOIN dag_runs r ON r.run_rowid = ti.run_rowid
        LEFT JOIN task_dependencies td
            ON td.dag_rowid = r.dag_rowid
            AND td.task_rowid = ti.task_rowid
        LEFT JOIN LatestTaskInstances up_ti
            ON up_ti.run_rowid = ti.run_rowid
            AND up_ti.task_rowid = td.depends_on_task_rowid
        WHERE ti.started_at > 0
        GROUP BY ti.run_rowid, ti.task_rowid, ti.started_at
    )
    SELECT
        CASE
            WHEN task_started_at > 0 AND (
                dep_count = 0 OR COALESCE(max_upstream_finished_at, 0) > 0
            ) THEN GREATEST(
                task_started_at - COALESCE(max_upstream_finished_at, r.started_at),
                0
            )
            ELSE NULL
        END AS lag_ms,
        dep_count
    FROM TaskDeps td
    JOIN dag_runs r ON r.run_rowid = td.run_rowid
    """
    )
