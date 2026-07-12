#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <string_view>
#endif

namespace dagforge::schema {

// MySQL 8 schema, version 1.
// Uses: AUTO_INCREMENT, BIGINT for timestamps, JSON type, composite PKs.
// All timestamps stored as Unix milliseconds (BIGINT).

inline constexpr std::string_view V1_SCHEMA = R"SQL(

CREATE TABLE IF NOT EXISTS schema_version (
    version INT NOT NULL PRIMARY KEY,
    applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS dags (
    dag_rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    dag_id VARCHAR(255) NOT NULL UNIQUE,
    version INT NOT NULL DEFAULT 1,
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL DEFAULT (''),
    tags JSON NOT NULL DEFAULT ('[]'),
    cron VARCHAR(128) NOT NULL DEFAULT '',
    timezone VARCHAR(64) NOT NULL DEFAULT 'UTC',
    max_concurrent_runs INT NOT NULL DEFAULT 1,
    catchup TINYINT NOT NULL DEFAULT 0,
    is_active TINYINT NOT NULL DEFAULT 1,
    is_paused TINYINT NOT NULL DEFAULT 0,
    start_date BIGINT NOT NULL DEFAULT 0,
    end_date BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    retention_days INT NOT NULL DEFAULT 30,
    INDEX idx_dags_active (is_active),
    INDEX idx_dags_paused (is_paused)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS dag_tasks (
    task_rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    dag_rowid BIGINT UNSIGNED NOT NULL,
    task_id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL DEFAULT '',
    command TEXT NOT NULL,
    working_dir VARCHAR(512) NOT NULL DEFAULT '',
    executor VARCHAR(64) NOT NULL DEFAULT 'shell',
    executor_config JSON NOT NULL DEFAULT ('{}'),
    timeout INT NOT NULL DEFAULT 300,
    retry_interval INT NOT NULL DEFAULT 60,
    max_retries INT NOT NULL DEFAULT 3,
    trigger_rule VARCHAR(64) NOT NULL DEFAULT 'all_success',
    depends_on_past TINYINT NOT NULL DEFAULT 0,
    UNIQUE KEY uq_dag_task (dag_rowid, task_id),
    CONSTRAINT fk_task_dag FOREIGN KEY (dag_rowid) REFERENCES dags(dag_rowid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS task_dependencies (
    dep_rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    dag_rowid BIGINT UNSIGNED NOT NULL,
    task_rowid BIGINT UNSIGNED NOT NULL,
    depends_on_task_rowid BIGINT UNSIGNED NOT NULL,
    dependency_type VARCHAR(32) NOT NULL DEFAULT 'success',
    UNIQUE KEY uq_dep (task_rowid, depends_on_task_rowid),
    CONSTRAINT fk_dep_dag FOREIGN KEY (dag_rowid) REFERENCES dags(dag_rowid) ON DELETE CASCADE,
    CONSTRAINT fk_dep_task FOREIGN KEY (task_rowid) REFERENCES dag_tasks(task_rowid) ON DELETE CASCADE,
    CONSTRAINT fk_dep_upstream FOREIGN KEY (depends_on_task_rowid) REFERENCES dag_tasks(task_rowid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS dag_runs (
    run_rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    dag_run_id VARCHAR(255) NOT NULL UNIQUE,
    dag_rowid BIGINT UNSIGNED NOT NULL,
    dag_version INT NOT NULL DEFAULT 1,
    state TINYINT UNSIGNED NOT NULL DEFAULT 0,
    trigger_type TINYINT UNSIGNED NOT NULL DEFAULT 0,
    scheduled_at BIGINT NOT NULL DEFAULT 0,
    started_at BIGINT NOT NULL DEFAULT 0,
    finished_at BIGINT NOT NULL DEFAULT 0,
    execution_date BIGINT NOT NULL DEFAULT 0,
    INDEX idx_runs_dag (dag_rowid),
    INDEX idx_runs_state (state),
    INDEX idx_runs_exec_date (dag_rowid, execution_date),
    CONSTRAINT fk_run_dag FOREIGN KEY (dag_rowid) REFERENCES dags(dag_rowid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS task_instances (
    run_rowid BIGINT UNSIGNED NOT NULL,
    task_rowid BIGINT UNSIGNED NOT NULL,
    attempt INT NOT NULL DEFAULT 1,
    state TINYINT UNSIGNED NOT NULL DEFAULT 0,
    worker_id VARCHAR(128) NOT NULL DEFAULT '',
    last_heartbeat BIGINT NOT NULL DEFAULT 0,
    execution_date BIGINT NOT NULL DEFAULT 0,
    started_at BIGINT NOT NULL DEFAULT 0,
    finished_at BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL DEFAULT 0,
    exit_code INT NOT NULL DEFAULT 0,
    error_message TEXT NOT NULL DEFAULT (''),
    error_type VARCHAR(64) NOT NULL DEFAULT '',
    PRIMARY KEY (run_rowid, task_rowid, attempt),
    INDEX idx_ti_state (state),
    INDEX idx_ti_worker (worker_id),
    INDEX idx_ti_queue (state, execution_date),
    INDEX idx_ti_history (task_rowid, execution_date, attempt),
    INDEX idx_ti_state_heartbeat (state, last_heartbeat),
    CONSTRAINT fk_ti_run FOREIGN KEY (run_rowid) REFERENCES dag_runs(run_rowid) ON DELETE CASCADE,
    CONSTRAINT fk_ti_task FOREIGN KEY (task_rowid) REFERENCES dag_tasks(task_rowid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS dag_watermarks (
    dag_rowid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    last_scheduled_at BIGINT NOT NULL DEFAULT 0,
    last_success_at BIGINT NOT NULL DEFAULT 0,
    last_failure_at BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT fk_wm_dag FOREIGN KEY (dag_rowid) REFERENCES dags(dag_rowid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS task_logs (
    log_rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_rowid BIGINT UNSIGNED NOT NULL,
    task_rowid BIGINT UNSIGNED NOT NULL,
    attempt INT NOT NULL DEFAULT 1,
    stream VARCHAR(16) NOT NULL DEFAULT 'stdout',
    logged_at BIGINT NOT NULL DEFAULT 0,
    content MEDIUMTEXT NOT NULL,
    INDEX idx_tl_run_task (run_rowid, task_rowid, attempt),
    INDEX idx_tl_logged_at (run_rowid, logged_at),
    CONSTRAINT fk_tl_run FOREIGN KEY (run_rowid) REFERENCES dag_runs(run_rowid) ON DELETE CASCADE,
    CONSTRAINT fk_tl_task FOREIGN KEY (task_rowid) REFERENCES dag_tasks(task_rowid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

)SQL";

inline constexpr int CURRENT_SCHEMA_VERSION = 4;

} // namespace dagforge::schema
