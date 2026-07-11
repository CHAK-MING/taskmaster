#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/dag/run_types.hpp"
#include "dagforge/util/id.hpp"

#include <chrono>
#include <cstdint>
#include <string>
#endif


namespace dagforge::orm {

// ============================================================================
// Table: dags - DAG definition master table
// Primary Key: dag_rowid (BIGINT UNSIGNED AUTO_INCREMENT)
// Business Key: dag_id (VARCHAR UNIQUE)
// ============================================================================
struct DAGRow {
  int64_t dag_rowid{0};    // PRIMARY KEY AUTO_INCREMENT
  std::string dag_id;      // UNIQUE NOT NULL - user-visible identifier
  int version{1};          // Version counter, incremented on definition change
  std::string name;        // NOT NULL
  std::string description; // DEFAULT ''
  std::string tags;        // JSON array for categorization (default '[]')
  std::string cron;        // Cron expression (nullable)
  std::string timezone;    // DEFAULT 'UTC'
  int max_concurrent_runs{1}; // DEFAULT 1
  int catchup{0};             // Whether to backfill missed runs
  int is_active{1};           // DEFAULT 1 (boolean as INTEGER)
  int is_paused{0};           // DEFAULT 0
  int64_t start_date{0};      // Effective start date (0 = no constraint)
  int64_t end_date{0};        // Effective end date (0 = no constraint)
  int64_t created_at{0};      // NOT NULL - Unix timestamp (milliseconds)
  int64_t updated_at{0};      // NOT NULL - Unix timestamp (milliseconds)
  int retention_days{30};     // Data retention policy
};

// ============================================================================
// Table: dag_tasks - Task definitions within a DAG
// Primary Key: task_rowid (BIGINT UNSIGNED AUTO_INCREMENT)
// Foreign Key: dag_rowid -> dags.dag_rowid
// ============================================================================
struct DAGTaskRow {
  int64_t task_rowid{0};       // PRIMARY KEY AUTO_INCREMENT
  int64_t dag_rowid{0};        // FK -> dags.dag_rowid
  std::string task_id;         // Task identifier within DAG
  std::string name;            // DEFAULT ''
  std::string command;         // NOT NULL - shell command or script
  std::string working_dir;     // DEFAULT ''
  std::string executor;        // DEFAULT 'shell'
  std::string executor_config; // JSON: executor-specific config (default '{}')
  int timeout{300};            // DEFAULT 300 (seconds)
  int retry_interval{60};      // DEFAULT 60 (seconds)
  int max_retries{3};          // DEFAULT 3
};

// ============================================================================
// Table: task_dependencies - Task dependency graph (replaces JSON deps field)
// Primary Key: dep_rowid (BIGINT UNSIGNED AUTO_INCREMENT)
// Foreign Keys: dag_rowid, task_rowid, depends_on_task_rowid
// ============================================================================
struct TaskDependencyRow {
  int64_t dep_rowid{0};             // PRIMARY KEY AUTO_INCREMENT
  int64_t dag_rowid{0};             // FK -> dags.dag_rowid
  int64_t task_rowid{0};            // FK -> dag_tasks.task_rowid (downstream)
  int64_t depends_on_task_rowid{0}; // FK -> dag_tasks.task_rowid (upstream)
  std::string dependency_type;      // 'success', 'failure', 'always', 'skip'
};

// ============================================================================
// Table: dag_runs - DAG run instances
// Primary Key: run_rowid (BIGINT UNSIGNED AUTO_INCREMENT)
// Business Key: dag_run_id (VARCHAR UNIQUE)
// Foreign Key: dag_rowid -> dags.dag_rowid
// ============================================================================
struct DAGRunRow {
  int64_t run_rowid{0};   // PRIMARY KEY AUTO_INCREMENT
  std::string dag_run_id; // UNIQUE NOT NULL - business identifier
  int64_t dag_rowid{0};   // FK -> dags.dag_rowid
  int dag_version{1};     // Snapshot of dags.version at trigger time
  std::string state;         // Stored as TINYINT code in MySQL, decoded in DTOs
  std::string trigger_type;  // Stored as TINYINT code in MySQL, decoded in DTOs
  int64_t scheduled_at{0};   // Unix timestamp (milliseconds)
  int64_t started_at{0};     // Unix timestamp (milliseconds)
  int64_t finished_at{0};    // Unix timestamp (milliseconds)
  int64_t execution_date{0}; // Logical execution date (milliseconds)
};

// ============================================================================
// Table: task_instances
// Primary Key: (run_rowid, task_rowid, attempt) - composite key
// Foreign Keys: run_rowid -> dag_runs, task_rowid -> dag_tasks
// ============================================================================
struct TaskInstanceRow {
  int64_t run_rowid{0};  // FK -> dag_runs.run_rowid
  int64_t task_rowid{0}; // FK -> dag_tasks.task_rowid (stable reference)
  int attempt{1};        // Retry attempt number (>= 1)
  std::string state;     // Stored as TINYINT code in MySQL, decoded in DTOs
  int64_t started_at{0};     // Unix timestamp (milliseconds)
  int64_t finished_at{0};    // Unix timestamp (milliseconds)
  int exit_code{0};          // Process exit code
  std::string error_message; // Error details (empty if success)
  std::string
      error_type; // Error classification: TIMEOUT, OOM, EXIT_ERROR, etc.
};

// ============================================================================
// Table: xcom_values (Cross-task Communication)
// Primary Key: xcom_rowid (BIGINT UNSIGNED AUTO_INCREMENT)
// Unique: (run_rowid, task_rowid, key)
// Foreign Keys: run_rowid -> dag_runs, task_rowid -> dag_tasks
// ============================================================================
struct XComValueRow {
  int64_t xcom_rowid{0};  // PRIMARY KEY AUTO_INCREMENT
  int64_t run_rowid{0};   // FK -> dag_runs.run_rowid
  int64_t task_rowid{0};  // FK -> dag_tasks.task_rowid
  std::string key;        // NOT NULL - XCom key
  std::string value;      // NOT NULL - JSON serialized value
  std::string value_type; // 'text', 'json', 'blob'
  int byte_size{0};       // Value size in bytes
  int64_t created_at{0};  // Unix timestamp (milliseconds)
  int64_t expires_at{0};  // Auto-expiry timestamp (0 = no expiry)
};

// ============================================================================
// Table: dag_watermarks (Scheduling watermark tracking)
// Primary Key: dag_rowid (BIGINT UNSIGNED)
// Foreign Key: dag_rowid -> dags.dag_rowid
// ============================================================================
struct DAGWatermarkRow {
  int64_t dag_rowid{0};         // PRIMARY KEY, FK -> dags.dag_rowid
  int64_t last_scheduled_at{0}; // Unix timestamp (milliseconds)
  int64_t last_success_at{0};   // Unix timestamp (milliseconds)
  int64_t last_failure_at{0};   // Unix timestamp (milliseconds)
};

// ============================================================================
// Table: task_logs - Per-task stdout/stderr log lines
// Primary Key: log_rowid (BIGINT UNSIGNED AUTO_INCREMENT)
// Foreign Keys: run_rowid -> dag_runs, task_rowid -> dag_tasks
// ============================================================================
struct TaskLogRow {
  int64_t log_rowid{0};  // PRIMARY KEY AUTO_INCREMENT
  int64_t run_rowid{0};  // FK -> dag_runs.run_rowid
  int64_t task_rowid{0}; // FK -> dag_tasks.task_rowid
  int attempt{1};        // Retry attempt number
  std::string stream;    // 'stdout' or 'stderr'
  int64_t logged_at{0};  // Unix timestamp (milliseconds)
  std::string content;   // Log content
};

// Query DTO: task_logs joined with dag_tasks (for task_id resolution)
struct TaskLogEntry {
  int64_t log_rowid{0};
  DAGId dag_id{};
  DAGRunId dag_run_id{};
  TaskId task_id{};
  int attempt{1};
  std::string stream;
  std::chrono::system_clock::time_point logged_at{};
  std::string content;
};

// Query DTO: dag_runs joined with dags
struct RunHistoryEntry {
  DAGRunId dag_run_id{};
  DAGId dag_id{};
  int64_t dag_rowid{0};
  int64_t run_rowid{0};
  int dag_version{1};
  DAGRunState state{DAGRunState::Running};
  TriggerType trigger_type{TriggerType::Manual};
  std::chrono::system_clock::time_point scheduled_at{};
  std::chrono::system_clock::time_point started_at{};
  std::chrono::system_clock::time_point finished_at{};
  std::chrono::system_clock::time_point execution_date{};
};

} // namespace dagforge::orm
