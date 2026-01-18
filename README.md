# TaskMaster

[中文文档](README_CN.md)

A high-performance DAG workflow orchestrator built with modern C++23, inspired by Apache Airflow.

![TaskMaster UI](image/web-ui.png)

## Features

- **DAG Workflows** - Define task dependencies with YAML
- **Trigger Rules** - Airflow-style conditional execution (all_success, all_failed, one_success, etc.)
- **Sensors** - Poll for conditions before proceeding (file existence, HTTP endpoints, commands)
- **Branching** - Conditional task execution based on XCom values
- **Catchup** - Automatic backfill of missed scheduled runs
- **XCom** - Cross-task communication with template variables
- **Scheduling** - Cron expressions with start/end date boundaries
- **Executors** - Shell, Docker, and Sensor execution modes
- **Web UI** - Real-time DAG visualization with React Flow
- **REST API** - Full control and monitoring
- **Persistence** - SQLite with crash recovery

## Quick Start

### Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

### Run

```bash
./build/bin/taskmaster serve -c system_config.yaml
# Web UI: http://localhost:8888
```

### Build Web UI (Optional)

```bash
cd web-ui && npm install && npm run build
```

## DAG Configuration

DAG files in `dags/` directory. Filename becomes DAG ID (e.g., `etl.yaml` → `etl`).

### Basic Structure

```yaml
name: ETL Pipeline
description: Daily data processing
cron: "0 2 * * *"
start_date: "2024-01-01"
catchup: true

tasks:
  - id: extract
    command: ./extract.sh --date {{ds}}
    
  - id: transform
    command: ./transform.sh
    dependencies: [extract]
    trigger_rule: all_success
    
  - id: load
    command: ./load.sh
    dependencies: [transform]
```

### Scheduling Fields

| Field | Description |
|-------|-------------|
| `cron` | Cron expression for automatic scheduling |
| `start_date` | Earliest date to schedule runs (YYYY-MM-DD) |
| `end_date` | Latest date to schedule runs (YYYY-MM-DD) |
| `catchup` | If true, backfill all missed runs since start_date |

### Task Fields

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique task identifier |
| `command` | string | Command to execute (supports template variables) |
| `executor` | string | `shell` (default), `docker`, or `sensor` |
| `dependencies` | array | Task IDs this task depends on |
| `trigger_rule` | string | When to run based on upstream states |
| `is_branch` | bool | If true, XCom output determines which downstream tasks run |
| `timeout` | int | Timeout in seconds (default: 300) |
| `max_retries` | int | Retry attempts on failure (default: 3) |

### Trigger Rules

| Rule | Description |
|------|-------------|
| `all_success` | Run when all upstream tasks succeed (default) |
| `all_failed` | Run when all upstream tasks fail |
| `all_done` | Run when all upstream tasks complete (success or fail) |
| `one_success` | Run when at least one upstream succeeds |
| `one_failed` | Run when at least one upstream fails |
| `none_failed` | Run if no upstream task failed (skipped allowed) |
| `none_skipped` | Run if no upstream task was skipped |

### Template Variables

Available in `command` field:

| Variable | Example | Description |
|----------|---------|-------------|
| `{{ds}}` | 2024-01-15 | Execution date (YYYY-MM-DD) |
| `{{ds_nodash}}` | 20240115 | Execution date without dashes |
| `{{execution_date}}` | 2024-01-15T00:00:00Z | Full ISO timestamp |
| `{{dag_id}}` | etl_pipeline | DAG identifier |
| `{{task_id}}` | extract | Current task identifier |
| `{{run_id}}` | etl_pipeline_20240115 | DAG run identifier |

### Branching

A branch task returns a JSON array of task IDs to execute:

```yaml
tasks:
  - id: check_data
    command: |
      if [ -f /data/large.csv ]; then
        echo '["process_large"]'
      else
        echo '["process_small"]'
      fi
    is_branch: true
    xcom_push:
      - key: branch
        source: json

  - id: process_large
    command: ./process_large.sh
    dependencies: [check_data]

  - id: process_small
    command: ./process_small.sh
    dependencies: [check_data]
```

### Sensors

Sensors poll until a condition is met:

```yaml
tasks:
  - id: wait_for_file
    executor: sensor
    sensor_type: file
    sensor_path: /data/input.csv
    sensor_interval: 30
    timeout: 3600

  - id: wait_for_api
    executor: sensor
    sensor_type: http
    sensor_url: http://api.example.com/ready
    sensor_expected_status: 200

  - id: wait_for_db
    executor: sensor
    sensor_type: command
    command: pg_isready -h localhost
```

| Sensor Type | Required Fields |
|-------------|-----------------|
| `file` | `sensor_path` |
| `http` | `sensor_url`, `sensor_expected_status` |
| `command` | `command` (exit 0 = ready) |

### XCom

Pass data between tasks:

```yaml
tasks:
  - id: producer
    command: echo '{"count": 42, "status": "ok"}'
    xcom_push:
      - key: result
        source: json

  - id: consumer
    command: echo "Processing $COUNT items"
    dependencies: [producer]
    xcom_pull:
      - key: result
        from: producer
        env: COUNT
        json_path: count
```

**Push sources:** `stdout`, `stderr`, `exit_code`, `json`

### Docker Executor

```yaml
tasks:
  - id: python_task
    executor: docker
    docker_image: python:3.11
    command: python -c "print('Hello from Docker')"
```

## System Configuration

`system_config.yaml`:

```yaml
scheduler:
  log_level: info
  tick_interval_ms: 1000
  max_concurrency: 10

storage:
  db_file: taskmaster.db

api:
  enabled: true
  host: 127.0.0.1
  port: 8888

dag_source:
  directory: ./dags
  scan_interval_sec: 60
```

## CLI Commands

```bash
taskmaster serve -c config.yaml    # Start service
taskmaster run --db db.sqlite dag  # Check DAG exists
taskmaster list --db db.sqlite     # List all DAGs
taskmaster validate -c config.yaml # Validate DAG files
taskmaster status --db db.sqlite dag [--run id]  # Show history
```

## REST API

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/status` | System status |
| GET | `/api/dags` | List DAGs |
| GET | `/api/dags/:id` | DAG details |
| POST | `/api/dags/:id/trigger` | Trigger run |
| GET | `/api/history` | Run history |
| GET | `/api/runs/:runId/tasks/:taskId/logs` | Task logs |

## Requirements

- C++23 (GCC 13+ / Clang 17+)
- CMake 3.25+
- Linux with io_uring

## License

MIT
