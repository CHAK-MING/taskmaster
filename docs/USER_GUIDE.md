> [!NOTE]
> **DAGForge is in Active Development.**
> We are building a high-performance DAG workflow orchestrator built with modern C++23.

<div align="center">

# DAGForge User Guide

[English](../README.md) | [简体中文](../README_CN.md) | [API Reference](API.md)

</div>

In-depth usage, patterns, and troubleshooting. For a quick overview see the [README](../README.md).

---

## 📑 Table of Contents

1. [First-time Setup](#1-first-time-setup)
2. [Running the Service](#2-running-the-service)
3. [Viewing Logs](#3-viewing-logs)
4. [DAG Hot-Reload](#4-dag-hot-reload)
5. [Trigger Rules — When to Use Each](#5-trigger-rules--when-to-use-each)
6. [XCom — Complete Examples](#6-xcom--complete-examples)
7. [Sensor Tasks](#7-sensor-tasks)
8. [Docker Tasks](#8-docker-tasks)
9. [Retries and Timeouts](#9-retries-and-timeouts)
10. [Branching DAGs](#10-branching-dags)
11. [Inspecting Runs](#11-inspecting-runs)
12. [Testing Individual Tasks](#12-testing-individual-tasks)
13. [Re-running Failed Tasks](#13-re-running-failed-tasks)
14. [Database Maintenance](#14-database-maintenance)
15. [Scripting / JSON Output](#15-scripting--json-output)
16. [CLI Cheatsheet](#16-cli-cheatsheet)
17. [Troubleshooting](#17-troubleshooting)

---

## 1. First-time Setup

### 🛠️ Prerequisites

- Linux (x86-64 or ARM64)
- GCC 15+ or Clang 19+
- build2 0.17+ (`b`, `bdep`, `bpkg`)
- Boost 1.87+
- MySQL 8.0+

> [!NOTE]
> Source builds are supported through **build2**. This repository does not ship
> a supported CMake configuration.

### 🗄️ MySQL Setup

```sql
-- Run as MySQL root
CREATE DATABASE dagforge CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'dagforge'@'%' IDENTIFIED BY 'dagforge';
GRANT ALL PRIVILEGES ON dagforge.* TO 'dagforge'@'%';
FLUSH PRIVILEGES;
```

### 🏗️ Build

```bash
./scripts/setup-build2.sh
./scripts/build.sh
# Binary: ./bin/dagforge
```

### ⚙️ Configure

The `system_config.toml` file maps to the internal configuration structures.

```toml
[compute]
threads = 0                 # 0 = auto-detect, dedicated CPU workers
queue_capacity = 1024       # bounded admission queue
pin_threads_to_cores = false
cpu_affinity_offset = 0

[scheduler]
log_level = "info"
log_file = ""
pid_file = ""
tick_interval_ms = 1000
max_concurrency = 10
shards = 0
scheduler_shards = 1
pin_shards_to_cores = false
cpu_affinity_offset = 0
zombie_reaper_interval_sec = 60
zombie_heartbeat_timeout_sec = 300

[database]
host = "127.0.0.1"
port = 3306
username = "dagforge"
password = "dagforge"
database = "dagforge"
pool_size = 4
connect_timeout = 5

[api]
enabled = true
host = "127.0.0.1"
port = 8888
reuse_port = false
tls_enabled = false
tls_cert_file = ""
tls_key_file = ""

[dag_source]
mode = "File"
directory = "./dags"
scan_interval_sec = 60
```

```bash
cp system_config.toml my_config.toml
# Edit [database] with your credentials
export DAGFORGE_CONFIG=my_config.toml   # skip -c on every command
```

### 🗃️ Initialize the Database Schema

```bash
dagforge db init
```

> [!TIP]
> This is **idempotent** — safe to run multiple times. Run `db migrate` after upgrading DAGForge to apply new schema changes.

### ✅ Validate DAG Files

```bash
dagforge validate          # all DAGs in [dag_source] directory
dagforge validate -f dags/hello_world.toml   # single file
```

---

## 2. Running the Service

### Foreground (Development)

```bash
dagforge serve start -c system_config.toml
# Logs go to stderr; API at http://127.0.0.1:8888
```

> [!TIP]
> Set `export DAGFORGE_CONFIG=system_config.toml` to skip `-c` on every command.

### Background Daemon (Production)

```bash
dagforge serve start -c system_config.toml -d --log-file /var/log/dagforge.log
```

The process detaches and redirects all output to the log file. `--log-file` is required when using `--daemon`.
`-d` is a shorthand for `--daemon`.

Check status or stop it with:

```bash
dagforge serve status -c system_config.toml
dagforge serve stop -c system_config.toml
# custom stop timeout (default: 10s)
dagforge serve stop -c system_config.toml --timeout 30
```

### Disable the API

```bash
dagforge serve start -c system_config.toml --no-api   # scheduler only, no HTTP server
```

### Log Levels

The default is `info`. To see scheduler internals:

```bash
dagforge serve start -c system_config.toml --log-level debug
```

### Shard Count Override

By default, DAGForge auto-detects CPU cores and uses that shard count. To override:

```bash
dagforge serve start -c system_config.toml --shards 4
```

Or set it permanently in `system_config.toml`:

```toml
[scheduler]
shards = 4
```

---

## 3. Viewing Logs

Task stdout and stderr are captured line-by-line and stored in the MySQL `task_logs` table. They are available immediately after each line is written.

### View all logs for the latest run

```bash
dagforge logs hello_world --latest -c system_config.toml
```

### View logs for a specific run

```bash
dagforge logs daily_etl --run <run_id> -c system_config.toml
```

### View logs for a specific task

```bash
dagforge logs daily_etl --latest --task extract -c system_config.toml
```

### Follow logs in real time (like `tail -f`)

```bash
dagforge logs daily_etl --latest -f -c system_config.toml
```

This polls and exits automatically when the run reaches a terminal state (success, failed, cancelled).

### View logs for a retry attempt

```bash
dagforge logs daily_etl --latest --task extract --attempt 2 -c system_config.toml
```

### JSON output (for scripting)

```bash
dagforge logs hello_world --latest --json -c system_config.toml
# Each line is a JSON object: {"task_id":"greet","attempt":1,"stream":"stdout","logged_at":"...","content":"..."}
```

### Via REST API

```bash
# All logs for a run
curl http://localhost:8888/api/runs/hello_world%2F2026-01-20T10:00:00/logs

# Logs for a specific task
curl "http://localhost:8888/api/runs/hello_world%2F2026-01-20T10:00:00/tasks/greet/logs?attempt=1"
```

### 🤷‍♂️ Why are there no logs?

Logs are only present if:
1. `dagforge db init` was run (creates the `task_logs` table).
2. The run happened **after** the logging system was active (v0.1.0-beta+).
3. The task actually produced stdout/stderr output.

Check that the run exists first:

```bash
dagforge inspect hello_world --latest
```

If the run shows `success` but `logs` shows nothing, the run predates the log table. Trigger a new run.

---

## 4. DAG Hot-Reload

The service scans the DAG source directory every `scan_interval_sec` seconds (default: 30). Changes to `.toml` files are picked up automatically — **no restart required**.

```toml
[dag_source]
directory         = "./dags"
scan_interval_sec = 60    # set to 0 to scan only at startup
```

**What triggers a reload:**
- **New `.toml` file added** → DAG registered in DB and scheduler.
- **Existing file modified** → DAG definition updated; in-flight runs use the old version.
- **File deleted** → DAG marked inactive in DB; no new runs scheduled. The DB record is kept. Use `dagforge db prune-stale` to remove it.

**To force an immediate reload** without waiting for the interval, restart the service. A `reload` command is planned for a future release.

---

## 5. Trigger Rules — When to Use Each

Trigger rules control **when a task becomes eligible to run** based on the states of its upstream dependencies.

| Rule | Runs when… | Typical use case |
|---|---|---|
| `all_success` | All upstreams succeeded | Normal pipeline step *(default)* |
| `all_failed` | All upstreams failed | Fallback / compensating action |
| `all_done` | All upstreams finished (any state) | Cleanup that must always run |
| `one_success` | ≥1 upstream succeeded | Fan-in after branching |
| `one_failed` | ≥1 upstream failed | Alert on first failure |
| `one_done` | ≥1 upstream finished | Early-exit aggregation |
| `none_failed` | No upstream failed (success or skipped OK) | Lenient pipeline — skips are fine |
| `none_skipped` | No upstream was skipped | Strict pipeline — all must run |
| `none_failed_min_one_success` | No failures AND ≥1 success | Safe fan-in with skips allowed |
| `all_done_min_one_success` | All done AND ≥1 success | Aggregate after mixed results |
| `all_skipped` | All upstreams were skipped | Branch not taken handler |
| `always` | Regardless of upstream state | Notifications, teardown |

### Pattern: cleanup task that always runs

```toml
[[tasks]]
id           = "cleanup"
command      = "rm -rf /tmp/work_dir"
dependencies = ["step_a", "step_b", "step_c"]
trigger_rule = "all_done"   # runs even if step_a failed
```

### Pattern: alert only on failure

```toml
[[tasks]]
id           = "alert"
command      = "curl -X POST $SLACK_WEBHOOK -d '{\"text\":\"Pipeline failed!\"}'"
dependencies = ["extract", "transform", "load"]
trigger_rule = "one_failed"
```

### Pattern: fan-in after branching

```toml
[[tasks]]
id           = "merge"
command      = "./merge.sh"
dependencies = ["branch_large", "branch_small"]
trigger_rule = "one_success"   # whichever branch ran
```

### Pattern: lenient pipeline (skips are OK)

```toml
[[tasks]]
id           = "report"
command      = "./report.sh"
dependencies = ["optional_enrich", "core_transform"]
trigger_rule = "none_failed"   # runs if core_transform succeeded, even if optional_enrich was skipped
```

---

## 6. XCom — Complete Examples

XCom (cross-communication) lets tasks share data. Values are stored in MySQL (`xcom_values` table) and resolved at task startup via template substitution.

### Example 1: Pass a count between tasks

```toml
id = "count_pipeline"

[[tasks]]
id      = "count_rows"
command = "wc -l < /data/input.csv | tr -d ' '"

[[tasks.xcom_push]]
key    = "row_count"
source = "stdout"       # captures the last non-empty stdout line

[[tasks]]
id           = "process"
command      = "echo Processing {{xcom.count_rows.row_count}} rows"
dependencies = ["count_rows"]
```

### Example 2: Inject XCom as environment variable

```toml
[[tasks]]
id      = "get_api_token"
command = "vault read -field=token secret/api"

[[tasks.xcom_push]]
key    = "token"
source = "stdout"

[[tasks]]
id           = "call_api"
command      = "curl -H \"Authorization: Bearer $API_TOKEN\" https://api.example.com/data"
dependencies = ["get_api_token"]

[[tasks.xcom_pull]]
key      = "token"
from     = "get_api_token"
env      = "API_TOKEN"
required = true           # task fails immediately if key is missing
```

### Example 3: Extract with regex

```toml
[[tasks]]
id      = "build"
command = "make && echo 'Build version: 1.4.2'"

[[tasks.xcom_push]]
key   = "version"
source = "stdout"
regex = "Build version: (\\S+)"   # capture group 1 is stored
```

### 📋 XCom Push Fields

| Field | Description |
|---|---|
| `key` | Key name to store the value under |
| `source` | `stdout` \| `stderr` \| `json` \| `exit_code` |
| `regex` | Regex pattern; capture group 1 is stored |
| `json_pointer` | RFC 6901 JSON Pointer, for example `/result/items/0` |

### 🔍 XCom Sources

| `source` | What is captured |
|---|---|
| `stdout` | Last non-empty line of stdout |
| `stderr` | Last non-empty line of stderr |
| `json` | Full stdout parsed as JSON (used for branching) |
| `exit_code` | Task exit code as a string |

---

## 7. Sensor Tasks

Sensors poll an external condition and block until it is met (or timeout).

```toml
[[tasks]]
id            = "wait_for_file"
executor      = "sensor"
sensor_type   = "file"
target        = "/data/input/ready.flag"
sensor_interval = 10      # poll every 10 seconds
timeout       = 3600      # fail after 1 hour
soft_fail     = false     # true = timeout → skipped instead of failed
```

### File sensor

```toml
executor      = "sensor"
sensor_type   = "file"
target        = "/path/to/file"
```

Succeeds when the file exists.

### HTTP sensor

```toml
executor             = "sensor"
sensor_type          = "http"
target               = "https://api.example.com/status"
sensor_http_method   = "GET"
sensor_expected_status = 200
```

Succeeds when the HTTP response status matches `sensor_expected_status`.

### Command sensor

```toml
executor      = "sensor"
sensor_type   = "command"
target        = "pg_isready -h db.internal"
```

Succeeds when the command exits with code 0.

### Soft-fail sensors

Set `soft_fail = true` to treat a timeout as `skipped` rather than `failed`. Useful for optional upstream dependencies:

```toml
[[tasks]]
id            = "wait_for_optional_feed"
executor      = "sensor"
sensor_type   = "file"
target        = "/data/optional_feed.csv"
timeout       = 300
soft_fail     = true

[[tasks]]
id           = "process"
command      = "./process.sh"
dependencies = ["wait_for_optional_feed"]
trigger_rule = "none_failed"   # runs even if sensor was skipped
```

---

## 8. Docker Tasks

Run tasks inside a Docker container. Docker fields are set directly on the task:

```toml
[[tasks]]
id           = "transform"
executor     = "docker"
command      = "python /app/transform.py --date {{ds}}"
docker_image = "myorg/etl:latest"
pull_policy  = "IfNotPresent"   # Never | IfNotPresent | Always
docker_socket = "/var/run/docker.sock"   # optional, default shown
```

Logs are captured from the container's stdout/stderr. There is no volume or env mapping in the TOML format; pass environment via the command string or use XCom.

---

## 9. Retries and Timeouts

Default values (applied unless overridden per-task or via `[default_args]`):
- `max_retries = 3`
- `retry_interval = 60` seconds
- `timeout = 3600` seconds

```toml
[[tasks]]
id             = "flaky_api_call"
command        = "curl https://unstable-api.example.com/data"
max_retries    = 5      # retry up to 5 times on non-zero exit
retry_interval = 30     # wait 30 seconds between retries
timeout        = 120    # kill the task after 120 seconds
```

Each retry is a separate attempt. Use `dagforge logs --attempt 2` to see logs from the second attempt.

When all retries are exhausted, the task state becomes `failed` and downstream tasks with `all_success` are skipped.

---

## 10. Branching DAGs

A branch task outputs a JSON array of task IDs to run. All other downstream tasks are skipped.

```toml
id = "branching_example"

[[tasks]]
id        = "decide"
command   = '''
SIZE=$(wc -l < /data/input.csv)
if [ $SIZE -gt 10000 ]; then
  echo '["process_large"]'
else
  echo '["process_small"]'
fi
'''
is_branch = true

[[tasks.xcom_push]]
key    = "branch"
source = "json"

[[tasks]]
id           = "process_large"
command      = "./batch_process.sh --mode large"
dependencies = ["decide"]

[[tasks]]
id           = "process_small"
command      = "./batch_process.sh --mode small"
dependencies = ["decide"]

[[tasks]]
id           = "finalize"
command      = "./finalize.sh"
dependencies = ["process_large", "process_small"]
trigger_rule = "one_success"   # whichever branch ran
```

**Rules for branch tasks:**
- `is_branch = true` must be set.
- The command must print a JSON array of task IDs to stdout on its last line.
- Task IDs in the array must be direct downstream dependents.
- Non-selected tasks are marked `skipped`.

---

## 11. Inspecting Runs

### Quick status check

```bash
dagforge inspect hello_world --latest
```

Output shows run state, start/finish times, and a table of task states.

### With XCom values

```bash
dagforge inspect daily_etl --latest --xcom
```

Shows XCom key-value pairs pushed by each task.

### With execution time histogram

```bash
dagforge inspect daily_etl --latest --details
```

The `--details` flag adds a per-task timing bar chart, useful for spotting bottlenecks:

```
extract     [##########                    ] 3.2s
transform   [##############################] 9.8s  ← slowest
load        [####                          ] 1.1s
```

### JSON output for scripting

```bash
dagforge inspect hello_world --latest --json | jq '.tasks[] | select(.state == "failed")'
```

---

## 12. Testing Individual Tasks

For development and debugging, you can test a single task in isolation without running the entire DAG. This ignores dependencies and runs the task immediately (similar to Airflow's `test` command).

```bash
dagforge test <dag_id> <task_id>
dagforge test my_pipeline process_data --json
```

---

## 13. Re-running Failed Tasks

After fixing the underlying issue, clear the failed tasks and let the scheduler retry:

```bash
# Clear only failed tasks (leaves successful tasks intact)
dagforge clear daily_etl --run <run_id> --failed

# Clear a specific task and all its downstream dependents
dagforge clear daily_etl --run <run_id> --task transform --downstream

# Clear everything and re-run from scratch
dagforge clear daily_etl --run <run_id> --all
```

After clearing, the scheduler picks up the run on its next tick (within `tick_interval_ms`) and re-executes the cleared tasks.

Find the run ID with:

```bash
dagforge list runs daily_etl --state failed
dagforge inspect daily_etl --latest   # shows run ID at the top
```

---

## 14. Database Maintenance

### Apply schema migrations after upgrade

```bash
dagforge db migrate
```

### Remove stale DAGs

When you delete or rename a DAG file, the DB record remains. To clean it up:

```bash
# Preview what would be deleted
dagforge db prune-stale --dry-run

# Actually delete
dagforge db prune-stale
```

A DAG is considered stale if it exists in the database but has no corresponding file in the DAG source directory.

---

## 15. Scripting / JSON Output

Service startup messages (e.g. `MySQL database opened`) go to **stderr**. CLI commands that support `--json` write structured output to **stdout** and diagnostics to **stderr**.

To capture only JSON from a CLI command:

```bash
result=$(dagforge list runs hello_world --json 2>/dev/null)
```

To reduce startup noise, lower the log level in config:

```toml
[scheduler]
log_level = "warn"   # only warnings and errors
```

Commands with `--json` support: `trigger`, `test`, `list dags`, `list runs`, `list tasks`, `inspect`, `logs`, `pause`, `unpause`, `clear`, `serve status`, `validate`.

---

## 16. CLI Cheatsheet

```bash
# Service Management
dagforge serve start  [-c file] [--pid-file path] [--daemon/-d] [--log-file path] [--no-api] [--log-level trace|debug|info|warn|error] [--shards N]
dagforge serve status [-c file] [--pid-file path] [--json]
dagforge serve stop   [-c file] [--pid-file path] [--timeout N] [--force]

# Trigger & Test
dagforge trigger <dag_id> [--wait] [-e execution_date] [--no-api] [--json]
dagforge test <dag_id> <task_id> [--json]

# Listing
dagforge list dags  [--include-stale] [--limit N] [--output table|json] [--json]
dagforge list runs  [dag_id] [--state failed|success|running] [--limit N] [--output table|json] [--json]
dagforge list tasks [dag_id] [--output table|json] [--json]

# Inspection and Logs
dagforge inspect <dag_id> [--run id|--latest] [--xcom] [--details] [--json]
dagforge logs <dag_id> [--run id|--latest] [--task task_id] [--attempt N] [-f|--follow] [--short-time] [--json]

# DAG Control
dagforge pause <dag_id> [--json]
dagforge unpause <dag_id> [--json]
dagforge clear <dag_id> --run <run_id> [--task <task_id>] [--failed] [--all] [--downstream] [--dry-run] [--json]

# Database Operations
dagforge db init
dagforge db migrate
dagforge db prune-stale [--dry-run]

# Validation
dagforge validate [-c file | -f dag.toml] [--json]
```

---

## 17. Troubleshooting

### `logs` shows no output after a successful run

- The task may have produced no stdout/stderr (e.g. a silent `true` command).
- Confirm the run ID is correct: `dagforge inspect <dag_id> --latest`.

### `inspect --latest` says "No runs found"

The DAG has never been triggered, or the DB is empty. Trigger a run:

```bash
dagforge trigger <dag_id> --wait
```

### `pause` / `unpause` says "DAG not found"

The DAG must be registered in the database first. Start the service and wait for the first scan, or run:

```bash
dagforge validate   # confirms the file is valid
dagforge serve start &    # starts the service; DAG is registered on first scan
sleep 5
dagforge pause <dag_id>
```

### `validate` error: missing required field `id`

The `id` field is the **DAG** identifier, not a task identifier. It must be at the top level of the file:

```toml
id   = "my_pipeline"   # ← top-level DAG id
name = "My Pipeline"

[[tasks]]
id = "step_one"        # ← task id, inside [[tasks]]
```

### `validate` error: dependency cycle detected

DAGForge validates that the DAG is acyclic. Check that no task depends (directly or transitively) on itself.

### `trigger --wait` exits with code 1

The run failed. Check task states:

```bash
dagforge inspect <dag_id> --latest
dagforge logs <dag_id> --latest
```

### MySQL connection errors

1. Verify credentials in `system_config.toml`.
2. Test connectivity: `mysql -h 127.0.0.1 -u dagforge -p dagforge`.
3. Ensure the database exists and the user has full privileges.
4. Check `pool_size` — too high a value may exhaust MySQL's `max_connections`.

### Tasks stuck in `running` after a crash

On restart, DAGForge's crash recovery marks any run that was `running` at shutdown as `failed` (watermark-based). This happens automatically within a few seconds of startup. If tasks remain stuck, run:

```bash
dagforge db migrate   # ensures recovery tables are up to date
```

Then restart the service.

### JSON output contains extra lines

Ensure you redirect stderr: `dagforge <cmd> --json 2>/dev/null`. The `--json` flag only affects stdout formatting; service logs always go to stderr.

### `--details` histogram — what does it mean?

The `--details` flag on `inspect` shows a per-task execution time bar chart normalized to the slowest task. It helps identify bottlenecks in a pipeline. It does **not** show task output — use `dagforge logs` for that.
