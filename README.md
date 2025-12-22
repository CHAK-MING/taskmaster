# TaskMaster

A high-performance DAG-based task scheduler written in modern C++23.

## Features

- **DAG-based Dependencies** - Define complex task workflows with directed acyclic graphs
- **Cron Scheduling** - Standard cron expression support for periodic tasks
- **Dual Operation Modes** - CLI daemon mode, Server mode with Web UI
- **REST API + WebSocket** - Full API for management and real-time log streaming
- **SQLite Persistence** - Crash recovery and run history
- **Modern Web UI** - React-based dashboard with DAG visualization (React Flow)
- **YAML/JSON Config** - Flexible configuration formats

## Quick Start

### Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

### CLI Mode (Daemon)

Run as a background daemon, executing tasks based on cron schedules:

```bash
# Run in foreground (for testing)
./build/bin/taskmaster -c config.yaml

# Run as daemon (background)
./build/bin/taskmaster -c config.yaml -d

# CLI mode with API enabled
./build/bin/taskmaster -c config.yaml --port 8080

# Trigger a specific DAG and exit
./build/bin/taskmaster -c config.yaml -t my_dag

# List all tasks and exit
./build/bin/taskmaster -c config.yaml -l
```

### Server Mode (API + Web UI)

Run with REST API and Web UI for dynamic DAG management:

```bash
# Start server on port 8080
./build/bin/taskmaster --server --port 8080

# Run as daemon
./build/bin/taskmaster --server --port 8080 -d

# With custom database path
./build/bin/taskmaster --server --port 8080 --db /var/lib/taskmaster/data.db
```

Access Web UI at `http://localhost:8080`

### Command Line Options

```
Options:
  -c, --config <file>   Config file (YAML or JSON)
  --server              Run in server mode (API only, no config file)
  --port <port>         API server port (default: 8080)
  --host <host>         API server host (default: 127.0.0.1)
  --db <file>           Database file (default: taskmaster.db)
  -d, --daemon          Run as daemon (background)
  -l, --list            List all tasks and exit
  -t, --trigger <dag>   Trigger a DAG run and exit
  -v, --version         Show version
  -h, --help            Show help
```

## Configuration Examples

### ETL Pipeline

A typical data warehouse ETL workflow:

```yaml
scheduler:
  log_level: info

tasks:
  - id: extract_mysql
    name: Extract from MySQL
    command: |
      mysqldump -h db.example.com -u etl_user -p$MYSQL_PASSWORD \
        --single-transaction sales_db orders customers \
        > /data/staging/mysql_$(date +%Y%m%d_%H).sql
    cron: "0 * * * *"  # Every hour
    timeout: 1800

  - id: extract_api
    name: Extract from REST API
    command: |
      curl -s -H "Authorization: Bearer $API_TOKEN" \
        "https://api.example.com/v1/transactions" \
        > /data/staging/api_$(date +%Y%m%d_%H).json
    cron: "0 * * * *"
    timeout: 600

  - id: transform
    name: Transform Data
    command: python3 /opt/etl/transform.py --input /data/staging --output /data/processed
    deps: [extract_mysql, extract_api]
    timeout: 3600

  - id: load_warehouse
    name: Load to Data Warehouse
    command: psql -h warehouse.example.com -U etl_user -d analytics -f /opt/etl/load.sql
    deps: [transform]
    timeout: 1800
```

### Database Backup

Daily backup with cloud upload:

```yaml
scheduler:
  log_level: info

tasks:
  - id: backup_postgres
    name: Backup PostgreSQL
    command: |
      pg_dump -h localhost -U postgres -d production | gzip > /backup/daily_$(date +%Y%m%d).sql.gz
    cron: "0 2 * * *"  # Daily at 2:00 AM
    timeout: 7200

  - id: upload_s3
    name: Upload to S3
    command: aws s3 cp /backup/ s3://company-backups/ --recursive --exclude "*.tmp"
    deps: [backup_postgres]
    timeout: 3600

  - id: cleanup
    name: Cleanup Old Backups
    command: find /backup -type f -mtime +7 -delete
    deps: [upload_s3]
    timeout: 300
```

### System Monitoring

Periodic health checks:

```yaml
scheduler:
  log_level: info

tasks:
  - id: check_disk
    name: Check Disk Usage
    command: |
      USAGE=$(df -h / | awk 'NR==2 {print $5}' | tr -d '%')
      [ "$USAGE" -gt 85 ] && echo "WARN: Disk at ${USAGE}%" && exit 1
      echo "OK: Disk at ${USAGE}%"
    cron: "*/5 * * * *"  # Every 5 minutes
    timeout: 30

  - id: check_api
    name: Check API Health
    command: |
      curl -sf http://localhost:8080/health || exit 1
    cron: "* * * * *"
    timeout: 30
    max_retries: 2
```

### Task Options

| Field         | Type   | Description                         |
| ------------- | ------ | ----------------------------------- |
| `id`          | string | Unique task identifier (required)   |
| `name`        | string | Display name                        |
| `command`     | string | Shell command to execute (required) |
| `cron`        | string | Cron expression for scheduling      |
| `deps`        | array  | List of dependency task IDs         |
| `timeout`     | int    | Timeout in seconds (default: 300)   |
| `max_retries` | int    | Max retry attempts (default: 3)     |
| `enabled`     | bool   | Enable/disable task (default: true) |

## REST API

| Method    | Endpoint                      | Description          |
| --------- | ----------------------------- | -------------------- |
| GET       | `/api/health`                 | Health check         |
| GET       | `/api/status`                 | System status        |
| GET       | `/api/dags`                   | List all DAGs        |
| POST      | `/api/dags`                   | Create new DAG       |
| GET       | `/api/dags/:id`               | Get DAG details      |
| PUT       | `/api/dags/:id`               | Update DAG           |
| DELETE    | `/api/dags/:id`               | Delete DAG           |
| POST      | `/api/dags/:id/trigger`       | Trigger DAG run      |
| GET       | `/api/dags/:id/tasks`         | List tasks in DAG    |
| POST      | `/api/dags/:id/tasks`         | Add task to DAG      |
| PUT       | `/api/dags/:id/tasks/:taskId` | Update task          |
| DELETE    | `/api/dags/:id/tasks/:taskId` | Delete task          |
| WebSocket | `/ws/logs`                    | Real-time log stream |

## Web UI

The web UI provides:

- Dashboard with system overview
- DAG management and visualization (React Flow)
- Real-time log viewer with filtering
- Task configuration editor

### Development

```bash
cd web-ui
npm install
npm run dev
```

### Production Build

```bash
cd web-ui
npm run build
# Output in web-ui/dist/
```

## Requirements

- C++23 compiler (GCC 13+ or Clang 17+)
- CMake 3.25+
- Linux (liburing for io_uring support)

### Dependencies (auto-fetched via CMake)

- nlohmann/json
- yaml-cpp
- SQLite3
- Crow (HTTP/WebSocket server)

## Project Structure

```
TaskMaster/
├── include/taskmaster/    # Header files
├── src/taskmaster/        # Source files
│   └── api/               # REST API & WebSocket
├── web-ui/                # React frontend
├── examples/              # Configuration examples
│   ├── etl_pipeline.yaml  # ETL workflow example
│   ├── backup_rotation.yaml # Backup example
│   └── monitoring.yaml    # Monitoring example
├── tests/                 # Unit tests
└── docs/                  # Documentation
```

## Roadmap

- [ ] Additional executors (Docker, Kubernetes, SSH)

## License

MIT