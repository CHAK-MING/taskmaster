# TaskMaster

A high-performance DAG task scheduler built with modern C++23.

![TaskMaster UI](image/web-ui.png)

## Features

- DAG-based task dependencies
- Cron scheduling with standard expressions
- Web UI with DAG visualization (React Flow)
- REST API + WebSocket real-time logs
- SQLite persistence with crash recovery

## Quick Start

### Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

### Run

```bash
# Start server with Web UI
./build/bin/taskmaster --server --port 8080

# Access UI at http://localhost:8080
```

### Build Web UI (Optional)

```bash
cd web-ui
npm install && npm run build
```

## Task Configuration

| Field         | Type   | Description                       |
| ------------- | ------ | --------------------------------- |
| `id`          | string | Unique identifier (required)      |
| `name`        | string | Display name                      |
| `command`     | string | Shell command (required)          |
| `cron`        | string | Cron expression                   |
| `deps`        | array  | Dependency task IDs               |
| `timeout`     | int    | Timeout in seconds (default: 300) |
| `max_retries` | int    | Max retries (default: 3)          |

## REST API

| Method    | Endpoint                      | Description      |
| --------- | ----------------------------- | ---------------- |
| GET       | `/api/dags`                   | List DAGs        |
| POST      | `/api/dags`                   | Create DAG       |
| GET       | `/api/dags/:id`               | Get DAG          |
| PUT       | `/api/dags/:id`               | Update DAG       |
| DELETE    | `/api/dags/:id`               | Delete DAG       |
| POST      | `/api/dags/:id/trigger`       | Trigger run      |
| GET       | `/api/dags/:id/tasks`         | List tasks       |
| POST      | `/api/dags/:id/tasks`         | Add task         |
| PUT       | `/api/dags/:id/tasks/:taskId` | Update task      |
| DELETE    | `/api/dags/:id/tasks/:taskId` | Delete task      |
| WebSocket | `/ws/logs`                    | Real-time logs   |

## Requirements

- C++23 (GCC 13+ / Clang 17+)
- CMake 3.25+
- Linux with io_uring

## License

MIT
