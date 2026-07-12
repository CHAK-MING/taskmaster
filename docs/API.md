> [!NOTE]
> **API Stability**
> API schemas are currently produced from code (`api_server.cpp`) and may evolve. For strict integration, pin your client to a specific DAGForge release tag.

<div align="center">

# DAGForge API Reference

</div>

This document describes the currently implemented HTTP and WebSocket APIs.

---

## 📡 Base URL

- Default: `http://127.0.0.1:8888`
- API prefix: `/api`

## 🔐 Auth and Versioning

- Authentication: none (current implementation)
- Versioning: none (current implementation)

## 📋 Common Response Rules

- Content type: `application/json` for API endpoints.
- Success status codes:
  - `200 OK`
  - `201 Created` (trigger run)
- Common error status codes:
  - `400 Bad Request`
  - `403 Forbidden`
  - `404 Not Found`
  - `409 Conflict`
  - `500 Internal Server Error`
  - `503 Service Unavailable`
- Error body format:

```json
{"error":"error message"}
```

---

## 🌐 HTTP Endpoints

### Health and Metrics

- `GET /api/health`
- `GET /api/status`
- `GET /metrics` (Prometheus text format)

### DAG Metadata

- `GET /api/dags`
- `GET /api/dags/{dag_id}`
- `GET /api/dags/{dag_id}/tasks`
- `GET /api/dags/{dag_id}/tasks/{task_id}`
- `GET /api/dags/{dag_id}/history`

### DAG Control

- `POST /api/dags/{dag_id}/trigger`
- `POST /api/dags/{dag_id}/pause`
- `POST /api/dags/{dag_id}/unpause`

### Run History and Runtime State

- `GET /api/history`
- `GET /api/history/{dag_run_id}`
- `GET /api/runs/{dag_run_id}/tasks`
- `GET /api/runs/{dag_run_id}/logs?limit=10000`
- `GET /api/runs/{dag_run_id}/tasks/{task_id}/logs?attempt=1&limit=5000`

---

## 📖 Request/Response Examples

### 🚀 Trigger a DAG run

```bash
curl -X POST http://127.0.0.1:8888/api/dags/hello_world/trigger \
  -H 'Content-Type: application/json' \
  -d '{}'
```

**Response (`201`):**

```json
{"dag_run_id":"019c...","status":"triggered"}
```

### 📋 List runs

```bash
curl http://127.0.0.1:8888/api/history
```

**Response (`200`, abbreviated):**

```json
{
  "runs": [
    {
      "dag_run_id": "019c...",
      "dag_id": "hello_world",
      "state": "success",
      "trigger_type": "manual",
      "started_at": "2026-02-23T04:55:08Z",
      "finished_at": "2026-02-23T04:55:08Z",
      "execution_date": "2026-02-23T04:55:08Z"
    }
  ]
}
```

### 🔍 Get tasks for one run

```bash
curl http://127.0.0.1:8888/api/runs/<dag_run_id>/tasks
```

**Response (`200`, abbreviated):**

```json
{
  "dag_run_id": "019c...",
  "tasks": [
    {
      "task_id": "greet",
      "state": "success",
      "attempt": 1,
      "exit_code": 0,
      "started_at": "2026-02-23T04:55:08Z",
      "finished_at": "2026-02-23T04:55:08Z",
      "error": ""
    }
  ]
}
```

---

## 🔌 WebSocket API

DAGForge accepts WebSocket upgrades when the HTTP server receives an upgrade request.  
In practice, clients should use:

- `ws://127.0.0.1:8888/ws`
- or `ws://127.0.0.1:8888/ws/logs` (commonly used by UI)

### Message Formats Sent by Server

**Log message:**

```json
{
  "type":"log",
  "timestamp":"2026-02-23T04:55:08Z",
  "dag_run_id":"019c...",
  "task_id":"greet",
  "stream":"stdout",
  "content":"hello world"
}
```

**Event message:**

```json
{
  "type":"event",
  "timestamp":"2026-02-23T04:55:08Z",
  "event":"task_status_changed",
  "dag_run_id":"019c...",
  "task_id":"greet",
  "data":"{\"state\":\"success\"}"
}
```
