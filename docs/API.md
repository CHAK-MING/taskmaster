# DAGForge 0.4 HTTP API

The API is available when `[api].enabled = true`. JSON responses use the
standard HTTP status code for success or failure.

When `api.bearer_token_env` is configured, every endpoint requires:

```http
Authorization: Bearer <token>
```

The token is read from the named environment variable. Missing or empty token
configuration prevents the API server from starting. Request body and
concurrency ceilings are controlled by `api.max_request_body_bytes` and
`api.max_concurrent_requests`.

## System endpoints

### `GET /api/health`

Returns:

```json
{"status":"healthy"}
```

### `GET /api/status`

Returns runtime state, whether the workflow runtime is enabled, active run
count, shard count, and a timestamp.

### `GET /metrics`

Returns Prometheus text format.

## Plan endpoints

### `POST /api/v1/workflows/plans`

Registers a JSON Workflow Plan v1.

The response has status `201 Created`:

```json
{
  "workflow_id": "example",
  "plan_id": "019...",
  "digest": "sha256...",
  "nodes": 3
}
```

Plans with the same canonical digest are deduplicated in the current process.

### `GET /api/v1/workflows/plans`

Lists plans currently registered in the in-memory control plane. The optional
`offset` and `limit` query parameters select a page. The response includes
`plans`, `total`, `offset`, and `limit`.

## Run endpoints

### `POST /api/v1/workflows/{workflow_id}/runs`

Starts a registered plan for `workflow_id`. When `plan_id` is omitted, the
latest registered plan is selected.

Optional body:

```json
{
  "plan_id": "019...",
  "source": "api",
  "event_type": "request",
  "payload": {"request":"hello"},
  "idempotency_key": "request-123",
  "principal": {
    "subject": "user-42",
    "roles": ["operator"]
  }
}
```

`Idempotency-Key` can be supplied as an HTTP header when the body field is
empty.

The response has status `202 Accepted`:

```json
{
  "run_id": "example__019...",
  "workflow_id": "example",
  "plan_id": "019..."
}
```

### `GET /api/v1/workflow-runs/{run_id}`

Returns the run state, task states, attempt history, stop intent, and errors.

Run states:

- `running`
- `pausing`
- `paused`
- `stopping`
- `succeeded`
- `failed`
- `cancelled`

Task states:

- `pending`
- `ready`
- `running`
- `retry_waiting`
- `succeeded`
- `failed`
- `skipped`
- `cancelled`

Attempt states:

- `starting`
- `running`
- `terminating`
- `succeeded`
- `failed`
- `timed_out`
- `cancelled`

Run, task, retry, and attempt timestamps are returned as Unix epoch
milliseconds with an `_at_ms` suffix when the timestamp exists.
Failed attempts include `failure_class`; stopped attempts include
`termination_reason`.

### `GET /api/v1/workflow-runs/{run_id}/outputs/{node_id}/{port}`

Returns a typed output value:

```json
{"value":true}
```

Primitive values and JSON are returned directly. Externalized values are
encoded as Artifact reference objects.

### `GET /api/v1/workflow-runs/{run_id}/evidence`

Returns evidence records for the run. The optional `offset` and `limit` query
parameters select a page. The response includes `evidence`, `total`, `offset`,
and `limit`.

## Artifact endpoints

### `POST /api/v1/artifacts`

Stores the raw request body as an Artifact. `Content-Type` becomes the stored
media type and defaults to `application/octet-stream`. The response has status
`201 Created` and contains `artifact_id`, `media_type`, `size_bytes`, and
`digest`.

### `GET /api/v1/artifacts/{artifact_id}`

Returns the Artifact bytes with the stored `Content-Type` and digest in the
`ETag` header.

### `DELETE /api/v1/artifacts/{artifact_id}`

Deletes the Artifact and returns `{"status":"deleted"}`.

### `POST /api/v1/workflow-runs/{run_id}/cancel`

Requests cancellation and returns:

```json
{"status":"stopping"}
```

The run remains `stopping` until every active attempt has terminated and its
sandbox process has been reaped.

### `POST /api/v1/workflow-runs/{run_id}/pause`

Stops dispatching new tasks. Active attempts continue normally. The run moves
from `pausing` to `paused` after the active-attempt count reaches zero.

### `POST /api/v1/workflow-runs/{run_id}/resume`

Moves a paused run back to `running` and resumes dispatch.

## Error behavior

- `400`: invalid path parameter, body, plan, or strict parser failure.
- `401`/`403`: mapped adapter or policy authorization failure.
- `404`: plan, run, or output not found.
- `409`: state or duplicate conflict when mapped by the core error.
- `413`: request body exceeds the configured limit.
- `429`: concurrent request limit reached.
- `503`: workflow runtime disabled.

## Current limitations

- plan registration without a Run is not persisted independently;
- Evidence persistence is append-only JSON Lines rather than a query database.

These are explicit 0.4 follow-up milestones rather than hidden guarantees.
