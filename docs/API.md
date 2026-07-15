# DAGForge 0.4 HTTP API

The API is available when `[api].enabled = true`. JSON responses use the
standard HTTP status code for success or failure. Error responses use one
stable envelope rather than a string-only field:

```json
{
  "error": {
    "kind": "already_exists",
    "code": "already_exists",
    "message": "Already exists",
    "details": {
      "cause": {
        "category": "dagforge",
        "value": 12,
        "message": "Already exists"
      }
    },
    "artifacts": []
  }
}
```

`kind` drives broad policy and `code` is the stable machine identifier.
Validation errors use codes such as `invalid_request`; callers do not need to
parse `message`.

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

Plans with the same canonical digest are deduplicated. When file storage is
enabled, the immutable Plan catalog is restored before Run checkpoints. A
stored `plan_id` cannot be overwritten with a different digest.

### `GET /api/v1/workflows/plans`

Lists plans currently registered in the control plane. The optional
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
empty. A retained key returns the original Run only when `workflow_id` and
`plan_id` match the original request. Reusing the key for a different Plan,
Workflow, repair parent, or repair Plan returns `409 Conflict` with
`error.code = "already_exists"`.

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
Repair Runs also return `parent_run_id`, `parent_plan_id`, `repair_revision`,
and `repair_reason`. Reused Tasks return `reused_from_run_id` and have no new
Attempts.

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

Failed Runs, Tasks, and Attempts expose the same structured `failure` object:

```json
{
  "kind": "unknown",
  "code": "command_exit_nonzero",
  "message": "Command exited with status 7",
  "details": {
    "exit_code": 7,
    "stdout": "partial output",
    "stderr": "invalid configuration",
    "runner_error": "",
    "stdout_streamed": false,
    "stderr_streamed": false
  },
  "artifacts": []
}
```

`kind` is the normalized DAGForge error used for retry and terminal-state
policy. `code` is the stable machine identifier. `details` is executor-owned
bounded JSON. Command failures can include exit status and captured output;
HTTP status failures can include status, headers, body, UTF-8 validity, and
body size. Credential-bearing response headers keep their names but return the
value `[redacted]` and `redacted: true`. Legacy string-only `error` and
`last_error` fields are not emitted.

When complete diagnostic JSON exceeds the inline limit, `details` becomes a
small externalization summary and `artifacts` contains a named reference:

```json
{
  "details": {
    "externalized": true,
    "artifact_id": "019...",
    "size_bytes": 98304
  },
  "artifacts": [{
    "name": "details",
    "artifact_id": "019...",
    "media_type": "application/json",
    "size_bytes": 98304,
    "digest": "sha256:..."
  }]
}
```

The complete JSON is retrieved through the existing Artifact endpoint.

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
and `limit`. Evidence `type` values are stable strings such as
`task_failed`, `run_failed`, and `checkpoint`. Failure Evidence stores the
same structured object under `metadata.failure`.

### `GET /api/v1/workflow-runs/{run_id}/failures`

Returns an executor-neutral failure report for an active or completed Run. The
report includes Run lineage, the Run failure, failed Task records, failed or
timed-out Attempt records, failure classification, termination reason, and all
diagnostic Artifact references. It does not interpret executor-owned fields
inside `details`.

### `POST /api/v1/workflow-runs/{run_id}/repairs`

Compiles a revised full Plan and starts an immutable child Run. The parent must
already be terminal and must belong to the same `workflow_id`.

```json
{
  "reason": "fix branch B response schema",
  "idempotency_key": "repair-42",
  "plan": {
    "workflow_id": "market-signal",
    "schema_version": 1,
    "nodes": []
  }
}
```

The `202 Accepted` response identifies the child and explains the conservative
reuse decision for every node:

```json
{
  "run_id": "market-signal__019...",
  "parent_run_id": "market-signal__018...",
  "plan_id": "019...",
  "nodes": [
    {"node_id":"fetch_hn","reused":true,"reason":"reused"},
    {"node_id":"analyze_hn","reused":false,
     "reason":"execution_contract_changed"},
    {"node_id":"aggregate","reused":false,
     "reason":"dependency_invalidated"}
  ]
}
```

Reuse requires the same node ID, executor, canonical config, input and output
contracts, timeout, incoming conditional edges, reusable dependencies, and
retained outputs. Missing Artifacts invalidate reuse. The parent checkpoint
and Evidence are never modified. A repair idempotency key is bound to both the
parent Run and revised Plan.

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

Every JSON error response contains `error.kind`, `error.code`,
`error.message`, `error.details`, and `error.artifacts`.

- `400`: invalid path parameter, body, plan, or strict parser failure.
- `401`/`403`: mapped adapter or policy authorization failure.
- `404`: plan, run, or output not found.
- `409`: state or duplicate conflict when mapped by the core error.
- `413`: request body exceeds the configured limit.
- `429`: concurrent request limit reached.
- `503`: workflow runtime disabled.

## Current limitations

- Evidence persistence is append-only JSON Lines rather than a query database.
- Plan and Run storage are file-backed single-process stores; there is no
  distributed ownership or database transaction layer.
- an interrupted external process or request is not reattached after restart;
  its old Attempt is closed as infrastructure failure and the Task is started
  with a new Attempt.

These are explicit 0.4 follow-up milestones rather than hidden guarantees.
