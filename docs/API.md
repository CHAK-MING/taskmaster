# DAGForge 0.4 HTTP API

The API is available when `[api].enabled = true`. JSON responses use the
standard HTTP status code for success or failure.

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

### `POST /api/v1/workflows/plans/toml`

Registers a TOML Workflow Plan v1. The request body is raw TOML, not JSON.

### `GET /api/v1/workflows/plans`

Lists plans currently registered in the in-memory control plane.

## Run endpoints

### `POST /api/v1/workflows/{workflow_id}/runs`

Starts the latest registered plan for `workflow_id`.

Optional body:

```json
{
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

Returns the run state, node states, attempts, errors, and pending approval IDs.

Run states:

- `queued`
- `running`
- `awaiting_approval`
- `success`
- `failed`
- `cancelled`

Node states:

- `pending`
- `ready`
- `running`
- `awaiting_approval`
- `success`
- `failed`
- `skipped`
- `cancelled`

### `GET /api/v1/workflow-runs/{run_id}/outputs/{node_id}/{port}`

Returns a typed output value:

```json
{"value":true}
```

Artifact, evaluation, tool-result, model-response, and message-list values are
encoded as structured JSON objects.

### `GET /api/v1/workflow-runs/{run_id}/evidence`

Returns evidence records for the run.

### `POST /api/v1/workflow-runs/{run_id}/cancel`

Requests cancellation and returns:

```json
{"status":"cancelled"}
```

## Approval endpoints

### `GET /api/v1/workflow-runs/{run_id}/approvals`

Lists pending approvals with approval ID, node ID, summary, and context.

### `POST /api/v1/workflow-runs/{run_id}/approvals/{approval_id}`

Body:

```json
{
  "approved": true,
  "comment": "reviewed",
  "principal": {
    "subject": "reviewer-7",
    "roles": ["reviewer"]
  }
}
```

Returns `202 Accepted` when the decision is accepted.

## Error behavior

- `400`: invalid path parameter, body, plan, or strict parser failure.
- `401`/`403`: mapped adapter or policy authorization failure.
- `404`: plan, run, output, or approval not found.
- `409`: state or duplicate conflict when mapped by the core error.
- `503`: workflow runtime disabled.

The current API has no authentication middleware. Bind to loopback or place it
behind a trusted gateway when operating outside a development environment.

## Current limitations

- plan registration and run state are in-memory;
- there is no endpoint to upload or download artifact bytes;
- run creation selects the latest plan for a workflow instead of an explicit
  plan ID;
- there is no durable event stream or restart recovery.

These are explicit 0.4 follow-up milestones rather than hidden guarantees.
