## Context

The authoritative execution state is split across the in-memory Runtime,
checkpoint files, output Artifacts, and Evidence. Checkpoints already contain
the source Plan, trigger, Run snapshot, and retained values, but they are
written only at explicit checkpoint nodes and terminal completion. On startup,
every non-terminal checkpoint is converted into a failed Run.

Repair and restart recovery are distinct operations:

- restart recovery continues the same immutable Run and Plan after process
  loss;
- repair creates a new Run for a revised Plan and may reuse successful work
  from a parent Run.

The Runtime remains deterministic and executor-neutral. An upper-layer AI may
choose a revised Plan, but it never mutates Runtime internals or executor state.

## Decisions

### Persist Plans independently

`PlanStore` owns the durable Plan catalog. `WorkflowControlPlane` persists a
canonical compiled Plan before publishing it in memory. Application startup
loads the catalog first, then restores Run checkpoints against those Plans.

Run checkpoints keep their source Plan as a self-contained recovery record,
but the checkpoint is not the Plan catalog.

### Persist accepted Runs before dispatch

`WorkflowRuntime::start` writes an initial checkpoint before returning the Run
ID. A non-empty idempotency key is published only after that write succeeds.
Stable Run and Task transitions refresh the checkpoint so completed nodes and
their values survive process loss.

Checkpoint storage remains the authoritative recovery source. Evidence is an
audit log, not a replay log.

### Resume interrupted Runs

Application startup loads checkpoints while the core Runtime is stopped.
Terminal Runs become completed snapshots. Non-terminal Runs are staged for
activation after the core Runtime starts.

During activation:

- succeeded, skipped, failed, and cancelled Tasks remain terminal;
- retained output values are restored before dispatch;
- an in-flight Attempt becomes a terminal infrastructure failure with code
  `runtime_restarted`;
- the interrupted Task becomes ready for a new Attempt;
- retry-waiting Tasks retain their deadline or become ready when it elapsed;
- paused Runs remain paused;
- running Runs dispatch only unfinished Tasks.

The old process or network operation is never reattached.

### Repair creates a child Run

A repair request contains a revised full Plan, a reason, and an optional
idempotency key. The revised Plan is compiled and persisted normally. Runtime
loads the parent checkpoint and creates a new Run with:

- `parent_run_id`;
- `parent_plan_id`;
- `repair_revision`;
- a new Run ID and immutable revised Plan ID.

The parent Run and its Evidence remain unchanged.

### Reuse is conservative and transitive

A successful parent node is reusable only when:

- the revised Plan contains the same node ID;
- executor type, canonical config, input bindings, output contract, timeout,
  and incoming conditional edges are unchanged;
- every dependency is itself reusable;
- every retained value required from that node still exists.

If one condition fails, the node is invalidated. Invalidation propagates to all
descendants through dependency reuse. Independent successful branches remain
eligible.

Reused Tasks are terminal `succeeded` Tasks in the child Run and record
`reused_from_run_id`. Their retained values are copied into the child
`RunValueStore`; no executor is called.

### Failure reports are a dedicated interface

`WorkflowRuntime::failure_report(run_id)` returns one executor-neutral
hierarchy containing:

- the Run failure;
- every Task failure;
- every failed, timed-out, or cancelled Attempt failure;
- diagnostic Artifact references carried by those failures.

The interface does not parse fields inside `details` and does not depend on a
concrete executor.

### Oversized details become Artifacts

Inline failure details are convenient for AI repair but must remain bounded.
Runtime serializes the complete details object and stores it as
`application/json` when it exceeds the failure-detail threshold. The failure
keeps a compact summary and a named Artifact reference. Existing Artifact GET
routes retrieve the complete payload.

Small details remain inline.

## Rejected Alternatives

### Mutate and resume the failed Run

Changing the Plan of an existing Run destroys provenance and makes
idempotency, Evidence, and checkpoint interpretation ambiguous.

### Rerun the whole DAG after repair

This is correct but wastes expensive HTTP, model, WASM, and MCP work and makes
large fan-out workflows impractical.

### Let each executor write recovery data directly

That couples concrete executors to Runtime persistence and creates multiple
authoritative result paths. Executors return outputs or one structured
failure; Runtime owns persistence.

### Replay Evidence to rebuild state

Evidence is append-only audit data and may be retained or paged independently.
Checkpoint state is smaller, direct, and already contains values required for
continued execution.
