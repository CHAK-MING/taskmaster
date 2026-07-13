# ADR 0001: Separate Run, Task, and Attempt State

## Status

Accepted.

## Context

The original workflow lifecycle stored one run state and one node state.
Retry, approval waits, cancellation, process termination, and finalization were
all projected through those two fields. That model could not distinguish a
task waiting to retry from a runnable task, or a cancellation request from a
confirmed process exit. It also allowed a run to become terminal before its
sandbox process had been reaped.

## Decision

DAGForge uses three owner-shard state machines:

- Run: `running`, `pausing`, `paused`, `stopping`, `succeeded`, `failed`,
  `cancelled`.
- Task: `pending`, `ready`, `running`, `retry_waiting`, `succeeded`, `failed`,
  `skipped`, `cancelled`.
- Attempt: `starting`, `running`, `terminating`, `succeeded`, `failed`,
  `timed_out`, `cancelled`.

Each Task has at most one active Attempt. Every retry creates a new Attempt
record. Terminal states are irreversible, duplicate or late completions are
ignored by Attempt ID, and a Run cannot become terminal until every Task and
Attempt is terminal.

Pause is a scheduler operation. It stops new dispatches and lets active
attempts finish; it does not freeze sandbox processes. Cancellation, run
deadline, and fail-fast first move the Run to `stopping`, request termination,
and wait for process reaping before selecting the final Run state.

Retryable failures enter `retry_waiting` with bounded exponential backoff.
Permanent failures do not retry. Plans choose either `continue_independent` or
`fail_fast` failure propagation.

Approval is not an executor or workflow node. A future external-wait feature
must be modeled as a separate runtime wait record rather than an executable
task type.

## Consequences

Snapshots and checkpoints contain Task records and Attempt history instead of
flat node state. Control-plane clients must consume `succeeded` rather than
`success`, and cancellation responses represent an accepted stop request,
not immediate terminal cancellation.

The state model is ready for durable storage because transition identity,
retry timing, stop intent, skip reason, and late-completion handling are now
explicit. Persistent recovery itself remains out of scope for this decision.
