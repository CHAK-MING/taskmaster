## Context

`ITaskExecutor` is the seam where concrete execution behavior becomes a
Workflow Attempt result. The current completion channel carries either outputs
or only `std::error_code`. This is sufficient for retry classification but not
for diagnosis: the concrete executor has already seen the process result or
HTTP response, while Runtime has no legitimate way to reconstruct it.

The correction belongs at that seam. Runtime must remain executor-neutral and
must not learn Command or HTTP protocols.

## Decisions

### One structured execution failure value

Workflow defines `ExecutionFailure` with four fields:

- `kind`: a normalized `dagforge::Error` used for state and retry policy;
- `code`: a stable snake_case machine identifier used by clients and AI;
- `message`: a concise human-readable summary;
- `details`: executor-neutral JSON whose concrete fields are owned by the
  executor that creates the failure.

`TaskExecutionResult` is an expected value of `ExecutorOutputs` or
`ExecutionFailure`. This type is used only for asynchronous Task completion.
Synchronous admission methods such as `start()` and `compile()` continue to
return the project-wide `Result<T>` because they do not represent a completed
Attempt.

### Normalize at the executor seam

Concrete executors translate lower-level errors before completion. They may
record the original category, numeric value, and message inside `details`, but
low-level error categories do not escape as the authoritative Workflow error.

Command owns Command diagnostic fields. HTTP owns HTTP diagnostic fields.
Runtime consumes only `kind`, `code`, `message`, and opaque `details`.

### One failure representation through the stack

Attempt, Task, and Run snapshots store `optional<ExecutionFailure>`. Legacy
string-only fields are removed. Checkpoint serialization stores the same four
fields. Evidence and HTTP JSON use one shared JSON projection so field names do
not drift between surfaces.

### Failure propagation rules

- An Attempt failure stores the executor failure unchanged.
- A retrying Task exposes the last Attempt failure until a later Attempt
  succeeds.
- A successful Task clears its failure.
- A failed Run copies the terminal failed Task failure.
- Runtime-originated failures use the same value with Runtime-owned machine
  codes such as `runtime_restarted` or `output_contract_violation`.
- Cancellation uses `Error::Cancelled`; timeout uses `Error::Timeout`.

### Diagnostics remain bounded

Executors expose only data already bounded by their configured transport or
sandbox limits. Command stdout/stderr and HTTP bodies do not gain a new
unbounded capture path. HTTP failure projection redacts credential-bearing
response header values while retaining names and redaction markers. No request
credentials are added to failure details.

### Persisted failures are validated at the storage boundary

Checkpoint encoding and decoding validate the failure invariant: non-success
kind, non-empty machine code and message, and object-shaped details. Invalid
failure data fails closed instead of being admitted into Runtime state.

### No compatibility layer

The checkpoint schema remains version 1 because this repository treats the
current product as a new pre-release baseline. Old string fields and old
readers are deleted rather than retained alongside the corrected contract.

## Rejected Alternatives

### Add executor-specific Evidence writes

This would create a second result path and split authority between completion
and Evidence. It also makes tests and future executors depend on Workflow
internals.

### Change the global `Result<T>` error type

Most project operations only need `std::error_code`. Replacing the global
error channel would spread JSON and diagnostic payload concerns into core,
I/O, configuration, and storage code that does not need them.

### Store only JSON

An untyped JSON error would force Runtime to parse executor-owned fields to
classify retries. The normalized `kind` keeps policy explicit while `details`
remains extensible.
