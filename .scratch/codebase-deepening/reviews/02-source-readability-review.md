# Source Readability Review

## Review lens

This review looks for repeated knowledge, mixed levels of abstraction, hidden
side effects, and files whose internal seams do not match their concepts. It
does not treat line count as a defect by itself.

## High-severity findings

### Side effects occur during CLI11 callback traversal

`command_line.cpp` callbacks call `execute()` directly. CLI11 can parse more
than one sibling subcommand in one argument vector, so parser traversal can send
multiple HTTP requests before the program returns an error. Parsing and command
execution must be separate phases.

### Run bootstrap knowledge is duplicated

New Run and Repair Run creation each coordinate lifecycle state, idempotency,
initial Checkpoint persistence, pending initialization accounting, owner-shard
posting, lifetime checks, and callback movement. Restore activation repeats the
post/accounting/lifetime pattern. A change to initialization safety currently
requires editing several paths in one large implementation.

### Executor compilation does not produce a true compiled contract

Both Command and HTTP executors parse the same private `NodeConfig` during
compile and start. Compile normalizes or authorizes fields, serializes the
private type back to `JsonPayload`, and start reparses it. This hides which
invariants are guaranteed after compilation and encourages defensive rechecks.

## Medium-severity findings

### Workflow routes repeat transport policy

Runtime availability, missing path parameters, strict body parsing, ID
construction, `Result` mapping, and Accepted/Created/Ok selection are repeated
across routes. The endpoint list should stay explicit, but transport policy
needs one owner.

### HTTP executor mixes four abstraction levels

The file contains Node schema, compile-time validation, request materialization,
connection pooling, one-request lifecycle, response diagnostics, and executor
lifecycle. Each concern is legitimate; their interleaving makes the happy path
hard to follow.

### Minijail runner mixes security planning and process completion

Filesystem trust checks and Minijail argument construction are interleaved in
the same translation unit as pipe line buffering, heartbeat loops, timeout,
process-group termination, and sink completion. These concerns change for
different reasons.

### Store catalogs duplicate filesystem reconciliation

Plan and Checkpoint stores independently merge in-memory records with directory
contents and validate filename-to-record identity. Durable writes are already
centralized; catalog reconciliation is not.

## Deliberately retained repetition

- Explicit Run, Task, and Attempt transition calls should remain visible.
- Route registration should remain one route per endpoint rather than becoming
  a data-driven mini-framework.
- Executor-specific compiled contracts should remain separate; sharing a
  generic “node config” abstraction would erase protocol invariants.
- Small `ok`/`fail` forwarding blocks are acceptable when they preserve the
  exact error source and keep the main sequence linear.
