# Architecture Survey

## Scope

The survey followed recent change hot spots: Workflow Runtime and recovery,
Workflow HTTP routes, Command and HTTP executors, sandbox process execution,
file-backed stores, and the active CLI redesign. The accepted
Run/Task/Attempt ADR is treated as fixed.

## Candidate 1 — Run admission and owner-shard execution

**Recommendation:** Strong

**Current shape:** `WorkflowRuntime` presents a reasonably small external
interface, but one implementation owns Run admission, idempotency, initial
Checkpoint persistence, restore validation, Repair planning, owner-shard state,
dispatch, Attempt completion, retry scheduling, cancellation, Evidence,
retention, and query projection.

**Deepening:** Keep the `WorkflowRuntime` interface. Introduce private modules
for Run admission/bootstrap and owner-shard Run execution. The external module
coordinates cross-shard commands and lifecycle; the owner-shard module owns an
active Run from activation to terminal settlement.

**Why it helps:** The caller still learns one interface. Internally, lifecycle
knowledge stops spanning two thousand lines. Run creation changes become local
to bootstrap; state-machine changes become local to the owner-shard engine.

## Candidate 2 — Compiled executor contracts

**Recommendation:** Strong

**Current shape:** Command and HTTP executors parse Node JSON during compilation,
normalize some fields, serialize it back to JSON, then parse it again during
Task start. Runtime materialization rechecks part of the same contract while
combining dynamic inputs.

**Deepening:** Give each executor its own private compiled Node contract. The
compile path owns parsing, normalization, policy authorization, static limits,
and output validation. Task start materializes only dynamic input values and
hands a ready request to a one-Attempt execution state machine.

**Why it helps:** Contract knowledge is expressed once. Compile-time failures
and runtime failures become easier to distinguish. Tests can exercise the
executor through `ITaskExecutor` without parsing implementation details.

## Candidate 3 — Workflow HTTP route adapter

**Recommendation:** Worth exploring

**Current shape:** Each route repeats optional subsystem discovery, path
parameter checks, ID construction, body parsing, `Result` mapping, and response
status selection. The route file is readable line-by-line but expensive to
change consistently.

**Deepening:** Add a private Workflow HTTP adapter with small operations such as
required Runtime/control access, typed path IDs, strict optional/required JSON
bodies, pagination, and one Result-to-response policy. Route registration remains
explicit so the endpoint inventory is visible.

**Why it helps:** Repetition disappears without hiding routes in a generic
framework. JSON and error policy gain locality.

## Candidate 4 — Sandbox plan and process supervisor

**Recommendation:** Strong

**Current shape:** Minijail path validation, execution-root management,
argument/environment planning, process launch, pipe streaming, output limits,
heartbeats, timeout, cancellation, registry, cleanup, and quiesce share one
file.

**Deepening:** Retain `ICommandRunner`. Build a private immutable Sandbox Plan at
runner creation/start preparation, then hand it to a Process Supervisor that
owns exactly one process group's I/O and terminal completion.

**Why it helps:** Security policy and process lifecycle stop interleaving. The
supervisor becomes the only place that must reason about reaping and callback
ordering.

## Candidate 5 — File catalog primitive

**Recommendation:** Worth exploring

**Current shape:** The durable-file module correctly concentrates atomic write,
append, fsync, and removal behavior. Plan and Checkpoint stores still duplicate
directory existence checks, enumeration, decode, filename identity validation,
and merge-with-memory behavior.

**Deepening:** Add a private typed catalog primitive only for the repeated
directory/index behavior. Keep encoding and domain validation in each store.

**Why it helps:** Filesystem consistency rules gain locality without creating a
generic storage framework or changing persistence formats.

## Top recommendation

The highest-value architectural target is Run admission plus owner-shard
execution. The first implementation slice should nevertheless be CLI command
selection because it is narrower, currently changing, and demonstrates the
same principle: a small product interface over a deep private implementation,
with parsing separated from side effects.
