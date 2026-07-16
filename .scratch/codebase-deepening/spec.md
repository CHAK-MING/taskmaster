# DAGForge Source Deepening

**Status:** ready-for-agent

## Problem Statement

DAGForge's product interfaces are small—CLI plus JSON and HTTP JSON—but several
active implementations concentrate unrelated lifecycle, transport, parsing,
policy, and persistence concerns in the same translation units. Readers must
reconstruct the same invariants repeatedly across Run creation, executor
compilation and start, HTTP routes, sandbox process supervision, and file-backed
catalogs. The result is difficult to review and unsafe to change even when the
external behavior is straightforward.

## Solution

Deepen the existing internal modules without changing the product interfaces or
the accepted Run/Task/Attempt semantics. Each refactor must give one concept a
small private interface that concentrates its knowledge, remove duplicated
contract work from callers, and preserve explicit orchestration where ordering
is the behavior.

The work proceeds from the narrowest active seam to the most stateful:

1. Make CLI11 parsing select a command without performing side effects, then
   execute exactly one selected command after parsing succeeds.
2. Centralize Workflow HTTP transport policy while keeping route registration
   explicit.
3. Isolate Run admission/bootstrap from owner-shard Run execution.
4. Give Command and HTTP executors separate compiled Node contracts.
5. Separate sandbox launch planning from one-process supervision.
6. Centralize repeated file-catalog reconciliation behind the existing durable
   file implementation.
7. Correct the agent guidance so future changes preserve these seams.

## User Stories

1. As a DAGForge maintainer, I want each source module to have one clear job, so
   that I can understand a change without loading an entire subsystem.
2. As a DAGForge maintainer, I want repeated lifecycle knowledge to have one
   owner, so that fixes do not require synchronized edits across several paths.
3. As a CLI user, I want one invocation to execute exactly one command, so that
   malformed command lines cannot perform partial side effects.
4. As a CLI user, I want IPv4, DNS, and IPv6 API origins to behave consistently,
   so that endpoint syntax does not leak transport implementation mistakes.
5. As an API maintainer, I want route registration to remain explicit, so that
   the HTTP surface can be audited from one file.
6. As an API maintainer, I want path, body, subsystem, and error policy to be
   centralized, so that routes cannot drift in validation or response behavior.
7. As a Workflow maintainer, I want Run admission separated from owner-shard
   execution, so that idempotency and persistence changes do not disturb the
   state machine.
8. As a Workflow maintainer, I want new Runs, restored Runs, and Repair Runs to
   share one bootstrap mechanism, so that initialization safety is consistent.
9. As an executor maintainer, I want compilation to produce a meaningful private
   contract, so that Task start does not repeat static validation.
10. As an executor maintainer, I want Command and HTTP contracts to remain
    distinct, so that protocol-specific invariants stay visible.
11. As a sandbox maintainer, I want launch policy separated from process
    supervision, so that security changes and lifecycle changes are locally
    reviewable.
12. As a storage maintainer, I want directory reconciliation to have one owner,
    so that Plan and Checkpoint catalogs follow identical file identity rules.
13. As an agent or human reviewer, I want `AGENTS.md` to state only current,
    load-bearing rules, so that detailed implementation recipes do not become
    stale duplicated policy.
14. As an integrator, I want CLI and HTTP JSON to remain the supported product
    interfaces, so that internal C++ refactors do not imply an SDK commitment.
15. As a maintainer, I want every extracted module to pass the deletion test, so
    that cleanup does not create shallow wrappers or more indirection.

## Implementation Decisions

- The supported product interfaces remain CLI/JSON and HTTP/JSON. Internal C++
  targets and headers are not a supported external SDK.
- The accepted Run, Task, and Attempt state machines remain unchanged.
- `WorkflowRuntime` remains the caller-facing Runtime module; new seams are
  private implementation modules.
- CLI11 remains the parser. Its callbacks may select or populate a command but
  must not start a Run, start a server, or issue an HTTP request.
- Each CLI invocation executes at most one selected leaf command after parsing
  and validation have completed.
- Workflow route registration stays explicit. Shared transport policy may be
  extracted, but endpoints are not generated from a generic route table.
- Command and HTTP executors receive separate private compiled contracts. No
  generic executor-config abstraction will erase their protocol differences.
- `ICommandRunner` remains the sandbox seam. Launch planning and process
  supervision are private modules behind it.
- The durable-file implementation remains the owner of atomic write, append,
  fsync, and removal behavior. Catalog refactoring must not change file formats.
- Vendored library source is the primary reference for version-sensitive
  behavior. Local wrappers are justified only by DAGForge policy or lifecycle,
  not by a preference to hide library syntax.
- New private headers shared by implementation files live under
  `src/dagforge/<subsystem>/detail/`. Stable library-target interfaces live under
  `include/dagforge/`, but this placement does not create a product SDK promise.

## Testing Decisions

- Tests exercise the highest existing seam for each slice: CLI subprocess
  behavior, HTTP endpoint behavior, `WorkflowRuntime`, `ITaskExecutor`,
  `ICommandRunner`, and Store interfaces.
- Refactor tickets preserve behavior first. Tests should lock externally visible
  contracts and lifecycle ordering rather than private helper functions.
- The CLI slice must cover multiple sibling commands, help/version, positional
  arguments, environment defaults, IPv6 endpoint resolution, and exactly-once
  execution.
- Runtime slices must preserve the accepted state-machine invariants, restore,
  Repair Run, idempotency, shutdown, and persistence failure behavior.
- Executor slices must distinguish compile-time contract rejection from
  run-time input/transport/process failure.
- Broad test-file reorganization is deferred until source seams are stable.

## Out of Scope

- A public C++ SDK or stable ABI.
- New Workflow Plan capabilities, Node kinds, or HTTP endpoints.
- Changes to Run/Task/Attempt state names or transition semantics.
- Replacing build2, CLI11, Glaze, Boost.Asio, Boost.Process, Minijail, or the
  current storage format.
- A general dependency-injection framework, generic route framework, generic
  executor schema, or generic persistence framework.
- Project-wide test cleanup before the production seams are corrected.

## Further Notes

The architecture survey and supporting reviews are indexed by
[`map.md`](map.md). Work the implementation frontier in ticket order and perform
a Standards plus Spec review after each slice.
