## Why

DAGForge can now expose structured execution failures, but a failed or
interrupted Run is still a dead end. Non-terminal checkpoints are converted
into failed Runs at restart, registered Plans without Runs are lost, and an
upper-layer repair controller must traverse snapshots and executor-specific
details to discover what failed.

The next foundation must make persistence and recovery authoritative before
new WASM, model, or MCP executors are added. A repair must create a new Run,
reuse only provably unchanged successful nodes, preserve the original Run, and
expose every failure through one executor-neutral interface.

## What Changes

- Persist the Plan catalog independently from Run checkpoints.
- Persist an initial checkpoint before accepting a Run and refresh the
  checkpoint at stable state transitions.
- Recover non-terminal Runs after restart by preserving completed work,
  finalizing interrupted Attempts as infrastructure failures, and dispatching
  only unfinished Tasks.
- Add Repair Runs linked to a parent Run and revised Plan.
- Reuse successful nodes only when the node execution contract, incoming
  conditions, dependencies, and retained outputs are unchanged.
- Expose a dedicated Run failure report that contains Run, Task, and Attempt
  failures without concrete executor types.
- Externalize oversized structured failure details into the existing Artifact
  store and return Artifact references from the failure report.
- Add stable Evidence for recovery, repair creation, node reuse, and node
  invalidation.

## Capabilities

### New Capabilities

- `durable-workflow-recovery`: Defines Plan persistence, restart recovery,
  Repair Runs, lineage, and safe successful-node reuse.
- `workflow-failure-reports`: Defines the executor-neutral interface for
  retrieving all failures and diagnostic Artifact references for a Run.

### Modified Capabilities

- `structured-execution-failures`: Structured failures may reference retained
  diagnostic Artifacts when inline details are externalized.

## North-star Workflow

The design must support a trigger that fans out to independent HTTP fetches,
per-branch model analysis, fan-in data normalization, a main model decision,
conditional routing, report generation, archival, and HTTP notification.
Failure or revision of one branch must not force unchanged successful sibling
branches to run again.

## Non-goals

- Implementing WASM, model, MCP, browser, or additional HTTP features.
- Running an autonomous planner or tool-calling agent inside Runtime.
- Reattaching to an operating-system process or network request that existed
  before restart.
- Preserving compatibility with pre-release checkpoint or control-plane JSON.

## Impact

- Workflow Plan storage, Runtime state, checkpoint codec, application startup,
  Evidence, workflow routes, tests, and documentation change.
- Existing executor implementations remain behind `ITaskExecutor`; recovery
  and repair do not parse Command, HTTP, WASM, model, or MCP fields.
