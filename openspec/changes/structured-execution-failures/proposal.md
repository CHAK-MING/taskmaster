## Why

Task execution currently collapses completion failures into
`std::error_code`. Command exit diagnostics and rejected HTTP response data are
therefore lost before Workflow Runtime, Evidence, persistence, and the control
plane can expose them. A human or AI repair loop cannot make a reliable Plan
revision from messages such as `unknown error` or `protocol error`.

The project is still pre-release. The execution contract can be corrected at
its owning seam instead of adding compatibility wrappers or executor-specific
side channels.

## What Changes

- Replace the Task completion `Result<ExecutorOutputs>` contract with one
  structured execution result shared by every Task executor.
- Define a stable machine code, normalized DAGForge error kind, human message,
  and JSON details for each execution failure.
- Make Command completion retain exit code, bounded stdout/stderr, timeout,
  resource-limit, and runner diagnostics.
- Make HTTP completion retain rejected status, response headers/body, and
  transport or protocol diagnostics.
- Store the same structured failure on Attempt, Task, and Run snapshots and in
  checkpoints.
- Emit structured failures through Evidence and workflow-run JSON responses.
- Remove legacy string-only `error` and `last_error` fields instead of keeping
  a parallel compatibility path.

## Capabilities

### New Capabilities

- `structured-execution-failures`: Defines the executor-neutral failure
  contract and its propagation through Runtime, persistence, Evidence, and the
  control plane.

### Modified Capabilities

- `task-executor-architecture`: Task completion uses the structured result at
  the existing Workflow-owned executor seam.

## Non-goals

- Plan revision, successful-node reuse, or repair-run orchestration.
- Executor capability/schema export.
- Adding LLM, WASM, browser, or other new executors.
- Preserving checkpoint or API compatibility with pre-release formats.

## Impact

- Workflow public types, executor implementations, Runtime state, storage
  codec, Evidence projection, workflow routes, tests, and documentation change.
- Plan JSON and successful output values remain unchanged.
