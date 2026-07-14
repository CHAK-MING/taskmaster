## Why

The current executor implementation uses the word "executor" for two different
abstraction levels. `workflow::ITaskExecutor` executes Workflow Tasks, while
`ICommandExecutor` is a Minijail-backed process runner. Command is implemented
as a Workflow adapter over the lower layer, but HTTP contains its complete Task
lifecycle inside a file named `http_adapter.cpp`. The naming, placement, and
ownership model therefore obscure the real architecture and require
`Application` to special-case Command shutdown.

## What Changes

- Keep the Workflow-owned Task execution seam and rename its public header to
  `task_executor.hpp`.
- Move concrete Workflow Task executors to `dagforge/executors` as
  `CommandTaskExecutor` and `HttpTaskExecutor`.
- Move Minijail and process supervision to `dagforge/sandbox`; rename the lower
  abstraction to `ICommandRunner` / `MinijailCommandRunner`.
- Make `CommandTaskExecutor` own its Command runner and make
  `ExecutorRegistry` own and quiesce all Task executors.
- Split HTTP node schema and egress policy from the HTTP Attempt lifecycle.
- Move module-specific configuration out of `SystemConfig` dependencies and
  let `SystemConfig` aggregate the module-owned config types.
- Preserve Workflow JSON, Plan digests, Task/Run semantics, HTTP behavior, and
  sandbox behavior.

## Capabilities

### New Capabilities

- `task-executor-architecture`: Defines Task executor ownership, lower-level
  runner seams, dependency direction, naming, and unified lifecycle.

### Modified Capabilities

- `http-task-execution`: The HTTP capability is implemented by a concrete
  `HttpTaskExecutor` while retaining the existing external behavior.

## Non-goals

- Changing Workflow JSON or adding a new executor.
- Changing retry, cancellation, timeout, output, security, or persistence
  semantics.
- Splitting the project into separately shipped packages in this change.
- Introducing additional interfaces for hypothetical implementations.

## Impact

- Executor, sandbox, Workflow, HTTP, Application, build manifests, module
  interfaces, tests, documentation, and architecture checks change.
- Existing real Workflow plans and control-plane APIs remain compatible.
