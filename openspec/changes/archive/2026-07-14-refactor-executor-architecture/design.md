## Context

The Workflow Runtime needs one stable seam for compiling and executing Task
types. Command execution additionally needs a lower seam because Workflow value
mapping and Minijail process supervision are separate responsibilities. HTTP
does not need a parallel transport interface because `HttpClient` already owns
that lower seam.

## Decisions

### Workflow owns the Task execution seam

`dagforge::workflow::ITaskExecutor` remains the only interface used by
`PlanCompiler` and `WorkflowRuntime`. It lives in `workflow/task_executor.hpp`.
`ExecutorRegistry` routes compile, start, cancel, and quiesce operations.

### Concrete Task executors live outside Workflow

`CommandTaskExecutor` and `HttpTaskExecutor` live under `dagforge/executors`.
They may depend on Workflow interfaces, but Workflow does not depend on their
implementations. Their public surface is factory functions returning
`shared_ptr<workflow::ITaskExecutor>`; implementation classes remain private.

### Command uses a runner seam

The lower Command abstraction is renamed to `ICommandRunner`. It accepts a
normalized `CommandSpec`, owns process lifecycle, and exposes start, cancel, and
quiesce. `MinijailCommandRunner` is its production implementation. Workflow
JSON parsing and Workflow value conversion do not enter the sandbox module.

### HTTP executor owns HTTP Task semantics

`HttpTaskExecutor` owns HTTP node compilation, Attempt state, timeout,
cancellation, status classification, and Workflow output mapping. Reusable
HTTP transport stays in `dagforge/http`. HTTP node schema and egress policy are
private executor modules so schema changes, security policy changes, and
lifecycle changes remain local.

### Registry owns executor lifecycle

`ExecutorRegistry::quiesce(timeout)` stops all registered Task executors from
accepting work and waits for their owned resources to settle. Application
quiesces Workflow Runtime first, then the registry, then Runtime threads. It
does not store or shut down Command-specific objects.

### Configuration follows the owning module

Sandbox configuration lives under `dagforge/sandbox`; HTTP Task executor
configuration lives under `dagforge/executors`. `SystemConfig` aggregates these
types instead of being included by lower modules.

### No speculative seams

The existing Task executor seam and Command runner seam are retained because
both have real callers and distinct responsibilities. No new transport,
policy, or factory hierarchy is introduced beyond the concrete ownership
required by this refactor.

## Dependency direction

```text
core
├── http
├── sandbox
└── workflow (Task executor seam and Runtime)

executors
├── workflow
├── http
└── sandbox

app
├── workflow
└── executors
```

## Compatibility

- Executor type strings remain `command` and `http`.
- Node config JSON remains unchanged.
- Compiled canonical JSON and Plan digests remain unchanged.
- Existing Task states, errors, retries, cancellation, output values, and
  security policy remain unchanged.
