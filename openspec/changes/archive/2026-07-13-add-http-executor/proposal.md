## Why

DAGForge currently ships only the sandboxed `command` executor, so the new
executor-neutral Workflow Runtime has not yet been proven against a genuinely
different asynchronous execution model. A governed HTTP executor is the
smallest useful second adapter and enables generated Workflows to call remote
services without granting arbitrary network access to sandboxed commands.

## What Changes

- Add a registered `http` task executor that performs HTTP/1.1 requests through
  the existing asynchronous HTTP client while keeping HTTP semantics out of
  `WorkflowRuntime`.
- Add strict executor-owned JSON configuration for method, absolute URL,
  headers, input-to-header/body bindings, and accepted response statuses.
- Add server-owned HTTP executor policy for enablement, exact origin allowlists,
  plaintext opt-in, request/response limits, and per-shard concurrency.
- Make HTTP connection, DNS resolution, TLS handshake, request I/O, timeout,
  and cancellation cooperate with the Task/Attempt lifecycle.
- Define stable `status`, `body`, `headers`, and `result` outputs and deterministic
  HTTP-status-to-failure classification.
- Extend CLI validation, system configuration, documentation, and real
  Command → HTTP → Command Workflow JSON end-to-end coverage.

## Capabilities

### New Capabilities

- `http-task-execution`: Governed asynchronous HTTP task compilation,
  execution, cancellation, resource bounds, outputs, and real mixed-executor
  Workflow validation.

### Modified Capabilities

None.

## Impact

- New executor adapter files under `include/dagforge/executor/` and
  `src/dagforge/executor/`.
- `SystemConfig`, TOML loading, `Application` construction, CLI validation,
  build2 source lists, and user documentation gain HTTP executor support.
- The existing `HttpClient` gains cancellable asynchronous DNS/connect/request
  operations but retains its current caller-facing behavior through defaulted
  cancellation parameters.
- No new third-party dependency is introduced; Boost.Asio, Boost.Beast,
  Boost.URL, OpenSSL, Glaze, and existing DAGForge abstractions are reused.
