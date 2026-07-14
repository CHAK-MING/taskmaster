## Context

DAGForge 0.4 has an executor-neutral `ITaskExecutor` seam, but only the
sandboxed Command adapter is currently registered. The repository already owns
an asynchronous Boost.Asio/Beast `HttpClient`, strict JSON utilities, owner-shard
Runtime semantics, and a real Workflow HTTP control plane. The HTTP executor
must reuse those modules, preserve the generic Runtime, and treat outbound HTTP
as a governed server capability rather than a permission granted by generated
Workflow JSON.

The implementation crosses configuration, application wiring, HTTP client I/O,
executor lifecycle, CLI validation, documentation, and real end-to-end tests.
It therefore needs an explicit design and security policy.

## Goals / Non-Goals

**Goals:**

- Prove the executor seam with an asynchronous, non-process execution model.
- Support deterministic HTTP/1.1 requests with static URLs, headers, optional
  input-derived headers/body, accepted-status policy, and stable outputs.
- Enforce exact origin authorization, plaintext opt-in, body/header limits,
  per-shard concurrency, total timeout, and cancellation.
- Reuse `HttpClient`, `Runtime`, `ExecutorRegistry`, Glaze, and existing error
  classification without adding dependencies or HTTP logic to WorkflowRuntime.
- Verify behavior through real JSON plans, a real target HTTP server, real
  DAGForge control-plane requests, and sanitizer runs.

**Non-Goals:**

- Redirect following, cookies, proxy support, connection pooling, HTTP/2,
  WebSocket, streaming responses, multipart bodies, client certificates, or
  arbitrary binary Workflow values.
- Dynamic URLs derived from task inputs. The first version keeps the complete
  origin and path in compiled Plan configuration so authorization happens at
  compile time.
- Artifact upload as an HTTP request body. HTTP response strings can still be
  externalized by the existing Run value store when large enough.
- Provider-specific model, MCP, OAuth, or retry protocols.

## Decisions

### Keep `ITaskExecutor` as the only Workflow seam

`HttpWorkflowAdapter` implements the existing `type`, `compile`, `start`, and
`cancel` interface. `WorkflowRuntime`, Plan graph semantics, retries, output
contracts, and state machines remain unchanged. This is preferred over adding
HTTP nodes or Runtime branches because it preserves locality and directly
tests the executor-neutral architecture.

### Use an exact server-owned origin allowlist

System configuration gains an `[http_executor]` section containing enablement,
`allow_plaintext`, exact `allowed_origins`, request/response body limits,
request/response header limits, and per-shard concurrency. Origins are
canonicalized as
scheme + lowercase host + effective port. URLs with credentials or fragments
are rejected. Plans cannot request broader access.

Exact origins are preferred over wildcard hosts, CIDRs, or a plan-level network
flag because the policy is easier to audit and resistant to suffix-matching
mistakes. Plain HTTP is disabled unless explicitly enabled. Redirects are not
followed, preventing an allowed endpoint from redirecting the executor to an
unauthorized origin.

### Define a strict executor-owned JSON contract

The v1 config contains:

- `method`: `GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `OPTIONS`, or `HEAD`.
- `url`: absolute static HTTP/HTTPS URL.
- `headers`: static name/value entries.
- `input_headers`: mappings from declared node inputs to header names.
- `body` or `body_input`: mutually exclusive static or input-derived body.
- `accepted_statuses`: optional unique HTTP statuses; omitted means 2xx.

The compiler rejects unknown fields, duplicate/invalid headers, CR/LF values,
executor-owned framing headers, undeclared inputs, bodies on GET/HEAD, invalid
statuses, unrecognized output ports, and oversized static bodies. It returns a
canonical JSON object so runtime start does not reinterpret untrusted syntax.

### Reuse and deepen `HttpClient`

`HttpClient` gains optional cancellation slots on connect and request methods.
Synchronous DNS resolution is replaced with cancellable `async_resolve`.
Existing call sites remain source compatible through defaulted parameters.
The client continues to own TLS trust loading, SNI, hostname verification,
HTTP parser limits, socket closure, and error propagation.

This is preferred over implementing Beast operations again inside the executor:
one deeper client module keeps transport behavior and fixes local.

### Keep active request state shard-owned

The executor stores one active map per Runtime shard. `start` runs on the
current owner shard, registers a shared request state, schedules a total
deadline, and spawns one lifecycle coroutine. `cancel` posts to all shards using
the existing executor-state helper, marks the matching request cancelled,
emits its cancellation signal, and closes an established client.

Completion, timer cancellation, active-map removal, and sink invocation all
occur on the request shard and are guarded by a completed flag. The generic
`execute_task_async` layer remains the second exactly-once guard and marshals the
result to the Run owner.

### Make HTTP status policy explicit

Accepted responses publish requested outputs. Non-accepted responses map as:

- 401/403 → `Unauthorized` (permanent)
- 404 → `NotFound` (permanent)
- 408 → `Timeout` (retryable timeout)
- 429 → `RateLimited` (retryable)
- other 4xx → `ProtocolError` (permanent)
- 5xx → `Unknown` (retryable under current Runtime classification)

This preserves generic Runtime retry policy without teaching it HTTP status
codes. Plans that need to inspect a normally non-success status list it in
`accepted_statuses`.

### Publish textual response values only

Outputs are `status` (integer), `body` (UTF-8 string), `headers` (JSON array of
name/value objects preserving duplicates), and `result` (same string as body).
Invalid UTF-8 is a protocol failure. This matches the current Workflow value
model and avoids silently placing arbitrary bytes into JSON strings. Binary
response support belongs in a later Artifact-aware executor contract.

### Test through public, real seams

The primary test starts a Python loopback HTTP target and the real DAGForge
service, registers real JSON plans through HTTP, and inspects Run snapshots and
outputs through HTTP. Fixtures cover mixed data flow, headers, accepted 404,
retrying 5xx, response-size rejection, timeout, and cancellation. Unit tests are
limited to reusable URL/config primitives where a real process seam is not
appropriate.

## Risks / Trade-offs

- [Exact allowlists require operational configuration] → Ship deny-by-default
  examples and allow CLI validation to load system configuration.
- [An allowlisted hostname can change DNS answers] → Treat allowlisting as
  explicit trust in that origin; require TLS by default so hostname
  verification protects HTTPS, and make plaintext a separate opt-in.
- [One connection per Attempt costs setup latency] → Keep v1 simple and
  cancellation-safe; add pooling only after profiling and a lifecycle design.
- [Failure results cannot expose non-accepted response bodies] → Allow plans to
  list statuses they need to inspect; a structured generic failure envelope is
  a separate Runtime-wide change.
- [Text-only bodies exclude binary APIs] → Fail closed on invalid UTF-8 and
  defer Artifact-aware binary transport rather than corrupt Workflow values.

## Migration Plan

1. Add the new configuration section with secure defaults and register the
   executor only when enabled.
2. Existing Command-only configurations continue to load because the section
   has defaults.
3. Operators explicitly add allowed origins and plaintext opt-in where needed.
4. Add `--config` to `dagforge validate` for policy-aware HTTP Plan validation.
5. Rollback consists of disabling `[http_executor].enabled`; no persisted Plan
   or Run schema changes are introduced.

## Open Questions

None for the v1 scope. Dynamic URLs, binary bodies, pooling, redirects, and
provider authentication are intentionally deferred.
