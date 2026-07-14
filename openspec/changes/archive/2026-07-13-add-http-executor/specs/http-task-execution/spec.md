## ADDED Requirements

### Requirement: HTTP executor registration and isolation
DAGForge SHALL register an `http` task executor when the server-owned HTTP
executor configuration is enabled, and `WorkflowRuntime` SHALL remain unaware
of HTTP methods, URLs, headers, and response semantics.

#### Scenario: HTTP plan compiles through the executor registry
- **WHEN** a Workflow node selects executor `http` with valid configuration
- **THEN** the Plan Compiler delegates validation to the HTTP executor and
  produces an immutable executor-owned configuration

#### Scenario: HTTP executor is disabled
- **WHEN** server configuration disables the HTTP executor
- **THEN** an HTTP Workflow plan is rejected as an unsupported executor

### Requirement: Strict HTTP task configuration
The HTTP executor SHALL accept only a strict JSON object containing a supported
method, an absolute HTTP or HTTPS URL, optional static headers, optional input
header bindings, at most one static or input-derived request body, and optional
accepted response statuses. Unknown fields, unsafe headers, duplicate headers,
undeclared input bindings, invalid methods, and ambiguous body configuration
MUST be rejected during Plan compilation.

#### Scenario: Valid input-derived request body
- **WHEN** a node declares an input and references it with `body_input`
- **THEN** compilation succeeds and the runtime value is serialized as the
  request body when the Attempt starts

#### Scenario: Undeclared body input
- **WHEN** `body_input` references a name that is not declared by the node
- **THEN** Plan compilation fails with an invalid argument error

#### Scenario: Request-smuggling header is declared
- **WHEN** configuration attempts to set `Host`, `Content-Length`,
  `Transfer-Encoding`, `Connection`, or another executor-owned hop-by-hop header
- **THEN** Plan compilation fails

### Requirement: Server-owned outbound origin policy
Every HTTP URL SHALL be authorized against an exact canonical origin allowlist
owned by system configuration. Plain HTTP SHALL require explicit server opt-in,
and Workflow JSON MUST NOT be able to expand or bypass the allowlist.

#### Scenario: Allowed HTTPS origin
- **WHEN** a task URL has the same canonical scheme, host, and port as an
  allowed origin
- **THEN** Plan compilation accepts the URL

#### Scenario: Unlisted origin
- **WHEN** a task URL does not match an allowed origin
- **THEN** Plan compilation fails with an unauthorized error

#### Scenario: Plain HTTP is not enabled
- **WHEN** a task uses an `http://` origin and plaintext requests are disabled
- **THEN** Plan compilation fails with an unauthorized error

### Requirement: Bounded asynchronous request execution
HTTP Attempts SHALL use asynchronous DNS resolution, connection, optional TLS
handshake, write, and read operations. The executor SHALL enforce the node
deadline across the complete Attempt, the configured request and response body
limits, request and response header limits, and a per-shard active-request
ceiling.

#### Scenario: Response exceeds configured limit
- **WHEN** a server returns a body larger than the configured response limit
- **THEN** the Attempt fails without publishing a partial body

#### Scenario: Per-shard concurrency is exhausted
- **WHEN** a new HTTP Attempt would exceed the configured active-request limit
  on its shard
- **THEN** start fails with a retryable queue-full error

#### Scenario: TLS endpoint succeeds
- **WHEN** an allowed HTTPS origin presents a certificate valid for its host
- **THEN** the executor completes the TLS handshake and sends the request

### Requirement: Request cancellation and timeout convergence
Cancellation and timeout SHALL interrupt in-flight DNS, connect, TLS, write,
and read operations. Every started HTTP Attempt MUST invoke completion at most
once and MUST eventually leave the active-request registry.

#### Scenario: Run cancellation during HTTP read
- **WHEN** a Run is cancelled while an HTTP response is pending
- **THEN** the socket operation is cancelled, the Attempt becomes cancelled,
  and the Run can reach its terminal state

#### Scenario: Node deadline expires
- **WHEN** the complete HTTP Attempt exceeds the node timeout
- **THEN** the executor cancels outstanding I/O and completes with timeout

### Requirement: Stable HTTP outputs
Successful HTTP execution SHALL expose only requested output ports from the
set `status`, `body`, `headers`, and `result`. `status` SHALL be an integer,
`body` and `result` SHALL be UTF-8 strings containing the response body, and
`headers` SHALL preserve duplicate response header fields in JSON form.

#### Scenario: Requested standard outputs
- **WHEN** an accepted HTTP response is received and all four standard outputs
  are requested
- **THEN** the executor publishes status, body, ordered header entries, and a
  result value equal to the body

#### Scenario: Response body is not valid UTF-8
- **WHEN** an accepted response contains invalid UTF-8 bytes
- **THEN** the Attempt fails with a protocol error instead of publishing an
  invalid Workflow string

### Requirement: Deterministic HTTP status handling
The executor SHALL treat status codes listed in `accepted_statuses` as success,
or all 2xx statuses when the list is omitted. Non-accepted statuses SHALL map
deterministically to DAGForge errors so authentication and client errors are
permanent while throttling, timeout, and server failures remain retryable.

#### Scenario: Explicitly accepted non-2xx status
- **WHEN** a task accepts status 404 and the server returns 404
- **THEN** the Attempt succeeds and publishes the response outputs

#### Scenario: Retryable server response
- **WHEN** a server returns a non-accepted 5xx status and the node has retries
- **THEN** the Attempt fails as retryable and WorkflowRuntime schedules the
  next Attempt according to node retry policy

#### Scenario: Permanent authorization response
- **WHEN** a server returns a non-accepted 401 or 403 status
- **THEN** the Attempt fails permanently without consuming configured retries

### Requirement: Real mixed-executor Workflow verification
The repository SHALL include real Workflow JSON and an unattended end-to-end
test that starts an actual HTTP target server and `dagforge serve`, then executes
Command → HTTP → Command data flow through the public control plane.

#### Scenario: Command to HTTP to Command pipeline
- **WHEN** the real Workflow test runs the mixed-executor JSON
- **THEN** a Command output becomes the HTTP request body, the real target
  response becomes a downstream Command input, and the declared Workflow output
  matches the independently known expected value

#### Scenario: Real retry timeout cancellation and limits
- **WHEN** the end-to-end suite runs HTTP retry, response-limit, timeout, and
  cancellation Workflows
- **THEN** snapshots and Attempt histories expose the required terminal states
  without fake or recording executors
