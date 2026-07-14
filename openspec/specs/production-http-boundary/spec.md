# production-http-boundary Specification

## Purpose
Define production HTTP listener, connection, outbound network, TLS, capacity,
timeout, cancellation, and shutdown guarantees shared by the control plane and
HTTP Task execution.
## Requirements
### Requirement: TLS listeners do not accept plaintext
When TLS credentials are configured, the HTTP server SHALL perform a TLS
handshake for every accepted connection and SHALL NOT route a plaintext request
on that listener.

#### Scenario: Plaintext sent to TLS listener
- **WHEN** a client sends an HTTP/1.1 plaintext preface to a TLS-only listener
- **THEN** the connection is closed without invoking a route

### Requirement: Connection lifecycle is bounded
The HTTP server SHALL enforce configured active-connection capacity, handshake
and request idle deadlines, parser header/body limits, and maximum requests per
connection.

#### Scenario: Slow client cannot block accepts
- **WHEN** one client connects and sends no protocol bytes
- **THEN** other clients continue to be accepted and the idle client is closed
  at the configured deadline

#### Scenario: Keep-alive request limit
- **WHEN** a connection reaches the configured request count
- **THEN** the final response disables keep-alive and the server closes it

### Requirement: Unsupported methods are rejected
The server SHALL NOT reinterpret an unsupported HTTP method as another method.

#### Scenario: TRACE request
- **WHEN** a request uses TRACE or another unsupported verb
- **THEN** the server returns Method Not Allowed without invoking a GET route

### Requirement: Resolved destinations obey network policy
Outbound HTTP SHALL evaluate every DNS result before connect and SHALL connect
only to addresses allowed by the server-owned network policy.

#### Scenario: Public origin resolves to loopback
- **WHEN** an allowed hostname resolves only to loopback or another denied
  special-use address
- **THEN** execution fails with an authorization error before connect

#### Scenario: Explicit private CIDR exception
- **WHEN** a resolved private address is contained by an allowed CIDR
- **THEN** the client may connect to that address

### Requirement: Outbound TLS policy is explicit
HTTPS execution SHALL verify the peer hostname, enforce the configured minimum
TLS version, and reject incomplete CA or client-identity configuration.

#### Scenario: mTLS pair is incomplete
- **WHEN** only a client certificate or only a private key is configured
- **THEN** HTTP executor construction fails

### Requirement: HTTP execution has global capacity
The HTTP executor SHALL enforce a process-wide active-request ceiling in
addition to the per-shard ceiling.

#### Scenario: Global limit exhausted
- **WHEN** the process-wide request count is at the configured maximum
- **THEN** a new task start fails with resource exhaustion and no socket opens

### Requirement: HTTP teardown is executor-affine and drained
HTTP server shutdown SHALL close active sockets on their owning asynchronous
executor and SHALL wait for connection handlers to unregister. Application
shutdown SHALL cancel active HTTP tasks and wait for their coroutine frames to
finish before Runtime threads stop.

#### Scenario: Shutdown during an outbound HTTP request
- **WHEN** Application shutdown begins while an HTTP task is blocked in DNS,
  connect, TLS, write, or read
- **THEN** the task is cancelled, the Run reaches a terminal state, and no HTTP
  client or Workflow coroutine remains live after Runtime teardown

#### Scenario: Server stop with an idle connection
- **WHEN** the HTTP server stops while a keep-alive connection is idle
- **THEN** socket closure runs on its owning executor and the handler is drained
  without a cross-thread socket race
