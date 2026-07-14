# HTTP task execution

## ADDED Requirements

### Requirement: HTTP connections are reused within bounded shard pools

The HTTP executor SHALL retain reusable HTTP/1.1 connections in a shard-owned
pool keyed by exact authorized Origin. The pool SHALL enforce administrator
configured per-Origin and per-shard idle limits.

#### Scenario: Retry to the same Origin

- **WHEN** sequential Attempts on one owner shard target the same Origin and
  the server keeps the connection alive
- **THEN** the executor reuses the established connection rather than repeating
  DNS, TCP connect, and TLS handshake

#### Scenario: Executor quiesce

- **WHEN** executor shutdown begins with idle pooled connections
- **THEN** all idle clients are closed and no new client is returned to the
  pool

### Requirement: HTTP transport stages have independent timeouts

The HTTP executor SHALL apply independent server-owned timeout values to DNS,
connect, TLS handshake, write, first-byte/header, and response-read stages while
remaining bounded by the Workflow Task timeout.

#### Scenario: Server accepts but does not respond

- **WHEN** the request write completes but no response header arrives before
  `first_byte_timeout_ms`
- **THEN** the Attempt fails with a first-byte timeout error

### Requirement: HTTP transport failures identify their stage

HTTP client errors SHALL identify DNS, connect, TLS handshake, write,
first-byte, or read failure. Timeout variants SHALL compare equivalent to the
generic timed-out condition so Workflow Runtime can classify them without an
HTTP dependency.

#### Scenario: TCP connection is refused

- **WHEN** DNS succeeds but no service accepts the target endpoint
- **THEN** the client reports a connect-stage failure rather than an unknown
  system error
