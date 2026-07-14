## Why

DAGForge can execute governed HTTP requests and launch commands through
Minijail, but the current boundaries still contain development-oriented
defaults and several production hazards: TLS-enabled API listeners also accept
plaintext, idle handshakes can consume accept capacity, outbound DNS results
are not checked against network policy, HTTP concurrency is only per shard,
and command execution can be configured to accept arbitrary programs despite
Minijail's documented known-binary threat model.

## What Changes

- Make the HTTP server explicitly plaintext or TLS-only, bound handshake/read
  time, limit active connections and requests per connection, reject unsupported
  methods, and expose parser limits through configuration.
- Harden outbound HTTP with resolved-address policy, private/special network
  denial by default, CIDR exceptions, a process-wide concurrency ceiling,
  minimum TLS version, optional private CA trust, and optional mTLS identity.
- Build HTTP requests with Boost.Beast messages rather than handwritten wire
  serialization.
- Define the command sandbox as a known-binary containment boundary, make
  program/environment allowlists fail closed by default, validate trusted
  helper/policy files at startup, harden workspace creation, and bound captured
  output and individual streamed lines.
- Add startup preflight and shutdown behavior so sandbox configuration errors
  are detected before accepting Workflows and active process groups are killed
  during application teardown.
- Add production-oriented tests, documentation, and release verification.

## Capabilities

### New Capabilities

- `production-http-boundary`: TLS-only inbound operation, bounded connection
  lifecycle, outbound SSRF controls, TLS policy, and global request capacity.
- `production-command-sandbox`: Known-binary threat model, fail-closed policy,
  trusted helper verification, bounded output, secure workspaces, and teardown.

### Modified Capabilities

- `http-task-execution`: HTTP task execution additionally obeys resolved-address
  policy, global capacity, and configured TLS identity/trust.

## Non-goals

- Treating Minijail as a security boundary for attacker-supplied binaries or
  attacker-controlled shared libraries.
- Adding redirects, proxy support, cookies, HTTP/2, or a general service mesh.
- Adding a container, gVisor, microVM, or Kubernetes runtime in this release.

## Impact

- `SystemConfig`, TOML loading, HTTP client/server, HTTP Workflow adapter,
  command executor, Application lifecycle, production configuration examples,
  tests, and release verification change.
- No new linked library is introduced. Linux address classification uses
  Boost.Asio; TLS remains OpenSSL; command isolation remains pinned Minijail.
