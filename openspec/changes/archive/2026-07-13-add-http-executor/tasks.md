## 1. Specification and Red Tests

- [x] 1.1 Validate the OpenSpec proposal, capability spec, design, and task graph.
- [x] 1.2 Add real HTTP Workflow JSON fixtures and extend the real service test with an actual loopback target server.
- [x] 1.3 Run the real HTTP suite before implementation and capture the expected unsupported-executor failure.

## 2. Configuration and URL Policy

- [x] 2.1 Add secure server-owned HTTP executor configuration and strict TOML loading/validation.
- [x] 2.2 Add canonical absolute HTTP URL/origin parsing used by policy checks.
- [x] 2.3 Wire policy-aware CLI Plan validation through an optional system config.

## 3. Cancellable HTTP Transport

- [x] 3.1 Replace synchronous DNS resolution in `HttpClient` with cancellable asynchronous resolution.
- [x] 3.2 Add optional cancellation slots to TCP, TLS, Unix connect and request operations without breaking current callers.
- [x] 3.3 Make response header and body limits configurable through the client.

## 4. HTTP Task Executor

- [x] 4.1 Implement strict HTTP executor config compilation, header safety, input binding validation, output validation, and origin admission.
- [x] 4.2 Implement request body/header materialization and stable status/body/headers/result outputs.
- [x] 4.3 Implement per-shard active request state, concurrency limits, total timeout, cancellation, cleanup, and exactly-once completion.
- [x] 4.4 Implement deterministic accepted-status and failure classification behavior.
- [x] 4.5 Register the HTTP executor in `Application` and add it to build2 sources.

## 5. Real Verification and Documentation

- [x] 5.1 Make the real Command → HTTP → Command dataflow, accepted-status, retry, response-limit, timeout, and cancellation Workflows pass.
- [x] 5.2 Run repeated real Workflow execution, all unit tests, module/convention checks, and diff checks.
- [x] 5.3 Run ASAN/UBSAN/LSAN and TSAN/runtime audit appropriate to the asynchronous lifecycle changes.
- [x] 5.4 Document the HTTP executor JSON contract, system policy, outputs, limitations, and real test command in English and Chinese docs.

## 6. Audit and Completion

- [x] 6.1 Audit the diff against AGENTS.md, the OpenSpec requirements, async/shard lifecycle rules, SSRF/header-smuggling risks, and executor-neutral architecture.
- [x] 6.2 Resolve all audit findings, rerun affected checks, mark every OpenSpec task complete, and validate the completed change.
