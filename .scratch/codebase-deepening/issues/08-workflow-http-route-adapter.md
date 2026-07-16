# 08 — Deepen the Workflow HTTP route adapter

**What to build:** Keep every Workflow endpoint explicit while centralizing the
shared rules for subsystem availability, typed path IDs, strict JSON bodies,
pagination, and `Result` to HTTP response conversion.

**Blocked by:** 07 — Make CLI parsing select one command

**Status:** resolved

- [x] Route registration still shows the complete endpoint inventory directly.
- [x] Runtime and Workflow Control Plane availability have one response policy.
- [x] Required path identifiers and JSON bodies have one strict extraction
      policy.
- [x] Pagination validation and response fields remain compatible.
- [x] Existing API contract tests pass without testing private helpers.

## Answer

Workflow routes now use one private `WorkflowHttpRequest` adapter. It owns the
transport knowledge that previously repeated across handlers: required Runtime
and Workflow Control Plane availability, typed path identifier extraction,
required or defaulted typed JSON bodies, lenient-compatible pagination, and the
request-body/header idempotency-key precedence rule. The adapter records the
first extraction failure, so each handler performs one explicit validation
check before entering its domain operation.

All 15 endpoint registrations remain directly visible in `workflows.cpp`.
Domain behavior, response fields, status codes, route paths, and JSON contracts
were not moved behind a generic route framework. `Result` error projection is
owned by the existing `result_error_response()` policy and now returns an HTTP
response directly instead of wrapping an operation that cannot fail in another
`Result`.

Verification:

- build, convention, and module graph checks passed;
- the complete `unit-api-tests` target passed: 120 tests from 14 suites;
- CLI scenarios passed, including semantic and raw HTTP API commands;
- the route inventory remains 15 explicit registrations;
- no direct subsystem lookup, path-parameter extraction, Start/Repair JSON
  parsing, or pagination parser remains in the route file.
