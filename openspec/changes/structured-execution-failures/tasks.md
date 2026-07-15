## 1. Execution contract

- [x] 1.1 Add the executor-neutral `ExecutionFailure` value and JSON projection.
- [x] 1.2 Replace Task completion and async registry adapters with
      `TaskExecutionResult`.
- [x] 1.3 Add focused contract tests for structured success, failure, and start
      rejection.

## 2. Concrete executors

- [x] 2.1 Map Command timeout, resource exhaustion, runner failure, and non-zero
      exit into structured failures with bounded diagnostics.
- [x] 2.2 Map rejected HTTP statuses, invalid responses, transport errors,
      cancellation, and timeout into structured failures.
- [x] 2.3 Add Command and HTTP tests that assert stable codes and details.

## 3. Runtime and persistence

- [x] 3.1 Replace Attempt, Task, and Run string errors with
      `optional<ExecutionFailure>`.
- [x] 3.2 Propagate Runtime-originated and executor-originated failures through
      retry, cancellation, terminal state, and Evidence paths.
- [x] 3.3 Round-trip structured failures through checkpoint storage.

## 4. Control plane and verification

- [x] 4.1 Emit structured snapshot failures and textual Evidence types from the
      workflow control-plane routes.
- [x] 4.2 Update API, persistence, scenario, and benchmark fixtures for the new
      contract.
- [x] 4.3 Run formatting, focused tests, full tests, module/convention checks,
      strict OpenSpec validation, and an AddressSanitizer workflow.
