# Standard test flow review

## Scope

This review covers the test target split, the standard test runner, prerequisite handling, sanitizer and coverage integration, CLI scenario expansion, and the one production defect exposed by the new output-failure scenario.

## Standards review

No blocking findings.

The build graph no longer compiles the same test source into multiple executables. `unit-tests`, `component-tests`, and `integration-tests` are mutually exclusive source sets, and `scripts/check-test-layout.py` makes duplicate or unassigned `*_test.cpp` files a build-time failure.

`scripts/test.sh` is the single local entry point. `quick` has no Minijail requirement; `integration` and `all` check Python, OpenSSL, Minijail, and the seccomp policy before building or running dependent tests. GoogleTest arguments remain transparent, with environment shortcuts for filter, repeat, and shuffle rather than a second filtering system.

The coverage workflow now selects raw profiles by ELF Build ID and reports `libdagforge.so` and the CLI executable against their own source sets. This removes profile mismatch warnings caused by combining test-executable and production-object instrumentation while preserving a weighted production-line total.

The expanded CLI transport scenarios assert actual HTTP methods, paths, headers, body bytes, TLS trust, output files, non-success status handling, stdin and `@file` bodies, forbidden framing headers, and prerequisite-independent validation errors. They do not call CLI private helpers.

The `/dev/full` output scenario found a real buffered-I/O defect: `ofstream::write()` could appear successful until destruction. `write_response_body()` now explicitly flushes and checks the stream before reporting success.

## Specification review

No blocking findings.

- Every one of the 20 C++ test source files is assigned exactly once.
- `quick` runs module smoke, 54 unit tests, and 202 component tests without Minijail.
- `integration` runs 19 Minijail tests and all CLI subprocess scenarios.
- `e2e` runs the real service, executor, sandbox, HTTP, cancellation, shutdown, and TLS scenarios and validates 24 real Workflow JSON plans.
- Missing Minijail fails with an explicit path before test execution.
- ASAN, TSAN, and UBSAN runtime audit passes against `component-tests`.
- Full ASAN/UBSAN passes for module smoke, all three C++ test targets, and CLI subprocess scenarios.
- Coverage produces separate library and CLI reports with no profile mismatch warnings and passes the 90% gate at 90.05%.

## Verification

- `bash scripts/test.sh all`
- `bash scripts/test-runtime-audit.sh`
- `bash scripts/test-coverage.sh`
- ASAN/UBSAN build and execution of `modules-foundation-smoke`, `unit-tests`, `component-tests`, `integration-tests`, and CLI scenarios
- `python3 scripts/check-test-layout.py`
- shell syntax checks and Python bytecode compilation
- module graph, agent convention, and diff whitespace checks
