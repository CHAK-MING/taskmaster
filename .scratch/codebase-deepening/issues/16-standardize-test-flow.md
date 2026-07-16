# 16 — Standardize the test flow

**What to build:** Replace overlapping GoogleTest executables with non-overlapping unit, component, and integration targets, then provide one script that owns the normal local verification sequence.

**Status:** resolved

**Owner:** OpenAI

- [x] Every C++ test source belongs to exactly one test executable.
- [x] The default test command is fast and does not require Minijail or real-service prerequisites.
- [x] Integration and end-to-end modes fail clearly when required tools or sandbox artifacts are absent.
- [x] Coverage and sanitizer workflows use the same test target taxonomy.
- [x] GoogleTest filters, repeat, shuffle, and sharding remain usable through the standard runner.
- [x] Developer documentation names one authoritative local test entry point.

**Evidence:** `scripts/check-test-layout.py` assigns 20 `*_test.cpp` sources exactly once across `unit-tests`, `component-tests`, and `integration-tests`. `bash scripts/test.sh all` passed 54 unit tests, 202 component tests, 19 integration tests, all CLI subprocess scenarios, and all real Workflow scenarios. Runtime ASAN/TSAN/UBSAN audit passed, full ASAN/UBSAN runs passed for all three C++ targets and CLI scenarios, and production line coverage passed at 90.05%.
