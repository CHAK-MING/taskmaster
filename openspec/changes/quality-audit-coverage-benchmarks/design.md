# Design

## Coverage

Use Clang source-based coverage with build2 `cc.reprocess` enabled so coverage
maps to original source files rather than build2's temporary `.ii` files. The
coverage command runs both `all-unit-tests` and the unattended real Workflow
suite, merges all process profiles, reports per-file coverage, and fails below
90% source-line coverage.

Coverage additions SHALL be scenario-driven. Tests should exercise observable
contracts, failure paths, cancellation, shutdown, retry, persistence, and API
behavior. Tests that only call getters or duplicate implementation branches to
raise a number are not acceptable.

## Standards and implementation audit

Automated scans cover naming, public-header guards, forbidden dependency
directions, raw Asio awaits, direct `std::expected` construction, header
namespace pollution, oversized source units, and suspicious TODO/fallback
patterns. Manual review focuses on Runtime dispatch, Workflow scheduling and
persistence, HTTP transport/executor lifecycle, Command sandbox supervision,
and Application shutdown.

In-scope violations are fixed immediately and protected by tests or static
checks. Large refactors without a demonstrated correctness, maintainability,
or performance problem are avoided.

## Benchmarks

Benchmarks SHALL describe a production question, isolate setup from measured
work, report throughput or latency in meaningful units, and use stable workload
sizes. The benchmark suite will cover at least:

- same-shard and cross-shard Runtime dispatch under controlled batches;
- Workflow compile/dispatch or value propagation without external process
  noise;
- HTTP keep-alive versus reconnect behavior against a local target;
- storage codec/checkpoint behavior at representative plan sizes.

Each reported benchmark is run with warmup and repeated samples. Median,
tail latency, standard deviation, and benchmark environment are retained in a
machine-readable result file.
