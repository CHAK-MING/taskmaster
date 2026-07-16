# 12 — Codebase duplication and redundancy audit

## Scope

This review covered all C++ files under `include/`, `src/`, and `tests/`: 179 files and approximately 33,500 lines at the start of the audit. The goal was to find repeated knowledge and redundant indirection, not to maximize a clone-reduction metric.

## Method

The audit combined exact file hashing, project include-graph construction, normalized seven-line clone detection, exact 32-token cross-file window detection, repeated helper-name and long-string searches, targeted semantic searches for validation and conversion rules, a thin-wrapper heuristic, focused Include-What-You-Use analysis where the compile database was usable, and deletion-test review for every proposed shared helper.

No exact duplicate production files, orphan private headers, internally unreferenced public headers, or large cross-file mechanical clones were found. Most apparent duplication was either interface declaration/definition pairing, type-local Glaze metadata, protocol-specific compile functions, or explicit lifecycle orchestration.

## Shared owners established

### Executor and sandbox configuration

Minijail resource-limit rules and HTTP egress request, response, concurrency, timeout, idle-pool, TLS-version, and client-identity rules now have one private owner in `src/dagforge/config/detail/executor_config_validation.hpp`. `SystemConfigLoader`, direct HTTP egress-policy creation, and Minijail runner creation use those predicates. The public configuration DTOs remain pure data rather than gaining implementation-oriented validation methods.

This also closed a consistency gap: direct `HttpEgressPolicy::create(...)` now rejects zero timeouts and inconsistent idle-connection limits instead of relying on the configuration loader to have run first.

### CommandSpec shape safety

Argument NUL checks and environment key, reserved-name, and value-shape checks now belong to `src/dagforge/sandbox/detail/command_validation.hpp`. `CommandPolicy` adds authorization policy on top, while the Minijail runner repeats only the call to the shared low-level predicate as a defense-in-depth boundary.

### WorkflowValue text projection

The canonical text projection for null, boolean, integer, real, string, JSON, and Artifact values now belongs to `workflow_value_text(...)` in `workflow/model`. Command environment mapping, HTTP scalar/JSON materialization, and Workflow string conditions use the same projection. HTTP still explicitly rejects Artifact values because that protocol contract differs from Command and Workflow condition semantics.

### Executor output contracts

The repeated loop that validates requested output ports now belongs to `executors/detail/task_executor_utils.hpp`. Command and HTTP retain separate supported-output tables so their protocol contracts remain locally visible.

### HTTP body views and hashing

`HttpResponse` now provides the same `body_as_string()` view already provided by `HttpRequest`, removing repeated byte-vector casts from CLI, executor, and tests. `StringHash` now delegates to the existing `util::hash_value` owner instead of selecting the hashing implementation independently.

### Test infrastructure

JSON payload creation, serialized-payload parsing, and payload materialization now live in `tests/json_test_utils.hpp`. Generic predicate/deadline polling now lives in `tests/test_utils.hpp::wait_until(...)` and replaced simple loops in Runtime, TimingWheel, HTTP client, and shard-affinity tests. Benchmark HTTP port selection now uses the existing test port helper instead of owning a second socket/bind implementation.

### Removed redundant wrappers

Two local `bytes_of(...)` wrappers were deleted in favor of direct `std::as_bytes(std::span{...})`, because a shared one-line wrapper would add vocabulary without owning policy. `PlanStore::encode_stored_plan(...)` was deleted because it only forwarded one call to `serialize_json(...)`.

## Deliberate non-extractions

Command and HTTP `parse_node_config(...)` and `encode_node_config(...)` remain separate. Their syntax is similar, but each is the entry to a distinct private typed contract and a generic schema wrapper would not own additional knowledge.

TLS validation was not generalized across `ApiConfig`, `HttpClientConfig`, and `HttpEgressConfig`. They are different models at different layers; a generic TLS helper would require a parameterized mirror of three contracts. Each model keeps its own invariant until a real shared TLS configuration concept exists.

Configuration-file loading, CLI binary/stdin loading, `/proc` reads, and durable storage reads remain separate. Their data types, path-trust assumptions, byte limits, durability requirements, and error contracts differ materially.

The two IO await sites that treat `operation_aborted` as normal cancellation remain local. The repeated policy is only a few lines, each wraps a different awaitable, and a helper would be a shallow error-forwarding adapter.

Plan and Checkpoint catalog loops remain after shared file discovery because decode, record identity, conflict detection, cache reconciliation, and ordering are store-specific policy. Glaze `rename_key` metadata remains type-local because it is part of each wire schema.

Public accessors, typed ID factories, error-category adapters, and interface forwarding methods were not removed merely for having one-line bodies. Their names are the stable contract and removing them would expose representation or erase domain vocabulary.

## Standards review

No unresolved Standards findings remain in the applied slice. Shared code was placed with the concept owner, no generic `common` or repository framework was introduced, public configuration DTOs were kept free of private implementation policy, and protocol-specific invariants remain visible at their boundaries.

Focused IWYU output was treated as advisory because C++ modules and header-unit preprocessing caused it to recommend internal Boost implementation headers and removal of required project interface headers. Only independently confirmed unused standard-library includes were removed.

## Spec review

No CLI, HTTP JSON, Workflow Plan, Checkpoint, Evidence, or Artifact format changed. The only intentional behavior tightening is that direct HTTP egress-policy construction now enforces the same timeout and idle-pool constraints as System Configuration loading.

## Verification

- build2 build, module graph, and agent-convention checks passed.
- Focused core tests passed: 85 tests across configuration, Runtime, TimingWheel, shard affinity, HTTP client, HTTP egress, HTTP executor, and Command runner.
- Focused Workflow and Command tests passed: 10 tests.
- Normal all-unit suite passed: 265/265.
- ASAN/UBSAN all-unit suite passed: 265/265.
- CLI subprocess scenarios all passed.
- Exact duplicate-file, include-graph, residual invariant, token-window, and `git diff --check` scans passed.

## Remaining work

The large Workflow runtime, HTTP executor, Minijail runner, and HTTP client still contain substantial stateful code, but their size is not evidence of duplication. Their remaining work belongs to the existing architecture tickets: typed HTTP compiled contract, sandbox plan/supervisor separation, Evidence recovery, bounded storage reads, Artifact pair reconciliation, and Workflow library-header classification.
