# Changelog

All notable changes to DAGForge will be documented in this file.

## [Unreleased]

### Breaking Changes
- Removed the unused in-process ComputePool, its Runtime submission API,
  configuration, metrics, and tests. Workflow CPU work remains isolated in
  sandboxed command processes.
- Replaced protocol-specific node variants with executor-neutral Tasks. Each
  Task selects a registered executor and carries executor-owned JSON config;
  Workflow Runtime no longer interprets Command, HTTP, Python, Model, or Tool
  behavior.
- Workflow Plans are now strict JSON only. System configuration remains TOML.
- Replaced the flat run/node lifecycle with explicit Run, Task, and Attempt
  state machines. Cancellation and fail-fast now remain `stopping` until every
  active attempt is reaped.
- Removed Approval nodes and approval control-plane routes. External waits are
  no longer modeled as executors.
- Removed XCom and XCom-based branching from the C++ runtime, including persistence, APIs, CLI, metrics, modules, tests, and examples.
- Removed the retired 0.3 TaskConfig, DAG manager, scheduler, cron, sensor, MySQL persistence, management CLI, DAG REST routes, and Web UI stacks.
- Replaced `[scheduler]`, `[database]`, and `[dag_source]` configuration with the 0.4 `[runtime]`, `[sandbox]`, `[workflow]`, and `[api]` contract.
- Removed legacy DAG/task IDs and the DAG-specific Lua task context.
- Replaced the Shell, Docker, Lua, Noop, and composite executor stack with a
  generic Task executor registry. The shipped `command` adapter remains
  sandboxed and has no direct-host fallback.
- Unknown TOML fields are rejected instead of being silently ignored.

### Added
- Added a reproducible Clang source-coverage workflow with a 90% production
  source-line gate, scenario-driven coverage expansion, repository convention
  checks, and normal/module/sanitizer validation.
- Added controlled Runtime, Plan, Workflow, HTTP keep-alive/reconnect, and
  checkpoint benchmarks with repeated samples, environment metadata, median,
  p95, p99, standard deviation, coefficient of variation, and throughput.
- Added bounded owner-shard HTTP/1.1 keep-alive pools, independent DNS,
  connect, TLS-handshake, write, first-byte, and response-read timeouts, and
  stage-specific transport errors that remain generically classifiable by the
  Workflow Runtime.
- Added an optional executor-neutral HTTP task adapter with strict JSON config,
  exact server-owned origin allowlists, HTTPS verification, input-derived
  headers and bodies, accepted-status policy, bounded responses, per-shard
  concurrency, cancellation, timeout, and stable response outputs.
- Added resolved-address egress policy with private/special-use denial and CIDR
  exceptions, process-wide HTTP capacity, TLS minimum-version control,
  additional CA trust, optional mTLS, and TLS-only API listeners.
- Added active HTTP connection, idle-time, parser, and requests-per-connection
  limits. Server shutdown closes and drains active connections.
- Added fail-closed known-binary Command policy, trusted Minijail/BPF preflight,
  private workdirs, stdout/stderr/line limits, and executor shutdown that
  kills and reaps active process groups.
- Added a server-owned Command program registry, exact name-to-path resolution
  without PATH lookup, and a minimal inherited-environment allowlist that
  rejects credential-like host variables.
- Added OpenSpec coverage and real Command → HTTP → Command Workflow JSON tests
  for TLS, retry, cancellation, timeout, response limits, UTF-8 validation,
  status handling, and outbound-policy rejection.
- Added explicit `plan_id` Run selection, Artifact upload/download/delete
  routes, collection pagination, and bounded completed-Run/Evidence retention.
- Added environment-backed Bearer authentication, parser and route request-body
  limits, and a global concurrent-request ceiling for the HTTP control plane.
- Added optional file-backed Run checkpoints, append-only Evidence, durable
  Artifacts, completed-run recovery, and explicit infrastructure failure for
  non-terminal Attempts found after restart.
- Added server-owned `AdmissionPolicy` checks for executor allowlists and plan
  budget ceilings. Command program and environment allowlists are enforced by
  `CommandTaskExecutor` rather than the generic Workflow layer.
- Added the generic `ITaskExecutor` and `ExecutorRegistry` boundary. The
  compiler delegates executor config validation, while the runtime routes
  start/cancel and enforces declared output ports.
- Added pause/resume, delayed retries with bounded exponential backoff,
  failure classification, per-attempt history, skip reasons, stop intent, and
  explicit `continue_independent` / `fail_fast` policies.
- Added the versioned WorkflowPlan, PlanCompiler, immutable ExecutionPlan, WorkflowRuntime, typed run values, artifacts, evidence, checkpoints, and workflow control-plane routes.
- Added a pinned Google Minijail helper with user/PID/mount/network/IPC/UTS/cgroup namespaces, Landlock, seccomp, private tmpfs, resource limits, integration tests, and release packaging.

### Changed
- Moved all TOML/environment-backed configuration DTOs into
  `dagforge::config`, grouped concrete executors under
  `executors/command` and `executors/http`, and kept existing `[sandbox]` and
  `[http_executor]` TOML sections compatible through the loader.
- Reduced the installed sandbox surface to `CommandSpec` and
  `ICommandRunner`; Minijail policy, launch, and process-management declarations
  are now private implementation headers, while single-use node schemas and
  shard state live directly in their executor `.cpp` files.
- Replaced the ambiguous executor/adapter layering with explicit ownership:
  Workflow defines `ITaskExecutor`, concrete Command and HTTP Task executors
  live under `executors`, and Minijail implements the lower
  `sandbox::ICommandRunner` contract.
- Task executors now own their lower execution resources and are quiesced
  uniformly through `ExecutorRegistry`; Application no longer has
  Command-specific ownership or shutdown branches.
- Reorganized subsystem boundaries: common HTTP client/parser/router/server
  code now lives under `dagforge/http`, while `app/api` contains only control
  plane assembly and routes.
- Split Workflow values, Plan IR, runtime snapshots, Evidence types, Plan JSON
  loading, Artifact storage, Evidence storage, and Checkpoint storage into
  focused components. Compatibility aggregate headers remain available, but
  internal code uses precise includes.
- Moved API route bodies and HTTP metric registry implementation out of
  headers, and removed unused legacy Buffer, URL, MySQL formatter, BatchWriter,
  and utility aggregate headers.
- Updated Agent coding guidance to remove the retired ComputePool and MySQL
  architecture and document current executor, HTTP, storage, and `detail/`
  placement rules.
- Removed the unused vendored Lua source distribution and its verification
  metadata.
- Removed the unused QueryParams helper and WebSocket stack; the optional API
  now contains only the HTTP control plane used by the runtime.
- Enum metadata and JSON/TOML enum serialization now share Glaze `enumerate` definitions.
- HTTP headers use Boost.Beast fields, preserving case-insensitive lookup and duplicate fields.
- HTTP requests are serialized through Boost.Beast messages instead of manual
  wire construction; unsupported inbound methods now return `405`.
- Program, environment, executor, and private-network policy defaults are now
  deny-by-default. Permissive settings are explicit development overrides.
- Renamed the Command sandbox directory model from workspace to execution root
  and per-Attempt workdir. Legacy TOML keys and the legacy environment override
  remain accepted by the loader.
- Shard hashes use `ankerl::unordered_dense` hashing.
- Removed the direct Boost.Filesystem dependency; Boost.Charconv remains linked because Boost.URL uses it in the current system build.
- Runtime benchmarks now target the shipped 0.4 runtime primitives instead of the retired Airflow-style scheduler stack.

### Fixed
- Workflow state notifications no longer save full checkpoints implicitly.
  Checkpoints are persisted after explicitly marked nodes and at Run terminal
  state, restoring the documented contract and removing quadratic state-copy
  behavior from ordinary Task transitions.
- Application shutdown now quiesces Workflow Runtime, drains active HTTP and
  Task coroutines through owner-shard barriers, and only then stops Runtime
  threads. HTTP server socket closure is executor-affine, eliminating the
  shutdown race and in-flight coroutine leaks found by TSAN and LSAN.
- Executor completion callbacks are marshalled back to the awaiting runtime
  executor and accepted exactly once, including synchronous and foreign-thread
  executor completions.
- Restarting an `Application` now rebuilds Workflow components quiesced during
  shutdown, and `init()` reconciles changes to `workflow.enabled`.
- Command environments are passed explicitly to the Minijail process, so
  sanitized variables and `input_env` mappings reach sandboxed commands even
  while the runtime uses Minijail static mode.
- A Run can only succeed when every published Workflow output exists, and
  scalar Workflow values remain JSON scalars in API responses.
- DNS results are filtered before connect, command policy is enforced again at
  the low-level process boundary, and output overflow terminates the sandbox
  instead of continuing with silently truncated data.

## [0.3.0] - 2026-03-30

### Changed
- Rebuilt the core around C++20 modules and moved the local build/release flow to build2.
- Refreshed packaging for the new modules-first release layout.

### Improved
- Expanded observability across runtime, scheduler, API, logging, WebSocket, and benchmark surfaces.
- Refreshed the web UI dashboard with clearer log visibility and a smoother day-to-day workflow.
- Improved hot-path performance and fixed several correctness and edge-case bugs.

### Added
- Broader benchmark coverage and updated benchmark artifacts for the 0.3.0 baseline.

### Deployment & Artifacts
- Refreshed the prebuilt Linux x86_64 tarball with the current binary, config, and web UI bundle.

## [0.2.0] - 2026-03-18

### Changed
- **Configuration System Refactor**
  - Reworked system and DAG configuration loading around the domain model instead of parallel adapter structs.
  - Expanded instance-level configuration coverage for scheduler, API, DAG source, daemon, TLS, executor, and runtime options.
  - Tightened CLI/config validation so invalid combinations fail earlier and with clearer diagnostics.
- **Database & Persistence Refactor**
  - Consolidated MySQL persistence paths and reduced duplicated query/error-handling code.
  - Improved task/run state persistence behavior for retries, invalid commands, timeouts, and executor edge cases.
  - Normalized task instance bookkeeping to avoid inconsistent attempt/state rows during recovery and retries.

### Improved
- **Performance**
  - Reduced scheduler/executor overhead in the hot path through runtime and process-management cleanup.
  - Added and expanded benchmark coverage for DAG engine, scheduler service, and Airflow-style workload comparisons.
  - Current repo benchmark artifact for `scene1_linear_100x10` reports `1237 ms` total task lag and `1.237 ms/task` average lag.

### Fixed
- **Correctness & Edge Cases**
  - Fixed timeout handling so timed-out tasks fail cleanly instead of remaining effectively stuck behind retry flow.
  - Fixed invalid command / non-zero shell exit handling and related task persistence state transitions.
  - Fixed large-output logging so shell stdout/stderr are streamed line-by-line instead of collapsing into a few oversized log records.
  - Fixed working-directory parsing/handling issues and cleaned up process lifecycle management shared by shell/sensor executors.

### Added
- **Testing**
  - Added targeted executor, persistence, validation, HTTP API, WebSocket, and end-to-end integration tests.
  - Added regression coverage for invalid commands, timeout behavior, non-zero exits, log streaming, WebSocket delivery, and sensor execution paths.
  - Expanded benchmark and verification coverage around scheduler throughput and Airflow-style scenarios.

## [0.1.0-beta] - Initial Beta Release

### Added
- **Core Architecture:**
  - High-performance DAG (Directed Acyclic Graph) workflow orchestrator built with C++23.
  - Seastar-inspired sharded async runtime minimizing lock contention.
  - TOML-based DAG definitions with hot-reload support.
- **Executors:**
  - `shell`: Native subprocess execution.
  - `docker`: Containerized task execution.
  - `sensor`: Polling tasks for files, HTTP endpoints, or shell commands.
- **Workflow Features:**
  - XCom cross-task communication via template variables (`{{ds}}`, `{{xcom_pull(...)}}`).
  - Branching DAGs via `is_branch = true` tasks.
  - Complete trigger rules (`all_success`, `all_failed`, `one_success`, etc.).
  - Configurable retries, timeouts, and soft-fails.
- **Storage & State:**
  - Asynchronous MySQL persistence using `Boost.Mysql`.
  - Watermark-based crash recovery for orphaned tasks.
- **CLI & APIs:**
  - Full-featured CLI for service management, DAG triggering, and inspection.
  - HTTP REST API for programmatic control.
  - WebSocket API for real-time logs and task status events.
- **Web UI:**
  - Modern React 19 dashboard.
  - Real-time DAG visualization via React Flow.
  - Live log streaming and run history inspection.

### Deployment & Artifacts
- **Docker:** Official images available at `ghcr.io/<owner>/dagforge:0.1.0-beta`.
- **Prebuilt Linux Tarball:** Self-contained archive with binary, config, and web-ui distribution.

### Documentation
- Complete `README.md` and `README_CN.md` with quickstart guides.
- Detailed `USER_GUIDE.md` covering all features and troubleshooting.
- Dedicated `API.md` for REST/WebSocket integrations.
