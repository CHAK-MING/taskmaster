# Changelog

All notable changes to DAGForge will be documented in this file.

## [Unreleased]

### Breaking Changes
- Replaced the flat run/node lifecycle with explicit Run, Task, and Attempt
  state machines. Cancellation and fail-fast now remain `stopping` until every
  active attempt is reaped.
- Removed Approval nodes and approval control-plane routes. External waits are
  no longer modeled as executors.
- Removed XCom and XCom-based branching from the C++ runtime, including persistence, APIs, CLI, metrics, modules, tests, and examples.
- Removed the retired 0.3 TaskConfig, DAG manager, scheduler, cron, sensor, MySQL persistence, management CLI, DAG REST routes, and Web UI stacks.
- Replaced `[scheduler]`, `[database]`, and `[dag_source]` configuration with the 0.4 `[runtime]`, `[compute]`, `[sandbox]`, `[workflow]`, and `[api]` contract.
- Removed legacy DAG/task IDs and the DAG-specific Lua task context.
- Replaced the Shell, Docker, Lua, Noop, composite, and registry executor stack with one sandboxed Command executor; Noop remains an inline workflow node.
- Unknown TOML fields are rejected instead of being silently ignored.

### Added
- Added pause/resume, delayed retries with bounded exponential backoff,
  failure classification, per-attempt history, skip reasons, stop intent, and
  explicit `continue_independent` / `fail_fast` policies.
- Added a dedicated bounded ComputePool for CPU-intensive work, with priority queues, cooperative cancellation, start deadlines, owner-shard completion routing, configuration, and Prometheus metrics.
- Added the versioned WorkflowPlan, PlanCompiler, immutable ExecutionPlan, WorkflowRuntime, typed run values, artifacts, evidence, checkpoints, and workflow control-plane routes.
- Added a pinned Google Minijail helper with user/PID/mount/network/IPC/UTS/cgroup namespaces, Landlock, seccomp, private tmpfs, resource limits, integration tests, and release packaging.

### Changed
- Removed the unused vendored Lua source distribution and its verification
  metadata.
- Removed the unused QueryParams helper and WebSocket stack; the optional API
  now contains only the HTTP control plane used by the runtime.
- Enum metadata and JSON/TOML enum serialization now share Glaze `enumerate` definitions.
- HTTP headers use Boost.Beast fields, preserving case-insensitive lookup and duplicate fields.
- Shard hashes use `ankerl::unordered_dense` hashing.
- Removed the direct Boost.Filesystem dependency; Boost.Charconv remains linked because Boost.URL uses it in the current system build.
- Runtime benchmarks now target the shipped 0.4 runtime primitives instead of the retired Airflow-style scheduler stack.

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
