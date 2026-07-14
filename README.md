# DAGForge

<div align="center">

**A high-performance, sharded workflow runtime built with modern C++23**

[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg?style=flat-square&logo=c%2B%2B)](https://en.cppreference.com/w/cpp/23)
[![License](https://img.shields.io/badge/license-Apache--2.0-white?labelColor=black&style=flat-square)](LICENSE)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/CHAK-MING/DAGForge)
[![Release](https://img.shields.io/github/v/release/CHAK-MING/dagforge?include_prereleases&style=flat-square)](https://github.com/CHAK-MING/dagforge/releases)

[English](README.md) | [简体中文](README_CN.md)

</div>

---

## ⚡ What is DAGForge?

**DAGForge** is a general-purpose DAG workflow runtime. An upstream
application submits a Workflow Plan; DAGForge validates, compiles, schedules,
executes, pauses, resumes, cancels, and observes that plan with deterministic
runtime semantics.

The runtime uses owner-shard state, C++23 coroutines, and bounded concurrency
to reduce cross-thread coordination on multi-core systems. Natural-language
understanding, planning, and agent loops stay above the runtime boundary.

---

## ✨ Core Features

- **🚀 Sharded runtime:** Workflow state belongs to one owner shard and is
  mutated through single-writer execution.
- **🧱 Immutable execution plans:** Strict JSON plans compile into immutable
  Execution Plans.
- **✅ Compile-time admission checks:** Nodes, dependencies, cycles, ports,
  conditional edges, policies, retry settings, and budgets are validated
  before execution.
- **🔗 Explicit typed data flow:** Nodes exchange values through declared
  input bindings and output ports instead of hidden shared state.
- **🔌 Executor-neutral scheduling:** The Workflow Runtime routes each Task by
  executor name and never interprets executor-specific configuration.
- **🛡️ Mandatory command sandboxing:** The shipped Command executor has no
  unsandboxed fallback and accepts exact allowlisted known binaries by default.
- **🌐 Governed HTTP execution:** The optional HTTP executor uses asynchronous
  DNS/TCP/TLS/HTTP I/O behind exact Origin and resolved-address policy.
- **🔄 Run / Task / Attempt states:** Pause, resume, delayed retry, timeout,
  fail-fast, cancellation, and process reaping have explicit lifecycle states.
- **📦 Artifacts:** Large values can be externalized as Artifact references.
- **🧾 Evidence and checkpoints:** Runtime decisions and selected task
  boundaries can be recorded for inspection.
- **🔁 Idempotent triggers:** Repeated idempotency keys reuse the existing Run.
- **📡 Optional HTTP control plane:** REST endpoints expose plans, Runs,
  outputs, evidence, lifecycle controls, health, status, and Prometheus
  metrics.

---

## 🏗️ Runtime Architecture

```text
Upstream AI / application
                 |
                 v
       JSON Workflow Plan v1
                 |
                 v
            Plan Compiler
                 |
                 v
       Immutable Execution Plan
                 |
                 v
    Workflow Runtime / Scheduler
                 |
                 v
          Executor Registry
          /               \
         v                 v
 Command Executor      HTTP Executor
         |              Async TCP/TLS
         v
 Minijail Sandbox
```

The JSON plan describes Tasks, dependencies, input/output bindings, execution
policies, an executor name, and opaque executor configuration. The compiler
validates the graph and asks each executor to validate its own configuration.
The Workflow Runtime owns Run/Task/Attempt state, scheduling, retries, output
propagation, pause/cancel, and completion. It does not know what a Task does.

---

## 🧩 Execution Model

Every Task contains an executor name and a JSON `config` object. The generic
compiler and runtime treat that object as opaque. `ExecutorRegistry` resolves
the name, delegates configuration compilation, starts the Task, and routes
cancellation.

The shipped `command` executor launches an external sandboxed process and owns
the `program`, `arguments`, `env`, and `input_env` contract. The optional
`http` executor owns methods, URLs, headers, request-body bindings, accepted
statuses, cancellation, and response outputs. Adding the second asynchronous
adapter requires no new Task type and no Workflow Runtime state-machine branch.

Owner-shard coroutines coordinate scheduling, timers, executor callbacks,
cancellation, and state changes. Executor implementations decide how their
work is performed.

---

## 🛡️ Command Sandbox

Commands are started through a pinned Google Minijail helper. Each command
receives:

- private user, PID, mount, network, IPC, UTS, and cgroup namespaces;
- Landlock filesystem restrictions;
- a seccomp denylist and `no_new_privs`;
- a private size-limited `/tmp`;
- memory, file, process, descriptor, CPU, and wall-time limits;
- an isolated writable workspace.

Minijail is used as a known-binary containment mechanism. Production
configuration requires administrator-installed absolute program paths and exact
environment allowlists. It is not intended to safely execute malicious native
binaries or attacker-controlled shared libraries. Missing or writable sandbox
helpers/policies, unsafe workspaces, unavailable Landlock, and invalid limits
fail during application initialization. DAGForge never falls back to direct
host execution.

Stdout, stderr, and unterminated streamed lines have independent hard limits.
Overflow kills the process group instead of silently truncating. Application
shutdown prevents new starts, kills active process groups, and waits for them
to be reaped before stopping Runtime threads.

`MinijailCommandExecutor` implements the Command execution interface directly.
Workflow scheduling depends only on `ICommandExecutor`; Minijail arguments,
process supervision, and sandbox state remain private to the low-level
executor implementation.

---

## 🌐 HTTP Executor

The optional `http` executor is disabled by default. Its outbound network
policy is owned by system configuration, not by Workflow JSON:

```toml
[http_executor]
enabled = true
allow_plaintext = false
deny_private_networks = true
allowed_origins = ["https://api.example.com"]
allowed_ip_cidrs = []
max_request_headers = 64
max_request_header_bytes = 65536
max_request_body_bytes = 1048576
max_response_headers = 128
max_response_header_bytes = 65536
max_response_body_bytes = 10485760
max_concurrent_requests_per_shard = 32
max_concurrent_requests = 256
tls_min_version = "1.2"
tls_ca_file = ""
tls_client_cert_file = ""
tls_client_key_file = ""
```

Origins match exact scheme, host, and effective port. Every DNS result is
checked before connect; loopback, link-local, private, multicast, documentation,
and other special-use ranges are denied by default unless covered by an
explicit CIDR. HTTPS uses SNI and hostname verification, supports a private CA
and optional mTLS identity, and enforces TLS 1.2 or 1.3 as configured. Redirects,
proxies, cookies, and dynamic URLs are intentionally unsupported in v1.
Per-shard and process-wide request ceilings apply before a socket opens.
Cancellation interrupts DNS, TCP connect, TLS handshake, write, and read.

When the control-plane listener has TLS credentials it is TLS-only; plaintext
is never auto-detected or routed on that port. The server owns active-connection,
idle-time, parser, requests-per-connection, and route concurrency limits and
closes active connections during shutdown. See
[`dags/http_pipeline.json`](dags/http_pipeline.json).

---

## 🚀 Quick Start

### 1) Requirements

- Linux x86-64 or ARM64 with user namespace, seccomp, and Landlock support
- GCC 15+
- build2 0.17+
- Boost 1.88+
- OpenSSL and libcap development packages
- Git, Make, and Python 3 for the pinned Minijail build

### 2) Build

```bash
./scripts/setup-build2.sh
./scripts/install-minijail.sh
./scripts/build.sh
```

The build scripts print the selected build2 configuration and executable path.

### 3) Validate a Workflow Plan

```bash
dagforge validate --file dags/hello_world.json
```

Executor policy is server-owned. Validate plans that use the optional HTTP
executor with the same system configuration used to run them:

```bash
dagforge validate \
  --config system_config.toml \
  --file dags/http_pipeline.json
```

### 4) Run Locally

```bash
dagforge run \
  --config system_config.toml \
  --file dags/hello_world.json \
  --wait
```

Trigger data can be supplied as JSON or text:

```bash
dagforge run \
  --config system_config.toml \
  --file dags/hello_world.json \
  --payload '{"request":"hello"}' \
  --wait
```

### 5) Start the HTTP Control Plane

```bash
dagforge serve --config system_config.toml
```

### 6) Docker Compose

```bash
docker compose up --build
```

---

## 📝 Workflow Plan

DAGForge accepts strict JSON Workflow Plans. Unknown fields are rejected.

```json
{
  "workflow_id": "hello-world",
  "schema_version": 1,
  "nodes": [
    {
      "id": "start",
      "executor": "command",
      "outputs": ["stdout", "stderr", "exit_code", "result"],
      "timeout_sec": 30,
      "config": {
        "program": "/bin/echo",
        "arguments": ["hello from DAGForge"],
        "env": [],
        "input_env": []
      }
    }
  ]
}
```

Generic input bindings remain outside executor configuration. An executor may
then map a named input according to its own contract. The Command executor, for
example, can map it to an environment variable through `config.input_env`.

See [`dags/hello_world.json`](dags/hello_world.json),
[`dags/http_pipeline.json`](dags/http_pipeline.json), and
[`docs/USER_GUIDE.md`](docs/USER_GUIDE.md) for the complete contract.

---

## 🔄 State Model

Plan nodes are represented as Tasks at runtime. Each real execution creates a
separate Attempt record.

| Layer | States |
| --- | --- |
| Run | `running`, `pausing`, `paused`, `stopping`, `succeeded`, `failed`, `cancelled` |
| Task | `pending`, `ready`, `running`, `retry_waiting`, `succeeded`, `failed`, `skipped`, `cancelled` |
| Attempt | `starting`, `running`, `terminating`, `succeeded`, `failed`, `timed_out`, `cancelled` |

Pause stops new dispatches while active attempts finish. Cancellation and
fail-fast remain in `stopping` until active attempts have terminated and their
processes have been reaped.

---

## ⚙️ System Configuration

The configuration file contains six top-level sections:

| Section | Purpose |
| --- | --- |
| `[runtime]` | Shard count and CPU affinity |
| `[sandbox]` | Command sandbox paths, limits, and command allowlists |
| `[workflow]` | Workflow runtime switch |
| `[admission]` | Server-owned executor allowlist and plan budget limits |
| `[storage]` | Optional durable Run, Evidence, and Artifact directory |
| `[api]` | Optional HTTP address, port, and TLS settings |

Workflow Plans cannot authorize themselves. `AdmissionPolicy` evaluates every
plan against the server configuration before it is compiled and registered.

The HTTP control plane supports a Bearer Token loaded from the environment,
plus configurable request-body and concurrent-request limits. Authentication
is disabled only when `api.bearer_token_env` is empty.

See [`system_config.toml`](system_config.toml) for a complete example.

---

## 📡 HTTP Control Plane

When `[api].enabled = true`, the service provides:

- Plan registration and listing;
- Workflow Run creation and status;
- Task output and Evidence queries;
- pause, resume, and cancellation;
- health, runtime status, and Prometheus metrics.

The HTTP server is not allocated when the API is disabled. When
`api.bearer_token_env` is configured, all routes require the corresponding
Bearer Token.

See [`docs/API.md`](docs/API.md) for endpoint details.

---

## 💾 Storage Boundary

Storage is in-memory by default. Setting `[storage].enabled = true` enables
atomic file checkpoints, append-only Evidence records, and file-backed
Artifacts below `[storage].directory`.

Completed Runs and outputs are restored after restart. A process cannot safely
reattach to a sandbox process created by the previous runtime instance, so any
non-terminal Run found during recovery is finalized as `failed`; its active
Attempt is recorded as an infrastructure failure instead of being presented as
still running.

`storage.max_completed_runs` and `storage.max_evidence_records` bound retained
history. API collection responses use `offset` and `limit` pagination.

---

## 🧪 Tests and Benchmarks

Run all unit and integration tests:

```bash
~/.local/share/build2-configs/dagforge-gcc/dagforge/bin/all-unit-tests
```

Run the real Workflow JSON suite against the HTTP service, Command executor,
and Minijail sandbox:

```bash
python3 scripts/test-real-workflows.py \
  --binary "$HOME/.local/share/build2-configs/dagforge-gcc/dagforge/bin/dagforge"
```

Run the runtime and memory benchmarks:

```bash
~/.local/share/build2-configs/dagforge-gcc/dagforge/bin/bench-core
```

---

## 📚 Documentation

- **[User Guide](docs/USER_GUIDE.md)** — Workflow plans, configuration, and runtime behavior.
- **[API Reference](docs/API.md)** — HTTP control-plane endpoints.
- **[State Machine ADR](docs/adr/0001-run-task-attempt-state-machine.md)** — Run, Task, and Attempt decisions.
- **[Clangd Setup](docs/CLANGD_SETUP.md)** — Modules and editor indexing.
- **[Benchmark Scope](docs/BENCH_REPORT.md)** — Current benchmark targets and reporting rules.

---

## 🤝 Contributing

1. Fork the repository.
2. Create a feature branch.
3. Add code and tests.
4. Push the branch and open a Pull Request.

---

## 📄 License

Released under the **Apache License 2.0**. See [`LICENSE`](LICENSE).
