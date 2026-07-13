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
- **🧱 Immutable execution plans:** Strict JSON or TOML plans compile into
  immutable Execution Plans.
- **✅ Compile-time admission checks:** Nodes, dependencies, cycles, ports,
  conditional edges, policies, retry settings, and budgets are validated
  before execution.
- **🔗 Explicit typed data flow:** Nodes exchange values through declared
  input bindings and output ports instead of hidden shared state.
- **🛡️ Mandatory command sandboxing:** Command is the only external-process
  executor, and it has no unsandboxed fallback.
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
Upstream application / workflow author
                 |
                 v
          Workflow Plan v1
                 |
                 v
            Plan Compiler
                 |
                 v
       Immutable Execution Plan
                 |
                 v
           Workflow Runtime
        /                     \
 CommandExecutor             ComputePool
        |
        v
   SandboxBackend
        |
        v
  Minijail Sandbox
```

The plan describes workflow intent. The runtime owns deterministic validation,
state transitions, scheduling, output propagation, and execution cleanup.

---

## 🧩 Execution Model

`CommandExecutor` is the only executor that launches an external process.
Every command uses an absolute program path and an explicit argument vector;
the runtime never inserts an implicit shell.

Every Workflow Plan node is a sandboxed command task. Upstream values are only
made visible to a command through explicit input bindings and `input_env`
mappings. HTTP calls, model inference, MCP tools, evaluation, and other domain
logic run as ordinary programs chosen by the upper layer; the C++ runtime does
not encode those protocols as node types.

`ComputePool` remains an internal runtime facility. It is not a Workflow Plan
operator and is selected by runtime implementation code when CPU work needs to
leave an owner shard.

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

Missing sandbox binaries, policies, or required kernel capabilities cause the
task to fail. DAGForge does not fall back to direct host execution.

`CommandExecutor` depends on the `ISandboxBackend` interface. Minijail is the
shipped backend; workflow scheduling and command execution do not depend on
Minijail arguments or process-management details.

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
dagforge validate --file dags/hello_world.toml
```

### 4) Run Locally

```bash
dagforge run \
  --config system_config.toml \
  --file dags/hello_world.toml \
  --wait
```

Trigger data can be supplied as JSON or text:

```bash
dagforge run \
  --config system_config.toml \
  --file dags/hello_world.toml \
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

DAGForge accepts strict JSON or TOML Workflow Plans. Unknown fields are
rejected.

Minimal TOML plan:

```toml
workflow_id = "hello-world"
schema_version = 1

[[nodes]]
id = "start"
outputs = ["stdout", "stderr", "exit_code", "result"]
timeout_sec = 30

[nodes.config]
program = "/bin/echo"
arguments = ["hello from DAGForge"]
```

Sandboxed command example:

```toml
[[nodes]]
id = "render"
outputs = ["stdout", "stderr", "exit_code", "result"]
timeout_sec = 30

[nodes.config]
program = "/usr/bin/python3"
arguments = ["-c", "print('hello from the sandbox')"]
env = [{ key = "MODE", value = "test" }]
```

An upstream output can be injected into the environment explicitly:

```toml
inputs = [{ input = "payload", source_node = "prepare", source_port = "result" }]

[nodes.config]
program = "/usr/bin/python3"
arguments = ["/workspace/consume.py"]
input_env = [{ input = "payload", environment = "DAGFORGE_INPUT" }]
```

See [`dags/hello_world.toml`](dags/hello_world.toml) and
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

The configuration file contains five top-level sections:

| Section | Purpose |
| --- | --- |
| `[runtime]` | Shard count and CPU affinity |
| `[compute]` | Bounded CPU worker pool and affinity |
| `[sandbox]` | Minijail paths, workspace root, and resource limits |
| `[workflow]` | Workflow switch and adapter/provider catalogs |
| `[api]` | Optional HTTP address, port, and TLS settings |

See [`system_config.toml`](system_config.toml) for a complete example.

---

## 📡 HTTP Control Plane

When `[api].enabled = true`, the service provides:

- Plan registration and listing;
- Workflow Run creation and status;
- Task output and Evidence queries;
- pause, resume, and cancellation;
- health, runtime status, and Prometheus metrics.

The HTTP server is not allocated when the API is disabled. The current control
plane has no built-in authentication middleware; bind it to loopback or place
it behind a trusted gateway.

See [`docs/API.md`](docs/API.md) for endpoint details.

---

## 💾 Storage Boundary

Plan, active/completed Run, Checkpoint, Evidence, and Artifact stores currently
use in-memory adapters. Process restart recovery and durable event storage are
not provided by the current build.

---

## 🧪 Tests and Benchmarks

Run all unit and integration tests:

```bash
~/.local/share/build2-configs/dagforge-gcc/dagforge/bin/all-unit-tests
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
