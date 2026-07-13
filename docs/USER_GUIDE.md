# DAGForge 0.4 User Guide

DAGForge 0.4 is a general DAG execution runtime. It accepts a strict,
versioned workflow plan, compiles that plan into an immutable execution plan,
and executes it on the sharded C++ runtime.

Natural-language interpretation and dynamic AI planning are intentionally
outside the runtime. An upstream application can generate plans and control
runs through the CLI or REST interface.

## 1. Build

Requirements:

- Linux
- GCC 15+
- build2 0.17+
- Boost 1.88+
- OpenSSL development libraries
- libcap development headers
- Git, Make, and Python 3 for building the pinned Minijail helper

Build the project:

```bash
./scripts/setup-build2.sh
./scripts/install-minijail.sh
./scripts/build.sh
```

The build script prints the selected build2 configuration and binary path.

## 2. System configuration

DAGForge uses strict TOML. Unknown fields are rejected.

```toml
[runtime]
shards = 0
pin_shards_to_cores = false
cpu_affinity_offset = 0

[compute]
threads = 0
queue_capacity = 1024
pin_threads_to_cores = false
cpu_affinity_offset = 0

[sandbox]
minijail_path = "~/.local/libexec/dagforge/minijail/minijail0"
seccomp_bpf_path = "~/.local/libexec/dagforge/minijail/dagforge_command.bpf"
workspace_root = "./workspaces"
max_memory_bytes = 1073741824
max_file_bytes = 67108864
tmp_bytes = 67108864
max_processes = 128
max_open_files = 256

[workflow]
enabled = true

[api]
enabled = false
host = "127.0.0.1"
port = 8888
reuse_port = false
tls_enabled = false
tls_cert_file = ""
tls_key_file = ""
```

### 2.1 Runtime

`runtime.shards` selects the number of owner shards. `0` selects the hardware
concurrency reported by the operating system.

CPU affinity is optional. When enabled, shard `0` begins at
`cpu_affinity_offset`.

Environment overrides:

- `DAGFORGE_RUNTIME_SHARDS`
- `DAGFORGE_RUNTIME_PIN_SHARDS`
- `DAGFORGE_RUNTIME_CPU_AFFINITY_OFFSET`

### 2.2 Compute pool

The compute pool is separate from I/O shards. It is an internal runtime
facility and is not exposed as a Workflow Plan node type.

- `threads = 0` selects an automatic thread count.
- `queue_capacity` is a hard bound on pending compute work.
- optional affinity settings control compute worker placement.

Environment overrides:

- `DAGFORGE_COMPUTE_THREADS`
- `DAGFORGE_COMPUTE_QUEUE_CAPACITY`
- `DAGFORGE_COMPUTE_PIN_THREADS`
- `DAGFORGE_COMPUTE_CPU_AFFINITY_OFFSET`

### 2.3 Command sandbox

All Command nodes run through the pinned Google Minijail helper. DAGForge does
not contain a direct subprocess fallback. Missing Minijail, missing seccomp
bytecode, unavailable Landlock, or unsupported namespace setup causes the node
to fail closed.

Each instance receives:

- user, PID, mount, network, IPC, UTS, and cgroup namespaces;
- `no_new_privs` and the DAGForge seccomp denylist;
- Landlock read/execute access to system runtimes and read/write access only to
  its instance workspace and private `/tmp`;
- CPU, address-space, file-size, process-count, and open-file limits.

The workspace root must not be inside the host temporary directory because the
sandbox mounts a private tmpfs over `/tmp`. Environment overrides are:

- `DAGFORGE_SANDBOX_MINIJAIL`
- `DAGFORGE_SANDBOX_SECCOMP_BPF`
- `DAGFORGE_SANDBOX_WORKSPACE_ROOT`
- `DAGFORGE_SANDBOX_MAX_MEMORY_BYTES`
- `DAGFORGE_SANDBOX_MAX_FILE_BYTES`
- `DAGFORGE_SANDBOX_TMP_BYTES`
- `DAGFORGE_SANDBOX_MAX_PROCESSES`
- `DAGFORGE_SANDBOX_MAX_OPEN_FILES`

### 2.4 Workflow runtime

`workflow.enabled` creates the workflow control plane and runtime.

### 2.5 Admission policy

Admission is owned by the server, not by the Workflow Plan:

```toml
[admission]
allow_unlisted_programs = false
allow_unlisted_environment = false
allowed_programs = ["/bin/echo", "/usr/bin/python3"]
allowed_environment = ["DAGFORGE_INPUT", "MODE"]
max_nodes = 256
max_parallel_nodes = 32
max_total_output_bytes = 67108864
max_run_duration_sec = 3600
```

When an `allow_unlisted_*` field is false, the matching allowlist is exact.
Admission also caps every plan budget. A syntactically valid plan can therefore
be rejected with `unauthorized` or `resource exhausted` before registration.

### 2.6 Storage

The default stores are in-memory. Enable file persistence explicitly:

```toml
[storage]
enabled = true
directory = "./state"
```

The directory contains atomic Run checkpoint files, an append-only Evidence
log, and Artifact data plus metadata. Completed Runs and outputs are restored
on startup. Non-terminal Runs cannot be reattached to an old sandbox process;
they recover as failed with an infrastructure failure recorded on the active
Attempt.

### 2.7 API

Set `api.enabled = true` to start the HTTP control plane. TLS requires both a
certificate chain and private key path.

Environment overrides:

- `DAGFORGE_API_ENABLED`
- `DAGFORGE_API_HOST`
- `DAGFORGE_API_PORT`
- `DAGFORGE_API_REUSE_PORT`
- `DAGFORGE_API_TLS_ENABLED`
- `DAGFORGE_API_TLS_CERT_FILE`
- `DAGFORGE_API_TLS_KEY_FILE`

## 3. Workflow Plan v1

Plans are accepted as JSON or TOML. `schema_version` must be `1`.

```toml
workflow_id = "hello-world"
schema_version = 1

[[nodes]]
id = "start"
name = "Start"
outputs = ["stdout", "stderr", "exit_code", "result"]
max_retries = 0
retry_initial_delay_ms = 1000
retry_max_delay_ms = 30000
timeout_sec = 30
checkpoint = false

[nodes.config]
program = "/bin/echo"
arguments = ["hello from DAGForge"]
```

### 3.1 Node fields

- `id`: unique within the workflow.
- `name`: optional display name.
- `config`: strict sandboxed command configuration.
- `inputs`: named bindings to an upstream node output.
- `outputs`: output port names. The default is `result`.
- `max_retries`: retries after the first attempt.
- `retry_initial_delay_ms`: delay before the first retry.
- `retry_max_delay_ms`: maximum exponential retry delay.
- `timeout_sec`: node deadline.
- `checkpoint`: save a runtime checkpoint after successful completion.

An input binding is:

```toml
[[nodes.inputs]]
input = "source"
node = "upstream"
port = "result"
```

### 3.2 Command configuration

```toml
[nodes.config]
program = "/usr/bin/python3"
arguments = ["-c", "print('hello')"]
env = [{ key = "MODE", value = "test" }]
```

`program` must be an absolute executable path. Arguments are passed directly;
there is no implicit shell. Use `/bin/sh` explicitly when shell syntax is
required. `PATH`, `HOME`, and `TMPDIR` are runtime-owned and cannot be
overridden by the node.

Inputs are not injected automatically. Map selected inputs to environment
variables explicitly:

```toml
[[nodes.inputs]]
input = "payload"
node = "prepare"
port = "result"

[nodes.config]
program = "/usr/bin/python3"
arguments = ["/workspace/consume.py"]
input_env = [{ input = "payload", environment = "DAGFORGE_INPUT" }]
```

HTTP calls, model inference, MCP tools, evaluation, and data transformation
are implemented by ordinary programs selected by the upper layer. They are
not C++ runtime node types.

### 3.3 Conditional edges

Conditions are explicit edges rather than executable branch tasks.

```toml
[[edges]]
source_node = "evaluate"
source_port = "passed"
target = "publish"

[edges.condition]
kind = "bool_equals"
expected_bool = true
```

Supported condition kinds:

- `always`
- `bool_equals`
- `string_equals`

### 3.4 Policy and budgets

```toml
[policy]
failure_policy = "continue_independent"

[policy.budget]
max_nodes = 256
max_parallel_nodes = 32
max_total_output_bytes = 67108864
max_run_duration_sec = 3600
```

`failure_policy` accepts:

- `continue_independent`: independent branches continue after a task fails;
- `fail_fast`: the run enters `stopping` and terminates other active attempts.

These fields describe execution behavior and requested resource bounds. They
do not grant permission to execute programs or expose environment variables;
that decision belongs to the server admission policy.

## 4. CLI

### Validate

```bash
dagforge validate --file workflow.toml
```

Validation loads the plan, rejects unknown fields, compiles the graph, and
prints the workflow ID, generated plan ID, digest, and node count.

### Local run

```bash
dagforge run \
  --config system_config.toml \
  --file workflow.toml \
  --payload '{"request":"hello"}' \
  --wait
```

Without `--wait`, the local process exits immediately after accepting the run.
Because current stores are in-memory, use `--wait` when the result matters.

### Service

```bash
dagforge serve --config system_config.toml
```

Use `SIGINT` or `SIGTERM` for graceful shutdown.

## 5. Runtime semantics

- A compiled plan is immutable for the life of a run.
- Each run is owned by one shard.
- Run and task state mutations occur on the owner shard.
- Input values reference explicit upstream output ports.
- A run uses `running`, `pausing`, `paused`, `stopping`, `succeeded`, `failed`,
  and `cancelled` states.
- A task uses `pending`, `ready`, `running`, `retry_waiting`, `succeeded`,
  `failed`, `skipped`, and `cancelled` states.
- Every execution creates a distinct Attempt record. A task has at most one
  active Attempt.
- Pause stops dispatching new tasks but lets active attempts finish; it never
  freezes sandbox processes.
- Retryable failures enter `retry_waiting` and use bounded exponential
  backoff. Permanent configuration and authorization failures do not retry.
- A failed dependency causes downstream tasks to be skipped with a recorded
  reason.
- Cancellation enters `stopping`, terminates active work, and becomes terminal
  only after every attempt is reaped. Command cancellation kills the complete
  Minijail process namespace.
- Duplicate non-empty idempotency keys return the original run ID while the
  process remains alive.
- Large string and JSON outputs are replaced with Artifact references.

## 6. Current durability boundary

The following adapters are currently in-memory:

- workflow plan catalog;
- artifact store;
- evidence ledger;
- checkpoint store;
- idempotency registry;
- completed-run cache.

Restarting the process loses these values. Do not treat the current checkpoint
interface as crash recovery.

## 7. Observability

The service exposes:

- `/api/health`
- `/api/status`
- `/metrics`

Runtime, compute-pool, HTTP, and active-workflow metrics are rendered in
Prometheus text format.

## 8. Docker

```bash
docker compose up --build
```

The Compose stack runs only DAGForge. It does not start MySQL. The Docker
socket is not mounted. The Compose profile disables the outer container's
default seccomp profile so Minijail can create nested namespaces; command nodes
still receive the inner DAGForge seccomp and Landlock policy.

## 9. Verification

```bash
bash scripts/check-module-graph.sh
bash scripts/check-agent-conventions.sh
bash scripts/install-minijail.sh
BUILD2_CONFIG_NAME=gcc ./scripts/build.sh
~/.local/share/build2-configs/dagforge-gcc/dagforge/bin/all-unit-tests
```

Run `bench-core` for Runtime and memory-arena microbenchmarks.
