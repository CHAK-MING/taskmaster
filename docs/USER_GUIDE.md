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

[sandbox]
minijail_path = "~/.local/libexec/dagforge/minijail/minijail0"
seccomp_bpf_path = "~/.local/libexec/dagforge/minijail/dagforge_command.bpf"
workspace_root = "./workspaces"
max_memory_bytes = 1073741824
max_file_bytes = 67108864
tmp_bytes = 67108864
max_processes = 128
max_open_files = 256
allow_unlisted_programs = true
allow_unlisted_environment = true
allowed_programs = []
allowed_environment = []

[workflow]
enabled = true

[http_executor]
enabled = false
allow_plaintext = false
allowed_origins = []
max_request_headers = 64
max_request_header_bytes = 65536
max_request_body_bytes = 1048576
max_response_header_bytes = 65536
max_response_body_bytes = 10485760
max_concurrent_requests_per_shard = 32

[admission]
allow_unlisted_executors = true
allowed_executors = []
max_nodes = 256
max_parallel_nodes = 32
max_total_output_bytes = 67108864
max_run_duration_sec = 3600

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

Owner shards run coroutine I/O, timers, scheduling, executor callbacks, and
workflow state transitions. The Workflow Runtime does not execute or interpret
executor-specific work.

Environment overrides:

- `DAGFORGE_RUNTIME_SHARDS`
- `DAGFORGE_RUNTIME_PIN_SHARDS`
- `DAGFORGE_RUNTIME_CPU_AFFINITY_OFFSET`

### 2.2 Command sandbox

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

Command-specific program and environment allowlists also live in `[sandbox]`.
They are enforced by the Command executor while compiling its JSON config.

### 2.3 HTTP executor

The HTTP executor is disabled by default. Enable it only with an exact list of
trusted origins:

```toml
[http_executor]
enabled = true
allow_plaintext = false
allowed_origins = ["https://api.example.com"]
max_request_headers = 64
max_request_header_bytes = 65536
max_request_body_bytes = 1048576
max_response_header_bytes = 65536
max_response_body_bytes = 10485760
max_concurrent_requests_per_shard = 32
```

An origin consists of scheme, host, and effective port. Matching is exact and
case-normalized; paths do not belong in `allowed_origins`. HTTPS is verified
with the OpenSSL trust store, including SNI and hostname verification. Plain
HTTP requires both `allow_plaintext = true` and a matching `http://` origin.
Redirects are not followed, so an allowed server cannot move a request to an
unauthorized origin.

The request, response, and header limits are hard parser/executor bounds. The
concurrency limit applies independently to each Runtime shard. Saturation fails
the Attempt with a retryable queue-full error, allowing the node retry policy
to decide whether to try again.

Environment overrides:

- `DAGFORGE_HTTP_EXECUTOR_ENABLED`
- `DAGFORGE_HTTP_EXECUTOR_ALLOW_PLAINTEXT`
- `DAGFORGE_HTTP_EXECUTOR_MAX_REQUEST_HEADERS`
- `DAGFORGE_HTTP_EXECUTOR_MAX_REQUEST_HEADER_BYTES`
- `DAGFORGE_HTTP_EXECUTOR_MAX_REQUEST_BODY_BYTES`
- `DAGFORGE_HTTP_EXECUTOR_MAX_RESPONSE_HEADER_BYTES`
- `DAGFORGE_HTTP_EXECUTOR_MAX_RESPONSE_BODY_BYTES`
- `DAGFORGE_HTTP_EXECUTOR_MAX_CONCURRENT_REQUESTS_PER_SHARD`

Origins intentionally have no environment-list override; keep them in the
auditable system configuration.

### 2.4 Workflow runtime

`workflow.enabled` creates the workflow control plane and runtime.

### 2.5 Admission policy

Admission is owned by the server, not by the Workflow Plan:

```toml
[admission]
allow_unlisted_executors = false
allowed_executors = ["command", "http"]
max_nodes = 256
max_parallel_nodes = 32
max_total_output_bytes = 67108864
max_run_duration_sec = 3600
```

When `allow_unlisted_executors` is false, the executor allowlist is exact.
Admission also caps every plan budget. Command program and environment policy
is deliberately separate because it belongs to that executor, not to the
generic Workflow Runtime.

### 2.6 Storage

The default stores are in-memory. Enable file persistence explicitly:

```toml
[storage]
enabled = true
directory = "./state"
max_completed_runs = 10000
max_evidence_records = 100000
```

The directory contains atomic Run checkpoint files, an append-only Evidence
log, and Artifact data plus metadata. Completed Runs and outputs are restored
on startup. Non-terminal Runs cannot be reattached to an old sandbox process;
they recover as failed with an infrastructure failure recorded on the active
Attempt.

When a retention limit is exceeded, the oldest completed Run or Evidence
record is removed. Evicted durable Run checkpoints are deleted as well.

### 2.7 API

Set `api.enabled = true` to start the HTTP control plane. TLS requires both a
certificate chain and private key path.

```toml
[api]
enabled = true
host = "127.0.0.1"
port = 8888
bearer_token_env = "DAGFORGE_API_TOKEN"
max_request_body_bytes = 1048576
max_concurrent_requests = 128
```

When `bearer_token_env` is set, the named environment variable must contain a
non-empty token before the server starts. Clients send it as
`Authorization: Bearer <token>`. Oversized requests are rejected at both the
HTTP parser and route boundary; saturated routes return `429`.

Environment overrides:

- `DAGFORGE_API_ENABLED`
- `DAGFORGE_API_HOST`
- `DAGFORGE_API_PORT`
- `DAGFORGE_API_REUSE_PORT`
- `DAGFORGE_API_TLS_ENABLED`
- `DAGFORGE_API_TLS_CERT_FILE`
- `DAGFORGE_API_TLS_KEY_FILE`
- `DAGFORGE_API_BEARER_TOKEN_ENV`
- `DAGFORGE_API_MAX_REQUEST_BODY_BYTES`
- `DAGFORGE_API_MAX_CONCURRENT_REQUESTS`

## 3. Workflow Plan v1

Plans are accepted as strict JSON. `schema_version` must be `1`.

```json
{
  "workflow_id": "hello-world",
  "schema_version": 1,
  "nodes": [
    {
      "id": "start",
      "name": "Start",
      "executor": "command",
      "outputs": ["stdout", "stderr", "exit_code", "result"],
      "max_retries": 0,
      "retry_initial_delay_ms": 1000,
      "retry_max_delay_ms": 30000,
      "timeout_sec": 30,
      "checkpoint": false,
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

### 3.1 Node fields

- `id`: unique within the workflow.
- `name`: optional display name.
- `executor`: registered executor name.
- `config`: executor-owned JSON object; opaque to the generic compiler and
  runtime.
- `inputs`: named bindings to an upstream node output.
- `outputs`: output port names. The default is `result`.
- `max_retries`: retries after the first attempt.
- `retry_initial_delay_ms`: delay before the first retry.
- `retry_max_delay_ms`: maximum exponential retry delay.
- `timeout_sec`: node deadline.
- `checkpoint`: save a runtime checkpoint after successful completion.

An input binding is:

```json
{"input":"source","node":"upstream","port":"result"}
```

### 3.2 Command configuration

```json
{
  "program": "/usr/bin/python3",
  "arguments": ["-c", "print('hello')"],
  "env": [{"key":"MODE","value":"test"}],
  "input_env": []
}
```

`program` must be an absolute executable path. Arguments are passed directly;
there is no implicit shell. Use `/bin/sh` explicitly when shell syntax is
required. `PATH`, `HOME`, and `TMPDIR` are runtime-owned and cannot be
overridden by the node.

Inputs are not injected automatically. Map selected inputs to environment
variables explicitly:

```json
{
  "inputs": [
    {"input":"payload","node":"prepare","port":"result"}
  ],
  "config": {
    "program": "/usr/bin/python3",
    "arguments": ["/workspace/consume.py"],
    "env": [],
    "input_env": [
      {"input":"payload","environment":"DAGFORGE_INPUT"}
    ]
  }
}
```

The Command executor owns this contract. Other executors define independent
configuration without changing Workflow graph semantics or Runtime state
transitions.

### 3.3 HTTP configuration

```json
{
  "method": "POST",
  "url": "https://api.example.com/v1/transform",
  "headers": [
    {"name":"Content-Type","value":"text/plain; charset=utf-8"}
  ],
  "input_headers": [
    {"input":"token","header":"X-Request-Token"}
  ],
  "body_input": "payload",
  "accepted_statuses": [200, 201]
}
```

The URL is static and absolute so the complete origin can be authorized while
the Plan is compiled. User information and fragments are rejected. Supported
methods are `GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `OPTIONS`, and `HEAD`.
`GET` and `HEAD` cannot carry a body.

`headers` defines static request headers. `input_headers` maps declared node
inputs to header values. Framing and hop-by-hop headers such as `Host`,
`Content-Length`, `Transfer-Encoding`, and `Connection` are executor-owned and
cannot be supplied by a Plan. Header names are unique case-insensitively, and
CR/LF values are rejected.

Use exactly one of `body` or `body_input`. A string, scalar, or JSON input is
serialized to text; Artifact request bodies are not supported in v1. When a
JSON input supplies the body and no content type was declared, the executor
sets `Content-Type: application/json`.

`accepted_statuses` is optional. When omitted or empty, all 2xx statuses are
successful. An explicitly accepted non-2xx status also publishes outputs.
Unaccepted 401/403/404 and other client errors are permanent; 408, 429, 5xx,
transport failures, and timeouts can participate in the node retry policy.

Supported outputs are:

- `status`: integer HTTP status code;
- `body`: UTF-8 response body;
- `headers`: ordered JSON array of `{name,value}` entries, preserving duplicate
  fields;
- `result`: alias of `body` for ordinary data-flow use.

Invalid UTF-8 fails with a protocol error. Binary request/response Artifacts,
redirects, cookies, proxies, HTTP/2, and connection pooling are outside the v1
HTTP executor contract.

See `dags/http_pipeline.json` for a real Command → HTTP → Command Workflow.

### 3.4 Conditional edges

Conditions are explicit edges rather than executable branch tasks.

```json
{
  "source_node":"evaluate",
  "source_port":"passed",
  "target":"publish",
  "condition":{"kind":"bool_equals","expected_bool":true}
}
```

Supported condition kinds:

- `always`
- `bool_equals`
- `string_equals`

### 3.5 Policy and budgets

```json
{
  "failure_policy":"continue_independent",
  "budget":{
    "max_nodes":256,
    "max_parallel_nodes":32,
    "max_total_output_bytes":67108864,
    "max_run_duration_sec":3600
  }
}
```

`failure_policy` accepts:

- `continue_independent`: independent branches continue after a task fails;
- `fail_fast`: the run enters `stopping` and terminates other active attempts.

These fields describe execution behavior and requested resource bounds. They
do not authorize an executor. Executor selection is checked by AdmissionPolicy;
executor-specific permissions are checked by the selected executor.

## 4. CLI

### Validate

```bash
dagforge validate --file workflow.json
```

Validation loads the plan, rejects unknown fields, compiles the graph, and
prints the workflow ID, generated plan ID, digest, and node count.

HTTP Plan validation must load the server policy:

```bash
dagforge validate --config system_config.toml --file http-workflow.json
```

### Local run

```bash
dagforge run \
  --config system_config.toml \
  --file workflow.json \
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
- Cancellation enters `stopping`, routes cancellation to each active executor,
  and becomes terminal only after every attempt completes. Command cancellation
  kills the complete Minijail process namespace; HTTP cancellation interrupts
  DNS, connect, TLS, write, and read operations and closes an established
  socket.
- Duplicate non-empty idempotency keys return the original run ID while the
  process remains alive.
- Large string and JSON outputs are replaced with Artifact references.

## 6. Current durability boundary

The plan catalog and idempotency registry remain process-local. Run
checkpoints, Evidence, Artifacts, and completed Run data can use the optional
file-backed storage configuration. Completed Runs are restored on startup;
non-terminal Attempts are finalized as infrastructure failures because an old
executor instance cannot be reattached safely.

## 7. Observability

The service exposes:

- `/api/health`
- `/api/status`
- `/metrics`

Runtime, HTTP, and active-workflow metrics are rendered in
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
python3 scripts/test-real-workflows.py \
  --binary ~/.local/share/build2-configs/dagforge-gcc/dagforge/bin/dagforge
```

Run `bench-core` for Runtime and memory-arena microbenchmarks.
