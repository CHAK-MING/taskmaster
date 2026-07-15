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
execution_root = "./executions"
max_memory_bytes = 1073741824
max_file_bytes = 67108864
tmp_bytes = 67108864
max_stdout_bytes = 10485760
max_stderr_bytes = 10485760
max_stream_line_bytes = 65536
max_processes = 128
max_open_files = 256
allow_unlisted_programs = false
allow_unlisted_environment = false
require_trusted_files = true
retain_workdirs = false
programs = [
  { name = "echo", path = "/bin/echo" },
  { name = "sh", path = "/bin/sh" },
  { name = "python3", path = "/usr/bin/python3" },
]
allowed_programs = []
allowed_environment = ["DAGFORGE_INPUT"]
inherited_environment = ["LANG", "LC_ALL", "LC_CTYPE", "TERM"]

[workflow]
enabled = true

[http_executor]
enabled = false
allow_plaintext = false
deny_private_networks = true
allowed_origins = []
allowed_ip_cidrs = []
max_request_headers = 64
max_request_header_bytes = 65536
max_request_body_bytes = 1048576
max_response_headers = 128
max_response_header_bytes = 65536
max_response_body_bytes = 10485760
max_concurrent_requests_per_shard = 32
max_concurrent_requests = 256
dns_timeout_ms = 5000
connect_timeout_ms = 10000
tls_handshake_timeout_ms = 10000
write_timeout_ms = 30000
first_byte_timeout_ms = 30000
read_timeout_ms = 30000
idle_connection_timeout_ms = 30000
max_idle_connections_per_origin = 4
max_idle_connections_per_shard = 32
tls_min_version = "1.2"
tls_ca_file = ""
tls_client_cert_file = ""
tls_client_key_file = ""

[admission]
allow_unlisted_executors = false
allowed_executors = ["command"]
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
tls_min_version = "1.2"
max_request_header_bytes = 65536
max_request_body_bytes = 1048576
connection_idle_timeout_ms = 30000
max_connections = 1024
max_requests_per_connection = 100
max_concurrent_requests = 128
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
not contain a direct subprocess fallback. The boundary is for registered,
administrator-installed **known binaries** processing untrusted inputs. It is
not a safe execution environment for malicious native binaries, Workflow-
uploaded executables, or attacker-writable shared libraries.

Missing or untrusted Minijail/BPF files, unavailable Landlock, unsafe execution
roots, invalid registries or allowlists, and invalid limits fail during application
initialization before the API accepts work.

Each instance receives:

- user, PID, mount, network, IPC, UTS, and cgroup namespaces;
- `no_new_privs` and the DAGForge seccomp denylist;
- Landlock read/execute access to system runtimes and read/write access only to
  its per-Attempt workdir and private `/tmp`;
- CPU, address-space, file-size, process-count, and open-file limits.
- independent stdout, stderr, and unterminated streamed-line limits.

The execution root must not be inside the host temporary directory because the
sandbox mounts a private tmpfs over `/tmp`. Environment overrides are:

- `DAGFORGE_SANDBOX_MINIJAIL`
- `DAGFORGE_SANDBOX_SECCOMP_BPF`
- `DAGFORGE_SANDBOX_EXECUTION_ROOT`
- `DAGFORGE_SANDBOX_MAX_MEMORY_BYTES`
- `DAGFORGE_SANDBOX_MAX_FILE_BYTES`
- `DAGFORGE_SANDBOX_TMP_BYTES`
- `DAGFORGE_SANDBOX_MAX_STDOUT_BYTES`
- `DAGFORGE_SANDBOX_MAX_STDERR_BYTES`
- `DAGFORGE_SANDBOX_MAX_STREAM_LINE_BYTES`
- `DAGFORGE_SANDBOX_MAX_PROCESSES`
- `DAGFORGE_SANDBOX_MAX_OPEN_FILES`

Command-specific program registration and environment policy also live in
`[sandbox]`. A slash-free name such as `python3` resolves only through
`programs`; DAGForge never searches PATH. Registered and legacy absolute paths
are canonicalized and checked both while compiling the node config and
immediately before process launch.

The runner always owns `PATH`, `HOME`, and `TMPDIR`. Only names listed in
`inherited_environment` are copied from the DAGForge process, and
credential-like names are rejected from inheritance. Workflow `env` and
`input_env` values still require `allowed_environment`.

`allow_unlisted_*` is a development override; production configuration should
keep both switches false and `require_trusted_files` true. Per-Attempt workdirs
are owner-only and removed after completion unless `retain_workdirs` is enabled.
The legacy `workspace_root`, `retain_workspaces`, and
`DAGFORGE_SANDBOX_WORKSPACE_ROOT` names remain accepted for compatibility.

Output-limit overflow kills the whole process group and reports resource
exhaustion. Application shutdown rejects new starts, kills active process
groups, and waits for reaping before Runtime threads stop.

### 2.3 HTTP executor

The HTTP executor is disabled by default. Enable it only with an exact list of
trusted origins:

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
dns_timeout_ms = 5000
connect_timeout_ms = 10000
tls_handshake_timeout_ms = 10000
write_timeout_ms = 30000
first_byte_timeout_ms = 30000
read_timeout_ms = 30000
idle_connection_timeout_ms = 30000
max_idle_connections_per_origin = 4
max_idle_connections_per_shard = 32
tls_min_version = "1.2"
tls_ca_file = ""
tls_client_cert_file = ""
tls_client_key_file = ""
```

An origin consists of scheme, host, and effective port. Matching is exact and
case-normalized; paths do not belong in `allowed_origins`. After DNS resolution,
every endpoint is checked before connect. By default loopback, link-local,
RFC1918/ULA, multicast, documentation, benchmarking, and reserved ranges are
denied. `allowed_ip_cidrs` provides explicit exceptions for required internal
services. It does not expand the Origin list.

HTTPS verifies SNI and the hostname, enforces `tls_min_version`, and uses the
system trust store plus `tls_ca_file` when one is supplied. Client certificate
and key must be configured together for mTLS. Plain HTTP requires both
`allow_plaintext = true` and a matching `http://` origin. Redirects are not
followed, so an allowed server cannot move a request to an unauthorized origin.

The request, response, and header limits are hard parser/executor bounds. Both
per-shard and process-wide concurrency limits are enforced before a socket
opens. Saturation fails the Attempt with resource exhaustion, allowing the node
retry policy to decide whether to try again.

Reusable HTTP/1.1 connections are retained in owner-shard pools keyed by the
exact authorized Origin. Pools are bounded by
`max_idle_connections_per_origin` and `max_idle_connections_per_shard`; idle
clients expire after `idle_connection_timeout_ms`. Pooling never bypasses
Origin or resolved-address policy because only clients created after those
checks enter the pool.

DNS, TCP connect, TLS handshake, request write, first response byte/header, and
subsequent response reads have independent timeouts. The node `timeout_sec`
remains the total upper bound and can cancel any stage. Attempt errors identify
the failed stage; timeout errors still classify as Workflow timeouts.

Environment overrides:

- `DAGFORGE_HTTP_EXECUTOR_ENABLED`
- `DAGFORGE_HTTP_EXECUTOR_ALLOW_PLAINTEXT`
- `DAGFORGE_HTTP_EXECUTOR_DENY_PRIVATE_NETWORKS`
- `DAGFORGE_HTTP_EXECUTOR_MAX_REQUEST_HEADERS`
- `DAGFORGE_HTTP_EXECUTOR_MAX_REQUEST_HEADER_BYTES`
- `DAGFORGE_HTTP_EXECUTOR_MAX_REQUEST_BODY_BYTES`
- `DAGFORGE_HTTP_EXECUTOR_MAX_RESPONSE_HEADERS`
- `DAGFORGE_HTTP_EXECUTOR_MAX_RESPONSE_HEADER_BYTES`
- `DAGFORGE_HTTP_EXECUTOR_MAX_RESPONSE_BODY_BYTES`
- `DAGFORGE_HTTP_EXECUTOR_MAX_CONCURRENT_REQUESTS_PER_SHARD`
- `DAGFORGE_HTTP_EXECUTOR_MAX_CONCURRENT_REQUESTS`
- `DAGFORGE_HTTP_EXECUTOR_DNS_TIMEOUT_MS`
- `DAGFORGE_HTTP_EXECUTOR_CONNECT_TIMEOUT_MS`
- `DAGFORGE_HTTP_EXECUTOR_TLS_HANDSHAKE_TIMEOUT_MS`
- `DAGFORGE_HTTP_EXECUTOR_WRITE_TIMEOUT_MS`
- `DAGFORGE_HTTP_EXECUTOR_FIRST_BYTE_TIMEOUT_MS`
- `DAGFORGE_HTTP_EXECUTOR_READ_TIMEOUT_MS`
- `DAGFORGE_HTTP_EXECUTOR_IDLE_CONNECTION_TIMEOUT_MS`
- `DAGFORGE_HTTP_EXECUTOR_MAX_IDLE_CONNECTIONS_PER_ORIGIN`
- `DAGFORGE_HTTP_EXECUTOR_MAX_IDLE_CONNECTIONS_PER_SHARD`
- `DAGFORGE_HTTP_EXECUTOR_TLS_MIN_VERSION`
- `DAGFORGE_HTTP_EXECUTOR_TLS_CA_FILE`
- `DAGFORGE_HTTP_EXECUTOR_TLS_CLIENT_CERT_FILE`
- `DAGFORGE_HTTP_EXECUTOR_TLS_CLIENT_KEY_FILE`

Origins and CIDR exceptions intentionally have no environment-list override;
keep them in the auditable system configuration.

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

The directory contains an independent immutable Plan catalog, atomic Run
checkpoint files, an append-only Evidence log, and Artifact data plus metadata.
An initial checkpoint is written before a Run ID is accepted. Stable Run and
Task transitions refresh the checkpoint.

Completed Runs and outputs are restored on startup. Non-terminal Runs continue
from their checkpoint. A process or request that was active when the process
stopped is not reattached: its old Attempt is closed with the infrastructure
failure code `runtime_restarted`, while the Task becomes ready for a new
Attempt. Succeeded Tasks and retained outputs are not rerun. Paused Runs remain
paused and retry-waiting Tasks preserve their retry deadline.

When a retention limit is exceeded, the oldest completed Run or Evidence
record is removed. Evicted durable Run checkpoints are deleted as well.

### 2.7 API

Set `api.enabled = true` to start the HTTP control plane. TLS requires both a
certificate chain and private key path. When TLS is enabled the listener is
TLS-only; plaintext is never detected and routed on the same port.

```toml
[api]
enabled = true
host = "127.0.0.1"
port = 8888
tls_enabled = true
tls_cert_file = "/etc/dagforge/api-chain.pem"
tls_key_file = "/etc/dagforge/api-key.pem"
tls_min_version = "1.2"
bearer_token_env = "DAGFORGE_API_TOKEN"
max_request_header_bytes = 65536
max_request_body_bytes = 1048576
connection_idle_timeout_ms = 30000
max_connections = 1024
max_requests_per_connection = 100
max_concurrent_requests = 128
```

When `bearer_token_env` is set, the named environment variable must contain a
non-empty token before the server starts. Clients send it as
`Authorization: Bearer <token>`. Oversized requests are rejected at both the
HTTP parser and route boundary; saturated routes return `429`. The server also
bounds active TCP connections, idle TLS/read time, and requests per keep-alive
connection. Unsupported verbs return `405` rather than being interpreted as
GET. `stop()` closes active connections and waits for handlers to exit.

Environment overrides:

- `DAGFORGE_API_ENABLED`
- `DAGFORGE_API_HOST`
- `DAGFORGE_API_PORT`
- `DAGFORGE_API_REUSE_PORT`
- `DAGFORGE_API_TLS_ENABLED`
- `DAGFORGE_API_TLS_CERT_FILE`
- `DAGFORGE_API_TLS_KEY_FILE`
- `DAGFORGE_API_TLS_MIN_VERSION`
- `DAGFORGE_API_BEARER_TOKEN_ENV`
- `DAGFORGE_API_MAX_REQUEST_HEADER_BYTES`
- `DAGFORGE_API_MAX_REQUEST_BODY_BYTES`
- `DAGFORGE_API_CONNECTION_IDLE_TIMEOUT_MS`
- `DAGFORGE_API_MAX_CONNECTIONS`
- `DAGFORGE_API_MAX_REQUESTS_PER_CONNECTION`
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
        "program": "echo",
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
- `checkpoint`: emit an explicit Checkpoint Evidence record after successful
  completion. Durable recovery no longer depends on this flag because stable
  Runtime transitions are persisted automatically.

An input binding is:

```json
{"input":"source","node":"upstream","port":"result"}
```

### 3.2 Command configuration

```json
{
  "program": "python3",
  "arguments": ["-c", "print('hello')"],
  "env": [{"key":"MODE","value":"test"}],
  "input_env": []
}
```

`program` should be an administrator-registered name. Absolute paths remain
supported when explicitly authorized, but relative paths and implicit PATH
lookup are never used. Arguments are passed directly; there is no implicit
shell. Use the registered `sh` program when shell syntax is required. `PATH`,
`HOME`, and `TMPDIR` are runtime-owned and cannot be overridden by the node.

The registry authorizes the initial executable, not every descendant process.
A registered shell can launch other binaries that remain readable/executable
inside the sandbox. Register a shell only when that broader command surface is
intended; filesystem restrictions, the private network namespace, seccomp, and
resource limits remain the containment boundary.

Inputs are not injected automatically. Map selected inputs to environment
variables explicitly:

```json
{
  "inputs": [
    {"input":"payload","node":"prepare","port":"result"}
  ],
  "config": {
    "program": "python3",
    "arguments": ["consume.py"],
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
- Executor completion failures are structured. Every failed Attempt, Task, and
  Run can expose a normalized `kind`, stable `code`, human `message`, and
  executor-owned bounded `details` object. Command diagnostics retain exit
  status/stdout/stderr; HTTP diagnostics retain rejected response or transport
  cause data, with credential-bearing response header values redacted. The same
  failure is persisted in checkpoints and Evidence, so an external repair
  controller can inspect it without parsing log text. Checkpoint loading rejects
  malformed failure objects instead of admitting them into Runtime state.
- Task output publication is atomic at the Runtime boundary. If one output
  violates the budget or storage contract, earlier outputs from the same
  completion are removed and are not exposed through the normal output API.
- Large failure details are stored through the existing Artifact store and the
  failure keeps a named `details` Artifact reference. The dedicated Run
  failure-report interface returns the complete Run/Task/Attempt hierarchy
  without knowing Command, HTTP, Model, WASM, or MCP fields.
- A failed dependency causes downstream tasks to be skipped with a recorded
  reason.
- Cancellation enters `stopping`, routes cancellation to each active executor,
  and becomes terminal only after every attempt completes. Command cancellation
  kills the complete Minijail process namespace; HTTP cancellation interrupts
  DNS, connect, TLS, write, and read operations and closes an established
  socket.
- Duplicate non-empty idempotency keys return the original Run ID while its
  checkpoint is retained, including after service restart, only when the
  original Workflow and Plan identity match. Repair keys additionally bind the
  parent Run. Conflicting reuse is rejected rather than returning an unrelated
  Run.
- Large string and JSON outputs are replaced with Artifact references.

## 6. Current durability boundary

Plan registration and Run checkpoints are independently persisted when file
storage is enabled. Startup restores the Plan catalog first, then terminal and
non-terminal Runs. The checkpoint trigger carries the idempotency key, so the
authoritative Run mapping is reconstructed while the checkpoint remains
retained. Checkpoint loading revalidates the Plan digest, output budget,
published outputs, successful Task ownership of normal outputs, and referenced
Artifact metadata/content before the Run is admitted.

Control-plane errors use the same structured shape as execution failures:
`kind`, stable `code`, `message`, bounded `details`, and `artifacts`. AI and
operator clients should branch on `code`, never on message text.

Repair does not mutate a failed Run. A revised full Plan is compiled and starts
a child Run with `parent_run_id`, `parent_plan_id`, `repair_revision`, and a
reason. Successful parent nodes are reused only when their execution contract,
incoming conditions, dependencies, retained outputs, and referenced Artifacts
remain valid. Invalidity propagates through descendants; independent successful
branches remain reusable.

The storage model is single-process and file-backed. Evidence remains an audit
log rather than a replay database, and an external executor operation cannot be
reattached across process loss.

The target multi-executor acceptance graph is recorded in
[`NORTH_STAR_WORKFLOW.md`](NORTH_STAR_WORKFLOW.md).

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

Run `bench-core` for Runtime dispatch, Plan processing, Workflow execution,
local HTTP transport, and checkpoint persistence benchmarks. See
[`BENCH_REPORT.md`](BENCH_REPORT.md) for the controlled runner and reporting
rules.
