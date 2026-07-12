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

Build the project:

```bash
./scripts/setup-build2.sh
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

[workflow]
enabled = true

[[workflow.model_providers]]
name = "openai"
base_url = "https://api.openai.com"
responses_path = "/v1/responses"
api_key_env = "OPENAI_API_KEY"
timeout_sec = 120
max_response_bytes = 16777216

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

The compute pool is separate from I/O shards and is used by Compute and
Evaluator nodes.

- `threads = 0` selects an automatic thread count.
- `queue_capacity` is a hard bound on pending compute work.
- optional affinity settings control compute worker placement.

Environment overrides:

- `DAGFORGE_COMPUTE_THREADS`
- `DAGFORGE_COMPUTE_QUEUE_CAPACITY`
- `DAGFORGE_COMPUTE_PIN_THREADS`
- `DAGFORGE_COMPUTE_CPU_AFFINITY_OFFSET`

### 2.3 Workflow adapters

`workflow.enabled` creates the workflow control plane and runtime.

Model providers use environment variables for credentials. The current plan
field names the environment variable directly; the secret value itself is never
embedded in the plan. Server-managed credential aliases are a later milestone.

MCP servers are configured with repeated `workflow.mcp_servers` tables:

```toml
[[workflow.mcp_servers]]
name = "tools"
url = "http://127.0.0.1:9000/mcp"
bearer_token_env = "MCP_TOKEN"
protocol_version = "2025-06-18"
timeout_sec = 120
max_response_bytes = 16777216
```

### 2.4 API

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
type = "noop"
outputs = ["result"]
max_retries = 0
timeout_sec = 30
checkpoint = false

[nodes.config]
```

### 3.1 Node fields

- `id`: unique within the workflow.
- `name`: optional display name.
- `type`: executor or runtime node type.
- `config`: type-specific strict configuration object.
- `inputs`: named bindings to an upstream node output.
- `outputs`: output port names. The default is `result`.
- `max_retries`: retries after the first attempt.
- `timeout_sec`: node deadline.
- `checkpoint`: save a runtime checkpoint after successful completion.

An input binding is:

```toml
[[nodes.inputs]]
input = "source"
node = "upstream"
port = "result"
```

### 3.2 Node types

#### Noop

Returns `true` without inputs, or forwards the first bound input.

#### Shell

```toml
[nodes.config]
command = "printf hello"
working_dir = "/tmp"
env = [{ key = "MODE", value = "test" }]
```

Shell execution is disabled by default. When
`require_approval_for_shell = true`, each Shell node must have an Approval
ancestor.

#### Docker

```toml
[nodes.config]
image = "alpine:3.22"
command = "printf hello"
working_dir = "/work"
docker_socket = "/var/run/docker.sock"
env = []
```

#### Lua

Specify exactly one of `script` or `script_file`:

```toml
[nodes.config]
script = "return {ok = true}"
max_instructions = 100000
max_memory_bytes = 8388608
```

The sandbox exposes `dagforge.log`, `dagforge.sleep`,
`dagforge.json_encode`, and `dagforge.json_decode`. The retired 0.3 DAG task
context is not exposed.

#### HTTP

```toml
[nodes.config]
url = "https://example.com/data"
method = "GET"
headers = []
body = ""
body_input = ""
expected_status = 200
```

#### Model

```toml
[nodes.config]
provider = "openai"
model = "gpt-5"
system_prompt = "Return valid JSON."
prompt = "Process: "
prompt_input = "$trigger"
credential = { name = "OPENAI_API_KEY" }
max_output_tokens = 4096
temperature = 0.0
```

#### Tool

Tool names use `server/tool` when multiple MCP servers are configured.

```toml
[nodes.config]
tool = "tools/search"
arguments = { query = "dag runtime" }
arguments_input = ""
credential = { name = "MCP_TOKEN" }
```

#### Compute

Supported operations are `identity`, `concat`, `sha256`, `json_parse`, and
`json_stringify`.

#### Evaluator

Supported operations are `truthy`, `equals`, `contains`, and
`score_at_least`.

#### Approval

An Approval node suspends the run until an external approval decision arrives
or the request expires.

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
- `evaluation_passed`

### 3.4 Policy and budgets

```toml
[policy]
allow_shell = false
allow_docker = true
allow_lua = true
allow_network = true
allow_model_calls = true
allow_tools = true
require_approval_for_shell = true
allowed_http_hosts = []
allowed_model_providers = []
allowed_tools = []

[policy.budget]
max_nodes = 256
max_parallel_nodes = 32
max_total_output_bytes = 67108864
max_model_tokens = 1000000
max_run_duration_sec = 3600
```

The current plan policy is validated by the compiler. Server-side admission
policy separation is a later 0.4 milestone.

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
- Node state mutations occur on the owner shard.
- Input values reference explicit upstream output ports.
- A failed dependency causes downstream nodes to be skipped.
- A failed node is retried up to `max_retries`.
- Run and node cancellation is cooperative for Compute and delegated to the
  active executor for process nodes.
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
socket mount is required only for Docker nodes.

## 9. Verification

```bash
bash scripts/check-module-graph.sh
bash scripts/check-agent-conventions.sh
BUILD2_CONFIG_NAME=gcc ./scripts/build.sh
~/.local/share/build2-configs/dagforge-gcc/dagforge/bin/all-unit-tests
```

Run `bench-core` for Runtime, memory arena, and Lua executor microbenchmarks.
