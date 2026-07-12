# DAGForge

**A general-purpose, high-performance DAG runtime built with C++23.**

[English](README.md) | [简体中文](README_CN.md)

DAGForge 0.4 is the execution layer for programmable workflows. An upstream
application—such as a Python AI planner—produces a versioned workflow plan;
DAGForge validates, compiles, schedules, executes, observes, and cancels that
plan with deterministic runtime semantics.

The runtime does not interpret natural language or own an agent loop. Those
concerns belong above the execution seam.

## Architecture

```text
AI application / workflow author
            |
            v
      WorkflowPlan v1
            |
            v
       PlanCompiler
            |
            v
   immutable ExecutionPlan
            |
            v
      WorkflowRuntime
     /       |        \
 executors  compute   adapters
```

The current runtime provides:

- owner-shard workflow execution on a Boost.Asio-based C++23 runtime;
- immutable compiled plans with cycle, port, policy, and resource validation;
- explicit typed node outputs and input bindings;
- bounded node and run concurrency, deadlines, retries, and cancellation;
- Shell, Docker, Lua, HTTP, Compute, Model, Tool, Evaluator, Approval, and Noop
  node types;
- artifact externalization for large values;
- checkpoints, evidence records, idempotent triggers, and approval gates;
- REST control-plane endpoints and Prometheus metrics.

The built-in checkpoint, evidence, artifact, plan, and completed-run stores are
currently in-memory adapters. Durable recovery is a later 0.4 milestone, not a
property of the current build.

## Requirements

- Linux x86-64 or ARM64
- GCC 15+
- build2 0.17+
- Boost 1.88+
- OpenSSL development libraries

MySQL and Node.js are not required by the 0.4 runtime core.

## Build

```bash
./scripts/setup-build2.sh
./scripts/build.sh
```

The executable is produced in the selected build2 configuration directory.
For the default configuration, `scripts/build.sh` prints the exact path.

Run the unit-test executable after building:

```bash
~/.local/share/build2-configs/dagforge-gcc/dagforge/bin/all-unit-tests
```

## Configuration

The supported top-level sections are:

- `[runtime]`: shard count and shard affinity;
- `[compute]`: bounded CPU worker pool;
- `[workflow]`: workflow adapters and provider catalogs;
- `[api]`: HTTP control plane.

See [`system_config.toml`](system_config.toml) for a complete example.

## CLI

Validate a workflow plan:

```bash
dagforge validate --file dags/hello_world.toml
```

Run a plan locally and wait for completion:

```bash
dagforge run \
  --config system_config.toml \
  --file dags/hello_world.toml \
  --wait
```

Start the REST service:

```bash
dagforge serve --config system_config.toml
```

## Workflow plan

A minimal TOML plan is:

```toml
workflow_id = "hello-world"
schema_version = 1

[[nodes]]
id = "start"
type = "noop"
outputs = ["result"]
timeout_sec = 30

[nodes.config]
```

Plans are accepted as strict JSON or TOML. Unknown fields are rejected.

## HTTP control plane

The service exposes plan registration, workflow execution, run status,
outputs, evidence, approvals, cancellation, health, status, and metrics. See
[`docs/API.md`](docs/API.md).

## Benchmarks

The 0.4 benchmark targets measure the current runtime primitives directly:

```bash
~/.local/share/build2-configs/dagforge-gcc/dagforge/bin/bench-core
```

Historical Airflow-style scheduler benchmarks were removed because they tested
the retired 0.3 DAG/scheduler/storage stack rather than the 0.4 runtime.

## Documentation

- [`docs/USER_GUIDE.md`](docs/USER_GUIDE.md)
- [`docs/API.md`](docs/API.md)
- [`docs/CLANGD_SETUP.md`](docs/CLANGD_SETUP.md)
- [`docs/BENCH_REPORT.md`](docs/BENCH_REPORT.md)

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
