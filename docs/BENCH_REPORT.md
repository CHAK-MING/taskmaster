# DAGForge 0.4 Benchmark Scope

The 0.4 benchmark suite measures the runtime that is actually shipped.
Historical Airflow-style benchmark scripts and generated 0.3 DAG fixtures were
removed because they exercised the retired scheduler, MySQL persistence, cron,
sensor, and task-configuration stack.

## Current targets

`bench-core` contains:

- runtime dispatch and cross-shard operations;
- memory-arena allocation behavior.

Command sandbox startup is covered by integration tests, not by `bench-core`.
Any future sandbox benchmark must report the Minijail revision, kernel, enabled
namespace features, Landlock ABI, and seccomp policy digest.

Build and run:

```bash
./scripts/build.sh
~/.local/share/build2-configs/dagforge-gcc/dagforge/bin/bench-core
```

## Reporting rules

Performance claims for 0.4 must identify:

- exact Git commit;
- compiler and build type;
- CPU model, NUMA topology, and affinity settings;
- shard and compute-pool configuration;
- benchmark command and repetitions;
- median and tail distribution, not only the best run.

Do not reuse 0.3 throughput or Airflow comparison numbers as claims about the
0.4 WorkflowRuntime. A new end-to-end benchmark will be added after the plan
contract and durable runtime store are stable.
