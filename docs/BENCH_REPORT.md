# DAGForge 0.4 Benchmark Scope

The 0.4 benchmark suite measures the runtime that is actually shipped.
Historical Airflow-style benchmark scripts and generated 0.3 DAG fixtures were
removed because they exercised the retired scheduler, MySQL persistence, cron,
sensor, and task-configuration stack.

## Current targets

`bench-core` contains four layers of measurements:

- runtime primitives: same-shard batching, balanced fan-out, hot-owner fan-in,
  external round-robin dispatch, and cold start/stop;
- plan processing: JSON parsing and compilation for linear and fan-out graphs;
- workflow execution: complete `WorkflowRuntime` runs for linear and fan-out
  graphs, including task state transitions, evidence, checkpoints, output
  storage, terminal callbacks, and explicit per-node checkpoint density;
- local transport and persistence: HTTP keep-alive versus reconnect for small
  and 16 KiB responses, plus in-memory and atomic file checkpoint saves at
  representative Run sizes.

The workflow execution scenarios use an immediate in-process executor. This
keeps command launch and external service latency out of the measurement so the
result represents DAGForge orchestration cost. HTTP transport is measured in a
separate local loopback scenario, preserving a clear boundary between runtime
orchestration and transport cost.

Command sandbox startup is covered by integration tests, not by `bench-core`.
Any future sandbox benchmark must report the Minijail revision, kernel, enabled
namespace features, Landlock ABI, and seccomp policy digest.

Build and run:

```bash
BUILD2_CONFIG_NAME=bench-release \
BUILD2_CC_COPTIONS='-O3 -DNDEBUG -march=native -fno-omit-frame-pointer' \
BUILD2_TARGETS='bin/exe{bench-core}' ./scripts/build.sh
python3 scripts/run-benchmarks.py \
  --binary ~/.local/share/build2-configs/dagforge-bench-release/dagforge/bin/bench-core \
  --repetitions 7 \
  --build-label 'gcc O3 NDEBUG march=native'
```

The runner performs warmup, random interleaving, repeated samples, and writes
`.git/benchmarks/raw.json`, `.git/benchmarks/environment.json`, and
`.git/benchmarks/summary.md`. The summary reports median, p95, p99, standard
deviation, coefficient of variation, and median throughput when the benchmark
exposes item counts. Results with high CV must be treated as noisy rather than
used for performance claims.

## Reporting rules

Performance claims for 0.4 must identify:

- exact Git commit;
- compiler and build type;
- CPU model, NUMA topology, and affinity settings;
- shard and compute-pool configuration;
- benchmark command and repetitions;
- median and tail distribution, not only the best run.

Do not reuse 0.3 throughput or Airflow comparison numbers as claims about the
0.4 WorkflowRuntime. The current end-to-end scenarios are DAGForge-only
baselines and are not competitor comparisons.

Representative audit results and the performance-related 0.4 follow-up work
are summarized in [`0.4_DEVELOPMENT_STATUS.md`](0.4_DEVELOPMENT_STATUS.md).
