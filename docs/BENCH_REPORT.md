# DAGForge 基准规范

## 测量范围

`bench-core` 只测量当前交付的 0.4 Runtime，不复用 0.3 Scheduler、MySQL、cron、sensor、XCom 或 Airflow 风格场景的数据。当前场景包括 shard dispatch、same-shard batching、fan-out、fan-in、cold start/stop、Plan JSON 解析与编译、完整 WorkflowRuntime 执行、本地 HTTP keep-alive/reconnect 和 Checkpoint 持久化。

Workflow 场景使用立即完成的进程内 test executor，使结果代表 DAGForge orchestration cost；Command sandbox 启动由 integration tests 验证，若后续增加 sandbox benchmark，必须记录 Minijail revision、kernel、namespace、Landlock ABI、seccomp digest 和 CPU affinity。

## 构建与运行

```bash
BUILD2_CONFIG_NAME=bench-release BUILD2_CC_COPTIONS='-O3 -DNDEBUG -march=native -fno-omit-frame-pointer' BUILD2_TARGETS='bin/exe{bench-core}' bash scripts/build.sh
python3 scripts/run-benchmarks.py --binary ~/.local/share/build2-configs/dagforge-bench-release/dagforge/bin/bench-core --repetitions 7 --build-label 'gcc O3 NDEBUG march=native'
```

runner 会执行 warmup、随机交错和重复采样，并写入 `.git/benchmarks/raw.json`、`.git/benchmarks/environment.json` 和 `.git/benchmarks/summary.md`。summary 报告 median、p95、p99、standard deviation、coefficient of variation 和可计算时的 median throughput；高 CV 结果必须标记为噪声，不得用于性能结论。

## 报告要求

- 标明 Git commit、compiler、build type、CPU、NUMA、affinity、shard count、命令和 repetition count。
- 报告 median 与 tail distribution，不只报告最快一次。
- 对比前后版本时使用同一机器、同一工具链、同一配置和随机化顺序，并保留 raw samples。
- 不把 0.3 数据或非同条件第三方数据表述为 0.4 WorkflowRuntime 的直接对比结论。
