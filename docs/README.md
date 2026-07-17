# DAGForge Documentation

- [User Guide](USER_GUIDE.md): build, configuration, Workflow Plan v1, CLI,
  runtime semantics, and current durability boundary.
- [HTTP API](API.md): current control-plane routes and response shapes.
- [Benchmark Scope](BENCH_REPORT.md): supported 0.4 benchmark targets and
  reporting rules.
- [Clangd Setup](CLANGD_SETUP.md): module-aware editor configuration.
- [`agents/`](agents/): repository conventions for automated coding agents.

Architecture decisions:

- [ADR 0001](adr/0001-run-task-attempt-state-machine.md): separate Run, Task,
  and Attempt state machines.
- [ADR 0002](adr/0002-cxx23-foundation-contracts.md): C++23-first foundation
  contracts, dependency boundaries, and verification gates.
