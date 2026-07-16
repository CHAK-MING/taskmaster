# 26 — Reduce Store lock hold time around filesystem I/O

**What to build:** Measure and, where justified, separate Store state coordination from digest, serialization, fsync, and directory-scan latency without weakening same-process consistency.

**Status:** ready-for-agent

**Priority:** measured-performance-backlog

**Reason this is not a current blocker:** The documented storage model is single-process, Checkpoint writes are now sparse, and current correctness depends on serializing Store operations. Optimization should follow contention measurements rather than speculative lock splitting.

- [ ] Add contention and large-Artifact benchmarks before changing locks.
- [ ] Preserve one logical writer per managed identity.
- [ ] Define behavior for concurrent `put`, `get`, `erase`, and reconciliation before implementation.
