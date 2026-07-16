# 16 — Bound durable storage reads

**What to build:** Add explicit byte limits to durable JSON, JSONL record, Checkpoint, Plan, and Artifact metadata reads before allocation and decode.

**Status:** resolved

**Owner:** OpenAI

**Audit evidence (2026-07-16):** A 512 MiB sparse Plan catalog file under a 300 MiB virtual-memory limit reached `storage_detail::read_all()`, threw an uncaught `std::bad_alloc`, and terminated `dagforge validate` with exit code 134. The same unbounded primitive is used by Plan, Checkpoint, Artifact content, and Artifact metadata reads.

- [x] Limits are owned by `StorageConfig` and passed explicitly to each persistent Store.
- [x] Oversized files return `ResourceExhausted` before full allocation and are checked again while reading or appending.
- [x] Plan, Checkpoint, Evidence file, Evidence record, Artifact metadata, and Artifact content limits remain distinct.
- [x] CLI startup reports the configuration/runtime phase without exposing persisted contents.

**Resolution evidence:** The 512 MiB sparse Plan black-box reproduction exits 1 with `resource exhausted` instead of aborting; focused byte-limit tests and Application startup tests pass under normal, ASAN, and UBSAN builds.
