# 21 — Amortize Evidence retention compaction

**What to build:** Preserve exact in-memory Evidence retention without rewriting the complete retained JSONL file after every append at capacity.

**Status:** resolved

**Owner:** OpenAI

**Audit evidence (2026-07-16):** The previous full-capacity path copied the complete vector and atomically rewrote the complete Evidence file for every new record, producing sustained O(n) write amplification at the default 100,000-record retention.

- [x] Ordinary retention overflow uses durable append and keeps the target inode stable.
- [x] A bounded stale prefix is compacted atomically in batches.
- [x] Compaction occurs before the configured file-byte ceiling would be exceeded.
- [x] Restart restores the exact latest retained window.

**Resolution evidence:** Inode and syscall tests prove repeated overflow appends do not rewrite the file; file-limit and restart tests prove bounded compaction and exact retained history.
