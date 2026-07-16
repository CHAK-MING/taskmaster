# 15 — Make Evidence recovery explicit

**What to build:** Replace constructor-side best-effort Evidence loading with an initialization path that returns `Result`, preserves ordered valid history, and documents the only tolerated crash-tail condition.

**Status:** resolved

**Owner:** OpenAI

**Audit evidence (2026-07-16):** A durable ledger containing a valid record, an interior malformed record, and another valid record was accepted by `dagforge validate`; the command exited 0 with no warning and left the corrupt file unchanged. Current construction also ignores retention rewrite failures.

- [x] Application fails initialization when the Evidence path is unreadable or interior records are malformed.
- [x] Only an unterminated final fragment classified by the DAGForge JSON wrapper as `Incomplete` is repaired; complete invalid EOF data and interior corruption fail closed.
- [x] Retention rewrite and crash-tail repair failures are returned rather than ignored.
- [x] Existing durable append and Run failure propagation remain unchanged.

**Resolution evidence:** `EvidenceLedger::open()` is Result-bearing; Application startup tests cover corruption, oversized files, valid final-record canonicalization, and incomplete crash-tail repair; full tests, ASAN, UBSAN, TSAN, coverage, and 10,000 fuzz runs pass.
