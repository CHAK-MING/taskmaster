# 17 — Reconcile Artifact file pairs

**What to build:** Add a non-destructive Artifact reconciliation report for complete pairs, orphan data, orphan metadata, malformed metadata, and content mismatch.

**Status:** resolved

**Owner:** OpenAI

**Audit evidence (2026-07-16):** Artifact deletion can currently return HTTP 500 after metadata has already been removed. Replacing the `.bin` file with a directory produced `DELETE` 500, a subsequent `GET` 404, and a remaining invisible orphan. Pair reconciliation is therefore required even after ordinary failed operations, not only after process crashes.

- [x] Metadata remains the visibility marker.
- [x] Reconciliation is deterministic, uses no-follow status inspection, and classifies invalid managed entries.
- [x] Reporting is non-destructive and separate from cleanup.
- [x] Existing Artifact IDs, metadata JSON, and binary files remain compatible.
- [x] Reconciliation reports partial-delete cleanup debt created by an ordinary API operation.

**Resolution evidence:** Store and Application tests cover complete pairs, orphan data, orphan metadata, malformed metadata, content mismatch, invalid entries, size failures, invalid roots, and startup warning behavior.
