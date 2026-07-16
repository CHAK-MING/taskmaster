# 20 — Make Artifact deletion outcome truthful

**What to build:** Define and implement a retryable Artifact deletion commit point so an error response cannot silently mean that metadata visibility was already removed while cleanup remains incomplete.

**Status:** resolved

**Owner:** OpenAI

**Audit evidence (2026-07-16):** With valid metadata and a `.bin` path replaced by a directory, `DELETE /api/v1/artifacts/{id}` removed metadata, failed data cleanup with HTTP 500, then `GET` returned 404 while the orphan remained. Retrying the delete cannot locate the resource through its visibility marker.

- [x] The API response states `logical_deleted`, `cleanup_deferred`, and `durability_deferred`, including the post-`unlink` directory-sync failure case.
- [x] Retrying a partially committed delete attempts orphan-data cleanup and is idempotent when cleanup succeeds.
- [x] Metadata remains the visibility marker for ordinary reads.
- [x] Cleanup debt is discoverable by the Artifact reconciliation report and Application startup warning.
- [x] Filesystem failures at the deletion boundary are normalized to `persistence_error`.

**Resolution evidence:** Store and HTTP black-box tests verify logical deletion, 404 read visibility, deferred cleanup, stable retry failure, and reconciliation discovery.
