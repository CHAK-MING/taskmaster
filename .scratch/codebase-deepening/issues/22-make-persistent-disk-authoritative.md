# 22 — Make persistent disk state authoritative

**What to build:** Prevent Plan and Checkpoint process caches from hiding deletion, replacement, or corruption of managed files in persistent mode.

**Status:** resolved

**Owner:** OpenAI

**Audit evidence (2026-07-16):** A Store instance could save a record, then continue returning the cached object after the corresponding file was removed, replaced, or corrupted; `list()` could also ignore a changed file with a cached identity.

- [x] Memory mode remains map-backed.
- [x] Persistent `load()` and `list()` read disk under the Store mutex.
- [x] Managed `.json` entries that are not regular files fail closed.
- [x] Checkpoint catalog ordering is deterministic by timestamp and Run ID.

**Resolution evidence:** Same-instance corruption, durable delete failure, managed-path type corruption, and deterministic memory/disk ordering tests pass across normal and sanitizer builds.
