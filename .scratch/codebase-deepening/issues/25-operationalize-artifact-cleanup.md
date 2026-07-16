# 25 — Operationalize Artifact cleanup debt

**What to build:** Add an operator-owned command or authenticated control-plane operation that exposes Artifact reconciliation and optionally removes only explicitly selected orphan data after a dry run.

**Status:** ready-for-agent

**Priority:** later-storage-operations

- [ ] Reporting remains non-destructive by default.
- [ ] Cleanup requires an explicit Artifact identity or reconciliation generation.
- [ ] Symlinks, directories, malformed metadata, and content mismatches are never recursively removed by a generic cleanup pass.
- [ ] Metrics expose reconciliation counts and deferred cleanup attempts.
