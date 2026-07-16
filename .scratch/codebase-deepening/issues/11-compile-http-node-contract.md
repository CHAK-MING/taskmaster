# 11 — Compile the HTTP Node contract once

**What to build:** Separate the HTTP Node's compiled contract, dynamic request
materialization, and one-request lifecycle while preserving egress policy,
pooling, limits, cancellation, and diagnostics.

**Blocked by:** 10 — Compile the Command Node contract once

**Status:** ready-for-agent

- [ ] Static URL authorization, headers, accepted statuses, body mode, and
      output validation happen during compilation.
- [ ] Task start materializes only dynamic inputs and acquires execution
      capacity.
- [ ] One-request state owns connection, timeout, cancellation, completion, and
      slot release exactly once.
- [ ] Connection pooling and diagnostic redaction retain existing behavior.
- [ ] `ITaskExecutor` remains the caller and test seam.
