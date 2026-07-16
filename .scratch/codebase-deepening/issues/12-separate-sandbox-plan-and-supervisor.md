# 12 — Separate Sandbox Plan and process supervision

**What to build:** Keep `ICommandRunner` while separating immutable Minijail
launch planning from the lifecycle of one sandbox process group.

**Blocked by:** 10 — Compile the Command Node contract once

**Status:** ready-for-agent

- [ ] File trust, execution-root, Minijail arguments, limits, and environment
      policy are owned by Sandbox Plan construction.
- [ ] One Process Supervisor owns launch, pipes, output limits, heartbeat,
      timeout, cancellation, reaping, cleanup, and final callback.
- [ ] Process completion and slot/registry release happen exactly once.
- [ ] `ICommandRunner` remains the external sandbox seam.
- [ ] Existing sandbox security and lifecycle behavior is unchanged.
