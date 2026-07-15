## 1. Durable Plan and Run state

- [x] 1.1 Add an in-memory/file-backed Plan store and persist canonical Plans
      through the control plane.
- [x] 1.2 Load the Plan catalog before Run checkpoints during application
      startup.
- [x] 1.3 Persist an initial Run checkpoint before accepting the Run and refresh
      checkpoints at stable state transitions.

## 2. Restart recovery

- [x] 2.1 Stage non-terminal checkpoints while the core Runtime is stopped.
- [x] 2.2 Activate restored Runs after Runtime startup, preserving completed
      Tasks and values while finalizing interrupted Attempts.
- [x] 2.3 Restore paused, retry-waiting, running, stopping, deadline, and
      idempotency semantics.
- [x] 2.4 Add restart tests for fan-out, partial completion, retry waiting,
      pause, and terminal retention.

## 3. Repair Runs

- [x] 3.1 Add Run lineage and Task reuse provenance to snapshots and storage.
- [x] 3.2 Add conservative repair planning for reusable and invalidated nodes.
- [x] 3.3 Start a child Run from a revised Plan, copy retained values for reused
      nodes, and dispatch only invalidated work.
- [x] 3.4 Add a repair control-plane route and tests covering fan-out/fan-in,
      independent branch reuse, changed-node invalidation, and idempotency.

## 4. Failure products

- [x] 4.1 Add diagnostic Artifact references to structured failures and
      checkpoint/API projection.
- [x] 4.2 Externalize oversized failure details through the existing Artifact
      store without executor-specific Runtime logic.
- [x] 4.3 Add a dedicated Run failure-report interface and HTTP route.

## 5. Verification

- [x] 5.1 Update Evidence types, API docs, user guide, development status, and
      the north-star workflow acceptance scenario.
- [x] 5.2 Run focused tests, full tests, real Workflow scenarios, coverage,
      strict OpenSpec validation, module/convention checks, and ASan+UBSan.
