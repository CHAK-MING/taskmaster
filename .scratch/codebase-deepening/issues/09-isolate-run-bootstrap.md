# 09 — Isolate Run bootstrap

**What to build:** Give new Runs, restored Runs, and Repair Runs one private
bootstrap mechanism for idempotency, initial Checkpoint persistence,
owner-shard activation, lifetime checks, and initialization accounting.

**Blocked by:** 08 — Deepen the Workflow HTTP route adapter

**Status:** resolved

- [x] New Run and Repair Run creation no longer duplicate lifecycle and posting
      mechanics.
- [x] Restore activation uses the same owner-shard activation discipline.
- [x] Idempotency and initial persistence remain atomic with admission.
- [x] `WorkflowRuntime` retains its existing external interface.
- [x] Restore, Repair Run, idempotency, shutdown, and persistence tests pass.

## Answer

Run admission now lives in `workflow_runtime_bootstrap.cpp`. New Runs and Repair
Runs enter one `bootstrap_run()` path that holds lifecycle admission, performs
the idempotency lookup, constructs one initial Checkpoint, persists that same
Checkpoint, records the idempotency binding, and hands the persisted object to
one `schedule_activation()` path. The idempotency lock remains held from lookup
through persistence and binding, eliminating the previous Repair Run race
between checking a key and recording its authoritative Run.

New, restored, and Repair Runs now share owner-shard selection, initialization
accounting, lifetime acquisition, queued activation, and completion notification.
The owner-shard Run/Task/Attempt engine remains in `workflow_runtime.cpp` behind
`initialize_checkpoint_run()`; no state transition or product interface moved.

The final review found a teardown defect in the old posting pattern: an expired
weak lifetime still reached a `scope_exit` that accessed the destroyed Runtime.
Initialization accounting now uses a shared tracker. Destruction waits for an
already-live queued activation when the core Runtime is running, while a queued
activation that starts after destruction only decrements the independent
tracker. `DestructionWaitsForQueuedRunActivation` deterministically covers this
ordering.

Verification:

- build, convention, module graph, scoped diff, and CLI scenario checks passed;
- focused Start, Restore, Repair, idempotency, persistence, quiesce, recovery,
  and destruction tests passed;
- the complete normal runtime target passed: 264 tests from 33 suites;
- the complete ASAN/UBSAN runtime target passed with leak detection enabled:
  264 tests from 33 suites.
