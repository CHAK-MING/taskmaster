# 19 — Restore sparse Checkpoint boundaries

**What to build:** Remove full Checkpoint persistence from ordinary Run and Task state notification paths while preserving initial admission, explicit Node checkpoint, terminal state, and persistence-failure semantics.

**Status:** resolved

**Owner:** OpenAI

**Audit evidence (2026-07-16):** A one-Node Workflow with no `checkpoint: true` produced seven atomic Run Checkpoint renames and 27 `fsync` calls. `bootstrap_run()` already persists the initial Checkpoint, while `emit_run_state()` and `emit_task_state()` currently persist again on every transition. This contradicts the documented fix for near-quadratic state-notification write amplification.

- [x] A non-checkpoint one-Node Run persists its initial and terminal Checkpoints only.
- [x] `checkpoint: true` persists after the selected Node succeeds.
- [x] Run and Task callbacks remain observable without becoming persistence boundaries.
- [x] Retry-waiting, paused, stopping, resume, and terminal recovery boundaries remain durable, and persistence failure still fails closed with `checkpoint_persist_failed`.
- [x] Regression tests and a syscall black-box count durable replacements.

**Resolution evidence:** The one-Node reproduction fell from seven Run Checkpoint renames to two; recovery-boundary tests, full tests, TSAN, ASAN, and UBSAN pass.
