# Run Bootstrap Review

## Standards

No findings after revision.

- The public `WorkflowRuntime` interface is unchanged. Bootstrap payloads and
  methods are private, and their concrete payload definitions live only in the
  bootstrap implementation file.
- The extraction follows knowledge ownership rather than file size: admission,
  initial persistence, idempotency, and activation posting moved together;
  owner-shard Run/Task/Attempt execution did not move.
- New and Repair Runs persist one authoritative initial Checkpoint and activate
  that same object. The previous reconstruction of a second initial Checkpoint
  was removed.
- `schedule_activation()` passes the deletion test: removing it would duplicate
  owner-shard routing, initialization counters, lifetime acquisition, teardown
  notification, and Checkpoint handoff across three lifecycle paths.
- Review identified an unconditional post-destruction `scope_exit` access. A
  shared initialization tracker and deterministic queued-activation destruction
  test now make the bootstrap lifetime rule explicit and sanitizer-verifiable.
- No generic workflow framework, public SDK type, new state, or alternate
  execution path was introduced.

## Spec

No findings.

- New Run and Repair Run creation share one atomic admission mechanism.
- Restore activation uses the same owner-shard posting and accounting path.
- A matching idempotency binding returns the authoritative Run; conflicting
  Workflow, Plan, or parent Run identity remains `AlreadyExists`.
- Initial persistence still completes before activation, and persistence failure
  still rejects the Run before executor dispatch.
- Repair reuse decisions, retained values, parent identity, revision, and
  Evidence behavior remain intact.
- Existing external methods, HTTP/JSON contracts, CLI behavior, and accepted
  Run/Task/Attempt state semantics are unchanged.
- Normal and ASAN/UBSAN runtime suites both passed all 264 tests.

Summary: Standards 0 findings; Spec 0 findings.
