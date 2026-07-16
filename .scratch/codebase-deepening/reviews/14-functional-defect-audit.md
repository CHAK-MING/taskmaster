# Functional defect audit

Date: 2026-07-16

## Scope

The audit used four independent lenses: durable recovery and resource limits, API/CLI observable behavior, Run lifecycle and persistence boundaries, and Artifact visibility/cleanup semantics. Every reported finding was confirmed against current source and a black-box or syscall-level reproducer.

Grok CLI 0.2.93 is installed, but this environment is not authenticated. `grok models` exposed only `grok-build`, and a one-turn headless probe produced no output and remained blocked until interrupted after more than 100 seconds. No finding below is attributed to Grok.

## Findings

### P0 — Evidence corruption is silently accepted at startup

`EvidenceLedger` loads in its constructor, ignores file-open state, skips every undecodable line, and ignores retention rewrite failure. A file containing valid history on both sides of an interior corrupt line was accepted by `dagforge validate` with exit code 0 and no diagnostic. The service can therefore present incomplete audit history as healthy startup.

Owner: `issues/15-make-evidence-recovery-explicit.md`.

### P0 — Durable reads are unbounded and can terminate the process

`storage_detail::read_all()` grows a vector until EOF without a byte ceiling and without translating allocation failure. A 512 MiB sparse Plan file under a 300 MiB address-space limit caused an uncaught `std::bad_alloc` and exit code 134. The primitive also backs Checkpoint, Artifact content, and Artifact metadata reads.

Owner: `issues/16-bound-storage-reads.md`.

### P1 — Checkpoint write amplification has regressed

`bootstrap_run()` persists an initial Checkpoint, but `emit_run_state()` and `emit_task_state()` also call `checkpoint(run)` on every transition. A one-Node Workflow without `checkpoint: true` produced seven Run Checkpoint atomic renames and 27 `fsync` calls. Since each Checkpoint serializes the full Run snapshot, this restores the state-notification write amplification already described as fixed in the 0.4 status document.

Owner: `issues/19-restore-sparse-checkpoint-boundaries.md`.

### P1 — Artifact DELETE can fail after logical visibility is already gone

`FileArtifactStore::erase()` removes metadata before data. When data cleanup fails, the API returns HTTP 500 even though subsequent reads return 404 and retry cannot discover the orphan through metadata. The observed error was also projected as `kind/code: unknown`.

Owners: `issues/20-make-artifact-delete-outcome-truthful.md` followed by `issues/17-reconcile-artifact-pairs.md`.

## Negative results

- No new API/CLI route mismatch was found beyond the Artifact delete semantics; semantic commands and raw transport scenarios already cover the current route inventory.
- No new owner-shard, cancellation, or teardown defect was established. The existing Runtime audit and full ASAN/UBSAN test layers remain the stronger evidence for those paths.
- Header classification and source placement remain maintainability work, not a blocker for product behavior.

## Release and feature gate

Do not begin a broad new feature wave yet. Recommended order:

1. Restore sparse Checkpoint boundaries; this is a narrow regression with direct syscall evidence.
2. Make Evidence recovery explicit; this protects audit correctness at startup.
3. Bound durable reads; this removes a local-state denial-of-service and process-abort path.
4. Make Artifact deletion truthful, then add pair reconciliation and optional cleanup.
5. Resume feature development after the full standard test flow, runtime audit, and coverage gate pass with these changes.
