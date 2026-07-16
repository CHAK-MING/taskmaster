# Storage hardening and project readiness review

## Scope

This review continued beyond the four reproduced storage blockers and examined recovery semantics, byte ceilings, crash consistency, cache coherence, deletion commit points, retention complexity, deterministic output, input robustness, thread and memory safety, strict compilation, coverage, fuzzing, release packaging, dependency pinning, CI, security reporting, and operator-facing documentation.

## Correctness findings resolved

- Checkpoint persistence now follows explicit recovery boundaries instead of every Run, Task, and Attempt notification.
- Evidence startup is Result-bearing, fails closed on committed corruption, repairs only a syntactically incomplete final fragment, and propagates rewrite failures.
- Durable reads and appends enforce typed Store limits before allocation and while data grows.
- Plan digests are verified against decoded content.
- Persistent Plan and Checkpoint reads treat disk as authoritative and fail closed on changed managed files.
- Evidence retention uses durable append plus bounded atomic compaction rather than an O(n) rewrite for every append at capacity.
- Artifact metadata remains the visibility marker; deletion reports logical commit, deferred cleanup, and deferred directory-entry durability separately.
- Artifact reconciliation is deterministic, no-follow, non-destructive, and visible during Application startup.
- Durable removal distinguishes pre-unlink failure from post-unlink directory-sync failure, eliminating a remaining false-failure ambiguity.
- Checkpoint catalog ordering is deterministic by creation time and Run ID.

## Formal quality evidence

- Full functional gate: unit tests, 220 component tests after the final durability tests, 19 Minijail integration tests, all CLI scenario groups, and 24 real Workflow JSON plans pass.
- Runtime sanitizer gate: ASAN, TSAN, and UBSAN pass.
- Storage sanitizer gate: all storage tests plus recovery and Application startup filters pass under ASAN and UBSAN; concurrent storage/runtime filters pass under TSAN.
- Coverage gate: production line coverage reached 90.06% against the unchanged 90% minimum.
- Fuzz gate: 10,000 structured Glaze parser iterations pass without crashes or sanitizer findings.
- Stress gate: 50 shuffled repetitions of the final 34-test storage and startup filter pass, totaling 1,700 executions.
- Strict compilation: Clang `-Wall -Wextra -Wpedantic -Werror` passes after suppressing only the repository-wide Glaze aggregate-initialization warnings `-Wmissing-braces` and `-Wmissing-designated-field-initializers`.
- Dependency gate: vendored dependency versions, licenses, and tree hashes pass repository verification.
- Release gate: a locally staged static release using the pinned installed Minijail revision passes archive reproducibility, path safety, ELF dependency, required-file, CLI execution, and helper execution verification.

## Release environment caveat

The clean Docker `release-verify` build cannot complete on this host because both official Minijail sources are unreachable. The installer now uses the pinned revision, the Android upstream and Google GitHub public mirror, three attempts per source, and a 45-second timeout per attempt. This is an external network failure rather than a source or archive failure; the new GitHub CI release-smoke job will exercise the same clean-container path on hosted infrastructure.

## Remaining known work

- Persistent records still lack an envelope version independent of Workflow Plan schema version; ticket 24 is the next storage-design frontier and should be completed before introducing a second on-disk representation.
- Artifact reconciliation is report-only; ticket 25 covers explicit operator cleanup and metrics.
- Store mutexes intentionally serialize filesystem I/O for correctness; ticket 26 requires measurements before any lock splitting.
- Multi-process file Store writers remain out of scope and are documented as unsupported.
- Vendored vulnerability intelligence and release SBOM generation are tracked by ticket 27; backup/restore procedure, disk quota metrics, Evidence indexing or rotation, and generated API compatibility contracts remain separately scoped operational work.

## Readiness decision

The repository is ready to continue implementation. No reproduced correctness, safety, coverage, deterministic-behavior, or local release-packaging blocker remains in the storage-hardening slice. New persistent-format work should begin with ticket 24 rather than adding unversioned files.
