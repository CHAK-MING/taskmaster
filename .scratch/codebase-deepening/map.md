# DAGForge Codebase Deepening Map

## Destination

Produce and execute staged changes that make DAGForge readable and locally reasoned, remove repeated contract knowledge, preserve the HTTP JSON and CLI product interfaces, keep accepted Run/Task/Attempt lifecycle semantics intact, and establish production-grade quality gates before new features are added.

## Working rules

- Issue tracker: local Markdown under `.scratch/codebase-deepening/`.
- Prefer deep private modules behind existing product interfaces; do not create a public C++ SDK by accident.
- Treat vendored source and pinned revisions as primary documentation for the exact dependencies in this checkout.
- Use black-box, syscall, sanitizer, coverage, fuzz, stress, and release evidence rather than relying on code inspection alone.
- Markdown prose uses one paragraph or list item per physical line and is not manually hard-wrapped.

## Resolved decisions

- [Recover the workflow language](issues/01-recover-workflow-language.md) — use Workflow Plan, Execution Plan, Node, Trigger, Run, Task, Attempt, Repair Run, Checkpoint, Evidence, Artifact, and Workflow Control Plane consistently.
- [Identify deepening candidates](issues/02-identify-deepening-candidates.md) and [locate repeated knowledge](issues/03-locate-repeated-knowledge.md) — deepen lifecycle and contract seams rather than splitting files only by size.
- [Use vendored libraries deliberately](issues/04-use-vendored-libraries-deliberately.md) — wrappers isolate product policy, not library syntax.
- [Make CLI parsing select one command](issues/07-pure-cli-command-selection.md), [deepen the Workflow HTTP route adapter](issues/08-workflow-http-route-adapter.md), and [isolate Run bootstrap](issues/09-isolate-run-bootstrap.md) — product entry points now hand typed work to focused internal seams.
- [Compile the Command Node contract once](issues/10-compile-command-node-contract.md) — normalized JSON remains durable while start consumes a typed process-local contract.
- [Centralize file catalog reconciliation](issues/13-centralize-file-catalog-reconciliation.md) — Plan and Checkpoint stores share one safe JSON catalog reader while retaining distinct typed policy.
- [Standardize the test flow](issues/16-standardize-test-flow.md) — repository scripts own unit, component, integration, E2E, sanitizer, coverage, and fuzz verification.
- [Restore sparse Checkpoint boundaries](issues/19-restore-sparse-checkpoint-boundaries.md) — persistence occurs at explicit recovery boundaries rather than every state notification.
- [Make Evidence recovery explicit](issues/15-make-evidence-recovery-explicit.md), [bound durable storage reads](issues/16-bound-storage-reads.md), and [amortize Evidence compaction](issues/21-amortize-evidence-compaction.md) — startup fails closed on committed corruption, all persistent reads are bounded, and retention no longer rewrites the complete ledger per append.
- [Make Artifact deletion truthful](issues/20-make-artifact-delete-outcome-truthful.md) and [reconcile Artifact pairs](issues/17-reconcile-artifact-pairs.md) — metadata is the logical commit marker, deferred cleanup is explicit, and cleanup debt is deterministically observable.
- [Make persistent disk state authoritative](issues/22-make-persistent-disk-authoritative.md) — persistent Plan and Checkpoint reads do not return stale cache entries after managed files change.
- [Formalize project quality and release gates](issues/23-formalize-project-gates.md) — full tests, strict warnings, sanitizers, 90% coverage, fuzzing, dependency verification, release validation, CI, security reporting, and contribution policy are repository-owned.
- [Version persistent storage envelopes](issues/24-version-persistent-envelopes.md) — Plan, Checkpoint, Evidence, and Artifact metadata now require independent version-1 disk envelopes, reject unversioned development data, preserve stable future-version errors, and use exact golden fixtures.

## Current readiness

The storage-hardening slice is complete enough for further implementation. All reproduced blockers are fixed, full product tests pass, ASAN/TSAN/UBSAN pass, production line coverage is 90.06%, 10,000 parser fuzz iterations pass, 50 shuffled storage repetitions pass, strict Clang warnings pass after suppressing only two known Glaze aggregate-initialization diagnostics, vendored dependency hashes pass, and a locally staged static release archive passes the repository release verifier.

The clean Docker release build is currently blocked only by this host being unable to reach either official Minijail source within the bounded retry budget. The installer now uses the pinned revision, both official sources, three attempts per source, and a 45-second per-attempt timeout; CI will exercise the same path on GitHub-hosted runners.

## Next implementation frontier

- [Compile the HTTP Node contract once](issues/11-compile-http-node-contract.md) — complete the typed executor-contract pattern without weakening HTTP-specific invariants.
- [Classify Workflow library headers](issues/18-classify-workflow-library-headers.md) — group the remaining public headers after storage behavior is stable.

## Later operational and measured-performance work

- [Operationalize Artifact cleanup debt](issues/25-operationalize-artifact-cleanup.md) — add explicit dry-run reporting, selected cleanup, and metrics.
- [Reduce Store lock hold time around filesystem I/O](issues/26-reduce-storage-lock-io.md) — optimize only after contention and large-Artifact measurements.
- [Automate vulnerability intelligence and release SBOM](issues/27-automate-vulnerability-intelligence-and-sbom.md) — cover custom vendored commits and release contents after selecting a verified advisory identity model.
- Add backup/restore procedures, disk-usage and quota metrics, Evidence query indexing or rotation, API compatibility generation, and authenticated operator workflows as separately scoped slices.

## Explicit non-goals

- A supported public C++ SDK or ABI contract.
- Multi-process writers for the file-backed Store implementation.
- Replacing build2, Boost.Asio, Glaze, CLI11, Minijail, or the current executor architecture without a separate evidence-driven proposal.
- Recursive or automatic deletion of malformed Artifact paths.
