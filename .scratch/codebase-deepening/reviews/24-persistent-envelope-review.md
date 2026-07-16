# Persistent storage envelope review

## Scope

This review covers the version-1 disk envelope for Stored Plans, Checkpoints, Evidence records, and Artifact metadata, including strict current-format enforcement, error classification, exact golden files, compiler portability, project conventions, and ticket-24 acceptance criteria.

## Standards review

- The change remains inside the private Workflow Storage implementation and does not create a public C++ persistence API or alter the CLI, HTTP JSON, or Workflow Plan product contracts.
- One internal codec owns the repeated `{format, version, payload}` knowledge. Plan, Checkpoint, Evidence, and Artifact Stores retain their distinct validation, identity, mutability, retention, and crash semantics.
- Serialization continues through the repository Glaze wrappers and returns `Result<T>` with project error codes.
- No legacy branch or in-place migration exists. Normal successful writes produce the current version-1 envelope through the existing durable Store paths, and unversioned payloads fail closed.
- Future-version detection is distinct from corruption: version values greater than the current writer return `Error::Unsupported`; unversioned, malformed, or contradictory current representations return `Error::ParseError`.
- The private reflected envelope types have named namespace linkage, so both GCC and Clang/Glaze builds succeed. This was verified after the sanitizer build exposed the anonymous-namespace portability defect.
- No generic repository, transaction manager, mirrored DTO hierarchy, or second persistence framework was introduced.

**Standards decision:** approved. The implementation deepens one private contract seam, preserves Store authority boundaries, and remains locally reasoned.

## Spec review

- Readers require the current envelope: the payload extracted from each of the four golden envelopes is rejected as unversioned input.
- Writers emit explicit versions: each decoded current fixture is re-encoded and compared byte-for-byte with its version-1 envelope fixture.
- Unknown future versions fail stably: version 2 returns `Error::Unsupported` for all four decoders.
- The pre-release format policy is defined: only explicit version 1 is supported; unversioned payloads, format mismatches, and explicit version 0 are parse errors.
- Storage envelope versioning is documented as independent of Workflow Plan `schema_version`.
- Existing integrity checks remain active, including Stored Plan digest recomputation and rich Checkpoint/Evidence model validation.

**Spec decision:** approved. Every ticket-24 acceptance criterion is implemented, documented, and directly exercised.

## Verification evidence

- Focused component regressions: Stored Plan corruption/digest drift, strict golden envelope enforcement, and rich Checkpoint/Evidence round-trip all pass.
- Quick gate: 54 unit tests and 228 component tests pass.
- Full product gate: 54 unit, 228 component, and 19 integration tests pass, together with every CLI and real Workflow scenario and 24 validated Workflow JSON plans.
- Strict compiler gate: the complete `src/lib{dagforge}` target builds with Clang under `-Wall -Wextra -Wpedantic -Werror`.
- Sanitizer gate: the focused persistence regressions pass under ASAN, TSAN, and UBSAN.
- Fixture validation: all four golden files parse as JSON and contain the expected current format, version, and payload fields.
- Source hygiene: `git diff --check` passes for the slice; the new codec is clang-formatted, and only the new test ranges were format-checked to avoid rewriting unrelated baseline formatting.

## Residual format rule

Until a persistent-data compatibility promise exists, a future format change replaces the development format and updates the golden files without adding a migration path. Exact-byte fixtures intentionally make accidental disk-format drift visible in review.
