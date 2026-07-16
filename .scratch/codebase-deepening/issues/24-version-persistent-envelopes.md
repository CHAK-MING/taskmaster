# 24 — Version persistent storage envelopes

**What to build:** Introduce an explicit storage-envelope version independent of Workflow Plan schema version for Plan, Checkpoint, Evidence, and Artifact metadata, with a strict current-format policy and contract tests.

**Status:** resolved

**Owner:** OpenAI

**Priority:** next-storage-design

**Reason this is not a current release blocker:** The project is still pre-release, so persisted development data is disposable. Establish one explicit current format before adding another persistent representation; do not introduce migration code before a compatibility promise exists.

- [x] Writers emit an explicit envelope version.
- [x] Readers reject unversioned payloads instead of carrying a legacy path.
- [x] Unknown future versions fail with the stable `Error::Unsupported` compatibility error.
- [x] Golden files define the exact current format for every persistent representation.

## Answer

Plan, Checkpoint, Evidence, and Artifact metadata now share an internal version-1 envelope codec while retaining their typed validation and Store-specific policy. Writers emit `{format, version, payload}` and readers accept only that explicit current envelope.

There is deliberately no backward-compatibility window while the project is pre-release. Unversioned payloads, mismatched format identifiers, explicit version 0, malformed envelopes, and invalid payload models return `Error::ParseError`; future versions return `Error::Unsupported`. Envelope versions remain independent of Workflow Plan `schema_version`.

One exact envelope golden file covers each persistent representation. The contract test requires exact version-1 writer bytes, rejects the corresponding unversioned payload, and verifies stable future-version and corruption errors.

Verification passed with 54 unit tests, 228 component tests, 19 Minijail integration tests, all CLI scenarios, all real Workflow scenarios, 24 validated Workflow JSON plans, the complete library under strict Clang warnings-as-errors, and the focused persistence regressions under ASAN, TSAN, and UBSAN.
