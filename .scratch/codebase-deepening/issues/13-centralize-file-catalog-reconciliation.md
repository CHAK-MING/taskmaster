# 13 — Centralize file catalog reconciliation

**What to build:** Put repeated directory enumeration, filename safety, and durable text loading behind a narrow private primitive while keeping Plan and Checkpoint codecs, identity, sorting, and conflict policy separate.

**Blocked by:** 09 — Isolate Run bootstrap

**Status:** resolved

**Owner:** OpenAI

- [x] Plan and Checkpoint stores no longer duplicate directory scanning rules.
- [x] Catalog filenames are validated as safe storage keys before decode.
- [x] Filename-to-record identity is validated in each owning Store.
- [x] Store-specific decode, validation, sorting, and conflict policy remain in their owning stores.
- [x] Durable file formats and atomic write semantics did not change.
- [x] Existing persistence and recovery behavior remains compatible.

**Evidence:** `json_file_catalog` owns only directory discovery and safe text loading. Full build and all-unit-tests passed 264/264, CLI subprocess scenarios passed, and ASAN/UBSAN storage, catalog, recovery, and Application restore tests passed 24/24.
