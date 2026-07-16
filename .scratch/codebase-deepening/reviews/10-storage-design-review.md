# Workflow storage design review

## Scope

Reviewed Application storage composition and restore ordering, PlanStore, CheckpointStore, EvidenceLedger, in-memory and file Artifact stores, RunValueStore cleanup behavior, durable file primitives, storage codecs, catalog tests, persistence failure tests, and restart tests.

## Findings

### High — Evidence startup errors are silent

`EvidenceLedger` performs file loading in a constructor that cannot return `Result`. Invalid records are skipped, unreadable paths produce an empty ledger, and retention rewrite failure during construction is ignored. Application can therefore start without knowing that audit history was lost or could not be normalized.

### High — Durable reads are unbounded

`load_file()` reads until EOF into a vector. Plans, Checkpoints, Evidence lines, and Artifact metadata have no pre-decode size limit. This is a local-state availability risk and should be fixed at the durable read seam.

### Medium — Evidence retention has quadratic operational behavior

After reaching `max_records`, every append copies the whole vector and atomically rewrites the whole JSONL file. The default limit is 100,000 records, so steady-state write amplification is not acceptable for an active service.

### Medium — Artifact pairs need reconciliation

Metadata-last creation and metadata-first deletion preserve visibility but permit orphan data files after crashes. There is no scan or report for orphan metadata, orphan data, malformed metadata, or digest mismatch.

### Medium — Checkpoint cache and disk conflict policy is implicit

The startup caller uses an empty cache, but `CheckpointStore::list()` is public and can merge cache and disk without detecting same-key disk mutation. Either make catalog loading an explicit startup-only operation or define conflict detection.

### Resolved — Plan digest integrity

A fresh PlanStore now recomputes the canonical Plan digest during decode and rejects drift independently of any prior cache entry.

### Resolved — Repeated JSON catalog scanning

PlanStore and CheckpointStore now share a private `json_file_catalog` seam for directory discovery and safe reads while retaining distinct typed policy.

## Verification

- Build and module graph passed after implementation files were grouped under `planning/`, `runtime/`, and `storage/`.
- Focused storage, catalog, recovery, and Application restore tests passed 24/24.
