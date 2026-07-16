# Workflow storage design

## Purpose

This document records the current storage authority model, crash semantics, known weaknesses, and the next implementation slices. It is not a proposal for a generic persistence framework.

## Directory ownership

- `src/dagforge/workflow/planning/` owns Plan validation, compilation, loading, and the Workflow Control Plane.
- `src/dagforge/workflow/runtime/` owns executor dispatch, Run bootstrap, Run/Task/Attempt execution, values, and execution failures.
- `src/dagforge/workflow/storage/` owns Plan, Checkpoint, Evidence, and Artifact persistence.
- `src/dagforge/workflow/storage/detail/durable_file.*` owns atomic replace, durable append, durable unlink, directory creation, fsync, regular-file checks, and no-follow reads.
- `src/dagforge/workflow/storage/detail/json_file_catalog.*` owns discovery of safe `.json` catalog files and durable text loading only.
- Store-specific codecs, identity checks, conflict rules, retention, and ordering remain with the owning Store.

## Authority model

- A persisted Plan file is the restart authority for one immutable `WorkflowPlanId`. Its stored digest must equal a digest recomputed from the stored Plan before the catalog accepts it.
- A persisted Checkpoint file is the restart authority for one mutable `WorkflowRunId`. Runtime memory is authoritative while the process is running, and a successful save writes the durable file before replacing the in-memory cache entry.
- Evidence is an ordered bounded log. The current in-memory vector and JSONL file are intended to advance together, but startup load failures are not yet represented explicitly.
- Artifact metadata is the visibility marker for a content blob. Reads verify metadata identity, byte count, and content digest before returning an Artifact.
- Cross-store recovery is intentionally compensating rather than transactional: Plans are restored first, then Checkpoints; a Checkpoint can reconstruct and persist a missing Plan catalog entry because the Checkpoint embeds its source Plan.

## Persistent format policy

- Plan, Checkpoint, Evidence, and Artifact metadata writers emit a three-field JSON envelope: `format`, `version`, and `payload`.
- The version-1 format identifiers are `dagforge.stored-plan`, `dagforge.checkpoint`, `dagforge.evidence`, and `dagforge.artifact-metadata`.
- Readers accept only the explicit current envelope. Unversioned development data is rejected rather than routed through a migration path.
- A version greater than the current writer version returns `Error::Unsupported`, so an older binary can distinguish a future format from corruption.
- A mismatched format identifier, explicit version 0, missing or malformed envelope fields, and invalid payload models return `Error::ParseError` after the normal bounded-read checks.
- Envelope versioning is independent of Workflow Plan `schema_version`; changing one does not imply changing the other.
- `tests/fixtures/storage/` contains one exact current-envelope golden file for every persistent representation. The contract test requires the writer to reproduce those bytes and verifies that an extracted unversioned payload is rejected.
- Until DAGForge publishes a persistent-data compatibility promise, a format change replaces the current development format and requires deleting stale local data, not adding a compatibility layer.

## Crash and failure semantics

- Plan and Checkpoint replacement is single-file atomic through temporary-file write, file fsync, rename, and parent-directory fsync.
- Evidence append fsyncs the log and rolls back the file length when an append or fsync fails.
- Artifact creation writes data first and metadata second. A crash between those writes can leave an invisible orphan data file, but cannot expose an Artifact whose metadata was never committed.
- Artifact deletion removes metadata before data. A crash can leave an invisible orphan data file; a later reconciliation pass is required to reclaim it.
- Store mutations update memory only after durable success. Checkpoint deletion deliberately retains the cache entry when durable deletion fails.
- Startup catalog corruption currently fails Plan or Checkpoint restoration as a whole. There is no quarantine mode.

## Correctness findings

1. Plan Catalog digest drift was only detected when a same-process cache entry disagreed with disk. The Store now recomputes the digest from every decoded persisted Plan, so a fresh process rejects tampered digest content.
2. Plan and Checkpoint catalog enumeration duplicated filesystem behavior and used throwing filesystem queries. The new private JSON catalog reader uses error-code APIs and centralizes missing-directory, non-directory, regular-file, extension, key-safety, and read behavior.
3. Evidence construction silently ignores unreadable or malformed records and cannot return an initialization error to `Application`. This can start the service with an incomplete audit history.
4. Evidence retention copies and rewrites the entire retained log for every append once capacity is reached. With the default 100,000 records this is unbounded write amplification.
5. Artifact storage has no startup reconciliation for orphan `.bin` or `.json` files. Current ordering preserves visibility correctness but can leak files after crashes or failed deletion.
6. Plan, Checkpoint, Evidence, and Artifact metadata reads have no configured byte limit before allocation and decode. A corrupt local file can cause excessive memory use during startup or read.
7. Checkpoint `list()` merges a cache snapshot with disk but does not detect a same-key disk mutation when the Run already exists in cache. The intended caller is startup restoration, where the cache is empty, but that restriction is not encoded in the interface.
8. PlanStore, CheckpointStore, and EvidenceLedger select memory-only versus durable behavior through an empty path. This is compact but makes construction mode implicit and should not spread to new stores.

## Design decisions

- Keep Plan and Checkpoint as separate typed Stores; they have different mutability and conflict semantics.
- Keep Artifact content storage separate from JSON catalogs; binary lifecycle and verification are materially different.
- Keep Evidence as a log rather than forcing it into the JSON catalog abstraction.
- Do not add a generic repository, ORM, transaction manager, or mirrored persistence DTO layer.
- Treat malformed persisted state as a startup correctness failure unless a narrowly documented crash-tail rule applies.
- Add storage limits at the durable read boundary rather than after full allocation.
- Make recovery reports explicit before adding automatic destructive cleanup.

## Next slices

1. Make Evidence startup recovery return a Result, distinguish a tolerable truncated final record from interior corruption, and remove silent rewrite failures.
2. Add bounded reads for catalog JSON, Evidence records, Artifact metadata, and Checkpoints, with limits owned by Storage Configuration.
3. Add Artifact pair reconciliation that reports complete, orphan-data, orphan-metadata, and corrupt pairs before optional cleanup.
4. Clarify Checkpoint catalog loading as an explicit startup operation or add same-key disk/cache conflict detection.
5. Revisit Evidence retention storage after correctness is explicit; likely use segmented files or bounded rotation rather than rewriting 100,000 records per append.
