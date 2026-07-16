# Agent Guidance Review

## What is still load-bearing

- Read the detailed coding, dependency, abstraction, and project-convention
  documents only as needed.
- Preserve shard ownership and lifecycle ordering.
- Use the project `Result`, JSON, HTTP, Runtime, and storage abstractions.
- Keep product configuration in `dagforge::config`.
- Keep executor implementations out of Workflow and composition in `app`.
- Keep stable library-target interfaces in `include/dagforge` and private shared
  implementation declarations under `src/**/detail`.
- Inspect vendored source before relying on advanced or version-sensitive
  behavior.

## What is misleading or duplicated

### “Public header” is underspecified

`include/dagforge` is the interface of internal build targets and module exports.
It is not automatically a supported external C++ SDK. The guide should state
this directly so product integration remains CLI/HTTP JSON.

### CLI parser state is not product configuration

The rule that all startup parameters belong under `include/dagforge/config`
should apply to durable System Configuration and executor policy, not private
CLI11 option structs or one-command request state.

### Detailed Glaze recipes dominate the hard-rule summary

The current summary repeats identity-meta, `modify`, `rename_key`, DOM, payload,
digest, and Evidence guidance already owned by `dependencies.md` and
`use-dagforge-abstractions.md`. This makes the root guide stale whenever one
serde detail changes.

### Architecture guidance needs a negative rule

The guide says where files belong but not when a new file/module is justified.
It should add the deletion test: do not split by line count; extract only when a
small interface concentrates knowledge that would otherwise spread across
callers.

## Corrected shape

Keep `AGENTS.md` as:

1. Project/product interface statement.
2. Reading routes to detailed documents.
3. A short set of hard architectural, async, JSON, error, and file-placement
   rules.
4. Local Matt-skill tracker/domain pointers.

Do not turn it into a second copy of the detailed standards.
