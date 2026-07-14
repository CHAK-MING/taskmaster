# Proposal: Organize configuration and executor layout

## Why

The current tree mixes deployment configuration, Workflow executor
implementations, sandbox policy, and private implementation helpers. That makes
directory placement unreliable: `HttpExecutorConfig` lives beside executor
code even though it is a system configuration contract, while command and HTTP
private files share one generic `executors/detail` directory.

## What changes

- Move all external system configuration types under `dagforge/config` and the
  `dagforge::config` namespace.
- Group concrete Workflow executors by executor kind:
  `executors/command` and `executors/http`.
- Keep only stable seams in `include/dagforge`; move concrete Minijail policy,
  launch, and process-management declarations to private `src` headers.
- Eliminate private headers that are only used by one implementation file by
  moving their declarations into that `.cpp`.
- Preserve the existing TOML section names and runtime behavior.
- Record the placement rules in `AGENTS.md` and the detailed coding guide.

## Non-goals

- No Workflow JSON schema changes.
- No HTTP, retry, cancellation, sandbox, or persistence behavior changes.
- No new executor type or plugin ABI.
- No change to the existing TOML keys in this refactor.
