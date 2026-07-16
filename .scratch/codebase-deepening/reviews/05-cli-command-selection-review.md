# CLI Command Selection Review

Reviewed against
[`07-pure-cli-command-selection.md`](../issues/07-pure-cli-command-selection.md)
and the repository standards in `AGENTS.md` and `docs/ai/`.

## Standards

No blocking findings.

- The change uses CLI11 for parsing rather than introducing another parser.
- The private declarations now live under `src/dagforge/app/cli/detail/` and are
  not promoted into the internal library interface.
- `CommandSelection` provides a real seam: deleting it would spread the
  parse-before-execute invariant back across every callback.
- The typed variant contains exactly the supported executable command models;
  it is not a speculative plugin abstraction.
- API endpoint parsing uses Boost.URL's distinct host-address and authority
  representations instead of reparsing strings.
- One unused standard header found during review was removed.

Judgement call: `command_line.cpp` remains long because it intentionally keeps
the complete command inventory and help contract visible. Splitting each
subcommand into a file would reduce locality and create shallow modules, so no
file-size refactor is recommended here.

## Spec

No missing or extra behavior found.

- CLI11 callbacks now select typed command data and do not execute server,
  Workflow, or HTTP side effects.
- Parsing rejects any invocation that selects more than one executable leaf
  before dispatch.
- Execution occurs exactly once after parsing succeeds.
- IPv6 resolver input and HTTP authority are handled separately.
- CLI declarations are private under `detail/`.
- Help, version, positional arguments, environment defaults, aliases, semantic
  API commands, raw requests, local Run, and Service behavior passed the focused
  scenario suite.

Summary: Standards 0 findings; Spec 0 findings.
