# 07 — Make CLI parsing select one command

**What to build:** Make every CLI invocation parse and validate without side
effects, select exactly one typed command, and execute it once after CLI11 has
finished. Keep the current command surface and product behavior.

**Blocked by:** 04 — Use vendored libraries deliberately; 06 — Choose the first
refactor sequence

**Status:** resolved

- [x] CLI11 callbacks never start a server, Run, or HTTP request.
- [x] A command-line containing multiple sibling leaf commands is rejected
      before any command executes.
- [x] IPv6 endpoints use an unbracketed resolver host and a bracketed HTTP
      authority.
- [x] CLI-only declarations are private under the CLI `detail` directory.
- [x] Existing help, version, positional arguments, environment defaults,
      aliases, semantic API commands, and raw request behavior remain intact.
- [x] Focused CLI scenarios and a clean build pass.

## Answer

CLI11 callbacks now only copy the selected leaf into a typed `ParsedCommand`.
After parsing and validation finish, DAGForge requires exactly one selected
command and executes it once. This prevents partial side effects when CLI11
accepts multiple sibling subcommands during traversal.

The API endpoint parser now uses Boost.URL `host_address()` for resolver input
and encoded authority for the HTTP Host header, fixing IPv6 loopback behavior.
CLI-only shared declarations moved under `src/dagforge/app/cli/detail/`.

Verification:

- incremental build and convention/module checks passed;
- multiple sibling commands return CLI11 validation code 105 without issuing a
  network request;
- help and version paths passed;
- all CLI scenarios passed, including IPv6, local Run, long-lived Service, Plan,
  Run, Artifact, and raw API commands.
