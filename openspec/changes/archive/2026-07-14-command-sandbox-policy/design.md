# Design

## Naming

`workspace` commonly means a persistent project checkout or Agent session.
DAGForge creates one private writable directory per command Attempt, so the
server-owned parent is named `execution_root` and the child is a `workdir`.

## Program resolution

The server config owns a registry of `{name, path}` entries. A slash-free plan
program is resolved only through this registry. DAGForge never performs PATH
search. Absolute paths continue through canonical trusted-file validation and
must be registered, explicitly allowlisted, or covered by the development
override.

The Plan compiler stores the canonical absolute path in the compiled node
configuration. The low-level runner validates it again before launch.

## Environment

The runner always sets `PATH`, `HOME`, and `TMPDIR` itself. It snapshots only
administrator-listed host variables when the runner is created. Workflow
`env` and `input_env` entries still require the existing server allowlist and
cannot replace reserved variables.

Variables with credential-like names are rejected from host inheritance to
avoid accidental token propagation. Explicit Workflow input mapping remains a
separate administrator-approved path.

## Filesystem and network

The only writable host-backed path is the per-attempt workdir. Standard runtime
files are exposed read-only as required by the executable. Landlock denies
reading host credential contents outside the allowlist; filesystem metadata may
still be observable because Landlock does not mediate `stat`. The Minijail
network namespace remains mandatory and has no Workflow-level override.
