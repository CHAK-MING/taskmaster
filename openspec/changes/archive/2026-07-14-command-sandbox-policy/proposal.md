# Command sandbox policy

## Why

The Command executor already runs every process inside Minijail, but its public
configuration still exposes implementation-era names such as `workspace_root`
and requires Workflow plans to use absolute program paths. Agent-style command
execution needs a simpler contract without giving Workflow authors control of
network, mounts, or sandbox modes.

## What changes

- Rename the server-owned execution directory model to `execution_root` and
  per-attempt `workdir` while preserving legacy TOML keys.
- Add an administrator-owned program registry so plans may use stable names
  such as `bash` without PATH lookup.
- Add an explicit allowlist for the small set of host environment variables
  copied into the sandbox.
- Keep command networking disabled and sensitive host file contents outside the
  filesystem allowlist unreadable.

## What does not change

- There remains one Minijail command backend.
- Workflow JSON cannot configure mounts, network, inheritance policy, or
  sandbox strength.
- Exact absolute-path allowlists remain supported for compatibility.
