# Command sandbox

DAGForge command nodes execute through Google Minijail at the revision recorded
in `MINIJAIL_REVISION`. The runtime never executes a command directly.

The fixed profile creates user, PID, mount, network, IPC, UTS, and cgroup
namespaces; enables `no_new_privs`; mounts a private size-limited `/tmp`; applies
Landlock path rules; installs the compiled seccomp denylist; and sets CPU,
address-space, file-size, process-count, and file-descriptor limits.

Install the helper and architecture-specific BPF program with:

```bash
bash scripts/install-minijail.sh
```

The default installation directory is
`~/.local/libexec/dagforge/minijail`. Set `MINIJAIL_INSTALL_DIR` to use another
location and update `[sandbox]` in `system_config.toml` accordingly.
