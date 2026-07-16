# Command sandbox

DAGForge command nodes execute through Google Minijail at the revision recorded
in `MINIJAIL_REVISION`. The runtime never executes a command directly.

## Security boundary

This is a **known-binary containment boundary**. It is intended for
administrator-installed, exact-path allowlisted programs processing untrusted
arguments and inputs. It is not a boundary for malicious binaries,
attacker-controlled shared libraries, or executables uploaded by Workflow
authors. Those workloads require a stronger isolation class such as a userspace
kernel or microVM.

The fixed profile creates user, PID, mount, network, IPC, UTS, and cgroup
namespaces; enables `no_new_privs`; mounts a private size-limited `/tmp`; applies
Landlock path rules; installs the compiled seccomp denylist; and sets CPU,
address-space, file-size, process-count, and file-descriptor limits.

DAGForge additionally:

- canonicalizes and checks program allowlists at Plan compile and process start;
- rejects group/other-writable Minijail and BPF files by default;
- rejects symlinked or temporary-directory workspace roots and creates
  owner-only per-Attempt directories;
- kills the process group when stdout, stderr, or an unterminated streamed line
  exceeds its configured bound;
- kills and waits for active process groups before Runtime shutdown.

Install the helper and architecture-specific BPF program with:

```bash
bash scripts/install-minijail.sh
```

The default installation directory is
`~/.local/libexec/dagforge/minijail`. Set `MINIJAIL_INSTALL_DIR` to use another
location and update `executors.command.minijail` in `system_config.json`
accordingly.

Production configuration keeps `allow_unlisted_programs` and
`allow_unlisted_environment` false and `require_trusted_files` true. The
permissive switches are development overrides and materially weaken the
boundary.
