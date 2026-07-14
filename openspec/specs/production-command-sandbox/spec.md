# production-command-sandbox Specification

## Purpose
Define the fail-closed production boundary for authorized Command execution,
including trusted program resolution, Minijail preflight, private workdirs,
server-controlled environments, bounded output, network isolation, and
complete process-group teardown.
## Requirements
### Requirement: Sandbox accepts only approved known binaries

Command plans SHALL resolve slash-free program names only through an exact
administrator-configured registry and SHALL never search the process PATH.
Absolute paths SHALL remain subject to canonical trusted-file validation and
administrator authorization.

#### Scenario: Registered program name

- **WHEN** a command plan uses `program: "bash"` and the server registers
  `bash` as `/bin/bash`
- **THEN** compilation stores the canonical absolute path and execution launches
  that exact file

#### Scenario: Unregistered program name

- **WHEN** a command plan uses a slash-free name that is absent from the server
  registry
- **THEN** compilation fails as unauthorized without consulting PATH

### Requirement: Sandbox dependencies are preflighted
The command executor SHALL validate Landlock availability, Minijail and seccomp
files, file permissions, execution root safety, and configured resource limits
before the application starts accepting work.

#### Scenario: Writable seccomp program
- **WHEN** the configured BPF file is writable by group or other
- **THEN** command executor creation fails

### Requirement: Output is bounded by termination
Captured stdout, stderr, and pending streamed-line buffers SHALL have configured
bounds. Exceeding a bound SHALL terminate the process group and report resource
exhaustion.

#### Scenario: Infinite output without newlines
- **WHEN** a command emits more than the pending-line or stream byte limit
- **THEN** the process is killed and completion reports resource exhaustion

### Requirement: Teardown kills active sandboxes
Application shutdown SHALL first quiesce Workflow Runtime, prevent new command
starts, and signal every active sandbox process group before Runtime threads
stop.

#### Scenario: Shutdown during a long command
- **WHEN** Application shutdown begins while a command is running
- **THEN** the process group is killed and no sandbox child remains running

### Requirement: Workdirs are private

Each command attempt SHALL use a canonical owner-only workdir under the
configured execution root and SHALL reject symlink traversal or temporary-root
placement.

#### Scenario: Pre-created workdir symlink

- **WHEN** an attempt workdir path is a symbolic link
- **THEN** command start fails without following it

#### Scenario: Legacy workspace configuration

- **WHEN** an existing configuration uses `workspace_root` or
  `retain_workspaces`
- **THEN** the loader maps those keys to the execution-root/workdir model

### Requirement: Sandbox environment is server controlled

The command runner SHALL construct a minimal environment with fixed reserved
variables, a bounded administrator-approved host inheritance list, and
Workflow variables allowed by Command policy.

#### Scenario: Host secret is present

- **WHEN** the DAGForge process contains a credential-like environment variable
  that is not an approved non-sensitive inherited variable
- **THEN** the variable is absent from the sandbox

#### Scenario: Workflow replaces PATH

- **WHEN** a command node attempts to set `PATH`, `HOME`, or `TMPDIR`
- **THEN** compilation or start rejects the command

### Requirement: Command networking remains unavailable

The production Command runner SHALL create a private network namespace for all
commands and SHALL expose no Workflow setting that enables external network
access.

#### Scenario: Workflow requests network access

- **WHEN** a Workflow command configuration contains an unknown network or
  sandbox-policy field
- **THEN** strict node parsing rejects the configuration
