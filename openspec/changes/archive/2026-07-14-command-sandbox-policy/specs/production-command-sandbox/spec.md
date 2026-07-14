# production-command-sandbox Delta

## MODIFIED Requirements

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

## RENAMED Requirements

- FROM: `### Requirement: Workspaces are private`
- TO: `### Requirement: Workdirs are private`

## ADDED Requirements

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
