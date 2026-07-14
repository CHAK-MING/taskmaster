## ADDED Requirements

### Requirement: Sandbox accepts only approved known binaries
Command plans SHALL require an exact administrator-configured absolute program
allowlist unless an explicitly documented development override is enabled.

#### Scenario: Default configuration has no program allowlist
- **WHEN** a command plan is compiled with default production configuration
- **THEN** compilation fails as unauthorized

#### Scenario: Approved path is a symlink to a different binary
- **WHEN** canonical resolution does not equal an approved canonical program
- **THEN** compilation or start fails as unauthorized

### Requirement: Sandbox dependencies are preflighted
The command executor SHALL validate Landlock availability, Minijail and seccomp
files, file permissions, workspace root safety, and configured resource limits
before the application starts accepting work.

#### Scenario: Writable seccomp program
- **WHEN** the configured BPF file is writable by group or other
- **THEN** command executor creation fails

### Requirement: Workspaces are private
Each command attempt SHALL use a canonical workspace under the configured root
with owner-only permissions and SHALL reject symlink traversal or temporary-root
placement.

#### Scenario: Pre-created workspace symlink
- **WHEN** an attempt workspace path is a symbolic link
- **THEN** command start fails without following it

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
