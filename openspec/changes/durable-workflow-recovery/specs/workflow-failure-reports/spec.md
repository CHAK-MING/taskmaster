## ADDED Requirements

### Requirement: Every Run failure is available through one interface

Runtime SHALL expose a dedicated failure report containing Run, Task, and
Attempt failures for active, completed, restored, and repaired Runs.

#### Scenario: Client requests a failed Run report

- **WHEN** a client requests the Run failure report
- **THEN** it SHALL receive the Run failure and every Task and Attempt failure
- **AND** each entry SHALL use the same `ExecutionFailure` contract.

### Requirement: Large diagnostics remain retrievable

Structured failure details SHALL remain bounded inline while preserving the
complete diagnostic payload as an Artifact when necessary.

#### Scenario: Failure details exceed the inline threshold

- **WHEN** serialized failure details exceed the configured threshold
- **THEN** Runtime SHALL store the complete JSON in the Artifact store
- **AND** the failure SHALL contain a named Artifact reference
- **AND** the dedicated failure report SHALL return that reference.

### Requirement: Failure reporting is decoupled from executors

The failure-report interface SHALL NOT expose concrete Command, HTTP, WASM,
model, MCP, sandbox, or transport types.

#### Scenario: Executor-specific detail fields change

- **WHEN** one executor changes the JSON fields inside `details`
- **THEN** the failure-report interface and persistence schema SHALL remain
  unchanged.
