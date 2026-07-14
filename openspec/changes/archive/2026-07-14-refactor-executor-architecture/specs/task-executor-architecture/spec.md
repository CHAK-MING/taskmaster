## ADDED Requirements

### Requirement: Workflow owns one Task execution interface

The Workflow module SHALL define one Task execution interface used by both Plan
compilation and Runtime execution. Concrete executor implementations SHALL NOT
be defined inside the Workflow module.

#### Scenario: Plan compilation

- **WHEN** a Plan node names an executor
- **THEN** the Plan compiler SHALL delegate configuration compilation through
  the Workflow Task executor registry
- **AND** SHALL NOT include concrete Command, HTTP, sandbox, or transport logic.

#### Scenario: Runtime execution

- **WHEN** a Task Attempt starts or is cancelled
- **THEN** Workflow Runtime SHALL route the operation through the same Task
  executor registry.

### Requirement: Concrete Task executors have explicit names and ownership

Concrete Workflow Task implementations SHALL be named for the Task type they
execute and SHALL live in the executors module.

#### Scenario: Command Task

- **WHEN** the `command` executor is registered
- **THEN** a `CommandTaskExecutor` SHALL own the lower Command runner used for
  all Command Attempts.

#### Scenario: HTTP Task

- **WHEN** the `http` executor is registered
- **THEN** a `HttpTaskExecutor` SHALL own HTTP Attempt state and use the common
  HTTP client for transport.

### Requirement: Sandbox exposes a Command runner, not a Workflow executor

The sandbox module SHALL accept normalized Command requests and SHALL NOT
depend on Workflow Plan, Workflow values, node JSON, or Workflow output ports.

#### Scenario: Minijail execution

- **WHEN** a Command Task has mapped its Workflow inputs into a `CommandSpec`
- **THEN** it SHALL submit that specification through `ICommandRunner`
- **AND** the Minijail implementation SHALL own process supervision and sandbox
  lifecycle.

### Requirement: Executor lifecycle is unified

The Task executor registry SHALL provide a bounded quiesce operation for all
registered executors.

#### Scenario: Application shutdown

- **WHEN** Application shuts down
- **THEN** it SHALL first quiesce Workflow Runtime
- **AND** SHALL quiesce the executor registry
- **AND** SHALL stop Runtime threads only after both operations complete.

#### Scenario: Executor extension

- **WHEN** a new Task executor is added
- **THEN** Application SHALL NOT require a new executor-specific ownership
  member or shutdown branch.

### Requirement: Existing Workflow behavior remains compatible

The architecture refactor SHALL preserve external Workflow and executor
behavior.

#### Scenario: Existing plans

- **WHEN** an existing Command or HTTP Workflow JSON document is validated and
  executed
- **THEN** it SHALL produce the same Plan digest, Run state, Task state,
  outputs, security decisions, and cancellation behavior as before the
  refactor.
