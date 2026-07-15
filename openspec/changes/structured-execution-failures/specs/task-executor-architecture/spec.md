## MODIFIED Requirements

### Requirement: Workflow owns one Task execution interface

The Workflow module SHALL define one Task execution interface used by both Plan
compilation and Runtime execution. Concrete executor implementations SHALL NOT
be defined inside the Workflow module. Asynchronous Task completion SHALL use
the Workflow-owned structured execution result and SHALL NOT expose concrete
executor or transport types.

#### Scenario: Plan compilation

- **WHEN** a Plan node names an executor
- **THEN** the Plan compiler SHALL delegate configuration compilation through
  the Workflow Task executor registry
- **AND** SHALL NOT include concrete Command, HTTP, sandbox, or transport logic.

#### Scenario: Runtime execution

- **WHEN** a Task Attempt starts, completes, or is cancelled
- **THEN** Workflow Runtime SHALL route the operation through the same Task
  executor registry
- **AND** completion SHALL carry either declared outputs or one structured
  execution failure.
