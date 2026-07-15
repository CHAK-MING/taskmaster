## MODIFIED Requirements

### Requirement: Runtime exposes one authoritative failure

Workflow Runtime SHALL preserve structured failures on snapshots, Evidence,
checkpoints, failure reports, and diagnostic Artifact references without
reducing them to strings or concrete executor types.

#### Scenario: Failure details remain inline

- **WHEN** a structured failure is within the inline diagnostic limit
- **THEN** its details SHALL remain directly available on Attempt, Task, Run,
  Evidence, checkpoint, and failure-report projections.

#### Scenario: Failure details are externalized

- **WHEN** a structured failure exceeds the inline diagnostic limit
- **THEN** the complete details SHALL be retained as an Artifact
- **AND** the same named Artifact reference SHALL propagate through snapshots,
  checkpoints, Evidence, and the failure report.

#### Scenario: Multi-output publication fails partway

- **WHEN** one output in a Task completion violates storage or budget policy
  after earlier outputs were accepted
- **THEN** Runtime SHALL remove every normal output from that completion
- **AND** SHALL expose the problem through the structured failure path.

#### Scenario: Control-plane operation fails

- **WHEN** Plan registration, Run start, repair, persistence, or lookup fails
- **THEN** the HTTP API SHALL return `kind`, stable `code`, `message`,
  `details`, and `artifacts`
- **AND** clients SHALL NOT need to parse message text.
