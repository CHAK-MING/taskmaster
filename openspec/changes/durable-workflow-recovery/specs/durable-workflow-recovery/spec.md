## ADDED Requirements

### Requirement: Plans survive application restart

The control plane SHALL persist every canonical accepted Plan independently
from Run checkpoints.

#### Scenario: Registered Plan has no Run

- **WHEN** a Plan is registered and the application restarts before any Run is
  created
- **THEN** the Plan SHALL remain addressable by Plan ID
- **AND** SHALL remain the latest Plan for its Workflow.

#### Scenario: Existing Plan ID has different content

- **WHEN** storage already contains a Plan ID with a different canonical digest
- **THEN** the new Plan SHALL be rejected
- **AND** the stored Plan SHALL NOT be overwritten.

### Requirement: Accepted Runs have a durable initial record

Runtime SHALL persist the Run identity, immutable Plan, trigger, lineage, and
initial Task states before reporting a newly created Run as accepted.

#### Scenario: Initial persistence fails

- **WHEN** the initial checkpoint cannot be stored
- **THEN** `start` SHALL fail
- **AND** SHALL NOT publish the idempotency key or dispatch a Task.

### Requirement: Idempotency keys bind request identity

Runtime SHALL bind a retained idempotency key to the operation, Workflow,
Plan, and repair parent identity that created the Run.

#### Scenario: Same key is reused for a different Plan

- **WHEN** a caller reuses a retained key with a different Workflow, Plan, or
  repair parent
- **THEN** Runtime SHALL reject the request as a conflict
- **AND** SHALL NOT return an unrelated Run ID.

#### Scenario: Conflicting checkpoints carry one key

- **WHEN** startup encounters two different Run records with the same
  idempotency key
- **THEN** recovery SHALL fail closed rather than selecting one implicitly.

### Requirement: Interrupted Runs resume from retained state

Runtime SHALL recover a non-terminal checkpoint without rerunning completed
Tasks.

#### Scenario: Process stops during one fan-out branch

- **WHEN** sibling Tasks have succeeded and one Attempt was active at process
  loss
- **THEN** the succeeded Tasks and values SHALL be retained
- **AND** the interrupted Attempt SHALL be recorded as
  `runtime_restarted`
- **AND** only unfinished work SHALL be dispatched after startup.

#### Scenario: Paused Run is restored

- **WHEN** a paused Run is loaded after restart
- **THEN** it SHALL remain paused until an explicit resume request.

### Requirement: Checkpoints are revalidated before admission

Runtime SHALL reject a checkpoint whose Plan digest, output ownership, output
budget, published outputs, or referenced Artifact content is inconsistent.

#### Scenario: Successful Run is missing a published output

- **WHEN** a checkpoint claims `succeeded` but omits a required published
  output
- **THEN** checkpoint loading SHALL reject it.

#### Scenario: Referenced Artifact is missing or mismatched

- **WHEN** a retained value references an absent Artifact or metadata that
  differs from the stored Artifact
- **THEN** recovery SHALL fail closed before the Run is admitted.

### Requirement: Retention does not create restart resurrection

Runtime SHALL remove a completed Run from memory and idempotency state only
after its durable checkpoint was deleted or was already absent.

#### Scenario: Checkpoint deletion fails

- **WHEN** retention cannot delete the oldest checkpoint
- **THEN** the Run SHALL remain retained in memory
- **AND** its idempotency binding SHALL remain authoritative.

### Requirement: Repair creates immutable lineage

A Plan repair SHALL create a child Run rather than mutate the parent Run.

#### Scenario: Revised Plan is submitted

- **WHEN** a client repairs a failed Run with a valid revised Plan
- **THEN** the child snapshot SHALL contain the parent Run ID, parent Plan ID,
  and incremented repair revision
- **AND** the parent snapshot and Evidence SHALL remain unchanged.

### Requirement: Successful-node reuse is conservative

Runtime SHALL reuse parent outputs only when the successful node and all
transitive execution inputs are unchanged.

#### Scenario: One branch changes in a fan-out/fan-in DAG

- **WHEN** one failed branch node is revised while successful sibling branches
  remain unchanged
- **THEN** unchanged successful siblings SHALL be reused
- **AND** the revised node and every dependent node SHALL run again
- **AND** reused executors SHALL NOT be invoked.

#### Scenario: Incoming condition changes

- **WHEN** a node's incoming conditional edge changes
- **THEN** that node SHALL be invalidated even when its executor config is
  unchanged.

### Requirement: Recovery remains executor-neutral

Workflow recovery SHALL operate only on Plan contracts, snapshots, values,
structured failures, and Artifact references.

#### Scenario: New executor kind is added

- **WHEN** a WASM, model, or MCP executor implements `ITaskExecutor`
- **THEN** restart recovery and Repair Run planning SHALL require no
  executor-specific Runtime branch.
