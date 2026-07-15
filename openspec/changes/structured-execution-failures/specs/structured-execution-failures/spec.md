## ADDED Requirements

### Requirement: Task completion returns a structured failure

The Workflow Task execution seam SHALL complete with either declared outputs
or one `ExecutionFailure` containing a normalized error kind, stable machine
code, human message, and JSON details.

#### Scenario: Executor completes unsuccessfully

- **WHEN** a concrete executor cannot complete an Attempt successfully
- **THEN** it SHALL return one structured failure through the normal completion
  callback
- **AND** SHALL NOT require an executor-specific Runtime or Evidence side
  channel.

#### Scenario: Executor start is rejected

- **WHEN** registry submission fails before asynchronous execution starts
- **THEN** the async Task adapter SHALL convert that rejection into a structured
  `executor_start_failed` completion
- **AND** SHALL preserve the normalized project error kind and original cause
  metadata.

### Requirement: Command failures preserve bounded diagnostics

The Command Task executor SHALL retain the bounded process diagnostics already
returned by the Command runner.

#### Scenario: Command exits non-zero

- **WHEN** a sandboxed command exits with a non-zero status
- **THEN** the failure code SHALL be `command_exit_nonzero`
- **AND** details SHALL include the exit code, stdout, stderr, and stream
  truncation indicators available from the runner.

#### Scenario: Command times out or exhausts a resource

- **WHEN** the runner reports timeout or resource exhaustion
- **THEN** the failure kind and machine code SHALL distinguish those conditions
- **AND** details SHALL retain the available process diagnostics.

### Requirement: HTTP failures preserve response and transport context

The HTTP Task executor SHALL retain bounded response data for rejected HTTP
statuses and structured cause data for transport and protocol failures.

#### Scenario: HTTP status is rejected

- **WHEN** a response status is outside the node's accepted set
- **THEN** the failure code SHALL be `http_status_rejected`
- **AND** details SHALL include status, headers, and a UTF-8 response body when
  one is available.
- **AND** credential-bearing response header values SHALL be redacted while
  preserving the header name and an explicit redaction marker.

#### Scenario: HTTP transport fails

- **WHEN** DNS, connection, TLS, write, first-byte, read, timeout, or
  cancellation prevents a response
- **THEN** completion SHALL contain a structured HTTP failure
- **AND** details SHALL preserve the original error category, value, and
  message.

### Requirement: Runtime exposes one authoritative failure

Workflow Runtime SHALL preserve structured failures on snapshots, Evidence,
and checkpoints without reducing them to strings.

#### Scenario: Attempt fails and retries

- **WHEN** an Attempt fails and retry policy schedules another Attempt
- **THEN** the Attempt and Task snapshots SHALL expose the structured failure
- **AND** retry classification SHALL use the normalized error kind.

#### Scenario: Run fails

- **WHEN** a Task failure terminates a Run
- **THEN** the Run snapshot SHALL expose the same structured failure
- **AND** Task-failed and Run-failed Evidence SHALL contain the same failure
  JSON projection.

#### Scenario: Checkpoint reload

- **WHEN** a failed or retrying snapshot is persisted and loaded
- **THEN** all structured failure fields SHALL round-trip unchanged.

#### Scenario: Checkpoint contains an invalid failure

- **WHEN** persisted failure data contains an invalid error kind, empty machine
  code, empty message, or non-object details
- **THEN** checkpoint loading SHALL fail closed with a parse error
- **AND** the invalid failure SHALL NOT enter Runtime state.

### Requirement: Control-plane errors are machine consumable

Workflow run and Evidence endpoints SHALL expose stable textual failure data
for automated clients.

#### Scenario: Failed run is queried

- **WHEN** a client queries a failed workflow run
- **THEN** run, Task, and Attempt failure objects SHALL contain `kind`, `code`,
  `message`, and `details`
- **AND** legacy string-only `error` and `last_error` fields SHALL not be
  emitted.

#### Scenario: Evidence is queried

- **WHEN** a client queries workflow Evidence
- **THEN** Evidence types SHALL be emitted as stable strings
- **AND** failure Evidence metadata SHALL contain the structured failure.
