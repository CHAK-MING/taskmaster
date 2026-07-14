# engineering-quality Specification

## ADDED Requirements

### Requirement: Production source coverage is reproducible

The repository SHALL provide one unattended command that builds an
instrumented configuration, runs unit tests and real Workflow scenarios,
merges child-process profiles, and reports coverage for production source
files.

#### Scenario: Coverage gate passes

- **WHEN** the complete coverage command runs on a supported development host
- **THEN** production source line coverage is at least 90%
- **AND** the command exits successfully with a per-file report

#### Scenario: Coverage falls below threshold

- **WHEN** measured production source line coverage is below the configured
  threshold
- **THEN** the coverage command exits unsuccessfully and prints the measured
  percentage

### Requirement: Coverage is scenario driven

Coverage tests SHALL exercise externally observable behavior, lifecycle
contracts, and meaningful failure paths rather than implementation-only calls
whose sole purpose is increasing the percentage.

#### Scenario: Missing shutdown branch

- **WHEN** coverage identifies an untested shutdown or cancellation branch on a
  critical path
- **THEN** the added test drives the public lifecycle operation and verifies the
  terminal state or resource cleanup

### Requirement: Engineering standards are auditable

The repository SHALL provide automated checks for rules that can be evaluated
mechanically and SHALL keep manual critical-path review findings documented in
the completed change.

#### Scenario: A mechanical rule is violated

- **WHEN** a source change introduces a forbidden dependency, public-header
  guard violation, raw Asio await, or other encoded standards violation
- **THEN** the conventions check fails with the responsible file

### Requirement: Benchmarks represent production questions

Each maintained benchmark SHALL state the production behavior it measures,
exclude one-time setup from the timed region, use representative workload
sizes, and expose throughput or latency in meaningful units.

#### Scenario: Runtime dispatch benchmark runs

- **WHEN** the benchmark suite measures a controlled batch of same-shard or
  cross-shard dispatches
- **THEN** it reports operations per second and latency without recreating the
  Runtime inside each measured operation

#### Scenario: Repeated measurement is requested

- **WHEN** the benchmark runner is invoked for a release measurement
- **THEN** it performs warmup and repeated samples and records median, tail, and
  standard-deviation data in a machine-readable result
