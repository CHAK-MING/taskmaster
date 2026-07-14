## MODIFIED Requirements

### Requirement: HTTP executor registration and isolation

DAGForge SHALL register a concrete `HttpTaskExecutor` from the executors
module when server-owned HTTP executor configuration is enabled.
`WorkflowRuntime` and the Workflow module SHALL remain unaware of HTTP methods,
URLs, headers, transport state, and response semantics. Reusable DNS/TCP/TLS/
HTTP transport SHALL remain in the HTTP module, while node schema, egress
policy, Attempt lifecycle, and Workflow output mapping SHALL be owned by the
HTTP Task executor.

#### Scenario: HTTP plan compiles through the executor registry

- **WHEN** a Workflow node selects executor `http` with valid configuration
- **THEN** the Plan Compiler delegates validation through the Task executor
  registry to `HttpTaskExecutor`
- **AND** produces an immutable executor-owned configuration without adding
  HTTP logic to Workflow.

#### Scenario: HTTP executor is disabled

- **WHEN** server configuration disables the HTTP executor
- **THEN** an HTTP Workflow plan is rejected as an unsupported executor.

#### Scenario: HTTP implementation placement

- **WHEN** HTTP Task execution is built
- **THEN** the concrete executor SHALL live under `dagforge/executors`
- **AND** the common HTTP client SHALL remain independent of Workflow values,
  Task state, and node configuration.
