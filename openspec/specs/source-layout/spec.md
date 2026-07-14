# source-layout Specification

## Purpose
Define source ownership and dependency boundaries for external configuration,
Workflow abstractions, concrete executors, reusable transport and sandbox
capabilities, and public versus private headers.
## Requirements
### Requirement: External configuration has one owner

All TOML/environment-backed configuration structures SHALL be declared under
`include/dagforge/config` in the `dagforge::config` namespace.

#### Scenario: HTTP executor configuration

- **WHEN** a maintainer locates the HTTP executor enable switch, egress
  allowlist, TLS paths, or resource limits
- **THEN** those declarations are found under `dagforge/config`, not beside the
  executor implementation

#### Scenario: Command sandbox configuration

- **WHEN** a maintainer locates command authorization or Minijail resource
  settings
- **THEN** those external settings are represented by command executor config
  types under `dagforge/config`

### Requirement: Concrete executors are grouped by kind

Command and HTTP Workflow executor code SHALL live under separate
`executors/command` and `executors/http` directories.

#### Scenario: New executor-specific implementation file

- **WHEN** an implementation detail changes only for HTTP execution
- **THEN** it is placed under `executors/http`, not a shared generic detail
  directory

### Requirement: Public headers expose stable seams only

Headers used solely by concrete Minijail or executor implementations SHALL not
be installed under `include/dagforge`.

#### Scenario: Single implementation user

- **WHEN** a declaration is used by one `.cpp`
- **THEN** it is defined in that `.cpp` rather than a private header

#### Scenario: Multiple implementation users

- **WHEN** a declaration is shared by multiple `.cpp` files but is not public
- **THEN** it may be declared in a private `src/**/detail/*.hpp` header

### Requirement: Existing configuration files remain valid

The source-layout refactor SHALL preserve existing TOML section and key names.

#### Scenario: Legacy production configuration

- **WHEN** a configuration uses `[sandbox]` and `[http_executor]`
- **THEN** the loader maps it into the new internal config model without a
  migration requirement

### Requirement: Placement rules are repository policy

The repository SHALL document the config, Workflow, executor, transport,
sandbox, and private-header placement rules in `AGENTS.md` and the detailed
coding guide.

#### Scenario: Future file placement

- **WHEN** a maintainer or coding agent adds or moves a source file
- **THEN** the repository rules require explicit ownership, dependency, and
  public-versus-private placement decisions before implementation
