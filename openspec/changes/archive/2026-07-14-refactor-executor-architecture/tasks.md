## 1. Contracts and placement

- [x] 1.1 Introduce the Workflow Task executor contract header and preserve the
  registry compile/start/cancel behavior.
- [x] 1.2 Introduce module-owned Sandbox and HTTP executor configuration types.
- [x] 1.3 Move Command process execution to `sandbox` and rename the seam to
  `ICommandRunner`.

## 2. Concrete Task executors

- [x] 2.1 Implement `CommandTaskExecutor` under `executors` and make it own the
  Command runner.
- [x] 2.2 Implement `HttpTaskExecutor` under `executors`.
- [x] 2.3 Extract private HTTP node configuration and egress policy modules.
- [x] 2.4 Remove the old Workflow adapter files and names.

## 3. Lifecycle and composition

- [x] 3.1 Add bounded executor quiesce to the Task executor interface and
  registry.
- [x] 3.2 Remove Command-specific ownership and shutdown logic from
  Application.
- [x] 3.3 Preserve shutdown order: API, Workflow Runtime, executor registry,
  Runtime.

## 4. Build, modules, and documentation

- [x] 4.1 Update build2 source lists, module exports, architecture checks, and
  include paths.
- [x] 4.2 Update AGENTS documentation, README architecture, and changelog.
- [x] 4.3 Remove all old adapter/executor names and reverse dependencies.

## 5. Verification

- [x] 5.1 Pass focused Task executor, Command runner, Application lifecycle,
  and Plan digest tests.
- [x] 5.2 Pass all unit tests and 24 real Workflow JSON executions.
- [x] 5.3 Pass ASAN/LSAN, UBSAN, focused TSAN/runtime audit, OpenSpec, module
  graph, convention, and diff checks.
