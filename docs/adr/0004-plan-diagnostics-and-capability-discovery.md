# ADR 0004: Structured Plan Diagnostics and Capability Discovery

## Status

Accepted.

## Context

DAGForge accepts strict Workflow Plans through the CLI and HTTP control plane. A caller that receives only a broad error category such as `invalid_argument` cannot identify the rejected Node, the responsible executor field, or a language-specific compile failure. This is inadequate for first-party tooling and for a future natural-language Plan authoring client, which must correct a draft through deterministic admission rather than infer failures from logs.

Plan authors also need a server-owned description of the capabilities they may target. Static documentation cannot represent the executor kinds constructed in one process, the current admission ceilings, administrator-registered Command names, HTTP egress policy, or executor protocol versions. Returning the complete system configuration would expose filesystem paths, credentials and unrelated operational settings.

The Workflow Runtime must remain deterministic and must not own model invocation, prompting or dynamic planning policy.

## Decision

### Plan diagnostic contract

Workflow Plan validation, graph compilation, executor configuration compilation and Plan registration use `PlanResult<T>`, whose failure is one `PlanDiagnostic`. The diagnostic contains a normalized DAGForge error kind, stable machine code, human message, absolute JSON Pointer into the submitted Workflow Plan, optional Node identity, optional executor type and a bounded JSON details object.

`ITaskExecutor::compile` returns `ExecutorCompileResult<T>`. Its failure contains the same executor-owned kind, code, message and details, but its path is relative to the Node `config` object. `PlanCompiler` is the only owner of the conversion to a Plan Diagnostic: it prefixes the Node path and attaches Node and executor identity. Registry-level failures such as an unregistered executor point to the Node `executor` field rather than `config`.

Admission returns the first failure only. The deterministic stage order is model validation, server admission policy, graph identifiers and references, cycle detection, executor compilation, compiled-config canonicalization and Plan digest creation. Graph errors therefore take precedence over executor configuration errors in a multi-fault draft.

The HTTP adapter maps caller-controlled Plan faults to 4xx responses. Admission policy denial returns 403; malformed, unsupported, cyclic, duplicate, missing and over-budget Plans return 400. Persistence, digest and internal encoding failures remain 5xx. Runtime operation status mapping is not reused for Plan admission.

`PlanDiagnostic` is a control-plane wire model. `ExecutorCompileFailure` is an internal repository interface and is not serialized independently. `Error` JSON serialization has one owner in the serialization layer.

### Source and compiled configuration

An Execution Plan retains both the user-submitted executor configuration and the executor-compiled configuration. Runtime execution reads only the compiled value. Checkpoints, Plan retrieval and the Plan catalog persist only the submitted Plan. The catalog stores `execution_digest` as the expected compiled execution identity and `source_digest` as the integrity identity of the submitted representation. On restore, DAGForge verifies `source_digest`, recompiles the submitted configuration, recomputes the execution digest and rejects a mismatch. Obsolete development formats are rejected rather than migrated. This prevents compiler-resolved host paths or other implementation details from becoming the public Workflow Plan or durable state.

### Workflow Capability Document

The Workflow Control Plane exposes one versioned Workflow Capability Document. It contains the capability schema version, Workflow Plan schema version, generated Workflow Plan JSON Schema, effective admission configuration, the sorted executor kinds constructed in the process, the subset currently allowed by admission, and one executor-owned description for each constructed kind.

Each executor description contains a stable type, summary, strict configuration JSON Schema, bounded examples and non-secret constraints. Command may disclose registered program names and permitted environment names but not resolved paths or values. HTTP may disclose allowed origins and numeric limits but not credential material, certificate paths or CIDR contents. Transform discloses its JSONata version and input/output protocol.

Executor descriptions are supplied through the existing `ITaskExecutor` seam and aggregated by `ExecutorRegistry`; `WorkflowControlPlane` combines them with admission. API routes and CLI commands are transport adapters and do not duplicate executor protocol knowledge.

`enabled_executors` means executor kinds constructed and registered in this process. `allowed_executors` is the intersection of those kinds with the current admission policy. The full admission object remains present so clients can distinguish an exact allowlist from `allow_unlisted_executors`.

### AI placement

Capability discovery and diagnostics are deterministic control-plane facilities. A future AI orchestration module remains outside `workflow/` and the executor lifecycle. It may consume the Capability Document, submit drafts, inspect Plan Diagnostics and request Repair Runs, but it cannot mutate an active Run or widen server policy.

## Consequences

Every executor must provide compile diagnostics and a capability description through one shared seam. New stable diagnostic codes and capability fields become compatibility inputs and require tests and documentation. Changing the capability envelope or incompatibly changing nested schemas requires a capability schema version review.

Storage and Application startup may project a Plan Diagnostic to the ordinary process error domain only after logging the stable code and path, because those paths are not interactive Plan-admission responses. HTTP Plan registration and repair preserve the complete diagnostic.

Capability discovery deliberately reports fewer facts than `SystemConfig`; it is not a configuration export or administrative introspection endpoint. Model provider configuration and credentials do not belong in this document.
