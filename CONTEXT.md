# DAGForge Workflow Runtime

DAGForge accepts Workflow Plans, turns them into immutable Execution Plans, and
runs them with explicit lifecycle, evidence, and recovery semantics.

## Language

**Workflow Plan**:
A user-authored declaration of a workflow graph, its dataflow, execution policy,
and published outputs.
_Avoid_: DAG definition, job definition

**Execution Plan**:
An accepted, immutable version of a Workflow Plan that is eligible to start
Runs.
_Avoid_: compiled DAG, runtime plan

**Node**:
One declared step in a Workflow Plan. A Node describes work; it is not itself a
runtime execution record.
_Avoid_: task definition, job step

**Trigger**:
The event context that requests a Run, including its source, event type,
principal, idempotency identity, and optional payload.
_Avoid_: request metadata, invocation

**Run**:
One execution of an Execution Plan created from a Trigger.
_Avoid_: workflow instance, job

**Task**:
The Run-scoped runtime record for one Node.
_Avoid_: node state, step instance

**Attempt**:
One concrete try to execute a Task. Retries create new Attempts rather than
reusing an existing Attempt.
_Avoid_: retry execution, process attempt

**Repair Run**:
A new Run derived from a terminal parent Run that reuses still-valid results
and reruns invalidated Tasks.
_Avoid_: resumed run, mutated run

**Checkpoint**:
A durable snapshot sufficient to restore a Run and its retained values without
inventing missing runtime history.
_Avoid_: cache, save point

**Evidence**:
An ordered audit record describing facts observed during a Run.
_Avoid_: log entry, event log

**Artifact**:
A content-addressed retained value stored outside an inline Workflow Value.
_Avoid_: blob, attachment

**Workflow Control Plane**:
The product surface that accepts Workflow Plans and selects Execution Plans for
Runs. Its supported external interfaces are HTTP JSON and the CLI.
_Avoid_: C++ SDK, workflow manager

**Plan Diagnostic**:
A structured explanation of why a Workflow Plan was rejected before it became an Execution Plan, including the stable failure code and the location in the submitted Plan.
_Avoid_: compiler log, validation string

**Workflow Capability Document**:
A versioned description of the Workflow Plan shape, server admission limits, and the executor protocols available to Plan authors.
_Avoid_: runtime configuration dump, executor registry internals
