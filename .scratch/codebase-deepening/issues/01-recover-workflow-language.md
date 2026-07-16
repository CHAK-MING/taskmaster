# Recover the workflow language

Type: research
Status: resolved
Blocked by: None

## Question

Which project-specific concepts must be named consistently before architecture
and source reviews can describe good seams without inventing synonyms?

## Answer

The accepted state ADR and current source converge on a compact language:
Workflow Plan, Execution Plan, Node, Trigger, Run, Task, Attempt, Repair Run,
Checkpoint, Evidence, Artifact, and Workflow Control Plane. The glossary now
lives in [`CONTEXT.md`](../../../CONTEXT.md).

The important distinctions are Plan versus Run, Node versus Task, and Task
versus Attempt. These distinctions are load-bearing: retries create Attempts,
recovery restores Runs, and Plan compilation must not be described as runtime
execution.
