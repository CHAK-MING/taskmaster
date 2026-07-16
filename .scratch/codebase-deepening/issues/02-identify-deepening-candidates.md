# Identify deepening candidates

Type: research
Status: resolved
Blocked by: 01

## Question

Which hot modules are shallow or internally fragmented enough that deepening
would materially improve locality and leverage?

## Answer

Five candidates survived the deletion test and are documented in
[`reviews/01-architecture-survey.md`](../reviews/01-architecture-survey.md):

1. Keep `WorkflowRuntime` as the external module, but isolate Run admission and
   owner-shard Run execution as private deep modules.
2. Give each executor a compiled private contract and a separate one-Attempt
   execution state machine.
3. Put Workflow HTTP request extraction and Result-to-response policy behind a
   small route adapter.
4. Separate sandbox launch planning from process supervision while retaining
   `ICommandRunner` as the seam.
5. Consolidate file catalog scanning and key/file identity checks behind the
   existing durable-file implementation.

File length alone was rejected as a reason to split. `WorkflowRuntime` is deep
at its external interface; its problem is poor internal locality, not excessive
public surface.
