# DAGForge North-Star Workflow

This document records the end-to-end workflow that the execution kernel must
support without executor-specific logic leaking into Workflow Runtime.

The concrete Model, WASM, and MCP executors are intentionally not designed by
the durable-recovery change. They will occupy the existing `ITaskExecutor`
seam later.

## Target graph

```mermaid
flowchart TD
    N1[Node 1: Trigger and context injection]
    N2A[Node 2A: HTTP - Hacker News]
    N2B[Node 2B: HTTP - GitHub trends]
    N2C[Node 2C: HTTP - arXiv papers]
    N3A[Node 3A: Model - topics and sentiment]
    N3B[Node 3B: Model - high-star projects]
    N3C[Node 3C: Model - paper summaries]
    N4[Node 4: Deterministic sandbox transform\nalign, deduplicate, merge JSON]
    N5[Node 5: Model - structured alpha assessment]
    N6{Node 6: Conditional routing}
    N7A[Node 7A: Model - deep investment report]
    N7B[Node 7B: Deterministic sandbox archive]
    N8A[Node 8A: HTTP - Slack delivery]
    N8B[Node 8B: No-op / end]

    N1 --> N2A --> N3A --> N4
    N1 --> N2B --> N3B --> N4
    N1 --> N2C --> N3C --> N4
    N4 --> N5 --> N6
    N6 -->|alpha_signal == high| N7A --> N8A
    N6 -->|alpha_signal == low| N7B --> N8B
```

Node 5 publishes a schema-constrained value such as:

```json
{
  "alpha_signal": "high",
  "reason": "..."
}
```

## Planned executor families

The intended product eventually includes four executor families:

1. a command/data-processing executor backed by a WASM sandbox;
2. an HTTP executor;
3. a model executor;
4. an MCP executor.

Their configuration and capability schemas are future work. Workflow Runtime
must continue to see only compiled config, typed inputs and outputs, lifecycle
signals, and `ExecutionFailure`.

## Acceptance invariants

The graph is considered supported only when all of the following hold:

- the three fetch and model branches execute concurrently and fan in exactly
  once all required inputs are available;
- a failed branch returns one structured failure hierarchy through the
  dedicated failure-report interface;
- oversized diagnostics are returned as named Artifact references and remain
  retrievable through the generic Artifact interface;
- process restart preserves successful branches and retained outputs, closes
  the interrupted Attempt as `runtime_restarted`, and creates a new Attempt
  only for unfinished work;
- a revised JSON Plan creates an immutable child Repair Run rather than
  mutating the failed parent;
- unchanged successful independent branches are reused, while a changed node
  and every dependent node are invalidated transitively;
- conditional routing uses the persisted structured output and does not
  execute the unselected branch;
- repeated start or repair requests with the same retained idempotency key
  return the authoritative existing Run ID;
- Plan, Run, Task, Attempt, failure, Artifact, lineage, and reuse provenance
  survive restart without executor-specific persistence code.

This scenario is the architectural acceptance target for later executor work.
New features should be evaluated by whether they deepen the existing seams
needed by this graph, not by whether they add another parallel control path.
