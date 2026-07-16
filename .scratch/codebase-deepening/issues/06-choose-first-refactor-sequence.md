# Choose the first refactor sequence

Type: research
Status: resolved
Blocked by: 02, 03, 04, 05

## Question

What sequence improves readability quickly without destabilizing the accepted
Run/Task/Attempt state machine?

## Answer

Use this order:

1. **CLI command selection** — it is the active change area, already has two
   reproduced correctness defects, and can be deepened without touching Runtime
   semantics. Parsing becomes pure command selection; one dispatcher performs
   execution after CLI11 finishes.
2. **Workflow HTTP route adapter** — remove repeated subsystem/path/Result
   plumbing while preserving every endpoint and JSON contract.
3. **Run bootstrap** — consolidate new Run, restored Run, and Repair Run admission
   mechanics before extracting the owner-shard engine.
4. **Executor contracts** — introduce a private compiled Node contract for
   Command and HTTP separately, then isolate one-Attempt execution state.
5. **Sandbox supervision and storage catalogs** — address after their callers no
   longer duplicate contract work.

This sequence starts with a narrow, demonstrable slice and postpones the most
stateful refactors until the terminology, invariants, and adapter seams are
explicit.
