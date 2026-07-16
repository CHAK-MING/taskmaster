# Locate repeated knowledge

Type: research
Status: resolved
Blocked by: 01

## Question

Where does the source repeat knowledge in ways that make changes unsafe or hard
to read?

## Answer

The repeated knowledge is summarized in
[`reviews/02-source-readability-review.md`](../reviews/02-source-readability-review.md).
The highest-cost repetitions are:

- Run creation and Repair Run creation both coordinate lifecycle locks,
  idempotency, initial Checkpoint persistence, pending initialization counters,
  owner-shard posting, and lifetime checks.
- Command and HTTP executors parse a JSON Node contract during compilation and
  parse it again at Task start, then partially repeat validation while rebuilding
  the executable request.
- Workflow HTTP routes repeatedly obtain optional subsystems, extract path
  parameters, convert IDs, await a Runtime call, and map `Result` to HTTP.
- CLI callbacks mutate one shared request object and execute side effects during
  CLI11 callback traversal, making parser behavior part of network semantics.
- Plan and Checkpoint stores repeat directory enumeration, cache merge, filename
  identity checks, decode, and sorting policy.

Small forwarding functions and explicit state transitions are not duplication
by themselves. They should remain when they make lifecycle ordering visible.
