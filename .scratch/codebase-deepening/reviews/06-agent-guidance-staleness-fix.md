# Agent Guidance Staleness Fix

## Scope

Review the detailed agent documents after the root `AGENTS.md` rewrite and
remove references that no longer describe the checkout.

## Findings fixed

- Replaced the removed legacy execution-service examples with the current
  `WorkflowRuntime` owner-shard implementation.
- Replaced the removed WebSocket and Config watcher test references with current
  Runtime, Workflow, HTTP executor, timing-wheel, and sandbox lifecycle tests.
- Replaced the old Command executor heartbeat reference with the current
  Minijail command runner.
- Removed hard-coded `dagforge::Error` table sizes and brittle line-number
  references; the two compile-time table assertions remain authoritative.
- Narrowed `config/` ownership to server-owned System Configuration and
  explicitly excluded Workflow Plan JSON and private CLI parser state.
- Clarified that `include/dagforge/**` is an exported internal source interface,
  not a supported public C++ SDK or ABI.
- Removed references to non-existent Workflow aggregation headers and retained
  the rule that first-party code includes precise concept headers.

## Result

The root guide remains short. Detailed documents now own implementation rules
without relying on deleted symbols, deleted tests, or numeric source offsets.
