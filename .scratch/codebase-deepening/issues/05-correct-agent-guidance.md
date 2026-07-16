# Correct agent guidance

Type: research
Status: resolved
Blocked by: 01, 02, 03, 04

## Question

Which parts of `AGENTS.md` are stale, duplicated, misleading, or too specific to
recent implementation work?

## Answer

See
[`reviews/04-agent-guidance-review.md`](../reviews/04-agent-guidance-review.md).

The current file contains useful hard rules, but it repeats large portions of
the detailed Glaze and module documentation and blurs two meanings of “public”:
headers exported by an internal build target versus a supported product SDK.
It also overstates that every startup-shaped value belongs in config DTOs,
which would incorrectly pull private CLI parser state into `include/dagforge`.

The corrected guide should remain a routing document plus a short set of
load-bearing rules. Detailed serde recipes, dependency versions, module setup,
and test matrices stay in `docs/ai/*.md`.
