# 18 — Classify Workflow library headers

**What to build:** Group stable Workflow headers by `model/`, `planning/`, `runtime/`, and `storage/` after measuring include dependencies and remove unused aggregate barrels instead of leaving forwarding clutter.

**Status:** ready-for-agent

- [ ] Internal includes use precise concept headers.
- [ ] `workflow_types.hpp` and `workflow_storage.hpp` are removed if they remain unused.
- [ ] Module and library targets continue to build without creating a public SDK promise.
- [ ] Apply the same density audit to `include/dagforge/core`, `include/dagforge/util`, and `include/dagforge/config` only after Workflow establishes the pattern.
