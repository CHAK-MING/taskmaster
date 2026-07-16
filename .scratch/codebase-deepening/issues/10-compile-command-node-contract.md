# 10 — Compile the Command Node contract once

**What to build:** Make Command executor compilation produce one normalized private contract so Task start only applies dynamic input environment values and starts the runner.

**Blocked by:** 09 — Isolate Run bootstrap

**Status:** resolved

**Owner:** OpenAI

- [x] Program authorization, static environment validation, and output contract validation happen during compilation.
- [x] Task start does not repeat static JSON parsing and validation.
- [x] Dynamic input conversion and duplicate environment handling remain explicit.
- [x] `ITaskExecutor` remains the test and caller seam through `CompiledExecutorConfig`.
- [x] Existing Command executor and sandbox behavior is unchanged.

**Evidence:** Full build passed, all-unit-tests passed 264/264, and all CLI subprocess scenarios passed. Command start rejects an encoded-only contract instead of falling back to JSON parsing.
