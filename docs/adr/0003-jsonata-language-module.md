# ADR 0003: Native JSONata Language Module

## Status

Accepted.

## Context

DAGForge needs a deterministic, reusable data-transformation language before adding a `transform` executor. The language implementation must be independently testable, preserve JSONata-standard diagnostics, run under explicit host resource limits, and avoid coupling language semantics to Workflow runtime lifecycle. Existing C++ ports were not selected because their maturity, maintenance and JSON representation did not meet the project boundary.

JSONata language failures are richer than an ordinary platform `std::error_code`: the compatibility contract includes the standard `S`, `T`, and `D` code, failure kind, UTF-8 byte offset, JavaScript-compatible UTF-16 position, token and bounded message. This is the same class of temporary rich diagnostic contract as `util::ParseResult<T>`, not a second replacement for DAGForge's ordinary `Result<T>`.

## Decision

### Semantic authority

DAGForge targets JSONata 2.2.2. The authority is the upstream `v2.2.2` source and official language-neutral test corpus at commit `6c7e95fdbf4405a1e741852a7cd8cd985b4305bb`. A compatibility claim is valid only when the pinned corpus passes without unapproved skips or alternate expected results.

### Module boundary

`libdagforge-jsonata` owns tokenization, parsing, path lowering, immutable compiled programs, runtime values, tuple streams, lexical environments, tail-call trampolining, built-ins, date/time pictures, regex adaptation and host budgets. Its public seam is `jsonata::Program::compile()` and `Program::evaluate()`. The module may depend on `dagforge-foundation` and PCRE2 through its private adapter; it may not depend on Workflow, concrete executors, HTTP, sandbox or app.

The supported toolchain exposes the language through its self-contained header seam rather than a named C++ module. Glaze's dynamic JSON templates are not GCC-module-safe under the supported GCC 15 configuration; DAGForge will not carry compiler suppressions or a private Glaze patch solely to manufacture a module interface. This decision can be revisited after the JSON seam or Glaze's module support changes.

Compiled `Program` objects are immutable and may be evaluated concurrently. Each evaluation owns its environments and continuation state while sharing one evaluation-wide budget across nested `$eval` calls. Runtime tuples, functions, sequences and regex handles remain private and cannot cross the public JSON boundary.

### Transform executor adapter

The Workflow-facing adapter lives in `executors/transform`, not in the language module. Its node protocol contains one required `expression` string. Plan compilation compiles that expression once into an immutable `jsonata::Program`; Task execution never reparses source.

Each declared Workflow input is always exposed as a property of the JSONata root object and is also installed as a same-named lexical binding. Expressions should use the variable form, for example `$message`, when the port name is a valid JSONata variable token. Other names remain accessible through the root object, for example `$lookup($, 'hyphen-name')`. JSON-compatible values retain their structural type; integer and floating-point inputs both follow JSONata's binary64 number semantics. Artifact values expose only immutable reference metadata (`type`, `artifact_id`, `media_type`, `size_bytes`, and `digest`); the executor does not read Artifact contents or permit an expression to forge an Artifact output.

A node with one declared output maps the complete JSONata result to that port. A node with multiple outputs must return an object whose keys exactly match the declared output ports. `undefined` and function values are execution failures because they cannot become retained Workflow values.

JSONata evaluation runs on executor-owned CPU worker threads rather than Runtime shard threads. Node cancellation is delivered through `std::stop_token`; the node deadline includes queue time; quiesce rejects new work, cancels accepted work, and waits for all callbacks to converge. Standard JSONata diagnostics remain in structured `ExecutionFailure.details` while the executor maps cancellation, timeout, resource exhaustion, and ordinary language failures into the Workflow error domain.

The adapter applies a conservative native call-depth ceiling below the language library's general-purpose default. This ensures non-tail recursion becomes a structured resource-exhaustion failure before a worker thread can exhaust its native stack, including under sanitizer instrumentation. Tail-recursive JSONata remains governed by the trampoline and evaluation step budget.

### Diagnostic result exception

The JSONata seam uses `DiagnosticResult<T> = std::expected<T, jsonata::Failure>`. This narrow exception to ordinary `Result<T>` exists to preserve standardized language diagnostics until the caller decides how to present or project them. It does not enter foundation, Workflow or executor interfaces. Expected diagnostic failures are still returned as values and never thrown.

Workflow Plan admission now carries the project-wide structured diagnostic contract accepted in ADR 0004. The Transform adapter returns its standard JSONata compile code, message, token and positions in bounded diagnostic details; `PlanCompiler` adds the Node identity and absolute JSON Pointer. Runtime evaluation failures continue to retain the same structured JSONata details. Transform does not expose a private diagnostic side channel.

### Regex dependency

PCRE2 is a private system dependency of `libdagforge-jsonata`; its types do not appear in public headers. Configuration must verify an 8-bit PCRE2 development package with the match, depth and heap limit APIs used by the adapter. The main `libdagforge` link interface does not export PCRE2.

### Host resource contract

Public limits are enforced, not advisory. Compile limits cover source, token, node, owned string and compiled-program storage. Evaluation limits cover steps, call depth, sequence/path cardinality, reachable value graph size, string bytes, cumulative lexical bindings created, regex matches, nested `$eval`, cancellation and an absolute steady-clock deadline. Statistics report observed peaks under the same accounting model.

## Consequences

The Transform executor adapts Workflow values and lifecycle to `Program`; it does not own parser or evaluator semantics. JSONata diagnostics remain exact inside the language boundary. Ordinary runtime and storage operations continue to use `Result<T>` and `std::error_code`, while Workflow Plan admission uses the narrow structured contract in ADR 0004.

Upstream JSONata and PCRE2 license notices and the pinned conformance acquisition procedure are release inputs. A JSONata version upgrade requires updating the pinned commit, rerunning the complete corpus, reviewing semantic changes and updating this ADR or a superseding ADR.
