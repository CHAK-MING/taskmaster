# ADR 0002: C++23-First Foundation Contracts

## Status

Accepted.

## Context

DAGForge had a useful but uneven foundation layer. Public headers mixed
textual-include and named-module assumptions, several third-party error types
crossed internal boundaries, parsing discarded useful failure information,
and low-level facilities such as cleanup, time, identifiers, logging, metrics,
and coroutine I/O did not share one explicit contract model.

The local Abseil source tree was reviewed as a design reference. It is not a
dependency decision. DAGForge already has C++23 as its language baseline,
Boost.Asio as its asynchronous runtime substrate, Glaze as its serialization
adapter, CLI11 for command-line parsing, and ankerl::unordered_dense for the
few measured hash-table hot paths. Importing a second status, time, hash,
callback, synchronization, or logging ecosystem would increase ABI, ODR,
build, and ownership complexity without a demonstrated product requirement.

The supported GCC 15/libstdc++ toolchain implements the C++23 facilities used
by this decision, including `std::expected`, `std::move_only_function`, chrono
time-zone parsing and formatting, `std::flat_map`, ranges conversions and
views, `std::source_location`, and `std::print`. The toolchain does not yet
provide the C++23 `<scope>` header, so cleanup needs one narrow compatibility
seam.

## Decision

### Language and dependency policy

DAGForge foundation code targets C++23 directly. It does not maintain a
C++17 or C++20 compatibility API. Standard-library facilities are preferred
when they express the required contract. Toolchain compatibility knowledge is
centralized rather than repeated at call sites.

Abseil may be consulted as primary-source design material, but DAGForge does
not expose `absl::*` types, include Abseil from public headers, or add Abseil
to the build graph. A future dependency requires a separate ADR with caller,
benchmark, binary-size, compile-time, and ownership evidence.

### Foundation layers

The dependency direction is:

1. **Base contracts**: standard library only. Error domains, `Result<T>`,
   source-location contract failures, and the scope-exit compatibility seam.
2. **Core values and policies**: base plus the standard library. Typed values,
   memory-resource policy, metrics primitives, and hashing contracts.
3. **Text, time, and serialization adapters**: lower layers plus Glaze only at
   the serialization seam. Parsing, enum metadata, JSON wrappers, and chrono
   formatting live here.
4. **I/O runtime adapters**: lower layers plus Boost.Asio. Native error codes,
   cancellation, timers, and executor handles are normalized here.
5. **Domain types**: Workflow, Run, Artifact, Evidence, and related identifiers
   depend on foundation; foundation never depends on workflow concepts.

Named modules export the same contracts as standalone public headers. Every
public header must compile independently under strict C++23 warnings.

### Error and parsing contracts

Ordinary fallible operations use `Result<T> = std::expected<T,
std::error_code>`. Static error domains must safely handle arbitrary integer
values. Rich workflow execution failures remain a separate domain type; they
are not forced into every low-level result.

Parsing uses typed errors while parsing is in progress and projects to stable
error codes only at compatibility boundaries. JSON parse failures retain
bounded location information without exposing Glaze types. Serialization
failure is returned to the caller; sentinel-success helpers such as
`dump_json()` are prohibited.

### Typed identifiers and enums

`TypedId<Tag>` owns string storage, comparison, hashing, and formatting.
Untrusted text enters through `parse()`. Trusted internal generation uses the
explicit `from_trusted()` entry. Per-domain traits state maximum byte lengths
and whether an empty value is a permitted unset sentinel. Requiredness remains
a domain-schema decision and is checked by the owning validator.

The Glaze adapter validates identifier length and control characters through
the same traits. Enum names and values are owned by DAGForge traits; Glaze
derives wire metadata from those traits instead of becoming the source of
domain meaning.

### Cleanup and memory-resource lifetime

`dagforge::scope_exit` is the only cleanup API. Its implementation selects
`std::scope_exit` when the standard header is available and otherwise uses the
toolchain's experimental implementation. No other source may include
`<experimental/scope>`.

`ThreadMemoryResourceOverride` is thread-local and strictly synchronous. It
requires a non-null resource, nested guards restore in LIFO order, and the
guard must be destroyed on its creating thread. It must not span `co_await` or
another suspension point.

### Time, metrics, I/O, and logging

Time formatting and parsing use C++23 chrono, format, and the system time-zone
database. DAGForge does not introduce a second civil-time type system.

Histogram construction validates strictly increasing buckets and accepts
chrono durations explicitly. Concurrent snapshots retain their documented
weak-snapshot semantics.

Boost.Asio error codes are normalized at an adapter boundary. Public sleep,
yield, and timer awaitables return `Result`; caller cancellation is propagated
as the DAGForge I/O cancellation error. `IoContext` exposes explicit native
handles and has no implicit conversion to Boost types.

Logging separates structured records, rendering, sinks, queueing, and
lifecycle. Records include `std::source_location`. Sink reconfiguration and
flush return `Result`. ANSI color is a sink capability, overflow behavior is
an explicit policy, dropped messages are observable, and tests can inject a
sink directly.

### Explicit non-decisions

- `function_ref` is not a C++23 facility and is not implemented without at
  least two demonstrated synchronous, non-owning callback callers.
- Standard containers, ankerl::unordered_dense, and any future specialized
  container are selected per workload; there is no bulk container rewrite.
- A small-vector or cord-like type requires allocation and workload evidence.
- `std::stacktrace` is not part of normal error handling and is not safe for a
  POSIX signal handler. Crash diagnostics require a separate platform design.
- There is no second status, time, flags, random, synchronization, logging, or
  hash framework.

## Enforcement

`scripts/check-foundation-contracts.py` enforces the architectural boundary:

- every public header has the module include shape and compiles standalone;
- the required C++23 library surface compiles on the supported toolchain;
- Layer 0 does not depend on Boost, Glaze, workflow, HTTP, or filesystem;
- experimental scope use stays behind the compatibility seam;
- `IoContext` cannot regain an implicit native conversion;
- Abseil, `function_ref`, and stacktrace do not enter public foundation APIs;
- TypedId JSON input goes through `parse()`;
- hidden JSON failure and the legacy memory-override API cannot return.

The named-module smoke target, unit tests, component tests, module graph check,
test-layout check, and strict warning build remain release gates.

## Implementation Record

The accepted design is implemented by the following repository seams:

| Contract | Primary implementation |
| --- | --- |
| Error domains and `Result` | `dagforge/core/error_domain.hpp`, `error.hpp`, `io/result.hpp` |
| C++23 and public-header gate | `scripts/check-foundation-contracts.py` |
| Cleanup and contract failure | `dagforge/core/scope_exit.hpp`, `contract.hpp` |
| Typed parsing and JSON payloads | `dagforge/util/parse.hpp`, `json.hpp` |
| Enum metadata | `dagforge/util/enum.hpp` and domain trait specializations |
| Typed identifiers | `dagforge/util/typed_id.hpp`, `id.hpp`, `id.cpp` |
| Metrics and time | `dagforge/core/metrics.hpp`, `dagforge/util/time.hpp` |
| I/O normalization | `dagforge/io/asio_error.hpp`, `context.hpp`, `timing_wheel.hpp` |
| Structured logging | `dagforge/util/log.hpp`, `src/dagforge/util/log.cpp` |
| Thread-bound allocation override | `dagforge/core/memory.hpp` |

## Consequences

Foundation changes now have a higher entry bar: a generic-looking wrapper is
not accepted unless it adds a precise lifetime, failure, ownership, or
performance contract. Public APIs are less coupled to implementation
libraries, invalid external values are rejected earlier, and named-module and
header consumers receive the same interface.

The project intentionally keeps several domain validators even when typed
identifiers validate text shape. An empty identifier can represent an unset
field in a raw model; the owning plan, checkpoint, storage, or API validator
decides whether that field is required in context.
