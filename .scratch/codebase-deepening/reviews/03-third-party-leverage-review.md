# Third-Party Leverage Review

## CLI11

The vendored CLI11 version already provides positional options, validators,
aliases, option groups, environment sources, help/version formatting, option
dependencies, and subcommand exclusions. DAGForge should not create a second
parser or copy validation mechanics.

The local seam should model a parsed DAGForge command and centralize product
defaults. CLI11 callbacks should select that command only. Running a Workflow or
sending HTTP must happen after parsing succeeds and exactly one leaf command is
selected.

The source confirms that maximum subcommand configuration is inheritable. It
does not make side-effectful callbacks a safe execution model. Sibling
`excludes()` can improve diagnostics, but a side-effect-free parse phase is the
fundamental protection.

## Boost.URL

Resolver input and HTTP authority are different values. Use decoded
`host_address()` for TCP/TLS connection and encoded authority for the Host
header. IPv6 demonstrates why the distinction matters: the resolver needs
`::1`, while HTTP authority needs `[::1]:port`.

URL parsing and normalization should remain in the module that owns endpoint or
egress policy. Do not add another string URL parser.

## Glaze

The vendored options default to `error_on_unknown_keys = true`. Strict typed JSON
should therefore be expressed once in `dagforge/util/json.hpp` and reused by
config, Plan, API contract, and executor contract parsing.

Private typed contracts are appropriate when a JSON shape is stable and parsed
more than once. `glz::obj` remains appropriate for local write-only responses.
Do not introduce mirror DTOs merely to move values between layers.

## Boost.Asio and Boost.Process

These libraries supply cancellation slots, timers, pipes, process launch, and
wait primitives. DAGForge still owns owner-shard mutation, process-group
semantics, output budgets, Evidence, Attempt completion, and quiesce ordering.
A wrapper is justified around those product semantics, not around every Asio or
Process call.

## Wrapper rule

Use a third-party type directly inside its owning implementation. Add a local
module when it provides one of these forms of leverage:

1. It prevents third-party types or callback semantics from leaking across a
   DAGForge interface.
2. It centralizes a product invariant used at multiple call sites.
3. It converts a callback/lifecycle primitive into a DAGForge lifecycle concept.

Otherwise, a wrapper is a shallow pass-through and should not exist.
