# Use vendored libraries deliberately

Type: research
Status: resolved
Blocked by: None

## Question

Where should DAGForge use vendored libraries directly, and where is a local
wrapper justified?

## Answer

See
[`reviews/03-third-party-leverage-review.md`](../reviews/03-third-party-leverage-review.md).

The rule is not “wrap every library.” Use the vendored interface directly inside
the implementation that owns it. Add a wrapper only when it centralizes a
DAGForge contract or prevents third-party types and lifecycle rules from leaking
across a product seam.

Concrete findings:

- CLI11 already supplies validators, option groups, environment defaults,
  aliases, exclusions, help, and version handling. DAGForge needs a thin command
  model above it, not a second parser.
- CLI11's maximum-subcommand setting is inheritable and does not by itself make
  side-effectful callbacks safe. Parsing should select a command; execution
  should happen once after parsing.
- Boost.URL exposes `host_address()` for resolver input and encoded authority for
  the Host header. Those are distinct values, especially for IPv6.
- Glaze defaults to rejecting unknown keys. DAGForge should keep strict typed
  parsing in its JSON wrapper and avoid route-local parser variations.
- Boost.Asio and Boost.Process provide primitives, not DAGForge lifecycle
  semantics. Cancellation, owner-shard state, output limits, and process-group
  reaping remain project-owned policy.
