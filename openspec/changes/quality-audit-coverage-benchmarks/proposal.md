# Audit quality, coverage, and benchmarks

DAGForge has strong unit and real Workflow suites, but it does not yet have a
repeatable source-coverage gate, a documented standards audit, or benchmarks
that map cleanly to production execution paths. Existing benchmark cases mix
framework overhead with the behavior being measured, and several critical
files have grown large enough that implementation quality must be reviewed
directly rather than inferred from passing tests.

This change establishes a reproducible source-line coverage workflow with a
90% minimum, adds scenario coverage where important behavior is missing,
audits the repository against AGENTS.md and `docs/ai`, fixes violations and
rough critical-path implementations found by the audit, and replaces weak
microbenchmarks with workloads tied to Runtime, Workflow, HTTP, and storage
behavior.

Third-party code, tests, generated module artifacts, and module interface units
are excluded from the coverage denominator. Production `.cpp` files under
`src/dagforge` and `src/main.cpp` form the measured source set.
