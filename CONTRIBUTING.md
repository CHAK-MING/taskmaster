# Contributing

## Development environment

DAGForge uses C++23, build2, Boost, OpenSSL, GoogleTest, and a pinned Minijail revision. Start with `scripts/setup-build2.sh`, install Minijail through `scripts/install-minijail.sh`, and use the repository scripts rather than constructing an independent build command.

## Required checks

Run `scripts/test.sh all` for functional verification, `scripts/test-runtime-audit.sh` for ASAN/TSAN/UBSAN, `scripts/test-coverage.sh` for the 90% production line gate, `FUZZ_RUNS=10000 scripts/run-glaze-fuzz.sh` for parser fuzzing, and `scripts/verify-vendored-deps.sh` for pinned dependency integrity. The Docker `test`, `audit`, and `release-verify` targets are the CI and release equivalents.

## Change discipline

Keep public contracts executor-neutral, preserve owner-shard runtime rules, treat persistent storage errors as explicit `Result` failures, and add a regression test for every corrected failure mode. Do not mix unrelated formatting or generated-file churn into a functional change. Markdown prose should use one paragraph or list item per physical line rather than manual hard wrapping.

## Pull requests

Describe the user-visible contract, failure model, compatibility impact, and evidence from tests or benchmarks. Call out changes to configuration, persistence formats, API response shapes, sandbox policy, release contents, or third-party revisions. Security-sensitive changes should follow `SECURITY.md` and avoid public exploit details before remediation.
