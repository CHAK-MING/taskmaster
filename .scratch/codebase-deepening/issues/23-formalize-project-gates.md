# 23 — Formalize project quality and release gates

**What to build:** Make the repository's functional, sanitizer, coverage, fuzz, dependency, release, security-reporting, and contribution requirements reproducible in CI and locally.

**Status:** resolved

**Owner:** OpenAI

- [x] Pull requests and main pushes run Docker-backed full tests, audit gates, and release smoke builds.
- [x] ASAN, TSAN, UBSAN, 90% production line coverage, and 10,000 parser fuzz runs have repository-owned commands.
- [x] Vendored dependencies remain hash-verified and GitHub Actions/Docker references receive Dependabot updates.
- [x] Security reporting and contribution requirements are documented.
- [x] Minijail fetching uses a pinned revision, bounded retries, and the two official public sources unless an explicit enterprise mirror is configured.

**Resolution evidence:** Full tests, strict warning compilation, runtime and storage sanitizer suites, 90.06% coverage, 10,000 fuzz runs, 50 shuffled storage repetitions, vendored verification, and a locally staged static release archive all pass.
