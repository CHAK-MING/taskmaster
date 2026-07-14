## 1. Specification and configuration

- [x] 1.1 Define inbound/outbound HTTP production requirements and sandbox
  threat model.
- [x] 1.2 Add fail-closed configuration fields, validation, examples, and docs.

## 2. HTTP boundary

- [x] 2.1 Make inbound listeners TLS-only when TLS is configured and dispatch
  accepted sockets without blocking the accept loop.
- [x] 2.2 Add connection, parser, idle-time, and requests-per-connection limits.
- [x] 2.3 Reject unsupported HTTP methods instead of coercing them to GET.
- [x] 2.4 Use Beast request serialization and enforce outbound address policy.
- [x] 2.5 Add TLS minimum version, private CA, mTLS, and global egress capacity.
- [x] 2.6 Add unit and real-network regression tests.

## 3. Command sandbox boundary

- [x] 3.1 Make allowlists fail closed and canonicalize approved programs.
- [x] 3.2 Preflight Minijail, seccomp BPF, Landlock, and workspace permissions.
- [x] 3.3 Bound captured output and streamed line buffers; terminate on overflow.
- [x] 3.4 Add executor shutdown that kills active process groups before Runtime
  teardown.
- [x] 3.5 Add sandbox policy, lifecycle, permission, and overflow tests.

## 4. Verification

- [x] 4.1 Pass normal unit tests and real Workflow tests.
- [x] 4.2 Pass ASAN/LSAN, UBSAN, and focused TSAN execution.
- [x] 4.3 Pass OpenSpec, module graph, convention, diff, and release checks.
