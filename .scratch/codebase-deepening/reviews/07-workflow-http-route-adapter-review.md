# Workflow HTTP Route Adapter Review

## Standards

No findings after revision.

- The adapter is private under `src/dagforge/app/api/detail/routes/`; no product
  interface or C++ SDK surface was added.
- `workflows.cpp` keeps its corresponding header first and directly lists all
  15 endpoint registrations.
- The adapter depends on `Application` rather than the larger `ApiContext`; its
  header forward-declares Runtime and Control Plane and keeps implementation
  includes in the `.cpp`.
- Typed JSON continues through DAGForge's Glaze wrappers. Typed IDs are created
  at one extraction seam, and domain validation remains in Workflow modules.
- Initial shallow response-forwarding methods were removed. Deleting the final
  adapter would reintroduce subsystem, path, body, pagination, and idempotency
  policy across many routes, so it passes the deletion test.
- No generic route registry, handler DSL, public abstraction, or private-helper
  test surface was introduced.

## Spec

No findings.

- Every Workflow, Run, Evidence, Artifact, and control route remains explicit.
- Missing Workflow subsystems retain one 503 response contract.
- Start Run accepts an empty body with the existing defaults; malformed typed
  JSON remains 400. Repair Run still requires a valid body and a Plan.
- Pagination retains the existing fallback and clamp behavior: invalid offset
  becomes 0 and limit is clamped to 1..1000.
- Domain `Result` errors retain the existing stable failure JSON and HTTP status
  mapping.
- Existing contract verification passed: 120 API tests and all CLI scenarios.

Summary: Standards 0 findings; Spec 0 findings.
