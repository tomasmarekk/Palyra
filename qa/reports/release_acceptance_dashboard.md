# Release Acceptance Dashboard

- Core capabilities: 7
- Stable capabilities: 1
- Gated-production capabilities: 6
- P0 blockers: 0
- Acceptance policy: maturity is derived from the canonical evidence pack; dashboard text cannot promote a capability
- Evidence pack: `infra/release/stable-core-evidence.json`
- Alert fixture: `qa/fixtures/core-runtime-alert-thresholds.v1.json`
- Synthetic drill: `qa/fixtures/core-runtime-runbook-drill.v1.json`
- Runbooks: [Runtime Incident Runbooks](runtime_incident_runbooks.md)

| Capability | Maturity | Evidence | Owner sign-off | P0 blockers | Runbooks |
| --- | --- | --- | --- | --- | --- |
| runtime_kernel_v2 | stable | passed | @tomasmarekk | 0 | core release rollback; continuity recovery blocked |
| provider_recovery | gated_production | passed | @tomasmarekk | 0 | continuity recovery blocked; core release rollback |
| continuity_safe_resume | gated_production | passed | @tomasmarekk | 0 | continuity recovery blocked; core release rollback |
| objective_loop | gated_production | passed | @tomasmarekk | 0 | core release rollback |
| managed_coding_runtime | gated_production | passed | @tomasmarekk | 0 | LSP crash; PTY orphan; core release rollback |
| work_graph | gated_production | passed | @tomasmarekk | 0 | WorkGraph stale claim; core release rollback |
| mcp_persistent_runtime | gated_production | passed | @tomasmarekk | 0 | MCP outage; core release rollback |

The stable RuntimeKernelV2 row is default-on for newly admitted sessions and is
backed by direct authoritative QA, no-hidden-fallback, performance, security,
and retirement gates. Gated-production rows are supported production surfaces
whose evidence meets their declared floor but does not claim the longer
production window required for stable maturity.

The canonical evidence pack owns maturity floors, exact gate references,
compatibility commitments, owner sign-off, rollback posture, SLI thresholds,
runbook drill results, and the support-bundle checklist. The release checker
fails when this rendered dashboard diverges from that source, when a capability
is downgraded below its evidence floor, or when any P0 blocker reappears.
