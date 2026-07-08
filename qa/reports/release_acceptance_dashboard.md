# Release Acceptance Dashboard

- P0 areas: 8
- Stable candidates: 0
- Roadmap checkbox policy: acceptance complete is not stable by itself

| Area | Maturity | Code complete | Tested | Stable candidate | Blockers |
| --- | --- | --- | --- | --- | --- |
| agent_harness_runtime | preview_only | true | false | false | failing required gates: harness-conformance |
| execution_gate_pipeline_v2 | preview_only | true | true | false | release hardening evidence required before stable promotion |
| provider_recovery | preview_only | true | true | false | provider recovery fixtures must remain green |
| replay_capture | preview_only | true | false | false | failing required gates: replay-fixtures |
| verification_runtime | gated_production | true | true | false | stable rollout requires release review |
| compaction_safeguard | gated_production | true | true | false | stable rollout requires replay gate evidence |
| advisor_fanout | preview_only | true | true | false | advisor outputs remain non-authoritative |
| lsp_service | preview_only | true | true | false | unavailable server fallback must stay covered |
