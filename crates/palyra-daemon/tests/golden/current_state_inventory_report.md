# Runtime Audit Baseline

Generated from the live daemon harness and committed CLI parity matrix.

Regenerate with one command:

```powershell
pwsh -NoLogo -File scripts/dev/generate-runtime-audit-baseline.ps1
```

Linux/macOS equivalent:

```bash
bash scripts/dev/generate-runtime-audit-baseline.sh
```

## Summary

- Capability catalog entries: `32`
- CLI families: `59`
- Method registry entries: `486`
- Compat routes registered: `6/6`
- Feature rollout flags: `26`
- Runtime preview controls: `8` capabilities
- Feature rollout maturity: `scaffold=1`, `preview_only=16`, `gated_production=4`, `stable=0`, `deprecated=0`, `blocked=5`

## State Buckets

| Bucket | Count | Source |
| --- | ---: | --- |
| `production` | `2` | roadmap area source map |
| `preview` | `3` | roadmap area source map |
| `disabled` | `2` | runtime_controls effective_state |
| `scaffold` | `3` | roadmap area source map |

## Source Of Truth

| Surface | Source paths | Why Palyra tracks it |
| --- | --- | --- |
| `capability_catalog` | `crates/palyra-daemon/src/transport/http/handlers/console/auth.rs`, `crates/palyra-daemon/src/transport/http/handlers/console/diagnostics.rs`, `crates/palyra-control-plane/src/models.rs` | public capability ids, surfaces, mutation classes, and contract paths |
| `runtime_diagnostics` | `crates/palyra-daemon/src/transport/http/handlers/console/diagnostics.rs`, `crates/palyra-daemon/src/runtime_diagnostics.rs` | runtime sections, health, metrics, roadmap, observability, and feature rollout payloads |
| `runtime_preview_controls` | `crates/palyra-daemon/src/runtime_preview_controls.rs`, `crates/palyra-common/src/runtime_preview.rs`, `crates/palyra-daemon/src/config/schema.rs` | preview capability modes, rollout gates, activation blockers, and shared wire names |
| `feature_rollout_maturity` | `crates/palyra-daemon/src/feature_rollout_maturity.rs`, `crates/palyra-daemon/src/config/schema.rs`, `crates/palyra-daemon/tests/current_state_inventory.rs` | rollout maturity states, owners, required tests, public exposure, and promotion blockers |
| `method_registry` | `crates/palyra-daemon/src/method_registry.rs`, `crates/palyra-daemon/src/transport/http/router.rs`, `crates/palyra-daemon/src/access_control.rs` | public method descriptors, route scopes, schema hashes, streaming flags, and idempotency support |
| `compat_routes` | `crates/palyra-daemon/src/transport/http/router.rs`, `crates/palyra-daemon/tests/current_state_inventory.rs` | registered OpenAI-compatible route surface probed by the live daemon harness |
| `cli_families` | `crates/palyra-cli/tests/cli_parity_matrix.toml`, `crates/palyra-cli/tests/cli_parity_report.md` | top-level CLI families and parity status used by operator handoff surfaces |
| `execution_backends` | `crates/palyra-daemon/src/execution_backends.rs`, `crates/palyra-daemon/src/application/tool_runtime` | local, desktop, Docker, networked worker, and SSH backend posture |

## Roadmap Area Map

| Area | Status | Evidence | Reason |
| --- | --- | --- | --- |
| `api` | `production` | `/console/v1/control-plane/capabilities`, `/v1/models`, `/v1/chat/completions`, `/v1/responses` | console and compat routes are registered in the live daemon harness |
| `mcp` | `scaffold` | `cli family: mcp`, `roadmap phase 5` | MCP serve is discoverable, while external MCP import/supervision remains roadmap work |
| `subagents` | `preview` | `runtime_controls.auxiliary_executor`, `cli families: agent, agents, sessions` | delegated work surfaces exist behind preview controls before durable subagent records land |
| `execution_backends` | `preview` | `execution_backends`, `runtime_controls.networked_workers` | local sandbox is available, while remote backends and workers remain gated or disabled |
| `qa_lab` | `scaffold` | `runtime_roadmap.phase0_harness`, `fixtures/golden/release_eval_inventory.json` | regression fixtures exist before the dedicated QA Lab manifest and runner |
| `hooks` | `preview` | `capability: hooks`, `cli family: hooks` | basic hook operability is exposed before the full agent hook taxonomy |
| `observability` | `production` | `/console/v1/diagnostics`, `runtime_health`, `agent_runtime_metrics`, `opentelemetry` | diagnostics and metrics sections are emitted by the live daemon harness |
| `provider_recovery` | `scaffold` | `feature_rollouts.provider_stream_normalizer`, `feature_rollouts.tool_repair` | recovery flags are visible but default-off before classifier and stream-normalizer work |

## Runtime Controls

| Capability | Mode | Effective state | Rollout | Blockers |
| --- | --- | --- | --- | --- |
| `auxiliary_executor` | `preview_only` | `preview_only` | `false` from `default` | - |
| `delivery_arbitration` | `disabled` | `disabled` | `false` from `default` | - |
| `flow_orchestration` | `preview_only` | `preview_only` | `false` from `default` | - |
| `networked_workers` | `disabled` | `disabled` | `false` from `default` | - |
| `pruning_policy_matrix` | `preview_only` | `preview_only` | `false` from `default` | - |
| `replay_capture` | `preview_only` | `preview_only` | `false` from `default` | - |
| `retrieval_dual_path` | `preview_only` | `preview_only` | `false` from `default` | - |
| `session_queue_policy` | `preview_only` | `preview_only` | `false` from `default` | - |

## Feature Rollouts

- Enabled: `0`
- Disabled/default-off: `26`

| Flag | Enabled | Source | Maturity | Owner | Public API exposure | Config path | Env var | Blockers |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `agent_plan_state` | `false` | `default` | `preview_only` | `agent plan state` | operator diagnostics: /console/v1/diagnostics, /admin/v1/status, palyra doctor --json | `feature_rollouts.agent_plan_state` | `PALYRA_EXPERIMENTAL_AGENT_PLAN_STATE` | `Enable feature_rollouts.agent_plan_state or PALYRA_EXPERIMENTAL_AGENT_PLAN_STATE after required tests and owner acceptance pass.`<br>`model-visible plan state must remain scoped to diagnostic-safe fields` |
| `attack_surface_audit` | `false` | `default` | `preview_only` | `security audit` | operator diagnostics: /console/v1/diagnostics, /admin/v1/status, palyra doctor --json | `feature_rollouts.attack_surface_audit` | `PALYRA_EXPERIMENTAL_ATTACK_SURFACE_AUDIT` | `Enable feature_rollouts.attack_surface_audit or PALYRA_EXPERIMENTAL_ATTACK_SURFACE_AUDIT after required tests and owner acceptance pass.`<br>`attack-surface audit output must remain redacted and policy-aligned` |
| `auxiliary_executor` | `false` | `default` | `preview_only` | `agent delegation` | runtime preview controls plus operator diagnostics and doctor | `feature_rollouts.auxiliary_executor` | `PALYRA_EXPERIMENTAL_AUXILIARY_EXECUTOR` | `Enable feature_rollouts.auxiliary_executor or PALYRA_EXPERIMENTAL_AUXILIARY_EXECUTOR after required tests and owner acceptance pass.`<br>`auxiliary task budget and count limits must remain enforced` |
| `channel_turn_kernel` | `false` | `default` | `gated_production` | `channel router` | operator diagnostics: /console/v1/diagnostics, /admin/v1/status, palyra doctor --json | `feature_rollouts.channel_turn_kernel` | `PALYRA_EXPERIMENTAL_CHANNEL_TURN_KERNEL` | `Enable feature_rollouts.channel_turn_kernel or PALYRA_EXPERIMENTAL_CHANNEL_TURN_KERNEL after required tests and owner acceptance pass.`<br>`production path is currently always on; rollout gate is diagnostic-only until channel kernel gating lands` |
| `compaction_safeguard` | `false` | `default` | `gated_production` | `session compaction` | operator diagnostics: /console/v1/diagnostics, /admin/v1/status, palyra doctor --json | `feature_rollouts.compaction_safeguard` | `PALYRA_EXPERIMENTAL_COMPACTION_SAFEGUARD` | `Enable feature_rollouts.compaction_safeguard or PALYRA_EXPERIMENTAL_COMPACTION_SAFEGUARD after required tests and owner acceptance pass.`<br>`compaction decisions must be replay-safe and expose bounded evidence` |
| `context_engine` | `false` | `default` | `preview_only` | `application/context_engine` | operator diagnostics: /console/v1/diagnostics, /admin/v1/status, palyra doctor --json | `feature_rollouts.context_engine` | `PALYRA_EXPERIMENTAL_CONTEXT_ENGINE` | `Enable feature_rollouts.context_engine or PALYRA_EXPERIMENTAL_CONTEXT_ENGINE after required tests and owner acceptance pass.`<br>`context assembly traces must stay redacted and replay-compatible before production rollout` |
| `delivery_arbitration` | `false` | `default` | `preview_only` | `channel delivery` | runtime preview controls plus operator diagnostics and doctor | `feature_rollouts.delivery_arbitration` | `PALYRA_EXPERIMENTAL_DELIVERY_ARBITRATION` | `Enable feature_rollouts.delivery_arbitration or PALYRA_EXPERIMENTAL_DELIVERY_ARBITRATION after required tests and owner acceptance pass.`<br>`delivery arbitration depends on active flow orchestration and bounded suppression` |
| `dynamic_tool_builder` | `false` | `default` | `scaffold` | `skills/tool runtime` | internal runtime flag; externally visible only through diagnostics and doctor | `feature_rollouts.dynamic_tool_builder` | `PALYRA_EXPERIMENTAL_DYNAMIC_TOOL_BUILDER` | `Enable feature_rollouts.dynamic_tool_builder or PALYRA_EXPERIMENTAL_DYNAMIC_TOOL_BUILDER after required tests and owner acceptance pass.`<br>`builder output is not yet covered by signed skill artifact compatibility tests` |
| `execution_backend_docker` | `false` | `default` | `blocked` | `execution backends` | internal runtime flag; externally visible only through diagnostics and doctor | `feature_rollouts.execution_backend_docker` | `PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_DOCKER` | `Enable feature_rollouts.execution_backend_docker or PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_DOCKER after required tests and owner acceptance pass.`<br>`Docker runner isolation and patch writeback parity are still roadmap-gated` |
| `execution_backend_networked_worker` | `false` | `default` | `blocked` | `workerd/execution backends` | runtime preview controls plus operator diagnostics and doctor | `feature_rollouts.execution_backend_networked_worker` | `PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_NETWORKED_WORKER` | `Enable feature_rollouts.execution_backend_networked_worker or PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_NETWORKED_WORKER after required tests and owner acceptance pass.`<br>`networked worker execution requires worker attestation and policy-bound remote tool subsets` |
| `execution_backend_remote_node` | `false` | `default` | `blocked` | `execution backends` | internal runtime flag; externally visible only through diagnostics and doctor | `feature_rollouts.execution_backend_remote_node` | `PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_REMOTE_NODE` | `Enable feature_rollouts.execution_backend_remote_node or PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_REMOTE_NODE after required tests and owner acceptance pass.`<br>`remote-node runner contract is not production-backed by attestation and cleanup evidence` |
| `execution_backend_ssh_tunnel` | `false` | `default` | `blocked` | `execution backends` | internal runtime flag; externally visible only through diagnostics and doctor | `feature_rollouts.execution_backend_ssh_tunnel` | `PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_SSH_TUNNEL` | `Enable feature_rollouts.execution_backend_ssh_tunnel or PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_SSH_TUNNEL after required tests and owner acceptance pass.`<br>`SSH worker RPC envelope and transport trust chain are not complete` |
| `execution_gate_pipeline_v2` | `false` | `default` | `preview_only` | `execution gate` | internal runtime flag; externally visible only through diagnostics and doctor | `feature_rollouts.execution_gate_pipeline_v2` | `PALYRA_EXPERIMENTAL_EXECUTION_GATE_PIPELINE_V2` | `Enable feature_rollouts.execution_gate_pipeline_v2 or PALYRA_EXPERIMENTAL_EXECUTION_GATE_PIPELINE_V2 after required tests and owner acceptance pass.`<br>`execution gate v2 must keep denial and degraded outcomes byte-stable before production` |
| `flow_orchestration` | `false` | `default` | `preview_only` | `flow orchestration` | runtime preview controls plus operator diagnostics and doctor | `feature_rollouts.flow_orchestration` | `PALYRA_EXPERIMENTAL_FLOW_ORCHESTRATION` | `Enable feature_rollouts.flow_orchestration or PALYRA_EXPERIMENTAL_FLOW_ORCHESTRATION after required tests and owner acceptance pass.`<br>`flow cancellation gates and retry budgets must be replay-visible` |
| `networked_workers` | `false` | `default` | `blocked` | `workerd/execution backends` | runtime preview controls plus operator diagnostics and doctor | `feature_rollouts.networked_workers` | `PALYRA_EXPERIMENTAL_NETWORKED_WORKERS` | `Enable feature_rollouts.networked_workers or PALYRA_EXPERIMENTAL_NETWORKED_WORKERS after required tests and owner acceptance pass.`<br>`networked workers also require feature_rollouts.execution_backend_networked_worker` |
| `objective_judge` | `false` | `default` | `preview_only` | `objective judge` | operator diagnostics: /console/v1/diagnostics, /admin/v1/status, palyra doctor --json | `feature_rollouts.objective_judge` | `PALYRA_EXPERIMENTAL_OBJECTIVE_JUDGE` | `Enable feature_rollouts.objective_judge or PALYRA_EXPERIMENTAL_OBJECTIVE_JUDGE after required tests and owner acceptance pass.`<br>`judge outcomes must stay advisory and replay-visible until acceptance gates land` |
| `progress_drafts` | `false` | `default` | `preview_only` | `progress drafts` | operator diagnostics: /console/v1/diagnostics, /admin/v1/status, palyra doctor --json | `feature_rollouts.progress_drafts` | `PALYRA_EXPERIMENTAL_PROGRESS_DRAFTS` | `Enable feature_rollouts.progress_drafts or PALYRA_EXPERIMENTAL_PROGRESS_DRAFTS after required tests and owner acceptance pass.`<br>`draft projection must not expose hidden transcript or secret material` |
| `provider_backed_evidence_compaction` | `false` | `default` | `preview_only` | `session compaction` | operator diagnostics: /console/v1/diagnostics, /admin/v1/status, palyra doctor --json | `feature_rollouts.provider_backed_evidence_compaction` | `PALYRA_EXPERIMENTAL_PROVIDER_BACKED_EVIDENCE_COMPACTION` | `Enable feature_rollouts.provider_backed_evidence_compaction or PALYRA_EXPERIMENTAL_PROVIDER_BACKED_EVIDENCE_COMPACTION after required tests and owner acceptance pass.`<br>`provider-backed compaction must degrade to local evidence when provider calls fail` |
| `provider_stream_normalizer` | `false` | `default` | `preview_only` | `model provider streaming` | internal runtime flag; externally visible only through diagnostics and doctor | `feature_rollouts.provider_stream_normalizer` | `PALYRA_EXPERIMENTAL_PROVIDER_STREAM_NORMALIZER` | `Enable feature_rollouts.provider_stream_normalizer or PALYRA_EXPERIMENTAL_PROVIDER_STREAM_NORMALIZER after required tests and owner acceptance pass.`<br>`stream normalization needs provider compatibility fixtures before stable rollout` |
| `pruning_policy_matrix` | `false` | `default` | `preview_only` | `memory/context pruning` | runtime preview controls plus operator diagnostics and doctor | `feature_rollouts.pruning_policy_matrix` | `PALYRA_EXPERIMENTAL_PRUNING_POLICY_MATRIX` | `Enable feature_rollouts.pruning_policy_matrix or PALYRA_EXPERIMENTAL_PRUNING_POLICY_MATRIX after required tests and owner acceptance pass.`<br>`manual apply and token-savings thresholds must be visible before production pruning` |
| `replay_capture` | `false` | `default` | `preview_only` | `replay` | runtime preview controls plus operator diagnostics and doctor | `feature_rollouts.replay_capture` | `PALYRA_EXPERIMENTAL_REPLAY_CAPTURE` | `Enable feature_rollouts.replay_capture or PALYRA_EXPERIMENTAL_REPLAY_CAPTURE after required tests and owner acceptance pass.`<br>`runtime decision capture must respect replay redaction limits` |
| `retrieval_dual_path` | `false` | `default` | `preview_only` | `memory/retrieval` | runtime preview controls plus operator diagnostics and doctor | `feature_rollouts.retrieval_dual_path` | `PALYRA_EXPERIMENTAL_RETRIEVAL_DUAL_PATH` | `Enable feature_rollouts.retrieval_dual_path or PALYRA_EXPERIMENTAL_RETRIEVAL_DUAL_PATH after required tests and owner acceptance pass.`<br>`branch timeout and prompt budget limits must stay bounded in runtime preview controls` |
| `safety_boundary` | `false` | `default` | `preview_only` | `safety` | operator diagnostics: /console/v1/diagnostics, /admin/v1/status, palyra doctor --json | `feature_rollouts.safety_boundary` | `PALYRA_EXPERIMENTAL_SAFETY_BOUNDARY` | `Enable feature_rollouts.safety_boundary or PALYRA_EXPERIMENTAL_SAFETY_BOUNDARY after required tests and owner acceptance pass.`<br>`safety transforms must preserve prompt-injection and secret-redaction regression coverage` |
| `session_queue_policy` | `false` | `default` | `preview_only` | `session lifecycle` | runtime preview controls plus operator diagnostics and doctor | `feature_rollouts.session_queue_policy` | `PALYRA_EXPERIMENTAL_SESSION_QUEUE_POLICY` | `Enable feature_rollouts.session_queue_policy or PALYRA_EXPERIMENTAL_SESSION_QUEUE_POLICY after required tests and owner acceptance pass.`<br>`queue depth and merge-window limits must remain bounded in preview controls` |
| `tool_repair` | `false` | `default` | `gated_production` | `run stream/tool repair` | operator diagnostics: /console/v1/diagnostics, /admin/v1/status, palyra doctor --json | `feature_rollouts.tool_repair` | `PALYRA_EXPERIMENTAL_TOOL_REPAIR` | `Enable feature_rollouts.tool_repair or PALYRA_EXPERIMENTAL_TOOL_REPAIR after required tests and owner acceptance pass.`<br>`tool repair must keep proposed fixes replay-safe and operator-auditable` |
| `verification_runtime` | `false` | `default` | `gated_production` | `verification runtime` | operator diagnostics: /console/v1/diagnostics, /admin/v1/status, palyra doctor --json | `feature_rollouts.verification_runtime` | `PALYRA_EXPERIMENTAL_VERIFICATION_RUNTIME` | `Enable feature_rollouts.verification_runtime or PALYRA_EXPERIMENTAL_VERIFICATION_RUNTIME after required tests and owner acceptance pass.`<br>`verification evidence must remain redacted and durable before stable rollout` |

## Compat Routes

| Method | Path | Registered |
| --- | --- | --- |
| `GET` | `/v1/models` | `true` |
| `GET` | `/v1/models/compat-probe` | `true` |
| `POST` | `/v1/embeddings` | `true` |
| `POST` | `/v1/chat/completions` | `true` |
| `POST` | `/v1/responses` | `true` |
| `POST` | `/v1/tools/invoke` | `true` |
