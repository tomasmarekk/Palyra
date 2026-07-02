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
- Compat routes registered: `6/6`
- Feature rollout flags: `26`
- Runtime preview controls: `8` capabilities

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

| Flag | Enabled | Source | Config path | Env var |
| --- | --- | --- | --- | --- |
| `agent_plan_state` | `false` | `default` | `feature_rollouts.agent_plan_state` | `PALYRA_EXPERIMENTAL_AGENT_PLAN_STATE` |
| `attack_surface_audit` | `false` | `default` | `feature_rollouts.attack_surface_audit` | `PALYRA_EXPERIMENTAL_ATTACK_SURFACE_AUDIT` |
| `auxiliary_executor` | `false` | `default` | `feature_rollouts.auxiliary_executor` | `PALYRA_EXPERIMENTAL_AUXILIARY_EXECUTOR` |
| `channel_turn_kernel` | `false` | `default` | `feature_rollouts.channel_turn_kernel` | `PALYRA_EXPERIMENTAL_CHANNEL_TURN_KERNEL` |
| `compaction_safeguard` | `false` | `default` | `feature_rollouts.compaction_safeguard` | `PALYRA_EXPERIMENTAL_COMPACTION_SAFEGUARD` |
| `context_engine` | `false` | `default` | `feature_rollouts.context_engine` | `PALYRA_EXPERIMENTAL_CONTEXT_ENGINE` |
| `delivery_arbitration` | `false` | `default` | `feature_rollouts.delivery_arbitration` | `PALYRA_EXPERIMENTAL_DELIVERY_ARBITRATION` |
| `dynamic_tool_builder` | `false` | `default` | `feature_rollouts.dynamic_tool_builder` | `PALYRA_EXPERIMENTAL_DYNAMIC_TOOL_BUILDER` |
| `execution_backend_docker` | `false` | `default` | `feature_rollouts.execution_backend_docker` | `PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_DOCKER` |
| `execution_backend_networked_worker` | `false` | `default` | `feature_rollouts.execution_backend_networked_worker` | `PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_NETWORKED_WORKER` |
| `execution_backend_remote_node` | `false` | `default` | `feature_rollouts.execution_backend_remote_node` | `PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_REMOTE_NODE` |
| `execution_backend_ssh_tunnel` | `false` | `default` | `feature_rollouts.execution_backend_ssh_tunnel` | `PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_SSH_TUNNEL` |
| `execution_gate_pipeline_v2` | `false` | `default` | `feature_rollouts.execution_gate_pipeline_v2` | `PALYRA_EXPERIMENTAL_EXECUTION_GATE_PIPELINE_V2` |
| `flow_orchestration` | `false` | `default` | `feature_rollouts.flow_orchestration` | `PALYRA_EXPERIMENTAL_FLOW_ORCHESTRATION` |
| `networked_workers` | `false` | `default` | `feature_rollouts.networked_workers` | `PALYRA_EXPERIMENTAL_NETWORKED_WORKERS` |
| `objective_judge` | `false` | `default` | `feature_rollouts.objective_judge` | `PALYRA_EXPERIMENTAL_OBJECTIVE_JUDGE` |
| `progress_drafts` | `false` | `default` | `feature_rollouts.progress_drafts` | `PALYRA_EXPERIMENTAL_PROGRESS_DRAFTS` |
| `provider_backed_evidence_compaction` | `false` | `default` | `feature_rollouts.provider_backed_evidence_compaction` | `PALYRA_EXPERIMENTAL_PROVIDER_BACKED_EVIDENCE_COMPACTION` |
| `provider_stream_normalizer` | `false` | `default` | `feature_rollouts.provider_stream_normalizer` | `PALYRA_EXPERIMENTAL_PROVIDER_STREAM_NORMALIZER` |
| `pruning_policy_matrix` | `false` | `default` | `feature_rollouts.pruning_policy_matrix` | `PALYRA_EXPERIMENTAL_PRUNING_POLICY_MATRIX` |
| `replay_capture` | `false` | `default` | `feature_rollouts.replay_capture` | `PALYRA_EXPERIMENTAL_REPLAY_CAPTURE` |
| `retrieval_dual_path` | `false` | `default` | `feature_rollouts.retrieval_dual_path` | `PALYRA_EXPERIMENTAL_RETRIEVAL_DUAL_PATH` |
| `safety_boundary` | `false` | `default` | `feature_rollouts.safety_boundary` | `PALYRA_EXPERIMENTAL_SAFETY_BOUNDARY` |
| `session_queue_policy` | `false` | `default` | `feature_rollouts.session_queue_policy` | `PALYRA_EXPERIMENTAL_SESSION_QUEUE_POLICY` |
| `tool_repair` | `false` | `default` | `feature_rollouts.tool_repair` | `PALYRA_EXPERIMENTAL_TOOL_REPAIR` |
| `verification_runtime` | `false` | `default` | `feature_rollouts.verification_runtime` | `PALYRA_EXPERIMENTAL_VERIFICATION_RUNTIME` |

## Compat Routes

| Method | Path | Registered |
| --- | --- | --- |
| `GET` | `/v1/models` | `true` |
| `GET` | `/v1/models/compat-probe` | `true` |
| `POST` | `/v1/embeddings` | `true` |
| `POST` | `/v1/chat/completions` | `true` |
| `POST` | `/v1/responses` | `true` |
| `POST` | `/v1/tools/invoke` | `true` |
