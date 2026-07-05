# Runtime boundary decision matrix

This report is the Phase 1 implementation map for the agent-runtime roadmap.
It names the existing Palyra surfaces that future milestones must extend
instead of creating parallel runtimes.

## Scope exclusions

Connector count, skill count, provider count, UI polish, the root README
scaffold, Tauri work, and web app polish are intentionally out of scope for
this roadmap wave. They do not unblock the backend runtime boundaries below.

## Security invariants for every milestone

- Raw secrets remain vault-owned and never enter diagnostics, support bundles,
  model-visible tool results, stdout/stderr previews, or replay fixtures.
- Approval denial is terminal for mutating tools and cannot be downgraded into
  a retry or continuation without a new operator decision.
- Policy, execution gate, sandbox, and process-runner checks remain host-owned.
- External harnesses, plugins, ACP actors, and middleware never receive direct
  journal write authority; they use host callbacks or typed request envelopes.
- Runtime decisions use stable reason codes, redacted metadata, and replay-safe
  event names before model-visible projection.

## Subsystem map

| Area | Current state | Target state | Priority | Existing source anchors | Ordering reason |
| --- | --- | --- | --- | --- | --- |
| Agent harness | Preview metadata exists in `crates/palyra-daemon/src/application/agent_harness.rs` and `crates/palyra-common/src/runtime_contracts.rs`. | Selection, lifecycle, callbacks, transcript mirror, and authority fences route through a default-off harness runtime. | P0 | `crates/palyra-common/src/runtime_roadmap.rs`, `crates/palyra-daemon/src/config/schema.rs` | Later hook, middleware, ACP, and external-harness work depends on a single host-owned execution boundary. |
| Run stream and tape | Existing run stream/tape helpers and public runtime events are snapshot-tested. | Golden trajectory fixtures compare user-visible, model-visible, and internal audit event sequences. | P0 | `crates/palyra-daemon/src/application/run_stream/`, `fixtures/golden/runtime_roadmap_phase1_trajectories.json` | Runtime changes need deterministic trajectory evidence before behavior changes. |
| Execution gate | Existing gate and process policies are present but new critical-path pipeline remains rollout-gated. | Gate decisions expose reason codes and denial paths before every side effect. | P0 | `crates/palyra-daemon/src/application/tool_security.rs`, `crates/palyra-daemon/src/config/schema.rs` | Tool bridge and middleware cannot safely activate before denial semantics are pinned. |
| Hook runtime | Hook surfaces and CLI family exist before inline critical-path call sites. | Inline hooks use timeout, panic, policy, approval, and audit fail-closed semantics. | P0 | `crates/palyra-daemon/src/hooks.rs`, `crates/palyra-common/src/runtime_contracts.rs` | Hook call sites can affect all runs, so they must follow harness and gate contracts. |
| Provider stream | Provider normalizer rollout exists, with additional recovery still default-off. | Stream normalization, repair, retries, and auth failover emit bounded recovery events. | P0 | `crates/palyra-daemon/src/model_provider/`, `crates/palyra-model-providers/` | Provider recovery feeds tool-call repair and turn recovery in later phases. |
| Tool result middleware | Public runtime contracts describe model-visible and audit projections. | Middleware may only preserve or downgrade model visibility while retaining host-owned audit artifacts. | P0 | `crates/palyra-common/src/runtime_contracts.rs`, `crates/palyra-daemon/src/application/tool_runtime/` | Middleware is on the critical model-visible path and must not precede gate invariants. |
| Terminal sessions | Process runner and execution backends exist behind conservative defaults. | Persistent sessions expose bounded cwd/env state, process handles, cleanup evidence, and sudo/disk guards. | P1 | `crates/palyra-daemon/src/sandbox_runner.rs`, `crates/palyra-daemon/src/execution_backends.rs` | Long-running coding workflows need runtime handles after core harness safety is stable. |
| LSP/code intelligence | Code diagnostics adapters exist as disabled process-runner-backed config. | LSP lifecycle and diagnostics delta feed model-visible code-intelligence tools through workspace-scoped contracts. | P1 | `crates/palyra-daemon/src/application/tool_runtime/code_intel.rs`, `crates/palyra-daemon/src/config/schema.rs` | Code intelligence depends on process safety and bounded diagnostics. |
| Browser rescue | Browserd and browser service config exist behind disabled defaults. | Rescue tools, vision, dialog handling, and CDP escape hatches are policy-gated and redacted. | P1 | `crates/palyra-browserd/`, `crates/palyra-daemon/src/config/schema.rs` | Browser rescue depends on provider and multimodal recovery boundaries. |
| ACP runtime | ACP contracts, validation, and console surfaces exist. | ACP runtime actors, permission relay, replay translators, and compaction handoff use host-owned queues. | P1 | `crates/palyra-daemon/src/acp/`, `crates/palyra-common/src/runtime_contracts.rs` | ACP should reuse harness, permission, and replay contracts instead of defining a separate authority model. |
| Learning lifecycle | Learning diagnostics and candidate counters exist. | Learning candidates remain scoped, reviewable, rollbackable, and excluded from prompt context until approved. | P2 | `crates/palyra-daemon/src/application/learning/`, `crates/palyra-daemon/src/transport/http/handlers/console/diagnostics.rs` | Learning should consume stable post-run artifacts, not shape core execution semantics. |

## Phase 1 implementation anchors

- Rollout flags: `crates/palyra-common/src/feature_rollouts.rs`
- Validated daemon config: `crates/palyra-daemon/src/config/schema.rs`
- Config loader precedence: `crates/palyra-daemon/src/config/load.rs`
- Maturity diagnostics: `crates/palyra-daemon/src/feature_rollout_maturity.rs`
- Runtime roadmap contracts: `crates/palyra-common/src/runtime_roadmap.rs`
- Operator diagnostics: `crates/palyra-daemon/src/transport/http/handlers/console/diagnostics.rs`
