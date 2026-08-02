# Runtime Incident Runbooks

These runbooks cover the P0 runtime surfaces listed in
[Release Acceptance Dashboard](release_acceptance_dashboard.md). They are
operator guidance for diagnosing, mitigating, and rolling back runtime incidents
without exposing prompts, credentials, raw provider payloads, or local paths.

## Shared Evidence

- Release status: [Release Acceptance Dashboard](release_acceptance_dashboard.md)
- Runtime path diagnostics: inspect `/console/v1/diagnostics` under
  `run_runtime_path` and `runtime_diagnostics.run_runtime_path`.
- Support bundle diagnostics: export with
  `palyra support-bundle export --output <path>` and verify that payloads are
  redacted before sharing outside the operator boundary.
- Admin health: inspect `/admin/v1/status` and `/admin/v1/metrics` for bounded,
  low-cardinality runtime health and counter evidence.

## Security Invariants

- Secrets remain vault-owned and never enter diagnostics, support bundles,
  model-visible tool results, stdout/stderr previews, or replay fixtures.
- Approval denial is terminal for mutating tools unless a new operator decision
  is recorded.
- Policy, execution gate, sandbox, and process-runner checks remain host-owned.
- External harnesses, plugins, ACP actors, and middleware never receive direct
  journal write authority.
- Runtime decisions use stable reason codes, redacted metadata, and replay-safe
  event names before model-visible projection.

## Agent Harness Runtime

### Symptoms

- Runs do not start after harness selection.
- Terminal events are missing `harness.run.cleaned_up`.
- Runtime path diagnostics show a harness owner that does not match the
  configured rollout posture.

### Diagnostics

- Check `/console/v1/diagnostics` for harness rollout posture and attempt owner.
- Run `cargo test -p palyra-daemon --test current_state_inventory --locked`.
- Compare run-stream tape events against the golden trajectory fixtures.

### Safe Mitigation

- Disable the harness rollout flag and keep the embedded run-stream path active.
- Preserve support bundle evidence before restarting the daemon.

### Rollback

- Revert the rollout flag to preview or disabled posture.
- Re-run the inventory drift check and confirm the dashboard remains
  `preview_only` until all required gates pass.

## Execution Gate Pipeline

### Symptoms

- Mutating tools run without a gate decision.
- Denied actions retry as continuations.
- Audit records are missing stable denial reason codes.

### Diagnostics

- Inspect tool security diagnostics and approval denial events.
- Run `bash scripts/test/run-critical-attack-scenarios.sh`.
- Check that model-visible tool projections never exceed audit visibility.

### Safe Mitigation

- Force the execution gate pipeline to fail closed.
- Quarantine affected tool families until denial and approval paths are
  deterministic again.

### Rollback

- Disable the critical-path rollout knob for the new gate path.
- Keep legacy host-owned checks enabled until targeted gate tests are green.

## Provider Recovery

### Symptoms

- Malformed stream chunks terminate turns without bounded recovery events.
- Provider auth failures loop instead of failing with safe metadata.
- Runtime diagnostics show recovery counters without terminal outcomes.

### Diagnostics

- Run provider stream recovery fixtures.
- Inspect `/console/v1/diagnostics` for provider recovery observations.
- Verify that support bundles contain redacted provider metadata only.

### Safe Mitigation

- Disable provider repair retries for the affected provider.
- Prefer explicit failure over speculative stream reconstruction when evidence
  is incomplete.

### Rollback

- Revert provider recovery rollout posture to preview.
- Keep normalized stream fixtures as the required promotion gate.

## Replay Capture

### Symptoms

- Replay fixtures contain prompts, secrets, or local paths.
- Captured events cannot reproduce terminal state.
- Golden replay checks drift without a reviewed source change.

### Diagnostics

- Run `bash scripts/test/run-replay-gate.sh`.
- Inspect fixture diffs for metadata-only redaction and stable event names.
- Compare failed captures with support bundle runtime path diagnostics.

### Safe Mitigation

- Stop publishing new replay fixtures from the affected runtime path.
- Keep live runs operational only if diagnostics remain redacted and bounded.

### Rollback

- Disable the capture path and restore the last known-good golden fixtures.
- Re-run replay and current-state inventory checks before promotion.

## Continuity Recovery Blocked or Unknown

### Symptoms

- `ContinuityCampaignReport` records `confirmation_required` or `terminalized`
  where a scenario was expected to resume.
- A recovery record has a stable `continuity.*` failure class but lacks one of
  the journal, metadata trace, cleanup, or final-outcome evidence references.
- Delivery or a mutating effect may have completed before acknowledgement.

### Diagnostics

- Run `just continuity-crash-campaign` and preserve
  `target/release-artifacts/continuity-campaign/report.json`.
- Match the failed case by scenario, crash boundary, reason code, and evidence
  references; do not infer success from process exit status alone.
- Inspect the authorized metadata trace and cleanup report for the exact run
  generation. Treat missing, corrupt, or generation-mismatched evidence as
  blocked.

### Safe Mitigation

- For `confirmation_required`, suspend automatic replay and obtain an operator
  decision using the durable confirmation surface.
- For `terminalized`, preserve the terminal outcome and start new work with a
  new idempotency identity only after cleanup is observable.
- For unknown delivery acknowledgement, do not resend until the delivery
  arbitration record proves that the original send was not confirmed.

### Rollback

- Return affected recovery paths to observe-only behavior while retaining all
  journal, metadata trace, and cleanup evidence.
- Do not delete recovery records, reuse a stale generation lease, or repeat a
  confirmed side effect during rollback.
- Re-run the full continuity gate on both Windows and Unix before restoring
  automatic recovery.

## Verification Runtime

### Symptoms

- Verification claims pass without linked evidence.
- Support bundles disagree with runtime status counters.
- Release acceptance marks a runtime stable without release review.

### Diagnostics

- Inspect `/admin/v1/status`, `/admin/v1/metrics`, and support bundle payloads.
- Run `cargo test -p palyra-daemon --locked runtime_diagnostics`.
- Confirm dashboard blockers still require release review for stable promotion.

### Safe Mitigation

- Treat verification output as advisory until counters and support bundles
  agree.
- Keep the runtime in gated production while evidence is incomplete.

### Rollback

- Revert verification maturity to preview or gated production as appropriate.
- Require release dashboard review before restoring stable candidacy.

## Compaction Safeguard

### Symptoms

- Mutating tool state changes after compaction retry.
- Transcript continuity loses approval or policy context.
- Runtime path diagnostics do not show compaction posture.

### Diagnostics

- Run compaction retry fixtures and replay gate checks.
- Inspect transcript and run path metadata for redacted continuity markers.
- Verify that support bundles exclude raw prompts and tool arguments.

### Safe Mitigation

- Disable automatic continuation through compaction for mutating workflows.
- Require a fresh operator approval when continuity evidence is incomplete.

### Rollback

- Revert the compaction safeguard rollout to gated production or preview.
- Keep replay gate evidence attached to any promotion attempt.

## Advisor Fanout

### Symptoms

- Advisor outputs are treated as authoritative decisions.
- Fanout tasks write directly to journal or tool state.
- Diagnostics include high-cardinality advisor payload labels.

### Diagnostics

- Inspect advisor runtime diagnostics for non-authoritative projection markers.
- Verify host authority checklist evidence.
- Confirm support bundles contain bounded summary metadata only.

### Safe Mitigation

- Disable advisor fanout and keep single-owner host decisions active.
- Drop advisor results that lack source and projection metadata.

### Rollback

- Return advisor fanout to preview-only posture.
- Re-run host authority boundary checks before re-enabling.

## LSP Crash

### Symptoms

- LSP startup failures block unrelated runtime work.
- Diagnostics stream full source files or local paths.
- Process cleanup leaves stale server handles.

### Diagnostics

- Inspect LSP diagnostics deltas and process-runner cleanup evidence.
- Run code-intelligence focused tests.
- Check support bundles for redacted path and source previews.

### Safe Mitigation

- Disable the affected LSP adapter and keep file-based diagnostics available.
- Cancel stale process handles through the host-owned process runner.

### Rollback

- Restore preview-only LSP posture.
- Require unavailable-server fallback coverage before promotion.

## Plugin Quarantine

### Symptoms

- A plugin generation reports a contract violation, fuel or memory exhaustion,
  forbidden capability access, or an invalid ABI result.
- Diagnostics show repeated quarantine attempts or a quarantined generation
  still present in the model-visible catalog.

### Diagnostics

- Preserve the stable reason code, plugin generation, contract kind, and
  bounded cleanup outcome from `/console/v1/diagnostics`.
- Run the plugin ABI v2 conformance suite and the critical attack scenarios.
- Verify the support bundle contains no plugin payload, secret value, local
  path, or raw Wasm bytes.

### Safe Mitigation

- Quarantine the affected plugin generation and remove it from catalog
  projection without granting fallback authority.
- Keep healthy built-in tools available through their normal policy and
  approval boundaries.

### Rollback

- Disable the affected plugin artifact or roll back to its last qualified,
  signed version.
- Preserve quarantine and invocation evidence; never replay an uncertain
  mutating call during rollback.

## PTY Orphan

### Symptoms

- A terminal command is terminal but its process lease remains active.
- Cleanup reports a surviving child or an unclosed PTY handle after the bounded
  drain deadline.

### Diagnostics

- Preserve the runtime handle, process lease generation, cleanup reason code,
  and bounded child-count evidence.
- Inspect managed-coding diagnostics without copying command input, terminal
  output, environment values, or workspace paths.
- Run the managed process cleanup and PTY lifecycle tests.

### Safe Mitigation

- Stop new commands for the affected lease owner and request host-owned
  process-tree cleanup.
- If cleanup cannot be proven, quarantine the execution environment and require
  operator review before reuse.

### Rollback

- Drain managed coding admission before release rollback.
- Preserve cleanup reports and worktree metadata; never treat a missing process
  as proof that its side effect did not occur.

## WorkGraph Stale Claim

### Symptoms

- A claim lease expires without terminal work evidence.
- A late result arrives for an older claim generation.
- The same ready node appears owned by more than one worker.

### Diagnostics

- Inspect the work item, claim generation, lease state, reclaim reason code, and
  late-result suppression counter.
- Run WorkGraph claim, stale-reclaim, and cancellation regression tests.
- Do not include task payloads, worker credentials, or raw error messages in the
  incident record.

### Safe Mitigation

- Reclaim only after the durable lease proves expiry and assign a new claim
  generation.
- Reject late results from stale generations and reconcile uncertain effects
  before any retry.

### Rollback

- Stop new claims, drain active leases, and retain the graph and claim ledger
  through release rollback.
- Never delete a stale claim or accept its late result to make the graph appear
  healthy.

## Core Release Rollback

### Symptoms

- A stable-core SLI reaches its critical threshold.
- A required gate, owner sign-off, support-bundle check, or legacy retirement
  invariant no longer qualifies.
- Diagnostics report `core.stable.release_blocked`.

### Diagnostics

- Export the redacted support bundle and preserve the stable evidence pack,
  alert state, runtime generation, cleanup status, and release version.
- Identify the breached capability and stable reason code without copying raw
  prompts, tool payloads, secrets, local paths, or high-cardinality identifiers.
- Run the release dashboard checker and the smallest affected runtime gate.

### Safe Mitigation

- Stop widening admission, drain affected actors, and quarantine only the
  failing generation or capability where its contract permits isolation.
- Suspend retries for uncertain effects and keep durable reconciliation records
  available to the prior release.

### Rollback

- Roll back to the prior qualified release; do not activate a hidden legacy
  branch inside the current process.
- Preserve journals, metadata traces, claims, catalog epochs, quarantine
  records, and cleanup evidence.
- Confirm the prior release can read the preserved data before restoring
  admission, and never repeat a confirmed side effect.

## Routine Scheduler

### Symptoms

- Cron jobs fire twice for the same schedule slot.
- Lease heartbeat timestamps stop advancing.
- Startup catch-up creates an unbounded queue or repeated failures never reach a
  dead-letter state.

### Diagnostics

- Inspect routine lease ledger entries, run ids, idempotency keys, and
  heartbeat epochs.
- Check `/admin/v1/status` for routine lease, catch-up, and cron security
  schema versions.
- Verify delivery targets and provider/model snapshots in routine diagnostics.

### Safe Mitigation

- Pause affected routines and preserve the lease ledger before restart.
- Cap startup catch-up to the configured missed-job limit.
- Route repeated failures to operator review instead of retrying indefinitely.

### Rollback

- Disable the routine rollout that introduced the incident.
- Keep existing routines paused until duplicate-fire and dead-letter evidence is
  reviewed.

## ACP Runtime

### Symptoms

- ACP actors bypass host-owned permission relay.
- Runtime registry exposes unredacted resources or model state.
- Method maturity claims production while required lifecycle methods are still
  preview-only.

### Diagnostics

- Inspect ACP runtime registry maturity and rollback preview metadata.
- Verify create, list, fork, wait, cancel, and delete method status.
- Confirm support bundles expose only redacted ACP resources and model posture.

### Safe Mitigation

- Keep ACP runtime methods in preview unless every lifecycle gate is complete.
- Route all permission requests through the host-owned approval broker.

### Rollback

- Revert ACP runtime maturity to preview.
- Disable production registration until permission relay and lifecycle tests are
  green.

## MCP Persistent Runtime Outage

### Symptoms

- A configured server remains in `handshaking`, repeatedly enters
  `reconnecting`, or becomes `quarantined`.
- A catalog epoch changes during active work and subsequent calls report stale
  schema or generation evidence.
- Stdio cleanup reports `mcp.runtime.stdio.cleanup_incomplete`, or daemon drain
  reports an MCP actor that did not stop within its bounded deadline.
- OAuth refresh, elicitation, or sampling callbacks are denied with a stable
  `mcp.runtime.*` reason code.

### Diagnostics

- Run `palyra mcp doctor <server-id> --json` and preserve the redacted lifecycle,
  generation, catalog epoch, and reason codes.
- Run `palyra mcp probe <server-id> --json` to distinguish a transport failure
  from catalog or policy rejection.
- Run `palyra mcp tools <server-id> --json` and verify that quarantined servers
  are absent from the current catalog epoch.
- Inspect `/admin/v1/status` and `/console/v1/diagnostics` for the bounded MCP
  supervisor snapshot. Do not copy raw stderr, credentials, endpoint query
  strings, or server payloads into an incident record.
- For stdio incidents, confirm that cleanup evidence identifies a completed
  process-tree close before restarting the daemon.

### Safe Mitigation

- Quarantine the affected server generation and allow healthy core tools to
  continue. Do not bypass broker policy, approval, output, or redaction gates.
- Correct the server config or credential reference, then run
  `palyra mcp reload --path <config-path> --dry-run --json`.
- Apply the reload only after the dry run identifies the intended server and no
  unrelated config changes. A new connection must take sole ownership of a new
  generation; never run a parallel per-call transport as a fallback.
- If bounded drain cannot prove transport or process cleanup, leave the server
  stopped and escalate with the redacted lifecycle evidence.

### Rollback

- Disable the affected server in configuration and reload through the normal
  config path.
- Preserve durable lifecycle, catalog, policy, conformance, and cleanup
  evidence; rollback must not delete it or replay an uncertain side effect.
- Re-enable only after `doctor`, `probe`, catalog inspection, restart recovery,
  and bounded drain all succeed for one owner generation.
