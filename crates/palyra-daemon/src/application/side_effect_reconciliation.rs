//! Durable, strategy-specific reconciliation receipts for uncertain tool effects.
//!
//! The tool registry declares *which* evidence can reconcile an operation.
//! This module is the runtime consumer: it records a bounded receipt after a
//! strategy-specific outcome is observed, then resolves `effect_unknown` only
//! when that exact receipt survives on the run tape.

use std::sync::Arc;

use palyra_common::runtime_contracts::{
    ProcessProvenance, ReconciliationStrategy, RuntimeGeneration, RuntimeOperationId,
    SideEffectFenceState,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tonic::Status;

use crate::{
    gateway::GatewayRuntimeState, journal::OrchestratorTapeAppendRequest,
    tool_protocol::ToolExecutionOutcome,
};

const RECONCILIATION_RECEIPT_EVENT_TYPE: &str = "tool.effect.reconciliation_receipt";
const RECONCILIATION_RECEIPT_SCHEMA_VERSION: u32 = 1;
const RECONCILIATION_TAPE_PAGE_LIMIT: usize = 128;
const MAX_RECONCILIATION_TAPE_EVENTS: usize = 4_096;

/// Exact fence binding carried by a strategy receipt.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SideEffectReconciliationBinding<'a> {
    pub(crate) operation_id: &'a RuntimeOperationId,
    pub(crate) generation: RuntimeGeneration,
    pub(crate) intent_sha256: &'a str,
    pub(crate) strategy: ReconciliationStrategy,
    pub(crate) external_idempotency_key_sha256: Option<&'a str>,
}

/// Result of an automatic reconciliation attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SideEffectReconciliationOutcome {
    /// An exact durable receipt proved that the original effect completed.
    Reconciled,
    /// No exact receipt exists, so the uncertain fence remains retry-blocking.
    Blocked { reason_code: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableSideEffectReconciliationReceiptV1 {
    schema_version: u32,
    operation_id: String,
    generation: u64,
    intent_sha256: String,
    proposal_id: String,
    tool_name: String,
    strategy: ReconciliationStrategy,
    receipt_kind: String,
    evidence_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    external_idempotency_key_sha256: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReceiptSelection {
    Exact,
    Absent,
    BindingMismatch,
    StaleGeneration,
    Invalid,
}

/// Persists a strategy-specific receipt before the result acknowledgement boundary.
///
/// A successful return of `false` means the outcome did not contain enough
/// evidence for that strategy. The caller may still settle the ordinary
/// result path, but an acknowledgement fault must remain `effect_unknown`.
#[allow(clippy::too_many_arguments, clippy::result_large_err)]
pub(crate) async fn record_side_effect_reconciliation_receipt(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    tool_name: &str,
    binding: SideEffectReconciliationBinding<'_>,
    outcome: &ToolExecutionOutcome,
) -> Result<bool, Status> {
    let Some(receipt) = receipt_from_outcome(proposal_id, tool_name, binding, outcome) else {
        return Ok(false);
    };
    let payload_json = serde_json::to_string(&receipt).map_err(|error| {
        Status::internal(format!("serialize tool side-effect reconciliation receipt: {error}"))
    })?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: RECONCILIATION_RECEIPT_EVENT_TYPE.to_owned(),
            payload_json,
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(true)
}

/// Reconciles an uncertain fence from an exact strategy receipt, if one exists.
///
/// Receipt evidence is historical, so its generation may precede the current
/// reconciliation generation after restart. A receipt from a generation newer
/// than the active reconciler is rejected. The fence transition itself always
/// uses the current generation and therefore never revives stale authority.
#[allow(clippy::too_many_arguments, clippy::result_large_err)]
pub(crate) async fn reconcile_unknown_tool_side_effect(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    binding: SideEffectReconciliationBinding<'_>,
) -> Result<SideEffectReconciliationOutcome, Status> {
    if binding.strategy == ReconciliationStrategy::None {
        return Ok(SideEffectReconciliationOutcome::Blocked {
            reason_code: "tool.effect.reconciliation.strategy_unavailable".to_owned(),
        });
    }

    let receipts = load_reconciliation_receipts(runtime_state, run_id).await?;
    let expected = DurableSideEffectReconciliationReceiptV1 {
        schema_version: RECONCILIATION_RECEIPT_SCHEMA_VERSION,
        operation_id: binding.operation_id.as_str().to_owned(),
        generation: binding.generation.get(),
        intent_sha256: binding.intent_sha256.to_owned(),
        proposal_id: proposal_id.to_owned(),
        tool_name: tool_name.to_owned(),
        strategy: binding.strategy,
        receipt_kind: receipt_kind(binding.strategy).to_owned(),
        evidence_sha256: String::new(),
        external_idempotency_key_sha256: binding.external_idempotency_key_sha256.map(str::to_owned),
    };
    let (selection, evidence_sha256) = select_exact_receipt(receipts.as_slice(), &expected);
    let reason_code = match selection {
        ReceiptSelection::Exact => reconciliation_reason_code(binding.strategy),
        ReceiptSelection::Absent => {
            return Ok(SideEffectReconciliationOutcome::Blocked {
                reason_code: blocked_reason_code(binding.strategy, "receipt_absent"),
            });
        }
        ReceiptSelection::BindingMismatch => {
            return Ok(SideEffectReconciliationOutcome::Blocked {
                reason_code: blocked_reason_code(binding.strategy, "receipt_mismatch"),
            });
        }
        ReceiptSelection::StaleGeneration => {
            return Ok(SideEffectReconciliationOutcome::Blocked {
                reason_code: blocked_reason_code(binding.strategy, "stale_generation"),
            });
        }
        ReceiptSelection::Invalid => {
            return Ok(SideEffectReconciliationOutcome::Blocked {
                reason_code: blocked_reason_code(binding.strategy, "receipt_invalid"),
            });
        }
    };
    runtime_state
        .transition_tool_side_effect_fence(
            binding.operation_id.clone(),
            SideEffectFenceState::Reconciled,
            binding.generation,
            reason_code.to_owned(),
            evidence_sha256,
        )
        .await?;
    Ok(SideEffectReconciliationOutcome::Reconciled)
}

async fn load_reconciliation_receipts(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
) -> Result<Vec<DurableSideEffectReconciliationReceiptV1>, Status> {
    let mut after_seq = None;
    let mut inspected = 0_usize;
    let mut receipts = Vec::new();
    loop {
        let snapshot = runtime_state
            .orchestrator_tape_snapshot(
                run_id.to_owned(),
                after_seq,
                Some(RECONCILIATION_TAPE_PAGE_LIMIT),
            )
            .await?;
        let page_event_count = snapshot.events.len();
        for event in snapshot.events {
            inspected = inspected.saturating_add(1);
            if event.event_type == RECONCILIATION_RECEIPT_EVENT_TYPE {
                // Invalid receipt events are retained as invalid sentinels so
                // the selector fails closed instead of treating corruption as
                // evidence absence.
                receipts.push(serde_json::from_str(event.payload_json.as_str()).unwrap_or_else(
                    |_| DurableSideEffectReconciliationReceiptV1 {
                        schema_version: 0,
                        operation_id: String::new(),
                        generation: 0,
                        intent_sha256: String::new(),
                        proposal_id: String::new(),
                        tool_name: String::new(),
                        strategy: ReconciliationStrategy::None,
                        receipt_kind: String::new(),
                        evidence_sha256: String::new(),
                        external_idempotency_key_sha256: None,
                    },
                ));
            }
            if inspected >= MAX_RECONCILIATION_TAPE_EVENTS {
                return Ok(receipts);
            }
        }
        let next_after_seq =
            advance_tape_cursor(after_seq, snapshot.next_after_seq, page_event_count)
                .map_err(Status::failed_precondition)?;
        let Some(next_after_seq) = next_after_seq else {
            return Ok(receipts);
        };
        after_seq = Some(next_after_seq);
    }
}

fn advance_tape_cursor(
    current: Option<i64>,
    next: Option<i64>,
    inspected_total: usize,
) -> Result<Option<i64>, &'static str> {
    let Some(next) = next else {
        return Ok(None);
    };
    if inspected_total == 0 || current.is_some_and(|current| next <= current) {
        return Err("tool side-effect reconciliation tape cursor did not advance");
    }
    Ok(Some(next))
}

fn receipt_from_outcome(
    proposal_id: &str,
    tool_name: &str,
    binding: SideEffectReconciliationBinding<'_>,
    outcome: &ToolExecutionOutcome,
) -> Option<DurableSideEffectReconciliationReceiptV1> {
    if !outcome.success
        || outcome.attestation.timed_out
        || !is_sha256(outcome.attestation.execution_sha256.as_str())
    {
        return None;
    }
    let payload = serde_json::from_slice::<Value>(outcome.output_json.as_slice()).ok()?;
    if !strategy_specific_outcome_is_observed(
        binding.strategy,
        binding.external_idempotency_key_sha256,
        &payload,
    ) {
        return None;
    }
    Some(DurableSideEffectReconciliationReceiptV1 {
        schema_version: RECONCILIATION_RECEIPT_SCHEMA_VERSION,
        operation_id: binding.operation_id.as_str().to_owned(),
        generation: binding.generation.get(),
        intent_sha256: binding.intent_sha256.to_owned(),
        proposal_id: proposal_id.to_owned(),
        tool_name: tool_name.to_owned(),
        strategy: binding.strategy,
        receipt_kind: receipt_kind(binding.strategy).to_owned(),
        evidence_sha256: outcome.attestation.execution_sha256.clone(),
        external_idempotency_key_sha256: binding.external_idempotency_key_sha256.map(str::to_owned),
    })
}

fn strategy_specific_outcome_is_observed(
    strategy: ReconciliationStrategy,
    external_idempotency_key_sha256: Option<&str>,
    payload: &Value,
) -> bool {
    match strategy {
        ReconciliationStrategy::WorkspaceDigest => {
            let checkpoint = payload
                .get("post_change_checkpoint")
                .or_else(|| payload.get("workspace_checkpoint"));
            checkpoint.is_some_and(|checkpoint| {
                checkpoint.get("checkpoint_stage").and_then(Value::as_str) == Some("post_change")
                    && checkpoint
                        .get("checkpoint_id")
                        .and_then(Value::as_str)
                        .is_some_and(|value| !value.trim().is_empty())
            })
        }
        ReconciliationStrategy::ProcessProvenance => process_provenance_is_observed(payload),
        ReconciliationStrategy::ExternalIdempotencyReceipt => {
            external_idempotency_key_sha256.is_some_and(is_sha256)
                && payload
                    .get("status_code")
                    .and_then(Value::as_u64)
                    .is_some_and(|status| (100..=599).contains(&status))
                && payload
                    .pointer("/egress/request_fingerprint_sha256")
                    .and_then(Value::as_str)
                    .is_some_and(is_sha256)
        }
        ReconciliationStrategy::DeliveryAcknowledgement => {
            payload.get("accepted").and_then(Value::as_bool) == Some(true)
                && payload
                    .pointer("/audit/event_type")
                    .and_then(Value::as_str)
                    .is_some_and(|event| event.starts_with("clarify.ask."))
        }
        ReconciliationStrategy::WorkerLeaseReceipt => {
            let sessions_spawn_receipt = payload.get("spawned").and_then(Value::as_bool)
                == Some(true)
                && payload
                    .get("task_id")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.trim().is_empty())
                && payload
                    .get("child_run_id")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.trim().is_empty());
            let delegation_receipt = payload.get("created").and_then(Value::as_bool) == Some(true)
                && payload
                    .pointer("/task/task_id")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.trim().is_empty());
            sessions_spawn_receipt || delegation_receipt
        }
        ReconciliationStrategy::None => false,
    }
}

fn process_provenance_is_observed(payload: &Value) -> bool {
    if payload.get("background").and_then(Value::as_bool) != Some(true)
        || payload
            .pointer("/process_handle/ownership_root_pid")
            .and_then(Value::as_u64)
            .is_none_or(|pid| pid == 0 || pid > u64::from(u32::MAX))
        || payload.pointer("/process_handle/process_tree").and_then(Value::as_bool) != Some(true)
    {
        return false;
    }
    let Some(raw_provenance) = payload.pointer("/process_handle/provenance").cloned() else {
        return false;
    };
    let Ok(provenance) = serde_json::from_value::<ProcessProvenance>(raw_provenance) else {
        return false;
    };
    provenance.validate().is_ok()
        && is_sha256(provenance.executable_sha256.as_str())
        && is_sha256(provenance.ownership_identity_sha256.as_str())
}

fn select_exact_receipt(
    receipts: &[DurableSideEffectReconciliationReceiptV1],
    expected: &DurableSideEffectReconciliationReceiptV1,
) -> (ReceiptSelection, Option<String>) {
    let mut selection = ReceiptSelection::Absent;
    for receipt in receipts {
        if !receipt_is_valid(receipt) {
            selection = ReceiptSelection::Invalid;
            continue;
        }
        if receipt.operation_id != expected.operation_id {
            continue;
        }
        if receipt.generation > expected.generation {
            selection = ReceiptSelection::StaleGeneration;
            continue;
        }
        if receipt.intent_sha256 != expected.intent_sha256
            || receipt.proposal_id != expected.proposal_id
            || receipt.tool_name != expected.tool_name
            || receipt.strategy != expected.strategy
            || receipt.receipt_kind != expected.receipt_kind
            || receipt.external_idempotency_key_sha256 != expected.external_idempotency_key_sha256
        {
            selection = ReceiptSelection::BindingMismatch;
            continue;
        }
        return (ReceiptSelection::Exact, Some(receipt.evidence_sha256.clone()));
    }
    (selection, None)
}

fn receipt_is_valid(receipt: &DurableSideEffectReconciliationReceiptV1) -> bool {
    receipt.schema_version == RECONCILIATION_RECEIPT_SCHEMA_VERSION
        && receipt.generation > 0
        && !receipt.operation_id.trim().is_empty()
        && receipt.operation_id.len() <= 256
        && !receipt.proposal_id.trim().is_empty()
        && receipt.proposal_id.len() <= 256
        && !receipt.tool_name.trim().is_empty()
        && receipt.tool_name.len() <= 256
        && is_sha256(receipt.intent_sha256.as_str())
        && is_sha256(receipt.evidence_sha256.as_str())
        && receipt.receipt_kind == receipt_kind(receipt.strategy)
        && match receipt.strategy {
            ReconciliationStrategy::ExternalIdempotencyReceipt => {
                receipt.external_idempotency_key_sha256.as_deref().is_some_and(is_sha256)
            }
            ReconciliationStrategy::None => false,
            _ => receipt.external_idempotency_key_sha256.is_none(),
        }
}

const fn receipt_kind(strategy: ReconciliationStrategy) -> &'static str {
    match strategy {
        ReconciliationStrategy::WorkspaceDigest => "workspace_post_change_digest",
        ReconciliationStrategy::ProcessProvenance => "process_provenance",
        ReconciliationStrategy::ExternalIdempotencyReceipt => "external_http_response",
        ReconciliationStrategy::DeliveryAcknowledgement => "delivery_acknowledgement",
        ReconciliationStrategy::WorkerLeaseReceipt => "worker_lease_receipt",
        ReconciliationStrategy::None => "none",
    }
}

const fn reconciliation_reason_code(strategy: ReconciliationStrategy) -> &'static str {
    match strategy {
        ReconciliationStrategy::WorkspaceDigest => "tool.effect.reconciled.workspace_digest",
        ReconciliationStrategy::ProcessProvenance => "tool.effect.reconciled.process_provenance",
        ReconciliationStrategy::ExternalIdempotencyReceipt => {
            "tool.effect.reconciled.external_idempotency_receipt"
        }
        ReconciliationStrategy::DeliveryAcknowledgement => {
            "tool.effect.reconciled.delivery_acknowledgement"
        }
        ReconciliationStrategy::WorkerLeaseReceipt => "tool.effect.reconciled.worker_lease_receipt",
        ReconciliationStrategy::None => "tool.effect.reconciliation.strategy_unavailable",
    }
}

fn blocked_reason_code(strategy: ReconciliationStrategy, suffix: &str) -> String {
    format!("tool.effect.reconciliation.{}.{}", strategy.as_str(), suffix)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use palyra_common::runtime_contracts::{
        ReconciliationStrategy, RuntimeGeneration, RuntimeIdempotencyClass, RuntimeOperationId,
        SideEffectFenceState, SideEffectFenceV1, SideEffectRestartPolicy, SideEffectRetryDecision,
        ToolExecutionSemantics,
    };
    use serde_json::{json, Value};

    use super::{
        advance_tape_cursor, receipt_from_outcome, receipt_kind,
        reconcile_unknown_tool_side_effect, record_side_effect_reconciliation_receipt,
        select_exact_receipt, DurableSideEffectReconciliationReceiptV1, ReceiptSelection,
        SideEffectReconciliationBinding, SideEffectReconciliationOutcome,
        RECONCILIATION_RECEIPT_SCHEMA_VERSION,
    };
    use crate::gateway::runtime::tests::{
        start_test_orchestrator_run, test_runtime_state, test_runtime_state_for_journal,
    };
    use crate::gateway::GatewayRuntimeState;
    use crate::tool_protocol::{ToolAttestation, ToolExecutionOutcome};

    const STRATEGIES: [ReconciliationStrategy; 5] = [
        ReconciliationStrategy::WorkspaceDigest,
        ReconciliationStrategy::ProcessProvenance,
        ReconciliationStrategy::ExternalIdempotencyReceipt,
        ReconciliationStrategy::DeliveryAcknowledgement,
        ReconciliationStrategy::WorkerLeaseReceipt,
    ];

    fn expected(strategy: ReconciliationStrategy) -> DurableSideEffectReconciliationReceiptV1 {
        DurableSideEffectReconciliationReceiptV1 {
            schema_version: RECONCILIATION_RECEIPT_SCHEMA_VERSION,
            operation_id: "tool:proposal-1".to_owned(),
            generation: 7,
            intent_sha256: "a".repeat(64),
            proposal_id: "proposal-1".to_owned(),
            tool_name: tool_name(strategy).to_owned(),
            strategy,
            receipt_kind: receipt_kind(strategy).to_owned(),
            evidence_sha256: String::new(),
            external_idempotency_key_sha256: (strategy
                == ReconciliationStrategy::ExternalIdempotencyReceipt)
                .then(|| "b".repeat(64)),
        }
    }

    fn exact_receipt(strategy: ReconciliationStrategy) -> DurableSideEffectReconciliationReceiptV1 {
        let mut receipt = expected(strategy);
        receipt.evidence_sha256 = "c".repeat(64);
        receipt
    }

    fn tool_name(strategy: ReconciliationStrategy) -> &'static str {
        match strategy {
            ReconciliationStrategy::WorkspaceDigest => "palyra.fs.apply_patch",
            ReconciliationStrategy::ProcessProvenance => "palyra.process.run",
            ReconciliationStrategy::ExternalIdempotencyReceipt => "palyra.http.fetch",
            ReconciliationStrategy::DeliveryAcknowledgement => "palyra.clarify.ask",
            ReconciliationStrategy::WorkerLeaseReceipt => "sessions_spawn",
            ReconciliationStrategy::None => "unsupported",
        }
    }

    fn outcome(output: Value) -> ToolExecutionOutcome {
        ToolExecutionOutcome {
            success: true,
            output_json: serde_json::to_vec(&output).expect("serialize output"),
            error: String::new(),
            attestation: ToolAttestation {
                attestation_id: "attestation-1".to_owned(),
                execution_sha256: "c".repeat(64),
                executed_at_unix_ms: 1,
                timed_out: false,
                executor: "test".to_owned(),
                sandbox_enforcement: "test".to_owned(),
                execution_manifest: None,
            },
        }
    }

    fn binding(
        operation_id: &RuntimeOperationId,
        strategy: ReconciliationStrategy,
    ) -> SideEffectReconciliationBinding<'_> {
        SideEffectReconciliationBinding {
            operation_id,
            generation: RuntimeGeneration::new(7).expect("generation"),
            intent_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            strategy,
            external_idempotency_key_sha256: (strategy
                == ReconciliationStrategy::ExternalIdempotencyReceipt)
                .then_some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        }
    }

    fn side_effect_fence(
        proposal_id: &str,
        tool_name: &str,
        generation: RuntimeGeneration,
        strategy: ReconciliationStrategy,
    ) -> SideEffectFenceV1 {
        let (operation_id, tool_execution_id) =
            GatewayRuntimeState::tool_side_effect_identities(proposal_id)
                .expect("tool side-effect identities should validate");
        let external_idempotency_key_required =
            strategy == ReconciliationStrategy::ExternalIdempotencyReceipt;
        SideEffectFenceV1 {
            schema_version: 1,
            operation_id,
            tool_execution_id,
            intent_generation: generation,
            observed_generation: generation,
            intent_sha256: "a".repeat(64),
            state: SideEffectFenceState::IntentRecorded,
            semantics: ToolExecutionSemantics {
                schema_version: 1,
                tool_name: tool_name.to_owned(),
                idempotency_class: if external_idempotency_key_required {
                    RuntimeIdempotencyClass::ExternalIdempotencyKey
                } else {
                    RuntimeIdempotencyClass::ReconciliableMutation
                },
                restart_policy: SideEffectRestartPolicy::ReconcileBeforeRetry,
                reconciliation_strategy: strategy,
                external_idempotency_key_required,
            },
            external_idempotency_key_sha256: external_idempotency_key_required
                .then(|| "b".repeat(64)),
            evidence_sha256: None,
            reason_code: "tool.effect.intent_recorded".to_owned(),
            updated_at_unix_ms: 1,
        }
    }

    fn observed_output(strategy: ReconciliationStrategy) -> Value {
        match strategy {
            ReconciliationStrategy::WorkspaceDigest => json!({
                "post_change_checkpoint": {
                    "checkpoint_id": "checkpoint-1",
                    "checkpoint_stage": "post_change"
                }
            }),
            ReconciliationStrategy::ProcessProvenance => json!({
                "background": true,
                "process_handle": {
                    "ownership_root_pid": 42,
                    "process_tree": true,
                    "provenance": {
                        "ownership_kind": "unix_process_group",
                        "start_token": "start",
                        "executable_sha256": "d".repeat(64),
                        "owner_nonce": "nonce",
                        "ownership_identity_sha256": "e".repeat(64)
                    }
                }
            }),
            ReconciliationStrategy::ExternalIdempotencyReceipt => json!({
                "status_code": 200,
                "egress": {"request_fingerprint_sha256": "d".repeat(64)}
            }),
            ReconciliationStrategy::DeliveryAcknowledgement => json!({
                "accepted": true,
                "audit": {"event_type": "clarify.ask.pending"}
            }),
            ReconciliationStrategy::WorkerLeaseReceipt => json!({
                "spawned": true,
                "task_id": "task-1",
                "child_run_id": "run-2"
            }),
            ReconciliationStrategy::None => Value::Null,
        }
    }

    #[test]
    fn every_strategy_requires_exact_binding_and_generation_evidence() {
        for strategy in STRATEGIES {
            let expected = expected(strategy);
            let exact = exact_receipt(strategy);
            assert_eq!(
                select_exact_receipt(std::slice::from_ref(&exact), &expected).0,
                ReceiptSelection::Exact,
                "{strategy:?} should accept exact evidence"
            );
            assert_eq!(
                select_exact_receipt(&[], &expected).0,
                ReceiptSelection::Absent,
                "{strategy:?} should block absent evidence"
            );

            let mut mismatched = exact.clone();
            mismatched.intent_sha256 = "d".repeat(64);
            assert_eq!(
                select_exact_receipt(&[mismatched], &expected).0,
                ReceiptSelection::BindingMismatch,
                "{strategy:?} should block mismatched intent evidence"
            );

            let mut stale = exact;
            stale.generation = expected.generation + 1;
            assert_eq!(
                select_exact_receipt(&[stale], &expected).0,
                ReceiptSelection::StaleGeneration,
                "{strategy:?} should block evidence outside active generation history"
            );
        }
    }

    #[test]
    fn every_strategy_builds_receipt_only_from_specific_outcome_evidence() {
        let operation_id =
            RuntimeOperationId::parse("tool:proposal-1").expect("operation id should validate");
        let cases = [
            (
                ReconciliationStrategy::WorkspaceDigest,
                json!({
                    "post_change_checkpoint":{
                        "checkpoint_id":"checkpoint-1",
                        "checkpoint_stage":"post_change"
                    }
                }),
            ),
            (
                ReconciliationStrategy::ProcessProvenance,
                json!({
                    "background": true,
                    "process_handle": {
                        "ownership_root_pid": 42,
                        "process_tree": true,
                        "provenance": {
                            "ownership_kind":"unix_process_group",
                            "start_token":"start",
                            "executable_sha256":"d".repeat(64),
                            "owner_nonce":"nonce",
                            "ownership_identity_sha256":"e".repeat(64)
                        }
                    }
                }),
            ),
            (
                ReconciliationStrategy::ExternalIdempotencyReceipt,
                json!({
                    "status_code": 200,
                    "egress": {"request_fingerprint_sha256":"d".repeat(64)}
                }),
            ),
            (
                ReconciliationStrategy::DeliveryAcknowledgement,
                json!({
                    "accepted": true,
                    "audit": {"event_type":"clarify.ask.pending"}
                }),
            ),
            (
                ReconciliationStrategy::WorkerLeaseReceipt,
                json!({
                    "spawned": true,
                    "task_id": "task-1",
                    "child_run_id": "run-2"
                }),
            ),
        ];
        for (strategy, output) in cases {
            let receipt = receipt_from_outcome(
                "proposal-1",
                tool_name(strategy),
                binding(&operation_id, strategy),
                &outcome(output),
            );
            assert!(receipt.is_some(), "{strategy:?} should emit its exact receipt");
        }
    }

    #[test]
    fn external_key_digest_without_response_receipt_is_not_evidence() {
        let operation_id =
            RuntimeOperationId::parse("tool:proposal-1").expect("operation id should validate");
        let receipt = receipt_from_outcome(
            "proposal-1",
            "palyra.http.fetch",
            binding(&operation_id, ReconciliationStrategy::ExternalIdempotencyReceipt),
            &outcome(json!({"idempotency_key_sha256":"b".repeat(64)})),
        );
        assert!(receipt.is_none());
    }

    #[test]
    fn malformed_process_provenance_and_preflight_checkpoint_are_not_receipts() {
        let operation_id =
            RuntimeOperationId::parse("tool:proposal-1").expect("operation id should validate");
        let malformed_process = receipt_from_outcome(
            "proposal-1",
            "palyra.process.run",
            binding(&operation_id, ReconciliationStrategy::ProcessProvenance),
            &outcome(json!({
                "background": true,
                "process_handle": {
                    "ownership_root_pid": 42,
                    "process_tree": true,
                    "provenance": {"start_token":"start"}
                }
            })),
        );
        assert!(malformed_process.is_none());

        let preflight_only = receipt_from_outcome(
            "proposal-1",
            "palyra.fs.apply_patch",
            binding(&operation_id, ReconciliationStrategy::WorkspaceDigest),
            &outcome(json!({
                "post_change_checkpoint": {
                    "checkpoint_id":"checkpoint-1",
                    "checkpoint_stage":"preflight"
                }
            })),
        );
        assert!(preflight_only.is_none());
    }

    #[test]
    fn uppercase_or_mismatched_digests_are_rejected() {
        let operation_id =
            RuntimeOperationId::parse("tool:proposal-1").expect("operation id should validate");
        let mut uppercase_outcome = outcome(json!({
            "status_code": 200,
            "egress": {"request_fingerprint_sha256":"d".repeat(64)}
        }));
        uppercase_outcome.attestation.execution_sha256 = "C".repeat(64);
        assert!(receipt_from_outcome(
            "proposal-1",
            "palyra.http.fetch",
            binding(&operation_id, ReconciliationStrategy::ExternalIdempotencyReceipt,),
            &uppercase_outcome,
        )
        .is_none());

        let expected = expected(ReconciliationStrategy::ExternalIdempotencyReceipt);
        let mut mismatched = exact_receipt(ReconciliationStrategy::ExternalIdempotencyReceipt);
        mismatched.external_idempotency_key_sha256 = Some("d".repeat(64));
        assert_eq!(
            select_exact_receipt(&[mismatched], &expected).0,
            ReceiptSelection::BindingMismatch
        );
    }

    #[tokio::test]
    async fn exact_receipts_reconcile_all_mutation_classes_after_runtime_reopen() {
        let state = test_runtime_state();
        let db_path = state.journal_config.db_path.clone();
        let mut persisted = Vec::new();

        for (index, strategy) in STRATEGIES.into_iter().enumerate() {
            let session_id = format!("session_side_effect_restart_{index}");
            let run_id = format!("run_side_effect_restart_{index}");
            let proposal_id = format!("proposal_side_effect_restart_{index}");
            let tool_name = tool_name(strategy).to_owned();
            start_test_orchestrator_run(&state, session_id.as_str(), run_id.as_str());
            let (_, generation) = state
                .runtime_generation_for_tool_blocking(run_id.as_str())
                .expect("tool generation query should succeed")
                .expect("tool generation should be active");
            let fence =
                side_effect_fence(proposal_id.as_str(), tool_name.as_str(), generation, strategy);
            assert_eq!(
                state
                    .prepare_tool_side_effect_fence(
                        session_id.clone(),
                        run_id.clone(),
                        fence.clone(),
                    )
                    .await
                    .expect("side-effect intent should persist"),
                SideEffectRetryDecision::Safe
            );
            state
                .transition_tool_side_effect_fence(
                    fence.operation_id.clone(),
                    SideEffectFenceState::EffectStarted,
                    generation,
                    "tool.effect.started".to_owned(),
                    None,
                )
                .await
                .expect("side effect should enter the started state");
            let mut tape_seq = 0;
            assert!(record_side_effect_reconciliation_receipt(
                &state,
                run_id.as_str(),
                &mut tape_seq,
                proposal_id.as_str(),
                tool_name.as_str(),
                SideEffectReconciliationBinding {
                    operation_id: &fence.operation_id,
                    generation,
                    intent_sha256: fence.intent_sha256.as_str(),
                    strategy,
                    external_idempotency_key_sha256: fence
                        .external_idempotency_key_sha256
                        .as_deref(),
                },
                &outcome(observed_output(strategy)),
            )
            .await
            .expect("strategy receipt should persist"));
            state
                .transition_tool_side_effect_fence(
                    fence.operation_id.clone(),
                    SideEffectFenceState::EffectUnknown,
                    generation,
                    "tool.effect.ack_unknown".to_owned(),
                    None,
                )
                .await
                .expect("acknowledgement fault should remain uncertain");
            persisted.push((session_id, run_id, proposal_id, tool_name, strategy, fence));
        }

        drop(state);
        let reopened = test_runtime_state_for_journal(db_path);
        for (session_id, run_id, proposal_id, tool_name, strategy, mut fence) in persisted {
            let (_, generation) = reopened
                .runtime_generation_for_tool_blocking(run_id.as_str())
                .expect("reopened generation query should succeed")
                .expect("reopened run generation should remain active");
            fence.observed_generation = generation;
            assert_eq!(
                reopened
                    .prepare_tool_side_effect_fence(
                        session_id.clone(),
                        run_id.clone(),
                        fence.clone(),
                    )
                    .await
                    .expect("reopened retry should load the durable fence"),
                SideEffectRetryDecision::ReconciliationRequired
            );
            assert_eq!(
                reconcile_unknown_tool_side_effect(
                    &reopened,
                    run_id.as_str(),
                    proposal_id.as_str(),
                    tool_name.as_str(),
                    SideEffectReconciliationBinding {
                        operation_id: &fence.operation_id,
                        generation,
                        intent_sha256: fence.intent_sha256.as_str(),
                        strategy,
                        external_idempotency_key_sha256: fence
                            .external_idempotency_key_sha256
                            .as_deref(),
                    },
                )
                .await
                .expect("reopened reconciliation should inspect durable receipts"),
                SideEffectReconciliationOutcome::Reconciled
            );
            assert_eq!(
                reopened
                    .prepare_tool_side_effect_fence(session_id, run_id, fence)
                    .await
                    .expect("reconciled fence should remain durable"),
                SideEffectRetryDecision::Completed
            );
        }
    }

    #[tokio::test]
    async fn restart_distinguishes_pre_effect_retry_from_unknown_effect_blocking() {
        let state = test_runtime_state();
        let db_path = state.journal_config.db_path.clone();

        let pre_effect_session = "session_side_effect_before_apply";
        let pre_effect_run = "run_side_effect_before_apply";
        let pre_effect_proposal = "proposal_side_effect_before_apply";
        start_test_orchestrator_run(&state, pre_effect_session, pre_effect_run);
        let (_, pre_effect_generation) = state
            .runtime_generation_for_tool_blocking(pre_effect_run)
            .expect("pre-effect generation query should succeed")
            .expect("pre-effect generation should be active");
        let pre_effect_fence = side_effect_fence(
            pre_effect_proposal,
            "palyra.fs.apply_patch",
            pre_effect_generation,
            ReconciliationStrategy::WorkspaceDigest,
        );
        assert_eq!(
            state
                .prepare_tool_side_effect_fence(
                    pre_effect_session.to_owned(),
                    pre_effect_run.to_owned(),
                    pre_effect_fence.clone(),
                )
                .await
                .expect("pre-effect intent should persist"),
            SideEffectRetryDecision::Safe
        );

        let process_session = "session_side_effect_process_unknown";
        let process_run = "run_side_effect_process_unknown";
        let process_proposal = "proposal_side_effect_process_unknown";
        start_test_orchestrator_run(&state, process_session, process_run);
        let (_, process_generation) = state
            .runtime_generation_for_tool_blocking(process_run)
            .expect("process generation query should succeed")
            .expect("process generation should be active");
        let process_fence = side_effect_fence(
            process_proposal,
            "palyra.process.run",
            process_generation,
            ReconciliationStrategy::ProcessProvenance,
        );
        state
            .prepare_tool_side_effect_fence(
                process_session.to_owned(),
                process_run.to_owned(),
                process_fence.clone(),
            )
            .await
            .expect("process intent should persist");
        state
            .transition_tool_side_effect_fence(
                process_fence.operation_id.clone(),
                SideEffectFenceState::EffectStarted,
                process_generation,
                "tool.effect.started".to_owned(),
                None,
            )
            .await
            .expect("process effect should enter the started state");
        state
            .transition_tool_side_effect_fence(
                process_fence.operation_id.clone(),
                SideEffectFenceState::EffectUnknown,
                process_generation,
                "tool.effect.ack_unknown".to_owned(),
                None,
            )
            .await
            .expect("missing process acknowledgement should remain uncertain");

        let http_session = "session_side_effect_http_unkeyed";
        let http_run = "run_side_effect_http_unkeyed";
        let http_proposal = "proposal_side_effect_http_unkeyed";
        start_test_orchestrator_run(&state, http_session, http_run);
        let (_, http_generation) = state
            .runtime_generation_for_tool_blocking(http_run)
            .expect("HTTP generation query should succeed")
            .expect("HTTP generation should be active");
        let mut http_fence = side_effect_fence(
            http_proposal,
            "palyra.http.fetch",
            http_generation,
            ReconciliationStrategy::WorkspaceDigest,
        );
        http_fence.semantics.idempotency_class = RuntimeIdempotencyClass::NonIdempotent;
        http_fence.semantics.restart_policy = SideEffectRestartPolicy::RequireConfirmation;
        http_fence.semantics.reconciliation_strategy = ReconciliationStrategy::None;
        http_fence.external_idempotency_key_sha256 = None;
        state
            .prepare_tool_side_effect_fence(
                http_session.to_owned(),
                http_run.to_owned(),
                http_fence.clone(),
            )
            .await
            .expect("unkeyed HTTP intent should persist");
        state
            .transition_tool_side_effect_fence(
                http_fence.operation_id.clone(),
                SideEffectFenceState::EffectStarted,
                http_generation,
                "tool.effect.started".to_owned(),
                None,
            )
            .await
            .expect("HTTP effect should enter the started state");
        state
            .transition_tool_side_effect_fence(
                http_fence.operation_id.clone(),
                SideEffectFenceState::EffectUnknown,
                http_generation,
                "tool.effect.ack_unknown".to_owned(),
                None,
            )
            .await
            .expect("missing HTTP acknowledgement should remain uncertain");

        drop(state);
        let reopened = test_runtime_state_for_journal(db_path);

        assert_eq!(
            reopened
                .prepare_tool_side_effect_fence(
                    pre_effect_session.to_owned(),
                    pre_effect_run.to_owned(),
                    pre_effect_fence,
                )
                .await
                .expect("intent-only workspace mutation should be retryable"),
            SideEffectRetryDecision::Safe
        );
        assert_eq!(
            reopened
                .prepare_tool_side_effect_fence(
                    process_session.to_owned(),
                    process_run.to_owned(),
                    process_fence.clone(),
                )
                .await
                .expect("unknown process effect should load"),
            SideEffectRetryDecision::ReconciliationRequired
        );
        assert_eq!(
            reconcile_unknown_tool_side_effect(
                &reopened,
                process_run,
                process_proposal,
                "palyra.process.run",
                SideEffectReconciliationBinding {
                    operation_id: &process_fence.operation_id,
                    generation: process_generation,
                    intent_sha256: process_fence.intent_sha256.as_str(),
                    strategy: ReconciliationStrategy::ProcessProvenance,
                    external_idempotency_key_sha256: None,
                },
            )
            .await
            .expect("unknown process effect should inspect durable evidence"),
            SideEffectReconciliationOutcome::Blocked {
                reason_code: "tool.effect.reconciliation.process_provenance.receipt_absent"
                    .to_owned(),
            }
        );
        assert_eq!(
            reopened
                .prepare_tool_side_effect_fence(
                    http_session.to_owned(),
                    http_run.to_owned(),
                    http_fence,
                )
                .await
                .expect("unknown unkeyed HTTP effect should load"),
            SideEffectRetryDecision::ConfirmationRequired
        );
    }

    #[test]
    fn pagination_cursor_must_advance() {
        assert_eq!(advance_tape_cursor(None, None, 0), Ok(None));
        assert!(advance_tape_cursor(None, Some(4), 0).is_err());
        assert!(advance_tape_cursor(Some(4), Some(4), 1).is_err());
        assert!(advance_tape_cursor(Some(4), Some(3), 1).is_err());
        assert_eq!(advance_tape_cursor(Some(4), Some(5), 1), Ok(Some(5)));
    }
}
