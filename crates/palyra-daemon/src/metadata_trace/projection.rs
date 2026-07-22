//! Closed conversion from orchestrator tape rows to strict metadata-trace events.
//!
//! Every mapping names each accepted field explicitly. Unknown keys and rich
//! payload subtrees are never copied, hashed wholesale, or recursively redacted.

use palyra_common::metadata_trace::{
    ApprovalMetadataV1, CapacityReachedMetadataV1, ContextAssembledMetadataV1,
    DeliveryIntentMetadataV1, MetadataTraceApprovalDecisionV1, MetadataTraceCapacityLimitV1,
    MetadataTraceDeliveryRouteV1, MetadataTraceDeliveryStateV1, MetadataTraceDifferentialOutcomeV1,
    MetadataTraceEventDataV1, MetadataTraceEventV1, MetadataTraceProviderAttemptOutcomeV1,
    MetadataTraceRecoveryStrategyV1, MetadataTraceRouteClassV1, MetadataTraceSchemaHashV1,
    MetadataTraceShadowClassificationV1, MetadataTraceShadowEnrollmentV1,
    MetadataTraceTerminalOutcomeV1, MetadataTraceToolGateDecisionV1, MetadataTraceToolOutcomeV1,
    ProviderAttemptMetadataV1, RecoveryMetadataV1, RuntimeSelectedMetadataV1,
    RuntimeShadowDifferentialMetadataV1, TerminalizationMetadataV1, ToolGateMetadataV1,
    ToolOutcomeMetadataV1, METADATA_TRACE_MAX_ATTEMPTS, METADATA_TRACE_MAX_CONTEXT_ITEMS,
    METADATA_TRACE_MAX_SCHEMA_HASHES, METADATA_TRACE_MAX_STAGE_DURATION_MS,
};
use serde_json::{Map, Value};

use crate::journal::OrchestratorTapeRecord;

use super::{
    hash_metadata_trace_approval_id, hash_metadata_trace_delivery_id,
    hash_metadata_trace_identifier, hash_metadata_trace_model_id, hash_metadata_trace_profile_id,
    hash_metadata_trace_provider_id, hash_metadata_trace_tool_id, projected_event_id_sha256,
    MetadataTraceIdentifierDomain, MetadataTraceProjectionContext,
};

const MAX_SOURCE_PAYLOAD_BYTES: usize = 64 * 1_024;
const MAX_MACHINE_IDENTIFIER_BYTES: usize = 96;
const MAX_REASON_CODE_BYTES: usize = 128;

struct ProjectedEvent {
    event: MetadataTraceEventDataV1,
    stage_duration_ms: Option<u64>,
}

/// Projects one rich orchestrator tape row into a closed metadata event.
///
/// Unsupported, malformed, oversized, or semantically unsafe rows return
/// `None`. The source payload is never preserved in the returned value.
#[must_use]
pub(crate) fn project_orchestrator_tape_record(
    record: &OrchestratorTapeRecord,
    context: MetadataTraceProjectionContext<'_>,
) -> Option<MetadataTraceEventV1> {
    if record.payload_json.len() > MAX_SOURCE_PAYLOAD_BYTES {
        return None;
    }
    let payload = serde_json::from_str::<Value>(record.payload_json.as_str()).ok()?;
    let payload = payload.as_object()?;
    let projected = match record.event_type.as_str() {
        "harness.selection" | "metadata.runtime_selected" => project_runtime_selected(payload),
        "runtime.shadow.differential" => project_runtime_shadow_differential(payload),
        "context.assembled" => project_context_assembled(payload),
        "provider.attempt.completed" => project_provider_attempt_completed(payload),
        "provider.lane.attested" => project_provider_lane_attested(payload),
        "provider.retry.started" => project_provider_retry_started(payload),
        "provider.route.changed" => project_provider_route_changed(payload),
        "tool.before_decision" => project_before_tool_decision(payload),
        "tool_proposal" => project_tool_proposal(payload),
        "tool_approval_request" => project_approval_request(payload),
        "tool_approval_response" => project_approval_response(payload),
        "tool_result" => project_tool_result(payload),
        "provider.recovery.decision" => project_provider_recovery(payload),
        "provider.turn_recovery.decision" => project_provider_turn_recovery(payload),
        "run.recovery" => project_run_recovery(payload),
        "message.replied" => project_delivery_intent(record, context),
        "status" => project_terminal_status(payload),
        _ => None,
    }?;
    build_event(
        context,
        projected_event_id_sha256(context, record)?,
        projected.stage_duration_ms,
        projected.event,
    )
}

/// Constructs the one-shot marker emitted when a trace hard cap is reached.
#[must_use]
pub(crate) fn metadata_trace_capacity_reached_event(
    context: MetadataTraceProjectionContext<'_>,
    limit_kind: MetadataTraceCapacityLimitV1,
    observed: u32,
    limit: u32,
    reason_code: &str,
) -> Option<MetadataTraceEventV1> {
    let reason_code = machine_reason_code(reason_code)?;
    build_explicit_event(
        context,
        "capacity_reached",
        None,
        MetadataTraceEventDataV1::CapacityReached(CapacityReachedMetadataV1 {
            limit_kind,
            observed,
            limit,
            reason_code,
        }),
    )
}

fn build_explicit_event(
    context: MetadataTraceProjectionContext<'_>,
    event_kind: &str,
    stage_duration_ms: Option<u64>,
    event: MetadataTraceEventDataV1,
) -> Option<MetadataTraceEventV1> {
    let identity =
        format!("{}:{}:{}:{event_kind}", context.run_id, context.generation, context.sequence);
    let event_id_sha256 =
        hash_metadata_trace_identifier(MetadataTraceIdentifierDomain::Event, identity.as_str())?;
    build_event(context, event_id_sha256, stage_duration_ms, event)
}

fn build_event(
    context: MetadataTraceProjectionContext<'_>,
    event_id_sha256: String,
    stage_duration_ms: Option<u64>,
    event: MetadataTraceEventDataV1,
) -> Option<MetadataTraceEventV1> {
    let causal_parent_event_id_sha256 = match context.causal_parent_event_id_sha256 {
        Some(value) if is_sha256_hex(value) => Some(value.to_owned()),
        Some(_) => return None,
        None => None,
    };
    let event = MetadataTraceEventV1 {
        sequence: context.sequence,
        generation: context.generation,
        recorded_at_unix_ms: context.recorded_at_unix_ms,
        event_id_sha256,
        causal_parent_event_id_sha256,
        stage_duration_ms,
        event,
    };
    event.validate_shape().ok()?;
    Some(event)
}

fn project_runtime_shadow_differential(payload: &Map<String, Value>) -> Option<ProjectedEvent> {
    if payload.get("schema_version")?.as_u64()? != 1
        || payload.get("event_name")?.as_str()? != "runtime.shadow.differential"
        || payload.get("redaction_level")?.as_str()? != "metadata_only"
        || payload.get("authoritative_runtime")?.as_str()? != "legacy"
        || !payload.get("shadow_side_effect_free")?.as_bool()?
    {
        return None;
    }
    let (enrollment, expected_enrollment_reason) = match payload.get("enrollment")?.as_str()? {
        "deterministic_sample" => (
            MetadataTraceShadowEnrollmentV1::DeterministicSample,
            "runtime.shadow.enrollment.deterministic_sample",
        ),
        "explicit_session" => (
            MetadataTraceShadowEnrollmentV1::ExplicitSession,
            "runtime.shadow.enrollment.explicit_session",
        ),
        _ => return None,
    };
    if payload.get("enrollment_reason_code")?.as_str()? != expected_enrollment_reason {
        return None;
    }
    let (classification, expected_reason_code, expected_promotion_blocked) = match payload
        .get("classification")?
        .as_str()?
    {
        "expected" => (
            MetadataTraceShadowClassificationV1::Expected,
            "runtime.shadow.differential_expected",
            false,
        ),
        "benign" => (
            MetadataTraceShadowClassificationV1::Benign,
            "runtime.shadow.differential_benign",
            false,
        ),
        "risky" => {
            (MetadataTraceShadowClassificationV1::Risky, "runtime.shadow.differential_risky", false)
        }
        "invariant_violation" => (
            MetadataTraceShadowClassificationV1::InvariantViolation,
            "runtime.shadow.differential_invariant_violation",
            true,
        ),
        _ => return None,
    };
    let reason_code = required_reason_code(payload, "reason_code")?;
    let promotion_blocked = payload.get("promotion_blocked")?.as_bool()?;
    if reason_code != expected_reason_code || promotion_blocked != expected_promotion_blocked {
        return None;
    }
    let runtime_selection = differential_outcome(payload, "runtime_selection")?;
    let context_segments = differential_outcome(payload, "context_segments")?;
    let context_safety = differential_outcome(payload, "context_safety")?;
    let token_budget = differential_outcome(payload, "token_budget")?;
    let tool_catalog = differential_outcome(payload, "tool_catalog")?;
    let policy_input = differential_outcome(payload, "policy_input")?;
    let phase_plan = differential_outcome(payload, "phase_plan")?;
    Some(ProjectedEvent {
        event: MetadataTraceEventDataV1::RuntimeShadowDifferential(
            RuntimeShadowDifferentialMetadataV1 {
                enrollment,
                classification,
                reason_code,
                runtime_selection,
                context_segments,
                context_safety,
                token_budget,
                tool_catalog,
                policy_input,
                phase_plan,
                promotion_blocked,
                shadow_side_effect_free: true,
            },
        ),
        stage_duration_ms: None,
    })
}

fn differential_outcome(
    payload: &Map<String, Value>,
    field: &str,
) -> Option<MetadataTraceDifferentialOutcomeV1> {
    match payload.get(field)?.as_str()? {
        "match" => Some(MetadataTraceDifferentialOutcomeV1::Match),
        "benign_difference" => Some(MetadataTraceDifferentialOutcomeV1::BenignDifference),
        "risky_difference" => Some(MetadataTraceDifferentialOutcomeV1::RiskyDifference),
        "invariant_violation" => Some(MetadataTraceDifferentialOutcomeV1::InvariantViolation),
        _ => None,
    }
}

fn project_runtime_selected(payload: &Map<String, Value>) -> Option<ProjectedEvent> {
    let harness_id = optional_machine_identifier(payload, "harness_id")?
        .unwrap_or_else(|| "embedded_default".to_owned());
    let harness_version = optional_machine_identifier(payload, "harness_version")?
        .unwrap_or_else(|| "legacy_v1".to_owned());
    let runtime_id = optional_machine_identifier(payload, "runtime_id")?
        .or(optional_machine_identifier(payload, "runtime_policy")?)
        .unwrap_or_else(|| "run_stream_host_owned".to_owned());
    let runtime_version = optional_machine_identifier(payload, "runtime_version")?
        .unwrap_or_else(|| "legacy_v1".to_owned());
    let route_class = match payload.get("route_class") {
        Some(value) => parse_route_class(value)?,
        None => {
            if payload.get("fallback_used").and_then(Value::as_bool).unwrap_or(false) {
                MetadataTraceRouteClassV1::Fallback
            } else {
                MetadataTraceRouteClassV1::Primary
            }
        }
    };
    let auth_profile_id_sha256 = optional_sha256(payload, "auth_profile_id_sha256")?;
    let schema_hashes = if let Some(value) = payload.get("schema_hashes") {
        parse_schema_hashes(value)?
    } else {
        optional_sha256(payload, "descriptor_hash")?
            .map(|sha256| {
                vec![MetadataTraceSchemaHashV1 {
                    schema_id: "agent_harness_descriptor".to_owned(),
                    sha256,
                }]
            })
            .unwrap_or_default()
    };
    Some(ProjectedEvent {
        event: MetadataTraceEventDataV1::RuntimeSelected(RuntimeSelectedMetadataV1 {
            harness_id,
            harness_version,
            runtime_id,
            runtime_version,
            route_class,
            auth_profile_id_sha256,
            schema_hashes,
        }),
        stage_duration_ms: optional_stage_duration(payload)?,
    })
}

fn project_context_assembled(payload: &Map<String, Value>) -> Option<ProjectedEvent> {
    let input_item_count = bounded_u32(payload.get("input_item_count")?)?;
    let retained_item_count = bounded_u32(payload.get("retained_item_count")?)?;
    if input_item_count > METADATA_TRACE_MAX_CONTEXT_ITEMS || retained_item_count > input_item_count
    {
        return None;
    }
    Some(ProjectedEvent {
        event: MetadataTraceEventDataV1::ContextAssembled(ContextAssembledMetadataV1 {
            context_engine_id: required_machine_identifier(payload, "context_engine_id")?,
            context_engine_version: required_machine_identifier(payload, "context_engine_version")?,
            context_schema_sha256: required_sha256(payload, "context_schema_sha256")?,
            input_item_count,
            retained_item_count,
        }),
        stage_duration_ms: Some(required_stage_duration(payload)?),
    })
}

fn project_provider_attempt_completed(payload: &Map<String, Value>) -> Option<ProjectedEvent> {
    // Preferred events must never persist the raw auth-profile identifier merely
    // so this layer can hash it after the fact.
    if payload.contains_key("auth_profile_id") {
        return None;
    }
    let provider_id = required_identifier_for_hash(payload, "provider_id")?;
    let model_id = required_identifier_for_hash(payload, "model_id")?;
    let attempt = bounded_u16(payload.get("attempt")?)?;
    if attempt == 0 || attempt > METADATA_TRACE_MAX_ATTEMPTS {
        return None;
    }
    Some(ProjectedEvent {
        event: MetadataTraceEventDataV1::ProviderAttempt(ProviderAttemptMetadataV1 {
            provider_id_sha256: hash_metadata_trace_provider_id(provider_id)?,
            model_id_sha256: hash_metadata_trace_model_id(model_id)?,
            route_class: parse_route_class(payload.get("route_class")?)?,
            auth_profile_id_sha256: optional_sha256(payload, "auth_profile_id_sha256")?,
            attempt,
            outcome: parse_provider_attempt_outcome(payload.get("outcome")?)?,
            reason_code: required_reason_code(payload, "reason_code")?,
        }),
        stage_duration_ms: Some(required_stage_duration(payload)?),
    })
}

fn project_provider_lane_attested(payload: &Map<String, Value>) -> Option<ProjectedEvent> {
    let route_class = match payload.get("provider_lane")?.as_str()? {
        "fixture" => MetadataTraceRouteClassV1::Fixture,
        "record_replay" => MetadataTraceRouteClassV1::RecordReplay,
        "live" => MetadataTraceRouteClassV1::Live,
        _ => return None,
    };
    let raw_auth_profile_id = payload
        .get("live_binding")
        .and_then(Value::as_object)
        .and_then(|binding| binding.get("auth_profile_id"))
        .and_then(Value::as_str);
    let auth_profile_id_sha256 = match raw_auth_profile_id {
        Some(value) => Some(hash_metadata_trace_profile_id(value)?),
        None => None,
    };
    provider_attempt_event(
        required_identifier_for_hash(payload, "provider_id")?,
        required_identifier_for_hash(payload, "model_id")?,
        route_class,
        auth_profile_id_sha256,
        1,
        MetadataTraceProviderAttemptOutcomeV1::Succeeded,
        "provider.lane.attested".to_owned(),
        None,
    )
}

fn project_provider_retry_started(payload: &Map<String, Value>) -> Option<ProjectedEvent> {
    let attempt_index = bounded_u16(payload.get("attempt_index")?)?;
    let attempt = attempt_index.checked_add(1)?;
    let provider_profile_id = required_identifier_for_hash(payload, "provider_profile_id")?;
    // Legacy retry rows expose an auth-profile ID but no provider ID. Keep a
    // non-correlating sentinel instead of mislabeling the profile hash as a provider.
    provider_attempt_event(
        "legacy_provider_unknown",
        required_identifier_for_hash(payload, "model_id")?,
        MetadataTraceRouteClassV1::Fallback,
        Some(hash_metadata_trace_profile_id(provider_profile_id)?),
        attempt,
        MetadataTraceProviderAttemptOutcomeV1::RetryableFailure,
        required_reason_code(payload, "reason_code")?,
        None,
    )
}

fn project_provider_route_changed(payload: &Map<String, Value>) -> Option<ProjectedEvent> {
    let transition_index = bounded_u16(payload.get("transition_index")?)?;
    let attempt = transition_index.checked_add(2)?;
    provider_attempt_event(
        required_identifier_for_hash(payload, "to_provider_id")?,
        required_identifier_for_hash(payload, "to_model_id")?,
        MetadataTraceRouteClassV1::Fallback,
        None,
        attempt,
        MetadataTraceProviderAttemptOutcomeV1::Started,
        required_reason_code(payload, "reason_code")?,
        None,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "closed provider-attempt fields stay explicit at the projection boundary"
)]
fn provider_attempt_event(
    provider_id: &str,
    model_id: &str,
    route_class: MetadataTraceRouteClassV1,
    auth_profile_id_sha256: Option<String>,
    attempt: u16,
    outcome: MetadataTraceProviderAttemptOutcomeV1,
    reason_code: String,
    stage_duration_ms: Option<u64>,
) -> Option<ProjectedEvent> {
    if attempt == 0 || attempt > METADATA_TRACE_MAX_ATTEMPTS {
        return None;
    }
    Some(ProjectedEvent {
        event: MetadataTraceEventDataV1::ProviderAttempt(ProviderAttemptMetadataV1 {
            provider_id_sha256: hash_metadata_trace_provider_id(provider_id)?,
            model_id_sha256: hash_metadata_trace_model_id(model_id)?,
            route_class,
            auth_profile_id_sha256,
            attempt,
            outcome,
            reason_code,
        }),
        stage_duration_ms,
    })
}

fn project_before_tool_decision(payload: &Map<String, Value>) -> Option<ProjectedEvent> {
    let report = payload.get("report")?.as_object()?;
    let decision = match report.get("final_decision")?.as_str()? {
        "allow" | "require_reread" | "require_smaller_patch" | "synthesize_result" => {
            MetadataTraceToolGateDecisionV1::Allowed
        }
        "require_approval" => MetadataTraceToolGateDecisionV1::ApprovalRequired,
        "block" | "fail_run" => MetadataTraceToolGateDecisionV1::Denied,
        _ => return None,
    };
    tool_gate_event(
        required_identifier_for_hash(payload, "proposal_id")?,
        decision,
        required_reason_code(report, "final_reason_code")?,
    )
}

fn project_tool_proposal(payload: &Map<String, Value>) -> Option<ProjectedEvent> {
    let approval_required = payload.get("approval_required")?.as_bool()?;
    tool_gate_event(
        required_identifier_for_hash(payload, "proposal_id")?,
        if approval_required {
            MetadataTraceToolGateDecisionV1::ApprovalRequired
        } else {
            MetadataTraceToolGateDecisionV1::Allowed
        },
        if approval_required {
            "tool.proposal.approval_required".to_owned()
        } else {
            "tool.proposal.allowed".to_owned()
        },
    )
}

fn tool_gate_event(
    tool_id: &str,
    decision: MetadataTraceToolGateDecisionV1,
    reason_code: String,
) -> Option<ProjectedEvent> {
    Some(ProjectedEvent {
        event: MetadataTraceEventDataV1::ToolGate(ToolGateMetadataV1 {
            tool_id_sha256: hash_metadata_trace_tool_id(tool_id)?,
            decision,
            reason_code,
        }),
        stage_duration_ms: None,
    })
}

fn project_approval_request(payload: &Map<String, Value>) -> Option<ProjectedEvent> {
    approval_event(
        required_identifier_for_hash(payload, "approval_id")?,
        MetadataTraceApprovalDecisionV1::Requested,
        "tool.approval.requested",
    )
}

fn project_approval_response(payload: &Map<String, Value>) -> Option<ProjectedEvent> {
    let approved = payload.get("approved")?.as_bool()?;
    approval_event(
        required_identifier_for_hash(payload, "approval_id")?,
        if approved {
            MetadataTraceApprovalDecisionV1::Approved
        } else {
            MetadataTraceApprovalDecisionV1::Denied
        },
        if approved { "tool.approval.approved" } else { "tool.approval.denied" },
    )
}

fn approval_event(
    approval_id: &str,
    decision: MetadataTraceApprovalDecisionV1,
    reason_code: &str,
) -> Option<ProjectedEvent> {
    Some(ProjectedEvent {
        event: MetadataTraceEventDataV1::Approval(ApprovalMetadataV1 {
            approval_id_sha256: hash_metadata_trace_approval_id(approval_id)?,
            decision,
            reason_code: reason_code.to_owned(),
        }),
        stage_duration_ms: None,
    })
}

fn project_tool_result(payload: &Map<String, Value>) -> Option<ProjectedEvent> {
    let success = payload.get("success")?.as_bool()?;
    let attempt = optional_u16(payload, "attempt")?.unwrap_or(1);
    if attempt == 0 || attempt > METADATA_TRACE_MAX_ATTEMPTS {
        return None;
    }
    let reason_code = if success {
        "tool.result.succeeded"
    } else {
        match payload
            .get("diagnostic")
            .and_then(Value::as_object)
            .and_then(|diagnostic| diagnostic.get("error_kind"))
            .and_then(Value::as_str)
        {
            Some("timeout") => "tool.result.timeout",
            Some("policy_denial") => "tool.result.policy_denial",
            Some("validation_error") => "tool.result.validation_error",
            _ => "tool.result.failed",
        }
    };
    Some(ProjectedEvent {
        event: MetadataTraceEventDataV1::ToolOutcome(ToolOutcomeMetadataV1 {
            tool_id_sha256: hash_metadata_trace_tool_id(required_identifier_for_hash(
                payload,
                "proposal_id",
            )?)?,
            attempt,
            outcome: if success {
                MetadataTraceToolOutcomeV1::Succeeded
            } else {
                MetadataTraceToolOutcomeV1::Failed
            },
            reason_code: reason_code.to_owned(),
        }),
        stage_duration_ms: optional_stage_duration(payload)?,
    })
}

fn project_provider_recovery(payload: &Map<String, Value>) -> Option<ProjectedEvent> {
    let strategy = match payload.get("decision")?.as_str()? {
        "retry_same_provider" => MetadataTraceRecoveryStrategyV1::RetrySameRoute,
        "failover_provider" => MetadataTraceRecoveryStrategyV1::ProviderFailover,
        "compact_and_retry" => MetadataTraceRecoveryStrategyV1::ContextCompaction,
        "fail_closed" => MetadataTraceRecoveryStrategyV1::IdempotencyGuard,
        "refresh_credential" | "ask_user" => MetadataTraceRecoveryStrategyV1::OperatorReview,
        _ => return None,
    };
    recovery_event(payload, strategy)
}

fn project_provider_turn_recovery(payload: &Map<String, Value>) -> Option<ProjectedEvent> {
    let strategy = match payload.get("action")?.as_str()? {
        "retry_same_provider"
        | "retry_with_prompt"
        | "backoff_retry"
        | "lower_reasoning_effort"
        | "shrink_multimodal"
        | "strip_unsupported_content" => MetadataTraceRecoveryStrategyV1::RetrySameRoute,
        "compact_and_retry" => MetadataTraceRecoveryStrategyV1::ContextCompaction,
        "failover_provider" => MetadataTraceRecoveryStrategyV1::ProviderFailover,
        "synthetic_tool_result" => MetadataTraceRecoveryStrategyV1::IdempotencyGuard,
        "refresh_credential" | "fail_deterministic" => {
            MetadataTraceRecoveryStrategyV1::OperatorReview
        }
        _ => return None,
    };
    recovery_event(payload, strategy)
}

fn recovery_event(
    payload: &Map<String, Value>,
    strategy: MetadataTraceRecoveryStrategyV1,
) -> Option<ProjectedEvent> {
    let attempt = optional_u16(payload, "attempt")?.unwrap_or(1);
    if attempt == 0 || attempt > METADATA_TRACE_MAX_ATTEMPTS {
        return None;
    }
    Some(ProjectedEvent {
        event: MetadataTraceEventDataV1::Recovery(RecoveryMetadataV1 {
            strategy,
            attempt,
            reason_code: required_reason_code(payload, "reason_code")?,
        }),
        stage_duration_ms: optional_stage_duration(payload)?,
    })
}

fn project_run_recovery(payload: &Map<String, Value>) -> Option<ProjectedEvent> {
    if payload.get("recovery_kind")?.as_str()? != "startup_orphaned_active_run" {
        return None;
    }
    Some(ProjectedEvent {
        event: MetadataTraceEventDataV1::Recovery(RecoveryMetadataV1 {
            strategy: MetadataTraceRecoveryStrategyV1::OperatorReview,
            attempt: 1,
            reason_code: "run.recovery.startup_orphaned_active_run".to_owned(),
        }),
        stage_duration_ms: None,
    })
}

fn project_delivery_intent(
    record: &OrchestratorTapeRecord,
    context: MetadataTraceProjectionContext<'_>,
) -> Option<ProjectedEvent> {
    let delivery_source = format!("{}:{}", context.run_id, record.seq);
    Some(ProjectedEvent {
        event: MetadataTraceEventDataV1::DeliveryIntent(DeliveryIntentMetadataV1 {
            delivery_id_sha256: hash_metadata_trace_delivery_id(delivery_source.as_str())?,
            route: MetadataTraceDeliveryRouteV1::Direct,
            state: MetadataTraceDeliveryStateV1::Planned,
            reason_code: "run.reply.delivery_planned".to_owned(),
        }),
        stage_duration_ms: None,
    })
}

fn project_terminal_status(payload: &Map<String, Value>) -> Option<ProjectedEvent> {
    let kind = payload.get("kind")?.as_str()?;
    let (outcome, default_reason_code, default_output_emitted) = match kind {
        "done" => (MetadataTraceTerminalOutcomeV1::Done, "run.status.done", true),
        "failed" | "needs_continuation" => {
            (MetadataTraceTerminalOutcomeV1::Failed, "run.status.failed", false)
        }
        "cancelled" => (MetadataTraceTerminalOutcomeV1::Cancelled, "run.status.cancelled", false),
        _ => return None,
    };
    let reason_code = match payload.get("reason_code") {
        Some(value) => machine_reason_code(value.as_str()?)?,
        None => default_reason_code.to_owned(),
    };
    let output_emitted = match payload.get("output_emitted") {
        Some(value) => value.as_bool()?,
        None => default_output_emitted,
    };
    let side_effect_may_have_occurred = match payload.get("side_effect_may_have_occurred") {
        Some(value) => value.as_bool()?,
        None => true,
    };
    Some(ProjectedEvent {
        event: MetadataTraceEventDataV1::Terminalization(TerminalizationMetadataV1 {
            outcome,
            reason_code,
            output_emitted,
            side_effect_may_have_occurred,
        }),
        stage_duration_ms: optional_stage_duration(payload)?,
    })
}

fn parse_route_class(value: &Value) -> Option<MetadataTraceRouteClassV1> {
    match value.as_str()? {
        "primary" => Some(MetadataTraceRouteClassV1::Primary),
        "fallback" => Some(MetadataTraceRouteClassV1::Fallback),
        "fixture" => Some(MetadataTraceRouteClassV1::Fixture),
        "record_replay" => Some(MetadataTraceRouteClassV1::RecordReplay),
        "live" => Some(MetadataTraceRouteClassV1::Live),
        _ => None,
    }
}

fn parse_provider_attempt_outcome(value: &Value) -> Option<MetadataTraceProviderAttemptOutcomeV1> {
    match value.as_str()? {
        "started" => Some(MetadataTraceProviderAttemptOutcomeV1::Started),
        "succeeded" => Some(MetadataTraceProviderAttemptOutcomeV1::Succeeded),
        "retryable_failure" => Some(MetadataTraceProviderAttemptOutcomeV1::RetryableFailure),
        "terminal_failure" => Some(MetadataTraceProviderAttemptOutcomeV1::TerminalFailure),
        "cancelled" => Some(MetadataTraceProviderAttemptOutcomeV1::Cancelled),
        _ => None,
    }
}

fn parse_schema_hashes(value: &Value) -> Option<Vec<MetadataTraceSchemaHashV1>> {
    let values = value.as_array()?;
    if values.len() > METADATA_TRACE_MAX_SCHEMA_HASHES {
        return None;
    }
    values
        .iter()
        .map(|value| {
            let value = value.as_object()?;
            Some(MetadataTraceSchemaHashV1 {
                schema_id: required_machine_identifier(value, "schema_id")?,
                sha256: required_sha256(value, "sha256")?,
            })
        })
        .collect()
}

fn required_identifier_for_hash<'a>(
    payload: &'a Map<String, Value>,
    field: &str,
) -> Option<&'a str> {
    let value = payload.get(field)?.as_str()?;
    if value.is_empty()
        || value.len() > MAX_MACHINE_IDENTIFIER_BYTES
        || value.trim().len() != value.len()
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return None;
    }
    Some(value)
}

fn required_machine_identifier(payload: &Map<String, Value>, field: &str) -> Option<String> {
    machine_identifier(payload.get(field)?.as_str()?)
}

fn optional_machine_identifier(
    payload: &Map<String, Value>,
    field: &str,
) -> Option<Option<String>> {
    match payload.get(field) {
        Some(value) => Some(Some(machine_identifier(value.as_str()?)?)),
        None => Some(None),
    }
}

fn machine_identifier(value: &str) -> Option<String> {
    if value.is_empty()
        || value.len() > MAX_MACHINE_IDENTIFIER_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'+'))
    {
        return None;
    }
    Some(value.to_owned())
}

fn required_reason_code(payload: &Map<String, Value>, field: &str) -> Option<String> {
    machine_reason_code(payload.get(field)?.as_str()?)
}

fn machine_reason_code(value: &str) -> Option<String> {
    if value.is_empty()
        || value.len() > MAX_REASON_CODE_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return None;
    }
    Some(value.to_owned())
}

fn required_sha256(payload: &Map<String, Value>, field: &str) -> Option<String> {
    let value = payload.get(field)?.as_str()?;
    is_sha256_hex(value).then(|| value.to_owned())
}

fn optional_sha256(payload: &Map<String, Value>, field: &str) -> Option<Option<String>> {
    match payload.get(field) {
        Some(value) => {
            let value = value.as_str()?;
            is_sha256_hex(value).then(|| Some(value.to_owned()))
        }
        None => Some(None),
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn required_stage_duration(payload: &Map<String, Value>) -> Option<u64> {
    bounded_stage_duration(payload.get("stage_duration_ms")?)
}

fn optional_stage_duration(payload: &Map<String, Value>) -> Option<Option<u64>> {
    match payload.get("stage_duration_ms") {
        Some(value) => Some(Some(bounded_stage_duration(value)?)),
        None => Some(None),
    }
}

fn bounded_stage_duration(value: &Value) -> Option<u64> {
    let value = value.as_u64()?;
    (value <= METADATA_TRACE_MAX_STAGE_DURATION_MS).then_some(value)
}

fn bounded_u16(value: &Value) -> Option<u16> {
    u16::try_from(value.as_u64()?).ok()
}

fn bounded_u32(value: &Value) -> Option<u32> {
    u32::try_from(value.as_u64()?).ok()
}

fn optional_u16(payload: &Map<String, Value>, field: &str) -> Option<Option<u16>> {
    match payload.get(field) {
        Some(value) => Some(Some(bounded_u16(value)?)),
        None => Some(None),
    }
}
