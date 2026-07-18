//! Adapter from internal run-stream protobuf events to the public runtime event taxonomy.

use palyra_common::runtime_contracts::{
    project_runtime_event_v2_to_public, validate_public_runtime_event,
    PublicRuntimeEventCorrelation, PublicRuntimeEventEnvelope, PublicRuntimeEventName,
    PublicRuntimeEventProjectionContext, RuntimeApprovalSubjectId, RuntimeEventEnvelopeV2,
    RuntimeEventId, RuntimeEventPayloadRef, RuntimeGeneration, RuntimeIdentitySetV1,
    RuntimeOperationId, RuntimeToolExecutionId, RuntimeToolProposalId,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{gateway::CANCELLED_REASON, transport::grpc::proto::palyra::common::v1 as common_v1};

/// Context needed to wrap one internal run-stream event as a public runtime event.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PublicRunStreamEventContext<'a> {
    pub(crate) event_id: &'a str,
    pub(crate) session_id: &'a str,
    pub(crate) generation: RuntimeGeneration,
    pub(crate) sequence: u64,
    pub(crate) occurred_at_unix_ms: i64,
    pub(crate) causal_parent_event_id: Option<&'a str>,
    pub(crate) request_id: Option<&'a str>,
}

/// Builds a deterministic public event id for stream adapters that only have local sequence.
#[must_use]
pub(crate) fn run_stream_public_event_id(run_id: &str, sequence: u64) -> String {
    format!("run_stream:{run_id}:{sequence}")
}

/// Converts a protobuf run-stream event into a public runtime event envelope.
///
/// Returns `None` for internal-only protobuf variants that do not have a
/// public event in the current taxonomy.
#[must_use]
pub(crate) fn public_runtime_event_from_run_stream_event(
    event: &common_v1::RunStreamEvent,
    context: PublicRunStreamEventContext<'_>,
) -> Option<PublicRuntimeEventEnvelope> {
    let run_id = event.run_id.as_ref()?.ulid.clone();
    let (event_name, mut correlation, payload) = public_event_parts(event)?;
    correlation.run_id.get_or_insert(run_id);
    correlation.session_id.get_or_insert_with(|| context.session_id.to_owned());
    if correlation.trace_id.is_none() {
        correlation.trace_id = correlation.run_id.clone();
    }
    correlation.generation.get_or_insert(context.generation.get());
    correlation.sequence.get_or_insert(context.sequence);
    if let Some(causal_parent_event_id) = context.causal_parent_event_id {
        correlation.causal_parent_event_id.get_or_insert_with(|| causal_parent_event_id.to_owned());
    }
    if let Some(request_id) = context.request_id {
        correlation.request_id.get_or_insert_with(|| request_id.to_owned());
    }
    if event_name.runtime_event_name().is_some() {
        let runtime_event =
            runtime_event_v2_from_run_stream_parts(event_name, &correlation, &payload, context)?;
        return project_runtime_event_v2_to_public(
            &runtime_event,
            payload,
            PublicRuntimeEventProjectionContext {
                parent_run_id: correlation.parent_run_id,
                request_id: correlation.request_id,
            },
        )
        .ok();
    }

    // Public-only compatibility events still carry V2 ordering correlation even though the
    // closed durable registry has not assigned them persistence semantics.
    let descriptor = event_name.descriptor();
    let public_event = PublicRuntimeEventEnvelope {
        schema_version: descriptor.schema_version,
        event: event_name,
        event_id: context.event_id.to_owned(),
        occurred_at_unix_ms: context.occurred_at_unix_ms,
        correlation,
        visibility: descriptor.visibility,
        redaction: descriptor.redaction,
        journal_mapping: descriptor.journal_mapping,
        payload,
        extensions: Default::default(),
    };
    validate_public_runtime_event(&public_event).ok()?;
    Some(public_event)
}

fn runtime_event_v2_from_run_stream_parts(
    public_name: PublicRuntimeEventName,
    correlation: &PublicRuntimeEventCorrelation,
    public_payload: &Value,
    context: PublicRunStreamEventContext<'_>,
) -> Option<RuntimeEventEnvelopeV2> {
    let event_name = public_name.runtime_event_name()?;
    let descriptor = event_name.descriptor();
    let run_id = correlation.run_id.as_deref()?;
    let (mut identities, legacy_adapter) =
        RuntimeIdentitySetV1::from_legacy_run(context.session_id, run_id, context.generation)
            .ok()?;
    if let Some(proposal_id) = correlation.tool_call_id.as_deref() {
        identities.tool_proposal_id = Some(RuntimeToolProposalId::parse(proposal_id).ok()?);
    }
    if let Some(approval_id) = correlation.approval_id.as_deref() {
        identities.approval_subject_id = Some(RuntimeApprovalSubjectId::parse(approval_id).ok()?);
    }
    if event_name == palyra_common::runtime_contracts::RuntimeEventName::ToolResultObserved {
        let proposal_id = correlation.tool_call_id.as_deref()?;
        identities.tool_execution_id =
            Some(RuntimeToolExecutionId::parse(format!("execution:{proposal_id}").as_str()).ok()?);
        identities.operation_id =
            Some(RuntimeOperationId::parse(format!("operation:{proposal_id}").as_str()).ok()?);
    }
    let payload_bytes = serde_json::to_vec(public_payload).ok()?;
    let mut envelope = RuntimeEventEnvelopeV2 {
        schema_version: palyra_common::runtime_contracts::RUNTIME_EVENT_ENVELOPE_SCHEMA_VERSION,
        event_id: RuntimeEventId::parse(context.event_id).ok()?,
        identities,
        sequence: context.sequence,
        causal_parent_event_id: context
            .causal_parent_event_id
            .map(RuntimeEventId::parse)
            .transpose()
            .ok()?,
        subsystem: descriptor.subsystem,
        phase: descriptor.phase,
        event_name,
        reason_code: format!("runtime.event.{}", event_name.as_str().replace('.', "_")),
        actor_kind: descriptor.actor_kind,
        retryability: descriptor.retryability,
        redaction_class: descriptor.redaction_class,
        terminal: descriptor.terminal,
        payload: RuntimeEventPayloadRef::Omitted {
            reason_code: "runtime.event.public_payload_projected".to_owned(),
            digest_sha256: Some(hex::encode(Sha256::digest(payload_bytes.as_slice()))),
            size_bytes: u64::try_from(payload_bytes.len()).unwrap_or(u64::MAX),
        },
        occurred_at_unix_ms: context.occurred_at_unix_ms,
        extensions: Default::default(),
    };
    if let Some(legacy_adapter) = legacy_adapter.reconcile_with_identities(&envelope.identities) {
        envelope.record_legacy_identity_adapter(legacy_adapter).ok()?;
    }
    envelope.validate().ok()?;
    Some(envelope)
}

/// Converts a protobuf run-stream event into JSON suitable for console or compat metadata.
#[must_use]
pub(crate) fn public_runtime_event_json_from_run_stream_event(
    event: &common_v1::RunStreamEvent,
    context: PublicRunStreamEventContext<'_>,
) -> Option<Value> {
    public_runtime_event_from_run_stream_event(event, context)
        .and_then(|event| serde_json::to_value(event).ok())
}

/// Projects a replayed wire event through its exact persisted V2 authority.
///
/// This path deliberately rejects event-name mismatches instead of rebuilding
/// generation or ordering correlation from a replay cursor.
#[must_use]
pub(crate) fn public_runtime_event_json_from_persisted_run_stream_event(
    event: &common_v1::RunStreamEvent,
    runtime_event: &RuntimeEventEnvelopeV2,
) -> Option<Value> {
    let (public_name, _, payload) = public_event_parts(event)?;
    if public_name.runtime_event_name()? != runtime_event.event_name {
        return None;
    }
    project_runtime_event_v2_to_public(
        runtime_event,
        payload,
        PublicRuntimeEventProjectionContext::default(),
    )
    .ok()
    .and_then(|event| serde_json::to_value(event).ok())
}

/// Returns whether a wire event belongs to the canonical V2/public mapping.
#[must_use]
pub(crate) fn run_stream_event_requires_persisted_v2(event: &common_v1::RunStreamEvent) -> bool {
    public_event_parts(event)
        .and_then(|(public_name, _, _)| public_name.runtime_event_name())
        .is_some()
}

fn public_event_parts(
    event: &common_v1::RunStreamEvent,
) -> Option<(PublicRuntimeEventName, PublicRuntimeEventCorrelation, Value)> {
    match event.body.as_ref()? {
        common_v1::run_stream_event::Body::Status(status) => {
            let kind = status_kind(status.kind);
            let event_name = public_status_event_name(kind, status.message.as_str())?;
            let payload = if event_name == PublicRuntimeEventName::Heartbeat {
                json!({
                    "status": "alive",
                    "message": status.message,
                })
            } else {
                json!({
                    "status": status_kind_label(kind),
                    "message": status.message,
                    "reason_code": public_status_reason_code(kind, status.message.as_str()),
                })
            };
            Some((event_name, PublicRuntimeEventCorrelation::default(), payload))
        }
        common_v1::run_stream_event::Body::ModelToken(model_token) => Some((
            PublicRuntimeEventName::ModelDelta,
            PublicRuntimeEventCorrelation::default(),
            json!({
                "delta": model_token.token,
                "is_final": model_token.is_final,
            }),
        )),
        common_v1::run_stream_event::Body::ToolProposal(proposal) => Some((
            PublicRuntimeEventName::ToolCallStarted,
            tool_correlation(proposal.proposal_id.as_ref()),
            json!({
                "tool_name": proposal.tool_name,
                "input_json": json_from_bytes(proposal.input_json.as_slice()),
                "approval_required": proposal.approval_required,
            }),
        )),
        common_v1::run_stream_event::Body::ToolDecision(decision) => Some((
            PublicRuntimeEventName::ToolCallDelta,
            tool_correlation(decision.proposal_id.as_ref()),
            json!({
                "kind": tool_decision_kind_label(decision.kind),
                "reason": decision.reason,
                "approval_required": decision.approval_required,
                "policy_enforced": decision.policy_enforced,
            }),
        )),
        common_v1::run_stream_event::Body::ToolResult(result) => Some((
            PublicRuntimeEventName::ToolCallCompleted,
            tool_correlation(result.proposal_id.as_ref()),
            json!({
                "success": result.success,
                "output_json": json_from_bytes(result.output_json.as_slice()),
                "error": result.error,
            }),
        )),
        common_v1::run_stream_event::Body::ToolApprovalRequest(request) => Some((
            PublicRuntimeEventName::ApprovalRequired,
            approval_correlation(request.proposal_id.as_ref(), request.approval_id.as_ref()),
            json!({
                "tool_name": request.tool_name,
                "request_summary": request.request_summary,
                "approval_required": request.approval_required,
                "input_json": json_from_bytes(request.input_json.as_slice()),
                "prompt": request.prompt.as_ref().map(approval_prompt_json),
            }),
        )),
        common_v1::run_stream_event::Body::ToolApprovalResponse(response) => Some((
            PublicRuntimeEventName::ApprovalResolved,
            approval_correlation(response.proposal_id.as_ref(), response.approval_id.as_ref()),
            json!({
                "approved": response.approved,
                "reason": response.reason,
                "decision_scope": approval_scope_label(response.decision_scope),
                "decision_scope_ttl_ms": response.decision_scope_ttl_ms,
            }),
        )),
        _ => None,
    }
}

fn public_status_event_name(
    kind: common_v1::stream_status::StatusKind,
    message: &str,
) -> Option<PublicRuntimeEventName> {
    match kind {
        common_v1::stream_status::StatusKind::Accepted => Some(PublicRuntimeEventName::RunQueued),
        common_v1::stream_status::StatusKind::InProgress if is_progress_heartbeat(message) => {
            Some(PublicRuntimeEventName::Heartbeat)
        }
        common_v1::stream_status::StatusKind::InProgress => {
            Some(PublicRuntimeEventName::RunStarted)
        }
        common_v1::stream_status::StatusKind::Done => Some(PublicRuntimeEventName::RunCompleted),
        common_v1::stream_status::StatusKind::Failed if message == CANCELLED_REASON => {
            Some(PublicRuntimeEventName::RunCancelled)
        }
        common_v1::stream_status::StatusKind::Failed => Some(PublicRuntimeEventName::RunFailed),
        common_v1::stream_status::StatusKind::Unspecified => None,
    }
}

fn is_progress_heartbeat(message: &str) -> bool {
    message.starts_with("waiting for ") || message.starts_with("progress:")
}

fn public_status_reason_code(
    kind: common_v1::stream_status::StatusKind,
    message: &str,
) -> Option<&'static str> {
    match kind {
        common_v1::stream_status::StatusKind::Accepted => Some("run_queued"),
        common_v1::stream_status::StatusKind::InProgress => Some("run_started"),
        common_v1::stream_status::StatusKind::Done => Some("run_completed"),
        common_v1::stream_status::StatusKind::Failed if message == CANCELLED_REASON => {
            Some("cancelled_by_request")
        }
        common_v1::stream_status::StatusKind::Failed => Some("run_failed"),
        common_v1::stream_status::StatusKind::Unspecified => None,
    }
}

fn status_kind(raw: i32) -> common_v1::stream_status::StatusKind {
    common_v1::stream_status::StatusKind::try_from(raw)
        .unwrap_or(common_v1::stream_status::StatusKind::Unspecified)
}

fn status_kind_label(kind: common_v1::stream_status::StatusKind) -> &'static str {
    match kind {
        common_v1::stream_status::StatusKind::Accepted => "accepted",
        common_v1::stream_status::StatusKind::InProgress => "in_progress",
        common_v1::stream_status::StatusKind::Done => "done",
        common_v1::stream_status::StatusKind::Failed => "failed",
        common_v1::stream_status::StatusKind::Unspecified => "unspecified",
    }
}

fn tool_decision_kind_label(raw: i32) -> &'static str {
    match common_v1::tool_decision::DecisionKind::try_from(raw)
        .unwrap_or(common_v1::tool_decision::DecisionKind::Unspecified)
    {
        common_v1::tool_decision::DecisionKind::Allow => "allow",
        common_v1::tool_decision::DecisionKind::Deny => "deny",
        common_v1::tool_decision::DecisionKind::Unspecified => "unspecified",
    }
}

fn approval_scope_label(raw: i32) -> &'static str {
    match common_v1::ApprovalDecisionScope::try_from(raw)
        .unwrap_or(common_v1::ApprovalDecisionScope::Unspecified)
    {
        common_v1::ApprovalDecisionScope::Once => "once",
        common_v1::ApprovalDecisionScope::Session => "session",
        common_v1::ApprovalDecisionScope::Timeboxed => "timeboxed",
        common_v1::ApprovalDecisionScope::Unspecified => "unspecified",
    }
}

fn tool_correlation(proposal_id: Option<&common_v1::CanonicalId>) -> PublicRuntimeEventCorrelation {
    PublicRuntimeEventCorrelation {
        tool_call_id: proposal_id.map(|value| value.ulid.clone()),
        ..PublicRuntimeEventCorrelation::default()
    }
}

fn approval_correlation(
    proposal_id: Option<&common_v1::CanonicalId>,
    approval_id: Option<&common_v1::CanonicalId>,
) -> PublicRuntimeEventCorrelation {
    PublicRuntimeEventCorrelation {
        approval_id: approval_id.map(|value| value.ulid.clone()),
        ..tool_correlation(proposal_id)
    }
}

fn json_from_bytes(bytes: &[u8]) -> Value {
    serde_json::from_slice::<Value>(bytes)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(bytes).to_string() }))
}

fn approval_prompt_json(prompt: &common_v1::ApprovalPrompt) -> Value {
    json!({
        "title": prompt.title,
        "risk_level": approval_risk_level_label(prompt.risk_level),
        "subject_id": prompt.subject_id,
        "summary": prompt.summary,
        "timeout_seconds": prompt.timeout_seconds,
        "details_json": json_from_bytes(prompt.details_json.as_slice()),
        "policy_explanation": prompt.policy_explanation,
        "options": prompt.options.iter().map(|option| {
            json!({
                "option_id": option.option_id,
                "label": option.label,
                "description": option.description,
                "default_selected": option.default_selected,
                "decision_scope": approval_scope_label(option.decision_scope),
                "timebox_ttl_ms": option.timebox_ttl_ms,
            })
        }).collect::<Vec<_>>(),
    })
}

fn approval_risk_level_label(raw: i32) -> &'static str {
    match common_v1::ApprovalRiskLevel::try_from(raw)
        .unwrap_or(common_v1::ApprovalRiskLevel::Unspecified)
    {
        common_v1::ApprovalRiskLevel::Low => "low",
        common_v1::ApprovalRiskLevel::Medium => "medium",
        common_v1::ApprovalRiskLevel::High => "high",
        common_v1::ApprovalRiskLevel::Critical => "critical",
        common_v1::ApprovalRiskLevel::Unspecified => "unspecified",
    }
}

#[cfg(test)]
mod tests {
    use palyra_common::runtime_contracts::{
        validate_public_runtime_event_sequence, PublicRuntimeEventName,
    };

    use super::*;

    fn context(event_id: &str) -> PublicRunStreamEventContext<'_> {
        let sequence =
            event_id.rsplit_once('_').and_then(|(_, value)| value.parse::<u64>().ok()).unwrap_or(1);
        PublicRunStreamEventContext {
            event_id,
            session_id: "session_01",
            generation: RuntimeGeneration::new(1).expect("generation"),
            sequence,
            occurred_at_unix_ms: 42,
            causal_parent_event_id: None,
            request_id: Some("request_01"),
        }
    }

    fn run_event(body: common_v1::run_stream_event::Body) -> common_v1::RunStreamEvent {
        common_v1::RunStreamEvent {
            v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
            run_id: Some(common_v1::CanonicalId { ulid: "run_01".to_owned() }),
            body: Some(body),
        }
    }

    #[test]
    fn run_stream_model_token_maps_to_public_model_delta() {
        let event =
            run_event(common_v1::run_stream_event::Body::ModelToken(common_v1::ModelToken {
                token: "hello".to_owned(),
                is_final: false,
            }));

        let public_event = public_runtime_event_from_run_stream_event(&event, context("evt_1"))
            .expect("model token should map");

        assert_eq!(public_event.event, PublicRuntimeEventName::ModelDelta);
        assert_eq!(public_event.correlation.run_id.as_deref(), Some("run_01"));
        assert_eq!(public_event.correlation.session_id.as_deref(), Some("session_01"));
        assert_eq!(public_event.correlation.generation, Some(1));
        assert_eq!(public_event.correlation.sequence, Some(1));
        assert_eq!(public_event.payload["delta"], "hello");
    }

    #[test]
    fn run_stream_model_token_uses_v2_hash_only_payload_boundary() {
        let event =
            run_event(common_v1::run_stream_event::Body::ModelToken(common_v1::ModelToken {
                token: "sensitive model output".to_owned(),
                is_final: false,
            }));
        let context = context("evt_7");
        let (public_name, mut correlation, payload) =
            public_event_parts(&event).expect("model token parts");
        correlation.run_id = Some("run_01".to_owned());
        correlation.session_id = Some("session_01".to_owned());

        let runtime_event =
            runtime_event_v2_from_run_stream_parts(public_name, &correlation, &payload, context)
                .expect("model token should map through V2");

        assert_eq!(
            runtime_event.event_name,
            palyra_common::runtime_contracts::RuntimeEventName::ModelDelta
        );
        assert_eq!(runtime_event.identities.generation.get(), 1);
        assert_eq!(runtime_event.sequence, 7);
        let serialized =
            serde_json::to_string(&runtime_event).expect("runtime event should serialize");
        assert!(!serialized.contains("sensitive model output"));
        assert!(matches!(
            runtime_event.payload,
            RuntimeEventPayloadRef::Omitted { digest_sha256: Some(_), .. }
        ));
    }

    #[test]
    fn replay_projection_uses_exact_persisted_v2_ordering() {
        let event =
            run_event(common_v1::run_stream_event::Body::ModelToken(common_v1::ModelToken {
                token: "replayed output".to_owned(),
                is_final: false,
            }));
        let (public_name, mut correlation, payload) =
            public_event_parts(&event).expect("model token parts");
        correlation.run_id = Some("run_01".to_owned());
        correlation.session_id = Some("session_01".to_owned());
        let mut runtime_event = runtime_event_v2_from_run_stream_parts(
            public_name,
            &correlation,
            &payload,
            context("evt_1"),
        )
        .expect("model token should map through V2");
        runtime_event.event_id = RuntimeEventId::parse("run_stream:run_01:12").expect("event id");
        runtime_event.sequence = 19;
        runtime_event.occurred_at_unix_ms = 1234;

        let public_event =
            public_runtime_event_json_from_persisted_run_stream_event(&event, &runtime_event)
                .expect("persisted projection");

        assert_eq!(public_event["event_id"], "run_stream:run_01:12");
        assert_eq!(public_event["occurred_at_unix_ms"], 1234);
        assert_eq!(public_event["correlation"]["generation"], 1);
        assert_eq!(public_event["correlation"]["sequence"], 19);
        assert_eq!(public_event["payload"]["delta"], "replayed output");
    }

    #[test]
    fn run_stream_waiting_status_maps_to_token_free_heartbeat() {
        let event = run_event(common_v1::run_stream_event::Body::Status(common_v1::StreamStatus {
            kind: common_v1::stream_status::StatusKind::InProgress as i32,
            message: "waiting for model provider response (elapsed_ms=20000, timeout_ms=90000)"
                .to_owned(),
        }));

        let public_event = public_runtime_event_from_run_stream_event(&event, context("evt_1"))
            .expect("waiting status should map to heartbeat");

        assert_eq!(public_event.event, PublicRuntimeEventName::Heartbeat);
        assert_eq!(public_event.payload["status"], "alive");
        assert!(public_event.payload.get("delta").is_none());
        assert!(public_event.payload.get("token").is_none());
    }

    #[test]
    fn run_stream_lifecycle_progress_maps_to_token_free_heartbeat() {
        let event = run_event(common_v1::run_stream_event::Body::Status(common_v1::StreamStatus {
            kind: common_v1::stream_status::StatusKind::InProgress as i32,
            message: "progress:agent_loop.started".to_owned(),
        }));

        let public_event = public_runtime_event_from_run_stream_event(&event, context("evt_1"))
            .expect("lifecycle progress should map to heartbeat");

        assert_eq!(public_event.event, PublicRuntimeEventName::Heartbeat);
        assert_eq!(public_event.payload["status"], "alive");
        assert!(public_event.payload.get("delta").is_none());
        assert!(public_event.payload.get("token").is_none());
    }

    #[test]
    fn run_stream_tool_approval_sequence_maps_to_public_ordering() {
        let tool_started =
            run_event(common_v1::run_stream_event::Body::ToolProposal(common_v1::ToolProposal {
                proposal_id: Some(common_v1::CanonicalId { ulid: "tool_01".to_owned() }),
                tool_name: "shell_command".to_owned(),
                input_json: br#"{"command":"date"}"#.to_vec(),
                approval_required: true,
            }));
        let approval_required = run_event(common_v1::run_stream_event::Body::ToolApprovalRequest(
            common_v1::ToolApprovalRequest {
                proposal_id: Some(common_v1::CanonicalId { ulid: "tool_01".to_owned() }),
                tool_name: "shell_command".to_owned(),
                input_json: br#"{"command":"date"}"#.to_vec(),
                approval_required: true,
                approval_id: Some(common_v1::CanonicalId { ulid: "approval_01".to_owned() }),
                prompt: Some(common_v1::ApprovalPrompt {
                    title: "Run command".to_owned(),
                    risk_level: common_v1::ApprovalRiskLevel::Low as i32,
                    subject_id: "tool_01".to_owned(),
                    summary: "Run date".to_owned(),
                    timeout_seconds: 30,
                    details_json: b"{}".to_vec(),
                    policy_explanation: "approval required".to_owned(),
                    options: Vec::new(),
                }),
                request_summary: "Run date".to_owned(),
            },
        ));
        let approval_resolved = run_event(common_v1::run_stream_event::Body::ToolApprovalResponse(
            common_v1::ToolApprovalResponse {
                proposal_id: Some(common_v1::CanonicalId { ulid: "tool_01".to_owned() }),
                approved: true,
                reason: "approved".to_owned(),
                approval_id: Some(common_v1::CanonicalId { ulid: "approval_01".to_owned() }),
                decision_scope: common_v1::ApprovalDecisionScope::Once as i32,
                decision_scope_ttl_ms: 0,
            },
        ));
        let tool_completed =
            run_event(common_v1::run_stream_event::Body::ToolResult(common_v1::ToolResult {
                proposal_id: Some(common_v1::CanonicalId { ulid: "tool_01".to_owned() }),
                success: true,
                output_json: b"{}".to_vec(),
                error: String::new(),
            }));

        let events = [
            (&tool_started, "evt_1"),
            (&approval_required, "evt_2"),
            (&approval_resolved, "evt_3"),
            (&tool_completed, "evt_4"),
        ]
        .into_iter()
        .map(|(event, event_id)| {
            public_runtime_event_from_run_stream_event(event, context(event_id))
                .expect("run stream event should map")
        })
        .collect::<Vec<_>>();

        validate_public_runtime_event_sequence(events.as_slice())
            .expect("approval sequence should follow public event ordering");
    }
}
