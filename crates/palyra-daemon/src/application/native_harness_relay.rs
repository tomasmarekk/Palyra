//! Host-owned native harness relay contracts.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const NATIVE_HARNESS_RELAY_SCHEMA_VERSION: u32 = 1;
pub const NATIVE_HARNESS_RELAY_AUDIT_EVENT: &str = "native_harness_relay.invocation";
const MAX_RELAY_PAYLOAD_BYTES: usize = 16 * 1024;
const MAX_RELAY_JSON_DEPTH: usize = 8;
const MAX_RELAY_JSON_NODES: usize = 512;
const MAX_RELAY_STRING_BYTES: usize = 2 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeHarnessRelayRegistration {
    pub run_id: String,
    pub session_id: String,
    pub harness_id: String,
    pub generation: u64,
    pub registered_at_unix_ms: i64,
    pub ttl_ms: i64,
    pub cancelled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeHarnessRelayEventKind {
    PreToolUse,
    PostToolUse,
    PermissionRequest,
    BeforeAgentFinalize,
}

impl NativeHarnessRelayEventKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreToolUse => "pre_tool_use",
            Self::PostToolUse => "post_tool_use",
            Self::PermissionRequest => "permission_request",
            Self::BeforeAgentFinalize => "before_agent_finalize",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeHarnessRelayRequest {
    pub event_kind: NativeHarnessRelayEventKind,
    pub generation: u64,
    pub observed_at_unix_ms: i64,
    pub payload: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeHarnessRelayReasonCode {
    RelayAccepted,
    PermissionMappedToApproval,
    FinalizeRevisionRequested,
    PayloadTooLarge,
    PayloadTooDeep,
    PayloadTooManyNodes,
    PayloadStringTooLarge,
    PostToolVisibilityEscalation,
    PermissionRateLimited,
    ExpiredRelay,
    StaleGeneration,
    CancelledRelay,
    ReviseBudgetExhausted,
}

impl NativeHarnessRelayReasonCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RelayAccepted => "native_relay.accepted",
            Self::PermissionMappedToApproval => "native_relay.permission_mapped_to_approval",
            Self::FinalizeRevisionRequested => "native_relay.finalize_revision_requested",
            Self::PayloadTooLarge => "native_relay.payload_too_large",
            Self::PayloadTooDeep => "native_relay.payload_too_deep",
            Self::PayloadTooManyNodes => "native_relay.payload_too_many_nodes",
            Self::PayloadStringTooLarge => "native_relay.payload_string_too_large",
            Self::PostToolVisibilityEscalation => "native_relay.post_tool_visibility_escalation",
            Self::PermissionRateLimited => "native_relay.permission_rate_limited",
            Self::ExpiredRelay => "native_relay.expired",
            Self::StaleGeneration => "native_relay.stale_generation",
            Self::CancelledRelay => "native_relay.cancelled",
            Self::ReviseBudgetExhausted => "native_relay.revise_budget_exhausted",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeHarnessRelayDecisionKind {
    Forwarded,
    ApprovalRequired,
    RevisionRequested,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeHarnessRelayDecision {
    pub schema_version: u32,
    pub event_kind: NativeHarnessRelayEventKind,
    pub decision: NativeHarnessRelayDecisionKind,
    pub reason_codes: Vec<NativeHarnessRelayReasonCode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_projection: Option<NativeHarnessRelayApprovalProjection>,
    pub approval_authority_granted: bool,
    pub direct_journal_authority_granted: bool,
    pub tool_executor_authority_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeHarnessRelayApprovalProjection {
    pub broker: String,
    pub subject_hash: String,
    pub approval_reason_code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeHarnessRelayAuditRecord {
    pub schema_version: u32,
    pub event_name: String,
    pub run_id: String,
    pub session_id: String,
    pub harness_id: String,
    pub event_kind: NativeHarnessRelayEventKind,
    pub generation: u64,
    pub decision: NativeHarnessRelayDecisionKind,
    pub reason_codes: Vec<NativeHarnessRelayReasonCode>,
    pub payload_bytes: usize,
    pub fail_closed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_requests_remaining: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeHarnessRelayEvaluation {
    pub decision: NativeHarnessRelayDecision,
    pub audit: NativeHarnessRelayAuditRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeHarnessRelayHostState {
    now_unix_ms: i64,
    revise_budget_remaining: u32,
}

impl NativeHarnessRelayHostState {
    #[must_use]
    pub const fn new(now_unix_ms: i64, revise_budget_remaining: u32) -> Self {
        Self { now_unix_ms, revise_budget_remaining }
    }

    #[must_use]
    pub const fn revise_budget_remaining(&self) -> u32 {
        self.revise_budget_remaining
    }

    fn consume_revise_budget(&mut self) -> bool {
        if self.revise_budget_remaining == 0 {
            return false;
        }
        self.revise_budget_remaining = self.revise_budget_remaining.saturating_sub(1);
        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeHarnessRelayRateLimit {
    max_permission_requests: u32,
    permission_requests_used: u32,
}

impl NativeHarnessRelayRateLimit {
    #[must_use]
    pub const fn new(max_permission_requests: u32) -> Self {
        Self { max_permission_requests, permission_requests_used: 0 }
    }

    #[must_use]
    pub const fn permission_requests_remaining(&self) -> u32 {
        self.max_permission_requests.saturating_sub(self.permission_requests_used)
    }

    fn consume_permission_request(&mut self) -> bool {
        if self.permission_requests_used >= self.max_permission_requests {
            return false;
        }
        self.permission_requests_used = self.permission_requests_used.saturating_add(1);
        true
    }
}

#[must_use]
pub fn evaluate_native_harness_relay(
    registration: &NativeHarnessRelayRegistration,
    request: &NativeHarnessRelayRequest,
    host_state: &mut NativeHarnessRelayHostState,
) -> NativeHarnessRelayDecision {
    evaluate_native_harness_relay_with_audit(registration, request, host_state, None).decision
}

#[must_use]
pub fn evaluate_native_harness_relay_with_audit(
    registration: &NativeHarnessRelayRegistration,
    request: &NativeHarnessRelayRequest,
    host_state: &mut NativeHarnessRelayHostState,
    mut rate_limit: Option<&mut NativeHarnessRelayRateLimit>,
) -> NativeHarnessRelayEvaluation {
    let decision = if registration.cancelled {
        rejected(request.event_kind, NativeHarnessRelayReasonCode::CancelledRelay)
    } else if request.generation != registration.generation {
        rejected(request.event_kind, NativeHarnessRelayReasonCode::StaleGeneration)
    } else if host_state.now_unix_ms
        > registration.registered_at_unix_ms.saturating_add(registration.ttl_ms.max(0))
    {
        rejected(request.event_kind, NativeHarnessRelayReasonCode::ExpiredRelay)
    } else if let Some(reason) = relay_payload_cap_reason(&request.payload) {
        rejected(request.event_kind, reason)
    } else if request.event_kind == NativeHarnessRelayEventKind::PostToolUse
        && post_tool_visibility_escalates(&request.payload)
    {
        rejected(request.event_kind, NativeHarnessRelayReasonCode::PostToolVisibilityEscalation)
    } else if request.event_kind == NativeHarnessRelayEventKind::PermissionRequest
        && rate_limit.as_deref_mut().is_some_and(|limit| !limit.consume_permission_request())
    {
        rejected(request.event_kind, NativeHarnessRelayReasonCode::PermissionRateLimited)
    } else if request.event_kind == NativeHarnessRelayEventKind::BeforeAgentFinalize
        && !host_state.consume_revise_budget()
    {
        rejected(request.event_kind, NativeHarnessRelayReasonCode::ReviseBudgetExhausted)
    } else {
        accepted_decision(registration, request)
    };

    let payload_bytes = serde_json::to_vec(&request.payload).map_or(0, |encoded| encoded.len());
    let permission_requests_remaining =
        rate_limit.as_deref().map(NativeHarnessRelayRateLimit::permission_requests_remaining);
    NativeHarnessRelayEvaluation {
        audit: NativeHarnessRelayAuditRecord {
            schema_version: NATIVE_HARNESS_RELAY_SCHEMA_VERSION,
            event_name: NATIVE_HARNESS_RELAY_AUDIT_EVENT.to_owned(),
            run_id: palyra_common::redaction::redact_diagnostic_text(registration.run_id.as_str()),
            session_id: palyra_common::redaction::redact_diagnostic_text(
                registration.session_id.as_str(),
            ),
            harness_id: palyra_common::redaction::redact_diagnostic_text(
                registration.harness_id.as_str(),
            ),
            event_kind: request.event_kind,
            generation: request.generation,
            decision: decision.decision,
            reason_codes: decision.reason_codes.clone(),
            payload_bytes,
            fail_closed: decision.decision == NativeHarnessRelayDecisionKind::Rejected,
            permission_requests_remaining,
        },
        decision,
    }
}

fn accepted_decision(
    registration: &NativeHarnessRelayRegistration,
    request: &NativeHarnessRelayRequest,
) -> NativeHarnessRelayDecision {
    match request.event_kind {
        NativeHarnessRelayEventKind::PermissionRequest => NativeHarnessRelayDecision {
            schema_version: NATIVE_HARNESS_RELAY_SCHEMA_VERSION,
            event_kind: request.event_kind,
            decision: NativeHarnessRelayDecisionKind::ApprovalRequired,
            reason_codes: vec![
                NativeHarnessRelayReasonCode::RelayAccepted,
                NativeHarnessRelayReasonCode::PermissionMappedToApproval,
            ],
            approval_projection: Some(permission_approval_projection(registration, request)),
            approval_authority_granted: false,
            direct_journal_authority_granted: false,
            tool_executor_authority_granted: false,
        },
        NativeHarnessRelayEventKind::BeforeAgentFinalize => NativeHarnessRelayDecision {
            schema_version: NATIVE_HARNESS_RELAY_SCHEMA_VERSION,
            event_kind: request.event_kind,
            decision: NativeHarnessRelayDecisionKind::RevisionRequested,
            reason_codes: vec![
                NativeHarnessRelayReasonCode::RelayAccepted,
                NativeHarnessRelayReasonCode::FinalizeRevisionRequested,
            ],
            approval_projection: None,
            approval_authority_granted: false,
            direct_journal_authority_granted: false,
            tool_executor_authority_granted: false,
        },
        NativeHarnessRelayEventKind::PreToolUse | NativeHarnessRelayEventKind::PostToolUse => {
            NativeHarnessRelayDecision {
                schema_version: NATIVE_HARNESS_RELAY_SCHEMA_VERSION,
                event_kind: request.event_kind,
                decision: NativeHarnessRelayDecisionKind::Forwarded,
                reason_codes: vec![NativeHarnessRelayReasonCode::RelayAccepted],
                approval_projection: None,
                approval_authority_granted: false,
                direct_journal_authority_granted: false,
                tool_executor_authority_granted: false,
            }
        }
    }
}

fn rejected(
    event_kind: NativeHarnessRelayEventKind,
    reason: NativeHarnessRelayReasonCode,
) -> NativeHarnessRelayDecision {
    NativeHarnessRelayDecision {
        schema_version: NATIVE_HARNESS_RELAY_SCHEMA_VERSION,
        event_kind,
        decision: NativeHarnessRelayDecisionKind::Rejected,
        reason_codes: vec![reason],
        approval_projection: None,
        approval_authority_granted: false,
        direct_journal_authority_granted: false,
        tool_executor_authority_granted: false,
    }
}

fn permission_approval_projection(
    registration: &NativeHarnessRelayRegistration,
    request: &NativeHarnessRelayRequest,
) -> NativeHarnessRelayApprovalProjection {
    let subject = format!(
        "{}:{}:{}:{}",
        registration.run_id, registration.session_id, registration.harness_id, request.generation
    );
    NativeHarnessRelayApprovalProjection {
        broker: "palyra_approval_broker".to_owned(),
        subject_hash: format!("sha256:{}", crate::sha256_hex(subject.as_bytes())),
        approval_reason_code: NativeHarnessRelayReasonCode::PermissionMappedToApproval
            .as_str()
            .to_owned(),
    }
}

fn relay_payload_cap_reason(payload: &Value) -> Option<NativeHarnessRelayReasonCode> {
    let encoded = serde_json::to_vec(payload).ok()?;
    if encoded.len() > MAX_RELAY_PAYLOAD_BYTES {
        return Some(NativeHarnessRelayReasonCode::PayloadTooLarge);
    }
    let mut node_count = 0;
    inspect_json_caps(payload, 0, &mut node_count)
}

fn inspect_json_caps(
    value: &Value,
    depth: usize,
    node_count: &mut usize,
) -> Option<NativeHarnessRelayReasonCode> {
    if depth > MAX_RELAY_JSON_DEPTH {
        return Some(NativeHarnessRelayReasonCode::PayloadTooDeep);
    }
    *node_count = node_count.saturating_add(1);
    if *node_count > MAX_RELAY_JSON_NODES {
        return Some(NativeHarnessRelayReasonCode::PayloadTooManyNodes);
    }
    match value {
        Value::String(raw) if raw.len() > MAX_RELAY_STRING_BYTES => {
            Some(NativeHarnessRelayReasonCode::PayloadStringTooLarge)
        }
        Value::Array(items) => items
            .iter()
            .find_map(|child| inspect_json_caps(child, depth.saturating_add(1), node_count)),
        Value::Object(object) => object
            .values()
            .find_map(|child| inspect_json_caps(child, depth.saturating_add(1), node_count)),
        _ => None,
    }
}

fn post_tool_visibility_escalates(payload: &Value) -> bool {
    payload.get("visibility_escalation").and_then(Value::as_bool).unwrap_or(false)
        || payload
            .get("requested_visibility")
            .and_then(Value::as_str)
            .map(|visibility| {
                matches!(
                    visibility.trim().to_ascii_lowercase().as_str(),
                    "model_visible" | "public" | "canonical_evidence"
                )
            })
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn registration() -> NativeHarnessRelayRegistration {
        NativeHarnessRelayRegistration {
            run_id: "run-1".to_owned(),
            session_id: "session-1".to_owned(),
            harness_id: "native-harness".to_owned(),
            generation: 2,
            registered_at_unix_ms: 1_000,
            ttl_ms: 10_000,
            cancelled: false,
        }
    }

    fn request(event_kind: NativeHarnessRelayEventKind) -> NativeHarnessRelayRequest {
        NativeHarnessRelayRequest {
            event_kind,
            generation: 2,
            observed_at_unix_ms: 2_000,
            payload: json!({ "tool_name": "palyra.fs.apply_patch" }),
        }
    }

    fn host_state() -> NativeHarnessRelayHostState {
        NativeHarnessRelayHostState::new(2_000, 1)
    }

    #[test]
    fn native_harness_requests_permission_without_authority() {
        let mut rate_limit = NativeHarnessRelayRateLimit::new(2);
        let mut host_state = host_state();
        let evaluation = evaluate_native_harness_relay_with_audit(
            &registration(),
            &request(NativeHarnessRelayEventKind::PermissionRequest),
            &mut host_state,
            Some(&mut rate_limit),
        );
        let decision = evaluation.decision;

        assert_eq!(decision.decision, NativeHarnessRelayDecisionKind::ApprovalRequired);
        assert!(decision.approval_projection.is_some());
        assert!(!decision.approval_authority_granted);
        assert!(!decision.tool_executor_authority_granted);
        assert_eq!(evaluation.audit.event_name, NATIVE_HARNESS_RELAY_AUDIT_EVENT);
        assert_eq!(evaluation.audit.permission_requests_remaining, Some(1));
        assert!(decision
            .reason_codes
            .contains(&NativeHarnessRelayReasonCode::PermissionMappedToApproval));
    }

    #[test]
    fn before_finalize_can_request_bounded_revision() {
        let mut host_state = host_state();
        let decision = evaluate_native_harness_relay(
            &registration(),
            &request(NativeHarnessRelayEventKind::BeforeAgentFinalize),
            &mut host_state,
        );

        assert_eq!(decision.decision, NativeHarnessRelayDecisionKind::RevisionRequested);
        assert_eq!(host_state.revise_budget_remaining(), 0);
        assert!(decision
            .reason_codes
            .contains(&NativeHarnessRelayReasonCode::FinalizeRevisionRequested));
    }

    #[test]
    fn before_finalize_cannot_self_grant_another_revision() {
        let mut host_state = NativeHarnessRelayHostState::new(2_000, 1);
        let request = request(NativeHarnessRelayEventKind::BeforeAgentFinalize);

        let first = evaluate_native_harness_relay(&registration(), &request, &mut host_state);
        let second = evaluate_native_harness_relay(&registration(), &request, &mut host_state);

        assert_eq!(first.decision, NativeHarnessRelayDecisionKind::RevisionRequested);
        assert_eq!(second.decision, NativeHarnessRelayDecisionKind::Rejected);
        assert_eq!(second.reason_codes, [NativeHarnessRelayReasonCode::ReviseBudgetExhausted]);
    }

    #[test]
    fn relay_expiry_uses_host_time() {
        let mut host_state = NativeHarnessRelayHostState::new(20_000, 1);
        let mut request = request(NativeHarnessRelayEventKind::PreToolUse);
        request.observed_at_unix_ms = 1_001;

        let decision = evaluate_native_harness_relay(&registration(), &request, &mut host_state);

        assert_eq!(decision.decision, NativeHarnessRelayDecisionKind::Rejected);
        assert_eq!(decision.reason_codes, [NativeHarnessRelayReasonCode::ExpiredRelay]);
    }

    #[test]
    fn expired_generation_is_denied() {
        let mut host_state = host_state();
        let decision = evaluate_native_harness_relay(
            &registration(),
            &NativeHarnessRelayRequest {
                generation: 1,
                ..request(NativeHarnessRelayEventKind::PreToolUse)
            },
            &mut host_state,
        );

        assert_eq!(decision.decision, NativeHarnessRelayDecisionKind::Rejected);
        assert_eq!(decision.reason_codes, [NativeHarnessRelayReasonCode::StaleGeneration]);
    }

    #[test]
    fn payload_cap_blocks_large_strings() {
        let mut host_state = host_state();
        let decision = evaluate_native_harness_relay(
            &registration(),
            &NativeHarnessRelayRequest {
                payload: json!({ "summary": "x".repeat(MAX_RELAY_STRING_BYTES + 1) }),
                ..request(NativeHarnessRelayEventKind::PostToolUse)
            },
            &mut host_state,
        );

        assert_eq!(decision.decision, NativeHarnessRelayDecisionKind::Rejected);
        assert_eq!(decision.reason_codes, [NativeHarnessRelayReasonCode::PayloadStringTooLarge]);
    }

    #[test]
    fn permission_rate_limit_fails_closed() {
        let mut rate_limit = NativeHarnessRelayRateLimit::new(1);
        let mut host_state = host_state();
        let first = evaluate_native_harness_relay_with_audit(
            &registration(),
            &request(NativeHarnessRelayEventKind::PermissionRequest),
            &mut host_state,
            Some(&mut rate_limit),
        );
        let second = evaluate_native_harness_relay_with_audit(
            &registration(),
            &request(NativeHarnessRelayEventKind::PermissionRequest),
            &mut host_state,
            Some(&mut rate_limit),
        );

        assert_eq!(first.decision.decision, NativeHarnessRelayDecisionKind::ApprovalRequired);
        assert_eq!(second.decision.decision, NativeHarnessRelayDecisionKind::Rejected);
        assert_eq!(
            second.decision.reason_codes,
            [NativeHarnessRelayReasonCode::PermissionRateLimited]
        );
        assert!(second.audit.fail_closed);
    }

    #[test]
    fn post_tool_relay_cannot_escalate_visibility() {
        let mut host_state = host_state();
        let decision = evaluate_native_harness_relay(
            &registration(),
            &NativeHarnessRelayRequest {
                event_kind: NativeHarnessRelayEventKind::PostToolUse,
                payload: json!({
                    "requested_visibility": "model_visible",
                    "summary": "safe metadata",
                }),
                ..request(NativeHarnessRelayEventKind::PostToolUse)
            },
            &mut host_state,
        );

        assert_eq!(decision.decision, NativeHarnessRelayDecisionKind::Rejected);
        assert_eq!(
            decision.reason_codes,
            [NativeHarnessRelayReasonCode::PostToolVisibilityEscalation]
        );
    }
}
