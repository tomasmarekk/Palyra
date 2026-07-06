//! Host-owned native harness relay contracts.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const NATIVE_HARNESS_RELAY_SCHEMA_VERSION: u32 = 1;
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
    pub revise_budget_remaining: u32,
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
    pub approval_authority_granted: bool,
    pub direct_journal_authority_granted: bool,
    pub tool_executor_authority_granted: bool,
}

#[must_use]
pub fn evaluate_native_harness_relay(
    registration: &NativeHarnessRelayRegistration,
    request: &NativeHarnessRelayRequest,
) -> NativeHarnessRelayDecision {
    if registration.cancelled {
        return rejected(request.event_kind, NativeHarnessRelayReasonCode::CancelledRelay);
    }
    if request.generation != registration.generation {
        return rejected(request.event_kind, NativeHarnessRelayReasonCode::StaleGeneration);
    }
    if request.observed_at_unix_ms
        > registration.registered_at_unix_ms.saturating_add(registration.ttl_ms.max(0))
    {
        return rejected(request.event_kind, NativeHarnessRelayReasonCode::ExpiredRelay);
    }
    if let Some(reason) = relay_payload_cap_reason(&request.payload) {
        return rejected(request.event_kind, reason);
    }

    match request.event_kind {
        NativeHarnessRelayEventKind::PermissionRequest => NativeHarnessRelayDecision {
            schema_version: NATIVE_HARNESS_RELAY_SCHEMA_VERSION,
            event_kind: request.event_kind,
            decision: NativeHarnessRelayDecisionKind::ApprovalRequired,
            reason_codes: vec![
                NativeHarnessRelayReasonCode::RelayAccepted,
                NativeHarnessRelayReasonCode::PermissionMappedToApproval,
            ],
            approval_authority_granted: false,
            direct_journal_authority_granted: false,
            tool_executor_authority_granted: false,
        },
        NativeHarnessRelayEventKind::BeforeAgentFinalize if request.revise_budget_remaining > 0 => {
            NativeHarnessRelayDecision {
                schema_version: NATIVE_HARNESS_RELAY_SCHEMA_VERSION,
                event_kind: request.event_kind,
                decision: NativeHarnessRelayDecisionKind::RevisionRequested,
                reason_codes: vec![
                    NativeHarnessRelayReasonCode::RelayAccepted,
                    NativeHarnessRelayReasonCode::FinalizeRevisionRequested,
                ],
                approval_authority_granted: false,
                direct_journal_authority_granted: false,
                tool_executor_authority_granted: false,
            }
        }
        NativeHarnessRelayEventKind::BeforeAgentFinalize => {
            rejected(request.event_kind, NativeHarnessRelayReasonCode::ReviseBudgetExhausted)
        }
        NativeHarnessRelayEventKind::PreToolUse | NativeHarnessRelayEventKind::PostToolUse => {
            NativeHarnessRelayDecision {
                schema_version: NATIVE_HARNESS_RELAY_SCHEMA_VERSION,
                event_kind: request.event_kind,
                decision: NativeHarnessRelayDecisionKind::Forwarded,
                reason_codes: vec![NativeHarnessRelayReasonCode::RelayAccepted],
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
        approval_authority_granted: false,
        direct_journal_authority_granted: false,
        tool_executor_authority_granted: false,
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
            revise_budget_remaining: 1,
        }
    }

    #[test]
    fn native_harness_requests_permission_without_authority() {
        let decision = evaluate_native_harness_relay(
            &registration(),
            &request(NativeHarnessRelayEventKind::PermissionRequest),
        );

        assert_eq!(decision.decision, NativeHarnessRelayDecisionKind::ApprovalRequired);
        assert!(!decision.approval_authority_granted);
        assert!(!decision.tool_executor_authority_granted);
        assert!(decision
            .reason_codes
            .contains(&NativeHarnessRelayReasonCode::PermissionMappedToApproval));
    }

    #[test]
    fn before_finalize_can_request_bounded_revision() {
        let decision = evaluate_native_harness_relay(
            &registration(),
            &request(NativeHarnessRelayEventKind::BeforeAgentFinalize),
        );

        assert_eq!(decision.decision, NativeHarnessRelayDecisionKind::RevisionRequested);
        assert!(decision
            .reason_codes
            .contains(&NativeHarnessRelayReasonCode::FinalizeRevisionRequested));
    }

    #[test]
    fn expired_generation_is_denied() {
        let decision = evaluate_native_harness_relay(
            &registration(),
            &NativeHarnessRelayRequest {
                generation: 1,
                ..request(NativeHarnessRelayEventKind::PreToolUse)
            },
        );

        assert_eq!(decision.decision, NativeHarnessRelayDecisionKind::Rejected);
        assert_eq!(decision.reason_codes, [NativeHarnessRelayReasonCode::StaleGeneration]);
    }

    #[test]
    fn payload_cap_blocks_large_strings() {
        let decision = evaluate_native_harness_relay(
            &registration(),
            &NativeHarnessRelayRequest {
                payload: json!({ "summary": "x".repeat(MAX_RELAY_STRING_BYTES + 1) }),
                ..request(NativeHarnessRelayEventKind::PostToolUse)
            },
        );

        assert_eq!(decision.decision, NativeHarnessRelayDecisionKind::Rejected);
        assert_eq!(decision.reason_codes, [NativeHarnessRelayReasonCode::PayloadStringTooLarge]);
    }
}
