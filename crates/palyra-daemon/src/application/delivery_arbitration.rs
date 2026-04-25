use std::collections::HashMap;

use palyra_common::{
    redaction::redact_url_segments_in_text,
    runtime_contracts::{DeliveryPolicy, ToolResultSensitivity},
    runtime_preview::{RuntimePreviewMode, RUNTIME_PREVIEW_SCHEMA_VERSION},
};
use serde::Serialize;
use serde_json::{json, Value};

use crate::{
    config::DeliveryArbitrationConfig,
    delegation::{DelegationExecutionMode, DelegationMergeApprovalSummary, DelegationSnapshot},
};

pub(crate) const DELIVERY_ARBITRATION_POLICY_ID: &str = "delivery_arbitration.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DeliverySurface {
    WebChat,
    ExternalChannel,
    Notification,
    AuditOnly,
}

impl DeliverySurface {
    #[must_use]
    pub(crate) fn from_channel(channel: Option<&str>) -> Self {
        let Some(raw_channel) = channel else {
            return Self::AuditOnly;
        };
        let normalized = raw_channel.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Self::AuditOnly;
        }
        if normalized == "web"
            || normalized == "console"
            || normalized.starts_with("web:")
            || normalized.starts_with("console:")
        {
            return Self::WebChat;
        }
        if normalized == "cli" || normalized.starts_with("cli:") {
            return Self::Notification;
        }
        Self::ExternalChannel
    }

    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::WebChat => "web_chat",
            Self::ExternalChannel => "external_channel",
            Self::Notification => "notification",
            Self::AuditOnly => "audit_only",
        }
    }

    #[must_use]
    pub(crate) const fn supports_replacement(self) -> bool {
        matches!(self, Self::WebChat)
    }

    #[must_use]
    pub(crate) const fn supports_annotation(self) -> bool {
        matches!(self, Self::WebChat | Self::ExternalChannel | Self::Notification)
    }

    #[must_use]
    pub(crate) const fn progress_presentation(self) -> &'static str {
        match self {
            Self::WebChat => "inline_timeline",
            Self::ExternalChannel => "periodic_summary",
            Self::Notification => "terminal_summary",
            Self::AuditOnly => "audit_only",
        }
    }

    #[must_use]
    pub(crate) const fn refresh_cadence_ms(self) -> u64 {
        match self {
            Self::WebChat => 1_000,
            Self::ExternalChannel => 30_000,
            Self::Notification => 60_000,
            Self::AuditOnly => 0,
        }
    }

    #[must_use]
    pub(crate) const fn max_progress_items(self) -> usize {
        match self {
            Self::WebChat => 8,
            Self::ExternalChannel => 4,
            Self::Notification => 3,
            Self::AuditOnly => 8,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DeliveryPolicySourceKind {
    SessionDefault,
    DelegationProfile,
    FlowDefinition,
}

impl DeliveryPolicySourceKind {
    #[must_use]
    const fn as_str(self) -> &'static str {
        match self {
            Self::SessionDefault => "session_default",
            Self::DelegationProfile => "delegation_profile",
            Self::FlowDefinition => "flow_definition",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliveryPolicySet {
    pub(crate) policy_id: String,
    pub(crate) surface: DeliverySurface,
    pub(crate) source_kind: DeliveryPolicySourceKind,
    pub(crate) source_id: Option<String>,
    pub(crate) policies: Vec<DeliveryPolicy>,
    pub(crate) mode: RuntimePreviewMode,
    pub(crate) descendant_preference: bool,
    pub(crate) suppression_limit: u32,
    pub(crate) delegation_profile_id: Option<String>,
    pub(crate) delegation_template_id: Option<String>,
    pub(crate) delegation_execution_mode: Option<DelegationExecutionMode>,
}

impl DeliveryPolicySet {
    #[must_use]
    pub(crate) fn contains(&self, policy: DeliveryPolicy) -> bool {
        self.policies.contains(&policy)
    }

    #[must_use]
    pub(crate) fn policy_names(&self) -> Vec<&'static str> {
        self.policies.iter().map(|policy| policy.as_str()).collect()
    }

    #[must_use]
    pub(crate) fn snapshot_json(&self) -> Value {
        json!({
            "schema_version": RUNTIME_PREVIEW_SCHEMA_VERSION,
            "policy_id": self.policy_id,
            "mode": self.mode.as_str(),
            "surface": self.surface.as_str(),
            "source": {
                "kind": self.source_kind.as_str(),
                "id": self.source_id,
                "delegation_profile_id": self.delegation_profile_id,
                "delegation_template_id": self.delegation_template_id,
                "delegation_execution_mode": self.delegation_execution_mode,
            },
            "policies": self.policy_names(),
            "descendant_preference": self.descendant_preference,
            "suppression_limit": self.suppression_limit,
            "channel": {
                "supports_replacement": self.surface.supports_replacement(),
                "supports_annotation": self.surface.supports_annotation(),
                "progress_presentation": self.surface.progress_presentation(),
                "refresh_cadence_ms": self.surface.refresh_cadence_ms(),
                "max_progress_items": self.surface.max_progress_items(),
            },
        })
    }
}

#[must_use]
pub(crate) fn resolve_delivery_policy(
    config: &DeliveryArbitrationConfig,
    delegation: Option<&DelegationSnapshot>,
    flow_definition_id: Option<&str>,
    channel: Option<&str>,
) -> DeliveryPolicySet {
    let surface = DeliverySurface::from_channel(channel);
    let source_kind = if delegation.is_some() {
        DeliveryPolicySourceKind::DelegationProfile
    } else if flow_definition_id.is_some() {
        DeliveryPolicySourceKind::FlowDefinition
    } else {
        DeliveryPolicySourceKind::SessionDefault
    };
    let source_id = delegation
        .map(|value| value.profile_id.clone())
        .or_else(|| flow_definition_id.map(ToOwned::to_owned));
    let mut policies =
        vec![DeliveryPolicy::DeliverInterimParent, DeliveryPolicy::MergeProgressUpdates];
    if config.descendant_preference {
        policies.push(DeliveryPolicy::PreferTerminalDescendant);
        policies.push(DeliveryPolicy::SuppressStaleParent);
    }
    if delegation.is_some_and(|snapshot| snapshot.merge_contract.approval_required) {
        policies.push(DeliveryPolicy::RequireFinalReview);
    }
    policies.sort_by_key(|policy| policy.as_str());
    policies.dedup();

    DeliveryPolicySet {
        policy_id: DELIVERY_ARBITRATION_POLICY_ID.to_owned(),
        surface,
        source_kind,
        source_id,
        policies,
        mode: config.mode,
        descendant_preference: config.descendant_preference,
        suppression_limit: config.suppression_limit,
        delegation_profile_id: delegation.map(|value| value.profile_id.clone()),
        delegation_template_id: delegation.and_then(|value| value.template_id.clone()),
        delegation_execution_mode: delegation.map(|value| value.execution_mode),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DeliveryDecisionAction {
    DeliverInterimParent,
    PreferTerminalDescendant,
    AnnotateSupersededParent,
    HoldForReview,
    AuditOnly,
}

impl DeliveryDecisionAction {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::DeliverInterimParent => "deliver_interim_parent",
            Self::PreferTerminalDescendant => "prefer_terminal_descendant",
            Self::AnnotateSupersededParent => "annotate_superseded_parent",
            Self::HoldForReview => "hold_for_review",
            Self::AuditOnly => "audit_only",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DeliveryDecisionInput<'a> {
    pub(crate) policy: &'a DeliveryPolicySet,
    pub(crate) parent_run_id: Option<&'a str>,
    pub(crate) parent_state: Option<&'a str>,
    pub(crate) descendant_run_id: Option<&'a str>,
    pub(crate) descendant_state: &'a str,
    pub(crate) approval_required: bool,
    pub(crate) approval_events: u64,
    pub(crate) approval_pending: bool,
    pub(crate) approval_denied: bool,
    pub(crate) observed_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DeliveryDecision {
    pub(crate) action: DeliveryDecisionAction,
    pub(crate) reason: String,
    pub(crate) parent_superseded: bool,
    pub(crate) parent_suppressed: bool,
    pub(crate) would_suppress_parent: bool,
    pub(crate) descendant_preferred: bool,
    pub(crate) review_required: bool,
    pub(crate) approval_pending: bool,
    pub(crate) audit_retained: bool,
    pub(crate) explain_json: Value,
}

impl DeliveryDecision {
    #[must_use]
    pub(crate) const fn suppression_count(&self) -> u64 {
        if self.parent_suppressed {
            1
        } else {
            0
        }
    }
}

#[allow(dead_code)]
mod phase_five_delivery_contracts {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum DeliveryAckState {
        Acked,
        Nacked,
        Unknown,
    }

    impl DeliveryAckState {
        #[must_use]
        pub(crate) const fn as_str(self) -> &'static str {
            match self {
                Self::Acked => "acked",
                Self::Nacked => "nacked",
                Self::Unknown => "unknown",
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub(crate) struct DeliveryRetryPolicy {
        pub(crate) max_attempts: u32,
        pub(crate) backoff_ms: u64,
    }

    impl Default for DeliveryRetryPolicy {
        fn default() -> Self {
            Self { max_attempts: 3, backoff_ms: 1_000 }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub(crate) struct DeliveryAttemptRecord {
        pub(crate) attempt_id: String,
        pub(crate) adapter: String,
        pub(crate) payload_digest_sha256: String,
        pub(crate) external_id: Option<String>,
        pub(crate) external_idempotency_key: Option<String>,
        pub(crate) ack_state: DeliveryAckState,
        pub(crate) retry_policy: DeliveryRetryPolicy,
        pub(crate) correlation_id: String,
        pub(crate) attempted_at_unix_ms: i64,
    }

    impl DeliveryAttemptRecord {
        #[must_use]
        pub(crate) fn snapshot_json(&self) -> Value {
            json!({
                "attempt_id": self.attempt_id,
                "adapter": self.adapter,
                "payload_digest_sha256": self.payload_digest_sha256,
                "external_id": self.external_id,
                "external_idempotency_key": self.external_idempotency_key,
                "ack_state": self.ack_state.as_str(),
                "retry_policy": self.retry_policy,
                "correlation_id": self.correlation_id,
                "attempted_at_unix_ms": self.attempted_at_unix_ms,
            })
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub(crate) struct DeliveryTrace {
        pub(crate) trace_id: String,
        pub(crate) attempts: Vec<DeliveryAttemptRecord>,
    }

    impl DeliveryTrace {
        #[must_use]
        pub(crate) fn latest_attempt(&self) -> Option<&DeliveryAttemptRecord> {
            self.attempts.iter().max_by_key(|attempt| attempt.attempted_at_unix_ms)
        }

        #[must_use]
        pub(crate) fn latest_ack_state(&self) -> DeliveryAckState {
            self.latest_attempt().map_or(DeliveryAckState::Unknown, |attempt| attempt.ack_state)
        }

        #[must_use]
        pub(crate) fn ack_uncertain(&self) -> bool {
            self.latest_ack_state() == DeliveryAckState::Unknown
        }

        #[must_use]
        pub(crate) fn snapshot_json(&self) -> Value {
            json!({
                "schema_version": RUNTIME_PREVIEW_SCHEMA_VERSION,
                "trace_id": self.trace_id,
                "latest_ack_state": self.latest_ack_state().as_str(),
                "ack_uncertain": self.ack_uncertain(),
                "attempts": self
                    .attempts
                    .iter()
                    .map(DeliveryAttemptRecord::snapshot_json)
                    .collect::<Vec<_>>(),
            })
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum DeliveryAckRecoveryAction {
        Complete,
        WaitExternalAck,
        RetryWithIdempotencyKey,
        OperatorReview,
        DeadLetter,
    }

    impl DeliveryAckRecoveryAction {
        #[must_use]
        pub(crate) const fn as_str(self) -> &'static str {
            match self {
                Self::Complete => "complete",
                Self::WaitExternalAck => "wait_external_ack",
                Self::RetryWithIdempotencyKey => "retry_with_idempotency_key",
                Self::OperatorReview => "operator_review",
                Self::DeadLetter => "dead_letter",
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub(crate) struct DeliveryAckRecoveryDecision {
        pub(crate) action: DeliveryAckRecoveryAction,
        pub(crate) reason: String,
        pub(crate) idempotency_key_required: bool,
    }

    impl DeliveryAckRecoveryDecision {
        #[must_use]
        pub(crate) fn snapshot_json(&self) -> Value {
            json!({
                "action": self.action.as_str(),
                "reason": self.reason,
                "idempotency_key_required": self.idempotency_key_required,
            })
        }
    }

    #[must_use]
    pub(crate) fn resolve_ack_recovery(trace: &DeliveryTrace) -> DeliveryAckRecoveryDecision {
        let Some(latest) = trace.latest_attempt() else {
            return DeliveryAckRecoveryDecision {
                action: DeliveryAckRecoveryAction::OperatorReview,
                reason: "delivery_trace_has_no_attempts".to_owned(),
                idempotency_key_required: true,
            };
        };
        match latest.ack_state {
            DeliveryAckState::Acked => DeliveryAckRecoveryDecision {
                action: DeliveryAckRecoveryAction::Complete,
                reason: "latest_attempt_acknowledged".to_owned(),
                idempotency_key_required: false,
            },
            DeliveryAckState::Nacked => DeliveryAckRecoveryDecision {
                action: DeliveryAckRecoveryAction::DeadLetter,
                reason: "latest_attempt_negative_ack".to_owned(),
                idempotency_key_required: false,
            },
            DeliveryAckState::Unknown if latest.external_idempotency_key.is_some() => {
                DeliveryAckRecoveryDecision {
                    action: DeliveryAckRecoveryAction::WaitExternalAck,
                    reason: "latest_attempt_uncertain_with_external_idempotency_key".to_owned(),
                    idempotency_key_required: false,
                }
            }
            DeliveryAckState::Unknown => DeliveryAckRecoveryDecision {
                action: DeliveryAckRecoveryAction::OperatorReview,
                reason: "latest_attempt_uncertain_without_external_idempotency_key".to_owned(),
                idempotency_key_required: true,
            },
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum FailureDestinationKind {
        AuditOnly,
        OperatorInbox,
        FallbackChannel,
    }

    impl FailureDestinationKind {
        #[must_use]
        pub(crate) const fn as_str(self) -> &'static str {
            match self {
                Self::AuditOnly => "audit_only",
                Self::OperatorInbox => "operator_inbox",
                Self::FallbackChannel => "fallback_channel",
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    pub(crate) struct FailureDestinationInput {
        pub(crate) surface: DeliverySurface,
        pub(crate) ack_state: DeliveryAckState,
        pub(crate) fallback_channel_configured: bool,
        pub(crate) channel_healthy: bool,
        pub(crate) sensitive: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub(crate) struct FailureDestinationDecision {
        pub(crate) destination: FailureDestinationKind,
        pub(crate) reason: String,
        pub(crate) requires_operator_action: bool,
    }

    impl FailureDestinationDecision {
        #[must_use]
        pub(crate) fn snapshot_json(&self) -> Value {
            json!({
                "destination": self.destination.as_str(),
                "reason": self.reason,
                "requires_operator_action": self.requires_operator_action,
            })
        }
    }

    #[must_use]
    pub(crate) fn resolve_failure_destination(
        input: FailureDestinationInput,
    ) -> FailureDestinationDecision {
        if input.sensitive || input.surface == DeliverySurface::AuditOnly {
            return FailureDestinationDecision {
                destination: FailureDestinationKind::AuditOnly,
                reason: "failure_contains_sensitive_output_or_audit_surface".to_owned(),
                requires_operator_action: input.ack_state != DeliveryAckState::Acked,
            };
        }
        if input.ack_state == DeliveryAckState::Unknown {
            return FailureDestinationDecision {
                destination: FailureDestinationKind::OperatorInbox,
                reason: "delivery_ack_uncertain_requires_operator_diagnostics".to_owned(),
                requires_operator_action: true,
            };
        }
        if input.fallback_channel_configured && input.channel_healthy {
            return FailureDestinationDecision {
                destination: FailureDestinationKind::FallbackChannel,
                reason: "primary_delivery_failed_and_fallback_channel_is_healthy".to_owned(),
                requires_operator_action: false,
            };
        }
        FailureDestinationDecision {
            destination: FailureDestinationKind::OperatorInbox,
            reason: "delivery_failed_without_healthy_fallback".to_owned(),
            requires_operator_action: true,
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub(crate) struct DeadLetterQueuePreview {
        pub(crate) redacted_preview: String,
        pub(crate) reason: String,
        pub(crate) attempts: usize,
        pub(crate) recommended_action: String,
        pub(crate) actor_scope: String,
        pub(crate) idempotency_key_required: bool,
    }

    impl DeadLetterQueuePreview {
        #[must_use]
        pub(crate) fn snapshot_json(&self) -> Value {
            json!(self)
        }
    }

    #[must_use]
    pub(crate) fn build_dead_letter_preview(
        payload_preview: &str,
        reason: &str,
        attempts: usize,
        actor_scope: &str,
        idempotency_key_present: bool,
    ) -> DeadLetterQueuePreview {
        let redacted = truncate_preview(&redact_url_segments_in_text(payload_preview), 240);
        let recommended_action = if idempotency_key_present {
            "retry_or_requeue_with_existing_idempotency_key"
        } else {
            "operator_review_before_retry"
        };
        DeadLetterQueuePreview {
            redacted_preview: redacted,
            reason: reason.to_owned(),
            attempts,
            recommended_action: recommended_action.to_owned(),
            actor_scope: actor_scope.to_owned(),
            idempotency_key_required: !idempotency_key_present,
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum ChildRunOutputGuardAction {
        Deliver,
        HoldForReview,
        AuditOnly,
    }

    impl ChildRunOutputGuardAction {
        #[must_use]
        pub(crate) const fn as_str(self) -> &'static str {
            match self {
                Self::Deliver => "deliver",
                Self::HoldForReview => "hold_for_review",
                Self::AuditOnly => "audit_only",
            }
        }
    }

    #[derive(Debug, Clone)]
    pub(crate) struct ChildRunOutputGuardInput<'a> {
        pub(crate) output_size_bytes: usize,
        pub(crate) max_external_bytes: usize,
        pub(crate) sensitivity: ToolResultSensitivity,
        pub(crate) target_surface: DeliverySurface,
        pub(crate) approval_required: bool,
        pub(crate) channel_healthy: bool,
        pub(crate) artifact_id: Option<&'a str>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub(crate) struct ChildRunOutputGuardDecision {
        pub(crate) action: ChildRunOutputGuardAction,
        pub(crate) reason: String,
        pub(crate) artifact_required: bool,
        pub(crate) operator_event_required: bool,
    }

    impl ChildRunOutputGuardDecision {
        #[must_use]
        pub(crate) fn snapshot_json(&self) -> Value {
            json!({
                "action": self.action.as_str(),
                "reason": self.reason,
                "artifact_required": self.artifact_required,
                "operator_event_required": self.operator_event_required,
            })
        }
    }

    #[must_use]
    pub(crate) fn guard_child_run_output(
        input: ChildRunOutputGuardInput<'_>,
    ) -> ChildRunOutputGuardDecision {
        if input.target_surface == DeliverySurface::AuditOnly {
            return ChildRunOutputGuardDecision {
                action: ChildRunOutputGuardAction::AuditOnly,
                reason: "target_surface_is_audit_only".to_owned(),
                artifact_required: input.artifact_id.is_none(),
                operator_event_required: false,
            };
        }
        if input.sensitivity.requires_full_read_gate() || input.approval_required {
            return ChildRunOutputGuardDecision {
                action: ChildRunOutputGuardAction::HoldForReview,
                reason: "sensitive_or_approval_gated_child_output".to_owned(),
                artifact_required: input.artifact_id.is_none(),
                operator_event_required: true,
            };
        }
        if input.output_size_bytes > input.max_external_bytes {
            return ChildRunOutputGuardDecision {
                action: ChildRunOutputGuardAction::HoldForReview,
                reason: "child_output_exceeds_external_delivery_budget".to_owned(),
                artifact_required: true,
                operator_event_required: true,
            };
        }
        if !input.channel_healthy {
            return ChildRunOutputGuardDecision {
                action: ChildRunOutputGuardAction::HoldForReview,
                reason: "target_channel_unhealthy".to_owned(),
                artifact_required: input.artifact_id.is_none(),
                operator_event_required: true,
            };
        }
        ChildRunOutputGuardDecision {
            action: ChildRunOutputGuardAction::Deliver,
            reason: "child_output_delivery_allowed".to_owned(),
            artifact_required: false,
            operator_event_required: false,
        }
    }
}

#[must_use]
pub(crate) fn arbitrate_delivery(input: DeliveryDecisionInput<'_>) -> DeliveryDecision {
    let policy = input.policy;
    let descendant_terminal = is_terminal_descendant_state(input.descendant_state);
    let descendant_success = is_success_descendant_state(input.descendant_state);
    let enforcement_enabled = matches!(policy.mode, RuntimePreviewMode::Enabled);
    let review_required_by_policy = policy.contains(DeliveryPolicy::RequireFinalReview)
        && (input.approval_required || input.approval_pending || input.approval_denied);
    let review_gate_active = review_required_by_policy
        && (input.approval_pending || input.approval_denied || input.approval_events == 0);

    let (action, reason, parent_superseded, would_suppress_parent, descendant_preferred) =
        if matches!(policy.mode, RuntimePreviewMode::Disabled) {
            (
                DeliveryDecisionAction::AuditOnly,
                "delivery_arbitration_disabled",
                false,
                false,
                false,
            )
        } else if review_gate_active {
            (
                DeliveryDecisionAction::HoldForReview,
                if input.approval_denied { "final_review_denied" } else { "final_review_required" },
                false,
                false,
                false,
            )
        } else if descendant_terminal
            && descendant_success
            && policy.contains(DeliveryPolicy::PreferTerminalDescendant)
        {
            let would_suppress = policy.contains(DeliveryPolicy::SuppressStaleParent)
                && policy.suppression_limit > 0;
            if policy.surface.supports_replacement() && would_suppress {
                (
                    DeliveryDecisionAction::PreferTerminalDescendant,
                    "terminal_descendant_preferred",
                    true,
                    true,
                    true,
                )
            } else {
                (
                    DeliveryDecisionAction::AnnotateSupersededParent,
                    "terminal_descendant_annotates_parent",
                    true,
                    false,
                    true,
                )
            }
        } else if descendant_terminal {
            (
                DeliveryDecisionAction::AnnotateSupersededParent,
                "terminal_descendant_status_retained",
                false,
                false,
                false,
            )
        } else {
            (
                DeliveryDecisionAction::DeliverInterimParent,
                "interim_parent_delivery_allowed",
                false,
                false,
                false,
            )
        };

    let parent_suppressed = enforcement_enabled && would_suppress_parent;
    let explain_json = json!({
        "schema_version": RUNTIME_PREVIEW_SCHEMA_VERSION,
        "policy": policy.snapshot_json(),
        "decision": action.as_str(),
        "reason": reason,
        "observed_at_unix_ms": input.observed_at_unix_ms,
        "parent_output": {
            "run_id": input.parent_run_id,
            "state": input.parent_state,
            "superseded": parent_superseded,
            "suppressed": parent_suppressed,
            "would_suppress": would_suppress_parent,
            "audit_retained": true,
        },
        "descendant_output": {
            "run_id": input.descendant_run_id,
            "state": input.descendant_state,
            "terminal": descendant_terminal,
            "preferred": descendant_preferred,
            "review_required": review_required_by_policy,
            "approval_pending": input.approval_pending,
            "approval_denied": input.approval_denied,
            "approval_events": input.approval_events,
        },
        "channel_fallback": {
            "surface": policy.surface.as_str(),
            "supports_replacement": policy.surface.supports_replacement(),
            "supports_annotation": policy.surface.supports_annotation(),
            "presentation": policy.surface.progress_presentation(),
        },
    });

    DeliveryDecision {
        action,
        reason: reason.to_owned(),
        parent_superseded,
        parent_suppressed,
        would_suppress_parent,
        descendant_preferred,
        review_required: review_required_by_policy,
        approval_pending: input.approval_pending,
        audit_retained: true,
        explain_json,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DeliveryProgressSourceKind {
    ChildRun,
    FlowStep,
    ApprovalWait,
}

const DELIVERY_PROGRESS_SOURCE_KINDS: [DeliveryProgressSourceKind; 3] = [
    DeliveryProgressSourceKind::ChildRun,
    DeliveryProgressSourceKind::FlowStep,
    DeliveryProgressSourceKind::ApprovalWait,
];

impl DeliveryProgressSourceKind {
    #[must_use]
    const fn as_str(self) -> &'static str {
        match self {
            Self::ChildRun => "child_run",
            Self::FlowStep => "flow_step",
            Self::ApprovalWait => "approval_wait",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliveryProgressUpdate {
    pub(crate) source_kind: DeliveryProgressSourceKind,
    pub(crate) source_id: String,
    pub(crate) label: String,
    pub(crate) state: String,
    pub(crate) detail: Option<String>,
    pub(crate) user_visible: bool,
    pub(crate) terminal: bool,
    pub(crate) observed_at_unix_ms: i64,
}

impl DeliveryProgressUpdate {
    #[must_use]
    pub(crate) fn child_run(
        source_id: impl Into<String>,
        state: impl Into<String>,
        detail: Option<String>,
        user_visible: bool,
        terminal: bool,
        observed_at_unix_ms: i64,
    ) -> Self {
        let state = state.into();
        Self {
            source_kind: DeliveryProgressSourceKind::ChildRun,
            source_id: source_id.into(),
            label: "Child run".to_owned(),
            state,
            detail,
            user_visible,
            terminal,
            observed_at_unix_ms,
        }
    }

    #[must_use]
    pub(crate) fn flow_step(
        source_id: impl Into<String>,
        label: impl Into<String>,
        state: impl Into<String>,
        detail: Option<String>,
        user_visible: bool,
        terminal: bool,
        observed_at_unix_ms: i64,
    ) -> Self {
        Self {
            source_kind: DeliveryProgressSourceKind::FlowStep,
            source_id: source_id.into(),
            label: label.into(),
            state: state.into(),
            detail,
            user_visible,
            terminal,
            observed_at_unix_ms,
        }
    }

    #[must_use]
    pub(crate) fn approval_wait(
        source_id: impl Into<String>,
        detail: Option<String>,
        observed_at_unix_ms: i64,
    ) -> Self {
        Self {
            source_kind: DeliveryProgressSourceKind::ApprovalWait,
            source_id: source_id.into(),
            label: "Approval".to_owned(),
            state: "waiting_for_approval".to_owned(),
            detail,
            user_visible: true,
            terminal: false,
            observed_at_unix_ms,
        }
    }

    #[must_use]
    fn summary_json(&self) -> Value {
        json!({
            "source_kind": self.source_kind.as_str(),
            "source_id": self.source_id,
            "label": self.label,
            "state": self.state,
            "detail": self.detail,
            "user_visible": self.user_visible,
            "terminal": self.terminal,
            "observed_at_unix_ms": self.observed_at_unix_ms,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MergedDeliveryProgress {
    pub(crate) surface: DeliverySurface,
    pub(crate) presentation: &'static str,
    pub(crate) refresh_cadence_ms: u64,
    pub(crate) max_items: usize,
    pub(crate) title: String,
    pub(crate) text: String,
    pub(crate) terminal_state: Option<String>,
    pub(crate) approval_wait_count: usize,
    pub(crate) hidden_count: usize,
    pub(crate) items: Vec<DeliveryProgressUpdate>,
}

impl MergedDeliveryProgress {
    #[must_use]
    pub(crate) fn snapshot_json(&self) -> Value {
        json!({
            "schema_version": RUNTIME_PREVIEW_SCHEMA_VERSION,
            "surface": self.surface.as_str(),
            "presentation": self.presentation,
            "refresh_cadence_ms": self.refresh_cadence_ms,
            "max_items": self.max_items,
            "title": self.title,
            "text": self.text,
            "terminal_state": self.terminal_state,
            "approval_wait_count": self.approval_wait_count,
            "hidden_count": self.hidden_count,
            "supported_source_kinds": DELIVERY_PROGRESS_SOURCE_KINDS
                .iter()
                .map(|source| source.as_str())
                .collect::<Vec<_>>(),
            "items": self.items.iter().map(DeliveryProgressUpdate::summary_json).collect::<Vec<_>>(),
        })
    }
}

#[must_use]
pub(crate) fn merge_delivery_progress_updates(
    updates: &[DeliveryProgressUpdate],
    surface: DeliverySurface,
    observed_at_unix_ms: i64,
) -> MergedDeliveryProgress {
    let mut latest_by_source: HashMap<
        (DeliveryProgressSourceKind, String),
        DeliveryProgressUpdate,
    > = HashMap::new();
    for update in updates {
        let key = (update.source_kind, update.source_id.clone());
        let replace = latest_by_source
            .get(&key)
            .is_none_or(|current| update.observed_at_unix_ms >= current.observed_at_unix_ms);
        if replace {
            latest_by_source.insert(key, update.clone());
        }
    }

    let mut merged = latest_by_source.into_values().collect::<Vec<_>>();
    let terminal_state = merged
        .iter()
        .filter(|update| update.terminal)
        .max_by_key(|update| update.observed_at_unix_ms)
        .map(|update| update.state.clone());
    let approval_wait_count = merged
        .iter()
        .filter(|update| update.source_kind == DeliveryProgressSourceKind::ApprovalWait)
        .count();

    merged.sort_by(|left, right| {
        progress_priority(right)
            .cmp(&progress_priority(left))
            .then_with(|| right.observed_at_unix_ms.cmp(&left.observed_at_unix_ms))
    });
    let total_count = merged.len();
    let max_items = surface.max_progress_items();
    let items = merged.into_iter().take(max_items).collect::<Vec<_>>();
    let hidden_count = total_count.saturating_sub(items.len());

    let title = progress_title(terminal_state.as_deref(), approval_wait_count);
    let latest = items.first();
    let text = match latest {
        Some(update) => format!(
            "{} update{} merged; latest {} is {}.",
            items.len(),
            if items.len() == 1 { "" } else { "s" },
            update.source_kind.as_str(),
            update.state
        ),
        None => format!(
            "No delivery progress events observed at {observed_at_unix_ms}; cadence is {} ms.",
            surface.refresh_cadence_ms()
        ),
    };

    MergedDeliveryProgress {
        surface,
        presentation: surface.progress_presentation(),
        refresh_cadence_ms: surface.refresh_cadence_ms(),
        max_items,
        title,
        text,
        terminal_state,
        approval_wait_count,
        hidden_count,
        items,
    }
}

#[must_use]
pub(crate) fn delivery_review_summary(summary: &DelegationMergeApprovalSummary) -> Value {
    json!({
        "approval_required": summary.approval_required,
        "approval_events": summary.approval_events,
        "approval_pending": summary.approval_pending,
        "approval_denied": summary.approval_denied,
    })
}

fn progress_priority(update: &DeliveryProgressUpdate) -> (u8, bool) {
    let severity = if update.terminal {
        4
    } else if update.source_kind == DeliveryProgressSourceKind::ApprovalWait {
        3
    } else if update.state == "failed" || update.state == "transport_error" {
        4
    } else if update.user_visible {
        2
    } else {
        1
    };
    (severity, update.user_visible)
}

fn progress_title(terminal_state: Option<&str>, approval_wait_count: usize) -> String {
    match terminal_state {
        Some("failed" | "transport_error") => "Delegated work failed".to_owned(),
        Some("cancelled" | "canceled") => "Delegated work cancelled".to_owned(),
        Some(_) => "Delegated work completed".to_owned(),
        None if approval_wait_count > 0 => "Delegated work waiting for approval".to_owned(),
        None => "Delegated work in progress".to_owned(),
    }
}

fn truncate_preview(value: &str, limit: usize) -> String {
    let mut output = String::with_capacity(limit.min(value.len()));
    for character in value.chars().take(limit) {
        output.push(character);
    }
    if value.chars().count() > limit {
        output.push_str("...");
    }
    output
}

fn is_terminal_descendant_state(state: &str) -> bool {
    matches!(state, "done" | "succeeded" | "completed" | "failed" | "cancelled" | "canceled")
}

fn is_success_descendant_state(state: &str) -> bool {
    matches!(state, "done" | "succeeded" | "completed")
}

#[cfg(test)]
mod tests {
    use super::phase_five_delivery_contracts::*;
    use super::*;
    use crate::delegation::{
        DelegationMemoryScopeKind, DelegationMergeContract, DelegationMergeStrategy,
        DelegationRole, DelegationRuntimeLimits,
    };

    fn enabled_config() -> DeliveryArbitrationConfig {
        DeliveryArbitrationConfig {
            mode: RuntimePreviewMode::Enabled,
            descendant_preference: true,
            suppression_limit: 2,
        }
    }

    fn delegation(approval_required: bool) -> DelegationSnapshot {
        DelegationSnapshot {
            profile_id: "review".to_owned(),
            display_name: "Review".to_owned(),
            description: None,
            template_id: Some("review_and_patch".to_owned()),
            role: DelegationRole::Review,
            execution_mode: DelegationExecutionMode::Serial,
            group_id: "group-1".to_owned(),
            model_profile: "default".to_owned(),
            tool_allowlist: Vec::new(),
            skill_allowlist: Vec::new(),
            memory_scope: DelegationMemoryScopeKind::ParentSession,
            budget_tokens: 1_000,
            max_attempts: 1,
            merge_contract: DelegationMergeContract {
                strategy: DelegationMergeStrategy::PatchReview,
                approval_required,
            },
            runtime_limits: DelegationRuntimeLimits::default(),
            agent_id: None,
        }
    }

    #[test]
    fn delegation_policy_maps_descendant_and_review_rules() {
        let snapshot = delegation(true);
        let policy = resolve_delivery_policy(&enabled_config(), Some(&snapshot), None, Some("web"));

        assert_eq!(policy.surface, DeliverySurface::WebChat);
        assert!(policy.contains(DeliveryPolicy::DeliverInterimParent));
        assert!(policy.contains(DeliveryPolicy::MergeProgressUpdates));
        assert!(policy.contains(DeliveryPolicy::PreferTerminalDescendant));
        assert!(policy.contains(DeliveryPolicy::SuppressStaleParent));
        assert!(policy.contains(DeliveryPolicy::RequireFinalReview));
        assert_eq!(policy.delegation_profile_id.as_deref(), Some("review"));
    }

    #[test]
    fn terminal_descendant_suppresses_web_parent_when_enabled() {
        let snapshot = delegation(false);
        let policy = resolve_delivery_policy(&enabled_config(), Some(&snapshot), None, Some("web"));
        let decision = arbitrate_delivery(DeliveryDecisionInput {
            policy: &policy,
            parent_run_id: Some("parent"),
            parent_state: Some("done"),
            descendant_run_id: Some("child"),
            descendant_state: "done",
            approval_required: false,
            approval_events: 0,
            approval_pending: false,
            approval_denied: false,
            observed_at_unix_ms: 10,
        });

        assert_eq!(decision.action, DeliveryDecisionAction::PreferTerminalDescendant);
        assert!(decision.descendant_preferred);
        assert!(decision.parent_superseded);
        assert!(decision.parent_suppressed);
        assert!(decision.audit_retained);
        assert_eq!(decision.suppression_count(), 1);
        assert_eq!(decision.explain_json["parent_output"]["audit_retained"], true);
    }

    #[test]
    fn approval_required_holds_terminal_descendant_until_review_resolves() {
        let snapshot = delegation(true);
        let policy = resolve_delivery_policy(&enabled_config(), Some(&snapshot), None, Some("web"));
        let decision = arbitrate_delivery(DeliveryDecisionInput {
            policy: &policy,
            parent_run_id: Some("parent"),
            parent_state: Some("done"),
            descendant_run_id: Some("child"),
            descendant_state: "done",
            approval_required: true,
            approval_events: 0,
            approval_pending: true,
            approval_denied: false,
            observed_at_unix_ms: 10,
        });

        assert_eq!(decision.action, DeliveryDecisionAction::HoldForReview);
        assert!(!decision.descendant_preferred);
        assert!(!decision.parent_suppressed);
        assert!(decision.review_required);
    }

    #[test]
    fn external_channel_annotates_when_replacement_is_unavailable() {
        let snapshot = delegation(false);
        let policy =
            resolve_delivery_policy(&enabled_config(), Some(&snapshot), None, Some("discord"));
        let decision = arbitrate_delivery(DeliveryDecisionInput {
            policy: &policy,
            parent_run_id: Some("parent"),
            parent_state: Some("done"),
            descendant_run_id: Some("child"),
            descendant_state: "done",
            approval_required: false,
            approval_events: 0,
            approval_pending: false,
            approval_denied: false,
            observed_at_unix_ms: 10,
        });

        assert_eq!(policy.surface, DeliverySurface::ExternalChannel);
        assert_eq!(decision.action, DeliveryDecisionAction::AnnotateSupersededParent);
        assert!(decision.parent_superseded);
        assert!(!decision.parent_suppressed);
        assert_eq!(decision.explain_json["channel_fallback"]["presentation"], "periodic_summary");
    }

    #[test]
    fn progress_merge_combines_child_flow_and_approval_waits() {
        let updates = vec![
            DeliveryProgressUpdate::child_run("child-1", "running", None, true, false, 10),
            DeliveryProgressUpdate::flow_step(
                "flow-1/step-1",
                "Fetch references",
                "running",
                Some("2 sources".to_owned()),
                true,
                false,
                20,
            ),
            DeliveryProgressUpdate::approval_wait("approval-1", Some("Tool call".to_owned()), 30),
        ];

        let merged = merge_delivery_progress_updates(&updates, DeliverySurface::WebChat, 40);

        assert_eq!(merged.presentation, "inline_timeline");
        assert_eq!(merged.approval_wait_count, 1);
        assert_eq!(merged.items.len(), 3);
        assert_eq!(merged.title, "Delegated work waiting for approval");
    }

    #[test]
    fn external_progress_merge_is_bounded_and_keeps_terminal_state() {
        let mut updates = (0..8)
            .map(|index| {
                DeliveryProgressUpdate::child_run(
                    format!("child-{index}"),
                    "running",
                    None,
                    index % 2 == 0,
                    false,
                    index,
                )
            })
            .collect::<Vec<_>>();
        updates.push(DeliveryProgressUpdate::child_run(
            "child-terminal",
            "completed",
            None,
            true,
            true,
            100,
        ));

        let merged =
            merge_delivery_progress_updates(&updates, DeliverySurface::ExternalChannel, 120);

        assert_eq!(merged.presentation, "periodic_summary");
        assert_eq!(merged.refresh_cadence_ms, 30_000);
        assert_eq!(merged.items.len(), 4);
        assert_eq!(merged.hidden_count, 5);
        assert_eq!(merged.terminal_state.as_deref(), Some("completed"));
        assert_eq!(merged.items[0].source_id, "child-terminal");
    }

    #[test]
    fn delivery_trace_preserves_ack_uncertainty_and_recovery_action() {
        let trace = DeliveryTrace {
            trace_id: "trace-1".to_owned(),
            attempts: vec![DeliveryAttemptRecord {
                attempt_id: "attempt-1".to_owned(),
                adapter: "discord".to_owned(),
                payload_digest_sha256: "sha256:abc".to_owned(),
                external_id: None,
                external_idempotency_key: Some("deliver:trace-1".to_owned()),
                ack_state: DeliveryAckState::Unknown,
                retry_policy: DeliveryRetryPolicy::default(),
                correlation_id: "corr-1".to_owned(),
                attempted_at_unix_ms: 10,
            }],
        };

        let recovery = resolve_ack_recovery(&trace);

        assert!(trace.ack_uncertain());
        assert_eq!(trace.latest_ack_state(), DeliveryAckState::Unknown);
        assert_eq!(recovery.action, DeliveryAckRecoveryAction::WaitExternalAck);
        assert!(!recovery.idempotency_key_required);
        assert_eq!(trace.snapshot_json()["latest_ack_state"], "unknown");
    }

    #[test]
    fn delivery_trace_without_idempotency_key_requires_operator_review() {
        let trace = DeliveryTrace {
            trace_id: "trace-2".to_owned(),
            attempts: vec![DeliveryAttemptRecord {
                attempt_id: "attempt-1".to_owned(),
                adapter: "slack".to_owned(),
                payload_digest_sha256: "sha256:def".to_owned(),
                external_id: None,
                external_idempotency_key: None,
                ack_state: DeliveryAckState::Unknown,
                retry_policy: DeliveryRetryPolicy::default(),
                correlation_id: "corr-2".to_owned(),
                attempted_at_unix_ms: 10,
            }],
        };

        let recovery = resolve_ack_recovery(&trace);

        assert_eq!(recovery.action, DeliveryAckRecoveryAction::OperatorReview);
        assert!(recovery.idempotency_key_required);
    }

    #[test]
    fn failure_destination_and_dead_letter_preview_are_redacted() {
        let destination = resolve_failure_destination(FailureDestinationInput {
            surface: DeliverySurface::ExternalChannel,
            ack_state: DeliveryAckState::Nacked,
            fallback_channel_configured: true,
            channel_healthy: true,
            sensitive: false,
        });
        let preview = build_dead_letter_preview(
            "failed POST https://example.test/send?token=secret-token&mode=full",
            "adapter_nack",
            3,
            "operator:alice",
            false,
        );

        assert_eq!(destination.destination, FailureDestinationKind::FallbackChannel);
        assert!(!destination.requires_operator_action);
        assert!(preview.redacted_preview.contains("token=<redacted>"));
        assert!(!preview.redacted_preview.contains("secret-token"));
        assert_eq!(preview.recommended_action, "operator_review_before_retry");
        assert!(preview.idempotency_key_required);
    }

    #[test]
    fn child_run_output_guard_blocks_sensitive_external_delivery() {
        let sensitive = guard_child_run_output(ChildRunOutputGuardInput {
            output_size_bytes: 128,
            max_external_bytes: 1_024,
            sensitivity: ToolResultSensitivity::Secret,
            target_surface: DeliverySurface::ExternalChannel,
            approval_required: false,
            channel_healthy: true,
            artifact_id: None,
        });
        let public = guard_child_run_output(ChildRunOutputGuardInput {
            output_size_bytes: 128,
            max_external_bytes: 1_024,
            sensitivity: ToolResultSensitivity::Public,
            target_surface: DeliverySurface::ExternalChannel,
            approval_required: false,
            channel_healthy: true,
            artifact_id: Some("artifact-1"),
        });

        assert_eq!(sensitive.action, ChildRunOutputGuardAction::HoldForReview);
        assert!(sensitive.artifact_required);
        assert!(sensitive.operator_event_required);
        assert_eq!(public.action, ChildRunOutputGuardAction::Deliver);
        assert!(!public.operator_event_required);
    }
}
