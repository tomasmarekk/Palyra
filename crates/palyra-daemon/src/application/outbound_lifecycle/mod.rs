use palyra_common::redaction::redact_auth_error;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::application::delivery_arbitration::DeliverySurface;

const OUTBOUND_LIFECYCLE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelOutboundCapabilities {
    pub(crate) typing: bool,
    pub(crate) reactions: bool,
    pub(crate) message_edit: bool,
    pub(crate) threads: bool,
    pub(crate) reply_to: bool,
    pub(crate) markdown: bool,
    pub(crate) max_message_bytes: usize,
}

impl ChannelOutboundCapabilities {
    #[must_use]
    pub(crate) fn for_channel(channel: &str, max_message_bytes: u64) -> Self {
        let surface = DeliverySurface::from_channel(Some(channel));
        let max_message_bytes =
            usize::try_from(max_message_bytes).ok().filter(|value| *value > 0).unwrap_or(2_000);
        match surface {
            DeliverySurface::WebChat => Self {
                typing: true,
                reactions: true,
                message_edit: true,
                threads: true,
                reply_to: true,
                markdown: true,
                max_message_bytes,
            },
            DeliverySurface::ExternalChannel => Self {
                typing: true,
                reactions: true,
                message_edit: false,
                threads: true,
                reply_to: true,
                markdown: true,
                max_message_bytes,
            },
            DeliverySurface::Notification => Self {
                typing: false,
                reactions: false,
                message_edit: false,
                threads: false,
                reply_to: true,
                markdown: false,
                max_message_bytes,
            },
            DeliverySurface::AuditOnly => Self {
                typing: false,
                reactions: false,
                message_edit: false,
                threads: false,
                reply_to: false,
                markdown: false,
                max_message_bytes: 0,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OutboundLifecyclePhase {
    Accepted,
    Acked,
    Queued,
    TypingStarted,
    Streaming,
    ToolRunning,
    WaitingApproval,
    Finalizing,
    Delivered,
    Failed,
    CleanedUp,
}

impl OutboundLifecyclePhase {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Acked => "acked",
            Self::Queued => "queued",
            Self::TypingStarted => "typing_started",
            Self::Streaming => "streaming",
            Self::ToolRunning => "tool_running",
            Self::WaitingApproval => "waiting_approval",
            Self::Finalizing => "finalizing",
            Self::Delivered => "delivered",
            Self::Failed => "failed",
            Self::CleanedUp => "cleaned_up",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OutboundDeliveryMode {
    DraftEdit,
    FinalMessage,
    AuditOnly,
}

impl OutboundDeliveryMode {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::DraftEdit => "draft_edit",
            Self::FinalMessage => "final_message",
            Self::AuditOnly => "audit_only",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OutboundLifecycleEvent {
    pub(crate) phase: OutboundLifecyclePhase,
    pub(crate) reason: String,
    pub(crate) observed_at_unix_ms: i64,
    pub(crate) cleanup_required: bool,
    pub(crate) cleanup_completed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutboundLifecycleStart {
    pub(crate) lifecycle_id: String,
    pub(crate) channel: String,
    pub(crate) run_id: String,
    pub(crate) binding_id: Option<String>,
    pub(crate) capabilities: ChannelOutboundCapabilities,
    pub(crate) draft_requested: bool,
    pub(crate) typing_requested: bool,
    pub(crate) reaction_requested: bool,
    pub(crate) observed_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OutboundLifecycle {
    pub(crate) schema_version: u32,
    pub(crate) lifecycle_id: String,
    pub(crate) channel: String,
    pub(crate) run_id: String,
    pub(crate) binding_id: Option<String>,
    pub(crate) capabilities: ChannelOutboundCapabilities,
    pub(crate) delivery_mode: OutboundDeliveryMode,
    pub(crate) cleanup_required: bool,
    pub(crate) cleanup_completed: bool,
    pub(crate) failure_reason: Option<String>,
    pub(crate) phases: Vec<OutboundLifecycleEvent>,
}

impl OutboundLifecycle {
    #[must_use]
    pub(crate) fn start(input: OutboundLifecycleStart) -> Self {
        let delivery_mode = select_delivery_mode(&input.capabilities, input.draft_requested);
        let mut lifecycle = Self {
            schema_version: OUTBOUND_LIFECYCLE_SCHEMA_VERSION,
            lifecycle_id: input.lifecycle_id,
            channel: input.channel,
            run_id: input.run_id,
            binding_id: input.binding_id,
            capabilities: input.capabilities,
            delivery_mode,
            cleanup_required: false,
            cleanup_completed: false,
            failure_reason: None,
            phases: Vec::new(),
        };
        lifecycle.record(
            OutboundLifecyclePhase::Accepted,
            "route_message_accepted",
            input.observed_at_unix_ms,
        );
        lifecycle.record(
            OutboundLifecyclePhase::Acked,
            "route_message_acknowledged",
            input.observed_at_unix_ms,
        );
        lifecycle.record(
            OutboundLifecyclePhase::Queued,
            "provider_turn_queued",
            input.observed_at_unix_ms,
        );
        if input.typing_requested && lifecycle.capabilities.typing {
            lifecycle.cleanup_required = true;
            lifecycle.record(
                OutboundLifecyclePhase::TypingStarted,
                "typing_indicator_started",
                input.observed_at_unix_ms,
            );
        }
        if input.reaction_requested && lifecycle.capabilities.reactions {
            lifecycle.cleanup_required = true;
        }
        lifecycle
    }

    #[allow(dead_code)]
    pub(crate) fn record_streaming(&mut self, observed_at_unix_ms: i64) {
        self.record(OutboundLifecyclePhase::Streaming, "provider_streaming", observed_at_unix_ms);
    }

    #[allow(dead_code)]
    pub(crate) fn record_tool_running(&mut self, observed_at_unix_ms: i64) {
        self.record(OutboundLifecyclePhase::ToolRunning, "tool_call_running", observed_at_unix_ms);
    }

    #[allow(dead_code)]
    pub(crate) fn record_waiting_approval(&mut self, observed_at_unix_ms: i64) {
        self.record(
            OutboundLifecyclePhase::WaitingApproval,
            "approval_waiting",
            observed_at_unix_ms,
        );
    }

    pub(crate) fn finalize_success(&mut self, observed_at_unix_ms: i64) {
        self.record(
            OutboundLifecyclePhase::Finalizing,
            "final_response_ready",
            observed_at_unix_ms,
        );
        self.record(
            OutboundLifecyclePhase::Delivered,
            "outbound_response_delivered",
            observed_at_unix_ms,
        );
        self.cleanup("success", observed_at_unix_ms);
    }

    pub(crate) fn finalize_failure(&mut self, reason: &str, observed_at_unix_ms: i64) {
        let safe_reason = redact_auth_error(reason);
        self.failure_reason = Some(safe_reason.clone());
        self.record(OutboundLifecyclePhase::Failed, safe_reason.as_str(), observed_at_unix_ms);
        self.cleanup("failure", observed_at_unix_ms);
    }

    #[allow(dead_code)]
    pub(crate) fn finalize_cancelled(&mut self, observed_at_unix_ms: i64) {
        self.failure_reason = Some("cancelled".to_owned());
        self.record(OutboundLifecyclePhase::Failed, "cancelled", observed_at_unix_ms);
        self.cleanup("cancellation", observed_at_unix_ms);
    }

    #[must_use]
    pub(crate) fn safe_snapshot_json(&self) -> Value {
        json!({
            "schema_version": self.schema_version,
            "lifecycle_id": self.lifecycle_id,
            "channel": self.channel,
            "run_id": self.run_id,
            "binding_id": self.binding_id,
            "capabilities": self.capabilities,
            "delivery_mode": self.delivery_mode.as_str(),
            "cleanup_required": self.cleanup_required,
            "cleanup_completed": self.cleanup_completed,
            "failure_reason": self.failure_reason,
            "phases": self.phases.iter().map(|event| {
                json!({
                    "phase": event.phase.as_str(),
                    "reason": event.reason,
                    "observed_at_unix_ms": event.observed_at_unix_ms,
                    "cleanup_required": event.cleanup_required,
                    "cleanup_completed": event.cleanup_completed,
                })
            }).collect::<Vec<_>>(),
        })
    }

    fn cleanup(&mut self, reason: &str, observed_at_unix_ms: i64) {
        if self.cleanup_completed {
            return;
        }
        self.cleanup_completed = true;
        self.record(OutboundLifecyclePhase::CleanedUp, reason, observed_at_unix_ms);
    }

    fn record(&mut self, phase: OutboundLifecyclePhase, reason: &str, observed_at_unix_ms: i64) {
        self.phases.push(OutboundLifecycleEvent {
            phase,
            reason: reason.to_owned(),
            observed_at_unix_ms,
            cleanup_required: self.cleanup_required,
            cleanup_completed: self.cleanup_completed,
        });
    }
}

#[must_use]
fn select_delivery_mode(
    capabilities: &ChannelOutboundCapabilities,
    draft_requested: bool,
) -> OutboundDeliveryMode {
    if capabilities.max_message_bytes == 0 {
        return OutboundDeliveryMode::AuditOnly;
    }
    if draft_requested && capabilities.message_edit {
        return OutboundDeliveryMode::DraftEdit;
    }
    OutboundDeliveryMode::FinalMessage
}

#[cfg(test)]
mod tests {
    use super::*;

    fn external_capabilities() -> ChannelOutboundCapabilities {
        ChannelOutboundCapabilities {
            typing: true,
            reactions: true,
            message_edit: false,
            threads: true,
            reply_to: true,
            markdown: true,
            max_message_bytes: 4_096,
        }
    }

    fn lifecycle(capabilities: ChannelOutboundCapabilities) -> OutboundLifecycle {
        OutboundLifecycle::start(OutboundLifecycleStart {
            lifecycle_id: "out-1".to_owned(),
            channel: "discord:ops".to_owned(),
            run_id: "run-1".to_owned(),
            binding_id: Some("cb_1".to_owned()),
            capabilities,
            draft_requested: true,
            typing_requested: true,
            reaction_requested: true,
            observed_at_unix_ms: 10,
        })
    }

    #[test]
    fn connector_without_edit_capability_falls_back_to_final_message() {
        let mut lifecycle = lifecycle(external_capabilities());
        lifecycle.finalize_success(20);

        assert_eq!(lifecycle.delivery_mode, OutboundDeliveryMode::FinalMessage);
        assert!(lifecycle.cleanup_completed);
        assert!(lifecycle
            .phases
            .iter()
            .any(|event| event.phase == OutboundLifecyclePhase::CleanedUp));
    }

    #[test]
    fn edit_capability_uses_draft_edit_flow_when_requested() {
        let mut capabilities = external_capabilities();
        capabilities.message_edit = true;
        let lifecycle = lifecycle(capabilities);

        assert_eq!(lifecycle.delivery_mode, OutboundDeliveryMode::DraftEdit);
        assert!(lifecycle.cleanup_required);
        assert!(lifecycle
            .phases
            .iter()
            .any(|event| event.phase == OutboundLifecyclePhase::TypingStarted));
    }

    #[test]
    fn failure_path_cleans_up_typing_and_reactions() {
        let mut lifecycle = lifecycle(external_capabilities());
        lifecycle.finalize_failure("provider failed authorization=Bearer SECRET", 30);

        assert!(lifecycle.cleanup_completed);
        assert_eq!(
            lifecycle.failure_reason.as_deref(),
            Some("provider failed authorization=<redacted> <redacted>")
        );
        assert_eq!(
            lifecycle.phases.last().map(|event| event.phase),
            Some(OutboundLifecyclePhase::CleanedUp)
        );
        let snapshot = lifecycle.safe_snapshot_json().to_string();
        assert!(!snapshot.contains("SECRET"));
    }

    #[test]
    fn audit_only_capabilities_disable_delivery_flow() {
        let lifecycle = lifecycle(ChannelOutboundCapabilities {
            max_message_bytes: 0,
            ..external_capabilities()
        });

        assert_eq!(lifecycle.delivery_mode, OutboundDeliveryMode::AuditOnly);
    }
}
