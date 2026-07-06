//! ACP replay and presentation translator contracts.

#![allow(dead_code)]

use std::collections::BTreeSet;

use palyra_common::runtime_contracts::AcpEventLedgerKind;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub(crate) const ACP_REPLAY_TRANSLATOR_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpRuntimeEvent {
    pub cursor: u64,
    pub runtime_event_id: String,
    pub event_type: String,
    pub redacted_summary: String,
    pub payload_sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcpReplaySafety {
    Replayable,
    NonReplayable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcpPresentationPolicy {
    UserVisible,
    OperatorVisible,
    InternalOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcpStopReason {
    Completed,
    Cancelled,
    ApprovalDenied,
    PolicyBlocked,
    Timeout,
    NativeCrashed,
    MalformedStream,
}

impl AcpStopReason {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::ApprovalDenied => "approval_denied",
            Self::PolicyBlocked => "policy_blocked",
            Self::Timeout => "timeout",
            Self::NativeCrashed => "native_crashed",
            Self::MalformedStream => "malformed_stream",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpReplayLedgerRecord {
    pub schema_version: u32,
    pub cursor: u64,
    pub runtime_event_id: String,
    pub ledger_kind: AcpEventLedgerKind,
    pub translated_event_hash: String,
    pub replay_safety: AcpReplaySafety,
    pub presentation_policy: AcpPresentationPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<AcpStopReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpTranslatedReplayEvent {
    pub record: AcpReplayLedgerRecord,
    pub duplicate: bool,
}

#[derive(Debug, Default)]
pub(crate) struct AcpReplayTranslator {
    delivered_hashes: BTreeSet<String>,
}

impl AcpReplayTranslator {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn translate(
        &mut self,
        event: AcpRuntimeEvent,
    ) -> Result<AcpTranslatedReplayEvent, AcpStopReason> {
        let ledger_kind = ledger_kind_for_event(event.event_type.as_str())?;
        let stop_reason = event.stop_reason.as_deref().map(map_stop_reason).transpose()?;
        let presentation_policy = presentation_policy_for_event(event.event_type.as_str());
        let replay_safety = replay_safety_for_event(event.event_type.as_str(), stop_reason);
        let translated_event_hash = crate::sha256_hex(
            format!(
                "{}|{}|{}|{}",
                event.cursor, event.runtime_event_id, event.event_type, event.payload_sha256
            )
            .as_bytes(),
        );
        let duplicate = !self.delivered_hashes.insert(translated_event_hash.clone());
        let record = AcpReplayLedgerRecord {
            schema_version: ACP_REPLAY_TRANSLATOR_SCHEMA_VERSION,
            cursor: event.cursor,
            runtime_event_id: event.runtime_event_id,
            ledger_kind,
            translated_event_hash,
            replay_safety,
            presentation_policy,
            stop_reason,
        };
        Ok(AcpTranslatedReplayEvent { record, duplicate })
    }
}

#[must_use]
pub(crate) fn support_bundle_acp_window(records: &[AcpReplayLedgerRecord]) -> Value {
    Value::Array(
        records
            .iter()
            .map(|record| {
                json!({
                    "schema_version": record.schema_version,
                    "cursor": record.cursor,
                    "runtime_event_hash": crate::sha256_hex(record.runtime_event_id.as_bytes()),
                    "translated_event_hash": record.translated_event_hash,
                    "ledger_kind": record.ledger_kind.as_str(),
                    "replay_safety": record.replay_safety,
                    "presentation_policy": record.presentation_policy,
                    "stop_reason": record.stop_reason.map(AcpStopReason::as_str),
                })
            })
            .collect(),
    )
}

fn ledger_kind_for_event(event_type: &str) -> Result<AcpEventLedgerKind, AcpStopReason> {
    match event_type {
        "session.update" | "message.delta" => Ok(AcpEventLedgerKind::SessionUpdate),
        "tool.call" | "tool.result" => Ok(AcpEventLedgerKind::ToolCallUpdate),
        "approval.request" => Ok(AcpEventLedgerKind::ApprovalPrompt),
        "approval.decision" => Ok(AcpEventLedgerKind::ApprovalDecision),
        "run.cancelled" => Ok(AcpEventLedgerKind::Cancel),
        "run.completed" | "run.failed" | "run.timeout" => Ok(AcpEventLedgerKind::Terminal),
        _ => Err(AcpStopReason::MalformedStream),
    }
}

fn map_stop_reason(value: &str) -> Result<AcpStopReason, AcpStopReason> {
    match value {
        "completed" => Ok(AcpStopReason::Completed),
        "cancelled" => Ok(AcpStopReason::Cancelled),
        "approval_denied" => Ok(AcpStopReason::ApprovalDenied),
        "policy_blocked" => Ok(AcpStopReason::PolicyBlocked),
        "timeout" => Ok(AcpStopReason::Timeout),
        "native_crashed" => Ok(AcpStopReason::NativeCrashed),
        "malformed_stream" => Ok(AcpStopReason::MalformedStream),
        _ => Err(AcpStopReason::MalformedStream),
    }
}

fn presentation_policy_for_event(event_type: &str) -> AcpPresentationPolicy {
    match event_type {
        "message.delta" | "run.completed" => AcpPresentationPolicy::UserVisible,
        "approval.request" | "approval.decision" | "tool.call" | "tool.result" => {
            AcpPresentationPolicy::OperatorVisible
        }
        _ => AcpPresentationPolicy::InternalOnly,
    }
}

fn replay_safety_for_event(
    event_type: &str,
    stop_reason: Option<AcpStopReason>,
) -> AcpReplaySafety {
    if matches!(stop_reason, Some(AcpStopReason::NativeCrashed | AcpStopReason::MalformedStream)) {
        return AcpReplaySafety::NonReplayable;
    }
    match event_type {
        "tool.call" | "approval.request" => AcpReplaySafety::NonReplayable,
        _ => AcpReplaySafety::Replayable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(cursor: u64, event_type: &str) -> AcpRuntimeEvent {
        AcpRuntimeEvent {
            cursor,
            runtime_event_id: format!("evt-{cursor}"),
            event_type: event_type.to_owned(),
            redacted_summary: "event summary".to_owned(),
            payload_sha256: "b".repeat(64),
            stop_reason: None,
        }
    }

    #[test]
    fn replay_cursor_idempotency_marks_duplicate_delivery() {
        let mut translator = AcpReplayTranslator::new();
        let first = translator.translate(event(1, "message.delta")).expect("event translates");
        let second = translator.translate(event(1, "message.delta")).expect("event translates");

        assert!(!first.duplicate);
        assert!(second.duplicate);
        assert_eq!(first.record.translated_event_hash, second.record.translated_event_hash);
    }

    #[test]
    fn presentation_policy_is_explicit() {
        let mut translator = AcpReplayTranslator::new();
        let approval =
            translator.translate(event(2, "approval.request")).expect("approval translates");
        let message = translator.translate(event(3, "message.delta")).expect("message translates");

        assert_eq!(approval.record.presentation_policy, AcpPresentationPolicy::OperatorVisible);
        assert_eq!(message.record.presentation_policy, AcpPresentationPolicy::UserVisible);
    }

    #[test]
    fn stop_reason_mapping_is_stable() {
        let mut translator = AcpReplayTranslator::new();
        let translated = translator
            .translate(AcpRuntimeEvent {
                stop_reason: Some("native_crashed".to_owned()),
                ..event(4, "run.failed")
            })
            .expect("terminal event translates");

        assert_eq!(translated.record.stop_reason, Some(AcpStopReason::NativeCrashed));
        assert_eq!(translated.record.replay_safety, AcpReplaySafety::NonReplayable);
    }

    #[test]
    fn malformed_event_is_classified() {
        let mut translator = AcpReplayTranslator::new();
        let error = translator
            .translate(event(5, "provider.raw.unknown"))
            .expect_err("unknown event must fail");

        assert_eq!(error, AcpStopReason::MalformedStream);
    }

    #[test]
    fn support_bundle_window_uses_hashes_not_raw_event_ids() {
        let mut translator = AcpReplayTranslator::new();
        let translated = translator.translate(event(6, "run.completed")).expect("event translates");
        let window = support_bundle_acp_window(&[translated.record]);
        let rendered = window.to_string();

        assert!(!rendered.contains("evt-6"));
        assert!(rendered.contains("translated_event_hash"));
    }
}
