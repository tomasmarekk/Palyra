//! Host-owned callback proxy for agent harness attempts.
//!
//! Harnesses report lifecycle, model, tool, and finalization observations
//! through this proxy. The proxy records redacted, bounded events and keeps
//! idempotency decisions host-owned; it never exposes a journal writer or tool
//! executor to the harness.

use std::collections::BTreeSet;

use palyra_common::{
    redaction::redact_diagnostic_text, runtime_contracts::AgentHarnessCallbackKind,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

const CALLBACK_PAYLOAD_LIMIT_BYTES: usize = 8 * 1024;

/// Capability scope attached to a harness callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessCallbackCapabilityScope {
    Lifecycle,
    ModelStream,
    ToolBridge,
    ApprovalRelay,
    Verification,
    FinalOutcome,
}

/// Redaction policy applied before a callback is retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessCallbackRedactionPolicy {
    MetadataOnly,
    RedactedPayload,
    RedactedSummary,
}

/// Callback request submitted by a harness.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessCallbackRequest {
    pub callback_kind: AgentHarnessCallbackKind,
    pub capability_scope: HarnessCallbackCapabilityScope,
    pub redaction_policy: HarnessCallbackRedactionPolicy,
    pub idempotency_key: String,
    pub payload: Value,
}

/// Host-retained callback event after redaction and idempotency checks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessCallbackRecord {
    pub callback_kind: AgentHarnessCallbackKind,
    pub capability_scope: HarnessCallbackCapabilityScope,
    pub redaction_policy: HarnessCallbackRedactionPolicy,
    pub idempotency_key: String,
    pub payload: Value,
    pub duplicate: bool,
}

/// Callback proxy failure with stable safe codes.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HarnessCallbackError {
    #[error("callback is not allowed for this attempt: {callback_kind}")]
    CallbackNotAllowed { callback_kind: String },
    #[error("callback idempotency key must be non-empty")]
    EmptyIdempotencyKey,
    #[error("direct journal authority is not available to harness callbacks")]
    DirectJournalAuthorityDenied,
}

/// Host-owned callback proxy with bounded redacted retention.
#[derive(Debug, Clone)]
pub struct HarnessCallbackProxy {
    allowed_callbacks: BTreeSet<AgentHarnessCallbackKind>,
    direct_journal_write_allowed: bool,
    observed_idempotency_keys: BTreeSet<String>,
    records: Vec<HarnessCallbackRecord>,
}

impl HarnessCallbackProxy {
    /// Builds a proxy from the callback capabilities already prepared for an attempt.
    #[must_use]
    pub fn new(
        allowed_callbacks: impl IntoIterator<Item = AgentHarnessCallbackKind>,
        direct_journal_write_allowed: bool,
    ) -> Self {
        Self {
            allowed_callbacks: allowed_callbacks.into_iter().collect(),
            direct_journal_write_allowed,
            observed_idempotency_keys: BTreeSet::new(),
            records: Vec::new(),
        }
    }

    /// Returns retained callback records.
    #[must_use]
    pub fn records(&self) -> &[HarnessCallbackRecord] {
        self.records.as_slice()
    }

    /// Rejects attempts to obtain direct journal authority.
    ///
    /// # Errors
    /// Always returns [`HarnessCallbackError::DirectJournalAuthorityDenied`] for public harnesses.
    pub fn require_no_direct_journal_authority(&self) -> Result<(), HarnessCallbackError> {
        if self.direct_journal_write_allowed {
            Ok(())
        } else {
            Err(HarnessCallbackError::DirectJournalAuthorityDenied)
        }
    }

    /// Emits one callback event after validation, redaction, and idempotency handling.
    ///
    /// # Errors
    /// Returns [`HarnessCallbackError`] when the callback is disallowed or lacks
    /// a stable idempotency key.
    pub fn emit(
        &mut self,
        request: HarnessCallbackRequest,
    ) -> Result<HarnessCallbackRecord, HarnessCallbackError> {
        if !self.allowed_callbacks.contains(&request.callback_kind) {
            return Err(HarnessCallbackError::CallbackNotAllowed {
                callback_kind: request.callback_kind.as_str().to_owned(),
            });
        }
        if request.idempotency_key.trim().is_empty() {
            return Err(HarnessCallbackError::EmptyIdempotencyKey);
        }

        let duplicate = !self.observed_idempotency_keys.insert(request.idempotency_key.clone());
        let record = HarnessCallbackRecord {
            callback_kind: request.callback_kind,
            capability_scope: request.capability_scope,
            redaction_policy: request.redaction_policy,
            idempotency_key: request.idempotency_key,
            payload: sanitize_callback_payload(request.payload, request.redaction_policy),
            duplicate,
        };
        if !duplicate {
            self.records.push(record.clone());
        }
        Ok(record)
    }
}

fn sanitize_callback_payload(payload: Value, policy: HarnessCallbackRedactionPolicy) -> Value {
    let redacted = match policy {
        HarnessCallbackRedactionPolicy::MetadataOnly => json!({
            "schema_version": 1,
            "redaction": "metadata_only",
        }),
        HarnessCallbackRedactionPolicy::RedactedSummary => {
            let summary = payload
                .get("summary")
                .and_then(Value::as_str)
                .map(redact_diagnostic_text)
                .unwrap_or_else(|| "<redacted-summary>".to_owned());
            json!({ "summary": summary })
        }
        HarnessCallbackRedactionPolicy::RedactedPayload => redact_value(payload),
    };
    bound_callback_payload(redacted)
}

fn redact_value(value: Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut redacted = serde_json::Map::new();
            for (key, child) in object {
                if palyra_common::redaction::is_sensitive_key(key.as_str()) {
                    redacted
                        .insert(key, Value::String(palyra_common::redaction::REDACTED.to_owned()));
                } else {
                    redacted.insert(key, redact_value(child));
                }
            }
            Value::Object(redacted)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(redact_value).collect()),
        Value::String(raw) => Value::String(redact_diagnostic_text(raw.as_str())),
        other => other,
    }
}

fn bound_callback_payload(payload: Value) -> Value {
    match serde_json::to_vec(&payload) {
        Ok(encoded) if encoded.len() <= CALLBACK_PAYLOAD_LIMIT_BYTES => payload,
        Ok(encoded) => json!({
            "schema_version": 1,
            "truncated": true,
            "original_bytes": encoded.len(),
            "limit_bytes": CALLBACK_PAYLOAD_LIMIT_BYTES,
        }),
        Err(_) => json!({
            "schema_version": 1,
            "truncated": true,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(key: &str) -> HarnessCallbackRequest {
        HarnessCallbackRequest {
            callback_kind: AgentHarnessCallbackKind::ToolEvent,
            capability_scope: HarnessCallbackCapabilityScope::ToolBridge,
            redaction_policy: HarnessCallbackRedactionPolicy::RedactedPayload,
            idempotency_key: key.to_owned(),
            payload: json!({
                "tool": "palyra.fs.read_file",
                "api_key": "secret-token",
                "summary": "read ok",
            }),
        }
    }

    #[test]
    fn callback_proxy_redacts_payload_and_blocks_journal_authority() {
        let mut proxy = HarnessCallbackProxy::new(
            [AgentHarnessCallbackKind::ToolEvent, AgentHarnessCallbackKind::FinalOutcome],
            false,
        );

        let record = proxy.emit(request("tool:1")).expect("callback should emit");
        let serialized = serde_json::to_string(&record).expect("record should serialize");

        assert_eq!(
            proxy.require_no_direct_journal_authority(),
            Err(HarnessCallbackError::DirectJournalAuthorityDenied)
        );
        assert!(!serialized.contains("secret-token"));
        assert_eq!(proxy.records().len(), 1);
    }

    #[test]
    fn callback_proxy_deduplicates_idempotency_keys() {
        let mut proxy = HarnessCallbackProxy::new([AgentHarnessCallbackKind::ToolEvent], false);

        let first = proxy.emit(request("tool:1")).expect("first callback should emit");
        let second = proxy.emit(request("tool:1")).expect("duplicate callback should return");

        assert!(!first.duplicate);
        assert!(second.duplicate);
        assert_eq!(proxy.records().len(), 1);
    }

    #[test]
    fn callback_proxy_rejects_disallowed_callback_kind() {
        let mut proxy = HarnessCallbackProxy::new([AgentHarnessCallbackKind::FinalOutcome], false);

        let error = proxy.emit(request("tool:1")).expect_err("tool callback should not be allowed");

        assert_eq!(
            error,
            HarnessCallbackError::CallbackNotAllowed { callback_kind: "tool_event".to_owned() }
        );
    }
}
