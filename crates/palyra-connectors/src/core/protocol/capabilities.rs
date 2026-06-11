//! Connector capability declarations and operation preflight contracts.
//!
//! Each provider advertises a [`ConnectorCapabilitySet`] so the daemon can gate
//! operations, attach policy actions, and surface approval/risk metadata
//! without provider-specific knowledge.

use serde::{Deserialize, Serialize};

use super::validation::{
    validate_non_empty_identifier, validate_optional_field, validate_permission_labels,
    ProtocolError, MAX_AUDIT_EVENT_TYPE_BYTES, MAX_OPERATION_REASON_BYTES, MAX_POLICY_ACTION_BYTES,
};

/// Support declaration for a single connector capability, with optional policy
/// metadata (action, approval mode, risk level, audit type, permissions).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorCapabilitySupport {
    pub supported: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_action: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_mode: Option<ConnectorApprovalMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk_level: Option<ConnectorRiskLevel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audit_event_type: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_permissions: Vec<String>,
}

impl ConnectorCapabilitySupport {
    /// Creates a supported capability with no extra policy metadata.
    #[must_use]
    pub fn supported() -> Self {
        Self {
            supported: true,
            reason: None,
            policy_action: None,
            approval_mode: None,
            risk_level: None,
            audit_event_type: None,
            required_permissions: Vec::new(),
        }
    }

    /// Creates an unsupported capability carrying the operator-facing reason.
    #[must_use]
    pub fn unsupported(reason: impl Into<String>) -> Self {
        Self {
            supported: false,
            reason: Some(reason.into()),
            policy_action: None,
            approval_mode: None,
            risk_level: None,
            audit_event_type: None,
            required_permissions: Vec::new(),
        }
    }

    /// Sets the policy action evaluated before this capability is exercised.
    #[must_use]
    pub fn with_policy_action(mut self, policy_action: impl Into<String>) -> Self {
        self.policy_action = Some(policy_action.into());
        self
    }

    /// Sets the approval mode required for this capability.
    #[must_use]
    pub fn with_approval_mode(mut self, approval_mode: ConnectorApprovalMode) -> Self {
        self.approval_mode = Some(approval_mode);
        self
    }

    /// Sets the risk level surfaced to operators and policy.
    #[must_use]
    pub fn with_risk_level(mut self, risk_level: ConnectorRiskLevel) -> Self {
        self.risk_level = Some(risk_level);
        self
    }

    /// Sets the audit event type recorded when this capability is used.
    #[must_use]
    pub fn with_audit_event_type(mut self, audit_event_type: impl Into<String>) -> Self {
        self.audit_event_type = Some(audit_event_type.into());
        self
    }

    /// Sets the provider-side permissions required for this capability.
    #[must_use]
    pub fn with_required_permissions<I, S>(mut self, required_permissions: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.required_permissions = required_permissions.into_iter().map(Into::into).collect();
        self
    }
}

/// Whether exercising a capability requires operator approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorApprovalMode {
    None,
    Conditional,
    Required,
}

/// Risk classification attached to a capability or operation preflight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorRiskLevel {
    Low,
    Medium,
    High,
    Conditional,
}

/// Per-operation support matrix for message-level capabilities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorMessageCapabilitySet {
    pub send: ConnectorCapabilitySupport,
    pub thread: ConnectorCapabilitySupport,
    pub reply: ConnectorCapabilitySupport,
    pub read: ConnectorCapabilitySupport,
    pub search: ConnectorCapabilitySupport,
    pub edit: ConnectorCapabilitySupport,
    pub delete: ConnectorCapabilitySupport,
    pub react_add: ConnectorCapabilitySupport,
    pub react_remove: ConnectorCapabilitySupport,
}

/// Full capability surface a provider advertises to the daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorCapabilitySet {
    pub lifecycle: ConnectorCapabilitySupport,
    pub status: ConnectorCapabilitySupport,
    pub logs: ConnectorCapabilitySupport,
    pub health_refresh: ConnectorCapabilitySupport,
    pub resolve: ConnectorCapabilitySupport,
    pub pairings: ConnectorCapabilitySupport,
    pub qr: ConnectorCapabilitySupport,
    pub webhook_ingress: ConnectorCapabilitySupport,
    pub message: ConnectorMessageCapabilitySet,
}

/// Policy decision an adapter reports alongside a message operation result.
///
/// `allowed == false` means the operation was denied by provider-side policy;
/// the result then carries the denial metadata instead of an effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorOperationPreflight {
    pub allowed: bool,
    pub policy_action: String,
    pub approval_mode: ConnectorApprovalMode,
    pub risk_level: ConnectorRiskLevel,
    pub audit_event_type: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_permissions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl ConnectorOperationPreflight {
    /// Validates identifier fields and size limits.
    ///
    /// # Errors
    /// Returns a protocol error naming the first invalid field.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_non_empty_identifier(
            self.policy_action.as_str(),
            "preflight.policy_action",
            MAX_POLICY_ACTION_BYTES,
        )?;
        validate_non_empty_identifier(
            self.audit_event_type.as_str(),
            "preflight.audit_event_type",
            MAX_AUDIT_EVENT_TYPE_BYTES,
        )?;
        validate_permission_labels(
            self.required_permissions.as_slice(),
            "preflight.required_permissions",
        )?;
        validate_optional_field(
            self.reason.as_deref(),
            "preflight.reason",
            MAX_OPERATION_REASON_BYTES,
        )?;
        Ok(())
    }
}
