use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

const MAX_CONNECTOR_ID_BYTES: usize = 128;
const MAX_CONNECTOR_PRINCIPAL_BYTES: usize = 128;
const MAX_ENVELOPE_ID_BYTES: usize = 128;
const MAX_CONVERSATION_ID_BYTES: usize = 256;
const MAX_IDENTITY_BYTES: usize = 256;
const MAX_MESSAGE_BYTES: usize = 128 * 1024;
const MAX_ATTACHMENTS_PER_MESSAGE: usize = 32;
const MAX_ATTACHMENT_REF_BYTES: usize = 1_024;
const MAX_ATTACHMENT_FILENAME_BYTES: usize = 512;
const MAX_ATTACHMENT_CONTENT_TYPE_BYTES: usize = 256;
const MAX_ATTACHMENT_ID_BYTES: usize = 128;
const MAX_ATTACHMENT_HASH_BYTES: usize = 128;
const MAX_ATTACHMENT_ORIGIN_BYTES: usize = 128;
const MAX_ATTACHMENT_POLICY_CONTEXT_BYTES: usize = 512;
const MAX_ATTACHMENT_INLINE_BASE64_BYTES: usize = 2 * 1024 * 1024;
const MAX_STRUCTURED_OUTPUT_BYTES: usize = 128 * 1024;
const MAX_A2UI_SURFACE_BYTES: usize = 128;
const MAX_A2UI_PATCH_BYTES: usize = 128 * 1024;
const MAX_POLICY_ACTION_BYTES: usize = 128;
const MAX_AUDIT_EVENT_TYPE_BYTES: usize = 128;
const MAX_PERMISSION_LABEL_BYTES: usize = 128;
const MAX_CURSOR_ID_BYTES: usize = 128;
const MAX_SEARCH_QUERY_BYTES: usize = 1_024;
const MAX_MESSAGE_LINK_BYTES: usize = 2_048;
const MAX_EMOJI_BYTES: usize = 128;
const MAX_OPERATION_REASON_BYTES: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorKind {
    Echo,
    Discord,
    Telegram,
    Slack,
}

impl ConnectorKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Echo => "echo",
            Self::Discord => "discord",
            Self::Telegram => "telegram",
            Self::Slack => "slack",
        }
    }

    #[must_use]
    pub fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "echo" => Some(Self::Echo),
            "discord" => Some(Self::Discord),
            "telegram" => Some(Self::Telegram),
            "slack" => Some(Self::Slack),
            _ => None,
        }
    }

    #[must_use]
    pub const fn default_availability(self) -> ConnectorAvailability {
        match self {
            Self::Discord => ConnectorAvailability::Supported,
            Self::Echo => ConnectorAvailability::InternalTestOnly,
            Self::Telegram | Self::Slack => ConnectorAvailability::Deferred,
        }
    }
}

impl Display for ConnectorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorAvailability {
    Supported,
    InternalTestOnly,
    Deferred,
}

impl ConnectorAvailability {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::InternalTestOnly => "internal_test_only",
            Self::Deferred => "deferred",
        }
    }

    #[must_use]
    pub fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "supported" => Some(Self::Supported),
            "internal_test_only" => Some(Self::InternalTestOnly),
            "deferred" => Some(Self::Deferred),
            _ => None,
        }
    }
}

impl Display for ConnectorAvailability {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorReadiness {
    Ready,
    MissingCredential,
    AuthFailed,
    Misconfigured,
}

impl ConnectorReadiness {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::MissingCredential => "missing_credential",
            Self::AuthFailed => "auth_failed",
            Self::Misconfigured => "misconfigured",
        }
    }

    #[must_use]
    pub fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "ready" => Some(Self::Ready),
            "missing_credential" => Some(Self::MissingCredential),
            "auth_failed" => Some(Self::AuthFailed),
            "misconfigured" => Some(Self::Misconfigured),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorLiveness {
    Stopped,
    Running,
    Restarting,
    Crashed,
}

impl ConnectorLiveness {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::Running => "running",
            Self::Restarting => "restarting",
            Self::Crashed => "crashed",
        }
    }

    #[must_use]
    pub fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "stopped" => Some(Self::Stopped),
            "running" => Some(Self::Running),
            "restarting" => Some(Self::Restarting),
            "crashed" => Some(Self::Crashed),
            _ => None,
        }
    }
}

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

    #[must_use]
    pub fn with_policy_action(mut self, policy_action: impl Into<String>) -> Self {
        self.policy_action = Some(policy_action.into());
        self
    }

    #[must_use]
    pub fn with_approval_mode(mut self, approval_mode: ConnectorApprovalMode) -> Self {
        self.approval_mode = Some(approval_mode);
        self
    }

    #[must_use]
    pub fn with_risk_level(mut self, risk_level: ConnectorRiskLevel) -> Self {
        self.risk_level = Some(risk_level);
        self
    }

    #[must_use]
    pub fn with_audit_event_type(mut self, audit_event_type: impl Into<String>) -> Self {
        self.audit_event_type = Some(audit_event_type.into());
        self
    }

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorApprovalMode {
    None,
    Conditional,
    Required,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorRiskLevel {
    Low,
    Medium,
    High,
    Conditional,
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorConversationTarget {
    pub conversation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
}

impl ConnectorConversationTarget {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_non_empty_identifier(
            self.conversation_id.as_str(),
            "target.conversation_id",
            MAX_CONVERSATION_ID_BYTES,
        )?;
        validate_optional_field(
            self.thread_id.as_deref(),
            "target.thread_id",
            MAX_CONVERSATION_ID_BYTES,
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorMessageLocator {
    #[serde(flatten)]
    pub target: ConnectorConversationTarget,
    pub message_id: String,
}

impl ConnectorMessageLocator {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.target.validate()?;
        validate_non_empty_identifier(
            self.message_id.as_str(),
            "message_id",
            MAX_ENVELOPE_ID_BYTES,
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorMessageReadRequest {
    #[serde(flatten)]
    pub target: ConnectorConversationTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub around_message_id: Option<String>,
    pub limit: usize,
}

impl ConnectorMessageReadRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.target.validate()?;
        validate_optional_field(self.message_id.as_deref(), "message_id", MAX_CURSOR_ID_BYTES)?;
        validate_optional_field(
            self.before_message_id.as_deref(),
            "before_message_id",
            MAX_CURSOR_ID_BYTES,
        )?;
        validate_optional_field(
            self.after_message_id.as_deref(),
            "after_message_id",
            MAX_CURSOR_ID_BYTES,
        )?;
        validate_optional_field(
            self.around_message_id.as_deref(),
            "around_message_id",
            MAX_CURSOR_ID_BYTES,
        )?;
        if self.limit == 0 {
            return Err(ProtocolError::InvalidField {
                field: "limit",
                reason: "must be greater than zero",
            });
        }
        if self.message_id.is_some()
            && (self.before_message_id.is_some()
                || self.after_message_id.is_some()
                || self.around_message_id.is_some())
        {
            return Err(ProtocolError::InvalidField {
                field: "message_id",
                reason: "exact message fetch cannot be combined with pagination cursors",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorMessageSearchRequest {
    #[serde(flatten)]
    pub target: ConnectorConversationTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_attachments: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_message_id: Option<String>,
    pub limit: usize,
}

impl ConnectorMessageSearchRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.target.validate()?;
        validate_optional_field(self.query.as_deref(), "query", MAX_SEARCH_QUERY_BYTES)?;
        validate_optional_field(self.author_id.as_deref(), "author_id", MAX_IDENTITY_BYTES)?;
        validate_optional_field(
            self.before_message_id.as_deref(),
            "before_message_id",
            MAX_CURSOR_ID_BYTES,
        )?;
        if self.limit == 0 {
            return Err(ProtocolError::InvalidField {
                field: "limit",
                reason: "must be greater than zero",
            });
        }
        if self.query.as_deref().map(str::trim).filter(|value| !value.is_empty()).is_none()
            && self.author_id.as_deref().map(str::trim).filter(|value| !value.is_empty()).is_none()
            && self.has_attachments.is_none()
        {
            return Err(ProtocolError::InvalidField {
                field: "query",
                reason: "search requires at least one filter",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorMessageEditRequest {
    pub locator: ConnectorMessageLocator,
    pub body: String,
}

impl ConnectorMessageEditRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.locator.validate()?;
        validate_message_body(self.body.as_str(), MAX_MESSAGE_BYTES, "body")?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorMessageDeleteRequest {
    pub locator: ConnectorMessageLocator,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl ConnectorMessageDeleteRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.locator.validate()?;
        validate_optional_field(self.reason.as_deref(), "reason", MAX_OPERATION_REASON_BYTES)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorMessageReactionRequest {
    pub locator: ConnectorMessageLocator,
    pub emoji: String,
}

impl ConnectorMessageReactionRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.locator.validate()?;
        validate_non_empty_identifier(self.emoji.as_str(), "emoji", MAX_EMOJI_BYTES)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConnectorMessageReactionRecord {
    pub emoji: String,
    #[serde(default)]
    pub count: u32,
    #[serde(default)]
    pub reacted_by_connector: bool,
}

impl ConnectorMessageReactionRecord {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_non_empty_identifier(self.emoji.as_str(), "reactions.emoji", MAX_EMOJI_BYTES)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorMessageRecord {
    pub locator: ConnectorMessageLocator,
    pub sender_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_display: Option<String>,
    pub body: String,
    pub created_at_unix_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edited_at_unix_ms: Option<i64>,
    #[serde(default)]
    pub is_direct_message: bool,
    #[serde(default)]
    pub is_connector_authored: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
    #[serde(default)]
    pub attachments: Vec<AttachmentRef>,
    #[serde(default)]
    pub reactions: Vec<ConnectorMessageReactionRecord>,
}

impl ConnectorMessageRecord {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.locator.validate()?;
        validate_non_empty_identifier(self.sender_id.as_str(), "sender_id", MAX_IDENTITY_BYTES)?;
        validate_optional_field(
            self.sender_display.as_deref(),
            "sender_display",
            MAX_IDENTITY_BYTES,
        )?;
        if self.body.len() > MAX_MESSAGE_BYTES {
            return Err(ProtocolError::InvalidField {
                field: "body",
                reason: "message body exceeds size limit",
            });
        }
        validate_optional_field(self.link.as_deref(), "link", MAX_MESSAGE_LINK_BYTES)?;
        validate_attachments(self.attachments.as_slice())?;
        for reaction in &self.reactions {
            reaction.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorMessageReadResult {
    pub preflight: ConnectorOperationPreflight,
    #[serde(flatten)]
    pub target: ConnectorConversationTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact_message_id: Option<String>,
    #[serde(default)]
    pub messages: Vec<ConnectorMessageRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_before_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_message_id: Option<String>,
}

impl ConnectorMessageReadResult {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.preflight.validate()?;
        self.target.validate()?;
        validate_optional_field(
            self.exact_message_id.as_deref(),
            "exact_message_id",
            MAX_CURSOR_ID_BYTES,
        )?;
        validate_optional_field(
            self.next_before_message_id.as_deref(),
            "next_before_message_id",
            MAX_CURSOR_ID_BYTES,
        )?;
        validate_optional_field(
            self.next_after_message_id.as_deref(),
            "next_after_message_id",
            MAX_CURSOR_ID_BYTES,
        )?;
        for message in &self.messages {
            message.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorMessageSearchResult {
    pub preflight: ConnectorOperationPreflight,
    #[serde(flatten)]
    pub target: ConnectorConversationTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_attachments: Option<bool>,
    #[serde(default)]
    pub matches: Vec<ConnectorMessageRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_before_message_id: Option<String>,
}

impl ConnectorMessageSearchResult {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.preflight.validate()?;
        self.target.validate()?;
        validate_optional_field(self.query.as_deref(), "query", MAX_SEARCH_QUERY_BYTES)?;
        validate_optional_field(self.author_id.as_deref(), "author_id", MAX_IDENTITY_BYTES)?;
        validate_optional_field(
            self.next_before_message_id.as_deref(),
            "next_before_message_id",
            MAX_CURSOR_ID_BYTES,
        )?;
        for message in &self.matches {
            message.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorMessageMutationStatus {
    Updated,
    Deleted,
    ReactionAdded,
    ReactionRemoved,
    Denied,
    Noop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConnectorMessageMutationDiff {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_body: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_body: Option<String>,
}

impl ConnectorMessageMutationDiff {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_optional_field(
            self.before_body.as_deref(),
            "diff.before_body",
            MAX_MESSAGE_BYTES,
        )?;
        validate_optional_field(self.after_body.as_deref(), "diff.after_body", MAX_MESSAGE_BYTES)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorMessageMutationResult {
    pub preflight: ConnectorOperationPreflight,
    pub locator: ConnectorMessageLocator,
    pub status: ConnectorMessageMutationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<ConnectorMessageRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff: Option<ConnectorMessageMutationDiff>,
}

impl ConnectorMessageMutationResult {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.preflight.validate()?;
        self.locator.validate()?;
        validate_optional_field(self.reason.as_deref(), "reason", MAX_OPERATION_REASON_BYTES)?;
        if let Some(message) = self.message.as_ref() {
            message.validate()?;
        }
        if let Some(diff) = self.diff.as_ref() {
            diff.validate()?;
        }
        Ok(())
    }
}

impl ConnectorCapabilitySet {
    #[must_use]
    pub fn for_connector(kind: ConnectorKind, availability: ConnectorAvailability) -> Self {
        match kind {
            ConnectorKind::Discord => Self {
                lifecycle: ConnectorCapabilitySupport::supported(),
                status: ConnectorCapabilitySupport::supported(),
                logs: ConnectorCapabilitySupport::supported(),
                health_refresh: ConnectorCapabilitySupport::supported(),
                resolve: ConnectorCapabilitySupport::supported(),
                pairings: ConnectorCapabilitySupport::supported(),
                qr: ConnectorCapabilitySupport::supported(),
                webhook_ingress: ConnectorCapabilitySupport::unsupported(
                    "discord connector does not expose generic webhook ingress management",
                ),
                message: ConnectorMessageCapabilitySet {
                    send: ConnectorCapabilitySupport::supported(),
                    thread: ConnectorCapabilitySupport::supported(),
                    reply: ConnectorCapabilitySupport::supported(),
                    read: ConnectorCapabilitySupport::unsupported(
                        "message read requires a dedicated Discord read surface that is not implemented yet",
                    ),
                    search: ConnectorCapabilitySupport::unsupported(
                        "message search requires a dedicated Discord search surface that is not implemented yet",
                    ),
                    edit: ConnectorCapabilitySupport::unsupported(
                        "message edit is not implemented for Discord in the current admin surface",
                    ),
                    delete: ConnectorCapabilitySupport::unsupported(
                        "message delete is not implemented for Discord in the current admin surface",
                    ),
                    react_add: ConnectorCapabilitySupport::unsupported(
                        "reaction add is not implemented in the current admin surface",
                    ),
                    react_remove: ConnectorCapabilitySupport::unsupported(
                        "reaction remove requires transport delete support that is not implemented yet",
                    ),
                },
            },
            ConnectorKind::Echo => Self {
                lifecycle: ConnectorCapabilitySupport::unsupported(
                    "echo connector is internal-test-only and is not a managed account surface",
                ),
                status: ConnectorCapabilitySupport::supported(),
                logs: ConnectorCapabilitySupport::supported(),
                health_refresh: ConnectorCapabilitySupport::unsupported(
                    "health refresh is only implemented for Discord connectors",
                ),
                resolve: ConnectorCapabilitySupport::unsupported(
                    "echo connector has no provider-specific entity resolution surface",
                ),
                pairings: ConnectorCapabilitySupport::unsupported(
                    "pairings are intended for user-facing providers, not the echo test connector",
                ),
                qr: ConnectorCapabilitySupport::unsupported(
                    "QR pairing output is unavailable for the echo test connector",
                ),
                webhook_ingress: ConnectorCapabilitySupport::unsupported(
                    "echo connector does not expose webhook ingress management",
                ),
                message: ConnectorMessageCapabilitySet {
                    send: ConnectorCapabilitySupport::unsupported(
                        "echo connector is reserved for internal diagnostics and not exposed as a user-facing message provider",
                    ),
                    thread: ConnectorCapabilitySupport::unsupported(
                        "echo connector does not model user-facing thread delivery",
                    ),
                    reply: ConnectorCapabilitySupport::unsupported(
                        "echo connector does not model reply semantics for user-facing workflows",
                    ),
                    read: ConnectorCapabilitySupport::unsupported(
                        "echo connector does not persist readable message history",
                    ),
                    search: ConnectorCapabilitySupport::unsupported(
                        "echo connector does not index message history for search",
                    ),
                    edit: ConnectorCapabilitySupport::unsupported(
                        "echo connector does not support editing delivered messages",
                    ),
                    delete: ConnectorCapabilitySupport::unsupported(
                        "echo connector does not support deleting delivered messages",
                    ),
                    react_add: ConnectorCapabilitySupport::unsupported(
                        "echo connector does not support message reactions",
                    ),
                    react_remove: ConnectorCapabilitySupport::unsupported(
                        "echo connector does not support message reactions",
                    ),
                },
            },
            ConnectorKind::Slack | ConnectorKind::Telegram => {
                let deferred_reason = if availability == ConnectorAvailability::Deferred {
                    format!(
                        "{} connector is deferred in the roadmap and unavailable in the current runtime",
                        kind.as_str()
                    )
                } else {
                    format!(
                        "{} connector is unavailable in the current runtime",
                        kind.as_str()
                    )
                };
                Self {
                    lifecycle: ConnectorCapabilitySupport::unsupported(deferred_reason.clone()),
                    status: ConnectorCapabilitySupport::supported(),
                    logs: ConnectorCapabilitySupport::supported(),
                    health_refresh: ConnectorCapabilitySupport::unsupported(deferred_reason.clone()),
                    resolve: ConnectorCapabilitySupport::unsupported(deferred_reason.clone()),
                    pairings: ConnectorCapabilitySupport::unsupported(deferred_reason.clone()),
                    qr: ConnectorCapabilitySupport::unsupported(deferred_reason.clone()),
                    webhook_ingress: ConnectorCapabilitySupport::unsupported(deferred_reason.clone()),
                    message: ConnectorMessageCapabilitySet {
                        send: ConnectorCapabilitySupport::unsupported(deferred_reason.clone()),
                        thread: ConnectorCapabilitySupport::unsupported(deferred_reason.clone()),
                        reply: ConnectorCapabilitySupport::unsupported(deferred_reason.clone()),
                        read: ConnectorCapabilitySupport::unsupported(deferred_reason.clone()),
                        search: ConnectorCapabilitySupport::unsupported(deferred_reason.clone()),
                        edit: ConnectorCapabilitySupport::unsupported(deferred_reason.clone()),
                        delete: ConnectorCapabilitySupport::unsupported(deferred_reason.clone()),
                        react_add: ConnectorCapabilitySupport::unsupported(deferred_reason.clone()),
                        react_remove: ConnectorCapabilitySupport::unsupported(deferred_reason),
                    },
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorInstanceSpec {
    pub connector_id: String,
    pub kind: ConnectorKind,
    pub principal: String,
    pub auth_profile_ref: Option<String>,
    pub token_vault_ref: Option<String>,
    pub egress_allowlist: Vec<String>,
    pub enabled: bool,
}

impl ConnectorInstanceSpec {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_non_empty_identifier(
            self.connector_id.as_str(),
            "connector_id",
            MAX_CONNECTOR_ID_BYTES,
        )?;
        validate_non_empty_identifier(
            self.principal.as_str(),
            "principal",
            MAX_CONNECTOR_PRINCIPAL_BYTES,
        )?;
        for host in &self.egress_allowlist {
            validate_host_pattern(host)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentKind {
    Image,
    #[default]
    File,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AttachmentRef {
    #[serde(default)]
    pub kind: AttachmentKind,
    #[serde(default)]
    pub attachment_id: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub artifact_ref: Option<String>,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub size_bytes: Option<u64>,
    #[serde(default)]
    pub content_hash: Option<String>,
    #[serde(default)]
    pub origin: Option<String>,
    #[serde(default)]
    pub policy_context: Option<String>,
    #[serde(default)]
    pub upload_requested: bool,
    #[serde(default)]
    pub inline_base64: Option<String>,
    #[serde(default)]
    pub width_px: Option<u32>,
    #[serde(default)]
    pub height_px: Option<u32>,
}

impl AttachmentRef {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_optional_field(
            self.attachment_id.as_deref(),
            "attachments.attachment_id",
            MAX_ATTACHMENT_ID_BYTES,
        )?;
        validate_optional_field(self.url.as_deref(), "attachments.url", MAX_ATTACHMENT_REF_BYTES)?;
        validate_optional_field(
            self.artifact_ref.as_deref(),
            "attachments.artifact_ref",
            MAX_ATTACHMENT_REF_BYTES,
        )?;
        validate_optional_field(
            self.filename.as_deref(),
            "attachments.filename",
            MAX_ATTACHMENT_FILENAME_BYTES,
        )?;
        validate_optional_field(
            self.content_type.as_deref(),
            "attachments.content_type",
            MAX_ATTACHMENT_CONTENT_TYPE_BYTES,
        )?;
        validate_optional_field(
            self.content_hash.as_deref(),
            "attachments.content_hash",
            MAX_ATTACHMENT_HASH_BYTES,
        )?;
        validate_optional_field(
            self.origin.as_deref(),
            "attachments.origin",
            MAX_ATTACHMENT_ORIGIN_BYTES,
        )?;
        validate_optional_field(
            self.policy_context.as_deref(),
            "attachments.policy_context",
            MAX_ATTACHMENT_POLICY_CONTEXT_BYTES,
        )?;
        validate_optional_field(
            self.inline_base64.as_deref(),
            "attachments.inline_base64",
            MAX_ATTACHMENT_INLINE_BASE64_BYTES,
        )?;
        if self.upload_requested
            && self
                .inline_base64
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
        {
            return Err(ProtocolError::InvalidField {
                field: "attachments.inline_base64",
                reason: "upload_requested attachments must include inline_base64",
            });
        }
        Ok(())
    }
}

pub type OutboundAttachment = AttachmentRef;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundA2uiUpdate {
    pub surface: String,
    #[serde(default)]
    pub patch_json: Vec<u8>,
}

impl OutboundA2uiUpdate {
    fn validate(&self, max_patch_bytes: usize) -> Result<(), ProtocolError> {
        validate_non_empty_identifier(
            self.surface.as_str(),
            "a2ui_update.surface",
            MAX_A2UI_SURFACE_BYTES,
        )?;
        validate_json_bytes(
            self.patch_json.as_slice(),
            "a2ui_update.patch_json",
            max_patch_bytes.min(MAX_A2UI_PATCH_BYTES),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboundMessageEvent {
    pub envelope_id: String,
    pub connector_id: String,
    pub conversation_id: String,
    pub thread_id: Option<String>,
    pub sender_id: String,
    pub sender_display: Option<String>,
    pub body: String,
    pub adapter_message_id: Option<String>,
    pub adapter_thread_id: Option<String>,
    pub received_at_unix_ms: i64,
    pub is_direct_message: bool,
    pub requested_broadcast: bool,
    #[serde(default)]
    pub attachments: Vec<AttachmentRef>,
}

impl InboundMessageEvent {
    pub fn validate(&self, max_body_bytes: usize) -> Result<(), ProtocolError> {
        validate_non_empty_identifier(
            self.envelope_id.as_str(),
            "envelope_id",
            MAX_ENVELOPE_ID_BYTES,
        )?;
        validate_non_empty_identifier(
            self.connector_id.as_str(),
            "connector_id",
            MAX_CONNECTOR_ID_BYTES,
        )?;
        validate_non_empty_identifier(
            self.conversation_id.as_str(),
            "conversation_id",
            MAX_CONVERSATION_ID_BYTES,
        )?;
        validate_non_empty_identifier(self.sender_id.as_str(), "sender_id", MAX_IDENTITY_BYTES)?;
        validate_message_body(self.body.as_str(), max_body_bytes, "body")?;
        validate_attachments(self.attachments.as_slice())?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutedOutboundMessage {
    pub text: String,
    pub thread_id: Option<String>,
    pub in_reply_to_message_id: Option<String>,
    pub broadcast: bool,
    pub auto_ack_text: Option<String>,
    pub auto_reaction: Option<String>,
    #[serde(default)]
    pub attachments: Vec<OutboundAttachment>,
    #[serde(default)]
    pub structured_json: Option<Vec<u8>>,
    #[serde(default)]
    pub a2ui_update: Option<OutboundA2uiUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteInboundResult {
    pub accepted: bool,
    pub queued_for_retry: bool,
    pub decision_reason: String,
    pub outputs: Vec<RoutedOutboundMessage>,
    pub route_key: Option<String>,
    pub retry_attempt: u32,
    #[serde(default)]
    pub route_message_latency_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundMessageRequest {
    pub envelope_id: String,
    pub connector_id: String,
    pub conversation_id: String,
    pub reply_thread_id: Option<String>,
    pub in_reply_to_message_id: Option<String>,
    pub text: String,
    pub broadcast: bool,
    pub auto_ack_text: Option<String>,
    pub auto_reaction: Option<String>,
    #[serde(default)]
    pub attachments: Vec<OutboundAttachment>,
    #[serde(default)]
    pub structured_json: Option<Vec<u8>>,
    #[serde(default)]
    pub a2ui_update: Option<OutboundA2uiUpdate>,
    pub timeout_ms: u64,
    pub max_payload_bytes: usize,
}

impl OutboundMessageRequest {
    pub fn validate(&self, max_text_bytes: usize) -> Result<(), ProtocolError> {
        validate_non_empty_identifier(
            self.envelope_id.as_str(),
            "envelope_id",
            MAX_ENVELOPE_ID_BYTES,
        )?;
        validate_non_empty_identifier(
            self.connector_id.as_str(),
            "connector_id",
            MAX_CONNECTOR_ID_BYTES,
        )?;
        validate_non_empty_identifier(
            self.conversation_id.as_str(),
            "conversation_id",
            MAX_CONVERSATION_ID_BYTES,
        )?;
        validate_message_body(self.text.as_str(), max_text_bytes, "text")?;
        validate_attachments(self.attachments.as_slice())?;
        if self.timeout_ms == 0 {
            return Err(ProtocolError::InvalidField {
                field: "timeout_ms",
                reason: "must be greater than zero",
            });
        }
        if self.max_payload_bytes == 0 {
            return Err(ProtocolError::InvalidField {
                field: "max_payload_bytes",
                reason: "must be greater than zero",
            });
        }
        let max_payload_bytes = self.max_payload_bytes.min(max_text_bytes);
        if let Some(structured_json) = self.structured_json.as_deref() {
            validate_json_bytes(
                structured_json,
                "structured_json",
                max_payload_bytes.min(MAX_STRUCTURED_OUTPUT_BYTES),
            )?;
        }
        if let Some(update) = self.a2ui_update.as_ref() {
            update.validate(max_payload_bytes)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryClass {
    RateLimit,
    TransientNetwork,
    ConnectorRestarting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeliveryOutcome {
    Delivered { native_message_id: String },
    Retry { class: RetryClass, reason: String, retry_after_ms: Option<u64> },
    PermanentFailure { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorQueueDepth {
    pub pending_outbox: u64,
    pub dead_letters: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorStatusSnapshot {
    pub connector_id: String,
    pub kind: ConnectorKind,
    pub availability: ConnectorAvailability,
    pub capabilities: ConnectorCapabilitySet,
    pub principal: String,
    pub enabled: bool,
    pub readiness: ConnectorReadiness,
    pub liveness: ConnectorLiveness,
    pub restart_count: u32,
    pub queue_depth: ConnectorQueueDepth,
    pub last_error: Option<String>,
    pub last_inbound_unix_ms: Option<i64>,
    pub last_outbound_unix_ms: Option<i64>,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("invalid field '{field}': {reason}")]
    InvalidField { field: &'static str, reason: &'static str },
}

fn validate_non_empty_identifier(
    raw: &str,
    field: &'static str,
    max_bytes: usize,
) -> Result<(), ProtocolError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(ProtocolError::InvalidField { field, reason: "cannot be empty" });
    }
    if trimmed.len() > max_bytes {
        return Err(ProtocolError::InvalidField { field, reason: "value exceeds size limit" });
    }
    Ok(())
}

fn validate_message_body(
    raw: &str,
    max_bytes: usize,
    field: &'static str,
) -> Result<(), ProtocolError> {
    if raw.trim().is_empty() {
        return Err(ProtocolError::InvalidField { field, reason: "cannot be empty" });
    }
    let max_bytes = max_bytes.clamp(1, MAX_MESSAGE_BYTES);
    if raw.len() > max_bytes {
        return Err(ProtocolError::InvalidField {
            field,
            reason: "message body exceeds size limit",
        });
    }
    Ok(())
}

fn validate_host_pattern(raw: &str) -> Result<(), ProtocolError> {
    let trimmed = raw.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return Err(ProtocolError::InvalidField {
            field: "egress_allowlist",
            reason: "host pattern cannot be empty",
        });
    }
    let stripped = trimmed.strip_prefix("*.").unwrap_or(trimmed.as_str());
    if stripped.is_empty()
        || !stripped.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '.')
    {
        return Err(ProtocolError::InvalidField {
            field: "egress_allowlist",
            reason: "host pattern contains unsupported characters",
        });
    }
    if stripped.starts_with('.') || stripped.ends_with('.') || stripped.contains("..") {
        return Err(ProtocolError::InvalidField {
            field: "egress_allowlist",
            reason: "host pattern is malformed",
        });
    }
    Ok(())
}

fn validate_optional_field(
    raw: Option<&str>,
    field: &'static str,
    max_bytes: usize,
) -> Result<(), ProtocolError> {
    let Some(value) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    if value.len() > max_bytes {
        return Err(ProtocolError::InvalidField { field, reason: "value exceeds size limit" });
    }
    Ok(())
}

fn validate_attachments(attachments: &[AttachmentRef]) -> Result<(), ProtocolError> {
    if attachments.len() > MAX_ATTACHMENTS_PER_MESSAGE {
        return Err(ProtocolError::InvalidField {
            field: "attachments",
            reason: "message exceeds attachment count limit",
        });
    }
    for attachment in attachments {
        attachment.validate()?;
    }
    Ok(())
}

fn validate_json_bytes(
    raw: &[u8],
    field: &'static str,
    max_bytes: usize,
) -> Result<(), ProtocolError> {
    if raw.is_empty() {
        return Err(ProtocolError::InvalidField { field, reason: "cannot be empty" });
    }
    if raw.len() > max_bytes {
        return Err(ProtocolError::InvalidField { field, reason: "value exceeds size limit" });
    }
    serde_json::from_slice::<Value>(raw)
        .map_err(|_| ProtocolError::InvalidField { field, reason: "value is not valid JSON" })?;
    Ok(())
}

fn validate_permission_labels(values: &[String], field: &'static str) -> Result<(), ProtocolError> {
    for value in values {
        validate_non_empty_identifier(value.as_str(), field, MAX_PERMISSION_LABEL_BYTES)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        AttachmentKind, AttachmentRef, ConnectorAvailability, ConnectorInstanceSpec, ConnectorKind,
        InboundMessageEvent, OutboundA2uiUpdate, OutboundMessageRequest, ProtocolError,
    };

    #[test]
    fn connector_spec_validation_rejects_malformed_allowlist() {
        let spec = ConnectorInstanceSpec {
            connector_id: "echo:default".to_owned(),
            kind: ConnectorKind::Echo,
            principal: "channel:echo:default".to_owned(),
            auth_profile_ref: None,
            token_vault_ref: None,
            egress_allowlist: vec!["bad host".to_owned()],
            enabled: true,
        };
        assert_eq!(
            spec.validate(),
            Err(ProtocolError::InvalidField {
                field: "egress_allowlist",
                reason: "host pattern contains unsupported characters",
            })
        );
    }

    #[test]
    fn connector_kind_default_availability_matches_discord_first_runtime_scope() {
        assert_eq!(ConnectorKind::Discord.default_availability(), ConnectorAvailability::Supported);
        assert_eq!(
            ConnectorKind::Echo.default_availability(),
            ConnectorAvailability::InternalTestOnly
        );
        assert_eq!(ConnectorKind::Slack.default_availability(), ConnectorAvailability::Deferred);
        assert_eq!(ConnectorKind::Telegram.default_availability(), ConnectorAvailability::Deferred);
    }

    #[test]
    fn inbound_validation_rejects_empty_payload() {
        let event = InboundMessageEvent {
            envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            thread_id: None,
            sender_id: "sender".to_owned(),
            sender_display: None,
            body: "   ".to_owned(),
            adapter_message_id: None,
            adapter_thread_id: None,
            received_at_unix_ms: 1,
            is_direct_message: true,
            requested_broadcast: false,
            attachments: Vec::new(),
        };
        assert_eq!(
            event.validate(1024),
            Err(ProtocolError::InvalidField { field: "body", reason: "cannot be empty" })
        );
    }

    #[test]
    fn outbound_validation_requires_timeout_and_payload_limit() {
        let request = OutboundMessageRequest {
            envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV:0".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: "ok".to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            structured_json: None,
            a2ui_update: None,
            timeout_ms: 0,
            max_payload_bytes: 0,
        };
        assert_eq!(
            request.validate(1024),
            Err(ProtocolError::InvalidField {
                field: "timeout_ms",
                reason: "must be greater than zero"
            })
        );
    }

    #[test]
    fn inbound_validation_rejects_excessive_attachment_count() {
        let mut event = InboundMessageEvent {
            envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            thread_id: None,
            sender_id: "sender".to_owned(),
            sender_display: None,
            body: "hello".to_owned(),
            adapter_message_id: None,
            adapter_thread_id: None,
            received_at_unix_ms: 1,
            is_direct_message: true,
            requested_broadcast: false,
            attachments: Vec::new(),
        };
        event.attachments = (0..33)
            .map(|index| AttachmentRef {
                kind: AttachmentKind::Image,
                url: Some(format!("https://cdn.example.test/{index}.png")),
                filename: Some(format!("{index}.png")),
                content_type: Some("image/png".to_owned()),
                size_bytes: Some(1_024),
                ..AttachmentRef::default()
            })
            .collect();
        assert_eq!(
            event.validate(1024),
            Err(ProtocolError::InvalidField {
                field: "attachments",
                reason: "message exceeds attachment count limit",
            })
        );
    }

    #[test]
    fn outbound_validation_rejects_invalid_structured_json() {
        let request = OutboundMessageRequest {
            envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV:0".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: "ok".to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            structured_json: Some(br#"{"missing":"brace""#.to_vec()),
            a2ui_update: None,
            timeout_ms: 1_000,
            max_payload_bytes: 8_192,
        };
        assert_eq!(
            request.validate(1024),
            Err(ProtocolError::InvalidField {
                field: "structured_json",
                reason: "value is not valid JSON",
            })
        );
    }

    #[test]
    fn outbound_validation_rejects_structured_json_over_payload_limit() {
        let request = OutboundMessageRequest {
            envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV:0".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: "ok".to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            structured_json: Some(br#"{"k":"0123456789"}"#.to_vec()),
            a2ui_update: None,
            timeout_ms: 1_000,
            max_payload_bytes: 8,
        };
        assert_eq!(
            request.validate(1024),
            Err(ProtocolError::InvalidField {
                field: "structured_json",
                reason: "value exceeds size limit",
            })
        );
    }

    #[test]
    fn outbound_validation_rejects_a2ui_patch_over_payload_limit() {
        let request = OutboundMessageRequest {
            envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV:0".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: "ok".to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            structured_json: None,
            a2ui_update: Some(OutboundA2uiUpdate {
                surface: "chat".to_owned(),
                patch_json: br#"{"op":"replace","path":"/title","value":"hello"}"#.to_vec(),
            }),
            timeout_ms: 1_000,
            max_payload_bytes: 16,
        };
        assert_eq!(
            request.validate(1024),
            Err(ProtocolError::InvalidField {
                field: "a2ui_update.patch_json",
                reason: "value exceeds size limit",
            })
        );
    }

    #[test]
    fn outbound_validation_rejects_invalid_a2ui_patch_json() {
        let request = OutboundMessageRequest {
            envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV:0".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: "ok".to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            structured_json: None,
            a2ui_update: Some(OutboundA2uiUpdate {
                surface: "chat".to_owned(),
                patch_json: br#"{"oops":"invalid""#.to_vec(),
            }),
            timeout_ms: 1_000,
            max_payload_bytes: 8_192,
        };
        assert_eq!(
            request.validate(1024),
            Err(ProtocolError::InvalidField {
                field: "a2ui_update.patch_json",
                reason: "value is not valid JSON",
            })
        );
    }
}
