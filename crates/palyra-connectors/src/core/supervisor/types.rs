//! Supervisor contracts: configuration, outcome types, errors, and the
//! [`ConnectorRouter`]/[`ConnectorAdapter`] traits providers implement.
//!
//! These traits are the channel-provider boundary — the supervisor core knows
//! providers only through them plus the neutral capability registry in
//! `crate::providers`.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::providers::{provider_availability, provider_capabilities};

use super::super::storage::ConnectorStoreError;
use super::super::{
    protocol::{
        ConnectorAvailability, ConnectorCapabilitySet, ConnectorKind,
        ConnectorMessageDeleteRequest, ConnectorMessageEditRequest, ConnectorMessageMutationResult,
        ConnectorMessageReactionRequest, ConnectorMessageReadRequest, ConnectorMessageReadResult,
        ConnectorMessageSearchRequest, ConnectorMessageSearchResult, ConnectorReadiness,
        DeliveryOutcome, InboundMessageEvent, OutboundMessageRequest, RetryClass,
        RouteInboundResult,
    },
    storage::ConnectorInstanceRecord,
};

/// Tunables governing dedupe, payload limits, retry backoff, and drain sizes.
#[derive(Debug, Clone)]
pub struct ConnectorSupervisorConfig {
    /// How long an inbound envelope id is remembered for duplicate detection.
    pub inbound_dedupe_window_ms: i64,
    pub max_inbound_body_bytes: usize,
    pub max_outbound_body_bytes: usize,
    /// Delivery attempts before an outbox entry is dead-lettered.
    pub max_retry_attempts: u32,
    /// Floor applied to every retry delay, including adapter-requested ones.
    pub min_retry_delay_ms: u64,
    /// First-retry delay; doubles per attempt up to `max_retry_delay_ms`.
    pub base_retry_delay_ms: u64,
    /// Ceiling applied to every retry delay, including adapter-requested ones.
    pub max_retry_delay_ms: u64,
    /// Re-check delay for outbox entries whose connector is disabled.
    pub disabled_poll_delay_ms: u64,
    /// Drain batch size used inline after ingesting an inbound event.
    pub immediate_drain_batch_size: usize,
    /// Drain batch size intended for periodic background drains.
    pub background_drain_batch_size: usize,
}

impl Default for ConnectorSupervisorConfig {
    fn default() -> Self {
        Self {
            inbound_dedupe_window_ms: 7 * 24 * 60 * 60 * 1_000,
            max_inbound_body_bytes: 64 * 1024,
            max_outbound_body_bytes: 64 * 1024,
            max_retry_attempts: 5,
            min_retry_delay_ms: 250,
            base_retry_delay_ms: 1_000,
            max_retry_delay_ms: 60_000,
            disabled_poll_delay_ms: 30_000,
            immediate_drain_batch_size: 64,
            background_drain_batch_size: 128,
        }
    }
}

/// Terminal disposition of one outbox entry dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DispatchResult {
    Delivered,
    Retried,
    DeadLettered,
}

/// Counters summarizing one outbox drain pass.
#[derive(Debug, Clone, Default, Serialize)]
pub struct DrainOutcome {
    pub processed: usize,
    pub delivered: usize,
    pub retried: usize,
    pub dead_lettered: usize,
}

/// Result of ingesting one inbound event, including routing and any
/// immediately attempted deliveries.
#[derive(Debug, Clone, Serialize)]
pub struct InboundIngestOutcome {
    pub accepted: bool,
    /// True when the envelope was dropped by the dedupe window; `accepted`
    /// is also true in that case because the original ingest succeeded.
    pub duplicate: bool,
    pub queued_for_retry: bool,
    pub decision_reason: String,
    pub route_key: Option<String>,
    pub enqueued_outbound: usize,
    /// Messages delivered by the inline drain that follows ingestion.
    pub immediate_delivery: usize,
}

/// Routing failure reported by a [`ConnectorRouter`] implementation.
#[derive(Debug, Error)]
pub enum ConnectorRouterError {
    #[error("{0}")]
    Message(String),
}

/// Provider-side failure reported by a [`ConnectorAdapter`] implementation.
#[derive(Debug, Error)]
pub enum ConnectorAdapterError {
    #[error("{0}")]
    Backend(String),
}

/// Operation surfaces an adapter SDK implementation covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorAdapterSdkOperation {
    Inbound,
    Outbound,
    Binding,
    RateLimit,
    ErrorMapping,
}

impl ConnectorAdapterSdkOperation {
    /// Returns the stable snake_case label matching the serde encoding.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
            Self::Binding => "binding",
            Self::RateLimit => "rate_limit",
            Self::ErrorMapping => "error_mapping",
        }
    }
}

/// Self-description of an adapter's SDK surface and contract versions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorAdapterSdkDescriptor {
    pub schema_version: u32,
    pub kind: ConnectorKind,
    pub operations: Vec<ConnectorAdapterSdkOperation>,
    pub binding_contract: String,
    pub delivery_receipt_contract: String,
    pub error_contract: String,
}

impl ConnectorAdapterSdkDescriptor {
    /// Returns the default full-surface descriptor for `kind`.
    #[must_use]
    pub fn for_kind(kind: ConnectorKind) -> Self {
        Self {
            schema_version: 1,
            kind,
            operations: vec![
                ConnectorAdapterSdkOperation::Inbound,
                ConnectorAdapterSdkOperation::Outbound,
                ConnectorAdapterSdkOperation::Binding,
                ConnectorAdapterSdkOperation::RateLimit,
                ConnectorAdapterSdkOperation::ErrorMapping,
            ],
            binding_contract: "conversation_binding_record".to_owned(),
            delivery_receipt_contract: "ack_nack_unknown_v1".to_owned(),
            error_contract: "connector_adapter_error_v1".to_owned(),
        }
    }
}

/// Routes accepted inbound events to the gateway on behalf of a principal.
#[async_trait]
pub trait ConnectorRouter: Send + Sync {
    /// Routes one validated, deduplicated inbound event.
    ///
    /// # Errors
    /// Returns [`ConnectorRouterError`] when routing fails outright; soft
    /// rejections are expressed through `RouteInboundResult::accepted`.
    async fn route_inbound(
        &self,
        principal: &str,
        event: &InboundMessageEvent,
    ) -> Result<RouteInboundResult, ConnectorRouterError>;
}

/// Provider integration surface invoked by the supervisor.
///
/// Only [`kind`](Self::kind) and [`send_outbound`](Self::send_outbound) are
/// mandatory; every other method has a neutral default (registry-backed
/// capabilities, no inbound polling, unsupported message operations) so
/// outbound-only adapters stay minimal.
#[async_trait]
pub trait ConnectorAdapter: Send + Sync {
    /// Provider kind this adapter serves; used as the registry key, so it
    /// must be constant for the adapter's lifetime.
    fn kind(&self) -> ConnectorKind;

    /// Describes the adapter's SDK contract surface.
    fn sdk_descriptor(&self) -> ConnectorAdapterSdkDescriptor {
        ConnectorAdapterSdkDescriptor::for_kind(self.kind())
    }

    /// Product availability tier; defaults to the provider registry value.
    fn availability(&self) -> ConnectorAvailability {
        provider_availability(self.kind())
    }

    /// Capability set; defaults to the provider registry value.
    fn capabilities(&self) -> ConnectorCapabilitySet {
        provider_capabilities(self.kind())
    }

    /// Splits one outbound request into provider-sized chunks before
    /// enqueueing; each returned request must keep a unique envelope id.
    ///
    /// # Errors
    /// Returns [`ConnectorAdapterError`] when the payload cannot be split.
    fn split_outbound(
        &self,
        _instance: &ConnectorInstanceRecord,
        request: &OutboundMessageRequest,
    ) -> Result<Vec<OutboundMessageRequest>, ConnectorAdapterError> {
        Ok(vec![request.clone()])
    }

    /// Optional provider-specific runtime state merged into status snapshots.
    fn runtime_snapshot(&self, _instance: &ConnectorInstanceRecord) -> Option<Value> {
        None
    }

    /// Stops any provider runtime for the instance; called before the
    /// instance is disabled or removed.
    ///
    /// # Errors
    /// Returns [`ConnectorAdapterError`] when shutdown fails.
    fn stop_runtime(&self, _connector_id: &str) -> Result<(), ConnectorAdapterError> {
        Ok(())
    }

    /// Pulls up to `limit` pending inbound events for poll-based providers;
    /// push-based providers keep the empty default.
    ///
    /// # Errors
    /// Returns [`ConnectorAdapterError`] when the provider poll fails.
    async fn poll_inbound(
        &self,
        _instance: &ConnectorInstanceRecord,
        _limit: usize,
    ) -> Result<Vec<InboundMessageEvent>, ConnectorAdapterError> {
        Ok(Vec::new())
    }

    /// Delivers one outbound request, classifying the result as delivered,
    /// retryable, or permanently failed via [`DeliveryOutcome`].
    ///
    /// # Errors
    /// Returns [`ConnectorAdapterError`] for transport-level failures; the
    /// supervisor treats those as transient and schedules a retry.
    async fn send_outbound(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &OutboundMessageRequest,
    ) -> Result<DeliveryOutcome, ConnectorAdapterError>;

    /// Reads messages for providers that support it; default is unsupported.
    ///
    /// # Errors
    /// Returns [`ConnectorAdapterError`] when unsupported or the read fails.
    async fn read_messages(
        &self,
        _instance: &ConnectorInstanceRecord,
        _request: &ConnectorMessageReadRequest,
    ) -> Result<ConnectorMessageReadResult, ConnectorAdapterError> {
        Err(ConnectorAdapterError::Backend(format!(
            "{} connector does not support message read",
            self.kind().as_str()
        )))
    }

    /// Searches messages for providers that support it; default is unsupported.
    ///
    /// # Errors
    /// Returns [`ConnectorAdapterError`] when unsupported or the search fails.
    async fn search_messages(
        &self,
        _instance: &ConnectorInstanceRecord,
        _request: &ConnectorMessageSearchRequest,
    ) -> Result<ConnectorMessageSearchResult, ConnectorAdapterError> {
        Err(ConnectorAdapterError::Backend(format!(
            "{} connector does not support message search",
            self.kind().as_str()
        )))
    }

    /// Edits a message for providers that support it; default is unsupported.
    ///
    /// # Errors
    /// Returns [`ConnectorAdapterError`] when unsupported or the edit fails.
    async fn edit_message(
        &self,
        _instance: &ConnectorInstanceRecord,
        _request: &ConnectorMessageEditRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        Err(ConnectorAdapterError::Backend(format!(
            "{} connector does not support message edit",
            self.kind().as_str()
        )))
    }

    /// Deletes a message for providers that support it; default is unsupported.
    ///
    /// # Errors
    /// Returns [`ConnectorAdapterError`] when unsupported or the delete fails.
    async fn delete_message(
        &self,
        _instance: &ConnectorInstanceRecord,
        _request: &ConnectorMessageDeleteRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        Err(ConnectorAdapterError::Backend(format!(
            "{} connector does not support message delete",
            self.kind().as_str()
        )))
    }

    /// Adds a reaction for providers that support it; default is unsupported.
    ///
    /// # Errors
    /// Returns [`ConnectorAdapterError`] when unsupported or the call fails.
    async fn add_reaction(
        &self,
        _instance: &ConnectorInstanceRecord,
        _request: &ConnectorMessageReactionRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        Err(ConnectorAdapterError::Backend(format!(
            "{} connector does not support reaction add",
            self.kind().as_str()
        )))
    }

    /// Removes a reaction for providers that support it; default is unsupported.
    ///
    /// # Errors
    /// Returns [`ConnectorAdapterError`] when unsupported or the call fails.
    async fn remove_reaction(
        &self,
        _instance: &ConnectorInstanceRecord,
        _request: &ConnectorMessageReactionRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        Err(ConnectorAdapterError::Backend(format!(
            "{} connector does not support reaction removal",
            self.kind().as_str()
        )))
    }
}

/// Failure modes of supervisor operations.
#[derive(Debug, Error)]
pub enum ConnectorSupervisorError {
    #[error(transparent)]
    Store(#[from] ConnectorStoreError),
    #[error("connector protocol validation failed: {0}")]
    Validation(String),
    #[error("connector instance not found: {0}")]
    NotFound(String),
    #[error("connector adapter missing for kind '{0}'")]
    MissingAdapter(ConnectorKind),
    #[error("router failed: {0}")]
    Router(String),
    #[error("adapter failed: {0}")]
    Adapter(String),
    #[error("failed to read system clock: {0}")]
    Clock(String),
}

/// Maps a permanent-failure reason onto a readiness state.
///
/// Adapter errors arrive as free-form strings, so this is a best-effort
/// substring heuristic for operator triage only — never use it for policy or
/// security decisions.
pub(super) fn classify_permanent_failure(reason: &str) -> ConnectorReadiness {
    let normalized = reason.trim().to_ascii_lowercase();
    if normalized.contains("credential missing") || normalized.contains("missing credential") {
        return ConnectorReadiness::MissingCredential;
    }
    if normalized.contains("auth")
        || normalized.contains("token")
        || normalized.contains("unauthorized")
        || normalized.contains("forbidden")
    {
        return ConnectorReadiness::AuthFailed;
    }
    ConnectorReadiness::Misconfigured
}

/// Label used in retry event details.
///
/// INTENTIONAL: this is the Debug (CamelCase) rendering, not
/// `RetryClass::as_str()` snake_case — stored events and their consumers
/// already rely on this form, so do not "unify" it.
pub(super) fn retry_class_label(class: RetryClass) -> String {
    format!("{class:?}")
}
