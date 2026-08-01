//! Deny-by-default host policy for MCP callbacks and restart-safe audit ports.

use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

use super::{
    McpCallbackBinding, McpCallbackResponsePayload, McpCatalogAuthority, McpElicitationRequest,
    McpHostCallbackError, McpHostCallbackPort, McpSamplingRequest, McpServerCallbackRequest,
    McpServerCallbackType, McpServerRecordV2,
};

const MAX_REASON_CODE_BYTES: usize = 192;
const MAX_IDENTIFIER_BYTES: usize = 256;
const MAX_SAFE_MESSAGE_BYTES: usize = 8 * 1024;

/// Durable MCP host-policy decision class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpPolicyAuditKind {
    /// OAuth credential refresh.
    OAuthRefresh,
    /// User elicitation callback.
    Elicitation,
    /// Bounded model sampling callback.
    Sampling,
    /// Host roots callback.
    Roots,
}

impl McpPolicyAuditKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::OAuthRefresh => "oauth_refresh",
            Self::Elicitation => "elicitation",
            Self::Sampling => "sampling",
            Self::Roots => "roots",
        }
    }
}

/// Durable MCP host-policy outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpPolicyAuditOutcome {
    /// Policy reserved and authorized the operation.
    Allowed,
    /// Policy denied the operation before external execution.
    Denied,
    /// Credential refresh returned a validated opaque handle.
    Refreshed,
    /// A host dependency failed without granting authority.
    Failed,
}

impl McpPolicyAuditOutcome {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::Denied => "denied",
            Self::Refreshed => "refreshed",
            Self::Failed => "failed",
        }
    }
}

/// Metadata-only, redaction-safe host-policy evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpPolicyAuditEventV1 {
    /// Deterministic operation identity used for replay suppression.
    pub event_id: String,
    /// Durable MCP server identity.
    pub server_id: String,
    /// Runtime generation that requested the operation.
    pub runtime_generation: u64,
    /// Catalog epoch, or zero for pre-catalog OAuth work.
    pub catalog_epoch: u64,
    /// Hash of principal, session, and origin or credential scope binding.
    pub binding_sha256: String,
    /// Policy operation class.
    pub kind: McpPolicyAuditKind,
    /// Host policy outcome.
    pub outcome: McpPolicyAuditOutcome,
    /// Tokens reserved by an allowed sampling request.
    pub reserved_output_tokens: u64,
    /// Stable redaction-safe decision reason.
    pub reason_code: String,
    /// Hash of the bounded request projection.
    pub request_sha256: String,
    /// Optional host evidence digest.
    pub evidence_sha256: Option<String>,
    /// Host decision time.
    pub occurred_at_unix_ms: i64,
}

impl McpPolicyAuditEventV1 {
    /// Validates durable bounds and outcome-specific invariants.
    ///
    /// # Errors
    /// Returns [`McpPolicyAuditStoreError::InvalidEvent`] for malformed evidence.
    pub fn validate(&self) -> Result<(), McpPolicyAuditStoreError> {
        let reservation_valid = match (self.kind, self.outcome) {
            (McpPolicyAuditKind::Sampling, McpPolicyAuditOutcome::Allowed) => {
                self.reserved_output_tokens > 0
            }
            _ => self.reserved_output_tokens == 0,
        };
        if !valid_identifier(&self.event_id)
            || !valid_identifier(&self.server_id)
            || self.runtime_generation == 0
            || !valid_sha256(&self.binding_sha256)
            || !valid_reason_code(&self.reason_code)
            || !valid_sha256(&self.request_sha256)
            || self.evidence_sha256.as_deref().is_some_and(|value| !valid_sha256(value))
            || self.occurred_at_unix_ms <= 0
            || !reservation_valid
        {
            return Err(McpPolicyAuditStoreError::InvalidEvent);
        }
        Ok(())
    }
}

/// Idempotent durable append result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpPolicyAuditAppendOutcome {
    /// New evidence was appended.
    Appended,
    /// Byte-equivalent evidence already existed.
    Existing,
}

/// Restart-restored sampling reservations in one policy window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct McpSamplingUsage {
    /// Allowed reservations in the window.
    pub requests: u64,
    /// Maximum output tokens reserved in the window.
    pub reserved_output_tokens: u64,
}

/// Durable MCP host-policy evidence boundary.
#[async_trait]
pub trait McpPolicyAuditStore: Send + Sync {
    /// Appends one idempotent metadata-only decision.
    async fn append_policy_event(
        &self,
        event: &McpPolicyAuditEventV1,
    ) -> Result<McpPolicyAuditAppendOutcome, McpPolicyAuditStoreError>;

    /// Loads sampling reservations for restart-safe rate enforcement.
    async fn sampling_usage(
        &self,
        server_id: &str,
        binding_sha256: &str,
        since_unix_ms: i64,
    ) -> Result<McpSamplingUsage, McpPolicyAuditStoreError>;
}

/// Durable audit storage failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum McpPolicyAuditStoreError {
    /// Event failed its bounded metadata contract.
    #[error("invalid mcp policy audit event")]
    InvalidEvent,
    /// Existing event identity has different evidence.
    #[error("mcp policy audit idempotency conflict")]
    IdempotencyConflict,
    /// Stored audit data is corrupt.
    #[error("corrupt mcp policy audit storage: {reason_code}")]
    Corrupt {
        /// Stable storage reason.
        reason_code: String,
    },
    /// Audit storage is unavailable.
    #[error("mcp policy audit storage unavailable: {reason_code}")]
    Unavailable {
        /// Stable storage reason.
        reason_code: String,
    },
}

/// Deny-by-default host policy and rate limits for MCP callbacks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpHostCallbackPolicy {
    /// Whether elicitation is enabled at all.
    pub elicitation_enabled: bool,
    /// Origins allowed to elicit user input.
    pub allowed_elicitation_origins: BTreeSet<String>,
    /// Host-selected sampling model; `None` disables sampling.
    pub sampling_model_id: Option<String>,
    /// Origins allowed to request bounded sampling.
    pub allowed_sampling_origins: BTreeSet<String>,
    /// Explicit tool names sampling may receive.
    pub allowed_sampling_tools: BTreeSet<String>,
    /// Maximum output tokens for one sampling request.
    pub max_sampling_output_tokens_per_request: u64,
    /// Rolling durable rate-limit window.
    pub sampling_window: Duration,
    /// Maximum allowed sampling reservations per window.
    pub max_sampling_requests_per_window: u64,
    /// Maximum reserved output tokens per window.
    pub max_sampling_output_tokens_per_window: u64,
}

impl Default for McpHostCallbackPolicy {
    fn default() -> Self {
        Self {
            elicitation_enabled: false,
            allowed_elicitation_origins: BTreeSet::new(),
            sampling_model_id: None,
            allowed_sampling_origins: BTreeSet::new(),
            allowed_sampling_tools: BTreeSet::new(),
            max_sampling_output_tokens_per_request: 0,
            sampling_window: Duration::from_secs(60),
            max_sampling_requests_per_window: 0,
            max_sampling_output_tokens_per_window: 0,
        }
    }
}

impl McpHostCallbackPolicy {
    /// Validates enabled policy surfaces and hard limits.
    ///
    /// # Errors
    /// Returns [`McpHostPolicyBuildError`] for unsafe or ambiguous policy.
    pub fn validate(&self) -> Result<(), McpHostPolicyBuildError> {
        if self.sampling_window.is_zero()
            || self
                .allowed_elicitation_origins
                .iter()
                .chain(&self.allowed_sampling_origins)
                .chain(&self.allowed_sampling_tools)
                .any(|value| !valid_identifier(value))
        {
            return Err(McpHostPolicyBuildError::InvalidPolicy);
        }
        let sampling_disabled = self.sampling_model_id.is_none()
            && self.max_sampling_output_tokens_per_request == 0
            && self.max_sampling_requests_per_window == 0
            && self.max_sampling_output_tokens_per_window == 0
            && self.allowed_sampling_origins.is_empty()
            && self.allowed_sampling_tools.is_empty();
        let sampling_enabled = self.sampling_model_id.as_deref().is_some_and(valid_identifier)
            && self.max_sampling_output_tokens_per_request > 0
            && self.max_sampling_requests_per_window > 0
            && self.max_sampling_output_tokens_per_window
                >= self.max_sampling_output_tokens_per_request
            && !self.allowed_sampling_origins.is_empty();
        if !sampling_disabled && !sampling_enabled {
            return Err(McpHostPolicyBuildError::InvalidPolicy);
        }
        if self.elicitation_enabled == self.allowed_elicitation_origins.is_empty() {
            return Err(McpHostPolicyBuildError::InvalidPolicy);
        }
        Ok(())
    }
}

/// Host execution request after elicitation policy authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpAuthorizedElicitationRequest {
    /// Idempotency key derived from server generation and callback identity.
    pub idempotency_key: String,
    /// Host-pinned callback binding.
    pub binding: McpCallbackBinding,
    /// Bounded elicitation payload.
    pub request: McpElicitationRequest,
}

/// Host execution request after sampling policy and budget authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpAuthorizedSamplingRequest {
    /// Idempotency key derived from server generation and callback identity.
    pub idempotency_key: String,
    /// Host-selected model, never server-selected.
    pub model_id: String,
    /// Host-pinned callback binding.
    pub binding: McpCallbackBinding,
    /// Redacted sampling input.
    pub input_json: Value,
    /// Explicit tools intersected with the host allowlist.
    pub allowed_tools: Vec<String>,
    /// Reserved output-token ceiling.
    pub max_output_tokens: u64,
}

/// Authorized elicitation delivery boundary.
#[async_trait]
pub trait McpElicitationExecutionPort: Send + Sync {
    /// Delivers one idempotent, host-bound elicitation request.
    async fn elicit(
        &self,
        request: &McpAuthorizedElicitationRequest,
    ) -> Result<Value, McpHostExecutionError>;
}

/// Authorized sampling execution boundary.
#[async_trait]
pub trait McpSamplingExecutionPort: Send + Sync {
    /// Executes one idempotent, host-budgeted model call.
    async fn sample(
        &self,
        request: &McpAuthorizedSamplingRequest,
    ) -> Result<Value, McpHostExecutionError>;
}

/// Host callback execution failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("mcp host callback execution failed: {reason_code}")]
pub struct McpHostExecutionError {
    /// Stable host-owned failure reason.
    pub reason_code: String,
}

/// Production callback policy service bound to one server and host session.
pub struct McpHostPolicyCallbackService {
    server_id: String,
    fixed_binding: Option<McpCallbackBinding>,
    authority: Arc<McpCatalogAuthority>,
    policy: McpHostCallbackPolicy,
    audit: Arc<dyn McpPolicyAuditStore>,
    elicitation: Option<Arc<dyn McpElicitationExecutionPort>>,
    sampling: Option<Arc<dyn McpSamplingExecutionPort>>,
    sampling_gate: AsyncMutex<()>,
}

struct McpPolicyEventDetails<'a> {
    kind: McpPolicyAuditKind,
    outcome: McpPolicyAuditOutcome,
    reserved_output_tokens: u64,
    reason_code: &'a str,
    evidence_sha256: Option<String>,
    occurred_at_unix_ms: i64,
}

impl std::fmt::Debug for McpHostPolicyCallbackService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpHostPolicyCallbackService")
            .field("server_id", &self.server_id)
            .field("elicitation_enabled", &self.policy.elicitation_enabled)
            .field("sampling_enabled", &self.policy.sampling_model_id.is_some())
            .finish_non_exhaustive()
    }
}

impl McpHostPolicyCallbackService {
    /// Creates a callback policy service with explicit host execution ports.
    ///
    /// # Errors
    /// Returns [`McpHostPolicyBuildError`] when policy, binding, or port
    /// availability would create an ambiguous or fail-open configuration.
    pub fn new(
        server_id: String,
        binding: McpCallbackBinding,
        authority: Arc<McpCatalogAuthority>,
        policy: McpHostCallbackPolicy,
        audit: Arc<dyn McpPolicyAuditStore>,
        elicitation: Option<Arc<dyn McpElicitationExecutionPort>>,
        sampling: Option<Arc<dyn McpSamplingExecutionPort>>,
    ) -> Result<Self, McpHostPolicyBuildError> {
        policy.validate()?;
        if !valid_identifier(&server_id)
            || !valid_identifier(&binding.principal_id)
            || !valid_identifier(&binding.session_id)
            || !valid_identifier(&binding.origin)
            || (policy.elicitation_enabled && elicitation.is_none())
            || (policy.sampling_model_id.is_some() && sampling.is_none())
        {
            return Err(McpHostPolicyBuildError::InvalidPolicy);
        }
        Self::build(server_id, Some(binding), authority, policy, audit, elicitation, sampling)
    }

    /// Creates a policy service whose binding is supplied by the sole
    /// host-admitted in-flight request in the actor.
    pub fn new_session_bound(
        server_id: String,
        authority: Arc<McpCatalogAuthority>,
        policy: McpHostCallbackPolicy,
        audit: Arc<dyn McpPolicyAuditStore>,
        elicitation: Option<Arc<dyn McpElicitationExecutionPort>>,
        sampling: Option<Arc<dyn McpSamplingExecutionPort>>,
    ) -> Result<Self, McpHostPolicyBuildError> {
        Self::build(server_id, None, authority, policy, audit, elicitation, sampling)
    }

    fn build(
        server_id: String,
        fixed_binding: Option<McpCallbackBinding>,
        authority: Arc<McpCatalogAuthority>,
        policy: McpHostCallbackPolicy,
        audit: Arc<dyn McpPolicyAuditStore>,
        elicitation: Option<Arc<dyn McpElicitationExecutionPort>>,
        sampling: Option<Arc<dyn McpSamplingExecutionPort>>,
    ) -> Result<Self, McpHostPolicyBuildError> {
        policy.validate()?;
        if !valid_identifier(&server_id)
            || fixed_binding.as_ref().is_some_and(|binding| !binding.is_valid())
            || (policy.elicitation_enabled && elicitation.is_none())
            || (policy.sampling_model_id.is_some() && sampling.is_none())
        {
            return Err(McpHostPolicyBuildError::InvalidPolicy);
        }
        Ok(Self {
            server_id,
            fixed_binding,
            authority,
            policy,
            audit,
            elicitation,
            sampling,
            sampling_gate: AsyncMutex::new(()),
        })
    }

    async fn handle_elicitation(
        &self,
        callback: &McpServerCallbackRequest,
        request: &McpElicitationRequest,
    ) -> Result<McpCallbackResponsePayload, McpHostCallbackError> {
        let origin_allowed = self.fixed_binding.is_none()
            || self.policy.allowed_elicitation_origins.contains(&callback.origin);
        if !self.policy.elicitation_enabled || !origin_allowed {
            self.audit_denied(
                callback,
                McpPolicyAuditKind::Elicitation,
                "mcp.runtime.elicitation.denied_by_default",
            )
            .await?;
            return Err(denied(
                "mcp.runtime.elicitation.denied_by_default",
                "elicitation is not authorized",
            ));
        }
        let Some(port) = self.elicitation.as_ref() else {
            return Err(unavailable("mcp.runtime.elicitation.port_unavailable"));
        };
        let event = self.policy_event(
            callback,
            McpPolicyAuditKind::Elicitation,
            McpPolicyAuditOutcome::Allowed,
            0,
            "mcp.runtime.elicitation.authorized",
            None,
        )?;
        if self.append_authorization(&event).await? == McpPolicyAuditAppendOutcome::Existing {
            return Err(denied(
                "mcp.runtime.elicitation.callback_replay",
                "elicitation callback was already processed",
            ));
        }
        let execution = McpAuthorizedElicitationRequest {
            idempotency_key: event.event_id,
            binding: callback_binding(callback),
            request: request.clone(),
        };
        port.elicit(&execution)
            .await
            .map(McpCallbackResponsePayload::Success)
            .map_err(|error| unavailable(error.reason_code))
    }

    async fn handle_sampling(
        &self,
        callback: &McpServerCallbackRequest,
        request: &McpSamplingRequest,
    ) -> Result<McpCallbackResponsePayload, McpHostCallbackError> {
        let Some(model_id) = self.policy.sampling_model_id.as_ref() else {
            self.audit_denied(
                callback,
                McpPolicyAuditKind::Sampling,
                "mcp.runtime.sampling.denied_by_default",
            )
            .await?;
            return Err(denied(
                "mcp.runtime.sampling.denied_by_default",
                "sampling is not authorized",
            ));
        };
        let tools_allowed = request
            .requested_tools
            .iter()
            .all(|tool| self.policy.allowed_sampling_tools.contains(tool));
        let origin_allowed = self.fixed_binding.is_none()
            || self.policy.allowed_sampling_origins.contains(&callback.origin);
        if !origin_allowed
            || request.max_output_tokens > self.policy.max_sampling_output_tokens_per_request
            || !tools_allowed
        {
            self.audit_denied(
                callback,
                McpPolicyAuditKind::Sampling,
                "mcp.runtime.sampling.policy_denied",
            )
            .await?;
            return Err(denied(
                "mcp.runtime.sampling.policy_denied",
                "sampling request exceeds host policy",
            ));
        }
        let Some(port) = self.sampling.as_ref() else {
            return Err(unavailable("mcp.runtime.sampling.port_unavailable"));
        };

        let _gate = self.sampling_gate.lock().await;
        let now = now_unix_ms().map_err(unavailable)?;
        let window_ms = i64::try_from(self.policy.sampling_window.as_millis())
            .map_err(|_| unavailable("mcp.runtime.sampling.window_overflow"))?;
        let binding_sha256 = callback_binding_sha256(callback);
        let usage = self
            .audit
            .sampling_usage(&self.server_id, &binding_sha256, now.saturating_sub(window_ms))
            .await
            .map_err(audit_unavailable)?;
        let next_requests = usage.requests.checked_add(1);
        let next_tokens = usage.reserved_output_tokens.checked_add(request.max_output_tokens);
        let rate_limit_exceeded = match (next_requests, next_tokens) {
            (Some(requests), Some(tokens)) => {
                requests > self.policy.max_sampling_requests_per_window
                    || tokens > self.policy.max_sampling_output_tokens_per_window
            }
            _ => true,
        };
        if rate_limit_exceeded {
            self.audit_denied_at(
                callback,
                McpPolicyAuditKind::Sampling,
                "mcp.runtime.sampling.rate_limited",
                now,
            )
            .await?;
            return Err(denied(
                "mcp.runtime.sampling.rate_limited",
                "sampling rate limit is exhausted",
            ));
        }
        let event = self.policy_event_at(
            callback,
            McpPolicyEventDetails {
                kind: McpPolicyAuditKind::Sampling,
                outcome: McpPolicyAuditOutcome::Allowed,
                reserved_output_tokens: request.max_output_tokens,
                reason_code: "mcp.runtime.sampling.authorized",
                evidence_sha256: None,
                occurred_at_unix_ms: now,
            },
        )?;
        if self.append_authorization(&event).await? == McpPolicyAuditAppendOutcome::Existing {
            return Err(denied(
                "mcp.runtime.sampling.callback_replay",
                "sampling callback was already processed",
            ));
        }
        drop(_gate);

        let execution = McpAuthorizedSamplingRequest {
            idempotency_key: event.event_id,
            model_id: model_id.clone(),
            binding: callback_binding(callback),
            input_json: request.input_json.clone(),
            allowed_tools: request.requested_tools.clone(),
            max_output_tokens: request.max_output_tokens,
        };
        port.sample(&execution)
            .await
            .map(McpCallbackResponsePayload::Success)
            .map_err(|error| unavailable(error.reason_code))
    }

    async fn audit_denied(
        &self,
        callback: &McpServerCallbackRequest,
        kind: McpPolicyAuditKind,
        reason_code: &str,
    ) -> Result<(), McpHostCallbackError> {
        let now = now_unix_ms().map_err(unavailable)?;
        self.audit_denied_at(callback, kind, reason_code, now).await
    }

    async fn audit_denied_at(
        &self,
        callback: &McpServerCallbackRequest,
        kind: McpPolicyAuditKind,
        reason_code: &str,
        now_unix_ms: i64,
    ) -> Result<(), McpHostCallbackError> {
        let event = self.policy_event_at(
            callback,
            McpPolicyEventDetails {
                kind,
                outcome: McpPolicyAuditOutcome::Denied,
                reserved_output_tokens: 0,
                reason_code,
                evidence_sha256: None,
                occurred_at_unix_ms: now_unix_ms,
            },
        )?;
        self.audit.append_policy_event(&event).await.map_err(audit_unavailable)?;
        Ok(())
    }

    async fn append_authorization(
        &self,
        event: &McpPolicyAuditEventV1,
    ) -> Result<McpPolicyAuditAppendOutcome, McpHostCallbackError> {
        self.audit.append_policy_event(event).await.map_err(audit_unavailable)
    }

    fn policy_event(
        &self,
        callback: &McpServerCallbackRequest,
        kind: McpPolicyAuditKind,
        outcome: McpPolicyAuditOutcome,
        reserved_output_tokens: u64,
        reason_code: &str,
        evidence_sha256: Option<String>,
    ) -> Result<McpPolicyAuditEventV1, McpHostCallbackError> {
        let now = now_unix_ms().map_err(unavailable)?;
        self.policy_event_at(
            callback,
            McpPolicyEventDetails {
                kind,
                outcome,
                reserved_output_tokens,
                reason_code,
                evidence_sha256,
                occurred_at_unix_ms: now,
            },
        )
    }

    fn policy_event_at(
        &self,
        callback: &McpServerCallbackRequest,
        details: McpPolicyEventDetails<'_>,
    ) -> Result<McpPolicyAuditEventV1, McpHostCallbackError> {
        let event = McpPolicyAuditEventV1 {
            event_id: format!(
                "{}:{}:{}:{}",
                details.kind.as_str(),
                self.server_id,
                callback.runtime_generation,
                callback.callback_id
            ),
            server_id: self.server_id.clone(),
            runtime_generation: callback.runtime_generation,
            catalog_epoch: callback.catalog_epoch,
            binding_sha256: callback_binding_sha256(callback),
            kind: details.kind,
            outcome: details.outcome,
            reserved_output_tokens: details.reserved_output_tokens,
            reason_code: details.reason_code.to_owned(),
            request_sha256: callback_request_sha256(callback),
            evidence_sha256: details.evidence_sha256,
            occurred_at_unix_ms: details.occurred_at_unix_ms,
        };
        event.validate().map_err(|_| unavailable("mcp.runtime.policy.audit_event_invalid"))?;
        Ok(event)
    }
}

#[async_trait]
impl McpHostCallbackPort for McpHostPolicyCallbackService {
    async fn handle_callback(
        &self,
        request: &McpServerCallbackRequest,
    ) -> Result<McpCallbackResponsePayload, McpHostCallbackError> {
        request
            .validate()
            .map_err(|_| denied("mcp.runtime.callback.invalid", "callback is invalid"))?;
        if let Some(binding) = self.fixed_binding.as_ref() {
            if request.principal_id != binding.principal_id
                || request.session_id != binding.session_id
                || request.origin != binding.origin
            {
                return Err(denied(
                    "mcp.runtime.callback.binding_mismatch",
                    "callback binding is not authorized",
                ));
            }
        }
        self.authority.validate_callback(request).map_err(|_| {
            denied("mcp.runtime.callback.stale_authority", "callback authority is stale")
        })?;
        match &request.callback {
            McpServerCallbackType::Elicitation(elicitation) => {
                self.handle_elicitation(request, elicitation).await
            }
            McpServerCallbackType::Sampling(sampling) => {
                self.handle_sampling(request, sampling).await
            }
            McpServerCallbackType::RootsList => {
                self.audit_denied(
                    request,
                    McpPolicyAuditKind::Roots,
                    "mcp.runtime.roots.denied_by_default",
                )
                .await?;
                Err(denied("mcp.runtime.roots.denied_by_default", "roots are not configured"))
            }
        }
    }

    fn runtime_record_committed(&self, record: &McpServerRecordV2) {
        let _ = self.authority.apply_committed(record);
    }
}

fn callback_binding(request: &McpServerCallbackRequest) -> McpCallbackBinding {
    McpCallbackBinding {
        principal_id: request.principal_id.clone(),
        session_id: request.session_id.clone(),
        origin: request.origin.clone(),
    }
}

/// Callback policy construction failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum McpHostPolicyBuildError {
    /// Policy contains unsafe or incomplete enabled surfaces.
    #[error("invalid mcp host callback policy")]
    InvalidPolicy,
}

fn callback_binding_sha256(request: &McpServerCallbackRequest) -> String {
    sha256_json(&json!({
        "principal_id": request.principal_id,
        "session_id": request.session_id,
        "origin": request.origin,
    }))
}

fn callback_request_sha256(request: &McpServerCallbackRequest) -> String {
    let callback = match &request.callback {
        McpServerCallbackType::Sampling(sampling) => json!({
            "kind": "sampling",
            "input": sampling.input_json,
            "requested_tools": sampling.requested_tools,
            "max_output_tokens": sampling.max_output_tokens,
        }),
        McpServerCallbackType::Elicitation(elicitation) => json!({
            "kind": "elicitation",
            "prompt": elicitation.prompt,
            "response_schema": elicitation.response_schema_json,
        }),
        McpServerCallbackType::RootsList => json!({"kind": "roots"}),
    };
    sha256_json(&json!({
        "callback_id": request.callback_id,
        "runtime_generation": request.runtime_generation,
        "catalog_epoch": request.catalog_epoch,
        "callback": callback,
    }))
}

fn sha256_json(value: &Value) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    hex::encode(Sha256::digest(bytes))
}

fn now_unix_ms() -> Result<i64, &'static str> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "mcp.runtime.policy.clock_before_epoch")?
        .as_millis();
    i64::try_from(millis).map_err(|_| "mcp.runtime.policy.clock_overflow")
}

fn audit_unavailable(error: McpPolicyAuditStoreError) -> McpHostCallbackError {
    let reason_code = match error {
        McpPolicyAuditStoreError::InvalidEvent => "mcp.runtime.policy.audit_invalid",
        McpPolicyAuditStoreError::IdempotencyConflict => {
            "mcp.runtime.policy.audit_idempotency_conflict"
        }
        McpPolicyAuditStoreError::Corrupt { .. } => "mcp.runtime.policy.audit_corrupt",
        McpPolicyAuditStoreError::Unavailable { .. } => "mcp.runtime.policy.audit_unavailable",
    };
    unavailable(reason_code)
}

fn denied(reason_code: impl Into<String>, safe_message: impl Into<String>) -> McpHostCallbackError {
    let safe_message = safe_message.into();
    McpHostCallbackError::Denied {
        reason_code: reason_code.into(),
        safe_message: safe_message.chars().take(MAX_SAFE_MESSAGE_BYTES).collect(),
    }
}

fn unavailable(reason_code: impl Into<String>) -> McpHostCallbackError {
    McpHostCallbackError::Unavailable { reason_code: reason_code.into() }
}

fn valid_identifier(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | ':' | '/')
        })
}

fn valid_reason_code(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_REASON_CODE_BYTES
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | ':')
        })
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::mcp_runtime::{
        McpRuntimeLifecycleState, McpServerRecordV2, McpSessionTransportKind,
    };
    use std::sync::Mutex;

    #[derive(Default)]
    struct MemoryAudit {
        events: Mutex<Vec<McpPolicyAuditEventV1>>,
    }

    #[async_trait]
    impl McpPolicyAuditStore for MemoryAudit {
        async fn append_policy_event(
            &self,
            event: &McpPolicyAuditEventV1,
        ) -> Result<McpPolicyAuditAppendOutcome, McpPolicyAuditStoreError> {
            event.validate()?;
            let mut events = self.events.lock().expect("audit lock should be healthy");
            if let Some(existing) =
                events.iter().find(|existing| existing.event_id == event.event_id)
            {
                return if existing == event {
                    Ok(McpPolicyAuditAppendOutcome::Existing)
                } else {
                    Err(McpPolicyAuditStoreError::IdempotencyConflict)
                };
            }
            events.push(event.clone());
            Ok(McpPolicyAuditAppendOutcome::Appended)
        }

        async fn sampling_usage(
            &self,
            server_id: &str,
            binding_sha256: &str,
            since_unix_ms: i64,
        ) -> Result<McpSamplingUsage, McpPolicyAuditStoreError> {
            let events = self.events.lock().expect("audit lock should be healthy");
            Ok(events
                .iter()
                .filter(|event| {
                    event.server_id == server_id
                        && event.binding_sha256 == binding_sha256
                        && event.kind == McpPolicyAuditKind::Sampling
                        && event.outcome == McpPolicyAuditOutcome::Allowed
                        && event.occurred_at_unix_ms >= since_unix_ms
                })
                .fold(McpSamplingUsage::default(), |mut usage, event| {
                    usage.requests = usage.requests.saturating_add(1);
                    usage.reserved_output_tokens =
                        usage.reserved_output_tokens.saturating_add(event.reserved_output_tokens);
                    usage
                }))
        }
    }

    #[derive(Default)]
    struct RecordingSampling {
        calls: Mutex<Vec<McpAuthorizedSamplingRequest>>,
    }

    #[async_trait]
    impl McpSamplingExecutionPort for RecordingSampling {
        async fn sample(
            &self,
            request: &McpAuthorizedSamplingRequest,
        ) -> Result<Value, McpHostExecutionError> {
            self.calls.lock().expect("sampling lock should be healthy").push(request.clone());
            Ok(json!({"text": "ok"}))
        }
    }

    fn binding() -> McpCallbackBinding {
        McpCallbackBinding {
            principal_id: "principal-a".to_owned(),
            session_id: "session-a".to_owned(),
            origin: "mcp:test".to_owned(),
        }
    }

    fn ready_authority() -> Arc<McpCatalogAuthority> {
        let authority = Arc::new(
            McpCatalogAuthority::new("server-a".to_owned()).expect("authority should validate"),
        );
        let configured = McpServerRecordV2::configured(
            "server-a".to_owned(),
            McpSessionTransportKind::Stdio,
            None,
            "trusted-local".to_owned(),
            1_000,
        )
        .expect("configured record should validate");
        let ready = configured
            .begin_start(1_001)
            .expect("startup should validate")
            .begin_handshake(1_001)
            .expect("handshake should validate")
            .mark_ready("a".repeat(64), 1_002)
            .expect("ready record should validate");
        assert_eq!(ready.lifecycle, McpRuntimeLifecycleState::Ready);
        authority.apply_committed(&ready).expect("ready authority should apply");
        authority
    }

    fn callback(callback_id: u64, callback: McpServerCallbackType) -> McpServerCallbackRequest {
        McpServerCallbackRequest {
            callback_id,
            runtime_generation: 1,
            catalog_epoch: 1,
            principal_id: "principal-a".to_owned(),
            session_id: "session-a".to_owned(),
            origin: "mcp:test".to_owned(),
            callback,
        }
    }

    #[tokio::test]
    async fn elicitation_is_denied_and_audited_by_default() {
        let audit = Arc::new(MemoryAudit::default());
        let service = McpHostPolicyCallbackService::new(
            "server-a".to_owned(),
            binding(),
            ready_authority(),
            McpHostCallbackPolicy::default(),
            audit.clone(),
            None,
            None,
        )
        .expect("deny policy should validate");
        let result = service
            .handle_callback(&callback(
                1,
                McpServerCallbackType::Elicitation(McpElicitationRequest {
                    prompt: "Enter a value".to_owned(),
                    response_schema_json: json!({"type": "string"}),
                }),
            ))
            .await;
        assert!(matches!(result, Err(McpHostCallbackError::Denied { .. })));
        let events = audit.events.lock().expect("audit lock should be healthy");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].outcome, McpPolicyAuditOutcome::Denied);
    }

    #[tokio::test]
    async fn sampling_uses_host_model_and_restart_safe_rate_reservations() {
        let audit = Arc::new(MemoryAudit::default());
        let sampling = Arc::new(RecordingSampling::default());
        let policy = McpHostCallbackPolicy {
            sampling_model_id: Some("host-model".to_owned()),
            allowed_sampling_origins: BTreeSet::from(["mcp:test".to_owned()]),
            allowed_sampling_tools: BTreeSet::from(["read_file".to_owned()]),
            max_sampling_output_tokens_per_request: 64,
            sampling_window: Duration::from_secs(60),
            max_sampling_requests_per_window: 1,
            max_sampling_output_tokens_per_window: 64,
            ..McpHostCallbackPolicy::default()
        };
        let service = McpHostPolicyCallbackService::new(
            "server-a".to_owned(),
            binding(),
            ready_authority(),
            policy,
            audit,
            None,
            Some(sampling.clone()),
        )
        .expect("sampling policy should validate");
        let request = |callback_id| {
            callback(
                callback_id,
                McpServerCallbackType::Sampling(McpSamplingRequest {
                    input_json: json!({"messages": []}),
                    requested_tools: vec!["read_file".to_owned()],
                    max_output_tokens: 64,
                }),
            )
        };

        assert!(service.handle_callback(&request(1)).await.is_ok());
        assert!(matches!(
            service.handle_callback(&request(2)).await,
            Err(McpHostCallbackError::Denied { reason_code, .. })
                if reason_code == "mcp.runtime.sampling.rate_limited"
        ));
        let calls = sampling.calls.lock().expect("sampling lock should be healthy");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].model_id, "host-model");
    }
}
