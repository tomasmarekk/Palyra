//! Generation-fenced OAuth credential refresh with per-scope single-flight.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{Mutex, OnceCell};

use super::{
    McpCatalogAuthority, McpPolicyAuditEventV1, McpPolicyAuditKind, McpPolicyAuditOutcome,
    McpPolicyAuditStore, McpPolicyAuditStoreError,
};

/// Host request for an opaque credential refresh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOAuthRefreshRequest {
    /// Caller-owned idempotency identity.
    pub request_id: String,
    /// Durable MCP server identity.
    pub server_id: String,
    /// Vault or auth registry scope, never raw credentials.
    pub credential_scope_id: String,
    /// Actor generation that needs the refreshed handle.
    pub expected_runtime_generation: u64,
    /// Minimum acceptable credential validity.
    pub minimum_valid_until_unix_ms: i64,
    /// Host request time.
    pub requested_at_unix_ms: i64,
}

impl McpOAuthRefreshRequest {
    /// Validates identity and time bounds before calling the credential service.
    ///
    /// # Errors
    /// Returns [`McpOAuthRefreshError::InvalidRequest`] for malformed input.
    pub fn validate(&self) -> Result<(), McpOAuthRefreshError> {
        if !valid_identifier(&self.request_id)
            || !valid_identifier(&self.server_id)
            || !valid_identifier(&self.credential_scope_id)
            || self.expected_runtime_generation == 0
            || self.requested_at_unix_ms <= 0
            || self.minimum_valid_until_unix_ms < self.requested_at_unix_ms
        {
            return Err(McpOAuthRefreshError::InvalidRequest);
        }
        Ok(())
    }
}

/// Validated opaque credential lease returned by the host auth boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOAuthCredentialLease {
    /// Runtime handle reference; never token material.
    pub credential_handle_id: String,
    /// Credential expiry.
    pub expires_at_unix_ms: i64,
    /// Digest of host-owned refresh evidence.
    pub evidence_sha256: String,
}

/// Credential availability and OAuth refresh boundary.
#[async_trait]
pub trait McpOAuthCredentialPort: Send + Sync {
    /// Returns an opaque credential handle and must honor `request_id`
    /// idempotently if the host performs a mutating refresh.
    async fn refresh(
        &self,
        request: &McpOAuthRefreshRequest,
    ) -> Result<McpOAuthCredentialLease, McpOAuthCredentialError>;
}

/// Host credential service failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("mcp oauth credential service failed: {reason_code}")]
pub struct McpOAuthCredentialError {
    /// Stable host-owned failure reason.
    pub reason_code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RefreshFlightKey {
    server_id: String,
    credential_scope_id: String,
    runtime_generation: u64,
    minimum_valid_until_unix_ms: i64,
}

type RefreshResult = Result<McpOAuthCredentialLease, McpOAuthCredentialError>;
type RefreshFlight = Arc<OnceCell<RefreshResult>>;

/// Per-scope single-flight OAuth refresh coordinator.
pub struct McpOAuthRefreshCoordinator {
    server_id: String,
    authority: Arc<McpCatalogAuthority>,
    credentials: Arc<dyn McpOAuthCredentialPort>,
    audit: Arc<dyn McpPolicyAuditStore>,
    flights: Mutex<BTreeMap<RefreshFlightKey, RefreshFlight>>,
}

impl std::fmt::Debug for McpOAuthRefreshCoordinator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpOAuthRefreshCoordinator")
            .field("server_id", &self.server_id)
            .finish_non_exhaustive()
    }
}

impl McpOAuthRefreshCoordinator {
    /// Creates a coordinator bound to one durable MCP server.
    ///
    /// # Errors
    /// Returns [`McpOAuthRefreshError::InvalidRequest`] for malformed server identity.
    pub fn new(
        server_id: String,
        authority: Arc<McpCatalogAuthority>,
        credentials: Arc<dyn McpOAuthCredentialPort>,
        audit: Arc<dyn McpPolicyAuditStore>,
    ) -> Result<Self, McpOAuthRefreshError> {
        if !valid_identifier(&server_id) {
            return Err(McpOAuthRefreshError::InvalidRequest);
        }
        Ok(Self { server_id, authority, credentials, audit, flights: Mutex::new(BTreeMap::new()) })
    }

    /// Refreshes one credential scope with generation fencing and single-flight.
    ///
    /// Concurrent equivalent requests invoke the host credential port once.
    /// Each caller records metadata-only durable evidence; no token or raw
    /// credential value enters this coordinator or journal.
    ///
    /// # Errors
    /// Returns a validation, stale generation, credential, evidence, or audit error.
    pub async fn refresh(
        &self,
        request: &McpOAuthRefreshRequest,
    ) -> Result<McpOAuthCredentialLease, McpOAuthRefreshError> {
        request.validate()?;
        if request.server_id != self.server_id {
            return Err(McpOAuthRefreshError::InvalidRequest);
        }
        self.authority
            .validate_runtime_generation(request.expected_runtime_generation)
            .map_err(|_| McpOAuthRefreshError::StaleAuthority)?;
        let key = RefreshFlightKey {
            server_id: request.server_id.clone(),
            credential_scope_id: request.credential_scope_id.clone(),
            runtime_generation: request.expected_runtime_generation,
            minimum_valid_until_unix_ms: request.minimum_valid_until_unix_ms,
        };
        let flight = {
            let mut flights = self.flights.lock().await;
            flights.entry(key.clone()).or_insert_with(|| Arc::new(OnceCell::new())).clone()
        };
        let result =
            flight.get_or_init(|| async { self.credentials.refresh(request).await }).await.clone();
        {
            let mut flights = self.flights.lock().await;
            if flights.get(&key).is_some_and(|active| Arc::ptr_eq(active, &flight)) {
                flights.remove(&key);
            }
        }

        match result {
            Ok(lease) => {
                validate_lease(request, &lease)?;
                let event = audit_event(
                    request,
                    McpPolicyAuditOutcome::Refreshed,
                    "mcp.runtime.oauth.refreshed",
                    Some(lease.evidence_sha256.clone()),
                )?;
                self.audit
                    .append_policy_event(&event)
                    .await
                    .map_err(McpOAuthRefreshError::Audit)?;
                Ok(lease)
            }
            Err(error) => {
                let event = audit_event(
                    request,
                    McpPolicyAuditOutcome::Failed,
                    "mcp.runtime.oauth.refresh_failed",
                    None,
                )?;
                self.audit
                    .append_policy_event(&event)
                    .await
                    .map_err(McpOAuthRefreshError::Audit)?;
                Err(McpOAuthRefreshError::Credential(error))
            }
        }
    }
}

/// OAuth coordinator failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum McpOAuthRefreshError {
    /// Request identity, generation, or time bounds are invalid.
    #[error("invalid mcp oauth refresh request")]
    InvalidRequest,
    /// Runtime generation is no longer authoritative.
    #[error("stale mcp oauth runtime authority")]
    StaleAuthority,
    /// Credential service returned an invalid handle or expiry.
    #[error("invalid mcp oauth credential lease")]
    InvalidLease,
    /// Host credential service failed.
    #[error(transparent)]
    Credential(#[from] McpOAuthCredentialError),
    /// Durable metadata audit failed.
    #[error(transparent)]
    Audit(#[from] McpPolicyAuditStoreError),
}

fn validate_lease(
    request: &McpOAuthRefreshRequest,
    lease: &McpOAuthCredentialLease,
) -> Result<(), McpOAuthRefreshError> {
    if !valid_identifier(&lease.credential_handle_id)
        || lease.expires_at_unix_ms < request.minimum_valid_until_unix_ms
        || !valid_sha256(&lease.evidence_sha256)
    {
        return Err(McpOAuthRefreshError::InvalidLease);
    }
    Ok(())
}

fn audit_event(
    request: &McpOAuthRefreshRequest,
    outcome: McpPolicyAuditOutcome,
    reason_code: &str,
    evidence_sha256: Option<String>,
) -> Result<McpPolicyAuditEventV1, McpOAuthRefreshError> {
    let request_projection = json!({
        "request_id": request.request_id,
        "server_id": request.server_id,
        "credential_scope_id": request.credential_scope_id,
        "expected_runtime_generation": request.expected_runtime_generation,
        "minimum_valid_until_unix_ms": request.minimum_valid_until_unix_ms,
    });
    let event = McpPolicyAuditEventV1 {
        event_id: format!("oauth_refresh:{}:{}", request.server_id, request.request_id),
        server_id: request.server_id.clone(),
        runtime_generation: request.expected_runtime_generation,
        catalog_epoch: 0,
        binding_sha256: sha256_json(&json!({
            "credential_scope_id": request.credential_scope_id
        })),
        kind: McpPolicyAuditKind::OAuthRefresh,
        outcome,
        reserved_output_tokens: 0,
        reason_code: reason_code.to_owned(),
        request_sha256: sha256_json(&request_projection),
        evidence_sha256,
        occurred_at_unix_ms: request.requested_at_unix_ms,
    };
    event.validate().map_err(McpOAuthRefreshError::Audit)?;
    Ok(event)
}

fn sha256_json(value: &serde_json::Value) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    hex::encode(Sha256::digest(bytes))
}

fn valid_identifier(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= 256
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | ':' | '/')
        })
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex as StdMutex,
    };

    use super::*;
    use crate::application::mcp_runtime::{
        McpPolicyAuditAppendOutcome, McpSamplingUsage, McpServerRecordV2, McpSessionTransportKind,
    };
    use tokio::sync::Notify;

    #[derive(Default)]
    struct MemoryAudit {
        events: StdMutex<Vec<McpPolicyAuditEventV1>>,
    }

    #[async_trait]
    impl McpPolicyAuditStore for MemoryAudit {
        async fn append_policy_event(
            &self,
            event: &McpPolicyAuditEventV1,
        ) -> Result<McpPolicyAuditAppendOutcome, McpPolicyAuditStoreError> {
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
            _server_id: &str,
            _binding_sha256: &str,
            _since_unix_ms: i64,
        ) -> Result<McpSamplingUsage, McpPolicyAuditStoreError> {
            Ok(McpSamplingUsage::default())
        }
    }

    struct BlockingCredentialPort {
        calls: AtomicUsize,
        started: Notify,
        release: Notify,
    }

    #[async_trait]
    impl McpOAuthCredentialPort for BlockingCredentialPort {
        async fn refresh(
            &self,
            request: &McpOAuthRefreshRequest,
        ) -> Result<McpOAuthCredentialLease, McpOAuthCredentialError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            self.release.notified().await;
            Ok(McpOAuthCredentialLease {
                credential_handle_id: "credential-handle-a".to_owned(),
                expires_at_unix_ms: request.minimum_valid_until_unix_ms + 1_000,
                evidence_sha256: "a".repeat(64),
            })
        }
    }

    fn handshaking_authority() -> Arc<McpCatalogAuthority> {
        let authority = Arc::new(
            McpCatalogAuthority::new("server-a".to_owned()).expect("authority should validate"),
        );
        let record = McpServerRecordV2::configured(
            "server-a".to_owned(),
            McpSessionTransportKind::StreamableHttp,
            Some("scope-a".to_owned()),
            "trusted-remote".to_owned(),
            1_000,
        )
        .expect("configured record should validate")
        .begin_start(1_001)
        .expect("startup should validate")
        .begin_handshake(1_001)
        .expect("handshake should validate");
        authority.apply_committed(&record).expect("authority should apply");
        authority
    }

    #[tokio::test]
    async fn equivalent_refreshes_share_one_host_call_and_audit_each_request() {
        let credentials = Arc::new(BlockingCredentialPort {
            calls: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
        });
        let audit = Arc::new(MemoryAudit::default());
        let coordinator = Arc::new(
            McpOAuthRefreshCoordinator::new(
                "server-a".to_owned(),
                handshaking_authority(),
                credentials.clone(),
                audit.clone(),
            )
            .expect("coordinator should validate"),
        );
        let request = |request_id: &str| McpOAuthRefreshRequest {
            request_id: request_id.to_owned(),
            server_id: "server-a".to_owned(),
            credential_scope_id: "scope-a".to_owned(),
            expected_runtime_generation: 1,
            minimum_valid_until_unix_ms: 2_000,
            requested_at_unix_ms: 1_500,
        };
        let first = {
            let coordinator = coordinator.clone();
            let request = request("request-a");
            tokio::spawn(async move { coordinator.refresh(&request).await })
        };
        credentials.started.notified().await;
        let second = {
            let coordinator = coordinator.clone();
            let request = request("request-b");
            tokio::spawn(async move { coordinator.refresh(&request).await })
        };
        loop {
            let strong_count = {
                let flights = coordinator.flights.lock().await;
                flights.values().next().map_or(0, Arc::strong_count)
            };
            if strong_count >= 3 {
                break;
            }
            tokio::task::yield_now().await;
        }
        credentials.release.notify_waiters();

        first.await.expect("first task should join").expect("first refresh should succeed");
        second.await.expect("second task should join").expect("second refresh should succeed");
        assert_eq!(credentials.calls.load(Ordering::SeqCst), 1);
        assert_eq!(audit.events.lock().expect("audit lock should be healthy").len(), 2);
    }
}
