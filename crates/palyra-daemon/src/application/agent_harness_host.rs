//! Bounded host authority exposed to asynchronous agent harnesses.
//!
//! Opaque grants, deadlines, generation checks, and redacted audit records keep
//! external runtimes on the same provider/tool/approval boundaries as the host.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use palyra_common::redaction::redact_diagnostic_text;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::watch;

const MAX_HOST_REQUEST_BYTES: usize = 256 * 1024;
const MAX_HOST_RESPONSE_BYTES: usize = 512 * 1024;
const MAX_AUDIT_RECORDS: usize = 2_048;
const MAX_HOST_CALL_RECORDS: usize = 2_048;

#[derive(Debug, Clone)]
enum HostCallRecord {
    InFlight { operation: HarnessHostOperation },
    Completed { operation: HarnessHostOperation, outcome: Result<Value, HarnessHostError> },
}

/// Host operation authorized by a scoped harness capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessHostOperation {
    GetRuntimeContext,
    RequestModelTurn,
    ProposeToolCall,
    AwaitToolOutcome,
    EmitTextDelta,
    EmitProgress,
    RequestCompaction,
    SideQuestion,
    CreateArtifact,
    Checkpoint,
    Heartbeat,
}

/// Opaque call identity used for idempotency and audit correlation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HarnessCallId(String);

impl HarnessCallId {
    /// Parses a bounded non-empty call identity.
    ///
    /// # Errors
    /// Returns [`HarnessHostError::InvalidCall`] for malformed identities.
    pub fn parse(value: impl Into<String>) -> Result<Self, HarnessHostError> {
        let value = value.into();
        if value.trim().is_empty() || value.len() > 128 {
            return Err(HarnessHostError::InvalidCall);
        }
        Ok(Self(value))
    }

    /// Returns the call identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// In-memory opaque grant. Serialization deliberately exposes only a digest.
#[derive(Clone, PartialEq, Eq)]
pub struct HarnessCapabilityHandle {
    opaque_ref: String,
}

impl std::fmt::Debug for HarnessCapabilityHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HarnessCapabilityHandle")
            .field("sha256", &sha256_hex(self.opaque_ref.as_bytes()))
            .finish()
    }
}

/// Metadata accompanying every host call.
#[derive(Debug, Clone)]
pub struct HarnessHostCallContext {
    pub call_id: HarnessCallId,
    pub harness_id: String,
    pub generation: u64,
    pub deadline_unix_ms: i64,
    pub capability: HarnessCapabilityHandle,
}

/// Redacted host-call audit record. Request and response payloads are never retained.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HarnessHostAuditRecord {
    pub call_id_sha256: String,
    pub harness_id: String,
    pub generation: u64,
    pub operation: HarnessHostOperation,
    pub request_bytes: usize,
    pub response_bytes: usize,
    pub outcome: String,
    pub duration_ms: u64,
}

#[derive(Debug, Clone)]
struct CapabilityGrant {
    harness_id: String,
    generation: u64,
    operations: Vec<HarnessHostOperation>,
    expires_at_unix_ms: i64,
}

/// In-memory issuer and validator for expiring harness capability handles.
#[derive(Debug, Default)]
pub struct HarnessCapabilityStore {
    grants: Mutex<BTreeMap<String, CapabilityGrant>>,
}

impl HarnessCapabilityStore {
    /// Issues a scoped capability for one harness generation.
    ///
    /// # Errors
    /// Returns [`HarnessHostError::InvalidCapability`] for empty scope or invalid expiry.
    pub fn issue(
        &self,
        harness_id: &str,
        generation: u64,
        operations: Vec<HarnessHostOperation>,
        expires_at_unix_ms: i64,
    ) -> Result<HarnessCapabilityHandle, HarnessHostError> {
        if harness_id.trim().is_empty()
            || harness_id.len() > 128
            || generation == 0
            || operations.is_empty()
            || expires_at_unix_ms <= now_unix_ms()
        {
            return Err(HarnessHostError::InvalidCapability);
        }
        let mut random = [0_u8; 32];
        getrandom::fill(&mut random).map_err(|_| HarnessHostError::CapabilityIssuanceFailed)?;
        let opaque_ref = hex::encode(random);
        let grant = CapabilityGrant {
            harness_id: harness_id.to_owned(),
            generation,
            operations,
            expires_at_unix_ms,
        };
        self.grants
            .lock()
            .map_err(|_| HarnessHostError::HostUnavailable)?
            .insert(opaque_ref.clone(), grant);
        Ok(HarnessCapabilityHandle { opaque_ref })
    }

    /// Revokes every grant owned by an invalidated generation.
    pub fn revoke_generation(&self, harness_id: &str, generation: u64) {
        if let Ok(mut grants) = self.grants.lock() {
            grants.retain(|_, grant| {
                grant.harness_id != harness_id || grant.generation != generation
            });
        }
    }

    fn authorize(
        &self,
        context: &HarnessHostCallContext,
        operation: HarnessHostOperation,
    ) -> Result<(), HarnessHostError> {
        let grants = self.grants.lock().map_err(|_| HarnessHostError::HostUnavailable)?;
        let grant = grants
            .get(context.capability.opaque_ref.as_str())
            .ok_or(HarnessHostError::InvalidCapability)?;
        let now = now_unix_ms();
        if context.deadline_unix_ms <= now {
            return Err(HarnessHostError::DeadlineExceeded);
        }
        if grant.expires_at_unix_ms <= now {
            return Err(HarnessHostError::ExpiredCapability);
        }
        if grant.harness_id != context.harness_id {
            return Err(HarnessHostError::ForeignCapability);
        }
        if grant.generation != context.generation {
            return Err(HarnessHostError::StaleGeneration {
                active: grant.generation,
                observed: context.generation,
            });
        }
        if !grant.operations.contains(&operation) {
            return Err(HarnessHostError::ScopeDenied);
        }
        Ok(())
    }
}

/// Cancellation source shared by host calls and the owning harness attempt.
#[derive(Debug, Clone)]
pub struct HarnessCancellationContext {
    receiver: watch::Receiver<bool>,
}

impl HarnessCancellationContext {
    /// Creates a cancellable context and its host-owned sender.
    #[must_use]
    pub fn channel() -> (watch::Sender<bool>, Self) {
        let (sender, receiver) = watch::channel(false);
        (sender, Self { receiver })
    }

    /// Returns whether cancellation has been observed.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.receiver.borrow()
    }

    /// Waits until cancellation is requested.
    pub async fn cancelled(&mut self) {
        while !*self.receiver.borrow_and_update() {
            if self.receiver.changed().await.is_err() {
                break;
            }
        }
    }
}

/// Host-owned implementation boundary for provider, tool, approval, and persistence authority.
#[async_trait]
pub trait HarnessHostBackend: Send + Sync {
    /// Executes one already-authorized call through the canonical host service.
    async fn invoke(
        &self,
        operation: HarnessHostOperation,
        payload: Value,
        cancellation: HarnessCancellationContext,
    ) -> Result<Value, HarnessHostError>;
}

/// Explicit asynchronous API available to an agent harness.
#[async_trait]
pub trait HarnessHost: Send + Sync {
    async fn get_runtime_context(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<Value, HarnessHostError>;
    async fn request_model_turn(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<Value, HarnessHostError>;
    async fn propose_tool_call(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<Value, HarnessHostError>;
    async fn await_tool_outcome(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<Value, HarnessHostError>;
    async fn emit_text_delta(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<(), HarnessHostError>;
    async fn emit_progress(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<(), HarnessHostError>;
    async fn request_compaction(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<Value, HarnessHostError>;
    async fn side_question(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<Value, HarnessHostError>;
    async fn create_artifact(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<Value, HarnessHostError>;
    async fn checkpoint(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<(), HarnessHostError>;
    async fn heartbeat(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<(), HarnessHostError>;
    fn is_cancelled(&self) -> bool;
}

/// Capability-checking, deadline-enforcing host facade.
pub struct GuardedHarnessHost<Backend> {
    backend: Arc<Backend>,
    capabilities: Arc<HarnessCapabilityStore>,
    cancellation: HarnessCancellationContext,
    max_call_timeout: Duration,
    audit: Mutex<Vec<HarnessHostAuditRecord>>,
    calls: Mutex<BTreeMap<String, HostCallRecord>>,
}

impl<Backend> GuardedHarnessHost<Backend>
where
    Backend: HarnessHostBackend,
{
    /// Constructs a guarded host facade.
    #[must_use]
    pub fn new(
        backend: Arc<Backend>,
        capabilities: Arc<HarnessCapabilityStore>,
        cancellation: HarnessCancellationContext,
        max_call_timeout: Duration,
    ) -> Self {
        Self {
            backend,
            capabilities,
            cancellation,
            max_call_timeout,
            audit: Mutex::new(Vec::new()),
            calls: Mutex::new(BTreeMap::new()),
        }
    }

    /// Returns bounded redacted call usage suitable for diagnostics.
    #[must_use]
    pub fn audit_records(&self) -> Vec<HarnessHostAuditRecord> {
        self.audit.lock().map_or_else(|_| Vec::new(), |records| records.clone())
    }

    async fn call(
        &self,
        operation: HarnessHostOperation,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<Value, HarnessHostError> {
        let started = Instant::now();
        let request_bytes =
            serde_json::to_vec(&request).map_err(|_| HarnessHostError::InvalidPayload)?.len();
        if request_bytes > MAX_HOST_REQUEST_BYTES {
            return Err(HarnessHostError::RequestTooLarge);
        }
        self.capabilities.authorize(&context, operation)?;
        if self.cancellation.is_cancelled() {
            return Err(HarnessHostError::Cancelled);
        }
        let remaining_ms = context.deadline_unix_ms.saturating_sub(now_unix_ms());
        let remaining = Duration::from_millis(u64::try_from(remaining_ms).unwrap_or(0))
            .min(self.max_call_timeout);
        if remaining.is_zero() {
            return Err(HarnessHostError::DeadlineExceeded);
        }
        if let Some(replayed) = self.begin_call(&context, operation)? {
            let response_bytes = replayed
                .as_ref()
                .ok()
                .and_then(|response| serde_json::to_vec(response).ok())
                .map_or(0, |bytes| bytes.len());
            self.record_audit(HarnessHostAuditRecord {
                call_id_sha256: sha256_hex(context.call_id.as_str().as_bytes()),
                harness_id: redact_diagnostic_text(context.harness_id.as_str()),
                generation: context.generation,
                operation,
                request_bytes,
                response_bytes,
                outcome: "idempotent_replay".to_owned(),
                duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            });
            return replayed;
        }
        let mut outcome = tokio::time::timeout(
            remaining,
            self.backend.invoke(operation, request, self.cancellation.clone()),
        )
        .await
        .map_err(|_| HarnessHostError::DeadlineExceeded)
        .and_then(|result| result);
        let encoded_response_bytes = outcome
            .as_ref()
            .ok()
            .map(serde_json::to_vec)
            .transpose()
            .map(|encoded| encoded.map_or(0, |bytes| bytes.len()));
        let response_bytes = match encoded_response_bytes {
            Ok(bytes) if bytes <= MAX_HOST_RESPONSE_BYTES => bytes,
            Ok(_) => {
                outcome = Err(HarnessHostError::ResponseTooLarge);
                0
            }
            Err(_) => {
                outcome = Err(HarnessHostError::InvalidPayload);
                0
            }
        };
        self.complete_call(&context, operation, outcome.clone());
        let outcome_label =
            outcome.as_ref().map_or_else(|error| error.reason_code(), |_| "completed");
        self.record_audit(HarnessHostAuditRecord {
            call_id_sha256: sha256_hex(context.call_id.as_str().as_bytes()),
            harness_id: redact_diagnostic_text(context.harness_id.as_str()),
            generation: context.generation,
            operation,
            request_bytes,
            response_bytes,
            outcome: outcome_label.to_owned(),
            duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        });
        outcome
    }

    fn begin_call(
        &self,
        context: &HarnessHostCallContext,
        operation: HarnessHostOperation,
    ) -> Result<Option<Result<Value, HarnessHostError>>, HarnessHostError> {
        let mut calls = self.calls.lock().map_err(|_| HarnessHostError::HostUnavailable)?;
        match calls.get(context.call_id.as_str()) {
            Some(HostCallRecord::InFlight { operation: observed }) if *observed == operation => {
                return Err(HarnessHostError::CallInFlight);
            }
            Some(HostCallRecord::Completed { operation: observed, outcome })
                if *observed == operation =>
            {
                return Ok(Some(outcome.clone()));
            }
            Some(_) => return Err(HarnessHostError::InvalidCall),
            None if calls.len() >= MAX_HOST_CALL_RECORDS => {
                return Err(HarnessHostError::HostUnavailable);
            }
            None => {}
        }
        calls.insert(context.call_id.as_str().to_owned(), HostCallRecord::InFlight { operation });
        Ok(None)
    }

    fn complete_call(
        &self,
        context: &HarnessHostCallContext,
        operation: HarnessHostOperation,
        outcome: Result<Value, HarnessHostError>,
    ) {
        if let Ok(mut calls) = self.calls.lock() {
            calls.insert(
                context.call_id.as_str().to_owned(),
                HostCallRecord::Completed { operation, outcome },
            );
        }
    }

    fn record_audit(&self, record: HarnessHostAuditRecord) {
        if let Ok(mut audit) = self.audit.lock() {
            if audit.len() == MAX_AUDIT_RECORDS {
                audit.remove(0);
            }
            audit.push(record);
        }
    }
}

#[async_trait]
impl<Backend> HarnessHost for GuardedHarnessHost<Backend>
where
    Backend: HarnessHostBackend,
{
    async fn get_runtime_context(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<Value, HarnessHostError> {
        self.call(HarnessHostOperation::GetRuntimeContext, context, request).await
    }

    async fn request_model_turn(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<Value, HarnessHostError> {
        self.call(HarnessHostOperation::RequestModelTurn, context, request).await
    }

    async fn propose_tool_call(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<Value, HarnessHostError> {
        self.call(HarnessHostOperation::ProposeToolCall, context, request).await
    }

    async fn await_tool_outcome(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<Value, HarnessHostError> {
        self.call(HarnessHostOperation::AwaitToolOutcome, context, request).await
    }

    async fn emit_text_delta(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<(), HarnessHostError> {
        self.call(HarnessHostOperation::EmitTextDelta, context, request).await.map(|_| ())
    }

    async fn emit_progress(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<(), HarnessHostError> {
        self.call(HarnessHostOperation::EmitProgress, context, request).await.map(|_| ())
    }

    async fn request_compaction(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<Value, HarnessHostError> {
        self.call(HarnessHostOperation::RequestCompaction, context, request).await
    }

    async fn side_question(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<Value, HarnessHostError> {
        self.call(HarnessHostOperation::SideQuestion, context, request).await
    }

    async fn create_artifact(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<Value, HarnessHostError> {
        self.call(HarnessHostOperation::CreateArtifact, context, request).await
    }

    async fn checkpoint(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<(), HarnessHostError> {
        self.call(HarnessHostOperation::Checkpoint, context, request).await.map(|_| ())
    }

    async fn heartbeat(
        &self,
        context: HarnessHostCallContext,
        request: Value,
    ) -> Result<(), HarnessHostError> {
        self.call(HarnessHostOperation::Heartbeat, context, request).await.map(|_| ())
    }

    fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

/// Fail-closed error returned by the harness host boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HarnessHostError {
    #[error("harness host call is invalid")]
    InvalidCall,
    #[error("harness host call is already in flight")]
    CallInFlight,
    #[error("harness capability is invalid")]
    InvalidCapability,
    #[error("harness capability issuance failed")]
    CapabilityIssuanceFailed,
    #[error("harness capability has expired")]
    ExpiredCapability,
    #[error("harness capability belongs to another runtime")]
    ForeignCapability,
    #[error("harness capability does not authorize this operation")]
    ScopeDenied,
    #[error("harness generation is stale")]
    StaleGeneration { active: u64, observed: u64 },
    #[error("harness host request deadline was exceeded")]
    DeadlineExceeded,
    #[error("harness host request was cancelled")]
    Cancelled,
    #[error("harness host request payload is invalid")]
    InvalidPayload,
    #[error("harness host request exceeded the size limit")]
    RequestTooLarge,
    #[error("harness host response exceeded the size limit")]
    ResponseTooLarge,
    #[error("harness host service is unavailable")]
    HostUnavailable,
    #[error("harness host backend failed: {reason_code}")]
    Backend { reason_code: String },
}

impl HarnessHostError {
    /// Returns a stable redaction-safe reason code.
    #[must_use]
    pub fn reason_code(&self) -> &str {
        match self {
            Self::InvalidCall => "harness.host.invalid_call",
            Self::CallInFlight => "harness.host.call_in_flight",
            Self::InvalidCapability => "harness.host.invalid_capability",
            Self::CapabilityIssuanceFailed => "harness.host.capability_issuance_failed",
            Self::ExpiredCapability => "harness.host.expired_capability",
            Self::ForeignCapability => "harness.host.foreign_capability",
            Self::ScopeDenied => "harness.host.scope_denied",
            Self::StaleGeneration { .. } => "harness.host.stale_generation",
            Self::DeadlineExceeded => "harness.host.deadline_exceeded",
            Self::Cancelled => "harness.host.cancelled",
            Self::InvalidPayload => "harness.host.invalid_payload",
            Self::RequestTooLarge => "harness.host.request_too_large",
            Self::ResponseTooLarge => "harness.host.response_too_large",
            Self::HostUnavailable => "harness.host.unavailable",
            Self::Backend { reason_code } => reason_code.as_str(),
        }
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use serde_json::json;

    #[derive(Debug)]
    struct EchoBackend;

    #[async_trait]
    impl HarnessHostBackend for EchoBackend {
        async fn invoke(
            &self,
            operation: HarnessHostOperation,
            payload: Value,
            _cancellation: HarnessCancellationContext,
        ) -> Result<Value, HarnessHostError> {
            Ok(json!({"operation": operation, "payload": payload}))
        }
    }

    #[derive(Debug, Default)]
    struct PolicyBackend {
        invocations: AtomicUsize,
    }

    #[async_trait]
    impl HarnessHostBackend for PolicyBackend {
        async fn invoke(
            &self,
            operation: HarnessHostOperation,
            payload: Value,
            mut cancellation: HarnessCancellationContext,
        ) -> Result<Value, HarnessHostError> {
            self.invocations.fetch_add(1, Ordering::Relaxed);
            match operation {
                HarnessHostOperation::ProposeToolCall => {
                    match payload.get("decision").and_then(Value::as_str) {
                        Some("allow") => Ok(json!({"outcome": "allowed"})),
                        Some("approval") => Ok(json!({"outcome": "approval_required"})),
                        _ => Ok(json!({"outcome": "denied"})),
                    }
                }
                HarnessHostOperation::RequestCompaction
                    if payload.get("phase").and_then(Value::as_str) != Some("provider") =>
                {
                    Err(HarnessHostError::Backend {
                        reason_code: "harness.compaction.wrong_phase".to_owned(),
                    })
                }
                HarnessHostOperation::Heartbeat
                    if payload.get("wait_for_cancel").and_then(Value::as_bool) == Some(true) =>
                {
                    cancellation.cancelled().await;
                    Err(HarnessHostError::Cancelled)
                }
                HarnessHostOperation::Heartbeat => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(Value::Null)
                }
                _ => Ok(Value::Null),
            }
        }
    }

    fn context(
        capability: HarnessCapabilityHandle,
        harness_id: &str,
        generation: u64,
    ) -> HarnessHostCallContext {
        context_with_call(capability, harness_id, generation, "call-1")
    }

    fn context_with_call(
        capability: HarnessCapabilityHandle,
        harness_id: &str,
        generation: u64,
        call_id: &str,
    ) -> HarnessHostCallContext {
        HarnessHostCallContext {
            call_id: HarnessCallId::parse(call_id).expect("call id"),
            harness_id: harness_id.to_owned(),
            generation,
            deadline_unix_ms: now_unix_ms() + 5_000,
            capability,
        }
    }

    #[tokio::test]
    async fn scoped_call_is_audited_without_payload() {
        let capabilities = Arc::new(HarnessCapabilityStore::default());
        let handle = capabilities
            .issue(
                "external",
                3,
                vec![HarnessHostOperation::ProposeToolCall],
                now_unix_ms() + 5_000,
            )
            .expect("capability");
        let (_cancel, cancellation) = HarnessCancellationContext::channel();
        let host = GuardedHarnessHost::new(
            Arc::new(EchoBackend),
            capabilities,
            cancellation,
            Duration::from_secs(1),
        );

        host.propose_tool_call(context(handle, "external", 3), json!({"secret":"not-retained"}))
            .await
            .expect("authorized call");

        let audit = host.audit_records();
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].operation, HarnessHostOperation::ProposeToolCall);
        assert!(!serde_json::to_string(&audit).expect("audit JSON").contains("not-retained"));
    }

    #[tokio::test]
    async fn foreign_expired_and_stale_handles_fail_closed() {
        let capabilities = Arc::new(HarnessCapabilityStore::default());
        let handle = capabilities
            .issue("external", 7, vec![HarnessHostOperation::Heartbeat], now_unix_ms() + 5_000)
            .expect("capability");
        let (_cancel, cancellation) = HarnessCancellationContext::channel();
        let host = GuardedHarnessHost::new(
            Arc::new(EchoBackend),
            capabilities,
            cancellation,
            Duration::from_secs(1),
        );

        let foreign = host
            .heartbeat(context(handle.clone(), "other", 7), Value::Null)
            .await
            .expect_err("foreign handle");
        assert_eq!(foreign, HarnessHostError::ForeignCapability);
        let stale = host
            .heartbeat(context(handle, "external", 8), Value::Null)
            .await
            .expect_err("stale generation");
        assert_eq!(stale, HarnessHostError::StaleGeneration { active: 7, observed: 8 });
    }

    #[tokio::test]
    async fn cancellation_stops_host_calls_before_backend_dispatch() {
        let capabilities = Arc::new(HarnessCapabilityStore::default());
        let handle = capabilities
            .issue("external", 2, vec![HarnessHostOperation::Heartbeat], now_unix_ms() + 5_000)
            .expect("capability");
        let (cancel, cancellation) = HarnessCancellationContext::channel();
        cancel.send(true).expect("cancellation receiver");
        let host = GuardedHarnessHost::new(
            Arc::new(EchoBackend),
            capabilities,
            cancellation,
            Duration::from_secs(1),
        );

        assert_eq!(
            host.heartbeat(context(handle, "external", 2), Value::Null).await,
            Err(HarnessHostError::Cancelled)
        );
    }

    #[tokio::test]
    async fn tool_decisions_and_duplicate_call_replay_stay_host_owned() {
        let capabilities = Arc::new(HarnessCapabilityStore::default());
        let handle = capabilities
            .issue(
                "external",
                11,
                vec![HarnessHostOperation::ProposeToolCall],
                now_unix_ms() + 5_000,
            )
            .expect("capability");
        let backend = Arc::new(PolicyBackend::default());
        let (_cancel, cancellation) = HarnessCancellationContext::channel();
        let host = GuardedHarnessHost::new(
            Arc::clone(&backend),
            capabilities,
            cancellation,
            Duration::from_secs(1),
        );

        for (call_id, decision, expected) in [
            ("allow", "allow", "allowed"),
            ("approval", "approval", "approval_required"),
            ("deny", "deny", "denied"),
        ] {
            let response = host
                .propose_tool_call(
                    context_with_call(handle.clone(), "external", 11, call_id),
                    json!({"decision": decision}),
                )
                .await
                .expect("host-owned tool decision");
            assert_eq!(response.get("outcome").and_then(Value::as_str), Some(expected));
        }
        let replayed = host
            .propose_tool_call(
                context_with_call(handle, "external", 11, "allow"),
                json!({"decision": "deny"}),
            )
            .await
            .expect("idempotent completed call replay");
        assert_eq!(replayed.get("outcome").and_then(Value::as_str), Some("allowed"));
        assert_eq!(backend.invocations.load(Ordering::Relaxed), 3);
        assert!(host.audit_records().iter().any(|record| record.outcome == "idempotent_replay"));
    }

    #[tokio::test]
    async fn oversize_artifact_wrong_phase_and_expired_handle_fail_closed() {
        let capabilities = Arc::new(HarnessCapabilityStore::default());
        let handle = capabilities
            .issue(
                "external",
                12,
                vec![HarnessHostOperation::CreateArtifact, HarnessHostOperation::RequestCompaction],
                now_unix_ms() + 5_000,
            )
            .expect("capability");
        let (_cancel, cancellation) = HarnessCancellationContext::channel();
        let host = GuardedHarnessHost::new(
            Arc::new(PolicyBackend::default()),
            Arc::clone(&capabilities),
            cancellation,
            Duration::from_secs(1),
        );

        let oversize = host
            .create_artifact(
                context_with_call(handle.clone(), "external", 12, "artifact"),
                json!({"bytes": "x".repeat(MAX_HOST_REQUEST_BYTES + 1)}),
            )
            .await
            .expect_err("oversize artifact");
        assert_eq!(oversize, HarnessHostError::RequestTooLarge);
        let wrong_phase = host
            .request_compaction(
                context_with_call(handle, "external", 12, "compaction"),
                json!({"phase": "tool"}),
            )
            .await
            .expect_err("wrong compaction phase");
        assert_eq!(wrong_phase.reason_code(), "harness.compaction.wrong_phase");

        let expiring = capabilities
            .issue("external", 12, vec![HarnessHostOperation::Heartbeat], now_unix_ms() + 20)
            .expect("expiring capability");
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(
            host.heartbeat(context_with_call(expiring, "external", 12, "expired"), Value::Null,)
                .await,
            Err(HarnessHostError::ExpiredCapability)
        );
    }

    #[tokio::test]
    async fn heartbeat_timeout_and_in_flight_cancellation_are_bounded() {
        let capabilities = Arc::new(HarnessCapabilityStore::default());
        let handle = capabilities
            .issue("external", 13, vec![HarnessHostOperation::Heartbeat], now_unix_ms() + 5_000)
            .expect("capability");
        let (_cancel, cancellation) = HarnessCancellationContext::channel();
        let timeout_host = GuardedHarnessHost::new(
            Arc::new(PolicyBackend::default()),
            Arc::clone(&capabilities),
            cancellation,
            Duration::from_millis(10),
        );
        assert_eq!(
            timeout_host
                .heartbeat(
                    context_with_call(handle.clone(), "external", 13, "timeout"),
                    Value::Null,
                )
                .await,
            Err(HarnessHostError::DeadlineExceeded)
        );

        let (cancel, cancellation) = HarnessCancellationContext::channel();
        let cancelling_host = Arc::new(GuardedHarnessHost::new(
            Arc::new(PolicyBackend::default()),
            capabilities,
            cancellation,
            Duration::from_secs(1),
        ));
        let call_host = Arc::clone(&cancelling_host);
        let call = tokio::spawn(async move {
            call_host
                .heartbeat(
                    context_with_call(handle, "external", 13, "cancel"),
                    json!({"wait_for_cancel": true}),
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.send(true).expect("host cancellation receiver");
        assert_eq!(call.await.expect("host call task"), Err(HarnessHostError::Cancelled));
    }
}
