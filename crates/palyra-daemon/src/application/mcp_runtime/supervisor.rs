//! Durable MCP runtime records, reconnect policy, and admission evidence.
//!
//! The supervisor keeps deterministic projections only. Journal persistence is
//! provided through an atomic compare-and-swap port owned by the host.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::transport::McpSessionTransportKind;

/// Schema version of the durable MCP server runtime record.
pub const MCP_SERVER_RECORD_SCHEMA_VERSION: u32 = 2;

const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_REASON_CODE_BYTES: usize = 192;
const MAX_SAFE_MESSAGE_BYTES: usize = 8 * 1024;
const DEFAULT_MAX_DESCRIPTOR_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_SCHEMA_NODES: usize = 8_192;
const DEFAULT_MAX_SCHEMA_DEPTH: usize = 64;

/// Durable lifecycle state for one configured MCP server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpRuntimeLifecycleState {
    /// Configuration is valid but no runtime generation is active.
    Configured,
    /// A new generation is connecting and negotiating MCP initialization.
    Handshaking,
    /// A persistent session is ready to accept requests.
    Ready,
    /// The active generation failed and is waiting for bounded reconnect.
    Reconnecting,
    /// The actor is draining accepted requests.
    Stopping,
    /// The actor stopped and released its transport.
    Stopped,
    /// Repeated or policy-significant failures require operator repair.
    Quarantined,
    /// Configuration explicitly disables the server.
    Disabled,
}

impl McpRuntimeLifecycleState {
    fn permits(self, next: Self) -> bool {
        if self == next {
            return true;
        }
        match self {
            Self::Configured => matches!(next, Self::Handshaking | Self::Disabled),
            Self::Handshaking => {
                matches!(
                    next,
                    Self::Ready | Self::Reconnecting | Self::Stopping | Self::Quarantined
                )
            }
            Self::Ready => {
                matches!(next, Self::Reconnecting | Self::Stopping | Self::Quarantined)
            }
            Self::Reconnecting => {
                matches!(next, Self::Handshaking | Self::Stopping | Self::Quarantined)
            }
            Self::Stopping => matches!(next, Self::Stopped | Self::Quarantined),
            Self::Stopped => matches!(next, Self::Handshaking | Self::Disabled),
            Self::Quarantined => {
                matches!(next, Self::Configured | Self::Stopping | Self::Disabled)
            }
            Self::Disabled => matches!(next, Self::Configured),
        }
    }
}

/// Durable generation, catalog, failure, and lifecycle projection for one server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerRecordV2 {
    /// Record schema version.
    pub schema_version: u32,
    /// Stable server configuration identity.
    pub server_id: String,
    /// Configured persistent transport.
    pub transport: McpSessionTransportKind,
    /// Durable lifecycle state.
    pub lifecycle: McpRuntimeLifecycleState,
    /// Monotonic transport ownership generation.
    pub runtime_generation: u64,
    /// Monotonic catalog epoch.
    pub catalog_epoch: u64,
    /// Digest of the catalog represented by `catalog_epoch`.
    pub catalog_digest: Option<String>,
    /// Host-owned credential scope reference, never raw credentials.
    pub credential_scope_id: Option<String>,
    /// Stable trust-policy profile.
    pub trust_profile_id: String,
    /// Consecutive connection or protocol failures.
    pub consecutive_failures: u32,
    /// Earliest reconnect time after a transient failure.
    pub next_retry_at_unix_ms: Option<i64>,
    /// Stable reason for quarantine.
    pub quarantine_reason_code: Option<String>,
    /// Monotonic compare-and-swap revision.
    pub revision: u64,
    /// Record creation time.
    pub created_at_unix_ms: i64,
    /// Last committed transition time.
    pub updated_at_unix_ms: i64,
}

impl McpServerRecordV2 {
    /// Creates a validated configured record with no active generation.
    ///
    /// # Errors
    /// Returns [`McpRuntimeSupervisorError::InvalidRecord`] for invalid identity metadata.
    pub fn configured(
        server_id: String,
        transport: McpSessionTransportKind,
        credential_scope_id: Option<String>,
        trust_profile_id: String,
        now_unix_ms: i64,
    ) -> Result<Self, McpRuntimeSupervisorError> {
        let record = Self {
            schema_version: MCP_SERVER_RECORD_SCHEMA_VERSION,
            server_id,
            transport,
            lifecycle: McpRuntimeLifecycleState::Configured,
            runtime_generation: 0,
            catalog_epoch: 0,
            catalog_digest: None,
            credential_scope_id,
            trust_profile_id,
            consecutive_failures: 0,
            next_retry_at_unix_ms: None,
            quarantine_reason_code: None,
            revision: 0,
            created_at_unix_ms: now_unix_ms,
            updated_at_unix_ms: now_unix_ms,
        };
        record.validate()?;
        Ok(record)
    }

    /// Validates durable bounds and state-dependent invariants.
    ///
    /// # Errors
    /// Returns [`McpRuntimeSupervisorError::InvalidRecord`] for a corrupt projection.
    pub fn validate(&self) -> Result<(), McpRuntimeSupervisorError> {
        let ready_invariants = self.lifecycle != McpRuntimeLifecycleState::Ready
            || (self.runtime_generation > 0
                && self.catalog_epoch > 0
                && self.catalog_digest.as_deref().is_some_and(valid_sha256));
        let quarantine_invariants = if self.lifecycle == McpRuntimeLifecycleState::Quarantined {
            self.quarantine_reason_code.as_deref().is_some_and(valid_reason_code)
        } else {
            self.quarantine_reason_code.is_none()
        };
        if self.schema_version != MCP_SERVER_RECORD_SCHEMA_VERSION
            || !valid_identifier(&self.server_id)
            || !valid_identifier(&self.trust_profile_id)
            || self.credential_scope_id.as_deref().is_some_and(|scope| !valid_identifier(scope))
            || self.catalog_digest.as_deref().is_some_and(|digest| !valid_sha256(digest))
            || !ready_invariants
            || !quarantine_invariants
            || self.created_at_unix_ms <= 0
            || self.updated_at_unix_ms < self.created_at_unix_ms
        {
            return Err(McpRuntimeSupervisorError::InvalidRecord {
                server_id: self.server_id.clone(),
            });
        }
        Ok(())
    }

    /// Plans a new handshaking generation.
    ///
    /// # Errors
    /// Returns an error when the transition is illegal or counters overflow.
    pub fn begin_handshake(&self, now_unix_ms: i64) -> Result<Self, McpRuntimeSupervisorError> {
        let mut next = self.transition(
            McpRuntimeLifecycleState::Handshaking,
            now_unix_ms,
            "mcp.runtime.handshake.started",
        )?;
        next.runtime_generation = self.runtime_generation.checked_add(1).ok_or(
            McpRuntimeSupervisorError::CounterExhausted {
                server_id: self.server_id.clone(),
                counter: "runtime_generation",
            },
        )?;
        next.next_retry_at_unix_ms = None;
        next.validate()?;
        Ok(next)
    }

    /// Plans a ready projection after successful initialization.
    ///
    /// A changed catalog digest advances the epoch; reconnecting to the exact
    /// same catalog preserves the prior epoch.
    ///
    /// # Errors
    /// Returns an error for an invalid digest, transition, or exhausted epoch.
    pub fn mark_ready(
        &self,
        catalog_digest: String,
        now_unix_ms: i64,
    ) -> Result<Self, McpRuntimeSupervisorError> {
        if !valid_sha256(&catalog_digest) {
            return Err(McpRuntimeSupervisorError::InvalidCatalogDigest {
                server_id: self.server_id.clone(),
            });
        }
        let mut next = self.transition(
            McpRuntimeLifecycleState::Ready,
            now_unix_ms,
            "mcp.runtime.session.ready",
        )?;
        if self.catalog_digest.as_deref() != Some(catalog_digest.as_str()) {
            next.catalog_epoch = self.catalog_epoch.checked_add(1).ok_or(
                McpRuntimeSupervisorError::CounterExhausted {
                    server_id: self.server_id.clone(),
                    counter: "catalog_epoch",
                },
            )?;
        }
        if next.catalog_epoch == 0 {
            next.catalog_epoch = 1;
        }
        next.catalog_digest = Some(catalog_digest);
        next.consecutive_failures = 0;
        next.next_retry_at_unix_ms = None;
        next.quarantine_reason_code = None;
        next.validate()?;
        Ok(next)
    }

    /// Plans a new catalog epoch after a validated change notification.
    ///
    /// # Errors
    /// Returns an error for stale state, an invalid digest, or counter exhaustion.
    pub fn advance_catalog(
        &self,
        catalog_digest: Option<String>,
        now_unix_ms: i64,
    ) -> Result<Self, McpRuntimeSupervisorError> {
        if self.lifecycle != McpRuntimeLifecycleState::Ready {
            return Err(McpRuntimeSupervisorError::IllegalTransition {
                server_id: self.server_id.clone(),
                from: self.lifecycle,
                to: self.lifecycle,
                reason_code: "mcp.runtime.catalog.not_ready".to_owned(),
            });
        }
        if catalog_digest.as_deref().is_some_and(|digest| !valid_sha256(digest)) {
            return Err(McpRuntimeSupervisorError::InvalidCatalogDigest {
                server_id: self.server_id.clone(),
            });
        }
        if catalog_digest.as_deref().is_some_and(|digest| {
            self.catalog_digest.as_deref().is_some_and(|current| current == digest)
        }) {
            return Ok(self.clone());
        }
        let mut next = self.clone_with_revision(now_unix_ms)?;
        next.catalog_epoch = self.catalog_epoch.checked_add(1).ok_or(
            McpRuntimeSupervisorError::CounterExhausted {
                server_id: self.server_id.clone(),
                counter: "catalog_epoch",
            },
        )?;
        if let Some(digest) = catalog_digest {
            next.catalog_digest = Some(digest);
        }
        next.validate()?;
        Ok(next)
    }

    /// Plans a host-owned configuration replacement from an inactive state.
    ///
    /// Active transports must first pass through stopping and stopped so a
    /// changed command, endpoint, credential scope, or trust profile can never
    /// inherit the previous generation's ownership.
    ///
    /// # Errors
    /// Returns an illegal-transition or invalid-record error when the current
    /// record is active or replacement metadata violates durable bounds.
    pub fn reconfigure(
        &self,
        transport: McpSessionTransportKind,
        credential_scope_id: Option<String>,
        trust_profile_id: String,
        now_unix_ms: i64,
    ) -> Result<Self, McpRuntimeSupervisorError> {
        if !matches!(
            self.lifecycle,
            McpRuntimeLifecycleState::Configured
                | McpRuntimeLifecycleState::Stopped
                | McpRuntimeLifecycleState::Disabled
                | McpRuntimeLifecycleState::Quarantined
        ) {
            return Err(McpRuntimeSupervisorError::IllegalTransition {
                server_id: self.server_id.clone(),
                from: self.lifecycle,
                to: McpRuntimeLifecycleState::Configured,
                reason_code: "mcp.runtime.config.active_owner".to_owned(),
            });
        }
        let mut next = self.clone_with_revision(now_unix_ms)?;
        next.transport = transport;
        next.credential_scope_id = credential_scope_id;
        next.trust_profile_id = trust_profile_id;
        next.lifecycle = McpRuntimeLifecycleState::Configured;
        next.next_retry_at_unix_ms = None;
        next.quarantine_reason_code = None;
        next.consecutive_failures = 0;
        next.validate()?;
        Ok(next)
    }

    /// Plans a reconnect or quarantine transition after a transport failure.
    ///
    /// # Errors
    /// Returns an error for invalid reason codes, transitions, or counter overflow.
    pub fn mark_failure(
        &self,
        policy: &McpReconnectPolicy,
        reason_code: &str,
        now_unix_ms: i64,
    ) -> Result<Self, McpRuntimeSupervisorError> {
        policy.validate()?;
        if !valid_reason_code(reason_code) {
            return Err(McpRuntimeSupervisorError::InvalidReasonCode);
        }
        let failures = self.consecutive_failures.checked_add(1).ok_or(
            McpRuntimeSupervisorError::CounterExhausted {
                server_id: self.server_id.clone(),
                counter: "consecutive_failures",
            },
        )?;
        let quarantined = failures >= policy.quarantine_after_failures;
        let next_state = if quarantined {
            McpRuntimeLifecycleState::Quarantined
        } else {
            McpRuntimeLifecycleState::Reconnecting
        };
        let mut next = self.transition(next_state, now_unix_ms, reason_code)?;
        next.consecutive_failures = failures;
        if quarantined {
            next.next_retry_at_unix_ms = None;
            next.quarantine_reason_code = Some(reason_code.to_owned());
        } else {
            let delay_ms = policy.delay_ms(&self.server_id, failures)?;
            let delay_ms =
                i64::try_from(delay_ms).map_err(|_| McpRuntimeSupervisorError::TimeOverflow)?;
            next.next_retry_at_unix_ms = Some(
                now_unix_ms.checked_add(delay_ms).ok_or(McpRuntimeSupervisorError::TimeOverflow)?,
            );
            next.quarantine_reason_code = None;
        }
        next.validate()?;
        Ok(next)
    }

    /// Plans an immediate reconnect after daemon restart without counting it as a failure.
    ///
    /// # Errors
    /// Returns an error when the restored lifecycle cannot reconnect or time regresses.
    pub fn recover_after_restart(
        &self,
        now_unix_ms: i64,
    ) -> Result<Self, McpRuntimeSupervisorError> {
        let mut next = self.transition(
            McpRuntimeLifecycleState::Reconnecting,
            now_unix_ms,
            "mcp.runtime.restart.reconnect",
        )?;
        next.next_retry_at_unix_ms = Some(now_unix_ms);
        next.quarantine_reason_code = None;
        next.validate()?;
        Ok(next)
    }

    /// Plans a lifecycle-only transition such as stopping or disabled.
    ///
    /// # Errors
    /// Returns an error for an illegal transition or invalid reason code.
    pub fn transition(
        &self,
        lifecycle: McpRuntimeLifecycleState,
        now_unix_ms: i64,
        reason_code: &str,
    ) -> Result<Self, McpRuntimeSupervisorError> {
        if !self.lifecycle.permits(lifecycle) || !valid_reason_code(reason_code) {
            return Err(McpRuntimeSupervisorError::IllegalTransition {
                server_id: self.server_id.clone(),
                from: self.lifecycle,
                to: lifecycle,
                reason_code: reason_code.to_owned(),
            });
        }
        let mut next = self.clone_with_revision(now_unix_ms)?;
        next.lifecycle = lifecycle;
        if lifecycle != McpRuntimeLifecycleState::Quarantined {
            next.quarantine_reason_code = None;
        }
        Ok(next)
    }

    fn clone_with_revision(&self, now_unix_ms: i64) -> Result<Self, McpRuntimeSupervisorError> {
        if now_unix_ms < self.updated_at_unix_ms {
            return Err(McpRuntimeSupervisorError::TimeRegression {
                server_id: self.server_id.clone(),
            });
        }
        let mut next = self.clone();
        next.revision =
            self.revision.checked_add(1).ok_or(McpRuntimeSupervisorError::CounterExhausted {
                server_id: self.server_id.clone(),
                counter: "revision",
            })?;
        next.updated_at_unix_ms = now_unix_ms;
        Ok(next)
    }
}

/// One durable lifecycle or catalog transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpRuntimeEventV2 {
    /// Stable server identity.
    pub server_id: String,
    /// Revision before the atomic transition.
    pub previous_revision: u64,
    /// Revision committed by the transition.
    pub revision: u64,
    /// Previous lifecycle state.
    pub previous_lifecycle: McpRuntimeLifecycleState,
    /// Committed lifecycle state.
    pub lifecycle: McpRuntimeLifecycleState,
    /// Active runtime generation.
    pub runtime_generation: u64,
    /// Active catalog epoch.
    pub catalog_epoch: u64,
    /// Stable transition reason.
    pub reason_code: String,
    /// Transition time.
    pub occurred_at_unix_ms: i64,
}

impl McpRuntimeEventV2 {
    /// Builds an event that exactly describes an adjacent record transition.
    ///
    /// # Errors
    /// Returns [`McpRuntimeSupervisorError::InvalidEvent`] for non-adjacent revisions.
    pub fn from_transition(
        previous: &McpServerRecordV2,
        next: &McpServerRecordV2,
        reason_code: impl Into<String>,
    ) -> Result<Self, McpRuntimeSupervisorError> {
        let reason_code = reason_code.into();
        if previous.server_id != next.server_id
            || previous.revision.checked_add(1) != Some(next.revision)
            || !valid_reason_code(&reason_code)
        {
            return Err(McpRuntimeSupervisorError::InvalidEvent);
        }
        Ok(Self {
            server_id: next.server_id.clone(),
            previous_revision: previous.revision,
            revision: next.revision,
            previous_lifecycle: previous.lifecycle,
            lifecycle: next.lifecycle,
            runtime_generation: next.runtime_generation,
            catalog_epoch: next.catalog_epoch,
            reason_code,
            occurred_at_unix_ms: next.updated_at_unix_ms,
        })
    }
}

/// Atomic persistence port for runtime records and append-only transition evidence.
#[async_trait]
pub trait McpRuntimeRecordStore: Send + Sync {
    /// Loads every durable MCP runtime projection during daemon startup.
    async fn load_all(&self) -> Result<Vec<McpServerRecordV2>, McpRuntimeStoreError>;

    /// Atomically inserts a newly configured revision-zero record.
    async fn insert_configured(
        &self,
        record: &McpServerRecordV2,
    ) -> Result<(), McpRuntimeStoreError>;

    /// Atomically commits a record and its event when the expected revision matches.
    async fn persist_transition(
        &self,
        expected_revision: u64,
        record: &McpServerRecordV2,
        event: &McpRuntimeEventV2,
    ) -> Result<(), McpRuntimeStoreError>;
}

/// Persistence failure returned by the host journal adapter.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum McpRuntimeStoreError {
    /// Another owner committed a conflicting revision.
    #[error("mcp runtime record revision conflict")]
    RevisionConflict {
        /// Revision supplied by the actor.
        expected: u64,
        /// Current durable revision when known.
        actual: Option<u64>,
    },
    /// Stored data failed schema or invariant validation.
    #[error("mcp runtime store contains corrupt data: {reason_code}")]
    Corrupt {
        /// Stable redaction-safe reason.
        reason_code: String,
    },
    /// Persistence was temporarily unavailable.
    #[error("mcp runtime store unavailable: {reason_code}")]
    Unavailable {
        /// Stable redaction-safe reason.
        reason_code: String,
    },
}

/// Deterministic in-memory projection restored from the durable store.
#[derive(Debug, Default)]
pub struct McpRuntimeSupervisor {
    records: BTreeMap<String, McpServerRecordV2>,
}

impl McpRuntimeSupervisor {
    /// Restores and validates all durable records.
    ///
    /// # Errors
    /// Returns an error for store failure, corrupt records, or duplicate server identities.
    pub async fn restore(
        store: &dyn McpRuntimeRecordStore,
    ) -> Result<Self, McpRuntimeSupervisorError> {
        let mut records = BTreeMap::new();
        for record in store.load_all().await? {
            record.validate()?;
            let server_id = record.server_id.clone();
            if records.insert(server_id.clone(), record).is_some() {
                return Err(McpRuntimeSupervisorError::DuplicateServer { server_id });
            }
        }
        Ok(Self { records })
    }

    /// Returns one restored record.
    pub fn record(&self, server_id: &str) -> Option<&McpServerRecordV2> {
        self.records.get(server_id)
    }

    /// Returns restored records in deterministic server-id order.
    pub fn records(&self) -> impl Iterator<Item = &McpServerRecordV2> {
        self.records.values()
    }

    /// Applies a record after its compare-and-swap transition has committed.
    ///
    /// # Errors
    /// Returns an error for an unknown server or a non-adjacent revision.
    pub fn apply_committed(
        &mut self,
        record: McpServerRecordV2,
    ) -> Result<(), McpRuntimeSupervisorError> {
        record.validate()?;
        let current = self.records.get(&record.server_id).ok_or_else(|| {
            McpRuntimeSupervisorError::UnknownServer { server_id: record.server_id.clone() }
        })?;
        if current.revision.checked_add(1) != Some(record.revision) {
            return Err(McpRuntimeSupervisorError::StaleProjection {
                server_id: record.server_id.clone(),
                expected_revision: current.revision.saturating_add(1),
                observed_revision: record.revision,
            });
        }
        self.records.insert(record.server_id.clone(), record);
        Ok(())
    }
}

/// Bounded exponential reconnect and quarantine policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpReconnectPolicy {
    /// Delay before the first reconnect.
    pub initial_delay_ms: u64,
    /// Maximum reconnect delay.
    pub max_delay_ms: u64,
    /// Deterministic jitter ceiling in basis points.
    pub jitter_basis_points: u16,
    /// Consecutive failure count that enters quarantine.
    pub quarantine_after_failures: u32,
}

impl Default for McpReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_delay_ms: 250,
            max_delay_ms: 30_000,
            jitter_basis_points: 2_000,
            quarantine_after_failures: 8,
        }
    }
}

impl McpReconnectPolicy {
    /// Validates reconnect bounds.
    ///
    /// # Errors
    /// Returns [`McpRuntimeSupervisorError::InvalidReconnectPolicy`] for unsafe values.
    pub fn validate(&self) -> Result<(), McpRuntimeSupervisorError> {
        if self.initial_delay_ms == 0
            || self.max_delay_ms < self.initial_delay_ms
            || self.jitter_basis_points > 10_000
            || self.quarantine_after_failures == 0
        {
            return Err(McpRuntimeSupervisorError::InvalidReconnectPolicy);
        }
        Ok(())
    }

    /// Computes capped exponential delay with stable per-server jitter.
    ///
    /// # Errors
    /// Returns an error when the policy is invalid.
    pub fn delay_ms(
        &self,
        server_id: &str,
        consecutive_failures: u32,
    ) -> Result<u64, McpRuntimeSupervisorError> {
        self.validate()?;
        let exponent = consecutive_failures.saturating_sub(1).min(63);
        let base = self
            .initial_delay_ms
            .saturating_mul(1_u64.checked_shl(exponent).unwrap_or(u64::MAX))
            .min(self.max_delay_ms);
        let jitter_ceiling = base.saturating_mul(u64::from(self.jitter_basis_points)) / 10_000;
        let jitter = if jitter_ceiling == 0 {
            0
        } else {
            stable_hash(server_id.as_bytes()).wrapping_add(u64::from(consecutive_failures))
                % jitter_ceiling.saturating_add(1)
        };
        Ok(base.saturating_add(jitter).min(self.max_delay_ms))
    }
}

/// Effect class used by host approval and policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpToolEffectClassification {
    /// Tool is declared read-only.
    ReadOnly,
    /// Tool may mutate external or local state.
    Mutating,
    /// Tool effect cannot be safely inferred and must be treated conservatively.
    Unknown,
}

/// External MCP tool descriptor proposed for dynamic registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpExternalToolDescriptor {
    /// Namespaced external tool name.
    pub name: String,
    /// Bounded redaction-safe description.
    pub description: String,
    /// JSON Schema for tool input.
    pub input_schema_json: Value,
    /// Optional JSON Schema for tool output.
    pub output_schema_json: Option<Value>,
    /// Host-visible effect class.
    pub effect: McpToolEffectClassification,
    /// Host approval policy class.
    pub approval_class: String,
}

/// Signed or otherwise host-verifiable descriptor attestation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDescriptorAttestation {
    /// Trust-store issuer identity.
    pub issuer_id: String,
    /// Issuer key or verification-method identity.
    pub key_id: String,
    /// SHA-256 digest of the canonical descriptor.
    pub descriptor_sha256: String,
    /// Opaque signature encoded by the configured trust verifier.
    pub signature: String,
}

/// Generation and epoch-bound request to register one trusted external tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustedExternalToolRegistrationRequest {
    /// MCP server that owns the descriptor.
    pub server_id: String,
    /// Active persistent runtime generation.
    pub runtime_generation: u64,
    /// Catalog epoch from which the descriptor was read.
    pub catalog_epoch: u64,
    /// Proposed tool descriptor.
    pub descriptor: McpExternalToolDescriptor,
    /// Cryptographic or equivalent host-verifiable attestation.
    pub attestation: McpDescriptorAttestation,
}

/// Identity returned by the host trust verifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpVerifiedDescriptorIdentity {
    /// Verified issuer identity.
    pub issuer_id: String,
    /// Verified key or method identity.
    pub key_id: String,
}

/// Trust-verification port for descriptor attestations.
pub trait McpDescriptorTrustVerifier: Send + Sync {
    /// Verifies the request against the canonical descriptor digest.
    ///
    /// # Errors
    /// Returns [`McpDescriptorAdmissionError::TrustVerificationFailed`] on rejection.
    fn verify(
        &self,
        request: &TrustedExternalToolRegistrationRequest,
        canonical_descriptor_sha256: &str,
    ) -> Result<McpVerifiedDescriptorIdentity, McpDescriptorAdmissionError>;
}

/// Bounds and trust policy for dynamic descriptor admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpDescriptorAdmissionPolicy {
    /// Maximum encoded descriptor bytes.
    pub max_descriptor_bytes: usize,
    /// Maximum nodes in either JSON schema.
    pub max_schema_nodes: usize,
    /// Maximum nesting depth in either JSON schema.
    pub max_schema_depth: usize,
    /// Issuers accepted for dynamic registration.
    pub trusted_issuer_ids: BTreeSet<String>,
    /// Whether mutating descriptors may be registered.
    pub allow_mutating_tools: bool,
}

impl Default for McpDescriptorAdmissionPolicy {
    fn default() -> Self {
        Self {
            max_descriptor_bytes: DEFAULT_MAX_DESCRIPTOR_BYTES,
            max_schema_nodes: DEFAULT_MAX_SCHEMA_NODES,
            max_schema_depth: DEFAULT_MAX_SCHEMA_DEPTH,
            trusted_issuer_ids: BTreeSet::new(),
            allow_mutating_tools: false,
        }
    }
}

/// Descriptor admitted after bounded validation and host trust verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpAdmittedToolDescriptor {
    /// Stable server identity.
    pub server_id: String,
    /// Runtime generation that supplied the descriptor.
    pub runtime_generation: u64,
    /// Catalog epoch that supplied the descriptor.
    pub catalog_epoch: u64,
    /// Validated descriptor.
    pub descriptor: McpExternalToolDescriptor,
    /// Canonical descriptor SHA-256.
    pub descriptor_sha256: String,
    /// Verified issuer identity.
    pub verified_issuer_id: String,
    /// Whether a new approval decision is required.
    pub requires_reapproval: bool,
}

/// Validates and admits one dynamic external tool descriptor.
///
/// A changed descriptor digest always requires a fresh host approval decision.
///
/// # Errors
/// Returns [`McpDescriptorAdmissionError`] for malformed, oversized, untrusted,
/// stale, or policy-denied registrations.
pub fn admit_external_tool_descriptor(
    request: TrustedExternalToolRegistrationRequest,
    previous: Option<&McpAdmittedToolDescriptor>,
    policy: &McpDescriptorAdmissionPolicy,
    verifier: &dyn McpDescriptorTrustVerifier,
) -> Result<McpAdmittedToolDescriptor, McpDescriptorAdmissionError> {
    validate_admission_policy(policy)?;
    validate_registration_request(&request, policy)?;
    let encoded = serde_json::to_vec(&request.descriptor)
        .map_err(|_| McpDescriptorAdmissionError::InvalidDescriptor)?;
    if encoded.len() > policy.max_descriptor_bytes {
        return Err(McpDescriptorAdmissionError::DescriptorTooLarge);
    }
    let canonical_descriptor_sha256 = hex::encode(Sha256::digest(&encoded));
    if request.attestation.descriptor_sha256 != canonical_descriptor_sha256 {
        return Err(McpDescriptorAdmissionError::DigestMismatch);
    }
    let identity = verifier.verify(&request, &canonical_descriptor_sha256)?;
    if identity.issuer_id != request.attestation.issuer_id
        || identity.key_id != request.attestation.key_id
        || !policy.trusted_issuer_ids.contains(&identity.issuer_id)
    {
        return Err(McpDescriptorAdmissionError::UntrustedIssuer);
    }
    if let Some(previous) = previous {
        if previous.server_id != request.server_id
            || previous.descriptor.name != request.descriptor.name
            || previous.runtime_generation > request.runtime_generation
            || previous.catalog_epoch > request.catalog_epoch
        {
            return Err(McpDescriptorAdmissionError::StaleRegistration);
        }
    }
    let requires_reapproval =
        previous.is_none_or(|previous| previous.descriptor_sha256 != canonical_descriptor_sha256);
    Ok(McpAdmittedToolDescriptor {
        server_id: request.server_id,
        runtime_generation: request.runtime_generation,
        catalog_epoch: request.catalog_epoch,
        descriptor: request.descriptor,
        descriptor_sha256: canonical_descriptor_sha256,
        verified_issuer_id: identity.issuer_id,
        requires_reapproval,
    })
}

/// Dynamic descriptor admission failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum McpDescriptorAdmissionError {
    /// Admission policy itself is unsafe or malformed.
    #[error("invalid mcp descriptor admission policy")]
    InvalidPolicy,
    /// Descriptor identity, schema, effect, or bounds are invalid.
    #[error("invalid mcp external tool descriptor")]
    InvalidDescriptor,
    /// Encoded descriptor exceeds the configured bound.
    #[error("mcp external tool descriptor exceeds the size limit")]
    DescriptorTooLarge,
    /// Schema exceeds node or nesting limits.
    #[error("mcp external tool schema exceeds structural limits")]
    SchemaTooComplex,
    /// Descriptor digest differs from its attestation.
    #[error("mcp external tool descriptor digest mismatch")]
    DigestMismatch,
    /// Attestation signature or trust evidence failed.
    #[error("mcp external tool trust verification failed")]
    TrustVerificationFailed,
    /// Verified issuer is not admitted by policy.
    #[error("mcp external tool issuer is not trusted")]
    UntrustedIssuer,
    /// Mutating tools are denied by the selected policy.
    #[error("mcp mutating external tools are denied")]
    MutatingToolDenied,
    /// Registration is older than the descriptor it would replace.
    #[error("stale mcp external tool registration")]
    StaleRegistration,
}

/// Required production conformance check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpConformanceCheckKind {
    /// Only one actor owns a generation.
    SingleOwnerGeneration,
    /// Late results and callbacks are generation fenced.
    GenerationFencing,
    /// Runtime records restore after restart.
    DurableRestore,
    /// Catalog changes advance durable epochs.
    CatalogEpochs,
    /// Sandbox, egress, vault, approval, and callback policy remain host-owned.
    HostPolicyEnforcement,
    /// Commands, events, callbacks, and payloads are bounded.
    BoundedResources,
    /// Drain and forced cleanup leave no orphan transport.
    CleanDrain,
}

/// Outcome of one conformance check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpConformanceCheckStatus {
    /// Check completed successfully.
    Passed,
    /// Check ran and failed.
    Failed,
    /// Check was not executed and cannot count toward qualification.
    NotRun,
}

/// Redaction-safe result for one conformance check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpConformanceCheck {
    /// Check identity.
    pub kind: McpConformanceCheckKind,
    /// Check outcome.
    pub status: McpConformanceCheckStatus,
    /// Stable evidence reference or failure reason.
    pub evidence_ref: String,
}

/// Persistent MCP runtime conformance report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpConformanceReportV1 {
    /// Stable report schema version.
    pub schema_version: u32,
    /// Server under test.
    pub server_id: String,
    /// Persistent transport under test.
    pub transport: McpSessionTransportKind,
    /// Runtime generation exercised by the report.
    pub runtime_generation: u64,
    /// Catalog epoch observed after notification coverage.
    pub catalog_epoch: u64,
    /// Start time.
    pub started_at_unix_ms: i64,
    /// Completion time.
    pub completed_at_unix_ms: i64,
    /// Bounded individual check results.
    pub checks: Vec<McpConformanceCheck>,
}

impl McpConformanceReportV1 {
    /// Returns whether every required persistent-runtime check passed exactly once.
    pub fn qualifies_for_production(&self) -> bool {
        if self.validate().is_err() {
            return false;
        }
        let required = BTreeSet::from([
            McpConformanceCheckKind::SingleOwnerGeneration,
            McpConformanceCheckKind::GenerationFencing,
            McpConformanceCheckKind::DurableRestore,
            McpConformanceCheckKind::CatalogEpochs,
            McpConformanceCheckKind::HostPolicyEnforcement,
            McpConformanceCheckKind::BoundedResources,
            McpConformanceCheckKind::CleanDrain,
        ]);
        let passed = self
            .checks
            .iter()
            .filter(|check| check.status == McpConformanceCheckStatus::Passed)
            .map(|check| check.kind)
            .collect::<BTreeSet<_>>();
        passed == required
    }

    /// Validates report identity, timestamps, evidence, and duplicate checks.
    ///
    /// # Errors
    /// Returns [`McpRuntimeSupervisorError::InvalidConformanceReport`] for invalid evidence.
    pub fn validate(&self) -> Result<(), McpRuntimeSupervisorError> {
        let mut seen = BTreeSet::new();
        if self.schema_version != 1
            || !valid_identifier(&self.server_id)
            || self.runtime_generation == 0
            || self.catalog_epoch == 0
            || self.started_at_unix_ms <= 0
            || self.completed_at_unix_ms < self.started_at_unix_ms
            || self.checks.len() != 7
            || self
                .checks
                .iter()
                .any(|check| !seen.insert(check.kind) || !valid_evidence_ref(&check.evidence_ref))
        {
            return Err(McpRuntimeSupervisorError::InvalidConformanceReport);
        }
        Ok(())
    }
}

/// Durable supervisor contract failure.
#[derive(Debug, Error)]
pub enum McpRuntimeSupervisorError {
    /// Durable record failed schema or invariant validation.
    #[error("invalid mcp runtime record for server {server_id}")]
    InvalidRecord {
        /// Server whose record failed.
        server_id: String,
    },
    /// Lifecycle transition is not permitted.
    #[error("illegal mcp runtime transition for server {server_id}: {reason_code}")]
    IllegalTransition {
        /// Server being transitioned.
        server_id: String,
        /// Current state.
        from: McpRuntimeLifecycleState,
        /// Requested state.
        to: McpRuntimeLifecycleState,
        /// Stable transition reason.
        reason_code: String,
    },
    /// A monotonic counter cannot advance.
    #[error("mcp runtime counter exhausted for server {server_id}: {counter}")]
    CounterExhausted {
        /// Server whose counter exhausted.
        server_id: String,
        /// Counter name.
        counter: &'static str,
    },
    /// Transition time regressed.
    #[error("mcp runtime time regressed for server {server_id}")]
    TimeRegression {
        /// Server whose timestamp regressed.
        server_id: String,
    },
    /// Catalog digest is malformed.
    #[error("invalid mcp catalog digest for server {server_id}")]
    InvalidCatalogDigest {
        /// Server whose digest was malformed.
        server_id: String,
    },
    /// Reconnect policy contains unsafe bounds.
    #[error("invalid mcp reconnect policy")]
    InvalidReconnectPolicy,
    /// Transition reason is malformed.
    #[error("invalid mcp runtime reason code")]
    InvalidReasonCode,
    /// Millisecond arithmetic exceeded supported time.
    #[error("mcp runtime timestamp overflow")]
    TimeOverflow,
    /// Durable transition event did not match adjacent records.
    #[error("invalid mcp runtime transition event")]
    InvalidEvent,
    /// Store returned two records for the same server.
    #[error("duplicate mcp runtime server {server_id}")]
    DuplicateServer {
        /// Duplicated server identity.
        server_id: String,
    },
    /// Requested server was not restored.
    #[error("unknown mcp runtime server {server_id}")]
    UnknownServer {
        /// Unknown server identity.
        server_id: String,
    },
    /// Applied projection did not immediately follow the current revision.
    #[error("stale mcp runtime projection for server {server_id}")]
    StaleProjection {
        /// Server identity.
        server_id: String,
        /// Required next revision.
        expected_revision: u64,
        /// Supplied revision.
        observed_revision: u64,
    },
    /// Conformance report is incomplete or malformed.
    #[error("invalid mcp conformance report")]
    InvalidConformanceReport,
    /// Durable store failed.
    #[error(transparent)]
    Store(#[from] McpRuntimeStoreError),
}

fn validate_admission_policy(
    policy: &McpDescriptorAdmissionPolicy,
) -> Result<(), McpDescriptorAdmissionError> {
    if policy.max_descriptor_bytes == 0
        || policy.max_schema_nodes == 0
        || policy.max_schema_depth == 0
        || policy.trusted_issuer_ids.is_empty()
        || policy.trusted_issuer_ids.iter().any(|issuer| !valid_identifier(issuer))
    {
        return Err(McpDescriptorAdmissionError::InvalidPolicy);
    }
    Ok(())
}

fn validate_registration_request(
    request: &TrustedExternalToolRegistrationRequest,
    policy: &McpDescriptorAdmissionPolicy,
) -> Result<(), McpDescriptorAdmissionError> {
    let descriptor = &request.descriptor;
    if !valid_identifier(&request.server_id)
        || request.runtime_generation == 0
        || request.catalog_epoch == 0
        || !valid_tool_name(&descriptor.name)
        || descriptor.description.len() > 16 * 1024
        || !valid_identifier(&descriptor.approval_class)
        || !valid_identifier(&request.attestation.issuer_id)
        || !valid_identifier(&request.attestation.key_id)
        || !valid_sha256(&request.attestation.descriptor_sha256)
        || request.attestation.signature.trim().is_empty()
        || request.attestation.signature.len() > 16 * 1024
    {
        return Err(McpDescriptorAdmissionError::InvalidDescriptor);
    }
    if descriptor.effect == McpToolEffectClassification::Mutating && !policy.allow_mutating_tools {
        return Err(McpDescriptorAdmissionError::MutatingToolDenied);
    }
    validate_json_shape(
        &descriptor.input_schema_json,
        policy.max_schema_nodes,
        policy.max_schema_depth,
    )?;
    if let Some(output_schema) = &descriptor.output_schema_json {
        validate_json_shape(output_schema, policy.max_schema_nodes, policy.max_schema_depth)?;
    }
    Ok(())
}

fn validate_json_shape(
    root: &Value,
    max_nodes: usize,
    max_depth: usize,
) -> Result<(), McpDescriptorAdmissionError> {
    let mut stack = vec![(root, 1_usize)];
    let mut nodes = 0_usize;
    while let Some((value, depth)) = stack.pop() {
        nodes = nodes.checked_add(1).ok_or(McpDescriptorAdmissionError::SchemaTooComplex)?;
        if nodes > max_nodes || depth > max_depth {
            return Err(McpDescriptorAdmissionError::SchemaTooComplex);
        }
        let child_depth =
            depth.checked_add(1).ok_or(McpDescriptorAdmissionError::SchemaTooComplex)?;
        match value {
            Value::Array(values) => {
                stack.extend(values.iter().map(|value| (value, child_depth)));
            }
            Value::Object(values) => {
                if values.keys().any(|key| key.len() > 256) {
                    return Err(McpDescriptorAdmissionError::InvalidDescriptor);
                }
                stack.extend(values.values().map(|value| (value, child_depth)));
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    Ok(())
}

fn valid_identifier(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | ':' | '/')
        })
}

fn valid_tool_name(value: &str) -> bool {
    valid_identifier(value) && value.contains('.')
}

fn valid_reason_code(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_REASON_CODE_BYTES
        && value.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '.' | '_' | '-')
        })
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_evidence_ref(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= MAX_SAFE_MESSAGE_BYTES
}

fn stable_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    const DIGEST_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const DIGEST_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn configured_record() -> McpServerRecordV2 {
        McpServerRecordV2::configured(
            "server-a".to_owned(),
            McpSessionTransportKind::Stdio,
            Some("vault-scope-a".to_owned()),
            "trusted-local".to_owned(),
            1,
        )
        .expect("fixture record is valid")
    }

    #[test]
    fn catalog_epoch_advances_only_for_changed_catalog() {
        let handshaking = configured_record().begin_handshake(2).expect("handshake starts");
        let ready = handshaking.mark_ready(DIGEST_A.to_owned(), 3).expect("session is ready");
        assert_eq!(ready.catalog_epoch, 1);

        let duplicate =
            ready.advance_catalog(Some(DIGEST_A.to_owned()), 4).expect("duplicate is accepted");
        assert_eq!(duplicate.revision, ready.revision);
        assert_eq!(duplicate.catalog_epoch, ready.catalog_epoch);

        let changed =
            ready.advance_catalog(Some(DIGEST_B.to_owned()), 4).expect("catalog advances");
        assert_eq!(changed.catalog_epoch, 2);
        assert_eq!(changed.revision, ready.revision + 1);
    }

    #[test]
    fn repeated_failures_quarantine_without_another_retry() {
        let policy =
            McpReconnectPolicy { quarantine_after_failures: 2, ..McpReconnectPolicy::default() };
        let handshaking = configured_record().begin_handshake(2).expect("handshake starts");
        let retry = handshaking
            .mark_failure(&policy, "mcp.runtime.transport.closed", 3)
            .expect("first failure retries");
        assert_eq!(retry.lifecycle, McpRuntimeLifecycleState::Reconnecting);
        assert!(retry.next_retry_at_unix_ms.is_some());

        let retrying = retry.begin_handshake(4_000).expect("retry starts");
        let quarantined = retrying
            .mark_failure(&policy, "mcp.runtime.transport.closed", 4_001)
            .expect("second failure quarantines");
        assert_eq!(quarantined.lifecycle, McpRuntimeLifecycleState::Quarantined);
        assert_eq!(
            quarantined.quarantine_reason_code.as_deref(),
            Some("mcp.runtime.transport.closed")
        );
        assert_eq!(quarantined.next_retry_at_unix_ms, None);
    }

    #[test]
    fn restart_reconnect_does_not_consume_failure_budget() {
        let handshaking = configured_record().begin_handshake(2).expect("handshake starts");
        let ready = handshaking.mark_ready(DIGEST_A.to_owned(), 3).expect("session is ready");
        let recovered = ready.recover_after_restart(4).expect("restart reconnect is planned");

        assert_eq!(recovered.lifecycle, McpRuntimeLifecycleState::Reconnecting);
        assert_eq!(recovered.consecutive_failures, 0);
        assert_eq!(recovered.next_retry_at_unix_ms, Some(4));
    }

    struct AcceptingVerifier;

    impl McpDescriptorTrustVerifier for AcceptingVerifier {
        fn verify(
            &self,
            request: &TrustedExternalToolRegistrationRequest,
            _canonical_descriptor_sha256: &str,
        ) -> Result<McpVerifiedDescriptorIdentity, McpDescriptorAdmissionError> {
            Ok(McpVerifiedDescriptorIdentity {
                issuer_id: request.attestation.issuer_id.clone(),
                key_id: request.attestation.key_id.clone(),
            })
        }
    }

    struct CountingVerifier {
        calls: AtomicUsize,
    }

    impl McpDescriptorTrustVerifier for CountingVerifier {
        fn verify(
            &self,
            request: &TrustedExternalToolRegistrationRequest,
            _canonical_descriptor_sha256: &str,
        ) -> Result<McpVerifiedDescriptorIdentity, McpDescriptorAdmissionError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(McpVerifiedDescriptorIdentity {
                issuer_id: request.attestation.issuer_id.clone(),
                key_id: request.attestation.key_id.clone(),
            })
        }
    }

    fn descriptor_request() -> TrustedExternalToolRegistrationRequest {
        let descriptor = McpExternalToolDescriptor {
            name: "test.read".to_owned(),
            description: "Reads a deterministic fixture.".to_owned(),
            input_schema_json: serde_json::json!({
                "type": "object",
                "properties": {"path": {"type": "string"}}
            }),
            output_schema_json: None,
            effect: McpToolEffectClassification::ReadOnly,
            approval_class: "read_only".to_owned(),
        };
        let digest = hex::encode(Sha256::digest(
            serde_json::to_vec(&descriptor).expect("descriptor serializes"),
        ));
        TrustedExternalToolRegistrationRequest {
            server_id: "server-a".to_owned(),
            runtime_generation: 2,
            catalog_epoch: 3,
            descriptor,
            attestation: McpDescriptorAttestation {
                issuer_id: "issuer-a".to_owned(),
                key_id: "key-a".to_owned(),
                descriptor_sha256: digest,
                signature: "verified-by-test-port".to_owned(),
            },
        }
    }

    #[test]
    fn descriptor_change_requires_reapproval() {
        let policy = McpDescriptorAdmissionPolicy {
            trusted_issuer_ids: BTreeSet::from(["issuer-a".to_owned()]),
            ..McpDescriptorAdmissionPolicy::default()
        };
        let first =
            admit_external_tool_descriptor(descriptor_request(), None, &policy, &AcceptingVerifier)
                .expect("first descriptor is admitted");
        assert!(first.requires_reapproval);

        let identical = admit_external_tool_descriptor(
            descriptor_request(),
            Some(&first),
            &policy,
            &AcceptingVerifier,
        )
        .expect("identical descriptor is admitted");
        assert!(!identical.requires_reapproval);

        let mut changed_request = descriptor_request();
        changed_request.descriptor.description = "Reads another fixture.".to_owned();
        changed_request.attestation.descriptor_sha256 = hex::encode(Sha256::digest(
            serde_json::to_vec(&changed_request.descriptor).expect("descriptor serializes"),
        ));
        let changed = admit_external_tool_descriptor(
            changed_request,
            Some(&identical),
            &policy,
            &AcceptingVerifier,
        )
        .expect("changed descriptor is admitted");
        assert!(changed.requires_reapproval);
    }

    #[test]
    fn schema_bombs_fail_before_trust_verification() {
        let policy = McpDescriptorAdmissionPolicy {
            max_schema_nodes: 8,
            max_schema_depth: 4,
            trusted_issuer_ids: BTreeSet::from(["issuer-a".to_owned()]),
            ..McpDescriptorAdmissionPolicy::default()
        };
        let verifier = CountingVerifier { calls: AtomicUsize::new(0) };

        let mut node_bomb = descriptor_request();
        node_bomb.descriptor.input_schema_json = serde_json::json!({
            "allOf": [
                {"type": "string"},
                {"type": "string"},
                {"type": "string"},
                {"type": "string"},
                {"type": "string"},
                {"type": "string"},
                {"type": "string"},
                {"type": "string"}
            ]
        });
        assert_eq!(
            admit_external_tool_descriptor(node_bomb, None, &policy, &verifier),
            Err(McpDescriptorAdmissionError::SchemaTooComplex)
        );

        let mut depth_bomb = descriptor_request();
        depth_bomb.descriptor.input_schema_json =
            serde_json::json!({"a": {"b": {"c": {"d": {"e": true}}}}});
        assert_eq!(
            admit_external_tool_descriptor(depth_bomb, None, &policy, &verifier),
            Err(McpDescriptorAdmissionError::SchemaTooComplex)
        );
        assert_eq!(
            verifier.calls.load(Ordering::Relaxed),
            0,
            "structural bounds must reject attacker-controlled schemas before trust work"
        );
    }

    #[test]
    fn conformance_requires_every_check_to_pass() {
        let mut report = McpConformanceReportV1 {
            schema_version: 1,
            server_id: "server-a".to_owned(),
            transport: McpSessionTransportKind::StreamableHttp,
            runtime_generation: 2,
            catalog_epoch: 3,
            started_at_unix_ms: 10,
            completed_at_unix_ms: 20,
            checks: [
                McpConformanceCheckKind::SingleOwnerGeneration,
                McpConformanceCheckKind::GenerationFencing,
                McpConformanceCheckKind::DurableRestore,
                McpConformanceCheckKind::CatalogEpochs,
                McpConformanceCheckKind::HostPolicyEnforcement,
                McpConformanceCheckKind::BoundedResources,
                McpConformanceCheckKind::CleanDrain,
            ]
            .into_iter()
            .map(|kind| McpConformanceCheck {
                kind,
                status: McpConformanceCheckStatus::Passed,
                evidence_ref: format!("artifact:{kind:?}"),
            })
            .collect(),
        };
        assert!(report.qualifies_for_production());

        report.checks[0].status = McpConformanceCheckStatus::NotRun;
        assert!(!report.qualifies_for_production());
    }
}
