//! Operator-managed inbound webhook integration registry.
//!
//! [`WebhookRegistry`] persists integrations in an owner-only TOML document
//! under the daemon state root and validates test payloads end to end:
//! envelope structure, source/event allowlists, payload-size budget,
//! HMAC-SHA256 signature against a vault-held secret, single-use replay
//! nonces, and the safety-boundary scan for untrusted content. Signing
//! secrets are referenced by vault ref only and never stored here. Consumed
//! by the control-plane console surfaces and the operator CLI.

use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use palyra_common::{
    parse_webhook_payload, verify_webhook_payload, ReplayNonceStore, WebhookPayloadError,
    WebhookSignatureVerifier,
};
use palyra_control_plane as control_plane;
use palyra_safety::{
    inspect_text, SafetyAction, SafetyContentKind, SafetyPhase, SafetySourceKind, TrustLabel,
};
use palyra_vault::{ensure_owner_only_dir, ensure_owner_only_file, Vault, VaultError, VaultRef};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

const REGISTRY_VERSION: u32 = 1;
const REGISTRY_FILE: &str = "webhooks.toml";
const MAX_WEBHOOK_COUNT: usize = 1_024;
const MAX_IDENTIFIER_BYTES: usize = 64;
const MAX_PROVIDER_BYTES: usize = 64;
const MAX_DISPLAY_NAME_BYTES: usize = 128;
const MAX_ALLOWED_FILTERS: usize = 64;
const MAX_FILTER_VALUE_BYTES: usize = 128;
const DEFAULT_MAX_PAYLOAD_BYTES: u64 = 64 * 1024;
const MAX_WEBHOOK_PAYLOAD_BYTES: u64 = 1_048_576;
const MAX_REPLAY_NONCES: usize = 8_192;
const HMAC_SHA256_BLOCK_BYTES: usize = 64;

/// Optional provider/enabled filters for listing integrations; `None`
/// fields match everything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookIntegrationListFilter {
    pub provider: Option<String>,
    pub enabled: Option<bool>,
}

/// Create-or-update request for one integration; normalized and validated
/// before it touches the registry. `max_payload_bytes = 0` selects the
/// default budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookIntegrationSetRequest {
    pub integration_id: String,
    pub provider: String,
    pub display_name: Option<String>,
    pub secret_vault_ref: String,
    pub allowed_events: Vec<String>,
    pub allowed_sources: Vec<String>,
    pub enabled: bool,
    pub signature_required: bool,
    pub max_payload_bytes: u64,
}

/// Result of a webhook test: the refreshed integration view plus the
/// detailed per-gate validation result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookIntegrationTestOutcome {
    pub integration: control_plane::WebhookIntegrationView,
    pub result: control_plane::WebhookIntegrationTestResult,
}

/// Persisted integration row; holds only the vault reference to the
/// signing secret, never the secret itself.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct WebhookIntegrationRecord {
    integration_id: String,
    provider: String,
    display_name: String,
    secret_vault_ref: String,
    #[serde(default)]
    allowed_events: Vec<String>,
    #[serde(default)]
    allowed_sources: Vec<String>,
    enabled: bool,
    signature_required: bool,
    max_payload_bytes: u64,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_test_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_test_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_test_at_unix_ms: Option<i64>,
}

/// Snapshot of whether an integration's signing secret is resolvable; an
/// integration may only be enabled while ready.
#[derive(Debug, Clone, PartialEq, Eq)]
struct WebhookReadiness {
    secret_status: WebhookSecretStatus,
    secret_present: bool,
    issues: Vec<String>,
}

impl WebhookReadiness {
    #[must_use]
    fn is_ready(&self) -> bool {
        self.secret_status == WebhookSecretStatus::Present && self.issues.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WebhookSecretStatus {
    InvalidRef,
    Missing,
    Present,
}

/// In-memory single-use nonce ledger for webhook test verification.
///
/// Process-local by design: it backs operator-triggered tests, not the
/// production ingest path, so nonces resetting on daemon restart is
/// acceptable.
#[derive(Debug, Default)]
struct WebhookReplayNonceStore {
    consumed: Mutex<BTreeMap<String, u64>>,
}

impl ReplayNonceStore for WebhookReplayNonceStore {
    fn consume_once(&self, nonce: &str, timestamp_unix_ms: u64) -> Result<(), WebhookPayloadError> {
        let mut consumed = self
            .consumed
            .lock()
            .map_err(|_| WebhookPayloadError::InvalidValue("replay_protection.nonce"))?;
        if consumed.contains_key(nonce) {
            return Err(WebhookPayloadError::InvalidValue("replay_protection.nonce"));
        }
        consumed.insert(nonce.to_owned(), timestamp_unix_ms);
        // Evict the oldest-timestamped nonce to bound memory; the freshest
        // nonces (the ones still worth replaying) stay protected longest.
        if consumed.len() > MAX_REPLAY_NONCES {
            let oldest_nonce = consumed
                .iter()
                .min_by_key(|(_, timestamp)| *timestamp)
                .map(|(nonce, _)| nonce.clone());
            if let Some(oldest_nonce) = oldest_nonce {
                consumed.remove(oldest_nonce.as_str());
            }
        }
        Ok(())
    }
}

/// HMAC-SHA256 verifier bound to a secret loaded from the vault for the
/// duration of one verification.
#[derive(Debug, Clone)]
struct VaultHmacSha256WebhookVerifier {
    secret: Vec<u8>,
}

impl WebhookSignatureVerifier for VaultHmacSha256WebhookVerifier {
    fn verify(&self, payload_bytes: &[u8], signature: &str) -> Result<(), WebhookPayloadError> {
        let signed_payload = webhook_signature_payload(payload_bytes)?;
        let expected_signature = hmac_sha256(self.secret.as_slice(), signed_payload.as_slice());
        let supplied_signature = decode_webhook_signature(signature)?;
        if constant_time_eq(expected_signature.as_slice(), supplied_signature.as_slice()) {
            Ok(())
        } else {
            Err(WebhookPayloadError::InvalidValue("replay_protection.signature"))
        }
    }
}

/// Aggregate registry posture for diagnostics surfaces: counts by enabled
/// state and secret readiness, plus the sorted provider list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WebhookDiagnosticsSnapshot {
    pub total: usize,
    pub enabled: usize,
    pub ready: usize,
    pub disabled: usize,
    pub invalid_secret_ref: usize,
    pub secret_not_found: usize,
    pub providers: Vec<String>,
}

/// File-backed registry of webhook integrations.
///
/// The TOML document and its long-lived file handle live behind separate
/// mutexes; every mutation rewrites the whole document before returning.
#[derive(Debug)]
pub struct WebhookRegistry {
    registry_path: RegistryPath,
    registry_file: Mutex<fs::File>,
    state: Mutex<RegistryDocument>,
    replay_nonces: WebhookReplayNonceStore,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegistryDocument {
    version: u32,
    #[serde(default)]
    webhooks: Vec<WebhookIntegrationRecord>,
}

impl Default for RegistryDocument {
    fn default() -> Self {
        Self { version: REGISTRY_VERSION, webhooks: Vec::new() }
    }
}

#[derive(Debug, Clone)]
struct RegistryPath {
    path: PathBuf,
}

impl RegistryPath {
    fn as_path(&self) -> &Path {
        self.path.as_path()
    }

    fn to_path_buf(&self) -> PathBuf {
        self.path.clone()
    }
}

/// Failure modes of webhook registry IO, parsing, and validation.
#[derive(Debug, Error)]
pub enum WebhookRegistryError {
    #[error("webhook registry lock poisoned")]
    LockPoisoned,
    #[error("failed to read webhook registry {path}: {source}")]
    ReadRegistry {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse webhook registry {path}: {source}")]
    ParseRegistry {
        path: PathBuf,
        #[source]
        source: Box<toml::de::Error>,
    },
    #[error("failed to write webhook registry {path}: {source}")]
    WriteRegistry {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize webhook registry: {0}")]
    SerializeRegistry(#[from] toml::ser::Error),
    #[error("unsupported webhook registry version {0}")]
    UnsupportedVersion(u32),
    #[error("webhook integration not found: {0}")]
    IntegrationNotFound(String),
    #[error("invalid {field}: {message}")]
    InvalidField { field: &'static str, message: String },
    #[error("too many webhook integrations configured")]
    RegistryLimitExceeded,
    #[error("system time before unix epoch: {0}")]
    InvalidSystemTime(#[from] std::time::SystemTimeError),
}

impl WebhookRegistry {
    /// Opens (or creates) the registry document under `state_root`,
    /// enforcing owner-only permissions on the directory and file.
    ///
    /// # Errors
    /// Returns read/parse/write variants on IO or TOML failure,
    /// [`WebhookRegistryError::UnsupportedVersion`] for unknown document
    /// versions, and [`WebhookRegistryError::RegistryLimitExceeded`] for
    /// oversized documents.
    pub fn open(state_root: &Path) -> Result<Self, WebhookRegistryError> {
        let registry_path = resolve_registry_path(state_root)?;
        let mut registry_file = open_registry_file(&registry_path)?;
        let document = load_registry_document(&registry_path, &mut registry_file)?;
        Ok(Self {
            registry_path,
            registry_file: Mutex::new(registry_file),
            state: Mutex::new(document),
            replay_nonces: WebhookReplayNonceStore::default(),
        })
    }

    /// Lists integration views matching `filter`, with secret readiness
    /// evaluated against `vault`.
    ///
    /// # Errors
    /// Returns [`WebhookRegistryError::LockPoisoned`] or
    /// [`WebhookRegistryError::InvalidField`] for a malformed provider
    /// filter.
    pub fn list_views(
        &self,
        filter: WebhookIntegrationListFilter,
        vault: &Vault,
    ) -> Result<Vec<control_plane::WebhookIntegrationView>, WebhookRegistryError> {
        let normalized_provider = match filter.provider.as_deref() {
            Some(provider) => Some(normalize_provider(provider)?),
            None => None,
        };
        let enabled_filter = filter.enabled;
        let state = self.state.lock().map_err(|_| WebhookRegistryError::LockPoisoned)?;
        let mut views = Vec::with_capacity(MAX_WEBHOOK_COUNT);
        for record in state.webhooks.iter().take(MAX_WEBHOOK_COUNT) {
            let provider_matches = match normalized_provider.as_ref() {
                Some(provider) => provider == &record.provider,
                None => true,
            };
            let enabled_matches = match enabled_filter {
                Some(enabled) => enabled == record.enabled,
                None => true,
            };
            if !provider_matches || !enabled_matches {
                continue;
            }
            views.push(self.view_from_record(record, vault)?);
        }
        Ok(views)
    }

    /// Returns one integration view, `None` when the id is unknown.
    ///
    /// # Errors
    /// Returns [`WebhookRegistryError::LockPoisoned`] or
    /// [`WebhookRegistryError::InvalidField`] for a malformed id.
    pub fn get_view(
        &self,
        integration_id: &str,
        vault: &Vault,
    ) -> Result<Option<control_plane::WebhookIntegrationView>, WebhookRegistryError> {
        let normalized = normalize_identifier(integration_id, "integration_id")?;
        let state = self.state.lock().map_err(|_| WebhookRegistryError::LockPoisoned)?;
        let Some(record) = state.webhooks.iter().find(|record| record.integration_id == normalized)
        else {
            return Ok(None);
        };
        Ok(Some(self.view_from_record(record, vault)?))
    }

    /// Creates or updates an integration and persists the registry.
    ///
    /// An integration cannot be enabled (or stay enabled) unless its
    /// signing secret resolves in the vault -- enabling fails closed.
    ///
    /// # Errors
    /// Returns [`WebhookRegistryError::InvalidField`] for malformed input
    /// or an unready integration being enabled,
    /// [`WebhookRegistryError::RegistryLimitExceeded`] at the registry
    /// cap, and lock/persistence variants otherwise.
    pub fn set_integration(
        &self,
        request: WebhookIntegrationSetRequest,
        vault: &Vault,
    ) -> Result<control_plane::WebhookIntegrationView, WebhookRegistryError> {
        let normalized = normalize_set_request(request)?;
        let mut state = self.state.lock().map_err(|_| WebhookRegistryError::LockPoisoned)?;
        let now = unix_ms_now()?;

        if let Some(existing) = state
            .webhooks
            .iter_mut()
            .find(|record| record.integration_id == normalized.integration_id)
        {
            existing.provider = normalized.provider;
            existing.display_name =
                normalized.display_name.clone().unwrap_or_else(|| existing.integration_id.clone());
            existing.secret_vault_ref = normalized.secret_vault_ref;
            existing.allowed_events = normalized.allowed_events;
            existing.allowed_sources = normalized.allowed_sources;
            existing.enabled = normalized.enabled;
            existing.signature_required = normalized.signature_required;
            existing.max_payload_bytes = normalized.max_payload_bytes;
            existing.updated_at_unix_ms = now;
            let readiness = evaluate_readiness(existing, vault);
            if existing.enabled && !readiness.is_ready() {
                return Err(WebhookRegistryError::InvalidField {
                    field: "secret_vault_ref",
                    message: readiness.issues.first().cloned().unwrap_or_else(|| {
                        "webhook integration is not ready to be enabled".to_owned()
                    }),
                });
            }
            let view = self.view_from_record(existing, vault)?;
            persist_registry(&self.registry_path, &self.registry_file, &state)?;
            return Ok(view);
        }

        if state.webhooks.len() >= MAX_WEBHOOK_COUNT {
            return Err(WebhookRegistryError::RegistryLimitExceeded);
        }

        let record = WebhookIntegrationRecord {
            integration_id: normalized.integration_id.clone(),
            provider: normalized.provider,
            display_name: normalized.display_name.unwrap_or(normalized.integration_id),
            secret_vault_ref: normalized.secret_vault_ref,
            allowed_events: normalized.allowed_events,
            allowed_sources: normalized.allowed_sources,
            enabled: normalized.enabled,
            signature_required: normalized.signature_required,
            max_payload_bytes: normalized.max_payload_bytes,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            last_test_status: None,
            last_test_message: None,
            last_test_at_unix_ms: None,
        };
        let readiness = evaluate_readiness(&record, vault);
        if record.enabled && !readiness.is_ready() {
            return Err(WebhookRegistryError::InvalidField {
                field: "secret_vault_ref",
                message: readiness
                    .issues
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "webhook integration is not ready to be enabled".to_owned()),
            });
        }
        let view = self.view_from_record(&record, vault)?;
        state.webhooks.push(record);
        state.webhooks.sort_by(|left, right| left.integration_id.cmp(&right.integration_id));
        persist_registry(&self.registry_path, &self.registry_file, &state)?;
        Ok(view)
    }

    /// Enables or disables an integration; enabling requires the signing
    /// secret to be resolvable (disabling always succeeds).
    ///
    /// # Errors
    /// Returns [`WebhookRegistryError::IntegrationNotFound`] for unknown
    /// ids, [`WebhookRegistryError::InvalidField`] when enabling an
    /// unready integration, and lock/persistence variants otherwise.
    pub fn set_enabled(
        &self,
        integration_id: &str,
        enabled: bool,
        vault: &Vault,
    ) -> Result<control_plane::WebhookIntegrationView, WebhookRegistryError> {
        let normalized = normalize_identifier(integration_id, "integration_id")?;
        let mut state = self.state.lock().map_err(|_| WebhookRegistryError::LockPoisoned)?;
        let record =
            state
                .webhooks
                .iter_mut()
                .find(|record| record.integration_id == normalized)
                .ok_or_else(|| WebhookRegistryError::IntegrationNotFound(normalized.clone()))?;
        if enabled {
            let readiness = evaluate_readiness(record, vault);
            if !readiness.is_ready() {
                return Err(WebhookRegistryError::InvalidField {
                    field: "secret_vault_ref",
                    message: readiness.issues.first().cloned().unwrap_or_else(|| {
                        "webhook integration is not ready to be enabled".to_owned()
                    }),
                });
            }
        }
        record.enabled = enabled;
        record.updated_at_unix_ms = unix_ms_now()?;
        let view = self.view_from_record(record, vault)?;
        persist_registry(&self.registry_path, &self.registry_file, &state)?;
        Ok(view)
    }

    /// Deletes an integration; returns `false` when nothing matched (the
    /// registry is only rewritten on an actual deletion).
    ///
    /// # Errors
    /// Returns [`WebhookRegistryError::InvalidField`] for a malformed id
    /// and lock/persistence variants otherwise.
    pub fn delete_integration(&self, integration_id: &str) -> Result<bool, WebhookRegistryError> {
        let normalized = normalize_identifier(integration_id, "integration_id")?;
        let mut state = self.state.lock().map_err(|_| WebhookRegistryError::LockPoisoned)?;
        let before_len = state.webhooks.len();
        state.webhooks.retain(|record| record.integration_id != normalized);
        let deleted = state.webhooks.len() != before_len;
        if deleted {
            persist_registry(&self.registry_path, &self.registry_file, &state)?;
        }
        Ok(deleted)
    }

    /// Runs a payload through every acceptance gate the integration would
    /// apply (structure, safety scan, allowlists, size budget, signature,
    /// replay nonce) and records the outcome on the integration.
    ///
    /// The test never errors on a bad payload -- gate failures are
    /// reported inside the returned result with a stable `outcome` string.
    ///
    /// # Errors
    /// Returns [`WebhookRegistryError::IntegrationNotFound`] for unknown
    /// ids and lock/clock/persistence variants otherwise.
    pub fn test_integration(
        &self,
        integration_id: &str,
        payload_bytes: &[u8],
        vault: &Vault,
    ) -> Result<WebhookIntegrationTestOutcome, WebhookRegistryError> {
        let normalized = normalize_identifier(integration_id, "integration_id")?;
        let mut state = self.state.lock().map_err(|_| WebhookRegistryError::LockPoisoned)?;
        let record =
            state
                .webhooks
                .iter_mut()
                .find(|record| record.integration_id == normalized)
                .ok_or_else(|| WebhookRegistryError::IntegrationNotFound(normalized.clone()))?;
        let readiness = evaluate_readiness(record, vault);
        let payload_scan = inspect_text(
            String::from_utf8_lossy(payload_bytes).as_ref(),
            SafetyPhase::PrePrompt,
            SafetySourceKind::Webhook,
            SafetyContentKind::WebhookPayload,
            TrustLabel::ExternalUntrusted,
        );
        let safety_findings = payload_scan.finding_codes();
        let safety_blocked = payload_scan.recommended_action == SafetyAction::Block;
        let safety_requires_review =
            payload_scan.recommended_action == SafetyAction::RequireApproval;
        let mut issues = Vec::<String>::new();
        if !record.enabled {
            issues.push("integration is disabled".to_owned());
        }
        if safety_blocked {
            issues.push("payload blocked by safety boundary".to_owned());
        } else if safety_requires_review {
            issues.push("payload requires safety review before prompt use".to_owned());
        }

        let result = match parse_webhook_payload(payload_bytes) {
            Ok(envelope) => {
                let signature_present = envelope.replay_protection.signature.is_some();
                let source_allowed = record.allowed_sources.is_empty()
                    || record.allowed_sources.iter().any(|entry| entry == &envelope.source);
                if !source_allowed {
                    issues.push(format!(
                        "payload source '{}' is not in the configured allowlist",
                        envelope.source
                    ));
                }
                let event_allowed = record.allowed_events.is_empty()
                    || record.allowed_events.iter().any(|entry| entry == &envelope.event);
                if !event_allowed {
                    issues.push(format!(
                        "payload event '{}' is not in the configured allowlist",
                        envelope.event
                    ));
                }
                let max_payload_ok = u64::try_from(payload_bytes.len()).unwrap_or(u64::MAX)
                    <= record.max_payload_bytes;
                if !max_payload_ok {
                    issues.push(format!(
                        "payload exceeds configured max_payload_bytes ({})",
                        record.max_payload_bytes
                    ));
                }
                let mut signature_verified = !record.signature_required;
                if record.signature_required {
                    if !signature_present {
                        issues.push("payload signature is required but missing".to_owned());
                    // Verification consumes the single-use nonce, so it only
                    // runs once every cheaper gate has passed; a payload that
                    // would be rejected anyway must not burn its nonce.
                    } else if record.enabled
                        && readiness.is_ready()
                        && source_allowed
                        && event_allowed
                        && max_payload_ok
                        && !safety_blocked
                        && !safety_requires_review
                    {
                        match verify_webhook_test_payload(
                            record,
                            payload_bytes,
                            vault,
                            &self.replay_nonces,
                        ) {
                            Ok(()) => {
                                signature_verified = true;
                            }
                            Err(error) => {
                                issues.push(format!(
                                    "payload signature verification failed: {error}"
                                ));
                            }
                        }
                    }
                }
                issues.extend(readiness.issues.clone());
                let valid = record.enabled
                    && readiness.is_ready()
                    && source_allowed
                    && event_allowed
                    && max_payload_ok
                    && signature_verified
                    && !safety_blocked
                    && !safety_requires_review;
                let outcome = if valid {
                    "accepted"
                } else if !record.enabled {
                    "disabled"
                } else if !readiness.is_ready() {
                    "not_ready"
                } else if safety_blocked {
                    "safety_blocked"
                } else if safety_requires_review {
                    "safety_review_required"
                } else if record.signature_required && !signature_present {
                    "signature_missing"
                } else if record.signature_required && !signature_verified {
                    "signature_invalid"
                } else {
                    "rejected"
                };
                let message = if issues.is_empty() {
                    if record.signature_required {
                        "payload passed structural, signature, replay, and policy validation"
                            .to_owned()
                    } else {
                        "payload passed structural and policy validation".to_owned()
                    }
                } else {
                    issues.join("; ")
                };
                control_plane::WebhookIntegrationTestResult {
                    integration_id: record.integration_id.clone(),
                    valid,
                    outcome: outcome.to_owned(),
                    message,
                    payload_bytes: u32::try_from(payload_bytes.len()).unwrap_or(u32::MAX),
                    trust_label: payload_scan.trust_label.as_str().to_owned(),
                    safety_action: payload_scan.recommended_action.as_str().to_owned(),
                    safety_findings: safety_findings.clone(),
                    event: Some(envelope.event),
                    source: Some(envelope.source),
                    signature_present,
                    secret_present: readiness.secret_present,
                }
            }
            Err(error) => {
                issues.extend(readiness.issues.clone());
                issues.push(format!("payload validation failed: {error}"));
                control_plane::WebhookIntegrationTestResult {
                    integration_id: record.integration_id.clone(),
                    valid: false,
                    outcome: "invalid_payload".to_owned(),
                    message: issues.join("; "),
                    payload_bytes: u32::try_from(payload_bytes.len()).unwrap_or(u32::MAX),
                    trust_label: payload_scan.trust_label.as_str().to_owned(),
                    safety_action: payload_scan.recommended_action.as_str().to_owned(),
                    safety_findings,
                    event: None,
                    source: None,
                    signature_present: false,
                    secret_present: readiness.secret_present,
                }
            }
        };

        let now = unix_ms_now()?;
        record.last_test_status = Some(if result.valid { "passed" } else { "failed" }.to_owned());
        record.last_test_message = Some(result.message.clone());
        record.last_test_at_unix_ms = Some(now);
        record.updated_at_unix_ms = now;
        let integration = self.view_from_record(record, vault)?;
        persist_registry(&self.registry_path, &self.registry_file, &state)?;
        Ok(WebhookIntegrationTestOutcome { integration, result })
    }

    /// Aggregates registry posture: enabled/ready/disabled counts, secret
    /// failure counts, and the provider inventory.
    ///
    /// # Errors
    /// Returns [`WebhookRegistryError::LockPoisoned`] when the state lock
    /// is poisoned.
    pub fn summary(
        &self,
        vault: &Vault,
    ) -> Result<WebhookDiagnosticsSnapshot, WebhookRegistryError> {
        let state = self.state.lock().map_err(|_| WebhookRegistryError::LockPoisoned)?;
        let mut providers = Vec::<String>::new();
        let mut enabled = 0_usize;
        let mut ready = 0_usize;
        let mut disabled = 0_usize;
        let mut invalid_secret_ref = 0_usize;
        let mut secret_not_found = 0_usize;
        for record in &state.webhooks {
            if !providers.contains(&record.provider) {
                providers.push(record.provider.clone());
            }
            let readiness = evaluate_readiness(record, vault);
            if record.enabled {
                enabled = enabled.saturating_add(1);
                match readiness.secret_status {
                    WebhookSecretStatus::Present => ready = ready.saturating_add(1),
                    WebhookSecretStatus::InvalidRef => {
                        invalid_secret_ref = invalid_secret_ref.saturating_add(1)
                    }
                    WebhookSecretStatus::Missing => {
                        secret_not_found = secret_not_found.saturating_add(1)
                    }
                }
            } else {
                disabled = disabled.saturating_add(1);
            }
        }
        providers.sort();
        Ok(WebhookDiagnosticsSnapshot {
            total: state.webhooks.len(),
            enabled,
            ready,
            disabled,
            invalid_secret_ref,
            secret_not_found,
            providers,
        })
    }

    /// Alias of [`Self::summary`] used by the diagnostics surface.
    ///
    /// # Errors
    /// Same as [`Self::summary`].
    pub fn diagnostics_snapshot(
        &self,
        vault: &Vault,
    ) -> Result<WebhookDiagnosticsSnapshot, WebhookRegistryError> {
        self.summary(vault)
    }

    fn view_from_record(
        &self,
        record: &WebhookIntegrationRecord,
        vault: &Vault,
    ) -> Result<control_plane::WebhookIntegrationView, WebhookRegistryError> {
        let readiness = evaluate_readiness(record, vault);
        Ok(control_plane::WebhookIntegrationView {
            integration_id: record.integration_id.clone(),
            provider: record.provider.clone(),
            display_name: record.display_name.clone(),
            secret_vault_ref: record.secret_vault_ref.clone(),
            secret_present: readiness.secret_present,
            allowed_events: record.allowed_events.clone(),
            allowed_sources: record.allowed_sources.clone(),
            enabled: record.enabled,
            signature_required: record.signature_required,
            max_payload_bytes: record.max_payload_bytes,
            status: self.status_for(record, &readiness).to_owned(),
            created_at_unix_ms: record.created_at_unix_ms,
            updated_at_unix_ms: record.updated_at_unix_ms,
            last_test_status: record.last_test_status.clone(),
            last_test_message: record.last_test_message.clone(),
            last_test_at_unix_ms: record.last_test_at_unix_ms,
        })
    }

    fn status_for(
        &self,
        record: &WebhookIntegrationRecord,
        readiness: &WebhookReadiness,
    ) -> &'static str {
        if !record.enabled {
            "disabled"
        } else {
            match readiness.secret_status {
                WebhookSecretStatus::Present => "ready",
                WebhookSecretStatus::InvalidRef => "invalid_secret_ref",
                WebhookSecretStatus::Missing => "secret_not_found",
            }
        }
    }
}

/// Checks whether the record's signing secret resolves in the vault; any
/// vault error other than not-found is reported as a missing secret so
/// readiness still fails closed.
fn evaluate_readiness(record: &WebhookIntegrationRecord, vault: &Vault) -> WebhookReadiness {
    let parsed = match VaultRef::parse(record.secret_vault_ref.as_str()) {
        Ok(value) => value,
        Err(error) => {
            return WebhookReadiness {
                secret_status: WebhookSecretStatus::InvalidRef,
                secret_present: false,
                issues: vec![format!("secret_vault_ref is invalid: {error}")],
            };
        }
    };
    match vault.get_secret(&parsed.scope, parsed.key.as_str()) {
        Ok(_) => WebhookReadiness {
            secret_status: WebhookSecretStatus::Present,
            secret_present: true,
            issues: Vec::new(),
        },
        Err(VaultError::NotFound) => WebhookReadiness {
            secret_status: WebhookSecretStatus::Missing,
            secret_present: false,
            issues: vec!["referenced signing secret was not found in the vault".to_owned()],
        },
        Err(error) => WebhookReadiness {
            secret_status: WebhookSecretStatus::Missing,
            secret_present: false,
            issues: vec![format!("failed to read signing secret metadata: {error}")],
        },
    }
}

/// Verifies signature and replay protection for a test payload using the
/// vault-held secret; errors are stringly because they feed operator-facing
/// issue lists, not control flow.
fn verify_webhook_test_payload(
    record: &WebhookIntegrationRecord,
    payload_bytes: &[u8],
    vault: &Vault,
    nonce_store: &dyn ReplayNonceStore,
) -> Result<(), String> {
    let secret = read_webhook_signing_secret(record, vault)?;
    let verifier = VaultHmacSha256WebhookVerifier { secret };
    verify_webhook_payload(payload_bytes, nonce_store, &verifier)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn read_webhook_signing_secret(
    record: &WebhookIntegrationRecord,
    vault: &Vault,
) -> Result<Vec<u8>, String> {
    let parsed = VaultRef::parse(record.secret_vault_ref.as_str())
        .map_err(|error| format!("secret_vault_ref is invalid: {error}"))?;
    let secret = vault.get_secret(&parsed.scope, parsed.key.as_str()).map_err(|error| {
        if matches!(error, VaultError::NotFound) {
            "referenced signing secret was not found in the vault".to_owned()
        } else {
            format!("failed to read signing secret: {error}")
        }
    })?;
    if secret.is_empty() {
        return Err("signing secret is empty".to_owned());
    }
    Ok(secret)
}

/// Produces the byte sequence the HMAC covers: the payload re-serialized
/// with `replay_protection.signature` removed.
///
/// Verification therefore depends on serde_json's canonical object
/// ordering (sorted keys), not on the sender's raw byte layout -- senders
/// must sign the same canonical form.
fn webhook_signature_payload(payload_bytes: &[u8]) -> Result<Vec<u8>, WebhookPayloadError> {
    let mut value = serde_json::from_slice::<Value>(payload_bytes)
        .map_err(|_| WebhookPayloadError::InvalidJson)?;
    let replay_protection = value
        .get_mut("replay_protection")
        .and_then(Value::as_object_mut)
        .ok_or(WebhookPayloadError::InvalidType("replay_protection"))?;
    replay_protection.remove("signature");
    serde_json::to_vec(&value).map_err(|_| WebhookPayloadError::InvalidJson)
}

/// RFC 2104 HMAC-SHA256 over `payload` with `secret`.
// AIDEV-NOTE: hand-rolled HMAC (block-sized key pad, inner/outer hash).
// The construction is the standard one and is pinned by the signature
// tests, but if the workspace ever adopts the `hmac` crate this should be
// replaced rather than maintained -- do not "optimize" the XOR loops.
fn hmac_sha256(secret: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut key_block = [0_u8; HMAC_SHA256_BLOCK_BYTES];
    if secret.len() > HMAC_SHA256_BLOCK_BYTES {
        let digest = Sha256::digest(secret);
        key_block[..digest.len()].copy_from_slice(digest.as_slice());
    } else {
        key_block[..secret.len()].copy_from_slice(secret);
    }

    let mut inner_key = [0x36_u8; HMAC_SHA256_BLOCK_BYTES];
    let mut outer_key = [0x5c_u8; HMAC_SHA256_BLOCK_BYTES];
    for index in 0..HMAC_SHA256_BLOCK_BYTES {
        inner_key[index] ^= key_block[index];
        outer_key[index] ^= key_block[index];
    }

    let mut inner = Sha256::new();
    inner.update(inner_key);
    inner.update(payload);
    let inner_digest = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_key);
    outer.update(inner_digest);
    outer.finalize().to_vec()
}

/// Decodes a hex signature, tolerating the common `sha256=`/`hmac-sha256=`
/// (and `:`) prefixes providers use; anything but 32 decoded bytes is
/// rejected.
fn decode_webhook_signature(signature: &str) -> Result<Vec<u8>, WebhookPayloadError> {
    let trimmed = signature.trim();
    let encoded = trimmed
        .strip_prefix("hmac-sha256=")
        .or_else(|| trimmed.strip_prefix("sha256="))
        .or_else(|| trimmed.strip_prefix("hmac-sha256:"))
        .or_else(|| trimmed.strip_prefix("sha256:"))
        .unwrap_or(trimmed);
    let decoded = hex::decode(encoded)
        .map_err(|_| WebhookPayloadError::InvalidValue("replay_protection.signature"))?;
    if decoded.len() != 32 {
        return Err(WebhookPayloadError::InvalidValue("replay_protection.signature"));
    }
    Ok(decoded)
}

/// Compares two MACs without short-circuiting on the first mismatching
/// byte; a plain `==` would leak the matching prefix length through
/// timing. The early length check is safe because MAC length is public.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let diff = left.iter().zip(right.iter()).fold(0_u8, |acc, (left, right)| acc | (left ^ right));
    diff == 0
}

fn normalize_set_request(
    request: WebhookIntegrationSetRequest,
) -> Result<WebhookIntegrationSetRequest, WebhookRegistryError> {
    let integration_id = normalize_identifier(request.integration_id, "integration_id")?;
    let provider = normalize_provider(request.provider)?;
    let display_name = match request.display_name {
        Some(display_name) => {
            let normalized = display_name.trim();
            if normalized.is_empty() {
                return Err(WebhookRegistryError::InvalidField {
                    field: "display_name",
                    message: "display_name cannot be empty".to_owned(),
                });
            }
            if normalized.len() > MAX_DISPLAY_NAME_BYTES {
                return Err(WebhookRegistryError::InvalidField {
                    field: "display_name",
                    message: format!("display_name must be at most {MAX_DISPLAY_NAME_BYTES} bytes"),
                });
            }
            Some(normalized.to_owned())
        }
        None => Some(integration_id.clone()),
    };
    let secret_vault_ref = normalize_secret_vault_ref(request.secret_vault_ref)?;
    let allowed_events = normalize_allowed_values(request.allowed_events, "allowed_events")?;
    let allowed_sources = normalize_allowed_values(request.allowed_sources, "allowed_sources")?;
    let max_payload_bytes = if request.max_payload_bytes == 0 {
        DEFAULT_MAX_PAYLOAD_BYTES
    } else {
        request.max_payload_bytes
    };
    if max_payload_bytes > MAX_WEBHOOK_PAYLOAD_BYTES {
        return Err(WebhookRegistryError::InvalidField {
            field: "max_payload_bytes",
            message: format!("max_payload_bytes must be between 1 and {MAX_WEBHOOK_PAYLOAD_BYTES}"),
        });
    }
    Ok(WebhookIntegrationSetRequest {
        integration_id,
        provider,
        display_name,
        secret_vault_ref,
        allowed_events,
        allowed_sources,
        enabled: request.enabled,
        signature_required: request.signature_required,
        max_payload_bytes,
    })
}

fn normalize_identifier(
    raw: impl AsRef<str>,
    field: &'static str,
) -> Result<String, WebhookRegistryError> {
    let normalized = raw.as_ref().trim();
    if normalized.is_empty() {
        return Err(WebhookRegistryError::InvalidField {
            field,
            message: "value is required".to_owned(),
        });
    }
    if normalized.len() > MAX_IDENTIFIER_BYTES {
        return Err(WebhookRegistryError::InvalidField {
            field,
            message: format!("value must be at most {MAX_IDENTIFIER_BYTES} bytes"),
        });
    }
    let normalized = normalized.to_ascii_lowercase();
    if !normalized
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '-' | '_' | '.'))
    {
        return Err(WebhookRegistryError::InvalidField {
            field,
            message: "value must contain only lowercase ASCII letters, digits, '.', '-', or '_'"
                .to_owned(),
        });
    }
    Ok(normalized)
}

fn normalize_provider(raw: impl AsRef<str>) -> Result<String, WebhookRegistryError> {
    let normalized = raw.as_ref().trim();
    if normalized.is_empty() {
        return Err(WebhookRegistryError::InvalidField {
            field: "provider",
            message: "provider is required".to_owned(),
        });
    }
    if normalized.len() > MAX_PROVIDER_BYTES {
        return Err(WebhookRegistryError::InvalidField {
            field: "provider",
            message: format!("provider must be at most {MAX_PROVIDER_BYTES} bytes"),
        });
    }
    let normalized = normalized.to_ascii_lowercase();
    if !normalized
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '-' | '_' | '.'))
    {
        return Err(WebhookRegistryError::InvalidField {
            field: "provider",
            message: "provider must contain only lowercase ASCII letters, digits, '.', '-', or '_'"
                .to_owned(),
        });
    }
    Ok(normalized)
}

fn normalize_secret_vault_ref(raw: String) -> Result<String, WebhookRegistryError> {
    let normalized = raw.trim();
    if normalized.is_empty() {
        return Err(WebhookRegistryError::InvalidField {
            field: "secret_vault_ref",
            message: "secret_vault_ref is required".to_owned(),
        });
    }
    VaultRef::parse(normalized).map_err(|error| WebhookRegistryError::InvalidField {
        field: "secret_vault_ref",
        message: error.to_string(),
    })?;
    Ok(normalized.to_owned())
}

fn normalize_allowed_values(
    values: Vec<String>,
    field: &'static str,
) -> Result<Vec<String>, WebhookRegistryError> {
    if values.len() > MAX_ALLOWED_FILTERS {
        return Err(WebhookRegistryError::InvalidField {
            field,
            message: format!("at most {MAX_ALLOWED_FILTERS} values are supported"),
        });
    }
    let mut normalized = Vec::<String>::new();
    for raw in values {
        let value = raw.trim();
        if value.is_empty() {
            return Err(WebhookRegistryError::InvalidField {
                field,
                message: "filter entries cannot be empty".to_owned(),
            });
        }
        if value.len() > MAX_FILTER_VALUE_BYTES {
            return Err(WebhookRegistryError::InvalidField {
                field,
                message: format!("filter entries must be at most {MAX_FILTER_VALUE_BYTES} bytes"),
            });
        }
        if !normalized.iter().any(|entry| entry == value) {
            normalized.push(value.to_owned());
        }
    }
    Ok(normalized)
}

fn open_registry_file(registry_path: &RegistryPath) -> Result<fs::File, WebhookRegistryError> {
    let registry_path = registry_path.as_path();
    if let Some(parent) = registry_path.parent() {
        fs::create_dir_all(parent).map_err(|source| WebhookRegistryError::WriteRegistry {
            path: parent.to_path_buf(),
            source,
        })?;
        ensure_owner_only_dir(parent).map_err(|source| WebhookRegistryError::WriteRegistry {
            path: parent.to_path_buf(),
            source: std::io::Error::other(source.to_string()),
        })?;
    }
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(registry_path)
        .map_err(|source| WebhookRegistryError::WriteRegistry {
            path: registry_path.to_path_buf(),
            source,
        })?;
    ensure_owner_only_file(registry_path).map_err(|source| {
        WebhookRegistryError::WriteRegistry {
            path: registry_path.to_path_buf(),
            source: std::io::Error::other(source.to_string()),
        }
    })?;
    Ok(file)
}

fn load_registry_document(
    registry_path: &RegistryPath,
    registry_file: &mut fs::File,
) -> Result<RegistryDocument, WebhookRegistryError> {
    registry_file.seek(SeekFrom::Start(0)).map_err(|source| {
        WebhookRegistryError::ReadRegistry { path: registry_path.to_path_buf(), source }
    })?;
    let mut raw = String::new();
    registry_file.read_to_string(&mut raw).map_err(|source| {
        WebhookRegistryError::ReadRegistry { path: registry_path.to_path_buf(), source }
    })?;
    if raw.is_empty() {
        let document = RegistryDocument::default();
        write_registry_document(registry_path, registry_file, &document)?;
        return Ok(document);
    }
    let parsed = toml::from_str::<RegistryDocument>(&raw).map_err(|source| {
        WebhookRegistryError::ParseRegistry {
            path: registry_path.to_path_buf(),
            source: Box::new(source),
        }
    })?;
    if parsed.version != REGISTRY_VERSION {
        return Err(WebhookRegistryError::UnsupportedVersion(parsed.version));
    }
    validate_registry_document(&parsed)?;
    Ok(parsed)
}

fn persist_registry(
    registry_path: &RegistryPath,
    registry_file: &Mutex<fs::File>,
    document: &RegistryDocument,
) -> Result<(), WebhookRegistryError> {
    let mut registry_file = registry_file.lock().map_err(|_| WebhookRegistryError::LockPoisoned)?;
    write_registry_document(registry_path, &mut registry_file, document)
}

// AIDEV-NOTE: the document is rewritten in place (set_len(0) + write +
// sync on the long-lived handle), matching the registry pattern used by
// objectives.rs and routines.rs. A crash between truncate and sync leaves
// a partial document; switching to atomic temp-file rename would also
// change the owner-only permission and file-handle locking flow.
fn write_registry_document(
    registry_path: &RegistryPath,
    registry_file: &mut fs::File,
    document: &RegistryDocument,
) -> Result<(), WebhookRegistryError> {
    let encoded = toml::to_string_pretty(document)?;
    registry_file.set_len(0).map_err(|source| WebhookRegistryError::WriteRegistry {
        path: registry_path.to_path_buf(),
        source,
    })?;
    registry_file.seek(SeekFrom::Start(0)).map_err(|source| {
        WebhookRegistryError::WriteRegistry { path: registry_path.to_path_buf(), source }
    })?;
    registry_file.write_all(encoded.as_bytes()).map_err(|source| {
        WebhookRegistryError::WriteRegistry { path: registry_path.to_path_buf(), source }
    })?;
    registry_file.sync_all().map_err(|source| WebhookRegistryError::WriteRegistry {
        path: registry_path.to_path_buf(),
        source,
    })?;
    ensure_owner_only_file(registry_path.as_path()).map_err(|source| {
        WebhookRegistryError::WriteRegistry {
            path: registry_path.to_path_buf(),
            source: std::io::Error::other(source.to_string()),
        }
    })?;
    Ok(())
}

/// Canonicalizes the state root and confirms the registry file resolves
/// directly inside it, rejecting symlink or traversal escapes.
fn resolve_registry_path(state_root: &Path) -> Result<RegistryPath, WebhookRegistryError> {
    ensure_owner_only_dir(state_root).map_err(|source| WebhookRegistryError::WriteRegistry {
        path: state_root.to_path_buf(),
        source: std::io::Error::other(source.to_string()),
    })?;
    let canonical_state_root = fs::canonicalize(state_root).map_err(|source| {
        WebhookRegistryError::WriteRegistry { path: state_root.to_path_buf(), source }
    })?;
    let registry_path = canonical_state_root.join(REGISTRY_FILE);
    let registry_parent =
        registry_path.parent().ok_or_else(|| WebhookRegistryError::WriteRegistry {
            path: registry_path.clone(),
            source: std::io::Error::other("webhook registry path has no parent"),
        })?;
    if registry_parent != canonical_state_root {
        return Err(WebhookRegistryError::WriteRegistry {
            path: registry_path,
            source: std::io::Error::other("webhook registry path escapes the state root"),
        });
    }
    Ok(RegistryPath { path: registry_path })
}

fn validate_registry_document(document: &RegistryDocument) -> Result<(), WebhookRegistryError> {
    if document.webhooks.len() > MAX_WEBHOOK_COUNT {
        return Err(WebhookRegistryError::RegistryLimitExceeded);
    }
    Ok(())
}

fn unix_ms_now() -> Result<i64, WebhookRegistryError> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use palyra_common::CANONICAL_JSON_ENVELOPE_VERSION;
    use serde_json::json;
    use tempfile::tempdir;
    use ulid::Ulid;

    use palyra_vault::{BackendPreference, VaultConfig, VaultScope};

    #[test]
    fn registry_persists_and_deletes_integrations() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let state_root = temp.path().join("state");
        let identity_root = state_root.join("identity");
        fs::create_dir_all(&identity_root)?;

        let registry = WebhookRegistry::open(&state_root)?;
        let vault = Vault::open_with_config(VaultConfig {
            root: Some(state_root.join("vault")),
            identity_store_root: Some(identity_root),
            backend_preference: BackendPreference::EncryptedFile,
            ..VaultConfig::default()
        })?;
        vault.put_secret(&VaultScope::Global, "github_repo_a", b"super-secret")?;

        let integration = registry.set_integration(
            WebhookIntegrationSetRequest {
                integration_id: "github_repo_a".to_owned(),
                provider: "github".to_owned(),
                display_name: Some("GitHub Repo A".to_owned()),
                secret_vault_ref: "global/github_repo_a".to_owned(),
                allowed_events: vec!["push".to_owned()],
                allowed_sources: vec!["github.repo_a".to_owned()],
                enabled: true,
                signature_required: true,
                max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            },
            &vault,
        )?;
        assert_eq!(integration.integration_id, "github_repo_a");
        assert!(integration.secret_present);

        let reopened = WebhookRegistry::open(&state_root)?;
        let listed = reopened
            .list_views(WebhookIntegrationListFilter { provider: None, enabled: None }, &vault)?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].provider, "github");

        let deleted = reopened.delete_integration("github_repo_a")?;
        assert!(deleted);
        assert!(reopened
            .list_views(WebhookIntegrationListFilter { provider: None, enabled: None }, &vault)?
            .is_empty());
        Ok(())
    }

    #[test]
    fn registry_rejects_documents_that_exceed_limit() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let state_root = temp.path().join("state");
        fs::create_dir_all(&state_root)?;
        let registry_path = state_root.join(REGISTRY_FILE);
        let oversized = RegistryDocument {
            version: REGISTRY_VERSION,
            webhooks: (0..=MAX_WEBHOOK_COUNT)
                .map(|index| WebhookIntegrationRecord {
                    integration_id: format!("hook-{index}"),
                    provider: "github".to_owned(),
                    display_name: format!("Hook {index}"),
                    secret_vault_ref: "global/github_repo_a".to_owned(),
                    allowed_events: Vec::new(),
                    allowed_sources: Vec::new(),
                    enabled: true,
                    signature_required: true,
                    max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                    last_test_status: None,
                    last_test_message: None,
                    last_test_at_unix_ms: None,
                })
                .collect(),
        };
        fs::write(&registry_path, toml::to_string_pretty(&oversized)?)?;

        let error = WebhookRegistry::open(&state_root).expect_err("oversized registry must fail");
        assert!(
            matches!(error, WebhookRegistryError::RegistryLimitExceeded),
            "registry should reject documents above the configured cap: {error}"
        );
        Ok(())
    }

    #[test]
    fn webhook_test_integration_surfaces_safety_blocking() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp = tempdir()?;
        let state_root = temp.path().join("state");
        let identity_root = state_root.join("identity");
        fs::create_dir_all(&identity_root)?;

        let registry = WebhookRegistry::open(&state_root)?;
        let vault = Vault::open_with_config(VaultConfig {
            root: Some(state_root.join("vault")),
            identity_store_root: Some(identity_root),
            backend_preference: BackendPreference::EncryptedFile,
            ..VaultConfig::default()
        })?;
        vault.put_secret(&VaultScope::Global, "github_repo_a", b"super-secret")?;

        registry.set_integration(
            WebhookIntegrationSetRequest {
                integration_id: "github_repo_a".to_owned(),
                provider: "github".to_owned(),
                display_name: Some("GitHub Repo A".to_owned()),
                secret_vault_ref: "global/github_repo_a".to_owned(),
                allowed_events: vec!["push".to_owned()],
                allowed_sources: vec!["github.repo_a".to_owned()],
                enabled: true,
                signature_required: false,
                max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            },
            &vault,
        )?;

        let payload_bytes = serde_json::to_vec(&json!({
            "v": CANONICAL_JSON_ENVELOPE_VERSION,
            "id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "event": "push",
            "source": "github.repo_a",
            "payload": {
                "body": "Authorization: Bearer super-secret-token-value"
            },
            "replay_protection": {
                "nonce": "01ARZ3NDEKTSV4RRFFQ69G5FAA",
                "timestamp_unix_ms": unix_ms_now()? as u64
            }
        }))?;

        let outcome =
            registry.test_integration("github_repo_a", payload_bytes.as_slice(), &vault)?;
        assert!(!outcome.result.valid);
        assert_eq!(outcome.result.outcome, "safety_blocked");
        assert_eq!(outcome.result.trust_label, "external_untrusted");
        assert_eq!(outcome.result.safety_action, "block");
        assert!(
            outcome
                .result
                .safety_findings
                .iter()
                .any(|finding| finding.starts_with("secret_leak.")),
            "webhook safety findings should surface a secret leak classification"
        );
        Ok(())
    }

    #[test]
    fn webhook_test_integration_rejects_forged_signature() -> Result<(), Box<dyn std::error::Error>>
    {
        let (registry, vault, _temp) = test_registry_with_signed_webhook()?;
        let payload_bytes = serde_json::to_vec(&json!({
            "v": CANONICAL_JSON_ENVELOPE_VERSION,
            "id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "event": "push",
            "source": "github.repo_a",
            "payload": {
                "channel": "C123",
                "text": "hello"
            },
            "replay_protection": {
                "nonce": "01ARZ3NDEKTSV4RRFFQ69G5FAB",
                "timestamp_unix_ms": unix_ms_now()? as u64,
                "signature": "sig:test"
            }
        }))?;

        let outcome =
            registry.test_integration("github_repo_a", payload_bytes.as_slice(), &vault)?;

        assert!(!outcome.result.valid);
        assert_eq!(outcome.result.outcome, "signature_invalid");
        assert!(outcome.result.signature_present);
        assert_eq!(outcome.integration.last_test_status.as_deref(), Some("failed"));
        assert!(
            outcome.result.message.contains("payload signature verification failed"),
            "forged signature failure should be explicit: {}",
            outcome.result.message
        );
        Ok(())
    }

    #[test]
    fn webhook_test_integration_accepts_valid_signature_once(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (registry, vault, _temp) = test_registry_with_signed_webhook()?;
        let nonce = Ulid::new().to_string();
        let payload_bytes =
            signed_test_webhook_payload("github.repo_a", "push", nonce.as_str(), b"super-secret")?;

        let first = registry.test_integration("github_repo_a", payload_bytes.as_slice(), &vault)?;
        assert!(first.result.valid);
        assert_eq!(first.result.outcome, "accepted");
        assert_eq!(first.integration.last_test_status.as_deref(), Some("passed"));

        let replayed =
            registry.test_integration("github_repo_a", payload_bytes.as_slice(), &vault)?;
        assert!(!replayed.result.valid);
        assert_eq!(replayed.result.outcome, "signature_invalid");
        assert!(
            replayed.result.message.contains("replay_protection.nonce"),
            "replayed payload should fail nonce consumption: {}",
            replayed.result.message
        );
        Ok(())
    }

    fn test_registry_with_signed_webhook(
    ) -> Result<(WebhookRegistry, Vault, tempfile::TempDir), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let state_root = temp.path().join("state");
        let identity_root = state_root.join("identity");
        fs::create_dir_all(&identity_root)?;

        let registry = WebhookRegistry::open(&state_root)?;
        let vault = Vault::open_with_config(VaultConfig {
            root: Some(state_root.join("vault")),
            identity_store_root: Some(identity_root),
            backend_preference: BackendPreference::EncryptedFile,
            ..VaultConfig::default()
        })?;
        vault.put_secret(&VaultScope::Global, "github_repo_a", b"super-secret")?;

        registry.set_integration(
            WebhookIntegrationSetRequest {
                integration_id: "github_repo_a".to_owned(),
                provider: "github".to_owned(),
                display_name: Some("GitHub Repo A".to_owned()),
                secret_vault_ref: "global/github_repo_a".to_owned(),
                allowed_events: vec!["push".to_owned()],
                allowed_sources: vec!["github.repo_a".to_owned()],
                enabled: true,
                signature_required: true,
                max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            },
            &vault,
        )?;

        Ok((registry, vault, temp))
    }

    fn signed_test_webhook_payload(
        source: &str,
        event: &str,
        nonce: &str,
        secret: &[u8],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut payload = json!({
            "v": CANONICAL_JSON_ENVELOPE_VERSION,
            "id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "event": event,
            "source": source,
            "payload": {
                "channel": "C123",
                "text": "hello"
            },
            "replay_protection": {
                "nonce": nonce,
                "timestamp_unix_ms": unix_ms_now()? as u64
            }
        });
        let unsigned_payload = serde_json::to_vec(&payload)?;
        let signature = hex::encode(hmac_sha256(secret, unsigned_payload.as_slice()));
        payload["replay_protection"]["signature"] = json!(format!("sha256={signature}"));
        Ok(serde_json::to_vec(&payload)?)
    }
}
