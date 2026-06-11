//! Strict webhook envelope parsing with replay protection and signature hooks.
//!
//! Validates untrusted inbound payloads field by field (closed field sets, size caps,
//! ±5-minute replay window) before any business logic sees them. Accept/reject behavior
//! and error values are contract surface exercised by the `webhook_payload_parser` and
//! `webhook_replay_verifier` fuzz targets — do not loosen or reorder checks.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::ids::validate_canonical_id;

const WEBHOOK_MAX_PAYLOAD_BYTES: usize = 1_048_576;
pub(crate) const WEBHOOK_MAX_REPLAY_SKEW_MS: u64 = 5 * 60 * 1_000;
const WEBHOOK_ALLOWED_FIELDS: &[&str] =
    &["v", "id", "event", "source", "payload", "replay_protection", "limits"];
const WEBHOOK_REPLAY_PROTECTION_ALLOWED_FIELDS: &[&str] =
    &["nonce", "timestamp_unix_ms", "signature"];
const WEBHOOK_LIMITS_ALLOWED_FIELDS: &[&str] = &["max_payload_bytes"];

/// A validated inbound webhook envelope (canonical JSON envelope v1).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WebhookEnvelope {
    pub v: u32,
    pub id: String,
    pub event: String,
    pub source: String,
    pub payload: Value,
    pub replay_protection: ReplayProtection,
}

/// Anti-replay metadata carried by every envelope: a one-time nonce, the sender timestamp,
/// and an optional transport signature.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayProtection {
    pub nonce: String,
    pub timestamp_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

/// Store that records nonces and rejects any nonce it has already seen.
pub trait ReplayNonceStore {
    /// Atomically records `nonce`; returns an error if it was consumed before.
    ///
    /// # Errors
    /// Implementations return a [`WebhookPayloadError`] for replayed nonces or store
    /// failures; both must reject the payload.
    fn consume_once(&self, nonce: &str, timestamp_unix_ms: u64) -> Result<(), WebhookPayloadError>;
}

/// Verifies a payload signature against the raw request bytes.
pub trait WebhookSignatureVerifier {
    /// Checks `signature` over `payload_bytes`.
    ///
    /// # Errors
    /// Implementations return a [`WebhookPayloadError`] when the signature does not
    /// validate; the payload must then be rejected.
    fn verify(&self, payload_bytes: &[u8], signature: &str) -> Result<(), WebhookPayloadError>;
}

/// Why a webhook payload was rejected; values are pinned contract surface.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum WebhookPayloadError {
    #[error("payload exceeds maximum size of {limit} bytes")]
    PayloadTooLarge { limit: usize },
    #[error("payload must be valid JSON")]
    InvalidJson,
    #[error("payload must be a JSON object")]
    NotAnObject,
    #[error("field '{0}' is required")]
    MissingField(&'static str),
    #[error("field '{0}' has an invalid type")]
    InvalidType(&'static str),
    #[error("field '{0}' cannot be empty")]
    EmptyField(&'static str),
    #[error("field '{0}' has an invalid value")]
    InvalidValue(&'static str),
}

/// Parses and validates a webhook payload against the current system clock.
///
/// # Errors
/// Returns a [`WebhookPayloadError`] for oversized input, malformed JSON, unknown fields,
/// invalid field values, or a replay timestamp outside the ±5-minute window.
pub fn parse_webhook_payload(input: &[u8]) -> Result<WebhookEnvelope, WebhookPayloadError> {
    let now_unix_ms = current_unix_ms()
        .map_err(|_| WebhookPayloadError::InvalidValue("replay_protection.timestamp_unix_ms"))?;
    parse_webhook_payload_with_now(input, now_unix_ms)
}

/// Parses a webhook payload and enforces signature plus one-time-nonce replay protection.
///
/// The signature is required here (unlike in [`parse_webhook_payload`]) and is verified
/// over the raw input bytes, not a re-serialized form, so there is no canonicalization
/// gap to exploit. The nonce is consumed only after the signature validates; otherwise
/// unauthenticated senders could burn nonces of legitimate pending deliveries.
///
/// # Errors
/// Returns a [`WebhookPayloadError`] for any parse failure, a missing or invalid
/// signature, or a replayed nonce.
pub fn verify_webhook_payload(
    input: &[u8],
    nonce_store: &dyn ReplayNonceStore,
    verifier: &dyn WebhookSignatureVerifier,
) -> Result<WebhookEnvelope, WebhookPayloadError> {
    let envelope = parse_webhook_payload(input)?;
    let signature = envelope
        .replay_protection
        .signature
        .as_deref()
        .ok_or(WebhookPayloadError::MissingField("replay_protection.signature"))?;
    verifier.verify(input, signature)?;
    nonce_store.consume_once(
        &envelope.replay_protection.nonce,
        envelope.replay_protection.timestamp_unix_ms,
    )?;
    Ok(envelope)
}

/// Clock-injectable parse core; `now_unix_ms` anchors the replay window in tests.
pub(crate) fn parse_webhook_payload_with_now(
    input: &[u8],
    now_unix_ms: u64,
) -> Result<WebhookEnvelope, WebhookPayloadError> {
    // Size check first: nothing else may touch the bytes before the cap is enforced.
    if input.len() > WEBHOOK_MAX_PAYLOAD_BYTES {
        return Err(WebhookPayloadError::PayloadTooLarge { limit: WEBHOOK_MAX_PAYLOAD_BYTES });
    }

    let root: Value =
        serde_json::from_slice(input).map_err(|_| WebhookPayloadError::InvalidJson)?;
    let object = root.as_object().ok_or(WebhookPayloadError::NotAnObject)?;
    reject_additional_properties(object, WEBHOOK_ALLOWED_FIELDS, "envelope.additional_properties")?;
    let declared_max_payload_bytes = validate_optional_limits(object)?;
    if let Some(max_payload_bytes) = declared_max_payload_bytes {
        if input.len() > max_payload_bytes as usize {
            return Err(WebhookPayloadError::InvalidValue("limits.max_payload_bytes"));
        }
    }

    let version = read_required_u32(object, "v")?;
    if version != crate::CANONICAL_JSON_ENVELOPE_VERSION {
        return Err(WebhookPayloadError::InvalidValue("v"));
    }
    let id = read_required_string(object, "id")?;
    validate_canonical_id(id.as_str()).map_err(|_| WebhookPayloadError::InvalidValue("id"))?;

    let event = read_required_string(object, "event")?;
    if event.len() > 128 {
        return Err(WebhookPayloadError::InvalidValue("event"));
    }
    let source = read_required_string(object, "source")?;
    if source.len() > 128 {
        return Err(WebhookPayloadError::InvalidValue("source"));
    }
    let payload = object.get("payload").ok_or(WebhookPayloadError::MissingField("payload"))?;
    let payload_object = payload.as_object().ok_or(WebhookPayloadError::InvalidType("payload"))?;
    if payload_object.len() > 2_048 {
        return Err(WebhookPayloadError::InvalidValue("payload"));
    }

    let replay_protection = read_replay_protection(object, now_unix_ms)?;

    Ok(WebhookEnvelope {
        v: version,
        id,
        event,
        source,
        payload: payload.clone(),
        replay_protection,
    })
}

fn read_replay_protection(
    object: &Map<String, Value>,
    now_unix_ms: u64,
) -> Result<ReplayProtection, WebhookPayloadError> {
    let replay_protection = object
        .get("replay_protection")
        .ok_or(WebhookPayloadError::MissingField("replay_protection"))?
        .as_object()
        .ok_or(WebhookPayloadError::InvalidType("replay_protection"))?;
    reject_additional_properties(
        replay_protection,
        WEBHOOK_REPLAY_PROTECTION_ALLOWED_FIELDS,
        "replay_protection.additional_properties",
    )?;

    let nonce = read_required_string(replay_protection, "nonce")?;
    if nonce.len() < 16 || nonce.len() > 128 {
        return Err(WebhookPayloadError::InvalidValue("replay_protection.nonce"));
    }

    let timestamp_unix_ms = replay_protection
        .get("timestamp_unix_ms")
        .ok_or(WebhookPayloadError::MissingField("replay_protection.timestamp_unix_ms"))?
        .as_u64()
        .ok_or(WebhookPayloadError::InvalidType("replay_protection.timestamp_unix_ms"))?;
    // The window is symmetric (past and future) so nonce stores only need to retain
    // nonces for a bounded interval; future-dated timestamps would otherwise let an
    // attacker replay a capture after the store evicts its nonce.
    let minimum_allowed = now_unix_ms.saturating_sub(WEBHOOK_MAX_REPLAY_SKEW_MS);
    let maximum_allowed = now_unix_ms.saturating_add(WEBHOOK_MAX_REPLAY_SKEW_MS);
    if timestamp_unix_ms < minimum_allowed || timestamp_unix_ms > maximum_allowed {
        return Err(WebhookPayloadError::InvalidValue("replay_protection.timestamp_unix_ms"));
    }

    let signature = match replay_protection.get("signature") {
        Some(value) => {
            let signature = value
                .as_str()
                .ok_or(WebhookPayloadError::InvalidType("replay_protection.signature"))?;
            if signature.len() > 4_096 {
                return Err(WebhookPayloadError::InvalidValue("replay_protection.signature"));
            }
            Some(signature.to_owned())
        }
        None => None,
    };

    Ok(ReplayProtection { nonce, timestamp_unix_ms, signature })
}

fn validate_optional_limits(
    object: &Map<String, Value>,
) -> Result<Option<u64>, WebhookPayloadError> {
    let Some(limits_value) = object.get("limits") else {
        return Ok(None);
    };
    let limits = limits_value.as_object().ok_or(WebhookPayloadError::InvalidType("limits"))?;
    reject_additional_properties(
        limits,
        WEBHOOK_LIMITS_ALLOWED_FIELDS,
        "limits.additional_properties",
    )?;

    if let Some(max_payload_bytes) = limits.get("max_payload_bytes") {
        let max_payload_bytes = max_payload_bytes
            .as_u64()
            .ok_or(WebhookPayloadError::InvalidType("limits.max_payload_bytes"))?;
        if max_payload_bytes == 0 || max_payload_bytes > WEBHOOK_MAX_PAYLOAD_BYTES as u64 {
            return Err(WebhookPayloadError::InvalidValue("limits.max_payload_bytes"));
        }
        return Ok(Some(max_payload_bytes));
    }

    Ok(None)
}

fn reject_additional_properties(
    object: &Map<String, Value>,
    allowed_fields: &[&str],
    field_name: &'static str,
) -> Result<(), WebhookPayloadError> {
    if object.keys().any(|key| !allowed_fields.contains(&key.as_str())) {
        return Err(WebhookPayloadError::InvalidValue(field_name));
    }
    Ok(())
}

fn current_unix_ms() -> Result<u64, std::time::SystemTimeError> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64)
}

fn read_required_u32(
    object: &Map<String, Value>,
    key: &'static str,
) -> Result<u32, WebhookPayloadError> {
    let value = object
        .get(key)
        .ok_or(WebhookPayloadError::MissingField(key))?
        .as_u64()
        .ok_or(WebhookPayloadError::InvalidType(key))?;
    value.try_into().map_err(|_| WebhookPayloadError::InvalidValue(key))
}

fn read_required_string(
    object: &Map<String, Value>,
    key: &'static str,
) -> Result<String, WebhookPayloadError> {
    let value = object
        .get(key)
        .ok_or(WebhookPayloadError::MissingField(key))?
        .as_str()
        .ok_or(WebhookPayloadError::InvalidType(key))?;
    if value.trim().is_empty() {
        return Err(WebhookPayloadError::EmptyField(key));
    }

    Ok(value.to_owned())
}
