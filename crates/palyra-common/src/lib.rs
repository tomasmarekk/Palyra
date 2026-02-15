use std::{
    path::{Component, PathBuf},
    time::Instant,
};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

pub const CANONICAL_PROTOCOL_MAJOR: u32 = 1;
pub const CANONICAL_JSON_ENVELOPE_VERSION: u32 = 1;
const WEBHOOK_MAX_PAYLOAD_BYTES: usize = 1_048_576;
const WEBHOOK_ALLOWED_FIELDS: &[&str] =
    &["v", "id", "event", "source", "payload", "replay_protection", "limits"];
const WEBHOOK_REPLAY_PROTECTION_ALLOWED_FIELDS: &[&str] =
    &["nonce", "timestamp_unix_ms", "signature"];
const WEBHOOK_LIMITS_ALLOWED_FIELDS: &[&str] = &["max_payload_bytes"];

#[derive(Debug, Clone, Serialize)]
pub struct BuildMetadata {
    pub version: &'static str,
    pub git_hash: &'static str,
    pub build_profile: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub service: String,
    pub status: String,
    pub version: String,
    pub git_hash: String,
    pub build_profile: String,
    pub uptime_seconds: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigPathParseError {
    #[error("config path cannot be empty")]
    Empty,
    #[error("config path contains an embedded NUL byte")]
    EmbeddedNul,
    #[error("config path cannot contain parent directory traversal ('..')")]
    ParentTraversal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WebhookEnvelope {
    pub v: u32,
    pub id: String,
    pub event: String,
    pub source: String,
    pub payload: Value,
    pub replay_protection: ReplayProtection,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayProtection {
    pub nonce: String,
    pub timestamp_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

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

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CanonicalIdError {
    #[error("canonical ID must be exactly 26 characters")]
    InvalidLength,
    #[error("canonical ID contains invalid character '{0}'")]
    InvalidCharacter(char),
}

#[must_use]
pub fn build_metadata() -> BuildMetadata {
    BuildMetadata {
        version: env!("CARGO_PKG_VERSION"),
        git_hash: option_env!("PALYRA_GIT_HASH").unwrap_or("unknown"),
        build_profile: if cfg!(debug_assertions) { "debug" } else { "release" },
    }
}

#[must_use]
pub fn health_response(service: &'static str, started_at: Instant) -> HealthResponse {
    let metadata = build_metadata();
    HealthResponse {
        service: service.to_owned(),
        status: "ok".to_owned(),
        version: metadata.version.to_owned(),
        git_hash: metadata.git_hash.to_owned(),
        build_profile: metadata.build_profile.to_owned(),
        uptime_seconds: started_at.elapsed().as_secs(),
    }
}

pub fn parse_config_path(raw: &str) -> Result<PathBuf, ConfigPathParseError> {
    if raw.trim().is_empty() {
        return Err(ConfigPathParseError::Empty);
    }
    if raw.contains('\0') {
        return Err(ConfigPathParseError::EmbeddedNul);
    }

    let path = PathBuf::from(raw);
    if path.components().any(|component| matches!(component, Component::ParentDir)) {
        return Err(ConfigPathParseError::ParentTraversal);
    }

    Ok(path)
}

pub fn parse_webhook_payload(input: &[u8]) -> Result<WebhookEnvelope, WebhookPayloadError> {
    if input.len() > WEBHOOK_MAX_PAYLOAD_BYTES {
        return Err(WebhookPayloadError::PayloadTooLarge { limit: WEBHOOK_MAX_PAYLOAD_BYTES });
    }

    let root: Value =
        serde_json::from_slice(input).map_err(|_| WebhookPayloadError::InvalidJson)?;
    let object = root.as_object().ok_or(WebhookPayloadError::NotAnObject)?;
    reject_additional_properties(object, WEBHOOK_ALLOWED_FIELDS, "envelope.additional_properties")?;
    validate_optional_limits(object)?;

    let version = read_required_u32(object, "v")?;
    if version != CANONICAL_JSON_ENVELOPE_VERSION {
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

    let replay_protection = read_replay_protection(object)?;

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

fn validate_optional_limits(object: &Map<String, Value>) -> Result<(), WebhookPayloadError> {
    let Some(limits_value) = object.get("limits") else {
        return Ok(());
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
    }

    Ok(())
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

pub fn validate_canonical_id(input: &str) -> Result<(), CanonicalIdError> {
    if input.len() != 26 {
        return Err(CanonicalIdError::InvalidLength);
    }
    for ch in input.chars() {
        if !is_valid_crockford_char(ch) {
            return Err(CanonicalIdError::InvalidCharacter(ch));
        }
    }
    Ok(())
}

fn is_valid_crockford_char(ch: char) -> bool {
    ch.is_ascii_digit() || matches!(ch, 'A'..='H' | 'J'..='K' | 'M'..='N' | 'P'..='T' | 'V'..='Z')
}

#[cfg(test)]
mod tests {
    use super::{
        parse_config_path, parse_webhook_payload, validate_canonical_id, CanonicalIdError,
        ConfigPathParseError, WebhookPayloadError,
    };

    #[test]
    fn parse_config_path_rejects_parent_traversal() {
        assert_eq!(
            parse_config_path("../config/palyra.toml"),
            Err(ConfigPathParseError::ParentTraversal)
        );
    }

    #[test]
    fn parse_config_path_accepts_relative_safe_path() {
        let path = parse_config_path("config/palyra.toml").expect("path should parse");
        assert_eq!(path.to_string_lossy(), "config/palyra.toml");
    }

    #[test]
    fn parse_webhook_payload_accepts_valid_envelope() {
        let payload = br#"{
            "v": 1,
            "id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "event": "message.created",
            "source": "slack",
            "payload": { "channel": "C123", "text": "hello" },
            "replay_protection": {
                "nonce": "1234567890abcdef",
                "timestamp_unix_ms": 1730000000000
            }
        }"#;

        let parsed = parse_webhook_payload(payload).expect("payload should parse");
        assert_eq!(parsed.v, 1);
        assert_eq!(parsed.id, "01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert_eq!(parsed.event, "message.created");
        assert_eq!(parsed.source, "slack");
        assert_eq!(parsed.replay_protection.nonce, "1234567890abcdef");
        assert_eq!(parsed.replay_protection.timestamp_unix_ms, 1_730_000_000_000);
        assert!(parsed.payload.is_object());
    }

    #[test]
    fn parse_webhook_payload_rejects_missing_payload() {
        let payload = br#"{
            "v": 1,
            "id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "event": "message.created",
            "source": "slack",
            "replay_protection": {
                "nonce": "1234567890abcdef",
                "timestamp_unix_ms": 1730000000000
            }
        }"#;

        assert_eq!(
            parse_webhook_payload(payload),
            Err(WebhookPayloadError::MissingField("payload"))
        );
    }

    #[test]
    fn parse_webhook_payload_rejects_missing_id() {
        let payload = br#"{
            "v": 1,
            "event": "message.created",
            "source": "slack",
            "payload": { "channel": "C123" },
            "replay_protection": {
                "nonce": "1234567890abcdef",
                "timestamp_unix_ms": 1730000000000
            }
        }"#;

        assert_eq!(parse_webhook_payload(payload), Err(WebhookPayloadError::MissingField("id")));
    }

    #[test]
    fn parse_webhook_payload_rejects_missing_replay_protection() {
        let payload = br#"{
            "v": 1,
            "id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "event": "message.created",
            "source": "slack",
            "payload": { "channel": "C123" }
        }"#;

        assert_eq!(
            parse_webhook_payload(payload),
            Err(WebhookPayloadError::MissingField("replay_protection"))
        );
    }

    #[test]
    fn parse_webhook_payload_rejects_invalid_version() {
        let payload = br#"{
            "v": 2,
            "id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "event": "message.created",
            "source": "slack",
            "payload": { "channel": "C123" },
            "replay_protection": {
                "nonce": "1234567890abcdef",
                "timestamp_unix_ms": 1730000000000
            }
        }"#;

        assert_eq!(parse_webhook_payload(payload), Err(WebhookPayloadError::InvalidValue("v")));
    }

    #[test]
    fn parse_webhook_payload_rejects_unknown_top_level_field() {
        let payload = br#"{
            "v": 1,
            "id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "event": "message.created",
            "source": "slack",
            "payload": { "channel": "C123" },
            "replay_protection": {
                "nonce": "1234567890abcdef",
                "timestamp_unix_ms": 1730000000000
            },
            "unexpected": true
        }"#;

        assert_eq!(
            parse_webhook_payload(payload),
            Err(WebhookPayloadError::InvalidValue("envelope.additional_properties"))
        );
    }

    #[test]
    fn parse_webhook_payload_rejects_unknown_replay_protection_field() {
        let payload = br#"{
            "v": 1,
            "id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "event": "message.created",
            "source": "slack",
            "payload": { "channel": "C123" },
            "replay_protection": {
                "nonce": "1234567890abcdef",
                "timestamp_unix_ms": 1730000000000,
                "unexpected": true
            }
        }"#;

        assert_eq!(
            parse_webhook_payload(payload),
            Err(WebhookPayloadError::InvalidValue("replay_protection.additional_properties"))
        );
    }

    #[test]
    fn parse_webhook_payload_accepts_limits_within_schema_range() {
        let payload = br#"{
            "v": 1,
            "id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "event": "message.created",
            "source": "slack",
            "payload": { "channel": "C123", "text": "hello" },
            "replay_protection": {
                "nonce": "1234567890abcdef",
                "timestamp_unix_ms": 1730000000000
            },
            "limits": {
                "max_payload_bytes": 1048576
            }
        }"#;

        let parsed = parse_webhook_payload(payload).expect("payload should parse");
        assert_eq!(parsed.v, 1);
    }

    #[test]
    fn parse_webhook_payload_rejects_oversized_input() {
        let payload = vec![b' '; 1_048_577];
        assert_eq!(
            parse_webhook_payload(payload.as_slice()),
            Err(WebhookPayloadError::PayloadTooLarge { limit: 1_048_576 })
        );
    }

    #[test]
    fn canonical_id_accepts_valid_ulid() {
        let valid = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        assert_eq!(validate_canonical_id(valid), Ok(()));
    }

    #[test]
    fn canonical_id_rejects_invalid_length() {
        assert_eq!(
            validate_canonical_id("01ARZ3NDEKTSV4RRFFQ69G5FA"),
            Err(CanonicalIdError::InvalidLength)
        );
    }

    #[test]
    fn canonical_id_rejects_invalid_characters() {
        assert_eq!(
            validate_canonical_id("01ARZ3NDEKTSV4RRFFQ69G5FAI"),
            Err(CanonicalIdError::InvalidCharacter('I'))
        );
    }
}
