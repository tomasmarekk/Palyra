use std::{
    path::{Component, PathBuf},
    time::Instant,
};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

pub const CANONICAL_PROTOCOL_MAJOR: u32 = 1;
pub const CANONICAL_JSON_ENVELOPE_VERSION: u32 = 1;

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
    pub event: String,
    pub source: String,
    pub payload: Value,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WebhookPayloadError {
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
    let root: Value =
        serde_json::from_slice(input).map_err(|_| WebhookPayloadError::InvalidJson)?;
    let object = root.as_object().ok_or(WebhookPayloadError::NotAnObject)?;

    let event = read_required_string(object, "event")?;
    let source = read_required_string(object, "source")?;
    let payload = object.get("payload").ok_or(WebhookPayloadError::MissingField("payload"))?;
    if !payload.is_object() {
        return Err(WebhookPayloadError::InvalidType("payload"));
    }

    Ok(WebhookEnvelope { event, source, payload: payload.clone() })
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
            "event": "message.created",
            "source": "slack",
            "payload": { "channel": "C123", "text": "hello" }
        }"#;

        let parsed = parse_webhook_payload(payload).expect("payload should parse");
        assert_eq!(parsed.event, "message.created");
        assert_eq!(parsed.source, "slack");
        assert!(parsed.payload.is_object());
    }

    #[test]
    fn parse_webhook_payload_rejects_missing_payload() {
        let payload = br#"{
            "event": "message.created",
            "source": "slack"
        }"#;

        assert_eq!(
            parse_webhook_payload(payload),
            Err(WebhookPayloadError::MissingField("payload"))
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
