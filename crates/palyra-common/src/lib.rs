use std::{
    env,
    ffi::OsString,
    path::{Component, PathBuf},
    time::Instant,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

pub mod daemon_config_schema;

pub const CANONICAL_PROTOCOL_MAJOR: u32 = 1;
pub const CANONICAL_JSON_ENVELOPE_VERSION: u32 = 1;
const WEBHOOK_MAX_PAYLOAD_BYTES: usize = 1_048_576;
const WEBHOOK_MAX_REPLAY_SKEW_MS: u64 = 5 * 60 * 1_000;
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

pub trait ReplayNonceStore {
    fn consume_once(&self, nonce: &str, timestamp_unix_ms: u64) -> Result<(), WebhookPayloadError>;
}

pub trait WebhookSignatureVerifier {
    fn verify(&self, payload_bytes: &[u8], signature: &str) -> Result<(), WebhookPayloadError>;
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

#[must_use]
pub fn default_config_search_paths() -> Vec<PathBuf> {
    #[cfg(windows)]
    {
        default_config_search_paths_from_env(env::var_os("APPDATA"), env::var_os("PROGRAMDATA"))
    }
    #[cfg(not(windows))]
    {
        default_config_search_paths_from_env(env::var_os("XDG_CONFIG_HOME"), env::var_os("HOME"))
    }
}

pub fn parse_daemon_bind_socket(
    bind_addr: &str,
    port: u16,
) -> Result<std::net::SocketAddr, std::net::AddrParseError> {
    if let Ok(ip) = bind_addr.parse::<std::net::IpAddr>() {
        return Ok(std::net::SocketAddr::new(ip, port));
    }
    format!("{bind_addr}:{port}").parse()
}

#[cfg(windows)]
fn default_config_search_paths_from_env(
    appdata: Option<OsString>,
    programdata: Option<OsString>,
) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(appdata) = appdata {
        paths.push(PathBuf::from(appdata).join("Palyra").join("palyra.toml"));
    }
    if let Some(programdata) = programdata {
        paths.push(PathBuf::from(programdata).join("Palyra").join("palyra.toml"));
    }
    paths
}

#[cfg(not(windows))]
fn default_config_search_paths_from_env(
    xdg_config_home: Option<OsString>,
    home: Option<OsString>,
) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(xdg_config_home) = xdg_config_home {
        paths.push(PathBuf::from(xdg_config_home).join("palyra").join("palyra.toml"));
    } else if let Some(home) = home {
        paths.push(PathBuf::from(home).join(".config").join("palyra").join("palyra.toml"));
    }
    paths.push(PathBuf::from("/etc/palyra/palyra.toml"));
    paths
}

pub fn parse_webhook_payload(input: &[u8]) -> Result<WebhookEnvelope, WebhookPayloadError> {
    let now_unix_ms = current_unix_ms()
        .map_err(|_| WebhookPayloadError::InvalidValue("replay_protection.timestamp_unix_ms"))?;
    parse_webhook_payload_with_now(input, now_unix_ms)
}

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

fn parse_webhook_payload_with_now(
    input: &[u8],
    now_unix_ms: u64,
) -> Result<WebhookEnvelope, WebhookPayloadError> {
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
    use std::{
        collections::HashSet,
        ffi::OsString,
        net::SocketAddr,
        path::PathBuf,
        sync::Mutex,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{
        default_config_search_paths_from_env, parse_config_path, parse_daemon_bind_socket,
        parse_webhook_payload_with_now, validate_canonical_id, verify_webhook_payload,
        CanonicalIdError, ConfigPathParseError, ReplayNonceStore, WebhookEnvelope,
        WebhookPayloadError, WebhookSignatureVerifier,
    };

    const REFERENCE_NOW_UNIX_MS: u64 = 1_730_000_000_000;

    fn parse_with_reference_now(payload: &[u8]) -> Result<WebhookEnvelope, WebhookPayloadError> {
        parse_webhook_payload_with_now(payload, REFERENCE_NOW_UNIX_MS)
    }

    #[derive(Default)]
    struct InMemoryReplayNonceStore {
        consumed_nonces: Mutex<HashSet<String>>,
    }

    impl ReplayNonceStore for InMemoryReplayNonceStore {
        fn consume_once(
            &self,
            nonce: &str,
            _timestamp_unix_ms: u64,
        ) -> Result<(), WebhookPayloadError> {
            let mut guard = self
                .consumed_nonces
                .lock()
                .map_err(|_| WebhookPayloadError::InvalidValue("replay_protection.nonce"))?;
            if !guard.insert(nonce.to_owned()) {
                return Err(WebhookPayloadError::InvalidValue("replay_protection.nonce"));
            }
            Ok(())
        }
    }

    struct PrefixSignatureVerifier;

    impl WebhookSignatureVerifier for PrefixSignatureVerifier {
        fn verify(
            &self,
            _payload_bytes: &[u8],
            signature: &str,
        ) -> Result<(), WebhookPayloadError> {
            if signature == "sig:test-valid" {
                Ok(())
            } else {
                Err(WebhookPayloadError::InvalidValue("replay_protection.signature"))
            }
        }
    }

    fn now_unix_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_millis() as u64
    }

    fn build_webhook_payload(
        nonce: &str,
        timestamp_unix_ms: u64,
        signature: Option<&str>,
    ) -> Vec<u8> {
        let signature_field =
            signature.map(|value| format!(r#","signature":"{value}""#)).unwrap_or_default();
        format!(
            r#"{{
            "v": 1,
            "id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "event": "message.created",
            "source": "slack",
            "payload": {{"channel": "C123", "text": "hello"}},
            "replay_protection": {{
                "nonce": "{nonce}",
                "timestamp_unix_ms": {timestamp_unix_ms}
                {signature_field}
            }}
        }}"#
        )
        .into_bytes()
    }

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

    #[cfg(not(windows))]
    #[test]
    fn default_config_search_paths_prefers_xdg_and_includes_etc() {
        let paths = default_config_search_paths_from_env(
            Some(OsString::from("/tmp/xdg-config")),
            Some(OsString::from("/tmp/home")),
        );
        assert_eq!(
            paths,
            vec![
                PathBuf::from("/tmp/xdg-config").join("palyra").join("palyra.toml"),
                PathBuf::from("/etc/palyra/palyra.toml"),
            ]
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn default_config_search_paths_falls_back_to_home_when_xdg_missing() {
        let paths = default_config_search_paths_from_env(None, Some(OsString::from("/tmp/home")));
        assert_eq!(
            paths,
            vec![
                PathBuf::from("/tmp/home").join(".config").join("palyra").join("palyra.toml"),
                PathBuf::from("/etc/palyra/palyra.toml"),
            ]
        );
    }

    #[cfg(windows)]
    #[test]
    fn default_config_search_paths_uses_appdata_and_programdata() {
        let paths = default_config_search_paths_from_env(
            Some(OsString::from(r"C:\Users\Test\AppData\Roaming")),
            Some(OsString::from(r"C:\ProgramData")),
        );
        assert_eq!(
            paths,
            vec![
                PathBuf::from(r"C:\Users\Test\AppData\Roaming").join("Palyra").join("palyra.toml"),
                PathBuf::from(r"C:\ProgramData").join("Palyra").join("palyra.toml"),
            ]
        );
    }

    #[test]
    fn parse_daemon_bind_socket_accepts_valid_loopback_endpoint() {
        let parsed = parse_daemon_bind_socket("127.0.0.1", 7142)
            .expect("loopback bind endpoint should parse");
        assert_eq!(
            parsed,
            "127.0.0.1:7142".parse::<SocketAddr>().expect("expected endpoint should parse"),
        );
    }

    #[test]
    fn parse_daemon_bind_socket_rejects_invalid_bind_host() {
        let result = parse_daemon_bind_socket("bad host value", 7142);
        assert!(result.is_err(), "invalid bind host should be rejected");
    }

    #[test]
    fn parse_daemon_bind_socket_accepts_ipv6_loopback_without_brackets() {
        let parsed = parse_daemon_bind_socket("::1", 7142)
            .expect("ipv6 loopback bind endpoint should parse");
        assert_eq!(
            parsed,
            "[::1]:7142".parse::<SocketAddr>().expect("expected endpoint should parse"),
        );
    }

    #[test]
    fn parse_daemon_bind_socket_accepts_non_loopback_ipv6_without_brackets() {
        let parsed =
            parse_daemon_bind_socket("2001:db8::1", 7142).expect("ipv6 bind endpoint should parse");
        assert_eq!(
            parsed,
            "[2001:db8::1]:7142".parse::<SocketAddr>().expect("expected endpoint should parse"),
        );
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

        let parsed = parse_with_reference_now(payload).expect("payload should parse");
        assert_eq!(parsed.v, 1);
        assert_eq!(parsed.id, "01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert_eq!(parsed.event, "message.created");
        assert_eq!(parsed.source, "slack");
        assert_eq!(parsed.replay_protection.nonce, "1234567890abcdef");
        assert_eq!(parsed.replay_protection.timestamp_unix_ms, 1_730_000_000_000);
        assert!(parsed.payload.is_object());
    }

    #[test]
    fn parse_webhook_payload_rejects_stale_replay_timestamp() {
        let stale_timestamp = REFERENCE_NOW_UNIX_MS - super::WEBHOOK_MAX_REPLAY_SKEW_MS - 1;
        let payload = format!(
            r#"{{
            "v": 1,
            "id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "event": "message.created",
            "source": "slack",
            "payload": {{ "channel": "C123" }},
            "replay_protection": {{
                "nonce": "1234567890abcdef",
                "timestamp_unix_ms": {stale_timestamp}
            }}
        }}"#
        );

        assert_eq!(
            parse_with_reference_now(payload.as_bytes()),
            Err(WebhookPayloadError::InvalidValue("replay_protection.timestamp_unix_ms"))
        );
    }

    #[test]
    fn parse_webhook_payload_rejects_future_replay_timestamp() {
        let future_timestamp = REFERENCE_NOW_UNIX_MS + super::WEBHOOK_MAX_REPLAY_SKEW_MS + 1;
        let payload = format!(
            r#"{{
            "v": 1,
            "id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "event": "message.created",
            "source": "slack",
            "payload": {{ "channel": "C123" }},
            "replay_protection": {{
                "nonce": "1234567890abcdef",
                "timestamp_unix_ms": {future_timestamp}
            }}
        }}"#
        );

        assert_eq!(
            parse_with_reference_now(payload.as_bytes()),
            Err(WebhookPayloadError::InvalidValue("replay_protection.timestamp_unix_ms"))
        );
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
            parse_with_reference_now(payload),
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

        assert_eq!(parse_with_reference_now(payload), Err(WebhookPayloadError::MissingField("id")));
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
            parse_with_reference_now(payload),
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

        assert_eq!(parse_with_reference_now(payload), Err(WebhookPayloadError::InvalidValue("v")));
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
            parse_with_reference_now(payload),
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
            parse_with_reference_now(payload),
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

        let parsed = parse_with_reference_now(payload).expect("payload should parse");
        assert_eq!(parsed.v, 1);
    }

    #[test]
    fn parse_webhook_payload_rejects_when_declared_limit_is_lower_than_payload_size() {
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
                "max_payload_bytes": 64
            }
        }"#;

        assert_eq!(
            parse_with_reference_now(payload),
            Err(WebhookPayloadError::InvalidValue("limits.max_payload_bytes"))
        );
    }

    #[test]
    fn parse_webhook_payload_rejects_oversized_input() {
        let payload = vec![b' '; 1_048_577];
        assert_eq!(
            parse_with_reference_now(payload.as_slice()),
            Err(WebhookPayloadError::PayloadTooLarge { limit: 1_048_576 })
        );
    }

    #[test]
    fn verify_webhook_payload_rejects_missing_signature() {
        let payload = build_webhook_payload("1234567890abcdef", now_unix_ms(), None);
        let nonce_store = InMemoryReplayNonceStore::default();
        let verifier = PrefixSignatureVerifier;

        assert_eq!(
            verify_webhook_payload(payload.as_slice(), &nonce_store, &verifier),
            Err(WebhookPayloadError::MissingField("replay_protection.signature"))
        );
    }

    #[test]
    fn verify_webhook_payload_rejects_invalid_signature() {
        let payload = build_webhook_payload("1234567890abcdef", now_unix_ms(), Some("sig:invalid"));
        let nonce_store = InMemoryReplayNonceStore::default();
        let verifier = PrefixSignatureVerifier;

        assert_eq!(
            verify_webhook_payload(payload.as_slice(), &nonce_store, &verifier),
            Err(WebhookPayloadError::InvalidValue("replay_protection.signature"))
        );
    }

    #[test]
    fn verify_webhook_payload_rejects_duplicate_nonce() {
        let payload =
            build_webhook_payload("1234567890abcdef", now_unix_ms(), Some("sig:test-valid"));
        let nonce_store = InMemoryReplayNonceStore::default();
        let verifier = PrefixSignatureVerifier;

        let first = verify_webhook_payload(payload.as_slice(), &nonce_store, &verifier);
        assert!(first.is_ok(), "fresh nonce should succeed");
        let second = verify_webhook_payload(payload.as_slice(), &nonce_store, &verifier);
        assert_eq!(second, Err(WebhookPayloadError::InvalidValue("replay_protection.nonce")));
    }

    #[test]
    fn verify_webhook_payload_accepts_valid_signature_and_fresh_nonce() {
        let payload =
            build_webhook_payload("abcdef1234567890", now_unix_ms(), Some("sig:test-valid"));
        let nonce_store = InMemoryReplayNonceStore::default();
        let verifier = PrefixSignatureVerifier;

        let result = verify_webhook_payload(payload.as_slice(), &nonce_store, &verifier);
        assert!(result.is_ok(), "valid signature and nonce should pass verification");
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
