//! Crate-internal tests for config paths, bind sockets, canonical IDs, and webhook
//! parsing/verification (including private clock-injected parse helpers).

use std::{
    collections::HashSet,
    ffi::OsString,
    net::SocketAddr,
    path::PathBuf,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    config::{
        default_config_search_paths_from_env, default_identity_store_root_from_env,
        default_state_root_from_env, parse_config_path,
    },
    ids::{validate_canonical_id, CanonicalIdError},
    net::parse_daemon_bind_socket,
    webhook::{
        parse_webhook_payload_with_now, verify_webhook_payload, ReplayNonceStore, WebhookEnvelope,
        WebhookPayloadError, WebhookSignatureVerifier, WEBHOOK_MAX_REPLAY_SKEW_MS,
    },
    ConfigPathParseError, IdentityStorePathError,
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
    fn verify(&self, _payload_bytes: &[u8], signature: &str) -> Result<(), WebhookPayloadError> {
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

fn test_nonce() -> String {
    format!("{:016x}", now_unix_ms())
}

fn build_webhook_payload(nonce: &str, timestamp_unix_ms: u64, signature: Option<&str>) -> Vec<u8> {
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

#[cfg(not(windows))]
#[test]
fn default_identity_store_root_prefers_xdg_state_home() {
    let path = default_identity_store_root_from_env(
        Some(OsString::from("/tmp/xdg-state")),
        Some(OsString::from("/tmp/home")),
    )
    .expect("xdg state home should produce a default identity root");
    assert_eq!(path, PathBuf::from("/tmp/xdg-state").join("palyra").join("identity"));
}

#[cfg(not(windows))]
#[test]
fn default_identity_store_root_falls_back_to_home_state_directory() {
    let path = default_identity_store_root_from_env(None, Some(OsString::from("/tmp/home")))
        .expect("home should produce a default identity root");
    assert_eq!(
        path,
        PathBuf::from("/tmp/home").join(".local").join("state").join("palyra").join("identity")
    );
}

#[cfg(not(windows))]
#[test]
fn default_state_root_uses_identity_parent_directory() {
    let path = default_state_root_from_env(
        Some(OsString::from("/tmp/xdg-state")),
        Some(OsString::from("/tmp/home")),
    )
    .expect("state root should resolve");
    assert_eq!(path, PathBuf::from("/tmp/xdg-state").join("palyra"));
}

#[cfg(not(windows))]
#[test]
fn default_identity_store_root_requires_home_when_xdg_is_missing() {
    assert_eq!(
        default_identity_store_root_from_env(None, None),
        Err(IdentityStorePathError::HomeNotSet)
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

#[cfg(windows)]
#[test]
fn default_identity_store_root_prefers_local_appdata() {
    let path = default_identity_store_root_from_env(
        Some(OsString::from(r"C:\Users\Test\AppData\Local")),
        Some(OsString::from(r"C:\Users\Test\AppData\Roaming")),
    )
    .expect("local appdata should produce a default identity root");
    assert_eq!(path, PathBuf::from(r"C:\Users\Test\AppData\Local").join("Palyra").join("identity"));
}

#[cfg(windows)]
#[test]
fn default_state_root_uses_identity_parent_directory() {
    let path = default_state_root_from_env(
        Some(OsString::from(r"C:\Users\Test\AppData\Local")),
        Some(OsString::from(r"C:\Users\Test\AppData\Roaming")),
    )
    .expect("state root should resolve");
    assert_eq!(path, PathBuf::from(r"C:\Users\Test\AppData\Local").join("Palyra"));
}

#[cfg(windows)]
#[test]
fn default_identity_store_root_requires_appdata_environment() {
    assert_eq!(
        default_identity_store_root_from_env(None, None),
        Err(IdentityStorePathError::AppDataNotSet)
    );
}

#[test]
fn parse_daemon_bind_socket_accepts_valid_loopback_endpoint() {
    let parsed =
        parse_daemon_bind_socket("127.0.0.1", 7142).expect("loopback bind endpoint should parse");
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
    let parsed =
        parse_daemon_bind_socket("::1", 7142).expect("ipv6 loopback bind endpoint should parse");
    assert_eq!(parsed, "[::1]:7142".parse::<SocketAddr>().expect("expected endpoint should parse"),);
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
    let stale_timestamp = REFERENCE_NOW_UNIX_MS - WEBHOOK_MAX_REPLAY_SKEW_MS - 1;
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
    let future_timestamp = REFERENCE_NOW_UNIX_MS + WEBHOOK_MAX_REPLAY_SKEW_MS + 1;
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
    let nonce = test_nonce();
    let payload = build_webhook_payload(&nonce, now_unix_ms(), None);
    let nonce_store = InMemoryReplayNonceStore::default();
    let verifier = PrefixSignatureVerifier;

    assert_eq!(
        verify_webhook_payload(payload.as_slice(), &nonce_store, &verifier),
        Err(WebhookPayloadError::MissingField("replay_protection.signature"))
    );
}

#[test]
fn verify_webhook_payload_rejects_invalid_signature() {
    let nonce = test_nonce();
    let payload = build_webhook_payload(&nonce, now_unix_ms(), Some("sig:invalid"));
    let nonce_store = InMemoryReplayNonceStore::default();
    let verifier = PrefixSignatureVerifier;

    assert_eq!(
        verify_webhook_payload(payload.as_slice(), &nonce_store, &verifier),
        Err(WebhookPayloadError::InvalidValue("replay_protection.signature"))
    );
}

#[test]
fn verify_webhook_payload_rejects_duplicate_nonce() {
    let nonce = test_nonce();
    let payload = build_webhook_payload(&nonce, now_unix_ms(), Some("sig:test-valid"));
    let nonce_store = InMemoryReplayNonceStore::default();
    let verifier = PrefixSignatureVerifier;

    let first = verify_webhook_payload(payload.as_slice(), &nonce_store, &verifier);
    assert!(first.is_ok(), "fresh nonce should succeed");
    let second = verify_webhook_payload(payload.as_slice(), &nonce_store, &verifier);
    assert_eq!(second, Err(WebhookPayloadError::InvalidValue("replay_protection.nonce")));
}

#[test]
fn verify_webhook_payload_accepts_valid_signature_and_fresh_nonce() {
    let nonce = test_nonce();
    let payload = build_webhook_payload(&nonce, now_unix_ms(), Some("sig:test-valid"));
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
