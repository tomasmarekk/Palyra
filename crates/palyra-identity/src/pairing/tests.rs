//! Unit tests for the pairing lifecycle: handshake rejections, session hygiene, rate
//! limiting, persistence/reload consistency, rotation/revocation, lock reclaim, and
//! redaction of secret material in Debug output.

#[cfg(not(windows))]
use std::time::Instant;
use std::{
    collections::HashMap,
    fs,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use proptest::prelude::*;
use tempfile::TempDir;
use x25519_dalek::{PublicKey as X25519PublicKey, StaticSecret};

use super::{
    helpers::{certificate_fingerprint_hex, constant_time_eq, derive_transcript_mac},
    persistence::{
        initialize_filesystem_state_lock_marker_with_writer, try_reclaim_stale_filesystem_lock,
        IDENTITY_STATE_LOCK_FILENAME, MAX_ACTIVE_PAIRING_SESSIONS,
    },
    *,
};
use crate::{
    device::DeviceIdentity,
    error::{IdentityError, IdentityResult},
    store::SecretStore,
};

struct ToggleFailSecretStore {
    state: Mutex<HashMap<String, Vec<u8>>>,
    fail_writes: Mutex<bool>,
}

impl ToggleFailSecretStore {
    fn new() -> Self {
        Self { state: Mutex::new(HashMap::new()), fail_writes: Mutex::new(false) }
    }

    fn set_fail_writes(&self, value: bool) {
        let mut guard = self.fail_writes.lock().expect("fail_writes lock should not be poisoned");
        *guard = value;
    }
}

impl SecretStore for ToggleFailSecretStore {
    fn write_secret(&self, key: &str, value: &[u8]) -> IdentityResult<()> {
        self.insert_value(key, value)
    }

    fn write_sealed_value(&self, key: &str, value: &[u8]) -> IdentityResult<()> {
        self.insert_value(key, value)
    }

    fn read_secret(&self, key: &str) -> IdentityResult<Vec<u8>> {
        let state = self
            .state
            .lock()
            .map_err(|_| IdentityError::Internal("store state lock poisoned".to_owned()))?;
        state.get(key).cloned().ok_or(IdentityError::SecretNotFound)
    }

    fn delete_secret(&self, key: &str) -> IdentityResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| IdentityError::Internal("store state lock poisoned".to_owned()))?;
        state.remove(key);
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ToggleFailSecretStore {
    fn insert_value(&self, key: &str, value: &[u8]) -> IdentityResult<()> {
        let fail_writes = self
            .fail_writes
            .lock()
            .map_err(|_| IdentityError::Internal("store fail_writes lock poisoned".to_owned()))?;
        if *fail_writes {
            return Err(IdentityError::Internal("injected write failure".to_owned()));
        }
        drop(fail_writes);

        let mut state = self
            .state
            .lock()
            .map_err(|_| IdentityError::Internal("store state lock poisoned".to_owned()))?;
        state.insert(key.to_owned(), value.to_vec());
        Ok(())
    }
}

fn sample_device_id() -> &'static str {
    "01ARZ3NDEKTSV4RRFFQ69G5FAV"
}

fn sample_second_device_id() -> &'static str {
    "01ARZ3NDEKTSV4RRFFQ69G5FAW"
}

fn start_pin_pairing(
    manager: &mut IdentityManager,
    proof: &str,
) -> (DeviceIdentity, PairingSession, DevicePairingHello) {
    start_pin_pairing_for_device(manager, sample_device_id(), proof)
}

fn start_pin_pairing_for_device(
    manager: &mut IdentityManager,
    device_id: &str,
    proof: &str,
) -> (DeviceIdentity, PairingSession, DevicePairingHello) {
    let device = DeviceIdentity::generate(device_id).expect("device should generate");
    let session = manager
        .start_pairing(
            PairingClientKind::Node,
            PairingMethod::Pin { code: "123456".to_owned() },
            SystemTime::now(),
        )
        .expect("pairing session should start");
    let hello = manager.build_device_hello(&session, &device, proof).expect("hello should build");
    (device, session, hello)
}

#[test]
fn pairing_method_debug_redacts_pin_secret_material() {
    let formatted = format!("{:?}", PairingMethod::Pin { code: "123456".to_owned() });
    assert!(!formatted.contains("123456"), "debug output must not contain raw pin");
    assert!(formatted.contains("code_len"), "debug output should retain diagnostic shape");
}

#[test]
fn pairing_session_debug_redacts_embedded_method_secret_material() {
    let session = PairingSession {
        session_id: "session-test".to_owned(),
        protocol_version: 1,
        client_kind: PairingClientKind::Desktop,
        method: PairingMethod::Qr { token: "SECRET_TOKEN_VALUE".to_owned() },
        gateway_ephemeral_public: [0_u8; 32],
        challenge: [1_u8; 32],
        expires_at_unix_ms: 1,
    };
    let formatted = format!("{session:?}");
    assert!(!formatted.contains("SECRET_TOKEN_VALUE"), "debug output must redact raw token");
    assert!(formatted.contains("token_len"), "debug output should preserve safe diagnostics");
}

#[test]
fn device_pairing_hello_debug_redacts_secret_material() {
    let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
    let (_, _, hello) = start_pin_pairing(&mut manager, "123456");

    let formatted = format!("{hello:?}");
    assert!(!formatted.contains("123456"), "debug output must redact proof");
    assert!(formatted.contains("<redacted>"), "sensitive binary fields should be redacted");
    assert!(formatted.contains("proof_len"), "debug output should preserve safe diagnostics");
}

#[cfg(not(windows))]
#[test]
fn filesystem_lock_is_not_reclaimed_when_owner_pid_is_live() {
    let root = TempDir::new().expect("temp directory should be created");
    let store = Arc::new(
        crate::FilesystemSecretStore::new(root.path())
            .expect("filesystem secret store should initialize"),
    );
    let mut manager = IdentityManager::with_store(store)
        .expect("manager with filesystem store should initialize");
    let lock_path = root.path().join(IDENTITY_STATE_LOCK_FILENAME);
    fs::write(&lock_path, format!("pid={} ts_ms=0\n", std::process::id()))
        .expect("lock marker should be written");

    let started = Instant::now();
    let result = manager.issue_gateway_server_certificate("localhost");
    assert!(result.is_err(), "operation should fail when lock cannot be acquired");
    assert!(lock_path.exists(), "existing lock file must not be force deleted");
    assert!(
        started.elapsed() >= super::persistence::IDENTITY_STATE_LOCK_TIMEOUT,
        "lock acquisition should wait for timeout before failing"
    );
}

#[cfg(unix)]
#[test]
fn filesystem_lock_reclaims_stale_dead_owner_marker() {
    let root = TempDir::new().expect("temp directory should be created");
    let store = Arc::new(
        crate::FilesystemSecretStore::new(root.path())
            .expect("filesystem secret store should initialize"),
    );
    let mut manager = IdentityManager::with_store(store)
        .expect("manager with filesystem store should initialize");
    let lock_path = root.path().join(IDENTITY_STATE_LOCK_FILENAME);
    fs::write(&lock_path, format!("pid={} ts_ms=0\n", i32::MAX))
        .expect("stale lock marker should be written");

    let issued = manager.issue_gateway_server_certificate("localhost");
    assert!(issued.is_ok(), "stale dead lock marker should be reclaimed");
    assert!(!lock_path.exists(), "stale lock file should be removed after successful mutation");
}

#[test]
fn filesystem_lock_marker_init_failure_removes_lock_file() {
    let root = TempDir::new().expect("temp directory should be created");
    let lock_path = root.path().join(IDENTITY_STATE_LOCK_FILENAME);
    fs::write(&lock_path, b"seed-lock").expect("seed lock file should be written");

    let error = initialize_filesystem_state_lock_marker_with_writer(
        &lock_path,
        "pid=1 ts_ms=0\n",
        |_payload| Err(std::io::Error::other("injected marker write failure")),
    )
    .expect_err("marker initialization should fail when writer returns error");
    assert!(
        matches!(error, IdentityError::Internal(message) if message.contains("failed to initialize identity state lock marker")),
        "marker initialization error should include lock-path context"
    );
    assert!(!lock_path.exists(), "lock file should be removed when marker initialization fails");
}

#[test]
fn filesystem_lock_reacquire_succeeds_after_marker_init_failure() {
    let root = TempDir::new().expect("temp directory should be created");
    let store = Arc::new(
        crate::FilesystemSecretStore::new(root.path())
            .expect("filesystem secret store should initialize"),
    );
    let mut manager = IdentityManager::with_store(store)
        .expect("manager with filesystem store should initialize");
    let lock_path = root.path().join(IDENTITY_STATE_LOCK_FILENAME);
    fs::write(&lock_path, b"seed-lock").expect("seed lock file should be written");

    let error = initialize_filesystem_state_lock_marker_with_writer(
        &lock_path,
        "pid=1 ts_ms=0\n",
        |_payload| Err(std::io::Error::other("injected marker sync failure")),
    )
    .expect_err("marker initialization should fail for injected writer failure");
    assert!(
        matches!(error, IdentityError::Internal(message) if message.contains("failed to initialize identity state lock marker")),
        "marker initialization failure should surface as identity internal error"
    );
    assert!(!lock_path.exists(), "failed marker initialization must remove lock artifact");

    let issued = manager.issue_gateway_server_certificate("localhost");
    assert!(issued.is_ok(), "subsequent lock acquisition should succeed immediately after cleanup");
    assert!(!lock_path.exists(), "lock file should be released after successful mutation finishes");
}

#[cfg(windows)]
#[test]
fn filesystem_lock_reclaims_stale_dead_owner_marker_windows() {
    let root = TempDir::new().expect("temp directory should be created");
    let lock_path = root.path().join(IDENTITY_STATE_LOCK_FILENAME);
    fs::write(&lock_path, format!("pid={} ts_ms=0\n", i32::MAX))
        .expect("stale lock marker should be written");

    let reclaimed =
        try_reclaim_stale_filesystem_lock(&lock_path, SystemTime::now(), Duration::ZERO)
            .expect("reclaim evaluation should succeed");
    assert!(reclaimed, "stale dead lock marker should be reclaimed on windows");
    assert!(!lock_path.exists(), "stale lock file should be removed after reclaim");
}

#[cfg(windows)]
#[test]
fn filesystem_lock_keeps_stale_live_owner_marker_windows() {
    let root = TempDir::new().expect("temp directory should be created");
    let lock_path = root.path().join(IDENTITY_STATE_LOCK_FILENAME);
    fs::write(&lock_path, format!("pid={} ts_ms=0\n", std::process::id()))
        .expect("live lock marker should be written");

    let reclaimed =
        try_reclaim_stale_filesystem_lock(&lock_path, SystemTime::now(), Duration::ZERO)
            .expect("reclaim evaluation should succeed");
    assert!(!reclaimed, "live owner lock marker must not be reclaimed");
    assert!(lock_path.exists(), "live owner lock file must remain in place");
}

#[cfg(not(windows))]
#[test]
fn malformed_recent_filesystem_lock_marker_is_not_reclaimed() {
    let root = TempDir::new().expect("temp directory should be created");
    let lock_path = root.path().join(IDENTITY_STATE_LOCK_FILENAME);
    fs::write(&lock_path, "invalid lock marker payload")
        .expect("malformed lock marker should be written");

    let reclaimed =
        try_reclaim_stale_filesystem_lock(&lock_path, SystemTime::now(), Duration::from_secs(60))
            .expect("reclaim evaluation should succeed");
    assert!(!reclaimed, "fresh malformed lock marker should not be reclaimed");
    assert!(lock_path.exists(), "fresh malformed lock marker must remain in place");
}

#[cfg(not(windows))]
#[test]
fn malformed_stale_filesystem_lock_marker_is_reclaimed() {
    let root = TempDir::new().expect("temp directory should be created");
    let lock_path = root.path().join(IDENTITY_STATE_LOCK_FILENAME);
    fs::write(&lock_path, "invalid lock marker payload")
        .expect("malformed lock marker should be written");

    let reclaimed =
        try_reclaim_stale_filesystem_lock(&lock_path, SystemTime::now(), Duration::ZERO)
            .expect("reclaim evaluation should succeed");
    assert!(reclaimed, "stale malformed lock marker should be reclaimed");
    assert!(!lock_path.exists(), "stale malformed lock marker should be removed");
}

#[test]
fn pairing_rejects_downgrade_attempt() {
    let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
    let device = DeviceIdentity::generate(sample_device_id()).expect("device should generate");
    let session = manager
        .start_pairing(
            PairingClientKind::Node,
            PairingMethod::Pin { code: "123456".to_owned() },
            SystemTime::now(),
        )
        .expect("pairing session should start");
    let mut hello =
        manager.build_device_hello(&session, &device, "123456").expect("hello should build");
    hello.protocol_version = 0;

    let result = manager.complete_pairing(hello, SystemTime::now());
    assert!(matches!(result, Err(IdentityError::PairingVersionMismatch { expected: 1, got: 0 })));
}

#[test]
fn failed_proof_invalidates_pairing_session() {
    let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
    let (_, session, wrong_hello) = start_pin_pairing(&mut manager, "000000");

    let first = manager.complete_pairing(wrong_hello, SystemTime::now());
    assert!(matches!(first, Err(IdentityError::InvalidPairingProof)));

    let device = DeviceIdentity::generate(sample_device_id()).expect("device should generate");
    let retry =
        manager.build_device_hello(&session, &device, "123456").expect("retry hello should build");
    let second = manager.complete_pairing(retry, SystemTime::now());
    assert!(matches!(second, Err(IdentityError::PairingSessionNotFound)));
}

#[test]
fn failed_signature_invalidates_pairing_session() {
    let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
    let (_, session, mut wrong_hello) = start_pin_pairing(&mut manager, "123456");
    wrong_hello.challenge_signature[0] ^= 0x01;

    let first = manager.complete_pairing(wrong_hello, SystemTime::now());
    assert!(matches!(first, Err(IdentityError::SignatureVerificationFailed)));

    let device = DeviceIdentity::generate(sample_device_id()).expect("device should generate");
    let retry =
        manager.build_device_hello(&session, &device, "123456").expect("retry hello should build");
    let second = manager.complete_pairing(retry, SystemTime::now());
    assert!(matches!(second, Err(IdentityError::PairingSessionNotFound)));
}

#[test]
fn failed_transcript_mac_invalidates_pairing_session() {
    let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
    let (_, session, mut wrong_hello) = start_pin_pairing(&mut manager, "123456");
    wrong_hello.transcript_mac[0] ^= 0x01;

    let first = manager.complete_pairing(wrong_hello, SystemTime::now());
    assert!(matches!(first, Err(IdentityError::TranscriptVerificationFailed)));

    let device = DeviceIdentity::generate(sample_device_id()).expect("device should generate");
    let retry =
        manager.build_device_hello(&session, &device, "123456").expect("retry hello should build");
    let second = manager.complete_pairing(retry, SystemTime::now());
    assert!(matches!(second, Err(IdentityError::PairingSessionNotFound)));
}

#[test]
fn constant_time_eq_returns_true_for_equal_inputs() {
    assert!(constant_time_eq(b"123456", b"123456"));
}

#[test]
fn constant_time_eq_returns_false_for_different_same_length_inputs() {
    assert!(!constant_time_eq(b"123456", b"123457"));
}

#[test]
fn constant_time_eq_returns_false_for_different_length_inputs() {
    assert!(!constant_time_eq(b"123456", b"1234567"));
}

#[test]
fn start_pairing_prunes_expired_sessions() {
    let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
    manager.set_pairing_window(Duration::from_millis(1));
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

    let expired = manager
        .start_pairing(
            PairingClientKind::Node,
            PairingMethod::Pin { code: "123456".to_owned() },
            now,
        )
        .expect("expired candidate should be created");

    let fresh = manager
        .start_pairing(
            PairingClientKind::Node,
            PairingMethod::Pin { code: "123456".to_owned() },
            now + Duration::from_secs(1),
        )
        .expect("fresh session should be created");

    assert!(!manager.active_sessions.contains_key(expired.session_id.as_str()));
    assert!(manager.active_sessions.contains_key(fresh.session_id.as_str()));
    assert_eq!(manager.active_sessions.len(), 1);
}

#[test]
fn start_pairing_enforces_active_session_capacity() {
    let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

    let seed = manager
        .start_pairing(
            PairingClientKind::Node,
            PairingMethod::Pin { code: "123456".to_owned() },
            now,
        )
        .expect("seed session should be created");
    let seed_active = manager
        .active_sessions
        .get(seed.session_id.as_str())
        .cloned()
        .expect("seed session should be in active set");

    manager.active_sessions.clear();
    for index in 0..MAX_ACTIVE_PAIRING_SESSIONS {
        manager.active_sessions.insert(format!("session-{index}"), seed_active.clone());
    }

    let result = manager.start_pairing(
        PairingClientKind::Node,
        PairingMethod::Pin { code: "123456".to_owned() },
        now,
    );

    assert!(matches!(
        result,
        Err(IdentityError::PairingSessionCapacityExceeded { limit: MAX_ACTIVE_PAIRING_SESSIONS })
    ));
}

#[test]
fn start_pairing_succeeds_when_under_capacity() {
    let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

    for offset in 0..32 {
        manager
            .start_pairing(
                PairingClientKind::Node,
                PairingMethod::Pin { code: "123456".to_owned() },
                now + Duration::from_secs(offset),
            )
            .expect("session should be created while under cap");
    }
}

#[test]
fn start_pairing_enforces_burst_rate_limit_and_recovers_after_window() {
    let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
    manager.set_pairing_start_rate_limit(2, Duration::from_secs(30));
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

    manager
        .start_pairing(
            PairingClientKind::Node,
            PairingMethod::Pin { code: "123456".to_owned() },
            now,
        )
        .expect("first session should be created");
    manager
        .start_pairing(
            PairingClientKind::Node,
            PairingMethod::Pin { code: "123456".to_owned() },
            now + Duration::from_secs(1),
        )
        .expect("second session should be created");

    let throttled = manager.start_pairing(
        PairingClientKind::Node,
        PairingMethod::Pin { code: "123456".to_owned() },
        now + Duration::from_secs(2),
    );
    assert!(matches!(
        throttled,
        Err(IdentityError::PairingSessionRateLimited { max: 2, window_ms: 30_000 })
    ));

    let recovered = manager.start_pairing(
        PairingClientKind::Node,
        PairingMethod::Pin { code: "123456".to_owned() },
        now + Duration::from_secs(32),
    );
    assert!(
        recovered.is_ok(),
        "rate limiter should recover deterministically once the window elapses"
    );
}

#[test]
fn identity_state_is_loaded_from_secret_store() {
    let store = Arc::new(crate::InMemorySecretStore::new());
    let mut first = IdentityManager::with_store(store.clone()).expect("manager should initialize");
    let ca_before = first.gateway_ca_certificate_pem();
    let (_, _, hello) = start_pin_pairing(&mut first, "123456");
    let paired = first.complete_pairing(hello, SystemTime::now()).expect("pairing should complete");

    drop(first);
    let second = IdentityManager::with_store(store).expect("manager should reload from store");

    assert_eq!(second.gateway_ca_certificate_pem(), ca_before);
    let restored =
        second.paired_device(sample_device_id()).expect("paired device should be rehydrated");
    assert_eq!(restored.current_certificate.sequence, paired.device.current_certificate.sequence);
    assert!(
        restored.current_certificate.private_key_pem.is_empty(),
        "private key must not be persisted in paired device records"
    );
    assert_eq!(restored.certificate_fingerprints.len(), 1);
}

#[test]
fn revocation_state_is_loaded_from_secret_store() {
    let store = Arc::new(crate::InMemorySecretStore::new());
    let mut first = IdentityManager::with_store(store.clone()).expect("manager should initialize");
    let (_, _, hello) = start_pin_pairing(&mut first, "123456");
    first.complete_pairing(hello, SystemTime::now()).expect("pairing should complete");
    first
        .revoke_device(sample_device_id(), "test", SystemTime::now())
        .expect("revocation should succeed");

    let revoked_fingerprints = first.revoked_certificate_fingerprints();
    assert!(!revoked_fingerprints.is_empty(), "revoked cert index should contain certificate");

    drop(first);
    let second = IdentityManager::with_store(store).expect("manager should reload from store");
    assert!(second.revoked_devices().contains(sample_device_id()));
    assert_eq!(second.revoked_certificate_fingerprints(), revoked_fingerprints);
}

#[test]
fn identity_state_bundle_excludes_gateway_ca_private_key_material() {
    let store = Arc::new(crate::InMemorySecretStore::new());
    let mut manager =
        IdentityManager::with_store(store.clone()).expect("manager should initialize");
    let (_, _, hello) = start_pin_pairing(&mut manager, "123456");
    manager.complete_pairing(hello, SystemTime::now()).expect("pairing should complete");

    let bundle = String::from_utf8(
        store
            .read_secret(super::persistence::IDENTITY_STATE_BUNDLE_KEY)
            .expect("identity state bundle should exist"),
    )
    .expect("identity state bundle should be utf-8");
    assert!(
        !bundle.contains("private_key_pem"),
        "public identity state bundle must not persist gateway CA private key"
    );

    let gateway_ca = String::from_utf8(
        store
            .read_secret(super::persistence::GATEWAY_CA_STATE_KEY)
            .expect("gateway CA state should exist"),
    )
    .expect("gateway CA state should be utf-8");
    assert!(
        gateway_ca.contains("private_key_pem"),
        "gateway CA state should continue to carry the CA private key for reload"
    );
}

#[test]
fn stale_manager_instance_does_not_clobber_existing_pairings() {
    let store = Arc::new(crate::InMemorySecretStore::new());
    let mut first =
        IdentityManager::with_store(store.clone()).expect("first manager should initialize");
    let mut stale =
        IdentityManager::with_store(store.clone()).expect("second manager should initialize");

    let (_, _, first_hello) =
        start_pin_pairing_for_device(&mut first, sample_device_id(), "123456");
    first.complete_pairing(first_hello, SystemTime::now()).expect("first pairing should complete");

    let (_, _, second_hello) =
        start_pin_pairing_for_device(&mut stale, sample_second_device_id(), "123456");
    stale
        .complete_pairing(second_hello, SystemTime::now())
        .expect("stale manager pairing should still preserve existing state");

    let reloaded = IdentityManager::with_store(store).expect("manager should reload");
    assert!(
        reloaded.paired_device(sample_device_id()).is_some(),
        "first pairing must remain present after stale manager write"
    );
    assert!(
        reloaded.paired_device(sample_second_device_id()).is_some(),
        "second pairing must be persisted"
    );
}

#[test]
fn rotate_device_certificate_revokes_previous_fingerprint() {
    let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
    let (_, _, hello) = start_pin_pairing(&mut manager, "123456");
    let paired =
        manager.complete_pairing(hello, SystemTime::now()).expect("pairing should complete");
    let previous_fingerprint =
        certificate_fingerprint_hex(&paired.device.current_certificate.certificate_pem)
            .expect("previous certificate fingerprint should parse");

    let rotated = manager
        .force_rotate_device_certificate(sample_device_id())
        .expect("certificate rotation should succeed");
    let rotated_fingerprint = certificate_fingerprint_hex(&rotated.certificate_pem)
        .expect("rotated certificate fingerprint should parse");

    let revoked = manager.revoked_certificate_fingerprints();
    assert!(
        revoked.contains(&previous_fingerprint),
        "previous certificate fingerprint must be revoked"
    );
    assert!(
        !revoked.contains(&rotated_fingerprint),
        "active rotated certificate fingerprint must remain valid"
    );
}

#[test]
fn repairing_same_device_revokes_superseded_certificate() {
    let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
    let (_, _, first_hello) = start_pin_pairing(&mut manager, "123456");
    let first_pairing = manager
        .complete_pairing(first_hello, SystemTime::now())
        .expect("first pairing should complete");
    let first_fingerprint =
        certificate_fingerprint_hex(&first_pairing.device.current_certificate.certificate_pem)
            .expect("first certificate fingerprint should parse");

    let replacement_device =
        DeviceIdentity::generate(sample_device_id()).expect("replacement device should generate");
    let replacement_session = manager
        .start_pairing(
            PairingClientKind::Node,
            PairingMethod::Pin { code: "123456".to_owned() },
            SystemTime::now(),
        )
        .expect("replacement pairing session should start");
    let replacement_hello = manager
        .build_device_hello(&replacement_session, &replacement_device, "123456")
        .expect("replacement hello should build");
    let second_pairing = manager
        .complete_pairing(replacement_hello, SystemTime::now())
        .expect("replacement pairing should complete");
    let second_fingerprint =
        certificate_fingerprint_hex(&second_pairing.device.current_certificate.certificate_pem)
            .expect("second certificate fingerprint should parse");

    let revoked = manager.revoked_certificate_fingerprints();
    assert!(
        revoked.contains(&first_fingerprint),
        "superseded certificate fingerprint must be revoked after re-pair"
    );
    assert!(
        !revoked.contains(&second_fingerprint),
        "newly paired certificate fingerprint must remain valid"
    );
}

#[test]
fn rotate_if_due_reissues_certificate_when_private_key_is_not_persisted() {
    let store = Arc::new(crate::InMemorySecretStore::new());
    let mut first = IdentityManager::with_store(store.clone()).expect("manager should initialize");
    let (_, _, hello) = start_pin_pairing(&mut first, "123456");
    let pairing =
        first.complete_pairing(hello, SystemTime::now()).expect("pairing should complete");
    drop(first);

    let mut second = IdentityManager::with_store(store).expect("manager should reload from store");
    let before = second
        .paired_device(sample_device_id())
        .expect("paired device should be available")
        .current_certificate
        .clone();
    assert!(
        before.private_key_pem.is_empty(),
        "private key should be absent after state rehydration"
    );
    let before_fingerprint =
        certificate_fingerprint_hex(&before.certificate_pem).expect("fingerprint should parse");

    let rotated = second
        .rotate_device_certificate_if_due(sample_device_id(), SystemTime::now())
        .expect("certificate should be reissued when private key is unavailable");
    assert!(!rotated.private_key_pem.is_empty(), "rotated certificate must include private key");
    assert!(
        rotated.sequence > pairing.device.current_certificate.sequence,
        "reissued certificate sequence must advance"
    );
    assert!(
        second.revoked_certificate_fingerprints().contains(&before_fingerprint),
        "previous certificate should be revoked after keyless reissue"
    );
}

#[test]
fn rotate_if_due_reloads_revocation_state_before_returning_cached_certificate() {
    let store = Arc::new(crate::InMemorySecretStore::new());
    let mut stale = IdentityManager::with_store(store.clone()).expect("manager should initialize");
    let (_, _, hello) = start_pin_pairing(&mut stale, "123456");
    stale.complete_pairing(hello, SystemTime::now()).expect("pairing should complete");
    let mut revoker =
        IdentityManager::with_store(store.clone()).expect("revoker should initialize");
    revoker
        .revoke_device(sample_device_id(), "operator revoked", SystemTime::now())
        .expect("revocation should succeed");

    let result = stale.rotate_device_certificate_if_due(sample_device_id(), SystemTime::now());
    assert!(
        matches!(result, Err(IdentityError::DeviceRevoked)),
        "rotate_if_due must refresh persisted revocation state before returning a certificate"
    );
}

#[test]
fn failed_bundle_write_does_not_persist_partial_rotation_state() {
    let store = Arc::new(ToggleFailSecretStore::new());
    let mut manager =
        IdentityManager::with_store(store.clone()).expect("manager should initialize");
    let (_, _, hello) = start_pin_pairing(&mut manager, "123456");
    let pairing =
        manager.complete_pairing(hello, SystemTime::now()).expect("pairing should complete");

    let baseline_sequence = pairing.device.current_certificate.sequence;
    let baseline_revoked = manager.revoked_certificate_fingerprints();

    store.set_fail_writes(true);
    let rotation = manager.force_rotate_device_certificate(sample_device_id());
    assert!(rotation.is_err(), "rotation should fail when bundle persistence fails");
    store.set_fail_writes(false);

    let reloaded =
        IdentityManager::with_store(store).expect("manager should reload from persisted state");
    let restored =
        reloaded.paired_device(sample_device_id()).expect("paired device should remain persisted");
    assert_eq!(
        restored.current_certificate.sequence, baseline_sequence,
        "failed write must not persist rotated certificate sequence"
    );
    assert_eq!(
        reloaded.revoked_certificate_fingerprints(),
        baseline_revoked,
        "failed write must not persist revocation side effects"
    );
}

#[test]
fn pending_pairing_private_key_roundtrips_via_secret_store() {
    let store = Arc::new(crate::InMemorySecretStore::new());
    let manager = IdentityManager::with_store(store).expect("manager should initialize");
    let request_id = "01ARZ3NDEKTSV4RRFFQ69G5FAX";

    manager
        .persist_pending_pairing_private_key(request_id, "PRIVATE KEY")
        .expect("pending pairing private key should persist");
    assert_eq!(
        manager
            .load_pending_pairing_private_key(request_id)
            .expect("pending pairing private key should reload"),
        Some("PRIVATE KEY".to_owned())
    );

    manager
        .delete_pending_pairing_private_key(request_id)
        .expect("pending pairing private key should delete");
    assert_eq!(
        manager
            .load_pending_pairing_private_key(request_id)
            .expect("deleted pending pairing key should read as absent"),
        None
    );
}

proptest! {
    #[test]
    fn transcript_mac_is_symmetric(
        gateway_secret_bytes in any::<[u8; 32]>(),
        device_secret_bytes in any::<[u8; 32]>(),
        challenge in any::<[u8; 32]>(),
    ) {
        let gateway_secret = StaticSecret::from(gateway_secret_bytes);
        let device_secret = StaticSecret::from(device_secret_bytes);
        let gateway_public = X25519PublicKey::from(&gateway_secret);
        let device_public = X25519PublicKey::from(&device_secret);

        let gateway_shared = gateway_secret.diffie_hellman(&device_public);
        let device_shared = device_secret.diffie_hellman(&gateway_public);
        let context = b"prop-test-context";
        let gateway_mac = derive_transcript_mac(gateway_shared.as_bytes(), &challenge, context)
            .expect("derive should work");
        let device_mac = derive_transcript_mac(device_shared.as_bytes(), &challenge, context)
            .expect("derive should work");

        prop_assert_eq!(gateway_mac, device_mac);
    }
}
