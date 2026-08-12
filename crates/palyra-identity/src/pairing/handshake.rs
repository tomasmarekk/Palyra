//! The pairing handshake: challenge issuance, device hello, and gateway verification.
//!
//! Security shape: the device signs a domain-separated payload (session, challenge,
//! proof) with its long-lived Ed25519 key, and both sides derive a transcript MAC from
//! an ephemeral X25519 agreement. A failed proof, signature, or MAC check consumes the
//! session, so the low-entropy proof cannot be brute-forced within one window.

use std::time::SystemTime;

use ed25519_dalek::{Signature, Signer, Verifier, VerifyingKey};
use getrandom::fill as fill_random_bytes;
use palyra_common::validate_canonical_id;
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey as X25519PublicKey, StaticSecret};

use crate::{
    device::DeviceIdentity,
    error::{IdentityError, IdentityResult},
    unix_ms, PAIRING_PROTOCOL_VERSION,
};

use super::{
    helpers::{
        certificate_fingerprint_hex, constant_time_eq, derive_transcript_mac,
        duration_to_millis_u64, pairing_signature_payload, transcript_context,
        validate_pairing_method,
    },
    models::ActivePairingSession,
    persistence::MAX_ACTIVE_PAIRING_SESSIONS,
    DevicePairingHello, IdentityManager, PairedDevice, PairingClientKind, PairingMethod,
    PairingResult, PairingSession, VerifiedPairing,
};

fn build_pending_pairing_session(
    client_kind: PairingClientKind,
    method: PairingMethod,
    started_at: SystemTime,
    pairing_window: std::time::Duration,
) -> IdentityResult<(String, PairingSession, ActivePairingSession)> {
    validate_pairing_method(&method)?;

    let session_id = ulid::Ulid::generate().to_string();
    let gateway_secret_bytes = secure_random_array()?;
    let gateway_ephemeral_secret = StaticSecret::from(gateway_secret_bytes);
    let gateway_ephemeral_public = X25519PublicKey::from(&gateway_ephemeral_secret).to_bytes();
    let challenge = secure_random_array()?;
    let expires_at = started_at + pairing_window;
    let session = PairingSession {
        session_id: session_id.clone(),
        protocol_version: PAIRING_PROTOCOL_VERSION,
        client_kind,
        method,
        gateway_ephemeral_public,
        challenge,
        expires_at_unix_ms: unix_ms(expires_at)?,
    };
    let active_session = ActivePairingSession { public: session.clone(), gateway_ephemeral_secret };
    Ok((session_id, session, active_session))
}

fn record_pairing_start(history: &mut std::collections::VecDeque<u64>, started_at_ms: u64) {
    history.push_back(started_at_ms);
}

impl IdentityManager {
    // `retain_session_id` keeps the session currently being completed alive even when
    // expired, so its expiry surfaces as PairingSessionExpired instead of the less
    // precise PairingSessionNotFound.
    fn prune_expired_sessions(
        &mut self,
        now: SystemTime,
        retain_session_id: Option<&str>,
    ) -> IdentityResult<()> {
        let now_ms = unix_ms(now)?;
        self.active_sessions.retain(|session_id, session| {
            session.public.expires_at_unix_ms > now_ms
                || retain_session_id.is_some_and(|retain| retain == session_id)
        });
        Ok(())
    }

    fn prune_pairing_start_history(&mut self, now_ms: u64) {
        let window_ms = duration_to_millis_u64(self.pairing_start_rate_limit_window);
        while let Some(issued_at_ms) = self.recent_pairing_starts.front().copied() {
            if now_ms.saturating_sub(issued_at_ms) >= window_ms {
                self.recent_pairing_starts.pop_front();
            } else {
                break;
            }
        }
    }

    /// Opens a pairing session: validates the proof shape, generates an ephemeral DH
    /// key and challenge, and registers the session until the pairing window closes.
    ///
    /// # Errors
    /// Returns [`IdentityError::InvalidPairingProof`] for a malformed PIN/QR value,
    /// [`IdentityError::PairingSessionRateLimited`] when the start budget is exhausted,
    /// [`IdentityError::PairingSessionCapacityExceeded`] at the active-session cap, and
    /// [`IdentityError::Cryptographic`] if OS randomness is unavailable.
    pub fn start_pairing(
        &mut self,
        client_kind: PairingClientKind,
        method: PairingMethod,
        now: SystemTime,
    ) -> IdentityResult<PairingSession> {
        let now_ms = unix_ms(now)?;
        self.prune_expired_sessions(now, None)?;
        self.prune_pairing_start_history(now_ms);
        if self.recent_pairing_starts.len() >= self.pairing_max_starts_per_window {
            return Err(IdentityError::PairingSessionRateLimited {
                max: self.pairing_max_starts_per_window,
                window_ms: duration_to_millis_u64(self.pairing_start_rate_limit_window),
            });
        }
        if self.active_sessions.len() >= MAX_ACTIVE_PAIRING_SESSIONS {
            return Err(IdentityError::PairingSessionCapacityExceeded {
                limit: MAX_ACTIVE_PAIRING_SESSIONS,
            });
        }

        let (session_id, session, active_session) =
            build_pending_pairing_session(client_kind, method, now, self.pairing_window)?;
        self.active_sessions.insert(session_id, active_session);
        record_pairing_start(&mut self.recent_pairing_starts, now_ms);
        Ok(session)
    }

    /// Convenience wrapper over [`build_device_pairing_hello`] (the device-side step).
    ///
    /// # Errors
    /// Same failure modes as [`build_device_pairing_hello`].
    pub fn build_device_hello(
        &self,
        session: &PairingSession,
        device: &DeviceIdentity,
        proof: &str,
    ) -> IdentityResult<DevicePairingHello> {
        build_device_pairing_hello(session, device, proof)
    }

    /// Verifies a device hello and immediately issues its certificate.
    ///
    /// Equivalent to [`Self::verify_pairing`] followed by
    /// [`Self::finalize_verified_pairing`] with no approval step in between.
    ///
    /// # Errors
    /// Union of the failure modes of the two underlying steps.
    pub fn complete_pairing(
        &mut self,
        hello: DevicePairingHello,
        now: SystemTime,
    ) -> IdentityResult<PairingResult> {
        let verified = self.verify_pairing(hello, now)?;
        self.finalize_verified_pairing(verified)
    }

    /// Cryptographically verifies a device hello without persisting anything, so a
    /// caller can interpose an approval decision before finalizing.
    ///
    /// Reloads persisted state under the cross-process lock first, so a revocation
    /// written by another process is honored.
    ///
    /// # Errors
    /// Returns the pairing rejection variants of [`IdentityError`] (session not
    /// found/expired, version/kind mismatch, proof/signature/transcript failure,
    /// device revoked, invalid device ID) plus lock and store failure modes.
    pub fn verify_pairing(
        &mut self,
        hello: DevicePairingHello,
        now: SystemTime,
    ) -> IdentityResult<VerifiedPairing> {
        let _guard = self.acquire_state_mutation_guard()?;
        self.reload_persisted_state()?;
        self.complete_pairing_inner(hello, now)
    }

    /// Issues a certificate for a verified pairing and persists the device record,
    /// revoking any certificates from a previous pairing of the same device ID.
    ///
    /// # Errors
    /// Returns [`IdentityError::DeviceRevoked`] if the device was revoked after
    /// verification, [`IdentityError::Cryptographic`] on issuance failure, plus lock
    /// and persistence failure modes.
    pub fn finalize_verified_pairing(
        &mut self,
        verified: VerifiedPairing,
    ) -> IdentityResult<PairingResult> {
        let _guard = self.acquire_state_mutation_guard()?;
        self.reload_persisted_state()?;
        if self.revoked_devices.contains_key(&verified.device_id) {
            return Err(IdentityError::DeviceRevoked);
        }
        let result = self.persist_verified_pairing(verified)?;
        self.persist_identity_state_bundle()?;
        Ok(result)
    }

    fn complete_pairing_inner(
        &mut self,
        hello: DevicePairingHello,
        now: SystemTime,
    ) -> IdentityResult<VerifiedPairing> {
        self.prune_expired_sessions(now, Some(hello.session_id.as_str()))?;
        validate_canonical_id(&hello.device_id)
            .map_err(|error| IdentityError::InvalidCanonicalDeviceId(error.to_string()))?;
        if self.revoked_devices.contains_key(&hello.device_id) {
            return Err(IdentityError::DeviceRevoked);
        }

        let active = self
            .active_sessions
            .get(&hello.session_id)
            .ok_or(IdentityError::PairingSessionNotFound)?
            .clone();
        if unix_ms(now)? > active.public.expires_at_unix_ms {
            self.active_sessions.remove(&hello.session_id);
            return Err(IdentityError::PairingSessionExpired);
        }
        if hello.protocol_version != active.public.protocol_version {
            return Err(IdentityError::PairingVersionMismatch {
                expected: active.public.protocol_version,
                got: hello.protocol_version,
            });
        }
        if hello.client_kind != active.public.client_kind {
            return Err(IdentityError::PairingClientKindMismatch);
        }
        // Constant-time comparison: the proof is a low-entropy secret (six-digit PIN),
        // so a timing oracle here would make it guessable. From this point on, every
        // failure consumes the session — one verification attempt per session.
        if !constant_time_eq(hello.proof.as_bytes(), active.public.method.proof().as_bytes()) {
            self.active_sessions.remove(&hello.session_id);
            return Err(IdentityError::InvalidPairingProof);
        }

        let verifying_key =
            VerifyingKey::from_bytes(&hello.device_signing_public).map_err(|_| {
                self.active_sessions.remove(&hello.session_id);
                IdentityError::SignatureVerificationFailed
            })?;
        let signature_payload = pairing_signature_payload(
            hello.protocol_version,
            &hello.session_id,
            &active.public.challenge,
            &active.public.gateway_ephemeral_public,
            &hello.device_id,
            hello.client_kind,
            &hello.proof,
        );
        let signature = Signature::from_bytes(&hello.challenge_signature);
        verifying_key.verify(&signature_payload, &signature).map_err(|_| {
            self.active_sessions.remove(&hello.session_id);
            IdentityError::SignatureVerificationFailed
        })?;

        // The transcript MAC proves the device actually holds the X25519 secret for
        // the public key it sent, binding the DH exchange to this exact session.
        let device_public = X25519PublicKey::from(hello.device_x25519_public);
        let shared_secret = active.gateway_ephemeral_secret.diffie_hellman(&device_public);
        let transcript_context = transcript_context(
            &hello.session_id,
            hello.protocol_version,
            &hello.device_id,
            hello.client_kind,
        );
        let expected_mac = derive_transcript_mac(
            shared_secret.as_bytes(),
            &active.public.challenge,
            &transcript_context,
        )?;
        if !constant_time_eq(expected_mac.as_slice(), hello.transcript_mac.as_slice()) {
            self.active_sessions.remove(&hello.session_id);
            return Err(IdentityError::TranscriptVerificationFailed);
        }

        self.active_sessions.remove(&hello.session_id);
        let transcript_hash_hex = hex::encode(Sha256::digest(expected_mac));
        let identity_fingerprint = hex::encode(Sha256::digest(hello.device_signing_public));
        let signing_public_key_hex = hex::encode(hello.device_signing_public);
        Ok(VerifiedPairing {
            device_id: hello.device_id,
            client_kind: hello.client_kind,
            identity_fingerprint,
            signing_public_key_hex,
            transcript_hash_hex,
        })
    }

    fn persist_verified_pairing(
        &mut self,
        verified: VerifiedPairing,
    ) -> IdentityResult<PairingResult> {
        let certificate = self
            .ca
            .issue_client_certificate(verified.device_id.as_str(), self.certificate_validity)?;
        let certificate_fingerprint = certificate_fingerprint_hex(&certificate.certificate_pem)?;

        let paired = PairedDevice {
            device_id: verified.device_id.clone(),
            client_kind: verified.client_kind,
            current_certificate: certificate.clone(),
            certificate_fingerprints: vec![certificate_fingerprint],
        };
        // Re-pairing the same device ID supersedes its old trust: every certificate
        // from the previous pairing is revoked so only the new leaf can connect.
        if let Some(previous) = self.paired_devices.get(&verified.device_id).cloned() {
            self.revoke_superseded_certificates(&previous)?;
        }
        self.paired_devices.insert(verified.device_id, paired.clone());

        Ok(PairingResult {
            device: paired,
            identity_fingerprint: verified.identity_fingerprint,
            signing_public_key_hex: verified.signing_public_key_hex,
            transcript_hash_hex: verified.transcript_hash_hex,
            gateway_ca_certificate_pem: self.ca.certificate_pem.clone(),
        })
    }
}

/// Builds the device-side hello for a pairing session: derives the transcript MAC from
/// an X25519 agreement with the gateway's ephemeral key and signs the challenge payload
/// with the device's long-lived Ed25519 key.
///
/// # Errors
/// Returns [`IdentityError::InvalidCanonicalDeviceId`] for a malformed device ID and
/// [`IdentityError::Cryptographic`] if MAC derivation fails.
pub fn build_device_pairing_hello(
    session: &PairingSession,
    device: &DeviceIdentity,
    proof: &str,
) -> IdentityResult<DevicePairingHello> {
    validate_canonical_id(&device.device_id)
        .map_err(|error| IdentityError::InvalidCanonicalDeviceId(error.to_string()))?;

    let gateway_public = X25519PublicKey::from(session.gateway_ephemeral_public);
    let shared_secret = device.x25519_secret().diffie_hellman(&gateway_public);
    let transcript_context = transcript_context(
        &session.session_id,
        session.protocol_version,
        device.device_id.as_str(),
        session.client_kind,
    );
    let transcript_mac =
        derive_transcript_mac(shared_secret.as_bytes(), &session.challenge, &transcript_context)?;

    let signature_payload = pairing_signature_payload(
        session.protocol_version,
        &session.session_id,
        &session.challenge,
        &session.gateway_ephemeral_public,
        &device.device_id,
        session.client_kind,
        proof,
    );
    let signature = device.signing_key().sign(&signature_payload);

    Ok(DevicePairingHello {
        session_id: session.session_id.clone(),
        protocol_version: session.protocol_version,
        device_id: device.device_id.clone(),
        client_kind: session.client_kind,
        proof: proof.to_owned(),
        device_signing_public: device.signing_public_key(),
        device_x25519_public: device.x25519_public_key(),
        challenge_signature: signature.to_bytes(),
        transcript_mac,
    })
}

fn secure_random_array<const N: usize>() -> IdentityResult<[u8; N]> {
    let mut bytes = [0_u8; N];
    fill_random_bytes(&mut bytes).map_err(|error| {
        IdentityError::Cryptographic(format!(
            "failed to read OS randomness for pairing session: {error}"
        ))
    })?;
    Ok(bytes)
}
