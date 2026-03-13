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
    PairingResult, PairingSession,
};

#[derive(Debug, Clone)]
struct VerifiedPairingOutcome {
    device_id: String,
    client_kind: PairingClientKind,
    identity_fingerprint: String,
    signing_public_key_hex: String,
    transcript_hash_hex: String,
}

fn build_pending_pairing_session(
    client_kind: PairingClientKind,
    method: PairingMethod,
    started_at: SystemTime,
    pairing_window: std::time::Duration,
) -> IdentityResult<(String, PairingSession, ActivePairingSession)> {
    validate_pairing_method(&method)?;

    let session_id = ulid::Ulid::new().to_string();
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

    pub fn build_device_hello(
        &self,
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
        let transcript_mac = derive_transcript_mac(
            shared_secret.as_bytes(),
            &session.challenge,
            &transcript_context,
        )?;

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

    pub fn complete_pairing(
        &mut self,
        hello: DevicePairingHello,
        now: SystemTime,
    ) -> IdentityResult<PairingResult> {
        let _guard = self.acquire_state_mutation_guard()?;
        self.reload_persisted_state()?;
        let verified = self.complete_pairing_inner(hello, now)?;
        let result = self.persist_verified_pairing(verified)?;
        self.persist_identity_state_bundle()?;
        Ok(result)
    }

    fn complete_pairing_inner(
        &mut self,
        hello: DevicePairingHello,
        now: SystemTime,
    ) -> IdentityResult<VerifiedPairingOutcome> {
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
        Ok(VerifiedPairingOutcome {
            device_id: hello.device_id,
            client_kind: hello.client_kind,
            identity_fingerprint,
            signing_public_key_hex,
            transcript_hash_hex,
        })
    }

    fn persist_verified_pairing(
        &mut self,
        verified: VerifiedPairingOutcome,
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

fn secure_random_array<const N: usize>() -> IdentityResult<[u8; N]> {
    let mut bytes = [0_u8; N];
    fill_random_bytes(&mut bytes).map_err(|error| {
        IdentityError::Cryptographic(format!(
            "failed to read OS randomness for pairing session: {error}"
        ))
    })?;
    Ok(bytes)
}
