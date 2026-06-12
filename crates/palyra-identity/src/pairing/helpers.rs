//! Crypto and policy primitives shared by the pairing handshake and rotation logic.
//!
//! The byte layouts produced by `pairing_signature_payload` and `transcript_context`
//! are protocol-v1 wire contracts: changing them invalidates every existing client.

use std::time::{Duration, SystemTime};

use hkdf::Hkdf;
use rustls::pki_types::{pem::PemObject, CertificateDer};
use sha2::{Digest, Sha256};

use crate::{
    ca::IssuedCertificate,
    error::{IdentityError, IdentityResult},
    unix_ms,
};

use super::{PairingClientKind, PairingMethod};

/// Returns whether `certificate` expires within `threshold` of `now`.
///
/// Rotation is proactive: a certificate is "due" while still valid so the replacement
/// is in place before the old one expires. Already-expired certificates are also due.
///
/// # Errors
/// Returns [`IdentityError::Internal`] if `now` precedes the Unix epoch or the
/// threshold overflows the millisecond range.
pub fn should_rotate_certificate(
    certificate: &IssuedCertificate,
    now: SystemTime,
    threshold: Duration,
) -> IdentityResult<bool> {
    let now_ms = unix_ms(now)?;
    let threshold_ms: u64 = threshold
        .as_millis()
        .try_into()
        .map_err(|_| IdentityError::Internal("rotation threshold overflow".to_owned()))?;
    Ok(certificate.expires_at_unix_ms <= now_ms.saturating_add(threshold_ms))
}

pub(super) fn duration_to_millis_u64(value: Duration) -> u64 {
    // Saturating conversion: the value is clamped to u64::MAX first, so the cast is
    // provably lossless.
    value.as_millis().min(u128::from(u64::MAX)) as u64
}

/// Validates the shape of the pairing proof before a session may be opened.
pub(super) fn validate_pairing_method(method: &PairingMethod) -> IdentityResult<()> {
    match method {
        PairingMethod::Pin { code } => {
            let valid = code.len() == 6 && code.chars().all(|ch| ch.is_ascii_digit());
            if !valid {
                return Err(IdentityError::InvalidPairingProof);
            }
        }
        PairingMethod::Qr { token } => {
            if token.len() < 16 || token.len() > 128 {
                return Err(IdentityError::InvalidPairingProof);
            }
        }
    }
    Ok(())
}

/// Canonical byte payload signed by the device's Ed25519 key (protocol v1).
///
/// The `palyra-pairing-v1` prefix domain-separates this signature from any other use
/// of the device key.
///
/// NOTE: variable-length fields are concatenated without length framing. This is
/// unambiguous today only because session_id is a fixed-length ULID, device_id is a
/// fixed 26-char canonical ID, and client_kind is a closed label set. Any new or
/// loosened field needs length prefixes — which means a protocol version bump.
pub(super) fn pairing_signature_payload(
    protocol_version: u32,
    session_id: &str,
    challenge: &[u8; 32],
    gateway_ephemeral_public: &[u8; 32],
    device_id: &str,
    client_kind: PairingClientKind,
    proof: &str,
) -> Vec<u8> {
    let mut payload = Vec::with_capacity(256);
    payload.extend_from_slice(b"palyra-pairing-v1");
    payload.extend_from_slice(&protocol_version.to_le_bytes());
    payload.extend_from_slice(session_id.as_bytes());
    payload.extend_from_slice(challenge);
    payload.extend_from_slice(gateway_ephemeral_public);
    payload.extend_from_slice(device_id.as_bytes());
    payload.extend_from_slice(client_kind.as_str().as_bytes());
    payload.extend_from_slice(proof.as_bytes());
    payload
}

/// HKDF `info` input binding the transcript MAC to this session's identity fields
/// (protocol v1 wire layout — see the framing note on [`pairing_signature_payload`]).
pub(super) fn transcript_context(
    session_id: &str,
    protocol_version: u32,
    device_id: &str,
    client_kind: PairingClientKind,
) -> Vec<u8> {
    let mut context = Vec::with_capacity(128);
    context.extend_from_slice(b"palyra-mtls-transcript-v1");
    context.extend_from_slice(session_id.as_bytes());
    context.extend_from_slice(&protocol_version.to_le_bytes());
    context.extend_from_slice(device_id.as_bytes());
    context.extend_from_slice(client_kind.as_str().as_bytes());
    context
}

/// Derives the 32-byte transcript MAC: HKDF-SHA256 with the session challenge as salt,
/// the X25519 shared secret as input keying material, and the transcript as `info`.
///
/// Symmetric by construction — gateway and device derive identical values, so the
/// gateway compares its derivation against the one the device sent.
pub(super) fn derive_transcript_mac(
    shared_secret: &[u8; 32],
    challenge: &[u8; 32],
    transcript_context: &[u8],
) -> IdentityResult<[u8; 32]> {
    let hkdf = Hkdf::<Sha256>::new(Some(challenge), shared_secret);
    let mut output = [0_u8; 32];
    hkdf.expand(transcript_context, &mut output)
        .map_err(|_| IdentityError::Cryptographic("hkdf expansion failed".to_owned()))?;
    Ok(output)
}

/// Best-effort constant-time equality for secret comparisons (proofs, MACs).
///
/// No early exit: the loop always runs over the longer input, padding the shorter one
/// with zeros, and the length difference is folded into the accumulator up front.
///
/// This is hand-rolled because the workspace does not currently carry a dedicated
/// compiler-barrier constant-time comparison dependency.
pub(super) fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut diff = left.len() ^ right.len();
    let max_len = left.len().max(right.len());

    for index in 0..max_len {
        let left_byte = left.get(index).copied().unwrap_or(0);
        let right_byte = right.get(index).copied().unwrap_or(0);
        diff |= usize::from(left_byte ^ right_byte);
    }

    diff == 0
}

/// Hex SHA-256 over the certificate's DER bytes.
///
/// Must stay byte-compatible with the DER-side fingerprint in `crate::mtls`, which is
/// what the revocation-aware verifier computes during handshakes.
pub(super) fn certificate_fingerprint_hex(certificate_pem: &str) -> IdentityResult<String> {
    let der = CertificateDer::from_pem_slice(certificate_pem.as_bytes())
        .map_err(|_| IdentityError::CertificateParsingFailed)?;
    Ok(hex::encode(Sha256::digest(der.as_ref())))
}
