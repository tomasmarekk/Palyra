//! Top-level artifact verification: cryptographic checks via the artifact
//! module, then publisher trust evaluation (allowlist before TOFU) producing a
//! [`SkillVerificationReport`] with derived grants, bindings, and audit event.
//!
//! Fail-closed by design: an unknown publisher without `allow_tofu`, or any
//! key mismatch, is an error — there is no "verified but untrusted" report.

use crate::artifact::{decode_zip, now_unix_ms, parse_and_verify_artifact, parse_verifying_key};
use crate::constants::SKILL_VERIFICATION_EVENT_KIND;
use crate::error::SkillPackagingError;
use crate::manifest::{assert_runtime_compatibility, collect_manifest_warnings};
use crate::models::{
    SkillArtifactInspection, SkillTrustStore, SkillVerificationAuditEvent, SkillVerificationReport,
    TrustDecision,
};
use crate::runtime::{capability_grants_from_manifest, policy_bindings_from_manifest};

/// Verifies an artifact end to end (structure, hash, signature, integrity,
/// runtime compatibility) and evaluates publisher trust against `trust_store`.
///
/// On [`TrustDecision::TofuNewlyPinned`] the observed key is inserted into the
/// in-memory `trust_store`; persisting the pin (e.g. via
/// [`SkillTrustStore::save`]) is the caller's responsibility.
///
/// # Errors
/// Returns any artifact verification error from
/// [`inspect_skill_artifact`], plus [`SkillPackagingError::UntrustedPublisher`]
/// (unknown publisher with `allow_tofu` disabled),
/// [`SkillPackagingError::TrustedPublisherKeyMismatch`], or
/// [`SkillPackagingError::TofuKeyMismatch`] from trust evaluation, and trust
/// store normalization errors for malformed store contents.
pub fn verify_skill_artifact(
    artifact_bytes: &[u8],
    trust_store: &mut SkillTrustStore,
    allow_tofu: bool,
) -> Result<SkillVerificationReport, SkillPackagingError> {
    let inspected = inspect_skill_artifact(artifact_bytes)?;

    trust_store.normalize()?;
    // The signature is internally consistent at this point, but the embedded
    // key is attacker-supplied; only the trust-store comparison below confers
    // authenticity.
    let verifying_key = parse_verifying_key(&inspected.signature)?;
    let observed_key = hex::encode(verifying_key.as_bytes());
    let publisher = inspected.manifest.publisher.clone();

    // Allowlist entries take precedence over TOFU pins: once an operator
    // explicitly curates keys for a publisher, a stale or attacker-seeded TOFU
    // pin must never be consulted for that publisher again.
    let trust_decision = if let Some(keys) = trust_store.trusted_publishers.get(&publisher) {
        if keys.iter().any(|key| key == &observed_key) {
            TrustDecision::Allowlisted
        } else {
            return Err(SkillPackagingError::TrustedPublisherKeyMismatch { publisher });
        }
    } else if let Some(pinned) = trust_store.tofu_publishers.get(&publisher) {
        if pinned == &observed_key {
            TrustDecision::TofuPinned
        } else {
            return Err(SkillPackagingError::TofuKeyMismatch { publisher });
        }
    } else if allow_tofu {
        trust_store.tofu_publishers.insert(publisher.clone(), observed_key);
        TrustDecision::TofuNewlyPinned
    } else {
        return Err(SkillPackagingError::UntrustedPublisher { publisher });
    };

    let capability_grants = capability_grants_from_manifest(&inspected.manifest);
    let policy_bindings = policy_bindings_from_manifest(&inspected.manifest);
    let manifest_warnings = inspected.manifest_warnings.clone();
    let audit_event = SkillVerificationAuditEvent {
        event_kind: SKILL_VERIFICATION_EVENT_KIND.to_owned(),
        skill_id: inspected.manifest.skill_id.clone(),
        publisher: inspected.manifest.publisher.clone(),
        version: inspected.manifest.version.clone(),
        payload_sha256: inspected.payload_sha256.clone(),
        trust_decision,
        verified_at_unix_ms: now_unix_ms(),
        policy_bindings: policy_bindings.clone(),
    };

    Ok(SkillVerificationReport {
        accepted: true,
        trust_decision,
        payload_sha256: inspected.payload_sha256,
        manifest: inspected.manifest,
        manifest_warnings,
        capability_grants,
        policy_bindings,
        audit_event,
    })
}

/// Decodes and cryptographically verifies an artifact without consulting any
/// trust store, returning the verified contents for installers.
///
/// Trust-neutral on purpose: signature, integrity, and runtime compatibility
/// are all enforced, but whether the signing key is trusted is left to
/// [`verify_skill_artifact`].
///
/// # Errors
/// Returns ZIP/size-limit errors from decoding, missing-entry, hash,
/// signature, and integrity errors from verification, and compatibility
/// errors when the manifest's declared range excludes the current host.
pub fn inspect_skill_artifact(
    artifact_bytes: &[u8],
) -> Result<SkillArtifactInspection, SkillPackagingError> {
    let entries = decode_zip(artifact_bytes)?;
    let parsed = parse_and_verify_artifact(&entries)?;
    assert_runtime_compatibility(&parsed.manifest.compat)?;
    Ok(SkillArtifactInspection {
        manifest_warnings: collect_manifest_warnings(&parsed.manifest),
        manifest: parsed.manifest,
        signature: parsed.signature,
        payload_sha256: parsed.payload_sha256,
        entries,
    })
}
