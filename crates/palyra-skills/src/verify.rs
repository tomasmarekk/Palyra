use crate::artifact::{decode_zip, now_unix_ms, parse_and_verify_artifact, parse_verifying_key};
use crate::constants::SKILL_VERIFICATION_EVENT_KIND;
use crate::error::SkillPackagingError;
use crate::manifest::{assert_runtime_compatibility, collect_manifest_warnings};
use crate::models::{
    SkillArtifactInspection, SkillTrustStore, SkillVerificationAuditEvent, SkillVerificationReport,
    TrustDecision,
};
use crate::runtime::{capability_grants_from_manifest, policy_bindings_from_manifest};

pub fn verify_skill_artifact(
    artifact_bytes: &[u8],
    trust_store: &mut SkillTrustStore,
    allow_tofu: bool,
) -> Result<SkillVerificationReport, SkillPackagingError> {
    let inspected = inspect_skill_artifact(artifact_bytes)?;

    trust_store.normalize()?;
    let verifying_key = parse_verifying_key(&inspected.signature)?;
    let observed_key = hex::encode(verifying_key.as_bytes());
    let publisher = inspected.manifest.publisher.clone();

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
