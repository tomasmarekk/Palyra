mod artifact;
mod audit;
mod constants;
mod error;
mod manifest;
mod models;
mod runtime;
mod trust;
mod verify;

pub use artifact::build_signed_skill_artifact;
pub use audit::audit_skill_artifact_security;
pub use constants::{
    DEFAULT_SKILL_AUDIT_MAX_EXPORTED_FUNCTIONS, DEFAULT_SKILL_AUDIT_MAX_MODULE_BYTES,
    PROVENANCE_PATH, SBOM_PATH, SIGNATURE_PATH, SKILL_ARTIFACT_EXTENSION, SKILL_MANIFEST_PATH,
    SKILL_MANIFEST_VERSION, SKILL_VERIFICATION_EVENT_KIND,
};
pub use error::SkillPackagingError;
pub use manifest::{parse_ed25519_signing_key, parse_manifest_toml};
pub use models::*;
pub use runtime::{
    capability_grants_from_manifest, policy_bindings_from_manifest, policy_requests_from_manifest,
};
pub use trust::builder_manifest_requires_review;
pub use verify::{inspect_skill_artifact, verify_skill_artifact};

#[cfg(test)]
pub(crate) use artifact::{decode_zip, encode_zip};
#[cfg(test)]
pub(crate) use constants::{MAX_ARTIFACT_BYTES, MAX_ENTRIES};

#[cfg(test)]
mod tests;
