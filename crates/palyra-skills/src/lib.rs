//! Skill artifact contract: manifest parsing, signed `.palyra-skill` packaging,
//! signature/integrity verification, publisher trust (allowlist + TOFU), security
//! audit, and extension lifecycle gates.
//!
//! The packaging path is [`build_signed_skill_artifact`]; the install path is
//! [`verify_skill_artifact`] / [`inspect_skill_artifact`] followed by
//! [`audit_skill_artifact_security`]. Verification is fail-closed: any signature,
//! integrity, trust, or compatibility failure is an error, never a degraded report.
//! Error/message strings and serde field names are pinned by CLI golden fixtures.

mod artifact;
mod audit;
mod constants;
mod contract;
mod dynamic_tools;
mod error;
mod extension;
mod install_policy;
mod lifecycle;
mod manifest;
mod models;
mod plugin_testkit;
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
pub use contract::{
    skill_manifest_contract_snapshot, SKILL_MANIFEST_CONTRACT_SNAPSHOT_SCHEMA_VERSION,
    SKILL_MANIFEST_CONTRACT_SNAPSHOT_VERSION,
};
pub use dynamic_tools::*;
pub use error::SkillPackagingError;
pub use extension::*;
pub use install_policy::*;
pub use lifecycle::*;
pub use manifest::{
    parse_ed25519_signing_key, parse_manifest_toml, plugin_manifest_validation_report,
};
pub use models::*;
pub use plugin_testkit::*;
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
