//! Well-known artifact entry paths, format versions, size limits, and serde
//! default helpers shared across packaging, verification, and audit.
//!
//! The `pub(crate)` limits and the signature constants are part of the wire
//! contract: changing them invalidates previously built or pinned artifacts.

/// Artifact entry path of the skill manifest (`skill.toml`).
pub const SKILL_MANIFEST_PATH: &str = "skill.toml";
/// File extension used for packaged skill artifacts.
pub const SKILL_ARTIFACT_EXTENSION: &str = ".palyra-skill";
/// Artifact entry path of the CycloneDX SBOM document.
pub const SBOM_PATH: &str = "sbom.cdx.json";
/// Artifact entry path of the build provenance document.
pub const PROVENANCE_PATH: &str = "provenance.json";
/// Artifact entry path of the detached signature document.
pub const SIGNATURE_PATH: &str = "signature.json";
/// Audit-journal event kind emitted for successful artifact verification.
pub const SKILL_VERIFICATION_EVENT_KIND: &str = "skill.artifact.verified";
/// Current skill manifest schema version produced by the packager.
pub const SKILL_MANIFEST_VERSION: u32 = 2;
/// Oldest manifest schema version still accepted on the verify path.
pub const LEGACY_SKILL_MANIFEST_VERSION: u32 = 1;
/// Default per-module size ceiling enforced by the security audit.
pub const DEFAULT_SKILL_AUDIT_MAX_MODULE_BYTES: u64 = 8 * 1024 * 1024;
/// Default exported-function ceiling enforced by the security audit.
pub const DEFAULT_SKILL_AUDIT_MAX_EXPORTED_FUNCTIONS: usize = 128;

pub(crate) const SIGNATURE_ALGORITHM: &str = "ed25519-sha256";
// Domain-separation prefix mixed into every payload hash so a skill payload
// digest can never be replayed as a signature input for another Palyra context.
pub(crate) const PAYLOAD_CONTEXT: &[u8] = b"palyra.skill.payload.v1";
pub(crate) const MAX_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_ENTRY_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_ENTRIES: usize = 256;

#[must_use]
pub(crate) fn default_manifest_version() -> u32 {
    SKILL_MANIFEST_VERSION
}

#[must_use]
pub(crate) fn default_operator_config_schema_version() -> u32 {
    1
}

#[must_use]
pub(crate) fn default_quota_timeout_ms() -> u64 {
    30_000
}

#[must_use]
pub(crate) fn default_quota_fuel_budget() -> u64 {
    10_000_000
}

#[must_use]
pub(crate) fn default_quota_max_memory() -> u64 {
    64 * 1024 * 1024
}
