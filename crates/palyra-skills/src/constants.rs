pub const SKILL_MANIFEST_PATH: &str = "skill.toml";
pub const SKILL_ARTIFACT_EXTENSION: &str = ".palyra-skill";
pub const SBOM_PATH: &str = "sbom.cdx.json";
pub const PROVENANCE_PATH: &str = "provenance.json";
pub const SIGNATURE_PATH: &str = "signature.json";
pub const SKILL_VERIFICATION_EVENT_KIND: &str = "skill.artifact.verified";
pub const SKILL_MANIFEST_VERSION: u32 = 2;
pub const LEGACY_SKILL_MANIFEST_VERSION: u32 = 1;
pub const DEFAULT_SKILL_AUDIT_MAX_MODULE_BYTES: u64 = 8 * 1024 * 1024;
pub const DEFAULT_SKILL_AUDIT_MAX_EXPORTED_FUNCTIONS: usize = 128;

pub(crate) const SIGNATURE_ALGORITHM: &str = "ed25519-sha256";
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
