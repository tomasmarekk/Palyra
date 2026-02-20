use std::{
    collections::{BTreeMap, BTreeSet},
    convert::TryInto,
    fs,
    io::{Cursor, Read, Write},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use palyra_common::{build_metadata, CANONICAL_PROTOCOL_MAJOR};
use palyra_plugins_runtime::CapabilityGrantSet;
use palyra_policy::PolicyRequest;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use zip::{write::SimpleFileOptions, CompressionMethod, DateTime, ZipArchive, ZipWriter};

pub const SKILL_MANIFEST_PATH: &str = "skill.toml";
pub const SKILL_ARTIFACT_EXTENSION: &str = ".palyra-skill";
pub const SBOM_PATH: &str = "sbom.cdx.json";
pub const PROVENANCE_PATH: &str = "provenance.json";
pub const SIGNATURE_PATH: &str = "signature.json";
pub const SKILL_VERIFICATION_EVENT_KIND: &str = "skill.artifact.verified";
pub const SKILL_MANIFEST_VERSION: u32 = 1;

const SIGNATURE_ALGORITHM: &str = "ed25519-sha256";
const PAYLOAD_CONTEXT: &[u8] = b"palyra.skill.payload.v1";
const MAX_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;
const MAX_ENTRY_BYTES: usize = 16 * 1024 * 1024;
const MAX_ENTRIES: usize = 256;

#[derive(Debug, Error)]
pub enum SkillPackagingError {
    #[error("manifest parse failed: {0}")]
    ManifestParse(String),
    #[error("manifest validation failed: {0}")]
    ManifestValidation(String),
    #[error("artifact size exceeds limit ({actual} > {limit})")]
    ArtifactTooLarge { actual: usize, limit: usize },
    #[error("artifact contains too many entries ({actual} > {limit})")]
    ArtifactTooManyEntries { actual: usize, limit: usize },
    #[error("artifact entry '{path}' exceeds limit ({actual} > {limit})")]
    ArtifactEntryTooLarge { path: String, actual: usize, limit: usize },
    #[error("artifact is missing required entry '{0}'")]
    MissingArtifactEntry(String),
    #[error("artifact contains duplicate entry '{0}'")]
    DuplicateArtifactEntry(String),
    #[error("artifact entry path is invalid: {0}")]
    InvalidArtifactPath(String),
    #[error("invalid SBOM payload: {0}")]
    InvalidSbom(String),
    #[error("invalid provenance payload: {0}")]
    InvalidProvenance(String),
    #[error("artifact payload hash mismatch")]
    PayloadHashMismatch,
    #[error("artifact signature verification failed")]
    SignatureVerificationFailed,
    #[error("signing key length is invalid: {actual}")]
    InvalidSigningKeyLength { actual: usize },
    #[error("untrusted publisher '{publisher}'")]
    UntrustedPublisher { publisher: String },
    #[error("trusted publisher key mismatch for '{publisher}'")]
    TrustedPublisherKeyMismatch { publisher: String },
    #[error("TOFU pinned key mismatch for '{publisher}'")]
    TofuKeyMismatch { publisher: String },
    #[error("requested min protocol major {requested} is higher than current {current}")]
    UnsupportedProtocolMajor { requested: u32, current: u32 },
    #[error("requested min runtime version {requested} is higher than current {current}")]
    UnsupportedRuntimeVersion { requested: String, current: String },
    #[error("I/O failed: {0}")]
    Io(String),
    #[error("zip handling failed: {0}")]
    Zip(String),
    #[error("serialization failed: {0}")]
    Serialization(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SkillManifest {
    #[serde(default = "default_manifest_version")]
    pub manifest_version: u32,
    pub skill_id: String,
    pub name: String,
    pub version: String,
    pub publisher: String,
    pub entrypoints: SkillEntrypoints,
    #[serde(default)]
    pub capabilities: SkillCapabilities,
    pub compat: SkillCompat,
    #[serde(default)]
    pub integrity: SkillIntegrity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SkillEntrypoints {
    pub tools: Vec<SkillToolEntrypoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SkillToolEntrypoint {
    pub id: String,
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub output_schema: Value,
    #[serde(default)]
    pub risk: SkillToolRisk,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillToolRisk {
    #[serde(default)]
    pub default_sensitive: bool,
    #[serde(default)]
    pub requires_approval: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillCapabilities {
    #[serde(default)]
    pub filesystem: SkillFilesystemCapabilities,
    #[serde(default)]
    pub http_egress_allowlist: Vec<String>,
    #[serde(default)]
    pub secrets: Vec<SkillSecretScope>,
    #[serde(default)]
    pub device_capabilities: Vec<String>,
    #[serde(default)]
    pub node_capabilities: Vec<String>,
    #[serde(default)]
    pub quotas: SkillQuotaConfig,
    #[serde(default)]
    pub wildcard_opt_in: SkillWildcardOptIn,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillFilesystemCapabilities {
    #[serde(default)]
    pub read_roots: Vec<String>,
    #[serde(default)]
    pub write_roots: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillSecretScope {
    pub scope: String,
    #[serde(default)]
    pub key_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillWildcardOptIn {
    #[serde(default)]
    pub filesystem: bool,
    #[serde(default)]
    pub http_egress: bool,
    #[serde(default)]
    pub secrets: bool,
    #[serde(default)]
    pub device: bool,
    #[serde(default)]
    pub node: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillQuotaConfig {
    #[serde(default = "default_quota_timeout_ms")]
    pub wall_clock_timeout_ms: u64,
    #[serde(default = "default_quota_fuel_budget")]
    pub fuel_budget: u64,
    #[serde(default = "default_quota_max_memory")]
    pub max_memory_bytes: u64,
}

impl Default for SkillQuotaConfig {
    fn default() -> Self {
        Self {
            wall_clock_timeout_ms: default_quota_timeout_ms(),
            fuel_budget: default_quota_fuel_budget(),
            max_memory_bytes: default_quota_max_memory(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillCompat {
    pub min_protocol_major: u32,
    pub min_runtime_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillIntegrity {
    #[serde(default)]
    pub files: Vec<SkillIntegrityEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillIntegrityEntry {
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillArtifactSignature {
    pub algorithm: String,
    pub publisher: String,
    pub key_id: String,
    pub public_key_base64: String,
    pub payload_sha256: String,
    pub signature_base64: String,
    pub signed_at_unix_ms: i64,
}

#[derive(Clone)]
pub struct SkillArtifactBuildRequest {
    pub manifest_toml: String,
    pub modules: Vec<ArtifactFile>,
    pub assets: Vec<ArtifactFile>,
    pub sbom_cyclonedx_json: Vec<u8>,
    pub provenance_json: Vec<u8>,
    pub signing_key: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactFile {
    pub path: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillArtifactBuildOutput {
    pub artifact_bytes: Vec<u8>,
    pub manifest: SkillManifest,
    pub payload_sha256: String,
    pub signature: SkillArtifactSignature,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillTrustStore {
    #[serde(default)]
    pub trusted_publishers: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub tofu_publishers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TrustDecision {
    Allowlisted,
    TofuPinned,
    TofuNewlyPinned,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillCapabilityGrantSnapshot {
    pub http_hosts: Vec<String>,
    pub secret_keys: Vec<String>,
    pub storage_prefixes: Vec<String>,
    pub channels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillPolicyBinding {
    pub action: String,
    pub resource: String,
    pub requires_approval: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillVerificationAuditEvent {
    pub event_kind: String,
    pub skill_id: String,
    pub publisher: String,
    pub version: String,
    pub payload_sha256: String,
    pub trust_decision: TrustDecision,
    pub verified_at_unix_ms: i64,
    pub policy_bindings: Vec<SkillPolicyBinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillVerificationReport {
    pub accepted: bool,
    pub trust_decision: TrustDecision,
    pub payload_sha256: String,
    pub manifest: SkillManifest,
    pub capability_grants: SkillCapabilityGrantSnapshot,
    pub policy_bindings: Vec<SkillPolicyBinding>,
    pub audit_event: SkillVerificationAuditEvent,
}

#[derive(Debug, Clone)]
struct ParsedArtifact {
    manifest: SkillManifest,
    signature: SkillArtifactSignature,
    payload_sha256: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SkillArtifactInspection {
    pub manifest: SkillManifest,
    pub signature: SkillArtifactSignature,
    pub payload_sha256: String,
    pub entries: BTreeMap<String, Vec<u8>>,
}

pub fn parse_manifest_toml(raw: &str) -> Result<SkillManifest, SkillPackagingError> {
    let manifest = toml::from_str::<SkillManifest>(raw)
        .map_err(|error| SkillPackagingError::ManifestParse(error.to_string()))?;
    validate_manifest(&manifest)?;
    Ok(manifest)
}

pub fn parse_ed25519_signing_key(secret: &[u8]) -> Result<[u8; 32], SkillPackagingError> {
    if secret.len() == 32 {
        let mut key = [0_u8; 32];
        key.copy_from_slice(secret);
        return Ok(key);
    }
    let trimmed = trim_ascii_whitespace(secret);
    if trimmed.len() == 32 {
        let mut key = [0_u8; 32];
        key.copy_from_slice(trimmed);
        return Ok(key);
    }
    let utf8 = std::str::from_utf8(trimmed)
        .map_err(|_| SkillPackagingError::InvalidSigningKeyLength { actual: trimmed.len() })?;
    let text = utf8.trim();
    if let Ok(hex_decoded) = hex::decode(text) {
        if hex_decoded.len() == 32 {
            let mut key = [0_u8; 32];
            key.copy_from_slice(hex_decoded.as_slice());
            return Ok(key);
        }
    }
    if let Ok(base64_decoded) = BASE64_STANDARD.decode(text.as_bytes()) {
        if base64_decoded.len() == 32 {
            let mut key = [0_u8; 32];
            key.copy_from_slice(base64_decoded.as_slice());
            return Ok(key);
        }
    }
    Err(SkillPackagingError::InvalidSigningKeyLength { actual: trimmed.len() })
}

pub fn build_signed_skill_artifact(
    request: SkillArtifactBuildRequest,
) -> Result<SkillArtifactBuildOutput, SkillPackagingError> {
    let mut manifest = parse_manifest_toml(request.manifest_toml.as_str())?;
    assert_runtime_compatibility(&manifest.compat)?;
    validate_sbom_payload(request.sbom_cyclonedx_json.as_slice())?;
    validate_provenance_payload(request.provenance_json.as_slice())?;

    let mut payload_entries: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for module in request.modules {
        if !module.path.ends_with(".wasm") {
            return Err(SkillPackagingError::ManifestValidation(format!(
                "module path '{}' must end with .wasm",
                module.path
            )));
        }
        insert_entry(
            &mut payload_entries,
            format!("modules/{}", normalize_artifact_path(module.path.as_str())?).as_str(),
            module.bytes,
        )?;
    }
    if payload_entries.is_empty() {
        return Err(SkillPackagingError::ManifestValidation(
            "artifact must include at least one module".to_owned(),
        ));
    }

    for asset in request.assets {
        insert_entry(
            &mut payload_entries,
            format!("assets/{}", normalize_artifact_path(asset.path.as_str())?).as_str(),
            asset.bytes,
        )?;
    }
    insert_entry(&mut payload_entries, SBOM_PATH, request.sbom_cyclonedx_json)?;
    insert_entry(&mut payload_entries, PROVENANCE_PATH, request.provenance_json)?;

    manifest.integrity.files = payload_entries
        .iter()
        .map(|(path, bytes)| SkillIntegrityEntry {
            path: path.clone(),
            sha256: sha256_hex(bytes.as_slice()),
        })
        .collect();

    let manifest_toml = toml::to_string_pretty(&manifest).map_err(|error| {
        SkillPackagingError::Serialization(format!("failed to serialize manifest: {error}"))
    })?;
    insert_entry(&mut payload_entries, SKILL_MANIFEST_PATH, manifest_toml.into_bytes())?;

    let payload_sha256 = compute_payload_hash_hex(
        payload_entries.iter().filter(|(path, _)| path.as_str() != SIGNATURE_PATH),
    );
    let signing_key = SigningKey::from_bytes(&request.signing_key);
    let verifying_key = VerifyingKey::from(&signing_key);
    let signature = signing_key.sign(payload_sha256.as_bytes());
    let signature_payload = SkillArtifactSignature {
        algorithm: SIGNATURE_ALGORITHM.to_owned(),
        publisher: manifest.publisher.clone(),
        key_id: key_id_for(&verifying_key),
        public_key_base64: BASE64_STANDARD.encode(verifying_key.as_bytes()),
        payload_sha256: payload_sha256.clone(),
        signature_base64: BASE64_STANDARD.encode(signature.to_bytes()),
        signed_at_unix_ms: now_unix_ms(),
    };
    let signature_json = serde_json::to_vec_pretty(&signature_payload).map_err(|error| {
        SkillPackagingError::Serialization(format!("failed to serialize signature: {error}"))
    })?;
    insert_entry(&mut payload_entries, SIGNATURE_PATH, signature_json)?;

    if payload_entries.len() > MAX_ENTRIES {
        return Err(SkillPackagingError::ArtifactTooManyEntries {
            actual: payload_entries.len(),
            limit: MAX_ENTRIES,
        });
    }
    let total_uncompressed = payload_entries.values().try_fold(0_usize, |sum, payload| {
        sum.checked_add(payload.len()).ok_or(SkillPackagingError::ArtifactTooLarge {
            actual: usize::MAX,
            limit: MAX_ARTIFACT_BYTES,
        })
    })?;
    if total_uncompressed > MAX_ARTIFACT_BYTES {
        return Err(SkillPackagingError::ArtifactTooLarge {
            actual: total_uncompressed,
            limit: MAX_ARTIFACT_BYTES,
        });
    }

    let artifact_bytes = encode_zip(payload_entries.iter())?;
    if artifact_bytes.len() > MAX_ARTIFACT_BYTES {
        return Err(SkillPackagingError::ArtifactTooLarge {
            actual: artifact_bytes.len(),
            limit: MAX_ARTIFACT_BYTES,
        });
    }
    Ok(SkillArtifactBuildOutput {
        artifact_bytes,
        manifest,
        payload_sha256,
        signature: signature_payload,
    })
}

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
        manifest: parsed.manifest,
        signature: parsed.signature,
        payload_sha256: parsed.payload_sha256,
        entries,
    })
}

#[must_use]
pub fn capability_grants_from_manifest(manifest: &SkillManifest) -> SkillCapabilityGrantSnapshot {
    let mut secret_keys = manifest
        .capabilities
        .secrets
        .iter()
        .flat_map(|scope| {
            scope.key_names.iter().map(|key| format!("{}/{}", scope.scope, key)).collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    secret_keys.sort();
    secret_keys.dedup();

    SkillCapabilityGrantSnapshot {
        http_hosts: dedupe_sorted(manifest.capabilities.http_egress_allowlist.as_slice()),
        secret_keys,
        storage_prefixes: dedupe_sorted(manifest.capabilities.filesystem.write_roots.as_slice()),
        channels: Vec::new(),
    }
}

#[must_use]
pub fn policy_bindings_from_manifest(manifest: &SkillManifest) -> Vec<SkillPolicyBinding> {
    let mut bindings = manifest
        .entrypoints
        .tools
        .iter()
        .map(|tool| SkillPolicyBinding {
            action: "tool.execute".to_owned(),
            resource: format!("tool:{}", tool.id),
            requires_approval: tool.risk.default_sensitive || tool.risk.requires_approval,
        })
        .collect::<Vec<_>>();

    let capability_resource = format!("skill:{}", manifest.skill_id);
    if !manifest.capabilities.http_egress_allowlist.is_empty() {
        bindings.push(SkillPolicyBinding {
            action: "skill.capability.http.egress".to_owned(),
            resource: capability_resource.clone(),
            requires_approval: true,
        });
    }
    if !manifest.capabilities.filesystem.write_roots.is_empty() {
        bindings.push(SkillPolicyBinding {
            action: "skill.capability.filesystem.write".to_owned(),
            resource: capability_resource.clone(),
            requires_approval: true,
        });
    }
    if !manifest.capabilities.secrets.is_empty() {
        bindings.push(SkillPolicyBinding {
            action: "skill.capability.vault.read".to_owned(),
            resource: capability_resource.clone(),
            requires_approval: true,
        });
    }
    if !manifest.capabilities.device_capabilities.is_empty()
        || !manifest.capabilities.node_capabilities.is_empty()
    {
        bindings.push(SkillPolicyBinding {
            action: "skill.capability.device.use".to_owned(),
            resource: capability_resource,
            requires_approval: true,
        });
    }

    let mut deduped = BTreeMap::new();
    for binding in bindings {
        deduped.insert(
            (binding.action.clone(), binding.resource.clone(), binding.requires_approval),
            binding,
        );
    }
    deduped.into_values().collect()
}

#[must_use]
pub fn policy_requests_from_manifest(manifest: &SkillManifest) -> Vec<PolicyRequest> {
    let principal = format!("skill:{}", manifest.skill_id);
    policy_bindings_from_manifest(manifest)
        .into_iter()
        .map(|binding| PolicyRequest {
            principal: principal.clone(),
            action: binding.action,
            resource: binding.resource,
        })
        .collect()
}

impl SkillTrustStore {
    pub fn load(path: &Path) -> Result<Self, SkillPackagingError> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let payload = fs::read(path).map_err(|error| {
            SkillPackagingError::Io(format!(
                "failed to read trust store {}: {error}",
                path.display()
            ))
        })?;
        let mut trust_store =
            serde_json::from_slice::<Self>(payload.as_slice()).map_err(|error| {
                SkillPackagingError::Serialization(format!(
                    "failed to parse trust store {}: {error}",
                    path.display()
                ))
            })?;
        trust_store.normalize()?;
        Ok(trust_store)
    }

    pub fn save(&self, path: &Path) -> Result<(), SkillPackagingError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                SkillPackagingError::Io(format!(
                    "failed to create trust store directory {}: {error}",
                    parent.display()
                ))
            })?;
        }
        let mut normalized = self.clone();
        normalized.normalize()?;
        let payload = serde_json::to_vec_pretty(&normalized).map_err(|error| {
            SkillPackagingError::Serialization(format!("failed to serialize trust store: {error}"))
        })?;
        fs::write(path, payload).map_err(|error| {
            SkillPackagingError::Io(format!(
                "failed to write trust store {}: {error}",
                path.display()
            ))
        })
    }

    pub fn add_trusted_key(
        &mut self,
        publisher: &str,
        public_key_hex: &str,
    ) -> Result<(), SkillPackagingError> {
        let publisher = normalize_identifier(publisher, "publisher")?;
        let key = normalize_public_key_hex(public_key_hex)?;
        let keys = self.trusted_publishers.entry(publisher).or_default();
        if !keys.iter().any(|existing| existing == &key) {
            keys.push(key);
            keys.sort();
            keys.dedup();
        }
        Ok(())
    }

    fn normalize(&mut self) -> Result<(), SkillPackagingError> {
        let mut trusted_publishers = BTreeMap::<String, Vec<String>>::new();
        for (publisher_raw, keys_raw) in &self.trusted_publishers {
            let publisher = normalize_identifier(publisher_raw, "publisher").map_err(|error| {
                SkillPackagingError::Serialization(format!(
                    "invalid trust-store publisher '{publisher_raw}': {error}"
                ))
            })?;
            let mut normalized_keys = Vec::with_capacity(keys_raw.len());
            for key_raw in keys_raw {
                let key = normalize_public_key_hex(key_raw).map_err(|error| {
                    SkillPackagingError::Serialization(format!(
                        "invalid trusted key for publisher '{publisher}': {error}"
                    ))
                })?;
                normalized_keys.push(key);
            }
            if normalized_keys.is_empty() {
                return Err(SkillPackagingError::Serialization(format!(
                    "trusted publisher '{publisher}' must include at least one key"
                )));
            }
            let keys = trusted_publishers.entry(publisher).or_default();
            keys.extend(normalized_keys);
            keys.sort();
            keys.dedup();
        }

        let mut tofu_publishers = BTreeMap::<String, String>::new();
        for (publisher_raw, key_raw) in &self.tofu_publishers {
            let publisher = normalize_identifier(publisher_raw, "publisher").map_err(|error| {
                SkillPackagingError::Serialization(format!(
                    "invalid trust-store TOFU publisher '{publisher_raw}': {error}"
                ))
            })?;
            let key = normalize_public_key_hex(key_raw).map_err(|error| {
                SkillPackagingError::Serialization(format!(
                    "invalid TOFU key for publisher '{publisher}': {error}"
                ))
            })?;
            if let Some(existing) = tofu_publishers.get(&publisher) {
                if existing != &key {
                    return Err(SkillPackagingError::Serialization(format!(
                        "conflicting TOFU keys for publisher '{publisher}'"
                    )));
                }
            }
            tofu_publishers.insert(publisher, key);
        }

        self.trusted_publishers = trusted_publishers;
        self.tofu_publishers = tofu_publishers;
        Ok(())
    }
}

impl SkillCapabilityGrantSnapshot {
    #[must_use]
    pub fn to_runtime_capability_grants(&self) -> CapabilityGrantSet {
        CapabilityGrantSet {
            http_hosts: self.http_hosts.clone(),
            secret_keys: self.secret_keys.clone(),
            storage_prefixes: self.storage_prefixes.clone(),
            channels: self.channels.clone(),
        }
        .canonicalized()
    }
}

fn parse_and_verify_artifact(
    entries: &BTreeMap<String, Vec<u8>>,
) -> Result<ParsedArtifact, SkillPackagingError> {
    let manifest_bytes = entries
        .get(SKILL_MANIFEST_PATH)
        .ok_or_else(|| SkillPackagingError::MissingArtifactEntry(SKILL_MANIFEST_PATH.to_owned()))?;
    let manifest_text = std::str::from_utf8(manifest_bytes.as_slice()).map_err(|error| {
        SkillPackagingError::ManifestParse(format!("manifest utf8 error: {error}"))
    })?;
    let manifest = parse_manifest_toml(manifest_text)?;

    let sbom = entries
        .get(SBOM_PATH)
        .ok_or_else(|| SkillPackagingError::MissingArtifactEntry(SBOM_PATH.to_owned()))?;
    validate_sbom_payload(sbom.as_slice())?;

    let provenance = entries
        .get(PROVENANCE_PATH)
        .ok_or_else(|| SkillPackagingError::MissingArtifactEntry(PROVENANCE_PATH.to_owned()))?;
    validate_provenance_payload(provenance.as_slice())?;

    if !entries.keys().any(|path| path.starts_with("modules/") && path.ends_with(".wasm")) {
        return Err(SkillPackagingError::MissingArtifactEntry("modules/*.wasm".to_owned()));
    }

    let signature_bytes = entries
        .get(SIGNATURE_PATH)
        .ok_or_else(|| SkillPackagingError::MissingArtifactEntry(SIGNATURE_PATH.to_owned()))?;
    let signature = serde_json::from_slice::<SkillArtifactSignature>(signature_bytes.as_slice())
        .map_err(|error| {
            SkillPackagingError::Serialization(format!("invalid signature: {error}"))
        })?;

    let payload_sha256 = compute_payload_hash_hex(
        entries.iter().filter(|(path, _)| path.as_str() != SIGNATURE_PATH),
    );
    if payload_sha256 != signature.payload_sha256 {
        return Err(SkillPackagingError::PayloadHashMismatch);
    }

    verify_signature(&signature, payload_sha256.as_str())?;
    if signature.publisher != manifest.publisher {
        return Err(SkillPackagingError::SignatureVerificationFailed);
    }

    let expected_integrity = entries
        .iter()
        .filter(|(path, _)| !matches!(path.as_str(), SKILL_MANIFEST_PATH | SIGNATURE_PATH))
        .map(|(path, bytes)| (path.clone(), sha256_hex(bytes.as_slice())))
        .collect::<BTreeMap<_, _>>();
    let declared_integrity = manifest
        .integrity
        .files
        .iter()
        .map(|entry| Ok((normalize_artifact_path(entry.path.as_str())?, entry.sha256.clone())))
        .collect::<Result<BTreeMap<_, _>, SkillPackagingError>>()?;
    if expected_integrity != declared_integrity {
        return Err(SkillPackagingError::ManifestValidation(
            "manifest integrity does not match artifact payload".to_owned(),
        ));
    }

    Ok(ParsedArtifact { manifest, signature, payload_sha256 })
}

fn validate_manifest(manifest: &SkillManifest) -> Result<(), SkillPackagingError> {
    if manifest.manifest_version != SKILL_MANIFEST_VERSION {
        return Err(SkillPackagingError::ManifestValidation(format!(
            "manifest_version must equal {}",
            SKILL_MANIFEST_VERSION
        )));
    }
    let publisher = normalize_identifier(manifest.publisher.as_str(), "publisher")?;
    normalize_identifier(manifest.skill_id.as_str(), "skill_id")?;
    parse_semver(manifest.version.as_str(), "version")?;
    parse_semver(manifest.compat.min_runtime_version.as_str(), "compat.min_runtime_version")?;
    if manifest.name.trim().is_empty() {
        return Err(SkillPackagingError::ManifestValidation("name cannot be empty".to_owned()));
    }
    if manifest.entrypoints.tools.is_empty() {
        return Err(SkillPackagingError::ManifestValidation(
            "entrypoints.tools cannot be empty".to_owned(),
        ));
    }

    let mut tool_ids = BTreeSet::new();
    for tool in &manifest.entrypoints.tools {
        let tool_id = normalize_identifier(tool.id.as_str(), "entrypoints.tools[].id")?;
        if !tool_id.starts_with(format!("{publisher}.").as_str()) {
            return Err(SkillPackagingError::ManifestValidation(format!(
                "tool id '{}' must be namespaced with '{}.'",
                tool.id, publisher
            )));
        }
        if !tool_ids.insert(tool_id) {
            return Err(SkillPackagingError::ManifestValidation(format!(
                "duplicate tool id '{}'",
                tool.id
            )));
        }
        if tool.name.trim().is_empty() || tool.description.trim().is_empty() {
            return Err(SkillPackagingError::ManifestValidation(format!(
                "tool '{}' must include non-empty name and description",
                tool.id
            )));
        }
        if !tool.input_schema.is_object() || !tool.output_schema.is_object() {
            return Err(SkillPackagingError::ManifestValidation(format!(
                "tool '{}' schemas must be JSON objects",
                tool.id
            )));
        }
    }
    for path in &manifest.capabilities.filesystem.read_roots {
        validate_capability_path(path, manifest.capabilities.wildcard_opt_in.filesystem)?;
    }
    for path in &manifest.capabilities.filesystem.write_roots {
        validate_capability_path(path, manifest.capabilities.wildcard_opt_in.filesystem)?;
    }
    for host in &manifest.capabilities.http_egress_allowlist {
        validate_host(host, manifest.capabilities.wildcard_opt_in.http_egress)?;
    }
    for scope in &manifest.capabilities.secrets {
        validate_secret_scope(scope.scope.as_str())?;
        if scope.key_names.is_empty() {
            return Err(SkillPackagingError::ManifestValidation(format!(
                "secret scope '{}' must list key_names",
                scope.scope
            )));
        }
        for key in &scope.key_names {
            validate_identifier_or_wildcard(
                key,
                "capabilities.secrets[].key_names",
                manifest.capabilities.wildcard_opt_in.secrets,
            )?;
        }
    }
    for capability in &manifest.capabilities.device_capabilities {
        validate_identifier_or_wildcard(
            capability,
            "capabilities.device_capabilities",
            manifest.capabilities.wildcard_opt_in.device,
        )?;
    }
    for capability in &manifest.capabilities.node_capabilities {
        validate_identifier_or_wildcard(
            capability,
            "capabilities.node_capabilities",
            manifest.capabilities.wildcard_opt_in.node,
        )?;
    }
    if manifest.capabilities.quotas.wall_clock_timeout_ms == 0
        || manifest.capabilities.quotas.max_memory_bytes < 64 * 1024
        || manifest.capabilities.quotas.fuel_budget == 0
    {
        return Err(SkillPackagingError::ManifestValidation(
            "capabilities.quotas values must be non-zero and memory >= 65536".to_owned(),
        ));
    }
    Ok(())
}

fn assert_runtime_compatibility(compat: &SkillCompat) -> Result<(), SkillPackagingError> {
    if compat.min_protocol_major > CANONICAL_PROTOCOL_MAJOR {
        return Err(SkillPackagingError::UnsupportedProtocolMajor {
            requested: compat.min_protocol_major,
            current: CANONICAL_PROTOCOL_MAJOR,
        });
    }
    let requested =
        parse_semver(compat.min_runtime_version.as_str(), "compat.min_runtime_version")?;
    let current_raw = build_metadata().version.to_owned();
    let current = parse_semver(current_raw.as_str(), "runtime version")?;
    if requested > current {
        return Err(SkillPackagingError::UnsupportedRuntimeVersion {
            requested: compat.min_runtime_version.clone(),
            current: current_raw,
        });
    }
    Ok(())
}

fn validate_sbom_payload(bytes: &[u8]) -> Result<(), SkillPackagingError> {
    let value = serde_json::from_slice::<Value>(bytes)
        .map_err(|error| SkillPackagingError::InvalidSbom(error.to_string()))?;
    let object = value
        .as_object()
        .ok_or_else(|| SkillPackagingError::InvalidSbom("SBOM must be JSON object".to_owned()))?;
    if object.get("bomFormat").and_then(Value::as_str) != Some("CycloneDX") {
        return Err(SkillPackagingError::InvalidSbom(
            "sbom.cdx.json must declare bomFormat='CycloneDX'".to_owned(),
        ));
    }
    if object.get("specVersion").and_then(Value::as_str).unwrap_or_default().is_empty() {
        return Err(SkillPackagingError::InvalidSbom(
            "sbom.cdx.json must include specVersion".to_owned(),
        ));
    }
    Ok(())
}

fn validate_provenance_payload(bytes: &[u8]) -> Result<(), SkillPackagingError> {
    let value = serde_json::from_slice::<Value>(bytes)
        .map_err(|error| SkillPackagingError::InvalidProvenance(error.to_string()))?;
    let object = value.as_object().ok_or_else(|| {
        SkillPackagingError::InvalidProvenance("provenance must be JSON object".to_owned())
    })?;
    if object.get("builder").and_then(Value::as_object).is_none() {
        return Err(SkillPackagingError::InvalidProvenance(
            "provenance must include builder object".to_owned(),
        ));
    }
    if object.get("subject").and_then(Value::as_array).is_none_or(Vec::is_empty) {
        return Err(SkillPackagingError::InvalidProvenance(
            "provenance must include non-empty subject array".to_owned(),
        ));
    }
    Ok(())
}

fn validate_capability_path(path: &str, wildcard_allowed: bool) -> Result<(), SkillPackagingError> {
    if path.contains('*') && !wildcard_allowed {
        return Err(SkillPackagingError::ManifestValidation(format!(
            "capability path '{}' uses wildcard without explicit opt-in",
            path
        )));
    }
    if !path.contains('*') {
        normalize_artifact_path(path)?;
    }
    Ok(())
}

fn validate_host(host: &str, wildcard_allowed: bool) -> Result<(), SkillPackagingError> {
    if host.contains('*') {
        if wildcard_allowed {
            return Ok(());
        }
        return Err(SkillPackagingError::ManifestValidation(format!(
            "host '{}' uses wildcard without explicit opt-in",
            host
        )));
    }
    let normalized = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.contains("..")
        || normalized.starts_with('.')
        || normalized.ends_with('.')
        || normalized.starts_with('-')
        || normalized.ends_with('-')
        || !normalized.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-'))
    {
        return Err(SkillPackagingError::ManifestValidation(format!("invalid host '{}'", host)));
    }
    Ok(())
}

fn validate_secret_scope(scope: &str) -> Result<(), SkillPackagingError> {
    if scope == "global"
        || scope.starts_with("principal:")
        || scope.starts_with("channel:")
        || scope.starts_with("skill:")
    {
        return Ok(());
    }
    Err(SkillPackagingError::ManifestValidation(format!("invalid secret scope '{}'", scope)))
}

fn validate_identifier_or_wildcard(
    value: &str,
    field: &str,
    wildcard_allowed: bool,
) -> Result<(), SkillPackagingError> {
    if value.contains('*') {
        if wildcard_allowed {
            return Ok(());
        }
        return Err(SkillPackagingError::ManifestValidation(format!(
            "{field} contains wildcard without explicit opt-in"
        )));
    }
    normalize_identifier(value, field).map(|_| ())
}

fn verify_signature(
    payload: &SkillArtifactSignature,
    payload_sha256: &str,
) -> Result<(), SkillPackagingError> {
    let verifying_key = parse_verifying_key(payload)?;
    let signature_bytes = BASE64_STANDARD
        .decode(payload.signature_base64.as_bytes())
        .map_err(|_| SkillPackagingError::SignatureVerificationFailed)?;
    let signature_array: [u8; 64] = signature_bytes
        .as_slice()
        .try_into()
        .map_err(|_| SkillPackagingError::SignatureVerificationFailed)?;
    let signature = Signature::from_bytes(&signature_array);
    verifying_key
        .verify(payload_sha256.as_bytes(), &signature)
        .map_err(|_| SkillPackagingError::SignatureVerificationFailed)
}

fn parse_verifying_key(
    payload: &SkillArtifactSignature,
) -> Result<VerifyingKey, SkillPackagingError> {
    if payload.algorithm != SIGNATURE_ALGORITHM {
        return Err(SkillPackagingError::SignatureVerificationFailed);
    }
    let bytes = BASE64_STANDARD
        .decode(payload.public_key_base64.as_bytes())
        .map_err(|_| SkillPackagingError::SignatureVerificationFailed)?;
    let array: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| SkillPackagingError::SignatureVerificationFailed)?;
    let key = VerifyingKey::from_bytes(&array)
        .map_err(|_| SkillPackagingError::SignatureVerificationFailed)?;
    if payload.key_id != key_id_for(&key) {
        return Err(SkillPackagingError::SignatureVerificationFailed);
    }
    Ok(key)
}

fn decode_zip(bytes: &[u8]) -> Result<BTreeMap<String, Vec<u8>>, SkillPackagingError> {
    if bytes.len() > MAX_ARTIFACT_BYTES {
        return Err(SkillPackagingError::ArtifactTooLarge {
            actual: bytes.len(),
            limit: MAX_ARTIFACT_BYTES,
        });
    }
    let cursor = Cursor::new(bytes);
    let mut archive =
        ZipArchive::new(cursor).map_err(|error| SkillPackagingError::Zip(error.to_string()))?;
    if archive.len() > MAX_ENTRIES {
        return Err(SkillPackagingError::ArtifactTooManyEntries {
            actual: archive.len(),
            limit: MAX_ENTRIES,
        });
    }
    let mut entries = BTreeMap::new();
    let mut total_uncompressed = 0_usize;
    for index in 0..archive.len() {
        let file =
            archive.by_index(index).map_err(|error| SkillPackagingError::Zip(error.to_string()))?;
        if file.is_dir() {
            continue;
        }
        let path = normalize_artifact_path(file.name())?;
        let declared_size = usize::try_from(file.size()).unwrap_or(usize::MAX);
        if declared_size > MAX_ENTRY_BYTES {
            return Err(SkillPackagingError::ArtifactEntryTooLarge {
                path,
                actual: declared_size,
                limit: MAX_ENTRY_BYTES,
            });
        }
        if total_uncompressed >= MAX_ARTIFACT_BYTES {
            return Err(SkillPackagingError::ArtifactTooLarge {
                actual: total_uncompressed,
                limit: MAX_ARTIFACT_BYTES,
            });
        }
        let remaining_total = MAX_ARTIFACT_BYTES - total_uncompressed;
        if declared_size > remaining_total {
            return Err(SkillPackagingError::ArtifactTooLarge {
                actual: total_uncompressed.saturating_add(declared_size),
                limit: MAX_ARTIFACT_BYTES,
            });
        }
        let entry_limit = remaining_total.min(MAX_ENTRY_BYTES);
        let mut payload = Vec::with_capacity(declared_size.min(entry_limit));
        let read_limit = u64::try_from(entry_limit).unwrap_or(u64::MAX).saturating_add(1);
        let mut limited_reader = file.take(read_limit);
        limited_reader
            .read_to_end(&mut payload)
            .map_err(|error| SkillPackagingError::Io(format!("zip read failed: {error}")))?;
        if payload.len() > entry_limit {
            if entry_limit < MAX_ENTRY_BYTES {
                return Err(SkillPackagingError::ArtifactTooLarge {
                    actual: total_uncompressed.saturating_add(payload.len()),
                    limit: MAX_ARTIFACT_BYTES,
                });
            }
            return Err(SkillPackagingError::ArtifactEntryTooLarge {
                path,
                actual: payload.len(),
                limit: MAX_ENTRY_BYTES,
            });
        }
        total_uncompressed = total_uncompressed.checked_add(payload.len()).ok_or(
            SkillPackagingError::ArtifactTooLarge { actual: usize::MAX, limit: MAX_ARTIFACT_BYTES },
        )?;
        if total_uncompressed > MAX_ARTIFACT_BYTES {
            return Err(SkillPackagingError::ArtifactTooLarge {
                actual: total_uncompressed,
                limit: MAX_ARTIFACT_BYTES,
            });
        }
        insert_entry(&mut entries, path.as_str(), payload)?;
    }
    Ok(entries)
}

fn encode_zip<'a>(
    entries: impl Iterator<Item = (&'a String, &'a Vec<u8>)>,
) -> Result<Vec<u8>, SkillPackagingError> {
    let options = SimpleFileOptions::default()
        .compression_method(CompressionMethod::Deflated)
        .last_modified_time(DateTime::default())
        .unix_permissions(0o644);
    let mut writer = ZipWriter::new(Cursor::new(Vec::<u8>::new()));
    for (path, payload) in entries {
        writer
            .start_file(path, options)
            .map_err(|error| SkillPackagingError::Zip(error.to_string()))?;
        writer
            .write_all(payload.as_slice())
            .map_err(|error| SkillPackagingError::Io(format!("zip write failed: {error}")))?;
    }
    writer
        .finish()
        .map_err(|error| SkillPackagingError::Zip(error.to_string()))
        .map(|cursor| cursor.into_inner())
}

fn insert_entry(
    entries: &mut BTreeMap<String, Vec<u8>>,
    path: &str,
    payload: Vec<u8>,
) -> Result<(), SkillPackagingError> {
    if payload.len() > MAX_ENTRY_BYTES {
        return Err(SkillPackagingError::ArtifactEntryTooLarge {
            path: path.to_owned(),
            actual: payload.len(),
            limit: MAX_ENTRY_BYTES,
        });
    }
    let normalized = normalize_artifact_path(path)?;
    if entries.insert(normalized.clone(), payload).is_some() {
        return Err(SkillPackagingError::DuplicateArtifactEntry(normalized));
    }
    Ok(())
}

fn compute_payload_hash_hex<'a>(
    entries: impl Iterator<Item = (&'a String, &'a Vec<u8>)>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(PAYLOAD_CONTEXT);
    for (path, payload) in entries {
        hash_len_prefixed(&mut hasher, path.as_bytes());
        hash_len_prefixed(&mut hasher, payload.as_slice());
    }
    format!("{:x}", hasher.finalize())
}

fn hash_len_prefixed(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn normalize_artifact_path(raw: &str) -> Result<String, SkillPackagingError> {
    let normalized = raw.trim().replace('\\', "/");
    if normalized.is_empty() || normalized.starts_with('/') || normalized.contains('\0') {
        return Err(SkillPackagingError::InvalidArtifactPath(raw.to_owned()));
    }
    if normalized.contains(':') {
        return Err(SkillPackagingError::InvalidArtifactPath(raw.to_owned()));
    }
    let segments = normalized.split('/').collect::<Vec<_>>();
    if segments.is_empty()
        || segments.iter().any(|segment| segment.is_empty() || *segment == "." || *segment == "..")
    {
        return Err(SkillPackagingError::InvalidArtifactPath(raw.to_owned()));
    }
    Ok(segments.join("/"))
}

fn normalize_identifier(value: &str, field: &str) -> Result<String, SkillPackagingError> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Err(SkillPackagingError::ManifestValidation(format!("{field} cannot be empty")));
    }
    if !normalized.chars().all(|ch| {
        ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-' | ':')
    }) {
        return Err(SkillPackagingError::ManifestValidation(format!(
            "{field} must use [a-z0-9._-:]"
        )));
    }
    Ok(normalized.to_owned())
}

fn normalize_public_key_hex(value: &str) -> Result<String, SkillPackagingError> {
    let normalized = value.trim().to_ascii_lowercase();
    let decoded = hex::decode(normalized.as_str()).map_err(|_| {
        SkillPackagingError::ManifestValidation("trusted publisher key must be hex".to_owned())
    })?;
    if decoded.len() != 32 {
        return Err(SkillPackagingError::ManifestValidation(
            "trusted publisher key must decode to 32 bytes".to_owned(),
        ));
    }
    Ok(normalized)
}

fn parse_semver(value: &str, field: &str) -> Result<(u32, u32, u32), SkillPackagingError> {
    let parts = value.trim().split('.').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(SkillPackagingError::ManifestValidation(format!(
            "{field} must use semantic version major.minor.patch"
        )));
    }
    let major = parts[0]
        .parse::<u32>()
        .map_err(|_| SkillPackagingError::ManifestValidation(format!("{field} major invalid")))?;
    let minor = parts[1]
        .parse::<u32>()
        .map_err(|_| SkillPackagingError::ManifestValidation(format!("{field} minor invalid")))?;
    let patch = parts[2]
        .parse::<u32>()
        .map_err(|_| SkillPackagingError::ManifestValidation(format!("{field} patch invalid")))?;
    Ok((major, minor, patch))
}

fn key_id_for(key: &VerifyingKey) -> String {
    let digest = Sha256::digest(key.as_bytes());
    format!("ed25519:{}", hex::encode(&digest[..8]))
}

fn trim_ascii_whitespace(raw: &[u8]) -> &[u8] {
    let start = raw.iter().position(|value| !value.is_ascii_whitespace()).unwrap_or(raw.len());
    let end =
        raw.iter().rposition(|value| !value.is_ascii_whitespace()).map_or(start, |index| index + 1);
    &raw[start..end]
}

fn dedupe_sorted(values: &[String]) -> Vec<String> {
    let mut normalized = values
        .iter()
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

fn sha256_hex(payload: &[u8]) -> String {
    format!("{:x}", Sha256::digest(payload))
}

fn now_unix_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

fn default_manifest_version() -> u32 {
    SKILL_MANIFEST_VERSION
}

fn default_quota_timeout_ms() -> u64 {
    30_000
}

fn default_quota_fuel_budget() -> u64 {
    10_000_000
}

fn default_quota_max_memory() -> u64 {
    64 * 1024 * 1024
}

#[cfg(test)]
mod tests {
    use super::{
        build_signed_skill_artifact, capability_grants_from_manifest, inspect_skill_artifact,
        parse_ed25519_signing_key, parse_manifest_toml, policy_requests_from_manifest,
        verify_skill_artifact, ArtifactFile, SkillArtifactBuildRequest, SkillPackagingError,
        SkillTrustStore, TrustDecision, MAX_ARTIFACT_BYTES, MAX_ENTRIES, SBOM_PATH, SIGNATURE_PATH,
    };
    use base64::Engine as _;

    fn sample_manifest() -> String {
        r#"
manifest_version = 1
skill_id = "acme.echo_http"
name = "Echo + HTTP"
version = "1.0.0"
publisher = "acme"

[entrypoints]
[[entrypoints.tools]]
id = "acme.echo"
name = "echo"
description = "Echo payload"
input_schema = { type = "object", properties = { text = { type = "string" } } }
output_schema = { type = "object", properties = { echo = { type = "string" } } }
risk = { default_sensitive = false, requires_approval = false }

[[entrypoints.tools]]
id = "acme.http_get"
name = "http_get"
description = "Fetch one URL"
input_schema = { type = "object", properties = { url = { type = "string" } } }
output_schema = { type = "object", properties = { status = { type = "number" } } }
risk = { default_sensitive = true, requires_approval = true }

[capabilities.filesystem]
read_roots = ["skills/data"]
write_roots = ["skills/cache"]

[capabilities]
http_egress_allowlist = ["api.example.com"]
device_capabilities = []
node_capabilities = []

[[capabilities.secrets]]
scope = "skill:acme.echo_http"
key_names = ["api_token"]

[capabilities.quotas]
wall_clock_timeout_ms = 2000
fuel_budget = 5000000
max_memory_bytes = 33554432

[compat]
min_protocol_major = 1
min_runtime_version = "0.1.0"
"#
        .trim()
        .to_owned()
    }

    fn sample_sbom() -> Vec<u8> {
        br#"{"bomFormat":"CycloneDX","specVersion":"1.6"}"#.to_vec()
    }

    fn sample_provenance() -> Vec<u8> {
        br#"{"builder":{"id":"palyra-ci"},"subject":[{"name":"module.wasm"}]}"#.to_vec()
    }

    fn sample_request() -> SkillArtifactBuildRequest {
        SkillArtifactBuildRequest {
            manifest_toml: sample_manifest(),
            modules: vec![ArtifactFile {
                path: "module.wasm".to_owned(),
                bytes: vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
            }],
            assets: vec![ArtifactFile {
                path: "templates/prompt.txt".to_owned(),
                bytes: b"hello".to_vec(),
            }],
            sbom_cyclonedx_json: sample_sbom(),
            provenance_json: sample_provenance(),
            signing_key: [5_u8; 32],
        }
    }

    fn unique_temp_trust_store_path() -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir()
            .join(format!("palyra-skills-trust-store-{nonce}-{}.json", std::process::id()))
    }

    #[test]
    fn manifest_rejects_wildcard_without_opt_in() {
        let manifest = sample_manifest().replace("api.example.com", "*");
        let error = parse_manifest_toml(manifest.as_str()).expect_err("manifest should fail");
        assert!(matches!(error, SkillPackagingError::ManifestValidation(_)));
    }

    #[test]
    fn manifest_rejects_non_namespaced_tool_ids() {
        let manifest = sample_manifest().replace("id = \"acme.echo\"", "id = \"echo\"");
        let error = parse_manifest_toml(manifest.as_str()).expect_err("manifest should fail");
        assert!(matches!(error, SkillPackagingError::ManifestValidation(_)));
    }

    #[test]
    fn build_verify_and_tofu_flow() {
        let output = build_signed_skill_artifact(sample_request()).expect("artifact should build");
        let mut trust_store = SkillTrustStore::default();
        let first = verify_skill_artifact(output.artifact_bytes.as_slice(), &mut trust_store, true)
            .expect("verify with TOFU should pass");
        assert_eq!(first.trust_decision, TrustDecision::TofuNewlyPinned);
        let second =
            verify_skill_artifact(output.artifact_bytes.as_slice(), &mut trust_store, false)
                .expect("verify with pinned TOFU should pass");
        assert_eq!(second.trust_decision, TrustDecision::TofuPinned);
    }

    #[test]
    fn verify_fails_if_sbom_missing() {
        let output = build_signed_skill_artifact(sample_request()).expect("artifact should build");
        let mut entries = super::decode_zip(output.artifact_bytes.as_slice()).expect("zip decode");
        entries.remove(SBOM_PATH);
        let rebuilt = super::encode_zip(entries.iter()).expect("zip encode");
        let mut trust_store = SkillTrustStore::default();
        let error = verify_skill_artifact(rebuilt.as_slice(), &mut trust_store, true)
            .expect_err("verify should fail");
        assert!(matches!(error, SkillPackagingError::MissingArtifactEntry(_)));
    }

    #[test]
    fn verify_detects_tamper() {
        let output = build_signed_skill_artifact(sample_request()).expect("artifact should build");
        let mut entries = super::decode_zip(output.artifact_bytes.as_slice()).expect("zip decode");
        let module = entries.get_mut("modules/module.wasm").expect("module entry should exist");
        module.push(0xFF);
        let rebuilt = super::encode_zip(entries.iter()).expect("zip encode");
        let mut trust_store = SkillTrustStore::default();
        let error = verify_skill_artifact(rebuilt.as_slice(), &mut trust_store, true)
            .expect_err("verify should fail");
        assert!(matches!(
            error,
            SkillPackagingError::PayloadHashMismatch
                | SkillPackagingError::SignatureVerificationFailed
        ));
    }

    #[test]
    fn inspect_returns_verified_entries_for_installer() {
        let output = build_signed_skill_artifact(sample_request()).expect("artifact should build");
        let inspected = inspect_skill_artifact(output.artifact_bytes.as_slice())
            .expect("artifact should inspect");
        assert_eq!(inspected.manifest.skill_id, "acme.echo_http");
        assert_eq!(inspected.payload_sha256, output.payload_sha256);
        assert!(
            inspected.entries.contains_key(SIGNATURE_PATH),
            "signature entry should be available for extraction"
        );
        assert!(
            inspected.entries.contains_key("modules/module.wasm"),
            "module entry should be available for extraction"
        );
    }

    #[test]
    fn verify_rejects_artifact_with_excessive_total_uncompressed_size() {
        let mut entries = std::collections::BTreeMap::new();
        let chunk = vec![0_u8; 1024 * 1024];
        for index in 0..65 {
            entries.insert(format!("assets/chunk-{index}.bin"), chunk.clone());
        }
        let encoded = super::encode_zip(entries.iter()).expect("zip encode");
        assert!(
            encoded.len() < MAX_ARTIFACT_BYTES,
            "test artifact should stay under compressed artifact limit"
        );
        let error =
            super::decode_zip(encoded.as_slice()).expect_err("decode should enforce total budget");
        match error {
            SkillPackagingError::ArtifactTooLarge { limit, .. } => {
                assert_eq!(limit, MAX_ARTIFACT_BYTES);
            }
            other => panic!("expected ArtifactTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn build_rejects_artifact_with_too_many_entries() {
        let mut request = sample_request();
        request.assets.clear();
        request.modules = (0..MAX_ENTRIES)
            .map(|index| ArtifactFile {
                path: format!("module-{index}.wasm"),
                bytes: vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
            })
            .collect();

        let error = build_signed_skill_artifact(request).expect_err("build should fail");
        match error {
            SkillPackagingError::ArtifactTooManyEntries { limit, .. } => {
                assert_eq!(limit, MAX_ENTRIES);
            }
            other => panic!("expected ArtifactTooManyEntries, got {other:?}"),
        }
    }

    #[test]
    fn build_rejects_artifact_with_excessive_total_payload_size() {
        let mut request = sample_request();
        request.assets.clear();
        let module = vec![0_u8; 14 * 1024 * 1024];
        request.modules = (0..5)
            .map(|index| ArtifactFile {
                path: format!("large-{index}.wasm"),
                bytes: module.clone(),
            })
            .collect();

        let error = build_signed_skill_artifact(request).expect_err("build should fail");
        match error {
            SkillPackagingError::ArtifactTooLarge { limit, .. } => {
                assert_eq!(limit, MAX_ARTIFACT_BYTES);
            }
            other => panic!("expected ArtifactTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn trust_store_load_rejects_invalid_publisher() {
        let path = unique_temp_trust_store_path();
        let payload = serde_json::json!({
            "trusted_publishers": { "Acme": [hex::encode([7_u8; 32])] },
            "tofu_publishers": {}
        });
        std::fs::write(&path, serde_json::to_vec(&payload).expect("json payload"))
            .expect("trust store should be written");

        let error = SkillTrustStore::load(path.as_path()).expect_err("load should fail");
        assert!(
            error.to_string().contains("invalid trust-store publisher"),
            "error should explain trust-store publisher validation: {error}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trust_store_load_rejects_invalid_key() {
        let path = unique_temp_trust_store_path();
        let payload = serde_json::json!({
            "trusted_publishers": { "acme": ["not-hex"] },
            "tofu_publishers": {}
        });
        std::fs::write(&path, serde_json::to_vec(&payload).expect("json payload"))
            .expect("trust store should be written");

        let error = SkillTrustStore::load(path.as_path()).expect_err("load should fail");
        assert!(
            error.to_string().contains("invalid trusted key for publisher"),
            "error should explain trust-store key validation: {error}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn mapping_to_runtime_grants_and_policy_requests() {
        let manifest = parse_manifest_toml(sample_manifest().as_str()).expect("manifest");
        let grants = capability_grants_from_manifest(&manifest);
        assert_eq!(grants.http_hosts, vec!["api.example.com".to_owned()]);
        assert_eq!(grants.storage_prefixes, vec!["skills/cache".to_owned()]);
        let requests = policy_requests_from_manifest(&manifest);
        assert!(
            requests.iter().any(|request| request.action == "tool.execute"),
            "tool policy requests should be generated"
        );
    }

    #[test]
    fn signing_key_parser_accepts_raw_hex_and_base64() {
        let key = [13_u8; 32];
        assert_eq!(parse_ed25519_signing_key(key.as_slice()).expect("raw key"), key);
        let hex = hex::encode(key);
        assert_eq!(parse_ed25519_signing_key(hex.as_bytes()).expect("hex key"), key);
        let base64 = base64::engine::general_purpose::STANDARD.encode(key);
        assert_eq!(parse_ed25519_signing_key(base64.as_bytes()).expect("base64 key"), key);
    }
}
