use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use palyra_common::versioned_json::{
    migrate_updated_at_metadata_v0_to_v1, parse_versioned_json, JsonMigrationFn,
    VersionedJsonFormat,
};
use palyra_skills::{SkillConfigProperty, SkillConfigValueType, SkillManifest};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::wasm_plugin_runner::WasmPluginRunnerPolicy;

use crate::*;

const PLUGIN_BINDINGS_LAYOUT_VERSION: u32 = 2;
const PLUGIN_BINDINGS_INDEX_FILE_NAME: &str = "bindings.json";
const PLUGIN_BINDINGS_INDEX_FORMAT: VersionedJsonFormat =
    VersionedJsonFormat::new("plugin bindings index", PLUGIN_BINDINGS_LAYOUT_VERSION);
const PLUGIN_CONFIG_INSTANCE_LAYOUT_VERSION: u32 = 1;
const PLUGIN_CONFIG_INSTANCE_FILE_NAME: &str = "config.json";
const PLUGIN_CONFIG_INSTANCE_FORMAT: VersionedJsonFormat =
    VersionedJsonFormat::new("plugin config instance", PLUGIN_CONFIG_INSTANCE_LAYOUT_VERSION);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PluginBindingsIndex {
    pub(crate) schema_version: u32,
    pub(crate) updated_at_unix_ms: i64,
    #[serde(default)]
    pub(crate) entries: Vec<PluginBindingRecord>,
}

impl Default for PluginBindingsIndex {
    fn default() -> Self {
        Self {
            schema_version: PLUGIN_BINDINGS_LAYOUT_VERSION,
            updated_at_unix_ms: 0,
            entries: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PluginBindingRecord {
    pub(crate) plugin_id: String,
    pub(crate) enabled: bool,
    pub(crate) skill_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) skill_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) tool_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) module_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) entrypoint: Option<String>,
    #[serde(default)]
    pub(crate) capability_profile: PluginCapabilityProfile,
    #[serde(default)]
    pub(crate) operator: PluginOperatorMetadata,
    #[serde(default)]
    pub(crate) discovery: PluginDiscoverySnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) config: Option<PluginConfigInstanceRef>,
    #[serde(default)]
    pub(crate) capability_diff: PluginCapabilityDiffCache,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PluginCapabilityProfile {
    #[serde(default)]
    pub(crate) http_hosts: Vec<String>,
    #[serde(default)]
    pub(crate) secrets: Vec<String>,
    #[serde(default)]
    pub(crate) storage_prefixes: Vec<String>,
    #[serde(default)]
    pub(crate) channels: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PluginOperatorMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) notes: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) owner_principal: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) updated_by: Option<String>,
    #[serde(default)]
    pub(crate) tags: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PluginDiscoveryState {
    #[default]
    Unknown,
    Installed,
    Invalid,
    RequiresMigration,
    SignatureFailed,
    MissingModule,
    FilesystemUnsafe,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PluginFilesystemIssue {
    pub(crate) code: String,
    pub(crate) severity: String,
    pub(crate) message: String,
    pub(crate) remediation: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PluginFilesystemSafetySnapshot {
    #[serde(default)]
    pub(crate) safe: bool,
    #[serde(default)]
    pub(crate) issues: Vec<PluginFilesystemIssue>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PluginDiscoverySnapshot {
    #[serde(default)]
    pub(crate) state: PluginDiscoveryState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) last_scanned_at_unix_ms: Option<i64>,
    #[serde(default)]
    pub(crate) reasons: Vec<String>,
    #[serde(default)]
    pub(crate) missing_paths: Vec<String>,
    #[serde(default)]
    pub(crate) filesystem: PluginFilesystemSafetySnapshot,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PluginConfigValidationState {
    #[default]
    Unknown,
    Valid,
    Missing,
    Invalid,
    RequiresMigration,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PluginConfigValidationSnapshot {
    #[serde(default)]
    pub(crate) state: PluginConfigValidationState,
    #[serde(default)]
    pub(crate) issues: Vec<String>,
    #[serde(default)]
    pub(crate) redacted_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PluginConfigInstanceRef {
    pub(crate) schema_version: u32,
    pub(crate) path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) contract_schema_version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) manifest_payload_sha256: Option<String>,
    #[serde(default)]
    pub(crate) validation: PluginConfigValidationSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PluginConfigInstance {
    pub(crate) schema_version: u32,
    pub(crate) plugin_id: String,
    pub(crate) skill_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) skill_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) contract_schema_version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) manifest_payload_sha256: Option<String>,
    #[serde(default)]
    pub(crate) values: BTreeMap<String, Value>,
    pub(crate) updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PluginCapabilityDiffCategory {
    MissingGrant,
    ExcessGrant,
    PolicyRestricted,
    WildcardRisk,
    UnusedDeclaredCapability,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PluginCapabilityDiffEntry {
    pub(crate) category: PluginCapabilityDiffCategory,
    pub(crate) capability_kind: String,
    pub(crate) value: String,
    pub(crate) message: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PluginCapabilityDiffCache {
    #[serde(default)]
    pub(crate) valid: bool,
    #[serde(default)]
    pub(crate) declared: PluginCapabilityProfile,
    #[serde(default)]
    pub(crate) granted: PluginCapabilityProfile,
    #[serde(default)]
    pub(crate) effective: PluginCapabilityProfile,
    #[serde(default)]
    pub(crate) entries: Vec<PluginCapabilityDiffEntry>,
}

#[derive(Debug, Clone)]
pub(crate) struct PluginBindingUpsert {
    pub(crate) plugin_id: String,
    pub(crate) enabled: bool,
    pub(crate) skill_id: String,
    pub(crate) skill_version: Option<String>,
    pub(crate) tool_id: Option<String>,
    pub(crate) module_path: Option<String>,
    pub(crate) entrypoint: Option<String>,
    pub(crate) capability_profile: PluginCapabilityProfile,
    pub(crate) operator: PluginOperatorMetadata,
}

pub(crate) fn resolve_plugins_root() -> Result<PathBuf> {
    let skills_root = resolve_skills_root().map_err(|response| {
        anyhow!("failed to resolve skills root (http {})", response.status())
    })?;
    let state_root =
        skills_root.parent().map(FsPath::to_path_buf).unwrap_or_else(|| skills_root.clone());
    Ok(state_root.join("plugins"))
}

pub(crate) fn plugin_bindings_index_path(plugins_root: &FsPath) -> PathBuf {
    plugins_root.join(PLUGIN_BINDINGS_INDEX_FILE_NAME)
}

pub(crate) fn plugin_root_path(plugins_root: &FsPath, plugin_id: &str) -> PathBuf {
    plugins_root.join(plugin_id)
}

pub(crate) fn plugin_config_instance_path(plugins_root: &FsPath, plugin_id: &str) -> PathBuf {
    plugin_root_path(plugins_root, plugin_id).join(PLUGIN_CONFIG_INSTANCE_FILE_NAME)
}

pub(crate) fn load_plugin_bindings_index(plugins_root: &FsPath) -> Result<PluginBindingsIndex> {
    let path = plugin_bindings_index_path(plugins_root);
    if !path.exists() {
        return Ok(PluginBindingsIndex::default());
    }
    let payload = fs::read(path.as_path())
        .with_context(|| format!("failed to read plugin bindings index {}", path.display()))?;
    let mut index = parse_versioned_json::<PluginBindingsIndex>(
        payload.as_slice(),
        PLUGIN_BINDINGS_INDEX_FORMAT,
        &[
            (0, migrate_updated_at_metadata_v0_to_v1 as JsonMigrationFn),
            (1, migrate_plugin_bindings_v1_to_v2 as JsonMigrationFn),
        ],
    )
    .with_context(|| format!("failed to parse plugin bindings index {}", path.display()))?;
    normalize_plugin_bindings_index(&mut index);
    Ok(index)
}

pub(crate) fn save_plugin_bindings_index(
    plugins_root: &FsPath,
    index: &PluginBindingsIndex,
) -> Result<()> {
    fs::create_dir_all(plugins_root)
        .with_context(|| format!("failed to create plugins root {}", plugins_root.display()))?;
    let mut normalized = index.clone();
    normalized.schema_version = PLUGIN_BINDINGS_LAYOUT_VERSION;
    normalized.updated_at_unix_ms = unix_ms_now().context("failed to read system clock")?;
    normalize_plugin_bindings_index(&mut normalized);
    let payload = serde_json::to_vec_pretty(&normalized)
        .context("failed to serialize plugin bindings index")?;
    let path = plugin_bindings_index_path(plugins_root);
    fs::write(path.as_path(), payload)
        .with_context(|| format!("failed to write plugin bindings index {}", path.display()))
}

pub(crate) fn normalize_plugin_binding_upsert(
    request: PluginBindingUpsert,
    now_unix_ms: i64,
    existing: Option<&PluginBindingRecord>,
) -> Result<PluginBindingRecord> {
    Ok(PluginBindingRecord {
        plugin_id: normalize_registry_identifier(request.plugin_id.as_str(), "plugin_id")?,
        enabled: request.enabled,
        skill_id: normalize_registry_identifier(request.skill_id.as_str(), "skill_id")?,
        skill_version: normalize_optional_text(request.skill_version),
        tool_id: normalize_optional_tool_id(request.tool_id)?,
        module_path: normalize_optional_module_path(request.module_path)?,
        entrypoint: normalize_optional_entrypoint(request.entrypoint)?,
        capability_profile: normalize_plugin_capability_profile(request.capability_profile)?,
        operator: normalize_plugin_operator_metadata(request.operator),
        discovery: existing.map(|entry| entry.discovery.clone()).unwrap_or_default(),
        config: existing.and_then(|entry| entry.config.clone()),
        capability_diff: existing.map(|entry| entry.capability_diff.clone()).unwrap_or_default(),
        created_at_unix_ms: existing.map(|entry| entry.created_at_unix_ms).unwrap_or(now_unix_ms),
        updated_at_unix_ms: now_unix_ms,
    })
}

pub(crate) fn upsert_plugin_binding(
    index: &mut PluginBindingsIndex,
    record: PluginBindingRecord,
) -> PluginBindingRecord {
    if let Some(existing) =
        index.entries.iter_mut().find(|entry| entry.plugin_id == record.plugin_id)
    {
        *existing = record.clone();
        return record;
    }
    index.entries.push(record.clone());
    normalize_plugin_bindings_index(index);
    record
}

pub(crate) fn set_plugin_binding_enabled(
    index: &mut PluginBindingsIndex,
    plugin_id: &str,
    enabled: bool,
    updated_by: Option<&str>,
) -> Result<PluginBindingRecord> {
    let plugin_id = normalize_registry_identifier(plugin_id, "plugin_id")?;
    let now = unix_ms_now().context("failed to read system clock")?;
    let entry = index
        .entries
        .iter_mut()
        .find(|entry| entry.plugin_id == plugin_id)
        .ok_or_else(|| anyhow!("plugin binding not found: {plugin_id}"))?;
    entry.enabled = enabled;
    entry.updated_at_unix_ms = now;
    entry.operator.updated_by = updated_by.and_then(|value| trim_to_option(value.to_owned()));
    Ok(entry.clone())
}

pub(crate) fn delete_plugin_binding(
    index: &mut PluginBindingsIndex,
    plugin_id: &str,
) -> Result<PluginBindingRecord> {
    let plugin_id = normalize_registry_identifier(plugin_id, "plugin_id")?;
    let position = index
        .entries
        .iter()
        .position(|entry| entry.plugin_id == plugin_id)
        .ok_or_else(|| anyhow!("plugin binding not found: {plugin_id}"))?;
    Ok(index.entries.remove(position))
}

pub(crate) fn plugin_binding(
    index: &PluginBindingsIndex,
    plugin_id: &str,
) -> Result<PluginBindingRecord> {
    let plugin_id = normalize_registry_identifier(plugin_id, "plugin_id")?;
    index
        .entries
        .iter()
        .find(|entry| entry.plugin_id == plugin_id)
        .cloned()
        .ok_or_else(|| anyhow!("plugin binding not found: {plugin_id}"))
}

fn normalize_plugin_bindings_index(index: &mut PluginBindingsIndex) {
    index.schema_version = PLUGIN_BINDINGS_LAYOUT_VERSION;
    for entry in &mut index.entries {
        if entry.created_at_unix_ms == 0 {
            entry.created_at_unix_ms = entry.updated_at_unix_ms;
        }
    }
    index.entries.sort_by(|left, right| left.plugin_id.cmp(&right.plugin_id));
}

fn migrate_plugin_bindings_v1_to_v2(_object: &mut Map<String, Value>) -> Result<()> {
    Ok(())
}

fn normalize_registry_identifier(raw: &str, field_name: &'static str) -> Result<String> {
    let trimmed = raw.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        bail!("{field_name} cannot be empty");
    }
    if trimmed.len() > 128
        || !trimmed.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-')
        })
    {
        bail!("{field_name} must use only a-z, 0-9, '.', '_' or '-'");
    }
    Ok(trimmed)
}

fn normalize_optional_text(value: Option<String>) -> Option<String> {
    value.and_then(trim_to_option)
}

fn normalize_optional_tool_id(value: Option<String>) -> Result<Option<String>> {
    let Some(tool_id) = value.and_then(trim_to_option) else {
        return Ok(None);
    };
    if !tool_id
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-'))
    {
        bail!("tool_id contains invalid characters");
    }
    Ok(Some(tool_id))
}

fn normalize_optional_module_path(value: Option<String>) -> Result<Option<String>> {
    let Some(path) = value.and_then(trim_to_option) else {
        return Ok(None);
    };
    if path.contains('\0')
        || path.contains("..")
        || path.starts_with('/')
        || path.starts_with('\\')
        || !path.starts_with("modules/")
        || !path.ends_with(".wasm")
    {
        bail!("module_path must reference a modules/*.wasm entry inside the signed skill artifact");
    }
    Ok(Some(path))
}

fn normalize_optional_entrypoint(value: Option<String>) -> Result<Option<String>> {
    let Some(entrypoint) = value.and_then(trim_to_option) else {
        return Ok(None);
    };
    if !entrypoint
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '_' | '-'))
    {
        bail!("entrypoint contains invalid characters");
    }
    Ok(Some(entrypoint))
}

fn normalize_plugin_capability_profile(
    profile: PluginCapabilityProfile,
) -> Result<PluginCapabilityProfile> {
    Ok(PluginCapabilityProfile {
        http_hosts: dedupe_sorted(profile.http_hosts.into_iter(), normalize_host_capability)?,
        secrets: dedupe_sorted(profile.secrets.into_iter(), normalize_identifier_capability)?,
        storage_prefixes: dedupe_sorted(
            profile.storage_prefixes.into_iter(),
            normalize_storage_prefix_capability,
        )?,
        channels: dedupe_sorted(profile.channels.into_iter(), normalize_identifier_capability)?,
    })
}

fn normalize_plugin_operator_metadata(
    mut operator: PluginOperatorMetadata,
) -> PluginOperatorMetadata {
    operator.display_name = operator.display_name.and_then(trim_to_option);
    operator.notes = operator.notes.and_then(trim_to_option);
    operator.owner_principal = operator.owner_principal.and_then(trim_to_option);
    operator.updated_by = operator.updated_by.and_then(trim_to_option);
    operator.tags = operator
        .tags
        .into_iter()
        .filter_map(trim_to_option)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    operator
}

fn normalize_host_capability(candidate: String) -> Result<String> {
    let normalized = candidate.trim().trim_end_matches('.').to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.starts_with('-')
        || normalized.ends_with('-')
        || normalized.starts_with('.')
        || normalized.ends_with('.')
        || normalized.contains("..")
        || !normalized
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '-'))
    {
        bail!("invalid http host capability '{}'", candidate.trim());
    }
    Ok(normalized)
}

fn normalize_identifier_capability(candidate: String) -> Result<String> {
    let normalized = candidate.trim().to_owned();
    if normalized.is_empty()
        || !normalized.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-' | '/')
        })
    {
        bail!("invalid capability identifier '{}'", candidate.trim());
    }
    Ok(normalized)
}

fn normalize_storage_prefix_capability(candidate: String) -> Result<String> {
    let normalized = candidate.trim().to_owned();
    if normalized.is_empty()
        || normalized.contains('\0')
        || normalized.contains("..")
        || normalized.starts_with('/')
        || normalized.starts_with('\\')
        || !normalized.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '/' | '.' | '_' | '-')
        })
    {
        bail!("invalid storage prefix capability '{}'", candidate.trim());
    }
    Ok(normalized)
}

fn dedupe_sorted<I, F>(values: I, normalize: F) -> Result<Vec<String>>
where
    I: IntoIterator<Item = String>,
    F: Fn(String) -> Result<String>,
{
    let mut normalized = BTreeSet::new();
    for candidate in values {
        if candidate.trim().is_empty() {
            continue;
        }
        normalized.insert(normalize(candidate)?);
    }
    Ok(normalized.into_iter().collect())
}

pub(crate) fn prepare_plugin_root(plugins_root: &FsPath, plugin_id: &str) -> Result<PathBuf> {
    let plugin_id = normalize_registry_identifier(plugin_id, "plugin_id")?;
    fs::create_dir_all(plugins_root)
        .with_context(|| format!("failed to create plugins root {}", plugins_root.display()))?;
    let plugin_root = plugin_root_path(plugins_root, plugin_id.as_str());
    fs::create_dir_all(plugin_root.as_path())
        .with_context(|| format!("failed to create plugin root {}", plugin_root.display()))?;
    Ok(plugin_root)
}

pub(crate) fn load_plugin_config_instance(
    plugins_root: &FsPath,
    plugin_id: &str,
) -> Result<Option<PluginConfigInstance>> {
    let plugin_id = normalize_registry_identifier(plugin_id, "plugin_id")?;
    let path = plugin_config_instance_path(plugins_root, plugin_id.as_str());
    if !path.exists() {
        return Ok(None);
    }
    let payload = fs::read(path.as_path())
        .with_context(|| format!("failed to read plugin config instance {}", path.display()))?;
    parse_versioned_json::<PluginConfigInstance>(
        payload.as_slice(),
        PLUGIN_CONFIG_INSTANCE_FORMAT,
        &[(0, migrate_updated_at_metadata_v0_to_v1 as JsonMigrationFn)],
    )
    .with_context(|| format!("failed to parse plugin config instance {}", path.display()))
    .map(Some)
}

pub(crate) fn save_plugin_config_instance(
    plugins_root: &FsPath,
    instance: &PluginConfigInstance,
) -> Result<()> {
    let plugin_root = prepare_plugin_root(plugins_root, instance.plugin_id.as_str())?;
    let path = plugin_root.join(PLUGIN_CONFIG_INSTANCE_FILE_NAME);
    let payload = serde_json::to_vec_pretty(instance)
        .context("failed to serialize plugin config instance")?;
    fs::write(path.as_path(), payload)
        .with_context(|| format!("failed to write plugin config instance {}", path.display()))
}

pub(crate) fn remove_plugin_config_instance(
    plugins_root: &FsPath,
    plugin_id: &str,
) -> Result<()> {
    let plugin_id = normalize_registry_identifier(plugin_id, "plugin_id")?;
    let path = plugin_config_instance_path(plugins_root, plugin_id.as_str());
    if path.exists() {
        fs::remove_file(path.as_path())
            .with_context(|| format!("failed to remove plugin config instance {}", path.display()))?;
    }
    let plugin_root = plugin_root_path(plugins_root, plugin_id.as_str());
    if plugin_root.exists() {
        let _ = fs::remove_dir(plugin_root.as_path());
    }
    Ok(())
}

pub(crate) fn inspect_plugin_filesystem_safety(
    plugins_root: &FsPath,
    plugin_id: &str,
) -> PluginFilesystemSafetySnapshot {
    let plugin_id = match normalize_registry_identifier(plugin_id, "plugin_id") {
        Ok(value) => value,
        Err(error) => {
            return PluginFilesystemSafetySnapshot {
                safe: false,
                issues: vec![PluginFilesystemIssue {
                    code: "invalid_plugin_id".to_owned(),
                    severity: "error".to_owned(),
                    message: error.to_string(),
                    remediation: "rename the plugin binding so plugin_id uses only safe registry characters"
                        .to_owned(),
                }],
            };
        }
    };
    let mut issues = Vec::new();
    let plugin_root = plugin_root_path(plugins_root, plugin_id.as_str());
    let config_path = plugin_root.join(PLUGIN_CONFIG_INSTANCE_FILE_NAME);
    inspect_path_safety(plugins_root, plugins_root, "plugins_root", &mut issues);
    inspect_path_safety(plugins_root, plugin_root.as_path(), "plugin_root", &mut issues);
    inspect_path_safety(plugins_root, config_path.as_path(), "plugin_config", &mut issues);
    PluginFilesystemSafetySnapshot { safe: issues.is_empty(), issues }
}

pub(crate) fn validate_plugin_config_instance(
    manifest: &SkillManifest,
    instance: Option<&PluginConfigInstance>,
    manifest_payload_sha256: Option<&str>,
) -> (PluginConfigValidationSnapshot, BTreeMap<String, Value>) {
    let Some(contract) = manifest.operator.config.as_ref() else {
        if let Some(instance) = instance {
            if !instance.values.is_empty() {
                return (
                    PluginConfigValidationSnapshot {
                        state: PluginConfigValidationState::Invalid,
                        issues: vec![
                            "plugin config instance exists but the manifest does not declare operator.config"
                                .to_owned(),
                        ],
                        redacted_fields: Vec::new(),
                    },
                    BTreeMap::new(),
                );
            }
        }
        return (
            PluginConfigValidationSnapshot {
                state: PluginConfigValidationState::Valid,
                issues: Vec::new(),
                redacted_fields: Vec::new(),
            },
            BTreeMap::new(),
        );
    };

    let mut effective = BTreeMap::new();
    let mut issues = Vec::new();
    let mut redacted_fields = contract
        .properties
        .iter()
        .filter(|(_, property)| property.redacted)
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    redacted_fields.sort();

    let instance_values = instance.map(|value| &value.values);
    if let Some(instance) = instance {
        if instance.contract_schema_version != Some(contract.schema_version) {
            issues.push(format!(
                "config contract schema mismatch (instance {:?}, manifest {})",
                instance.contract_schema_version, contract.schema_version
            ));
            return (
                PluginConfigValidationSnapshot {
                    state: PluginConfigValidationState::RequiresMigration,
                    issues,
                    redacted_fields,
                },
                BTreeMap::new(),
            );
        }
        if manifest_payload_sha256.is_some()
            && instance.manifest_payload_sha256.as_deref() != manifest_payload_sha256
        {
            issues.push("config manifest digest does not match the currently installed skill".to_owned());
            return (
                PluginConfigValidationSnapshot {
                    state: PluginConfigValidationState::RequiresMigration,
                    issues,
                    redacted_fields,
                },
                BTreeMap::new(),
            );
        }
        for key in instance.values.keys() {
            if !contract.properties.contains_key(key) {
                issues.push(format!("config property '{}' is not declared by the manifest", key));
            }
        }
    }

    for (name, property) in &contract.properties {
        let value = instance_values
            .and_then(|values| values.get(name))
            .cloned()
            .or_else(|| property.default.clone());
        match value {
            Some(value) => match validate_config_value_against_property(name.as_str(), property, &value) {
                Ok(()) => {
                    effective.insert(name.clone(), value);
                }
                Err(error) => issues.push(error),
            },
            None if contract.required.iter().any(|required| required == name) => {
                issues.push(format!("required config property '{}' is missing", name));
            }
            None => {}
        }
    }

    let state = if issues.is_empty() {
        PluginConfigValidationState::Valid
    } else if issues.iter().all(|message| message.contains("required config property")) {
        PluginConfigValidationState::Missing
    } else {
        PluginConfigValidationState::Invalid
    };
    (
        PluginConfigValidationSnapshot { state, issues, redacted_fields },
        effective,
    )
}

pub(crate) fn redact_plugin_config_values(
    values: &BTreeMap<String, Value>,
    validation: &PluginConfigValidationSnapshot,
) -> BTreeMap<String, Value> {
    let redacted = validation
        .redacted_fields
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    values
        .iter()
        .map(|(name, value)| {
            if redacted.contains(name.as_str()) {
                (name.clone(), Value::String("[redacted]".to_owned()))
            } else {
                (name.clone(), value.clone())
            }
        })
        .collect()
}

pub(crate) fn build_plugin_capability_diff(
    manifest: &SkillManifest,
    granted: &PluginCapabilityProfile,
    policy: &WasmPluginRunnerPolicy,
) -> PluginCapabilityDiffCache {
    let declared = plugin_capability_profile_from_manifest(manifest);
    let granted = normalize_plugin_capability_profile(granted.clone()).unwrap_or_default();
    let policy_profile = plugin_capability_profile_from_wasm_policy(policy);
    let effective = intersect_profiles(&intersect_profiles(&declared, &granted), &policy_profile);
    let mut entries = Vec::new();
    append_profile_diffs(
        &mut entries,
        PluginCapabilityDiffCategory::ExcessGrant,
        "binding grants capability not declared by manifest",
        &granted,
        &declared,
    );
    append_profile_diffs(
        &mut entries,
        PluginCapabilityDiffCategory::MissingGrant,
        "manifest declares capability that binding does not grant",
        &declared,
        &granted,
    );
    append_profile_diffs(
        &mut entries,
        PluginCapabilityDiffCategory::UnusedDeclaredCapability,
        "declared capability is currently unused because the binding omits it",
        &declared,
        &granted,
    );
    append_profile_diffs(
        &mut entries,
        PluginCapabilityDiffCategory::PolicyRestricted,
        "binding grant is restricted by runtime policy",
        &granted,
        &effective,
    );
    append_wildcard_risks(&mut entries, &declared);
    entries.sort_by(|left, right| {
        left.capability_kind
            .cmp(&right.capability_kind)
            .then_with(|| left.value.cmp(&right.value))
            .then_with(|| format!("{:?}", left.category).cmp(&format!("{:?}", right.category)))
    });
    PluginCapabilityDiffCache {
        valid: !entries.iter().any(|entry| {
            matches!(
                entry.category,
                PluginCapabilityDiffCategory::ExcessGrant | PluginCapabilityDiffCategory::PolicyRestricted
            )
        }),
        declared,
        granted,
        effective,
        entries,
    }
}

pub(crate) fn build_plugin_discovery_snapshot(
    binding: &PluginBindingRecord,
    resolved_ok: bool,
    resolve_error: Option<&str>,
    skill_trust_decision: Option<&str>,
    filesystem: PluginFilesystemSafetySnapshot,
    config_validation: Option<&PluginConfigValidationSnapshot>,
) -> PluginDiscoverySnapshot {
    let mut reasons = Vec::new();
    let mut missing_paths = Vec::new();
    let state = if !filesystem.safe {
        reasons.extend(filesystem.issues.iter().map(|issue| issue.message.clone()));
        PluginDiscoveryState::FilesystemUnsafe
    } else if matches!(skill_trust_decision, Some("untrusted_override")) {
        reasons.push("installed skill artifact is running under untrusted override".to_owned());
        PluginDiscoveryState::SignatureFailed
    } else if matches!(
        config_validation.map(|snapshot| snapshot.state),
        Some(PluginConfigValidationState::RequiresMigration)
    ) {
        reasons.extend(
            config_validation
                .into_iter()
                .flat_map(|snapshot| snapshot.issues.iter().cloned()),
        );
        PluginDiscoveryState::RequiresMigration
    } else if let Some(error) = resolve_error {
        reasons.push(error.to_owned());
        if error.contains("module") {
            if let Some(path) = binding.module_path.clone() {
                missing_paths.push(path);
            }
            PluginDiscoveryState::MissingModule
        } else {
            PluginDiscoveryState::Invalid
        }
    } else if matches!(
        config_validation.map(|snapshot| snapshot.state),
        Some(PluginConfigValidationState::Missing | PluginConfigValidationState::Invalid)
    ) {
        reasons.extend(
            config_validation
                .into_iter()
                .flat_map(|snapshot| snapshot.issues.iter().cloned()),
        );
        PluginDiscoveryState::Invalid
    } else if resolved_ok {
        PluginDiscoveryState::Installed
    } else {
        PluginDiscoveryState::Unknown
    };
    PluginDiscoverySnapshot {
        state,
        last_scanned_at_unix_ms: unix_ms_now().ok(),
        reasons,
        missing_paths,
        filesystem,
    }
}

fn inspect_path_safety(
    plugins_root: &FsPath,
    path: &Path,
    label: &'static str,
    issues: &mut Vec<PluginFilesystemIssue>,
) {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return;
    };
    if metadata.file_type().is_symlink() {
        issues.push(PluginFilesystemIssue {
            code: format!("{label}_symlink"),
            severity: "error".to_owned(),
            message: format!("{} must not be a symlink: {}", label.replace('_', " "), path.display()),
            remediation:
                "replace the symlink with a real directory or file under the managed plugins root"
                    .to_owned(),
        });
        return;
    }
    if let Ok(canonical_root) = plugins_root.canonicalize() {
        if let Ok(canonical_path) = path.canonicalize() {
            if !canonical_path.starts_with(canonical_root.as_path()) {
                issues.push(PluginFilesystemIssue {
                    code: format!("{label}_escape"),
                    severity: "error".to_owned(),
                    message: format!(
                        "{} escapes the managed plugins root: {}",
                        label.replace('_', " "),
                        path.display()
                    ),
                    remediation:
                        "move the plugin files back under the managed plugins root and remove traversal indirection"
                            .to_owned(),
                });
            }
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if metadata.permissions().mode() & 0o022 != 0 {
            issues.push(PluginFilesystemIssue {
                code: format!("{label}_writable"),
                severity: "error".to_owned(),
                message: format!(
                    "{} is group/world writable: {}",
                    label.replace('_', " "),
                    path.display()
                ),
                remediation:
                    "restrict permissions so only the owning account can modify managed plugin files"
                        .to_owned(),
            });
        }
    }
}

fn validate_config_value_against_property(
    name: &str,
    property: &SkillConfigProperty,
    value: &Value,
) -> std::result::Result<(), String> {
    let valid = match property.value_type {
        SkillConfigValueType::String => value.is_string(),
        SkillConfigValueType::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
        SkillConfigValueType::Number => value.is_number(),
        SkillConfigValueType::Boolean => value.is_boolean(),
        SkillConfigValueType::StringList => value
            .as_array()
            .is_some_and(|values| values.iter().all(|candidate| candidate.as_str().is_some())),
    };
    if !valid {
        return Err(format!(
            "config property '{}' does not match declared type '{}'",
            name,
            config_value_type_label(property.value_type)
        ));
    }
    if !property.enum_values.is_empty() {
        let Some(raw) = value.as_str() else {
            return Err(format!(
                "config property '{}' must be a string because enum_values are declared",
                name
            ));
        };
        if !property.enum_values.iter().any(|candidate| candidate == raw) {
            return Err(format!(
                "config property '{}' must be one of [{}]",
                name,
                property.enum_values.join(", ")
            ));
        }
    }
    Ok(())
}

fn config_value_type_label(value_type: SkillConfigValueType) -> &'static str {
    match value_type {
        SkillConfigValueType::String => "string",
        SkillConfigValueType::Integer => "integer",
        SkillConfigValueType::Number => "number",
        SkillConfigValueType::Boolean => "boolean",
        SkillConfigValueType::StringList => "string_list",
    }
}

fn plugin_capability_profile_from_wasm_policy(policy: &WasmPluginRunnerPolicy) -> PluginCapabilityProfile {
    PluginCapabilityProfile {
        http_hosts: policy.allowed_http_hosts.clone(),
        secrets: policy.allowed_secrets.clone(),
        storage_prefixes: policy.allowed_storage_prefixes.clone(),
        channels: policy.allowed_channels.clone(),
    }
}

fn intersect_profiles(
    left: &PluginCapabilityProfile,
    right: &PluginCapabilityProfile,
) -> PluginCapabilityProfile {
    PluginCapabilityProfile {
        http_hosts: intersect_capability_list(left.http_hosts.as_slice(), right.http_hosts.as_slice()),
        secrets: intersect_capability_list(left.secrets.as_slice(), right.secrets.as_slice()),
        storage_prefixes: intersect_capability_list(
            left.storage_prefixes.as_slice(),
            right.storage_prefixes.as_slice(),
        ),
        channels: intersect_capability_list(left.channels.as_slice(), right.channels.as_slice()),
    }
}

fn intersect_capability_list(left: &[String], right: &[String]) -> Vec<String> {
    let right = right.iter().map(String::as_str).collect::<BTreeSet<_>>();
    left.iter()
        .filter(|candidate| right.contains(candidate.as_str()))
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn append_profile_diffs(
    entries: &mut Vec<PluginCapabilityDiffEntry>,
    category: PluginCapabilityDiffCategory,
    message: &'static str,
    source: &PluginCapabilityProfile,
    target: &PluginCapabilityProfile,
) {
    append_capability_list_diffs(
        entries,
        category,
        "http_hosts",
        source.http_hosts.as_slice(),
        target.http_hosts.as_slice(),
        message,
    );
    append_capability_list_diffs(
        entries,
        category,
        "secrets",
        source.secrets.as_slice(),
        target.secrets.as_slice(),
        message,
    );
    append_capability_list_diffs(
        entries,
        category,
        "storage_prefixes",
        source.storage_prefixes.as_slice(),
        target.storage_prefixes.as_slice(),
        message,
    );
    append_capability_list_diffs(
        entries,
        category,
        "channels",
        source.channels.as_slice(),
        target.channels.as_slice(),
        message,
    );
}

fn append_capability_list_diffs(
    entries: &mut Vec<PluginCapabilityDiffEntry>,
    category: PluginCapabilityDiffCategory,
    capability_kind: &'static str,
    source: &[String],
    target: &[String],
    message: &'static str,
) {
    let target = target.iter().map(String::as_str).collect::<BTreeSet<_>>();
    for value in source
        .iter()
        .filter(|candidate| !target.contains(candidate.as_str()))
        .cloned()
        .collect::<BTreeSet<_>>()
    {
        entries.push(PluginCapabilityDiffEntry {
            category,
            capability_kind: capability_kind.to_owned(),
            value,
            message: message.to_owned(),
        });
    }
}

fn append_wildcard_risks(entries: &mut Vec<PluginCapabilityDiffEntry>, profile: &PluginCapabilityProfile) {
    append_wildcard_entries(entries, "http_hosts", profile.http_hosts.as_slice());
    append_wildcard_entries(entries, "secrets", profile.secrets.as_slice());
    append_wildcard_entries(entries, "storage_prefixes", profile.storage_prefixes.as_slice());
    append_wildcard_entries(entries, "channels", profile.channels.as_slice());
}

fn append_wildcard_entries(
    entries: &mut Vec<PluginCapabilityDiffEntry>,
    capability_kind: &'static str,
    values: &[String],
) {
    for value in values.iter().filter(|candidate| candidate.contains('*')) {
        entries.push(PluginCapabilityDiffEntry {
            category: PluginCapabilityDiffCategory::WildcardRisk,
            capability_kind: capability_kind.to_owned(),
            value: value.clone(),
            message: "wildcard capability expands operator blast radius and should be reviewed"
                .to_owned(),
        });
    }
}

impl PluginCapabilityProfile {
    pub(crate) fn is_empty(&self) -> bool {
        self.http_hosts.is_empty()
            && self.secrets.is_empty()
            && self.storage_prefixes.is_empty()
            && self.channels.is_empty()
    }

    pub(crate) fn to_requested_capabilities(
        &self,
    ) -> crate::wasm_plugin_runner::WasmPluginRequestedCapabilities {
        crate::wasm_plugin_runner::WasmPluginRequestedCapabilities {
            http_hosts: self.http_hosts.clone(),
            secrets: self.secrets.clone(),
            storage_prefixes: self.storage_prefixes.clone(),
            channels: self.channels.clone(),
        }
    }
}

pub(crate) fn plugin_capability_profile_from_manifest(
    manifest: &SkillManifest,
) -> PluginCapabilityProfile {
    PluginCapabilityProfile {
        http_hosts: manifest.capabilities.http_egress_allowlist.clone(),
        secrets: manifest
            .capabilities
            .secrets
            .iter()
            .flat_map(|scope| scope.key_names.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
        storage_prefixes: manifest.capabilities.filesystem.write_roots.clone(),
        channels: manifest.capabilities.node_capabilities.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs};

    use palyra_skills::parse_manifest_toml;
    use serde_json::Value;
    use tempfile::tempdir;

    use super::{
        build_plugin_capability_diff, inspect_plugin_filesystem_safety, load_plugin_bindings_index,
        plugin_bindings_index_path, redact_plugin_config_values, save_plugin_bindings_index,
        validate_plugin_config_instance, PluginBindingRecord, PluginBindingsIndex,
        PluginCapabilityDiffCategory, PluginCapabilityDiffCache, PluginCapabilityDiffEntry,
        PluginCapabilityProfile, PluginConfigInstance, PluginConfigInstanceRef,
        PluginConfigValidationSnapshot, PluginConfigValidationState, PluginDiscoverySnapshot,
        PluginDiscoveryState, PluginFilesystemSafetySnapshot, PLUGIN_BINDINGS_LAYOUT_VERSION,
    };
    use crate::wasm_plugin_runner::WasmPluginRunnerPolicy;

    #[test]
    fn load_plugin_bindings_index_migrates_legacy_metadata() {
        let tempdir = tempdir().expect("temporary directory should be created");
        let index_path = plugin_bindings_index_path(tempdir.path());
        fs::write(index_path, br#"{"entries":[]}"#)
            .expect("legacy plugin bindings index should be written");
        let index = load_plugin_bindings_index(tempdir.path())
            .expect("legacy plugin bindings index should load");
        assert_eq!(index.schema_version, PLUGIN_BINDINGS_LAYOUT_VERSION);
        assert_eq!(index.updated_at_unix_ms, 0);
        assert!(index.entries.is_empty());
    }

    #[test]
    fn load_plugin_bindings_index_migrates_v1_binding_records_to_v2_defaults() {
        let tempdir = tempdir().expect("temporary directory should be created");
        let index_path = plugin_bindings_index_path(tempdir.path());
        fs::write(
            index_path,
            br#"{
  "schema_version": 1,
  "updated_at_unix_ms": 42,
  "entries": [
    {
      "plugin_id": "acme.echo_plugin",
      "enabled": true,
      "skill_id": "acme.echo_http",
      "skill_version": "1.2.3",
      "tool_id": "acme.echo",
      "module_path": "modules/plugin.wasm",
      "entrypoint": "run",
      "capability_profile": {
        "http_hosts": ["api.example.com"],
        "secrets": ["api_token"],
        "storage_prefixes": ["skills/cache"],
        "channels": ["discord"]
      },
      "operator": {
        "display_name": "Echo plugin",
        "tags": ["prod"]
      },
      "created_at_unix_ms": 0,
      "updated_at_unix_ms": 42
    }
  ]
}"#,
        )
        .expect("legacy v1 plugin bindings index should be written");

        let index =
            load_plugin_bindings_index(tempdir.path()).expect("legacy v1 plugin bindings should load");
        assert_eq!(index.schema_version, PLUGIN_BINDINGS_LAYOUT_VERSION);
        assert_eq!(index.entries.len(), 1);
        let entry = &index.entries[0];
        assert_eq!(entry.plugin_id, "acme.echo_plugin");
        assert_eq!(entry.created_at_unix_ms, 42);
        assert_eq!(entry.discovery.state, PluginDiscoveryState::Unknown);
        assert!(entry.discovery.reasons.is_empty());
        assert!(entry.config.is_none());
        assert!(entry.capability_diff.entries.is_empty());
    }

    #[test]
    fn save_and_load_plugin_bindings_index_round_trips_v2_operability_metadata() {
        let tempdir = tempdir().expect("temporary directory should be created");
        let expected = PluginBindingsIndex {
            schema_version: PLUGIN_BINDINGS_LAYOUT_VERSION,
            updated_at_unix_ms: 1234,
            entries: vec![PluginBindingRecord {
                plugin_id: "acme.echo_plugin".to_owned(),
                enabled: true,
                skill_id: "acme.echo_http".to_owned(),
                skill_version: Some("1.2.3".to_owned()),
                tool_id: Some("acme.echo".to_owned()),
                module_path: Some("modules/plugin.wasm".to_owned()),
                entrypoint: Some("run".to_owned()),
                capability_profile: PluginCapabilityProfile {
                    http_hosts: vec!["api.example.com".to_owned()],
                    secrets: vec!["api_token".to_owned()],
                    storage_prefixes: vec!["skills/cache".to_owned()],
                    channels: vec!["discord".to_owned()],
                },
                operator: super::PluginOperatorMetadata {
                    display_name: Some("Echo plugin".to_owned()),
                    notes: Some("ops managed".to_owned()),
                    owner_principal: Some("user:ops".to_owned()),
                    updated_by: Some("admin:web-console".to_owned()),
                    tags: vec!["prod".to_owned()],
                },
                discovery: PluginDiscoverySnapshot {
                    state: PluginDiscoveryState::Installed,
                    last_scanned_at_unix_ms: Some(4321),
                    reasons: vec!["installed".to_owned()],
                    missing_paths: Vec::new(),
                    filesystem: PluginFilesystemSafetySnapshot { safe: true, issues: Vec::new() },
                },
                config: Some(PluginConfigInstanceRef {
                    schema_version: 1,
                    path: "acme.echo_plugin/config.json".to_owned(),
                    contract_schema_version: Some(1),
                    manifest_payload_sha256: Some("digest-1".to_owned()),
                    validation: PluginConfigValidationSnapshot {
                        state: PluginConfigValidationState::Valid,
                        issues: Vec::new(),
                        redacted_fields: vec!["api_token".to_owned()],
                    },
                }),
                capability_diff: PluginCapabilityDiffCache {
                    valid: false,
                    declared: PluginCapabilityProfile {
                        http_hosts: vec!["api.example.com".to_owned()],
                        secrets: vec!["api_token".to_owned()],
                        storage_prefixes: vec!["skills/cache".to_owned()],
                        channels: vec!["discord".to_owned()],
                    },
                    granted: PluginCapabilityProfile {
                        http_hosts: vec!["api.example.com".to_owned()],
                        secrets: vec!["api_token".to_owned(), "extra_secret".to_owned()],
                        storage_prefixes: vec!["skills/cache".to_owned()],
                        channels: vec!["discord".to_owned()],
                    },
                    effective: PluginCapabilityProfile {
                        http_hosts: vec!["api.example.com".to_owned()],
                        secrets: vec!["api_token".to_owned()],
                        storage_prefixes: vec!["skills/cache".to_owned()],
                        channels: vec!["discord".to_owned()],
                    },
                    entries: vec![PluginCapabilityDiffEntry {
                        category: PluginCapabilityDiffCategory::ExcessGrant,
                        capability_kind: "secrets".to_owned(),
                        value: "extra_secret".to_owned(),
                        message: "binding grants capability not declared by manifest".to_owned(),
                    }],
                },
                created_at_unix_ms: 1111,
                updated_at_unix_ms: 2222,
            }],
        };

        save_plugin_bindings_index(tempdir.path(), &expected)
            .expect("v2 plugin bindings index should save");
        let loaded =
            load_plugin_bindings_index(tempdir.path()).expect("v2 plugin bindings index should load");
        assert_eq!(loaded.schema_version, expected.schema_version);
        assert!(
            loaded.updated_at_unix_ms >= expected.updated_at_unix_ms,
            "save should refresh the index updated_at timestamp"
        );
        assert_eq!(loaded.entries, expected.entries);
    }

    #[test]
    fn load_plugin_bindings_index_rejects_invalid_entries_payload() {
        let tempdir = tempdir().expect("temporary directory should be created");
        let index_path = plugin_bindings_index_path(tempdir.path());
        fs::write(index_path, br#"{"schema_version":2,"updated_at_unix_ms":0,"entries":{}}"#)
            .expect("corrupted plugin bindings index should be written");
        let error =
            load_plugin_bindings_index(tempdir.path()).expect_err("invalid entries payload must fail");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("expected a sequence"),
            "invalid entries payload should explain the sequence type mismatch: {rendered}"
        );
    }

    #[test]
    fn validate_plugin_config_instance_applies_defaults_and_redacts_sensitive_fields() {
        let manifest = plugin_manifest_with_operator_config(
            r#"
[operator.config.properties.api_base_url]
type = "string"
title = "API base URL"
default = "https://api.example.com"

[operator.config.properties.api_token]
type = "string"
title = "API token"
redacted = true
"#,
        );
        let instance = PluginConfigInstance {
            schema_version: 1,
            plugin_id: "acme.echo_plugin".to_owned(),
            skill_id: "acme.echo_http".to_owned(),
            skill_version: Some("1.2.3".to_owned()),
            contract_schema_version: Some(1),
            manifest_payload_sha256: Some("digest-1".to_owned()),
            values: BTreeMap::from([(
                "api_token".to_owned(),
                Value::String("secret-token".to_owned()),
            )]),
            updated_at_unix_ms: 42,
        };

        let (validation, effective) =
            validate_plugin_config_instance(&manifest, Some(&instance), Some("digest-1"));
        assert_eq!(validation.state, PluginConfigValidationState::Valid);
        assert_eq!(
            effective.get("api_base_url"),
            Some(&Value::String("https://api.example.com".to_owned()))
        );
        assert_eq!(
            effective.get("api_token"),
            Some(&Value::String("secret-token".to_owned()))
        );
        assert_eq!(validation.redacted_fields, vec!["api_token".to_owned()]);

        let redacted = redact_plugin_config_values(&effective, &validation);
        assert_eq!(
            redacted.get("api_token"),
            Some(&Value::String("[redacted]".to_owned()))
        );
    }

    #[test]
    fn validate_plugin_config_instance_reports_missing_required_values() {
        let manifest = plugin_manifest_with_operator_config(
            r#"
[operator.config.properties.api_base_url]
type = "string"
title = "API base URL"

[operator.config.properties.api_token]
type = "string"
title = "API token"
redacted = true
"#,
        );
        let (validation, effective) = validate_plugin_config_instance(&manifest, None, Some("digest-1"));
        assert_eq!(validation.state, PluginConfigValidationState::Missing);
        assert!(
            validation
                .issues
                .iter()
                .any(|issue| issue.contains("required config property 'api_base_url' is missing"))
        );
        assert!(
            validation
                .issues
                .iter()
                .any(|issue| issue.contains("required config property 'api_token' is missing"))
        );
        assert!(effective.is_empty());
    }

    #[test]
    fn validate_plugin_config_instance_requires_migration_when_contract_changes() {
        let manifest = plugin_manifest_with_operator_config(
            r#"
[operator.config.properties.api_base_url]
type = "string"
title = "API base URL"

[operator.config.properties.api_token]
type = "string"
title = "API token"
redacted = true
"#,
        );
        let instance = PluginConfigInstance {
            schema_version: 1,
            plugin_id: "acme.echo_plugin".to_owned(),
            skill_id: "acme.echo_http".to_owned(),
            skill_version: Some("1.2.3".to_owned()),
            contract_schema_version: Some(2),
            manifest_payload_sha256: Some("digest-old".to_owned()),
            values: BTreeMap::from([(
                "api_token".to_owned(),
                Value::String("secret-token".to_owned()),
            )]),
            updated_at_unix_ms: 42,
        };

        let (validation, effective) =
            validate_plugin_config_instance(&manifest, Some(&instance), Some("digest-1"));
        assert_eq!(validation.state, PluginConfigValidationState::RequiresMigration);
        assert!(effective.is_empty());
        assert!(
            validation
                .issues
                .iter()
                .any(|issue| issue.contains("config contract schema mismatch"))
        );
    }

    #[test]
    fn build_plugin_capability_diff_reports_all_main_problem_categories() {
        let manifest = parse_manifest_toml(
            r#"
manifest_version = 2
skill_id = "acme.echo_http"
name = "Echo + HTTP"
version = "1.2.3"
publisher = "acme"

[entrypoints]
[[entrypoints.tools]]
id = "acme.echo"
name = "echo"
description = "Echo payload"
input_schema = { type = "object" }
output_schema = { type = "object" }
risk = { default_sensitive = false, requires_approval = false }

[capabilities]
wildcard_opt_in = { http_egress = true }
http_egress_allowlist = ["*.example.com"]
device_capabilities = []
node_capabilities = ["discord"]

[[capabilities.secrets]]
scope = "skill:acme.echo_http"
key_names = ["api_token"]

[capabilities.filesystem]
read_roots = []
write_roots = ["skills/cache"]

[capabilities.quotas]
wall_clock_timeout_ms = 2000
fuel_budget = 500000
max_memory_bytes = 1048576

[compat]
required_protocol_major = 1
min_palyra_version = "0.1.0"

[operator]
display_name = "Echo HTTP"
summary = "Sample operator metadata"

[operator.plugin]
default_tool_id = "acme.echo"
default_module_path = "modules/module.wasm"
default_entrypoint = "run"

[operator.config]
schema_version = 1
required = ["api_base_url", "api_token"]

[operator.config.properties.api_base_url]
type = "string"
title = "API base URL"

[operator.config.properties.api_token]
type = "string"
title = "API token"
redacted = true
"#,
        )
        .expect("capability diff test manifest should parse");
        let granted = PluginCapabilityProfile {
            http_hosts: vec!["api.example.com".to_owned()],
            secrets: vec!["api_token".to_owned(), "extra_secret".to_owned()],
            storage_prefixes: Vec::new(),
            channels: vec!["discord".to_owned(), "slack".to_owned()],
        };
        let policy = WasmPluginRunnerPolicy {
            enabled: true,
            allow_inline_modules: false,
            max_module_size_bytes: 131_072,
            fuel_budget: 500_000,
            max_memory_bytes: 1_048_576,
            max_table_elements: 128,
            max_instances: 1,
            allowed_http_hosts: Vec::new(),
            allowed_secrets: vec!["api_token".to_owned()],
            allowed_storage_prefixes: Vec::new(),
            allowed_channels: vec!["discord".to_owned()],
        };

        let diff = build_plugin_capability_diff(&manifest, &granted, &policy);
        assert!(!diff.valid, "capability drift should make the diff invalid");
        let categories = diff
            .entries
            .iter()
            .map(|entry| format!("{:?}", entry.category))
            .collect::<std::collections::BTreeSet<_>>();
        assert!(categories.contains("MissingGrant"));
        assert!(categories.contains("ExcessGrant"));
        assert!(categories.contains("PolicyRestricted"));
        assert!(categories.contains("WildcardRisk"));
        assert!(categories.contains("UnusedDeclaredCapability"));
    }

    #[test]
    fn inspect_plugin_filesystem_safety_rejects_invalid_plugin_identifiers() {
        let tempdir = tempdir().expect("temporary directory should be created");
        let snapshot = inspect_plugin_filesystem_safety(tempdir.path(), "../escape");
        assert!(!snapshot.safe);
        assert!(
            snapshot
                .issues
                .iter()
                .any(|issue| issue.code == "invalid_plugin_id" && issue.severity == "error")
        );
    }

    #[cfg(unix)]
    #[test]
    fn inspect_plugin_filesystem_safety_flags_world_writable_plugin_roots() {
        use std::os::unix::fs::PermissionsExt;

        let tempdir = tempdir().expect("temporary directory should be created");
        let plugin_root = tempdir.path().join("acme.echo_plugin");
        fs::create_dir_all(&plugin_root).expect("plugin root should be created");
        let mut permissions = fs::metadata(&plugin_root)
            .expect("plugin root metadata should load")
            .permissions();
        permissions.set_mode(0o777);
        fs::set_permissions(&plugin_root, permissions)
            .expect("plugin root permissions should be updated");

        let snapshot = inspect_plugin_filesystem_safety(tempdir.path(), "acme.echo_plugin");
        assert!(!snapshot.safe);
        assert!(
            snapshot
                .issues
                .iter()
                .any(|issue| issue.code == "plugin_root_writable")
        );
    }

    fn plugin_manifest_with_operator_config(config_properties_toml: &str) -> palyra_skills::SkillManifest {
        let manifest_toml = format!(
            r#"
manifest_version = 2
skill_id = "acme.echo_http"
name = "Echo + HTTP"
version = "1.2.3"
publisher = "acme"

[entrypoints]
[[entrypoints.tools]]
id = "acme.echo"
name = "echo"
description = "Echo payload"
input_schema = {{ type = "object" }}
output_schema = {{ type = "object" }}
risk = {{ default_sensitive = false, requires_approval = false }}

[capabilities.filesystem]
read_roots = []
write_roots = []

[capabilities]
http_egress_allowlist = ["api.example.com"]
device_capabilities = []
node_capabilities = []

[[capabilities.secrets]]
scope = "skill:acme.echo_http"
key_names = ["api_token"]

[capabilities.quotas]
wall_clock_timeout_ms = 2000
fuel_budget = 500000
max_memory_bytes = 1048576

[compat]
required_protocol_major = 1
min_palyra_version = "0.1.0"

[operator]
display_name = "Echo HTTP"
summary = "Sample operator metadata"

[operator.plugin]
default_tool_id = "acme.echo"
default_module_path = "modules/module.wasm"
default_entrypoint = "run"

[operator.config]
schema_version = 1
required = ["api_base_url", "api_token"]
{config_properties_toml}
"#
        );
        parse_manifest_toml(manifest_toml.trim())
            .expect("plugin test manifest with operator config should parse")
    }
}
