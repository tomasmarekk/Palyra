use std::{collections::BTreeSet, fs, path::PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::*;

const PLUGIN_BINDINGS_LAYOUT_VERSION: u32 = 1;
const PLUGIN_BINDINGS_INDEX_FILE_NAME: &str = "bindings.json";

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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

pub(crate) fn load_plugin_bindings_index(plugins_root: &FsPath) -> Result<PluginBindingsIndex> {
    let path = plugin_bindings_index_path(plugins_root);
    if !path.exists() {
        return Ok(PluginBindingsIndex::default());
    }
    let payload = fs::read(path.as_path())
        .with_context(|| format!("failed to read plugin bindings index {}", path.display()))?;
    let mut index = serde_json::from_slice::<PluginBindingsIndex>(payload.as_slice())
        .with_context(|| format!("failed to parse plugin bindings index {}", path.display()))?;
    if index.schema_version != PLUGIN_BINDINGS_LAYOUT_VERSION {
        bail!("unsupported plugin bindings schema version {}", index.schema_version);
    }
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
    index.entries.sort_by(|left, right| left.plugin_id.cmp(&right.plugin_id));
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

impl PluginCapabilityProfile {
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
