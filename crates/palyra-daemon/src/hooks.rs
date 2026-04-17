use std::{fs, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use palyra_common::versioned_json::{
    migrate_updated_at_metadata_v0_to_v1, parse_versioned_json, VersionedJsonFormat,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::task::JoinHandle;

use crate::{
    gateway::GatewayRuntimeState,
    journal::{JournalEventRecord, SkillExecutionStatus},
    plugins::{
        load_plugin_bindings_index, plugin_binding, resolve_plugins_root, PluginBindingRecord,
    },
    transport::grpc::auth::RequestContext,
    wasm_plugin_runner::{resolve_installed_skill_module, run_resolved_wasm_plugin},
    *,
};

const HOOK_BINDINGS_LAYOUT_VERSION: u32 = 1;
const HOOK_BINDINGS_INDEX_FILE_NAME: &str = "bindings.json";
const HOOK_JOURNAL_POLL_INTERVAL_MS: u64 = 1_000;
const HOOK_JOURNAL_SNAPSHOT_LIMIT: usize = 128;
const HOOK_BINDINGS_INDEX_FORMAT: VersionedJsonFormat =
    VersionedJsonFormat::new("hook bindings index", HOOK_BINDINGS_LAYOUT_VERSION);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HookBindingsIndex {
    pub(crate) schema_version: u32,
    pub(crate) updated_at_unix_ms: i64,
    #[serde(default)]
    pub(crate) entries: Vec<HookBindingRecord>,
}

impl Default for HookBindingsIndex {
    fn default() -> Self {
        Self {
            schema_version: HOOK_BINDINGS_LAYOUT_VERSION,
            updated_at_unix_ms: 0,
            entries: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HookBindingRecord {
    pub(crate) hook_id: String,
    pub(crate) event: String,
    pub(crate) plugin_id: String,
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) operator: HookOperatorMetadata,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HookOperatorMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) notes: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) owner_principal: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) updated_by: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct HookBindingUpsert {
    pub(crate) hook_id: String,
    pub(crate) event: String,
    pub(crate) plugin_id: String,
    pub(crate) enabled: bool,
    pub(crate) operator: HookOperatorMetadata,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum HookEventKind {
    GatewayStartup,
    SkillEnabled,
    SkillQuarantined,
    SkillDisabled,
}

impl HookEventKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::GatewayStartup => "gateway:startup",
            Self::SkillEnabled => "skill:enabled",
            Self::SkillQuarantined => "skill:quarantined",
            Self::SkillDisabled => "skill:disabled",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct HookDispatchOutcome {
    pub(crate) hook: HookBindingRecord,
    pub(crate) plugin: PluginBindingRecord,
    pub(crate) success: bool,
    pub(crate) error: Option<String>,
    pub(crate) output_json: Value,
}

pub(crate) fn resolve_hooks_root() -> Result<PathBuf> {
    let plugins_root = resolve_plugins_root()?;
    let state_root =
        plugins_root.parent().map(FsPath::to_path_buf).unwrap_or_else(|| plugins_root.clone());
    Ok(state_root.join("hooks"))
}

pub(crate) fn hook_bindings_index_path(hooks_root: &FsPath) -> PathBuf {
    hooks_root.join(HOOK_BINDINGS_INDEX_FILE_NAME)
}

pub(crate) fn load_hook_bindings_index(hooks_root: &FsPath) -> Result<HookBindingsIndex> {
    let path = hook_bindings_index_path(hooks_root);
    if !path.exists() {
        return Ok(HookBindingsIndex::default());
    }
    let payload = fs::read(path.as_path())
        .with_context(|| format!("failed to read hook bindings index {}", path.display()))?;
    let mut index = parse_versioned_json::<HookBindingsIndex>(
        payload.as_slice(),
        HOOK_BINDINGS_INDEX_FORMAT,
        &[(0, migrate_updated_at_metadata_v0_to_v1)],
    )
    .with_context(|| format!("failed to parse hook bindings index {}", path.display()))?;
    normalize_hook_bindings_index(&mut index);
    Ok(index)
}

pub(crate) fn save_hook_bindings_index(
    hooks_root: &FsPath,
    index: &HookBindingsIndex,
) -> Result<()> {
    fs::create_dir_all(hooks_root)
        .with_context(|| format!("failed to create hooks root {}", hooks_root.display()))?;
    let mut normalized = index.clone();
    normalized.schema_version = HOOK_BINDINGS_LAYOUT_VERSION;
    normalized.updated_at_unix_ms = unix_ms_now().context("failed to read system clock")?;
    normalize_hook_bindings_index(&mut normalized);
    let payload = serde_json::to_vec_pretty(&normalized)
        .context("failed to serialize hook bindings index")?;
    let path = hook_bindings_index_path(hooks_root);
    fs::write(path.as_path(), payload)
        .with_context(|| format!("failed to write hook bindings index {}", path.display()))
}

pub(crate) fn hook_binding(index: &HookBindingsIndex, hook_id: &str) -> Result<HookBindingRecord> {
    let hook_id = normalize_hook_identifier(hook_id, "hook_id")?;
    index
        .entries
        .iter()
        .find(|entry| entry.hook_id == hook_id)
        .cloned()
        .ok_or_else(|| anyhow!("hook binding not found: {hook_id}"))
}

pub(crate) fn hooks_for_plugin(
    index: &HookBindingsIndex,
    plugin_id: &str,
) -> Vec<HookBindingRecord> {
    index.entries.iter().filter(|entry| entry.plugin_id == plugin_id).cloned().collect()
}

pub(crate) fn normalize_hook_binding_upsert(
    request: HookBindingUpsert,
    now_unix_ms: i64,
    existing: Option<&HookBindingRecord>,
) -> Result<HookBindingRecord> {
    Ok(HookBindingRecord {
        hook_id: normalize_hook_identifier(request.hook_id.as_str(), "hook_id")?,
        event: normalize_hook_event(request.event.as_str())?.to_owned(),
        plugin_id: normalize_hook_identifier(request.plugin_id.as_str(), "plugin_id")?,
        enabled: request.enabled,
        operator: normalize_hook_operator_metadata(request.operator),
        created_at_unix_ms: existing.map(|entry| entry.created_at_unix_ms).unwrap_or(now_unix_ms),
        updated_at_unix_ms: now_unix_ms,
    })
}

pub(crate) fn upsert_hook_binding(
    index: &mut HookBindingsIndex,
    record: HookBindingRecord,
) -> HookBindingRecord {
    if let Some(existing) = index.entries.iter_mut().find(|entry| entry.hook_id == record.hook_id) {
        *existing = record.clone();
        return record;
    }
    index.entries.push(record.clone());
    normalize_hook_bindings_index(index);
    record
}

pub(crate) fn set_hook_binding_enabled(
    index: &mut HookBindingsIndex,
    hook_id: &str,
    enabled: bool,
    updated_by: Option<&str>,
) -> Result<HookBindingRecord> {
    let hook_id = normalize_hook_identifier(hook_id, "hook_id")?;
    let now = unix_ms_now().context("failed to read system clock")?;
    let entry = index
        .entries
        .iter_mut()
        .find(|entry| entry.hook_id == hook_id)
        .ok_or_else(|| anyhow!("hook binding not found: {hook_id}"))?;
    entry.enabled = enabled;
    entry.updated_at_unix_ms = now;
    entry.operator.updated_by = updated_by.and_then(|value| trim_to_option(value.to_owned()));
    Ok(entry.clone())
}

pub(crate) fn delete_hook_binding(
    index: &mut HookBindingsIndex,
    hook_id: &str,
) -> Result<HookBindingRecord> {
    let hook_id = normalize_hook_identifier(hook_id, "hook_id")?;
    let position = index
        .entries
        .iter()
        .position(|entry| entry.hook_id == hook_id)
        .ok_or_else(|| anyhow!("hook binding not found: {hook_id}"))?;
    Ok(index.entries.remove(position))
}

pub(crate) fn normalize_hook_event(raw: &str) -> Result<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "gateway:startup" => Ok(HookEventKind::GatewayStartup.as_str()),
        "skill:enabled" => Ok(HookEventKind::SkillEnabled.as_str()),
        "skill:quarantined" => Ok(HookEventKind::SkillQuarantined.as_str()),
        "skill:disabled" => Ok(HookEventKind::SkillDisabled.as_str()),
        other => bail!("unsupported hook event '{other}'"),
    }
}

pub(crate) fn spawn_hook_runtime(
    runtime: Arc<GatewayRuntimeState>,
    policy: crate::wasm_plugin_runner::WasmPluginRunnerPolicy,
    execution_timeout: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_journal_seq = match runtime.recent_journal_snapshot(1).await {
            Ok(snapshot) => snapshot.events.iter().map(|event| event.seq).max().unwrap_or(0),
            Err(error) => {
                warn!(error = %error, "failed to initialize hook journal cursor");
                0
            }
        };

        if let Err(error) = dispatch_named_event(
            Arc::clone(&runtime),
            &policy,
            execution_timeout,
            HookEventKind::GatewayStartup.as_str(),
            json!({ "source": "gateway.startup" }),
        )
        .await
        {
            warn!(error = %error, "startup hook dispatch failed");
        }

        loop {
            tokio::time::sleep(Duration::from_millis(HOOK_JOURNAL_POLL_INTERVAL_MS)).await;
            match runtime.recent_journal_snapshot(HOOK_JOURNAL_SNAPSHOT_LIMIT).await {
                Ok(snapshot) => {
                    let mut events = snapshot
                        .events
                        .into_iter()
                        .filter(|event| event.seq > last_journal_seq)
                        .collect::<Vec<_>>();
                    events.sort_by_key(|event| event.seq);
                    for event in events {
                        last_journal_seq = last_journal_seq.max(event.seq);
                        if let Some((hook_event, payload)) = hook_event_from_journal(event) {
                            if let Err(error) = dispatch_named_event(
                                Arc::clone(&runtime),
                                &policy,
                                execution_timeout,
                                hook_event,
                                payload,
                            )
                            .await
                            {
                                warn!(
                                    error = %error,
                                    hook_event,
                                    "journal-driven hook dispatch failed"
                                );
                            }
                        }
                    }
                }
                Err(error) => {
                    warn!(error = %error, "failed to poll journal for hook events");
                }
            }
        }
    })
}

pub(crate) async fn dispatch_named_event(
    runtime: Arc<GatewayRuntimeState>,
    policy: &crate::wasm_plugin_runner::WasmPluginRunnerPolicy,
    execution_timeout: Duration,
    event: &str,
    event_payload: Value,
) -> Result<Vec<HookDispatchOutcome>> {
    let hooks_root = resolve_hooks_root()?;
    let hooks_index = load_hook_bindings_index(hooks_root.as_path())?;
    let plugins_root = resolve_plugins_root()?;
    let plugins_index = load_plugin_bindings_index(plugins_root.as_path())?;
    let mut outcomes = Vec::new();

    for hook in
        hooks_index.entries.into_iter().filter(|entry| entry.enabled && entry.event == event)
    {
        let plugin = match plugin_binding(&plugins_index, hook.plugin_id.as_str()) {
            Ok(plugin) if plugin.enabled => plugin,
            Ok(plugin) => {
                let message = "plugin binding is disabled".to_owned();
                record_hook_event(
                    Arc::clone(&runtime),
                    "hook.failed",
                    &hook,
                    Some(&plugin),
                    json!({ "event": event, "reason": message, "event_payload": event_payload }),
                )
                .await;
                outcomes.push(HookDispatchOutcome {
                    hook,
                    plugin,
                    success: false,
                    error: Some(message),
                    output_json: json!({}),
                });
                continue;
            }
            Err(error) => {
                record_hook_event(
                    Arc::clone(&runtime),
                    "hook.failed",
                    &hook,
                    None,
                    json!({
                        "event": event,
                        "reason": sanitize_http_error_message(error.to_string().as_str()),
                        "event_payload": event_payload,
                    }),
                )
                .await;
                continue;
            }
        };

        let resolved = match resolve_installed_skill_module(
            plugin.skill_id.as_str(),
            plugin.skill_version.as_deref(),
            plugin.module_path.as_deref(),
            plugin.entrypoint.as_deref(),
            plugin.tool_id.as_deref(),
        ) {
            Ok(resolved) => resolved,
            Err(error) => {
                let message = sanitize_http_error_message(error.message.as_str());
                record_hook_event(
                    Arc::clone(&runtime),
                    "hook.failed",
                    &hook,
                    Some(&plugin),
                    json!({
                        "event": event,
                        "reason": message,
                        "event_payload": event_payload,
                    }),
                )
                .await;
                outcomes.push(HookDispatchOutcome {
                    hook,
                    plugin,
                    success: false,
                    error: Some(message),
                    output_json: json!({}),
                });
                continue;
            }
        };

        let skill_status = runtime
            .skill_status(resolved.skill_id.clone(), resolved.skill_version.clone())
            .await
            .map_err(|error| anyhow!("failed to load skill status for hook dispatch: {error}"))?;
        if skill_status.as_ref().is_some_and(|record| {
            matches!(
                record.status,
                SkillExecutionStatus::Quarantined | SkillExecutionStatus::Disabled
            )
        }) {
            let message = skill_status
                .as_ref()
                .map(|record| {
                    if matches!(record.status, SkillExecutionStatus::Quarantined) {
                        "skill is quarantined"
                    } else {
                        "skill is disabled"
                    }
                })
                .unwrap_or("skill is unavailable")
                .to_owned();
            record_hook_event(
                Arc::clone(&runtime),
                "hook.failed",
                &hook,
                Some(&plugin),
                json!({
                    "event": event,
                    "reason": message,
                    "skill_id": resolved.skill_id,
                    "skill_version": resolved.skill_version,
                    "event_payload": event_payload,
                }),
            )
            .await;
            outcomes.push(HookDispatchOutcome {
                hook,
                plugin,
                success: false,
                error: Some(message),
                output_json: json!({}),
            });
            continue;
        }

        match run_resolved_wasm_plugin(
            policy,
            &resolved,
            plugin.capability_profile.to_requested_capabilities(),
            execution_timeout,
        ) {
            Ok(success) => {
                let output_json = serde_json::from_slice::<Value>(success.output_json.as_slice())
                    .unwrap_or_else(|_| json!({}));
                record_hook_event(
                    Arc::clone(&runtime),
                    "hook.dispatched",
                    &hook,
                    Some(&plugin),
                    json!({
                        "event": event,
                        "skill_id": resolved.skill_id,
                        "skill_version": resolved.skill_version,
                        "module_path": resolved.module_path,
                        "entrypoint": resolved.entrypoint,
                        "output": output_json,
                        "event_payload": event_payload,
                    }),
                )
                .await;
                outcomes.push(HookDispatchOutcome {
                    hook,
                    plugin,
                    success: true,
                    error: None,
                    output_json,
                });
            }
            Err(error) => {
                let message = sanitize_http_error_message(error.message.as_str());
                record_hook_event(
                    Arc::clone(&runtime),
                    "hook.failed",
                    &hook,
                    Some(&plugin),
                    json!({
                        "event": event,
                        "skill_id": resolved.skill_id,
                        "skill_version": resolved.skill_version,
                        "reason": message,
                        "event_payload": event_payload,
                    }),
                )
                .await;
                outcomes.push(HookDispatchOutcome {
                    hook,
                    plugin,
                    success: false,
                    error: Some(message),
                    output_json: json!({}),
                });
            }
        }
    }

    Ok(outcomes)
}

fn hook_event_from_journal(event: JournalEventRecord) -> Option<(&'static str, Value)> {
    let payload = serde_json::from_str::<Value>(event.payload_json.as_str()).ok()?;
    let event_name = payload.get("event").and_then(Value::as_str)?;
    let mapped = match event_name {
        "skill.enabled" => HookEventKind::SkillEnabled.as_str(),
        "skill.quarantined" => HookEventKind::SkillQuarantined.as_str(),
        "skill.disabled" => HookEventKind::SkillDisabled.as_str(),
        _ => return None,
    };
    Some((mapped, payload))
}

async fn record_hook_event(
    runtime: Arc<GatewayRuntimeState>,
    event: &str,
    hook: &HookBindingRecord,
    plugin: Option<&PluginBindingRecord>,
    details: Value,
) {
    let context = RequestContext {
        principal: SYSTEM_DAEMON_PRINCIPAL.to_owned(),
        device_id: SYSTEM_DAEMON_DEVICE_ID.to_owned(),
        channel: Some(SYSTEM_VAULT_CHANNEL.to_owned()),
    };
    if let Err(error) = runtime
        .record_console_event(
            &context,
            event,
            json!({
                "hook_id": hook.hook_id,
                "plugin_id": plugin.map(|record| record.plugin_id.clone()).unwrap_or_else(|| hook.plugin_id.clone()),
                "details": details,
            }),
        )
        .await
    {
        warn!(error = %error, hook_id = %hook.hook_id, "failed to record hook event");
    }
}

fn normalize_hook_bindings_index(index: &mut HookBindingsIndex) {
    index.entries.sort_by(|left, right| left.hook_id.cmp(&right.hook_id));
}

fn normalize_hook_identifier(raw: &str, field_name: &'static str) -> Result<String> {
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

fn normalize_hook_operator_metadata(mut operator: HookOperatorMetadata) -> HookOperatorMetadata {
    operator.display_name = operator.display_name.and_then(trim_to_option);
    operator.notes = operator.notes.and_then(trim_to_option);
    operator.owner_principal = operator.owner_principal.and_then(trim_to_option);
    operator.updated_by = operator.updated_by.and_then(trim_to_option);
    operator
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{hook_bindings_index_path, load_hook_bindings_index, HOOK_BINDINGS_LAYOUT_VERSION};

    #[test]
    fn load_hook_bindings_index_migrates_legacy_metadata() {
        let tempdir = tempdir().expect("temporary directory should be created");
        let index_path = hook_bindings_index_path(tempdir.path());
        fs::write(index_path, br#"{"entries":[]}"#)
            .expect("legacy hook bindings index should be written");
        let index = load_hook_bindings_index(tempdir.path())
            .expect("legacy hook bindings index should load");
        assert_eq!(index.schema_version, HOOK_BINDINGS_LAYOUT_VERSION);
        assert_eq!(index.updated_at_unix_ms, 0);
        assert!(index.entries.is_empty());
    }
}
