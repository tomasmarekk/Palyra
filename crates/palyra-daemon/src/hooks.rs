//! Hook binding registry and wasm-plugin hook dispatch.
//!
//! Persists hook-to-plugin bindings under `<state_root>/hooks/`, polls the journal for
//! hook-worthy events, executes the bound wasm plugins, and resolves run-lifecycle decisions
//! (continue, annotate, request approval, block, transform preview, fail). A terminal lifecycle
//! resolution is surfaced as a dispatch error so the calling run path stops fail-closed.

use std::{fs, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use palyra_common::{
    runtime_contracts::{
        resolve_run_lifecycle_hook_decisions, AgentHookKind, RunLifecycleHookDecision,
        RunLifecycleHookDecisionKind, RunLifecycleHookPhase, RunLifecycleHookResolution,
    },
    versioned_json::{
        migrate_updated_at_metadata_v0_to_v1, parse_versioned_json, VersionedJsonFormat,
    },
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
const LIFECYCLE_HOOK_EXIT_CONTINUE: i64 = 0;
const LIFECYCLE_HOOK_EXIT_ANNOTATE: i64 = 10;
const LIFECYCLE_HOOK_EXIT_REQUEST_APPROVAL: i64 = 20;
const LIFECYCLE_HOOK_EXIT_BLOCK: i64 = 30;
const LIFECYCLE_HOOK_EXIT_TRANSFORM_PREVIEW: i64 = 40;
const LIFECYCLE_HOOK_EXIT_FAIL_RUN: i64 = 50;

/// On-disk index of all hook bindings; entries are kept sorted by `hook_id`.
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

/// One persisted binding from a hook event to a plugin, plus operator bookkeeping.
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

/// Free-form operator annotations attached to a hook binding; all fields are optional.
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

/// Caller-provided fields for creating or replacing a hook binding; normalized before storage.
#[derive(Debug, Clone)]
pub(crate) struct HookBindingUpsert {
    pub(crate) hook_id: String,
    pub(crate) event: String,
    pub(crate) plugin_id: String,
    pub(crate) enabled: bool,
    pub(crate) operator: HookOperatorMetadata,
}

/// Canonical hook event names: gateway/skill events plus the run-lifecycle phases re-exported
/// from [`RunLifecycleHookPhase`].
#[derive(Debug, Clone, Copy)]
pub(crate) enum HookEventKind {
    GatewayStartup,
    SkillEnabled,
    SkillQuarantined,
    SkillDisabled,
    RunStarted,
    RunFinished,
    BeforeContextBuild,
    AfterContextBuild,
    BeforeToolPolicy,
    AfterToolResult,
    MemoryCandidateCreated,
    LearningCandidateReviewed,
    ArtifactCreated,
    ApprovalRequested,
    RunBeforeRun,
    RunBeforeTool,
    RunAfterTool,
    RunBeforeDelivery,
    RunAfterRun,
}

impl HookEventKind {
    /// Returns the canonical `scope:event` wire name.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::GatewayStartup => "gateway:startup",
            Self::SkillEnabled => "skill:enabled",
            Self::SkillQuarantined => "skill:quarantined",
            Self::SkillDisabled => "skill:disabled",
            Self::RunStarted => "run_started",
            Self::RunFinished => "run_finished",
            Self::BeforeContextBuild => "before_context_build",
            Self::AfterContextBuild => "after_context_build",
            Self::BeforeToolPolicy => "before_tool_policy",
            Self::AfterToolResult => "after_tool_result",
            Self::MemoryCandidateCreated => "memory_candidate_created",
            Self::LearningCandidateReviewed => "learning_candidate_reviewed",
            Self::ArtifactCreated => "artifact_created",
            Self::ApprovalRequested => "approval_requested",
            Self::RunBeforeRun => RunLifecycleHookPhase::BeforeRun.event_name(),
            Self::RunBeforeTool => RunLifecycleHookPhase::BeforeTool.event_name(),
            Self::RunAfterTool => RunLifecycleHookPhase::AfterTool.event_name(),
            Self::RunBeforeDelivery => RunLifecycleHookPhase::BeforeDelivery.event_name(),
            Self::RunAfterRun => RunLifecycleHookPhase::AfterRun.event_name(),
        }
    }
}

/// Result of executing one hook binding during [`dispatch_named_event`].
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct HookDispatchOutcome {
    pub(crate) hook: HookBindingRecord,
    pub(crate) plugin: PluginBindingRecord,
    pub(crate) success: bool,
    pub(crate) error: Option<String>,
    pub(crate) output_json: Value,
}

/// Hook dispatch report used by inline run-stream call sites.
#[derive(Debug, Clone)]
pub(crate) struct HookDispatchReport {
    pub(crate) outcomes: Vec<HookDispatchOutcome>,
    pub(crate) lifecycle_resolution: Option<RunLifecycleHookResolution>,
}

/// Redacted event envelope passed to constrained plugin hooks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct HookEventEnvelope {
    pub(crate) schema_version: u32,
    pub(crate) event: String,
    pub(crate) redacted: bool,
    pub(crate) payload: Value,
    pub(crate) forbidden_authorities: Vec<String>,
}

/// Resolves the hooks storage root as a sibling of the plugins root.
///
/// # Errors
///
/// Fails when the plugins root itself cannot be resolved.
pub(crate) fn resolve_hooks_root() -> Result<PathBuf> {
    let plugins_root = resolve_plugins_root()?;
    let state_root =
        plugins_root.parent().map(FsPath::to_path_buf).unwrap_or_else(|| plugins_root.clone());
    Ok(state_root.join("hooks"))
}

/// Returns the path of the bindings index file inside `hooks_root`.
pub(crate) fn hook_bindings_index_path(hooks_root: &FsPath) -> PathBuf {
    hooks_root.join(HOOK_BINDINGS_INDEX_FILE_NAME)
}

/// Loads the bindings index, migrating legacy layouts; a missing file yields an empty index.
///
/// # Errors
///
/// Fails when the index file exists but cannot be read or parsed.
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

/// Persists the bindings index, re-stamping schema version and update time and re-sorting
/// entries so the on-disk order stays deterministic.
///
/// # Errors
///
/// Fails when the hooks root cannot be created or the index cannot be serialized or written.
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

/// Looks up one binding by normalized hook id.
///
/// # Errors
///
/// Fails when `hook_id` is malformed or no binding with that id exists.
pub(crate) fn hook_binding(index: &HookBindingsIndex, hook_id: &str) -> Result<HookBindingRecord> {
    let hook_id = normalize_hook_identifier(hook_id, "hook_id")?;
    index
        .entries
        .iter()
        .find(|entry| entry.hook_id == hook_id)
        .cloned()
        .ok_or_else(|| anyhow!("hook binding not found: {hook_id}"))
}

/// Returns all bindings (enabled or not) that target `plugin_id`.
pub(crate) fn hooks_for_plugin(
    index: &HookBindingsIndex,
    plugin_id: &str,
) -> Vec<HookBindingRecord> {
    index.entries.iter().filter(|entry| entry.plugin_id == plugin_id).cloned().collect()
}

/// Validates and normalizes an upsert into a storable record, preserving the original
/// `created_at_unix_ms` when replacing an existing binding.
///
/// # Errors
///
/// Fails when the hook id, event name, or plugin id does not normalize.
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

/// Inserts or replaces a binding in the in-memory index, keeping entries sorted on insert.
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

/// Toggles a binding and stamps who changed it; returns the updated record.
///
/// # Errors
///
/// Fails when `hook_id` is malformed, the binding does not exist, or the clock is unavailable.
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

/// Removes a binding from the in-memory index and returns it.
///
/// # Errors
///
/// Fails when `hook_id` is malformed or no binding with that id exists.
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

/// Normalizes an event name (including run-lifecycle aliases such as `before_run`) to its
/// canonical `scope:event` form.
///
/// # Errors
///
/// Fails for event names outside the supported gateway, skill, and run-lifecycle set.
pub(crate) fn normalize_hook_event(raw: &str) -> Result<&'static str> {
    let normalized = raw.trim().to_ascii_lowercase();
    if let Some(kind) = AgentHookKind::parse(normalized.as_str()) {
        return Ok(kind.as_str());
    }
    if let Some(phase) = RunLifecycleHookPhase::parse_hook_event(normalized.as_str()) {
        return Ok(hook_event_kind_for_lifecycle_phase(phase).as_str());
    }
    match normalized.as_str() {
        "gateway:startup" => Ok(HookEventKind::GatewayStartup.as_str()),
        "skill:enabled" => Ok(HookEventKind::SkillEnabled.as_str()),
        "skill:quarantined" => Ok(HookEventKind::SkillQuarantined.as_str()),
        "skill:disabled" => Ok(HookEventKind::SkillDisabled.as_str()),
        "run_started" | "run:started" => Ok(HookEventKind::RunStarted.as_str()),
        "run_finished" | "run:finished" => Ok(HookEventKind::RunFinished.as_str()),
        "before_context_build" | "context:before_build" => {
            Ok(HookEventKind::BeforeContextBuild.as_str())
        }
        "after_context_build" | "context:after_build" => {
            Ok(HookEventKind::AfterContextBuild.as_str())
        }
        "before_tool_policy" | "tool:before_policy" => Ok(HookEventKind::BeforeToolPolicy.as_str()),
        "after_tool_result" | "tool:after_result" => Ok(HookEventKind::AfterToolResult.as_str()),
        "memory_candidate_created" | "memory:candidate_created" => {
            Ok(HookEventKind::MemoryCandidateCreated.as_str())
        }
        "learning_candidate_reviewed" | "learning:candidate_reviewed" => {
            Ok(HookEventKind::LearningCandidateReviewed.as_str())
        }
        "artifact_created" | "artifact:created" => Ok(HookEventKind::ArtifactCreated.as_str()),
        "approval_requested" | "approval:requested" => {
            Ok(HookEventKind::ApprovalRequested.as_str())
        }
        other => bail!("unsupported hook event '{other}'"),
    }
}

/// Builds the hook event envelope with secret-like fields replaced by a marker.
///
/// Adapters can preview exactly what a plugin would receive without executing
/// untrusted code, and dispatch records the same redacted envelope for audit.
pub(crate) fn build_redacted_hook_event_envelope(
    event: &str,
    payload: Value,
) -> Result<HookEventEnvelope> {
    let event = normalize_hook_event(event)?.to_owned();
    Ok(HookEventEnvelope {
        schema_version: 1,
        event,
        redacted: true,
        payload: redact_hook_payload(payload),
        forbidden_authorities: vec![
            "approve_tool".to_owned(),
            "read_raw_secret".to_owned(),
            "mutate_historical_journal".to_owned(),
            "bypass_policy".to_owned(),
        ],
    })
}

/// Validates that a plugin hook output stays advisory and non-authoritative.
pub(crate) fn validate_hook_output_authority(output: &Value) -> Result<()> {
    let mut denied_paths = Vec::new();
    collect_forbidden_hook_output_paths(output, "$", &mut denied_paths);
    if denied_paths.is_empty() {
        return Ok(());
    }
    bail!("hook output requested forbidden authority at {}", denied_paths.join(", "));
}

fn hook_event_kind_for_lifecycle_phase(phase: RunLifecycleHookPhase) -> HookEventKind {
    match phase {
        RunLifecycleHookPhase::BeforeRun => HookEventKind::RunBeforeRun,
        RunLifecycleHookPhase::BeforeTool => HookEventKind::RunBeforeTool,
        RunLifecycleHookPhase::AfterTool => HookEventKind::RunAfterTool,
        RunLifecycleHookPhase::BeforeDelivery => HookEventKind::RunBeforeDelivery,
        RunLifecycleHookPhase::AfterRun => HookEventKind::RunAfterRun,
    }
}

fn redact_hook_payload(value: Value) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .into_iter()
                .map(|(key, value)| {
                    if is_secret_like_hook_key(key.as_str()) {
                        (key, Value::String("<redacted>".to_owned()))
                    } else {
                        (key, redact_hook_payload(value))
                    }
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(redact_hook_payload).collect()),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => value,
    }
}

fn collect_forbidden_hook_output_paths(value: &Value, path: &str, denied_paths: &mut Vec<String>) {
    match value {
        Value::Object(fields) => {
            for (key, child) in fields {
                let child_path = format!("{path}.{key}");
                let normalized = key.to_ascii_lowercase();
                if matches!(
                    normalized.as_str(),
                    "approve"
                        | "approved"
                        | "approve_tool"
                        | "tool_approved"
                        | "raw_secret"
                        | "read_raw_secret"
                        | "mutate_journal"
                        | "mutate_historical_journal"
                        | "bypass_policy"
                ) {
                    denied_paths.push(child_path);
                    continue;
                }
                collect_forbidden_hook_output_paths(child, child_path.as_str(), denied_paths);
            }
        }
        Value::Array(values) => {
            for (index, child) in values.iter().enumerate() {
                collect_forbidden_hook_output_paths(
                    child,
                    format!("{path}[{index}]").as_str(),
                    denied_paths,
                );
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn is_secret_like_hook_key(key: &str) -> bool {
    let normalized = key
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch.to_ascii_lowercase() } else { '_' })
        .collect::<String>();
    [
        "access_token",
        "api_key",
        "authorization",
        "client_secret",
        "cookie",
        "password",
        "private_key",
        "raw_secret",
        "refresh_token",
        "secret",
        "token",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

/// Spawns the long-lived hook runtime: fires the startup hook once, then polls the journal and
/// dispatches hook events for new entries. The task runs until the daemon shuts down.
pub(crate) fn spawn_hook_runtime(
    runtime: Arc<GatewayRuntimeState>,
    policy: crate::wasm_plugin_runner::WasmPluginRunnerPolicy,
    execution_timeout: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut lifecycle = runtime.daemon_lifecycle.subscribe();
        // Start the cursor at the newest journal seq so daemon restarts do not replay historical
        // skill events into hooks; only events recorded after startup are dispatched.
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
            tokio::select! {
                () = tokio::time::sleep(Duration::from_millis(HOOK_JOURNAL_POLL_INTERVAL_MS)) => {}
                changed = lifecycle.changed() => {
                    if changed.is_err() || lifecycle.borrow().phase.stops_subsystems() {
                        break;
                    }
                    continue;
                }
            }
            if lifecycle.borrow().phase.stops_subsystems() {
                break;
            }
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

/// Executes every enabled hook bound to `event` and journals each outcome.
///
/// Per-hook plugin failures are captured in the returned [`HookDispatchOutcome`]s rather than
/// aborting the dispatch. For run-lifecycle events the individual plugin decisions are resolved
/// into a single [`RunLifecycleHookResolution`] afterwards.
///
/// # Errors
///
/// Fails when the hook or plugin indexes cannot be loaded, a skill status lookup fails, a
/// lifecycle decision is invalid for its phase set, or the resolved lifecycle decision is
/// terminal (request approval, block, or fail run) -- the error text then carries the decision.
pub(crate) async fn dispatch_named_event(
    runtime: Arc<GatewayRuntimeState>,
    policy: &crate::wasm_plugin_runner::WasmPluginRunnerPolicy,
    execution_timeout: Duration,
    event: &str,
    event_payload: Value,
) -> Result<Vec<HookDispatchOutcome>> {
    let report =
        dispatch_named_event_with_report(runtime, policy, execution_timeout, event, event_payload)
            .await?;
    if let Some(resolution) = report.lifecycle_resolution.as_ref() {
        enforce_run_lifecycle_resolution(event, resolution)?;
    }
    Ok(report.outcomes)
}

/// Executes an event and returns lifecycle resolution without enforcing it.
///
/// Inline run-stream call sites use this to distinguish non-terminal
/// annotations from host-interpreted decisions such as `request_approval` and
/// `block` while preserving the existing event dispatch/audit behavior.
pub(crate) async fn dispatch_named_event_with_report(
    runtime: Arc<GatewayRuntimeState>,
    policy: &crate::wasm_plugin_runner::WasmPluginRunnerPolicy,
    execution_timeout: Duration,
    event: &str,
    event_payload: Value,
) -> Result<HookDispatchReport> {
    let lifecycle_phase = RunLifecycleHookPhase::parse_hook_event(event);
    let event_envelope =
        serde_json::to_value(build_redacted_hook_event_envelope(event, event_payload.clone())?)
            .unwrap_or_else(|_| json!({}));
    let hooks_root = resolve_hooks_root()?;
    let hooks_index = load_hook_bindings_index(hooks_root.as_path())?;
    let plugins_root = resolve_plugins_root()?;
    let plugins_index = load_plugin_bindings_index(plugins_root.as_path())?;
    let mut outcomes = Vec::new();
    let mut lifecycle_decisions = Vec::new();

    // The index is persisted sorted by hook_id, so hooks for the same event always run in a
    // deterministic order; lifecycle decision resolution depends on that stability.
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
                    json!({
                        "event": event,
                        "reason": message,
                        "event_payload": event_payload,
                        "event_envelope": event_envelope.clone(),
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
                        "event_envelope": event_envelope.clone(),
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
                        "event_envelope": event_envelope.clone(),
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
                    "event_envelope": event_envelope.clone(),
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

        // Run-lifecycle hooks execute with no requested capabilities: a plugin sitting on the
        // run path only returns a decision and must not gain its full capability profile there.
        let requested_capabilities = if lifecycle_phase.is_some() {
            crate::wasm_plugin_runner::WasmPluginRequestedCapabilities::default()
        } else {
            plugin.capability_profile.to_requested_capabilities()
        };
        match run_resolved_wasm_plugin(policy, &resolved, requested_capabilities, execution_timeout)
        {
            Ok(success) => {
                let mut output_json =
                    serde_json::from_slice::<Value>(success.output_json.as_slice())
                        .unwrap_or_else(|_| json!({}));
                if let Err(error) = validate_hook_output_authority(&output_json) {
                    let message = sanitize_http_error_message(error.to_string().as_str());
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
                            "event_envelope": event_envelope.clone(),
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
                if let Some(phase) = lifecycle_phase {
                    let decision =
                        lifecycle_decision_from_wasm_output(phase, &hook, &plugin, &output_json);
                    if !decision.kind.is_allowed_in_phase(phase) {
                        let message = format!(
                            "{} is not allowed during {}",
                            decision.kind.as_str(),
                            phase.as_str()
                        );
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
                                "decision": decision,
                                "event_payload": event_payload,
                                "event_envelope": event_envelope.clone(),
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
                    output_json = attach_lifecycle_decision(output_json, &decision);
                    lifecycle_decisions.push(decision);
                }
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
                        "event_envelope": event_envelope.clone(),
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
                        "event_envelope": event_envelope.clone(),
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

    let mut lifecycle_resolution = None;
    if let Some(phase) = lifecycle_phase {
        let resolution = resolve_run_lifecycle_hook_decisions(phase, lifecycle_decisions)
            .map_err(|error| anyhow!("invalid lifecycle hook decision: {}", error.message))?;
        if let Some(selected_outcome) =
            outcomes.iter().find(|outcome| outcome.hook.hook_id == resolution.selected.hook_id)
        {
            record_hook_event(
                Arc::clone(&runtime),
                "hook.decision",
                &selected_outcome.hook,
                Some(&selected_outcome.plugin),
                json!({
                    "event": event,
                    "resolution": resolution,
                    "event_payload": event_payload,
                    "event_envelope": event_envelope,
                }),
            )
            .await;
        }
        lifecycle_resolution = Some(resolution);
    }

    Ok(HookDispatchReport { outcomes, lifecycle_resolution })
}

fn enforce_run_lifecycle_resolution(
    event: &str,
    resolution: &RunLifecycleHookResolution,
) -> Result<()> {
    if !resolution.terminal {
        return Ok(());
    }
    let selected = &resolution.selected;
    let action = match selected.kind {
        RunLifecycleHookDecisionKind::RequestApproval => "requested approval",
        RunLifecycleHookDecisionKind::Block => "blocked",
        RunLifecycleHookDecisionKind::FailRun => "failed run",
        _ => "stopped",
    };
    let reason = selected
        .reason
        .as_deref()
        .map(sanitize_http_error_message)
        .unwrap_or_else(|| "no reason provided".to_owned());
    bail!(
        "terminal lifecycle hook decision for {}: {} by hook {} plugin {} ({})",
        event,
        action,
        selected.hook_id,
        selected.plugin_id,
        reason
    )
}

// Only skill lifecycle journal events fan out to hooks here; run-lifecycle hook events are
// dispatched inline by the run path so their decisions can gate it synchronously.
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

fn lifecycle_decision_from_wasm_output(
    phase: RunLifecycleHookPhase,
    hook: &HookBindingRecord,
    plugin: &PluginBindingRecord,
    output_json: &Value,
) -> RunLifecycleHookDecision {
    let exit_code = output_json.get("exit_code").and_then(Value::as_i64).unwrap_or_default();
    let kind = lifecycle_decision_kind_from_exit_code(exit_code);
    let mut decision =
        RunLifecycleHookDecision::new(phase, kind, hook.hook_id.clone(), plugin.plugin_id.clone());
    decision.reason = Some(format!("plugin exit code {exit_code}"));
    decision.annotations = json!({
        "exit_code": exit_code,
        "entrypoint": output_json.get("entrypoint").cloned(),
        "duration_ms": output_json.get("duration_ms").cloned(),
    });
    if kind == RunLifecycleHookDecisionKind::TransformPreview {
        decision.transformed_preview = output_json
            .get("transform_preview")
            .cloned()
            .or_else(|| output_json.get("preview").cloned());
    }
    decision
}

// Exit-code mapping fails closed: anything negative or at/above the fail-run threshold becomes
// FailRun, while unknown codes inside the reserved 0..50 range degrade to a harmless Annotate.
fn lifecycle_decision_kind_from_exit_code(exit_code: i64) -> RunLifecycleHookDecisionKind {
    match exit_code {
        LIFECYCLE_HOOK_EXIT_CONTINUE => RunLifecycleHookDecisionKind::Continue,
        LIFECYCLE_HOOK_EXIT_ANNOTATE => RunLifecycleHookDecisionKind::Annotate,
        LIFECYCLE_HOOK_EXIT_REQUEST_APPROVAL => RunLifecycleHookDecisionKind::RequestApproval,
        LIFECYCLE_HOOK_EXIT_BLOCK => RunLifecycleHookDecisionKind::Block,
        LIFECYCLE_HOOK_EXIT_TRANSFORM_PREVIEW => RunLifecycleHookDecisionKind::TransformPreview,
        code if !(0..LIFECYCLE_HOOK_EXIT_FAIL_RUN).contains(&code) => {
            RunLifecycleHookDecisionKind::FailRun
        }
        _ => RunLifecycleHookDecisionKind::Annotate,
    }
}

fn attach_lifecycle_decision(mut output_json: Value, decision: &RunLifecycleHookDecision) -> Value {
    let decision_json = serde_json::to_value(decision).unwrap_or_else(|_| json!({}));
    if let Value::Object(ref mut object) = output_json {
        object.insert("lifecycle_decision".to_owned(), decision_json);
        return output_json;
    }
    json!({
        "output": output_json,
        "lifecycle_decision": decision_json,
    })
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

    use palyra_common::runtime_contracts::{
        resolve_run_lifecycle_hook_decisions, RunLifecycleHookDecision,
        RunLifecycleHookDecisionKind, RunLifecycleHookPhase,
    };
    use serde_json::json;
    use tempfile::tempdir;

    use super::{
        attach_lifecycle_decision, build_redacted_hook_event_envelope,
        enforce_run_lifecycle_resolution, hook_bindings_index_path,
        lifecycle_decision_kind_from_exit_code, load_hook_bindings_index, normalize_hook_event,
        validate_hook_output_authority, HOOK_BINDINGS_LAYOUT_VERSION,
    };

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

    #[test]
    fn load_hook_bindings_index_pins_deterministic_hook_order() {
        let tempdir = tempdir().expect("temporary directory should be created");
        let index_path = hook_bindings_index_path(tempdir.path());
        fs::write(
            index_path,
            serde_json::to_vec(&json!({
                "schema_version": HOOK_BINDINGS_LAYOUT_VERSION,
                "updated_at_unix_ms": 1,
                "entries": [
                    {
                        "hook_id": "z-last",
                        "event": "run.before",
                        "plugin_id": "plugin-z",
                        "enabled": true,
                        "operator": {},
                        "created_at_unix_ms": 1,
                        "updated_at_unix_ms": 1
                    },
                    {
                        "hook_id": "a-first",
                        "event": "run.before",
                        "plugin_id": "plugin-a",
                        "enabled": true,
                        "operator": {},
                        "created_at_unix_ms": 1,
                        "updated_at_unix_ms": 1
                    }
                ]
            }))
            .expect("hook fixture should serialize"),
        )
        .expect("hook bindings index should be written");

        let index =
            load_hook_bindings_index(tempdir.path()).expect("hook bindings index should load");
        let hook_ids = index.entries.iter().map(|entry| entry.hook_id.as_str()).collect::<Vec<_>>();
        assert_eq!(hook_ids, vec!["a-first", "z-last"]);
    }

    #[test]
    fn normalize_hook_event_accepts_run_lifecycle_aliases() {
        assert_eq!(
            normalize_hook_event("before_run").expect("before_run alias should normalize"),
            RunLifecycleHookPhase::BeforeRun.event_name()
        );
        assert_eq!(
            normalize_hook_event("run:before_delivery")
                .expect("canonical lifecycle event should normalize"),
            "run:before_delivery"
        );
    }

    #[test]
    fn normalize_hook_event_accepts_constrained_hook_api_events() {
        assert_eq!(
            normalize_hook_event("run_started").expect("run_started should normalize"),
            "run_started"
        );
        assert_eq!(
            normalize_hook_event("tool:before_policy")
                .expect("tool before policy alias should normalize"),
            "before_tool_policy"
        );
        assert_eq!(
            normalize_hook_event("approval:requested").expect("approval alias should normalize"),
            "approval_requested"
        );
        assert_eq!(
            normalize_hook_event("before_prompt_build").expect("agent hook should normalize"),
            "before_prompt_build"
        );
        assert_eq!(
            normalize_hook_event("tool_result_model_feed")
                .expect("tool result middleware hook should normalize"),
            "tool_result_model_feed"
        );
    }

    #[test]
    fn hook_event_envelope_redacts_secret_payload_fields() {
        let envelope = build_redacted_hook_event_envelope(
            "after_tool_result",
            json!({
                "tool": "mcp.docs.search",
                "api_token": "plain-secret",
                "nested": { "client_secret": "secret" },
            }),
        )
        .expect("envelope should build");

        assert_eq!(envelope.event, "after_tool_result");
        assert_eq!(
            envelope.payload.pointer("/api_token").and_then(serde_json::Value::as_str),
            Some("<redacted>")
        );
        assert_eq!(
            envelope.payload.pointer("/nested/client_secret").and_then(serde_json::Value::as_str),
            Some("<redacted>")
        );
        assert!(envelope.forbidden_authorities.iter().any(|entry| entry == "approve_tool"));
    }

    #[test]
    fn hook_output_authority_rejects_tool_approval_and_journal_mutation() {
        let error = validate_hook_output_authority(&json!({
            "annotations": {},
            "approve_tool": true,
            "nested": { "mutate_historical_journal": true },
        }))
        .expect_err("forbidden authorities should be rejected")
        .to_string();

        assert!(error.contains("$.approve_tool"), "{error}");
        assert!(error.contains("$.nested.mutate_historical_journal"), "{error}");
    }

    #[test]
    fn lifecycle_exit_codes_map_to_typed_decisions() {
        assert_eq!(
            lifecycle_decision_kind_from_exit_code(0),
            RunLifecycleHookDecisionKind::Continue
        );
        assert_eq!(
            lifecycle_decision_kind_from_exit_code(20),
            RunLifecycleHookDecisionKind::RequestApproval
        );
        assert_eq!(
            lifecycle_decision_kind_from_exit_code(50),
            RunLifecycleHookDecisionKind::FailRun
        );
        assert_eq!(
            lifecycle_decision_kind_from_exit_code(-1),
            RunLifecycleHookDecisionKind::FailRun
        );
    }

    #[test]
    fn lifecycle_decision_is_attached_to_plugin_output() {
        let decision = palyra_common::runtime_contracts::RunLifecycleHookDecision::new(
            RunLifecycleHookPhase::BeforeTool,
            RunLifecycleHookDecisionKind::Block,
            "hook.policy",
            "plugin.policy",
        );
        let output = attach_lifecycle_decision(json!({"exit_code": 30}), &decision);
        assert_eq!(
            output.pointer("/lifecycle_decision/kind").and_then(serde_json::Value::as_str),
            Some("block")
        );
    }

    #[test]
    fn terminal_lifecycle_resolution_is_returned_as_dispatch_error() {
        let mut decision = RunLifecycleHookDecision::new(
            RunLifecycleHookPhase::BeforeTool,
            RunLifecycleHookDecisionKind::Block,
            "hook.policy",
            "plugin.policy",
        );
        decision.reason = Some("policy denied shell access".to_owned());
        let resolution =
            resolve_run_lifecycle_hook_decisions(RunLifecycleHookPhase::BeforeTool, vec![decision])
                .expect("terminal lifecycle decision should resolve");

        let error = enforce_run_lifecycle_resolution("run:before_tool", &resolution).unwrap_err();

        assert!(error.to_string().contains("run:before_tool"));
        assert!(error.to_string().contains("blocked"));
        assert!(error.to_string().contains("hook.policy"));
    }

    #[test]
    fn non_terminal_lifecycle_resolution_allows_dispatch() {
        let decision = RunLifecycleHookDecision::new(
            RunLifecycleHookPhase::BeforeTool,
            RunLifecycleHookDecisionKind::Annotate,
            "hook.note",
            "plugin.note",
        );
        let resolution =
            resolve_run_lifecycle_hook_decisions(RunLifecycleHookPhase::BeforeTool, vec![decision])
                .expect("non-terminal lifecycle decision should resolve");

        enforce_run_lifecycle_resolution("run:before_tool", &resolution)
            .expect("non-terminal lifecycle decision should not stop dispatch");
    }
}
