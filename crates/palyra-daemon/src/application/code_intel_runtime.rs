//! Code-intelligence runtime lifecycle state for workspace diagnostics.
//!
//! This module owns the daemon-side supervisor read model: provider probe
//! observations become stable LSP-client lifecycle handles, audit events, and
//! broken-server cache entries without starting language-server processes.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Schema version for code-intelligence runtime snapshots and journal payloads.
pub(crate) const CODE_INTEL_RUNTIME_SCHEMA_VERSION: u32 = 1;
/// Journal event emitted when an LSP provider becomes available for a workspace.
pub(crate) const CODE_INTEL_PROVIDER_STARTED_EVENT: &str = "code_intel.provider.started";
/// Journal event emitted when an LSP provider is unavailable or degraded.
pub(crate) const CODE_INTEL_PROVIDER_DEGRADED_EVENT: &str = "code_intel.provider.degraded";
/// Journal event emitted for a post-mutation diagnostics delta.
pub(crate) const CODE_INTEL_DIAGNOSTICS_DELTA_EVENT: &str = "code_intel.diagnostics.delta";
/// Redaction posture for durable code-intelligence runtime payloads.
pub(crate) const CODE_INTEL_REDACTION_LEVEL: &str = "metadata_only";

const UNSCOPED_WORKSPACE_ROOT: &str = "unscoped";

/// Language/provider family used for code diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CodeIntelLanguage {
    Rust,
    TypeScript,
    JavaScript,
    Python,
    Go,
    Java,
    C,
    Cpp,
    CSharp,
    Ruby,
    Php,
    Yaml,
    Json,
    Shell,
}

impl CodeIntelLanguage {
    /// Stable, deterministic language catalog exposed by code-intelligence health.
    pub(crate) const ALL: &'static [Self] = &[
        Self::Rust,
        Self::TypeScript,
        Self::JavaScript,
        Self::Python,
        Self::Go,
        Self::Java,
        Self::C,
        Self::Cpp,
        Self::CSharp,
        Self::Ruby,
        Self::Php,
        Self::Yaml,
        Self::Json,
        Self::Shell,
    ];

    /// Stable lowercase language id used in reason codes and handle ids.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::TypeScript => "typescript",
            Self::JavaScript => "javascript",
            Self::Python => "python",
            Self::Go => "go",
            Self::Java => "java",
            Self::C => "c",
            Self::Cpp => "cpp",
            Self::CSharp => "csharp",
            Self::Ruby => "ruby",
            Self::Php => "php",
            Self::Yaml => "yaml",
            Self::Json => "json",
            Self::Shell => "shell",
        }
    }

    /// Default LSP provider name for this language.
    pub(crate) const fn provider_name(self) -> &'static str {
        match self {
            Self::Rust => "rust-analyzer",
            Self::TypeScript => "typescript-language-server",
            Self::JavaScript => "typescript-language-server",
            Self::Python => "pyright",
            Self::Go => "gopls",
            Self::Java => "jdtls",
            Self::C | Self::Cpp => "clangd",
            Self::CSharp => "omnisharp",
            Self::Ruby => "solargraph",
            Self::Php => "intelephense",
            Self::Yaml => "yaml-language-server",
            Self::Json => "vscode-json-language-server",
            Self::Shell => "bash-language-server",
        }
    }

    /// Infers the provider language from a workspace-relative path.
    pub(crate) fn from_path(path: &str) -> Option<Self> {
        let lower = path.to_ascii_lowercase();
        if lower.ends_with(".rs") {
            Some(Self::Rust)
        } else if lower.ends_with(".ts") || lower.ends_with(".tsx") {
            Some(Self::TypeScript)
        } else if lower.ends_with(".js")
            || lower.ends_with(".jsx")
            || lower.ends_with(".mjs")
            || lower.ends_with(".cjs")
        {
            Some(Self::JavaScript)
        } else if lower.ends_with(".py") || lower.ends_with(".pyi") {
            Some(Self::Python)
        } else if lower.ends_with(".go") {
            Some(Self::Go)
        } else if lower.ends_with(".java") {
            Some(Self::Java)
        } else if lower.ends_with(".c") || lower.ends_with(".h") {
            Some(Self::C)
        } else if lower.ends_with(".cc")
            || lower.ends_with(".cpp")
            || lower.ends_with(".cxx")
            || lower.ends_with(".hh")
            || lower.ends_with(".hpp")
            || lower.ends_with(".hxx")
        {
            Some(Self::Cpp)
        } else if lower.ends_with(".cs") {
            Some(Self::CSharp)
        } else if lower.ends_with(".rb") || lower.ends_with(".rake") || lower.ends_with("gemfile") {
            Some(Self::Ruby)
        } else if lower.ends_with(".php") || lower.ends_with(".phtml") {
            Some(Self::Php)
        } else if lower.ends_with(".yaml") || lower.ends_with(".yml") {
            Some(Self::Yaml)
        } else if lower.ends_with(".json") || lower.ends_with(".jsonc") {
            Some(Self::Json)
        } else if lower.ends_with(".sh")
            || lower.ends_with(".bash")
            || lower.ends_with(".zsh")
            || lower.ends_with(".ksh")
        {
            Some(Self::Shell)
        } else {
            None
        }
    }
}

/// Probe status produced by the read-only diagnostics adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodeIntelProviderProbeStatus {
    Disabled,
    Skipped,
    Restarting,
    Ready,
    MissingBinary,
    Degraded,
    Failed,
    Unknown,
}

impl CodeIntelProviderProbeStatus {
    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "disabled" => Self::Disabled,
            "skipped" => Self::Skipped,
            "restarting" => Self::Restarting,
            "ready" => Self::Ready,
            "missing_binary" => Self::MissingBinary,
            "degraded" => Self::Degraded,
            "failed" => Self::Failed,
            _ => Self::Unknown,
        }
    }

    const fn lifecycle_status(self) -> LspClientLifecycleStatus {
        match self {
            Self::Disabled => LspClientLifecycleStatus::Disabled,
            Self::Skipped => LspClientLifecycleStatus::Skipped,
            Self::Restarting => LspClientLifecycleStatus::Restarting,
            Self::Ready => LspClientLifecycleStatus::Ready,
            Self::MissingBinary => LspClientLifecycleStatus::MissingBinary,
            Self::Degraded | Self::Unknown => LspClientLifecycleStatus::Degraded,
            Self::Failed => LspClientLifecycleStatus::Failed,
        }
    }
}

/// Provider observation accepted by the runtime supervisor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodeIntelProviderObservation {
    pub(crate) provider: String,
    pub(crate) language: CodeIntelLanguage,
    pub(crate) status: CodeIntelProviderProbeStatus,
    pub(crate) binary_label: String,
    pub(crate) reason_code: String,
    pub(crate) repair_hint: String,
}

impl CodeIntelProviderObservation {
    /// Builds an observation from the diagnostics adapter's provider status.
    pub(crate) fn from_status_fields(
        provider: &str,
        language: CodeIntelLanguage,
        status: &str,
        binary: &str,
        reason_code: &str,
        repair_hint: &str,
    ) -> Self {
        Self {
            provider: non_empty_or_default(provider, language.provider_name()),
            language,
            status: CodeIntelProviderProbeStatus::parse(status),
            binary_label: binary_label(binary),
            reason_code: non_empty_or_default(reason_code, "code_intel.provider_unknown"),
            repair_hint: non_empty_or_default(repair_hint, "Inspect code-intelligence runtime."),
        }
    }
}

/// Aggregate state of the code-intelligence runtime read model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CodeIntelRuntimeStatus {
    Disabled,
    Idle,
    Healthy,
    Degraded,
}

/// First-rollout behavior for the supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CodeIntelRuntimeMode {
    Disabled,
    ObserveOnly,
}

/// Lifecycle state for a per-workspace, per-language LSP client handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LspClientLifecycleStatus {
    Disabled,
    Skipped,
    Starting,
    Ready,
    MissingBinary,
    Degraded,
    Restarting,
    Failed,
    BrokenCached,
    Stopped,
}

impl LspClientLifecycleStatus {
    const fn is_degraded(self) -> bool {
        matches!(self, Self::MissingBinary | Self::Degraded | Self::Failed | Self::BrokenCached)
    }

    const fn is_active(self) -> bool {
        matches!(
            self,
            Self::Starting | Self::Ready | Self::Degraded | Self::Restarting | Self::BrokenCached
        )
    }
}

/// Daemon-managed lifecycle handle for one LSP provider scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LspServerHandle {
    pub(crate) schema_version: u32,
    pub(crate) handle_id: String,
    pub(crate) workspace_root: Option<String>,
    pub(crate) language: CodeIntelLanguage,
    pub(crate) provider: String,
    pub(crate) binary_label: String,
    pub(crate) status: LspClientLifecycleStatus,
    pub(crate) reason_code: String,
    pub(crate) repair_hint: String,
    pub(crate) started_at_unix_ms: Option<i64>,
    pub(crate) last_used_at_unix_ms: Option<i64>,
    pub(crate) degraded_at_unix_ms: Option<i64>,
    pub(crate) crash_count: u32,
    pub(crate) last_diagnostics_refresh_unix_ms: Option<i64>,
    pub(crate) timeout_ms: u64,
    pub(crate) idle_reap_ms: u64,
    pub(crate) redaction_level: String,
}

/// Broken-provider cache entry used to avoid repeated long LSP timeouts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BrokenLspServerCacheEntry {
    pub(crate) schema_version: u32,
    pub(crate) workspace_root: Option<String>,
    pub(crate) language: CodeIntelLanguage,
    pub(crate) provider: String,
    pub(crate) reason_code: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) retry_after_unix_ms: i64,
    pub(crate) timeout_ms: u64,
    pub(crate) redaction_level: String,
}

/// Operator-facing code-intelligence supervisor snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CodeIntelRuntimeSnapshot {
    pub(crate) schema_version: u32,
    pub(crate) enabled: bool,
    pub(crate) mode: CodeIntelRuntimeMode,
    pub(crate) status: CodeIntelRuntimeStatus,
    pub(crate) workspace_root: Option<String>,
    pub(crate) clients: Vec<LspServerHandle>,
    pub(crate) broken_server_cache: Vec<BrokenLspServerCacheEntry>,
    pub(crate) timeout_ms: u64,
    pub(crate) idle_reap_ms: u64,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) journal_events: Vec<String>,
    pub(crate) redaction_level: String,
}

/// Audit event returned when an observation changes provider lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CodeIntelRuntimeAuditEvent {
    pub(crate) schema_version: u32,
    pub(crate) event_type: String,
    pub(crate) provider: String,
    pub(crate) language: CodeIntelLanguage,
    pub(crate) status: LspClientLifecycleStatus,
    pub(crate) reason_code: String,
    pub(crate) workspace_root: Option<String>,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) redaction_level: String,
}

/// Result of applying provider observations to the runtime read model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodeIntelRuntimeObservationOutcome {
    pub(crate) snapshot: CodeIntelRuntimeSnapshot,
    pub(crate) audit_events: Vec<CodeIntelRuntimeAuditEvent>,
}

/// Inputs for one provider observation pass.
pub(crate) struct CodeIntelRuntimeObservationRequest<'a> {
    pub(crate) enabled: bool,
    pub(crate) workspace_root: Option<&'a str>,
    pub(crate) observations: &'a [CodeIntelProviderObservation],
    pub(crate) timeout_ms: u64,
    pub(crate) idle_reap_ms: u64,
    pub(crate) now_unix_ms: i64,
    pub(crate) evidence_refs: &'a [String],
}

/// Inputs for a snapshot-only read.
pub(crate) struct CodeIntelRuntimeSnapshotRequest<'a> {
    pub(crate) enabled: bool,
    pub(crate) workspace_root: Option<&'a str>,
    pub(crate) timeout_ms: u64,
    pub(crate) idle_reap_ms: u64,
    pub(crate) now_unix_ms: i64,
}

/// Inputs for recording a provider timeout into the broken-server cache.
#[cfg(test)]
pub(crate) struct CodeIntelBrokenServerRequest<'a> {
    pub(crate) workspace_root: Option<&'a str>,
    pub(crate) language: CodeIntelLanguage,
    pub(crate) provider: &'a str,
    pub(crate) reason_code: &'a str,
    pub(crate) timeout_ms: u64,
    pub(crate) retry_after_ms: u64,
    pub(crate) now_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CodeIntelClientKey {
    workspace_root: String,
    language: CodeIntelLanguage,
}

impl CodeIntelClientKey {
    fn new(workspace_root: Option<&str>, language: CodeIntelLanguage) -> Self {
        Self {
            workspace_root: workspace_root
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(UNSCOPED_WORKSPACE_ROOT)
                .to_owned(),
            language,
        }
    }
}

/// In-memory supervisor read model for code-intelligence provider lifecycle.
#[derive(Debug, Default)]
pub(crate) struct CodeIntelRuntime {
    handles: BTreeMap<CodeIntelClientKey, LspServerHandle>,
    broken_server_cache: BTreeMap<CodeIntelClientKey, BrokenLspServerCacheEntry>,
}

impl CodeIntelRuntime {
    /// Creates an empty code-intelligence runtime read model.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Applies provider probe observations and returns the updated snapshot.
    pub(crate) fn observe(
        &mut self,
        request: CodeIntelRuntimeObservationRequest<'_>,
    ) -> CodeIntelRuntimeObservationOutcome {
        self.reap_stale(request.now_unix_ms, request.idle_reap_ms);
        if !request.enabled {
            return CodeIntelRuntimeObservationOutcome {
                snapshot: self.snapshot_from_state(CodeIntelRuntimeSnapshotRequest {
                    enabled: false,
                    workspace_root: request.workspace_root,
                    timeout_ms: request.timeout_ms,
                    idle_reap_ms: request.idle_reap_ms,
                    now_unix_ms: request.now_unix_ms,
                }),
                audit_events: Vec::new(),
            };
        }

        let mut audit_events = Vec::new();
        for observation in request.observations {
            let key = CodeIntelClientKey::new(request.workspace_root, observation.language);
            let previous = self.handles.get(&key).cloned();
            let status = self.lifecycle_status_for(&key, observation);
            let handle = LspServerHandle {
                schema_version: CODE_INTEL_RUNTIME_SCHEMA_VERSION,
                handle_id: handle_id(request.workspace_root, observation.language),
                workspace_root: normalized_workspace_root(request.workspace_root),
                language: observation.language,
                provider: observation.provider.clone(),
                binary_label: observation.binary_label.clone(),
                status,
                reason_code: lifecycle_reason_code(&key, observation, status),
                repair_hint: lifecycle_repair_hint(observation, status),
                started_at_unix_ms: started_at(previous.as_ref(), status, request.now_unix_ms),
                last_used_at_unix_ms: status.is_active().then_some(request.now_unix_ms),
                degraded_at_unix_ms: status.is_degraded().then_some(request.now_unix_ms),
                crash_count: crash_count(previous.as_ref(), observation, status),
                last_diagnostics_refresh_unix_ms: Some(request.now_unix_ms),
                timeout_ms: request.timeout_ms,
                idle_reap_ms: request.idle_reap_ms,
                redaction_level: CODE_INTEL_REDACTION_LEVEL.to_owned(),
            };
            if let Some(event) = audit_event_for_transition(
                previous.as_ref(),
                &handle,
                request.now_unix_ms,
                request.evidence_refs,
            ) {
                audit_events.push(event);
            }
            self.handles.insert(key, handle);
        }

        let snapshot = self.snapshot_from_state(CodeIntelRuntimeSnapshotRequest {
            enabled: true,
            workspace_root: request.workspace_root,
            timeout_ms: request.timeout_ms,
            idle_reap_ms: request.idle_reap_ms,
            now_unix_ms: request.now_unix_ms,
        });
        CodeIntelRuntimeObservationOutcome { snapshot, audit_events }
    }

    /// Returns a current snapshot and reaps expired idle/cache entries.
    pub(crate) fn snapshot(
        &mut self,
        request: CodeIntelRuntimeSnapshotRequest<'_>,
    ) -> CodeIntelRuntimeSnapshot {
        self.reap_stale(request.now_unix_ms, request.idle_reap_ms);
        self.snapshot_from_state(request)
    }

    /// Records an explicit provider timeout in the broken-server cache.
    #[cfg(test)]
    pub(crate) fn record_broken_server(
        &mut self,
        request: CodeIntelBrokenServerRequest<'_>,
    ) -> CodeIntelRuntimeSnapshot {
        let key = CodeIntelClientKey::new(request.workspace_root, request.language);
        self.broken_server_cache.insert(
            key,
            BrokenLspServerCacheEntry {
                schema_version: CODE_INTEL_RUNTIME_SCHEMA_VERSION,
                workspace_root: normalized_workspace_root(request.workspace_root),
                language: request.language,
                provider: non_empty_or_default(request.provider, request.language.provider_name()),
                reason_code: non_empty_or_default(
                    request.reason_code,
                    "code_intel.provider_timeout",
                ),
                created_at_unix_ms: request.now_unix_ms,
                retry_after_unix_ms: add_ms(request.now_unix_ms, request.retry_after_ms),
                timeout_ms: request.timeout_ms,
                redaction_level: CODE_INTEL_REDACTION_LEVEL.to_owned(),
            },
        );
        self.snapshot(CodeIntelRuntimeSnapshotRequest {
            enabled: true,
            workspace_root: request.workspace_root,
            timeout_ms: request.timeout_ms,
            idle_reap_ms: request.retry_after_ms,
            now_unix_ms: request.now_unix_ms,
        })
    }

    fn lifecycle_status_for(
        &self,
        key: &CodeIntelClientKey,
        observation: &CodeIntelProviderObservation,
    ) -> LspClientLifecycleStatus {
        if self.broken_server_cache.contains_key(key)
            && observation.status == CodeIntelProviderProbeStatus::Ready
        {
            return LspClientLifecycleStatus::BrokenCached;
        }
        observation.status.lifecycle_status()
    }

    fn snapshot_from_state(
        &self,
        request: CodeIntelRuntimeSnapshotRequest<'_>,
    ) -> CodeIntelRuntimeSnapshot {
        if !request.enabled {
            return CodeIntelRuntimeSnapshot {
                schema_version: CODE_INTEL_RUNTIME_SCHEMA_VERSION,
                enabled: false,
                mode: CodeIntelRuntimeMode::Disabled,
                status: CodeIntelRuntimeStatus::Disabled,
                workspace_root: normalized_workspace_root(request.workspace_root),
                clients: Vec::new(),
                broken_server_cache: Vec::new(),
                timeout_ms: request.timeout_ms,
                idle_reap_ms: request.idle_reap_ms,
                reason_codes: vec!["code_intel.disabled".to_owned()],
                journal_events: vec![
                    CODE_INTEL_PROVIDER_STARTED_EVENT.to_owned(),
                    CODE_INTEL_PROVIDER_DEGRADED_EVENT.to_owned(),
                    CODE_INTEL_DIAGNOSTICS_DELTA_EVENT.to_owned(),
                ],
                redaction_level: CODE_INTEL_REDACTION_LEVEL.to_owned(),
            };
        }
        let mut clients = self
            .handles
            .values()
            .filter(|handle| {
                workspace_matches(handle.workspace_root.as_deref(), request.workspace_root)
            })
            .cloned()
            .collect::<Vec<_>>();
        clients.sort_by(|left, right| {
            left.workspace_root
                .cmp(&right.workspace_root)
                .then(left.language.cmp(&right.language))
                .then(left.provider.cmp(&right.provider))
        });
        let mut broken_server_cache = self
            .broken_server_cache
            .values()
            .filter(|entry| {
                workspace_matches(entry.workspace_root.as_deref(), request.workspace_root)
            })
            .cloned()
            .collect::<Vec<_>>();
        broken_server_cache.sort_by(|left, right| {
            left.workspace_root.cmp(&right.workspace_root).then(left.language.cmp(&right.language))
        });
        let reason_codes = reason_codes_for_snapshot(&clients, &broken_server_cache);
        CodeIntelRuntimeSnapshot {
            schema_version: CODE_INTEL_RUNTIME_SCHEMA_VERSION,
            enabled: request.enabled,
            mode: if request.enabled {
                CodeIntelRuntimeMode::ObserveOnly
            } else {
                CodeIntelRuntimeMode::Disabled
            },
            status: runtime_status(request.enabled, &clients),
            workspace_root: normalized_workspace_root(request.workspace_root),
            clients,
            broken_server_cache,
            timeout_ms: request.timeout_ms,
            idle_reap_ms: request.idle_reap_ms,
            reason_codes,
            journal_events: vec![
                CODE_INTEL_PROVIDER_STARTED_EVENT.to_owned(),
                CODE_INTEL_PROVIDER_DEGRADED_EVENT.to_owned(),
                CODE_INTEL_DIAGNOSTICS_DELTA_EVENT.to_owned(),
            ],
            redaction_level: CODE_INTEL_REDACTION_LEVEL.to_owned(),
        }
    }

    fn reap_stale(&mut self, now_unix_ms: i64, idle_reap_ms: u64) {
        self.handles.retain(|_, handle| {
            handle
                .last_used_at_unix_ms
                .is_none_or(|last_used| add_ms(last_used, idle_reap_ms) > now_unix_ms)
        });
        self.broken_server_cache.retain(|_, entry| entry.retry_after_unix_ms > now_unix_ms);
    }
}

fn audit_event_for_transition(
    previous: Option<&LspServerHandle>,
    handle: &LspServerHandle,
    now_unix_ms: i64,
    evidence_refs: &[String],
) -> Option<CodeIntelRuntimeAuditEvent> {
    if previous.is_some_and(|previous| previous.status == handle.status) {
        return None;
    }
    let event_type = if handle.status == LspClientLifecycleStatus::Ready {
        CODE_INTEL_PROVIDER_STARTED_EVENT
    } else if handle.status.is_degraded()
        || handle.status == LspClientLifecycleStatus::MissingBinary
    {
        CODE_INTEL_PROVIDER_DEGRADED_EVENT
    } else {
        return None;
    };
    Some(CodeIntelRuntimeAuditEvent {
        schema_version: CODE_INTEL_RUNTIME_SCHEMA_VERSION,
        event_type: event_type.to_owned(),
        provider: handle.provider.clone(),
        language: handle.language,
        status: handle.status,
        reason_code: handle.reason_code.clone(),
        workspace_root: handle.workspace_root.clone(),
        created_at_unix_ms: now_unix_ms,
        evidence_refs: evidence_refs.to_vec(),
        redaction_level: CODE_INTEL_REDACTION_LEVEL.to_owned(),
    })
}

fn started_at(
    previous: Option<&LspServerHandle>,
    status: LspClientLifecycleStatus,
    now_unix_ms: i64,
) -> Option<i64> {
    previous
        .and_then(|handle| handle.started_at_unix_ms)
        .or_else(|| status.is_active().then_some(now_unix_ms))
}

fn crash_count(
    previous: Option<&LspServerHandle>,
    observation: &CodeIntelProviderObservation,
    status: LspClientLifecycleStatus,
) -> u32 {
    let previous_count = previous.map(|handle| handle.crash_count).unwrap_or(0);
    if status == LspClientLifecycleStatus::Failed || provider_status_looks_crash_like(observation) {
        previous_count.saturating_add(1)
    } else {
        previous_count
    }
}

fn provider_status_looks_crash_like(observation: &CodeIntelProviderObservation) -> bool {
    matches!(
        observation.status,
        CodeIntelProviderProbeStatus::Failed | CodeIntelProviderProbeStatus::Restarting
    ) || observation.reason_code.contains("spawn_failed")
        || observation.reason_code.contains("pipe_failed")
        || observation.reason_code.contains("timeout")
}

fn lifecycle_reason_code(
    key: &CodeIntelClientKey,
    observation: &CodeIntelProviderObservation,
    status: LspClientLifecycleStatus,
) -> String {
    if status == LspClientLifecycleStatus::BrokenCached {
        return self_broken_reason_code(key);
    }
    observation.reason_code.clone()
}

fn lifecycle_repair_hint(
    observation: &CodeIntelProviderObservation,
    status: LspClientLifecycleStatus,
) -> String {
    if status == LspClientLifecycleStatus::BrokenCached {
        "Provider is temporarily cached as degraded after a timeout; retry after the cache entry expires."
            .to_owned()
    } else {
        observation.repair_hint.clone()
    }
}

fn runtime_status(enabled: bool, clients: &[LspServerHandle]) -> CodeIntelRuntimeStatus {
    if !enabled {
        return CodeIntelRuntimeStatus::Disabled;
    }
    if clients.is_empty()
        || clients.iter().all(|client| matches!(client.status, LspClientLifecycleStatus::Skipped))
    {
        return CodeIntelRuntimeStatus::Idle;
    }
    if clients.iter().any(|client| client.status.is_degraded()) {
        CodeIntelRuntimeStatus::Degraded
    } else {
        CodeIntelRuntimeStatus::Healthy
    }
}

fn reason_codes_for_snapshot(
    clients: &[LspServerHandle],
    broken_server_cache: &[BrokenLspServerCacheEntry],
) -> Vec<String> {
    let mut reason_codes = clients
        .iter()
        .filter(|client| client.status != LspClientLifecycleStatus::Ready)
        .map(|client| client.reason_code.clone())
        .chain(broken_server_cache.iter().map(|entry| entry.reason_code.clone()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if reason_codes.is_empty() {
        reason_codes.push("code_intel.runtime.ready".to_owned());
    }
    reason_codes
}

fn workspace_matches(handle_root: Option<&str>, requested_root: Option<&str>) -> bool {
    let Some(requested) = normalized_workspace_root(requested_root) else {
        return true;
    };
    handle_root == Some(requested.as_str())
}

fn normalized_workspace_root(workspace_root: Option<&str>) -> Option<String> {
    workspace_root.map(str::trim).filter(|value| !value.is_empty()).map(str::to_owned)
}

fn handle_id(workspace_root: Option<&str>, language: CodeIntelLanguage) -> String {
    let root_id = workspace_root_hash(workspace_root.unwrap_or(UNSCOPED_WORKSPACE_ROOT));
    format!("lsp:{}:{root_id}", language.as_str())
}

fn workspace_root_hash(workspace_root: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(workspace_root.as_bytes());
    hasher.finalize().iter().take(6).map(|byte| format!("{byte:02x}")).collect()
}

fn self_broken_reason_code(key: &CodeIntelClientKey) -> String {
    format!("code_intel.provider_broken_cached.{}", key.language.as_str())
}

fn binary_label(binary: &str) -> String {
    let trimmed = binary.trim();
    if trimmed.is_empty() {
        return "<unset>".to_owned();
    }
    let path = Path::new(trimmed);
    if path.is_absolute() || path.components().count() > 1 {
        return path
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.trim().is_empty())
            .unwrap_or("<configured-binary>")
            .to_owned();
    }
    trimmed.to_owned()
}

fn non_empty_or_default(value: &str, fallback: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        fallback.to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn add_ms(timestamp_unix_ms: i64, delta_ms: u64) -> i64 {
    timestamp_unix_ms.saturating_add(i64::try_from(delta_ms).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(
        language: CodeIntelLanguage,
        status: &str,
        reason_code: &str,
    ) -> CodeIntelProviderObservation {
        CodeIntelProviderObservation::from_status_fields(
            language.provider_name(),
            language,
            status,
            language.provider_name(),
            reason_code,
            "repair",
        )
    }

    #[test]
    fn runtime_snapshot_serializes_lifecycle_contract() {
        let mut runtime = CodeIntelRuntime::new();
        let observations =
            vec![observation(CodeIntelLanguage::Rust, "ready", "code_intel.provider_ready.rust")];
        let outcome = runtime.observe(CodeIntelRuntimeObservationRequest {
            enabled: true,
            workspace_root: Some("workspace"),
            observations: observations.as_slice(),
            timeout_ms: 2_000,
            idle_reap_ms: 60_000,
            now_unix_ms: 100,
            evidence_refs: &["mutation:1".to_owned()],
        });

        let json = serde_json::to_string(&outcome.snapshot).expect("snapshot should serialize");
        let decoded: CodeIntelRuntimeSnapshot =
            serde_json::from_str(json.as_str()).expect("snapshot should deserialize");
        assert_eq!(decoded.schema_version, CODE_INTEL_RUNTIME_SCHEMA_VERSION);
        assert_eq!(decoded.status, CodeIntelRuntimeStatus::Healthy);
        assert_eq!(decoded.clients[0].status, LspClientLifecycleStatus::Ready);
        assert_eq!(decoded.clients[0].binary_label, "rust-analyzer");
        assert_eq!(decoded.clients[0].crash_count, 0);
        assert_eq!(decoded.clients[0].last_diagnostics_refresh_unix_ms, Some(100));
        assert_eq!(decoded.redaction_level, CODE_INTEL_REDACTION_LEVEL);
        assert!(decoded
            .journal_events
            .iter()
            .any(|event| event == CODE_INTEL_DIAGNOSTICS_DELTA_EVENT));
        assert_eq!(outcome.audit_events.len(), 1);
        assert_eq!(outcome.audit_events[0].event_type, CODE_INTEL_PROVIDER_STARTED_EVENT);
    }

    #[test]
    fn provider_observation_redacts_configured_binary_path_to_label() {
        let observation = CodeIntelProviderObservation::from_status_fields(
            "rust-analyzer",
            CodeIntelLanguage::Rust,
            "ready",
            "workspace/bin/rust-analyzer",
            "code_intel.provider_ready.rust",
            "repair",
        );

        assert_eq!(observation.binary_label, "rust-analyzer");
    }

    #[test]
    fn missing_provider_degrades_without_failing_runtime() {
        let mut runtime = CodeIntelRuntime::new();
        let observations = vec![observation(
            CodeIntelLanguage::TypeScript,
            "missing_binary",
            "code_intel.provider_missing.typescript",
        )];
        let outcome = runtime.observe(CodeIntelRuntimeObservationRequest {
            enabled: true,
            workspace_root: Some("workspace"),
            observations: observations.as_slice(),
            timeout_ms: 2_000,
            idle_reap_ms: 60_000,
            now_unix_ms: 100,
            evidence_refs: &["mutation:1".to_owned()],
        });

        assert_eq!(outcome.snapshot.status, CodeIntelRuntimeStatus::Degraded);
        assert_eq!(outcome.audit_events[0].event_type, CODE_INTEL_PROVIDER_DEGRADED_EVENT);
        assert_eq!(outcome.snapshot.clients[0].status, LspClientLifecycleStatus::MissingBinary);
    }

    #[test]
    fn crash_like_provider_observation_increments_crash_count() {
        let mut runtime = CodeIntelRuntime::new();
        let observations = vec![observation(
            CodeIntelLanguage::Rust,
            "degraded",
            "code_intel.rust.cargo_check_timeout",
        )];
        let outcome = runtime.observe(CodeIntelRuntimeObservationRequest {
            enabled: true,
            workspace_root: Some("workspace"),
            observations: observations.as_slice(),
            timeout_ms: 2_000,
            idle_reap_ms: 60_000,
            now_unix_ms: 100,
            evidence_refs: &["mutation:1".to_owned()],
        });

        assert_eq!(outcome.snapshot.clients[0].status, LspClientLifecycleStatus::Degraded);
        assert_eq!(outcome.snapshot.clients[0].crash_count, 1);
        assert_eq!(outcome.snapshot.clients[0].last_diagnostics_refresh_unix_ms, Some(100));
    }

    #[test]
    fn broken_server_cache_overrides_ready_probe_until_retry_expires() {
        let mut runtime = CodeIntelRuntime::new();
        runtime.record_broken_server(CodeIntelBrokenServerRequest {
            workspace_root: Some("workspace"),
            language: CodeIntelLanguage::Python,
            provider: "pyright",
            reason_code: "code_intel.provider_timeout.python",
            timeout_ms: 2_000,
            retry_after_ms: 60_000,
            now_unix_ms: 100,
        });
        let observations = vec![observation(
            CodeIntelLanguage::Python,
            "ready",
            "code_intel.provider_ready.python",
        )];
        let cached = runtime.observe(CodeIntelRuntimeObservationRequest {
            enabled: true,
            workspace_root: Some("workspace"),
            observations: observations.as_slice(),
            timeout_ms: 2_000,
            idle_reap_ms: 60_000,
            now_unix_ms: 1_000,
            evidence_refs: &[],
        });

        assert_eq!(cached.snapshot.status, CodeIntelRuntimeStatus::Degraded);
        assert_eq!(cached.snapshot.clients[0].status, LspClientLifecycleStatus::BrokenCached);

        let refreshed = runtime.observe(CodeIntelRuntimeObservationRequest {
            enabled: true,
            workspace_root: Some("workspace"),
            observations: observations.as_slice(),
            timeout_ms: 2_000,
            idle_reap_ms: 60_000,
            now_unix_ms: 61_000,
            evidence_refs: &[],
        });
        assert_eq!(refreshed.snapshot.status, CodeIntelRuntimeStatus::Healthy);
        assert_eq!(refreshed.snapshot.clients[0].status, LspClientLifecycleStatus::Ready);
    }

    #[test]
    fn idle_cleanup_reaps_stale_handles() {
        let mut runtime = CodeIntelRuntime::new();
        let observations =
            vec![observation(CodeIntelLanguage::Rust, "ready", "code_intel.provider_ready.rust")];
        runtime.observe(CodeIntelRuntimeObservationRequest {
            enabled: true,
            workspace_root: Some("workspace"),
            observations: observations.as_slice(),
            timeout_ms: 2_000,
            idle_reap_ms: 100,
            now_unix_ms: 1_000,
            evidence_refs: &[],
        });

        let snapshot = runtime.snapshot(CodeIntelRuntimeSnapshotRequest {
            enabled: true,
            workspace_root: Some("workspace"),
            timeout_ms: 2_000,
            idle_reap_ms: 100,
            now_unix_ms: 1_200,
        });

        assert!(snapshot.clients.is_empty());
        assert_eq!(snapshot.status, CodeIntelRuntimeStatus::Idle);
    }
}
