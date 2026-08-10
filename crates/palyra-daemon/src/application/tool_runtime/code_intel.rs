//! Bounded code-diagnostics adapter for workspace mutations.
//!
//! The adapter is intentionally read-only: it validates touched paths against
//! the configured workspace, probes configured provider binaries, normalizes
//! diagnostics into workspace-relative paths, and computes before/after deltas.
//! Provider process orchestration can grow behind this contract without
//! changing the `palyra.fs.apply_patch` output shape.

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::Arc,
};

use palyra_common::{
    redaction::redact_diagnostic_text, workspace_patch::WorkspacePatchFileAttestation,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command as TokioCommand,
    time::{timeout, Duration},
};

use crate::{
    agents::AgentResolveRequest,
    application::code_intel_runtime::{
        CodeIntelLanguage, CodeIntelProviderObservation, CodeIntelProviderSnapshotAuthority,
        CodeIntelRuntimeSnapshot, CODE_INTEL_REDACTION_LEVEL,
    },
    application::tool_runtime::workspace_scope::{
        relative_path_should_use_active_root, session_active_workspace_root,
        workspace_root_override_targets_active_root,
        workspace_roots_with_run_launch_context_for_agent_source,
    },
    config::CodeIntelConfig,
    gateway::{
        GatewayRuntimeState, ManagedRuntimeHealthAuthority, ManagedRuntimeHealthFamily,
        ToolRuntimeExecutionContext, CODE_DEFINITION_TOOL_NAME, CODE_DIAGNOSTICS_TOOL_NAME,
        CODE_HEALTH_TOOL_NAME, CODE_HOVER_TOOL_NAME, CODE_OUTLINE_TOOL_NAME,
        CODE_REFERENCES_TOOL_NAME, CODE_SYMBOLS_TOOL_NAME, CODE_WORKSPACE_SYMBOLS_TOOL_NAME,
    },
    tool_protocol::{build_tool_execution_outcome, ToolExecutionOutcome},
};

const CODE_INTEL_SCHEMA_VERSION: u32 = 1;
const MAX_DIAGNOSTIC_MESSAGE_CHARS: usize = 320;
const RUST_ANALYZER_CARGO_CHECK_SOURCE: &str = "rust-analyzer/cargo-check";
const RUST_ANALYZER_CARGO_CHECK_COMMAND: &str = "cargo";
const RUST_ANALYZER_CARGO_CHECK_ARGS: &[&str] =
    &["check", "--quiet", "--workspace", "--message-format=json", "--all-targets", "--keep-going"];
const RUST_ANALYZER_ERROR_HINT_CHARS: usize = 512;
const TYPESCRIPT_LANGUAGE_SERVER_TSC_SOURCE: &str = "typescript-language-server/tsc";
const TYPESCRIPT_TSC_COMMAND: &str = "tsc";
const TYPESCRIPT_TSC_ARGS: &[&str] = &["--noEmit", "--pretty", "false"];
const TYPESCRIPT_ERROR_HINT_CHARS: usize = 512;
const PYRIGHT_SOURCE: &str = "pyright";
const PYRIGHT_CLI_COMMAND: &str = "pyright";
const PYRIGHT_ARGS: &[&str] = &["--outputjson"];
const PYRIGHT_ERROR_HINT_CHARS: usize = 512;
const CODE_INTEL_TOOL_INPUT_MAX_BYTES: usize = 16 * 1024;
const CODE_INTEL_MAX_SOURCE_BYTES: u64 = 256 * 1024;
const CODE_INTEL_MAX_SYMBOLS: usize = 200;
const CODE_INTEL_MAX_WORKSPACE_FILES: usize = 1_000;
const CODE_INTEL_MAX_WORKSPACE_DEPTH: usize = 24;
const CODE_INTEL_MAX_WORKSPACE_RESULTS: usize = 200;
const CODE_INTEL_CONTEXT_SYMBOL_LIMIT: usize = 24;
const CODE_INTEL_SKIPPED_DIRS: &[&str] =
    &[".git", "node_modules", "target", "dist", "build", ".next", ".svelte-kit"];
const CODE_INTEL_MANIFEST_NAMES: &[&str] =
    &["Cargo.toml", "package.json", "pyproject.toml", "tsconfig.json", "jsconfig.json"];
const LSP_REGISTRY_ONLY_FALLBACK: &str = "registry_only_lexical_fallback";
const CODE_INTEL_READ_ONLY_DIAGNOSTICS_REASON: &str =
    "code_intel.diagnostics.provider_execution_blocked";
pub(crate) const CODE_INTEL_RUST_SNAPSHOT_CAPTURED_EVENT: &str =
    "code_intel.rust.snapshot_captured";
pub(crate) const CODE_INTEL_TYPESCRIPT_SNAPSHOT_CAPTURED_EVENT: &str =
    "code_intel.typescript.snapshot_captured";
pub(crate) const CODE_INTEL_PYTHON_SNAPSHOT_CAPTURED_EVENT: &str =
    "code_intel.python.snapshot_captured";

/// Diagnostics-only language-server descriptor exposed in redacted snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct LanguageServerDescriptor {
    pub language: CodeIntelLanguage,
    pub provider: String,
    pub binary_label: String,
    pub file_patterns: Vec<String>,
    pub diagnostics_only: bool,
    pub supports_symbols: bool,
    pub supports_references: bool,
    pub fallback: String,
    pub timeout_ms: u64,
    pub idle_reap_ms: u64,
    pub redaction_level: String,
    #[serde(skip)]
    binary: String,
    #[serde(skip)]
    integration: LanguageServerIntegration,
}

/// Whether this rollout executes the provider or only exposes safe fallback metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LanguageServerIntegration {
    ExternalDiagnostics,
    RegistryOnly,
}

/// Static registry for diagnostics-capable and fallback language servers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LanguageServerRegistry {
    descriptors: BTreeMap<CodeIntelLanguage, LanguageServerDescriptor>,
}

impl LanguageServerRegistry {
    fn from_config(config: &CodeIntelConfig) -> Self {
        let descriptors = CodeIntelLanguage::ALL
            .iter()
            .copied()
            .map(|language| {
                let binary = configured_lsp_binary(language, config);
                (
                    language,
                    LanguageServerDescriptor {
                        language,
                        provider: language.provider_name().to_owned(),
                        binary_label: diagnostic_binary_label(binary),
                        file_patterns: language_file_patterns(language),
                        diagnostics_only: true,
                        supports_symbols: language_supports_symbols(language),
                        supports_references: language_supports_references(language),
                        fallback: language_fallback(language).to_owned(),
                        timeout_ms: config.timeout_ms,
                        idle_reap_ms: config.idle_reap_ms,
                        redaction_level:
                            crate::application::code_intel_runtime::CODE_INTEL_REDACTION_LEVEL
                                .to_owned(),
                        binary: binary.to_owned(),
                        integration: language_server_integration(language),
                    },
                )
            })
            .collect();
        Self { descriptors }
    }

    fn descriptors(&self) -> Vec<LanguageServerDescriptor> {
        self.descriptors.values().cloned().collect()
    }
}

fn configured_lsp_binary(language: CodeIntelLanguage, config: &CodeIntelConfig) -> &str {
    match language {
        CodeIntelLanguage::Rust => config.rust_analyzer_binary.as_str(),
        CodeIntelLanguage::TypeScript => config.typescript_server_binary.as_str(),
        CodeIntelLanguage::JavaScript => config.typescript_server_binary.as_str(),
        CodeIntelLanguage::Python => config.pyright_binary.as_str(),
        CodeIntelLanguage::Go => "gopls",
        CodeIntelLanguage::Java => "jdtls",
        CodeIntelLanguage::C | CodeIntelLanguage::Cpp => "clangd",
        CodeIntelLanguage::CSharp => "omnisharp",
        CodeIntelLanguage::Ruby => "solargraph",
        CodeIntelLanguage::Php => "intelephense",
        CodeIntelLanguage::Yaml => "yaml-language-server",
        CodeIntelLanguage::Json => "vscode-json-language-server",
        CodeIntelLanguage::Shell => "bash-language-server",
    }
}

fn language_file_patterns(language: CodeIntelLanguage) -> Vec<String> {
    match language {
        CodeIntelLanguage::Rust => &["**/*.rs"][..],
        CodeIntelLanguage::TypeScript => &["**/*.ts", "**/*.tsx"][..],
        CodeIntelLanguage::JavaScript => &["**/*.js", "**/*.jsx", "**/*.mjs", "**/*.cjs"][..],
        CodeIntelLanguage::Python => &["**/*.py", "**/*.pyi"][..],
        CodeIntelLanguage::Go => &["**/*.go"][..],
        CodeIntelLanguage::Java => &["**/*.java"][..],
        CodeIntelLanguage::C => &["**/*.c", "**/*.h"][..],
        CodeIntelLanguage::Cpp => &["**/*.cc", "**/*.cpp", "**/*.cxx", "**/*.hh", "**/*.hpp"][..],
        CodeIntelLanguage::CSharp => &["**/*.cs"][..],
        CodeIntelLanguage::Ruby => &["**/*.rb", "**/*.rake", "**/Gemfile"][..],
        CodeIntelLanguage::Php => &["**/*.php", "**/*.phtml"][..],
        CodeIntelLanguage::Yaml => &["**/*.yaml", "**/*.yml"][..],
        CodeIntelLanguage::Json => &["**/*.json", "**/*.jsonc"][..],
        CodeIntelLanguage::Shell => &["**/*.sh", "**/*.bash", "**/*.zsh", "**/*.ksh"][..],
    }
    .iter()
    .map(|pattern| (*pattern).to_owned())
    .collect()
}

const fn language_supports_symbols(language: CodeIntelLanguage) -> bool {
    matches!(
        language,
        CodeIntelLanguage::Rust
            | CodeIntelLanguage::TypeScript
            | CodeIntelLanguage::JavaScript
            | CodeIntelLanguage::Python
            | CodeIntelLanguage::Go
            | CodeIntelLanguage::Java
            | CodeIntelLanguage::C
            | CodeIntelLanguage::Cpp
            | CodeIntelLanguage::CSharp
            | CodeIntelLanguage::Ruby
            | CodeIntelLanguage::Php
            | CodeIntelLanguage::Shell
    )
}

const fn language_supports_references(language: CodeIntelLanguage) -> bool {
    language_supports_symbols(language)
}

const fn language_fallback(language: CodeIntelLanguage) -> &'static str {
    if language_supports_symbols(language) {
        "bounded_lexical_index"
    } else {
        LSP_REGISTRY_ONLY_FALLBACK
    }
}

const fn language_server_integration(language: CodeIntelLanguage) -> LanguageServerIntegration {
    match language {
        CodeIntelLanguage::Rust | CodeIntelLanguage::TypeScript | CodeIntelLanguage::Python => {
            LanguageServerIntegration::ExternalDiagnostics
        }
        _ => LanguageServerIntegration::RegistryOnly,
    }
}

const fn provider_binary_config_key(language: CodeIntelLanguage) -> Option<&'static str> {
    match language {
        CodeIntelLanguage::Rust => Some("rust_analyzer"),
        CodeIntelLanguage::TypeScript | CodeIntelLanguage::JavaScript => Some("typescript_server"),
        CodeIntelLanguage::Python => Some("pyright"),
        _ => None,
    }
}

/// Diagnostics supervisor facade for provider process lifecycle decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LspProcessManager {
    registry: LanguageServerRegistry,
}

impl LspProcessManager {
    fn from_config(config: &CodeIntelConfig) -> Self {
        Self { registry: LanguageServerRegistry::from_config(config) }
    }

    fn provider_statuses(
        &self,
        touched_languages: &BTreeSet<CodeIntelLanguage>,
    ) -> Vec<CodeIntelProviderStatus> {
        self.registry
            .descriptors()
            .into_iter()
            .map(|descriptor| provider_status_for_descriptor(descriptor, touched_languages))
            .collect()
    }

    fn disabled_provider_statuses(&self) -> Vec<CodeIntelProviderStatus> {
        self.registry
            .descriptors()
            .into_iter()
            .map(|descriptor| CodeIntelProviderStatus {
                provider: descriptor.provider,
                language: descriptor.language,
                status: "disabled".to_owned(),
                binary: descriptor.binary_label,
                reason_code: "code_intel.disabled".to_owned(),
                repair_hint:
                    "Set tool_call.code_intel.enabled=true to enable post-write diagnostics."
                        .to_owned(),
                managed_health_authority: None,
                managed_health_snapshot_authority: None,
            })
            .collect()
    }
}

/// Probes one registered language provider without starting a process or
/// opening a workspace. The result is limited to configuration and executable
/// discovery, preserving the observe-only rollout boundary.
pub(crate) fn probe_code_intel_provider(
    config: &CodeIntelConfig,
    language_id: &str,
) -> Option<CodeIntelProviderStatus> {
    let language_id = language_id.trim().to_ascii_lowercase();
    let language = CodeIntelLanguage::ALL
        .iter()
        .copied()
        .find(|language| language.as_str() == language_id.as_str())?;
    if !config.enabled {
        return LspProcessManager::from_config(config)
            .disabled_provider_statuses()
            .into_iter()
            .find(|status| status.language == language);
    }
    let touched_languages = [language].into_iter().collect::<BTreeSet<_>>();
    LspProcessManager::from_config(config)
        .provider_statuses(&touched_languages)
        .into_iter()
        .find(|status| status.language == language)
}

/// Result of resolving the workspace root used by diagnostics providers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct WorkspaceRootResolution {
    pub workspace_root: Option<PathBuf>,
    pub reason_codes: Vec<String>,
    pub redaction_level: String,
}

/// Workspace-root resolver that keeps diagnostics scoped to agent roots.
pub(crate) struct WorkspaceRootResolver<'a> {
    config: &'a CodeIntelConfig,
    workspace_roots: &'a [PathBuf],
}

impl<'a> WorkspaceRootResolver<'a> {
    fn new(config: &'a CodeIntelConfig, workspace_roots: &'a [PathBuf]) -> Self {
        Self { config, workspace_roots }
    }

    fn resolve(&self) -> WorkspaceRootResolution {
        let mut reason_codes = Vec::new();
        if let Some(configured) = self.config.workspace_root.as_ref() {
            if self.workspace_roots.is_empty()
                || self.workspace_roots.iter().any(|root| path_is_within_root(configured, root))
            {
                return WorkspaceRootResolution {
                    workspace_root: Some(configured.clone()),
                    reason_codes,
                    redaction_level:
                        crate::application::code_intel_runtime::CODE_INTEL_REDACTION_LEVEL
                            .to_owned(),
                };
            }
            reason_codes.push("code_intel.workspace_root_rejected".to_owned());
        }
        WorkspaceRootResolution {
            workspace_root: self.workspace_roots.first().cloned(),
            reason_codes,
            redaction_level: crate::application::code_intel_runtime::CODE_INTEL_REDACTION_LEVEL
                .to_owned(),
        }
    }
}

/// One line-shift rule for comparing diagnostics across a patch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RangeShift {
    pub path: String,
    pub start_line: u32,
    pub old_line_count: u32,
    pub new_line_count: u32,
}

/// Maps after-patch diagnostic positions back to pre-patch coordinates.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RangeShiftMapper {
    shifts_by_path: BTreeMap<String, Vec<RangeShift>>,
}

impl RangeShiftMapper {
    fn new(shifts: Vec<RangeShift>) -> Self {
        let mut shifts_by_path = BTreeMap::<String, Vec<RangeShift>>::new();
        for shift in shifts {
            shifts_by_path.entry(shift.path.clone()).or_default().push(shift);
        }
        for shifts in shifts_by_path.values_mut() {
            shifts.sort_by_key(|shift| shift.start_line);
        }
        Self { shifts_by_path }
    }

    fn identity_for_files(files: &BTreeSet<String>) -> Self {
        let shifts_by_path =
            files.iter().map(|path| (path.clone(), Vec::new())).collect::<BTreeMap<_, _>>();
        Self { shifts_by_path }
    }

    fn map_after_position(&self, path: &str, line: u32, column: u32) -> (u32, u32) {
        let Some(shifts) = self.shifts_by_path.get(path) else {
            return (line, column);
        };
        let mut mapped_line = i64::from(line);
        for shift in shifts {
            if line <= shift.start_line {
                continue;
            }
            mapped_line -= i64::from(shift.new_line_count) - i64::from(shift.old_line_count);
        }
        (u32::try_from(mapped_line.max(1)).unwrap_or(u32::MAX), column)
    }
}

/// Normalized diagnostic severity. Higher ranks are worse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticSeverity {
    Hint,
    Info,
    Warning,
    Error,
}

impl DiagnosticSeverity {
    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "error" | "err" => Self::Error,
            "2" | "warning" | "warn" => Self::Warning,
            "3" | "information" | "info" => Self::Info,
            _ => Self::Hint,
        }
    }
}

/// One workspace-relative diagnostic item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CodeDiagnostic {
    pub language: CodeIntelLanguage,
    pub path: String,
    pub line: u32,
    pub column: u32,
    pub severity: DiagnosticSeverity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    pub message: String,
    pub source: String,
}

/// Read-only provider status emitted even when diagnostics are degraded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CodeIntelProviderStatus {
    pub provider: String,
    pub language: CodeIntelLanguage,
    pub status: String,
    pub binary: String,
    pub reason_code: String,
    pub repair_hint: String,
    #[serde(skip)]
    managed_health_authority: Option<ManagedRuntimeHealthAuthority>,
    #[serde(skip)]
    managed_health_snapshot_authority: Option<CodeIntelProviderSnapshotAuthority>,
}

/// Diagnostics captured at one point in time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DiagnosticSnapshot {
    pub schema_version: u32,
    pub enabled: bool,
    pub workspace_root: Option<String>,
    pub files: Vec<String>,
    pub provider_status: Vec<CodeIntelProviderStatus>,
    pub items: Vec<CodeDiagnostic>,
    pub truncated: bool,
    pub degraded: bool,
    pub reason_codes: Vec<String>,
}

/// Before/after delta returned in a successful patch output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DiagnosticDelta {
    pub schema_version: u32,
    pub enabled: bool,
    pub new_errors: usize,
    pub new_warnings: usize,
    pub items: Vec<CodeDiagnostic>,
    pub truncated: bool,
    pub provider_status: Vec<CodeIntelProviderStatus>,
    pub degraded: bool,
    pub reason_codes: Vec<String>,
}

/// Captures a diagnostics snapshot for touched files. Missing providers
/// degrade the snapshot instead of failing the caller's mutation.
#[must_use]
pub(crate) fn capture_diagnostic_snapshot(
    config: &CodeIntelConfig,
    workspace_roots: &[PathBuf],
    files_touched: &[WorkspacePatchFileAttestation],
) -> DiagnosticSnapshot {
    let process_manager = LspProcessManager::from_config(config);
    if !config.enabled {
        return DiagnosticSnapshot {
            schema_version: CODE_INTEL_SCHEMA_VERSION,
            enabled: false,
            workspace_root: None,
            files: Vec::new(),
            provider_status: process_manager.disabled_provider_statuses(),
            items: Vec::new(),
            truncated: false,
            degraded: false,
            reason_codes: vec!["code_intel.disabled".to_owned()],
        };
    }

    let workspace_resolution = WorkspaceRootResolver::new(config, workspace_roots).resolve();
    let workspace_root = workspace_resolution.workspace_root;
    let mut reason_codes = workspace_resolution.reason_codes;
    let mut files =
        normalize_touched_files(files_touched, workspace_root.as_deref(), &mut reason_codes);
    files.sort();
    files.dedup();

    let languages =
        files.iter().filter_map(|path| CodeIntelLanguage::from_path(path)).collect::<BTreeSet<_>>();

    let provider_status = process_manager.provider_statuses(&languages);
    reason_codes.extend(
        provider_status
            .iter()
            .filter(|status| status.status != "ready" && status.status != "skipped")
            .map(|status| status.reason_code.clone()),
    );
    reason_codes.sort();
    reason_codes.dedup();

    DiagnosticSnapshot {
        schema_version: CODE_INTEL_SCHEMA_VERSION,
        enabled: true,
        workspace_root: workspace_root.as_ref().map(|path| normalize_path_for_output(path)),
        files,
        provider_status,
        items: Vec::new(),
        truncated: false,
        degraded: !reason_codes.is_empty(),
        reason_codes,
    }
}

/// Captures diagnostics and invokes enabled language providers behind the
/// conservative code-intelligence rollout flag.
#[cfg(test)]
pub(crate) async fn capture_diagnostic_snapshot_with_providers(
    config: &CodeIntelConfig,
    workspace_roots: &[PathBuf],
    files_touched: &[WorkspacePatchFileAttestation],
) -> DiagnosticSnapshot {
    capture_diagnostic_snapshot_with_health_blocks(
        config,
        workspace_roots,
        files_touched,
        &BTreeSet::new(),
    )
    .await
}

/// Captures diagnostics under exact shared-health authority for each touched
/// language. Blocked providers remain metadata-only degraded observations,
/// and late results are suppressed by the captured generation.
pub(crate) async fn capture_diagnostic_snapshot_with_managed_health(
    runtime_state: &Arc<GatewayRuntimeState>,
    config: &CodeIntelConfig,
    workspace_roots: &[PathBuf],
    files_touched: &[WorkspacePatchFileAttestation],
) -> DiagnosticSnapshot {
    let touched_languages = files_touched
        .iter()
        .filter_map(|file| CodeIntelLanguage::from_path(file.path.as_str()))
        .collect::<BTreeSet<_>>();
    let mut authorities = BTreeMap::<CodeIntelLanguage, ManagedRuntimeHealthAuthority>::new();
    let mut blocked_languages = BTreeSet::new();
    for language in touched_languages {
        match runtime_state
            .admit_managed_runtime_health(ManagedRuntimeHealthFamily::Lsp, language.as_str())
        {
            Ok(authority) => {
                authorities.insert(language, authority);
            }
            Err(_) => {
                blocked_languages.insert(language);
            }
        }
    }
    let mut snapshot = capture_diagnostic_snapshot_with_health_blocks(
        config,
        workspace_roots,
        files_touched,
        &blocked_languages,
    )
    .await;
    for (language, authority) in authorities {
        let Some(status) =
            snapshot.provider_status.iter_mut().find(|status| status.language == language)
        else {
            continue;
        };
        let succeeded = {
            status.status == "ready"
                || status.status == "skipped"
                || status.reason_code.starts_with("code_intel.provider_registry_only.")
        };
        let observation_applied = runtime_state.record_managed_runtime_health_observation(
            &authority,
            succeeded,
            if succeeded {
                "runtime.health.lsp_observation_succeeded"
            } else {
                "runtime.health.lsp_observation_failed"
            },
        );
        status.managed_health_authority = Some(authority);
        status.managed_health_snapshot_authority = Some(if observation_applied {
            CodeIntelProviderSnapshotAuthority::Authoritative
        } else {
            CodeIntelProviderSnapshotAuthority::Stale
        });
    }
    snapshot
}

async fn capture_diagnostic_snapshot_with_health_blocks(
    config: &CodeIntelConfig,
    workspace_roots: &[PathBuf],
    files_touched: &[WorkspacePatchFileAttestation],
    blocked_languages: &BTreeSet<CodeIntelLanguage>,
) -> DiagnosticSnapshot {
    let mut snapshot = capture_diagnostic_snapshot(config, workspace_roots, files_touched);
    if !snapshot.enabled {
        return snapshot;
    }
    for language in blocked_languages {
        mark_provider_degraded(
            &mut snapshot,
            *language,
            "runtime.health.lsp_admission_blocked",
            "Shared runtime health blocked this exact language-provider generation.",
        );
    }
    let workspace_root = configured_workspace_root(config, workspace_roots);
    let rust_files = snapshot
        .files
        .iter()
        .filter(|path| CodeIntelLanguage::from_path(path) == Some(CodeIntelLanguage::Rust))
        .cloned()
        .collect::<BTreeSet<_>>();
    if !rust_files.is_empty() && provider_ready(&snapshot, CodeIntelLanguage::Rust) {
        if let Some(workspace_root) = workspace_root.as_deref() {
            let provider = RustAnalyzerProvider::from_config(config);
            match provider.capture(workspace_root, &rust_files).await {
                RustAnalyzerCaptureOutcome::Captured { items, truncated, reason_codes } => {
                    snapshot.items.extend(items);
                    snapshot.truncated |= truncated;
                    snapshot.degraded |= truncated;
                    snapshot.reason_codes.extend(reason_codes);
                    set_provider_status(
                        &mut snapshot,
                        CodeIntelLanguage::Rust,
                        "ready",
                        CODE_INTEL_RUST_SNAPSHOT_CAPTURED_EVENT,
                        "Rust diagnostics snapshot captured through the rust-analyzer check pipeline.",
                    );
                }
                RustAnalyzerCaptureOutcome::Degraded { reason_code, repair_hint } => {
                    mark_provider_degraded(
                        &mut snapshot,
                        CodeIntelLanguage::Rust,
                        reason_code.as_str(),
                        repair_hint.as_str(),
                    );
                }
            }
        } else {
            mark_provider_degraded(
                &mut snapshot,
                CodeIntelLanguage::Rust,
                "code_intel.workspace_root_missing",
                "No workspace root was available for Rust diagnostics.",
            );
        }
    }

    let typescript_files = snapshot
        .files
        .iter()
        .filter(|path| CodeIntelLanguage::from_path(path) == Some(CodeIntelLanguage::TypeScript))
        .cloned()
        .collect::<BTreeSet<_>>();
    if !typescript_files.is_empty() && provider_ready(&snapshot, CodeIntelLanguage::TypeScript) {
        if let Some(workspace_root) = workspace_root.as_deref() {
            let provider = TypescriptLanguageServerProvider::from_config(config);
            match provider.capture(workspace_root, &typescript_files).await {
                TypescriptCaptureOutcome::Captured { items, truncated, reason_codes } => {
                    snapshot.items.extend(items);
                    snapshot.truncated |= truncated;
                    snapshot.degraded |= truncated;
                    snapshot.reason_codes.extend(reason_codes);
                    set_provider_status(
                        &mut snapshot,
                        CodeIntelLanguage::TypeScript,
                        "ready",
                        CODE_INTEL_TYPESCRIPT_SNAPSHOT_CAPTURED_EVENT,
                        "TypeScript diagnostics snapshot captured through the language-server compiler diagnostics pipeline.",
                    );
                }
                TypescriptCaptureOutcome::Degraded { reason_code, repair_hint } => {
                    mark_provider_degraded(
                        &mut snapshot,
                        CodeIntelLanguage::TypeScript,
                        reason_code.as_str(),
                        repair_hint.as_str(),
                    );
                }
            }
        } else {
            mark_provider_degraded(
                &mut snapshot,
                CodeIntelLanguage::TypeScript,
                "code_intel.workspace_root_missing",
                "No workspace root was available for TypeScript diagnostics.",
            );
        }
    }

    let python_files = snapshot
        .files
        .iter()
        .filter(|path| CodeIntelLanguage::from_path(path) == Some(CodeIntelLanguage::Python))
        .cloned()
        .collect::<BTreeSet<_>>();
    if !python_files.is_empty() && provider_ready(&snapshot, CodeIntelLanguage::Python) {
        if let Some(workspace_root) = workspace_root.as_deref() {
            let provider = PyrightProvider::from_config(config);
            match provider.capture(workspace_root, &python_files).await {
                PyrightCaptureOutcome::Captured { items, truncated, reason_codes } => {
                    snapshot.items.extend(items);
                    snapshot.truncated |= truncated;
                    snapshot.degraded |= truncated;
                    snapshot.reason_codes.extend(reason_codes);
                    set_provider_status(
                        &mut snapshot,
                        CodeIntelLanguage::Python,
                        "ready",
                        CODE_INTEL_PYTHON_SNAPSHOT_CAPTURED_EVENT,
                        "Python diagnostics snapshot captured through the pyright diagnostics pipeline.",
                    );
                }
                PyrightCaptureOutcome::Degraded { reason_code, repair_hint } => {
                    mark_provider_degraded(
                        &mut snapshot,
                        CodeIntelLanguage::Python,
                        reason_code.as_str(),
                        repair_hint.as_str(),
                    );
                }
            }
        } else {
            mark_provider_degraded(
                &mut snapshot,
                CodeIntelLanguage::Python,
                "code_intel.workspace_root_missing",
                "No workspace root was available for Python diagnostics.",
            );
        }
    }
    finish_diagnostic_snapshot(snapshot)
}

fn finish_diagnostic_snapshot(mut snapshot: DiagnosticSnapshot) -> DiagnosticSnapshot {
    snapshot.reason_codes.sort();
    snapshot.reason_codes.dedup();
    snapshot.items.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then(left.line.cmp(&right.line))
            .then(left.column.cmp(&right.column))
            .then(left.source.cmp(&right.source))
            .then(left.message.cmp(&right.message))
    });
    snapshot
}

/// Computes diagnostics that are new or worse in `after` for touched files.
#[must_use]
pub(crate) fn diagnostic_delta(
    config: &CodeIntelConfig,
    before: &DiagnosticSnapshot,
    after: &DiagnosticSnapshot,
) -> DiagnosticDelta {
    diagnostic_delta_with_range_shifts(config, before, after, Vec::new())
}

/// Computes diagnostics delta while mapping after-patch line positions back to baseline lines.
#[must_use]
pub(crate) fn diagnostic_delta_with_range_shifts(
    config: &CodeIntelConfig,
    before: &DiagnosticSnapshot,
    after: &DiagnosticSnapshot,
    range_shifts: Vec<RangeShift>,
) -> DiagnosticDelta {
    let range_shift_mapper = if range_shifts.is_empty() {
        let touched = after.files.iter().cloned().collect::<BTreeSet<_>>();
        RangeShiftMapper::identity_for_files(&touched)
    } else {
        RangeShiftMapper::new(range_shifts)
    };
    diagnostic_delta_with_mapper(config, before, after, &range_shift_mapper)
}

fn diagnostic_delta_with_mapper(
    config: &CodeIntelConfig,
    before: &DiagnosticSnapshot,
    after: &DiagnosticSnapshot,
    range_shift_mapper: &RangeShiftMapper,
) -> DiagnosticDelta {
    if !after.enabled {
        return DiagnosticDelta {
            schema_version: CODE_INTEL_SCHEMA_VERSION,
            enabled: false,
            new_errors: 0,
            new_warnings: 0,
            items: Vec::new(),
            truncated: false,
            provider_status: after.provider_status.clone(),
            degraded: after.degraded,
            reason_codes: after.reason_codes.clone(),
        };
    }

    let touched = after.files.iter().cloned().collect::<BTreeSet<_>>();
    let before_severity_by_key = before
        .items
        .iter()
        .map(|item| (diagnostic_key_without_severity(item), item.severity))
        .collect::<BTreeMap<_, _>>();

    let mut items = Vec::new();
    let mut truncated = after.truncated;
    for item in &after.items {
        if !touched.contains(item.path.as_str()) {
            continue;
        }
        let previous = before_severity_by_key
            .get(&diagnostic_key_without_severity_with_mapper(item, range_shift_mapper));
        if previous.is_none_or(|severity| item.severity > *severity) {
            if items.len() >= config.max_items {
                truncated = true;
                break;
            }
            items.push(item.clone());
        }
    }
    items.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then(left.line.cmp(&right.line))
            .then(left.column.cmp(&right.column))
            .then(left.source.cmp(&right.source))
            .then(left.message.cmp(&right.message))
    });

    let new_errors = items.iter().filter(|item| item.severity == DiagnosticSeverity::Error).count();
    let new_warnings =
        items.iter().filter(|item| item.severity == DiagnosticSeverity::Warning).count();
    let mut reason_codes = before.reason_codes.clone();
    reason_codes.extend(after.reason_codes.iter().cloned());
    reason_codes.sort();
    reason_codes.dedup();

    DiagnosticDelta {
        schema_version: CODE_INTEL_SCHEMA_VERSION,
        enabled: true,
        new_errors,
        new_warnings,
        items,
        truncated,
        provider_status: after.provider_status.clone(),
        degraded: before.degraded || after.degraded || truncated,
        reason_codes,
    }
}

/// Inserts the stable diagnostics block into a successful tool output.
pub(crate) fn append_diagnostics_output(output_value: &mut Value, delta: DiagnosticDelta) {
    let Some(payload) = output_value.as_object_mut() else {
        return;
    };
    let diagnostics = serde_json::to_value(delta).unwrap_or_else(|error| {
        serde_json::json!({
            "schema_version": CODE_INTEL_SCHEMA_VERSION,
            "enabled": false,
            "new_errors": 0,
            "new_warnings": 0,
            "items": [],
            "truncated": false,
            "provider_status": [],
            "degraded": true,
            "reason_codes": ["code_intel.serialize_failed"],
            "error": error.to_string(),
        })
    });
    payload.insert("diagnostics".to_owned(), diagnostics);
}

/// Converts provider statuses from a diagnostics snapshot into runtime
/// supervisor observations.
pub(crate) fn provider_runtime_observations(
    snapshot: &DiagnosticSnapshot,
) -> Vec<CodeIntelProviderObservation> {
    snapshot
        .provider_status
        .iter()
        .map(|status| {
            let mut observation = CodeIntelProviderObservation::from_status_fields(
                status.provider.as_str(),
                status.language,
                status.status.as_str(),
                status.binary.as_str(),
                status.reason_code.as_str(),
                status.repair_hint.as_str(),
            );
            if let Some(authority) = status.managed_health_authority.as_ref() {
                observation = observation
                    .with_runtime_authority(authority.component_id.clone(), authority.generation);
            }
            if let Some(snapshot_authority) = status.managed_health_snapshot_authority {
                observation = observation.with_snapshot_authority(snapshot_authority);
            }
            observation
        })
        .collect()
}

/// Returns capture-time authority classifications for managed provider data.
#[must_use]
pub(crate) fn provider_snapshot_authority(
    snapshot: &DiagnosticSnapshot,
) -> BTreeMap<CodeIntelLanguage, CodeIntelProviderSnapshotAuthority> {
    snapshot
        .provider_status
        .iter()
        .filter_map(|status| {
            status.managed_health_snapshot_authority.map(|authority| (status.language, authority))
        })
        .collect()
}

/// Merges provider classifications while preserving stale-wins semantics.
pub(crate) fn merge_provider_snapshot_authority(
    target: &mut BTreeMap<CodeIntelLanguage, CodeIntelProviderSnapshotAuthority>,
    source: &BTreeMap<CodeIntelLanguage, CodeIntelProviderSnapshotAuthority>,
) {
    for (language, authority) in source {
        target
            .entry(*language)
            .and_modify(|current| {
                if *authority == CodeIntelProviderSnapshotAuthority::Stale {
                    *current = CodeIntelProviderSnapshotAuthority::Stale;
                }
            })
            .or_insert(*authority);
    }
}

/// Removes stale provider data before a diagnostics snapshot is serialized or compared.
#[must_use]
pub(crate) fn project_authoritative_diagnostic_snapshot(
    mut snapshot: DiagnosticSnapshot,
    authority_by_language: &BTreeMap<CodeIntelLanguage, CodeIntelProviderSnapshotAuthority>,
) -> DiagnosticSnapshot {
    let stale_languages = authority_by_language
        .iter()
        .filter_map(|(language, authority)| {
            (*authority == CodeIntelProviderSnapshotAuthority::Stale).then_some(*language)
        })
        .collect::<BTreeSet<_>>();
    if stale_languages.is_empty() {
        return snapshot;
    }

    snapshot.provider_status.retain(|status| !stale_languages.contains(&status.language));
    snapshot.items.retain(|item| !stale_languages.contains(&item.language));
    snapshot.reason_codes.retain(|reason_code| {
        !stale_languages
            .iter()
            .any(|language| reason_code_belongs_to_language(reason_code, *language))
    });
    snapshot.truncated =
        snapshot.reason_codes.iter().any(|reason_code| reason_code.ends_with(".output_truncated"));
    snapshot.degraded = snapshot.truncated
        || snapshot
            .provider_status
            .iter()
            .any(|status| status.status != "ready" && status.status != "skipped")
        || snapshot
            .reason_codes
            .iter()
            .any(|reason_code| !is_provider_snapshot_success_reason(reason_code));
    snapshot
}

/// Applies one stale-language mask to both sides of a workspace-patch comparison.
#[must_use]
pub(crate) fn project_workspace_patch_diagnostic_pair(
    before: DiagnosticSnapshot,
    after: DiagnosticSnapshot,
    authority_by_language: &BTreeMap<CodeIntelLanguage, CodeIntelProviderSnapshotAuthority>,
) -> (DiagnosticSnapshot, DiagnosticSnapshot) {
    (
        project_authoritative_diagnostic_snapshot(before, authority_by_language),
        project_authoritative_diagnostic_snapshot(after, authority_by_language),
    )
}

fn reason_code_belongs_to_language(reason_code: &str, language: CodeIntelLanguage) -> bool {
    let language = language.as_str();
    let language_prefix = format!("code_intel.{language}.");
    let language_suffix = format!(".{language}");
    reason_code.starts_with(language_prefix.as_str())
        || reason_code.ends_with(language_suffix.as_str())
}

fn is_provider_snapshot_success_reason(reason_code: &str) -> bool {
    matches!(
        reason_code,
        CODE_INTEL_RUST_SNAPSHOT_CAPTURED_EVENT
            | CODE_INTEL_TYPESCRIPT_SNAPSHOT_CAPTURED_EVENT
            | CODE_INTEL_PYTHON_SNAPSHOT_CAPTURED_EVENT
    )
}

/// Inserts code-intelligence runtime lifecycle details into the diagnostics
/// output block.
pub(crate) fn append_runtime_output(
    output_value: &mut Value,
    runtime_snapshot: &CodeIntelRuntimeSnapshot,
) {
    let Some(payload) = output_value.as_object_mut() else {
        return;
    };
    let runtime_value = serde_json::to_value(runtime_snapshot).unwrap_or_else(|error| {
        serde_json::json!({
            "schema_version": crate::application::code_intel_runtime::CODE_INTEL_RUNTIME_SCHEMA_VERSION,
            "enabled": false,
            "mode": "disabled",
            "status": "degraded",
            "clients": [],
            "broken_server_cache": [],
            "reason_codes": ["code_intel.runtime_serialize_failed"],
            "error": error.to_string(),
            "redaction_level": crate::application::code_intel_runtime::CODE_INTEL_REDACTION_LEVEL,
        })
    });
    if let Some(diagnostics) = payload.get_mut("diagnostics").and_then(Value::as_object_mut) {
        diagnostics.insert("runtime".to_owned(), runtime_value);
    }
}

/// Executes one read-only model-facing code-intelligence tool.
pub(crate) async fn execute_code_intel_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let input = match parse_code_intel_tool_input(tool_name, input_json) {
        Ok(input) => input,
        Err(error) => {
            return code_intel_tool_outcome(
                proposal_id,
                tool_name,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };
    let workspace = match resolve_code_intel_workspace(
        runtime_state,
        context,
        tool_name,
        input.workspace_root.as_deref(),
        input.path.as_deref().unwrap_or_default(),
    )
    .await
    {
        Ok(workspace) => workspace,
        Err(error) => {
            return code_intel_tool_outcome(
                proposal_id,
                tool_name,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };

    let result = match tool_name {
        CODE_HEALTH_TOOL_NAME => code_intel_health_output(runtime_state, &workspace),
        CODE_DIAGNOSTICS_TOOL_NAME => {
            code_intel_diagnostics_output(runtime_state, &workspace, &input)
        }
        CODE_SYMBOLS_TOOL_NAME | CODE_OUTLINE_TOOL_NAME => {
            code_intel_symbols_output(tool_name, &workspace, &input)
        }
        CODE_DEFINITION_TOOL_NAME => code_intel_definition_output(&workspace, &input),
        CODE_REFERENCES_TOOL_NAME => code_intel_references_output(&workspace, &input),
        CODE_HOVER_TOOL_NAME => code_intel_hover_output(&workspace, &input),
        CODE_WORKSPACE_SYMBOLS_TOOL_NAME => code_intel_workspace_symbols_output(&workspace, &input),
        _ => Err(format!("{tool_name} is not a code-intelligence tool")),
    };

    match result.and_then(|value| {
        serde_json::to_vec(&value)
            .map_err(|error| format!("{tool_name} failed to serialize output: {error}"))
    }) {
        Ok(output_json) => code_intel_tool_outcome(
            proposal_id,
            tool_name,
            input_json,
            true,
            output_json,
            String::new(),
        ),
        Err(error) => code_intel_tool_outcome(
            proposal_id,
            tool_name,
            input_json,
            false,
            b"{}".to_vec(),
            error,
        ),
    }
}

#[derive(Debug, Clone, Deserialize)]
struct CodeIntelToolInput {
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    workspace_root: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    line: Option<u32>,
    #[serde(default)]
    column: Option<u32>,
    #[serde(default)]
    max_results: Option<usize>,
}

#[derive(Debug, Clone)]
struct CodeIntelWorkspace {
    roots: Vec<(usize, PathBuf)>,
    primary_root_index: usize,
    primary_root: PathBuf,
    display_root: String,
    provider_status: Vec<CodeIntelProviderStatus>,
    runtime_cwd_hints: Vec<RuntimeCwdHint>,
}

#[derive(Debug, Clone)]
struct ResolvedCodeIntelFile {
    workspace_root_index: usize,
    canonical_path: PathBuf,
    display_path: String,
    language: Option<CodeIntelLanguage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CodeSymbol {
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) language: CodeIntelLanguage,
    pub(crate) path: String,
    pub(crate) line: u32,
    pub(crate) column: u32,
    pub(crate) signature: String,
    pub(crate) visibility: String,
    pub(crate) source_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeCwdHint {
    pub(crate) cwd: String,
    pub(crate) manifest_path: String,
    pub(crate) project_kind: String,
    pub(crate) confidence: String,
    pub(crate) reason_code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CodeSemanticContext {
    pub(crate) schema_version: u32,
    pub(crate) provider: String,
    pub(crate) source: String,
    pub(crate) symbols: Vec<CodeSymbol>,
    pub(crate) source_refs: Vec<String>,
    pub(crate) truncated: bool,
    pub(crate) cache_policy: String,
    pub(crate) reason_codes: Vec<String>,
}

/// Builds bounded symbol context for prompt assembly or tool outputs.
pub(crate) struct CodeSemanticContextProvider;

impl CodeSemanticContextProvider {
    #[must_use]
    pub(crate) fn from_symbols(symbols: &[CodeSymbol], truncated: bool) -> CodeSemanticContext {
        let mut selected =
            symbols.iter().take(CODE_INTEL_CONTEXT_SYMBOL_LIMIT).cloned().collect::<Vec<_>>();
        selected.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then(left.line.cmp(&right.line))
                .then(left.name.cmp(&right.name))
        });
        let source_refs = selected.iter().map(|symbol| symbol.source_ref.clone()).collect();
        CodeSemanticContext {
            schema_version: CODE_INTEL_SCHEMA_VERSION,
            provider: "palyra.code.semantic_context".to_owned(),
            source: "bounded_lexical_index".to_owned(),
            symbols: selected,
            source_refs,
            truncated: truncated || symbols.len() > CODE_INTEL_CONTEXT_SYMBOL_LIMIT,
            cache_policy: "volatile_workspace_snapshot".to_owned(),
            reason_codes: vec!["code_intel.semantic_context.lexical_fallback".to_owned()],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PatchImpactAnalysis {
    pub(crate) schema_version: u32,
    pub(crate) files: Vec<String>,
    pub(crate) languages: Vec<CodeIntelLanguage>,
    pub(crate) touched_symbols: Vec<CodeSymbol>,
    pub(crate) diagnostics_before_count: usize,
    pub(crate) diagnostics_after_count: usize,
    pub(crate) new_errors: usize,
    pub(crate) new_warnings: usize,
    pub(crate) risk_level: String,
    pub(crate) verification_guidance: Vec<String>,
    pub(crate) runtime_cwd_hints: Vec<RuntimeCwdHint>,
    pub(crate) truncated: bool,
    pub(crate) reason_codes: Vec<String>,
}

/// Inserts semantic patch-impact evidence into a successful apply-patch output.
pub(crate) fn append_patch_impact_output(
    output_value: &mut Value,
    workspace_roots: &[PathBuf],
    files_touched: &[WorkspacePatchFileAttestation],
    diagnostic_before: &DiagnosticSnapshot,
    diagnostic_after: &DiagnosticSnapshot,
    diagnostic_delta: &DiagnosticDelta,
) {
    let Some(payload) = output_value.as_object_mut() else {
        return;
    };
    let impact = patch_impact_analysis(
        workspace_roots,
        files_touched,
        diagnostic_before,
        diagnostic_after,
        diagnostic_delta,
    );
    let value = serde_json::to_value(impact).unwrap_or_else(|error| {
        json!({
            "schema_version": CODE_INTEL_SCHEMA_VERSION,
            "files": [],
            "languages": [],
            "touched_symbols": [],
            "diagnostics_before_count": 0,
            "diagnostics_after_count": 0,
            "new_errors": 0,
            "new_warnings": 0,
            "risk_level": "unknown",
            "verification_guidance": ["Inspect diagnostics manually because patch impact serialization failed."],
            "runtime_cwd_hints": [],
            "truncated": false,
            "reason_codes": ["code_intel.patch_impact.serialize_failed"],
            "error": error.to_string(),
        })
    });
    payload.insert("patch_impact_analysis".to_owned(), value);
}

fn parse_code_intel_tool_input(
    tool_name: &str,
    input_json: &[u8],
) -> Result<CodeIntelToolInput, String> {
    if input_json.len() > CODE_INTEL_TOOL_INPUT_MAX_BYTES {
        return Err(format!("{tool_name} input exceeds {CODE_INTEL_TOOL_INPUT_MAX_BYTES} bytes"));
    }
    let mut input = if input_json.trim_ascii().is_empty() {
        CodeIntelToolInput {
            path: None,
            workspace_root: None,
            symbol: None,
            query: None,
            line: None,
            column: None,
            max_results: None,
        }
    } else {
        serde_json::from_slice::<CodeIntelToolInput>(input_json).map_err(|error| {
            format!("{tool_name} input must match code-intelligence schema: {error}")
        })?
    };
    input.path = input
        .path
        .map(|path| normalize_code_intel_path_input(path.as_str()))
        .filter(|path| !path.is_empty());
    input.workspace_root = input
        .workspace_root
        .map(|path| normalize_code_intel_path_input(path.as_str()))
        .filter(|path| !path.is_empty());
    input.symbol =
        input.symbol.map(|symbol| symbol.trim().to_owned()).filter(|symbol| !symbol.is_empty());
    input.query =
        input.query.map(|query| query.trim().to_owned()).filter(|query| !query.is_empty());
    if let Some(path) = input.path.as_deref() {
        validate_code_intel_path_syntax(path, tool_name)?;
    }
    if let Some(root) = input.workspace_root.as_deref() {
        validate_code_intel_path_syntax(root, tool_name)?;
    }
    if matches!(tool_name, CODE_SYMBOLS_TOOL_NAME | CODE_OUTLINE_TOOL_NAME | CODE_HOVER_TOOL_NAME)
        && input.path.is_none()
    {
        return Err(format!("{tool_name} requires non-empty string field 'path'"));
    }
    if matches!(
        tool_name,
        CODE_DEFINITION_TOOL_NAME | CODE_REFERENCES_TOOL_NAME | CODE_HOVER_TOOL_NAME
    ) && input.symbol.is_none()
        && (input.line.is_none() || input.column.is_none())
    {
        return Err(format!("{tool_name} requires 'symbol' or both 'line' and 'column'"));
    }
    if tool_name == CODE_WORKSPACE_SYMBOLS_TOOL_NAME && input.query.is_none() {
        return Err(format!("{tool_name} requires non-empty string field 'query'"));
    }
    Ok(input)
}

async fn resolve_code_intel_workspace(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    tool_name: &str,
    workspace_root: Option<&str>,
    requested_path: &str,
) -> Result<CodeIntelWorkspace, String> {
    let agent_outcome = runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: context.principal.to_owned(),
            channel: context.channel.map(str::to_owned),
            session_id: Some(context.session_id.to_owned()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await
        .map_err(|error| {
            format!("{tool_name} failed to resolve agent workspace: {}", error.message())
        })?;
    let agent_roots =
        agent_outcome.agent.workspace_roots.iter().map(PathBuf::from).collect::<Vec<_>>();
    let agent_roots = workspace_roots_with_run_launch_context_for_agent_source(
        runtime_state,
        context.run_id,
        agent_roots.as_slice(),
        agent_outcome.source,
    )
    .await;
    let roots = resolve_code_intel_roots(
        runtime_state,
        context.session_id,
        tool_name,
        agent_roots.as_slice(),
        workspace_root,
        requested_path,
    )
    .await?;
    let canonical_roots = canonicalize_code_intel_roots(roots.as_slice(), tool_name)?;
    let Some((primary_root_index, primary_root)) = canonical_roots.first().cloned() else {
        return Err(format!("{tool_name} agent has no accessible workspace roots"));
    };
    let provider_status =
        code_intel_provider_health(&runtime_state.config.code_intel, roots.as_slice());
    let runtime_cwd_hints = detect_runtime_cwd_hints(canonical_roots.as_slice());
    Ok(CodeIntelWorkspace {
        roots: canonical_roots,
        primary_root_index,
        display_root: format!("workspace_root:{primary_root_index}"),
        primary_root,
        provider_status,
        runtime_cwd_hints,
    })
}

async fn resolve_code_intel_roots(
    runtime_state: &Arc<GatewayRuntimeState>,
    session_id: &str,
    tool_name: &str,
    agent_roots: &[PathBuf],
    workspace_root: Option<&str>,
    requested_path: &str,
) -> Result<Vec<PathBuf>, String> {
    if let Some(workspace_root) = workspace_root.map(str::trim).filter(|root| !root.is_empty()) {
        if let Some(active_root) =
            session_active_workspace_root(runtime_state, session_id, agent_roots).await?
        {
            if workspace_root_override_targets_active_root(workspace_root, &active_root) {
                return Ok(vec![active_root.root]);
            }
        }
        return resolve_code_intel_root_override(tool_name, agent_roots, workspace_root)
            .map(|root| vec![root]);
    }
    if let Some(active_root) =
        session_active_workspace_root(runtime_state, session_id, agent_roots).await?
    {
        if relative_path_should_use_active_root(requested_path, &active_root) {
            return Ok(code_intel_roots_with_active_first(active_root.root, agent_roots));
        }
    }
    Ok(agent_roots.to_vec())
}

fn code_intel_health_output(
    runtime_state: &Arc<GatewayRuntimeState>,
    workspace: &CodeIntelWorkspace,
) -> Result<Value, String> {
    let runtime = runtime_state.code_intel_runtime_snapshot();
    let descriptors = LanguageServerRegistry::from_config(&runtime_state.config.code_intel)
        .descriptors()
        .into_iter()
        .map(|descriptor| {
            json!({
                "language": descriptor.language,
                "provider": descriptor.provider,
                "binary_label": descriptor.binary_label,
                "file_patterns": descriptor.file_patterns,
                "diagnostics_only": descriptor.diagnostics_only,
                "supports_symbols": descriptor.supports_symbols,
                "supports_references": descriptor.supports_references,
                "fallback": descriptor.fallback,
                "timeout_ms": descriptor.timeout_ms,
                "idle_reap_ms": descriptor.idle_reap_ms,
                "redaction_level": descriptor.redaction_level,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema_version": CODE_INTEL_SCHEMA_VERSION,
        "enabled": runtime_state.config.code_intel.enabled,
        "workspace_root": workspace.display_root.as_str(),
        "workspace_root_index": workspace.primary_root_index,
        "status": runtime.status,
        "mode": runtime.mode,
        "runtime": runtime,
        "provider_status": &workspace.provider_status,
        "descriptors": descriptors,
        "capabilities": {
            "diagnostics": true,
            "symbols": true,
            "definition": true,
            "references": true,
            "hover": true,
            "outline": true,
            "workspace_symbols": true,
            "patch_impact": true,
            "runtime_cwd_hints": true
        },
        "runtime_cwd_hints": &workspace.runtime_cwd_hints,
        "redaction_level": CODE_INTEL_REDACTION_LEVEL,
    }))
}

fn code_intel_diagnostics_output(
    runtime_state: &Arc<GatewayRuntimeState>,
    workspace: &CodeIntelWorkspace,
    input: &CodeIntelToolInput,
) -> Result<Value, String> {
    let touched_files = if let Some(path) = input.path.as_deref() {
        let file = resolve_code_intel_file(workspace, path, CODE_DIAGNOSTICS_TOOL_NAME)?;
        vec![WorkspacePatchFileAttestation {
            path: file.display_path,
            workspace_root_index: file.workspace_root_index,
            operation: "inspect".to_owned(),
            moved_from: None,
            before_sha256: None,
            before_size_bytes: None,
            after_sha256: None,
            after_size_bytes: None,
        }]
    } else {
        Vec::new()
    };
    let snapshot = capture_read_only_diagnostic_snapshot(
        &runtime_state.config.code_intel,
        std::slice::from_ref(&workspace.primary_root),
        touched_files.as_slice(),
    );
    Ok(project_code_intel_diagnostics_output(
        snapshot,
        runtime_state.code_intel_runtime_snapshot(),
        &BTreeMap::new(),
        &workspace.runtime_cwd_hints,
    ))
}

fn capture_read_only_diagnostic_snapshot(
    config: &CodeIntelConfig,
    workspace_roots: &[PathBuf],
    files_touched: &[WorkspacePatchFileAttestation],
) -> DiagnosticSnapshot {
    let mut snapshot = capture_diagnostic_snapshot(config, workspace_roots, files_touched);
    if !snapshot.enabled || snapshot.files.is_empty() {
        return snapshot;
    }

    let touched_languages = snapshot
        .files
        .iter()
        .filter_map(|path| CodeIntelLanguage::from_path(path))
        .collect::<BTreeSet<_>>();
    for language in touched_languages {
        if provider_ready(&snapshot, language) {
            // Provider availability is safe to inspect, but launching it would turn this
            // filesystem-read tool into an unapproved process-execution path.
            mark_provider_degraded(
                &mut snapshot,
                language,
                CODE_INTEL_READ_ONLY_DIAGNOSTICS_REASON,
                "Run active diagnostics only through an explicitly approved sandboxed process tool.",
            );
        }
    }
    snapshot.degraded = true;
    snapshot.reason_codes.push(CODE_INTEL_READ_ONLY_DIAGNOSTICS_REASON.to_owned());
    finish_diagnostic_snapshot(snapshot)
}

fn project_code_intel_diagnostics_output(
    snapshot: DiagnosticSnapshot,
    runtime_snapshot: CodeIntelRuntimeSnapshot,
    runtime_authority: &BTreeMap<CodeIntelLanguage, CodeIntelProviderSnapshotAuthority>,
    runtime_cwd_hints: &[RuntimeCwdHint],
) -> Value {
    let mut authority = provider_snapshot_authority(&snapshot);
    merge_provider_snapshot_authority(&mut authority, runtime_authority);
    let snapshot = project_authoritative_diagnostic_snapshot(snapshot, &authority);
    json!({
        "schema_version": CODE_INTEL_SCHEMA_VERSION,
        "execution_mode": "passive",
        "snapshot": snapshot,
        "runtime": runtime_snapshot,
        "runtime_cwd_hints": runtime_cwd_hints,
        "redaction_level": CODE_INTEL_REDACTION_LEVEL,
    })
}

fn code_intel_symbols_output(
    tool_name: &str,
    workspace: &CodeIntelWorkspace,
    input: &CodeIntelToolInput,
) -> Result<Value, String> {
    let path = input.path.as_deref().unwrap_or_default();
    let file = resolve_code_intel_file(workspace, path, tool_name)?;
    let source = read_code_intel_source(file.canonical_path.as_path(), tool_name)?;
    let (symbols, truncated) = extract_symbols_from_source(
        file.language,
        file.display_path.as_str(),
        source.as_str(),
        input_limit(input),
    );
    let semantic_context = CodeSemanticContextProvider::from_symbols(symbols.as_slice(), truncated);
    Ok(json!({
        "schema_version": CODE_INTEL_SCHEMA_VERSION,
        "path": file.display_path,
        "workspace_root_index": file.workspace_root_index,
        "language": file.language,
        "symbols": symbols,
        "semantic_context": semantic_context,
        "provider_status": &workspace.provider_status,
        "truncated": truncated,
        "source": "bounded_lexical_index",
        "redaction_level": CODE_INTEL_REDACTION_LEVEL,
    }))
}

fn code_intel_definition_output(
    workspace: &CodeIntelWorkspace,
    input: &CodeIntelToolInput,
) -> Result<Value, String> {
    let query = code_intel_query_symbol(workspace, input, CODE_DEFINITION_TOOL_NAME)?;
    let (definitions, truncated) =
        collect_workspace_symbols(workspace, Some(query.as_str()), true, input_limit(input))?;
    Ok(json!({
        "schema_version": CODE_INTEL_SCHEMA_VERSION,
        "symbol": query,
        "definitions": definitions,
        "provider_status": &workspace.provider_status,
        "truncated": truncated,
        "source": "bounded_lexical_index",
        "redaction_level": CODE_INTEL_REDACTION_LEVEL,
    }))
}

fn code_intel_references_output(
    workspace: &CodeIntelWorkspace,
    input: &CodeIntelToolInput,
) -> Result<Value, String> {
    let query = code_intel_query_symbol(workspace, input, CODE_REFERENCES_TOOL_NAME)?;
    let (references, truncated) =
        collect_symbol_references(workspace, query.as_str(), input_limit(input))?;
    Ok(json!({
        "schema_version": CODE_INTEL_SCHEMA_VERSION,
        "symbol": query,
        "references": references,
        "provider_status": &workspace.provider_status,
        "truncated": truncated,
        "source": "bounded_lexical_index",
        "redaction_level": CODE_INTEL_REDACTION_LEVEL,
    }))
}

fn code_intel_hover_output(
    workspace: &CodeIntelWorkspace,
    input: &CodeIntelToolInput,
) -> Result<Value, String> {
    let path = input.path.as_deref().unwrap_or_default();
    let file = resolve_code_intel_file(workspace, path, CODE_HOVER_TOOL_NAME)?;
    let source = read_code_intel_source(file.canonical_path.as_path(), CODE_HOVER_TOOL_NAME)?;
    let (symbols, truncated) = extract_symbols_from_source(
        file.language,
        file.display_path.as_str(),
        source.as_str(),
        CODE_INTEL_MAX_SYMBOLS,
    );
    let query = code_intel_query_symbol_in_file(input, symbols.as_slice())?;
    let symbol = symbols.iter().find(|symbol| symbol.name == query).cloned();
    let hover = symbol.as_ref().map(|symbol| {
        json!({
            "name": symbol.name,
            "kind": symbol.kind,
            "signature": symbol.signature,
            "location": {
                "path": symbol.path,
                "line": symbol.line,
                "column": symbol.column,
            },
            "visibility": symbol.visibility,
            "source_ref": symbol.source_ref,
        })
    });
    Ok(json!({
        "schema_version": CODE_INTEL_SCHEMA_VERSION,
        "symbol": query,
        "hover": hover,
        "provider_status": &workspace.provider_status,
        "truncated": truncated,
        "source": "bounded_lexical_index",
        "redaction_level": CODE_INTEL_REDACTION_LEVEL,
    }))
}

fn code_intel_workspace_symbols_output(
    workspace: &CodeIntelWorkspace,
    input: &CodeIntelToolInput,
) -> Result<Value, String> {
    let query = input.query.as_deref().unwrap_or_default();
    let (symbols, truncated) =
        collect_workspace_symbols(workspace, Some(query), false, input_limit(input))?;
    let semantic_context = CodeSemanticContextProvider::from_symbols(symbols.as_slice(), truncated);
    Ok(json!({
        "schema_version": CODE_INTEL_SCHEMA_VERSION,
        "query": query,
        "symbols": symbols,
        "semantic_context": semantic_context,
        "provider_status": &workspace.provider_status,
        "runtime_cwd_hints": &workspace.runtime_cwd_hints,
        "truncated": truncated,
        "source": "bounded_lexical_index",
        "redaction_level": CODE_INTEL_REDACTION_LEVEL,
    }))
}

fn code_intel_query_symbol(
    workspace: &CodeIntelWorkspace,
    input: &CodeIntelToolInput,
    tool_name: &str,
) -> Result<String, String> {
    if let Some(symbol) = input.symbol.as_deref() {
        return Ok(symbol.to_owned());
    }
    let path = input
        .path
        .as_deref()
        .ok_or_else(|| format!("{tool_name} requires 'path' when resolving a position"))?;
    let file = resolve_code_intel_file(workspace, path, tool_name)?;
    let source = read_code_intel_source(file.canonical_path.as_path(), tool_name)?;
    let (symbols, _) = extract_symbols_from_source(
        file.language,
        file.display_path.as_str(),
        source.as_str(),
        CODE_INTEL_MAX_SYMBOLS,
    );
    code_intel_query_symbol_in_file(input, symbols.as_slice())
}

fn code_intel_query_symbol_in_file(
    input: &CodeIntelToolInput,
    symbols: &[CodeSymbol],
) -> Result<String, String> {
    if let Some(symbol) = input.symbol.as_deref() {
        return Ok(symbol.to_owned());
    }
    let line = input.line.unwrap_or_default();
    let column = input.column.unwrap_or_default();
    symbols
        .iter()
        .filter(|symbol| symbol.line <= line)
        .min_by_key(|symbol| {
            let line_distance = line.saturating_sub(symbol.line);
            let column_distance = column.abs_diff(symbol.column);
            (line_distance, column_distance)
        })
        .map(|symbol| symbol.name.clone())
        .ok_or_else(|| "no symbol found near requested line/column".to_owned())
}

fn input_limit(input: &CodeIntelToolInput) -> usize {
    input
        .max_results
        .filter(|limit| *limit > 0)
        .unwrap_or(CODE_INTEL_MAX_WORKSPACE_RESULTS)
        .min(CODE_INTEL_MAX_WORKSPACE_RESULTS)
}

fn extract_symbols_from_source(
    language: Option<CodeIntelLanguage>,
    path: &str,
    source: &str,
    max_symbols: usize,
) -> (Vec<CodeSymbol>, bool) {
    let Some(language) = language else {
        return (Vec::new(), false);
    };
    let mut symbols = Vec::new();
    let mut truncated = false;
    for (index, line) in source.lines().enumerate() {
        if symbols.len() >= max_symbols {
            truncated = true;
            break;
        }
        if let Some(symbol) = parse_symbol_line(language, path, index.saturating_add(1), line) {
            symbols.push(symbol);
        }
    }
    symbols.sort_by(|left, right| left.line.cmp(&right.line).then(left.name.cmp(&right.name)));
    (symbols, truncated)
}

fn parse_symbol_line(
    language: CodeIntelLanguage,
    path: &str,
    line_number: usize,
    line: &str,
) -> Option<CodeSymbol> {
    match language {
        CodeIntelLanguage::Rust => parse_rust_symbol_line(path, line_number, line),
        CodeIntelLanguage::TypeScript => {
            parse_typescript_symbol_line(CodeIntelLanguage::TypeScript, path, line_number, line)
        }
        CodeIntelLanguage::JavaScript => {
            parse_typescript_symbol_line(CodeIntelLanguage::JavaScript, path, line_number, line)
        }
        CodeIntelLanguage::Python => parse_python_symbol_line(path, line_number, line),
        CodeIntelLanguage::Go => parse_go_symbol_line(path, line_number, line),
        CodeIntelLanguage::Java => parse_java_symbol_line(path, line_number, line),
        CodeIntelLanguage::C => parse_c_family_symbol_line(
            CodeIntelLanguage::C,
            path,
            line_number,
            line,
            &["struct ", "enum ", "typedef "],
        ),
        CodeIntelLanguage::Cpp => parse_c_family_symbol_line(
            CodeIntelLanguage::Cpp,
            path,
            line_number,
            line,
            &["class ", "struct ", "enum ", "namespace "],
        ),
        CodeIntelLanguage::CSharp => parse_csharp_symbol_line(path, line_number, line),
        CodeIntelLanguage::Ruby => parse_ruby_symbol_line(path, line_number, line),
        CodeIntelLanguage::Php => parse_php_symbol_line(path, line_number, line),
        CodeIntelLanguage::Shell => parse_shell_symbol_line(path, line_number, line),
        CodeIntelLanguage::Yaml | CodeIntelLanguage::Json => None,
    }
}

fn parse_rust_symbol_line(path: &str, line_number: usize, line: &str) -> Option<CodeSymbol> {
    let trimmed = line.trim_start();
    let visibility = if trimmed.starts_with("pub ") || trimmed.starts_with("pub(") {
        "public"
    } else {
        "private"
    };
    let without_visibility = trimmed
        .strip_prefix("pub(crate) ")
        .or_else(|| trimmed.strip_prefix("pub(super) "))
        .or_else(|| trimmed.strip_prefix("pub "))
        .unwrap_or(trimmed);
    for (keyword, kind) in [
        ("async fn ", "function"),
        ("fn ", "function"),
        ("struct ", "struct"),
        ("enum ", "enum"),
        ("trait ", "trait"),
        ("impl ", "impl"),
        ("const ", "constant"),
        ("static ", "static"),
    ] {
        if let Some(rest) = without_visibility.strip_prefix(keyword) {
            let name = take_identifier(rest)?;
            return Some(code_symbol(
                CodeIntelLanguage::Rust,
                path,
                line_number,
                line,
                name,
                kind,
                visibility,
            ));
        }
    }
    None
}

fn parse_typescript_symbol_line(
    language: CodeIntelLanguage,
    path: &str,
    line_number: usize,
    line: &str,
) -> Option<CodeSymbol> {
    let trimmed = line.trim_start();
    let visibility = if trimmed.starts_with("export ") { "public" } else { "module_private" };
    let without_export = trimmed
        .strip_prefix("export default ")
        .or_else(|| trimmed.strip_prefix("export "))
        .unwrap_or(trimmed);
    let without_async = without_export.strip_prefix("async ").unwrap_or(without_export);
    for (keyword, kind) in [
        ("function ", "function"),
        ("class ", "class"),
        ("interface ", "interface"),
        ("type ", "type"),
        ("const ", "constant"),
        ("let ", "variable"),
        ("var ", "variable"),
    ] {
        if let Some(rest) = without_async.strip_prefix(keyword) {
            let name = take_identifier(rest)?;
            return Some(code_symbol(language, path, line_number, line, name, kind, visibility));
        }
    }
    None
}

fn parse_python_symbol_line(path: &str, line_number: usize, line: &str) -> Option<CodeSymbol> {
    let trimmed = line.trim_start();
    let (rest, kind) = trimmed
        .strip_prefix("async def ")
        .map(|rest| (rest, "function"))
        .or_else(|| trimmed.strip_prefix("def ").map(|rest| (rest, "function")))
        .or_else(|| trimmed.strip_prefix("class ").map(|rest| (rest, "class")))?;
    let name = take_identifier(rest)?;
    let visibility = if name.starts_with('_') { "private" } else { "public" };
    Some(code_symbol(CodeIntelLanguage::Python, path, line_number, line, name, kind, visibility))
}

fn parse_go_symbol_line(path: &str, line_number: usize, line: &str) -> Option<CodeSymbol> {
    let trimmed = line.trim_start();
    if let Some(rest) = trimmed.strip_prefix("func ") {
        let rest = rest.trim_start();
        let rest = if rest.starts_with('(') {
            rest.find(')').and_then(|index| rest.get(index.saturating_add(1)..))?.trim_start()
        } else {
            rest
        };
        let name = take_identifier(rest)?;
        let visibility = if name.chars().next().is_some_and(char::is_uppercase) {
            "public"
        } else {
            "package_private"
        };
        return Some(code_symbol(
            CodeIntelLanguage::Go,
            path,
            line_number,
            line,
            name,
            "function",
            visibility,
        ));
    }
    for (keyword, kind) in [("type ", "type"), ("const ", "constant"), ("var ", "variable")] {
        if let Some(rest) = trimmed.strip_prefix(keyword) {
            let name = take_identifier(rest)?;
            return Some(code_symbol(
                CodeIntelLanguage::Go,
                path,
                line_number,
                line,
                name,
                kind,
                "package_private",
            ));
        }
    }
    None
}

fn parse_java_symbol_line(path: &str, line_number: usize, line: &str) -> Option<CodeSymbol> {
    let trimmed = strip_leading_modifiers(
        line.trim_start(),
        &["public", "private", "protected", "static", "final", "abstract", "sealed"],
    );
    for (keyword, kind) in
        [("class ", "class"), ("interface ", "interface"), ("enum ", "enum"), ("record ", "record")]
    {
        if let Some(rest) = trimmed.strip_prefix(keyword) {
            let name = take_identifier(rest)?;
            return Some(code_symbol(
                CodeIntelLanguage::Java,
                path,
                line_number,
                line,
                name,
                kind,
                java_visibility(line),
            ));
        }
    }
    parse_callable_symbol_line(
        CodeIntelLanguage::Java,
        path,
        line_number,
        line,
        "method",
        java_visibility(line),
    )
}

fn parse_c_family_symbol_line(
    language: CodeIntelLanguage,
    path: &str,
    line_number: usize,
    line: &str,
    type_keywords: &[&str],
) -> Option<CodeSymbol> {
    let trimmed = line.trim_start();
    for keyword in type_keywords {
        if let Some(rest) = trimmed.strip_prefix(keyword) {
            let name = take_identifier(rest)?;
            return Some(code_symbol(
                language,
                path,
                line_number,
                line,
                name,
                keyword.trim(),
                "translation_unit",
            ));
        }
    }
    parse_callable_symbol_line(language, path, line_number, line, "function", "translation_unit")
}

fn parse_csharp_symbol_line(path: &str, line_number: usize, line: &str) -> Option<CodeSymbol> {
    let trimmed = strip_leading_modifiers(
        line.trim_start(),
        &["public", "private", "protected", "internal", "static", "sealed", "abstract"],
    );
    for (keyword, kind) in [
        ("class ", "class"),
        ("interface ", "interface"),
        ("enum ", "enum"),
        ("struct ", "struct"),
        ("record ", "record"),
    ] {
        if let Some(rest) = trimmed.strip_prefix(keyword) {
            let name = take_identifier(rest)?;
            return Some(code_symbol(
                CodeIntelLanguage::CSharp,
                path,
                line_number,
                line,
                name,
                kind,
                csharp_visibility(line),
            ));
        }
    }
    parse_callable_symbol_line(
        CodeIntelLanguage::CSharp,
        path,
        line_number,
        line,
        "method",
        csharp_visibility(line),
    )
}

fn parse_ruby_symbol_line(path: &str, line_number: usize, line: &str) -> Option<CodeSymbol> {
    let trimmed = line.trim_start();
    let (rest, kind) = trimmed
        .strip_prefix("def ")
        .map(|rest| (rest, "function"))
        .or_else(|| trimmed.strip_prefix("class ").map(|rest| (rest, "class")))
        .or_else(|| trimmed.strip_prefix("module ").map(|rest| (rest, "module")))?;
    let name = take_identifier(rest)?;
    Some(code_symbol(CodeIntelLanguage::Ruby, path, line_number, line, name, kind, "public"))
}

fn parse_php_symbol_line(path: &str, line_number: usize, line: &str) -> Option<CodeSymbol> {
    let trimmed = strip_leading_modifiers(
        line.trim_start(),
        &["public", "private", "protected", "static", "final", "abstract"],
    );
    let (rest, kind) = trimmed
        .strip_prefix("function ")
        .map(|rest| (rest, "function"))
        .or_else(|| trimmed.strip_prefix("class ").map(|rest| (rest, "class")))
        .or_else(|| trimmed.strip_prefix("interface ").map(|rest| (rest, "interface")))
        .or_else(|| trimmed.strip_prefix("trait ").map(|rest| (rest, "trait")))?;
    let name = take_identifier(rest.trim_start_matches('&'))?;
    Some(code_symbol(CodeIntelLanguage::Php, path, line_number, line, name, kind, "public"))
}

fn parse_shell_symbol_line(path: &str, line_number: usize, line: &str) -> Option<CodeSymbol> {
    let trimmed = line.trim_start();
    if let Some(rest) = trimmed.strip_prefix("function ") {
        let name = take_identifier(rest)?;
        return Some(code_symbol(
            CodeIntelLanguage::Shell,
            path,
            line_number,
            line,
            name,
            "function",
            "script",
        ));
    }
    let (candidate, rest) = trimmed.split_once("()")?;
    if !rest.trim_start().starts_with('{') {
        return None;
    }
    let name = take_identifier(candidate.trim())?;
    Some(code_symbol(CodeIntelLanguage::Shell, path, line_number, line, name, "function", "script"))
}

fn parse_callable_symbol_line(
    language: CodeIntelLanguage,
    path: &str,
    line_number: usize,
    line: &str,
    kind: &str,
    visibility: &str,
) -> Option<CodeSymbol> {
    let trimmed = line.trim();
    if trimmed.starts_with("//")
        || trimmed.starts_with('#')
        || trimmed.ends_with(';')
        || !trimmed.contains('(')
        || !trimmed.contains(')')
    {
        return None;
    }
    for control in ["if", "for", "while", "switch", "catch", "return"] {
        if trimmed.starts_with(control) {
            return None;
        }
    }
    let before_paren = trimmed.split_once('(')?.0.trim_end();
    let name_start = before_paren
        .char_indices()
        .rev()
        .find_map(|(index, ch)| (!is_identifier_char(ch)).then_some(index + ch.len_utf8()))
        .unwrap_or(0);
    let name = before_paren.get(name_start..)?.trim();
    let name = take_identifier(name)?;
    Some(code_symbol(language, path, line_number, line, name, kind, visibility))
}

fn strip_leading_modifiers<'a>(mut value: &'a str, modifiers: &[&str]) -> &'a str {
    loop {
        let before = value;
        for modifier in modifiers {
            if let Some(rest) = value.strip_prefix(modifier) {
                if rest.chars().next().is_some_and(char::is_whitespace) {
                    value = rest.trim_start();
                    break;
                }
            }
        }
        if value == before {
            return value;
        }
    }
}

fn java_visibility(line: &str) -> &'static str {
    if line.trim_start().starts_with("public ") {
        "public"
    } else if line.trim_start().starts_with("private ") {
        "private"
    } else if line.trim_start().starts_with("protected ") {
        "protected"
    } else {
        "package_private"
    }
}

fn csharp_visibility(line: &str) -> &'static str {
    if line.trim_start().starts_with("public ") {
        "public"
    } else if line.trim_start().starts_with("private ") {
        "private"
    } else if line.trim_start().starts_with("protected ") {
        "protected"
    } else if line.trim_start().starts_with("internal ") {
        "internal"
    } else {
        "private"
    }
}

fn code_symbol(
    language: CodeIntelLanguage,
    path: &str,
    line_number: usize,
    source_line: &str,
    name: &str,
    kind: &str,
    visibility: &str,
) -> CodeSymbol {
    let column = source_line.find(name).map(|index| index.saturating_add(1)).unwrap_or(1);
    let line = u32::try_from(line_number).unwrap_or(u32::MAX);
    let column = u32::try_from(column).unwrap_or(u32::MAX);
    CodeSymbol {
        name: name.to_owned(),
        kind: kind.to_owned(),
        language,
        path: path.to_owned(),
        line,
        column,
        signature: bound_message_with_limit(source_line.trim(), 180),
        visibility: visibility.to_owned(),
        source_ref: format!("{path}:{line}:{column}"),
    }
}

fn take_identifier(raw: &str) -> Option<&str> {
    let trimmed = raw.trim_start();
    let end = trimmed
        .char_indices()
        .find_map(|(index, ch)| (!is_identifier_char(ch)).then_some(index))
        .unwrap_or(trimmed.len());
    (end > 0).then_some(&trimmed[..end])
}

fn is_identifier_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn collect_workspace_symbols(
    workspace: &CodeIntelWorkspace,
    query: Option<&str>,
    exact: bool,
    max_results: usize,
) -> Result<(Vec<CodeSymbol>, bool), String> {
    let mut state = CodeIntelWorkspaceScanState::new(max_results);
    scan_code_intel_workspace(workspace.primary_root.as_path(), &mut state)?;
    let query_lower = query.map(str::to_ascii_lowercase);
    let mut symbols = Vec::new();
    for file in state.files {
        let Some(language) = CodeIntelLanguage::from_path(file.display_path.as_str()) else {
            continue;
        };
        let source = read_code_intel_source(
            file.canonical_path.as_path(),
            CODE_WORKSPACE_SYMBOLS_TOOL_NAME,
        )?;
        let (file_symbols, truncated) = extract_symbols_from_source(
            Some(language),
            file.display_path.as_str(),
            source.as_str(),
            max_results,
        );
        state.truncated |= truncated;
        for symbol in file_symbols {
            let matched = match query_lower.as_deref() {
                Some(query) if exact => symbol.name.eq_ignore_ascii_case(query),
                Some(query) => symbol.name.to_ascii_lowercase().contains(query),
                None => true,
            };
            if matched {
                symbols.push(symbol);
                if symbols.len() >= max_results {
                    return Ok((symbols, true));
                }
            }
        }
    }
    symbols.sort_by(|left, right| {
        left.name.cmp(&right.name).then(left.path.cmp(&right.path)).then(left.line.cmp(&right.line))
    });
    Ok((symbols, state.truncated))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CodeReference {
    path: String,
    line: u32,
    column: u32,
    line_text: String,
    source_ref: String,
}

fn collect_symbol_references(
    workspace: &CodeIntelWorkspace,
    symbol: &str,
    max_results: usize,
) -> Result<(Vec<CodeReference>, bool), String> {
    let mut state = CodeIntelWorkspaceScanState::new(max_results);
    scan_code_intel_workspace(workspace.primary_root.as_path(), &mut state)?;
    let mut references = Vec::new();
    for file in state.files {
        let source =
            read_code_intel_source(file.canonical_path.as_path(), CODE_REFERENCES_TOOL_NAME)?;
        for (line_index, line) in source.lines().enumerate() {
            for column in identifier_match_columns(line, symbol) {
                let line_number = u32::try_from(line_index.saturating_add(1)).unwrap_or(u32::MAX);
                let column = u32::try_from(column).unwrap_or(u32::MAX);
                references.push(CodeReference {
                    path: file.display_path.clone(),
                    line: line_number,
                    column,
                    line_text: bound_message_with_limit(line, 240),
                    source_ref: format!("{}:{line_number}:{column}", file.display_path),
                });
                if references.len() >= max_results {
                    return Ok((references, true));
                }
            }
        }
    }
    Ok((references, state.truncated))
}

fn identifier_match_columns(line: &str, symbol: &str) -> Vec<usize> {
    line.match_indices(symbol)
        .filter_map(|(index, _)| {
            let before = line[..index].chars().next_back();
            let after = line[index.saturating_add(symbol.len())..].chars().next();
            let before_boundary = before.is_none_or(|ch| !is_identifier_char(ch));
            let after_boundary = after.is_none_or(|ch| !is_identifier_char(ch));
            (before_boundary && after_boundary).then_some(index.saturating_add(1))
        })
        .collect()
}

#[derive(Debug, Clone)]
struct CodeIntelWorkspaceFile {
    canonical_path: PathBuf,
    display_path: String,
}

#[derive(Debug)]
struct CodeIntelWorkspaceScanState {
    files: Vec<CodeIntelWorkspaceFile>,
    files_seen: usize,
    truncated: bool,
    max_results: usize,
}

impl CodeIntelWorkspaceScanState {
    fn new(max_results: usize) -> Self {
        Self { files: Vec::new(), files_seen: 0, truncated: false, max_results }
    }

    fn has_capacity(&self) -> bool {
        self.files_seen < CODE_INTEL_MAX_WORKSPACE_FILES
            && self.files.len() < self.max_results.saturating_mul(8).max(32)
    }
}

fn scan_code_intel_workspace(
    root: &Path,
    state: &mut CodeIntelWorkspaceScanState,
) -> Result<(), String> {
    scan_code_intel_workspace_recursive(root, root, state, 0)
}

fn scan_code_intel_workspace_recursive(
    root: &Path,
    path: &Path,
    state: &mut CodeIntelWorkspaceScanState,
    depth: usize,
) -> Result<(), String> {
    if !state.has_capacity() || depth > CODE_INTEL_MAX_WORKSPACE_DEPTH {
        state.truncated = true;
        return Ok(());
    }
    let mut entries = fs::read_dir(path)
        .map_err(|error| format!("failed to read code-intelligence workspace directory: {error}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to read code-intelligence workspace entry: {error}"))?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        if !state.has_capacity() {
            state.truncated = true;
            break;
        }
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| {
            format!("failed to inspect code-intelligence workspace entry: {error}")
        })?;
        if file_type.is_dir() {
            if entry
                .file_name()
                .to_str()
                .is_some_and(|name| CODE_INTEL_SKIPPED_DIRS.iter().any(|skip| skip == &name))
            {
                continue;
            }
            scan_code_intel_workspace_recursive(
                root,
                path.as_path(),
                state,
                depth.saturating_add(1),
            )?;
        } else if file_type.is_file()
            && CodeIntelLanguage::from_path(normalize_path_for_output(path.as_path()).as_str())
                .is_some()
        {
            state.files_seen = state.files_seen.saturating_add(1);
            let metadata = entry.metadata().map_err(|error| {
                format!("failed to inspect code-intelligence workspace file: {error}")
            })?;
            if metadata.len() > CODE_INTEL_MAX_SOURCE_BYTES {
                state.truncated = true;
                continue;
            }
            let display_path = path
                .strip_prefix(root)
                .map(normalize_path_for_output)
                .unwrap_or_else(|_| normalize_path_for_output(path.as_path()));
            state.files.push(CodeIntelWorkspaceFile { canonical_path: path, display_path });
        }
    }
    Ok(())
}

fn resolve_code_intel_file(
    workspace: &CodeIntelWorkspace,
    path: &str,
    tool_name: &str,
) -> Result<ResolvedCodeIntelFile, String> {
    let requested = Path::new(path);
    if requested.is_absolute() {
        if requested.components().any(|component| matches!(component, Component::ParentDir)) {
            return Err(format!("{tool_name} path escapes agent workspace roots"));
        }
        let canonical_path = fs::canonicalize(requested).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                format!("{tool_name} file not found in agent workspace roots: {path}")
            } else {
                format!("{tool_name} failed to resolve path: {error}")
            }
        })?;
        let (workspace_root_index, canonical_root) = workspace
            .roots
            .iter()
            .find(|(_, root)| path_is_within_root(canonical_path.as_path(), root))
            .ok_or_else(|| format!("{tool_name} path escapes agent workspace roots"))?;
        return resolved_code_intel_file(
            *workspace_root_index,
            canonical_root,
            canonical_path,
            path,
            tool_name,
        );
    }

    for (workspace_root_index, canonical_root) in &workspace.roots {
        let candidate = canonical_root.join(requested);
        let canonical_path = match fs::canonicalize(candidate.as_path()) {
            Ok(path) => path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(format!(
                    "{tool_name} failed to resolve path in workspace root: {error}"
                ));
            }
        };
        if !path_is_within_root(canonical_path.as_path(), canonical_root) {
            return Err(format!("{tool_name} path escapes agent workspace roots"));
        }
        return resolved_code_intel_file(
            *workspace_root_index,
            canonical_root,
            canonical_path,
            path,
            tool_name,
        );
    }
    Err(format!("{tool_name} file not found in agent workspace roots: {path}"))
}

fn resolved_code_intel_file(
    workspace_root_index: usize,
    canonical_root: &Path,
    canonical_path: PathBuf,
    requested_path: &str,
    tool_name: &str,
) -> Result<ResolvedCodeIntelFile, String> {
    if !canonical_path.is_file() {
        return Err(format!("{tool_name} target is not a regular file: {requested_path}"));
    }
    let display_path = canonical_path
        .strip_prefix(canonical_root)
        .map(normalize_path_for_output)
        .unwrap_or_else(|_| requested_path.to_owned());
    Ok(ResolvedCodeIntelFile {
        workspace_root_index,
        language: CodeIntelLanguage::from_path(display_path.as_str()),
        canonical_path,
        display_path,
    })
}

fn read_code_intel_source(path: &Path, tool_name: &str) -> Result<String, String> {
    let metadata = fs::metadata(path)
        .map_err(|error| format!("{tool_name} failed to inspect source file: {error}"))?;
    if metadata.len() > CODE_INTEL_MAX_SOURCE_BYTES {
        return Err(format!(
            "{tool_name} source file exceeds {} bytes",
            CODE_INTEL_MAX_SOURCE_BYTES
        ));
    }
    fs::read_to_string(path)
        .map_err(|error| format!("{tool_name} failed to read source file: {error}"))
}

fn code_intel_provider_health(
    config: &CodeIntelConfig,
    workspace_roots: &[PathBuf],
) -> Vec<CodeIntelProviderStatus> {
    let manager = LspProcessManager::from_config(config);
    if !config.enabled {
        return manager.disabled_provider_statuses();
    }
    let languages = CodeIntelLanguage::ALL.iter().copied().collect::<BTreeSet<_>>();
    let mut statuses = manager.provider_statuses(&languages);
    if workspace_roots.is_empty() {
        for status in &mut statuses {
            status.status = "degraded".to_owned();
            status.reason_code = "code_intel.workspace_root_missing".to_owned();
            status.repair_hint =
                "Configure at least one workspace root for code intelligence.".to_owned();
        }
    }
    statuses
}

fn patch_impact_analysis(
    workspace_roots: &[PathBuf],
    files_touched: &[WorkspacePatchFileAttestation],
    diagnostic_before: &DiagnosticSnapshot,
    diagnostic_after: &DiagnosticSnapshot,
    diagnostic_delta: &DiagnosticDelta,
) -> PatchImpactAnalysis {
    let mut files = files_touched
        .iter()
        .map(|file| file.path.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    files.sort();
    let languages = files
        .iter()
        .filter_map(|path| CodeIntelLanguage::from_path(path))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut touched_symbols = Vec::new();
    let mut truncated = false;
    for file in files_touched {
        if touched_symbols.len() >= CODE_INTEL_CONTEXT_SYMBOL_LIMIT {
            truncated = true;
            break;
        }
        if let Some(root) = workspace_roots.get(file.workspace_root_index) {
            let canonical_root = fs::canonicalize(root).unwrap_or_else(|_| root.clone());
            let candidate = canonical_root.join(file.path.as_str());
            let Ok(canonical) = fs::canonicalize(candidate.as_path()) else {
                continue;
            };
            if !path_is_within_root(canonical.as_path(), canonical_root.as_path())
                || !canonical.is_file()
            {
                continue;
            }
            let Ok(source) = read_code_intel_source(canonical.as_path(), "palyra.fs.apply_patch")
            else {
                continue;
            };
            let (symbols, symbols_truncated) = extract_symbols_from_source(
                CodeIntelLanguage::from_path(file.path.as_str()),
                file.path.as_str(),
                source.as_str(),
                CODE_INTEL_CONTEXT_SYMBOL_LIMIT.saturating_sub(touched_symbols.len()),
            );
            truncated |= symbols_truncated;
            touched_symbols.extend(symbols);
        }
    }
    let risk_level = patch_impact_risk_level(
        touched_symbols.as_slice(),
        files_touched,
        diagnostic_delta,
        diagnostic_after.degraded,
    );
    let verification_guidance = patch_impact_verification_guidance(
        risk_level.as_str(),
        languages.as_slice(),
        diagnostic_delta,
    );
    let canonical_roots =
        canonicalize_code_intel_roots(workspace_roots, "palyra.fs.apply_patch").unwrap_or_default();
    PatchImpactAnalysis {
        schema_version: CODE_INTEL_SCHEMA_VERSION,
        files,
        languages,
        touched_symbols,
        diagnostics_before_count: diagnostic_before.items.len(),
        diagnostics_after_count: diagnostic_after.items.len(),
        new_errors: diagnostic_delta.new_errors,
        new_warnings: diagnostic_delta.new_warnings,
        risk_level,
        verification_guidance,
        runtime_cwd_hints: detect_runtime_cwd_hints(canonical_roots.as_slice()),
        truncated,
        reason_codes: vec!["code_intel.patch_impact.lexical_analysis".to_owned()],
    }
}

fn patch_impact_risk_level(
    symbols: &[CodeSymbol],
    files_touched: &[WorkspacePatchFileAttestation],
    diagnostic_delta: &DiagnosticDelta,
    diagnostics_degraded: bool,
) -> String {
    if diagnostic_delta.new_errors > 0 {
        return "high".to_owned();
    }
    if diagnostics_degraded
        || diagnostic_delta.new_warnings > 0
        || files_touched.len() > 3
        || symbols.iter().any(|symbol| symbol.visibility == "public")
    {
        return "medium".to_owned();
    }
    "low".to_owned()
}

fn patch_impact_verification_guidance(
    risk_level: &str,
    languages: &[CodeIntelLanguage],
    diagnostic_delta: &DiagnosticDelta,
) -> Vec<String> {
    let mut guidance = Vec::new();
    if diagnostic_delta.new_errors > 0 {
        guidance.push(
            "Fix new code-intelligence errors before treating the patch as verified.".to_owned(),
        );
    }
    if languages.contains(&CodeIntelLanguage::Rust) {
        guidance.push("Run targeted Rust tests or cargo check for touched crates.".to_owned());
    }
    if languages.contains(&CodeIntelLanguage::TypeScript) {
        guidance.push("Run the relevant TypeScript or web check for touched packages.".to_owned());
    }
    if languages.contains(&CodeIntelLanguage::Python) {
        guidance
            .push("Run the relevant Python type or unit checks for touched modules.".to_owned());
    }
    if languages.contains(&CodeIntelLanguage::Go) {
        guidance.push("Run go test or go test ./... for touched Go packages.".to_owned());
    }
    if languages.iter().any(|language| {
        matches!(
            language,
            CodeIntelLanguage::Java
                | CodeIntelLanguage::C
                | CodeIntelLanguage::Cpp
                | CodeIntelLanguage::CSharp
                | CodeIntelLanguage::Ruby
                | CodeIntelLanguage::Php
                | CodeIntelLanguage::JavaScript
                | CodeIntelLanguage::Yaml
                | CodeIntelLanguage::Json
                | CodeIntelLanguage::Shell
        )
    }) {
        guidance.push(
            "Run the project-specific lint, typecheck, or unit checks for touched languages."
                .to_owned(),
        );
    }
    if risk_level == "medium" || risk_level == "high" {
        guidance.push("Inspect references for public or multi-file symbol changes.".to_owned());
    }
    if guidance.is_empty() {
        guidance.push(
            "Run the narrowest meaningful local verification for the touched files.".to_owned(),
        );
    }
    guidance
}

fn detect_runtime_cwd_hints(canonical_roots: &[(usize, PathBuf)]) -> Vec<RuntimeCwdHint> {
    let mut hints = Vec::new();
    for (_, root) in canonical_roots {
        detect_runtime_cwd_hints_recursive(root, root, &mut hints, 0);
        if hints.len() >= 24 {
            break;
        }
    }
    hints.sort_by(|left, right| {
        left.cwd
            .cmp(&right.cwd)
            .then(left.manifest_path.cmp(&right.manifest_path))
            .then(left.project_kind.cmp(&right.project_kind))
    });
    hints
        .dedup_by(|left, right| left.cwd == right.cwd && left.manifest_path == right.manifest_path);
    hints.truncate(24);
    hints
}

fn detect_runtime_cwd_hints_recursive(
    root: &Path,
    directory: &Path,
    hints: &mut Vec<RuntimeCwdHint>,
    depth: usize,
) {
    if depth > 4 || hints.len() >= 24 {
        return;
    }
    for manifest in CODE_INTEL_MANIFEST_NAMES {
        let manifest_path = directory.join(manifest);
        if manifest_path.is_file() {
            let cwd = directory
                .strip_prefix(root)
                .map(normalize_path_for_output)
                .unwrap_or_else(|_| normalize_path_for_output(directory));
            let manifest_path = manifest_path
                .strip_prefix(root)
                .map(normalize_path_for_output)
                .unwrap_or_else(|_| normalize_path_for_output(manifest_path.as_path()));
            hints.push(RuntimeCwdHint {
                cwd: if cwd.is_empty() { ".".to_owned() } else { cwd },
                manifest_path,
                project_kind: project_kind_for_manifest(manifest).to_owned(),
                confidence: "manifest_detected".to_owned(),
                reason_code: "code_intel.runtime_cwd.manifest_detected".to_owned(),
            });
        }
    }
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    let mut entries = entries.filter_map(Result::ok).collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        if hints.len() >= 24 {
            break;
        }
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| CODE_INTEL_SKIPPED_DIRS.iter().any(|skip| skip == &name))
        {
            continue;
        }
        detect_runtime_cwd_hints_recursive(
            root,
            entry.path().as_path(),
            hints,
            depth.saturating_add(1),
        );
    }
}

fn project_kind_for_manifest(manifest: &str) -> &'static str {
    match manifest {
        "Cargo.toml" => "rust",
        "package.json" | "tsconfig.json" | "jsconfig.json" => "javascript",
        "pyproject.toml" => "python",
        _ => "unknown",
    }
}

fn code_intel_roots_with_active_first(active_root: PathBuf, roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut ordered = vec![active_root];
    for root in roots {
        if !ordered.iter().any(|existing| same_code_intel_root(existing, root)) {
            ordered.push(root.clone());
        }
    }
    ordered
}

fn same_code_intel_root(left: &Path, right: &Path) -> bool {
    let left = fs::canonicalize(left).unwrap_or_else(|_| left.to_path_buf());
    let right = fs::canonicalize(right).unwrap_or_else(|_| right.to_path_buf());
    if left == right {
        return true;
    }
    #[cfg(windows)]
    {
        left.to_string_lossy()
            .replace('\\', "/")
            .eq_ignore_ascii_case(&right.to_string_lossy().replace('\\', "/"))
    }
    #[cfg(not(windows))]
    {
        false
    }
}

fn resolve_code_intel_root_override(
    tool_name: &str,
    agent_roots: &[PathBuf],
    workspace_root: &str,
) -> Result<PathBuf, String> {
    let canonical_roots = canonicalize_code_intel_roots(agent_roots, tool_name)?;
    if canonical_roots.is_empty() {
        return Err(format!("{tool_name} agent has no accessible workspace roots"));
    }
    let requested = Path::new(workspace_root);
    if requested.is_absolute() {
        let canonical = fs::canonicalize(requested).map_err(|error| {
            format!("{tool_name} failed to resolve workspace_root {workspace_root}: {error}")
        })?;
        if !canonical.is_dir() {
            return Err(format!("{tool_name} workspace_root is not a directory: {workspace_root}"));
        }
        if canonical_roots.iter().any(|(_, root)| path_is_within_root(canonical.as_path(), root)) {
            return Ok(canonical);
        }
        return Err(format!(
            "{tool_name} workspace_root escapes agent workspace roots: {workspace_root}"
        ));
    }
    for (_, root) in &canonical_roots {
        if root.file_name().is_some_and(|name| path_component_eq_str(name, workspace_root)) {
            return Ok(root.clone());
        }
        let candidate = root.join(requested);
        let Ok(canonical) = fs::canonicalize(candidate.as_path()) else {
            continue;
        };
        if canonical.is_dir() && path_is_within_root(canonical.as_path(), root) {
            return Ok(canonical);
        }
    }
    Err(format!(
        "{tool_name} workspace_root does not exist inside agent workspace roots: {workspace_root}"
    ))
}

fn canonicalize_code_intel_roots(
    roots: &[PathBuf],
    tool_name: &str,
) -> Result<Vec<(usize, PathBuf)>, String> {
    let mut canonical = Vec::new();
    for (index, root) in roots.iter().enumerate() {
        match fs::canonicalize(root) {
            Ok(path) if path.is_dir() => canonical.push((index, path)),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "{tool_name} failed to resolve workspace root {index}: {error}"
                ));
            }
        }
    }
    Ok(canonical)
}

fn normalize_code_intel_path_input(path: &str) -> String {
    let normalized = path.trim().replace('\\', "/");
    let without_current = normalized.strip_prefix("./").unwrap_or(normalized.as_str());
    match without_current {
        "." | "/workspace" | "/workspace/" | "workspace" | "workspace/" => String::new(),
        _ => without_current
            .strip_prefix("/workspace/")
            .or_else(|| without_current.strip_prefix("workspace/"))
            .unwrap_or(without_current)
            .to_owned(),
    }
}

fn validate_code_intel_path_syntax(path: &str, tool_name: &str) -> Result<(), String> {
    if path.chars().any(char::is_control) {
        return Err(format!("{tool_name} path contains unsupported characters"));
    }
    if path.contains(':') && !looks_like_windows_drive_path(path) {
        return Err(format!("{tool_name} path contains unsupported characters"));
    }
    if path.is_empty() || Path::new(path).is_absolute() {
        return Ok(());
    }
    if !Path::new(path).components().all(|component| matches!(component, Component::Normal(_))) {
        return Err(format!(
            "{tool_name} path must not contain root, prefix, '.', or '..' components"
        ));
    }
    Ok(())
}

fn looks_like_windows_drive_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\')
}

fn path_component_eq_str(component: &std::ffi::OsStr, value: &str) -> bool {
    #[cfg(windows)]
    {
        component.to_string_lossy().eq_ignore_ascii_case(value)
    }
    #[cfg(not(windows))]
    {
        component == std::ffi::OsStr::new(value)
    }
}

fn code_intel_tool_outcome(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    build_tool_execution_outcome(
        proposal_id,
        tool_name,
        input_json,
        success,
        output_json,
        error,
        false,
        "code_intel_runtime".to_owned(),
        "workspace_roots".to_owned(),
    )
}

/// Rust diagnostics provider backed by the rust-analyzer check pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RustAnalyzerProvider {
    pub provider: String,
    pub binary: String,
    pub check_command: String,
    pub check_args: Vec<String>,
    pub timeout_ms: u64,
    pub max_output_bytes: u64,
    pub max_items: usize,
    pub redaction_level: String,
}

impl RustAnalyzerProvider {
    fn from_config(config: &CodeIntelConfig) -> Self {
        Self {
            provider: CodeIntelLanguage::Rust.provider_name().to_owned(),
            binary: config.rust_analyzer_binary.clone(),
            check_command: RUST_ANALYZER_CARGO_CHECK_COMMAND.to_owned(),
            check_args: RUST_ANALYZER_CARGO_CHECK_ARGS
                .iter()
                .map(|arg| (*arg).to_owned())
                .collect(),
            timeout_ms: config.timeout_ms,
            max_output_bytes: config.max_output_bytes,
            max_items: config.max_items,
            redaction_level: crate::application::code_intel_runtime::CODE_INTEL_REDACTION_LEVEL
                .to_owned(),
        }
    }

    async fn capture(
        &self,
        workspace_root: &Path,
        touched_files: &BTreeSet<String>,
    ) -> RustAnalyzerCaptureOutcome {
        if !executable_is_available(self.binary.as_str()) {
            return RustAnalyzerCaptureOutcome::degraded(
                "code_intel.provider_missing.rust",
                "Install rust-analyzer or set tool_call.code_intel.rust_analyzer_binary to an executable path.",
            );
        }
        if !workspace_root.is_dir() {
            return RustAnalyzerCaptureOutcome::degraded(
                "code_intel.rust.workspace_root_missing",
                "Rust diagnostics require an existing workspace root.",
            );
        }
        let output = match self.run_cargo_check_json(workspace_root).await {
            Ok(output) => output,
            Err(error) => {
                let repair_hint = error.repair_hint();
                return RustAnalyzerCaptureOutcome::degraded(
                    error.reason_code(),
                    repair_hint.as_str(),
                );
            }
        };
        let normalizer = RustDiagnosticNormalizer {
            workspace_root: workspace_root.to_path_buf(),
            touched_files: touched_files.clone(),
            max_items: self.max_items,
        };
        let (items, parse_truncated) = normalizer.normalize_cargo_json(output.stdout.as_slice());
        let truncated = parse_truncated || output.stdout_truncated || output.stderr_truncated;
        if items.is_empty() && !output.status_success {
            let hint = bounded_error_hint(output.stderr.as_slice());
            return RustAnalyzerCaptureOutcome::degraded(
                "code_intel.rust.cargo_check_failed",
                hint.as_str(),
            );
        }
        let mut reason_codes = vec![CODE_INTEL_RUST_SNAPSHOT_CAPTURED_EVENT.to_owned()];
        if truncated {
            reason_codes.push("code_intel.rust.output_truncated".to_owned());
        }
        RustAnalyzerCaptureOutcome::Captured { items, truncated, reason_codes }
    }

    async fn run_cargo_check_json(
        &self,
        workspace_root: &Path,
    ) -> Result<RustAnalyzerProcessOutput, RustAnalyzerRunError> {
        let mut command = TokioCommand::new(RUST_ANALYZER_CARGO_CHECK_COMMAND);
        command
            .args(RUST_ANALYZER_CARGO_CHECK_ARGS)
            .current_dir(workspace_root)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| {
            RustAnalyzerRunError::Spawn(redact_diagnostic_text(&error.to_string()))
        })?;
        let stdout = child.stdout.take().ok_or(RustAnalyzerRunError::MissingPipe("stdout"))?;
        let stderr = child.stderr.take().ok_or(RustAnalyzerRunError::MissingPipe("stderr"))?;
        let max_output_bytes = max_output_bytes(self.max_output_bytes);
        let stdout_task = tokio::spawn(read_bounded_stream(stdout, max_output_bytes));
        let stderr_task = tokio::spawn(read_bounded_stream(stderr, max_output_bytes));
        let status = match timeout(Duration::from_millis(self.timeout_ms), child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => {
                return Err(RustAnalyzerRunError::Wait(redact_diagnostic_text(&error.to_string())));
            }
            Err(_) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                let _ = stdout_task.await;
                let _ = stderr_task.await;
                return Err(RustAnalyzerRunError::Timeout);
            }
        };
        let stdout = stdout_task
            .await
            .map_err(|error| {
                RustAnalyzerRunError::Output(redact_diagnostic_text(&error.to_string()))
            })?
            .map_err(|error| {
                RustAnalyzerRunError::Output(redact_diagnostic_text(&error.to_string()))
            })?;
        let stderr = stderr_task
            .await
            .map_err(|error| {
                RustAnalyzerRunError::Output(redact_diagnostic_text(&error.to_string()))
            })?
            .map_err(|error| {
                RustAnalyzerRunError::Output(redact_diagnostic_text(&error.to_string()))
            })?;
        Ok(RustAnalyzerProcessOutput {
            stdout: stdout.bytes,
            stderr: stderr.bytes,
            stdout_truncated: stdout.truncated,
            stderr_truncated: stderr.truncated,
            status_success: status.success(),
        })
    }
}

/// Normalizes rust-analyzer/cargo JSON messages into compact diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RustDiagnosticNormalizer {
    pub workspace_root: PathBuf,
    pub touched_files: BTreeSet<String>,
    pub max_items: usize,
}

impl RustDiagnosticNormalizer {
    fn normalize_cargo_json(&self, raw: &[u8]) -> (Vec<CodeDiagnostic>, bool) {
        let mut items = Vec::new();
        let mut truncated = false;
        for line in String::from_utf8_lossy(raw).lines() {
            if items.len() >= self.max_items {
                truncated = true;
                break;
            }
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            let Some(item) = self.normalize_cargo_message(&value) else {
                continue;
            };
            items.push(item);
        }
        items.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then(left.line.cmp(&right.line))
                .then(left.column.cmp(&right.column))
                .then(left.source.cmp(&right.source))
                .then(left.message.cmp(&right.message))
        });
        items.dedup_by(|left, right| {
            diagnostic_key_without_severity(left) == diagnostic_key_without_severity(right)
        });
        (items, truncated)
    }

    fn normalize_cargo_message(&self, value: &Value) -> Option<CodeDiagnostic> {
        if value.get("reason").and_then(Value::as_str) != Some("compiler-message") {
            return None;
        }
        let message = value.get("message")?;
        let span = primary_cargo_span(message)?;
        let file_name = span.get("file_name").and_then(Value::as_str)?;
        let path = normalize_diagnostic_path(file_name, self.workspace_root.as_path())?;
        if !self.touched_files.contains(path.as_str()) {
            return None;
        }
        let message_text = message
            .get("message")
            .and_then(Value::as_str)
            .map(redact_diagnostic_text)
            .map(|text| bound_message(text.as_str()))
            .filter(|text| !text.trim().is_empty())?;
        let code = message
            .get("code")
            .and_then(|code| code.get("code"))
            .and_then(Value::as_str)
            .map(redact_diagnostic_text)
            .filter(|value| !value.trim().is_empty());
        Some(CodeDiagnostic {
            language: CodeIntelLanguage::Rust,
            path,
            line: read_u32(span, &["line_start"]).unwrap_or(1),
            column: read_u32(span, &["column_start"]).unwrap_or(1),
            severity: message
                .get("level")
                .and_then(Value::as_str)
                .map(DiagnosticSeverity::parse)
                .unwrap_or(DiagnosticSeverity::Warning),
            code,
            message: message_text,
            source: RUST_ANALYZER_CARGO_CHECK_SOURCE.to_owned(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RustAnalyzerCaptureOutcome {
    Captured { items: Vec<CodeDiagnostic>, truncated: bool, reason_codes: Vec<String> },
    Degraded { reason_code: String, repair_hint: String },
}

impl RustAnalyzerCaptureOutcome {
    fn degraded(reason_code: &str, repair_hint: &str) -> Self {
        Self::Degraded { reason_code: reason_code.to_owned(), repair_hint: repair_hint.to_owned() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RustAnalyzerProcessOutput {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    stdout_truncated: bool,
    stderr_truncated: bool,
    status_success: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RustAnalyzerRunError {
    Spawn(String),
    MissingPipe(&'static str),
    Timeout,
    Wait(String),
    Output(String),
}

impl RustAnalyzerRunError {
    fn reason_code(&self) -> &'static str {
        match self {
            Self::Spawn(_) => "code_intel.rust.cargo_check_spawn_failed",
            Self::MissingPipe(_) => "code_intel.rust.cargo_check_pipe_failed",
            Self::Timeout => "code_intel.rust.cargo_check_timeout",
            Self::Wait(_) | Self::Output(_) => "code_intel.rust.cargo_check_failed",
        }
    }

    fn repair_hint(&self) -> String {
        match self {
            Self::Spawn(error) => format!("Failed to start cargo check for Rust diagnostics: {error}"),
            Self::MissingPipe(pipe) => format!("Failed to capture cargo check {pipe} for Rust diagnostics."),
            Self::Timeout => "Rust diagnostics timed out; increase tool_call.code_intel.timeout_ms or inspect rust-analyzer health.".to_owned(),
            Self::Wait(error) | Self::Output(error) => {
                format!("Rust diagnostics failed while reading cargo check output: {error}")
            }
        }
    }
}

/// TypeScript diagnostics provider gated by the configured language server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TypescriptLanguageServerProvider {
    pub provider: String,
    pub binary: String,
    pub check_command: String,
    pub check_args: Vec<String>,
    pub timeout_ms: u64,
    pub max_output_bytes: u64,
    pub max_items: usize,
    pub redaction_level: String,
}

impl TypescriptLanguageServerProvider {
    fn from_config(config: &CodeIntelConfig) -> Self {
        Self {
            provider: CodeIntelLanguage::TypeScript.provider_name().to_owned(),
            binary: config.typescript_server_binary.clone(),
            check_command: TYPESCRIPT_TSC_COMMAND.to_owned(),
            check_args: TYPESCRIPT_TSC_ARGS.iter().map(|arg| (*arg).to_owned()).collect(),
            timeout_ms: config.timeout_ms,
            max_output_bytes: config.max_output_bytes,
            max_items: config.max_items,
            redaction_level: crate::application::code_intel_runtime::CODE_INTEL_REDACTION_LEVEL
                .to_owned(),
        }
    }

    async fn capture(
        &self,
        workspace_root: &Path,
        touched_files: &BTreeSet<String>,
    ) -> TypescriptCaptureOutcome {
        if !executable_is_available(self.binary.as_str()) {
            return TypescriptCaptureOutcome::degraded(
                "code_intel.provider_missing.typescript",
                "Install typescript-language-server or set tool_call.code_intel.typescript_server_binary to an executable path.",
            );
        }
        if !workspace_root.is_dir() {
            return TypescriptCaptureOutcome::degraded(
                "code_intel.typescript.workspace_root_missing",
                "TypeScript diagnostics require an existing workspace root.",
            );
        }

        let project_roots = typescript_project_roots(workspace_root, touched_files);
        if project_roots.is_empty() {
            return TypescriptCaptureOutcome::degraded(
                "code_intel.typescript.project_root_missing",
                "No tsconfig.json, jsconfig.json, or package.json was found for touched TypeScript files.",
            );
        }

        let mut items = Vec::new();
        let mut truncated = false;
        let mut failed_without_items_hint = None;
        for project_root in project_roots {
            let Some(check_command) =
                resolve_typescript_check_command(project_root.as_path(), workspace_root)
            else {
                return TypescriptCaptureOutcome::degraded(
                    "code_intel.typescript.tsc_missing",
                    "Install TypeScript locally or make tsc available on PATH for language-server diagnostics.",
                );
            };
            let output =
                match self.run_tsc_no_emit(project_root.as_path(), check_command.as_path()).await {
                    Ok(output) => output,
                    Err(error) => {
                        let repair_hint = error.repair_hint();
                        return TypescriptCaptureOutcome::degraded(
                            error.reason_code(),
                            repair_hint.as_str(),
                        );
                    }
                };
            let normalizer = TypescriptDiagnosticNormalizer {
                workspace_root: workspace_root.to_path_buf(),
                touched_files: touched_files.clone(),
                max_items: self.max_items.saturating_sub(items.len()),
            };
            let combined_output = output.combined_output();
            let (mut project_items, parse_truncated) =
                normalizer.normalize_tsc_output(combined_output.as_slice());
            truncated |= parse_truncated || output.stdout_truncated || output.stderr_truncated;
            if project_items.is_empty() && !output.status_success {
                failed_without_items_hint = Some(bounded_typescript_error_hint(&output));
            }
            items.append(&mut project_items);
            if items.len() >= self.max_items {
                break;
            }
        }

        items.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then(left.line.cmp(&right.line))
                .then(left.column.cmp(&right.column))
                .then(left.source.cmp(&right.source))
                .then(left.message.cmp(&right.message))
        });
        items.dedup_by(|left, right| {
            diagnostic_key_without_severity(left) == diagnostic_key_without_severity(right)
        });
        if items.is_empty() {
            if let Some(hint) = failed_without_items_hint {
                return TypescriptCaptureOutcome::degraded(
                    "code_intel.typescript.tsc_failed",
                    hint.as_str(),
                );
            }
        }

        let mut reason_codes = vec![CODE_INTEL_TYPESCRIPT_SNAPSHOT_CAPTURED_EVENT.to_owned()];
        if truncated {
            reason_codes.push("code_intel.typescript.output_truncated".to_owned());
        }
        TypescriptCaptureOutcome::Captured { items, truncated, reason_codes }
    }

    async fn run_tsc_no_emit(
        &self,
        project_root: &Path,
        command_path: &Path,
    ) -> Result<TypescriptProcessOutput, TypescriptRunError> {
        let mut command = TokioCommand::new(command_path);
        command
            .args(TYPESCRIPT_TSC_ARGS)
            .current_dir(project_root)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| {
            TypescriptRunError::Spawn(redact_diagnostic_text(&error.to_string()))
        })?;
        let stdout = child.stdout.take().ok_or(TypescriptRunError::MissingPipe("stdout"))?;
        let stderr = child.stderr.take().ok_or(TypescriptRunError::MissingPipe("stderr"))?;
        let max_output_bytes = max_output_bytes(self.max_output_bytes);
        let stdout_task = tokio::spawn(read_bounded_stream(stdout, max_output_bytes));
        let stderr_task = tokio::spawn(read_bounded_stream(stderr, max_output_bytes));
        let status = match timeout(Duration::from_millis(self.timeout_ms), child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => {
                return Err(TypescriptRunError::Wait(redact_diagnostic_text(&error.to_string())));
            }
            Err(_) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                let _ = stdout_task.await;
                let _ = stderr_task.await;
                return Err(TypescriptRunError::Timeout);
            }
        };
        let stdout = stdout_task
            .await
            .map_err(|error| {
                TypescriptRunError::Output(redact_diagnostic_text(&error.to_string()))
            })?
            .map_err(|error| {
                TypescriptRunError::Output(redact_diagnostic_text(&error.to_string()))
            })?;
        let stderr = stderr_task
            .await
            .map_err(|error| {
                TypescriptRunError::Output(redact_diagnostic_text(&error.to_string()))
            })?
            .map_err(|error| {
                TypescriptRunError::Output(redact_diagnostic_text(&error.to_string()))
            })?;
        Ok(TypescriptProcessOutput {
            stdout: stdout.bytes,
            stderr: stderr.bytes,
            stdout_truncated: stdout.truncated,
            stderr_truncated: stderr.truncated,
            status_success: status.success(),
        })
    }
}

/// Normalizes TypeScript compiler diagnostics into compact LSP-style items.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TypescriptDiagnosticNormalizer {
    pub workspace_root: PathBuf,
    pub touched_files: BTreeSet<String>,
    pub max_items: usize,
}

impl TypescriptDiagnosticNormalizer {
    fn normalize_tsc_output(&self, raw: &[u8]) -> (Vec<CodeDiagnostic>, bool) {
        let mut items = Vec::new();
        let mut truncated = false;
        for line in String::from_utf8_lossy(raw).lines() {
            if items.len() >= self.max_items {
                truncated = true;
                break;
            }
            let Some(item) = self.normalize_tsc_line(line) else {
                continue;
            };
            items.push(item);
        }
        items.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then(left.line.cmp(&right.line))
                .then(left.column.cmp(&right.column))
                .then(left.source.cmp(&right.source))
                .then(left.message.cmp(&right.message))
        });
        items.dedup_by(|left, right| {
            diagnostic_key_without_severity(left) == diagnostic_key_without_severity(right)
        });
        (items, truncated)
    }

    fn normalize_tsc_line(&self, line: &str) -> Option<CodeDiagnostic> {
        let location = parse_typescript_location(line)?;
        let path = normalize_diagnostic_path(location.path, self.workspace_root.as_path())?;
        if !self.touched_files.contains(path.as_str()) {
            return None;
        }
        let (severity, code, message) = parse_typescript_diagnostic_body(location.body)?;
        Some(CodeDiagnostic {
            language: CodeIntelLanguage::TypeScript,
            path,
            line: location.line,
            column: location.column,
            severity,
            code,
            message,
            source: TYPESCRIPT_LANGUAGE_SERVER_TSC_SOURCE.to_owned(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TypescriptLocation<'a> {
    path: &'a str,
    line: u32,
    column: u32,
    body: &'a str,
}

fn parse_typescript_location(line: &str) -> Option<TypescriptLocation<'_>> {
    let trimmed = line.trim();
    let (location, body) = trimmed.rsplit_once("): ")?;
    let position_start = location.rfind('(')?;
    let path = location[..position_start].trim();
    let position = &location[position_start + 1..];
    let (line_raw, column_raw) = position.split_once(',')?;
    let line = line_raw.trim().parse::<u32>().ok().filter(|value| *value > 0)?;
    let column = column_raw.trim().parse::<u32>().ok().filter(|value| *value > 0)?;
    Some(TypescriptLocation { path, line, column, body })
}

fn parse_typescript_diagnostic_body(
    body: &str,
) -> Option<(DiagnosticSeverity, Option<String>, String)> {
    let (header, message) = body.split_once(':')?;
    let mut header_parts = header.split_whitespace();
    let severity = header_parts.next().map(DiagnosticSeverity::parse)?;
    let code = header_parts
        .find(|part| part.starts_with("TS"))
        .map(redact_diagnostic_text)
        .filter(|value| !value.trim().is_empty());
    let message = redact_diagnostic_text(message);
    let message = bound_message(message.as_str());
    (!message.trim().is_empty()).then_some((severity, code, message))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TypescriptCaptureOutcome {
    Captured { items: Vec<CodeDiagnostic>, truncated: bool, reason_codes: Vec<String> },
    Degraded { reason_code: String, repair_hint: String },
}

impl TypescriptCaptureOutcome {
    fn degraded(reason_code: &str, repair_hint: &str) -> Self {
        Self::Degraded { reason_code: reason_code.to_owned(), repair_hint: repair_hint.to_owned() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TypescriptProcessOutput {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    stdout_truncated: bool,
    stderr_truncated: bool,
    status_success: bool,
}

impl TypescriptProcessOutput {
    fn combined_output(&self) -> Vec<u8> {
        let mut output = self.stdout.clone();
        if !self.stderr.is_empty() {
            if !output.is_empty() {
                output.push(b'\n');
            }
            output.extend_from_slice(self.stderr.as_slice());
        }
        output
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TypescriptRunError {
    Spawn(String),
    MissingPipe(&'static str),
    Timeout,
    Wait(String),
    Output(String),
}

impl TypescriptRunError {
    fn reason_code(&self) -> &'static str {
        match self {
            Self::Spawn(_) => "code_intel.typescript.tsc_spawn_failed",
            Self::MissingPipe(_) => "code_intel.typescript.tsc_pipe_failed",
            Self::Timeout => "code_intel.typescript.tsc_timeout",
            Self::Wait(_) | Self::Output(_) => "code_intel.typescript.tsc_failed",
        }
    }

    fn repair_hint(&self) -> String {
        match self {
            Self::Spawn(error) => {
                format!("Failed to start tsc for TypeScript diagnostics: {error}")
            }
            Self::MissingPipe(pipe) => {
                format!("Failed to capture tsc {pipe} for TypeScript diagnostics.")
            }
            Self::Timeout => "TypeScript diagnostics timed out; increase tool_call.code_intel.timeout_ms or inspect typescript-language-server health.".to_owned(),
            Self::Wait(error) | Self::Output(error) => {
                format!("TypeScript diagnostics failed while reading tsc output: {error}")
            }
        }
    }
}

/// Python diagnostics provider backed by Pyright JSON output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PyrightProvider {
    pub provider: String,
    pub binary: String,
    pub check_command: String,
    pub check_args: Vec<String>,
    pub timeout_ms: u64,
    pub max_output_bytes: u64,
    pub max_items: usize,
    pub redaction_level: String,
}

impl PyrightProvider {
    fn from_config(config: &CodeIntelConfig) -> Self {
        Self {
            provider: CodeIntelLanguage::Python.provider_name().to_owned(),
            binary: config.pyright_binary.clone(),
            check_command: PYRIGHT_CLI_COMMAND.to_owned(),
            check_args: PYRIGHT_ARGS.iter().map(|arg| (*arg).to_owned()).collect(),
            timeout_ms: config.timeout_ms,
            max_output_bytes: config.max_output_bytes,
            max_items: config.max_items,
            redaction_level: crate::application::code_intel_runtime::CODE_INTEL_REDACTION_LEVEL
                .to_owned(),
        }
    }

    async fn capture(
        &self,
        workspace_root: &Path,
        touched_files: &BTreeSet<String>,
    ) -> PyrightCaptureOutcome {
        let Some(provider_command) = resolve_executable_path(self.binary.as_str()) else {
            return PyrightCaptureOutcome::degraded(
                "code_intel.provider_missing.python",
                "Install pyright-langserver or set tool_call.code_intel.pyright_binary to an executable path.",
            );
        };
        if !workspace_root.is_dir() {
            return PyrightCaptureOutcome::degraded(
                "code_intel.python.workspace_root_missing",
                "Python diagnostics require an existing workspace root.",
            );
        }
        let Some(check_command) =
            resolve_pyright_check_command(self.binary.as_str(), provider_command.as_path())
        else {
            return PyrightCaptureOutcome::degraded(
                "code_intel.python.pyright_cli_missing",
                "Install the pyright CLI next to the configured pyright-langserver or configure tool_call.code_intel.pyright_binary to a trusted pyright executable.",
            );
        };

        let output = match self
            .run_pyright_json(workspace_root, check_command.as_path(), touched_files)
            .await
        {
            Ok(output) => output,
            Err(error) => {
                let repair_hint = error.repair_hint();
                return PyrightCaptureOutcome::degraded(error.reason_code(), repair_hint.as_str());
            }
        };
        let normalizer = PyrightDiagnosticNormalizer {
            workspace_root: workspace_root.to_path_buf(),
            touched_files: touched_files.clone(),
            max_items: self.max_items,
        };
        let (items, parse_truncated) = normalizer.normalize_pyright_json(output.stdout.as_slice());
        let truncated = parse_truncated || output.stdout_truncated || output.stderr_truncated;
        if items.is_empty() && !output.status_success {
            let hint = bounded_pyright_error_hint(&output);
            return PyrightCaptureOutcome::degraded(
                "code_intel.python.pyright_failed",
                hint.as_str(),
            );
        }

        let mut reason_codes = vec![CODE_INTEL_PYTHON_SNAPSHOT_CAPTURED_EVENT.to_owned()];
        if truncated {
            reason_codes.push("code_intel.python.output_truncated".to_owned());
        }
        PyrightCaptureOutcome::Captured { items, truncated, reason_codes }
    }

    async fn run_pyright_json(
        &self,
        workspace_root: &Path,
        command_path: &Path,
        touched_files: &BTreeSet<String>,
    ) -> Result<PyrightProcessOutput, PyrightRunError> {
        let mut command = TokioCommand::new(command_path);
        command
            .args(PYRIGHT_ARGS)
            .args(touched_files)
            .current_dir(workspace_root)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|error| PyrightRunError::Spawn(redact_diagnostic_text(&error.to_string())))?;
        let stdout = child.stdout.take().ok_or(PyrightRunError::MissingPipe("stdout"))?;
        let stderr = child.stderr.take().ok_or(PyrightRunError::MissingPipe("stderr"))?;
        let max_output_bytes = max_output_bytes(self.max_output_bytes);
        let stdout_task = tokio::spawn(read_bounded_stream(stdout, max_output_bytes));
        let stderr_task = tokio::spawn(read_bounded_stream(stderr, max_output_bytes));
        let status = match timeout(Duration::from_millis(self.timeout_ms), child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => {
                return Err(PyrightRunError::Wait(redact_diagnostic_text(&error.to_string())));
            }
            Err(_) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                let _ = stdout_task.await;
                let _ = stderr_task.await;
                return Err(PyrightRunError::Timeout);
            }
        };
        let stdout = stdout_task
            .await
            .map_err(|error| PyrightRunError::Output(redact_diagnostic_text(&error.to_string())))?
            .map_err(|error| PyrightRunError::Output(redact_diagnostic_text(&error.to_string())))?;
        let stderr = stderr_task
            .await
            .map_err(|error| PyrightRunError::Output(redact_diagnostic_text(&error.to_string())))?
            .map_err(|error| PyrightRunError::Output(redact_diagnostic_text(&error.to_string())))?;
        Ok(PyrightProcessOutput {
            stdout: stdout.bytes,
            stderr: stderr.bytes,
            stdout_truncated: stdout.truncated,
            stderr_truncated: stderr.truncated,
            status_success: status.success(),
        })
    }
}

/// Normalizes Pyright JSON diagnostics into compact LSP-style items.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PyrightDiagnosticNormalizer {
    pub workspace_root: PathBuf,
    pub touched_files: BTreeSet<String>,
    pub max_items: usize,
}

impl PyrightDiagnosticNormalizer {
    fn normalize_pyright_json(&self, raw: &[u8]) -> (Vec<CodeDiagnostic>, bool) {
        let Ok(value) = serde_json::from_slice::<Value>(raw) else {
            return (Vec::new(), false);
        };
        let Some(diagnostics) = value.get("generalDiagnostics").and_then(Value::as_array) else {
            return (Vec::new(), false);
        };
        let mut items = Vec::new();
        let mut truncated = false;
        for entry in diagnostics {
            if items.len() >= self.max_items {
                truncated = true;
                break;
            }
            let Some(item) = self.normalize_pyright_entry(entry) else {
                continue;
            };
            items.push(item);
        }
        items.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then(left.line.cmp(&right.line))
                .then(left.column.cmp(&right.column))
                .then(left.source.cmp(&right.source))
                .then(left.message.cmp(&right.message))
        });
        items.dedup_by(|left, right| {
            diagnostic_key_without_severity(left) == diagnostic_key_without_severity(right)
        });
        (items, truncated)
    }

    fn normalize_pyright_entry(&self, entry: &Value) -> Option<CodeDiagnostic> {
        let file = entry.get("file").and_then(Value::as_str)?;
        let path = normalize_diagnostic_path(file, self.workspace_root.as_path())?;
        if !self.touched_files.contains(path.as_str()) {
            return None;
        }
        let range_start = entry.get("range")?.get("start")?;
        let line = read_zero_based_u32(range_start, "line").unwrap_or(0).saturating_add(1);
        let column = read_zero_based_u32(range_start, "character").unwrap_or(0).saturating_add(1);
        let message = entry
            .get("message")
            .and_then(Value::as_str)
            .map(redact_diagnostic_text)
            .map(|text| bound_message(text.as_str()))
            .filter(|value| !value.trim().is_empty())?;
        let code = entry
            .get("rule")
            .or_else(|| entry.get("code"))
            .and_then(Value::as_str)
            .map(redact_diagnostic_text)
            .filter(|value| !value.trim().is_empty());
        Some(CodeDiagnostic {
            language: CodeIntelLanguage::Python,
            path,
            line,
            column,
            severity: entry
                .get("severity")
                .and_then(Value::as_str)
                .map(DiagnosticSeverity::parse)
                .unwrap_or(DiagnosticSeverity::Warning),
            code,
            message,
            source: PYRIGHT_SOURCE.to_owned(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PyrightCaptureOutcome {
    Captured { items: Vec<CodeDiagnostic>, truncated: bool, reason_codes: Vec<String> },
    Degraded { reason_code: String, repair_hint: String },
}

impl PyrightCaptureOutcome {
    fn degraded(reason_code: &str, repair_hint: &str) -> Self {
        Self::Degraded { reason_code: reason_code.to_owned(), repair_hint: repair_hint.to_owned() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PyrightProcessOutput {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    stdout_truncated: bool,
    stderr_truncated: bool,
    status_success: bool,
}

impl PyrightProcessOutput {
    fn combined_output(&self) -> Vec<u8> {
        let mut output = self.stdout.clone();
        if !self.stderr.is_empty() {
            if !output.is_empty() {
                output.push(b'\n');
            }
            output.extend_from_slice(self.stderr.as_slice());
        }
        output
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PyrightRunError {
    Spawn(String),
    MissingPipe(&'static str),
    Timeout,
    Wait(String),
    Output(String),
}

impl PyrightRunError {
    fn reason_code(&self) -> &'static str {
        match self {
            Self::Spawn(_) => "code_intel.python.pyright_spawn_failed",
            Self::MissingPipe(_) => "code_intel.python.pyright_pipe_failed",
            Self::Timeout => "code_intel.python.pyright_timeout",
            Self::Wait(_) | Self::Output(_) => "code_intel.python.pyright_failed",
        }
    }

    fn repair_hint(&self) -> String {
        match self {
            Self::Spawn(error) => {
                format!("Failed to start pyright for Python diagnostics: {error}")
            }
            Self::MissingPipe(pipe) => {
                format!("Failed to capture pyright {pipe} for Python diagnostics.")
            }
            Self::Timeout => "Python diagnostics timed out; increase tool_call.code_intel.timeout_ms or inspect pyright health.".to_owned(),
            Self::Wait(error) | Self::Output(error) => {
                format!("Python diagnostics failed while reading pyright output: {error}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BoundedStreamOutput {
    bytes: Vec<u8>,
    truncated: bool,
}

fn provider_ready(snapshot: &DiagnosticSnapshot, language: CodeIntelLanguage) -> bool {
    snapshot
        .provider_status
        .iter()
        .any(|status| status.language == language && status.status == "ready")
}

fn set_provider_status(
    snapshot: &mut DiagnosticSnapshot,
    language: CodeIntelLanguage,
    status_value: &str,
    reason_code: &str,
    repair_hint: &str,
) {
    if let Some(status) =
        snapshot.provider_status.iter_mut().find(|status| status.language == language)
    {
        status.status = status_value.to_owned();
        status.reason_code = reason_code.to_owned();
        status.repair_hint = repair_hint.to_owned();
    }
}

fn mark_provider_degraded(
    snapshot: &mut DiagnosticSnapshot,
    language: CodeIntelLanguage,
    reason_code: &str,
    repair_hint: &str,
) {
    snapshot.degraded = true;
    snapshot.reason_codes.push(reason_code.to_owned());
    set_provider_status(snapshot, language, "degraded", reason_code, repair_hint);
}

fn primary_cargo_span(message: &Value) -> Option<&Value> {
    let spans = message.get("spans").and_then(Value::as_array)?;
    spans
        .iter()
        .find(|span| span.get("is_primary").and_then(Value::as_bool).unwrap_or(false))
        .or_else(|| spans.first())
}

async fn read_bounded_stream<R>(
    mut reader: R,
    max_bytes: usize,
) -> std::io::Result<BoundedStreamOutput>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            return Ok(BoundedStreamOutput { bytes, truncated: false });
        }
        let remaining = max_bytes.saturating_sub(bytes.len());
        if read > remaining {
            bytes.extend_from_slice(&buffer[..remaining]);
            return Ok(BoundedStreamOutput { bytes, truncated: true });
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
}

fn max_output_bytes(configured: u64) -> usize {
    usize::try_from(configured).unwrap_or(usize::MAX)
}

fn bounded_error_hint(stderr: &[u8]) -> String {
    let stderr = String::from_utf8_lossy(stderr);
    let redacted = redact_diagnostic_text(stderr.as_ref());
    let hint = bound_message_with_limit(redacted.as_str(), RUST_ANALYZER_ERROR_HINT_CHARS);
    if hint.trim().is_empty() {
        "Rust diagnostics command failed without emitting a useful stderr summary.".to_owned()
    } else {
        hint
    }
}

fn bounded_typescript_error_hint(output: &TypescriptProcessOutput) -> String {
    let combined = output.combined_output();
    let text = String::from_utf8_lossy(combined.as_slice());
    let redacted = redact_diagnostic_text(text.as_ref());
    let hint = bound_message_with_limit(redacted.as_str(), TYPESCRIPT_ERROR_HINT_CHARS);
    if hint.trim().is_empty() {
        "TypeScript diagnostics command failed without emitting a useful output summary.".to_owned()
    } else {
        hint
    }
}

fn bounded_pyright_error_hint(output: &PyrightProcessOutput) -> String {
    let combined = output.combined_output();
    let text = String::from_utf8_lossy(combined.as_slice());
    let redacted = redact_diagnostic_text(text.as_ref());
    let hint = bound_message_with_limit(redacted.as_str(), PYRIGHT_ERROR_HINT_CHARS);
    if hint.trim().is_empty() {
        "Pyright diagnostics command failed without emitting a useful output summary.".to_owned()
    } else {
        hint
    }
}

fn typescript_project_roots(
    workspace_root: &Path,
    touched_files: &BTreeSet<String>,
) -> BTreeSet<PathBuf> {
    touched_files
        .iter()
        .filter_map(|path| nearest_typescript_project_root(workspace_root, path))
        .collect()
}

fn nearest_typescript_project_root(workspace_root: &Path, relative_path: &str) -> Option<PathBuf> {
    let file_path = workspace_root.join(relative_path);
    let mut current = file_path.parent()?.to_path_buf();
    let mut package_root = None;
    loop {
        if !path_is_within_root(current.as_path(), workspace_root) {
            return None;
        }
        if current.join("tsconfig.json").is_file() || current.join("jsconfig.json").is_file() {
            return Some(current);
        }
        if package_root.is_none() && current.join("package.json").is_file() {
            package_root = Some(current.clone());
        }
        if current == workspace_root {
            break;
        }
        current = current.parent()?.to_path_buf();
    }
    package_root
}

fn resolve_typescript_check_command(project_root: &Path, workspace_root: &Path) -> Option<PathBuf> {
    for root in [project_root, workspace_root] {
        for name in executable_candidates(TYPESCRIPT_TSC_COMMAND) {
            let candidate = root.join("node_modules").join(".bin").join(name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    executable_is_available(TYPESCRIPT_TSC_COMMAND).then(|| PathBuf::from(TYPESCRIPT_TSC_COMMAND))
}

fn resolve_pyright_check_command(
    configured_binary: &str,
    configured_command: &Path,
) -> Option<PathBuf> {
    if !configured_binary.to_ascii_lowercase().contains("langserver") {
        return Some(configured_command.to_path_buf());
    }

    let parent = configured_command.parent()?;
    for name in executable_candidates(PYRIGHT_CLI_COMMAND) {
        let candidate = parent.join(name);
        if candidate.is_file() {
            return fs::canonicalize(candidate).ok();
        }
    }
    None
}

/// Parses an LSP-like JSON diagnostic payload used by provider adapters and
/// tests. Paths are normalized relative to `workspace_root`; outside paths
/// are dropped rather than leaked.
#[cfg(test)]
pub(crate) fn parse_lsp_diagnostics_json(
    raw: &str,
    language: CodeIntelLanguage,
    workspace_root: &Path,
    max_items: usize,
) -> (Vec<CodeDiagnostic>, bool) {
    let Ok(value) = serde_json::from_str::<Value>(raw) else {
        return (Vec::new(), false);
    };
    let diagnostics =
        value.get("diagnostics").and_then(Value::as_array).or_else(|| value.as_array());
    let Some(diagnostics) = diagnostics else {
        return (Vec::new(), false);
    };

    let mut items = Vec::new();
    let mut truncated = false;
    for entry in diagnostics {
        if items.len() >= max_items {
            truncated = true;
            break;
        }
        if let Some(item) = parse_lsp_diagnostic_entry(entry, language, workspace_root) {
            items.push(item);
        }
    }
    (items, truncated)
}

#[cfg(test)]
fn parse_lsp_diagnostic_entry(
    entry: &Value,
    language: CodeIntelLanguage,
    workspace_root: &Path,
) -> Option<CodeDiagnostic> {
    let path = entry
        .get("path")
        .or_else(|| entry.get("file"))
        .or_else(|| entry.get("uri"))
        .and_then(Value::as_str)?;
    let path = normalize_diagnostic_path(path, workspace_root)?;
    let line = read_u32(entry, &["line", "start_line", "range_start_line"]).unwrap_or(1);
    let column = read_u32(entry, &["column", "start_column", "range_start_column"]).unwrap_or(1);
    let severity =
        entry.get("severity").map(parse_json_severity).unwrap_or(DiagnosticSeverity::Warning);
    let code = entry
        .get("code")
        .and_then(|value| {
            value.as_str().map(str::to_owned).or_else(|| value.as_i64().map(|n| n.to_string()))
        })
        .filter(|value| !value.trim().is_empty());
    let message = entry
        .get("message")
        .and_then(Value::as_str)
        .map(bound_message)
        .filter(|value| !value.trim().is_empty())?;
    let source = entry
        .get("source")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(language.provider_name())
        .to_owned();
    Some(CodeDiagnostic { language, path, line, column, severity, code, message, source })
}

#[cfg(test)]
fn parse_json_severity(value: &Value) -> DiagnosticSeverity {
    value
        .as_str()
        .map(DiagnosticSeverity::parse)
        .or_else(|| {
            value.as_u64().map(|number| DiagnosticSeverity::parse(number.to_string().as_str()))
        })
        .unwrap_or(DiagnosticSeverity::Warning)
}

fn read_u32(entry: &Value, keys: &[&str]) -> Option<u32> {
    keys.iter().find_map(|key| {
        let value = entry.get(*key)?;
        let parsed = value.as_u64().or_else(|| value.as_str()?.trim().parse::<u64>().ok())?;
        u32::try_from(parsed).ok().filter(|value| *value > 0)
    })
}

fn read_zero_based_u32(entry: &Value, key: &str) -> Option<u32> {
    let value = entry.get(key)?;
    let parsed = value.as_u64().or_else(|| value.as_str()?.trim().parse::<u64>().ok())?;
    u32::try_from(parsed).ok()
}

fn configured_workspace_root(
    config: &CodeIntelConfig,
    workspace_roots: &[PathBuf],
) -> Option<PathBuf> {
    WorkspaceRootResolver::new(config, workspace_roots).resolve().workspace_root
}

fn normalize_touched_files(
    files_touched: &[WorkspacePatchFileAttestation],
    workspace_root: Option<&Path>,
    reason_codes: &mut Vec<String>,
) -> Vec<String> {
    files_touched
        .iter()
        .filter_map(|file| {
            normalize_workspace_relative_path(file.path.as_str()).or_else(|| {
                reason_codes.push("code_intel.path_rejected".to_owned());
                None
            })
        })
        .filter(|path| {
            workspace_root.is_none_or(|root| {
                let joined = root.join(path);
                path_is_within_root(joined.as_path(), root)
            })
        })
        .collect()
}

fn provider_status_for_descriptor(
    descriptor: LanguageServerDescriptor,
    touched_languages: &BTreeSet<CodeIntelLanguage>,
) -> CodeIntelProviderStatus {
    if !touched_languages.contains(&descriptor.language) {
        return CodeIntelProviderStatus {
            provider: descriptor.provider,
            language: descriptor.language,
            status: "skipped".to_owned(),
            binary: descriptor.binary_label,
            reason_code: format!("code_intel.provider_skipped.{}", descriptor.language.as_str()),
            repair_hint: "No touched file uses this language provider.".to_owned(),
            managed_health_authority: None,
            managed_health_snapshot_authority: None,
        };
    }
    if descriptor.integration == LanguageServerIntegration::RegistryOnly {
        return CodeIntelProviderStatus {
            provider: descriptor.provider,
            language: descriptor.language,
            status: "degraded".to_owned(),
            binary: descriptor.binary_label,
            reason_code: format!(
                "code_intel.provider_registry_only.{}",
                descriptor.language.as_str()
            ),
            repair_hint:
                "Provider is registered for routing and semantic fallback; external diagnostics are not executed by this rollout."
                    .to_owned(),
            managed_health_authority: None,
            managed_health_snapshot_authority: None,
        };
    }
    if executable_is_available(descriptor.binary.as_str()) {
        CodeIntelProviderStatus {
            provider: descriptor.provider,
            language: descriptor.language,
            status: "ready".to_owned(),
            binary: descriptor.binary_label,
            reason_code: format!("code_intel.provider_ready.{}", descriptor.language.as_str()),
            repair_hint: "Provider binary was found in the configured path.".to_owned(),
            managed_health_authority: None,
            managed_health_snapshot_authority: None,
        }
    } else {
        CodeIntelProviderStatus {
            provider: descriptor.provider,
            language: descriptor.language,
            status: "missing_binary".to_owned(),
            binary: descriptor.binary_label.clone(),
            reason_code: format!("code_intel.provider_missing.{}", descriptor.language.as_str()),
            repair_hint: format!(
                "Install '{}'{}.",
                descriptor.binary_label,
                provider_binary_config_key(descriptor.language).map_or_else(
                    || " before enabling this provider".to_owned(),
                    |key| format!(
                        " or set tool_call.code_intel.{key}_binary to an executable path"
                    ),
                )
            ),
            managed_health_authority: None,
            managed_health_snapshot_authority: None,
        }
    }
}

fn diagnostic_binary_label(binary: &str) -> String {
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

fn executable_is_available(binary: &str) -> bool {
    let candidate = Path::new(binary);
    if candidate.components().count() > 1 || candidate.is_absolute() {
        return candidate.is_file();
    }
    let Some(paths) = env::var_os("PATH") else {
        return false;
    };
    env::split_paths(&paths).any(|directory| {
        executable_candidates(binary).iter().any(|name| directory.join(name).is_file())
    })
}

/// Resolves an operator-configured executable before any child process adopts
/// a workspace-controlled current directory.
fn resolve_executable_path(binary: &str) -> Option<PathBuf> {
    let candidate = Path::new(binary);
    if candidate.components().count() > 1 || candidate.is_absolute() {
        return if candidate.is_file() { fs::canonicalize(candidate).ok() } else { None };
    }
    let paths = env::var_os("PATH")?;
    env::split_paths(&paths).find_map(|directory| {
        executable_candidates(binary).into_iter().map(|name| directory.join(name)).find_map(
            |candidate| {
                if candidate.is_file() {
                    fs::canonicalize(candidate).ok()
                } else {
                    None
                }
            },
        )
    })
}

fn executable_candidates(binary: &str) -> Vec<String> {
    if cfg!(windows) && Path::new(binary).extension().is_none() {
        let mut names = vec![binary.to_owned()];
        for suffix in [".exe", ".cmd", ".bat"] {
            names.push(format!("{binary}{suffix}"));
        }
        names
    } else {
        vec![binary.to_owned()]
    }
}

fn diagnostic_key_without_severity(item: &CodeDiagnostic) -> String {
    diagnostic_key_parts_without_severity(
        item.path.as_str(),
        item.line,
        item.column,
        item.code.as_deref(),
        item.message.as_str(),
        item.source.as_str(),
    )
}

fn diagnostic_key_without_severity_with_mapper(
    item: &CodeDiagnostic,
    range_shift_mapper: &RangeShiftMapper,
) -> String {
    let (line, column) =
        range_shift_mapper.map_after_position(item.path.as_str(), item.line, item.column);
    diagnostic_key_parts_without_severity(
        item.path.as_str(),
        line,
        column,
        item.code.as_deref(),
        item.message.as_str(),
        item.source.as_str(),
    )
}

fn diagnostic_key_parts_without_severity(
    path: &str,
    line: u32,
    column: u32,
    code: Option<&str>,
    message: &str,
    source: &str,
) -> String {
    format!("{}\0{}\0{}\0{}\0{}\0{}", path, line, column, code.unwrap_or(""), message, source)
}

fn normalize_diagnostic_path(path: &str, workspace_root: &Path) -> Option<String> {
    let trimmed = path.trim().strip_prefix("file://").unwrap_or(path.trim());
    let candidate = PathBuf::from(trimmed);
    if candidate.is_absolute() {
        let relative = candidate.strip_prefix(workspace_root).ok()?;
        normalize_workspace_relative_path(normalize_path_for_output(relative).as_str())
    } else {
        normalize_workspace_relative_path(trimmed)
    }
}

fn normalize_workspace_relative_path(path: &str) -> Option<String> {
    let candidate = Path::new(path.trim());
    if candidate.as_os_str().is_empty() || candidate.is_absolute() {
        return None;
    }
    let mut parts = Vec::new();
    for component in candidate.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().to_string()),
            Component::CurDir => {}
            _ => return None,
        }
    }
    (!parts.is_empty()).then(|| parts.join("/"))
}

fn path_is_within_root(path: &Path, root: &Path) -> bool {
    let normalized_path = path.components().collect::<Vec<_>>();
    let normalized_root = root.components().collect::<Vec<_>>();
    normalized_path.starts_with(normalized_root.as_slice())
}

fn normalize_path_for_output(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn bound_message(message: &str) -> String {
    bound_message_with_limit(message, MAX_DIAGNOSTIC_MESSAGE_CHARS)
}

fn bound_message_with_limit(message: &str, max_chars: usize) -> String {
    let trimmed = message.trim();
    let mut bounded = trimmed.chars().take(max_chars).collect::<String>();
    if trimmed.chars().count() > max_chars {
        bounded.push_str("...");
    }
    bounded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn touched(path: &str) -> WorkspacePatchFileAttestation {
        WorkspacePatchFileAttestation {
            path: path.to_owned(),
            workspace_root_index: 0,
            operation: "update".to_owned(),
            moved_from: None,
            before_sha256: None,
            before_size_bytes: None,
            after_sha256: None,
            after_size_bytes: None,
        }
    }

    #[test]
    fn disabled_snapshot_is_noop_with_provider_status() {
        let snapshot = capture_diagnostic_snapshot(
            &CodeIntelConfig::default(),
            &[PathBuf::from("workspace")],
            &[touched("src/lib.rs")],
        );
        assert!(!snapshot.enabled);
        assert_eq!(snapshot.reason_codes, vec!["code_intel.disabled"]);
        assert_eq!(snapshot.provider_status.len(), CodeIntelLanguage::ALL.len());
    }

    #[test]
    fn read_only_diagnostics_never_admit_provider_execution() {
        let workspace = tempfile::tempdir().expect("workspace should be created");
        let source_dir = workspace.path().join("src");
        std::fs::create_dir_all(source_dir.as_path()).expect("source directory should be created");
        std::fs::write(source_dir.join("lib.rs"), "pub fn value() -> u8 { 1 }\n")
            .expect("source file should be written");
        let config = CodeIntelConfig {
            enabled: true,
            rust_analyzer_binary: std::env::current_exe()
                .expect("test executable path should resolve")
                .to_string_lossy()
                .into_owned(),
            ..CodeIntelConfig::default()
        };

        let snapshot = capture_read_only_diagnostic_snapshot(
            &config,
            &[workspace.path().to_path_buf()],
            &[touched("src/lib.rs")],
        );

        let rust_status = snapshot
            .provider_status
            .iter()
            .find(|status| status.language == CodeIntelLanguage::Rust)
            .expect("rust provider status should be present");
        assert_eq!(rust_status.status, "degraded");
        assert_eq!(rust_status.reason_code, CODE_INTEL_READ_ONLY_DIAGNOSTICS_REASON);
        assert!(snapshot.items.is_empty());
        assert!(snapshot.degraded);
        assert!(snapshot
            .reason_codes
            .iter()
            .any(|reason| { reason.as_str() == CODE_INTEL_READ_ONLY_DIAGNOSTICS_REASON }));
    }

    #[test]
    fn direct_diagnostics_late_generation_suppresses_raw_provider_data() {
        let snapshot = DiagnosticSnapshot {
            schema_version: CODE_INTEL_SCHEMA_VERSION,
            enabled: true,
            workspace_root: Some("workspace".to_owned()),
            files: vec!["src/lib.rs".to_owned(), "apps/web/src/app.ts".to_owned()],
            provider_status: vec![
                CodeIntelProviderStatus {
                    provider: "rust-analyzer".to_owned(),
                    language: CodeIntelLanguage::Rust,
                    status: "failed".to_owned(),
                    binary: "rust-analyzer".to_owned(),
                    reason_code: "code_intel.rust.stale-secret-reason".to_owned(),
                    repair_hint: "stale-secret-repair".to_owned(),
                    managed_health_authority: None,
                    managed_health_snapshot_authority: Some(
                        CodeIntelProviderSnapshotAuthority::Authoritative,
                    ),
                },
                CodeIntelProviderStatus {
                    provider: "typescript-language-server".to_owned(),
                    language: CodeIntelLanguage::TypeScript,
                    status: "ready".to_owned(),
                    binary: "typescript-language-server".to_owned(),
                    reason_code: CODE_INTEL_TYPESCRIPT_SNAPSHOT_CAPTURED_EVENT.to_owned(),
                    repair_hint: "current provider".to_owned(),
                    managed_health_authority: None,
                    managed_health_snapshot_authority: Some(
                        CodeIntelProviderSnapshotAuthority::Authoritative,
                    ),
                },
            ],
            items: vec![
                CodeDiagnostic {
                    language: CodeIntelLanguage::Rust,
                    path: "src/lib.rs".to_owned(),
                    line: 1,
                    column: 1,
                    severity: DiagnosticSeverity::Error,
                    code: Some("stale-secret-code".to_owned()),
                    message: "stale-secret-message".to_owned(),
                    source: "stale-secret-provider".to_owned(),
                },
                CodeDiagnostic {
                    language: CodeIntelLanguage::TypeScript,
                    path: "apps/web/src/app.ts".to_owned(),
                    line: 2,
                    column: 1,
                    severity: DiagnosticSeverity::Warning,
                    code: Some("TS1".to_owned()),
                    message: "current warning".to_owned(),
                    source: TYPESCRIPT_LANGUAGE_SERVER_TSC_SOURCE.to_owned(),
                },
            ],
            truncated: false,
            degraded: true,
            reason_codes: vec![
                "code_intel.rust.stale-secret-reason".to_owned(),
                CODE_INTEL_TYPESCRIPT_SNAPSHOT_CAPTURED_EVENT.to_owned(),
            ],
        };
        let runtime_snapshot = {
            let mut runtime = crate::application::code_intel_runtime::CodeIntelRuntime::new();
            runtime.snapshot(
                crate::application::code_intel_runtime::CodeIntelRuntimeSnapshotRequest {
                    enabled: true,
                    workspace_root: Some("workspace"),
                    timeout_ms: 2_000,
                    idle_reap_ms: 60_000,
                    now_unix_ms: 100,
                },
            )
        };
        let runtime_authority = BTreeMap::from([
            (CodeIntelLanguage::Rust, CodeIntelProviderSnapshotAuthority::Stale),
            (CodeIntelLanguage::TypeScript, CodeIntelProviderSnapshotAuthority::Authoritative),
        ]);

        let output = project_code_intel_diagnostics_output(
            snapshot,
            runtime_snapshot,
            &runtime_authority,
            &[],
        );

        assert_eq!(output["execution_mode"], "passive");
        assert_eq!(output["snapshot"]["provider_status"].as_array().map(Vec::len), Some(1));
        assert_eq!(output["snapshot"]["provider_status"][0]["language"], "type_script");
        assert_eq!(output["snapshot"]["items"].as_array().map(Vec::len), Some(1));
        assert_eq!(output["snapshot"]["items"][0]["language"], "type_script");
        assert_eq!(output["snapshot"]["degraded"], false);
        let serialized =
            serde_json::to_string(&output).expect("direct diagnostics should serialize");
        assert!(!serialized.contains("stale-secret"));
    }

    #[test]
    fn language_server_registry_redacts_binary_paths_and_lists_supported_languages() {
        let config = CodeIntelConfig {
            enabled: true,
            rust_analyzer_binary: "tools/rust-analyzer".to_owned(),
            ..CodeIntelConfig::default()
        };
        let manager = LspProcessManager::from_config(&config);
        let descriptors = manager.registry.descriptors();
        let rust = descriptors
            .iter()
            .find(|descriptor| descriptor.language == CodeIntelLanguage::Rust)
            .expect("rust descriptor should be registered");
        let statuses = manager.provider_statuses(&BTreeSet::from([CodeIntelLanguage::Rust]));
        let rust_status = statuses
            .iter()
            .find(|status| status.language == CodeIntelLanguage::Rust)
            .expect("rust status should exist");

        assert_eq!(descriptors.len(), CodeIntelLanguage::ALL.len());
        assert!(rust.diagnostics_only);
        assert_eq!(rust.binary_label, "rust-analyzer");
        assert_eq!(rust_status.binary, "rust-analyzer");
        assert!(rust.supports_symbols);
        assert!(rust.supports_references);
        assert!(!rust_status.repair_hint.contains("tools/"));
    }

    #[test]
    fn registry_only_language_degrades_to_explicit_fallback() {
        let config = CodeIntelConfig { enabled: true, ..CodeIntelConfig::default() };
        let manager = LspProcessManager::from_config(&config);
        let statuses = manager.provider_statuses(&BTreeSet::from([CodeIntelLanguage::Go]));
        let go_status = statuses
            .iter()
            .find(|status| status.language == CodeIntelLanguage::Go)
            .expect("go status should exist");

        assert_eq!(go_status.status, "degraded");
        assert_eq!(go_status.reason_code, "code_intel.provider_registry_only.go");
        assert!(go_status.repair_hint.contains("semantic fallback"));
    }

    #[test]
    fn lexical_symbol_extractor_supports_primary_and_fallback_languages() {
        let rust_fixture = include_str!("../../../../../fixtures/code-intel/rust/src/lib.rs");
        let typescript_fixture =
            include_str!("../../../../../fixtures/code-intel/typescript/src/widget.ts");
        let python_fixture =
            include_str!("../../../../../fixtures/code-intel/python/src/widget.py");
        let javascript_fixture =
            include_str!("../../../../../fixtures/code-intel/javascript/src/widget.js");
        let go_fixture = include_str!("../../../../../fixtures/code-intel/go/widget.go");
        let java_fixture = include_str!("../../../../../fixtures/code-intel/java/src/Widget.java");

        let (rust_symbols, rust_truncated) = extract_symbols_from_source(
            Some(CodeIntelLanguage::Rust),
            "src/lib.rs",
            rust_fixture,
            16,
        );
        let (typescript_symbols, typescript_truncated) = extract_symbols_from_source(
            Some(CodeIntelLanguage::TypeScript),
            "src/widget.ts",
            typescript_fixture,
            16,
        );
        let (python_symbols, python_truncated) = extract_symbols_from_source(
            Some(CodeIntelLanguage::Python),
            "src/widget.py",
            python_fixture,
            16,
        );
        let (javascript_symbols, javascript_truncated) = extract_symbols_from_source(
            Some(CodeIntelLanguage::JavaScript),
            "src/widget.js",
            javascript_fixture,
            16,
        );
        let (go_symbols, go_truncated) =
            extract_symbols_from_source(Some(CodeIntelLanguage::Go), "widget.go", go_fixture, 16);
        let (java_symbols, java_truncated) = extract_symbols_from_source(
            Some(CodeIntelLanguage::Java),
            "src/Widget.java",
            java_fixture,
            16,
        );

        assert!(!rust_truncated);
        assert!(!typescript_truncated);
        assert!(!python_truncated);
        assert!(!javascript_truncated);
        assert!(!go_truncated);
        assert!(!java_truncated);
        assert!(rust_symbols.iter().any(|symbol| symbol.name == "build_widget"));
        assert!(typescript_symbols.iter().any(|symbol| symbol.name == "buildWidget"));
        assert!(python_symbols.iter().any(|symbol| symbol.name == "build_widget"));
        assert!(javascript_symbols.iter().any(|symbol| symbol.name == "buildWidget"));
        assert!(go_symbols.iter().any(|symbol| symbol.name == "BuildWidget"));
        assert!(java_symbols.iter().any(|symbol| symbol.name == "Widget"));
    }

    #[test]
    fn semantic_context_bounds_symbol_segments_with_refs() {
        let symbols = (0..40)
            .map(|index| CodeSymbol {
                name: format!("symbol_{index}"),
                kind: "function".to_owned(),
                language: CodeIntelLanguage::Rust,
                path: "src/lib.rs".to_owned(),
                line: index + 1,
                column: 1,
                signature: format!("fn symbol_{index}()"),
                visibility: "private".to_owned(),
                source_ref: format!("src/lib.rs:{}:1", index + 1),
            })
            .collect::<Vec<_>>();

        let context = CodeSemanticContextProvider::from_symbols(symbols.as_slice(), false);

        assert_eq!(context.symbols.len(), CODE_INTEL_CONTEXT_SYMBOL_LIMIT);
        assert!(context.truncated);
        assert_eq!(context.source_refs.len(), CODE_INTEL_CONTEXT_SYMBOL_LIMIT);
        assert_eq!(context.cache_policy, "volatile_workspace_snapshot");
    }

    #[test]
    fn runtime_cwd_hints_detect_manifests() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        std::fs::create_dir_all(temp.path().join("crates/example"))
            .expect("fixture directory should be created");
        std::fs::write(temp.path().join("Cargo.toml"), "[workspace]\n")
            .expect("root manifest should be written");
        std::fs::write(temp.path().join("crates/example/Cargo.toml"), "[package]\n")
            .expect("crate manifest should be written");
        let roots = vec![(0, temp.path().to_path_buf())];

        let hints = detect_runtime_cwd_hints(roots.as_slice());

        assert!(hints.iter().any(|hint| hint.manifest_path == "Cargo.toml"));
        assert!(hints.iter().any(|hint| hint.cwd == "crates/example"));
    }

    #[test]
    fn patch_impact_marks_public_symbol_changes_medium_risk() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        std::fs::create_dir_all(temp.path().join("src"))
            .expect("source directory should be created");
        std::fs::write(temp.path().join("src/lib.rs"), "pub fn build_widget() {}\n")
            .expect("source file should be written");
        let files = vec![touched("src/lib.rs")];
        let before = DiagnosticSnapshot {
            schema_version: CODE_INTEL_SCHEMA_VERSION,
            enabled: true,
            workspace_root: Some("workspace".to_owned()),
            files: vec!["src/lib.rs".to_owned()],
            provider_status: Vec::new(),
            items: Vec::new(),
            truncated: false,
            degraded: false,
            reason_codes: Vec::new(),
        };
        let after = before.clone();
        let delta = DiagnosticDelta {
            schema_version: CODE_INTEL_SCHEMA_VERSION,
            enabled: true,
            new_errors: 0,
            new_warnings: 0,
            items: Vec::new(),
            truncated: false,
            provider_status: Vec::new(),
            degraded: false,
            reason_codes: Vec::new(),
        };

        let impact = patch_impact_analysis(
            &[temp.path().to_path_buf()],
            files.as_slice(),
            &before,
            &after,
            &delta,
        );

        assert_eq!(impact.risk_level, "medium");
        assert!(impact.touched_symbols.iter().any(|symbol| symbol.name == "build_widget"));
        assert!(impact.verification_guidance.iter().any(|guidance| guidance.contains("Rust")));
    }

    #[test]
    fn workspace_root_resolver_rejects_out_of_scope_config_root() {
        let config = CodeIntelConfig {
            enabled: true,
            workspace_root: Some(PathBuf::from("../outside")),
            ..CodeIntelConfig::default()
        };
        let snapshot = capture_diagnostic_snapshot(
            &config,
            &[PathBuf::from("workspace")],
            &[touched("src/lib.rs")],
        );

        assert_eq!(snapshot.workspace_root.as_deref(), Some("workspace"));
        assert!(snapshot
            .reason_codes
            .iter()
            .any(|code| code == "code_intel.workspace_root_rejected"));
    }

    #[test]
    fn range_shift_mapper_maps_after_patch_lines_to_before_positions() {
        let mapper = RangeShiftMapper::new(vec![RangeShift {
            path: "src/lib.rs".to_owned(),
            start_line: 10,
            old_line_count: 1,
            new_line_count: 3,
        }]);

        assert_eq!(mapper.map_after_position("src/lib.rs", 9, 2), (9, 2));
        assert_eq!(mapper.map_after_position("src/lib.rs", 14, 4), (12, 4));
        assert_eq!(mapper.map_after_position("src/other.rs", 14, 4), (14, 4));
    }

    #[test]
    fn enabled_snapshot_rejects_paths_outside_workspace_shape() {
        let config = CodeIntelConfig { enabled: true, ..CodeIntelConfig::default() };
        let snapshot = capture_diagnostic_snapshot(
            &config,
            &[PathBuf::from("workspace")],
            &[touched("../outside.rs"), touched("src/lib.rs")],
        );
        assert_eq!(snapshot.files, vec!["src/lib.rs"]);
        assert!(snapshot.reason_codes.iter().any(|code| code == "code_intel.path_rejected"));
    }

    #[test]
    fn lsp_json_parser_normalizes_relative_paths_and_bounds_items() {
        let raw = r#"{
            "diagnostics": [
                {"path":"src/lib.rs","line":2,"column":4,"severity":"error","code":"E0425","message":"cannot find value","source":"rust-analyzer"},
                {"path":"../secret.rs","line":1,"column":1,"severity":"warning","message":"hidden"}
            ]
        }"#;
        let (items, truncated) =
            parse_lsp_diagnostics_json(raw, CodeIntelLanguage::Rust, Path::new("workspace"), 8);
        assert!(!truncated);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].path, "src/lib.rs");
        assert_eq!(items[0].severity, DiagnosticSeverity::Error);
    }

    #[test]
    fn rust_diagnostic_normalizer_filters_touched_files() {
        let raw = br#"{"reason":"compiler-message","message":{"message":"expected expression","code":{"code":"E0425"},"level":"error","spans":[{"file_name":"src/lib.rs","line_start":3,"column_start":9,"is_primary":true}]}}
{"reason":"compiler-message","message":{"message":"unrelated warning","code":{"code":"unused"},"level":"warning","spans":[{"file_name":"src/other.rs","line_start":1,"column_start":1,"is_primary":true}]}}"#;
        let normalizer = RustDiagnosticNormalizer {
            workspace_root: PathBuf::from("workspace"),
            touched_files: BTreeSet::from(["src/lib.rs".to_owned()]),
            max_items: 8,
        };

        let (items, truncated) = normalizer.normalize_cargo_json(raw);

        assert!(!truncated);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].path, "src/lib.rs");
        assert_eq!(items[0].line, 3);
        assert_eq!(items[0].column, 9);
        assert_eq!(items[0].severity, DiagnosticSeverity::Error);
        assert_eq!(items[0].source, RUST_ANALYZER_CARGO_CHECK_SOURCE);
    }

    #[test]
    fn typescript_diagnostic_normalizer_filters_touched_files() {
        let raw = b"apps/web/src/App.tsx(12,8): error TS2304: Cannot find name 'missing'.\napps/web/src/Other.ts(1,1): warning TS6133: 'unused' is declared but its value is never read.";
        let normalizer = TypescriptDiagnosticNormalizer {
            workspace_root: PathBuf::from("workspace"),
            touched_files: BTreeSet::from(["apps/web/src/App.tsx".to_owned()]),
            max_items: 8,
        };

        let (items, truncated) = normalizer.normalize_tsc_output(raw);

        assert!(!truncated);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].language, CodeIntelLanguage::TypeScript);
        assert_eq!(items[0].path, "apps/web/src/App.tsx");
        assert_eq!(items[0].line, 12);
        assert_eq!(items[0].column, 8);
        assert_eq!(items[0].severity, DiagnosticSeverity::Error);
        assert_eq!(items[0].code.as_deref(), Some("TS2304"));
        assert_eq!(items[0].source, TYPESCRIPT_LANGUAGE_SERVER_TSC_SOURCE);
    }

    #[test]
    fn pyright_check_command_stays_with_configured_provider_installation() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let provider_bin = tempdir.path().join("provider-bin");
        let workspace_bin = tempdir.path().join("workspace").join("node_modules").join(".bin");
        fs::create_dir_all(provider_bin.as_path()).expect("provider bin should be created");
        fs::create_dir_all(workspace_bin.as_path()).expect("workspace bin should be created");
        let executable_suffix = if cfg!(windows) { ".exe" } else { "" };
        let configured_binary = provider_bin.join(format!("pyright-langserver{executable_suffix}"));
        let workspace_pyright = workspace_bin.join(format!("pyright{executable_suffix}"));
        fs::write(configured_binary.as_path(), b"provider")
            .expect("configured provider should exist");
        fs::write(workspace_pyright.as_path(), b"workspace-controlled")
            .expect("workspace pyright should exist");
        let configured_command = resolve_executable_path(
            configured_binary.to_str().expect("configured provider path should be UTF-8"),
        )
        .expect("configured provider should resolve");

        assert!(
            resolve_pyright_check_command(
                configured_binary.to_str().expect("configured provider path should be UTF-8"),
                configured_command.as_path(),
            )
            .is_none(),
            "workspace-local pyright must not satisfy the configured provider"
        );

        let trusted_pyright = provider_bin.join(format!("pyright{executable_suffix}"));
        fs::write(trusted_pyright.as_path(), b"trusted")
            .expect("trusted pyright sibling should exist");
        assert_eq!(
            resolve_pyright_check_command(
                configured_binary.to_str().expect("configured provider path should be UTF-8"),
                configured_command.as_path(),
            ),
            Some(
                fs::canonicalize(trusted_pyright)
                    .expect("trusted pyright sibling should canonicalize")
            )
        );
    }

    #[test]
    fn pyright_diagnostic_normalizer_filters_touched_files() {
        let raw = br#"{
            "generalDiagnostics": [
                {
                    "file": "src/app.py",
                    "severity": "error",
                    "message": "\"missing\" is not defined",
                    "range": {"start": {"line": 4, "character": 8}},
                    "rule": "reportUndefinedVariable"
                },
                {
                    "file": "src/other.py",
                    "severity": "warning",
                    "message": "Import is not accessed",
                    "range": {"start": {"line": 1, "character": 0}},
                    "rule": "reportUnusedImport"
                }
            ]
        }"#;
        let normalizer = PyrightDiagnosticNormalizer {
            workspace_root: PathBuf::from("workspace"),
            touched_files: BTreeSet::from(["src/app.py".to_owned()]),
            max_items: 8,
        };

        let (items, truncated) = normalizer.normalize_pyright_json(raw);

        assert!(!truncated);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].language, CodeIntelLanguage::Python);
        assert_eq!(items[0].path, "src/app.py");
        assert_eq!(items[0].line, 5);
        assert_eq!(items[0].column, 9);
        assert_eq!(items[0].severity, DiagnosticSeverity::Error);
        assert_eq!(items[0].code.as_deref(), Some("reportUndefinedVariable"));
        assert_eq!(items[0].source, PYRIGHT_SOURCE);
    }

    #[test]
    fn rust_diagnostic_delta_reports_new_syntax_error() {
        let config = CodeIntelConfig { enabled: true, max_items: 8, ..CodeIntelConfig::default() };
        let before = DiagnosticSnapshot {
            schema_version: CODE_INTEL_SCHEMA_VERSION,
            enabled: true,
            workspace_root: Some("workspace".to_owned()),
            files: vec!["src/lib.rs".to_owned()],
            provider_status: Vec::new(),
            items: Vec::new(),
            truncated: false,
            degraded: false,
            reason_codes: vec![CODE_INTEL_RUST_SNAPSHOT_CAPTURED_EVENT.to_owned()],
        };
        let after = DiagnosticSnapshot {
            items: vec![CodeDiagnostic {
                language: CodeIntelLanguage::Rust,
                path: "src/lib.rs".to_owned(),
                line: 3,
                column: 9,
                severity: DiagnosticSeverity::Error,
                code: Some("E0425".to_owned()),
                message: "expected expression".to_owned(),
                source: RUST_ANALYZER_CARGO_CHECK_SOURCE.to_owned(),
            }],
            ..before.clone()
        };

        let delta = diagnostic_delta(&config, &before, &after);

        assert_eq!(delta.new_errors, 1);
        assert_eq!(delta.items.len(), 1);
        assert_eq!(delta.items[0].code.as_deref(), Some("E0425"));
    }

    #[test]
    fn typescript_diagnostic_delta_reports_new_error() {
        let config = CodeIntelConfig { enabled: true, max_items: 8, ..CodeIntelConfig::default() };
        let before = DiagnosticSnapshot {
            schema_version: CODE_INTEL_SCHEMA_VERSION,
            enabled: true,
            workspace_root: Some("workspace".to_owned()),
            files: vec!["apps/web/src/App.tsx".to_owned()],
            provider_status: Vec::new(),
            items: Vec::new(),
            truncated: false,
            degraded: false,
            reason_codes: vec![CODE_INTEL_TYPESCRIPT_SNAPSHOT_CAPTURED_EVENT.to_owned()],
        };
        let after = DiagnosticSnapshot {
            items: vec![CodeDiagnostic {
                language: CodeIntelLanguage::TypeScript,
                path: "apps/web/src/App.tsx".to_owned(),
                line: 12,
                column: 8,
                severity: DiagnosticSeverity::Error,
                code: Some("TS2304".to_owned()),
                message: "Cannot find name 'missing'.".to_owned(),
                source: TYPESCRIPT_LANGUAGE_SERVER_TSC_SOURCE.to_owned(),
            }],
            ..before.clone()
        };

        let delta = diagnostic_delta(&config, &before, &after);

        assert_eq!(delta.new_errors, 1);
        assert_eq!(delta.items.len(), 1);
        assert_eq!(delta.items[0].code.as_deref(), Some("TS2304"));
    }

    #[test]
    fn python_diagnostic_delta_reports_new_error() {
        let config = CodeIntelConfig { enabled: true, max_items: 8, ..CodeIntelConfig::default() };
        let before = DiagnosticSnapshot {
            schema_version: CODE_INTEL_SCHEMA_VERSION,
            enabled: true,
            workspace_root: Some("workspace".to_owned()),
            files: vec!["src/app.py".to_owned()],
            provider_status: Vec::new(),
            items: Vec::new(),
            truncated: false,
            degraded: false,
            reason_codes: vec![CODE_INTEL_PYTHON_SNAPSHOT_CAPTURED_EVENT.to_owned()],
        };
        let after = DiagnosticSnapshot {
            items: vec![CodeDiagnostic {
                language: CodeIntelLanguage::Python,
                path: "src/app.py".to_owned(),
                line: 5,
                column: 9,
                severity: DiagnosticSeverity::Error,
                code: Some("reportUndefinedVariable".to_owned()),
                message: "\"missing\" is not defined".to_owned(),
                source: PYRIGHT_SOURCE.to_owned(),
            }],
            ..before.clone()
        };

        let delta = diagnostic_delta(&config, &before, &after);

        assert_eq!(delta.new_errors, 1);
        assert_eq!(delta.items.len(), 1);
        assert_eq!(delta.items[0].code.as_deref(), Some("reportUndefinedVariable"));
    }

    #[tokio::test]
    async fn missing_rust_analyzer_degrades_without_failing_snapshot() {
        let config = CodeIntelConfig {
            enabled: true,
            rust_analyzer_binary: "palyra-rust-analyzer-missing-for-test".to_owned(),
            ..CodeIntelConfig::default()
        };

        let snapshot = capture_diagnostic_snapshot_with_providers(
            &config,
            &[PathBuf::from("workspace")],
            &[touched("src/lib.rs")],
        )
        .await;

        let rust_status = snapshot
            .provider_status
            .iter()
            .find(|status| status.language == CodeIntelLanguage::Rust)
            .expect("rust provider status should be present");
        assert_eq!(rust_status.status, "missing_binary");
        assert!(snapshot.items.is_empty());
    }

    #[tokio::test]
    async fn missing_typescript_language_server_degrades_without_failing_snapshot() {
        let config = CodeIntelConfig {
            enabled: true,
            typescript_server_binary: "palyra-typescript-language-server-missing-for-test"
                .to_owned(),
            ..CodeIntelConfig::default()
        };

        let snapshot = capture_diagnostic_snapshot_with_providers(
            &config,
            &[PathBuf::from("workspace")],
            &[touched("apps/web/src/App.tsx")],
        )
        .await;

        let typescript_status = snapshot
            .provider_status
            .iter()
            .find(|status| status.language == CodeIntelLanguage::TypeScript)
            .expect("typescript provider status should be present");
        assert_eq!(typescript_status.status, "missing_binary");
        assert!(snapshot.items.is_empty());
    }

    #[tokio::test]
    async fn missing_pyright_degrades_without_failing_snapshot() {
        let config = CodeIntelConfig {
            enabled: true,
            pyright_binary: "palyra-pyright-missing-for-test".to_owned(),
            ..CodeIntelConfig::default()
        };

        let snapshot = capture_diagnostic_snapshot_with_providers(
            &config,
            &[PathBuf::from("workspace")],
            &[touched("src/app.py")],
        )
        .await;

        let python_status = snapshot
            .provider_status
            .iter()
            .find(|status| status.language == CodeIntelLanguage::Python)
            .expect("python provider status should be present");
        assert_eq!(python_status.status, "missing_binary");
        assert!(snapshot.items.is_empty());
    }

    #[test]
    fn diagnostic_delta_returns_new_and_worse_items_for_touched_files() {
        let config = CodeIntelConfig { enabled: true, max_items: 8, ..CodeIntelConfig::default() };
        let before = DiagnosticSnapshot {
            schema_version: CODE_INTEL_SCHEMA_VERSION,
            enabled: true,
            workspace_root: Some("workspace".to_owned()),
            files: vec!["src/lib.rs".to_owned()],
            provider_status: Vec::new(),
            items: vec![CodeDiagnostic {
                language: CodeIntelLanguage::Rust,
                path: "src/lib.rs".to_owned(),
                line: 1,
                column: 1,
                severity: DiagnosticSeverity::Warning,
                code: Some("E0001".to_owned()),
                message: "same issue".to_owned(),
                source: "rust-analyzer".to_owned(),
            }],
            truncated: false,
            degraded: false,
            reason_codes: Vec::new(),
        };
        let mut after = before.clone();
        after.items = vec![
            CodeDiagnostic { severity: DiagnosticSeverity::Error, ..before.items[0].clone() },
            CodeDiagnostic {
                language: CodeIntelLanguage::TypeScript,
                path: "web/app.ts".to_owned(),
                line: 1,
                column: 1,
                severity: DiagnosticSeverity::Error,
                code: Some("TS2304".to_owned()),
                message: "cannot find name".to_owned(),
                source: "typescript-language-server".to_owned(),
            },
        ];
        after.files = vec!["src/lib.rs".to_owned()];

        let delta = diagnostic_delta(&config, &before, &after);
        assert_eq!(delta.new_errors, 1);
        assert_eq!(delta.items.len(), 1);
        assert_eq!(delta.items[0].path, "src/lib.rs");
    }

    #[test]
    fn diagnostic_delta_maps_after_lines_to_baseline_positions() {
        let config = CodeIntelConfig { enabled: true, max_items: 8, ..CodeIntelConfig::default() };
        let before = DiagnosticSnapshot {
            schema_version: CODE_INTEL_SCHEMA_VERSION,
            enabled: true,
            workspace_root: Some("workspace".to_owned()),
            files: vec!["src/lib.rs".to_owned()],
            provider_status: Vec::new(),
            items: vec![CodeDiagnostic {
                language: CodeIntelLanguage::Rust,
                path: "src/lib.rs".to_owned(),
                line: 12,
                column: 4,
                severity: DiagnosticSeverity::Warning,
                code: Some("unused".to_owned()),
                message: "same issue".to_owned(),
                source: "rust-analyzer".to_owned(),
            }],
            truncated: false,
            degraded: false,
            reason_codes: Vec::new(),
        };
        let after = DiagnosticSnapshot {
            items: vec![CodeDiagnostic { line: 14, ..before.items[0].clone() }],
            ..before.clone()
        };

        let delta = diagnostic_delta_with_range_shifts(
            &config,
            &before,
            &after,
            vec![RangeShift {
                path: "src/lib.rs".to_owned(),
                start_line: 10,
                old_line_count: 1,
                new_line_count: 3,
            }],
        );

        assert_eq!(delta.new_warnings, 0);
        assert!(delta.items.is_empty());
    }

    #[test]
    fn diagnostic_delta_caps_items_and_marks_truncated() {
        let config = CodeIntelConfig { enabled: true, max_items: 1, ..CodeIntelConfig::default() };
        let before = DiagnosticSnapshot {
            schema_version: CODE_INTEL_SCHEMA_VERSION,
            enabled: true,
            workspace_root: Some("workspace".to_owned()),
            files: vec!["src/lib.rs".to_owned()],
            provider_status: Vec::new(),
            items: Vec::new(),
            truncated: false,
            degraded: false,
            reason_codes: Vec::new(),
        };
        let after = DiagnosticSnapshot {
            items: vec![
                CodeDiagnostic {
                    language: CodeIntelLanguage::Rust,
                    path: "src/lib.rs".to_owned(),
                    line: 1,
                    column: 1,
                    severity: DiagnosticSeverity::Error,
                    code: Some("E1".to_owned()),
                    message: "first".to_owned(),
                    source: "rust-analyzer".to_owned(),
                },
                CodeDiagnostic {
                    language: CodeIntelLanguage::Rust,
                    path: "src/lib.rs".to_owned(),
                    line: 2,
                    column: 1,
                    severity: DiagnosticSeverity::Warning,
                    code: Some("E2".to_owned()),
                    message: "second".to_owned(),
                    source: "rust-analyzer".to_owned(),
                },
            ],
            ..before.clone()
        };

        let delta = diagnostic_delta(&config, &before, &after);
        assert!(delta.truncated);
        assert!(delta.degraded);
        assert_eq!(delta.items.len(), 1);
    }
}
