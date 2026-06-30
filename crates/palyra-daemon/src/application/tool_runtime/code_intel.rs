//! Bounded code-diagnostics adapter for workspace mutations.
//!
//! The adapter is intentionally read-only: it validates touched paths against
//! the configured workspace, probes configured provider binaries, normalizes
//! diagnostics into workspace-relative paths, and computes before/after deltas.
//! Provider process orchestration can grow behind this contract without
//! changing the `palyra.fs.apply_patch` output shape.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    path::{Component, Path, PathBuf},
    process::Stdio,
};

use palyra_common::{
    redaction::redact_diagnostic_text, workspace_patch::WorkspacePatchFileAttestation,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command as TokioCommand,
    time::{timeout, Duration},
};

use crate::{
    application::code_intel_runtime::{
        CodeIntelLanguage, CodeIntelProviderObservation, CodeIntelRuntimeSnapshot,
    },
    config::CodeIntelConfig,
};

const CODE_INTEL_SCHEMA_VERSION: u32 = 1;
const MAX_DIAGNOSTIC_MESSAGE_CHARS: usize = 320;
const RUST_ANALYZER_CARGO_CHECK_SOURCE: &str = "rust-analyzer/cargo-check";
const RUST_ANALYZER_CARGO_CHECK_COMMAND: &str = "cargo";
const RUST_ANALYZER_CARGO_CHECK_ARGS: &[&str] =
    &["check", "--quiet", "--workspace", "--message-format=json", "--all-targets", "--keep-going"];
const RUST_ANALYZER_ERROR_HINT_CHARS: usize = 512;
pub(crate) const CODE_INTEL_RUST_SNAPSHOT_CAPTURED_EVENT: &str =
    "code_intel.rust.snapshot_captured";

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
    if !config.enabled {
        return DiagnosticSnapshot {
            schema_version: CODE_INTEL_SCHEMA_VERSION,
            enabled: false,
            workspace_root: None,
            files: Vec::new(),
            provider_status: disabled_provider_statuses(config),
            items: Vec::new(),
            truncated: false,
            degraded: false,
            reason_codes: vec!["code_intel.disabled".to_owned()],
        };
    }

    let workspace_root = configured_workspace_root(config, workspace_roots);
    let mut reason_codes = Vec::new();
    let mut files =
        normalize_touched_files(files_touched, workspace_root.as_deref(), &mut reason_codes);
    files.sort();
    files.dedup();

    let languages =
        files.iter().filter_map(|path| CodeIntelLanguage::from_path(path)).collect::<BTreeSet<_>>();

    let provider_status = provider_statuses_for_languages(config, &languages);
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
pub(crate) async fn capture_diagnostic_snapshot_with_providers(
    config: &CodeIntelConfig,
    workspace_roots: &[PathBuf],
    files_touched: &[WorkspacePatchFileAttestation],
) -> DiagnosticSnapshot {
    let mut snapshot = capture_diagnostic_snapshot(config, workspace_roots, files_touched);
    if !snapshot.enabled {
        return snapshot;
    }
    let rust_files = snapshot
        .files
        .iter()
        .filter(|path| CodeIntelLanguage::from_path(path) == Some(CodeIntelLanguage::Rust))
        .cloned()
        .collect::<BTreeSet<_>>();
    if rust_files.is_empty() || !provider_ready(&snapshot, CodeIntelLanguage::Rust) {
        return snapshot;
    }
    let Some(workspace_root) = configured_workspace_root(config, workspace_roots) else {
        mark_provider_degraded(
            &mut snapshot,
            CodeIntelLanguage::Rust,
            "code_intel.workspace_root_missing",
            "No workspace root was available for Rust diagnostics.",
        );
        return snapshot;
    };
    let provider = RustAnalyzerProvider::from_config(config);
    match provider.capture(workspace_root.as_path(), &rust_files).await {
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
        let previous = before_severity_by_key.get(&diagnostic_key_without_severity(item));
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
            CodeIntelProviderObservation::from_status_fields(
                status.provider.as_str(),
                status.language,
                status.status.as_str(),
                status.binary.as_str(),
                status.reason_code.as_str(),
                status.repair_hint.as_str(),
            )
        })
        .collect()
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

fn configured_workspace_root(
    config: &CodeIntelConfig,
    workspace_roots: &[PathBuf],
) -> Option<PathBuf> {
    config.workspace_root.clone().or_else(|| workspace_roots.first().cloned())
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

fn provider_statuses_for_languages(
    config: &CodeIntelConfig,
    touched_languages: &BTreeSet<CodeIntelLanguage>,
) -> Vec<CodeIntelProviderStatus> {
    [
        (CodeIntelLanguage::Rust, config.rust_analyzer_binary.as_str()),
        (CodeIntelLanguage::TypeScript, config.typescript_server_binary.as_str()),
        (CodeIntelLanguage::Python, config.pyright_binary.as_str()),
    ]
    .into_iter()
    .map(|(language, binary)| {
        if !touched_languages.contains(&language) {
            return CodeIntelProviderStatus {
                provider: language.provider_name().to_owned(),
                language,
                status: "skipped".to_owned(),
                binary: binary.to_owned(),
                reason_code: format!("code_intel.provider_skipped.{}", language.as_str()),
                repair_hint: "No touched file uses this language provider.".to_owned(),
            };
        }
        if executable_is_available(binary) {
            CodeIntelProviderStatus {
                provider: language.provider_name().to_owned(),
                language,
                status: "ready".to_owned(),
                binary: binary.to_owned(),
                reason_code: format!("code_intel.provider_ready.{}", language.as_str()),
                repair_hint: "Provider binary was found in the configured path.".to_owned(),
            }
        } else {
            CodeIntelProviderStatus {
                provider: language.provider_name().to_owned(),
                language,
                status: "missing_binary".to_owned(),
                binary: binary.to_owned(),
                reason_code: format!("code_intel.provider_missing.{}", language.as_str()),
                repair_hint: format!(
                    "Install '{}' or set tool_call.code_intel.{}_binary to an executable path.",
                    binary,
                    match language {
                        CodeIntelLanguage::Rust => "rust_analyzer",
                        CodeIntelLanguage::TypeScript => "typescript_server",
                        CodeIntelLanguage::Python => "pyright",
                    }
                ),
            }
        }
    })
    .collect()
}

fn disabled_provider_statuses(config: &CodeIntelConfig) -> Vec<CodeIntelProviderStatus> {
    [
        (CodeIntelLanguage::Rust, config.rust_analyzer_binary.as_str()),
        (CodeIntelLanguage::TypeScript, config.typescript_server_binary.as_str()),
        (CodeIntelLanguage::Python, config.pyright_binary.as_str()),
    ]
    .into_iter()
    .map(|(language, binary)| CodeIntelProviderStatus {
        provider: language.provider_name().to_owned(),
        language,
        status: "disabled".to_owned(),
        binary: binary.to_owned(),
        reason_code: "code_intel.disabled".to_owned(),
        repair_hint: "Set tool_call.code_intel.enabled=true to enable post-write diagnostics."
            .to_owned(),
    })
    .collect()
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
    format!(
        "{}\0{}\0{}\0{}\0{}\0{}",
        item.path,
        item.line,
        item.column,
        item.code.as_deref().unwrap_or(""),
        item.message,
        item.source
    )
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
        assert_eq!(snapshot.provider_status.len(), 3);
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
