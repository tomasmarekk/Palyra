use serde::{Deserialize, Serialize};

use super::super::lsp_workspace_supervisor::LspLanguageV2;

/// Stable schema for persistent LSP document synchronization and diagnostic deltas.
pub const LSP_DOCUMENT_SYNC_SCHEMA_VERSION: u32 = 2;

/// One immutable owner-scoped artifact reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticsArtifactRefV2 {
    /// Opaque artifact identity.
    pub artifact_id: String,
    /// SHA-256 of the exact artifact bytes.
    pub sha256: String,
    /// Exact serialized byte count.
    pub byte_count: u64,
    /// Stable media type.
    pub content_type: String,
}

/// LSP range using the protocol's zero-based positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticRangeV2 {
    /// Inclusive start line.
    pub start_line: u32,
    /// Inclusive start character.
    pub start_character: u32,
    /// Exclusive end line.
    pub end_line: u32,
    /// Exclusive end character.
    pub end_character: u32,
}

/// Normalized diagnostic severity ordered from most to least blocking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverityV2 {
    /// Compilation or type error.
    Error,
    /// Warning.
    Warning,
    /// Informational finding.
    Information,
    /// Hint.
    Hint,
}

/// Bounded model-visible diagnostic projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedDiagnosticV2 {
    /// Workspace-relative source path.
    pub relative_path: String,
    /// Stable identity that excludes position so line shifts remain comparable.
    pub identity_sha256: String,
    /// Current LSP range.
    pub range: DiagnosticRangeV2,
    /// Severity.
    pub severity: DiagnosticSeverityV2,
    /// Optional provider code.
    pub code: Option<String>,
    /// Optional provider label.
    pub source: Option<String>,
    /// Bounded human-readable message.
    pub message: String,
    /// Whether any model-visible string was shortened.
    pub text_truncated: bool,
}

/// Preexisting diagnostic paired across an edit, including any range shift.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnchangedDiagnosticV2 {
    /// Baseline projection.
    pub before: NormalizedDiagnosticV2,
    /// Post-edit projection.
    pub after: NormalizedDiagnosticV2,
    /// Signed line movement.
    pub line_shift: i64,
    /// Signed character movement when the start line is unchanged.
    pub character_shift: i64,
}

/// Document generation captured in a baseline or delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticDocumentGenerationV2 {
    /// Workspace-relative source path.
    pub relative_path: String,
    /// Hash of the full document URI.
    pub uri_sha256: String,
    /// LSP document version.
    pub document_version: i64,
    /// Number of full diagnostics in the owner-only artifact.
    pub diagnostic_count: usize,
    /// Hash of the raw diagnostics array.
    pub diagnostics_sha256: String,
}

/// Durable descriptor for diagnostics immediately before an edit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticsBaselineDescriptorV2 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Opaque baseline identity.
    pub baseline_id: String,
    /// Exact server handle.
    pub handle_id: String,
    /// Exact server generation.
    pub server_generation: u64,
    /// Captured document generations.
    pub documents: Vec<DiagnosticDocumentGenerationV2>,
    /// Full baseline diagnostics.
    pub artifact: DiagnosticsArtifactRefV2,
    /// Capture timestamp.
    pub created_at_unix_ms: i64,
    /// Stable evidence reason.
    pub reason_code: String,
}

/// Terminal status of a diagnostics verification attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsDeltaStatusV2 {
    /// No blocking diagnostics were introduced.
    Verified,
    /// At least one new error was introduced.
    BlockingDiagnostics,
    /// A server did not publish diagnostics before the bounded deadline.
    DiagnosticsTimedOut,
    /// The baseline and result do not share one server generation.
    ServerGenerationChanged,
    /// LSP verification was unavailable and an explicit CLI fallback is required.
    FallbackRequired,
}

/// Explicit non-LSP verification tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsFallbackToolV2 {
    /// Rust compiler diagnostics through `cargo check`.
    CargoCheck,
    /// TypeScript compiler diagnostics without emit.
    TscNoEmit,
    /// Python type diagnostics through Pyright.
    Pyright,
}

impl DiagnosticsFallbackToolV2 {
    /// Stable operator-facing command label. Arguments remain host policy.
    pub const fn command_label(self) -> &'static str {
        match self {
            Self::CargoCheck => "cargo check",
            Self::TscNoEmit => "tsc --noEmit",
            Self::Pyright => "pyright",
        }
    }
}

/// Explicit fallback recommendation without arbitrary caller-supplied commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticsFallbackPlanV2 {
    /// Host-selected fallback.
    pub tool: DiagnosticsFallbackToolV2,
    /// Human-readable command label.
    pub command_label: String,
    /// Stable degradation reason.
    pub reason_code: String,
}

/// Generation-aware diagnostics delta returned by a patch workflow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticsDeltaV2 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Baseline identity.
    pub baseline_id: String,
    /// Exact server handle.
    pub handle_id: String,
    /// Baseline server generation.
    pub baseline_server_generation: u64,
    /// Observed post-edit server generation, when available.
    pub result_server_generation: Option<u64>,
    /// Terminal verification status.
    pub status: DiagnosticsDeltaStatusV2,
    /// Post-edit document generations.
    pub documents: Vec<DiagnosticDocumentGenerationV2>,
    /// New diagnostics, bounded for the model-visible result.
    pub introduced: Vec<NormalizedDiagnosticV2>,
    /// Resolved diagnostics, bounded for the model-visible result.
    pub resolved: Vec<NormalizedDiagnosticV2>,
    /// Preexisting diagnostics, bounded for the model-visible result.
    pub unchanged: Vec<UnchangedDiagnosticV2>,
    /// Full introduced count.
    pub introduced_count: usize,
    /// Full resolved count.
    pub resolved_count: usize,
    /// Full unchanged count.
    pub unchanged_count: usize,
    /// Full count of introduced errors.
    pub blocking_introduced_count: usize,
    /// Whether model-visible collections were capped.
    pub truncated: bool,
    /// Full baseline and post-edit diagnostics plus classifications.
    pub full_diagnostics_artifact: Option<DiagnosticsArtifactRefV2>,
    /// Explicit fallback when LSP evidence is unavailable.
    pub fallback: Option<DiagnosticsFallbackPlanV2>,
    /// Stable redacted reasons.
    pub reason_codes: Vec<String>,
}

impl DiagnosticsDeltaV2 {
    /// Whether this delta is sufficient positive verification evidence.
    #[must_use]
    pub const fn verified(&self) -> bool {
        matches!(self.status, DiagnosticsDeltaStatusV2::Verified)
    }
}

/// Current synchronized document state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LspDocumentStateV2 {
    /// Exact server handle.
    pub handle_id: String,
    /// Exact server generation.
    pub server_generation: u64,
    /// Language policy.
    pub language: LspLanguageV2,
    /// Workspace-relative source path.
    pub relative_path: String,
    /// Hash of the full document URI.
    pub uri_sha256: String,
    /// Current document version.
    pub document_version: i64,
    /// Version of the latest diagnostics notification.
    pub diagnostics_version: Option<i64>,
    /// Latest diagnostic count.
    pub diagnostic_count: usize,
    /// Stable state reason.
    pub reason_code: String,
}

/// Result of synchronizing rollback content back into live LSP state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LspRollbackOutcomeV2 {
    /// Exact server generation.
    pub server_generation: u64,
    /// Documents synchronized after rollback.
    pub documents: Vec<LspDocumentStateV2>,
    /// Whether all documents produced exact-version diagnostics.
    pub synchronized: bool,
    /// Stable result reason.
    pub reason_code: String,
}

/// Maps a supported language to its explicit compiler fallback.
#[must_use]
pub const fn fallback_tool_for_language(language: LspLanguageV2) -> DiagnosticsFallbackToolV2 {
    match language {
        LspLanguageV2::Rust => DiagnosticsFallbackToolV2::CargoCheck,
        LspLanguageV2::TypeScript => DiagnosticsFallbackToolV2::TscNoEmit,
        LspLanguageV2::Python => DiagnosticsFallbackToolV2::Pyright,
    }
}
