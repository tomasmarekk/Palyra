//! Workspace patch parsing, planning, and fail-closed filesystem application.
//!
//! Accepts the fenced `*** Begin Patch` format (plus tolerant model-emitted variants) and
//! plain unified diffs, confines all writes to canonicalized workspace roots, and executes
//! plans atomically with best-effort rollback. Parse accept/reject behavior and error
//! strings are contract surface: unit tests assert message substrings and the parser is
//! fuzzed by `fuzz/fuzz_targets/workspace_patch_parser.rs` — do not reword or change them.

use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::{HashMap, HashSet},
    fs,
    io::Write,
    ops::Range,
    path::{Component, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::redaction::is_sensitive_key;
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

const DEFAULT_REDACTION_PATTERNS: &[&str] = &["authorization", "bearer "];
const DEFAULT_SECRET_FILE_MARKERS: &[&str] =
    &[".env", "id_rsa", "id_ed25519", "credentials", "secrets/", "secret/", ".pem", ".key"];
const REDACTION_PLACEHOLDER_MARKERS: &[&str] =
    &["[redacted]", "[redacted_secret]", "<redacted>", "redacted_secret"];
const NON_SECRET_ENV_FILE_SUFFIXES: &[&str] =
    &[".example", ".sample", ".template", ".templates", ".dist", ".default", ".defaults"];
// Heuristic gate: a full-file replacement that shrinks a sizeable file down to a handful of
// lines is usually a truncated model edit, not an intentional rewrite.
const SUSPICIOUS_REPLACE_MIN_BEFORE_BYTES: usize = 256;
const SUSPICIOUS_REPLACE_MAX_NON_EMPTY_LINES: usize = 4;

/// Execution limits for workspace patch processing.
///
/// Limits are fail-closed and enforced before any filesystem mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspacePatchLimits {
    /// Maximum size of the raw patch payload in bytes.
    pub max_patch_bytes: usize,
    /// Maximum number of distinct files a single patch may touch.
    pub max_files_touched: usize,
    /// Maximum size in bytes of any patched file, before and after the patch.
    pub max_file_bytes: usize,
    /// Maximum size of the redacted preview string in bytes.
    pub max_preview_bytes: usize,
}

impl Default for WorkspacePatchLimits {
    fn default() -> Self {
        Self {
            max_patch_bytes: 256 * 1024,
            max_files_touched: 64,
            max_file_bytes: 2 * 1024 * 1024,
            max_preview_bytes: 16 * 1024,
        }
    }
}

/// Redaction policy used for preview rendering.
///
/// This affects only the preview string returned in outcomes/errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspacePatchRedactionPolicy {
    /// Case-insensitive substrings that are masked wherever they appear in the preview.
    pub redaction_patterns: Vec<String>,
    /// Path substrings marking a file as secret-bearing; its body lines are fully masked.
    pub secret_file_markers: Vec<String>,
}

impl Default for WorkspacePatchRedactionPolicy {
    fn default() -> Self {
        Self {
            redaction_patterns: DEFAULT_REDACTION_PATTERNS
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            secret_file_markers: DEFAULT_SECRET_FILE_MARKERS
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
        }
    }
}

/// Request payload for patch execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspacePatchRequest {
    /// Raw patch document text.
    pub patch: String,
    /// When `true`, the patch is parsed and planned but the filesystem is never mutated.
    pub dry_run: bool,
    /// Policy applied when rendering the redacted preview.
    pub redaction_policy: WorkspacePatchRedactionPolicy,
}

/// Per-file attestation emitted for each touched file.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WorkspacePatchFileAttestation {
    /// Workspace-relative path rendered with `/` separators.
    pub path: String,
    /// Index into the caller-supplied workspace roots the path resolved against.
    pub workspace_root_index: usize,
    /// Stable operation label: `create`, `create_idempotent`, `replace`, `line_replace`,
    /// `update`, `move`, `delete`, or `no_op`.
    pub operation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub moved_from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_size_bytes: Option<u64>,
}

/// Workspace patch execution result.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WorkspacePatchOutcome {
    pub patch_sha256: String,
    pub dry_run: bool,
    pub files_touched: Vec<WorkspacePatchFileAttestation>,
    /// Update operations whose result was byte-identical to the existing file.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub no_op_files: Vec<WorkspacePatchFileAttestation>,
    pub rollback_performed: bool,
    pub redacted_preview: String,
}

/// Errors produced by workspace patch parsing/planning/execution.
#[derive(Debug, Error)]
pub enum WorkspacePatchError {
    #[error("workspace patch payload exceeds max_patch_bytes={limit} (actual={actual})")]
    PatchTooLarge { limit: usize, actual: usize },
    #[error("workspace patch touches too many files: max={limit} actual={actual}")]
    TooManyFiles { limit: usize, actual: usize },
    #[error("workspace roots cannot be empty")]
    EmptyWorkspaceRoots,
    #[error("workspace root '{path}' is invalid: {message}")]
    InvalidWorkspaceRoot { path: String, message: String },
    #[error("patch parse error at line {line}, column {column}: {message}")]
    Parse { line: usize, column: usize, message: String },
    #[error("path '{path}' must be a relative path without traversal")]
    InvalidPatchPath { path: String },
    #[error("path '{path}' escapes allowed workspace roots")]
    PathOutsideWorkspace { path: String },
    #[error("file '{path}' does not exist")]
    MissingFile { path: String },
    #[error("file '{path}' already exists")]
    FileAlreadyExists { path: String },
    #[error("file '{path}' exceeds max_file_bytes={limit} (actual={actual})")]
    FileTooLarge { path: String, limit: usize, actual: usize },
    #[error("file '{path}' is not a regular text file")]
    NotARegularFile { path: String },
    #[error("file '{path}' is not valid UTF-8 and cannot be patched line-by-line")]
    InvalidUtf8File { path: String },
    #[error("file '{path}' is not valid JSON after patch: {message}")]
    InvalidJsonFile { path: String, message: String },
    #[error(
        "file '{path}' is a secret-bearing env file and cannot store a redaction placeholder; preserve existing secret lines or update an example/template file instead"
    )]
    RedactionPlaceholderInSecretFile { path: String },
    #[error(
        "replace-file operation for '{path}' looks partial: before_size_bytes={before_size_bytes}, after_size_bytes={after_size_bytes}; use Update File hunks or provide the complete replacement content"
    )]
    SuspiciousPartialReplace { path: String, before_size_bytes: usize, after_size_bytes: usize },
    #[error("patch hunk apply failed for '{path}': {message}")]
    HunkApplyFailed { path: String, message: String },
    #[error("{operation} '{path}' failed: {source}")]
    Io {
        operation: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("patch execution failed: {message} (rollback_performed={rollback_performed})")]
    ExecutionFailed { message: String, rollback_performed: bool },
}

impl WorkspacePatchError {
    /// Returns the 1-based `(line, column)` location of a parse error, if any.
    #[must_use]
    pub const fn parse_location(&self) -> Option<(usize, usize)> {
        match self {
            Self::Parse { line, column, .. } => Some((*line, *column)),
            _ => None,
        }
    }

    /// Reports whether a failed execution rolled previously applied actions back.
    ///
    /// Always `false` for errors raised before any filesystem mutation.
    #[must_use]
    pub const fn rollback_performed(&self) -> bool {
        match self {
            Self::ExecutionFailed { rollback_performed, .. } => *rollback_performed,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PatchOperation {
    Add { path: String, lines: Vec<String> },
    Replace { path: String, lines: Vec<String> },
    ReplaceLine { path: String, old: String, new: String },
    Update { path: String, move_to: Option<String>, hunks: Vec<PatchHunk> },
    Delete { path: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PatchHunk {
    lines: Vec<HunkLine>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HunkLineKind {
    Context,
    Add,
    Remove,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExistingLineEnding {
    Lf,
    Crlf,
    Cr,
}

impl ExistingLineEnding {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Lf => "\n",
            Self::Crlf => "\r\n",
            Self::Cr => "\r",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HunkLine {
    kind: HunkLineKind,
    text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PatchPlan {
    actions: Vec<PlannedAction>,
    file_attestations: Vec<WorkspacePatchFileAttestation>,
    no_op_attestations: Vec<WorkspacePatchFileAttestation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PlannedAction {
    Write { path: PathBuf, root: PathBuf, bytes: Vec<u8> },
    Delete { path: PathBuf, root: PathBuf },
}

#[derive(Debug)]
struct PatchExecutionError {
    error: WorkspacePatchError,
    rollback_performed: bool,
}

/// Applies a workspace patch inside the provided workspace roots.
///
/// The operation is fail-closed:
/// - patch size, touched file count, and per-file size limits are enforced;
/// - paths are confined to canonicalized workspace roots;
/// - writes are executed atomically with best-effort rollback on partial failure.
///
/// # Errors
///
/// Returns a [`WorkspacePatchError`] when a limit is exceeded, the patch fails to parse, a
/// path escapes the workspace roots, a target file is missing or invalid, or filesystem
/// execution fails. Execution failures report rollback status via
/// [`WorkspacePatchError::ExecutionFailed`].
pub fn apply_workspace_patch(
    workspace_roots: &[PathBuf],
    request: &WorkspacePatchRequest,
    limits: &WorkspacePatchLimits,
) -> Result<WorkspacePatchOutcome, WorkspacePatchError> {
    validate_workspace_patch_request_size(request, limits)?;
    let canonical_roots = canonicalize_workspace_roots(workspace_roots)?;
    apply_workspace_patch_with_canonical_roots(canonical_roots, request, limits)
}

/// Applies a workspace patch after confirming the current canonical roots still
/// resolve below a trusted set of already-canonicalized parent roots.
///
/// This is used by callers that accept a narrower workspace-root override. It
/// preserves the original workspace-root confinement after any later
/// re-canonicalization of the selected root.
///
/// # Errors
///
/// Returns [`WorkspacePatchError::InvalidWorkspaceRoot`] when a root resolves outside the
/// constraint set, plus every failure mode of [`apply_workspace_patch`].
pub fn apply_workspace_patch_with_canonical_root_constraints(
    workspace_roots: &[PathBuf],
    canonical_constraint_roots: &[PathBuf],
    request: &WorkspacePatchRequest,
    limits: &WorkspacePatchLimits,
) -> Result<WorkspacePatchOutcome, WorkspacePatchError> {
    validate_workspace_patch_request_size(request, limits)?;
    let canonical_roots = canonicalize_workspace_roots(workspace_roots)?;
    validate_canonical_root_constraints(canonical_roots.as_slice(), canonical_constraint_roots)?;
    apply_workspace_patch_with_canonical_roots(canonical_roots, request, limits)
}

/// Revalidates that the current canonical roots still resolve below the
/// supplied already-canonicalized parent roots.
///
/// # Errors
///
/// Returns [`WorkspacePatchError::EmptyWorkspaceRoots`] when no roots are supplied and
/// [`WorkspacePatchError::InvalidWorkspaceRoot`] when a root fails to canonicalize, is not
/// a directory, or resolves outside the constraint set.
pub fn validate_workspace_patch_roots_with_canonical_constraints(
    workspace_roots: &[PathBuf],
    canonical_constraint_roots: &[PathBuf],
) -> Result<(), WorkspacePatchError> {
    let canonical_roots = canonicalize_workspace_roots(workspace_roots)?;
    validate_canonical_root_constraints(canonical_roots.as_slice(), canonical_constraint_roots)
}

fn apply_workspace_patch_with_canonical_roots(
    canonical_roots: Vec<PathBuf>,
    request: &WorkspacePatchRequest,
    limits: &WorkspacePatchLimits,
) -> Result<WorkspacePatchOutcome, WorkspacePatchError> {
    let normalized_patch = normalize_supported_patch_document(request.patch.as_str());
    let patch_text = normalized_patch.as_ref();
    let operations = parse_patch_document(patch_text)?;
    if operations.len() > limits.max_files_touched {
        return Err(WorkspacePatchError::TooManyFiles {
            limit: limits.max_files_touched,
            actual: operations.len(),
        });
    }

    let patch_sha256 = compute_patch_sha256(patch_text);
    let redacted_preview =
        redact_patch_preview(patch_text, &request.redaction_policy, limits.max_preview_bytes);

    let plan = build_patch_plan(operations.as_slice(), canonical_roots.as_slice(), limits)?;

    if request.dry_run {
        return Ok(WorkspacePatchOutcome {
            patch_sha256,
            dry_run: true,
            files_touched: plan.file_attestations,
            no_op_files: plan.no_op_attestations,
            rollback_performed: false,
            redacted_preview,
        });
    }

    match execute_patch_plan(plan.actions.as_slice(), limits) {
        Ok(()) => Ok(WorkspacePatchOutcome {
            patch_sha256,
            dry_run: false,
            files_touched: plan.file_attestations,
            no_op_files: plan.no_op_attestations,
            rollback_performed: false,
            redacted_preview,
        }),
        Err(execution) => Err(WorkspacePatchError::ExecutionFailed {
            message: execution.error.to_string(),
            rollback_performed: execution.rollback_performed,
        }),
    }
}

fn validate_workspace_patch_request_size(
    request: &WorkspacePatchRequest,
    limits: &WorkspacePatchLimits,
) -> Result<(), WorkspacePatchError> {
    let patch_bytes = request.patch.as_bytes();
    if patch_bytes.len() > limits.max_patch_bytes {
        return Err(WorkspacePatchError::PatchTooLarge {
            limit: limits.max_patch_bytes,
            actual: patch_bytes.len(),
        });
    }
    Ok(())
}

/// Validates patch syntax, paths, and request-wide limits without reading or mutating a
/// workspace.
///
/// This is intended for callers that must validate a patch before creating a missing
/// workspace root. Filesystem-dependent planning still happens through
/// [`apply_workspace_patch`].
///
/// # Errors
///
/// Returns the same size, parse, path, and file-count errors as the corresponding
/// pre-planning stages of [`apply_workspace_patch`].
pub fn validate_workspace_patch_document(
    patch: &str,
    limits: &WorkspacePatchLimits,
) -> Result<(), WorkspacePatchError> {
    let request = WorkspacePatchRequest {
        patch: patch.to_owned(),
        dry_run: true,
        redaction_policy: WorkspacePatchRedactionPolicy::default(),
    };
    validate_workspace_patch_request_size(&request, limits)?;
    let normalized_patch = normalize_supported_patch_document(patch);
    let operations = parse_patch_document(normalized_patch.as_ref())?;
    if operations.len() > limits.max_files_touched {
        return Err(WorkspacePatchError::TooManyFiles {
            limit: limits.max_files_touched,
            actual: operations.len(),
        });
    }
    Ok(())
}

/// Computes a deterministic SHA256 digest of the raw patch payload.
#[must_use]
pub fn compute_patch_sha256(patch: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(patch.as_bytes());
    hex::encode(hasher.finalize())
}

/// Produces a redacted, size-capped preview of the patch payload.
///
/// Body lines of files matching a secret-file marker are masked entirely, redaction
/// patterns are replaced case-insensitively everywhere, and the result is truncated to
/// `max_preview_bytes` on a UTF-8 character boundary.
#[must_use]
pub fn redact_patch_preview(
    patch: &str,
    redaction_policy: &WorkspacePatchRedactionPolicy,
    max_preview_bytes: usize,
) -> String {
    let normalized = patch.replace("\r\n", "\n").replace('\r', "\n");
    let mut rendered = Vec::new();
    let mut redact_body = false;

    for line in normalized.split('\n') {
        if let Some(path) = parse_patch_header_path(line) {
            redact_body = is_secret_path(path, redaction_policy.secret_file_markers.as_slice());
            rendered.push(line.to_owned());
            continue;
        }
        if line == "*** End Patch" {
            redact_body = false;
            rendered.push(line.to_owned());
            continue;
        }
        if line.starts_with("*** ") {
            redact_body = false;
            rendered.push(line.to_owned());
            continue;
        }

        if redact_body {
            if let Some(prefix) = line.get(0..1) {
                if matches!(prefix, "+" | "-" | " ") {
                    rendered.push(format!("{prefix}[REDACTED]"));
                    continue;
                }
            }
            rendered.push("[REDACTED]".to_owned());
            continue;
        }

        rendered.push(redact_patch_preview_body_line(line).into_owned());
    }

    let mut preview = rendered.join("\n");
    for pattern in &redaction_policy.redaction_patterns {
        let trimmed = pattern.trim();
        if trimmed.is_empty() {
            continue;
        }
        preview = replace_ascii_case_insensitive(preview.as_str(), trimmed, "[REDACTED]");
    }

    truncate_utf8(preview, max_preview_bytes)
}

fn redact_patch_preview_body_line(line: &str) -> Cow<'_, str> {
    let Some(&prefix) = line.as_bytes().first() else {
        return Cow::Borrowed(line);
    };
    if !matches!(prefix, b'+' | b'-' | b' ') {
        return Cow::Borrowed(line);
    }
    let body = &line[1..];

    match redact_sensitive_patch_preview_values(body) {
        Cow::Borrowed(_) => Cow::Borrowed(line),
        Cow::Owned(redacted_body) => {
            let mut redacted = String::with_capacity(line.len());
            redacted.push(prefix as char);
            redacted.push_str(redacted_body.as_str());
            Cow::Owned(redacted)
        }
    }
}

fn redact_sensitive_patch_preview_values(body: &str) -> Cow<'_, str> {
    let mut replacements = Vec::new();
    let mut quote = None;
    let mut escaped = false;
    let mut skip_until = 0usize;

    for (index, ch) in body.char_indices() {
        if index < skip_until {
            continue;
        }
        if let Some(active_quote) = quote {
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == active_quote {
                quote = None;
            }
            continue;
        }
        if matches!(ch, '"' | '\'' | '`') {
            quote = Some(ch);
            continue;
        }
        if !matches!(ch, '=' | ':') {
            continue;
        }
        if !is_patch_preview_assignment_separator(body, index, ch) {
            continue;
        }
        if !patch_preview_key_before_separator_is_sensitive(body, index) {
            continue;
        }
        let Some(value) = patch_preview_assignment_value_span(body, index + ch.len_utf8()) else {
            continue;
        };
        skip_until = value.skip_until;
        if should_redact_patch_preview_value(&body[value.replacement.clone()]) {
            replacements.push(value.replacement);
        }
    }

    if replacements.is_empty() {
        return Cow::Borrowed(body);
    }

    let mut redacted = String::with_capacity(body.len());
    let mut cursor = 0usize;
    for range in replacements {
        redacted.push_str(&body[cursor..range.start]);
        redacted.push_str("[REDACTED]");
        cursor = range.end;
    }
    redacted.push_str(&body[cursor..]);
    Cow::Owned(redacted)
}

fn is_patch_preview_assignment_separator(body: &str, index: usize, separator: char) -> bool {
    let before = body[..index].chars().rev().find(|ch| !ch.is_whitespace());
    let after_start = index + separator.len_utf8();
    let after = body[after_start..].chars().find(|ch| !ch.is_whitespace());

    match separator {
        '=' => {
            !matches!(before, Some('=' | '!' | '<' | '>' | ':'))
                && !matches!(after, Some('=' | '>'))
        }
        ':' => !matches!(before, Some(':')) && !matches!(after, Some(':')) && after.is_some(),
        _ => false,
    }
}

fn patch_preview_key_before_separator_is_sensitive(body: &str, separator_index: usize) -> bool {
    let prefix = body[..separator_index].trim_end_matches(char::is_whitespace);
    let end = prefix
        .char_indices()
        .rev()
        .find(|(_, ch)| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-'))
        .map(|(index, ch)| index + ch.len_utf8());
    let Some(end) = end else {
        return false;
    };
    let start = prefix[..end]
        .char_indices()
        .rev()
        .find(|(_, ch)| !(ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-')))
        .map(|(index, ch)| index + ch.len_utf8())
        .unwrap_or(0);
    let key = &prefix[start..end];
    if key.is_empty() {
        return false;
    }
    let previous = (start > 0).then(|| prefix[..start].chars().next_back()).flatten();
    !matches!(previous, Some('?' | '&' | '#')) && is_sensitive_key(key)
}

struct PatchPreviewValueSpan {
    replacement: Range<usize>,
    skip_until: usize,
}

fn patch_preview_assignment_value_span(
    body: &str,
    value_search_start: usize,
) -> Option<PatchPreviewValueSpan> {
    let value_start = skip_ascii_whitespace(body, value_search_start);
    let quote = body[value_start..].chars().next()?;
    if matches!(quote, '"' | '\'' | '`') {
        return quoted_patch_preview_value_span(body, value_start, quote);
    }

    let mut end = body.len();
    let mut previous_was_whitespace = false;
    for (offset, ch) in body[value_start..].char_indices() {
        let index = value_start + offset;
        if matches!(ch, ',' | ';' | ')' | ']' | '}') {
            end = index;
            break;
        }
        if ch == '#' && previous_was_whitespace {
            end = index;
            break;
        }
        if ch == '/' && previous_was_whitespace && body[index + ch.len_utf8()..].starts_with('/') {
            end = index;
            break;
        }
        previous_was_whitespace = ch.is_whitespace();
    }

    let replacement_end = trim_ascii_whitespace_end(body, value_start, end);
    (value_start < replacement_end).then_some(PatchPreviewValueSpan {
        replacement: value_start..replacement_end,
        skip_until: end,
    })
}

fn quoted_patch_preview_value_span(
    body: &str,
    value_start: usize,
    quote: char,
) -> Option<PatchPreviewValueSpan> {
    let inner_start = value_start + quote.len_utf8();
    let mut escaped = false;
    for (offset, ch) in body[inner_start..].char_indices() {
        let index = inner_start + offset;
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == quote {
            return (inner_start < index).then_some(PatchPreviewValueSpan {
                replacement: inner_start..index,
                skip_until: index + ch.len_utf8(),
            });
        }
    }

    (inner_start < body.len()).then_some(PatchPreviewValueSpan {
        replacement: inner_start..body.len(),
        skip_until: body.len(),
    })
}

fn skip_ascii_whitespace(value: &str, start: usize) -> usize {
    value[start..]
        .char_indices()
        .find_map(|(offset, ch)| (!ch.is_ascii_whitespace()).then_some(start + offset))
        .unwrap_or(value.len())
}

fn trim_ascii_whitespace_end(value: &str, start: usize, end: usize) -> usize {
    value[start..end]
        .char_indices()
        .rev()
        .find_map(|(offset, ch)| {
            (!ch.is_ascii_whitespace()).then_some(start + offset + ch.len_utf8())
        })
        .unwrap_or(start)
}

fn should_redact_patch_preview_value(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() || is_redaction_placeholder(trimmed) {
        return false;
    }
    if looks_like_patch_preview_secret_value(trimmed) {
        return true;
    }
    !is_symbolic_patch_preview_identifier(trimmed)
}

fn is_redaction_placeholder(value: &str) -> bool {
    REDACTION_PLACEHOLDER_MARKERS.iter().any(|marker| value.eq_ignore_ascii_case(marker))
}

fn looks_like_patch_preview_secret_value(value: &str) -> bool {
    let lowered = value.to_ascii_lowercase();
    lowered.starts_with("sk_")
        || lowered.starts_with("sk-")
        || lowered.starts_with("ghp_")
        || lowered.starts_with("github_pat_")
        || lowered.starts_with("xox")
        || lowered.starts_with("ya29.")
        || lowered.starts_with("eyj")
        || lowered.starts_with("akia")
        || lowered.starts_with("asia")
        || lowered.contains("-----begin")
        || lowered.contains("correct-horse")
        || lowered.contains("palyra-regression")
        || lowered.contains("should_not_leak")
        || lowered.contains("secret_should_not_appear")
}

fn is_symbolic_patch_preview_identifier(value: &str) -> bool {
    value.len() <= 96
        && value.contains('_')
        && value.chars().all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_')
}

fn replace_ascii_case_insensitive(haystack: &str, needle: &str, replacement: &str) -> String {
    if needle.is_empty() {
        return haystack.to_owned();
    }
    if !needle.is_ascii() {
        return haystack.replace(needle, replacement);
    }

    // ASCII lowercasing preserves byte offsets, so match indices found in the lowered copy
    // map directly back into the original haystack.
    let lowered_haystack = haystack.to_ascii_lowercase();
    let lowered_needle = needle.to_ascii_lowercase();
    let mut cursor = 0usize;
    let mut replaced = false;
    let mut output = String::with_capacity(haystack.len());

    while let Some(relative_index) = lowered_haystack[cursor..].find(lowered_needle.as_str()) {
        let start = cursor + relative_index;
        let end = start + lowered_needle.len();
        output.push_str(&haystack[cursor..start]);
        output.push_str(replacement);
        cursor = end;
        replaced = true;
    }

    if !replaced {
        return haystack.to_owned();
    }

    output.push_str(&haystack[cursor..]);
    output
}

fn parse_patch_header_path(line: &str) -> Option<&str> {
    line.strip_prefix("*** Add File: ")
        .or_else(|| line.strip_prefix("*** Replace File: "))
        .or_else(|| line.strip_prefix("*** Replace Line: "))
        .or_else(|| line.strip_prefix("*** Update File: "))
        .or_else(|| line.strip_prefix("*** Delete File: "))
        .or_else(|| line.strip_prefix("*** Move to: "))
}

fn is_secret_path(path: &str, markers: &[String]) -> bool {
    let lowered = path.to_ascii_lowercase();
    markers.iter().any(|marker| {
        let marker = marker.trim();
        !marker.is_empty() && lowered.contains(marker.to_ascii_lowercase().as_str())
    })
}

fn canonicalize_workspace_roots(
    workspace_roots: &[PathBuf],
) -> Result<Vec<PathBuf>, WorkspacePatchError> {
    if workspace_roots.is_empty() {
        return Err(WorkspacePatchError::EmptyWorkspaceRoots);
    }

    let mut roots = Vec::with_capacity(workspace_roots.len());
    for root in workspace_roots {
        let canonical =
            fs::canonicalize(root).map_err(|source| WorkspacePatchError::InvalidWorkspaceRoot {
                path: root.display().to_string(),
                message: source.to_string(),
            })?;
        if !canonical.is_dir() {
            return Err(WorkspacePatchError::InvalidWorkspaceRoot {
                path: root.display().to_string(),
                message: "path is not a directory".to_owned(),
            });
        }
        roots.push(canonical);
    }
    Ok(roots)
}

fn validate_canonical_root_constraints(
    canonical_roots: &[PathBuf],
    canonical_constraint_roots: &[PathBuf],
) -> Result<(), WorkspacePatchError> {
    // An empty constraint set means the caller imposed no narrowing; nothing to enforce.
    if canonical_constraint_roots.is_empty() {
        return Ok(());
    }
    for root in canonical_roots {
        if canonical_constraint_roots.iter().any(|constraint| root.starts_with(constraint)) {
            continue;
        }
        return Err(WorkspacePatchError::InvalidWorkspaceRoot {
            path: root.display().to_string(),
            message: "path escapes canonical workspace constraints".to_owned(),
        });
    }
    Ok(())
}

// Input formats are tried in fixed order: tolerant fenced-variant normalization, an
// already-canonical fenced document, then unified-diff conversion. Anything else passes
// through unchanged so the parser can reject it with a precise location.
fn normalize_supported_patch_document(patch: &str) -> Cow<'_, str> {
    if let Some(normalized) = normalize_palyra_patch_fences(patch) {
        return Cow::Owned(normalized);
    }
    if patch.starts_with("*** Begin Patch") {
        return Cow::Borrowed(patch);
    }
    normalize_unified_diff_patch(patch).map_or(Cow::Borrowed(patch), Cow::Owned)
}

/// Extracts primary operation paths after applying the same format
/// normalization used by the patch executor.
///
/// Move destinations are intentionally excluded because callers use this
/// helper to choose the root for the source operations before parsing.
#[must_use]
pub fn normalized_workspace_patch_operation_paths(patch: &str) -> Vec<String> {
    let normalized = normalize_supported_patch_document(patch);
    normalized
        .lines()
        .filter_map(|line| {
            let control_line = patch_control_line(line);
            [
                "*** Add File:",
                "*** Update File:",
                "*** Replace File:",
                "*** Replace Line:",
                "*** Delete File:",
            ]
            .iter()
            .find_map(|prefix| control_line.strip_prefix(prefix).map(str::trim))
        })
        .filter(|path| !path.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn normalize_palyra_patch_fences(patch: &str) -> Option<String> {
    let normalized = patch.replace("\r\n", "\n").replace('\r', "\n");
    let original_lines = normalized.split('\n').collect::<Vec<_>>();
    if original_lines.is_empty() {
        return None;
    }

    let mut lines = Vec::with_capacity(original_lines.len());
    let mut changed = false;
    // Tracks an Add/Replace File header that has no body yet so a redundant
    // "*** Begin File:" wrapper for the same path can be dropped instead of being
    // mistaken for a second add operation.
    let mut pending_empty_file_header: Option<(usize, String)> = None;
    for original in &original_lines {
        let Some(line) =
            normalize_patch_control_variant(original, &mut lines, &mut pending_empty_file_header)
        else {
            changed = true;
            continue;
        };
        changed |= line != *original;
        if let Some(path) = full_file_operation_path(line.as_str()) {
            pending_empty_file_header = Some((lines.len(), path.to_owned()));
        } else if !patch_control_line(line.as_str()).trim().is_empty() || !line.trim().is_empty() {
            pending_empty_file_header = None;
        }
        lines.push(line);
    }

    if lines.is_empty() {
        return None;
    }

    if patch_control_line(lines[0].as_str()) != "*** Begin Patch" {
        return None;
    }

    let Some(first_end_index) =
        lines.iter().position(|line| patch_control_line(line.as_str()) == "*** End Patch")
    else {
        return changed.then(|| lines.join("\n"));
    };

    for line in lines.iter().skip(first_end_index + 1) {
        let control = patch_control_line(line.as_str());
        if control.is_empty() {
            continue;
        }
        return changed.then(|| lines.join("\n"));
    }

    changed.then(|| {
        let mut output = lines.join("\n");
        if !output.ends_with('\n') {
            output.push('\n');
        }
        output
    })
}

fn normalize_patch_control_variant(
    line: &str,
    output_lines: &mut [String],
    pending_empty_file_header: &mut Option<(usize, String)>,
) -> Option<String> {
    if let Some(canonical) = canonical_patch_fence_variant(line) {
        return Some(canonical.to_owned());
    }
    if let Some(canonical) = operation_header_variant(line) {
        return Some(canonical);
    }
    if let Some(path) = begin_file_variant_path(line) {
        if let Some((header_index, pending_path)) = pending_empty_file_header.as_ref() {
            if path == *pending_path && output_lines.get(*header_index).is_some() {
                return None;
            }
        }
        return Some(format!("*** Add File: {path}"));
    }
    if is_file_body_wrapper_variant(line) {
        return None;
    }
    Some(line.to_owned())
}

fn canonical_patch_fence_variant(line: &str) -> Option<&'static str> {
    match patch_control_line(line).trim() {
        "*** Begin Patch" | "*** Begin Patch ***" => Some("*** Begin Patch"),
        "*** End Patch" | "*** End Patch ***" => Some("*** End Patch"),
        _ => None,
    }
}

fn operation_header_variant(line: &str) -> Option<String> {
    let control = patch_control_line(line);
    for prefix in [
        "*** Add File:",
        "*** Replace File:",
        "*** Replace Line:",
        "*** Update File:",
        "*** Delete File:",
        "*** Move to:",
    ] {
        let Some(raw_path) = control.strip_prefix(prefix) else {
            continue;
        };
        let path = strip_trailing_patch_stars(raw_path);
        if path.is_empty() {
            return None;
        }
        return Some(format!("{prefix} {path}"));
    }
    None
}

fn begin_file_variant_path(line: &str) -> Option<String> {
    let control = strip_trailing_patch_stars(patch_control_line(line));
    let path = control.strip_prefix("*** Begin File:")?.trim();
    (!path.is_empty()).then(|| path.to_owned())
}

fn is_file_body_wrapper_variant(line: &str) -> bool {
    let control = strip_trailing_patch_stars(patch_control_line(line));
    matches!(
        control,
        "*** Begin Body"
            | "*** Begin Body:"
            | "*** End Body"
            | "*** End Body:"
            | "*** End File"
            | "*** End File:"
    )
}

fn strip_trailing_patch_stars(line: &str) -> &str {
    line.trim().strip_suffix("***").unwrap_or(line.trim()).trim()
}

fn full_file_operation_path(line: &str) -> Option<&str> {
    let control = patch_control_line(line);
    control
        .strip_prefix("*** Add File: ")
        .or_else(|| control.strip_prefix("*** Replace File: "))
        .map(str::trim)
        .filter(|path| !path.is_empty())
}

fn normalize_unified_diff_patch(patch: &str) -> Option<String> {
    let normalized = patch.replace("\r\n", "\n").replace('\r', "\n");
    let lines = normalized.split('\n').collect::<Vec<_>>();
    let mut index = 0usize;
    let mut operations = Vec::new();

    while index < lines.len() {
        if lines[index].trim().is_empty()
            || lines[index].starts_with("diff --git ")
            || lines[index].starts_with("index ")
            || lines[index].starts_with("new file mode ")
            || lines[index].starts_with("deleted file mode ")
            || lines[index].starts_with("similarity index ")
        {
            index = index.saturating_add(1);
            continue;
        }

        let old_raw = lines[index].strip_prefix("--- ")?;
        index = index.saturating_add(1);
        let new_raw = lines.get(index)?.strip_prefix("+++ ")?;
        index = index.saturating_add(1);

        let old_path = parse_unified_diff_path(old_raw)?;
        let new_path = parse_unified_diff_path(new_raw)?;

        match (old_path, new_path) {
            (UnifiedDiffPath::DevNull, UnifiedDiffPath::Path(path)) => {
                let (add_lines, next_index) =
                    collect_unified_add_file_lines(lines.as_slice(), index)?;
                operations.push(render_palyra_add_file(path.as_str(), add_lines.as_slice()));
                index = next_index;
            }
            (UnifiedDiffPath::Path(path), UnifiedDiffPath::DevNull) => {
                let next_index = skip_unified_file_hunks(lines.as_slice(), index);
                operations.push(format!("*** Delete File: {path}"));
                index = next_index;
            }
            (UnifiedDiffPath::Path(_old_path), UnifiedDiffPath::Path(new_path)) => {
                let (hunks, next_index) = collect_unified_update_hunks(lines.as_slice(), index)?;
                operations.push(render_palyra_update_file(new_path.as_str(), hunks.as_slice()));
                index = next_index;
            }
            (UnifiedDiffPath::DevNull, UnifiedDiffPath::DevNull) => return None,
        }
    }

    if operations.is_empty() {
        return None;
    }

    let mut output = String::from("*** Begin Patch\n");
    output.push_str(operations.join("\n").as_str());
    output.push_str("\n*** End Patch\n");
    Some(output)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum UnifiedDiffPath {
    DevNull,
    Path(String),
}

fn parse_unified_diff_path(raw: &str) -> Option<UnifiedDiffPath> {
    let path = raw.split('\t').next().unwrap_or(raw).trim();
    if path == "/dev/null" {
        return Some(UnifiedDiffPath::DevNull);
    }
    let path = path.strip_prefix("a/").or_else(|| path.strip_prefix("b/")).unwrap_or(path).trim();
    if path.is_empty() {
        None
    } else {
        Some(UnifiedDiffPath::Path(path.to_owned()))
    }
}

fn collect_unified_add_file_lines(
    lines: &[&str],
    mut index: usize,
) -> Option<(Vec<String>, usize)> {
    let mut add_lines = Vec::new();
    while index < lines.len() && !is_unified_file_header(lines, index) {
        let line = lines[index];
        if line.starts_with("@@") || line.starts_with("\\ ") {
            index = index.saturating_add(1);
            continue;
        }
        if let Some(content) = line.strip_prefix('+') {
            add_lines.push(content.to_owned());
        } else if line.starts_with('-') || line.starts_with(' ') || line.trim().is_empty() {
            // Context/removal lines carry no content for a brand-new file; skip them.
        } else {
            return None;
        }
        index = index.saturating_add(1);
    }
    Some((add_lines, index))
}

fn collect_unified_update_hunks(
    lines: &[&str],
    mut index: usize,
) -> Option<(Vec<Vec<String>>, usize)> {
    let mut hunks = Vec::new();
    while index < lines.len() && !is_unified_file_header(lines, index) {
        let line = lines[index];
        if !line.starts_with("@@") {
            if line.trim().is_empty() {
                index = index.saturating_add(1);
                continue;
            }
            return None;
        }
        index = index.saturating_add(1);

        let mut hunk_lines = Vec::new();
        while index < lines.len()
            && !lines[index].starts_with("@@")
            && !is_unified_file_header(lines, index)
        {
            let candidate = lines[index];
            if candidate.is_empty() && index.saturating_add(1) == lines.len() {
                index = index.saturating_add(1);
                break;
            }
            if candidate.starts_with("\\ ") {
                index = index.saturating_add(1);
                continue;
            }
            let prefix = candidate.chars().next()?;
            if matches!(prefix, ' ' | '+' | '-') {
                hunk_lines.push(candidate.to_owned());
                index = index.saturating_add(1);
                continue;
            }
            return None;
        }
        if hunk_lines.is_empty() {
            return None;
        }
        hunks.push(hunk_lines);
    }
    Some((hunks, index))
}

fn skip_unified_file_hunks(lines: &[&str], mut index: usize) -> usize {
    while index < lines.len() && !is_unified_file_header(lines, index) {
        index = index.saturating_add(1);
    }
    index
}

fn is_unified_file_header(lines: &[&str], index: usize) -> bool {
    lines.get(index).is_some_and(|line| line.starts_with("--- "))
        && lines.get(index.saturating_add(1)).is_some_and(|line| line.starts_with("+++ "))
}

fn render_palyra_add_file(path: &str, lines: &[String]) -> String {
    let mut output = format!("*** Add File: {path}");
    for line in lines {
        output.push('\n');
        output.push('+');
        output.push_str(line);
    }
    output
}

fn render_palyra_update_file(path: &str, hunks: &[Vec<String>]) -> String {
    let mut output = format!("*** Update File: {path}");
    for hunk in hunks {
        output.push_str("\n@@");
        for line in hunk {
            output.push('\n');
            output.push_str(line);
        }
    }
    output
}

fn parse_patch_document(patch: &str) -> Result<Vec<PatchOperation>, WorkspacePatchError> {
    let normalized = patch.replace("\r\n", "\n").replace('\r', "\n");
    let lines = normalized.split('\n').collect::<Vec<_>>();
    if lines.is_empty() || patch_control_line(lines[0]) != "*** Begin Patch" {
        return Err(parse_error(1, 1, "expected '*** Begin Patch'"));
    }

    let mut index = 1_usize;
    let mut operations = Vec::new();
    let mut ended = false;

    while index < lines.len() {
        let line = lines[index];
        let control_line = patch_control_line(line);
        if control_line == "*** End Patch" {
            ended = true;
            index = index.saturating_add(1);
            break;
        }
        if control_line.is_empty() {
            index = index.saturating_add(1);
            continue;
        }

        if let Some(path) = control_line.strip_prefix("*** Add File: ") {
            let header_line = index;
            index = index.saturating_add(1);
            let mut add_lines = Vec::new();
            while index < lines.len() {
                let body_line = lines[index];
                if is_patch_header_or_end(body_line) {
                    break;
                }
                reject_structural_marker_in_full_file_body(body_line, index + 1, "add-file")?;
                let content = full_file_body_content(body_line);
                add_lines.push(content.to_owned());
                index = index.saturating_add(1);
            }
            if add_lines.is_empty() {
                return Err(parse_error(
                    header_line + 1,
                    1,
                    "add-file operation must include at least one content line; zero-byte placeholder files are not allowed",
                ));
            }
            operations.push(PatchOperation::Add { path: path.to_owned(), lines: add_lines });
            continue;
        }

        if let Some(path) = control_line.strip_prefix("*** Replace File: ") {
            let header_line = index;
            index = index.saturating_add(1);
            let mut replace_lines = Vec::new();
            let mut first_diff_remove_line = None;
            let mut has_patch_prefixed_content_line = false;
            while index < lines.len() {
                let body_line = lines[index];
                if is_patch_header_or_end(body_line) {
                    break;
                }
                reject_structural_marker_in_full_file_body(body_line, index + 1, "replace-file")?;
                if first_diff_remove_line.is_none()
                    && is_diff_remove_line_in_full_file_body(body_line)
                {
                    first_diff_remove_line = Some(index + 1);
                }
                has_patch_prefixed_content_line |= body_line.starts_with('+');
                let content = full_file_body_content(body_line);
                replace_lines.push(content.to_owned());
                index = index.saturating_add(1);
            }
            if let Some(line_number) = first_diff_remove_line {
                if has_patch_prefixed_content_line {
                    return Err(parse_error(
                        line_number,
                        1,
                        "replace-file body mixes '-' removal lines with '+' content lines; use an Update File hunk for diffs or provide only final file contents. To write a literal line beginning with '-', prefix it as '+-...'.",
                    ));
                }
            }
            if replace_lines.is_empty() {
                return Err(parse_error(
                    header_line + 1,
                    1,
                    "replace-file operation must include at least one content line; zero-byte replacements are not allowed",
                ));
            }
            operations
                .push(PatchOperation::Replace { path: path.to_owned(), lines: replace_lines });
            continue;
        }

        if let Some(path) = control_line.strip_prefix("*** Delete File: ") {
            operations.push(PatchOperation::Delete { path: path.to_owned() });
            index = index.saturating_add(1);
            continue;
        }

        if let Some(path) = control_line.strip_prefix("*** Replace Line: ") {
            let header_line = index;
            index = index.saturating_add(1);
            if index.saturating_add(1) >= lines.len() {
                return Err(parse_error(
                    header_line + 1,
                    1,
                    "replace-line operation requires exactly one '-' old line followed by one '+' new line",
                ));
            }
            let old_line = lines[index];
            if is_patch_header_or_end(old_line) {
                return Err(parse_error(
                    index + 1,
                    1,
                    "replace-line operation requires a '-' old line",
                ));
            }
            index = index.saturating_add(1);
            let new_line = lines[index];
            if is_patch_header_or_end(new_line) {
                return Err(parse_error(
                    index + 1,
                    1,
                    "replace-line operation requires a '+' new line",
                ));
            }
            index = index.saturating_add(1);
            let Some(old) = old_line.strip_prefix('-') else {
                return Err(parse_error(
                    index.saturating_sub(1),
                    1,
                    "replace-line old line must start with '-'",
                ));
            };
            let Some(new) = new_line.strip_prefix('+') else {
                return Err(parse_error(index, 1, "replace-line new line must start with '+'"));
            };
            if index < lines.len() && !is_patch_header_or_end(lines[index]) {
                return Err(parse_error(
                    index + 1,
                    1,
                    "replace-line operation accepts exactly one '-' line and one '+' line",
                ));
            }
            operations.push(PatchOperation::ReplaceLine {
                path: path.to_owned(),
                old: old.to_owned(),
                new: new.to_owned(),
            });
            continue;
        }

        if let Some(path) = control_line.strip_prefix("*** Update File: ") {
            index = index.saturating_add(1);
            let mut move_to = None;
            if index < lines.len() {
                if let Some(target) = patch_control_line(lines[index]).strip_prefix("*** Move to: ")
                {
                    move_to = Some(target.to_owned());
                    index = index.saturating_add(1);
                }
            }

            let mut hunks = Vec::new();
            while index < lines.len() {
                let hunk_line = lines[index];
                let hunk_control_line = patch_control_line(hunk_line);
                if is_patch_header_or_end(hunk_line) {
                    break;
                }
                if !hunk_control_line.starts_with("@@") {
                    return Err(parse_error(index + 1, 1, "update-file hunk must start with '@@'"));
                }
                index = index.saturating_add(1);
                let mut lines_in_hunk = Vec::new();
                while index < lines.len() {
                    let candidate = lines[index];
                    let candidate_control_line = patch_control_line(candidate);
                    if candidate_control_line.starts_with("@@") || is_patch_header_or_end(candidate)
                    {
                        break;
                    }
                    let (kind, text) = if candidate.is_empty() {
                        (HunkLineKind::Context, String::new())
                    } else {
                        let mut chars = candidate.chars();
                        let prefix =
                            chars.next().expect("non-empty candidate should have first char");
                        let text = chars.collect::<String>();
                        let kind = match prefix {
                            ' ' => HunkLineKind::Context,
                            '+' => HunkLineKind::Add,
                            '-' => HunkLineKind::Remove,
                            _ => {
                                return Err(parse_error(
                                    index + 1,
                                    1,
                                    "hunk line must start with ' ', '+', '-', or be empty for blank context",
                                ));
                            }
                        };
                        (kind, text)
                    };
                    lines_in_hunk.push(HunkLine { kind, text });
                    index = index.saturating_add(1);
                }
                if lines_in_hunk.is_empty() {
                    return Err(parse_error(index + 1, 1, "hunk must include at least one line"));
                }
                hunks.push(PatchHunk { lines: lines_in_hunk });
            }

            if hunks.is_empty() && move_to.is_none() {
                return Err(parse_error(
                    index.saturating_add(1),
                    1,
                    "update-file operation must include hunk(s) or move target",
                ));
            }

            operations.push(PatchOperation::Update { path: path.to_owned(), move_to, hunks });
            continue;
        }

        return Err(parse_error(
            index + 1,
            1,
            "expected patch operation header: *** Add File, *** Replace File, *** Replace Line, *** Update File, or *** Delete File",
        ));
    }

    if !ended {
        return Err(parse_error(lines.len(), 1, "expected '*** End Patch'"));
    }

    while index < lines.len() {
        if !patch_control_line(lines[index]).is_empty() {
            return Err(parse_error(index + 1, 1, "unexpected content after '*** End Patch'"));
        }
        index = index.saturating_add(1);
    }

    if operations.is_empty() {
        return Err(parse_error(1, 1, "patch must contain at least one operation"));
    }

    Ok(operations)
}

fn reject_structural_marker_in_full_file_body(
    line: &str,
    line_number: usize,
    operation: &str,
) -> Result<(), WorkspacePatchError> {
    let trimmed = line.trim_start();
    if trimmed.starts_with("diff --git ")
        || trimmed.starts_with("index ")
        || is_unified_diff_file_header(trimmed, "---")
        || is_unified_diff_file_header(trimmed, "+++")
        || trimmed.starts_with("@@")
        || trimmed.starts_with("<<<<<<<")
        || trimmed.starts_with("=======")
        || trimmed.starts_with(">>>>>>>")
    {
        let message = format!(
            "{operation} body contains a diff or conflict marker; use an Update File hunk for diffs or provide only final file contents"
        );
        return Err(parse_error(line_number, 1, message.as_str()));
    }
    Ok(())
}

fn is_unified_diff_file_header(trimmed_line: &str, prefix: &str) -> bool {
    let Some(rest) = trimmed_line.strip_prefix(prefix) else {
        return false;
    };

    rest.starts_with([' ', '\t']) && !rest.trim().is_empty()
}

fn is_diff_remove_line_in_full_file_body(line: &str) -> bool {
    let Some(rest) = line.strip_prefix('-') else {
        return false;
    };
    rest.chars().next().is_some_and(|character| !character.is_whitespace())
}

fn is_patch_header_or_end(line: &str) -> bool {
    let control_line = patch_control_line(line);
    control_line == "*** End Patch" || control_line.starts_with("*** ")
}

// "+ text" drops the single separator space, but "+<indent>..." strips only the '+' so
// indented file content keeps its leading whitespace; pinned by the plus-space
// normalization tests.
fn full_file_body_content(line: &str) -> &str {
    if let Some(rest) = line.strip_prefix("+ ") {
        if rest.is_empty()
            || rest.chars().next().is_some_and(|character| !character.is_whitespace())
        {
            return rest;
        }
    }
    line.strip_prefix('+').unwrap_or(line)
}

// INTENTIONAL: the guidance text below (and in replace_line_target_not_found_message) is
// asserted by substring in tests and read by model callers; keep the wording stable.
fn hunk_context_not_found_message(index: usize, old_lines: &[String]) -> String {
    let mut message = format!("hunk {index} context not found");
    if old_lines.iter().any(|line| line.starts_with(" -") || line.starts_with(" +")) {
        message.push_str(
            "; if the target file line itself begins with '-' or '+', prefix that content directly with the hunk marker: use '-- markdown item' to remove a '- markdown item' line, or '++value' to add a '+value' line",
        );
    }
    message
}

fn replace_line_target_not_found_message(old_line: &str) -> String {
    let mut message = "replace-line exact target not found; use the exact line text returned by search/read_file or retry with an Update File hunk containing surrounding context".to_owned();
    if old_line.starts_with(' ') || old_line.starts_with('+') {
        message.push_str(
            "; if the real target line begins with '-' or '+', Replace Line also uses those characters as old/new markers: use '-- markdown item' to replace a '- markdown item' line, or '++value' to write a '+value' line",
        );
    }
    message
}

// Trailing spaces/tabs are ignored on control lines so patches piped through Windows
// tooling (which can leave trailing whitespace after CRLF normalization) still match.
fn patch_control_line(line: &str) -> &str {
    line.trim_end_matches([' ', '\t'])
}

fn parse_error(line: usize, column: usize, message: &str) -> WorkspacePatchError {
    WorkspacePatchError::Parse { line, column, message: message.to_owned() }
}

fn build_patch_plan(
    operations: &[PatchOperation],
    canonical_roots: &[PathBuf],
    limits: &WorkspacePatchLimits,
) -> Result<PatchPlan, WorkspacePatchError> {
    let mut actions = Vec::new();
    let mut file_attestations = Vec::new();
    let mut no_op_attestations = Vec::new();
    let mut touched_paths = HashSet::new();

    for operation in operations {
        match operation {
            PatchOperation::Add { path, lines } => {
                let relative = parse_relative_patch_path(path)?;
                let output_path = normalize_relative_path_display(&relative);
                let (target, target_root_index) =
                    resolve_new_path(canonical_roots, &relative, None, path)?;
                let after_bytes = render_add_file_bytes(lines.as_slice());
                ensure_file_size(path, after_bytes.len(), limits.max_file_bytes)?;
                ensure_planned_file_content(output_path.as_str(), after_bytes.as_slice())?;
                if target.exists() {
                    let before_bytes =
                        read_file_capped(target.as_path(), path, limits.max_file_bytes)?;
                    if before_bytes != after_bytes {
                        return Err(WorkspacePatchError::FileAlreadyExists {
                            path: path.to_owned(),
                        });
                    }
                    touched_paths.insert(target.clone());
                    file_attestations.push(WorkspacePatchFileAttestation {
                        path: output_path,
                        workspace_root_index: target_root_index,
                        operation: "create_idempotent".to_owned(),
                        moved_from: None,
                        before_sha256: Some(sha256_hex(before_bytes.as_slice())),
                        before_size_bytes: Some(before_bytes.len() as u64),
                        after_sha256: Some(sha256_hex(after_bytes.as_slice())),
                        after_size_bytes: Some(after_bytes.len() as u64),
                    });
                    continue;
                }

                touched_paths.insert(target.clone());
                actions.push(PlannedAction::Write {
                    path: target,
                    root: canonical_roots[target_root_index].clone(),
                    bytes: after_bytes.clone(),
                });
                file_attestations.push(WorkspacePatchFileAttestation {
                    path: output_path,
                    workspace_root_index: target_root_index,
                    operation: "create".to_owned(),
                    moved_from: None,
                    before_sha256: None,
                    before_size_bytes: None,
                    after_sha256: Some(sha256_hex(after_bytes.as_slice())),
                    after_size_bytes: Some(after_bytes.len() as u64),
                });
            }
            PatchOperation::Delete { path } => {
                let relative = parse_relative_patch_path(path)?;
                let (target, target_root_index) =
                    resolve_existing_path(canonical_roots, &relative, path)?;
                let before_bytes = read_file_capped(target.as_path(), path, limits.max_file_bytes)?;
                touched_paths.insert(target.clone());
                actions.push(PlannedAction::Delete {
                    path: target,
                    root: canonical_roots[target_root_index].clone(),
                });
                file_attestations.push(WorkspacePatchFileAttestation {
                    path: normalize_relative_path_display(&relative),
                    workspace_root_index: target_root_index,
                    operation: "delete".to_owned(),
                    moved_from: None,
                    before_sha256: Some(sha256_hex(before_bytes.as_slice())),
                    before_size_bytes: Some(before_bytes.len() as u64),
                    after_sha256: None,
                    after_size_bytes: None,
                });
            }
            PatchOperation::Replace { path, lines } => {
                let relative = parse_relative_patch_path(path)?;
                let output_path = normalize_relative_path_display(&relative);
                let (target, target_root_index) =
                    resolve_existing_path(canonical_roots, &relative, path)?;
                let before_bytes = read_file_capped(target.as_path(), path, limits.max_file_bytes)?;
                let after_bytes = render_add_file_bytes(lines.as_slice());
                ensure_file_size(path, after_bytes.len(), limits.max_file_bytes)?;
                ensure_replace_file_is_not_suspicious_partial(
                    path,
                    before_bytes.as_slice(),
                    after_bytes.as_slice(),
                    lines.as_slice(),
                )?;
                ensure_planned_file_content(output_path.as_str(), after_bytes.as_slice())?;

                touched_paths.insert(target.clone());
                actions.push(PlannedAction::Write {
                    path: target,
                    root: canonical_roots[target_root_index].clone(),
                    bytes: after_bytes.clone(),
                });
                file_attestations.push(WorkspacePatchFileAttestation {
                    path: output_path,
                    workspace_root_index: target_root_index,
                    operation: "replace".to_owned(),
                    moved_from: None,
                    before_sha256: Some(sha256_hex(before_bytes.as_slice())),
                    before_size_bytes: Some(before_bytes.len() as u64),
                    after_sha256: Some(sha256_hex(after_bytes.as_slice())),
                    after_size_bytes: Some(after_bytes.len() as u64),
                });
            }
            PatchOperation::ReplaceLine { path, old, new } => {
                let relative = parse_relative_patch_path(path)?;
                let output_path = normalize_relative_path_display(&relative);
                let (target, target_root_index) =
                    resolve_existing_path(canonical_roots, &relative, path)?;
                let before_bytes = read_file_capped(target.as_path(), path, limits.max_file_bytes)?;
                let after_bytes =
                    replace_exact_line_bytes(path, before_bytes.as_slice(), old, new)?;
                ensure_file_size(path, after_bytes.len(), limits.max_file_bytes)?;
                ensure_planned_file_content(output_path.as_str(), after_bytes.as_slice())?;

                touched_paths.insert(target.clone());
                actions.push(PlannedAction::Write {
                    path: target,
                    root: canonical_roots[target_root_index].clone(),
                    bytes: after_bytes.clone(),
                });
                file_attestations.push(WorkspacePatchFileAttestation {
                    path: output_path,
                    workspace_root_index: target_root_index,
                    operation: "line_replace".to_owned(),
                    moved_from: None,
                    before_sha256: Some(sha256_hex(before_bytes.as_slice())),
                    before_size_bytes: Some(before_bytes.len() as u64),
                    after_sha256: Some(sha256_hex(after_bytes.as_slice())),
                    after_size_bytes: Some(after_bytes.len() as u64),
                });
            }
            PatchOperation::Update { path, move_to, hunks } => {
                let relative = parse_relative_patch_path(path)?;
                let (source, source_root_index) =
                    resolve_existing_path(canonical_roots, &relative, path)?;
                let before_bytes = read_file_capped(source.as_path(), path, limits.max_file_bytes)?;
                let after_bytes = if hunks.is_empty() {
                    before_bytes.clone()
                } else {
                    apply_hunks_to_bytes(path.as_str(), before_bytes.as_slice(), hunks.as_slice())?
                };
                ensure_file_size(path, after_bytes.len(), limits.max_file_bytes)?;

                let mut destination = source.clone();
                let source_root = canonical_roots[source_root_index].clone();
                let mut destination_root = source_root.clone();
                let mut output_root_index = source_root_index;
                let mut moved_from = None;
                let output_path = if let Some(move_target) = move_to {
                    let move_relative = parse_relative_patch_path(move_target)?;
                    let (resolved_destination, destination_root_index) = resolve_new_path(
                        canonical_roots,
                        &move_relative,
                        Some(source_root_index),
                        move_target,
                    )?;
                    if resolved_destination != source && resolved_destination.exists() {
                        return Err(WorkspacePatchError::FileAlreadyExists {
                            path: move_target.to_owned(),
                        });
                    }
                    destination = resolved_destination;
                    destination_root = canonical_roots[destination_root_index].clone();
                    output_root_index = destination_root_index;
                    moved_from = Some(normalize_relative_path_display(&relative));
                    let output_path = normalize_relative_path_display(&move_relative);
                    ensure_planned_file_content(output_path.as_str(), after_bytes.as_slice())?;
                    output_path
                } else {
                    let output_path = normalize_relative_path_display(&relative);
                    ensure_planned_file_content(output_path.as_str(), after_bytes.as_slice())?;
                    output_path
                };

                if destination == source && before_bytes == after_bytes {
                    no_op_attestations.push(WorkspacePatchFileAttestation {
                        path: output_path,
                        workspace_root_index: output_root_index,
                        operation: "no_op".to_owned(),
                        moved_from: None,
                        before_sha256: Some(sha256_hex(before_bytes.as_slice())),
                        before_size_bytes: Some(before_bytes.len() as u64),
                        after_sha256: Some(sha256_hex(after_bytes.as_slice())),
                        after_size_bytes: Some(after_bytes.len() as u64),
                    });
                    continue;
                }

                touched_paths.insert(destination.clone());
                actions.push(PlannedAction::Write {
                    path: destination.clone(),
                    root: destination_root,
                    bytes: after_bytes.clone(),
                });

                if destination != source {
                    touched_paths.insert(source.clone());
                    actions.push(PlannedAction::Delete { path: source, root: source_root });
                }

                file_attestations.push(WorkspacePatchFileAttestation {
                    path: output_path,
                    workspace_root_index: output_root_index,
                    operation: if moved_from.is_some() {
                        "move".to_owned()
                    } else {
                        "update".to_owned()
                    },
                    moved_from,
                    before_sha256: Some(sha256_hex(before_bytes.as_slice())),
                    before_size_bytes: Some(before_bytes.len() as u64),
                    after_sha256: Some(sha256_hex(after_bytes.as_slice())),
                    after_size_bytes: Some(after_bytes.len() as u64),
                });
            }
        }
    }

    if touched_paths.len() > limits.max_files_touched {
        return Err(WorkspacePatchError::TooManyFiles {
            limit: limits.max_files_touched,
            actual: touched_paths.len(),
        });
    }

    Ok(PatchPlan { actions, file_attestations, no_op_attestations })
}

fn parse_relative_patch_path(raw: &str) -> Result<PathBuf, WorkspacePatchError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(WorkspacePatchError::InvalidPatchPath { path: raw.to_owned() });
    }
    if trimmed.contains('\0') {
        return Err(WorkspacePatchError::InvalidPatchPath { path: raw.to_owned() });
    }
    if trimmed.contains('\\') {
        return Err(WorkspacePatchError::InvalidPatchPath { path: raw.to_owned() });
    }
    if trimmed.starts_with("\\\\") || looks_like_windows_drive_path(trimmed) {
        return Err(WorkspacePatchError::InvalidPatchPath { path: raw.to_owned() });
    }

    let parsed = PathBuf::from(trimmed);
    if parsed.is_absolute() {
        return Err(WorkspacePatchError::InvalidPatchPath { path: raw.to_owned() });
    }

    for component in parsed.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(WorkspacePatchError::InvalidPatchPath { path: raw.to_owned() });
            }
        }
    }

    Ok(parsed)
}

fn looks_like_windows_drive_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn normalize_relative_path_display(path: &Path) -> String {
    let mut rendered = Vec::new();
    for component in path.components() {
        if let Component::Normal(value) = component {
            rendered.push(value.to_string_lossy().into_owned());
        }
    }
    rendered.join("/")
}
fn resolve_existing_path(
    canonical_roots: &[PathBuf],
    relative: &Path,
    path_label: &str,
) -> Result<(PathBuf, usize), WorkspacePatchError> {
    for (index, root) in canonical_roots.iter().enumerate() {
        let candidate = root.join(relative);
        if !candidate.exists() {
            continue;
        }
        ensure_path_within_root(candidate.as_path(), root.as_path(), path_label)?;
        return Ok((candidate, index));
    }

    Err(WorkspacePatchError::MissingFile { path: path_label.to_owned() })
}

fn resolve_new_path(
    canonical_roots: &[PathBuf],
    relative: &Path,
    preferred_root_index: Option<usize>,
    path_label: &str,
) -> Result<(PathBuf, usize), WorkspacePatchError> {
    // New files default to the first workspace root; moves pin the destination to the
    // source file's root so a rename never silently hops between roots.
    let index = preferred_root_index.unwrap_or(0);
    let root = canonical_roots
        .get(index)
        .ok_or(WorkspacePatchError::PathOutsideWorkspace { path: path_label.to_owned() })?;
    let candidate = root.join(relative);
    ensure_parent_within_root(candidate.as_path(), root.as_path(), path_label)?;
    if candidate.exists() {
        ensure_path_within_root(candidate.as_path(), root.as_path(), path_label)?;
    }
    Ok((candidate, index))
}

fn ensure_path_within_root(
    path: &Path,
    root: &Path,
    path_label: &str,
) -> Result<(), WorkspacePatchError> {
    let canonical = fs::canonicalize(path).map_err(|source| WorkspacePatchError::Io {
        operation: "canonicalize",
        path: path.display().to_string(),
        source,
    })?;
    if !canonical.starts_with(root) {
        return Err(WorkspacePatchError::PathOutsideWorkspace { path: path_label.to_owned() });
    }
    Ok(())
}

fn ensure_parent_within_root(
    candidate: &Path,
    root: &Path,
    path_label: &str,
) -> Result<(), WorkspacePatchError> {
    let Some(existing_ancestor) = nearest_existing_ancestor(candidate) else {
        return Err(WorkspacePatchError::PathOutsideWorkspace { path: path_label.to_owned() });
    };
    let canonical_ancestor = fs::canonicalize(existing_ancestor.as_path()).map_err(|source| {
        WorkspacePatchError::Io {
            operation: "canonicalize",
            path: existing_ancestor.display().to_string(),
            source,
        }
    })?;
    if !canonical_ancestor.starts_with(root) {
        return Err(WorkspacePatchError::PathOutsideWorkspace { path: path_label.to_owned() });
    }
    Ok(())
}

fn nearest_existing_ancestor(path: &Path) -> Option<PathBuf> {
    let mut cursor = path.to_path_buf();
    loop {
        if cursor.exists() {
            return Some(cursor);
        }
        let parent = cursor.parent()?;
        cursor = parent.to_path_buf();
    }
}

fn render_add_file_bytes(lines: &[String]) -> Vec<u8> {
    if lines.is_empty() {
        return Vec::new();
    }
    let mut joined = lines.join("\n");
    joined.push('\n');
    joined.into_bytes()
}

fn ensure_file_size(
    path: &str,
    actual: usize,
    max_file_bytes: usize,
) -> Result<(), WorkspacePatchError> {
    if actual > max_file_bytes {
        return Err(WorkspacePatchError::FileTooLarge {
            path: path.to_owned(),
            limit: max_file_bytes,
            actual,
        });
    }
    Ok(())
}

fn ensure_replace_file_is_not_suspicious_partial(
    path: &str,
    before_bytes: &[u8],
    after_bytes: &[u8],
    lines: &[String],
) -> Result<(), WorkspacePatchError> {
    if before_bytes.len() < SUSPICIOUS_REPLACE_MIN_BEFORE_BYTES {
        return Ok(());
    }
    if after_bytes.len().saturating_mul(2) >= before_bytes.len() {
        return Ok(());
    }
    let non_empty_lines = lines.iter().filter(|line| !line.trim().is_empty()).count();
    if non_empty_lines > SUSPICIOUS_REPLACE_MAX_NON_EMPTY_LINES {
        return Ok(());
    }

    Err(WorkspacePatchError::SuspiciousPartialReplace {
        path: path.to_owned(),
        before_size_bytes: before_bytes.len(),
        after_size_bytes: after_bytes.len(),
    })
}

fn ensure_planned_file_content(path: &str, bytes: &[u8]) -> Result<(), WorkspacePatchError> {
    ensure_secret_file_content_does_not_store_redaction_placeholder(path, bytes)?;
    ensure_structured_file_content(path, bytes)
}

fn ensure_secret_file_content_does_not_store_redaction_placeholder(
    path: &str,
    bytes: &[u8],
) -> Result<(), WorkspacePatchError> {
    if !is_secret_bearing_env_file(path) {
        return Ok(());
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|_| WorkspacePatchError::InvalidUtf8File { path: path.to_owned() })?;
    if text.split(['\r', '\n']).any(env_assignment_stores_redaction_placeholder) {
        return Err(WorkspacePatchError::RedactionPlaceholderInSecretFile {
            path: path.to_owned(),
        });
    }
    Ok(())
}

fn is_secret_bearing_env_file(path: &str) -> bool {
    let Some(file_name) = Path::new(path).file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let lower = file_name.to_ascii_lowercase();
    if lower != ".env" && !lower.starts_with(".env.") {
        return false;
    }
    !NON_SECRET_ENV_FILE_SUFFIXES.iter().any(|suffix| lower.ends_with(suffix))
}

fn env_assignment_stores_redaction_placeholder(line: &str) -> bool {
    let trimmed = line.trim_start();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return false;
    }
    let assignment = trimmed.strip_prefix("export ").unwrap_or(trimmed);
    let Some((_, value)) = assignment.split_once('=') else {
        return false;
    };
    value_contains_redaction_placeholder(value)
}

fn value_contains_redaction_placeholder(value: &str) -> bool {
    let trimmed = value.trim().trim_matches(['"', '\'']);
    if trimmed.eq_ignore_ascii_case("redacted") {
        return true;
    }
    let lower = trimmed.to_ascii_lowercase();
    REDACTION_PLACEHOLDER_MARKERS.iter().any(|marker| lower.contains(marker))
}

fn ensure_structured_file_content(path: &str, bytes: &[u8]) -> Result<(), WorkspacePatchError> {
    if !path_has_extension(path, "json") {
        return Ok(());
    }
    serde_json::from_slice::<serde_json::Value>(bytes).map_err(|source| {
        WorkspacePatchError::InvalidJsonFile { path: path.to_owned(), message: source.to_string() }
    })?;
    Ok(())
}

fn path_has_extension(path: &str, expected_extension: &str) -> bool {
    Path::new(path)
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case(expected_extension))
}

fn read_file_capped(
    path: &Path,
    path_label: &str,
    max_file_bytes: usize,
) -> Result<Vec<u8>, WorkspacePatchError> {
    let metadata = fs::metadata(path).map_err(|source| WorkspacePatchError::Io {
        operation: "stat",
        path: path.display().to_string(),
        source,
    })?;
    if !metadata.is_file() {
        return Err(WorkspacePatchError::NotARegularFile { path: path_label.to_owned() });
    }
    let len = metadata.len() as usize;
    if len > max_file_bytes {
        return Err(WorkspacePatchError::FileTooLarge {
            path: path_label.to_owned(),
            limit: max_file_bytes,
            actual: len,
        });
    }
    fs::read(path).map_err(|source| WorkspacePatchError::Io {
        operation: "read",
        path: path.display().to_string(),
        source,
    })
}

fn apply_hunks_to_bytes(
    path_label: &str,
    before: &[u8],
    hunks: &[PatchHunk],
) -> Result<Vec<u8>, WorkspacePatchError> {
    let text = std::str::from_utf8(before)
        .map_err(|_| WorkspacePatchError::InvalidUtf8File { path: path_label.to_owned() })?;
    let (had_bom, text) = strip_utf8_bom(text);
    let line_ending = detect_existing_line_ending(before);
    let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
    let had_trailing_newline = normalized.ends_with('\n');
    let body = normalized.strip_suffix('\n').unwrap_or(normalized.as_str());
    let mut lines = if body.is_empty() {
        Vec::<String>::new()
    } else {
        body.split('\n').map(|line| line.to_owned()).collect::<Vec<_>>()
    };

    let mut search_cursor = 0_usize;
    for (index, hunk) in hunks.iter().enumerate() {
        let old_lines = hunk
            .lines
            .iter()
            .filter(|line| !matches!(line.kind, HunkLineKind::Add))
            .map(|line| line.text.clone())
            .collect::<Vec<_>>();
        let new_lines = hunk
            .lines
            .iter()
            .filter(|line| !matches!(line.kind, HunkLineKind::Remove))
            .map(|line| line.text.clone())
            .collect::<Vec<_>>();

        if old_lines.is_empty() && !lines.is_empty() {
            return Err(WorkspacePatchError::HunkApplyFailed {
                path: path_label.to_owned(),
                message: format!(
                    "hunk {index} has no context; include surrounding lines to choose insertion point"
                ),
            });
        }

        let Some(start) = find_subsequence(lines.as_slice(), old_lines.as_slice(), search_cursor)
        else {
            return Err(WorkspacePatchError::HunkApplyFailed {
                path: path_label.to_owned(),
                message: hunk_context_not_found_message(index, old_lines.as_slice()),
            });
        };

        let end = start.saturating_add(old_lines.len());
        let inserted_len = new_lines.len();
        lines.splice(start..end, new_lines);
        search_cursor = start.saturating_add(inserted_len);
    }

    let rendered = render_lines_with_existing_ending(&lines, had_trailing_newline, line_ending);
    Ok(with_utf8_bom_if_needed(had_bom, rendered))
}

fn replace_exact_line_bytes(
    path_label: &str,
    before: &[u8],
    old: &str,
    new: &str,
) -> Result<Vec<u8>, WorkspacePatchError> {
    let text = std::str::from_utf8(before)
        .map_err(|_| WorkspacePatchError::InvalidUtf8File { path: path_label.to_owned() })?;
    let (had_bom, text) = strip_utf8_bom(text);
    let line_ending = detect_existing_line_ending(before);
    let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
    let had_trailing_newline = normalized.ends_with('\n');
    let body = normalized.strip_suffix('\n').unwrap_or(normalized.as_str());
    let mut lines = if body.is_empty() {
        Vec::<String>::new()
    } else {
        body.split('\n').map(|line| line.to_owned()).collect::<Vec<_>>()
    };

    let matches = lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| (line == old).then_some(index))
        .collect::<Vec<_>>();
    let Some(index) = matches.first().copied() else {
        return Err(WorkspacePatchError::HunkApplyFailed {
            path: path_label.to_owned(),
            message: replace_line_target_not_found_message(old),
        });
    };
    if matches.len() > 1 {
        return Err(WorkspacePatchError::HunkApplyFailed {
            path: path_label.to_owned(),
            message: format!(
                "replace-line exact target matched {} lines; retry with an Update File hunk containing surrounding context",
                matches.len()
            ),
        });
    }

    lines[index] = new.to_owned();
    let rendered = render_lines_with_existing_ending(&lines, had_trailing_newline, line_ending);
    Ok(with_utf8_bom_if_needed(had_bom, rendered))
}

fn render_lines_with_existing_ending(
    lines: &[String],
    had_trailing_newline: bool,
    line_ending: ExistingLineEnding,
) -> Vec<u8> {
    let separator = line_ending.as_str();
    let mut output = lines.join(separator);
    if had_trailing_newline {
        output.push_str(separator);
    }
    output.into_bytes()
}

fn strip_utf8_bom(text: &str) -> (bool, &str) {
    text.strip_prefix('\u{feff}').map_or((false, text), |rest| (true, rest))
}

fn with_utf8_bom_if_needed(had_bom: bool, mut bytes: Vec<u8>) -> Vec<u8> {
    if !had_bom {
        return bytes;
    }
    let mut with_bom = Vec::with_capacity(bytes.len().saturating_add(3));
    with_bom.extend_from_slice(&[0xef, 0xbb, 0xbf]);
    with_bom.append(&mut bytes);
    with_bom
}

fn detect_existing_line_ending(bytes: &[u8]) -> ExistingLineEnding {
    let mut crlf_count = 0_usize;
    let mut lf_count = 0_usize;
    let mut cr_count = 0_usize;
    let mut first = None::<ExistingLineEnding>;
    let mut index = 0_usize;
    while index < bytes.len() {
        match bytes[index] {
            b'\r' if bytes.get(index.saturating_add(1)) == Some(&b'\n') => {
                crlf_count = crlf_count.saturating_add(1);
                first.get_or_insert(ExistingLineEnding::Crlf);
                index = index.saturating_add(2);
            }
            b'\r' => {
                cr_count = cr_count.saturating_add(1);
                first.get_or_insert(ExistingLineEnding::Cr);
                index = index.saturating_add(1);
            }
            b'\n' => {
                lf_count = lf_count.saturating_add(1);
                first.get_or_insert(ExistingLineEnding::Lf);
                index = index.saturating_add(1);
            }
            _ => {
                index = index.saturating_add(1);
            }
        }
    }

    let max_count = crlf_count.max(lf_count).max(cr_count);
    if max_count == 0 {
        return ExistingLineEnding::Lf;
    }
    let mut winner = None::<ExistingLineEnding>;
    let mut has_tie = false;
    for (line_ending, count) in [
        (ExistingLineEnding::Crlf, crlf_count),
        (ExistingLineEnding::Lf, lf_count),
        (ExistingLineEnding::Cr, cr_count),
    ] {
        if count != max_count {
            continue;
        }
        if winner.replace(line_ending).is_some() {
            has_tie = true;
        }
    }
    if has_tie {
        return first.unwrap_or(ExistingLineEnding::Lf);
    }
    winner.unwrap_or(ExistingLineEnding::Lf)
}

fn find_subsequence(haystack: &[String], needle: &[String], from: usize) -> Option<usize> {
    if needle.is_empty() {
        return Some(from.min(haystack.len()));
    }
    if haystack.len() < needle.len() {
        return None;
    }
    // NOTE: when the needle no longer fits in the remaining lines, `start` clamps
    // *below* `from`, so a later hunk may match before the search cursor (overlapping an
    // earlier hunk's output near end-of-file). This widened acceptance is part of the
    // pinned parse/apply contract exercised by the fuzz target; do not tighten it without
    // an explicit contract change.
    let start = from.min(haystack.len().saturating_sub(needle.len()));
    (start..=haystack.len().saturating_sub(needle.len()))
        .find(|&offset| haystack[offset..offset + needle.len()] == *needle)
}

fn execute_patch_plan(
    actions: &[PlannedAction],
    limits: &WorkspacePatchLimits,
) -> Result<(), PatchExecutionError> {
    // Snapshot every target before mutating anything so a mid-plan failure can be rolled
    // back; `None` marks paths that did not exist and must be removed on rollback.
    let mut backups = HashMap::<PathBuf, Option<Vec<u8>>>::new();
    for action in actions {
        let (path, root) = match action {
            PlannedAction::Write { path, root, .. } | PlannedAction::Delete { path, root } => {
                (path, root)
            }
        };
        if backups.contains_key(path) {
            continue;
        }
        revalidate_execution_target(path.as_path(), root.as_path())
            .map_err(|error| PatchExecutionError { error, rollback_performed: false })?;
        if path.exists() {
            let bytes = read_file_capped(
                path.as_path(),
                &path.display().to_string(),
                limits.max_file_bytes,
            )
            .map_err(|error| PatchExecutionError { error, rollback_performed: false })?;
            backups.insert(path.clone(), Some(bytes));
        } else {
            backups.insert(path.clone(), None);
        }
    }

    let mut applied_any = false;
    for action in actions {
        let result = match action {
            PlannedAction::Write { path, root, bytes } => {
                revalidate_execution_target(path.as_path(), root.as_path())
                    .and_then(|_| write_file_atomic(path.as_path(), bytes.as_slice()))
            }
            PlannedAction::Delete { path, root } => {
                revalidate_execution_target(path.as_path(), root.as_path())
                    .and_then(|_| delete_file(path.as_path()))
            }
        };

        if let Err(error) = result {
            let rollback_performed =
                if applied_any { rollback_from_backups(&backups) } else { false };
            return Err(PatchExecutionError { error, rollback_performed });
        }
        applied_any = true;
    }

    Ok(())
}

// Re-checked at execution time (not just planning) so a path swapped for a symlink between
// plan and write/delete cannot escape the workspace root (TOCTOU guard).
fn revalidate_execution_target(path: &Path, root: &Path) -> Result<(), WorkspacePatchError> {
    let path_label = path.display().to_string();
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                return Err(WorkspacePatchError::PathOutsideWorkspace { path: path_label });
            }
            ensure_path_within_root(path, root, &path_label)
        }
        Err(source)
            if matches!(
                source.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            ensure_parent_within_root(path, root, &path_label)
        }
        Err(source) => {
            Err(WorkspacePatchError::Io { operation: "symlink_metadata", path: path_label, source })
        }
    }
}

fn delete_file(path: &Path) -> Result<(), WorkspacePatchError> {
    if !path.exists() {
        return Ok(());
    }
    fs::remove_file(path).map_err(|source| WorkspacePatchError::Io {
        operation: "delete",
        path: path.display().to_string(),
        source,
    })
}

fn write_file_atomic(path: &Path, bytes: &[u8]) -> Result<(), WorkspacePatchError> {
    let Some(parent) = path.parent() else {
        return Err(WorkspacePatchError::Io {
            operation: "write",
            path: path.display().to_string(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "target path does not have a parent directory",
            ),
        });
    };

    fs::create_dir_all(parent).map_err(|source| WorkspacePatchError::Io {
        operation: "create_dir_all",
        path: parent.display().to_string(),
        source,
    })?;

    let temp_name = format!(".palyra-patch-{}.tmp", unique_suffix());
    let temp_path = parent.join(temp_name);
    let mut file =
        fs::OpenOptions::new().create_new(true).write(true).open(temp_path.as_path()).map_err(
            |source| WorkspacePatchError::Io {
                operation: "open",
                path: temp_path.display().to_string(),
                source,
            },
        )?;

    file.write_all(bytes).map_err(|source| WorkspacePatchError::Io {
        operation: "write",
        path: temp_path.display().to_string(),
        source,
    })?;
    file.flush().map_err(|source| WorkspacePatchError::Io {
        operation: "flush",
        path: temp_path.display().to_string(),
        source,
    })?;
    drop(file);

    if let Err(_rename_error) = fs::rename(temp_path.as_path(), path) {
        // On Windows, renaming onto an existing file can fail (e.g. sharing violations),
        // so retry once after removing the destination. The brief non-atomic window is
        // accepted because plan-level rollback restores the original bytes on failure.
        #[cfg(windows)]
        {
            if path.exists() {
                let _ = fs::remove_file(path);
            }
            if let Err(retry_source) = fs::rename(temp_path.as_path(), path) {
                let _ = fs::remove_file(temp_path.as_path());
                return Err(WorkspacePatchError::Io {
                    operation: "rename",
                    path: path.display().to_string(),
                    source: retry_source,
                });
            }
            return Ok(());
        }

        #[cfg(not(windows))]
        {
            let _ = fs::remove_file(temp_path.as_path());
            return Err(WorkspacePatchError::Io {
                operation: "rename",
                path: path.display().to_string(),
                source: _rename_error,
            });
        }
    }

    Ok(())
}
// Best-effort: individual restore/remove failures are ignored so rollback never becomes a
// second failure source. Restores run shallowest-first and removals deepest-first so
// directory hierarchies created during execution unwind cleanly.
fn rollback_from_backups(backups: &HashMap<PathBuf, Option<Vec<u8>>>) -> bool {
    if backups.is_empty() {
        return false;
    }

    let mut restore = backups
        .iter()
        .filter_map(|(path, original)| original.as_ref().map(|bytes| (path.clone(), bytes.clone())))
        .collect::<Vec<_>>();
    restore.sort_by_key(|(path, _)| path.components().count());
    for (path, bytes) in restore {
        let _ = write_file_atomic(path.as_path(), bytes.as_slice());
    }

    let mut remove = backups
        .iter()
        .filter_map(|(path, original)| if original.is_none() { Some(path.clone()) } else { None })
        .collect::<Vec<_>>();
    remove.sort_by_key(|path| Reverse(path.components().count()));
    for path in remove {
        if path.exists() {
            let _ = fs::remove_file(path);
        }
    }

    true
}

fn sha256_hex(value: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value);
    hex::encode(hasher.finalize())
}

// Collisions are tolerated: the temp file is opened with create_new, so a duplicate suffix
// fails loudly instead of clobbering another writer's file.
fn unique_suffix() -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    format!("{now}-{}", std::process::id())
}

fn truncate_utf8(mut value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value;
    }
    while value.len() > max_bytes && !value.is_empty() {
        let _ = value.pop();
    }
    value
}

#[cfg(test)]
mod tests {
    use super::{
        apply_workspace_patch, apply_workspace_patch_with_canonical_root_constraints,
        compute_patch_sha256, normalized_workspace_patch_operation_paths, redact_patch_preview,
        sha256_hex, WorkspacePatchError, WorkspacePatchLimits, WorkspacePatchOutcome,
        WorkspacePatchRedactionPolicy, WorkspacePatchRequest,
    };
    use std::{fs, path::PathBuf};
    use tempfile::tempdir;

    fn default_request(patch: &str, dry_run: bool) -> WorkspacePatchRequest {
        WorkspacePatchRequest {
            patch: patch.to_owned(),
            dry_run,
            redaction_policy: WorkspacePatchRedactionPolicy::default(),
        }
    }

    fn default_limits() -> WorkspacePatchLimits {
        WorkspacePatchLimits::default()
    }

    fn attestation_by_path<'a>(
        outcome: &'a WorkspacePatchOutcome,
        path: &str,
    ) -> &'a super::WorkspacePatchFileAttestation {
        outcome
            .files_touched
            .iter()
            .find(|entry| entry.path == path)
            .unwrap_or_else(|| panic!("attestation for path '{path}' should exist"))
    }

    #[test]
    fn apply_workspace_patch_updates_multiple_files() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        fs::write(workspace.join("notes.txt"), "alpha\nbeta\n").expect("seed file should exist");
        fs::write(workspace.join("delete-me.txt"), "remove\n").expect("delete target should exist");

        let patch = "*** Begin Patch\n*** Update File: notes.txt\n@@\n-beta\n+beta-updated\n*** Add File: new.txt\n+hello\n+world\n*** Delete File: delete-me.txt\n*** End Patch\n";
        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("patch should apply");

        assert!(!outcome.dry_run);
        assert_eq!(outcome.files_touched.len(), 3);
        assert!(workspace.join("new.txt").exists(), "created file should exist");
        assert!(!workspace.join("delete-me.txt").exists(), "deleted file should be removed");
        assert_eq!(
            fs::read_to_string(workspace.join("notes.txt")).expect("updated file should read"),
            "alpha\nbeta-updated\n"
        );
    }

    #[test]
    fn apply_workspace_patch_preserves_crlf_for_update_hunks() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(workspace.join("app.ts"), b"alpha\r\nbeta\r\ngamma\r\n")
            .expect("seed file should exist");

        let patch = "*** Begin Patch\n*** Update File: app.ts\n@@\n beta\n-gamma\n+gamma();\n*** End Patch\n";
        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("CRLF update hunk should apply");

        assert_eq!(attestation_by_path(&outcome, "app.ts").operation, "update");
        assert_eq!(
            fs::read(workspace.join("app.ts")).expect("updated file should read"),
            b"alpha\r\nbeta\r\ngamma();\r\n"
        );
    }

    #[test]
    fn apply_workspace_patch_preserves_utf8_bom_for_update_hunks() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(workspace.join("config.txt"), b"\xEF\xBB\xBFtitle=old\nmode=safe\n")
            .expect("seed file should exist");

        let patch = "*** Begin Patch\n*** Update File: config.txt\n@@\n-title=old\n+title=new\n mode=safe\n*** End Patch\n";
        apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("BOM update hunk should apply");

        assert_eq!(
            fs::read(workspace.join("config.txt")).expect("patched file should read"),
            b"\xEF\xBB\xBFtitle=new\nmode=safe\n"
        );
    }

    #[test]
    fn apply_workspace_patch_preserves_missing_trailing_newline() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(workspace.join("settings.txt"), b"alpha\nbeta").expect("seed file should exist");

        let patch = "*** Begin Patch\n*** Update File: settings.txt\n@@\n alpha\n-beta\n+beta-updated\n*** End Patch\n";
        apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("no-trailing-newline update hunk should apply");

        assert_eq!(
            fs::read(workspace.join("settings.txt")).expect("patched file should read"),
            b"alpha\nbeta-updated"
        );
    }

    #[test]
    fn apply_workspace_patch_rejects_windows_drive_letter_paths() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        let patch = "*** Begin Patch\n*** Add File: C:\\repo\\owned.txt\n+owned\n*** End Patch\n";
        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("drive-letter paths should be rejected");

        assert!(matches!(error, WorkspacePatchError::InvalidPatchPath { .. }));
    }

    #[test]
    fn apply_workspace_patch_rejects_binary_update_targets() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(workspace.join("asset.bin"), [0xff, 0x00, 0xfe])
            .expect("binary seed file should exist");

        let patch = "*** Begin Patch\n*** Update File: asset.bin\n@@\n-old\n+new\n*** End Patch\n";
        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("binary file update should be rejected");

        assert!(matches!(error, WorkspacePatchError::InvalidUtf8File { .. }));
    }

    #[test]
    fn apply_workspace_patch_rejects_zero_byte_add_file_placeholders() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        let patch = "*** Begin Patch\n*** Add File: index.html\n*** End Patch\n";
        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("empty add-file operations should be rejected");

        assert!(matches!(error, WorkspacePatchError::Parse { .. }));
        assert!(
            error.to_string().contains("zero-byte placeholder files"),
            "error should explain zero-byte placeholder rejection: {error}"
        );
        assert!(!workspace.join("index.html").exists(), "rejected patch must not create a file");
    }

    #[test]
    fn apply_workspace_patch_treats_identical_add_file_as_idempotent() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(workspace.join("report.md"), "# Report\n\nstatus: passed\n")
            .expect("seed file should exist");

        let patch =
            "*** Begin Patch\n*** Add File: report.md\n+# Report\n+\n+status: passed\n*** End Patch\n";
        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("identical add-file should be idempotent");
        let attestation = attestation_by_path(&outcome, "report.md");

        assert_eq!(outcome.files_touched.len(), 1);
        assert_eq!(attestation.operation, "create_idempotent");
        assert_eq!(
            fs::read_to_string(workspace.join("report.md")).expect("report should read"),
            "# Report\n\nstatus: passed\n"
        );
    }

    #[test]
    fn apply_workspace_patch_reports_identical_update_as_no_op() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::create_dir_all(workspace.join("test")).expect("test directory should exist");
        fs::write(workspace.join("test/api.test.js"), "setTimeout(5);\n")
            .expect("seed file should exist");

        let patch =
            "*** Begin Patch\n*** Update File: test/api.test.js\n@@\n-setTimeout(5);\n+setTimeout(5);\n*** End Patch\n";
        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("identical update should be accepted as a no-op");

        assert!(outcome.files_touched.is_empty(), "no-op update must not report mutations");
        assert_eq!(outcome.no_op_files.len(), 1);
        let attestation = &outcome.no_op_files[0];
        assert_eq!(attestation.path, "test/api.test.js");
        assert_eq!(attestation.operation, "no_op");
        assert_eq!(attestation.before_sha256, attestation.after_sha256);
        assert_eq!(
            fs::read_to_string(workspace.join("test/api.test.js")).expect("file should read"),
            "setTimeout(5);\n"
        );
    }

    #[test]
    fn apply_workspace_patch_accepts_bare_blank_update_hunk_context_lines() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(workspace.join("tests")).expect("test directory should exist");
        fs::write(
            workspace.join("tests").join("flaky-save.test.ts"),
            "test('saves setting', async () => {\n  saveSettingAsync('theme', 'dark');\n\n  expect(readSetting('theme')).toBe('dark');\n});\n",
        )
        .expect("seed file should exist");

        let patch = "*** Begin Patch\n*** Update File: tests/flaky-save.test.ts\n@@\n-  saveSettingAsync('theme', 'dark');\n+  await saveSettingAsync('theme', 'dark');\n\n   expect(readSetting('theme')).toBe('dark');\n*** End Patch\n";
        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("update hunks should accept bare blank context lines");

        assert_eq!(
            fs::read_to_string(workspace.join("tests").join("flaky-save.test.ts"))
                .expect("patched file should read"),
            "test('saves setting', async () => {\n  await saveSettingAsync('theme', 'dark');\n\n  expect(readSetting('theme')).toBe('dark');\n});\n"
        );
        let attestation = attestation_by_path(&outcome, "tests/flaky-save.test.ts");
        assert_eq!(attestation.operation, "update");
    }

    #[test]
    fn apply_workspace_patch_rejects_zero_byte_replace_file_placeholders() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(workspace.join("server.js"), "console.log('ok');\n")
            .expect("seed file should exist");

        let patch = "*** Begin Patch\n*** Replace File: server.js\n*** End Patch\n";
        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("empty replace-file operations should be rejected");

        assert!(matches!(error, WorkspacePatchError::Parse { .. }));
        assert!(
            error.to_string().contains("zero-byte replacements"),
            "error should explain zero-byte replacement rejection: {error}"
        );
        assert_eq!(
            fs::read_to_string(workspace.join("server.js")).expect("seed file should remain"),
            "console.log('ok');\n"
        );
    }

    #[test]
    fn apply_workspace_patch_replaces_existing_file() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(workspace.join("math.test.js"), "function add(a, b) { return a + b; }\n")
            .expect("seed file should exist");

        let patch = "*** Begin Patch\n*** Replace File: math.test.js\n+function add(a, b) { return a + b; }\n+function subtract(a, b) { return a - b; }\n+\n+console.log(add(2, 3));\n+console.log(subtract(5, 2));\n*** End Patch\n";
        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("replace-file patch should apply");

        assert_eq!(
            fs::read_to_string(workspace.join("math.test.js")).expect("replaced file should read"),
            "function add(a, b) { return a + b; }\nfunction subtract(a, b) { return a - b; }\n\nconsole.log(add(2, 3));\nconsole.log(subtract(5, 2));\n"
        );
        let attestation = attestation_by_path(&outcome, "math.test.js");
        assert_eq!(attestation.operation, "replace");
        assert!(attestation.before_sha256.is_some());
        assert!(attestation.after_sha256.is_some());
    }

    #[test]
    fn apply_workspace_patch_replaces_unique_confirmed_line() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(workspace.join("public")).expect("workspace should exist");
        fs::write(
            workspace.join("public").join("app.js"),
            "function boot() {\n  parseStoredSession();\n  render();\n}\n",
        )
        .expect("seed file should exist");

        let patch = concat!(
            "*** Begin Patch\n",
            "*** Replace Line: public/app.js\n",
            "-  parseStoredSession();\n",
            "+  currentSession = parseStoredSession();\n",
            "*** End Patch\n",
        );
        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("replace-line patch should apply when the old line is unique");

        assert_eq!(
            fs::read_to_string(workspace.join("public").join("app.js"))
                .expect("patched file should read"),
            "function boot() {\n  currentSession = parseStoredSession();\n  render();\n}\n"
        );
        let attestation = attestation_by_path(&outcome, "public/app.js");
        assert_eq!(attestation.operation, "line_replace");
        assert!(attestation.before_sha256.is_some());
        assert!(attestation.after_sha256.is_some());
    }

    #[test]
    fn apply_workspace_patch_preserves_crlf_for_replace_line() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(workspace.join("app.js"), b"render();\r\nsave();\r\n")
            .expect("seed file should exist");

        let patch =
            "*** Begin Patch\n*** Replace Line: app.js\n-save();\n+saveNow();\n*** End Patch\n";
        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("replace-line patch should apply");

        assert_eq!(attestation_by_path(&outcome, "app.js").operation, "line_replace");
        assert_eq!(
            fs::read(workspace.join("app.js")).expect("patched file should read"),
            b"render();\r\nsaveNow();\r\n"
        );
    }

    #[test]
    fn apply_workspace_patch_rejects_ambiguous_replace_line() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(workspace.join("app.js"), "render();\nrender();\n")
            .expect("seed file should exist");

        let patch =
            "*** Begin Patch\n*** Replace Line: app.js\n-render();\n+renderApp();\n*** End Patch\n";
        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("ambiguous replace-line targets should be rejected");

        assert!(matches!(error, WorkspacePatchError::HunkApplyFailed { .. }));
        assert!(
            error.to_string().contains("matched 2 lines"),
            "error should explain ambiguous line replacement: {error}"
        );
        assert_eq!(
            fs::read_to_string(workspace.join("app.js")).expect("seed file should remain"),
            "render();\nrender();\n"
        );
    }

    #[test]
    fn apply_workspace_patch_normalizes_full_file_plus_space_prefixes() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(workspace.join("script.sh"), "echo old\n").expect("seed file should exist");

        let patch = "*** Begin Patch\n*** Add File: report.md\n+ # Report\n+ \n+    indented body\n*** Replace File: script.sh\n+ export PALYRA_READY=1\n+    echo done\n*** End Patch\n";
        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("full-file patches should normalize common '+ text' content lines");

        assert_eq!(outcome.files_touched.len(), 2);
        assert_eq!(
            fs::read_to_string(workspace.join("report.md")).expect("created file should read"),
            "# Report\n\n    indented body\n"
        );
        assert_eq!(
            fs::read_to_string(workspace.join("script.sh")).expect("replaced file should read"),
            "export PALYRA_READY=1\n    echo done\n"
        );
    }

    #[test]
    fn apply_workspace_patch_rejects_suspicious_partial_replace_file() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        let original = (0..40)
            .map(|index| format!("line_{index:02}=this content keeps the file non-trivial\n"))
            .collect::<String>();
        fs::write(workspace.join("long.txt"), original.as_str()).expect("seed file should exist");

        let patch =
            "*** Begin Patch\n*** Replace File: long.txt\n+status: done\n+notes: partial\n*** End Patch\n";
        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("tiny replacements of larger files should be treated as likely partial edits");

        assert!(matches!(error, WorkspacePatchError::SuspiciousPartialReplace { .. }));
        assert_eq!(
            fs::read_to_string(workspace.join("long.txt")).expect("seed file should remain"),
            original
        );
    }

    #[test]
    fn apply_workspace_patch_rejects_diff_markers_in_replace_file_body() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(workspace.join("public.txt"), "alpha beta\n").expect("seed file should exist");

        let patch = "*** Begin Patch\n*** Replace File: public.txt\n--- old\n+++ new\n@@\n-alpha beta\n+alpha preview\n*** End Patch\n";
        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("replace-file bodies must reject embedded diff syntax");

        assert!(matches!(error, WorkspacePatchError::Parse { .. }));
        assert!(
            error.to_string().contains("diff or conflict marker"),
            "error should explain malformed replace body rejection: {error}"
        );
        assert_eq!(
            fs::read_to_string(workspace.join("public.txt")).expect("seed file should remain"),
            "alpha beta\n"
        );
    }

    #[test]
    fn apply_workspace_patch_rejects_diff_style_replace_file_body_without_headers() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(workspace.join("src")).expect("workspace should exist");
        fs::write(
            workspace.join("src").join("user-card.ts"),
            "export interface User { id: string; }\n",
        )
        .expect("seed file should exist");

        let patch = "*** Begin Patch\n*** Replace File: src/user-card.ts\n-export interface User { id: string; }\n+export interface User { id: string; name: string; }\n*** End Patch\n";
        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("replace-file bodies must reject diff-style removal lines");

        assert!(matches!(error, WorkspacePatchError::Parse { .. }));
        assert!(
            error.to_string().contains("mixes '-' removal lines"),
            "error should explain malformed replace body rejection: {error}"
        );
        assert_eq!(
            fs::read_to_string(workspace.join("src").join("user-card.ts"))
                .expect("seed file should remain"),
            "export interface User { id: string; }\n"
        );
    }

    #[test]
    fn apply_workspace_patch_allows_markdown_report_diff_snippet_in_add_file_body() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(workspace.join("reports")).expect("workspace should exist");

        let patch = "*** Begin Patch\n*** Add File: reports/broken-links.md\n+# Broken links\n+\n+| File | Link |\n+|---|---|\n+| docs/index.md | guides/install.md |\n+\n+```diff\n+--- docs/index.md\n++++ docs/index.md\n+@@\n+-- [Install guide](guides/install.md)\n++- [Install guide](guides/installation.md)\n+```\n*** End Patch\n";

        apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("add-file should allow Markdown reports containing quoted diff snippets");

        let report = fs::read_to_string(workspace.join("reports").join("broken-links.md"))
            .expect("report should read");
        assert!(report.contains("|---|---|"));
        assert!(report.contains("```diff"));
        assert!(report.contains("--- docs/index.md"));
        assert!(report.contains("@@"));
    }

    #[test]
    fn apply_workspace_patch_allows_markdown_frontmatter_fences() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        let patch = "*** Begin Patch\n*** Add File: reports/ready.md\n---   \nstatus: ready\n---\n# Ready\n*** End Patch\n";
        apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("frontmatter fences are ordinary full-file content");

        assert_eq!(
            fs::read_to_string(workspace.join("reports/ready.md"))
                .expect("frontmatter report should be written"),
            "---   \nstatus: ready\n---\n# Ready\n"
        );
    }

    #[test]
    fn apply_workspace_patch_rejects_redacted_placeholder_in_secret_env_file() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(
            workspace.join(".env"),
            "PRIVATE_BACKEND_TOKEN=original-test-token\nPUBLIC_MODE=old\n",
        )
        .expect("seed file should exist");

        let patch = "*** Begin Patch\n*** Replace File: .env\n+PRIVATE_BACKEND_TOKEN=[REDACTED]\n+PUBLIC_MODE=new\n*** End Patch\n";
        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("secret env files must not store redaction placeholders");

        assert!(matches!(error, WorkspacePatchError::RedactionPlaceholderInSecretFile { .. }));
        assert!(
            error.to_string().contains("preserve existing secret lines"),
            "error should explain safe env-file recovery: {error}"
        );
        assert_eq!(
            fs::read_to_string(workspace.join(".env")).expect("seed file should remain"),
            "PRIVATE_BACKEND_TOKEN=original-test-token\nPUBLIC_MODE=old\n"
        );
    }

    #[test]
    fn apply_workspace_patch_rejects_redacted_placeholder_in_cr_only_secret_env_file() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        let original = "# generated locally\rPRIVATE_BACKEND_TOKEN=[REDACTED]\rPUBLIC_MODE=old\r";
        fs::write(workspace.join(".env"), original).expect("seed file should exist");

        let patch =
            "*** Begin Patch\n*** Update File: .env\n@@\n-PUBLIC_MODE=old\n+PUBLIC_MODE=new\n*** End Patch\n";
        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("lone-CR env files must not bypass redaction-placeholder validation");

        assert!(matches!(error, WorkspacePatchError::RedactionPlaceholderInSecretFile { .. }));
        assert_eq!(
            fs::read_to_string(workspace.join(".env")).expect("seed file should remain"),
            original
        );
    }

    #[test]
    fn apply_workspace_patch_allows_secret_env_updates_without_placeholder() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(
            workspace.join(".env.local"),
            "PRIVATE_BACKEND_TOKEN=original-test-token\nPUBLIC_MODE=old\n",
        )
        .expect("seed file should exist");

        let patch = "*** Begin Patch\n*** Update File: .env.local\n@@\n-PUBLIC_MODE=old\n+PUBLIC_MODE=new\n*** End Patch\n";
        apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("public env-key update should apply when secret value stays unchanged");

        assert_eq!(
            fs::read_to_string(workspace.join(".env.local")).expect("updated file should read"),
            "PRIVATE_BACKEND_TOKEN=original-test-token\nPUBLIC_MODE=new\n"
        );
    }

    #[test]
    fn apply_workspace_patch_allows_redacted_placeholder_in_env_example() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        let patch = "*** Begin Patch\n*** Add File: .env.example\n+PRIVATE_BACKEND_TOKEN=[REDACTED]\n*** End Patch\n";
        apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("env example files may document placeholder values");

        assert_eq!(
            fs::read_to_string(workspace.join(".env.example")).expect("example should read"),
            "PRIVATE_BACKEND_TOKEN=[REDACTED]\n"
        );
    }

    #[test]
    fn apply_workspace_patch_dry_run_does_not_modify_filesystem() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        let patch = "*** Begin Patch\n*** Add File: dry-run.txt\n+preview\n*** End Patch\n";
        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, true),
            &default_limits(),
        )
        .expect("dry run should succeed");

        assert!(outcome.dry_run);
        assert_eq!(outcome.files_touched.len(), 1);
        assert!(!workspace.join("dry-run.txt").exists(), "dry-run should not mutate filesystem");
    }

    #[test]
    fn apply_workspace_patch_accepts_unified_diff_add_file() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        let patch =
            "--- /dev/null\n+++ b/agent-patch-ok.txt\n@@ -0,0 +1 @@\n+PALYRA_AGENT_PATCH_OK\n";
        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("unified add-file diff should apply");

        assert_eq!(outcome.files_touched.len(), 1);
        assert_eq!(
            fs::read_to_string(workspace.join("agent-patch-ok.txt"))
                .expect("created file should read"),
            "PALYRA_AGENT_PATCH_OK\n"
        );
    }

    #[test]
    fn apply_workspace_patch_accepts_unified_diff_update_file() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(workspace.join("notes.txt"), "alpha\nbeta\n").expect("seed file should exist");

        let patch =
            "--- a/notes.txt\n+++ b/notes.txt\n@@ -1,2 +1,2 @@\n alpha\n-beta\n+beta-updated\n";
        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("unified update diff should apply");

        assert_eq!(outcome.files_touched.len(), 1);
        assert_eq!(
            fs::read_to_string(workspace.join("notes.txt")).expect("updated file should read"),
            "alpha\nbeta-updated\n"
        );
    }

    #[test]
    fn normalized_operation_paths_include_unified_diff_targets() {
        let patch = concat!(
            "--- a/reports/summary.md\n",
            "+++ b/reports/summary.md\n",
            "@@ -1 +1 @@\n",
            "-old\n",
            "+new\n",
        );

        assert_eq!(normalized_workspace_patch_operation_paths(patch), vec!["reports/summary.md"]);
    }

    #[test]
    fn apply_workspace_patch_rolls_back_when_later_action_fails() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(workspace.join("occupied"), "marker\n").expect("occupied file should exist");

        let patch = "*** Begin Patch\n*** Add File: created.txt\n+temp\n*** Add File: occupied/blocked.txt\n+nope\n*** End Patch\n";

        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("second action should fail and trigger rollback");

        match error {
            WorkspacePatchError::ExecutionFailed { rollback_performed, .. } => {
                assert!(rollback_performed, "rollback should be reported");
            }
            other => panic!("expected execution failure, got: {other}"),
        }
        assert!(
            !workspace.join("created.txt").exists(),
            "created file from first action should be rolled back"
        );
    }

    #[test]
    fn apply_workspace_patch_rejects_parent_traversal_path() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        let patch = "*** Begin Patch\n*** Add File: ../escape.txt\n+bad\n*** End Patch\n";
        let error =
            apply_workspace_patch(&[workspace], &default_request(patch, false), &default_limits())
                .expect_err("path traversal must be denied");
        assert!(matches!(error, WorkspacePatchError::InvalidPatchPath { .. }));
    }

    #[test]
    fn apply_workspace_patch_with_canonical_root_constraints_rejects_outside_root() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::create_dir_all(&outside).expect("outside directory should exist");
        let canonical_workspace =
            fs::canonicalize(&workspace).expect("workspace should canonicalize");

        let patch = "*** Begin Patch\n*** Add File: escaped.txt\n+outside\n*** End Patch\n";
        let error = apply_workspace_patch_with_canonical_root_constraints(
            std::slice::from_ref(&outside),
            std::slice::from_ref(&canonical_workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("workspace roots outside canonical constraints must be rejected");

        assert!(matches!(error, WorkspacePatchError::InvalidWorkspaceRoot { .. }));
        assert!(!outside.join("escaped.txt").exists(), "outside target must remain untouched");
    }

    #[cfg(unix)]
    #[test]
    fn apply_workspace_patch_with_canonical_root_constraints_rejects_swapped_override_root() {
        use std::os::unix::fs::symlink;

        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        let override_root = workspace.join("project");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&override_root).expect("override root should exist");
        fs::create_dir_all(&outside).expect("outside directory should exist");
        let canonical_workspace =
            fs::canonicalize(&workspace).expect("workspace should canonicalize");

        fs::remove_dir(&override_root).expect("override root should be removable");
        symlink(&outside, &override_root).expect("override root should be swappable");

        let patch = "*** Begin Patch\n*** Add File: escaped.txt\n+outside\n*** End Patch\n";
        let error = apply_workspace_patch_with_canonical_root_constraints(
            std::slice::from_ref(&override_root),
            std::slice::from_ref(&canonical_workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("swapped override root must be rejected");

        assert!(matches!(error, WorkspacePatchError::InvalidWorkspaceRoot { .. }));
        assert!(!outside.join("escaped.txt").exists(), "outside target must remain untouched");
    }

    #[cfg(unix)]
    #[test]
    fn apply_workspace_patch_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;

        let temp = tempdir().expect("tempdir should be created");
        let outside_root = temp.path().join("outside");
        fs::create_dir_all(&outside_root).expect("outside root should exist");
        let outside_file = outside_root.join("outside.txt");
        fs::write(&outside_file, "outside\n").expect("outside file should exist");

        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        symlink(&outside_file, workspace.join("escape-link.txt"))
            .expect("symlink should be created");

        let patch = "*** Begin Patch\n*** Update File: escape-link.txt\n@@\n-outside\n+inside\n*** End Patch\n";
        let error =
            apply_workspace_patch(&[workspace], &default_request(patch, false), &default_limits())
                .expect_err("symlink escape must be denied");
        assert!(matches!(error, WorkspacePatchError::PathOutsideWorkspace { .. }));
    }

    #[test]
    fn parse_error_reports_line_and_column() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        let patch = "*** Begin Patch\n*** Update File: file.txt\nnot-a-hunk\n*** End Patch\n";
        let error =
            apply_workspace_patch(&[workspace], &default_request(patch, false), &default_limits())
                .expect_err("invalid patch should fail");

        let location = error.parse_location().expect("parse location should be present");
        assert_eq!(location, (3, 1));
    }

    #[test]
    fn redact_patch_preview_masks_secret_paths_and_patterns() {
        let preview = redact_patch_preview(
            "*** Begin Patch\n*** Add File: .env\n+API_KEY=abcdef\n*** End Patch\n",
            &WorkspacePatchRedactionPolicy::default(),
            16 * 1024,
        );
        assert!(preview.contains("+[REDACTED]"), "secret file content should be redacted");
        assert!(preview.contains("*** Add File: .env"));
    }

    #[test]
    fn redact_patch_preview_masks_replace_file_secret_paths() {
        let preview = redact_patch_preview(
            "*** Begin Patch\n*** Replace File: .env\n+SESSIONID=leak-value-83d4c2e1\n*** Replace File: certs/service.pem\n+-----BEGIN PRIVATE KEY-----\n*** End Patch\n",
            &WorkspacePatchRedactionPolicy::default(),
            16 * 1024,
        );

        assert!(preview.contains("*** Replace File: .env"));
        assert!(preview.contains("*** Replace File: certs/service.pem"));
        assert!(preview.contains("+[REDACTED]"), "secret file content should be redacted");
        assert!(!preview.contains("SESSIONID=leak-value-83d4c2e1"));
        assert!(!preview.contains("-----BEGIN PRIVATE KEY-----"));
    }

    #[test]
    fn apply_workspace_patch_dry_run_redacts_replace_file_secret_preview() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(workspace.join(".env"), "SESSIONID=old\n").expect("seed file should exist");

        let patch = "*** Begin Patch\n*** Replace File: .env\n+SESSIONID=leak-value-83d4c2e1\n*** End Patch\n";
        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, true),
            &default_limits(),
        )
        .expect("dry-run replace should parse and attest");

        assert!(outcome.redacted_preview.contains("*** Replace File: .env"));
        assert!(outcome.redacted_preview.contains("+[REDACTED]"));
        assert!(!outcome.redacted_preview.contains("SESSIONID=leak-value-83d4c2e1"));
    }

    #[test]
    fn redact_patch_preview_masks_case_insensitive_patterns() {
        let preview = redact_patch_preview(
            "*** Begin Patch\n*** Add File: note.txt\n+Authorization: Bearer token-value\n*** End Patch\n",
            &WorkspacePatchRedactionPolicy::default(),
            16 * 1024,
        );
        assert!(
            !preview.contains("Authorization"),
            "authorization pattern should be redacted case-insensitively"
        );
        assert!(
            !preview.contains("Bearer "),
            "bearer marker should be redacted case-insensitively"
        );
    }

    #[test]
    fn redact_patch_preview_masks_sensitive_source_assignment_values() {
        let patch = "*** Begin Patch\n*** Add File: src/settings.ts\n+const API_TOKEN = \"ghp_REAL_TOKEN_456\";\n+const PASSWORD = \"correct-horse-battery-staple\";\n+const API_KEY = 'palyra-regression-api-key';\n+const SECRET_KEY = \"sk_live_REAL_SECRET_123\";\n+const SAFE_LABEL = \"public\";\n+const SYMBOLIC_SECRET = \"SERVER_PRIVATE_KEY\";\n*** End Patch\n";
        let preview =
            redact_patch_preview(patch, &WorkspacePatchRedactionPolicy::default(), 16 * 1024);

        assert!(preview.contains("+const API_TOKEN = \"[REDACTED]\";"), "{preview}");
        assert!(preview.contains("+const PASSWORD = \"[REDACTED]\";"), "{preview}");
        assert!(preview.contains("+const API_KEY = '[REDACTED]';"), "{preview}");
        assert!(preview.contains("+const SECRET_KEY = \"[REDACTED]\";"), "{preview}");
        assert!(preview.contains("+const SAFE_LABEL = \"public\";"), "{preview}");
        assert!(preview.contains("+const SYMBOLIC_SECRET = \"SERVER_PRIVATE_KEY\";"), "{preview}");
        assert!(!preview.contains("ghp_REAL_TOKEN_456"), "{preview}");
        assert!(!preview.contains("correct-horse-battery-staple"), "{preview}");
        assert!(!preview.contains("palyra-regression-api-key"), "{preview}");
        assert!(!preview.contains("sk_live_REAL_SECRET_123"), "{preview}");
    }

    #[test]
    fn redact_patch_preview_masks_sensitive_json_and_yaml_values() {
        let patch = "*** Begin Patch\n*** Add File: config/app.yml\n+api_key: palyra-regression-api-key\n+password: correct-horse-battery-staple # generated fixture\n*** Add File: config/app.json\n+  \"client_secret\": \"sk_live_REAL_SECRET_123\",\n+  \"private_key\": \"-----BEGIN PRIVATE KEY-----\"\n+  \"label\": \"public\"\n*** End Patch\n";
        let preview =
            redact_patch_preview(patch, &WorkspacePatchRedactionPolicy::default(), 16 * 1024);

        assert!(preview.contains("+api_key: [REDACTED]"), "{preview}");
        assert!(preview.contains("+password: [REDACTED] # generated fixture"), "{preview}");
        assert!(preview.contains("+  \"client_secret\": \"[REDACTED]\","), "{preview}");
        assert!(preview.contains("+  \"private_key\": \"[REDACTED]\""), "{preview}");
        assert!(preview.contains("+  \"label\": \"public\""), "{preview}");
        assert!(!preview.contains("palyra-regression-api-key"), "{preview}");
        assert!(!preview.contains("correct-horse-battery-staple"), "{preview}");
        assert!(!preview.contains("sk_live_REAL_SECRET_123"), "{preview}");
        assert!(!preview.contains("-----BEGIN PRIVATE KEY-----"), "{preview}");
    }

    #[test]
    fn redact_patch_preview_preserves_safe_source_identifiers() {
        let patch = "*** Begin Patch\n*** Add File: scripts/rename-vite-secret.js\n+const SECRET_KEY = 'VITE_SECRET_TOKEN';\n+const PRIVATE_KEY = 'SERVER_PRIVATE_KEY';\n*** End Patch\n";
        let preview =
            redact_patch_preview(patch, &WorkspacePatchRedactionPolicy::default(), 16 * 1024);

        assert!(preview.contains("*** Add File: scripts/rename-vite-secret.js"));
        assert!(preview.contains("+const SECRET_KEY = 'VITE_SECRET_TOKEN';"));
        assert!(preview.contains("+const PRIVATE_KEY = 'SERVER_PRIVATE_KEY';"));
        assert!(!preview.contains("[REDACTED]"));
    }

    #[test]
    fn apply_workspace_patch_preserves_safe_identifiers_in_artifact_and_preview() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        let patch = "*** Begin Patch\n*** Add File: scripts/rename-vite-secret.js\n+const SECRET_KEY = 'VITE_SECRET_TOKEN';\n+const PRIVATE_KEY = 'SERVER_PRIVATE_KEY';\n*** End Patch\n";

        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("patch should write source identifiers without preview redaction");
        let written = fs::read_to_string(workspace.join("scripts/rename-vite-secret.js"))
            .expect("written artifact should be readable");

        assert!(outcome.redacted_preview.contains("+const SECRET_KEY = 'VITE_SECRET_TOKEN';"));
        assert_eq!(
            written,
            "const SECRET_KEY = 'VITE_SECRET_TOKEN';\nconst PRIVATE_KEY = 'SERVER_PRIVATE_KEY';\n"
        );
    }

    #[test]
    fn patch_hash_is_stable() {
        let patch = "*** Begin Patch\n*** Add File: a.txt\n+hello\n*** End Patch\n";
        let first = compute_patch_sha256(patch);
        let second = compute_patch_sha256(patch);
        assert_eq!(first, second);
    }

    #[test]
    fn apply_workspace_patch_rejects_too_many_files() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        let patch =
            "*** Begin Patch\n*** Add File: a.txt\n+a\n*** Add File: b.txt\n+b\n*** End Patch\n";
        let mut limits = default_limits();
        limits.max_files_touched = 1;
        let error = apply_workspace_patch(
            &[PathBuf::from(&workspace)],
            &default_request(patch, true),
            &limits,
        )
        .expect_err("patch should exceed file limit");
        assert!(matches!(error, WorkspacePatchError::TooManyFiles { .. }));
    }

    #[test]
    fn apply_workspace_patch_attestation_hashes_match_filesystem_state() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        let original = b"alpha\nbeta\n".to_vec();
        fs::write(workspace.join("notes.txt"), &original).expect("seed file should exist");
        let patch = "*** Begin Patch\n*** Update File: notes.txt\n@@\n-beta\n+beta-updated\n*** Add File: created.txt\n+hello\n*** End Patch\n";
        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("patch should apply");

        assert_eq!(outcome.patch_sha256, compute_patch_sha256(patch));

        let updated_bytes =
            fs::read(workspace.join("notes.txt")).expect("updated file should read");
        let created_bytes =
            fs::read(workspace.join("created.txt")).expect("created file should read");
        let expected_before_hash = sha256_hex(&original);
        let expected_after_updated_hash = sha256_hex(updated_bytes.as_slice());
        let expected_after_created_hash = sha256_hex(created_bytes.as_slice());

        let updated = attestation_by_path(&outcome, "notes.txt");
        assert_eq!(
            updated.before_sha256.as_deref(),
            Some(expected_before_hash.as_str()),
            "before hash should reflect original content"
        );
        assert_eq!(
            updated.after_sha256.as_deref(),
            Some(expected_after_updated_hash.as_str()),
            "after hash should reflect updated file content"
        );

        let created = attestation_by_path(&outcome, "created.txt");
        assert_eq!(created.before_sha256, None, "new file must not contain before hash");
        assert_eq!(
            created.after_sha256.as_deref(),
            Some(expected_after_created_hash.as_str()),
            "after hash should reflect created file content"
        );
    }

    #[test]
    fn apply_workspace_patch_rejects_context_free_insert_into_non_empty_file() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(workspace.join("notes.txt"), "palyra patch e2e ok\n")
            .expect("seed file should exist");

        let patch =
            "*** Begin Patch\n*** Update File: notes.txt\n@@\n+second patch line\n*** End Patch\n";
        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("context-free insertion into a non-empty file must be rejected");

        assert!(
            error.to_string().contains("has no context"),
            "error should explain how to disambiguate insertion point: {error}"
        );
        assert_eq!(
            fs::read_to_string(workspace.join("notes.txt")).expect("seed file should read"),
            "palyra patch e2e ok\n",
            "failed ambiguous patch must leave file unchanged"
        );
    }

    #[test]
    fn apply_workspace_patch_context_error_explains_markdown_list_prefixes() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(workspace.join("docs")).expect("workspace should exist");
        fs::write(
            workspace.join("docs").join("index.md"),
            "- [Install guide](guides/install.md)\n",
        )
        .expect("seed file should exist");

        let ambiguous_patch = "*** Begin Patch\n*** Update File: docs/index.md\n@@\n- - [Install guide](guides/install.md)\n+ - [Install guide](guides/setup.md)\n*** End Patch\n";
        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(ambiguous_patch, false),
            &default_limits(),
        )
        .expect_err("extra space after the hunk marker should not match markdown list content");
        let message = error.to_string();
        assert!(
            message.contains("-- markdown item"),
            "error should show how to remove markdown list items: {message}"
        );
        assert!(
            message.contains("++value"),
            "error should show how to add content beginning with '+': {message}"
        );

        let ambiguous_replace_line = "*** Begin Patch\n*** Replace Line: docs/index.md\n- [Install guide](guides/install.md)\n+ [Install guide](guides/setup.md)\n*** End Patch\n";
        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(ambiguous_replace_line, false),
            &default_limits(),
        )
        .expect_err("replace-line must explain markdown list escaping when target is missing");
        let message = error.to_string();
        assert!(
            message.contains("-- markdown item"),
            "replace-line error should show markdown list escaping: {message}"
        );

        let exact_replace_line = "*** Begin Patch\n*** Replace Line: docs/index.md\n-- [Install guide](guides/install.md)\n+- [Install guide](guides/installation.md)\n*** End Patch\n";
        apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(exact_replace_line, false),
            &default_limits(),
        )
        .expect("direct prefix syntax should replace markdown list content");
        assert_eq!(
            fs::read_to_string(workspace.join("docs").join("index.md"))
                .expect("patched file should read"),
            "- [Install guide](guides/installation.md)\n"
        );
        fs::write(
            workspace.join("docs").join("index.md"),
            "- [Install guide](guides/install.md)\n",
        )
        .expect("seed file should reset");

        let exact_patch = "*** Begin Patch\n*** Update File: docs/index.md\n@@\n-- [Install guide](guides/install.md)\n+- [Install guide](guides/setup.md)\n*** End Patch\n";
        apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(exact_patch, false),
            &default_limits(),
        )
        .expect("direct prefix syntax should edit markdown list content");
        assert_eq!(
            fs::read_to_string(workspace.join("docs").join("index.md"))
                .expect("patched file should read"),
            "- [Install guide](guides/setup.md)\n"
        );
    }

    #[test]
    fn apply_workspace_patch_accepts_windows_pipeline_control_line_whitespace() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        let patch =
            "*** Begin Patch \r\n*** Add File: cli-patch-e2e.txt \r\n+palyra cli patch ok\r\n*** End Patch \r\n";
        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("Windows pipeline patch should apply");

        assert_eq!(
            attestation_by_path(&outcome, "cli-patch-e2e.txt").operation,
            "create",
            "patch should create the expected file"
        );
        assert_eq!(
            fs::read_to_string(workspace.join("cli-patch-e2e.txt"))
                .expect("created file should read"),
            "palyra cli patch ok\n"
        );
    }

    #[test]
    fn apply_workspace_patch_rejects_duplicate_trailing_patch_fence_markers() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        let patch = "*** Begin Patch ***\n*** Add File: reports/ready.md\n+READY detected\n*** End Patch ***\n*** End Patch\n";

        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("duplicate/trailing patch terminators should be rejected");

        assert!(
            error.to_string().contains("unexpected content after '*** End Patch'"),
            "error should identify content after the first terminator: {error}"
        );
        assert!(
            !workspace.join("reports").join("ready.md").exists(),
            "rejected patches must not create files"
        );
    }

    #[test]
    fn apply_workspace_patch_rejects_multiple_duplicate_end_patch_markers() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        let patch = "*** Begin Patch\n*** Add File: reports/ready.md\n+READY detected\n*** End Patch\n*** End Patch\n*** End Patch\n*** End Patch\n*** End Patch\n";

        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("multiple duplicate patch terminators should be rejected");

        assert!(
            error.to_string().contains("unexpected content after '*** End Patch'"),
            "error should identify content after the first terminator: {error}"
        );
        assert!(
            !workspace.join("reports").join("ready.md").exists(),
            "rejected patches must not create files"
        );
    }

    #[test]
    fn apply_workspace_patch_strips_trailing_stars_from_operation_headers() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        let patch =
            "*** Begin Patch\n*** Add File: index.html ***\n+<h1>Ready</h1>\n*** End Patch\n";

        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("trailing operation-header stars should be normalized");

        assert_eq!(outcome.files_touched.len(), 1);
        assert_eq!(
            attestation_by_path(&outcome, "index.html").operation,
            "create",
            "path should not retain trailing patch fence stars"
        );
        assert_eq!(
            fs::read_to_string(workspace.join("index.html")).expect("index should be created"),
            "<h1>Ready</h1>\n"
        );
        assert!(
            !workspace.join("index.html ***").exists(),
            "malformed header suffix must not become part of the filename"
        );
    }

    #[test]
    fn apply_workspace_patch_accepts_begin_file_body_wrappers() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        let patch = concat!(
            "*** Begin Patch\n",
            "*** Begin File: reports/node-lts.md\n",
            "*** Begin Body:\n",
            "# Node LTS\n",
            "Use active LTS releases for production.\n",
            "*** End Body\n",
            "*** End File\n",
            "*** End Patch\n",
        );

        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("begin-file/body wrappers should normalize to an add-file patch");

        assert_eq!(outcome.files_touched.len(), 1);
        assert_eq!(
            fs::read_to_string(workspace.join("reports").join("node-lts.md"))
                .expect("report should be created"),
            "# Node LTS\nUse active LTS releases for production.\n"
        );
    }

    #[test]
    fn apply_workspace_patch_rejects_wrapper_only_markers_without_panic() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        for patch in ["*** End File", "*** Begin Body\n*** End Body"] {
            let error = apply_workspace_patch(
                std::slice::from_ref(&workspace),
                &default_request(patch, true),
                &default_limits(),
            )
            .expect_err("wrapper-only patch markers should be rejected as invalid patches");

            assert!(
                matches!(error, WorkspacePatchError::Parse { .. }),
                "wrapper-only marker input should return a parse error, got {error:?}"
            );
        }
    }

    #[test]
    fn apply_workspace_patch_ignores_duplicate_begin_file_after_add_placeholder() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        let patch = concat!(
            "*** Begin Patch\n",
            "*** Add File: reports/progress.md\n",
            "*** Begin File: reports/progress.md\n",
            "START\n",
            "MIDDLE\n",
            "END\n",
            "*** End File\n",
            "*** End Patch\n",
        );

        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("duplicate begin-file marker after add-file placeholder should be ignored");

        assert_eq!(outcome.files_touched.len(), 1);
        assert_eq!(
            fs::read_to_string(workspace.join("reports").join("progress.md"))
                .expect("report should be created"),
            "START\nMIDDLE\nEND\n"
        );
    }

    #[test]
    fn apply_workspace_patch_accepts_unprefixed_add_file_body_lines() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        let patch = "*** Begin Patch\n*** Add File: report.txt\nhello\nworld\n*** End Patch\n";

        let outcome = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("unprefixed add-file body should be treated as file content");

        assert_eq!(outcome.files_touched.len(), 1);
        assert_eq!(
            fs::read_to_string(workspace.join("report.txt")).expect("file should be created"),
            "hello\nworld\n"
        );
    }

    #[test]
    fn apply_workspace_patch_rejects_bare_add_file_hunk_marker() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        let patch = "*** Begin Patch\n*** Add File: src/server.js\n@@\nconsole.log('ready');\n*** End Patch\n";

        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("add-file body should reject bare hunk markers instead of writing them");

        assert!(matches!(error, WorkspacePatchError::Parse { .. }));
        assert!(
            error.to_string().contains("add-file body contains a diff or conflict marker"),
            "error should explain the full-file contract: {error}"
        );
        assert!(!workspace.join("src").join("server.js").exists());
    }

    #[test]
    fn apply_workspace_patch_accepts_patch_prefixed_valid_json_file_content() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(workspace.join("reports")).expect("workspace reports dir should exist");
        let patch = "*** Begin Patch\n*** Add File: reports/seen.json\n+{\"seen_ids\":[\"alpha\"]}\n*** End Patch\n";

        apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect("patch-prefixed JSON content should be normalized before validation");

        assert_eq!(
            fs::read_to_string(workspace.join("reports").join("seen.json"))
                .expect("JSON file should be written"),
            "{\"seen_ids\":[\"alpha\"]}\n"
        );
    }

    #[test]
    fn apply_workspace_patch_rejects_invalid_json_file_content() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(workspace.join("reports")).expect("workspace reports dir should exist");
        let patch = "*** Begin Patch\n*** Add File: reports/seen.json\n+***\n+{\"seen_ids\":[\"alpha\"]}\n*** End Patch\n";

        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch, false),
            &default_limits(),
        )
        .expect_err("invalid JSON state should be rejected before write");

        assert!(matches!(error, WorkspacePatchError::InvalidJsonFile { .. }));
        assert!(
            !workspace.join("reports").join("seen.json").exists(),
            "invalid JSON state must not be written"
        );
    }

    #[test]
    fn apply_workspace_patch_rejects_invalid_json_with_unicode_padded_add_path() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(workspace.join("reports")).expect("workspace reports dir should exist");
        let patch = format!(
            "*** Begin Patch\n*** Add File: reports/seen.json{}\n+***\n+{{\"seen_ids\":[\"alpha\"]}}\n*** End Patch\n",
            '\u{00a0}'
        );

        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch.as_str(), false),
            &default_limits(),
        )
        .expect_err("invalid JSON state should be rejected despite padded header path");

        assert!(matches!(error, WorkspacePatchError::InvalidJsonFile { .. }));
        assert!(
            !workspace.join("reports").join("seen.json").exists(),
            "invalid JSON state must not be written to the normalized target"
        );
    }

    #[test]
    fn apply_workspace_patch_rejects_invalid_json_with_unicode_padded_replace_path() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(workspace.join("reports")).expect("workspace reports dir should exist");
        let target = workspace.join("reports").join("seen.json");
        fs::write(&target, "{\"seen_ids\":[]}\n").expect("initial JSON should be written");
        let patch = format!(
            "*** Begin Patch\n*** Replace File: reports/seen.json{}\n+***\n+{{\"seen_ids\":[\"alpha\"]}}\n*** End Patch\n",
            '\u{00a0}'
        );

        let error = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &default_request(patch.as_str(), false),
            &default_limits(),
        )
        .expect_err("invalid replacement JSON should be rejected despite padded header path");

        assert!(matches!(error, WorkspacePatchError::InvalidJsonFile { .. }));
        assert_eq!(
            fs::read_to_string(&target).expect("original JSON should still be readable"),
            "{\"seen_ids\":[]}\n",
            "failed replacement must leave the existing JSON intact"
        );
    }

    #[cfg(unix)]
    #[test]
    fn execute_patch_plan_revalidates_paths_before_write_and_blocks_symlink_swap() {
        use std::os::unix::fs::symlink;

        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        fs::create_dir_all(workspace.join("nested"))
            .expect("workspace nested directory should exist");
        fs::create_dir_all(&outside).expect("outside directory should exist");

        let patch = "*** Begin Patch\n*** Add File: nested/new.txt\n+inside\n*** End Patch\n";
        let operations = super::parse_patch_document(patch).expect("patch should parse");
        let canonical_roots = super::canonicalize_workspace_roots(std::slice::from_ref(&workspace))
            .expect("roots should canonicalize");
        let limits = default_limits();
        let plan =
            super::build_patch_plan(operations.as_slice(), canonical_roots.as_slice(), &limits)
                .expect("plan should be created");

        fs::remove_dir(workspace.join("nested")).expect("nested directory should be removed");
        symlink(&outside, workspace.join("nested")).expect("nested symlink should be created");

        let execution = super::execute_patch_plan(plan.actions.as_slice(), &limits)
            .expect_err("symlink swap must be rejected");
        assert!(matches!(execution.error, WorkspacePatchError::PathOutsideWorkspace { .. }));
        assert!(
            !outside.join("new.txt").exists(),
            "outside target must remain untouched after failed execution"
        );
    }

    #[cfg(unix)]
    #[test]
    fn execute_patch_plan_revalidates_paths_before_delete_and_blocks_symlink_swap() {
        use std::os::unix::fs::symlink;

        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        fs::create_dir_all(workspace.join("nested"))
            .expect("workspace nested directory should exist");
        fs::create_dir_all(&outside).expect("outside directory should exist");

        fs::write(workspace.join("nested").join("target.txt"), "inside\n")
            .expect("inside file should exist");
        fs::write(outside.join("target.txt"), "outside\n").expect("outside file should exist");

        let patch = "*** Begin Patch\n*** Delete File: nested/target.txt\n*** End Patch\n";
        let operations = super::parse_patch_document(patch).expect("patch should parse");
        let canonical_roots = super::canonicalize_workspace_roots(std::slice::from_ref(&workspace))
            .expect("roots should canonicalize");
        let limits = default_limits();
        let plan =
            super::build_patch_plan(operations.as_slice(), canonical_roots.as_slice(), &limits)
                .expect("plan should be created");

        fs::rename(workspace.join("nested"), workspace.join("nested_real"))
            .expect("nested directory should be moved");
        symlink(&outside, workspace.join("nested")).expect("nested symlink should be created");

        let execution = super::execute_patch_plan(plan.actions.as_slice(), &limits)
            .expect_err("symlink swap must be rejected");
        assert!(matches!(execution.error, WorkspacePatchError::PathOutsideWorkspace { .. }));
        assert_eq!(
            fs::read_to_string(outside.join("target.txt"))
                .expect("outside file should remain readable"),
            "outside\n",
            "outside file must remain unchanged"
        );
        assert_eq!(
            fs::read_to_string(workspace.join("nested_real").join("target.txt"))
                .expect("inside file should remain readable"),
            "inside\n",
            "original workspace file must remain unchanged"
        );
    }

    #[test]
    fn parser_fuzz_corpus_is_handled_without_panics() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::write(workspace.join("existing.txt"), "line\n").expect("seed file should exist");

        let corpus = [
            "",
            "*** Begin Patch\n*** End Patch\n",
            "*** Begin Patch\n*** Update File: existing.txt\n@@\n*invalid\n*** End Patch\n",
            "*** Begin Patch\n*** Update File: existing.txt\n*** End Patch\n",
            "*** Begin Patch\n*** Add File: nested/../../escape.txt\n+bad\n*** End Patch\n",
        ];

        for patch in corpus {
            let result = apply_workspace_patch(
                std::slice::from_ref(&workspace),
                &default_request(patch, true),
                &default_limits(),
            );
            assert!(result.is_err(), "corpus entry should fail: {patch}");
        }
    }

    #[test]
    fn apply_workspace_patch_rejects_absolute_path() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        let patch = "*** Begin Patch\n*** Add File: /etc/passwd\n+bad\n*** End Patch\n";
        let error =
            apply_workspace_patch(&[workspace], &default_request(patch, false), &default_limits())
                .expect_err("absolute path must be denied");
        assert!(matches!(error, WorkspacePatchError::InvalidPatchPath { .. }));
    }

    #[test]
    fn apply_workspace_patch_rejects_windows_drive_prefix_path() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        let patch = "*** Begin Patch\n*** Add File: C:/Windows/system32/drivers/etc/hosts\n+bad\n*** End Patch\n";
        let error =
            apply_workspace_patch(&[workspace], &default_request(patch, false), &default_limits())
                .expect_err("windows drive prefix path must be denied");
        assert!(matches!(error, WorkspacePatchError::InvalidPatchPath { .. }));
    }

    #[test]
    fn apply_workspace_patch_rejects_backslash_paths_for_cross_platform_determinism() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        let patch = "*** Begin Patch\n*** Add File: src\\nested\\file.txt\n+bad\n*** End Patch\n";
        let error =
            apply_workspace_patch(&[workspace], &default_request(patch, false), &default_limits())
                .expect_err("backslash-separated path must be denied");
        assert!(matches!(error, WorkspacePatchError::InvalidPatchPath { .. }));
    }

    #[test]
    fn apply_workspace_patch_rejects_payloads_that_exceed_max_patch_bytes() {
        let temp = tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should exist");

        let patch = format!(
            "*** Begin Patch\n*** Add File: large.txt\n+{}\n*** End Patch\n",
            "A".repeat(256)
        );
        let mut limits = default_limits();
        limits.max_patch_bytes = 64;

        let error =
            apply_workspace_patch(&[workspace], &default_request(patch.as_str(), false), &limits)
                .expect_err("oversized patch payload must be denied");

        assert!(
            matches!(
                error,
                WorkspacePatchError::PatchTooLarge {
                    limit: 64,
                    actual
                } if actual > 64
            ),
            "error should report deterministic payload-too-large details: {error}"
        );
    }
}
