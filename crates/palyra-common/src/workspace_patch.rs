use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    fs,
    io::Write,
    path::{Component, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

const DEFAULT_REDACTION_PATTERNS: &[&str] =
    &["api_key", "authorization", "bearer ", "secret", "token", "password"];
const DEFAULT_SECRET_FILE_MARKERS: &[&str] =
    &[".env", "id_rsa", "id_ed25519", "credentials", "secrets/", "secret/", ".pem", ".key"];

/// Execution limits for workspace patch processing.
///
/// Limits are fail-closed and enforced before any filesystem mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspacePatchLimits {
    pub max_patch_bytes: usize,
    pub max_files_touched: usize,
    pub max_file_bytes: usize,
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
    pub redaction_patterns: Vec<String>,
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
    pub patch: String,
    pub dry_run: bool,
    pub redaction_policy: WorkspacePatchRedactionPolicy,
}

/// Per-file attestation emitted for each touched file.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WorkspacePatchFileAttestation {
    pub path: String,
    pub workspace_root_index: usize,
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
    #[must_use]
    pub const fn parse_location(&self) -> Option<(usize, usize)> {
        match self {
            Self::Parse { line, column, .. } => Some((*line, *column)),
            _ => None,
        }
    }

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct HunkLine {
    kind: HunkLineKind,
    text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PatchPlan {
    actions: Vec<PlannedAction>,
    file_attestations: Vec<WorkspacePatchFileAttestation>,
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
pub fn apply_workspace_patch(
    workspace_roots: &[PathBuf],
    request: &WorkspacePatchRequest,
    limits: &WorkspacePatchLimits,
) -> Result<WorkspacePatchOutcome, WorkspacePatchError> {
    let patch_bytes = request.patch.as_bytes();
    if patch_bytes.len() > limits.max_patch_bytes {
        return Err(WorkspacePatchError::PatchTooLarge {
            limit: limits.max_patch_bytes,
            actual: patch_bytes.len(),
        });
    }

    let canonical_roots = canonicalize_workspace_roots(workspace_roots)?;
    let operations = parse_patch_document(request.patch.as_str())?;
    if operations.len() > limits.max_files_touched {
        return Err(WorkspacePatchError::TooManyFiles {
            limit: limits.max_files_touched,
            actual: operations.len(),
        });
    }

    let patch_sha256 = compute_patch_sha256(request.patch.as_str());
    let redacted_preview = redact_patch_preview(
        request.patch.as_str(),
        &request.redaction_policy,
        limits.max_preview_bytes,
    );

    let plan = build_patch_plan(operations.as_slice(), canonical_roots.as_slice(), limits)?;

    if request.dry_run {
        return Ok(WorkspacePatchOutcome {
            patch_sha256,
            dry_run: true,
            files_touched: plan.file_attestations,
            rollback_performed: false,
            redacted_preview,
        });
    }

    match execute_patch_plan(plan.actions.as_slice(), limits) {
        Ok(()) => Ok(WorkspacePatchOutcome {
            patch_sha256,
            dry_run: false,
            files_touched: plan.file_attestations,
            rollback_performed: false,
            redacted_preview,
        }),
        Err(execution) => Err(WorkspacePatchError::ExecutionFailed {
            message: execution.error.to_string(),
            rollback_performed: execution.rollback_performed,
        }),
    }
}

/// Computes a deterministic SHA256 digest of the raw patch payload.
#[must_use]
pub fn compute_patch_sha256(patch: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(patch.as_bytes());
    hex::encode(hasher.finalize())
}

/// Produces a redacted, size-capped preview of the patch payload.
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

        rendered.push(line.to_owned());
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

fn replace_ascii_case_insensitive(haystack: &str, needle: &str, replacement: &str) -> String {
    if needle.is_empty() {
        return haystack.to_owned();
    }
    if !needle.is_ascii() {
        return haystack.replace(needle, replacement);
    }

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
fn parse_patch_document(patch: &str) -> Result<Vec<PatchOperation>, WorkspacePatchError> {
    let normalized = patch.replace("\r\n", "\n").replace('\r', "\n");
    let lines = normalized.split('\n').collect::<Vec<_>>();
    if lines.is_empty() || lines[0] != "*** Begin Patch" {
        return Err(parse_error(1, 1, "expected '*** Begin Patch'"));
    }

    let mut index = 1_usize;
    let mut operations = Vec::new();
    let mut ended = false;

    while index < lines.len() {
        let line = lines[index];
        if line == "*** End Patch" {
            ended = true;
            index = index.saturating_add(1);
            break;
        }
        if line.is_empty() {
            index = index.saturating_add(1);
            continue;
        }

        if let Some(path) = line.strip_prefix("*** Add File: ") {
            index = index.saturating_add(1);
            let mut add_lines = Vec::new();
            while index < lines.len() {
                let body_line = lines[index];
                if is_patch_header_or_end(body_line) {
                    break;
                }
                let Some(content) = body_line.strip_prefix('+') else {
                    return Err(parse_error(
                        index + 1,
                        1,
                        "add-file body must contain '+' prefixed lines",
                    ));
                };
                add_lines.push(content.to_owned());
                index = index.saturating_add(1);
            }
            operations.push(PatchOperation::Add { path: path.to_owned(), lines: add_lines });
            continue;
        }

        if let Some(path) = line.strip_prefix("*** Delete File: ") {
            operations.push(PatchOperation::Delete { path: path.to_owned() });
            index = index.saturating_add(1);
            continue;
        }

        if let Some(path) = line.strip_prefix("*** Update File: ") {
            index = index.saturating_add(1);
            let mut move_to = None;
            if index < lines.len() {
                if let Some(target) = lines[index].strip_prefix("*** Move to: ") {
                    move_to = Some(target.to_owned());
                    index = index.saturating_add(1);
                }
            }

            let mut hunks = Vec::new();
            while index < lines.len() {
                let hunk_line = lines[index];
                if is_patch_header_or_end(hunk_line) {
                    break;
                }
                if !hunk_line.starts_with("@@") {
                    return Err(parse_error(index + 1, 1, "update-file hunk must start with '@@'"));
                }
                index = index.saturating_add(1);
                let mut lines_in_hunk = Vec::new();
                while index < lines.len() {
                    let candidate = lines[index];
                    if candidate.starts_with("@@") || is_patch_header_or_end(candidate) {
                        break;
                    }
                    let mut chars = candidate.chars();
                    let Some(prefix) = chars.next() else {
                        return Err(parse_error(
                            index + 1,
                            1,
                            "hunk line cannot be empty; expected one of ' ', '+', '-'",
                        ));
                    };
                    let text = chars.collect::<String>();
                    let kind = match prefix {
                        ' ' => HunkLineKind::Context,
                        '+' => HunkLineKind::Add,
                        '-' => HunkLineKind::Remove,
                        _ => {
                            return Err(parse_error(
                                index + 1,
                                1,
                                "hunk line must start with ' ', '+', or '-'",
                            ));
                        }
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
            "expected patch operation header: *** Add File, *** Update File, or *** Delete File",
        ));
    }

    if !ended {
        return Err(parse_error(lines.len(), 1, "expected '*** End Patch'"));
    }

    while index < lines.len() {
        if !lines[index].is_empty() {
            return Err(parse_error(index + 1, 1, "unexpected content after '*** End Patch'"));
        }
        index = index.saturating_add(1);
    }

    if operations.is_empty() {
        return Err(parse_error(1, 1, "patch must contain at least one operation"));
    }

    Ok(operations)
}

fn is_patch_header_or_end(line: &str) -> bool {
    line == "*** End Patch" || line.starts_with("*** ")
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
    let mut touched_paths = HashSet::new();

    for operation in operations {
        match operation {
            PatchOperation::Add { path, lines } => {
                let relative = parse_relative_patch_path(path)?;
                let (target, target_root_index) =
                    resolve_new_path(canonical_roots, &relative, None, path)?;
                if target.exists() {
                    return Err(WorkspacePatchError::FileAlreadyExists { path: path.to_owned() });
                }
                let after_bytes = render_add_file_bytes(lines.as_slice());
                ensure_file_size(path, after_bytes.len(), limits.max_file_bytes)?;

                touched_paths.insert(target.clone());
                actions.push(PlannedAction::Write {
                    path: target,
                    root: canonical_roots[target_root_index].clone(),
                    bytes: after_bytes.clone(),
                });
                file_attestations.push(WorkspacePatchFileAttestation {
                    path: normalize_relative_path_display(&relative),
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
                    normalize_relative_path_display(&move_relative)
                } else {
                    normalize_relative_path_display(&relative)
                };

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

    Ok(PatchPlan { actions, file_attestations })
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

        let Some(start) = find_subsequence(lines.as_slice(), old_lines.as_slice(), search_cursor)
        else {
            return Err(WorkspacePatchError::HunkApplyFailed {
                path: path_label.to_owned(),
                message: format!("hunk {index} context not found"),
            });
        };

        let end = start.saturating_add(old_lines.len());
        let inserted_len = new_lines.len();
        lines.splice(start..end, new_lines);
        search_cursor = start.saturating_add(inserted_len);
    }

    let mut output = lines.join("\n");
    if had_trailing_newline {
        output.push('\n');
    }
    Ok(output.into_bytes())
}

fn find_subsequence(haystack: &[String], needle: &[String], from: usize) -> Option<usize> {
    if needle.is_empty() {
        return Some(from.min(haystack.len()));
    }
    if haystack.len() < needle.len() {
        return None;
    }
    let start = from.min(haystack.len().saturating_sub(needle.len()));
    (start..=haystack.len().saturating_sub(needle.len()))
        .find(|&offset| haystack[offset..offset + needle.len()] == *needle)
}

fn execute_patch_plan(
    actions: &[PlannedAction],
    limits: &WorkspacePatchLimits,
) -> Result<(), PatchExecutionError> {
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
        apply_workspace_patch, compute_patch_sha256, redact_patch_preview, sha256_hex,
        WorkspacePatchError, WorkspacePatchLimits, WorkspacePatchOutcome,
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
            "*** Begin Patch\n*** Add File: bad.txt\nmissing-plus\n*** End Patch\n",
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
