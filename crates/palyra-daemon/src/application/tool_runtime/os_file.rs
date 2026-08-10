//! `palyra.fs.os_file` tool backend: scoped OS-level file operations.
//!
//! Unlike the workspace tools, this backend may reach outside the agent
//! workspace, but only within the allowlist of workspace roots, user-owned
//! roots (`USERPROFILE`/`HOME` or configured `PALYRA_OS_FILE_ROOTS`), temp
//! directories, and run-launch path-env roots. Every requested path must be
//! absolute, free of `.`/`..` components, canonicalized (or resolved through
//! its nearest existing ancestor for new targets), and outside protected OS
//! paths before I/O.
//! Treat any change to that pipeline as a security change.
//!
//! Reads stay model-visible even when the safety scanner finds secrets: the
//! text is replaced with redacted placeholders and flagged via
//! `text_authoritative`/`redaction_notice` instead of failing the call. All
//! operations are bounded by the `MAX_OS_FILE_*` constants below.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::UNIX_EPOCH,
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palyra_safety::{
    redact_text_for_export, SafetyContentKind, SafetyFindingCategory, SafetySourceKind, TrustLabel,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{
    agents::AgentResolveRequest,
    application::tool_runtime::workspace_scope::{
        run_launch_context_path_env, workspace_roots_with_run_launch_context_for_agent_source,
    },
    gateway::{GatewayRuntimeState, ToolRuntimeExecutionContext, OS_FILE_TOOL_NAME},
    tool_protocol::{build_tool_execution_outcome, ToolExecutionOutcome},
};

const MAX_OS_FILE_READ_BYTES: u64 = 128 * 1024;
const MAX_OS_FILE_TOOL_INPUT_BYTES: usize = 384 * 1024;
const MAX_OS_FILE_WRITE_BYTES: usize = 256 * 1024;
const MAX_OS_FILE_LIST_ENTRIES: usize = 200;
const MAX_OS_FILE_SEARCH_QUERY_BYTES: usize = 512;
const MAX_OS_FILE_SEARCH_MATCHES: usize = 100;
const MAX_OS_FILE_SEARCH_FILES: usize = 10_000;
const MAX_OS_FILE_SEARCH_DEPTH: usize = 8;
const MAX_OS_FILE_SEARCH_FILE_BYTES: u64 = 128 * 1024;
const MAX_OS_FILE_SEARCH_EXCERPT_CHARS: usize = 240;
const PALYRA_OS_FILE_ROOTS_ENV: &str = "PALYRA_OS_FILE_ROOTS";

/// Model-supplied tool input; one flat schema shared by all operations.
#[derive(Debug, Deserialize)]
struct OsFileInput {
    operation: OsFileOperation,
    path: String,
    #[serde(default)]
    target_path: Option<String>,
    #[serde(default)]
    content_text: Option<String>,
    #[serde(default)]
    bytes_base64: Option<String>,
    #[serde(default)]
    create_parent_dirs: Option<bool>,
    #[serde(default)]
    overwrite: Option<bool>,
    #[serde(default)]
    full_replace: Option<bool>,
    #[serde(default)]
    dry_run: Option<bool>,
    #[serde(default)]
    offset_bytes: Option<u64>,
    #[serde(default)]
    max_bytes: Option<u64>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    case_sensitive: Option<bool>,
    #[serde(default)]
    max_entries: Option<usize>,
    #[serde(default)]
    max_matches: Option<usize>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum OsFileOperation {
    Stat,
    Read,
    Write,
    Copy,
    Move,
    DeleteFile,
    DeleteEmptyDir,
    Mkdir,
    ListDir,
    Search,
}

/// Per-call access policy: the canonicalized roots a path may resolve into,
/// plus the run-launch env-var-to-path bindings usable as path prefixes.
#[derive(Debug, Clone)]
struct OsFilePolicy {
    workspace_roots: Vec<PathBuf>,
    user_os_roots: Vec<PathBuf>,
    path_env: BTreeMap<String, PathBuf>,
}

/// A requested path paired with its canonical resolution.
///
/// For not-yet-existing targets, `resolved_path` is the canonicalized nearest
/// existing ancestor re-joined with the missing suffix, so containment checks
/// always run against canonical forms.
#[derive(Debug, Clone)]
struct ResolvedOsPath {
    requested_path: PathBuf,
    resolved_path: PathBuf,
    existed: bool,
}

/// Executes a `palyra.fs.os_file` tool call.
///
/// Validates input size and schema, resolves the caller's path policy from
/// the agent bound to `context` (never from model input), runs the requested
/// operation under that policy, and reports every failure as an unsuccessful
/// [`ToolExecutionOutcome`] rather than an error so the tool loop stays
/// alive.
pub(crate) async fn execute_os_file_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    if input_json.len() > MAX_OS_FILE_TOOL_INPUT_BYTES {
        return os_file_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("{OS_FILE_TOOL_NAME} input exceeds {MAX_OS_FILE_TOOL_INPUT_BYTES} bytes"),
        );
    }
    let input = match serde_json::from_slice::<OsFileInput>(input_json) {
        Ok(input) => input,
        Err(error) => {
            return os_file_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("{OS_FILE_TOOL_NAME} input must match OS file schema: {error}"),
            );
        }
    };
    let policy = match resolve_os_file_policy(runtime_state, context).await {
        Ok(policy) => policy,
        Err(error) => {
            return os_file_outcome(proposal_id, input_json, false, b"{}".to_vec(), error);
        }
    };
    let output = match execute_os_file_operation(&policy, &input) {
        Ok(output) => output,
        Err(error) => {
            return os_file_outcome(proposal_id, input_json, false, b"{}".to_vec(), error);
        }
    };
    match serde_json::to_vec(&output) {
        Ok(output_json) => {
            os_file_outcome(proposal_id, input_json, true, output_json, String::new())
        }
        Err(error) => os_file_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("{OS_FILE_TOOL_NAME} failed to serialize output: {error}"),
        ),
    }
}

async fn resolve_os_file_policy(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
) -> Result<OsFilePolicy, String> {
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
            format!(
                "{OS_FILE_TOOL_NAME} failed to resolve agent OS file policy: {}",
                error.message()
            )
        })?;
    let agent_workspace_roots =
        agent_outcome.agent.workspace_roots.iter().map(PathBuf::from).collect::<Vec<_>>();
    let workspace_roots = workspace_roots_with_run_launch_context_for_agent_source(
        runtime_state,
        context.run_id,
        agent_workspace_roots.as_slice(),
        agent_outcome.source,
    )
    .await
    .iter()
    .filter_map(|root| canonicalize_existing_dir(root.as_path()).ok())
    .collect::<Vec<_>>();
    let path_env = run_launch_context_path_env(runtime_state, context.run_id).await;
    let mut user_os_roots = user_owned_os_roots();
    for root in path_env.values() {
        push_canonical_root(&mut user_os_roots, root.clone());
    }
    Ok(OsFilePolicy { workspace_roots, user_os_roots, path_env })
}

fn execute_os_file_operation(policy: &OsFilePolicy, input: &OsFileInput) -> Result<Value, String> {
    match input.operation {
        OsFileOperation::Stat => stat_path(policy, input),
        OsFileOperation::Read => read_path(policy, input),
        OsFileOperation::Write => write_path(policy, input),
        OsFileOperation::Copy => copy_path(policy, input),
        OsFileOperation::Move => move_path(policy, input),
        OsFileOperation::DeleteFile => delete_file_path(policy, input),
        OsFileOperation::DeleteEmptyDir => delete_empty_dir_path(policy, input),
        OsFileOperation::Mkdir => mkdir_path(policy, input),
        OsFileOperation::ListDir => list_dir_path(policy, input),
        OsFileOperation::Search => search_path(policy, input),
    }
}

fn stat_path(policy: &OsFilePolicy, input: &OsFileInput) -> Result<Value, String> {
    let path = resolve_existing_os_path(policy, input.path.as_str())?;
    ensure_os_path_allowed(policy, &path)?;
    let metadata = fs::metadata(path.resolved_path.as_path()).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} failed to inspect {}: {error}", input.path.trim())
    })?;
    Ok(json!({
        "operation": "stat",
        "path": display_path(path.requested_path.as_path()),
        "resolved_path": display_path(path.resolved_path.as_path()),
        "kind": metadata_kind(&metadata),
        "size_bytes": metadata.len(),
        "readonly": metadata.permissions().readonly(),
        "modified_unix_ms": metadata_modified_unix_ms(&metadata),
        "dry_run": false,
    }))
}

fn read_path(policy: &OsFilePolicy, input: &OsFileInput) -> Result<Value, String> {
    let path = resolve_existing_os_path(policy, input.path.as_str())?;
    ensure_os_path_allowed(policy, &path)?;
    let mut file = File::open(path.resolved_path.as_path()).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} failed to open {}: {error}", input.path.trim())
    })?;
    let size_bytes = file
        .metadata()
        .map_err(|error| {
            format!("{OS_FILE_TOOL_NAME} failed to inspect {}: {error}", input.path.trim())
        })?
        .len();
    let offset_bytes = input.offset_bytes.unwrap_or(0);
    file.seek(SeekFrom::Start(offset_bytes)).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} failed to seek {}: {error}", input.path.trim())
    })?;
    let requested_max = input.max_bytes.unwrap_or(MAX_OS_FILE_READ_BYTES);
    let read_limit = requested_max.min(MAX_OS_FILE_READ_BYTES);
    // Pre-allocate at most 8 KiB regardless of the (caller-influenced) read
    // limit; `read_to_end` grows the buffer as real bytes arrive.
    let mut buffer = Vec::with_capacity(usize::try_from(read_limit.min(8192)).unwrap_or(8192));
    file.take(read_limit).read_to_end(&mut buffer).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} failed to read {}: {error}", input.path.trim())
    })?;
    let returned_bytes = u64::try_from(buffer.len()).expect("OS file read size must fit u64");
    let eof = offset_bytes.saturating_add(returned_bytes) >= size_bytes;
    let chunk_sha256 = hex::encode(Sha256::digest(buffer.as_slice()));
    // Redacted reads stay model-visible by design: the model gets placeholder
    // text plus `text_authoritative=false` and a notice, instead of a hard
    // failure that would dead-end the task. `chunk_sha256` above is computed
    // over the raw bytes, so it will not match the redacted text.
    let (text, bytes_base64, redacted, redaction_reasons) = visible_file_content(buffer);
    let text_authoritative = text.as_ref().map(|_| !redacted);
    let redaction_notice = redacted.then(|| {
        "text contains redacted secret placeholders; use it for structure only and do not write the redacted text back verbatim".to_owned()
    });
    let redaction_reasons = redacted.then_some(redaction_reasons);
    Ok(json!({
        "operation": "read",
        "path": display_path(path.requested_path.as_path()),
        "resolved_path": display_path(path.resolved_path.as_path()),
        "offset_bytes": offset_bytes,
        "returned_bytes": returned_bytes,
        "size_bytes": size_bytes,
        "eof": eof,
        "chunk_sha256": chunk_sha256,
        "text": text,
        "text_authoritative": text_authoritative,
        "redaction_notice": redaction_notice,
        "redaction_reasons": redaction_reasons,
        "bytes_base64": bytes_base64,
        "redacted": redacted,
        "dry_run": false,
    }))
}

fn write_path(policy: &OsFilePolicy, input: &OsFileInput) -> Result<Value, String> {
    let bytes = input_write_bytes(input)?;
    let path = resolve_target_os_path(policy, input.path.as_str())?;
    ensure_os_path_allowed(policy, &path)?;
    let dry_run = input.dry_run.unwrap_or(false);
    let create_parent_dirs = input.create_parent_dirs.unwrap_or(true);
    let overwrite = input.overwrite.unwrap_or(true);
    let parent = path.resolved_path.parent().ok_or_else(|| {
        format!("{OS_FILE_TOOL_NAME} write target has no parent: {}", input.path.trim())
    })?;
    let existed_before = path.resolved_path.exists();
    let parent_existed_before = parent.exists();
    if existed_before && !overwrite {
        return Err(format!(
            "{OS_FILE_TOOL_NAME} refusing to overwrite existing file {}",
            input.path.trim()
        ));
    }
    let existing_size_bytes = if existed_before {
        Some(
            fs::metadata(path.resolved_path.as_path())
                .map_err(|error| {
                    format!(
                        "{OS_FILE_TOOL_NAME} failed to inspect write target {}: {error}",
                        input.path.trim()
                    )
                })?
                .len(),
        )
    } else {
        None
    };
    guard_existing_file_write_intent(input, existing_size_bytes, bytes.len())?;
    if !parent_existed_before && !create_parent_dirs {
        return Err(format!(
            "{OS_FILE_TOOL_NAME} parent directory does not exist for {}",
            input.path.trim()
        ));
    }
    let content_sha256 = hex::encode(Sha256::digest(bytes.as_slice()));
    if !dry_run {
        if create_parent_dirs {
            fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "{OS_FILE_TOOL_NAME} failed to create parent directories for {}: {error}",
                    input.path.trim()
                )
            })?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path.resolved_path.as_path())
            .map_err(|error| {
                format!(
                    "{OS_FILE_TOOL_NAME} failed to open write target {}: {error}",
                    input.path.trim()
                )
            })?;
        file.write_all(bytes.as_slice()).map_err(|error| {
            format!("{OS_FILE_TOOL_NAME} failed to write {}: {error}", input.path.trim())
        })?;
        file.sync_all().map_err(|error| {
            format!("{OS_FILE_TOOL_NAME} failed to sync {}: {error}", input.path.trim())
        })?;
    }
    Ok(json!({
        "operation": "write",
        "path": display_path(path.requested_path.as_path()),
        "resolved_path": display_path(path.resolved_path.as_path()),
        "bytes_written": bytes.len(),
        "content_sha256": content_sha256,
        "existed_before": existed_before,
        "existing_size_bytes": existing_size_bytes,
        "full_replace": input.full_replace.unwrap_or(false),
        "created_parent_dirs": create_parent_dirs && !parent_existed_before,
        "dry_run": dry_run,
    }))
}

/// Rejects writes over an existing file unless `full_replace=true`.
///
/// `operation=write` always truncates, but models routinely send a fragment
/// expecting append/partial-edit semantics; failing closed with an
/// explanation prevents silent data loss on user files.
fn guard_existing_file_write_intent(
    input: &OsFileInput,
    existing_size_bytes: Option<u64>,
    new_size_bytes: usize,
) -> Result<(), String> {
    let Some(existing_size_bytes) = existing_size_bytes else {
        return Ok(());
    };
    if input.full_replace.unwrap_or(false) {
        return Ok(());
    }
    Err(format!(
        "{OS_FILE_TOOL_NAME} refusing write to existing file {} without full_replace=true: existing_size_bytes={} new_size_bytes={}. operation=write replaces the entire file; append/partial-edit semantics are unsupported. Use palyra.fs.apply_patch for scoped workspace edits, or read the original content and retry with full_replace=true only when replacing the whole file is intentional.",
        input.path.trim(),
        existing_size_bytes,
        new_size_bytes
    ))
}

fn copy_path(policy: &OsFilePolicy, input: &OsFileInput) -> Result<Value, String> {
    let source = resolve_existing_os_path(policy, input.path.as_str())?;
    let target = resolve_copy_move_target_path(policy, required_target_path(input)?)?;
    ensure_os_path_allowed(policy, &source)?;
    ensure_os_path_allowed(policy, &target)?;
    let dry_run = input.dry_run.unwrap_or(false);
    let create_parent_dirs = input.create_parent_dirs.unwrap_or(true);
    let overwrite = input.overwrite.unwrap_or(true);
    prepare_target_parent(&target, create_parent_dirs, overwrite, dry_run, "copy")?;
    let source_size = fs::metadata(source.resolved_path.as_path())
        .map_err(|error| format!("{OS_FILE_TOOL_NAME} failed to inspect source: {error}"))?
        .len();
    if !dry_run {
        fs::copy(source.resolved_path.as_path(), target.resolved_path.as_path()).map_err(
            |error| format!("{OS_FILE_TOOL_NAME} failed to copy {}: {error}", input.path.trim()),
        )?;
    }
    Ok(json!({
        "operation": "copy",
        "path": display_path(source.requested_path.as_path()),
        "resolved_path": display_path(source.resolved_path.as_path()),
        "target_path": display_path(target.requested_path.as_path()),
        "resolved_target_path": display_path(target.resolved_path.as_path()),
        "target_workspace_relative_path": workspace_relative_path(policy, target.resolved_path.as_path()),
        "source_size_bytes": source_size,
        "target_existed_before": target.existed,
        "dry_run": dry_run,
    }))
}

fn move_path(policy: &OsFilePolicy, input: &OsFileInput) -> Result<Value, String> {
    let source = resolve_existing_os_path(policy, input.path.as_str())?;
    let target = resolve_copy_move_target_path(policy, required_target_path(input)?)?;
    ensure_os_path_allowed(policy, &source)?;
    ensure_os_path_allowed(policy, &target)?;
    let dry_run = input.dry_run.unwrap_or(false);
    let create_parent_dirs = input.create_parent_dirs.unwrap_or(true);
    let overwrite = input.overwrite.unwrap_or(true);
    prepare_target_parent(&target, create_parent_dirs, overwrite, dry_run, "move")?;
    let source_size = fs::metadata(source.resolved_path.as_path())
        .map_err(|error| format!("{OS_FILE_TOOL_NAME} failed to inspect source: {error}"))?
        .len();
    if !dry_run {
        // Remove an existing target first: unlike Unix, `fs::rename` on
        // Windows fails when the destination exists.
        if target.existed {
            fs::remove_file(target.resolved_path.as_path()).map_err(|error| {
                format!("{OS_FILE_TOOL_NAME} failed to replace target before move: {error}")
            })?;
        }
        fs::rename(source.resolved_path.as_path(), target.resolved_path.as_path()).map_err(
            |error| format!("{OS_FILE_TOOL_NAME} failed to move {}: {error}", input.path.trim()),
        )?;
    }
    Ok(json!({
        "operation": "move",
        "path": display_path(source.requested_path.as_path()),
        "resolved_path": display_path(source.resolved_path.as_path()),
        "target_path": display_path(target.requested_path.as_path()),
        "resolved_target_path": display_path(target.resolved_path.as_path()),
        "target_workspace_relative_path": workspace_relative_path(policy, target.resolved_path.as_path()),
        "source_size_bytes": source_size,
        "target_existed_before": target.existed,
        "dry_run": dry_run,
    }))
}

fn delete_file_path(policy: &OsFilePolicy, input: &OsFileInput) -> Result<Value, String> {
    let path = resolve_existing_os_path(policy, input.path.as_str())?;
    ensure_os_path_allowed(policy, &path)?;
    let metadata = fs::metadata(path.resolved_path.as_path()).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} failed to inspect {}: {error}", input.path.trim())
    })?;
    if !metadata.is_file() {
        return Err(format!("{OS_FILE_TOOL_NAME} delete_file only removes regular files"));
    }
    let dry_run = input.dry_run.unwrap_or(false);
    if !dry_run {
        fs::remove_file(path.resolved_path.as_path()).map_err(|error| {
            format!("{OS_FILE_TOOL_NAME} failed to delete {}: {error}", input.path.trim())
        })?;
    }
    Ok(json!({
        "operation": "delete_file",
        "path": display_path(path.requested_path.as_path()),
        "resolved_path": display_path(path.resolved_path.as_path()),
        "size_bytes": metadata.len(),
        "dry_run": dry_run,
    }))
}

fn delete_empty_dir_path(policy: &OsFilePolicy, input: &OsFileInput) -> Result<Value, String> {
    let path = resolve_existing_os_path(policy, input.path.as_str())?;
    ensure_os_path_allowed(policy, &path)?;
    let metadata = fs::metadata(path.resolved_path.as_path()).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} failed to inspect {}: {error}", input.path.trim())
    })?;
    if !metadata.is_dir() {
        return Err(format!("{OS_FILE_TOOL_NAME} delete_empty_dir only removes empty directories"));
    }
    let dry_run = input.dry_run.unwrap_or(false);
    if !dry_run {
        fs::remove_dir(path.resolved_path.as_path()).map_err(|error| {
            format!(
                "{OS_FILE_TOOL_NAME} failed to remove empty directory {}: {error}",
                display_path(path.resolved_path.as_path())
            )
        })?;
    }
    Ok(json!({
        "operation": "delete_empty_dir",
        "path": display_path(path.requested_path.as_path()),
        "resolved_path": display_path(path.resolved_path.as_path()),
        "dry_run": dry_run,
    }))
}

fn mkdir_path(policy: &OsFilePolicy, input: &OsFileInput) -> Result<Value, String> {
    let path = resolve_target_os_path(policy, input.path.as_str())?;
    ensure_os_path_allowed(policy, &path)?;
    let dry_run = input.dry_run.unwrap_or(false);
    if !dry_run {
        fs::create_dir_all(path.resolved_path.as_path()).map_err(|error| {
            format!("{OS_FILE_TOOL_NAME} failed to create directory {}: {error}", input.path.trim())
        })?;
    }
    Ok(json!({
        "operation": "mkdir",
        "path": display_path(path.requested_path.as_path()),
        "resolved_path": display_path(path.resolved_path.as_path()),
        "existed_before": path.existed,
        "dry_run": dry_run,
    }))
}

fn list_dir_path(policy: &OsFilePolicy, input: &OsFileInput) -> Result<Value, String> {
    let path = resolve_existing_os_path(policy, input.path.as_str())?;
    ensure_os_path_allowed(policy, &path)?;
    let metadata = fs::metadata(path.resolved_path.as_path()).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} failed to inspect {}: {error}", input.path.trim())
    })?;
    if !metadata.is_dir() {
        return Err(format!("{OS_FILE_TOOL_NAME} list_dir requires a directory path"));
    }
    let max_entries =
        input.max_entries.unwrap_or(MAX_OS_FILE_LIST_ENTRIES).clamp(1, MAX_OS_FILE_LIST_ENTRIES);
    let mut entries = Vec::new();
    let mut skipped_entries = 0usize;
    let read_dir = fs::read_dir(path.resolved_path.as_path()).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} failed to list directory {}: {error}", input.path.trim())
    })?;
    for entry in read_dir {
        if entries.len() >= max_entries {
            skipped_entries = skipped_entries.saturating_add(1);
            continue;
        }
        let Ok(entry) = entry else {
            skipped_entries = skipped_entries.saturating_add(1);
            continue;
        };
        let entry_path = entry.path();
        let Ok(canonical_entry) = fs::canonicalize(entry_path.as_path()) else {
            skipped_entries = skipped_entries.saturating_add(1);
            continue;
        };
        // Hide entries that resolve outside the listed directory (symlinks
        // pointing elsewhere) so listings never leak paths the policy checks
        // would reject on a follow-up read.
        if !path_starts_with(canonical_entry.as_path(), path.resolved_path.as_path()) {
            skipped_entries = skipped_entries.saturating_add(1);
            continue;
        }
        let Ok(metadata) = fs::metadata(canonical_entry.as_path()) else {
            skipped_entries = skipped_entries.saturating_add(1);
            continue;
        };
        entries.push(json!({
            "name": entry.file_name().to_string_lossy(),
            "path": display_path(entry_path.as_path()),
            "resolved_path": display_path(canonical_entry.as_path()),
            "kind": metadata_kind(&metadata),
            "size_bytes": metadata.len(),
            "readonly": metadata.permissions().readonly(),
            "modified_unix_ms": metadata_modified_unix_ms(&metadata),
        }));
    }
    entries.sort_by(|left, right| {
        left.get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .cmp(right.get("name").and_then(Value::as_str).unwrap_or_default())
    });
    let entry_count = entries.len();
    Ok(json!({
        "operation": "list_dir",
        "path": display_path(path.requested_path.as_path()),
        "resolved_path": display_path(path.resolved_path.as_path()),
        "entries": entries,
        "entry_count": entry_count,
        "skipped_entries": skipped_entries,
        "truncated": skipped_entries > 0,
        "dry_run": false,
    }))
}

#[derive(Debug)]
struct OsFileSearchState {
    query: String,
    normalized_query: String,
    case_sensitive: bool,
    max_matches: usize,
    matches: Vec<Value>,
    files_scanned: usize,
    dirs_scanned: usize,
    skipped_files: usize,
    skipped_dirs: usize,
    truncated: bool,
}

impl OsFileSearchState {
    fn new(query: String, case_sensitive: bool, max_matches: usize) -> Self {
        let normalized_query = normalize_search_text(query.as_str(), case_sensitive);
        Self {
            query,
            normalized_query,
            case_sensitive,
            max_matches,
            matches: Vec::new(),
            files_scanned: 0,
            dirs_scanned: 0,
            skipped_files: 0,
            skipped_dirs: 0,
            truncated: false,
        }
    }

    fn has_capacity(&self) -> bool {
        self.matches.len() < self.max_matches
    }

    fn push_match(&mut self, value: Value) {
        if self.has_capacity() {
            self.matches.push(value);
        } else {
            self.truncated = true;
        }
    }
}

fn search_path(policy: &OsFilePolicy, input: &OsFileInput) -> Result<Value, String> {
    let path = resolve_existing_os_path(policy, input.path.as_str())?;
    ensure_os_path_allowed(policy, &path)?;
    let query = input
        .query
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{OS_FILE_TOOL_NAME} search requires non-empty query"))?;
    if query.len() > MAX_OS_FILE_SEARCH_QUERY_BYTES {
        return Err(format!(
            "{OS_FILE_TOOL_NAME} search query exceeds {MAX_OS_FILE_SEARCH_QUERY_BYTES} bytes"
        ));
    }
    let max_matches = input
        .max_matches
        .unwrap_or(MAX_OS_FILE_SEARCH_MATCHES)
        .clamp(1, MAX_OS_FILE_SEARCH_MATCHES);
    let mut state = OsFileSearchState::new(
        query.to_owned(),
        input.case_sensitive.unwrap_or(false),
        max_matches,
    );
    search_path_recursive(
        path.resolved_path.as_path(),
        path.resolved_path.as_path(),
        &mut state,
        0,
    )?;
    let OsFileSearchState {
        matches,
        files_scanned,
        dirs_scanned,
        skipped_files,
        skipped_dirs,
        truncated,
        ..
    } = state;
    let match_count = matches.len();
    Ok(json!({
        "operation": "search",
        "path": display_path(path.requested_path.as_path()),
        "resolved_path": display_path(path.resolved_path.as_path()),
        "query": query,
        "case_sensitive": input.case_sensitive.unwrap_or(false),
        "matches": matches,
        "match_count": match_count,
        "files_scanned": files_scanned,
        "dirs_scanned": dirs_scanned,
        "skipped_files": skipped_files,
        "skipped_dirs": skipped_dirs,
        "truncated": truncated,
        "dry_run": false,
    }))
}

fn search_path_recursive(
    root: &Path,
    path: &Path,
    state: &mut OsFileSearchState,
    depth: usize,
) -> Result<(), String> {
    if !state.has_capacity() {
        state.truncated = true;
        return Ok(());
    }
    if depth > MAX_OS_FILE_SEARCH_DEPTH {
        state.truncated = true;
        state.skipped_dirs = state.skipped_dirs.saturating_add(1);
        return Ok(());
    }
    let canonical = fs::canonicalize(path).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} failed to resolve search path {}: {error}", display_path(path))
    })?;
    if !path_starts_with(canonical.as_path(), root) {
        state.skipped_files = state.skipped_files.saturating_add(1);
        return Ok(());
    }
    let metadata = fs::metadata(canonical.as_path()).map_err(|error| {
        format!(
            "{OS_FILE_TOOL_NAME} failed to inspect search path {}: {error}",
            display_path(canonical.as_path())
        )
    })?;
    if metadata.is_dir() {
        search_directory(root, canonical.as_path(), state, depth)?;
    } else if metadata.is_file() {
        search_file(canonical.as_path(), metadata.len(), state)?;
    } else {
        state.skipped_files = state.skipped_files.saturating_add(1);
    }
    Ok(())
}

fn search_directory(
    root: &Path,
    path: &Path,
    state: &mut OsFileSearchState,
    depth: usize,
) -> Result<(), String> {
    state.dirs_scanned = state.dirs_scanned.saturating_add(1);
    let entries = fs::read_dir(path).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} failed to search directory {}: {error}", display_path(path))
    })?;
    for entry in entries {
        if !state.has_capacity() {
            state.truncated = true;
            break;
        }
        let Ok(entry) = entry else {
            state.skipped_files = state.skipped_files.saturating_add(1);
            continue;
        };
        search_path_recursive(root, entry.path().as_path(), state, depth + 1)?;
        if state.files_scanned >= MAX_OS_FILE_SEARCH_FILES {
            state.truncated = true;
            break;
        }
    }
    Ok(())
}

fn search_file(path: &Path, size_bytes: u64, state: &mut OsFileSearchState) -> Result<(), String> {
    if state.files_scanned >= MAX_OS_FILE_SEARCH_FILES {
        state.truncated = true;
        return Ok(());
    }
    state.files_scanned = state.files_scanned.saturating_add(1);
    let display = display_path(path);
    let normalized_path = normalize_search_text(display.as_str(), state.case_sensitive);
    if normalized_path.contains(state.normalized_query.as_str()) {
        state.push_match(json!({
            "path": display,
            "kind": "path",
            "size_bytes": size_bytes,
        }));
    }
    if size_bytes > MAX_OS_FILE_SEARCH_FILE_BYTES || !state.has_capacity() {
        if size_bytes > MAX_OS_FILE_SEARCH_FILE_BYTES {
            state.skipped_files = state.skipped_files.saturating_add(1);
        }
        return Ok(());
    }
    let bytes = fs::read(path).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} failed to read search file {display}: {error}")
    })?;
    let Ok(text) = String::from_utf8(bytes) else {
        state.skipped_files = state.skipped_files.saturating_add(1);
        return Ok(());
    };
    for (line_index, line) in text.lines().enumerate() {
        if !state.has_capacity() {
            state.truncated = true;
            break;
        }
        let normalized_line = normalize_search_text(line, state.case_sensitive);
        let Some(match_index) = normalized_line.find(state.normalized_query.as_str()) else {
            continue;
        };
        // Excerpts pass through the same secret-leak scan as full reads so a
        // search cannot be used to exfiltrate values that a read would
        // redact.
        let excerpt = search_excerpt(line, match_index, state.query.len());
        let redaction = redact_text_for_export(
            excerpt.as_str(),
            SafetySourceKind::Workspace,
            SafetyContentKind::WorkspaceDocument,
            TrustLabel::TrustedLocal,
        );
        let redacted =
            redaction.redacted || redaction.scan.has_category(SafetyFindingCategory::SecretLeak);
        state.push_match(json!({
            "path": display,
            "kind": "content",
            "line_number": line_index + 1,
            "excerpt": if redacted { redaction.redacted_text } else { excerpt },
            "redacted": redacted,
            "size_bytes": size_bytes,
        }));
    }
    Ok(())
}

fn normalize_search_text(value: &str, case_sensitive: bool) -> String {
    if case_sensitive {
        value.to_owned()
    } else {
        value.to_ascii_lowercase()
    }
}

fn search_excerpt(line: &str, match_start: usize, query_len: usize) -> String {
    let match_start = floor_char_boundary(line, match_start.min(line.len()));
    let start = line[..match_start]
        .char_indices()
        .rev()
        .nth(MAX_OS_FILE_SEARCH_EXCERPT_CHARS / 2)
        .map(|(index, _)| index)
        .unwrap_or(0);
    let raw_end = floor_char_boundary(line, match_start.saturating_add(query_len).min(line.len()));
    let end = line[raw_end..]
        .char_indices()
        .nth(MAX_OS_FILE_SEARCH_EXCERPT_CHARS / 2)
        .map(|(index, _)| raw_end.saturating_add(index))
        .unwrap_or_else(|| line.len());
    line[start.min(line.len())..end.min(line.len())].to_owned()
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while index > 0 && !value.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn prepare_target_parent(
    target: &ResolvedOsPath,
    create_parent_dirs: bool,
    overwrite: bool,
    dry_run: bool,
    operation: &str,
) -> Result<(), String> {
    if target.existed && !overwrite {
        return Err(format!(
            "{OS_FILE_TOOL_NAME} refusing to overwrite existing {operation} target"
        ));
    }
    let parent = target
        .resolved_path
        .parent()
        .ok_or_else(|| format!("{OS_FILE_TOOL_NAME} {operation} target has no parent directory"))?;
    if !parent.exists() && !create_parent_dirs {
        return Err(format!("{OS_FILE_TOOL_NAME} {operation} target parent does not exist"));
    }
    if !dry_run && create_parent_dirs {
        fs::create_dir_all(parent).map_err(|error| {
            format!("{OS_FILE_TOOL_NAME} failed to create {operation} target parent: {error}")
        })?;
    }
    Ok(())
}

fn input_write_bytes(input: &OsFileInput) -> Result<Vec<u8>, String> {
    match (input.content_text.as_ref(), input.bytes_base64.as_ref()) {
        (Some(text), Some(bytes_base64)) if bytes_base64.is_empty() => input_text_bytes(text),
        (Some(text), Some(bytes_base64)) if text.is_empty() => input_base64_bytes(bytes_base64),
        (Some(_), Some(_)) => Err(format!(
            "{OS_FILE_TOOL_NAME} write accepts either content_text or bytes_base64, not both"
        )),
        (Some(text), None) => input_text_bytes(text),
        (None, Some(bytes_base64)) => input_base64_bytes(bytes_base64),
        (None, None) => {
            Err(format!("{OS_FILE_TOOL_NAME} write requires content_text or bytes_base64"))
        }
    }
}

fn input_text_bytes(text: &str) -> Result<Vec<u8>, String> {
    if text.len() > MAX_OS_FILE_WRITE_BYTES {
        return Err(format!(
            "{OS_FILE_TOOL_NAME} content_text exceeds {MAX_OS_FILE_WRITE_BYTES} bytes"
        ));
    }
    Ok(text.as_bytes().to_vec())
}

fn input_base64_bytes(bytes_base64: &str) -> Result<Vec<u8>, String> {
    let decoded = BASE64_STANDARD.decode(bytes_base64.as_bytes()).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} bytes_base64 must be valid base64: {error}")
    })?;
    if decoded.len() > MAX_OS_FILE_WRITE_BYTES {
        return Err(format!(
            "{OS_FILE_TOOL_NAME} bytes_base64 decoded payload exceeds {MAX_OS_FILE_WRITE_BYTES} bytes"
        ));
    }
    Ok(decoded)
}

/// Converts read bytes into model-visible `(text, bytes_base64, redacted)`.
///
/// UTF-8 content is scanned for secret leaks; when the scanner redacts or
/// flags a leak, the redacted text replaces the raw text (model-visible, but
/// marked non-authoritative by the caller). Non-UTF-8 content is returned
/// base64-encoded instead.
fn visible_file_content(buffer: Vec<u8>) -> (Option<String>, Option<String>, bool, Vec<String>) {
    match String::from_utf8(buffer) {
        Ok(text) => {
            let redaction = redact_text_for_export(
                text.as_str(),
                SafetySourceKind::Workspace,
                SafetyContentKind::WorkspaceDocument,
                TrustLabel::TrustedLocal,
            );
            let redacted = redaction.redacted
                || redaction.scan.has_category(SafetyFindingCategory::SecretLeak);
            let redaction_reasons = secret_redaction_reason_codes(&redaction);
            let visible_text = if redacted { redaction.redacted_text } else { text };
            (
                Some(visible_text),
                None,
                redacted,
                if redacted { redaction_reasons } else { Vec::new() },
            )
        }
        Err(error) => visible_non_utf8_file_content(error.into_bytes()),
    }
}

fn visible_non_utf8_file_content(
    bytes: Vec<u8>,
) -> (Option<String>, Option<String>, bool, Vec<String>) {
    let lossy_text = String::from_utf8_lossy(bytes.as_slice());
    let redaction = redact_text_for_export(
        lossy_text.as_ref(),
        SafetySourceKind::Workspace,
        SafetyContentKind::WorkspaceDocument,
        TrustLabel::TrustedLocal,
    );
    let redacted =
        redaction.redacted || redaction.scan.has_category(SafetyFindingCategory::SecretLeak);
    if redacted {
        let redaction_reasons = secret_redaction_reason_codes(&redaction);
        return (Some(redaction.redacted_text), None, true, redaction_reasons);
    }
    (None, Some(BASE64_STANDARD.encode(bytes)), false, Vec::new())
}

fn secret_redaction_reason_codes(redaction: &palyra_safety::ExportRedactionOutcome) -> Vec<String> {
    let mut reasons = redaction
        .scan
        .findings
        .iter()
        .filter(|finding| finding.category == SafetyFindingCategory::SecretLeak)
        .map(|finding| finding.code.clone())
        .collect::<Vec<_>>();
    reasons.sort();
    reasons.dedup();
    reasons
}

fn required_target_path(input: &OsFileInput) -> Result<&str, String> {
    input
        .target_path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{OS_FILE_TOOL_NAME} operation requires non-empty target_path"))
}

/// Resolves a copy/move `target_path`, which -- unlike `path` -- may be
/// workspace-relative: env-prefixed and absolute targets resolve as OS
/// paths, while `workspace/...` aliases and bare relative paths resolve
/// against the first workspace root (the "import into workspace" flow).
fn resolve_copy_move_target_path(
    policy: &OsFilePolicy,
    target_path: &str,
) -> Result<ResolvedOsPath, String> {
    let trimmed = target_path.trim();
    if path_env_prefix(trimmed)?.is_some() {
        return resolve_target_os_path(policy, trimmed);
    }
    if is_workspace_relative_target(trimmed) || !Path::new(trimmed).is_absolute() {
        return resolve_workspace_relative_target_path(policy, trimmed);
    }
    resolve_target_os_path(policy, trimmed)
}

fn resolve_workspace_relative_target_path(
    policy: &OsFilePolicy,
    target_path: &str,
) -> Result<ResolvedOsPath, String> {
    let root = policy.workspace_roots.first().ok_or_else(|| {
        format!("{OS_FILE_TOOL_NAME} target_path cannot be workspace-relative without an active workspace root")
    })?;
    let relative = normalize_workspace_relative_target(target_path)?;
    let requested_path = root.join(relative.as_path());
    if requested_path.exists() {
        let resolved_path = fs::canonicalize(requested_path.as_path()).map_err(|error| {
            format!("{OS_FILE_TOOL_NAME} failed to resolve existing workspace target: {error}")
        })?;
        ensure_workspace_relative_target_in_workspace(root, resolved_path.as_path())?;
        return Ok(ResolvedOsPath { requested_path, resolved_path, existed: true });
    }
    let (existing_ancestor, missing_suffix) = nearest_existing_ancestor(requested_path.as_path())?;
    let canonical_ancestor = fs::canonicalize(existing_ancestor.as_path()).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} failed to resolve workspace target ancestor: {error}")
    })?;
    let resolved_path = canonical_ancestor.join(missing_suffix);
    ensure_workspace_relative_target_in_workspace(root, resolved_path.as_path())?;
    Ok(ResolvedOsPath { requested_path, resolved_path, existed: false })
}

fn ensure_workspace_relative_target_in_workspace(
    root: &Path,
    resolved_path: &Path,
) -> Result<(), String> {
    if path_starts_with(resolved_path, root) {
        return Ok(());
    }
    Err(format!(
        "{OS_FILE_TOOL_NAME} workspace-relative target_path must stay inside the active workspace"
    ))
}

fn is_workspace_relative_target(target_path: &str) -> bool {
    let normalized = target_path.trim().replace('\\', "/");
    normalized == "workspace"
        || normalized.starts_with("workspace/")
        || normalized == "/workspace"
        || normalized.starts_with("/workspace/")
}

fn normalize_workspace_relative_target(target_path: &str) -> Result<PathBuf, String> {
    let normalized = target_path.trim().replace('\\', "/");
    let relative = match normalized.as_str() {
        "workspace" | "/workspace" => "",
        _ => normalized
            .strip_prefix("/workspace/")
            .or_else(|| normalized.strip_prefix("workspace/"))
            .unwrap_or(normalized.as_str())
            .trim_matches('/'),
    };
    if relative.is_empty() {
        return Err(format!(
            "{OS_FILE_TOOL_NAME} workspace-relative target_path must include a file path"
        ));
    }
    let path = PathBuf::from(relative);
    for component in path.components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(format!(
                "{OS_FILE_TOOL_NAME} workspace-relative target_path must stay inside the active workspace"
            ));
        }
    }
    Ok(path)
}

fn resolve_existing_os_path(policy: &OsFilePolicy, path: &str) -> Result<ResolvedOsPath, String> {
    let requested_path = parse_absolute_os_path(policy, path)?;
    let resolved_path = fs::canonicalize(requested_path.as_path()).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} path does not resolve to an existing OS file target: {error}")
    })?;
    Ok(ResolvedOsPath { requested_path, resolved_path, existed: true })
}

fn resolve_target_os_path(policy: &OsFilePolicy, path: &str) -> Result<ResolvedOsPath, String> {
    let requested_path = parse_absolute_os_path(policy, path)?;
    if requested_path.exists() {
        let resolved_path = fs::canonicalize(requested_path.as_path()).map_err(|error| {
            format!("{OS_FILE_TOOL_NAME} failed to resolve existing target: {error}")
        })?;
        return Ok(ResolvedOsPath { requested_path, resolved_path, existed: true });
    }
    let (existing_ancestor, missing_suffix) = nearest_existing_ancestor(requested_path.as_path())?;
    let canonical_ancestor = fs::canonicalize(existing_ancestor.as_path()).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} failed to resolve target ancestor: {error}")
    })?;
    let resolved_path = canonical_ancestor.join(missing_suffix);
    Ok(ResolvedOsPath { requested_path, resolved_path, existed: false })
}

/// Parses a model-supplied path into an absolute [`PathBuf`], expanding a
/// leading `%VAR%`/`$VAR`/`${VAR}` prefix only from policy-bound launch context.
///
/// `.`/`..` components and control characters are rejected up front, before
/// any canonicalization, so traversal cannot hide behind a not-yet-existing
/// suffix that `fs::canonicalize` would never see.
fn parse_absolute_os_path(policy: &OsFilePolicy, path: &str) -> Result<PathBuf, String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(format!("{OS_FILE_TOOL_NAME} path must be non-empty"));
    }
    if trimmed.chars().any(char::is_control) {
        return Err(format!("{OS_FILE_TOOL_NAME} path contains unsupported characters"));
    }
    let parsed = expand_env_prefixed_os_path(policy, trimmed)?;
    if !parsed.is_absolute() {
        return Err(format!("{OS_FILE_TOOL_NAME} path must be an absolute OS path"));
    }
    for component in parsed.components() {
        if matches!(component, Component::CurDir | Component::ParentDir) {
            return Err(format!(
                "{OS_FILE_TOOL_NAME} path must not contain '.' or '..' components"
            ));
        }
    }
    Ok(parsed)
}

fn expand_env_prefixed_os_path(policy: &OsFilePolicy, path: &str) -> Result<PathBuf, String> {
    let Some((key, suffix)) = path_env_prefix(path)? else {
        return Ok(PathBuf::from(path));
    };
    let value = policy.path_env.get(key).ok_or_else(|| {
        format!(
            "{OS_FILE_TOOL_NAME} path references environment variable `{key}` that is not available in this run's launch context"
        )
    })?;
    append_env_path_suffix(value.clone(), suffix)
}

fn path_env_prefix(path: &str) -> Result<Option<(&str, &str)>, String> {
    if let Some(rest) = path.strip_prefix('%') {
        let Some(end) = rest.find('%') else {
            return Err(format!("{OS_FILE_TOOL_NAME} path has malformed %VAR% environment prefix"));
        };
        let key = &rest[..end];
        validate_path_env_key(key)?;
        return Ok(Some((key, &rest[end + 1..])));
    }
    if let Some(rest) = path.strip_prefix("${") {
        let Some(end) = rest.find('}') else {
            return Err(format!(
                "{OS_FILE_TOOL_NAME} path has malformed ${{VAR}} environment prefix"
            ));
        };
        let key = &rest[..end];
        validate_path_env_key(key)?;
        return Ok(Some((key, &rest[end + 1..])));
    }
    if let Some(rest) = path.strip_prefix('$') {
        let key_len = rest
            .char_indices()
            .take_while(|(_, ch)| ch.is_ascii_alphanumeric() || *ch == '_')
            .map(|(index, ch)| index + ch.len_utf8())
            .last()
            .unwrap_or(0);
        if key_len == 0 {
            return Err(format!("{OS_FILE_TOOL_NAME} path has malformed $VAR environment prefix"));
        }
        let key = &rest[..key_len];
        validate_path_env_key(key)?;
        return Ok(Some((key, &rest[key_len..])));
    }
    Ok(None)
}

fn validate_path_env_key(key: &str) -> Result<(), String> {
    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return Err(format!("{OS_FILE_TOOL_NAME} path environment variable name is empty"));
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(format!(
            "{OS_FILE_TOOL_NAME} path environment variable name must use ASCII letters, digits, or underscores"
        ));
    }
    Ok(())
}

fn append_env_path_suffix(mut base: PathBuf, suffix: &str) -> Result<PathBuf, String> {
    let relative_suffix = suffix.trim_start_matches(['/', '\\']);
    if relative_suffix.is_empty() {
        return Ok(base);
    }
    for segment in relative_suffix.split(['/', '\\']) {
        if segment.is_empty() {
            continue;
        }
        if segment == "." || segment == ".." || segment.contains(':') {
            return Err(format!(
                "{OS_FILE_TOOL_NAME} environment path suffix must stay relative to the expanded root"
            ));
        }
        if segment.chars().any(char::is_control) {
            return Err(format!("{OS_FILE_TOOL_NAME} path contains unsupported characters"));
        }
        base.push(segment);
    }
    Ok(base)
}

/// Splits `path` into its nearest existing ancestor directory and the
/// missing suffix, so the ancestor can be canonicalized and containment
/// checked even though the target itself does not exist yet.
fn nearest_existing_ancestor(path: &Path) -> Result<(PathBuf, PathBuf), String> {
    let mut cursor = path.to_path_buf();
    while !cursor.exists() {
        if !cursor.pop() {
            return Err(format!("{OS_FILE_TOOL_NAME} target path has no existing ancestor"));
        }
    }
    if !cursor.is_dir() {
        let Some(parent) = cursor.parent() else {
            return Err(format!("{OS_FILE_TOOL_NAME} target ancestor has no parent directory"));
        };
        cursor = parent.to_path_buf();
    }
    let suffix = path.strip_prefix(cursor.as_path()).map_err(|_| {
        format!("{OS_FILE_TOOL_NAME} failed to resolve target path relative to existing ancestor")
    })?;
    Ok((cursor, suffix.to_path_buf()))
}

/// Central containment gate: every operation must pass its resolved path
/// through this before any I/O.
///
/// The deny-list runs first so a protected OS path is rejected even when an
/// allowed root contains it; the path is then accepted only when it resolves
/// under a workspace root or an approved user-owned root.
fn ensure_os_path_allowed(policy: &OsFilePolicy, path: &ResolvedOsPath) -> Result<(), String> {
    if protected_os_path(path.resolved_path.as_path()) {
        return Err(format!("{OS_FILE_TOOL_NAME} denied protected OS path"));
    }
    if policy
        .workspace_roots
        .iter()
        .any(|root| path_starts_with(path.resolved_path.as_path(), root.as_path()))
    {
        return Ok(());
    }
    if policy
        .user_os_roots
        .iter()
        .any(|root| path_starts_with(path.resolved_path.as_path(), root.as_path()))
    {
        return Ok(());
    }
    Err(format!(
        "{OS_FILE_TOOL_NAME} path is outside agent workspace roots and approved user-owned OS roots"
    ))
}

/// Builds the user-owned OS roots. Explicitly configured roots replace the
/// implicit profile roots so operators can narrow host filesystem access.
fn user_owned_os_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(configured_roots) = configured_user_os_roots() {
        for root in configured_roots {
            push_canonical_root(&mut roots, root);
        }
    } else {
        for key in ["USERPROFILE", "HOME"] {
            if let Some(value) = std::env::var_os(key) {
                push_canonical_root(&mut roots, PathBuf::from(value));
            }
        }
    }
    push_canonical_root(&mut roots, std::env::temp_dir());
    #[cfg(unix)]
    {
        push_canonical_root(&mut roots, PathBuf::from("/var/tmp"));
    }
    roots
}

fn configured_user_os_roots() -> Option<Vec<PathBuf>> {
    let value = std::env::var_os(PALYRA_OS_FILE_ROOTS_ENV)?;
    let roots = std::env::split_paths(&value)
        .filter(|path| !path.as_os_str().is_empty())
        .collect::<Vec<_>>();
    if roots.is_empty() {
        None
    } else {
        Some(roots)
    }
}

fn push_canonical_root(roots: &mut Vec<PathBuf>, root: PathBuf) {
    if let Ok(canonical) = canonicalize_existing_dir(root.as_path()) {
        if !roots.iter().any(|existing| same_path(existing.as_path(), canonical.as_path())) {
            roots.push(canonical);
        }
    }
}

fn canonicalize_existing_dir(path: &Path) -> Result<PathBuf, String> {
    let canonical = fs::canonicalize(path).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} failed to resolve OS root {}: {error}", display_path(path))
    })?;
    if !canonical.is_dir() {
        return Err(format!("{OS_FILE_TOOL_NAME} OS root is not a directory"));
    }
    Ok(canonical)
}

/// Deny-list of OS locations the tool must never touch (drive/filesystem
/// roots and system directories), checked on canonicalized paths so symlinks
/// cannot disguise them.
fn protected_os_path(path: &Path) -> bool {
    #[cfg(windows)]
    {
        let normalized = path.to_string_lossy().replace('\\', "/").to_ascii_lowercase();
        normalized.ends_with(":/")
            || normalized.contains(":/windows")
            || normalized.contains(":/program files")
            || normalized.contains(":/program files (x86)")
            || normalized.contains(":/system volume information")
    }
    #[cfg(not(windows))]
    {
        let normalized = path.to_string_lossy().replace('\\', "/");
        if normalized == "/" {
            return true;
        }
        for prefix in ["/etc", "/bin", "/sbin", "/usr", "/lib", "/lib64", "/System", "/Library"] {
            if normalized == prefix || normalized.starts_with(format!("{prefix}/").as_str()) {
                return true;
            }
        }
        false
    }
}

/// Component-wise prefix check with a Windows fallback that compares
/// separator-normalized, lowercased strings, because NTFS paths are
/// case-insensitive and the two sides may differ in drive-letter or segment
/// casing even after canonicalization.
fn path_starts_with(path: &Path, root: &Path) -> bool {
    if path.starts_with(root) {
        return true;
    }
    #[cfg(windows)]
    {
        let path = path.to_string_lossy().replace('\\', "/").to_ascii_lowercase();
        let root = root.to_string_lossy().replace('\\', "/").to_ascii_lowercase();
        path == root || path.starts_with(format!("{root}/").as_str())
    }
    #[cfg(not(windows))]
    {
        false
    }
}

fn same_path(left: &Path, right: &Path) -> bool {
    #[cfg(windows)]
    {
        left.to_string_lossy()
            .replace('\\', "/")
            .eq_ignore_ascii_case(&right.to_string_lossy().replace('\\', "/"))
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

fn metadata_kind(metadata: &fs::Metadata) -> &'static str {
    if metadata.is_dir() {
        "directory"
    } else if metadata.is_file() {
        "file"
    } else {
        "other"
    }
}

fn metadata_modified_unix_ms(metadata: &fs::Metadata) -> Option<u64> {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn workspace_relative_path(policy: &OsFilePolicy, path: &Path) -> Option<String> {
    policy.workspace_roots.iter().find_map(|root| {
        path.strip_prefix(root.as_path())
            .ok()
            .filter(|relative| !relative.as_os_str().is_empty())
            .map(display_path)
    })
}

fn os_file_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    build_tool_execution_outcome(
        proposal_id,
        OS_FILE_TOOL_NAME,
        input_json,
        success,
        output_json,
        error,
        false,
        "os_file".to_owned(),
        "approved_os_paths".to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn os_file_tempdir() -> tempfile::TempDir {
        let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("target")
            .join("os-file-tests");
        fs::create_dir_all(base.as_path()).expect("os-file temp base should exist");
        let base = fs::canonicalize(base.as_path()).expect("os-file temp base should canonicalize");
        tempfile::Builder::new()
            .prefix("os-file-")
            .tempdir_in(base)
            .expect("os-file tempdir should be created")
    }

    #[cfg(windows)]
    fn stable_os_file_env_root(name: &str) -> PathBuf {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("target")
            .join("os-file-env-roots")
            .join(name);
        fs::create_dir_all(root.as_path()).expect("stable os-file env root should exist");
        fs::canonicalize(root.as_path()).expect("stable os-file env root should canonicalize")
    }

    struct ScopedEnvVar {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl ScopedEnvVar {
        fn set(key: &'static str, value: &Path) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }

        #[cfg(windows)]
        fn set_raw(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }

        #[cfg(windows)]
        fn remove(key: &'static str) -> Self {
            let previous = std::env::var_os(key);
            std::env::remove_var(key);
            Self { key, previous }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            match self.previous.as_ref() {
                Some(previous) => std::env::set_var(self.key, previous),
                None => std::env::remove_var(self.key),
            }
        }
    }

    #[cfg(unix)]
    fn create_directory_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn create_directory_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
        std::os::windows::fs::symlink_dir(target, link)
    }

    fn test_policy(root: &Path) -> OsFilePolicy {
        OsFilePolicy {
            workspace_roots: vec![fs::canonicalize(root).expect("root should canonicalize")],
            user_os_roots: vec![fs::canonicalize(root).expect("root should canonicalize")],
            path_env: BTreeMap::new(),
        }
    }

    #[test]
    fn os_file_write_and_read_absolute_user_path() {
        let tempdir = os_file_tempdir();
        let policy = test_policy(tempdir.path());
        let target = tempdir.path().join("os-level").join("reports").join("outside-report.md");

        let write = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Write,
                path: target.to_string_lossy().into_owned(),
                target_path: None,
                content_text: Some("palyra-os-level-ok\n".to_owned()),
                bytes_base64: None,
                create_parent_dirs: Some(true),
                overwrite: Some(true),
                full_replace: None,
                dry_run: Some(false),
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect("absolute user path write should succeed");

        assert_eq!(write.get("operation").and_then(Value::as_str), Some("write"));
        assert!(target.is_file());

        let read = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Read,
                path: target.to_string_lossy().into_owned(),
                target_path: None,
                content_text: None,
                bytes_base64: None,
                create_parent_dirs: None,
                overwrite: None,
                full_replace: None,
                dry_run: None,
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect("absolute user path read should succeed");

        assert_eq!(read.get("text").and_then(Value::as_str), Some("palyra-os-level-ok\n"));
        assert!(read.get("resolved_path").and_then(Value::as_str).is_some());
    }

    #[test]
    fn os_file_write_ignores_empty_bytes_base64_when_content_text_is_present() {
        let tempdir = os_file_tempdir();
        let policy = test_policy(tempdir.path());
        let target = tempdir.path().join("reports").join("text-write.md");

        let write = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Write,
                path: target.to_string_lossy().into_owned(),
                target_path: None,
                content_text: Some("report from content_text\n".to_owned()),
                bytes_base64: Some(String::new()),
                create_parent_dirs: Some(true),
                overwrite: Some(true),
                full_replace: None,
                dry_run: Some(false),
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect("empty bytes_base64 should not make text writes ambiguous");

        assert_eq!(write.get("operation").and_then(Value::as_str), Some("write"));
        assert_eq!(
            fs::read_to_string(target.as_path()).expect("text target should be written"),
            "report from content_text\n"
        );
    }

    #[test]
    fn os_file_write_ignores_empty_content_text_when_bytes_base64_is_present() {
        let tempdir = os_file_tempdir();
        let policy = test_policy(tempdir.path());
        let target = tempdir.path().join("reports").join("binary-write.bin");

        execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Write,
                path: target.to_string_lossy().into_owned(),
                target_path: None,
                content_text: Some(String::new()),
                bytes_base64: Some("YmluYXJ5LXNhZmUK".to_owned()),
                create_parent_dirs: Some(true),
                overwrite: Some(true),
                full_replace: None,
                dry_run: Some(false),
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect("empty content_text should not make base64 writes ambiguous");

        assert_eq!(
            fs::read(target.as_path()).expect("binary target should be written"),
            b"binary-safe\n"
        );
    }

    #[test]
    fn os_file_write_rejects_existing_file_without_full_replace_intent() {
        let tempdir = os_file_tempdir();
        let policy = test_policy(tempdir.path());
        let target = tempdir.path().join("notes").join("customer-note.md");
        fs::create_dir_all(target.parent().expect("target parent")).expect("parent dir");
        let original = "Customer: Ada Lovelace\nStatus: pending review\nNotes:\n- Keep original terms.\n- Confirm support window before renewal.\n";
        fs::write(target.as_path(), original.as_bytes()).expect("fixture should be written");

        let error = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Write,
                path: target.to_string_lossy().into_owned(),
                target_path: None,
                content_text: Some("Reviewed by Palyra E2E\n".to_owned()),
                bytes_base64: None,
                create_parent_dirs: Some(true),
                overwrite: Some(true),
                full_replace: None,
                dry_run: Some(false),
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect_err("existing-file write should require explicit full_replace intent");

        assert!(error.contains("refusing write to existing file"), "unexpected error: {error}");
        assert!(error.contains("append/partial-edit semantics are unsupported"), "{error}");
        assert!(
            error.contains("palyra.fs.apply_patch"),
            "error should guide scoped edits: {error}"
        );
        assert!(
            error.contains("full_replace=true"),
            "error should explain explicit full replacement: {error}"
        );
        assert_eq!(
            fs::read_to_string(target.as_path()).expect("target should remain readable"),
            original,
            "rejected fragment write must leave the existing file unchanged"
        );

        let replaced = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Write,
                path: target.to_string_lossy().into_owned(),
                target_path: None,
                content_text: Some(format!("{original}Reviewed by Palyra E2E\n")),
                bytes_base64: None,
                create_parent_dirs: Some(true),
                overwrite: Some(true),
                full_replace: Some(true),
                dry_run: Some(false),
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect("explicit full_replace should allow intentional whole-file replacement");

        assert_eq!(replaced.get("full_replace").and_then(Value::as_bool), Some(true));
        assert_eq!(
            fs::read_to_string(target.as_path()).expect("replacement should be readable"),
            format!("{original}Reviewed by Palyra E2E\n")
        );
    }

    #[test]
    fn os_file_move_accepts_workspace_relative_target_path() {
        let tempdir = os_file_tempdir();
        let workspace = tempdir.path().join("workspace");
        let os_root = tempdir.path().join("os-root");
        let inbox = os_root.join("downloads").join("inbox");
        fs::create_dir_all(workspace.as_path()).expect("workspace should exist");
        fs::create_dir_all(inbox.as_path()).expect("inbox should exist");
        let source = inbox.join("orders-valid.csv");
        fs::write(source.as_path(), "id,name,total\n1,Ada,42\n").expect("source should exist");
        let policy = OsFilePolicy {
            workspace_roots: vec![fs::canonicalize(workspace.as_path()).expect("workspace root")],
            user_os_roots: vec![fs::canonicalize(os_root.as_path()).expect("os root")],
            path_env: BTreeMap::new(),
        };

        let moved = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Move,
                path: source.to_string_lossy().into_owned(),
                target_path: Some("data/imported/orders-valid.csv".to_owned()),
                content_text: None,
                bytes_base64: None,
                create_parent_dirs: Some(true),
                overwrite: Some(false),
                full_replace: None,
                dry_run: Some(false),
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect("workspace-relative move target should import an OS file into the workspace");

        let imported = workspace.join("data").join("imported").join("orders-valid.csv");
        assert!(!source.exists(), "move should remove the source file");
        assert_eq!(
            fs::read_to_string(imported.as_path()).expect("imported file should be readable"),
            "id,name,total\n1,Ada,42\n"
        );
        assert_eq!(
            moved.get("target_workspace_relative_path").and_then(Value::as_str),
            Some("data/imported/orders-valid.csv")
        );
        assert_eq!(moved.get("target_existed_before").and_then(Value::as_bool), Some(false));
    }

    #[test]
    fn os_file_copy_rejects_workspace_relative_symlink_target_escape() {
        let tempdir = os_file_tempdir();
        let workspace = tempdir.path().join("workspace");
        let os_root = tempdir.path().join("os-root");
        let inbox = os_root.join("downloads").join("inbox");
        let autostart = os_root.join(".config").join("autostart");
        fs::create_dir_all(workspace.as_path()).expect("workspace should exist");
        fs::create_dir_all(inbox.as_path()).expect("inbox should exist");
        fs::create_dir_all(autostart.as_path()).expect("autostart should exist");
        let source = inbox.join("payload.desktop");
        fs::write(source.as_path(), "[Desktop Entry]\nName=Palyra\n").expect("source should exist");
        let link = workspace.join("data");
        if let Err(error) = create_directory_symlink(autostart.as_path(), link.as_path()) {
            eprintln!(
                "skipping os_file workspace-relative symlink regression because symlink creation failed: {error}"
            );
            return;
        }
        let policy = OsFilePolicy {
            workspace_roots: vec![fs::canonicalize(workspace.as_path()).expect("workspace root")],
            user_os_roots: vec![fs::canonicalize(os_root.as_path()).expect("os root")],
            path_env: BTreeMap::new(),
        };

        let error = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Copy,
                path: source.to_string_lossy().into_owned(),
                target_path: Some("data/payload.desktop".to_owned()),
                content_text: None,
                bytes_base64: None,
                create_parent_dirs: Some(true),
                overwrite: Some(false),
                full_replace: None,
                dry_run: Some(false),
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect_err(
            "workspace-relative target must not resolve through a symlink outside workspace",
        );

        assert!(error.contains("workspace-relative target_path must stay inside"));
        assert!(
            !autostart.join("payload.desktop").exists(),
            "denied workspace-relative target must not write through the symlink"
        );
    }

    #[test]
    fn os_file_workspace_target_alias_requires_relative_file_path() {
        let error = normalize_workspace_relative_target("/workspace")
            .expect_err("workspace alias alone should not name an import destination");
        assert!(error.contains("must include a file path"));

        let relative = normalize_workspace_relative_target("/workspace/data/file.txt")
            .expect("workspace alias with relative suffix should be accepted");
        assert_eq!(relative, PathBuf::from("data").join("file.txt"));
    }

    #[test]
    fn os_file_read_expands_launch_context_env_path_prefixes() {
        let tempdir = os_file_tempdir();
        let os_root = tempdir.path().join("os-root");
        let inbox = os_root.join("downloads").join("inbox");
        fs::create_dir_all(inbox.as_path()).expect("inbox should exist");
        let target = inbox.join("orders-valid.csv");
        fs::write(target.as_path(), "id,name,total\n1,Ada,42\n").expect("fixture should exist");
        let canonical_os_root =
            fs::canonicalize(os_root.as_path()).expect("os root should canonicalize");
        let policy = OsFilePolicy {
            workspace_roots: vec![fs::canonicalize(tempdir.path()).expect("workspace root")],
            user_os_roots: vec![canonical_os_root.clone()],
            path_env: BTreeMap::from([("PALYRA_E2E_OS_ROOT".to_owned(), canonical_os_root)]),
        };

        for env_path in [
            "%PALYRA_E2E_OS_ROOT%/downloads/inbox/orders-valid.csv",
            "$PALYRA_E2E_OS_ROOT/downloads/inbox/orders-valid.csv",
            "${PALYRA_E2E_OS_ROOT}/downloads/inbox/orders-valid.csv",
        ] {
            let read = execute_os_file_operation(
                &policy,
                &OsFileInput {
                    operation: OsFileOperation::Read,
                    path: env_path.to_owned(),
                    target_path: None,
                    content_text: None,
                    bytes_base64: None,
                    create_parent_dirs: None,
                    overwrite: None,
                    full_replace: None,
                    dry_run: None,
                    offset_bytes: None,
                    max_bytes: None,
                    query: None,
                    case_sensitive: None,
                    max_entries: None,
                    max_matches: None,
                },
            )
            .expect("leading environment path should expand before allowlist validation");
            assert_eq!(read.get("text").and_then(Value::as_str), Some("id,name,total\n1,Ada,42\n"));
            assert_eq!(
                read.get("resolved_path").and_then(Value::as_str),
                Some(
                    display_path(
                        fs::canonicalize(target.as_path()).expect("target canonical").as_path()
                    )
                    .as_str()
                )
            );
        }
    }

    #[test]
    fn os_file_env_path_suffix_must_stay_relative_to_expanded_root() {
        let tempdir = os_file_tempdir();
        let canonical_root = fs::canonicalize(tempdir.path()).expect("tempdir should canonicalize");
        let mut policy = test_policy(tempdir.path());
        policy.path_env.insert("PALYRA_E2E_OS_ROOT".to_owned(), canonical_root);
        let error = parse_absolute_os_path(&policy, "%PALYRA_E2E_OS_ROOT%/../escape.txt")
            .expect_err("environment path suffix must not contain parent traversal");
        assert!(error.contains("must stay relative to the expanded root"));
    }

    #[test]
    fn os_file_read_rejects_daemon_process_env_path_prefixes() {
        let _guard = crate::test_env::lock();
        let tempdir = os_file_tempdir();
        let secret_path = tempdir.path().join("application_default_credentials.json");
        fs::write(secret_path.as_path(), "credential_file_contents=do-not-read\n")
            .expect("credential fixture should exist");
        let policy = test_policy(tempdir.path());
        let _root = ScopedEnvVar::set("GOOGLE_APPLICATION_CREDENTIALS", secret_path.as_path());

        let error = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Read,
                path: "$GOOGLE_APPLICATION_CREDENTIALS".to_owned(),
                target_path: None,
                content_text: None,
                bytes_base64: None,
                create_parent_dirs: None,
                overwrite: None,
                full_replace: None,
                dry_run: None,
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect_err("daemon process env path prefixes must not expand");

        assert!(error.contains("not available in this run's launch context"));
        assert!(error.contains("GOOGLE_APPLICATION_CREDENTIALS"));
        assert!(
            !error.contains(display_path(secret_path.as_path()).as_str()),
            "error must not disclose the daemon env value: {error}"
        );
    }

    #[test]
    fn os_file_read_expands_launch_context_path_env_without_process_env() {
        let tempdir = os_file_tempdir();
        let e2e_home = tempdir.path().join("S090-home");
        let config_dir = e2e_home.join(".config").join("palyra-e2e");
        fs::create_dir_all(config_dir.as_path()).expect("config dir should be created");
        let settings = config_dir.join("settings.toml");
        fs::write(settings.as_path(), "default_model = \"MiniMax-M3\"\n")
            .expect("settings should be written");
        let canonical_home =
            fs::canonicalize(e2e_home.as_path()).expect("home should canonicalize");
        let policy = OsFilePolicy {
            workspace_roots: vec![fs::canonicalize(tempdir.path()).expect("root canonicalizes")],
            user_os_roots: vec![canonical_home.clone()],
            path_env: BTreeMap::from([("PALYRA_E2E_HOME".to_owned(), canonical_home)]),
        };

        let read = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Read,
                path: "$PALYRA_E2E_HOME/.config/palyra-e2e/settings.toml".to_owned(),
                target_path: None,
                content_text: None,
                bytes_base64: None,
                create_parent_dirs: None,
                overwrite: None,
                full_replace: None,
                dry_run: None,
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect("launch-context env root should expand and pass allowlist validation");

        assert_eq!(read.get("operation").and_then(Value::as_str), Some("read"));
        assert_eq!(
            read.get("text").and_then(Value::as_str),
            Some("default_model = \"MiniMax-M3\"\n")
        );
    }

    #[test]
    fn os_file_read_redacts_provider_key_values() {
        let tempdir = os_file_tempdir();
        let policy = test_policy(tempdir.path());
        let target = tempdir.path().join("settings.toml");
        fs::write(
            target.as_path(),
            "provider_key = \"palyra_os_secret_abcdef\"\nmode = \"test\"\n",
        )
        .expect("secret-bearing OS file should be written");

        let read = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Read,
                path: target.to_string_lossy().into_owned(),
                target_path: None,
                content_text: None,
                bytes_base64: None,
                create_parent_dirs: None,
                overwrite: None,
                full_replace: None,
                dry_run: None,
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect("absolute user path read should succeed");

        let text = read.get("text").and_then(Value::as_str).expect("read text should be present");
        assert_eq!(read.get("redacted").and_then(Value::as_bool), Some(true));
        assert_eq!(read.get("text_authoritative").and_then(Value::as_bool), Some(false));
        assert!(read
            .get("redaction_notice")
            .and_then(Value::as_str)
            .is_some_and(|notice| notice.contains("structure only")));
        assert!(read.get("redaction_reasons").and_then(Value::as_array).is_some_and(|reasons| {
            reasons.iter().any(|reason| reason.as_str() == Some("secret_leak.assignment.key"))
        }));
        assert!(text.contains("provider_key = \"[REDACTED_SECRET]\""));
        assert!(!text.contains("palyra_os_secret_abcdef"));
    }

    #[test]
    fn non_utf8_visible_content_scans_lossy_text_before_base64_fallback() {
        let mut bytes = vec![0xff, 0xfe, b'\n'];
        bytes.extend_from_slice(b"provider_key = \"palyra_os_secret_abcdef\"\n");

        let (text, bytes_base64, redacted, redaction_reasons) = visible_file_content(bytes);

        let text = text.expect("secret-bearing lossy content should return redacted text");
        assert!(redacted);
        assert!(bytes_base64.is_none());
        assert!(redaction_reasons.iter().any(|reason| reason == "secret_leak.assignment.key"));
        assert!(text.contains("provider_key = \"[REDACTED_SECRET]\""));
        assert!(!text.contains("palyra_os_secret_abcdef"));
    }

    #[test]
    fn os_file_read_preserves_benign_auth_session_storage_key() {
        let tempdir = os_file_tempdir();
        let policy = test_policy(tempdir.path());
        let target = tempdir.path().join("app.js");
        let contents = "const sessionKey = \"s058-auth-session\";\n\
                        localStorage.setItem(sessionKey, JSON.stringify(state));\n";
        fs::write(target.as_path(), contents).expect("OS file should be written");

        let read = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Read,
                path: target.to_string_lossy().into_owned(),
                target_path: None,
                content_text: None,
                bytes_base64: None,
                create_parent_dirs: None,
                overwrite: None,
                full_replace: None,
                dry_run: None,
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect("absolute user path read should succeed");

        assert_eq!(read.get("redacted").and_then(Value::as_bool), Some(false));
        assert_eq!(read.get("text_authoritative").and_then(Value::as_bool), Some(true));
        assert_eq!(read.get("redaction_reasons"), Some(&Value::Null));
        assert_eq!(read.get("text").and_then(Value::as_str), Some(contents));
    }

    #[test]
    fn os_file_read_preserves_public_password_fixture_values() {
        let tempdir = os_file_tempdir();
        let policy = test_policy(tempdir.path());
        let target = tempdir.path().join("Dockerfile");
        let contents = "ENV PASSWORD=password1\nRUN echo password\n";
        fs::write(target.as_path(), contents).expect("OS file should be written");

        let read = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Read,
                path: target.to_string_lossy().into_owned(),
                target_path: None,
                content_text: None,
                bytes_base64: None,
                create_parent_dirs: None,
                overwrite: None,
                full_replace: None,
                dry_run: None,
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect("absolute user path read should succeed");

        assert_eq!(read.get("redacted").and_then(Value::as_bool), Some(false));
        assert_eq!(read.get("text").and_then(Value::as_str), Some(contents));
        assert_eq!(read.get("redaction_reasons"), Some(&Value::Null));
    }

    #[test]
    fn os_file_rejects_path_outside_workspace_and_user_roots() {
        let allowed_root = os_file_tempdir();
        let outside_root = os_file_tempdir();
        let workspace = allowed_root.path().join("workspace");
        let outside = outside_root.path().join("outside").join("report.md");
        fs::create_dir_all(workspace.as_path()).expect("workspace should exist");
        fs::create_dir_all(outside.parent().expect("outside parent")).expect("outside parent");
        let policy = OsFilePolicy {
            workspace_roots: vec![fs::canonicalize(workspace).expect("workspace canonical")],
            user_os_roots: vec![
                fs::canonicalize(allowed_root.path()).expect("allowed root canonical")
            ],
            path_env: BTreeMap::new(),
        };

        let error = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Write,
                path: outside.to_string_lossy().into_owned(),
                target_path: None,
                content_text: Some("outside\n".to_owned()),
                bytes_base64: None,
                create_parent_dirs: Some(true),
                overwrite: Some(true),
                full_replace: None,
                dry_run: Some(false),
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect_err("outside path should require an approved root");

        assert!(error.contains("approved user-owned OS roots"), "unexpected error: {error}");
    }

    #[test]
    fn os_file_rejects_filesystem_root_even_when_root_is_allowlisted() {
        let tempdir = os_file_tempdir();
        let filesystem_root = tempdir
            .path()
            .ancestors()
            .last()
            .expect("tempdir should have a filesystem root")
            .to_path_buf();
        let mut policy = test_policy(tempdir.path());
        policy.workspace_roots = vec![filesystem_root.clone()];
        policy.user_os_roots = vec![filesystem_root.clone()];

        let error = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Stat,
                path: filesystem_root.to_string_lossy().into_owned(),
                target_path: None,
                content_text: None,
                bytes_base64: None,
                create_parent_dirs: None,
                overwrite: None,
                full_replace: None,
                dry_run: None,
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect_err("protected filesystem root must remain denied");

        assert!(error.contains("denied protected OS path"), "unexpected error: {error}");
    }

    #[test]
    fn os_file_delete_empty_dir_removes_only_empty_directories() {
        let tempdir = os_file_tempdir();
        let policy = test_policy(tempdir.path());
        let non_empty = tempdir.path().join("scratch").join("non-empty");
        fs::create_dir_all(non_empty.as_path()).expect("non-empty dir should exist");
        fs::write(non_empty.join("keep.txt"), "keep\n").expect("nested file should exist");

        let error = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::DeleteEmptyDir,
                path: non_empty.to_string_lossy().into_owned(),
                target_path: None,
                content_text: None,
                bytes_base64: None,
                create_parent_dirs: None,
                overwrite: None,
                full_replace: None,
                dry_run: None,
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect_err("non-empty directories must not be removed");
        assert!(error.contains("failed to remove empty directory"), "unexpected error: {error}");
        assert!(non_empty.exists(), "non-empty directory should remain");

        let empty = tempdir.path().join("scratch").join("empty");
        fs::create_dir_all(empty.as_path()).expect("empty dir should exist");
        let removed = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::DeleteEmptyDir,
                path: empty.to_string_lossy().into_owned(),
                target_path: None,
                content_text: None,
                bytes_base64: None,
                create_parent_dirs: None,
                overwrite: None,
                full_replace: None,
                dry_run: None,
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect("empty directory cleanup should succeed");

        assert_eq!(removed.get("operation").and_then(Value::as_str), Some("delete_empty_dir"));
        assert!(!empty.exists(), "empty directory should be removed");
    }

    #[test]
    fn os_file_configured_roots_replace_implicit_user_profile_root() {
        let _guard = crate::test_env::lock();
        let configured_root = os_file_tempdir();
        let real_home_root = os_file_tempdir();
        let _configured = ScopedEnvVar::set(PALYRA_OS_FILE_ROOTS_ENV, configured_root.path());
        let _userprofile = ScopedEnvVar::set("USERPROFILE", real_home_root.path());
        let _home = ScopedEnvVar::set("HOME", real_home_root.path());

        let roots = user_owned_os_roots();
        let configured_root =
            fs::canonicalize(configured_root.path()).expect("configured root should canonicalize");
        let real_home_root =
            fs::canonicalize(real_home_root.path()).expect("real home should canonicalize");

        assert!(
            roots.iter().any(|root| same_path(root.as_path(), configured_root.as_path())),
            "configured OS file root should be allowed: {roots:?}"
        );
        assert!(
            !roots.iter().any(|root| same_path(root.as_path(), real_home_root.as_path())),
            "implicit user profile roots must not be allowed when PALYRA_OS_FILE_ROOTS is set: {roots:?}"
        );
    }

    #[test]
    #[cfg(windows)]
    fn os_file_user_roots_do_not_auto_allow_system_drive_var_tmp() {
        let _guard = crate::test_env::lock();
        let tempdir = os_file_tempdir();
        let profile_root = tempdir.path().join("profile");
        let temp_root = stable_os_file_env_root("windows-user-temp");
        fs::create_dir_all(profile_root.as_path()).expect("profile root should exist");
        let _configured = ScopedEnvVar::remove(PALYRA_OS_FILE_ROOTS_ENV);
        let _userprofile = ScopedEnvVar::set("USERPROFILE", profile_root.as_path());
        let _home = ScopedEnvVar::set("HOME", profile_root.as_path());
        let _temp = ScopedEnvVar::set("TEMP", temp_root.as_path());
        let _tmp = ScopedEnvVar::set("TMP", temp_root.as_path());
        let _system_drive = ScopedEnvVar::set_raw("SystemDrive", "C:");

        let roots = user_owned_os_roots();
        let canonical_temp = fs::canonicalize(temp_root).expect("temp root should canonicalize");

        assert!(
            roots.iter().any(|root| same_path(root.as_path(), canonical_temp.as_path())),
            "Windows TEMP should remain an approved user-owned OS root: {roots:?}"
        );
        assert!(
            roots.iter().all(|root| !is_windows_drive_var_tmp(root.as_path())),
            "SystemDrive must not auto-approve machine-wide var tmp roots: {roots:?}"
        );
    }

    #[cfg(windows)]
    fn is_windows_drive_var_tmp(path: &Path) -> bool {
        let normalized = path.to_string_lossy().replace('\\', "/").to_ascii_lowercase();
        let mut chars = normalized.chars();
        matches!(chars.next(), Some(ch) if ch.is_ascii_alphabetic())
            && chars.next() == Some(':')
            && chars.as_str() == "/var/tmp"
    }

    #[test]
    fn os_file_dry_run_does_not_write() {
        let tempdir = os_file_tempdir();
        let policy = test_policy(tempdir.path());
        let target = tempdir.path().join("reports").join("dry-run.md");

        execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Write,
                path: target.to_string_lossy().into_owned(),
                target_path: None,
                content_text: Some("dry-run\n".to_owned()),
                bytes_base64: None,
                create_parent_dirs: Some(true),
                overwrite: Some(true),
                full_replace: None,
                dry_run: Some(true),
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: None,
                max_matches: None,
            },
        )
        .expect("dry-run write should validate");

        assert!(!target.exists());
    }

    #[test]
    fn os_file_lists_and_searches_user_cache_paths() {
        let tempdir = os_file_tempdir();
        let policy = test_policy(tempdir.path());
        let cache_dir = tempdir.path().join(".cache").join("palyra").join("memory");
        fs::create_dir_all(cache_dir.as_path()).expect("cache dir should be created");
        let token_file = cache_dir.join("palyra_e2e_delete_me.cache");
        let other_file = cache_dir.join("keep.cache");
        fs::write(token_file.as_path(), "token=palyra_e2e_delete_me\n")
            .expect("token cache fixture should be written");
        fs::write(other_file.as_path(), "token=keep\n").expect("other cache fixture");

        let listed = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::ListDir,
                path: cache_dir.to_string_lossy().into_owned(),
                target_path: None,
                content_text: None,
                bytes_base64: None,
                create_parent_dirs: None,
                overwrite: None,
                full_replace: None,
                dry_run: None,
                offset_bytes: None,
                max_bytes: None,
                query: None,
                case_sensitive: None,
                max_entries: Some(10),
                max_matches: None,
            },
        )
        .expect("OS cache dir list should succeed");

        assert_eq!(listed.get("entry_count").and_then(Value::as_u64), Some(2));

        let searched = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Search,
                path: cache_dir.to_string_lossy().into_owned(),
                target_path: None,
                content_text: None,
                bytes_base64: None,
                create_parent_dirs: None,
                overwrite: None,
                full_replace: None,
                dry_run: None,
                offset_bytes: None,
                max_bytes: None,
                query: Some("palyra_e2e_delete_me".to_owned()),
                case_sensitive: Some(false),
                max_entries: None,
                max_matches: Some(10),
            },
        )
        .expect("OS cache search should succeed");

        let matches = searched
            .get("matches")
            .and_then(Value::as_array)
            .expect("search matches should be an array");
        assert!(
            matches.iter().any(|value| value.get("kind").and_then(Value::as_str) == Some("path")),
            "search should find matching file names: {searched}"
        );
        assert!(
            matches
                .iter()
                .any(|value| value.get("kind").and_then(Value::as_str) == Some("content")),
            "search should find matching cache contents: {searched}"
        );
    }

    #[test]
    fn os_file_search_default_budget_scans_past_one_thousand_files() {
        let tempdir = os_file_tempdir();
        let policy = test_policy(tempdir.path());
        for index in 0..1_050 {
            fs::write(tempdir.path().join(format!("noise-{index:04}.txt")), "noise\n")
                .expect("noise fixture should be written");
        }
        let target = tempdir.path().join("orders-valid.csv");
        fs::write(target.as_path(), "id,name,total\n1,Ada,42\n").expect("target fixture");

        let searched = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Search,
                path: tempdir.path().to_string_lossy().into_owned(),
                target_path: None,
                content_text: None,
                bytes_base64: None,
                create_parent_dirs: None,
                overwrite: None,
                full_replace: None,
                dry_run: None,
                offset_bytes: None,
                max_bytes: None,
                query: Some("orders-valid.csv".to_owned()),
                case_sensitive: Some(false),
                max_entries: None,
                max_matches: Some(10),
            },
        )
        .expect("broad OS search should scan past the previous 1000-file budget");

        assert_eq!(searched.get("truncated").and_then(Value::as_bool), Some(false));
        assert!(
            searched.get("files_scanned").and_then(Value::as_u64).unwrap_or_default() > 1_000,
            "search should scan past 1000 files: {searched}"
        );
        let matches = searched
            .get("matches")
            .and_then(Value::as_array)
            .expect("search matches should be an array");
        assert!(
            matches.iter().any(|value| {
                value
                    .get("path")
                    .and_then(Value::as_str)
                    .is_some_and(|path| path.ends_with("orders-valid.csv"))
            }),
            "search should find the target file path: {searched}"
        );
    }
}
