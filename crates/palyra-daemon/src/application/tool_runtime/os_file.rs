use std::{
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
    application::tool_runtime::workspace_scope::workspace_roots_with_run_launch_context,
    gateway::{GatewayRuntimeState, ToolRuntimeExecutionContext, OS_FILE_TOOL_NAME},
    tool_protocol::{build_tool_execution_outcome, ToolExecutionOutcome},
};

const MAX_OS_FILE_READ_BYTES: u64 = 128 * 1024;
const MAX_OS_FILE_TOOL_INPUT_BYTES: usize = 384 * 1024;
const MAX_OS_FILE_WRITE_BYTES: usize = 256 * 1024;
const MAX_OS_FILE_LIST_ENTRIES: usize = 200;
const MAX_OS_FILE_SEARCH_QUERY_BYTES: usize = 512;
const MAX_OS_FILE_SEARCH_MATCHES: usize = 100;
const MAX_OS_FILE_SEARCH_FILES: usize = 1_000;
const MAX_OS_FILE_SEARCH_DEPTH: usize = 8;
const MAX_OS_FILE_SEARCH_FILE_BYTES: u64 = 128 * 1024;
const MAX_OS_FILE_SEARCH_EXCERPT_CHARS: usize = 240;
const OS_FILE_LARGE_SHRINK_MIN_EXISTING_BYTES: u64 = 1024;
const OS_FILE_LARGE_SHRINK_MAX_NEW_PERCENT: u64 = 50;
const OS_FILE_LARGE_SHRINK_MIN_DELTA_BYTES: u64 = 512;
const PALYRA_OS_FILE_ROOTS_ENV: &str = "PALYRA_OS_FILE_ROOTS";

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
    Mkdir,
    ListDir,
    Search,
}

#[derive(Debug, Clone)]
struct OsFilePolicy {
    workspace_roots: Vec<PathBuf>,
    user_os_roots: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
struct ResolvedOsPath {
    requested_path: PathBuf,
    resolved_path: PathBuf,
    existed: bool,
}

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
    let workspace_roots = workspace_roots_with_run_launch_context(
        runtime_state,
        context.run_id,
        agent_workspace_roots.as_slice(),
    )
    .await
    .iter()
    .filter_map(|root| canonicalize_existing_dir(root.as_path()).ok())
    .collect::<Vec<_>>();
    let user_os_roots = user_owned_os_roots();
    Ok(OsFilePolicy { workspace_roots, user_os_roots })
}

fn execute_os_file_operation(policy: &OsFilePolicy, input: &OsFileInput) -> Result<Value, String> {
    match input.operation {
        OsFileOperation::Stat => stat_path(policy, input),
        OsFileOperation::Read => read_path(policy, input),
        OsFileOperation::Write => write_path(policy, input),
        OsFileOperation::Copy => copy_path(policy, input),
        OsFileOperation::Move => move_path(policy, input),
        OsFileOperation::DeleteFile => delete_file_path(policy, input),
        OsFileOperation::Mkdir => mkdir_path(policy, input),
        OsFileOperation::ListDir => list_dir_path(policy, input),
        OsFileOperation::Search => search_path(policy, input),
    }
}

fn stat_path(policy: &OsFilePolicy, input: &OsFileInput) -> Result<Value, String> {
    let path = resolve_existing_os_path(input.path.as_str())?;
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
    let path = resolve_existing_os_path(input.path.as_str())?;
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
    let mut buffer = Vec::with_capacity(usize::try_from(read_limit.min(8192)).unwrap_or(8192));
    file.take(read_limit).read_to_end(&mut buffer).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} failed to read {}: {error}", input.path.trim())
    })?;
    let returned_bytes = u64::try_from(buffer.len()).expect("OS file read size must fit u64");
    let eof = offset_bytes.saturating_add(returned_bytes) >= size_bytes;
    let chunk_sha256 = hex::encode(Sha256::digest(buffer.as_slice()));
    let (text, bytes_base64, redacted) = visible_file_content(buffer);
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
        "bytes_base64": bytes_base64,
        "redacted": redacted,
        "dry_run": false,
    }))
}

fn write_path(policy: &OsFilePolicy, input: &OsFileInput) -> Result<Value, String> {
    let bytes = input_write_bytes(input)?;
    let path = resolve_target_os_path(input.path.as_str())?;
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
    guard_large_full_file_shrink(input, existing_size_bytes, bytes.len())?;
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

fn guard_large_full_file_shrink(
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
    if !looks_like_large_full_file_shrink(existing_size_bytes, new_size_bytes) {
        return Ok(());
    }
    Err(format!(
        "{OS_FILE_TOOL_NAME} refusing large full-file shrink for existing file {}: existing_size_bytes={} new_size_bytes={}. operation=write with overwrite=true replaces the entire file. Use palyra.fs.apply_patch for scoped workspace edits, or retry with full_replace=true only when replacing the whole file is intentional.",
        input.path.trim(),
        existing_size_bytes,
        new_size_bytes
    ))
}

fn looks_like_large_full_file_shrink(existing_size_bytes: u64, new_size_bytes: usize) -> bool {
    if existing_size_bytes < OS_FILE_LARGE_SHRINK_MIN_EXISTING_BYTES {
        return false;
    }
    let Ok(new_size_bytes) = u64::try_from(new_size_bytes) else {
        return true;
    };
    if existing_size_bytes.saturating_sub(new_size_bytes) < OS_FILE_LARGE_SHRINK_MIN_DELTA_BYTES {
        return false;
    }
    new_size_bytes.saturating_mul(100)
        <= existing_size_bytes.saturating_mul(OS_FILE_LARGE_SHRINK_MAX_NEW_PERCENT)
}

fn copy_path(policy: &OsFilePolicy, input: &OsFileInput) -> Result<Value, String> {
    let source = resolve_existing_os_path(input.path.as_str())?;
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
    let source = resolve_existing_os_path(input.path.as_str())?;
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
    let path = resolve_existing_os_path(input.path.as_str())?;
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

fn mkdir_path(policy: &OsFilePolicy, input: &OsFileInput) -> Result<Value, String> {
    let path = resolve_target_os_path(input.path.as_str())?;
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
    let path = resolve_existing_os_path(input.path.as_str())?;
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
    let path = resolve_existing_os_path(input.path.as_str())?;
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
        let excerpt = search_excerpt(line, match_index, state.query.len());
        let redaction = redact_text_for_export(
            excerpt.as_str(),
            SafetySourceKind::Workspace,
            SafetyContentKind::WorkspaceDocument,
            TrustLabel::TrustedLocal,
        );
        let redacted = redaction.scan.has_category(SafetyFindingCategory::SecretLeak);
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
        (Some(_), Some(_)) => Err(format!(
            "{OS_FILE_TOOL_NAME} write accepts either content_text or bytes_base64, not both"
        )),
        (Some(text), None) => {
            if text.len() > MAX_OS_FILE_WRITE_BYTES {
                return Err(format!(
                    "{OS_FILE_TOOL_NAME} content_text exceeds {MAX_OS_FILE_WRITE_BYTES} bytes"
                ));
            }
            Ok(text.as_bytes().to_vec())
        }
        (None, Some(bytes_base64)) => {
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
        (None, None) => {
            Err(format!("{OS_FILE_TOOL_NAME} write requires content_text or bytes_base64"))
        }
    }
}

fn visible_file_content(buffer: Vec<u8>) -> (Option<String>, Option<String>, bool) {
    match String::from_utf8(buffer) {
        Ok(text) => {
            let redaction = redact_text_for_export(
                text.as_str(),
                SafetySourceKind::Workspace,
                SafetyContentKind::WorkspaceDocument,
                TrustLabel::TrustedLocal,
            );
            let redacted = redaction.scan.has_category(SafetyFindingCategory::SecretLeak);
            let visible_text = if redacted { redaction.redacted_text } else { text };
            (Some(visible_text), None, redacted)
        }
        Err(error) => (None, Some(BASE64_STANDARD.encode(error.into_bytes())), false),
    }
}

fn required_target_path(input: &OsFileInput) -> Result<&str, String> {
    input
        .target_path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{OS_FILE_TOOL_NAME} operation requires non-empty target_path"))
}

fn resolve_copy_move_target_path(
    policy: &OsFilePolicy,
    target_path: &str,
) -> Result<ResolvedOsPath, String> {
    let trimmed = target_path.trim();
    if path_env_prefix(trimmed)?.is_some() {
        return resolve_target_os_path(trimmed);
    }
    if is_workspace_relative_target(trimmed) || !Path::new(trimmed).is_absolute() {
        return resolve_workspace_relative_target_path(policy, trimmed);
    }
    resolve_target_os_path(trimmed)
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
        return Ok(ResolvedOsPath { requested_path, resolved_path, existed: true });
    }
    let (existing_ancestor, missing_suffix) = nearest_existing_ancestor(requested_path.as_path())?;
    let canonical_ancestor = fs::canonicalize(existing_ancestor.as_path()).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} failed to resolve workspace target ancestor: {error}")
    })?;
    let resolved_path = canonical_ancestor.join(missing_suffix);
    Ok(ResolvedOsPath { requested_path, resolved_path, existed: false })
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

fn resolve_existing_os_path(path: &str) -> Result<ResolvedOsPath, String> {
    let requested_path = parse_absolute_os_path(path)?;
    let resolved_path = fs::canonicalize(requested_path.as_path()).map_err(|error| {
        format!("{OS_FILE_TOOL_NAME} path does not resolve to an existing OS file target: {error}")
    })?;
    Ok(ResolvedOsPath { requested_path, resolved_path, existed: true })
}

fn resolve_target_os_path(path: &str) -> Result<ResolvedOsPath, String> {
    let requested_path = parse_absolute_os_path(path)?;
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

fn parse_absolute_os_path(path: &str) -> Result<PathBuf, String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(format!("{OS_FILE_TOOL_NAME} path must be non-empty"));
    }
    if trimmed.chars().any(char::is_control) {
        return Err(format!("{OS_FILE_TOOL_NAME} path contains unsupported characters"));
    }
    let parsed = expand_env_prefixed_os_path(trimmed)?;
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

fn expand_env_prefixed_os_path(path: &str) -> Result<PathBuf, String> {
    let Some((key, suffix)) = path_env_prefix(path)? else {
        return Ok(PathBuf::from(path));
    };
    let value = std::env::var_os(key).filter(|value| !value.is_empty()).ok_or_else(|| {
        format!("{OS_FILE_TOOL_NAME} path references unset environment variable `{key}`")
    })?;
    append_env_path_suffix(PathBuf::from(value), suffix)
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
    let relative_suffix = suffix.trim_start_matches(|ch| ch == '/' || ch == '\\');
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

fn ensure_os_path_allowed(policy: &OsFilePolicy, path: &ResolvedOsPath) -> Result<(), String> {
    if protected_os_path(path.resolved_path.as_path()) {
        return Err(format!(
            "{OS_FILE_TOOL_NAME} denied protected OS path {}",
            display_path(path.resolved_path.as_path())
        ));
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
        "{OS_FILE_TOOL_NAME} path {} is outside agent workspace roots and approved user-owned OS roots",
        display_path(path.resolved_path.as_path())
    ))
}

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
    #[cfg(windows)]
    push_windows_drive_temp_roots(&mut roots);
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

#[cfg(windows)]
fn push_windows_drive_temp_roots(roots: &mut Vec<PathBuf>) {
    let Some(system_drive) = std::env::var_os("SystemDrive") else {
        return;
    };
    for candidate in windows_drive_temp_root_candidates(system_drive.to_string_lossy().as_ref()) {
        push_canonical_root(roots, candidate);
    }
}

#[cfg(windows)]
fn windows_drive_temp_root_candidates(system_drive: &str) -> Vec<PathBuf> {
    let drive = system_drive.trim().trim_end_matches(['\\', '/']);
    let bytes = drive.as_bytes();
    if bytes.len() != 2 || bytes[1] != b':' || !bytes[0].is_ascii_alphabetic() {
        return Vec::new();
    }
    vec![PathBuf::from(format!("{drive}\\var\\tmp"))]
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
    use std::sync::{Mutex, OnceLock};

    static OS_FILE_ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

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
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            match self.previous.as_ref() {
                Some(previous) => std::env::set_var(self.key, previous),
                None => std::env::remove_var(self.key),
            }
        }
    }

    fn test_policy(root: &Path) -> OsFilePolicy {
        OsFilePolicy {
            workspace_roots: vec![fs::canonicalize(root).expect("root should canonicalize")],
            user_os_roots: vec![fs::canonicalize(root).expect("root should canonicalize")],
        }
    }

    #[test]
    fn os_file_write_and_read_absolute_user_path() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
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
    fn os_file_write_rejects_large_shrink_without_full_replace_intent() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let policy = test_policy(tempdir.path());
        let target = tempdir.path().join("public").join("index.html");
        fs::create_dir_all(target.parent().expect("target parent")).expect("parent dir");
        let original = format!("<!doctype html>\n<body>\n{}\n</body>\n", "x".repeat(4096));
        fs::write(target.as_path(), original.as_bytes()).expect("fixture should be written");

        let error = execute_os_file_operation(
            &policy,
            &OsFileInput {
                operation: OsFileOperation::Write,
                path: target.to_string_lossy().into_owned(),
                target_path: None,
                content_text: Some("<span>fragment</span>\n".to_owned()),
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
        .expect_err("large shrink full-file write should require explicit full_replace intent");

        assert!(error.contains("large full-file shrink"), "unexpected error: {error}");
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
                content_text: Some("<span>fragment</span>\n".to_owned()),
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
            "<span>fragment</span>\n"
        );
    }

    #[test]
    fn os_file_move_accepts_workspace_relative_target_path() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
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
    fn os_file_workspace_target_alias_requires_relative_file_path() {
        let error = normalize_workspace_relative_target("/workspace")
            .expect_err("workspace alias alone should not name an import destination");
        assert!(error.contains("must include a file path"));

        let relative = normalize_workspace_relative_target("/workspace/data/file.txt")
            .expect("workspace alias with relative suffix should be accepted");
        assert_eq!(relative, PathBuf::from("data").join("file.txt"));
    }

    #[test]
    fn os_file_read_expands_leading_env_path_prefixes() {
        let _guard =
            OS_FILE_ENV_LOCK.get_or_init(|| Mutex::new(())).lock().expect("env lock poisoned");
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let os_root = tempdir.path().join("os-root");
        let inbox = os_root.join("downloads").join("inbox");
        fs::create_dir_all(inbox.as_path()).expect("inbox should exist");
        let target = inbox.join("orders-valid.csv");
        fs::write(target.as_path(), "id,name,total\n1,Ada,42\n").expect("fixture should exist");
        let policy = OsFilePolicy {
            workspace_roots: vec![fs::canonicalize(tempdir.path()).expect("workspace root")],
            user_os_roots: vec![fs::canonicalize(os_root.as_path()).expect("os root")],
        };
        let _root = ScopedEnvVar::set("PALYRA_TEST_OS_ROOT", os_root.as_path());

        for env_path in [
            "%PALYRA_TEST_OS_ROOT%/downloads/inbox/orders-valid.csv",
            "$PALYRA_TEST_OS_ROOT/downloads/inbox/orders-valid.csv",
            "${PALYRA_TEST_OS_ROOT}/downloads/inbox/orders-valid.csv",
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
        let _guard =
            OS_FILE_ENV_LOCK.get_or_init(|| Mutex::new(())).lock().expect("env lock poisoned");
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let _root = ScopedEnvVar::set("PALYRA_TEST_OS_ROOT", tempdir.path());

        let error = parse_absolute_os_path("%PALYRA_TEST_OS_ROOT%/../escape.txt")
            .expect_err("environment path suffix must not contain parent traversal");
        assert!(error.contains("must stay relative to the expanded root"));
    }

    #[test]
    fn os_file_read_redacts_provider_key_values() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
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
        assert!(text.contains("provider_key = \"[REDACTED_SECRET]\""));
        assert!(!text.contains("palyra_os_secret_abcdef"));
    }

    #[test]
    fn os_file_rejects_path_outside_workspace_and_user_roots() {
        let allowed_root = tempfile::tempdir().expect("allowed root should be created");
        let outside_root = tempfile::tempdir().expect("outside root should be created");
        let workspace = allowed_root.path().join("workspace");
        let outside = outside_root.path().join("outside").join("report.md");
        fs::create_dir_all(workspace.as_path()).expect("workspace should exist");
        fs::create_dir_all(outside.parent().expect("outside parent")).expect("outside parent");
        let policy = OsFilePolicy {
            workspace_roots: vec![fs::canonicalize(workspace).expect("workspace canonical")],
            user_os_roots: vec![
                fs::canonicalize(allowed_root.path()).expect("allowed root canonical")
            ],
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
    fn os_file_configured_roots_replace_implicit_user_profile_root() {
        let _guard =
            OS_FILE_ENV_LOCK.get_or_init(|| Mutex::new(())).lock().expect("env lock poisoned");
        let configured_root = tempfile::tempdir().expect("configured root should be created");
        let real_home_root = tempfile::tempdir().expect("real home root should be created");
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
            "implicit user profile roots must be suppressed when PALYRA_OS_FILE_ROOTS is set: {roots:?}"
        );
    }

    #[test]
    fn os_file_dry_run_does_not_write() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
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
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
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
    #[cfg(windows)]
    fn windows_drive_temp_root_candidates_include_drive_var_tmp() {
        assert_eq!(windows_drive_temp_root_candidates("C:"), vec![PathBuf::from(r"C:\var\tmp")]);
        assert_eq!(windows_drive_temp_root_candidates(r"C:\"), vec![PathBuf::from(r"C:\var\tmp")]);
        assert!(windows_drive_temp_root_candidates("").is_empty());
        assert!(windows_drive_temp_root_candidates(r"\\server\share").is_empty());
    }
}
