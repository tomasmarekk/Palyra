#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(target_os = "macos")]
use std::os::unix::ffi::OsStringExt;
#[cfg(windows)]
use std::os::windows::{ffi::OsStrExt, io::AsRawHandle};
use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palyra_safety::{
    redact_text_for_export, SafetyContentKind, SafetyFindingCategory, SafetySourceKind, TrustLabel,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    agents::AgentResolveRequest,
    application::tool_runtime::workspace_scope::{
        relative_path_should_use_active_root, session_active_workspace_root,
        workspace_root_override_targets_active_root, workspace_roots_with_run_launch_context,
    },
    gateway::{
        GatewayRuntimeState, ToolRuntimeExecutionContext, MAX_WORKSPACE_LIST_DIR_TOOL_INPUT_BYTES,
        MAX_WORKSPACE_READ_FILE_BYTES, MAX_WORKSPACE_READ_FILE_TOOL_INPUT_BYTES,
        MAX_WORKSPACE_SEARCH_TOOL_INPUT_BYTES, WORKSPACE_LIST_DIR_TOOL_NAME,
        WORKSPACE_READ_FILE_TOOL_NAME, WORKSPACE_SEARCH_TOOL_NAME,
    },
    tool_protocol::{build_tool_execution_outcome, ToolExecutionOutcome},
};

const WORKSPACE_LIST_DIR_DEFAULT_ENTRIES: usize = 128;
const WORKSPACE_LIST_DIR_MAX_ENTRIES: usize = 512;
const WORKSPACE_SEARCH_DEFAULT_MATCHES: usize = 64;
const WORKSPACE_SEARCH_MAX_MATCHES: usize = 200;
const WORKSPACE_SEARCH_MAX_FILES: usize = 2_000;
const WORKSPACE_SEARCH_MAX_FILE_BYTES: u64 = 1024 * 1024;
const WORKSPACE_SEARCH_MAX_DIRS: usize = 2_000;
const WORKSPACE_SEARCH_MAX_DEPTH: usize = 32;
const WORKSPACE_SEARCH_MAX_DIR_ENTRIES: usize = 2_000;
const WORKSPACE_SEARCH_MAX_LINE_TEXT_BYTES: usize = 4 * 1024;
const WORKSPACE_SEARCH_MAX_OUTPUT_BYTES: usize = 512 * 1024;
const WORKSPACE_SEARCH_MATCH_JSON_OVERHEAD_BYTES: usize = 160;
const WORKSPACE_SEARCH_SKIPPED_DIRS: &[&str] =
    &[".git", "node_modules", "target", "dist", "build", ".next", ".svelte-kit"];

#[derive(Debug, Deserialize)]
struct WorkspaceReadFileInput {
    path: String,
    #[serde(default)]
    workspace_root: Option<String>,
    #[serde(default)]
    offset_bytes: u64,
    #[serde(default)]
    max_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct WorkspaceListDirInput {
    #[serde(default)]
    path: String,
    #[serde(default)]
    workspace_root: Option<String>,
    #[serde(default)]
    max_entries: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct WorkspaceSearchInput {
    query: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    workspace_root: Option<String>,
    #[serde(default)]
    case_sensitive: Option<bool>,
    #[serde(default)]
    max_matches: Option<u64>,
}

#[derive(Debug, Serialize)]
struct WorkspaceReadFileOutput {
    path: String,
    workspace_root_index: usize,
    offset_bytes: u64,
    returned_bytes: u64,
    size_bytes: u64,
    eof: bool,
    chunk_sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bytes_base64: Option<String>,
    #[serde(skip_serializing_if = "is_false")]
    redacted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    text_authoritative: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    redaction_notice: Option<String>,
}

#[derive(Debug, Serialize)]
struct WorkspaceListDirOutput {
    path: String,
    workspace_root_index: usize,
    entries: Vec<WorkspaceListDirEntry>,
    truncated: bool,
}

#[derive(Debug, Serialize)]
struct WorkspaceSearchOutput {
    query: String,
    path: String,
    workspace_root_index: usize,
    case_sensitive: bool,
    matches: Vec<WorkspaceSearchMatch>,
    truncated: bool,
    files_scanned: usize,
    files_with_matches: usize,
    skipped_files: usize,
    skipped_dirs: usize,
}

#[derive(Debug, Serialize)]
struct WorkspaceSearchMatch {
    path: String,
    line: usize,
    column: usize,
    line_text: String,
    #[serde(skip_serializing_if = "is_false")]
    redacted: bool,
}

#[derive(Debug, Serialize)]
struct WorkspaceListDirEntry {
    name: String,
    path: String,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    size_bytes: Option<u64>,
}

pub(crate) async fn execute_workspace_read_file_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let input = match parse_workspace_read_file_input(input_json) {
        Ok(input) => input,
        Err(error) => {
            return workspace_read_file_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };

    let agent_outcome = match runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: context.principal.to_owned(),
            channel: context.channel.map(str::to_owned),
            session_id: Some(context.session_id.to_owned()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            return workspace_read_file_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!(
                    "{WORKSPACE_READ_FILE_TOOL_NAME} failed to resolve agent workspace: {}",
                    error.message()
                ),
            );
        }
    };

    let agent_workspace_roots =
        agent_outcome.agent.workspace_roots.iter().map(PathBuf::from).collect::<Vec<_>>();
    let agent_workspace_roots = workspace_roots_with_run_launch_context(
        runtime_state,
        context.run_id,
        agent_workspace_roots.as_slice(),
    )
    .await;
    let workspace_roots = resolve_workspace_file_roots(
        runtime_state,
        context.session_id,
        WORKSPACE_READ_FILE_TOOL_NAME,
        agent_workspace_roots.as_slice(),
        input.workspace_root.as_deref(),
        input.path.as_str(),
        true,
    )
    .await;
    let workspace_roots = match workspace_roots {
        Ok(roots) => roots,
        Err(error) => {
            return workspace_read_file_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };
    let read = match read_workspace_file_from_roots(workspace_roots.as_slice(), &input) {
        Ok(read) => read,
        Err(error) => {
            return workspace_read_file_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };

    match serde_json::to_vec(&read) {
        Ok(output_json) => {
            workspace_read_file_outcome(proposal_id, input_json, true, output_json, String::new())
        }
        Err(error) => workspace_read_file_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("{WORKSPACE_READ_FILE_TOOL_NAME} failed to serialize output: {error}"),
        ),
    }
}

pub(crate) async fn execute_workspace_list_dir_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let input = match parse_workspace_list_dir_input(input_json) {
        Ok(input) => input,
        Err(error) => {
            return workspace_list_dir_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };

    let agent_outcome = match runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: context.principal.to_owned(),
            channel: context.channel.map(str::to_owned),
            session_id: Some(context.session_id.to_owned()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            return workspace_list_dir_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!(
                    "{WORKSPACE_LIST_DIR_TOOL_NAME} failed to resolve agent workspace: {}",
                    error.message()
                ),
            );
        }
    };

    let agent_workspace_roots =
        agent_outcome.agent.workspace_roots.iter().map(PathBuf::from).collect::<Vec<_>>();
    let agent_workspace_roots = workspace_roots_with_run_launch_context(
        runtime_state,
        context.run_id,
        agent_workspace_roots.as_slice(),
    )
    .await;
    let workspace_roots = resolve_workspace_file_roots(
        runtime_state,
        context.session_id,
        WORKSPACE_LIST_DIR_TOOL_NAME,
        agent_workspace_roots.as_slice(),
        input.workspace_root.as_deref(),
        input.path.as_str(),
        true,
    )
    .await;
    let workspace_roots = match workspace_roots {
        Ok(roots) => roots,
        Err(error) => {
            return workspace_list_dir_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };
    let listing = match list_workspace_dir_from_roots(workspace_roots.as_slice(), &input) {
        Ok(listing) => listing,
        Err(error) => {
            return workspace_list_dir_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };

    match serde_json::to_vec(&listing) {
        Ok(output_json) => {
            workspace_list_dir_outcome(proposal_id, input_json, true, output_json, String::new())
        }
        Err(error) => workspace_list_dir_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("{WORKSPACE_LIST_DIR_TOOL_NAME} failed to serialize output: {error}"),
        ),
    }
}

pub(crate) async fn execute_workspace_search_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let input = match parse_workspace_search_input(input_json) {
        Ok(input) => input,
        Err(error) => {
            return workspace_search_outcome(proposal_id, input_json, false, b"{}".to_vec(), error);
        }
    };

    let agent_outcome = match runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: context.principal.to_owned(),
            channel: context.channel.map(str::to_owned),
            session_id: Some(context.session_id.to_owned()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            return workspace_search_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!(
                    "{WORKSPACE_SEARCH_TOOL_NAME} failed to resolve agent workspace: {}",
                    error.message()
                ),
            );
        }
    };

    let agent_workspace_roots =
        agent_outcome.agent.workspace_roots.iter().map(PathBuf::from).collect::<Vec<_>>();
    let agent_workspace_roots = workspace_roots_with_run_launch_context(
        runtime_state,
        context.run_id,
        agent_workspace_roots.as_slice(),
    )
    .await;
    let workspace_roots = resolve_workspace_file_roots(
        runtime_state,
        context.session_id,
        WORKSPACE_SEARCH_TOOL_NAME,
        agent_workspace_roots.as_slice(),
        input.workspace_root.as_deref(),
        input.path.as_str(),
        true,
    )
    .await;
    let workspace_roots = match workspace_roots {
        Ok(roots) => roots,
        Err(error) => {
            return workspace_search_outcome(proposal_id, input_json, false, b"{}".to_vec(), error);
        }
    };
    let search = match search_workspace_from_roots(workspace_roots.as_slice(), &input) {
        Ok(search) => search,
        Err(error) => {
            return workspace_search_outcome(proposal_id, input_json, false, b"{}".to_vec(), error);
        }
    };

    match serde_json::to_vec(&search) {
        Ok(output_json) => {
            workspace_search_outcome(proposal_id, input_json, true, output_json, String::new())
        }
        Err(error) => workspace_search_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("{WORKSPACE_SEARCH_TOOL_NAME} failed to serialize output: {error}"),
        ),
    }
}

fn parse_workspace_read_file_input(input_json: &[u8]) -> Result<WorkspaceReadFileInput, String> {
    if input_json.len() > MAX_WORKSPACE_READ_FILE_TOOL_INPUT_BYTES {
        return Err(format!(
            "{WORKSPACE_READ_FILE_TOOL_NAME} input exceeds {MAX_WORKSPACE_READ_FILE_TOOL_INPUT_BYTES} bytes"
        ));
    }

    let mut input =
        serde_json::from_slice::<WorkspaceReadFileInput>(input_json).map_err(|error| {
            format!("{WORKSPACE_READ_FILE_TOOL_NAME} input must match file read schema: {error}")
        })?;
    input.path = input.path.trim().to_owned();
    if input.path.is_empty() {
        return Err(format!(
            "{WORKSPACE_READ_FILE_TOOL_NAME} requires non-empty string field 'path'"
        ));
    }
    if matches!(input.max_bytes, Some(0)) {
        return Err(format!("{WORKSPACE_READ_FILE_TOOL_NAME} max_bytes must be >= 1"));
    }
    input.workspace_root = normalize_optional_workspace_root(input.workspace_root);
    input.path = normalize_workspace_path_input(input.path.as_str());
    validate_workspace_path_syntax(input.path.as_str(), WORKSPACE_READ_FILE_TOOL_NAME)?;
    Ok(input)
}

fn parse_workspace_list_dir_input(input_json: &[u8]) -> Result<WorkspaceListDirInput, String> {
    if input_json.len() > MAX_WORKSPACE_LIST_DIR_TOOL_INPUT_BYTES {
        return Err(format!(
            "{WORKSPACE_LIST_DIR_TOOL_NAME} input exceeds {MAX_WORKSPACE_LIST_DIR_TOOL_INPUT_BYTES} bytes"
        ));
    }

    let mut input =
        serde_json::from_slice::<WorkspaceListDirInput>(input_json).map_err(|error| {
            format!(
                "{WORKSPACE_LIST_DIR_TOOL_NAME} input must match directory listing schema: {error}"
            )
        })?;
    if matches!(input.max_entries, Some(0)) {
        return Err(format!("{WORKSPACE_LIST_DIR_TOOL_NAME} max_entries must be >= 1"));
    }
    input.workspace_root = normalize_optional_workspace_root(input.workspace_root);
    input.path = normalize_workspace_path_input(input.path.as_str());
    validate_workspace_path_syntax(input.path.as_str(), WORKSPACE_LIST_DIR_TOOL_NAME)?;
    Ok(input)
}

fn parse_workspace_search_input(input_json: &[u8]) -> Result<WorkspaceSearchInput, String> {
    if input_json.len() > MAX_WORKSPACE_SEARCH_TOOL_INPUT_BYTES {
        return Err(format!(
            "{WORKSPACE_SEARCH_TOOL_NAME} input exceeds {MAX_WORKSPACE_SEARCH_TOOL_INPUT_BYTES} bytes"
        ));
    }

    let mut input =
        serde_json::from_slice::<WorkspaceSearchInput>(input_json).map_err(|error| {
            format!("{WORKSPACE_SEARCH_TOOL_NAME} input must match search schema: {error}")
        })?;
    input.query = input.query.trim().to_owned();
    if input.query.is_empty() {
        return Err(format!(
            "{WORKSPACE_SEARCH_TOOL_NAME} requires non-empty string field 'query'"
        ));
    }
    if input.query.len() > 512 {
        return Err(format!("{WORKSPACE_SEARCH_TOOL_NAME} query exceeds 512 bytes"));
    }
    if matches!(input.max_matches, Some(0)) {
        return Err(format!("{WORKSPACE_SEARCH_TOOL_NAME} max_matches must be >= 1"));
    }
    input.workspace_root = normalize_optional_workspace_root(input.workspace_root);
    input.path = normalize_workspace_path_input(input.path.as_str());
    validate_workspace_path_syntax(input.path.as_str(), WORKSPACE_SEARCH_TOOL_NAME)?;
    Ok(input)
}

fn normalize_optional_workspace_root(workspace_root: Option<String>) -> Option<String> {
    workspace_root.map(|value| value.trim().to_owned()).filter(|value| !value.is_empty())
}

fn normalize_workspace_path_input(path: &str) -> String {
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

fn validate_workspace_path_syntax(path: &str, tool_name: &str) -> Result<(), String> {
    if path.chars().any(char::is_control) {
        return Err(format!("{tool_name} path contains unsupported characters"));
    }
    if path.contains(':') && !looks_like_windows_drive_path(path) {
        return Err(format!("{tool_name} path contains unsupported characters"));
    }
    if path.is_empty() {
        return Ok(());
    }

    let parsed = Path::new(path);
    if parsed.is_absolute() {
        return Ok(());
    }
    if !parsed.components().all(|component| matches!(component, Component::Normal(_))) {
        return Err(format!(
            "{tool_name} path must not contain root, prefix, '.', or '..' components"
        ));
    }
    Ok(())
}

async fn resolve_workspace_file_roots(
    runtime_state: &Arc<GatewayRuntimeState>,
    session_id: &str,
    tool_name: &str,
    agent_workspace_roots: &[PathBuf],
    workspace_root: Option<&str>,
    requested_path: &str,
    use_active_session_root: bool,
) -> Result<Vec<PathBuf>, String> {
    if let Some(workspace_root) = workspace_root {
        let workspace_root = workspace_root.trim();
        if !workspace_root.is_empty() {
            if let Some(active_root) =
                session_active_workspace_root(runtime_state, session_id, agent_workspace_roots)
                    .await?
            {
                if workspace_root_override_targets_active_root(workspace_root, &active_root) {
                    return Ok(vec![active_root.root]);
                }
            }
            return resolve_workspace_root_override(
                tool_name,
                agent_workspace_roots,
                workspace_root,
            )
            .map(|root| vec![root]);
        }
    }
    if use_active_session_root {
        if let Some(active_root) =
            session_active_workspace_root(runtime_state, session_id, agent_workspace_roots).await?
        {
            if requested_path.is_empty()
                || relative_path_should_use_active_root(requested_path, &active_root)
            {
                return Ok(workspace_roots_with_active_first(
                    active_root.root,
                    agent_workspace_roots,
                ));
            }
        }
    }
    Ok(agent_workspace_roots.to_vec())
}

fn workspace_roots_with_active_first(
    active_root: PathBuf,
    workspace_roots: &[PathBuf],
) -> Vec<PathBuf> {
    let mut roots = Vec::with_capacity(workspace_roots.len().saturating_add(1));
    roots.push(active_root);
    for root in workspace_roots {
        if roots.iter().any(|existing| same_workspace_file_root(existing, root)) {
            continue;
        }
        roots.push(root.clone());
    }
    roots
}

fn same_workspace_file_root(left: &Path, right: &Path) -> bool {
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

#[cfg(test)]
fn resolve_workspace_file_roots_for_override(
    tool_name: &str,
    agent_workspace_roots: &[PathBuf],
    workspace_root: Option<&str>,
) -> Result<Vec<PathBuf>, String> {
    let Some(workspace_root) = workspace_root else {
        return Ok(agent_workspace_roots.to_vec());
    };
    let workspace_root = workspace_root.trim();
    if workspace_root.is_empty() {
        return Ok(agent_workspace_roots.to_vec());
    }
    resolve_workspace_root_override(tool_name, agent_workspace_roots, workspace_root)
        .map(|root| vec![root])
}

fn resolve_workspace_root_override(
    tool_name: &str,
    agent_workspace_roots: &[PathBuf],
    workspace_root: &str,
) -> Result<PathBuf, String> {
    if workspace_root.chars().any(char::is_control) {
        return Err(format!("{tool_name} workspace_root contains unsupported characters"));
    }

    let canonical_roots = canonicalize_workspace_roots(agent_workspace_roots, tool_name)?;
    if canonical_roots.is_empty() {
        return Err(format!("{tool_name} agent has no accessible workspace roots"));
    }

    let requested = Path::new(workspace_root);
    if requested.is_absolute() {
        return canonicalize_workspace_root_override(
            tool_name,
            requested,
            &canonical_roots,
            workspace_root,
        );
    }
    validate_relative_workspace_root_override(tool_name, requested, workspace_root)?;
    if let Some(root) = workspace_root_override_matching_existing_root_basename(
        requested,
        canonical_roots.as_slice(),
    ) {
        return Ok(root);
    }
    for (_, canonical_root) in &canonical_roots {
        let candidate = canonical_root.join(requested);
        match canonicalize_workspace_root_override(
            tool_name,
            candidate.as_path(),
            &canonical_roots,
            workspace_root,
        ) {
            Ok(path) => return Ok(path),
            Err(error) if error.contains("does not exist") => {}
            Err(error) => return Err(error),
        }
    }
    Err(format!(
        "{tool_name} workspace_root does not exist inside agent workspace roots: {workspace_root}"
    ))
}

fn workspace_root_override_matching_existing_root_basename(
    requested: &Path,
    canonical_roots: &[(usize, PathBuf)],
) -> Option<PathBuf> {
    let mut components = requested.components();
    let Some(Component::Normal(component)) = components.next() else {
        return None;
    };
    if components.next().is_some() {
        return None;
    }
    canonical_roots
        .iter()
        .find(|(_, root)| {
            root.file_name().is_some_and(|basename| path_component_eq(basename, component))
        })
        .map(|(_, root)| root.clone())
}

fn path_component_eq(left: &std::ffi::OsStr, right: &std::ffi::OsStr) -> bool {
    #[cfg(windows)]
    {
        left.to_string_lossy().eq_ignore_ascii_case(&right.to_string_lossy())
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

fn relative_workspace_path_candidates(path: &str, canonical_root: &Path) -> Vec<String> {
    let mut candidates = Vec::with_capacity(2);
    if let Some(stripped) = strip_duplicate_workspace_root_basename(path, canonical_root) {
        candidates.push(stripped);
    }
    if !candidates.iter().any(|candidate| candidate == path) {
        candidates.push(path.to_owned());
    }
    candidates
}

fn strip_duplicate_workspace_root_basename(path: &str, canonical_root: &Path) -> Option<String> {
    if path.is_empty() || Path::new(path).is_absolute() {
        return None;
    }
    let root_basename = canonical_root.file_name()?;
    let mut components = Path::new(path).components();
    let Some(Component::Normal(first)) = components.next() else {
        return None;
    };
    if !path_component_eq(root_basename, first) {
        return None;
    }
    let mut stripped = Vec::new();
    for component in components {
        match component {
            Component::Normal(value) => stripped.push(value.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(stripped.join("/"))
}

fn canonicalize_workspace_root_override(
    tool_name: &str,
    candidate: &Path,
    canonical_roots: &[(usize, PathBuf)],
    workspace_root: &str,
) -> Result<PathBuf, String> {
    let canonical_candidate = fs::canonicalize(candidate).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            format!(
                "{tool_name} workspace_root does not exist inside agent workspace roots: {workspace_root}"
            )
        } else {
            format!("{tool_name} failed to resolve workspace_root {workspace_root}: {error}")
        }
    })?;
    if !canonical_candidate.is_dir() {
        return Err(format!("{tool_name} workspace_root is not a directory: {workspace_root}"));
    }
    if canonical_roots.iter().any(|(_, root)| canonical_candidate.starts_with(root)) {
        return Ok(canonical_candidate);
    }
    Err(format!("{tool_name} workspace_root escapes agent workspace roots: {workspace_root}"))
}

fn validate_relative_workspace_root_override(
    tool_name: &str,
    path: &Path,
    raw_workspace_root: &str,
) -> Result<(), String> {
    for component in path.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(format!(
                    "{tool_name} workspace_root must stay inside agent workspace roots: {raw_workspace_root}"
                ));
            }
        }
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

fn read_workspace_file_from_roots(
    workspace_roots: &[PathBuf],
    input: &WorkspaceReadFileInput,
) -> Result<WorkspaceReadFileOutput, String> {
    let canonical_roots =
        canonicalize_workspace_roots(workspace_roots, WORKSPACE_READ_FILE_TOOL_NAME)?;
    if canonical_roots.is_empty() {
        return Err(format!(
            "{WORKSPACE_READ_FILE_TOOL_NAME} agent has no accessible workspace roots"
        ));
    }

    let requested = Path::new(input.path.as_str());
    if requested.is_absolute() {
        let (workspace_root_index, canonical_target, display_path) =
            resolve_absolute_workspace_file(canonical_roots.as_slice(), requested, input)?;
        let canonical_root = canonical_roots
            .iter()
            .find_map(|(index, root)| {
                (*index == workspace_root_index).then_some(root.as_path())
            })
            .ok_or_else(|| {
                format!(
                    "{WORKSPACE_READ_FILE_TOOL_NAME} internal error resolving workspace root {workspace_root_index}"
                )
            })?;
        return read_workspace_file_chunk(
            workspace_root_index,
            canonical_root,
            canonical_target,
            display_path,
            input,
        );
    }

    for (workspace_root_index, canonical_root) in &canonical_roots {
        for relative_path in relative_workspace_path_candidates(input.path.as_str(), canonical_root)
        {
            let candidate = if relative_path.is_empty() {
                canonical_root.clone()
            } else {
                canonical_root.join(Path::new(relative_path.as_str()))
            };
            let canonical_target = match fs::canonicalize(&candidate) {
                Ok(path) => path,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(format!(
                        "{WORKSPACE_READ_FILE_TOOL_NAME} failed to resolve path in workspace root {workspace_root_index}: {error}"
                    ));
                }
            };
            if !canonical_target.starts_with(canonical_root.as_path()) {
                return Err(format!(
                    "{WORKSPACE_READ_FILE_TOOL_NAME} path escapes agent workspace roots"
                ));
            }
            if !canonical_target.is_file() {
                return Err(read_file_not_regular_file_error(input.path.as_str()));
            }

            let display_path = canonical_target
                .strip_prefix(canonical_root)
                .map(normalize_relative_path_display)
                .unwrap_or(relative_path);
            return read_workspace_file_chunk(
                *workspace_root_index,
                canonical_root.as_path(),
                canonical_target,
                display_path,
                input,
            );
        }
    }

    Err(format!(
        "{WORKSPACE_READ_FILE_TOOL_NAME} file not found in agent workspace roots: {}",
        display_requested_path(input.path.as_str())
    ))
}

fn canonicalize_workspace_roots(
    workspace_roots: &[PathBuf],
    tool_name: &str,
) -> Result<Vec<(usize, PathBuf)>, String> {
    let mut canonical_roots = Vec::with_capacity(workspace_roots.len());
    for (workspace_root_index, workspace_root) in workspace_roots.iter().enumerate() {
        match fs::canonicalize(workspace_root) {
            Ok(path) if path.is_dir() => canonical_roots.push((workspace_root_index, path)),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "{tool_name} failed to resolve workspace root {workspace_root_index}: {error}"
                ));
            }
        }
    }
    Ok(canonical_roots)
}

fn resolve_absolute_workspace_file(
    canonical_roots: &[(usize, PathBuf)],
    requested: &Path,
    input: &WorkspaceReadFileInput,
) -> Result<(usize, PathBuf, String), String> {
    if requested.components().any(|component| matches!(component, Component::ParentDir)) {
        return Err(format!("{WORKSPACE_READ_FILE_TOOL_NAME} path escapes agent workspace roots"));
    }
    let (workspace_root_index, canonical_root) =
        find_lexical_workspace_root(canonical_roots, requested).ok_or_else(|| {
            format!("{WORKSPACE_READ_FILE_TOOL_NAME} path escapes agent workspace roots")
        })?;
    let canonical_target = fs::canonicalize(requested).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            format!(
                "{WORKSPACE_READ_FILE_TOOL_NAME} file not found in agent workspace roots: {}",
                display_requested_path(input.path.as_str())
            )
        } else {
            format!("{WORKSPACE_READ_FILE_TOOL_NAME} failed to resolve path: {error}")
        }
    })?;
    if !path_stays_inside_workspace_root(canonical_target.as_path(), canonical_root) {
        return Err(format!("{WORKSPACE_READ_FILE_TOOL_NAME} path escapes agent workspace roots"));
    }
    if !canonical_target.is_file() {
        return Err(read_file_not_regular_file_error(input.path.as_str()));
    }
    let display_path = canonical_target
        .strip_prefix(canonical_root)
        .map(normalize_relative_path_display)
        .unwrap_or_else(|_| display_requested_path(input.path.as_str()).to_owned());
    Ok((workspace_root_index, canonical_target, display_path))
}

fn find_lexical_workspace_root<'a>(
    canonical_roots: &'a [(usize, PathBuf)],
    requested: &Path,
) -> Option<(usize, &'a Path)> {
    canonical_roots
        .iter()
        .find(|(_, canonical_root)| {
            path_stays_inside_workspace_root(requested, canonical_root.as_path())
        })
        .map(|(index, canonical_root)| (*index, canonical_root.as_path()))
}

fn read_file_not_regular_file_error(path: &str) -> String {
    format!(
        "{WORKSPACE_READ_FILE_TOOL_NAME} target is not a regular file: {}; use {WORKSPACE_LIST_DIR_TOOL_NAME} to inspect workspace directories",
        display_requested_path(path)
    )
}

fn list_workspace_dir_from_roots(
    workspace_roots: &[PathBuf],
    input: &WorkspaceListDirInput,
) -> Result<WorkspaceListDirOutput, String> {
    let canonical_roots =
        canonicalize_workspace_roots(workspace_roots, WORKSPACE_LIST_DIR_TOOL_NAME)?;
    if canonical_roots.is_empty() {
        return Err(format!(
            "{WORKSPACE_LIST_DIR_TOOL_NAME} agent has no accessible workspace roots"
        ));
    }

    let requested = Path::new(input.path.as_str());
    if requested.is_absolute() {
        let (workspace_root_index, canonical_target, display_path) =
            resolve_absolute_workspace_dir(canonical_roots.as_slice(), requested, input)?;
        return list_workspace_directory(
            workspace_root_index,
            canonical_roots
                .iter()
                .find_map(|(index, root)| {
                    (*index == workspace_root_index).then_some(root.as_path())
                })
                .ok_or_else(|| {
                    format!(
                        "{WORKSPACE_LIST_DIR_TOOL_NAME} failed to resolve workspace root for directory listing"
                    )
                })?,
            canonical_target,
            display_path,
            input,
        );
    }

    for (workspace_root_index, canonical_root) in &canonical_roots {
        for relative_path in relative_workspace_path_candidates(input.path.as_str(), canonical_root)
        {
            let candidate = if relative_path.is_empty() {
                canonical_root.clone()
            } else {
                canonical_root.join(Path::new(relative_path.as_str()))
            };
            let canonical_target = match fs::canonicalize(&candidate) {
                Ok(path) => path,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(format!(
                        "{WORKSPACE_LIST_DIR_TOOL_NAME} failed to resolve path in workspace root {workspace_root_index}: {error}"
                    ));
                }
            };
            if !canonical_target.starts_with(canonical_root.as_path()) {
                return Err(format!(
                    "{WORKSPACE_LIST_DIR_TOOL_NAME} path escapes agent workspace roots"
                ));
            }
            if !canonical_target.is_dir() {
                return Err(format!(
                    "{WORKSPACE_LIST_DIR_TOOL_NAME} target is not a directory: {}",
                    display_requested_path(input.path.as_str())
                ));
            }

            let display_path = canonical_target
                .strip_prefix(canonical_root)
                .map(normalize_relative_path_display)
                .unwrap_or(relative_path);
            return list_workspace_directory(
                *workspace_root_index,
                canonical_root.as_path(),
                canonical_target,
                display_path,
                input,
            );
        }
    }

    Err(format!(
        "{WORKSPACE_LIST_DIR_TOOL_NAME} directory not found in agent workspace roots: {}",
        display_requested_path(input.path.as_str())
    ))
}

fn resolve_absolute_workspace_dir(
    canonical_roots: &[(usize, PathBuf)],
    requested: &Path,
    input: &WorkspaceListDirInput,
) -> Result<(usize, PathBuf, String), String> {
    let canonical_target = fs::canonicalize(requested).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            format!(
                "{WORKSPACE_LIST_DIR_TOOL_NAME} directory not found in agent workspace roots: {}",
                display_requested_path(input.path.as_str())
            )
        } else {
            format!("{WORKSPACE_LIST_DIR_TOOL_NAME} failed to resolve path: {error}")
        }
    })?;
    for (workspace_root_index, canonical_root) in canonical_roots {
        if canonical_target.starts_with(canonical_root) {
            if !canonical_target.is_dir() {
                return Err(format!(
                    "{WORKSPACE_LIST_DIR_TOOL_NAME} target is not a directory: {}",
                    display_requested_path(input.path.as_str())
                ));
            }
            let display_path = canonical_target
                .strip_prefix(canonical_root)
                .map(normalize_relative_path_display)
                .unwrap_or_else(|_| display_requested_path(input.path.as_str()).to_owned());
            return Ok((*workspace_root_index, canonical_target, display_path));
        }
    }
    Err(format!("{WORKSPACE_LIST_DIR_TOOL_NAME} path escapes agent workspace roots"))
}

fn list_workspace_directory(
    workspace_root_index: usize,
    canonical_root: &Path,
    path: PathBuf,
    display_path: String,
    input: &WorkspaceListDirInput,
) -> Result<WorkspaceListDirOutput, String> {
    let max_entries = input
        .max_entries
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(WORKSPACE_LIST_DIR_DEFAULT_ENTRIES)
        .min(WORKSPACE_LIST_DIR_MAX_ENTRIES);
    let mut entries = Vec::new();
    for entry_result in fs::read_dir(path.as_path()).map_err(|error| {
        format!(
            "{WORKSPACE_LIST_DIR_TOOL_NAME} failed to read workspace directory {}: {error}",
            display_requested_path(input.path.as_str())
        )
    })? {
        let entry = entry_result.map_err(|error| {
            format!(
                "{WORKSPACE_LIST_DIR_TOOL_NAME} failed to read directory entry for {}: {error}",
                display_requested_path(input.path.as_str())
            )
        })?;
        let file_type = entry.file_type().map_err(|error| {
            format!(
                "{WORKSPACE_LIST_DIR_TOOL_NAME} failed to inspect directory entry for {}: {error}",
                display_requested_path(input.path.as_str())
            )
        })?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let raw_entry_path = entry.path();
        let path = raw_entry_path
            .strip_prefix(canonical_root)
            .map(normalize_relative_path_display)
            .unwrap_or_else(|_| {
                if display_path == "." {
                    name.clone()
                } else {
                    format!("{display_path}/{name}")
                }
            });
        let size_bytes = if file_type.is_file() {
            entry.metadata().ok().map(|metadata| metadata.len())
        } else {
            None
        };
        let kind = if file_type.is_dir() {
            "directory"
        } else if file_type.is_file() {
            "file"
        } else if file_type.is_symlink() {
            "symlink"
        } else {
            "other"
        };
        entries.push(WorkspaceListDirEntry { name, path, kind: kind.to_owned(), size_bytes });
    }
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    let truncated = entries.len() > max_entries;
    entries.truncate(max_entries);

    Ok(WorkspaceListDirOutput { path: display_path, workspace_root_index, entries, truncated })
}

fn search_workspace_from_roots(
    workspace_roots: &[PathBuf],
    input: &WorkspaceSearchInput,
) -> Result<WorkspaceSearchOutput, String> {
    let canonical_roots =
        canonicalize_workspace_roots(workspace_roots, WORKSPACE_SEARCH_TOOL_NAME)?;
    if canonical_roots.is_empty() {
        return Err(format!(
            "{WORKSPACE_SEARCH_TOOL_NAME} agent has no accessible workspace roots"
        ));
    }

    let requested = Path::new(input.path.as_str());
    if requested.is_absolute() {
        let (workspace_root_index, canonical_target, display_path) =
            resolve_absolute_workspace_search_path(canonical_roots.as_slice(), requested, input)?;
        let canonical_root = canonical_roots
            .iter()
            .find_map(|(index, root)| (*index == workspace_root_index).then_some(root.as_path()))
            .ok_or_else(|| {
                format!("{WORKSPACE_SEARCH_TOOL_NAME} failed to resolve workspace root for search")
            })?;
        return search_workspace_path(
            workspace_root_index,
            canonical_root,
            canonical_target,
            display_path,
            input,
        );
    }

    for (workspace_root_index, canonical_root) in &canonical_roots {
        for relative_path in relative_workspace_path_candidates(input.path.as_str(), canonical_root)
        {
            let candidate = if relative_path.is_empty() {
                canonical_root.clone()
            } else {
                canonical_root.join(Path::new(relative_path.as_str()))
            };
            let canonical_target = match fs::canonicalize(&candidate) {
                Ok(path) => path,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(format!(
                        "{WORKSPACE_SEARCH_TOOL_NAME} failed to resolve path in workspace root {workspace_root_index}: {error}"
                    ));
                }
            };
            if !canonical_target.starts_with(canonical_root.as_path()) {
                return Err(format!(
                    "{WORKSPACE_SEARCH_TOOL_NAME} path escapes agent workspace roots"
                ));
            }
            if !canonical_target.is_file() && !canonical_target.is_dir() {
                return Err(format!(
                    "{WORKSPACE_SEARCH_TOOL_NAME} target is not a file or directory: {}",
                    display_requested_path(input.path.as_str())
                ));
            }

            let display_path = canonical_target
                .strip_prefix(canonical_root)
                .map(normalize_relative_path_display)
                .unwrap_or(relative_path);
            return search_workspace_path(
                *workspace_root_index,
                canonical_root.as_path(),
                canonical_target,
                display_path,
                input,
            );
        }
    }

    Err(format!(
        "{WORKSPACE_SEARCH_TOOL_NAME} path not found in agent workspace roots: {}",
        display_requested_path(input.path.as_str())
    ))
}

fn resolve_absolute_workspace_search_path(
    canonical_roots: &[(usize, PathBuf)],
    requested: &Path,
    input: &WorkspaceSearchInput,
) -> Result<(usize, PathBuf, String), String> {
    let canonical_target = fs::canonicalize(requested).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            format!(
                "{WORKSPACE_SEARCH_TOOL_NAME} path not found in agent workspace roots: {}",
                input.path
            )
        } else {
            format!("{WORKSPACE_SEARCH_TOOL_NAME} failed to resolve path: {error}")
        }
    })?;
    for (workspace_root_index, canonical_root) in canonical_roots {
        if canonical_target.starts_with(canonical_root) {
            if !canonical_target.is_file() && !canonical_target.is_dir() {
                return Err(format!(
                    "{WORKSPACE_SEARCH_TOOL_NAME} target is not a file or directory: {}",
                    display_requested_path(input.path.as_str())
                ));
            }
            let display_path = canonical_target
                .strip_prefix(canonical_root)
                .map(normalize_relative_path_display)
                .unwrap_or_else(|_| input.path.clone());
            return Ok((*workspace_root_index, canonical_target, display_path));
        }
    }
    Err(format!("{WORKSPACE_SEARCH_TOOL_NAME} path escapes agent workspace roots"))
}

fn search_workspace_path(
    workspace_root_index: usize,
    canonical_root: &Path,
    path: PathBuf,
    display_path: String,
    input: &WorkspaceSearchInput,
) -> Result<WorkspaceSearchOutput, String> {
    let max_matches = input
        .max_matches
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(WORKSPACE_SEARCH_DEFAULT_MATCHES)
        .min(WORKSPACE_SEARCH_MAX_MATCHES);
    let case_sensitive = input.case_sensitive.unwrap_or(true);
    let mut state = WorkspaceSearchState::new(input.query.as_str(), case_sensitive, max_matches);
    search_workspace_path_recursive(canonical_root, path.as_path(), &mut state, 0)?;

    Ok(WorkspaceSearchOutput {
        query: input.query.clone(),
        path: display_path,
        workspace_root_index,
        case_sensitive,
        matches: state.matches,
        truncated: state.truncated,
        files_scanned: state.files_scanned,
        files_with_matches: state.files_with_matches,
        skipped_files: state.skipped_files,
        skipped_dirs: state.skipped_dirs,
    })
}

struct WorkspaceSearchState {
    query: String,
    normalized_query: String,
    case_sensitive: bool,
    max_matches: usize,
    matches: Vec<WorkspaceSearchMatch>,
    truncated: bool,
    files_scanned: usize,
    files_with_matches: usize,
    skipped_files: usize,
    skipped_dirs: usize,
    dirs_visited: usize,
    estimated_output_bytes: usize,
}

impl WorkspaceSearchState {
    fn new(query: &str, case_sensitive: bool, max_matches: usize) -> Self {
        Self {
            query: query.to_owned(),
            normalized_query: if case_sensitive {
                query.to_owned()
            } else {
                query.to_ascii_lowercase()
            },
            case_sensitive,
            max_matches,
            matches: Vec::new(),
            truncated: false,
            files_scanned: 0,
            files_with_matches: 0,
            skipped_files: 0,
            skipped_dirs: 0,
            dirs_visited: 0,
            estimated_output_bytes: 256,
        }
    }

    fn has_capacity(&self) -> bool {
        self.matches.len() < self.max_matches
            && self.files_scanned < WORKSPACE_SEARCH_MAX_FILES
            && self.estimated_output_bytes < WORKSPACE_SEARCH_MAX_OUTPUT_BYTES
    }

    fn has_directory_capacity(&self) -> bool {
        self.dirs_visited < WORKSPACE_SEARCH_MAX_DIRS
    }

    fn visit_directory(&mut self) -> bool {
        if !self.has_directory_capacity() {
            self.truncated = true;
            self.skipped_dirs = self.skipped_dirs.saturating_add(1);
            return false;
        }
        self.dirs_visited = self.dirs_visited.saturating_add(1);
        true
    }

    fn reserve_match_output(&mut self, path: &str, line_text: &str) -> bool {
        let estimated = path
            .len()
            .saturating_mul(2)
            .saturating_add(line_text.len().saturating_mul(2))
            .saturating_add(WORKSPACE_SEARCH_MATCH_JSON_OVERHEAD_BYTES);
        let next = self.estimated_output_bytes.saturating_add(estimated);
        if next > WORKSPACE_SEARCH_MAX_OUTPUT_BYTES {
            self.truncated = true;
            return false;
        }
        self.estimated_output_bytes = next;
        true
    }
}

fn search_workspace_path_recursive(
    canonical_root: &Path,
    path: &Path,
    state: &mut WorkspaceSearchState,
    depth: usize,
) -> Result<(), String> {
    if !state.has_capacity() {
        state.truncated = true;
        return Ok(());
    }
    let metadata = fs::metadata(path).map_err(|error| {
        format!(
            "{WORKSPACE_SEARCH_TOOL_NAME} failed to inspect workspace path {}: {error}",
            path.to_string_lossy()
        )
    })?;
    if metadata.is_dir() {
        if depth >= WORKSPACE_SEARCH_MAX_DEPTH {
            state.truncated = true;
            state.skipped_dirs = state.skipped_dirs.saturating_add(1);
            return Ok(());
        }
        search_workspace_directory_recursive(canonical_root, path, state, depth)?;
    } else if metadata.is_file() {
        search_workspace_file(canonical_root, path, metadata.len(), state)?;
    } else {
        state.skipped_files = state.skipped_files.saturating_add(1);
    }
    Ok(())
}

fn search_workspace_directory_recursive(
    canonical_root: &Path,
    path: &Path,
    state: &mut WorkspaceSearchState,
    depth: usize,
) -> Result<(), String> {
    if !state.visit_directory() {
        return Ok(());
    }
    let mut entries = Vec::new();
    for entry_result in fs::read_dir(path).map_err(|error| {
        format!(
            "{WORKSPACE_SEARCH_TOOL_NAME} failed to read workspace directory {}: {error}",
            path.to_string_lossy()
        )
    })? {
        if entries.len() >= WORKSPACE_SEARCH_MAX_DIR_ENTRIES {
            state.truncated = true;
            break;
        }
        let entry = entry_result.map_err(|error| {
            format!(
                "{WORKSPACE_SEARCH_TOOL_NAME} failed to read directory entry for {}: {error}",
                path.to_string_lossy()
            )
        })?;
        entries.push(entry);
    }
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        if !state.has_capacity() {
            state.truncated = true;
            break;
        }
        let file_type = entry.file_type().map_err(|error| {
            format!(
                "{WORKSPACE_SEARCH_TOOL_NAME} failed to inspect directory entry for {}: {error}",
                path.to_string_lossy()
            )
        })?;
        if file_type.is_dir()
            && should_skip_search_dir(entry.file_name().to_string_lossy().as_ref())
        {
            state.skipped_dirs = state.skipped_dirs.saturating_add(1);
            continue;
        }
        if file_type.is_symlink() {
            if file_type.is_dir() {
                state.skipped_dirs = state.skipped_dirs.saturating_add(1);
            } else {
                state.skipped_files = state.skipped_files.saturating_add(1);
            }
            continue;
        }
        search_workspace_path_recursive(canonical_root, entry.path().as_path(), state, depth + 1)?;
    }
    Ok(())
}

fn should_skip_search_dir(name: &str) -> bool {
    WORKSPACE_SEARCH_SKIPPED_DIRS.iter().any(|candidate| candidate == &name)
}

fn search_workspace_file(
    canonical_root: &Path,
    path: &Path,
    size_bytes: u64,
    state: &mut WorkspaceSearchState,
) -> Result<(), String> {
    if state.files_scanned >= WORKSPACE_SEARCH_MAX_FILES {
        state.truncated = true;
        return Ok(());
    }
    state.files_scanned = state.files_scanned.saturating_add(1);
    if size_bytes > WORKSPACE_SEARCH_MAX_FILE_BYTES {
        state.skipped_files = state.skipped_files.saturating_add(1);
        return Ok(());
    }
    let bytes = fs::read(path).map_err(|error| {
        format!(
            "{WORKSPACE_SEARCH_TOOL_NAME} failed to read workspace file {}: {error}",
            path.to_string_lossy()
        )
    })?;
    let Ok(text) = String::from_utf8(bytes) else {
        state.skipped_files = state.skipped_files.saturating_add(1);
        return Ok(());
    };
    let display_path = path
        .strip_prefix(canonical_root)
        .map(normalize_relative_path_display)
        .unwrap_or_else(|_| path.to_string_lossy().into_owned());
    let before = state.matches.len();
    for (line_index, line) in text.lines().enumerate() {
        search_workspace_line(display_path.as_str(), line_index + 1, line, state);
        if state.matches.len() >= state.max_matches {
            state.truncated = true;
            break;
        }
    }
    if state.matches.len() > before {
        state.files_with_matches = state.files_with_matches.saturating_add(1);
    }
    Ok(())
}

fn search_workspace_line(
    path: &str,
    line_number: usize,
    line: &str,
    state: &mut WorkspaceSearchState,
) {
    let haystack = if state.case_sensitive { line.to_owned() } else { line.to_ascii_lowercase() };
    let needle = state.normalized_query.clone();
    let query_len = state.query.len().max(1);
    let mut search_start = 0usize;
    while let Some(relative_index) = haystack[search_start..].find(needle.as_str()) {
        let byte_index = search_start + relative_index;
        let column = line[..byte_index].chars().count() + 1;
        let excerpt = workspace_search_line_excerpt(line, byte_index, query_len);
        let (line_text, redacted) = redact_workspace_search_line(excerpt.as_str());
        if !state.reserve_match_output(path, line_text.as_str()) {
            return;
        }
        state.matches.push(WorkspaceSearchMatch {
            path: path.to_owned(),
            line: line_number,
            column,
            line_text,
            redacted,
        });
        if state.matches.len() >= state.max_matches {
            return;
        }
        search_start = byte_index.saturating_add(query_len);
        if search_start >= haystack.len() {
            return;
        }
    }
}

fn workspace_search_line_excerpt(line: &str, match_start: usize, match_len: usize) -> String {
    if line.len() <= WORKSPACE_SEARCH_MAX_LINE_TEXT_BYTES {
        return line.to_owned();
    }
    let match_end = match_start.saturating_add(match_len).min(line.len());
    let mut start = match_start.saturating_sub(WORKSPACE_SEARCH_MAX_LINE_TEXT_BYTES / 2);
    if match_end > start.saturating_add(WORKSPACE_SEARCH_MAX_LINE_TEXT_BYTES) {
        start = match_end.saturating_sub(WORKSPACE_SEARCH_MAX_LINE_TEXT_BYTES);
    }
    if line.len().saturating_sub(start) < WORKSPACE_SEARCH_MAX_LINE_TEXT_BYTES {
        start = line.len().saturating_sub(WORKSPACE_SEARCH_MAX_LINE_TEXT_BYTES);
    }
    start = floor_char_boundary(line, start);
    let mut end = start.saturating_add(WORKSPACE_SEARCH_MAX_LINE_TEXT_BYTES).min(line.len());
    if end < match_end {
        end = match_end;
    }
    end = floor_char_boundary(line, end);
    if end <= start {
        return String::new();
    }
    let mut excerpt = String::new();
    if start > 0 {
        excerpt.push_str("...");
    }
    excerpt.push_str(&line[start..end]);
    if end < line.len() {
        excerpt.push_str("...");
    }
    excerpt
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while index > 0 && !value.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn redact_workspace_search_line(line: &str) -> (String, bool) {
    let redaction = redact_text_for_export(
        line,
        SafetySourceKind::Workspace,
        SafetyContentKind::WorkspaceDocument,
        TrustLabel::TrustedLocal,
    );
    let redacted = redaction.scan.has_category(SafetyFindingCategory::SecretLeak);
    if redacted {
        (redaction.redacted_text, true)
    } else {
        (line.to_owned(), false)
    }
}

fn read_workspace_file_chunk(
    workspace_root_index: usize,
    canonical_root: &Path,
    path: PathBuf,
    display_path: String,
    input: &WorkspaceReadFileInput,
) -> Result<WorkspaceReadFileOutput, String> {
    let mut file = File::open(path.as_path()).map_err(|error| {
        format!(
            "{WORKSPACE_READ_FILE_TOOL_NAME} failed to open workspace file {}: {error}",
            input.path
        )
    })?;
    let opened_path = canonicalize_open_file_path(&file, input.path.as_str())?;
    if !path_stays_inside_workspace_root(opened_path.as_path(), canonical_root) {
        return Err(format!("{WORKSPACE_READ_FILE_TOOL_NAME} path escapes agent workspace roots"));
    }
    let size_bytes = file
        .metadata()
        .map_err(|error| {
            format!(
                "{WORKSPACE_READ_FILE_TOOL_NAME} failed to inspect workspace file {}: {error}",
                input.path
            )
        })?
        .len();
    file.seek(SeekFrom::Start(input.offset_bytes)).map_err(|error| {
        format!(
            "{WORKSPACE_READ_FILE_TOOL_NAME} failed to seek workspace file {}: {error}",
            input.path
        )
    })?;

    let max_bytes = input.max_bytes.unwrap_or(MAX_WORKSPACE_READ_FILE_BYTES);
    let read_limit = usize::try_from(max_bytes.min(MAX_WORKSPACE_READ_FILE_BYTES))
        .expect("workspace read cap must fit usize");
    let mut buffer = Vec::with_capacity(read_limit.min(8192));
    file.take(read_limit as u64).read_to_end(&mut buffer).map_err(|error| {
        format!(
            "{WORKSPACE_READ_FILE_TOOL_NAME} failed to read workspace file {}: {error}",
            input.path
        )
    })?;

    let returned_bytes =
        u64::try_from(buffer.len()).expect("returned workspace file chunk size must fit u64");
    let eof = input.offset_bytes.saturating_add(returned_bytes) >= size_bytes;
    let chunk_sha256 = hex::encode(Sha256::digest(buffer.as_slice()));
    let (text, bytes_base64, redacted) = match String::from_utf8(buffer) {
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
    };
    let text_authoritative = redacted.then_some(false);
    let redaction_notice = redacted.then(|| {
        "text contains redacted secret placeholders; use it for structure only and do not write the redacted text back verbatim".to_owned()
    });

    Ok(WorkspaceReadFileOutput {
        path: display_path,
        workspace_root_index,
        offset_bytes: input.offset_bytes,
        returned_bytes,
        size_bytes,
        eof,
        chunk_sha256,
        text,
        bytes_base64,
        redacted,
        text_authoritative,
        redaction_notice,
    })
}

fn path_stays_inside_workspace_root(candidate: &Path, root: &Path) -> bool {
    if candidate.starts_with(root) {
        return true;
    }
    #[cfg(target_os = "macos")]
    {
        macos_path_alias_key(candidate).is_some_and(|candidate| {
            macos_path_alias_key(root).is_some_and(|root| {
                normalized_path_key_starts_with(candidate.as_str(), root.as_str())
            })
        })
    }
    #[cfg(windows)]
    {
        windows_path_alias_key(candidate).is_some_and(|candidate| {
            windows_path_alias_key(root).is_some_and(|root| {
                normalized_path_key_starts_with(candidate.as_str(), root.as_str())
            })
        })
    }
    #[cfg(not(any(target_os = "macos", windows)))]
    {
        false
    }
}

#[cfg(target_os = "macos")]
fn macos_path_alias_key(path: &Path) -> Option<String> {
    let normalized = path.to_string_lossy().replace('\\', "/");
    if normalized.is_empty() {
        return None;
    }
    let normalized = normalized
        .strip_prefix("/System/Volumes/Data")
        .filter(|suffix| suffix.is_empty() || suffix.starts_with('/'))
        .unwrap_or(normalized.as_str());
    for alias_prefix in ["/private/var", "/private/tmp", "/private/etc"] {
        if normalized == alias_prefix {
            return Some(alias_prefix.trim_start_matches("/private").to_owned());
        }
        if let Some(suffix) = normalized.strip_prefix(alias_prefix) {
            if suffix.starts_with('/') {
                return Some(format!("{}{suffix}", alias_prefix.trim_start_matches("/private")));
            }
        }
    }
    Some(normalized.to_owned())
}

#[cfg(any(target_os = "macos", windows))]
fn normalized_path_key_starts_with(candidate: &str, root: &str) -> bool {
    if candidate == root {
        return true;
    }
    candidate.strip_prefix(root).is_some_and(|suffix| suffix.starts_with('/'))
}

#[cfg(windows)]
fn windows_path_alias_key(path: &Path) -> Option<String> {
    windows_existing_path_alias_key(path).or_else(|| windows_lexical_path_alias_key(path))
}

#[cfg(windows)]
fn windows_existing_path_alias_key(path: &Path) -> Option<String> {
    if let Some(long_path) = windows_long_path_name(path) {
        if let Some(key) = windows_normalized_path_alias_key(long_path.as_str()) {
            return Some(key);
        }
    }

    let deverbatim = windows_deverbatim_path_string(path)?;
    let long_path = windows_long_path_name(Path::new(deverbatim.as_str()))?;
    windows_normalized_path_alias_key(long_path.as_str())
}

#[cfg(windows)]
fn windows_long_path_name(path: &Path) -> Option<String> {
    use windows_sys::Win32::Storage::FileSystem::GetLongPathNameW;

    let mut source = path.as_os_str().encode_wide().collect::<Vec<_>>();
    if source.is_empty() {
        return None;
    }
    source.push(0);

    let mut buffer = vec![0_u16; 260];
    loop {
        let length = unsafe {
            // SAFETY: Both buffers are valid nul-terminated UTF-16 buffers. The destination size
            // passed to Win32 matches the allocated buffer length.
            GetLongPathNameW(
                source.as_ptr(),
                buffer.as_mut_ptr(),
                u32::try_from(buffer.len()).ok()?,
            )
        };
        if length == 0 {
            return None;
        }
        let length = usize::try_from(length).ok()?;
        if length < buffer.len() {
            buffer.truncate(length);
            return Some(String::from_utf16_lossy(buffer.as_slice()));
        }
        buffer.resize(length.saturating_add(1), 0);
    }
}

#[cfg(windows)]
fn windows_deverbatim_path_string(path: &Path) -> Option<String> {
    let normalized = path.to_string_lossy().replace('\\', "/");
    let lower = normalized.to_ascii_lowercase();
    let deverbatim = if lower.starts_with("//?/unc/") {
        format!("//{}", &normalized[8..])
    } else if lower.starts_with("//?/") || lower.starts_with("//./") {
        normalized[4..].to_owned()
    } else {
        return None;
    };
    Some(deverbatim.replace('/', "\\"))
}

#[cfg(windows)]
fn windows_lexical_path_alias_key(path: &Path) -> Option<String> {
    windows_normalized_path_alias_key(path.to_string_lossy().as_ref())
}

#[cfg(windows)]
fn windows_normalized_path_alias_key(path: &str) -> Option<String> {
    let normalized = path.replace('\\', "/");
    if normalized.is_empty() {
        return None;
    }
    let mut key = normalized.to_ascii_lowercase();
    if let Some(suffix) = key.strip_prefix("//?/unc/") {
        key = format!("//{suffix}");
    } else if let Some(suffix) = key.strip_prefix("//?/") {
        key = suffix.to_owned();
    } else if let Some(suffix) = key.strip_prefix("//./") {
        key = suffix.to_owned();
    }
    while key.ends_with('/') && key.len() > 3 {
        key.pop();
    }
    Some(key)
}

#[cfg(target_os = "linux")]
fn canonicalize_open_file_path(file: &File, input_path: &str) -> Result<PathBuf, String> {
    let fd_path = format!("/proc/self/fd/{}", file.as_raw_fd());

    fs::canonicalize(fd_path.as_str()).map_err(|error| {
        format!(
            "{WORKSPACE_READ_FILE_TOOL_NAME} failed to resolve opened workspace file {input_path}: {error}"
        )
    })
}

#[cfg(target_os = "macos")]
fn canonicalize_open_file_path(file: &File, input_path: &str) -> Result<PathBuf, String> {
    let mut buffer = vec![0 as libc::c_char; libc::PATH_MAX as usize];
    let result = unsafe {
        // SAFETY: The file descriptor is borrowed from a live `File`, and `buffer` is a writable
        // C buffer large enough for macOS `F_GETPATH` to write a nul-terminated path.
        libc::fcntl(file.as_raw_fd(), libc::F_GETPATH, buffer.as_mut_ptr())
    };
    if result == -1 {
        return Err(format!(
            "{WORKSPACE_READ_FILE_TOOL_NAME} failed to resolve opened workspace file {input_path}: {}",
            std::io::Error::last_os_error()
        ));
    }
    let opened_path = unsafe {
        // SAFETY: `F_GETPATH` succeeded and writes a nul-terminated path into `buffer`.
        std::ffi::CStr::from_ptr(buffer.as_ptr())
    };
    let opened_path = PathBuf::from(std::ffi::OsString::from_vec(opened_path.to_bytes().to_vec()));
    fs::canonicalize(opened_path.as_path()).map_err(|error| {
        format!(
            "{WORKSPACE_READ_FILE_TOOL_NAME} failed to resolve opened workspace file {input_path}: {error}"
        )
    })
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn canonicalize_open_file_path(file: &File, input_path: &str) -> Result<PathBuf, String> {
    let fd_path = format!("/dev/fd/{}", file.as_raw_fd());

    fs::canonicalize(fd_path.as_str()).map_err(|error| {
        format!(
            "{WORKSPACE_READ_FILE_TOOL_NAME} failed to resolve opened workspace file {input_path}: {error}"
        )
    })
}

#[cfg(windows)]
fn canonicalize_open_file_path(file: &File, input_path: &str) -> Result<PathBuf, String> {
    use windows_sys::Win32::Storage::FileSystem::{
        GetFinalPathNameByHandleW, FILE_NAME_NORMALIZED, VOLUME_NAME_DOS,
    };

    let mut buffer = vec![0_u16; 260];
    loop {
        let length = unsafe {
            // SAFETY: The file handle is borrowed from a live `File`, and `buffer` is a valid
            // writable UTF-16 buffer with the length passed to the Win32 API.
            GetFinalPathNameByHandleW(
                file.as_raw_handle(),
                buffer.as_mut_ptr(),
                u32::try_from(buffer.len()).unwrap_or(u32::MAX),
                FILE_NAME_NORMALIZED | VOLUME_NAME_DOS,
            )
        };
        if length == 0 {
            return Err(format!(
                "{WORKSPACE_READ_FILE_TOOL_NAME} failed to resolve opened workspace file {input_path}: {}",
                std::io::Error::last_os_error()
            ));
        }
        let length = usize::try_from(length).map_err(|_| {
            format!(
                "{WORKSPACE_READ_FILE_TOOL_NAME} failed to resolve opened workspace file {input_path}: path length exceeds platform limits"
            )
        })?;
        if length < buffer.len() {
            buffer.truncate(length);
            return Ok(PathBuf::from(String::from_utf16_lossy(buffer.as_slice())));
        }
        buffer.resize(length.saturating_add(1), 0);
    }
}

#[cfg(not(any(unix, windows)))]
fn canonicalize_open_file_path(_file: &File, input_path: &str) -> Result<PathBuf, String> {
    Err(format!(
        "{WORKSPACE_READ_FILE_TOOL_NAME} failed to resolve opened workspace file {input_path}: unsupported platform"
    ))
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn normalize_relative_path_display(path: &Path) -> String {
    let mut rendered = Vec::new();
    for component in path.components() {
        if let Component::Normal(value) = component {
            rendered.push(value.to_string_lossy().into_owned());
        }
    }
    if rendered.is_empty() {
        ".".to_owned()
    } else {
        rendered.join("/")
    }
}

fn display_requested_path(path: &str) -> &str {
    if path.is_empty() {
        "."
    } else {
        path
    }
}

fn workspace_read_file_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    build_tool_execution_outcome(
        proposal_id,
        WORKSPACE_READ_FILE_TOOL_NAME,
        input_json,
        success,
        output_json,
        error,
        false,
        "workspace_file".to_owned(),
        "workspace_roots".to_owned(),
    )
}

fn workspace_list_dir_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    build_tool_execution_outcome(
        proposal_id,
        WORKSPACE_LIST_DIR_TOOL_NAME,
        input_json,
        success,
        output_json,
        error,
        false,
        "workspace_file".to_owned(),
        "workspace_roots".to_owned(),
    )
}

fn workspace_search_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    build_tool_execution_outcome(
        proposal_id,
        WORKSPACE_SEARCH_TOOL_NAME,
        input_json,
        success,
        output_json,
        error,
        false,
        "workspace_file".to_owned(),
        "workspace_roots".to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(windows)]
    fn windows_short_path_name(path: &Path) -> Option<PathBuf> {
        use windows_sys::Win32::Storage::FileSystem::GetShortPathNameW;

        let mut source = path.as_os_str().encode_wide().collect::<Vec<_>>();
        if source.is_empty() {
            return None;
        }
        source.push(0);

        let mut buffer = vec![0_u16; 260];
        loop {
            let length = unsafe {
                // SAFETY: Both buffers are valid nul-terminated UTF-16 buffers. The destination
                // size passed to Win32 matches the allocated buffer length.
                GetShortPathNameW(
                    source.as_ptr(),
                    buffer.as_mut_ptr(),
                    u32::try_from(buffer.len()).ok()?,
                )
            };
            if length == 0 {
                return None;
            }
            let length = usize::try_from(length).ok()?;
            if length < buffer.len() {
                buffer.truncate(length);
                return Some(PathBuf::from(String::from_utf16_lossy(buffer.as_slice())));
            }
            buffer.resize(length.saturating_add(1), 0);
        }
    }

    #[test]
    fn workspace_root_scope_check_rejects_prefix_sibling() {
        assert!(!path_stays_inside_workspace_root(
            Path::new("/tmp/workspace-extra/file.txt"),
            Path::new("/tmp/workspace")
        ));
    }

    #[test]
    #[cfg(windows)]
    fn workspace_root_scope_check_accepts_windows_short_and_opened_long_aliases() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        fs::create_dir_all(workspace.join("nested")).expect("workspace should exist");
        let file_path = workspace.join("nested").join("calc.js");
        fs::write(file_path.as_path(), "export const add = (a, b) => a + b;\n")
            .expect("workspace file should be written");
        let short_workspace =
            windows_short_path_name(workspace.as_path()).unwrap_or_else(|| workspace.clone());
        let file = File::open(file_path.as_path()).expect("workspace file should open");
        let opened_path = canonicalize_open_file_path(&file, "nested/calc.js")
            .expect("opened workspace file should resolve");

        assert!(
            path_stays_inside_workspace_root(opened_path.as_path(), short_workspace.as_path()),
            "opened path {} should stay inside workspace root {}",
            opened_path.display(),
            short_workspace.display()
        );
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn workspace_root_scope_check_accepts_private_var_alias() {
        assert!(path_stays_inside_workspace_root(
            Path::new("/private/var/folders/palyra/workspace/file.txt"),
            Path::new("/var/folders/palyra/workspace")
        ));
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn workspace_root_scope_check_accepts_data_volume_private_var_alias() {
        assert!(path_stays_inside_workspace_root(
            Path::new("/System/Volumes/Data/private/var/folders/palyra/workspace/file.txt"),
            Path::new("/var/folders/palyra/workspace")
        ));
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn canonicalize_open_file_path_resolves_macos_file_descriptor_target() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let file_path = tempdir.path().join("opened.txt");
        fs::write(file_path.as_path(), "opened").expect("workspace file should be written");
        let file = File::open(file_path.as_path()).expect("workspace file should open");

        let opened_path = canonicalize_open_file_path(&file, "opened.txt")
            .expect("macOS opened file path should resolve to the target file");
        let canonical_file =
            fs::canonicalize(file_path.as_path()).expect("workspace file should canonicalize");

        assert!(path_stays_inside_workspace_root(opened_path.as_path(), tempdir.path()));
        assert_eq!(
            macos_path_alias_key(opened_path.as_path()),
            macos_path_alias_key(canonical_file.as_path())
        );
    }

    #[test]
    fn read_workspace_file_returns_utf8_text() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let file_path = tempdir.path().join("agent-e2e-tool-test.js");
        let contents = "export function add(a, b) { return a + b; }\nexport const meaning = 42;\n";
        fs::write(file_path, contents).expect("workspace file should be written");
        let input = WorkspaceReadFileInput {
            path: "agent-e2e-tool-test.js".to_owned(),
            workspace_root: None,
            offset_bytes: 0,
            max_bytes: None,
        };

        let output = read_workspace_file_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect("workspace file should be readable");

        assert_eq!(output.text.as_deref(), Some(contents));
        assert_eq!(output.path, "agent-e2e-tool-test.js");
        assert_eq!(output.bytes_base64, None);
        assert_eq!(output.returned_bytes, contents.len() as u64);
        assert!(output.eof);
        assert_eq!(output.workspace_root_index, 0);
        assert!(!output.redacted);
    }

    #[test]
    fn read_workspace_file_redacts_secret_like_source_literals() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let file_path = tempdir.path().join("app.js");
        let contents = "const publicValue = 'visible';\n\
             const privateValue = 'DUMMY_SECRET_SHOULD_NOT_APPEAR';\n\
             const modelToken = 'palyra_test_secret_123456';\n";
        fs::write(file_path, contents).expect("workspace file should be written");
        let input = WorkspaceReadFileInput {
            path: "app.js".to_owned(),
            workspace_root: None,
            offset_bytes: 0,
            max_bytes: None,
        };

        let output = read_workspace_file_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect("workspace file should be readable");
        let text = output.text.as_deref().expect("utf8 text should be returned");

        assert!(output.redacted);
        assert!(text.contains("publicValue"));
        assert!(text.contains("[REDACTED_SECRET]"));
        assert!(
            !text.contains("DUMMY_SECRET_SHOULD_NOT_APPEAR"),
            "source literal should be redacted from tool output: {text}"
        );
        assert!(
            !text.contains("palyra_test_secret_123456"),
            "test harness secret marker should be redacted from tool output: {text}"
        );
    }

    #[test]
    fn read_workspace_file_preserves_env_secret_identifiers() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let file_path = tempdir.path().join("app.js");
        let contents =
            "const apiKey = import.meta.env.PRIVATE_API_KEY;\nconst token = process.env.ACCESS_TOKEN;\n";
        fs::write(file_path, contents).expect("workspace file should be written");
        let input = WorkspaceReadFileInput {
            path: "app.js".to_owned(),
            workspace_root: None,
            offset_bytes: 0,
            max_bytes: None,
        };

        let output = read_workspace_file_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect("workspace file should be readable");

        assert!(!output.redacted);
        assert_eq!(output.text.as_deref(), Some(contents));
    }

    #[test]
    fn read_workspace_file_preserves_safe_storage_and_env_key_identifiers() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let file_path = tempdir.path().join("app.js");
        let contents = "const STORAGE_KEY = \"todo-app:items:v1\";\n\
                        const FILTER_KEY = \"todo-app:filter:v1\";\n\
                        const SECRET_KEY = 'VITE_SECRET_TOKEN';\n\
                        const PRIVATE_KEY = 'SERVER_PRIVATE_KEY';\n";
        fs::write(file_path, contents).expect("workspace file should be written");
        let input = WorkspaceReadFileInput {
            path: "app.js".to_owned(),
            workspace_root: None,
            offset_bytes: 0,
            max_bytes: None,
        };

        let output = read_workspace_file_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect("workspace file should be readable");

        assert!(!output.redacted);
        assert_eq!(output.text.as_deref(), Some(contents));
        assert_eq!(output.text_authoritative, None);
        assert_eq!(output.redaction_notice, None);
    }

    #[test]
    fn read_workspace_file_preserves_env_reference_fallback_expressions() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let file_path = tempdir.path().join("config.js");
        let contents = "function readConfig(env = process.env) {\n\
                        return {\n\
                        apiKey: env.PALYRA_API_KEY || '',\n\
                        accessToken: process.env.ACCESS_TOKEN ?? \"\",\n\
                        };\n\
                        }\n";
        fs::write(file_path, contents).expect("workspace file should be written");
        let input = WorkspaceReadFileInput {
            path: "config.js".to_owned(),
            workspace_root: None,
            offset_bytes: 0,
            max_bytes: None,
        };

        let output = read_workspace_file_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect("workspace file should be readable");

        assert!(!output.redacted);
        assert_eq!(output.text.as_deref(), Some(contents));
        assert_eq!(output.text_authoritative, None);
        assert_eq!(output.redaction_notice, None);
    }

    #[test]
    fn read_workspace_file_preserves_obvious_api_key_placeholders() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let file_path = tempdir.path().join(".env.example");
        let contents = "PORT=3000\nPALYRA_API_KEY=TODO\nSERVICE_API_KEY=your_api_key_here\n";
        fs::write(file_path, contents).expect("workspace file should be written");
        let input = WorkspaceReadFileInput {
            path: ".env.example".to_owned(),
            workspace_root: None,
            offset_bytes: 0,
            max_bytes: None,
        };

        let output = read_workspace_file_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect("workspace file should be readable");

        assert!(!output.redacted);
        assert_eq!(output.text.as_deref(), Some(contents));
    }

    #[test]
    fn read_workspace_file_marks_redacted_text_as_non_authoritative() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let file_path = tempdir.path().join(".env");
        let contents = "APP_SECRET=server-only-demo-secret\nVITE_PUBLIC_LABEL=Palyra Preview\n";
        fs::write(file_path, contents).expect("workspace file should be written");
        let input = WorkspaceReadFileInput {
            path: ".env".to_owned(),
            workspace_root: None,
            offset_bytes: 0,
            max_bytes: None,
        };

        let output = read_workspace_file_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect("workspace file should be readable");
        let text = output.text.as_deref().expect("utf8 text should be returned");

        assert!(output.redacted);
        assert_eq!(output.text_authoritative, Some(false));
        assert!(output
            .redaction_notice
            .as_deref()
            .is_some_and(|notice| notice.contains("do not write")));
        assert!(text.contains("APP_SECRET=[REDACTED_SECRET]"));
        assert!(text.contains("VITE_PUBLIC_LABEL=Palyra Preview"));
        assert!(!text.contains("server-only-demo-secret"));
    }

    #[test]
    fn read_workspace_file_preserves_safe_secret_placeholders() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let file_path = tempdir.path().join("smoke.js");
        let contents = "const env = { PALYRA_E2E_API_KEY: 'test-placeholder' };\n\
                        assert.strictEqual(config.apiKey, 'test-placeholder');\n";
        fs::write(file_path, contents).expect("workspace file should be written");
        let input = WorkspaceReadFileInput {
            path: "smoke.js".to_owned(),
            workspace_root: None,
            offset_bytes: 0,
            max_bytes: None,
        };

        let output = read_workspace_file_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect("workspace file should be readable");

        assert!(!output.redacted);
        assert_eq!(output.text.as_deref(), Some(contents));
    }

    #[test]
    fn read_workspace_file_preserves_cookie_regex_and_benign_token_fixture() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let file_path = tempdir.path().join("app.js");
        let contents = "const match = document.cookie.match(/(?:^|; )theme=([^;]*)/);\n\
                        const fixture = 'token=a%3Db%3Dc';\n\
                        const selector = '#password';\n";
        fs::write(file_path, contents).expect("workspace file should be written");
        let input = WorkspaceReadFileInput {
            path: "app.js".to_owned(),
            workspace_root: None,
            offset_bytes: 0,
            max_bytes: None,
        };

        let output = read_workspace_file_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect("workspace file should be readable");

        assert!(!output.redacted);
        assert_eq!(output.text.as_deref(), Some(contents));
    }

    #[test]
    fn read_workspace_file_returns_bounded_chunk() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        fs::write(tempdir.path().join("chunk.txt"), "abcdef").expect("workspace file should exist");
        let input = WorkspaceReadFileInput {
            path: "chunk.txt".to_owned(),
            workspace_root: None,
            offset_bytes: 2,
            max_bytes: Some(3),
        };

        let output = read_workspace_file_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect("workspace file chunk should be readable");

        assert_eq!(output.text.as_deref(), Some("cde"));
        assert_eq!(output.returned_bytes, 3);
        assert!(!output.eof);
    }

    #[test]
    fn read_workspace_file_accepts_absolute_path_inside_root() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        fs::create_dir_all(workspace.join("nested")).expect("workspace should exist");
        let file_path = workspace.join("nested").join("calc.js");
        fs::write(&file_path, "export const add = (a, b) => a + b;\n")
            .expect("workspace file should be written");
        let input = WorkspaceReadFileInput {
            path: file_path.to_string_lossy().into_owned(),
            workspace_root: None,
            offset_bytes: 0,
            max_bytes: None,
        };

        let output = read_workspace_file_from_roots(&[workspace], &input)
            .expect("absolute workspace file should be readable");

        assert_eq!(output.path, "nested/calc.js");
        assert_eq!(output.text.as_deref(), Some("export const add = (a, b) => a + b;\n"));
    }

    #[test]
    fn read_workspace_file_accepts_workspace_virtual_absolute_alias() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        fs::create_dir_all(tempdir.path().join("nested")).expect("nested dir should exist");
        fs::write(tempdir.path().join("nested").join("calc.js"), "export const answer = 42;\n")
            .expect("workspace file should be written");
        let mut input = parse_workspace_read_file_input(
            br#"{"path":"/workspace/nested/calc.js","offset_bytes":0}"#,
        )
        .expect("virtual workspace path should parse");

        assert_eq!(input.path, "nested/calc.js");
        input.max_bytes = None;
        let output = read_workspace_file_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect("virtual workspace path should be readable");

        assert_eq!(output.path, "nested/calc.js");
        assert_eq!(output.text.as_deref(), Some("export const answer = 42;\n"));
    }

    #[test]
    fn read_workspace_file_accepts_workspace_prefix_alias() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        fs::create_dir_all(tempdir.path().join("scenarios")).expect("scenarios dir should exist");
        fs::write(tempdir.path().join("scenarios").join("app.js"), "console.log('ok');\n")
            .expect("workspace file should be written");
        let input = parse_workspace_read_file_input(br#"{"path":"workspace/scenarios/app.js"}"#)
            .expect("workspace alias path should parse");

        let output = read_workspace_file_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect("workspace alias path should be readable");

        assert_eq!(output.path, "scenarios/app.js");
        assert_eq!(output.text.as_deref(), Some("console.log('ok');\n"));
    }

    #[test]
    fn read_workspace_file_accepts_workspace_root_override() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let project = workspace.join("agent-smoke");
        fs::create_dir_all(&project).expect("project dir should exist");
        fs::write(project.join("calculator.js"), "export const add = (a, b) => a + b;\n")
            .expect("workspace file should be written");
        let input = parse_workspace_read_file_input(
            br#"{"path":"calculator.js","workspace_root":"agent-smoke"}"#,
        )
        .expect("workspace_root override should parse");
        let roots = resolve_workspace_file_roots_for_override(
            WORKSPACE_READ_FILE_TOOL_NAME,
            std::slice::from_ref(&workspace),
            input.workspace_root.as_deref(),
        )
        .expect("workspace_root override should resolve");

        let output =
            read_workspace_file_from_roots(roots.as_slice(), &input).expect("file should read");

        assert_eq!(output.path, "calculator.js");
        assert_eq!(output.text.as_deref(), Some("export const add = (a, b) => a + b;\n"));
        assert_eq!(output.workspace_root_index, 0);
    }

    #[test]
    fn read_workspace_file_workspace_root_basename_targets_existing_launch_root() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let project = tempdir.path().join("task-workspace");
        fs::create_dir_all(project.join("reports")).expect("project report dir should exist");
        fs::write(project.join("reports").join("summary.md"), "ok\n")
            .expect("workspace file should be written");
        let input = parse_workspace_read_file_input(
            br#"{"path":"reports/summary.md","workspace_root":"task-workspace"}"#,
        )
        .expect("workspace_root override should parse");
        let roots = resolve_workspace_file_roots_for_override(
            WORKSPACE_READ_FILE_TOOL_NAME,
            std::slice::from_ref(&project),
            input.workspace_root.as_deref(),
        )
        .expect("workspace_root basename should resolve to the existing project root");

        let output =
            read_workspace_file_from_roots(roots.as_slice(), &input).expect("file should read");

        assert_eq!(output.text.as_deref(), Some("ok\n"));
        assert!(!project.join("task-workspace").exists());
    }

    #[test]
    fn read_workspace_file_rejects_workspace_root_override_outside_agent_roots() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let outside = tempdir.path().join("outside");
        fs::create_dir_all(&workspace).expect("workspace dir should exist");
        fs::create_dir_all(&outside).expect("outside dir should exist");
        let input = parse_workspace_read_file_input(
            format!(
                r#"{{"path":"notes.txt","workspace_root":"{}"}}"#,
                outside.to_string_lossy().replace('\\', "\\\\")
            )
            .as_bytes(),
        )
        .expect("absolute workspace_root should parse");

        let error = resolve_workspace_file_roots_for_override(
            WORKSPACE_READ_FILE_TOOL_NAME,
            std::slice::from_ref(&workspace),
            input.workspace_root.as_deref(),
        )
        .expect_err("outside workspace_root should be rejected");

        assert!(error.contains("escapes agent workspace roots"), "unexpected error: {error}");
    }

    #[test]
    fn read_workspace_file_rejects_absolute_path_outside_root() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let outside = tempdir.path().join("outside");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::create_dir_all(&outside).expect("outside directory should exist");
        let outside_file = outside.join("secret.txt");
        fs::write(&outside_file, "secret").expect("outside file should be written");
        let input = WorkspaceReadFileInput {
            path: outside_file.to_string_lossy().into_owned(),
            workspace_root: None,
            offset_bytes: 0,
            max_bytes: None,
        };

        let error = read_workspace_file_from_roots(&[workspace], &input)
            .expect_err("outside absolute path should be rejected");

        assert!(error.contains("escapes agent workspace roots"), "unexpected error: {error}");
    }

    #[test]
    fn read_workspace_file_returns_uniform_error_for_outside_absolute_paths() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let outside = tempdir.path().join("outside");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::create_dir_all(&outside).expect("outside directory should exist");
        let existing_outside = outside.join("secret.txt");
        fs::write(existing_outside.as_path(), "secret").expect("outside file should be written");
        let missing_outside = outside.join("missing.txt");
        let outside_inputs = [existing_outside, missing_outside]
            .into_iter()
            .map(|path| WorkspaceReadFileInput {
                path: path.to_string_lossy().into_owned(),
                workspace_root: None,
                offset_bytes: 0,
                max_bytes: None,
            })
            .collect::<Vec<_>>();

        let errors = outside_inputs
            .iter()
            .map(|input| {
                read_workspace_file_from_roots(std::slice::from_ref(&workspace), input)
                    .expect_err("outside absolute path should be rejected")
            })
            .collect::<Vec<_>>();

        assert_eq!(
            errors,
            vec![
                format!("{WORKSPACE_READ_FILE_TOOL_NAME} path escapes agent workspace roots"),
                format!("{WORKSPACE_READ_FILE_TOOL_NAME} path escapes agent workspace roots"),
            ]
        );
    }

    #[test]
    fn read_workspace_file_rejects_absolute_parent_traversal_without_probe() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let outside = tempdir.path().join("outside");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::create_dir_all(&outside).expect("outside directory should exist");
        let outside_file = outside.join("secret.txt");
        fs::write(outside_file.as_path(), "secret").expect("outside file should be written");
        let input = WorkspaceReadFileInput {
            path: workspace
                .join("..")
                .join("outside")
                .join("secret.txt")
                .to_string_lossy()
                .into_owned(),
            workspace_root: None,
            offset_bytes: 0,
            max_bytes: None,
        };

        let error = read_workspace_file_from_roots(&[workspace], &input)
            .expect_err("absolute parent traversal should be rejected before resolution");

        assert_eq!(
            error,
            format!("{WORKSPACE_READ_FILE_TOOL_NAME} path escapes agent workspace roots")
        );
    }

    #[test]
    fn read_workspace_file_rejects_absolute_host_path_even_when_near_workspace_root() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let outside = tempdir.path().join("outside");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::create_dir_all(&outside).expect("outside directory should exist");
        let outside_file = outside.join("notes.txt");
        fs::write(&outside_file, "host note\n").expect("outside file should be written");
        let input = WorkspaceReadFileInput {
            path: outside_file.to_string_lossy().into_owned(),
            workspace_root: None,
            offset_bytes: 0,
            max_bytes: None,
        };

        let error = read_workspace_file_from_roots(&[workspace], &input)
            .expect_err("host file reads outside workspace roots should be rejected");

        assert!(error.contains("escapes agent workspace roots"), "unexpected error: {error}");
    }

    #[test]
    fn read_workspace_file_chunk_rejects_opened_file_outside_root() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let outside = tempdir.path().join("outside");
        fs::create_dir_all(workspace.as_path()).expect("workspace should exist");
        fs::create_dir_all(outside.as_path()).expect("outside should exist");
        let outside_file = outside.join("secret.txt");
        fs::write(outside_file.as_path(), "outside secret\n").expect("outside file should exist");
        let canonical_workspace =
            fs::canonicalize(workspace.as_path()).expect("workspace should canonicalize");
        let canonical_outside =
            fs::canonicalize(outside_file.as_path()).expect("outside file should canonicalize");
        let input = WorkspaceReadFileInput {
            path: "inside.txt".to_owned(),
            workspace_root: None,
            offset_bytes: 0,
            max_bytes: None,
        };

        let error = read_workspace_file_chunk(
            0,
            canonical_workspace.as_path(),
            canonical_outside,
            "inside.txt".to_owned(),
            &input,
        )
        .expect_err("post-open path validation should reject outside files");

        assert!(error.contains("escapes agent workspace roots"), "unexpected error: {error}");
    }

    #[test]
    fn read_workspace_file_rejects_parent_traversal() {
        let error =
            parse_workspace_read_file_input(br#"{"path":"../outside.txt"}"#).expect_err("path");

        assert!(error.contains("must not contain"), "unexpected validation error: {error}");
    }

    #[test]
    fn read_workspace_file_directory_error_points_to_list_dir() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        fs::create_dir_all(tempdir.path().join("scenarios")).expect("scenarios dir should exist");
        let input = parse_workspace_read_file_input(br#"{"path":"workspace/scenarios"}"#)
            .expect("workspace alias directory should parse");

        let error = read_workspace_file_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect_err("directory read should fail");

        assert!(error.contains(WORKSPACE_LIST_DIR_TOOL_NAME), "unexpected error: {error}");
    }

    #[test]
    fn workspace_file_tools_strip_duplicate_root_basename_from_relative_paths() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("S036_session_recall");
        fs::create_dir_all(workspace.join("docs")).expect("workspace dirs should exist");
        fs::write(workspace.join("docs").join("plan.md"), "ship it\n")
            .expect("workspace file should be written");
        let roots = [workspace.clone()];

        let read_input =
            parse_workspace_read_file_input(br#"{"path":"S036_session_recall/docs/plan.md"}"#)
                .expect("read input should parse");
        let read = read_workspace_file_from_roots(&roots, &read_input)
            .expect("duplicated basename read path should resolve");
        assert_eq!(read.path, "docs/plan.md");
        assert_eq!(read.text.as_deref(), Some("ship it\n"));

        let list_input = parse_workspace_list_dir_input(
            br#"{"path":"S036_session_recall/docs","max_entries":10}"#,
        )
        .expect("list input should parse");
        let listed = list_workspace_dir_from_roots(&roots, &list_input)
            .expect("duplicated basename list path should resolve");
        assert_eq!(listed.path, "docs");
        assert_eq!(
            listed.entries.iter().map(|entry| entry.path.as_str()).collect::<Vec<_>>(),
            vec!["docs/plan.md"]
        );

        let search_input = parse_workspace_search_input(
            br#"{"path":"S036_session_recall","query":"ship","max_matches":10}"#,
        )
        .expect("search input should parse");
        let searched = search_workspace_from_roots(&roots, &search_input)
            .expect("duplicated basename search path should resolve");
        assert_eq!(searched.path, ".");
        assert_eq!(
            searched.matches.iter().map(|entry| entry.path.as_str()).collect::<Vec<_>>(),
            vec!["docs/plan.md"]
        );
    }

    #[test]
    fn list_workspace_dir_returns_sorted_entries_for_workspace_alias() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        fs::create_dir_all(tempdir.path().join("scenarios").join("nested"))
            .expect("nested dir should exist");
        fs::write(tempdir.path().join("scenarios").join("b.txt"), "bravo")
            .expect("workspace file should be written");
        fs::write(tempdir.path().join("scenarios").join("a.txt"), "alpha")
            .expect("workspace file should be written");
        let input =
            parse_workspace_list_dir_input(br#"{"path":"/workspace/scenarios","max_entries":10}"#)
                .expect("workspace alias list input should parse");

        let output = list_workspace_dir_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect("workspace directory should be listed");

        assert_eq!(output.path, "scenarios");
        assert_eq!(output.workspace_root_index, 0);
        assert!(!output.truncated);
        assert_eq!(
            output.entries.iter().map(|entry| entry.path.as_str()).collect::<Vec<_>>(),
            vec!["scenarios/a.txt", "scenarios/b.txt", "scenarios/nested"]
        );
        assert_eq!(output.entries[0].kind, "file");
        assert_eq!(output.entries[0].size_bytes, Some(5));
        assert_eq!(output.entries[2].kind, "directory");
        assert_eq!(output.entries[2].size_bytes, None);
    }

    #[test]
    fn list_workspace_dir_falls_back_after_active_root_miss() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let launch_root = tempdir.path().join("S097_cross_boundary_audit");
        let active_root = tempdir.path().join("home").join("S097").join(".config");
        fs::create_dir_all(launch_root.join("config")).expect("launch config dir should exist");
        fs::create_dir_all(active_root.join("palyra-e2e")).expect("active config dir should exist");
        fs::write(launch_root.join("config").join("app.toml"), "mode = 'safe'\n")
            .expect("workspace config should exist");
        fs::write(active_root.join("palyra-e2e").join("settings.toml"), "mode = 'user'\n")
            .expect("home config should exist");

        let roots =
            workspace_roots_with_active_first(active_root, std::slice::from_ref(&launch_root));
        let input = parse_workspace_list_dir_input(br#"{"path":"config","max_entries":10}"#)
            .expect("list input should parse");
        let output = list_workspace_dir_from_roots(roots.as_slice(), &input)
            .expect("list_dir should fall back to launch workspace");

        assert_eq!(output.path, "config");
        assert_eq!(output.workspace_root_index, 1);
        assert_eq!(
            output.entries.iter().map(|entry| entry.path.as_str()).collect::<Vec<_>>(),
            vec!["config/app.toml"]
        );
    }

    #[test]
    fn list_workspace_dir_accepts_workspace_root_override() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let project = workspace.join("notes-api");
        fs::create_dir_all(project.join("tests")).expect("project dirs should exist");
        fs::write(project.join("server.js"), "console.log('ok');\n")
            .expect("server file should exist");
        fs::write(project.join("tests").join("api.test.js"), "console.log('test');\n")
            .expect("test file should exist");
        let input = parse_workspace_list_dir_input(
            br#"{"path":".","workspace_root":"notes-api","max_entries":10}"#,
        )
        .expect("list input should parse");
        let roots = resolve_workspace_file_roots_for_override(
            WORKSPACE_LIST_DIR_TOOL_NAME,
            std::slice::from_ref(&workspace),
            input.workspace_root.as_deref(),
        )
        .expect("workspace_root override should resolve");

        let output =
            list_workspace_dir_from_roots(roots.as_slice(), &input).expect("dir should list");

        assert_eq!(output.path, ".");
        assert_eq!(
            output.entries.iter().map(|entry| entry.path.as_str()).collect::<Vec<_>>(),
            vec!["server.js", "tests"]
        );
    }

    #[test]
    fn search_workspace_finds_identifier_in_docs_and_skips_dependencies() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let project = workspace.join("client-id-api");
        fs::create_dir_all(project.join("src")).expect("src should exist");
        fs::create_dir_all(project.join("docs")).expect("docs should exist");
        fs::create_dir_all(project.join("node_modules").join("pkg"))
            .expect("node_modules should exist");
        fs::write(project.join("src").join("order.js"), "export const customerId = 1;\n")
            .expect("source file should be written");
        fs::write(
            project.join("docs").join("usage.md"),
            "Use customerId when creating an order.\n",
        )
        .expect("docs file should be written");
        fs::write(project.join("node_modules").join("pkg").join("index.js"), "customerId\n")
            .expect("dependency file should be written");
        let input = parse_workspace_search_input(
            br#"{"query":"customerId","workspace_root":"client-id-api","max_matches":10}"#,
        )
        .expect("search input should parse");
        let roots = resolve_workspace_file_roots_for_override(
            WORKSPACE_SEARCH_TOOL_NAME,
            std::slice::from_ref(&workspace),
            input.workspace_root.as_deref(),
        )
        .expect("workspace_root override should resolve");

        let output = search_workspace_from_roots(roots.as_slice(), &input)
            .expect("workspace search should complete");

        assert!(!output.truncated);
        assert_eq!(
            output.matches.iter().map(|entry| entry.path.as_str()).collect::<Vec<_>>(),
            vec!["docs/usage.md", "src/order.js"]
        );
        assert_eq!(output.files_with_matches, 2);
        assert_eq!(output.skipped_dirs, 1);
    }

    #[test]
    fn search_workspace_redacts_secret_like_matching_lines() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        fs::write(tempdir.path().join("config.txt"), "token=DUMMY_SECRET_SHOULD_NOT_APPEAR\n")
            .expect("workspace file should be written");
        let input = parse_workspace_search_input(br#"{"query":"token"}"#)
            .expect("search input should parse");

        let output = search_workspace_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect("workspace search should complete");

        assert_eq!(output.matches.len(), 1);
        assert!(output.matches[0].redacted);
        assert!(output.matches[0].line_text.contains("[REDACTED_SECRET]"));
        assert!(!output.matches[0].line_text.contains("DUMMY_SECRET_SHOULD_NOT_APPEAR"));
    }

    #[test]
    fn search_workspace_bounds_long_line_output() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let long_line = "a".repeat(WORKSPACE_SEARCH_MAX_FILE_BYTES as usize);
        fs::write(tempdir.path().join("large.txt"), long_line)
            .expect("workspace file should be written");
        let input = parse_workspace_search_input(br#"{"query":"a","max_matches":200}"#)
            .expect("search input should parse");

        let output = search_workspace_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect("workspace search should complete");
        let serialized = serde_json::to_vec(&output).expect("output should serialize");

        assert!(output.truncated, "search should stop at output budget");
        assert!(!output.matches.is_empty());
        assert!(output.matches.len() < WORKSPACE_SEARCH_MAX_MATCHES);
        assert!(
            output
                .matches
                .iter()
                .all(|entry| entry.line_text.len() <= WORKSPACE_SEARCH_MAX_LINE_TEXT_BYTES + 6),
            "match line excerpts should stay bounded"
        );
        assert!(
            serialized.len() <= WORKSPACE_SEARCH_MAX_OUTPUT_BYTES,
            "serialized search output should stay bounded: {}",
            serialized.len()
        );
    }

    #[test]
    fn search_workspace_bounds_recursive_depth() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let mut current = tempdir.path().to_path_buf();
        for depth in 0..=WORKSPACE_SEARCH_MAX_DEPTH {
            current = current.join(format!("d{depth}"));
            fs::create_dir(current.as_path()).expect("nested directory should be created");
        }
        fs::write(current.join("needle.txt"), "deep needle\n")
            .expect("deep workspace file should be written");
        let input = parse_workspace_search_input(br#"{"query":"needle","max_matches":10}"#)
            .expect("search input should parse");

        let output = search_workspace_from_roots(&[tempdir.path().to_path_buf()], &input)
            .expect("workspace search should complete");

        assert!(output.truncated, "search should truncate at max recursion depth");
        assert!(output.matches.is_empty(), "file past recursion depth should not be scanned");
        assert!(output.skipped_dirs > 0, "truncated deep directory should be counted");
    }

    #[test]
    fn list_workspace_dir_rejects_absolute_host_path_even_when_near_workspace_root() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let outside = tempdir.path().join("outside");
        fs::create_dir_all(&workspace).expect("workspace should exist");
        fs::create_dir_all(&outside).expect("outside directory should exist");
        fs::write(outside.join("notes.txt"), "host note\n")
            .expect("outside file should be written");
        let input = WorkspaceListDirInput {
            path: outside.to_string_lossy().into_owned(),
            workspace_root: None,
            max_entries: None,
        };

        let error = list_workspace_dir_from_roots(&[workspace], &input)
            .expect_err("host directory listings outside workspace roots should be rejected");

        assert!(error.contains("escapes agent workspace roots"), "unexpected error: {error}");
    }

    #[test]
    fn list_workspace_dir_rejects_parent_traversal() {
        let error = parse_workspace_list_dir_input(br#"{"path":"../outside"}"#).expect_err("path");

        assert!(error.contains("must not contain"), "unexpected validation error: {error}");
    }
}
