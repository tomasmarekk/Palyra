//! `palyra.fs.apply_patch` tool: scoped patch application over agent roots.
//!
//! This module owns input validation, workspace-root scoping (explicit
//! override, session focus, launch context), and patch header path
//! normalization; the patch grammar parsing and application engine lives in
//! `palyra_common::workspace_patch`. Every request is planned as a dry run
//! first, and real writes go through `checkpoint_flow`, which brackets the
//! apply with preflight/post-change checkpoints and risk assessment.
//!
//! Error strings, recovery hints, and the grammar hint are pinned verbatim
//! by tests; root containment decisions are security-sensitive. Keep both
//! byte-identical unless tests move with the change.

mod checkpoint_flow;

use std::{
    fs,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use palyra_common::workspace_patch::{
    apply_workspace_patch, apply_workspace_patch_with_canonical_root_constraints,
    compute_patch_sha256, redact_patch_preview, WorkspacePatchError, WorkspacePatchLimits,
    WorkspacePatchOutcome, WorkspacePatchRedactionPolicy, WorkspacePatchRequest,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tracing::warn;
use ulid::Ulid;

use crate::{
    agents::AgentResolveRequest,
    application::tool_runtime::workspace_scope::{
        relative_path_should_use_active_root, session_active_workspace_root,
        workspace_root_override_targets_active_root,
        workspace_roots_with_run_launch_context_for_agent_source,
    },
    gateway::{
        current_unix_ms, GatewayRuntimeState, MAX_PATCH_TOOL_MARKER_BYTES,
        MAX_PATCH_TOOL_PATTERN_BYTES, MAX_PATCH_TOOL_REDACTION_PATTERNS,
        MAX_PATCH_TOOL_SECRET_FILE_MARKERS, MAX_WORKSPACE_PATCH_TOOL_INPUT_BYTES,
    },
    tool_protocol::{
        failed_tool_output_json, tool_output_json_is_empty_object, ToolAttestation,
        ToolExecutionOutcome,
    },
};

use checkpoint_flow::WorkspacePatchMutationRequest;

/// Model-facing patch grammar primer attached to every failure payload so
/// the model can self-repair; pinned verbatim by tests.
const WORKSPACE_PATCH_GRAMMAR_HINT: &str = "Use a complete Palyra patch document: begin with exactly '*** Begin Patch', then operation headers like '*** Add File: path', '*** Replace File: path', '*** Replace Line: path', or '*** Update File: path', end with exactly one '*** End Patch'. Never send a partial or truncated patch. For large file creation or multi-file changes, split work into multiple smaller complete apply_patch calls. Add-file and replace-file content lines may start with '+', and a bare '+' or '+ ' writes a blank line. Use Add File only for missing files. If search/read_file confirmed one exact target line, use Replace Line with exactly one '-' old line and one '+' new line. If an Update File hunk fails with context not found, read the current file and retry with Replace Line for a unique exact line, fresh context hunks, or Replace File plus the full intended file content. Update-file hunks must start with '@@'; hunk lines should start with ' ', '+', or '-', and a bare empty hunk line is accepted as blank context. To edit file content that itself begins with '-' or '+', prefix it directly with the hunk marker, for example '-- markdown item' removes '- markdown item' and '++value' adds '+value'. JSON files are validated after patch planning; if JSON validation fails, retry with the complete valid JSON file content.";

/// Borrowed execution context for one apply_patch invocation.
pub(crate) struct WorkspacePatchToolRequest<'a> {
    pub(crate) principal: &'a str,
    pub(crate) device_id: &'a str,
    pub(crate) channel: Option<&'a str>,
    pub(crate) session_id: &'a str,
    pub(crate) run_id: &'a str,
    pub(crate) proposal_id: &'a str,
    pub(crate) input_json: &'a [u8],
}

/// Scoping decision for one patch application.
#[derive(Debug, Clone)]
struct ResolvedWorkspacePatchRoots {
    /// Root(s) the patch operations are resolved against.
    roots: Vec<PathBuf>,
    /// Canonical agent roots enforced as containment constraints when the
    /// patch runs against a narrowed override/focus root; empty means
    /// `roots` are the agent roots themselves.
    canonical_constraint_roots: Vec<PathBuf>,
    /// Paths of the narrowed root relative to each agent root, prepended to
    /// touched paths during risk assessment so prefix rules match as if the
    /// patch ran from the agent root.
    risk_path_prefixes: Vec<String>,
}

impl<'a> WorkspacePatchToolRequest<'a> {
    /// Builds a patch request from the generic tool runtime context.
    pub(crate) fn from_runtime_context(
        context: crate::gateway::ToolRuntimeExecutionContext<'a>,
        proposal_id: &'a str,
        input_json: &'a [u8],
    ) -> Self {
        Self {
            principal: context.principal,
            device_id: context.device_id,
            channel: context.channel,
            session_id: context.session_id,
            run_id: context.run_id,
            proposal_id,
            input_json,
        }
    }
}

/// Executes the apply_patch tool end to end: validates input, resolves the
/// patch scope, plans the patch (always a dry run first), and either returns
/// the plan (`dry_run`) or hands off to `checkpoint_flow` for the real
/// mutation.
///
/// Never fails as a function; failures are reported in the outcome's error
/// string together with recovery and grammar hints.
pub(crate) async fn execute_workspace_patch_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    request: WorkspacePatchToolRequest<'_>,
) -> ToolExecutionOutcome {
    let WorkspacePatchToolRequest {
        principal,
        device_id,
        channel,
        session_id,
        run_id,
        proposal_id,
        input_json,
    } = request;
    if input_json.len() > MAX_WORKSPACE_PATCH_TOOL_INPUT_BYTES {
        return workspace_patch_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!(
                "palyra.fs.apply_patch input exceeds {MAX_WORKSPACE_PATCH_TOOL_INPUT_BYTES} bytes"
            ),
        );
    }

    let parsed = match serde_json::from_slice::<Value>(input_json) {
        Ok(Value::Object(map)) => map,
        Ok(_) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.fs.apply_patch requires JSON object input".to_owned(),
            );
        }
        Err(error) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.fs.apply_patch invalid JSON input: {error}"),
            );
        }
    };

    let patch = match parsed.get("patch").and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => value.to_owned(),
        _ => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.fs.apply_patch requires non-empty string field 'patch'".to_owned(),
            );
        }
    };
    if let Err(message) = reject_env_prefixed_workspace_patch_paths(patch.as_str()) {
        return workspace_patch_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            message,
        );
    }

    let dry_run = match parsed.get("dry_run") {
        Some(Value::Bool(value)) => *value,
        Some(_) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.fs.apply_patch dry_run must be a boolean".to_owned(),
            );
        }
        None => false,
    };

    let mut redaction_policy = WorkspacePatchRedactionPolicy::default();
    match parse_patch_string_array_field(
        &parsed,
        "redaction_patterns",
        MAX_PATCH_TOOL_REDACTION_PATTERNS,
        MAX_PATCH_TOOL_PATTERN_BYTES,
    ) {
        Ok(Some(patterns)) => {
            extend_patch_string_defaults(&mut redaction_policy.redaction_patterns, patterns);
        }
        Ok(None) => {}
        Err(message) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                message,
            );
        }
    }
    match parse_patch_string_array_field(
        &parsed,
        "secret_file_markers",
        MAX_PATCH_TOOL_SECRET_FILE_MARKERS,
        MAX_PATCH_TOOL_MARKER_BYTES,
    ) {
        Ok(Some(markers)) => {
            extend_patch_string_defaults(&mut redaction_policy.secret_file_markers, markers);
        }
        Ok(None) => {}
        Err(message) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                message,
            );
        }
    }

    let agent_outcome = match runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: principal.to_owned(),
            channel: channel.map(str::to_owned),
            session_id: Some(session_id.to_owned()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!(
                    "palyra.fs.apply_patch failed to resolve agent workspace: {}",
                    error.message()
                ),
            );
        }
    };
    let agent_workspace_roots =
        agent_outcome.agent.workspace_roots.iter().map(PathBuf::from).collect::<Vec<_>>();
    let agent_workspace_roots = workspace_roots_with_run_launch_context_for_agent_source(
        runtime_state,
        run_id,
        agent_workspace_roots.as_slice(),
        agent_outcome.source,
    )
    .await;
    let resolved_workspace_roots = match resolve_workspace_patch_roots(
        runtime_state,
        session_id,
        &parsed,
        patch.as_str(),
        dry_run,
        agent_workspace_roots.as_slice(),
    )
    .await
    {
        Ok(workspace_roots) => workspace_roots,
        Err(message) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                message,
            );
        }
    };
    let workspace_roots = resolved_workspace_roots.roots;
    let canonical_constraint_roots = resolved_workspace_roots.canonical_constraint_roots;
    let risk_path_prefixes = resolved_workspace_roots.risk_path_prefixes;
    let limits = WorkspacePatchLimits::default();
    let (patch, planning_request) = workspace_patch_planning_request(
        patch.as_str(),
        workspace_roots.as_slice(),
        &redaction_policy,
    );

    let planned_outcome = match apply_workspace_patch_with_resolved_roots(
        workspace_roots.as_slice(),
        canonical_constraint_roots.as_slice(),
        &planning_request,
        &limits,
    ) {
        Ok(outcome) => outcome,
        Err(error) => {
            return workspace_patch_error_outcome(
                proposal_id,
                input_json,
                dry_run,
                patch.as_str(),
                &redaction_policy,
                &limits,
                &error,
            );
        }
    };

    if dry_run {
        return serialize_workspace_patch_success(
            proposal_id,
            input_json,
            &planned_outcome,
            workspace_roots.as_slice(),
        );
    }

    checkpoint_flow::execute_workspace_patch_mutation(
        runtime_state,
        WorkspacePatchMutationRequest {
            principal,
            device_id,
            channel,
            session_id,
            run_id,
            proposal_id,
            input_json,
            patch: patch.as_str(),
            redaction_policy: &redaction_policy,
            limits: &limits,
            workspace_roots: workspace_roots.as_slice(),
            canonical_constraint_roots: canonical_constraint_roots.as_slice(),
            risk_path_prefixes: risk_path_prefixes.as_slice(),
            planned_outcome,
        },
    )
    .await
}

fn workspace_patch_planning_request(
    patch: &str,
    workspace_roots: &[PathBuf],
    redaction_policy: &WorkspacePatchRedactionPolicy,
) -> (String, WorkspacePatchRequest) {
    let normalized_patch = normalize_workspace_patch_header_paths(patch, workspace_roots);
    let request = WorkspacePatchRequest {
        patch: normalized_patch.clone(),
        dry_run: true,
        redaction_policy: redaction_policy.clone(),
    };
    (normalized_patch, request)
}

/// Dispatches to the constrained engine entry point when canonical
/// constraint roots are present (narrowed override/focus scope), otherwise
/// to the plain one.
fn apply_workspace_patch_with_resolved_roots(
    workspace_roots: &[PathBuf],
    canonical_constraint_roots: &[PathBuf],
    request: &WorkspacePatchRequest,
    limits: &WorkspacePatchLimits,
) -> Result<WorkspacePatchOutcome, WorkspacePatchError> {
    if canonical_constraint_roots.is_empty() {
        apply_workspace_patch(workspace_roots, request, limits)
    } else {
        apply_workspace_patch_with_canonical_root_constraints(
            workspace_roots,
            canonical_constraint_roots,
            request,
            limits,
        )
    }
}

/// Resolves the scope one patch may write to.
///
/// Precedence: an explicit `workspace_root` override (with the session's
/// active focus spelling honored first), then the session focus when every
/// patch operation path re-roots under it, then the agent roots unchanged.
///
/// # Errors
/// Returns tool-facing errors for invalid overrides and session-state load
/// failures.
async fn resolve_workspace_patch_roots(
    runtime_state: &Arc<GatewayRuntimeState>,
    session_id: &str,
    parsed: &serde_json::Map<String, Value>,
    patch: &str,
    dry_run: bool,
    agent_workspace_roots: &[PathBuf],
) -> Result<ResolvedWorkspacePatchRoots, String> {
    if let Some(value) = parsed.get("workspace_root") {
        let Some(raw_workspace_root) = value.as_str() else {
            return Err("palyra.fs.apply_patch workspace_root must be a string".to_owned());
        };
        let workspace_root = raw_workspace_root.trim();
        if !workspace_root.is_empty() {
            if let Some(active_root) =
                session_active_workspace_root(runtime_state, session_id, agent_workspace_roots)
                    .await?
            {
                if workspace_root_override_targets_active_root(workspace_root, &active_root) {
                    return resolved_active_workspace_patch_roots(
                        active_root.root.as_path(),
                        agent_workspace_roots,
                    );
                }
            }
            return resolve_workspace_root_override(
                agent_workspace_roots,
                workspace_root,
                !dry_run,
            );
        }
    }
    if let Some(active_root) =
        session_active_workspace_root(runtime_state, session_id, agent_workspace_roots).await?
    {
        if patch_should_use_active_root(patch, &active_root) {
            return resolved_active_workspace_patch_roots(
                active_root.root.as_path(),
                agent_workspace_roots,
            );
        }
    }
    Ok(ResolvedWorkspacePatchRoots {
        roots: agent_workspace_roots.to_vec(),
        canonical_constraint_roots: Vec::new(),
        risk_path_prefixes: Vec::new(),
    })
}

/// The focus root applies only when every operation path in the patch
/// re-roots cleanly under it; a single top-level path keeps the patch at the
/// agent roots so files are never silently nested under the focus.
fn patch_should_use_active_root(
    patch: &str,
    active_root: &crate::application::tool_runtime::workspace_scope::ActiveWorkspaceRoot,
) -> bool {
    let operation_paths = patch_operation_paths(patch);
    !operation_paths.is_empty()
        && operation_paths
            .iter()
            .all(|path| relative_path_should_use_active_root(path, active_root))
}

/// Extracts operation target paths (excluding `Move to:` destinations) for
/// the active-root decision.
fn patch_operation_paths(patch: &str) -> Vec<String> {
    patch
        .lines()
        .filter_map(|line| {
            [
                "*** Add File:",
                "*** Update File:",
                "*** Replace File:",
                "*** Replace Line:",
                "*** Delete File:",
            ]
            .iter()
            .find_map(|prefix| line.strip_prefix(prefix).map(str::trim))
        })
        .filter(|path| !path.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

/// Rejects patches whose header paths start with a Palyra OS environment
/// prefix before the patch is even parsed; those targets belong to the
/// OS-file tool, never the workspace patch tool.
///
/// # Errors
/// Returns a tool-facing message naming the offending path.
fn reject_env_prefixed_workspace_patch_paths(patch: &str) -> Result<(), String> {
    for path in workspace_patch_header_paths(patch) {
        if looks_like_palyra_env_prefixed_os_path(path.as_str()) {
            return Err(format!(
                "palyra.fs.apply_patch patch path `{path}` starts with a Palyra OS environment prefix; use palyra.fs.os_file for OS-level paths or pass a workspace-relative path"
            ));
        }
    }
    Ok(())
}

/// Extracts every header path including `Move to:` destinations; used for
/// the OS-env-prefix rejection, which must also cover move targets.
fn workspace_patch_header_paths(patch: &str) -> Vec<String> {
    patch
        .lines()
        .filter_map(|line| {
            [
                "*** Add File:",
                "*** Update File:",
                "*** Replace File:",
                "*** Replace Line:",
                "*** Delete File:",
                "*** Move to:",
            ]
            .iter()
            .find_map(|prefix| line.strip_prefix(prefix).map(str::trim))
        })
        .filter(|path| !path.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn looks_like_palyra_env_prefixed_os_path(path: &str) -> bool {
    path.starts_with("%PALYRA_") || path.starts_with("$PALYRA_") || path.starts_with("${PALYRA_")
}

/// Rewrites patch header paths into root-relative form before parsing:
/// strips duplicated root basenames/tails and converts absolute in-root
/// paths, returning the patch untouched when no line changes.
fn normalize_workspace_patch_header_paths(patch: &str, workspace_roots: &[PathBuf]) -> String {
    let mut changed = false;
    let mut lines = patch
        .split('\n')
        .map(|line| {
            let Some(normalized) = normalize_workspace_patch_header_line(line, workspace_roots)
            else {
                return line.to_owned();
            };
            changed = true;
            normalized
        })
        .collect::<Vec<_>>();
    // split('\n') + join("\n") round-trips newline-terminated patches via the
    // trailing empty segment; the pop is a guard for inputs without one.
    if !patch.ends_with('\n') && lines.last().is_some_and(|line| line.is_empty()) {
        lines.pop();
    }
    if changed {
        lines.join("\n")
    } else {
        patch.to_owned()
    }
}

fn normalize_workspace_patch_header_line(
    line: &str,
    workspace_roots: &[PathBuf],
) -> Option<String> {
    const PATCH_PATH_PREFIXES: &[&str] = &[
        "*** Add File: ",
        "*** Replace File: ",
        "*** Replace Line: ",
        "*** Update File: ",
        "*** Delete File: ",
        "*** Move to: ",
    ];
    for prefix in PATCH_PATH_PREFIXES {
        let Some(raw_path) = line.strip_prefix(prefix) else {
            continue;
        };
        let normalized_path = normalize_workspace_patch_header_path(raw_path, workspace_roots)?;
        return Some(format!("{prefix}{normalized_path}"));
    }
    None
}

/// Maps one header path to its root-relative form: absolute paths must stay
/// lexically inside a root (`..` rejected outright), relative paths get a
/// duplicated root prefix stripped. `None` keeps the original line.
fn normalize_workspace_patch_header_path(
    path: &str,
    workspace_roots: &[PathBuf],
) -> Option<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return None;
    }
    let requested = Path::new(trimmed);
    if requested.is_absolute() {
        if requested.components().any(|component| matches!(component, Component::ParentDir)) {
            return None;
        }
        let comparable_requested =
            canonicalize_existing_header_path(requested).unwrap_or_else(|| requested.to_path_buf());
        return workspace_roots.iter().find_map(|root| {
            if !path_stays_inside_workspace_root_lexical(comparable_requested.as_path(), root) {
                return None;
            }
            absolute_workspace_path_relative_to_root(comparable_requested.as_path(), root)
        });
    }
    workspace_roots
        .iter()
        .find_map(|root| strip_duplicate_workspace_root_prefix(trimmed, root.as_path()))
}

/// Canonicalizes the nearest existing ancestor and re-appends the missing
/// tail, so paths that do not exist yet (Add File targets) still compare
/// against canonical roots.
fn canonicalize_existing_header_path(path: &Path) -> Option<PathBuf> {
    let mut existing = path;
    let mut missing_components = Vec::new();
    while !existing.exists() {
        missing_components.push(existing.file_name()?.to_owned());
        existing = existing.parent()?;
    }

    let mut canonical = std::fs::canonicalize(existing).ok()?;
    for component in missing_components.iter().rev() {
        canonical.push(component);
    }
    Some(canonical)
}

/// Renders an absolute in-root path relative to `root`; Windows falls back
/// to a case-insensitive, separator-normalized comparison.
fn absolute_workspace_path_relative_to_root(path: &Path, root: &Path) -> Option<String> {
    if let Ok(relative) = path.strip_prefix(root) {
        return normalized_relative_path_display(relative);
    }
    #[cfg(windows)]
    {
        let path = comparable_windows_path(path);
        let root = comparable_windows_path(root);
        let suffix = path.strip_prefix(root.as_str()).map(|value| value.trim_start_matches('/'))?;
        if suffix.is_empty() {
            None
        } else {
            normalized_relative_path_display(Path::new(suffix))
        }
    }
    #[cfg(not(windows))]
    {
        None
    }
}

/// Strips the longest suffix of `root` that the relative `path` repeats as a
/// prefix, e.g. `scenario-runs/S037/src/x.ts` under a root ending in
/// `scenario-runs/S037` becomes `src/x.ts`; a path equal to the repeated
/// prefix alone is left untouched.
fn strip_duplicate_workspace_root_prefix(path: &str, root: &Path) -> Option<String> {
    if path.is_empty() || Path::new(path).is_absolute() {
        return None;
    }
    let path_components = normalized_normal_components(Path::new(path))?;
    let root_components = normalized_normal_components(root)?;
    let max_prefix_len = path_components.len().min(root_components.len());
    for prefix_len in (1..=max_prefix_len).rev() {
        if path_components.len() <= prefix_len {
            continue;
        }
        let root_suffix = &root_components[root_components.len() - prefix_len..];
        let path_prefix = &path_components[..prefix_len];
        if root_suffix
            .iter()
            .zip(path_prefix.iter())
            .all(|(left, right)| path_segment_eq(left, right))
        {
            return Some(path_components[prefix_len..].join("/"));
        }
    }
    None
}

fn normalized_normal_components(path: &Path) -> Option<Vec<String>> {
    let mut normalized = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => normalized.push(value.to_string_lossy().into_owned()),
            Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
            Component::ParentDir => return None,
        }
    }
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn path_segment_eq(left: &str, right: &str) -> bool {
    #[cfg(windows)]
    {
        left.eq_ignore_ascii_case(right)
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

/// Lexical (no filesystem access) containment check; Windows compares
/// case-insensitively on normalized separators.
fn path_stays_inside_workspace_root_lexical(path: &Path, root: &Path) -> bool {
    if path.starts_with(root) {
        return true;
    }
    #[cfg(windows)]
    {
        let path = comparable_windows_path(path);
        let root = comparable_windows_path(root);
        path == root || path.starts_with(format!("{root}/").as_str())
    }
    #[cfg(not(windows))]
    {
        false
    }
}

/// Lowercased, forward-slashed, de-verbatimed Windows path key for lexical
/// comparison.
#[cfg(windows)]
fn comparable_windows_path(path: &Path) -> String {
    let normalized = path.to_string_lossy().replace('\\', "/");
    normalized.strip_prefix("//?/").unwrap_or(normalized.as_str()).to_ascii_lowercase()
}

/// Resolves a `workspace_root` override for the patch tool.
///
/// Mirrors the workspace_file override resolution (existing-root basename
/// match first, then join under each root) with one addition: for real
/// writes (`create_missing_relative`), a missing relative override whose
/// nearest existing ancestor canonicalizes in-root is created so a patch can
/// target a fresh project directory. Dry runs never mutate the filesystem.
///
/// # Errors
/// Returns tool-facing errors for control characters, traversal components,
/// escapes, non-directories, and (for dry runs) missing roots.
fn resolve_workspace_root_override(
    agent_workspace_roots: &[PathBuf],
    workspace_root: &str,
    create_missing_relative: bool,
) -> Result<ResolvedWorkspacePatchRoots, String> {
    if workspace_root.chars().any(char::is_control) {
        return Err(
            "palyra.fs.apply_patch workspace_root contains unsupported characters".to_owned()
        );
    }

    let canonical_roots = canonicalize_agent_workspace_roots(agent_workspace_roots)?;
    if canonical_roots.is_empty() {
        return Err("palyra.fs.apply_patch agent has no accessible workspace roots".to_owned());
    }

    let normalized_workspace_root = normalize_workspace_root_override_input(workspace_root);
    let workspace_root = normalized_workspace_root.as_str();
    let requested = Path::new(workspace_root);
    if requested.is_absolute() {
        let root =
            canonicalize_workspace_root_override(requested, &canonical_roots, workspace_root)?;
        let risk_path_prefixes =
            workspace_root_risk_path_prefixes_from_canonical(root.as_path(), &canonical_roots);
        return Ok(ResolvedWorkspacePatchRoots {
            roots: vec![root],
            canonical_constraint_roots: canonical_roots,
            risk_path_prefixes,
        });
    }
    validate_relative_workspace_root_override(requested, workspace_root)?;
    if let Some(root) = workspace_root_override_matching_existing_root_basename(
        requested,
        canonical_roots.as_slice(),
    ) {
        let risk_path_prefixes =
            workspace_root_risk_path_prefixes_from_canonical(root.as_path(), &canonical_roots);
        return Ok(ResolvedWorkspacePatchRoots {
            roots: vec![root],
            canonical_constraint_roots: canonical_roots,
            risk_path_prefixes,
        });
    }
    for canonical_root in &canonical_roots {
        let candidate = canonical_root.join(requested);
        match canonicalize_workspace_root_override(
            candidate.as_path(),
            &canonical_roots,
            workspace_root,
        ) {
            Ok(root) => {
                let risk_path_prefixes = workspace_root_risk_path_prefixes_from_canonical(
                    root.as_path(),
                    &canonical_roots,
                );
                return Ok(ResolvedWorkspacePatchRoots {
                    roots: vec![root],
                    canonical_constraint_roots: canonical_roots.clone(),
                    risk_path_prefixes,
                });
            }
            Err(error) if error.contains("does not exist") => {}
            Err(error) => return Err(error),
        }
    }
    if create_missing_relative {
        let Some(canonical_root) = canonical_roots.first() else {
            return Err("palyra.fs.apply_patch agent has no accessible workspace roots".to_owned());
        };
        let created = create_missing_relative_workspace_root(
            canonical_root,
            requested,
            &canonical_roots,
            workspace_root,
        )?;
        let risk_path_prefixes =
            workspace_root_risk_path_prefixes_from_canonical(created.as_path(), &canonical_roots);
        return Ok(ResolvedWorkspacePatchRoots {
            roots: vec![created],
            canonical_constraint_roots: canonical_roots,
            risk_path_prefixes,
        });
    }
    Err(format!(
        "palyra.fs.apply_patch workspace_root does not exist inside agent workspace roots: {workspace_root}"
    ))
}

/// Matches a single-component override against the basename of an existing
/// canonical root, so naming the root directory itself resolves to that root
/// instead of creating or targeting a same-named subdirectory.
fn workspace_root_override_matching_existing_root_basename(
    requested: &Path,
    canonical_roots: &[PathBuf],
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
        .find(|root| {
            root.file_name().is_some_and(|basename| path_component_eq(basename, component))
        })
        .cloned()
}

/// Normalizes a raw override to `/` separators and strips the `/workspace`
/// virtual alias that models commonly emit for the root.
fn normalize_workspace_root_override_input(workspace_root: &str) -> String {
    let normalized = workspace_root.trim().replace('\\', "/");
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

/// Builds the narrowed scope for the session focus root: the focus becomes
/// the only writable root while the canonical agent roots stay attached as
/// containment constraints.
fn resolved_active_workspace_patch_roots(
    active_root: &Path,
    agent_workspace_roots: &[PathBuf],
) -> Result<ResolvedWorkspacePatchRoots, String> {
    let canonical_constraint_roots = canonicalize_agent_workspace_roots(agent_workspace_roots)?;
    let active_root = fs::canonicalize(active_root).map_err(|error| {
        format!("palyra.fs.apply_patch failed to resolve active workspace root: {error}")
    })?;
    let risk_path_prefixes = workspace_root_risk_path_prefixes_from_canonical(
        active_root.as_path(),
        canonical_constraint_roots.as_slice(),
    );
    Ok(ResolvedWorkspacePatchRoots {
        roots: vec![active_root],
        canonical_constraint_roots,
        risk_path_prefixes,
    })
}

/// Computes the chosen root's path relative to each agent root; risk
/// assessment prepends these so prefix rules (e.g. CI workflow paths) still
/// match when the patch runs from a narrowed root.
fn workspace_root_risk_path_prefixes_from_canonical(
    root: &Path,
    canonical_roots: &[PathBuf],
) -> Vec<String> {
    let mut prefixes = Vec::new();
    for canonical_root in canonical_roots {
        if root == canonical_root {
            continue;
        }
        let Ok(relative) = root.strip_prefix(canonical_root) else {
            continue;
        };
        if let Some(prefix) = normalized_relative_path_display(relative) {
            if !prefixes.contains(&prefix) {
                prefixes.push(prefix);
            }
        }
    }
    prefixes
}

/// Renders a relative path with `/` separators, rejecting traversal/rooted
/// components; `None` for empty or non-normal paths.
fn normalized_relative_path_display(path: &Path) -> Option<String> {
    let mut segments = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(segment) => segments.push(segment.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    if segments.is_empty() {
        None
    } else {
        Some(segments.join("/"))
    }
}

/// Creates a missing relative override directory, but only after proving the
/// nearest existing ancestor canonicalizes inside an agent root; the
/// containment proof must precede `create_dir_all` so nothing is ever
/// created outside the agent roots.
fn create_missing_relative_workspace_root(
    canonical_root: &Path,
    requested: &Path,
    canonical_roots: &[PathBuf],
    workspace_root: &str,
) -> Result<PathBuf, String> {
    let candidate = canonical_root.join(requested);
    let mut nearest_existing = candidate.clone();
    while !nearest_existing.exists() {
        if nearest_existing == *canonical_root || !nearest_existing.pop() {
            return Err(format!(
                "palyra.fs.apply_patch workspace_root does not exist inside agent workspace roots: {workspace_root}"
            ));
        }
    }
    let canonical_existing = fs::canonicalize(nearest_existing.as_path()).map_err(|error| {
        format!("palyra.fs.apply_patch failed to resolve workspace_root {workspace_root}: {error}")
    })?;
    if !canonical_existing.is_dir() {
        return Err(format!(
            "palyra.fs.apply_patch workspace_root parent is not a directory: {workspace_root}"
        ));
    }
    if !canonical_roots.iter().any(|root| canonical_existing.starts_with(root)) {
        return Err(format!(
            "palyra.fs.apply_patch workspace_root escapes agent workspace roots: {workspace_root}"
        ));
    }
    fs::create_dir_all(candidate.as_path()).map_err(|error| {
        format!("palyra.fs.apply_patch failed to create workspace_root {workspace_root}: {error}")
    })?;
    canonicalize_workspace_root_override(candidate.as_path(), canonical_roots, workspace_root)
}

/// Canonicalizes agent roots, silently dropping entries that are missing or
/// not directories (launch roots can vanish); any other IO failure aborts so
/// a root is never skipped because of a transient error.
///
/// # Errors
/// Returns an error when canonicalizing a root fails for a reason other than
/// the root not existing.
fn canonicalize_agent_workspace_roots(
    agent_workspace_roots: &[PathBuf],
) -> Result<Vec<PathBuf>, String> {
    let mut canonical_roots = Vec::with_capacity(agent_workspace_roots.len());
    for root in agent_workspace_roots {
        match fs::canonicalize(root) {
            Ok(canonical_root) if canonical_root.is_dir() => canonical_roots.push(canonical_root),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "palyra.fs.apply_patch failed to resolve agent workspace root {}: {error}",
                    root.display()
                ));
            }
        }
    }
    Ok(canonical_roots)
}

/// Canonicalizes an override candidate and verifies it is a directory inside
/// one of the canonical agent roots.
fn canonicalize_workspace_root_override(
    candidate: &Path,
    canonical_roots: &[PathBuf],
    workspace_root: &str,
) -> Result<PathBuf, String> {
    let canonical_candidate = fs::canonicalize(candidate).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            format!(
                "palyra.fs.apply_patch workspace_root does not exist inside agent workspace roots: {workspace_root}"
            )
        } else {
            format!("palyra.fs.apply_patch failed to resolve workspace_root {workspace_root}: {error}")
        }
    })?;
    if !canonical_candidate.is_dir() {
        return Err(format!(
            "palyra.fs.apply_patch workspace_root is not a directory: {workspace_root}"
        ));
    }
    if canonical_roots.iter().any(|root| canonical_candidate.starts_with(root)) {
        return Ok(canonical_candidate);
    }
    Err(format!(
        "palyra.fs.apply_patch workspace_root escapes agent workspace roots: {workspace_root}"
    ))
}

fn validate_relative_workspace_root_override(
    path: &Path,
    raw_workspace_root: &str,
) -> Result<(), String> {
    for component in path.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(format!(
                    "palyra.fs.apply_patch workspace_root must stay inside agent workspace roots: {raw_workspace_root}"
                ));
            }
        }
    }
    Ok(())
}

/// Serializes a successful (or dry-run) patch outcome into the tool result.
fn serialize_workspace_patch_success(
    proposal_id: &str,
    input_json: &[u8],
    outcome: &WorkspacePatchOutcome,
    workspace_roots: &[PathBuf],
) -> ToolExecutionOutcome {
    let mut output_value = match serde_json::to_value(outcome) {
        Ok(value) => value,
        Err(error) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.fs.apply_patch failed to serialize output: {error}"),
            );
        }
    };
    augment_workspace_patch_output_paths(&mut output_value, workspace_roots);
    match serde_json::to_vec(&output_value) {
        Ok(output_json) => workspace_patch_tool_execution_outcome(
            proposal_id,
            input_json,
            true,
            output_json,
            String::new(),
        ),
        Err(error) => workspace_patch_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.fs.apply_patch failed to serialize output: {error}"),
        ),
    }
}

pub(super) fn augment_workspace_patch_output_paths(
    output_value: &mut Value,
    workspace_roots: &[PathBuf],
) {
    for files_key in ["files_touched", "no_op_files"] {
        let Some(files) = output_value.get_mut(files_key).and_then(Value::as_array_mut) else {
            continue;
        };
        for file in files {
            let Some(file) = file.as_object_mut() else {
                continue;
            };
            let Some(root_index) = file
                .get("workspace_root_index")
                .and_then(Value::as_u64)
                .and_then(|index| usize::try_from(index).ok())
            else {
                continue;
            };
            let Some(relative_path) = file.get("path").and_then(Value::as_str) else {
                continue;
            };
            let Some(root) = workspace_roots.get(root_index) else {
                continue;
            };
            let workspace_root = fs::canonicalize(root).unwrap_or_else(|_| root.clone());
            let resolved_path = workspace_root.join(Path::new(relative_path));
            file.insert(
                "workspace_root".to_owned(),
                Value::String(workspace_root.to_string_lossy().into_owned()),
            );
            file.insert(
                "resolved_path".to_owned(),
                Value::String(resolved_path.to_string_lossy().into_owned()),
            );
        }
    }
}

/// Builds the failure outcome for a patch error: logs once, then returns a
/// diagnostic payload (redacted preview, parse location, recovery and
/// grammar hints) the model can use to self-repair.
fn workspace_patch_error_outcome(
    proposal_id: &str,
    input_json: &[u8],
    dry_run: bool,
    patch: &str,
    redaction_policy: &WorkspacePatchRedactionPolicy,
    limits: &WorkspacePatchLimits,
    error: &WorkspacePatchError,
) -> ToolExecutionOutcome {
    if let Some((line, column)) = error.parse_location() {
        warn!(
            proposal_id = %proposal_id,
            line,
            column,
            error = %error,
            "workspace patch parse failed"
        );
    } else {
        warn!(
            proposal_id = %proposal_id,
            error = %error,
            "workspace patch execution failed"
        );
    }
    let failure_payload = json!({
        "patch_sha256": compute_patch_sha256(patch),
        "dry_run": dry_run,
        "files_touched": [],
        "rollback_performed": error.rollback_performed(),
        "redacted_preview": redact_patch_preview(
            patch,
            redaction_policy,
            limits.max_preview_bytes
        ),
        "parse_error": error
            .parse_location()
            .map(|(line, column)| json!({ "line": line, "column": column })),
        "recovery_hint": workspace_patch_recovery_hint(error),
        "grammar_hint": WORKSPACE_PATCH_GRAMMAR_HINT,
        "error": error.to_string(),
    });
    let output_json = serde_json::to_vec(&failure_payload).unwrap_or_else(|_| b"{}".to_vec());
    workspace_patch_tool_execution_outcome(
        proposal_id,
        input_json,
        false,
        output_json,
        format!(
            "palyra.fs.apply_patch failed: {error}. {} {WORKSPACE_PATCH_GRAMMAR_HINT}",
            workspace_patch_recovery_hint(error)
        ),
    )
}

/// Picks the model-facing recovery instruction for an error class; hints are
/// pinned by tests.
fn workspace_patch_recovery_hint(error: &WorkspacePatchError) -> &'static str {
    match error {
        WorkspacePatchError::Parse { message, .. }
            if message.contains("unexpected content after '*** End Patch'") =>
        {
            "Remove any duplicate terminator or text after the final '*** End Patch', then retry with one complete patch."
        }
        WorkspacePatchError::Parse { message, .. }
            if message.contains("expected '*** Begin Patch'") =>
        {
            "Start the patch with exactly '*** Begin Patch' on its own line, not a Markdown-decorated variant."
        }
        WorkspacePatchError::InvalidJsonFile { .. } => {
            "Read or reconstruct the intended JSON and retry with Replace File or Add File containing complete valid JSON only."
        }
        WorkspacePatchError::HunkApplyFailed { .. } => {
            "Read or search the current file and retry with Replace Line for one unique exact line, fresh context hunks, or Replace File containing the full intended file content."
        }
        WorkspacePatchError::SuspiciousPartialReplace { .. } => {
            "Read the current file and retry with Update File hunks, or use Replace File with the complete intended file content."
        }
        _ => "Inspect the patch error and retry with a smaller complete patch that preserves workspace-relative paths.",
    }
}

/// Appends caller-supplied entries to the policy defaults, skipping
/// duplicates; defaults can only be extended, never removed.
pub(crate) fn extend_patch_string_defaults(defaults: &mut Vec<String>, additions: Vec<String>) {
    for addition in additions {
        if !defaults.contains(&addition) {
            defaults.push(addition);
        }
    }
}

/// Parses an optional string-array tool input field with item-count and
/// per-item byte caps; entries are trimmed and empty ones dropped. `None`
/// means the field was absent.
///
/// # Errors
/// Returns a tool-facing message when the field is not a string array or a
/// cap is exceeded.
pub(crate) fn parse_patch_string_array_field(
    payload: &serde_json::Map<String, Value>,
    field_name: &str,
    max_items: usize,
    max_item_bytes: usize,
) -> Result<Option<Vec<String>>, String> {
    let Some(value) = payload.get(field_name) else {
        return Ok(None);
    };
    let Value::Array(values) = value else {
        return Err(format!("palyra.fs.apply_patch {field_name} must be an array of strings"));
    };
    if values.len() > max_items {
        return Err(format!("palyra.fs.apply_patch {field_name} exceeds limit ({max_items})"));
    }
    let mut parsed = Vec::with_capacity(values.len());
    for value in values {
        let Some(raw) = value.as_str() else {
            return Err(format!("palyra.fs.apply_patch {field_name} must be an array of strings"));
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.len() > max_item_bytes {
            return Err(format!(
                "palyra.fs.apply_patch {field_name} entries must be <= {max_item_bytes} bytes"
            ));
        }
        parsed.push(trimmed.to_owned());
    }
    Ok(Some(parsed))
}

/// Wraps tool output in the standard outcome with a deterministic
/// attestation hash; failures carrying an empty output object get the
/// canonical failed-tool payload so callers always receive diagnostics.
fn workspace_patch_tool_execution_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    let executed_at_unix_ms = current_unix_ms();
    let output_json = if !success && tool_output_json_is_empty_object(output_json.as_slice()) {
        failed_tool_output_json(
            "palyra.fs.apply_patch",
            error.as_str(),
            false,
            "workspace_patch",
            "workspace_roots",
        )
    } else {
        output_json
    };
    // Length-prefixing every field makes the attestation hash unambiguous:
    // no two distinct (proposal, input, output, error) tuples can
    // concatenate to the same byte stream.
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.fs.apply_patch.attestation.v1");
    hasher.update((proposal_id.len() as u64).to_be_bytes());
    hasher.update(proposal_id.as_bytes());
    hasher.update((input_json.len() as u64).to_be_bytes());
    hasher.update(input_json);
    hasher.update([u8::from(success)]);
    hasher.update((output_json.len() as u64).to_be_bytes());
    hasher.update(output_json.as_slice());
    hasher.update((error.len() as u64).to_be_bytes());
    hasher.update(error.as_bytes());
    hasher.update(executed_at_unix_ms.to_be_bytes());
    let execution_sha256 = hex::encode(hasher.finalize());

    ToolExecutionOutcome {
        success,
        output_json,
        error,
        attestation: ToolAttestation {
            attestation_id: Ulid::new().to_string(),
            execution_sha256,
            executed_at_unix_ms,
            timed_out: false,
            executor: "workspace_patch".to_owned(),
            sandbox_enforcement: "workspace_roots".to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_workspace_patch_header_paths, patch_operation_paths,
        patch_should_use_active_root, reject_env_prefixed_workspace_patch_paths,
        resolve_workspace_root_override, serialize_workspace_patch_success,
        workspace_patch_error_outcome, workspace_patch_planning_request,
        workspace_patch_recovery_hint, workspace_patch_tool_execution_outcome,
        WORKSPACE_PATCH_GRAMMAR_HINT,
    };
    use crate::application::tool_runtime::workspace_scope::ActiveWorkspaceRoot;
    use palyra_common::workspace_patch::{
        apply_workspace_patch, apply_workspace_patch_with_canonical_root_constraints,
        WorkspacePatchError, WorkspacePatchLimits, WorkspacePatchRedactionPolicy,
        WorkspacePatchRequest,
    };
    use serde_json::Value;

    #[test]
    fn workspace_patch_validation_failures_return_diagnostic_payload() {
        let outcome = workspace_patch_tool_execution_outcome(
            "01ARZ3NDEKTSV4RRFFQ69G5FA1",
            br#"{"patch":""}"#,
            false,
            b"{}".to_vec(),
            "palyra.fs.apply_patch requires non-empty string field 'patch'".to_owned(),
        );

        assert!(!outcome.success);
        let output =
            serde_json::from_slice::<Value>(outcome.output_json.as_slice()).expect("output JSON");
        assert_eq!(output.get("success").and_then(Value::as_bool), Some(false));
        assert_eq!(output.get("tool").and_then(Value::as_str), Some("palyra.fs.apply_patch"));
        assert!(
            output
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("requires non-empty string field 'patch'"),
            "error should be surfaced in output JSON: {output}"
        );
        assert!(
            output.get("recovery_hint").and_then(Value::as_str).is_some(),
            "diagnostic payload should include a recovery hint: {output}"
        );
        assert!(
            output.get("grammar_hint").and_then(Value::as_str).is_some(),
            "apply_patch diagnostics should include grammar guidance: {output}"
        );
    }

    #[test]
    fn workspace_patch_success_output_includes_resolved_root_paths() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let launch_root = tempdir.path().join("launch");
        let registry_root = tempdir.path().join("state").join("workspace");
        std::fs::create_dir_all(&launch_root).expect("launch root should exist");
        std::fs::create_dir_all(&registry_root).expect("registry root should exist");
        let workspace_roots = vec![launch_root.clone(), registry_root.clone()];

        let outcome = apply_workspace_patch(
            workspace_roots.as_slice(),
            &WorkspacePatchRequest {
                patch: "*** Begin Patch\n*** Add File: math.test.js\n+test('adds', () => {});\n*** End Patch\n"
                    .to_owned(),
                dry_run: false,
                redaction_policy: WorkspacePatchRedactionPolicy::default(),
            },
            &WorkspacePatchLimits::default(),
        )
        .expect("patch should apply under launch root");

        let tool_outcome = serialize_workspace_patch_success(
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            br#"{"patch":"..."}"#,
            &outcome,
            workspace_roots.as_slice(),
        );
        let output = serde_json::from_slice::<Value>(tool_outcome.output_json.as_slice())
            .expect("output JSON should parse");
        let file =
            output["files_touched"][0].as_object().expect("file attestation should be an object");
        let expected_root =
            std::fs::canonicalize(launch_root.as_path()).expect("launch root should canonicalize");
        let expected_file = expected_root.join("math.test.js");
        let expected_root_text = expected_root.to_string_lossy().into_owned();
        let expected_file_text = expected_file.to_string_lossy().into_owned();

        assert_eq!(file["workspace_root_index"], 0);
        assert_eq!(file["workspace_root"].as_str(), Some(expected_root_text.as_str()));
        assert_eq!(file["resolved_path"].as_str(), Some(expected_file_text.as_str()));
        assert!(!registry_root.join("math.test.js").exists());
    }

    #[test]
    fn workspace_root_override_targets_existing_subdirectory() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let project = workspace.join("e2e-cli").join("file-tool-smoke");
        std::fs::create_dir_all(&project).expect("project directory should exist");

        let roots = resolve_workspace_root_override(
            std::slice::from_ref(&workspace),
            "e2e-cli/file-tool-smoke",
            false,
        )
        .expect("workspace root override should resolve");
        assert_eq!(roots.risk_path_prefixes, vec!["e2e-cli/file-tool-smoke"]);
        assert_eq!(
            roots.canonical_constraint_roots,
            vec![std::fs::canonicalize(&workspace).expect("workspace should canonicalize")]
        );
        let patch = "*** Begin Patch\n*** Add File: calc.js\n+export const add = (a, b) => a + b;\n*** End Patch\n";

        apply_workspace_patch_with_canonical_root_constraints(
            roots.roots.as_slice(),
            roots.canonical_constraint_roots.as_slice(),
            &WorkspacePatchRequest {
                patch: patch.to_owned(),
                dry_run: false,
                redaction_policy: Default::default(),
            },
            &WorkspacePatchLimits::default(),
        )
        .expect("patch should apply inside project root");

        assert!(project.join("calc.js").is_file());
        assert!(!workspace.join("calc.js").exists());
    }

    #[test]
    fn workspace_root_override_accepts_virtual_workspace_root_alias() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace directory should exist");

        let roots =
            resolve_workspace_root_override(std::slice::from_ref(&workspace), "/workspace", false)
                .expect("/workspace should resolve to the agent workspace root");

        assert_eq!(
            roots.roots,
            vec![std::fs::canonicalize(&workspace).expect("workspace should canonicalize")]
        );
    }

    #[test]
    fn workspace_root_override_accepts_virtual_workspace_subdirectory_alias() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let project = workspace.join("project");
        std::fs::create_dir_all(&project).expect("project directory should exist");

        let roots = resolve_workspace_root_override(
            std::slice::from_ref(&workspace),
            "/workspace/project",
            false,
        )
        .expect("/workspace/project should resolve inside the agent workspace root");

        assert_eq!(
            roots.roots,
            vec![std::fs::canonicalize(&project).expect("project should canonicalize")]
        );
    }

    #[test]
    fn workspace_root_override_basename_targets_existing_launch_root() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let project = tempdir.path().join("task-workspace");
        std::fs::create_dir_all(&project).expect("project directory should exist");

        let roots =
            resolve_workspace_root_override(std::slice::from_ref(&project), "task-workspace", true)
                .expect("workspace root basename should resolve to the existing project root");

        assert_eq!(
            roots.roots,
            vec![std::fs::canonicalize(&project).expect("project should canonicalize")]
        );
        assert!(
            !project.join("task-workspace").exists(),
            "basename override must not create a nested duplicate project directory"
        );
    }

    #[test]
    fn workspace_root_override_creates_missing_relative_directory_for_write() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace directory should exist");

        let roots = resolve_workspace_root_override(
            std::slice::from_ref(&workspace),
            "agent-browser-smoke",
            true,
        )
        .expect("missing relative workspace root should be created for apply_patch writes");
        let root = roots.roots.first().expect("created root should be returned");

        assert!(root.is_dir());
        assert_eq!(
            roots.canonical_constraint_roots,
            vec![std::fs::canonicalize(&workspace).expect("workspace should canonicalize")]
        );
        assert_eq!(
            root,
            &std::fs::canonicalize(workspace.join("agent-browser-smoke"))
                .expect("created root should canonicalize")
        );
        assert_eq!(roots.risk_path_prefixes, vec!["agent-browser-smoke"]);
    }

    #[test]
    fn workspace_root_override_does_not_create_missing_relative_directory_for_dry_run() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace directory should exist");

        let error = resolve_workspace_root_override(
            std::slice::from_ref(&workspace),
            "web-research-smoke",
            false,
        )
        .expect_err("dry-run planning should not create missing workspace roots");

        assert!(error.contains("does not exist inside agent workspace roots"));
        assert!(
            !workspace.join("web-research-smoke").exists(),
            "dry-run resolution must not mutate the filesystem"
        );
    }

    #[test]
    fn workspace_root_override_rejects_outside_directory() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let outside = tempdir.path().join("outside");
        std::fs::create_dir_all(&workspace).expect("workspace directory should exist");
        std::fs::create_dir_all(&outside).expect("outside directory should exist");

        let error =
            resolve_workspace_root_override(&[workspace], outside.to_string_lossy().as_ref(), true)
                .expect_err("outside workspace_root should be rejected");

        assert!(error.contains("escapes agent workspace roots"), "unexpected error: {error}");
    }

    #[test]
    fn workspace_root_override_rejects_host_directory_even_when_near_workspace_root() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let outside = tempdir.path().join("outside");
        std::fs::create_dir_all(&workspace).expect("workspace directory should exist");
        std::fs::create_dir_all(&outside).expect("outside directory should exist");

        let error =
            resolve_workspace_root_override(&[workspace], outside.to_string_lossy().as_ref(), true)
                .expect_err("host workspace_root should be rejected");

        assert!(error.contains("escapes agent workspace roots"), "unexpected error: {error}");
    }

    #[test]
    fn patch_operation_paths_extracts_add_update_replace_line_delete_targets() {
        let patch = concat!(
            "*** Begin Patch\n",
            "*** Add File: package.json\n",
            "+{}\n",
            "*** Update File: src/index.js\n",
            "@@\n",
            "-old\n",
            "+new\n",
            "*** Replace File: README.md\n",
            "+docs\n",
            "*** Replace Line: public/app.js\n",
            "-old\n",
            "+new\n",
            "*** Delete File: tmp.txt\n",
            "*** End Patch\n",
        );

        assert_eq!(
            patch_operation_paths(patch),
            vec!["package.json", "src/index.js", "README.md", "public/app.js", "tmp.txt"]
        );
    }

    #[test]
    fn workspace_patch_rejects_palyra_env_prefixed_os_paths() {
        for patch in [
            "*** Begin Patch\n*** Add File: %PALYRA_E2E_OS_ROOT%/hosts.d/palyra-e2e.hosts\n+127.0.0.1 palyra.test\n*** End Patch\n",
            "*** Begin Patch\n*** Add File: $PALYRA_E2E_HOME/Desktop/export.csv\n+id,total\n*** End Patch\n",
            "*** Begin Patch\n*** Update File: docs/source.txt\n*** Move to: ${PALYRA_E2E_HOME}/Desktop/source.txt\n*** End Patch\n",
        ] {
            let error = reject_env_prefixed_workspace_patch_paths(patch)
                .expect_err("env-prefixed OS path should be rejected before patch parsing");

            assert!(
                error.contains("palyra.fs.os_file"),
                "error should direct callers to OS-file tool: {error}"
            );
        }
    }

    #[test]
    fn active_workspace_patch_scope_requires_existing_active_parent() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let reports = workspace.join("reports");
        let audit_fixture = workspace.join("audit-fixture");
        std::fs::create_dir_all(reports.as_path()).expect("reports should exist");
        std::fs::create_dir_all(audit_fixture.as_path()).expect("fixture should exist");
        let active = ActiveWorkspaceRoot {
            root: std::fs::canonicalize(reports.as_path()).expect("reports should canonicalize"),
            relative_path: "reports".to_owned(),
        };

        let audit_patch = concat!(
            "*** Begin Patch\n",
            "*** Add File: audit-fixture/alpha.txt\n",
            "+alpha\n",
            "*** End Patch\n",
        );
        let report_patch = concat!(
            "*** Begin Patch\n",
            "*** Add File: summary.md\n",
            "+summary\n",
            "*** End Patch\n",
        );

        assert!(
            !patch_should_use_active_root(audit_patch, &active),
            "top-level workspace paths must not be silently nested under the active focus"
        );
        assert!(
            patch_should_use_active_root(report_patch, &active),
            "single-file writes without an explicit prefix should still target the active focus"
        );
    }

    #[test]
    fn workspace_patch_header_paths_strip_duplicate_root_basename() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("S036_session_recall");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should exist");
        let patch = concat!(
            "*** Begin Patch\n",
            "*** Add File: S036_session_recall/feature_flag.test.ts\n",
            "+test\n",
            "*** End Patch\n",
        );

        let normalized = normalize_workspace_patch_header_paths(patch, &[workspace]);

        assert!(normalized.contains("*** Add File: feature_flag.test.ts"));
        assert!(!normalized.contains("S036_session_recall/feature_flag.test.ts"));
    }

    #[test]
    fn workspace_patch_planning_request_uses_normalized_patch() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("S036_session_recall");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should exist");
        let patch = concat!(
            "*** Begin Patch\n",
            "*** Add File: S036_session_recall/feature_flag.test.ts\n",
            "+test\n",
            "*** End Patch\n",
        );

        let (normalized, request) = workspace_patch_planning_request(
            patch,
            &[workspace],
            &WorkspacePatchRedactionPolicy::default(),
        );

        assert_eq!(request.patch, normalized);
        assert!(request.patch.contains("*** Add File: feature_flag.test.ts"));
        assert!(!request.patch.contains("S036_session_recall/feature_flag.test.ts"));
    }

    #[test]
    fn workspace_patch_header_paths_strip_duplicate_active_root_tail_prefix() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let active_root = tempdir
            .path()
            .join("workspace")
            .join("scenario-runs")
            .join("S037_context_rules_project");
        std::fs::create_dir_all(active_root.as_path()).expect("active root should exist");
        let patch = concat!(
            "*** Begin Patch\n",
            "*** Add File: scenario-runs/S037_context_rules_project/src/slug.ts\n",
            "+export const slug = 'ok';\n",
            "*** Add File: scenario-runs/S037_context_rules_project/reports/S037-result.md\n",
            "+done\n",
            "*** End Patch\n",
        );

        let normalized = normalize_workspace_patch_header_paths(patch, &[active_root]);

        assert!(normalized.contains("*** Add File: src/slug.ts"));
        assert!(normalized.contains("*** Add File: reports/S037-result.md"));
        assert!(!normalized.contains("scenario-runs/S037_context_rules_project/src/slug.ts"));
        assert!(!normalized.contains("scenario-runs/S037_context_rules_project/reports"));
    }

    #[test]
    fn workspace_patch_header_paths_keep_top_level_prefix_for_workspace_root() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should exist");
        let patch = concat!(
            "*** Begin Patch\n",
            "*** Add File: scenario-runs/S037_context_rules_project/src/slug.ts\n",
            "+export const slug = 'ok';\n",
            "*** End Patch\n",
        );

        let normalized = normalize_workspace_patch_header_paths(patch, &[workspace]);

        assert!(normalized
            .contains("*** Add File: scenario-runs/S037_context_rules_project/src/slug.ts"));
    }

    #[test]
    fn workspace_patch_header_paths_accept_absolute_paths_inside_workspace_root() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("S038_shared_r2");
        std::fs::create_dir_all(workspace.join("docs")).expect("workspace docs should exist");
        let absolute_target = workspace.join("docs").join("user-guide.md");
        let patch = format!(
            "*** Begin Patch\n*** Add File: {}\n+guide\n*** End Patch\n",
            absolute_target.display()
        );

        let normalized = normalize_workspace_patch_header_paths(
            patch.as_str(),
            &[std::fs::canonicalize(workspace.as_path()).expect("workspace canonical")],
        );

        assert!(normalized.contains("*** Add File: docs/user-guide.md"));
        assert!(!normalized.contains(absolute_target.to_string_lossy().as_ref()));
    }

    #[test]
    fn parse_failure_result_includes_repairable_patch_grammar_hint() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace directory should exist");
        let limits = WorkspacePatchLimits::default();
        let request = WorkspacePatchRequest {
            patch: "function sum(a, b) { return a + b; }".to_owned(),
            dry_run: true,
            redaction_policy: Default::default(),
        };
        let error = apply_workspace_patch(std::slice::from_ref(&workspace), &request, &limits)
            .expect_err("raw file contents should fail patch parsing");

        let outcome = workspace_patch_error_outcome(
            "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            br#"{"patch":"function sum(a, b) { return a + b; }"}"#,
            true,
            request.patch.as_str(),
            &request.redaction_policy,
            &limits,
            &error,
        );

        assert!(!outcome.success);
        assert!(outcome.error.contains(WORKSPACE_PATCH_GRAMMAR_HINT));
        let payload: Value =
            serde_json::from_slice(outcome.output_json.as_slice()).expect("valid failure json");
        assert_eq!(
            payload.get("grammar_hint").and_then(Value::as_str),
            Some(WORKSPACE_PATCH_GRAMMAR_HINT)
        );
        assert_eq!(payload.pointer("/parse_error/line").and_then(Value::as_u64), Some(1));
    }

    #[test]
    fn json_patch_failure_result_includes_specific_recovery_hint() {
        let error = WorkspacePatchError::InvalidJsonFile {
            path: "reports/seen.json".to_owned(),
            message: "expected value at line 1 column 1".to_owned(),
        };

        let outcome = workspace_patch_error_outcome(
            "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            br#"{"patch":"*** Begin Patch\n*** Add File: reports/seen.json\n+***\n*** End Patch\n"}"#,
            false,
            "*** Begin Patch\n*** Add File: reports/seen.json\n+***\n*** End Patch\n",
            &Default::default(),
            &WorkspacePatchLimits::default(),
            &error,
        );

        let expected_hint = workspace_patch_recovery_hint(&error);

        assert!(!outcome.success);
        assert!(
            outcome.error.contains(expected_hint),
            "expected error to include recovery hint: {}",
            outcome.error
        );
        let payload: Value =
            serde_json::from_slice(outcome.output_json.as_slice()).expect("valid failure json");
        assert_eq!(payload.get("recovery_hint").and_then(Value::as_str), Some(expected_hint));
    }
}
