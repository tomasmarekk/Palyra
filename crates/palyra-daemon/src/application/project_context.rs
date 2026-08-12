//! Discovery and gating of repo-local project context files (AGENTS.md,
//! PALYRA.md, scoped `.palyra/context/*.md`) for prompt injection.
//!
//! For each turn the engine derives "focus paths" from the prompt (explicit
//! `@file:`/`@folder:` references plus absolute/relative workspace paths in
//! prose), walks from each focus directory up to its workspace root, and
//! loads context files in precedence order (compatibility files before the
//! preferred PALYRA.md, parents before deeper directories). Every file is
//! risk-scanned; risky entries require explicit per-session approval and
//! blocked entries never inject. Session state (focus paths, disabled and
//! approved entry ids) persists in the journal. The rendered block is
//! consumed by `application::context_engine` and the project-context
//! console APIs.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use palyra_common::{
    context_references::{parse_context_references, ContextReferenceKind},
    project_context::{
        normalize_project_context_content, project_context_filenames, scan_project_context_content,
        ProjectContextFileKind, ProjectContextRiskAction, ProjectContextRiskScan,
        PREFERRED_PROJECT_CONTEXT_FILENAME,
    },
};
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::{
    agents::AgentResolveRequest,
    domain::workspace::{scan_workspace_content_for_prompt_injection, WorkspaceRiskState},
    gateway::GatewayRuntimeState,
    journal::{
        SessionProjectContextStateCopyRequest, SessionProjectContextStateRecord,
        SessionProjectContextStateUpsertRequest,
    },
    transport::grpc::auth::RequestContext,
};

// Budgets keeping the project-context stack bounded regardless of how many
// context files a workspace accumulates.
const MAX_PROJECT_CONTEXT_FILE_CHARS: usize = 24 * 1_024;
const MAX_PROJECT_CONTEXT_TOTAL_CHARS: usize = 64 * 1_024;
const MAX_PROJECT_CONTEXT_STACK_ENTRIES: usize = 24;

/// Injection state of one context file. Only `Active`, `ActiveWithApproval`,
/// and `Warning` entries reach the prompt; the rest are surfaced to the
/// console for operator action.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProjectContextEntryStatus {
    Active,
    ActiveWithApproval,
    Warning,
    ApprovalRequired,
    Blocked,
    Disabled,
}

impl ProjectContextEntryStatus {
    /// Wire name of the status as stored in [`ProjectContextStackEntry`].
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match *self {
            Self::Active => "active",
            Self::ActiveWithApproval => "active_with_approval",
            Self::Warning => "warning",
            Self::ApprovalRequired => "approval_required",
            Self::Blocked => "blocked",
            Self::Disabled => "disabled",
        }
    }
}

/// Workspace-relative directory the session is focused on, plus the signal
/// that selected it (reference marker, prompt path, or persisted state).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ProjectContextFocusPath {
    pub(crate) path: String,
    pub(crate) reason: String,
}

/// Snapshot of the whole project-context stack for one session: per-status
/// counts, aggregated warnings, the focus paths, and every discovered entry
/// in precedence order. Serialized to the console APIs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ProjectContextPreviewEnvelope {
    pub(crate) generated_at_unix_ms: i64,
    pub(crate) active_estimated_tokens: usize,
    pub(crate) active_entries: usize,
    pub(crate) blocked_entries: usize,
    pub(crate) approval_required_entries: usize,
    pub(crate) disabled_entries: usize,
    pub(crate) warnings: Vec<String>,
    pub(crate) focus_paths: Vec<ProjectContextFocusPath>,
    pub(crate) entries: Vec<ProjectContextStackEntry>,
}

/// One discovered context file with its precedence position, risk scan,
/// session gating flags, and (possibly truncated) content. `entry_id`
/// hashes root + directory + filename -- not the content -- so disable and
/// approve decisions survive file edits.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ProjectContextStackEntry {
    pub(crate) entry_id: String,
    pub(crate) order: usize,
    pub(crate) path: String,
    pub(crate) directory: String,
    pub(crate) source_kind: String,
    pub(crate) source_label: String,
    pub(crate) precedence_label: String,
    pub(crate) depth: usize,
    pub(crate) root: bool,
    pub(crate) active: bool,
    pub(crate) disabled: bool,
    pub(crate) approved: bool,
    pub(crate) status: String,
    pub(crate) estimated_tokens: usize,
    pub(crate) content_hash: String,
    pub(crate) loaded_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) modified_at_unix_ms: Option<i64>,
    pub(crate) byte_size: usize,
    pub(crate) line_count: usize,
    pub(crate) discovery_reasons: Vec<String>,
    pub(crate) warnings: Vec<String>,
    pub(crate) risk: ProjectContextRiskScan,
    pub(crate) preview_text: String,
    pub(crate) resolved_text: String,
}

/// Result of scaffolding a fresh PALYRA.md into the workspace root.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ProjectContextScaffoldOutcome {
    pub(crate) path: String,
    pub(crate) content_hash: String,
    pub(crate) preview_text: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) overwritten: bool,
}

/// Builds the project-context preview for a session, deriving fresh focus
/// paths from `input_text` and merging them with the persisted ones. With
/// `persist_focus_paths` the merged set is written back to session state
/// (skipped when nothing changed to avoid a journal write per turn).
///
/// # Errors
/// Fails with `failed_precondition` when the resolved agent has no
/// workspace roots, and propagates journal and filesystem read failures.
pub(crate) async fn preview_project_context(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    input_text: &str,
    persist_focus_paths: bool,
) -> Result<ProjectContextPreviewEnvelope, Status> {
    let workspace_roots = resolve_workspace_roots(runtime_state, context, session_id).await?;
    let now_unix_ms = crate::unix_ms_now()
        .map_err(|error| Status::internal(format!("failed to read system clock: {error}")))?;
    let mut state = runtime_state
        .session_project_context_state(session_id.to_owned())
        .await?
        .unwrap_or_else(|| SessionProjectContextStateRecord::new(session_id));

    let draft_focus_paths = derive_focus_paths_from_prompt(input_text, workspace_roots.as_slice());
    let next_focus_paths =
        merge_focus_paths(state.focus_paths.as_slice(), draft_focus_paths.as_slice());
    let next_focus_path_strings =
        next_focus_paths.iter().map(|entry| entry.path.clone()).collect::<Vec<_>>();

    if persist_focus_paths && next_focus_path_strings != state.focus_paths {
        state = runtime_state
            .upsert_session_project_context_state(SessionProjectContextStateUpsertRequest {
                session_id: session_id.to_owned(),
                focus_paths: next_focus_path_strings,
                disabled_entry_ids: state.disabled_entry_ids.clone(),
                approved_entry_ids: state.approved_entry_ids.clone(),
                last_refreshed_at_unix_ms: state.last_refreshed_at_unix_ms,
            })
            .await?;
    }

    build_project_context_preview(
        workspace_roots.as_slice(),
        next_focus_paths.as_slice(),
        state.disabled_entry_ids.as_slice(),
        state.approved_entry_ids.as_slice(),
        now_unix_ms,
    )
}

/// Re-reads the stack from disk and stamps `last_refreshed_at_unix_ms` in
/// session state, preserving focus/disable/approve decisions.
///
/// # Errors
/// Same failure modes as [`preview_project_context`].
pub(crate) async fn refresh_project_context(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
) -> Result<ProjectContextPreviewEnvelope, Status> {
    let now_unix_ms = crate::unix_ms_now()
        .map_err(|error| Status::internal(format!("failed to read system clock: {error}")))?;
    let state = runtime_state
        .session_project_context_state(session_id.to_owned())
        .await?
        .unwrap_or_else(|| SessionProjectContextStateRecord::new(session_id));
    runtime_state
        .upsert_session_project_context_state(SessionProjectContextStateUpsertRequest {
            session_id: session_id.to_owned(),
            focus_paths: state.focus_paths.clone(),
            disabled_entry_ids: state.disabled_entry_ids.clone(),
            approved_entry_ids: state.approved_entry_ids.clone(),
            last_refreshed_at_unix_ms: Some(now_unix_ms),
        })
        .await?;
    preview_project_context(runtime_state, context, session_id, "", false).await
}

/// Disables one entry for the session (idempotent) and returns the updated
/// preview.
///
/// # Errors
/// Same failure modes as [`preview_project_context`].
pub(crate) async fn disable_project_context_entry(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    entry_id: &str,
) -> Result<ProjectContextPreviewEnvelope, Status> {
    let state = runtime_state
        .session_project_context_state(session_id.to_owned())
        .await?
        .unwrap_or_else(|| SessionProjectContextStateRecord::new(session_id));
    let mut disabled = state.disabled_entry_ids;
    if !entry_id.trim().is_empty() {
        disabled.push(entry_id.trim().to_owned());
    }
    runtime_state
        .upsert_session_project_context_state(SessionProjectContextStateUpsertRequest {
            session_id: session_id.to_owned(),
            focus_paths: state.focus_paths,
            disabled_entry_ids: normalize_string_set(disabled),
            approved_entry_ids: state.approved_entry_ids,
            last_refreshed_at_unix_ms: state.last_refreshed_at_unix_ms,
        })
        .await?;
    preview_project_context(runtime_state, context, session_id, "", false).await
}

/// Removes one entry from the session's disabled set and returns the
/// updated preview.
///
/// # Errors
/// Same failure modes as [`preview_project_context`].
pub(crate) async fn enable_project_context_entry(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    entry_id: &str,
) -> Result<ProjectContextPreviewEnvelope, Status> {
    let state = runtime_state
        .session_project_context_state(session_id.to_owned())
        .await?
        .unwrap_or_else(|| SessionProjectContextStateRecord::new(session_id));
    let disabled = state
        .disabled_entry_ids
        .into_iter()
        .filter(|candidate| candidate != entry_id.trim())
        .collect::<Vec<_>>();
    runtime_state
        .upsert_session_project_context_state(SessionProjectContextStateUpsertRequest {
            session_id: session_id.to_owned(),
            focus_paths: state.focus_paths,
            disabled_entry_ids: disabled,
            approved_entry_ids: state.approved_entry_ids,
            last_refreshed_at_unix_ms: state.last_refreshed_at_unix_ms,
        })
        .await?;
    preview_project_context(runtime_state, context, session_id, "", false).await
}

/// Marks one approval-gated entry as approved for the session (idempotent)
/// and returns the updated preview. Approval does not override `Blocked`.
///
/// # Errors
/// Same failure modes as [`preview_project_context`].
pub(crate) async fn approve_project_context_entry(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    entry_id: &str,
) -> Result<ProjectContextPreviewEnvelope, Status> {
    let state = runtime_state
        .session_project_context_state(session_id.to_owned())
        .await?
        .unwrap_or_else(|| SessionProjectContextStateRecord::new(session_id));
    let mut approved = state.approved_entry_ids;
    if !entry_id.trim().is_empty() {
        approved.push(entry_id.trim().to_owned());
    }
    runtime_state
        .upsert_session_project_context_state(SessionProjectContextStateUpsertRequest {
            session_id: session_id.to_owned(),
            focus_paths: state.focus_paths,
            disabled_entry_ids: state.disabled_entry_ids,
            approved_entry_ids: normalize_string_set(approved),
            last_refreshed_at_unix_ms: state.last_refreshed_at_unix_ms,
        })
        .await?;
    preview_project_context(runtime_state, context, session_id, "", false).await
}

/// Copies focus/disable/approve state to another session, used when a
/// session is forked or handed off so its gating decisions carry over.
///
/// # Errors
/// Propagates journal failures from the copy.
pub(crate) async fn copy_project_context_state(
    runtime_state: &Arc<GatewayRuntimeState>,
    source_session_id: &str,
    target_session_id: &str,
) -> Result<(), Status> {
    runtime_state
        .copy_session_project_context_state(SessionProjectContextStateCopyRequest {
            source_session_id: source_session_id.to_owned(),
            target_session_id: target_session_id.to_owned(),
        })
        .await
        .map(|_| ())
}

/// Writes the PALYRA.md starter template into the first workspace root,
/// naming the project after `project_name` or the root directory.
///
/// # Errors
/// Fails with `failed_precondition` when the file already exists and
/// `force` is not set or when no workspace root is configured, and with
/// `internal` on clock or write failures.
pub(crate) async fn scaffold_project_context_file(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    project_name: Option<&str>,
    force: bool,
) -> Result<ProjectContextScaffoldOutcome, Status> {
    let workspace_roots = resolve_workspace_roots(runtime_state, context, session_id).await?;
    let root = workspace_roots.first().ok_or_else(|| {
        Status::failed_precondition("no workspace roots are configured for the resolved agent")
    })?;
    let path = root.join(PREFERRED_PROJECT_CONTEXT_FILENAME);
    let existed_before = path.exists();
    if existed_before && !force {
        return Err(Status::failed_precondition(format!(
            "{PREFERRED_PROJECT_CONTEXT_FILENAME} already exists in the workspace root",
        )));
    }
    let now_unix_ms = crate::unix_ms_now()
        .map_err(|error| Status::internal(format!("failed to read system clock: {error}")))?;
    let project_name = project_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| root.file_name().and_then(|value| value.to_str()).map(ToOwned::to_owned))
        .unwrap_or_else(|| "Workspace".to_owned());
    let content = render_project_context_template(project_name.as_str());
    fs::write(path.as_path(), content.as_bytes()).map_err(|error| {
        Status::internal(format!("failed to write {}: {error}", path.to_string_lossy()))
    })?;
    let normalized = normalize_project_context_content(content.as_str());
    Ok(ProjectContextScaffoldOutcome {
        path: PREFERRED_PROJECT_CONTEXT_FILENAME.to_owned(),
        content_hash: normalized.content_hash,
        preview_text: normalized.preview_text,
        created_at_unix_ms: now_unix_ms,
        overwritten: existed_before,
    })
}

/// Renders the active entries as the `<project_context>` prompt block
/// followed by `fallback_prompt`. Returns `None` when nothing is active so
/// callers skip the block entirely.
pub(crate) fn render_project_context_prompt(
    preview: &ProjectContextPreviewEnvelope,
    fallback_prompt: &str,
) -> Option<String> {
    let active_entries = preview.entries.iter().filter(|entry| entry.active).collect::<Vec<_>>();
    if active_entries.is_empty() {
        return None;
    }
    let mut block = String::from("<project_context>\n");
    block.push_str(
        "Deterministic project rules are ordered from broader compatibility files to preferred, more specific files. Later items are more specific.\n",
    );
    block.push_str(
        "Apply active project context as repo-local workspace conventions after system, developer, user, policy, sandbox, and tool-result constraints. Do not silently relax file patterns, language choices, test naming rules, formatting requirements, or documentation style for tool convenience; report the exact blocker before deviating.\n",
    );
    for entry in active_entries {
        block.push_str(
            format!(
                "{}. path={} source={} precedence={} hash={}\n",
                entry.order,
                entry.path,
                entry.source_label,
                entry.precedence_label,
                entry.content_hash
            )
            .as_str(),
        );
        if !entry.discovery_reasons.is_empty() {
            block.push_str(format!("   reasons={}\n", entry.discovery_reasons.join(", ")).as_str());
        }
        block.push_str(entry.resolved_text.as_str());
        if !entry.resolved_text.ends_with('\n') {
            block.push('\n');
        }
        block.push('\n');
    }
    block.push_str("</project_context>\n\n");
    block.push_str(fallback_prompt);
    Some(block)
}

/// Core stack builder: walks the candidate directories in precedence order,
/// loads and risk-scans each context file under the shared budgets, and
/// applies the session's disable/approve decisions.
fn build_project_context_preview(
    workspace_roots: &[PathBuf],
    focus_paths: &[ProjectContextFocusPath],
    disabled_entry_ids: &[String],
    approved_entry_ids: &[String],
    now_unix_ms: i64,
) -> Result<ProjectContextPreviewEnvelope, Status> {
    let mut candidate_directories = discover_candidate_directories(workspace_roots, focus_paths)?;
    // Discovery never returns empty in practice (each root seeds itself),
    // but fall back to the bare roots so the preview cannot go blank.
    if candidate_directories.is_empty() {
        candidate_directories = workspace_roots
            .iter()
            .map(|root| CandidateDirectory {
                root: root.clone(),
                relative_directory: ".".to_owned(),
                directory_path: root.clone(),
                depth: 0,
                reasons: vec!["workspace_root".to_owned()],
            })
            .collect();
    }

    let disabled_ids = disabled_entry_ids.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let approved_ids = approved_entry_ids.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let mut warnings = Vec::new();
    let mut total_chars = 0usize;
    let mut entries = Vec::new();

    for candidate in candidate_directories {
        for file_candidate in project_context_file_candidates(candidate.directory_path.as_path())? {
            let path = candidate.directory_path.join(file_candidate.relative_path.as_path());
            if !path.is_file() {
                continue;
            }
            // Canonicalize both sides so a symlinked context file cannot
            // smuggle content from outside the workspace boundary.
            let canonical_path = path.canonicalize().unwrap_or_else(|_| path.clone());
            let canonical_root =
                candidate.root.canonicalize().unwrap_or_else(|_| candidate.root.clone());
            if !canonical_path.starts_with(canonical_root.as_path()) {
                warnings.push(format!(
                    "Skipped {} because it resolves outside the workspace boundary.",
                    file_candidate.display_path
                ));
                continue;
            }
            let display_path = display_context_path(
                candidate.relative_directory.as_str(),
                file_candidate.display_path.as_str(),
            );
            let raw = fs::read_to_string(path.as_path()).map_err(|error| {
                Status::internal(format!(
                    "failed to read project context file {}: {error}",
                    path.to_string_lossy()
                ))
            })?;
            let normalized = normalize_project_context_content(raw.as_str());
            let remaining_chars = MAX_PROJECT_CONTEXT_TOTAL_CHARS.saturating_sub(total_chars);
            if remaining_chars == 0 {
                warnings.push(
                    "Project context preview reached the maximum total size budget.".to_owned(),
                );
                break;
            }
            let per_file_budget = remaining_chars.min(MAX_PROJECT_CONTEXT_FILE_CHARS);
            let (truncated_text, was_truncated) =
                truncate_project_context_text(normalized.normalized_text.as_str(), per_file_budget);
            if was_truncated {
                warnings.push(format!(
                    "Truncated {} because the project context preview reached its size budget.",
                    display_path
                ));
            }
            total_chars = total_chars.saturating_add(truncated_text.len());

            // The id hashes location, not content, so a session's disable
            // and approve decisions survive edits to the file.
            let entry_id = crate::sha256_hex(
                format!(
                    "{}\n{}\n{}",
                    candidate.root.to_string_lossy(),
                    candidate.relative_directory,
                    file_candidate.display_path
                )
                .as_bytes(),
            );
            let metadata = fs::metadata(path.as_path()).ok();
            // Combine the project-context scanner with the shared workspace
            // prompt-injection scanner; the stricter verdict wins via merge.
            let mut risk = scan_project_context_content(raw.as_str());
            risk = risk.merge(&workspace_risk_as_project_context(raw.as_str()));
            let disabled = disabled_ids.contains(entry_id.as_str());
            let approved = approved_ids.contains(entry_id.as_str());
            let (active, status, mut entry_warnings) =
                evaluate_entry_status(disabled, approved, &risk);
            if !entry_warnings.is_empty() {
                warnings.extend(entry_warnings.clone());
            }
            let estimated_tokens = estimate_text_tokens(truncated_text.as_str());
            let preview_text =
                normalize_project_context_content(truncated_text.as_str()).preview_text;
            entries.push(ProjectContextStackEntry {
                entry_id,
                order: entries.len() + 1,
                path: display_path,
                directory: candidate.relative_directory.clone(),
                source_kind: file_candidate.source_kind,
                source_label: file_candidate.source_label,
                precedence_label: file_candidate.precedence_label,
                depth: candidate.depth,
                root: candidate.depth == 0,
                active,
                disabled,
                approved,
                status: status.as_str().to_owned(),
                estimated_tokens,
                content_hash: normalized.content_hash,
                loaded_at_unix_ms: now_unix_ms,
                modified_at_unix_ms: metadata.and_then(metadata_modified_at_unix_ms),
                byte_size: normalized.byte_size,
                line_count: normalized.line_count,
                discovery_reasons: candidate.reasons.clone(),
                warnings: std::mem::take(&mut entry_warnings),
                risk,
                preview_text,
                resolved_text: truncated_text,
            });
            if entries.len() >= MAX_PROJECT_CONTEXT_STACK_ENTRIES {
                warnings.push(
                    "Project context preview reached the maximum stack entry limit.".to_owned(),
                );
                break;
            }
        }
        if entries.len() >= MAX_PROJECT_CONTEXT_STACK_ENTRIES
            || total_chars >= MAX_PROJECT_CONTEXT_TOTAL_CHARS
        {
            break;
        }
    }

    let active_estimated_tokens =
        entries.iter().filter(|entry| entry.active).map(|entry| entry.estimated_tokens).sum();
    let active_entries = entries.iter().filter(|entry| entry.active).count();
    let blocked_entries = entries
        .iter()
        .filter(|entry| entry.status == ProjectContextEntryStatus::Blocked.as_str())
        .count();
    let approval_required_entries = entries
        .iter()
        .filter(|entry| entry.status == ProjectContextEntryStatus::ApprovalRequired.as_str())
        .count();
    let disabled_entries = entries
        .iter()
        .filter(|entry| entry.status == ProjectContextEntryStatus::Disabled.as_str())
        .count();

    Ok(ProjectContextPreviewEnvelope {
        generated_at_unix_ms: now_unix_ms,
        active_estimated_tokens,
        active_entries,
        blocked_entries,
        approval_required_entries,
        disabled_entries,
        warnings: normalize_string_set(warnings),
        focus_paths: focus_paths.to_vec(),
        entries,
    })
}

fn truncate_project_context_text(input: &str, max_chars: usize) -> (String, bool) {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return (input.to_owned(), false);
    }
    (input.chars().take(max_chars).collect::<String>(), true)
}

/// Derives focus directories from a prompt: explicit workspace-flavored
/// `@` references first, then absolute and relative paths mentioned in
/// prose. First reason wins per path (`BTreeMap::entry` keeps the earliest),
/// so an explicit reference outranks a prose mention of the same path.
fn derive_focus_paths_from_prompt(
    input_text: &str,
    workspace_roots: &[PathBuf],
) -> Vec<ProjectContextFocusPath> {
    let parsed = parse_context_references(input_text);
    let mut reasons_by_path = BTreeMap::<String, String>::new();
    for reference in parsed.references {
        let target = reference.target.as_deref();
        let include = matches!(
            reference.kind,
            ContextReferenceKind::File
                | ContextReferenceKind::Folder
                | ContextReferenceKind::Diff
                | ContextReferenceKind::Staged
        );
        if !include {
            continue;
        }
        let normalized =
            normalize_focus_path(target.unwrap_or(".")).unwrap_or_else(|| ".".to_owned());
        reasons_by_path
            .entry(normalized)
            .or_insert_with(|| format!("@{}", reference.kind.as_str()));
    }
    for focus_path in derive_absolute_workspace_focus_paths(input_text, workspace_roots) {
        reasons_by_path.entry(focus_path).or_insert_with(|| "prompt_workspace_path".to_owned());
    }
    for focus_path in derive_relative_workspace_focus_paths(input_text, workspace_roots) {
        reasons_by_path.entry(focus_path).or_insert_with(|| "prompt_workspace_path".to_owned());
    }
    reasons_by_path
        .into_iter()
        .map(|(path, reason)| ProjectContextFocusPath { path, reason })
        .collect()
}

/// Maps absolute paths mentioned in the prompt to workspace-relative focus
/// directories. Paths outside every workspace root are ignored; the
/// workspace root itself is not a focus (it is always scanned anyway).
fn derive_absolute_workspace_focus_paths(
    input_text: &str,
    workspace_roots: &[PathBuf],
) -> Vec<String> {
    let canonical_roots = workspace_roots
        .iter()
        .filter_map(|root| root.canonicalize().ok().filter(|path| path.is_dir()))
        .collect::<Vec<_>>();
    if canonical_roots.is_empty() {
        return Vec::new();
    }

    let mut focus_paths = BTreeSet::<String>::new();
    for raw_path in extract_absolute_prompt_paths(input_text) {
        let parsed = PathBuf::from(raw_path.as_str());
        if !parsed.is_absolute() {
            continue;
        }
        // The prompt may name a file that does not exist yet ("create
        // project/notes.md"); walking up to the nearest existing ancestor
        // still yields a useful focus directory.
        let existing = nearest_existing_path(parsed.as_path());
        let Some(existing) = existing else {
            continue;
        };
        let canonical = existing.canonicalize().unwrap_or(existing);
        let directory = if canonical.is_dir() {
            canonical
        } else {
            canonical.parent().map(Path::to_path_buf).unwrap_or(canonical)
        };
        for root in &canonical_roots {
            if !directory.starts_with(root) || directory == *root {
                continue;
            }
            let relative = directory.strip_prefix(root).unwrap_or(Path::new(""));
            let display = normalize_directory_display(relative);
            if display != "." {
                focus_paths.insert(display);
            }
        }
    }
    focus_paths.into_iter().collect()
}

/// Like the absolute variant, but for relative paths in prose
/// ("fixtures/scope-limited-replace"): each candidate is joined onto every
/// workspace root and kept only where something exists under that root.
fn derive_relative_workspace_focus_paths(
    input_text: &str,
    workspace_roots: &[PathBuf],
) -> Vec<String> {
    let canonical_roots = workspace_roots
        .iter()
        .filter_map(|root| root.canonicalize().ok().filter(|path| path.is_dir()))
        .collect::<Vec<_>>();
    if canonical_roots.is_empty() {
        return Vec::new();
    }

    let mut focus_paths = BTreeSet::<String>::new();
    for raw_path in extract_relative_prompt_paths(input_text) {
        let Some(relative_path) = normalize_relative_prompt_path(raw_path.as_str()) else {
            continue;
        };
        for root in &canonical_roots {
            let candidate = root.join(relative_path.as_str());
            let Some(existing) = nearest_existing_path(candidate.as_path()) else {
                continue;
            };
            let canonical = existing.canonicalize().unwrap_or(existing);
            let directory = if canonical.is_dir() {
                canonical
            } else {
                canonical.parent().map(Path::to_path_buf).unwrap_or(canonical)
            };
            if !directory.starts_with(root) || directory == *root {
                continue;
            }
            let relative = directory.strip_prefix(root).unwrap_or(Path::new(""));
            let display = normalize_directory_display(relative);
            if display != "." {
                focus_paths.insert(display);
            }
        }
    }
    focus_paths.into_iter().collect()
}

/// Walks up the ancestor chain to the closest path that exists on disk.
fn nearest_existing_path(path: &Path) -> Option<PathBuf> {
    let mut cursor = path.to_path_buf();
    loop {
        if cursor.exists() {
            return Some(cursor);
        }
        if !cursor.pop() {
            return None;
        }
    }
}

/// Scans prose for absolute path candidates: `/unix/style` and
/// `C:\windows\style` starts at a word boundary, terminated by whitespace
/// or quote characters (a leading quote extends the path until its closing
/// quote, allowing embedded spaces). Cursor arithmetic steps by
/// `len_utf8`, so every slice index stays on a char boundary.
fn extract_absolute_prompt_paths(input_text: &str) -> Vec<String> {
    let mut paths = Vec::new();
    let mut cursor = 0usize;
    while cursor < input_text.len() {
        let Some((relative_start, _)) = input_text[cursor..]
            .char_indices()
            .find(|(index, ch)| looks_like_prompt_path_start(input_text, cursor + *index, *ch))
        else {
            break;
        };
        let start = cursor + relative_start;
        let quote = preceding_quote(input_text, start);
        let mut end = start;
        while end < input_text.len() {
            let ch = input_text[end..].chars().next().unwrap_or_default();
            if prompt_path_terminates(ch, quote) {
                break;
            }
            end = end.saturating_add(ch.len_utf8());
        }
        let candidate = trim_prompt_path_candidate(&input_text[start..end]);
        if !candidate.is_empty() {
            paths.push(candidate.to_owned());
        }
        let terminator_len = input_text[end..].chars().next().map(char::len_utf8).unwrap_or(0);
        cursor = end.saturating_add(terminator_len);
    }
    paths
}

/// Companion to [`extract_absolute_prompt_paths`] for relative candidates;
/// a token only counts as a path once it contains a separator, so ordinary
/// words are never treated as focus paths.
fn extract_relative_prompt_paths(input_text: &str) -> Vec<String> {
    let mut paths = Vec::new();
    let mut cursor = 0usize;
    while cursor < input_text.len() {
        let Some((relative_start, _)) = input_text[cursor..].char_indices().find(|(index, ch)| {
            looks_like_relative_prompt_path_start(input_text, cursor + *index, *ch)
        }) else {
            break;
        };
        let start = cursor + relative_start;
        let quote = preceding_quote(input_text, start);
        let mut end = start;
        let mut saw_separator = false;
        while end < input_text.len() {
            let ch = input_text[end..].chars().next().unwrap_or_default();
            if prompt_path_terminates(ch, quote) {
                break;
            }
            saw_separator |= matches!(ch, '/' | '\\');
            end = end.saturating_add(ch.len_utf8());
        }
        let candidate = trim_prompt_path_candidate(&input_text[start..end]);
        if saw_separator && !candidate.is_empty() {
            paths.push(candidate.to_owned());
        }
        let terminator_len = input_text[end..].chars().next().map(char::len_utf8).unwrap_or(0);
        cursor = end.saturating_add(terminator_len);
    }
    paths
}

fn looks_like_relative_prompt_path_start(input_text: &str, start: usize, ch: char) -> bool {
    if matches!(ch, '/' | '\\') {
        return false;
    }
    if !matches!(ch, '.' | '_' | '-' | 'A'..='Z' | 'a'..='z' | '0'..='9') {
        return false;
    }
    prompt_path_start_has_boundary(input_text, start)
}

fn looks_like_prompt_path_start(input_text: &str, start: usize, ch: char) -> bool {
    if ch == '/' {
        return prompt_path_start_has_boundary(input_text, start)
            && input_text[start + ch.len_utf8()..]
                .chars()
                .next()
                .is_some_and(|next| !next.is_whitespace() && !matches!(next, '/' | '\\'));
    }
    if !ch.is_ascii_alphabetic() {
        return false;
    }
    let bytes = input_text.as_bytes();
    prompt_path_start_has_boundary(input_text, start)
        && bytes.get(start + 1) == Some(&b':')
        && matches!(bytes.get(start + 2), Some(b'\\' | b'/'))
}

fn prompt_path_start_has_boundary(input_text: &str, start: usize) -> bool {
    start == 0
        || input_text[..start].chars().next_back().is_some_and(|ch| {
            ch.is_whitespace() || matches!(ch, '"' | '\'' | '`' | '(' | '[' | '{' | '<' | '=')
        })
}

fn preceding_quote(input_text: &str, start: usize) -> Option<char> {
    input_text[..start].chars().next_back().filter(|ch| matches!(ch, '"' | '\''))
}

fn prompt_path_terminates(ch: char, quote: Option<char>) -> bool {
    if Some(ch) == quote {
        return true;
    }
    quote.is_none() && (ch.is_whitespace() || matches!(ch, '"' | '\'' | '`' | '<' | '>' | '|'))
}

fn trim_prompt_path_candidate(candidate: &str) -> &str {
    candidate.trim().trim_matches(['.', ',', ';', ':', '!', '?', ')', ']', '}'])
}

/// Normalizes a relative prose path to forward slashes. Rejects URLs and
/// anything with a colon (drive letters, schemes), absolute paths, bare
/// single-segment names, and any traversal component.
fn normalize_relative_prompt_path(path: &str) -> Option<String> {
    let normalized = path.trim().replace('\\', "/");
    if normalized.is_empty()
        || normalized.contains("://")
        || normalized.contains(':')
        || normalized.starts_with('/')
    {
        return None;
    }
    let trimmed = normalized.trim_start_matches("./").trim_matches('/');
    if trimmed.is_empty() || !trimmed.contains('/') {
        return None;
    }
    let parsed = Path::new(trimmed);
    if parsed.is_absolute() {
        return None;
    }
    let mut components = Vec::new();
    for component in parsed.components() {
        match component {
            Component::Normal(segment) => components.push(segment.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    (!components.is_empty()).then(|| components.join("/"))
}

/// Merges persisted focus paths with newly derived ones; persisted entries
/// keep their original reason and derived ones never displace them.
fn merge_focus_paths(
    existing: &[String],
    derived: &[ProjectContextFocusPath],
) -> Vec<ProjectContextFocusPath> {
    let mut reasons_by_path = BTreeMap::<String, String>::new();
    for path in existing {
        if let Some(normalized) = normalize_focus_path(path) {
            reasons_by_path.entry(normalized).or_insert_with(|| "persisted_focus".to_owned());
        }
    }
    for entry in derived {
        reasons_by_path.entry(entry.path.clone()).or_insert_with(|| entry.reason.clone());
    }
    reasons_by_path
        .into_iter()
        .map(|(path, reason)| ProjectContextFocusPath { path, reason })
        .collect()
}

fn normalize_focus_path(path: &str) -> Option<String> {
    let normalized = path.trim().replace('\\', "/");
    if normalized.is_empty() {
        return None;
    }
    if normalized == "." {
        return Some(".".to_owned());
    }
    let trimmed = normalized.trim_start_matches("./").trim_matches('/');
    if trimmed.is_empty() {
        Some(".".to_owned())
    } else {
        Some(trimmed.to_owned())
    }
}

/// Directory that may contribute context files, with the discovery reasons
/// that selected it.
#[derive(Debug, Clone)]
struct CandidateDirectory {
    root: PathBuf,
    relative_directory: String,
    directory_path: PathBuf,
    depth: usize,
    reasons: Vec<String>,
}

/// Collects every directory whose context files apply: each workspace root
/// plus, for each focus path, the focus directory and all of its ancestors
/// up to the root -- a deeper file inherits its parents' conventions. The
/// final root/depth/name sort yields the injection order (broader context
/// before more specific context).
fn discover_candidate_directories(
    workspace_roots: &[PathBuf],
    focus_paths: &[ProjectContextFocusPath],
) -> Result<Vec<CandidateDirectory>, Status> {
    let mut directories = BTreeMap::<(String, String), CandidateDirectory>::new();
    for root in workspace_roots {
        let canonical_root = root.canonicalize().unwrap_or_else(|_| root.clone());
        upsert_candidate_directory(
            &mut directories,
            CandidateDirectory {
                root: canonical_root.clone(),
                relative_directory: ".".to_owned(),
                directory_path: canonical_root.clone(),
                depth: 0,
                reasons: vec!["workspace_root".to_owned()],
            },
        );
        for focus in focus_paths {
            if let Some((focus_directory, relative_directory, depth)) =
                resolve_focus_directory(canonical_root.as_path(), focus.path.as_str())?
            {
                let mut cursor = focus_directory.as_path();
                let mut relative_path = PathBuf::from(relative_directory.as_str());
                let mut current_depth = depth;
                loop {
                    upsert_candidate_directory(
                        &mut directories,
                        CandidateDirectory {
                            root: canonical_root.clone(),
                            relative_directory: normalize_directory_display(
                                relative_path.as_path(),
                            ),
                            directory_path: cursor.to_path_buf(),
                            depth: current_depth,
                            reasons: vec![format!("{}:{}", focus.reason, focus.path)],
                        },
                    );
                    if current_depth == 0 || cursor == canonical_root.as_path() {
                        break;
                    }
                    cursor = cursor.parent().ok_or_else(|| {
                        Status::internal(
                            "project context ancestor resolution escaped the workspace",
                        )
                    })?;
                    relative_path = relative_path
                        .parent()
                        .map(Path::to_path_buf)
                        .unwrap_or_else(|| PathBuf::from("."));
                    current_depth = current_depth.saturating_sub(1);
                }
            }
        }
    }

    let mut sorted = directories.into_values().collect::<Vec<_>>();
    sorted.sort_by(|left, right| {
        left.root
            .cmp(&right.root)
            .then(left.depth.cmp(&right.depth))
            .then(left.relative_directory.cmp(&right.relative_directory))
    });
    Ok(sorted)
}

/// Inserts or merges a candidate, accumulating discovery reasons when the
/// same directory is reached through several focus paths.
fn upsert_candidate_directory(
    directories: &mut BTreeMap<(String, String), CandidateDirectory>,
    candidate: CandidateDirectory,
) {
    let key = (candidate.root.to_string_lossy().into_owned(), candidate.relative_directory.clone());
    let entry = directories.entry(key).or_insert(candidate.clone());
    let mut reasons = entry.reasons.clone();
    reasons.extend(candidate.reasons);
    entry.reasons = normalize_string_set(reasons);
}

/// Resolves a focus path inside `root` to `(canonical directory, display
/// path, depth)`. A focus naming a file resolves to its parent directory;
/// traversal, absolute components, and canonicalized escapes are rejected.
fn resolve_focus_directory(
    root: &Path,
    relative_focus_path: &str,
) -> Result<Option<(PathBuf, String, usize)>, Status> {
    let normalized_focus =
        normalize_focus_path(relative_focus_path).unwrap_or_else(|| ".".to_owned());
    if normalized_focus == "." {
        return Ok(Some((root.to_path_buf(), ".".to_owned(), 0)));
    }

    let mut candidate = root.to_path_buf();
    for component in Path::new(normalized_focus.as_str()).components() {
        match component {
            Component::Normal(value) => candidate.push(value),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(Status::invalid_argument(
                    "project context focus path cannot escape the workspace root",
                ));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(Status::invalid_argument(
                    "project context focus path must stay relative to the workspace root",
                ));
            }
        }
    }

    let chosen = if candidate.is_dir() {
        candidate
    } else {
        candidate.parent().unwrap_or(root).to_path_buf()
    };
    let canonical = chosen.canonicalize().unwrap_or(chosen);
    if !canonical.starts_with(root) {
        return Err(Status::invalid_argument(
            "project context focus path escaped the workspace root",
        ));
    }
    let relative = canonical.strip_prefix(root).unwrap_or(Path::new("")).to_path_buf();
    let display = normalize_directory_display(relative.as_path());
    let depth = if display == "." { 0 } else { display.split('/').count() };
    Ok(Some((canonical, display, depth)))
}

/// Renders a relative directory with forward slashes, using "." for the
/// root itself.
fn normalize_directory_display(path: &Path) -> String {
    let rendered = path
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => value.to_str().map(ToOwned::to_owned),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/");
    if rendered.is_empty() {
        ".".to_owned()
    } else {
        rendered
    }
}

/// Known context filenames sorted by precedence rank, so compatibility
/// files (AGENTS.md) load before the preferred PALYRA.md within a
/// directory and later entries win on conflict.
fn ordered_project_context_kinds() -> Vec<ProjectContextFileKind> {
    let mut kinds = project_context_filenames()
        .iter()
        .filter_map(|name| ProjectContextFileKind::from_filename(name))
        .collect::<Vec<_>>();
    kinds.sort_by_key(|kind| kind.precedence_rank());
    kinds
}

/// Context file that may exist in a candidate directory, with its labels.
#[derive(Debug, Clone)]
struct ProjectContextFileCandidate {
    relative_path: PathBuf,
    display_path: String,
    source_kind: String,
    source_label: String,
    precedence_label: String,
}

/// Lists the well-known context filenames plus any scoped
/// `.palyra/context/*.md` documents (sorted for deterministic order) found
/// in this directory.
fn project_context_file_candidates(
    directory_path: &Path,
) -> Result<Vec<ProjectContextFileCandidate>, Status> {
    let mut candidates = ordered_project_context_kinds()
        .into_iter()
        .map(|kind| ProjectContextFileCandidate {
            relative_path: PathBuf::from(kind.display_name()),
            display_path: kind.display_name().to_owned(),
            source_kind: kind.as_str().to_owned(),
            source_label: kind.display_name().to_owned(),
            precedence_label: kind.precedence_label().to_owned(),
        })
        .collect::<Vec<_>>();

    let scoped_context_dir = directory_path.join(".palyra").join("context");
    if scoped_context_dir.is_dir() {
        let mut scoped_files = fs::read_dir(scoped_context_dir.as_path())
            .map_err(|error| {
                Status::internal(format!(
                    "failed to read scoped project context directory {}: {error}",
                    scoped_context_dir.to_string_lossy()
                ))
            })?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let path = entry.path();
                let file_name = path.file_name()?.to_str()?.to_owned();
                (path.is_file() && file_name.to_ascii_lowercase().ends_with(".md"))
                    .then_some(file_name)
            })
            .collect::<Vec<_>>();
        scoped_files.sort();
        scoped_files.dedup();
        candidates.extend(scoped_files.into_iter().map(|file_name| {
            let display_path = format!(".palyra/context/{file_name}");
            ProjectContextFileCandidate {
                relative_path: PathBuf::from(".palyra").join("context").join(&file_name),
                display_path: display_path.clone(),
                source_kind: "palyra_context_md".to_owned(),
                source_label: display_path,
                precedence_label: "workspace_scoped".to_owned(),
            }
        }));
    }
    Ok(candidates)
}

/// Maps session flags and the risk verdict to `(active, status, warnings)`.
/// Precedence: a session disable beats everything; then the risk action
/// decides, with `ApprovalRequired` activating only when the session
/// explicitly approved the entry. `Blocked` cannot be approved around.
fn evaluate_entry_status(
    disabled: bool,
    approved: bool,
    risk: &ProjectContextRiskScan,
) -> (bool, ProjectContextEntryStatus, Vec<String>) {
    if disabled {
        return (
            false,
            ProjectContextEntryStatus::Disabled,
            vec!["This project context file is temporarily disabled for the active session."
                .to_owned()],
        );
    }
    match risk.recommended_action {
        ProjectContextRiskAction::Allow => (true, ProjectContextEntryStatus::Active, Vec::new()),
        ProjectContextRiskAction::Warning => (
            true,
            ProjectContextEntryStatus::Warning,
            vec!["This project context file contains warnings and should be reviewed.".to_owned()],
        ),
        ProjectContextRiskAction::ApprovalRequired if approved => (
            true,
            ProjectContextEntryStatus::ActiveWithApproval,
            vec!["This project context file is active because it was explicitly approved for the session.".to_owned()],
        ),
        ProjectContextRiskAction::ApprovalRequired => (
            false,
            ProjectContextEntryStatus::ApprovalRequired,
            vec!["This project context file requires explicit approval before it can be injected.".to_owned()],
        ),
        ProjectContextRiskAction::Blocked => (
            false,
            ProjectContextEntryStatus::Blocked,
            vec!["This project context file is blocked and will not be injected.".to_owned()],
        ),
    }
}

/// Translates the shared workspace prompt-injection scanner's verdict into
/// project-context risk vocabulary (warning stays warning, quarantine
/// becomes blocked) so both scanners can be merged.
fn workspace_risk_as_project_context(content: &str) -> ProjectContextRiskScan {
    let workspace_scan = scan_workspace_content_for_prompt_injection(content);
    match workspace_scan.state {
        WorkspaceRiskState::Clean => ProjectContextRiskScan::default(),
        WorkspaceRiskState::Warning => {
            let mut scan = ProjectContextRiskScan::default();
            for reason in workspace_scan.reasons {
                scan.push(
                    ProjectContextRiskAction::Warning,
                    format!("workspace:{reason}").as_str(),
                    "Workspace prompt-injection warning",
                    "The shared workspace scanner flagged this content as suspicious.",
                    Some(reason),
                );
            }
            scan
        }
        WorkspaceRiskState::Quarantined => {
            let mut scan = ProjectContextRiskScan::default();
            for reason in workspace_scan.reasons {
                scan.push(
                    ProjectContextRiskAction::Blocked,
                    format!("workspace:{reason}").as_str(),
                    "Workspace prompt-injection block",
                    "The shared workspace scanner blocked this content as high risk.",
                    Some(reason),
                );
            }
            scan
        }
    }
}

fn display_context_path(directory: &str, file_name: &str) -> String {
    if directory == "." {
        file_name.to_owned()
    } else {
        format!("{directory}/{file_name}")
    }
}

fn metadata_modified_at_unix_ms(metadata: fs::Metadata) -> Option<i64> {
    metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
        // Millisecond timestamps fit i64 for any realistic clock; the cast
        // is intentional narrowing from u128.
        .map(|duration| duration.as_millis() as i64)
}

/// Token heuristic for context files: the larger of the word count and a
/// bytes/4 floor, so both prose and dense markup are bounded from above.
fn estimate_text_tokens(text: &str) -> usize {
    text.split_whitespace().count().max(text.len() / 4)
}

/// Trims, drops empties, deduplicates, and sorts -- the canonical form for
/// persisted id/warning lists so comparisons are order-insensitive.
fn normalize_string_set(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Starter PALYRA.md content used by [`scaffold_project_context_file`].
fn render_project_context_template(project_name: &str) -> String {
    format!(
        "# PALYRA.md\n\n## Purpose\n- Summarize what `{project_name}` does and what outcome matters most for changes in this repo.\n\n## Working Agreements\n- Describe repo-specific guardrails, release expectations, and quality bars.\n- Call out the canonical commands or checks that must pass before a change is considered done.\n\n## Architecture Notes\n- Explain where the primary modules live and where new code should usually go.\n- Mention integration boundaries, generated files, or other areas that require extra care.\n\n## Definition of Done\n- List the tests, lint, typecheck, or manual verifications expected for normal changes.\n- Mention any security, migration, or rollout checks that cannot be skipped.\n\n## Out Of Scope\n- Record changes that should not be made casually in this repository.\n- Capture known anti-patterns or compatibility constraints.\n"
    )
}

/// Resolves the session agent's workspace roots, canonicalized up front so
/// every later `starts_with` boundary check compares canonical forms.
async fn resolve_workspace_roots(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
) -> Result<Vec<PathBuf>, Status> {
    let resolved = runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: context.principal.clone(),
            channel: context.channel.clone(),
            session_id: Some(session_id.to_owned()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await?;
    let roots = resolved
        .agent
        .workspace_roots
        .iter()
        .map(PathBuf::from)
        .map(|path| path.canonicalize().unwrap_or(path))
        .collect::<Vec<_>>();
    if roots.is_empty() {
        return Err(Status::failed_precondition(
            "no workspace roots are configured for the resolved agent",
        ));
    }
    Ok(roots)
}

#[cfg(test)]
mod tests {
    use super::{
        build_project_context_preview, derive_focus_paths_from_prompt,
        extract_absolute_prompt_paths, normalize_focus_path, render_project_context_prompt,
        ProjectContextEntryStatus, ProjectContextFocusPath, ProjectContextPreviewEnvelope,
        ProjectContextStackEntry,
    };
    use palyra_common::project_context::ProjectContextRiskScan;
    use std::{fs, path::PathBuf};
    use ulid::Ulid;

    fn temp_project_context_root() -> PathBuf {
        std::env::temp_dir().join(format!("palyra-project-context-{}", Ulid::generate()))
    }

    #[test]
    fn derives_focus_paths_from_file_and_folder_references() {
        let focus = derive_focus_paths_from_prompt(
            "Review @file:apps/web/src/App.tsx and @folder:crates/palyra-daemon/src",
            &[],
        );
        assert_eq!(focus.len(), 2);
        assert_eq!(focus[0].path, "apps/web/src/App.tsx");
        assert_eq!(focus[1].path, "crates/palyra-daemon/src");
    }

    #[test]
    fn derives_focus_path_from_absolute_workspace_prompt_path() {
        let root = temp_project_context_root();
        let project = root.join("notes-api");
        fs::create_dir_all(project.as_path()).expect("project directory should exist");
        let prompt = format!("Work only inside {}. Create package.json there.", project.display());

        let focus = derive_focus_paths_from_prompt(prompt.as_str(), std::slice::from_ref(&root));

        assert_eq!(focus.len(), 1);
        assert_eq!(focus[0].path, "notes-api");
        assert_eq!(focus[0].reason, "prompt_workspace_path");
    }

    #[test]
    fn derives_focus_path_from_relative_workspace_prompt_path() {
        let root = temp_project_context_root();
        let project = root.join("fixtures").join("scope-limited-replace");
        fs::create_dir_all(project.join("src").as_path()).expect("project directory should exist");
        let prompt = "Work only in fixtures/scope-limited-replace. \
            Update src/public/banner.txt and create reports/scope-report.md.";

        let focus = derive_focus_paths_from_prompt(prompt, std::slice::from_ref(&root));

        assert_eq!(focus.len(), 1);
        assert_eq!(focus[0].path, "fixtures/scope-limited-replace");
        assert_eq!(focus[0].reason, "prompt_workspace_path");
    }

    #[test]
    fn extracts_unix_prompt_paths_followed_by_multibyte_whitespace() {
        let paths = extract_absolute_prompt_paths("/tmp\u{00a0}/var/log");

        assert_eq!(paths, vec!["/tmp".to_owned(), "/var/log".to_owned()]);
    }

    #[test]
    fn normalizes_focus_paths() {
        assert_eq!(normalize_focus_path("./apps/web"), Some("apps/web".to_owned()));
        assert_eq!(normalize_focus_path(" . "), Some(".".to_owned()));
    }

    #[test]
    fn render_prompt_uses_only_active_entries() {
        let prompt = render_project_context_prompt(
            &ProjectContextPreviewEnvelope {
                generated_at_unix_ms: 0,
                active_estimated_tokens: 12,
                active_entries: 1,
                blocked_entries: 0,
                approval_required_entries: 0,
                disabled_entries: 0,
                warnings: Vec::new(),
                focus_paths: vec![ProjectContextFocusPath {
                    path: ".".to_owned(),
                    reason: "workspace_root".to_owned(),
                }],
                entries: vec![
                    ProjectContextStackEntry {
                        entry_id: "one".to_owned(),
                        order: 1,
                        path: "PALYRA.md".to_owned(),
                        directory: ".".to_owned(),
                        source_kind: "palyra_md".to_owned(),
                        source_label: "PALYRA.md".to_owned(),
                        precedence_label: "preferred".to_owned(),
                        depth: 0,
                        root: true,
                        active: true,
                        disabled: false,
                        approved: false,
                        status: ProjectContextEntryStatus::Active.as_str().to_owned(),
                        estimated_tokens: 12,
                        content_hash: "hash".to_owned(),
                        loaded_at_unix_ms: 0,
                        modified_at_unix_ms: None,
                        byte_size: 12,
                        line_count: 1,
                        discovery_reasons: vec!["workspace_root".to_owned()],
                        warnings: Vec::new(),
                        risk: ProjectContextRiskScan::default(),
                        preview_text: "rule".to_owned(),
                        resolved_text: "rule".to_owned(),
                    },
                    ProjectContextStackEntry {
                        entry_id: "two".to_owned(),
                        order: 2,
                        path: "AGENTS.md".to_owned(),
                        directory: ".".to_owned(),
                        source_kind: "agents_md".to_owned(),
                        source_label: "AGENTS.md".to_owned(),
                        precedence_label: "compatibility_primary".to_owned(),
                        depth: 0,
                        root: true,
                        active: false,
                        disabled: true,
                        approved: false,
                        status: ProjectContextEntryStatus::Disabled.as_str().to_owned(),
                        estimated_tokens: 0,
                        content_hash: "hash".to_owned(),
                        loaded_at_unix_ms: 0,
                        modified_at_unix_ms: None,
                        byte_size: 0,
                        line_count: 0,
                        discovery_reasons: vec!["workspace_root".to_owned()],
                        warnings: Vec::new(),
                        risk: ProjectContextRiskScan::default(),
                        preview_text: String::new(),
                        resolved_text: "disabled".to_owned(),
                    },
                ],
            },
            "Explain the issue.",
        )
        .expect("project context prompt should render");
        assert!(prompt.contains("<project_context>"));
        assert!(prompt.contains("repo-local workspace conventions"));
        assert!(prompt.contains("Do not silently relax file patterns"));
        assert!(prompt.contains("report the exact blocker before deviating"));
        assert!(prompt.contains("PALYRA.md"));
        assert!(!prompt.contains("disabled"));
        assert!(prompt.ends_with("Explain the issue."));
    }

    #[test]
    fn preview_loads_scoped_palyra_context_markdown_inside_workspace() {
        let root = temp_project_context_root();
        let scoped_dir = root.join(".palyra").join("context");
        fs::create_dir_all(scoped_dir.as_path()).expect("scoped context dir should be created");
        fs::write(root.join("PALYRA.md"), "# Workspace\nRoot rules")
            .expect("root project context should be written");
        fs::write(scoped_dir.join("ops.md"), "# Ops\nScoped rules")
            .expect("scoped project context should be written");

        let preview =
            build_project_context_preview(std::slice::from_ref(&root), &[], &[], &[], 1_700_000)
                .expect("project context preview should build");

        let source_kinds =
            preview.entries.iter().map(|entry| entry.source_kind.as_str()).collect::<Vec<_>>();
        assert!(source_kinds.contains(&"palyra_md"));
        assert!(source_kinds.contains(&"palyra_context_md"));
        let scoped_entry = preview
            .entries
            .iter()
            .find(|entry| entry.source_kind == "palyra_context_md")
            .expect("scoped .palyra/context markdown should be loaded");
        assert_eq!(scoped_entry.path, ".palyra/context/ops.md");
        assert_eq!(scoped_entry.precedence_label, "workspace_scoped");
        assert!(scoped_entry.active);

        fs::remove_dir_all(root.as_path())
            .expect("temporary project context root should be removed");
    }
}
