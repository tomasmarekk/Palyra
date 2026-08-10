//! Console session catalog handlers for the `/console/v1/sessions` route
//! family.
//!
//! Builds the session catalog served to the web console: every request loads
//! the caller's full scoped session set, enriches it with pending approvals,
//! workspace activity, project context, agent bindings, and family metadata,
//! then filters, sorts, and pages the result in memory. The list `cursor` is
//! an offset into the filtered ordering, not a stable key. All operator
//! visible text passes through redaction before truncation. Response shapes
//! are part of the `/console/v1` wire contract consumed by `apps/web`.

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    agents::{
        AgentBindingQuery, AgentBindingRequest, AgentRecord, AgentUnbindRequest,
        SessionAgentBinding,
    },
    application::session_queue::{analyze_session_queue, pending_queue_depth, SessionQueuePolicy},
    *,
};

const DEFAULT_SESSION_CATALOG_LIMIT: usize = 25;
const MAX_SESSION_CATALOG_LIMIT: usize = 100;
const SESSION_CATALOG_FETCH_PAGE: usize = 128;
const SESSION_CATALOG_APPROVAL_PAGE: usize = 256;
const SESSION_CATALOG_WORKSPACE_PAGE: usize = 256;
const SESSION_CATALOG_TITLE_LEN: usize = 72;
const SESSION_CATALOG_PREVIEW_LEN: usize = 180;
const SESSION_CATALOG_RELATIVES_LIMIT: usize = 4;
const SESSION_CATALOG_RECAP_ITEMS_LIMIT: usize = 4;
const SESSION_PUBLIC_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const SESSION_PUBLIC_SNAPSHOT_REDACTED: &str = "<redacted>";

/// Query parameters accepted by the session catalog list endpoint; every
/// field is optional and missing filters leave the catalog unrestricted.
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleSessionCatalogQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    include_archived: Option<bool>,
    #[serde(default)]
    archived: Option<bool>,
    #[serde(default)]
    sort: Option<String>,
    #[serde(default)]
    title_source: Option<String>,
    #[serde(default)]
    has_pending_approvals: Option<bool>,
    #[serde(default)]
    branch_state: Option<String>,
    #[serde(default)]
    has_context_files: Option<bool>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    model_profile: Option<String>,
    #[serde(default)]
    title_state: Option<String>,
}

#[derive(Debug, Serialize)]
struct SessionCatalogSummary {
    active_sessions: usize,
    archived_sessions: usize,
    sessions_with_pending_approvals: usize,
    sessions_with_active_runs: usize,
    sessions_with_context_files: usize,
}

/// Wire envelope for `GET /console/v1/sessions`: the filtered page plus
/// catalog-wide summary counts and an echo of the applied query.
#[derive(Debug, Serialize)]
pub(crate) struct SessionCatalogListEnvelope {
    contract: control_plane::ContractDescriptor,
    sessions: Vec<SessionCatalogRecord>,
    summary: SessionCatalogSummary,
    query: SessionCatalogQueryEcho,
    page: control_plane::PageInfo,
}

/// Wire envelope for a single fully enriched session catalog record.
#[derive(Debug, Serialize)]
pub(crate) struct SessionCatalogDetailEnvelope {
    contract: control_plane::ContractDescriptor,
    session: SessionCatalogRecord,
}

/// Wire envelope for session mutations (archive, quick-controls updates);
/// `action` names which mutation produced the returned record.
#[derive(Debug, Serialize)]
pub(crate) struct SessionCatalogMutationEnvelope {
    contract: control_plane::ContractDescriptor,
    session: SessionCatalogRecord,
    action: &'static str,
}

/// Wire envelope for project-context endpoints: the refreshed session record
/// plus the context preview produced by the requested `action`.
#[derive(Debug, Serialize)]
pub(crate) struct SessionProjectContextEnvelope {
    contract: control_plane::ContractDescriptor,
    session: SessionCatalogRecord,
    preview: crate::application::project_context::ProjectContextPreviewEnvelope,
    action: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    scaffold: Option<crate::application::project_context::ProjectContextScaffoldOutcome>,
}

/// Body for quick-controls updates.
///
/// The nested `Option<Option<_>>` fields distinguish "field omitted" (outer
/// `None`, leave the override untouched) from an explicit JSON `null` (inner
/// `None`, clear the override back to the inherited value).
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleSessionQuickControlsUpdateRequest {
    #[serde(default)]
    agent_id: Option<Option<String>>,
    #[serde(default)]
    model_profile: Option<Option<String>>,
    #[serde(default)]
    thinking: Option<Option<bool>>,
    #[serde(default)]
    trace: Option<Option<bool>>,
    #[serde(default)]
    verbose: Option<Option<bool>>,
    #[serde(default)]
    reset_to_default: Option<bool>,
}

/// Wire envelope for run aborts: whether cancellation was requested and the
/// reason that was recorded for it.
#[derive(Debug, Serialize)]
pub(crate) struct SessionCatalogRunAbortEnvelope {
    contract: control_plane::ContractDescriptor,
    run_id: String,
    cancel_requested: bool,
    reason: String,
}

/// Wire envelope for `GET /console/v1/sessions/{session_id}/snapshot`:
/// a redacted, operator-facing aggregate of lifecycle, queue, binding, usage,
/// approval, and safe-operation state for one session.
#[derive(Debug, Serialize)]
pub(crate) struct SessionPublicSnapshotEnvelope {
    contract: control_plane::ContractDescriptor,
    snapshot: SessionPublicSnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct SessionPublicSnapshot {
    schema_version: u32,
    identity: SessionSnapshotIdentity,
    lifecycle: SessionSnapshotLifecycle,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_run: Option<SessionSnapshotRun>,
    queue: SessionSnapshotQueue,
    binding: SessionSnapshotBinding,
    compaction: SessionSnapshotCompaction,
    suspend_resume: SessionSnapshotSuspendResume,
    approvals: SessionSnapshotApprovals,
    usage: SessionSnapshotUsage,
    safe_operations: SessionSnapshotSafeOperations,
    subagents: SessionSnapshotSubagents,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SessionSnapshotIdentity {
    session_id: String,
    session_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_label: Option<String>,
    title: String,
    branch_state: String,
    owner: SessionSnapshotOwner,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
struct SessionSnapshotOwner {
    principal: &'static str,
    device_id: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    channel: Option<String>,
    redaction_level: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct SessionSnapshotLifecycle {
    state: String,
    reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_state: Option<String>,
    queue_busy_state: String,
    updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    archived_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
struct SessionSnapshotRun {
    run_id: String,
    state: String,
    cancel_requested: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    cancel_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    started_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    updated_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_at_unix_ms: Option<i64>,
    origin_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    origin_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_run_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SessionSnapshotQueue {
    paused: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pause_reason: Option<String>,
    control_updated_at_unix_ms: i64,
    active_run_id: Option<String>,
    pending_depth: usize,
    total_count: usize,
    terminal_count: usize,
    busy_state: String,
    recommendation: String,
    can_accept_followups: bool,
    safe_boundary: crate::application::session_queue::SessionQueueSafeBoundary,
    policy: Value,
    metrics: Value,
    analysis: Value,
}

#[derive(Debug, Clone, Serialize)]
struct SessionSnapshotBinding {
    agent: SessionCatalogQuickControlRecord,
    model: SessionCatalogQuickControlRecord,
    thinking: SessionCatalogToggleControlRecord,
    trace: SessionCatalogToggleControlRecord,
    verbose: SessionCatalogToggleControlRecord,
    reset_to_default_available: bool,
}

#[derive(Debug, Clone, Serialize)]
struct SessionSnapshotCompaction {
    state: String,
    can_preview: bool,
    can_apply: bool,
    artifact_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    latest_artifact_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SessionSnapshotSuspendResume {
    paused: bool,
    can_pause: bool,
    can_resume: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pause_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SessionSnapshotApprovals {
    pending: bool,
    pending_count: usize,
    active_run_pending_approval: bool,
}

#[derive(Debug, Clone, Serialize)]
struct SessionSnapshotUsage {
    tokens: SessionSnapshotTokenCounters,
    cost: SessionSnapshotCostCounters,
}

#[derive(Debug, Clone, Serialize)]
struct SessionSnapshotTokenCounters {
    prompt: Option<u64>,
    completion: Option<u64>,
    total: Option<u64>,
    source: String,
}

#[derive(Debug, Clone, Serialize)]
struct SessionSnapshotCostCounters {
    estimated_usd: Option<f64>,
    currency: Option<String>,
    source: String,
}

#[derive(Debug, Clone, Serialize)]
struct SessionSnapshotSafeOperations {
    can_start_run: bool,
    can_cancel: bool,
    can_fork: bool,
    can_compact: bool,
    can_repair_binding: bool,
    can_pause_queue: bool,
    can_resume_queue: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    blocking_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SessionSnapshotSubagents {
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_session_id: Option<String>,
    child_count: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    child_session_ids: Vec<String>,
    subagent_count: usize,
    stale_link_count: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    records: Vec<crate::delegation::SubagentSessionRecord>,
}

#[derive(Debug, Serialize)]
struct SessionCatalogQueryEcho {
    limit: usize,
    cursor: usize,
    q: Option<String>,
    include_archived: bool,
    archived: Option<bool>,
    sort: String,
    title_source: Option<String>,
    has_pending_approvals: Option<bool>,
    branch_state: Option<String>,
    has_context_files: Option<bool>,
    agent_id: Option<String>,
    model_profile: Option<String>,
    title_state: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SessionCatalogRecord {
    session_id: String,
    session_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_label: Option<String>,
    title: String,
    title_source: String,
    title_generation_state: String,
    manual_title_locked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    auto_title_updated_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    manual_title_updated_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    preview: Option<String>,
    preview_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_intent: Option<String>,
    last_intent_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_summary: Option<String>,
    last_summary_state: String,
    branch_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_session_id: Option<String>,
    principal: String,
    device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    channel: Option<String>,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_run_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_run_started_at_unix_ms: Option<i64>,
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
    archived: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    archived_at_unix_ms: Option<i64>,
    pending_approvals: usize,
    has_context_files: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_context_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model_profile: Option<String>,
    artifact_count: usize,
    family: SessionCatalogFamilyRecord,
    recap: SessionCatalogRecapRecord,
    quick_controls: SessionCatalogQuickControlsRecord,
}

#[derive(Debug, Clone, Serialize)]
struct SessionCatalogFamilyRecord {
    root_title: String,
    sequence: u64,
    family_size: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_title: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    relatives: Vec<SessionCatalogFamilyRelative>,
}

#[derive(Debug, Clone, Serialize)]
struct SessionCatalogFamilyRelative {
    session_id: String,
    title: String,
    branch_state: String,
    relation: String,
}

#[derive(Debug, Clone, Serialize)]
struct SessionCatalogRecapRecord {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    touched_files: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    active_context_files: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    project_context: Option<SessionProjectContextRecord>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    recent_artifacts: Vec<SessionCatalogArtifactRecord>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    ctas: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SessionCatalogArtifactRecord {
    artifact_id: String,
    kind: String,
    label: String,
}

#[derive(Debug, Clone, Serialize)]
struct SessionCatalogQuickControlsRecord {
    agent: SessionCatalogQuickControlRecord,
    model: SessionCatalogQuickControlRecord,
    thinking: SessionCatalogToggleControlRecord,
    trace: SessionCatalogToggleControlRecord,
    verbose: SessionCatalogToggleControlRecord,
    reset_to_default_available: bool,
}

#[derive(Debug, Clone, Serialize)]
struct SessionCatalogQuickControlRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    display_value: String,
    source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    inherited_value: Option<String>,
    override_active: bool,
}

#[derive(Debug, Clone, Serialize)]
struct SessionCatalogToggleControlRecord {
    value: bool,
    source: String,
    inherited_value: bool,
    override_active: bool,
}

#[derive(Debug, Clone, Serialize)]
struct SessionProjectContextRecord {
    generated_at_unix_ms: i64,
    active_entries: usize,
    blocked_entries: usize,
    approval_required_entries: usize,
    disabled_entries: usize,
    active_estimated_tokens: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    focus_paths: Vec<SessionProjectContextFocusRecord>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    entries: Vec<SessionProjectContextEntryRecord>,
}

#[derive(Debug, Clone, Serialize)]
struct SessionProjectContextFocusRecord {
    path: String,
    reason: String,
}

#[derive(Debug, Clone, Serialize)]
struct SessionProjectContextEntryRecord {
    entry_id: String,
    order: usize,
    path: String,
    source_kind: String,
    source_label: String,
    precedence_label: String,
    depth: usize,
    root: bool,
    active: bool,
    disabled: bool,
    approved: bool,
    status: String,
    content_hash: String,
    loaded_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    modified_at_unix_ms: Option<i64>,
    estimated_tokens: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    discovery_reasons: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
    preview_text: String,
}

#[derive(Debug, Clone, Default)]
struct SessionWorkspaceSummary {
    touched_files: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct SessionDetailContext {
    recent_artifacts: Vec<SessionCatalogArtifactRecord>,
    artifact_count: usize,
    compaction_artifact_count: usize,
    latest_compaction_artifact_id: Option<String>,
}

#[derive(Debug, Clone)]
struct SessionPublicQueueContext {
    queued_inputs: Vec<journal::OrchestratorQueuedInputRecord>,
    control: journal::OrchestratorSessionQueueControlRecord,
    policy: SessionQueuePolicy,
    safe_boundary: crate::application::session_queue::SessionQueueSafeBoundary,
    active_run_id: Option<String>,
}

#[derive(Debug)]
struct SessionCatalogContext {
    pending_approvals_by_session: HashMap<String, usize>,
    workspace_by_session: HashMap<String, SessionWorkspaceSummary>,
    project_context_by_session:
        HashMap<String, crate::application::project_context::ProjectContextPreviewEnvelope>,
    family_by_session: HashMap<String, SessionCatalogFamilyRecord>,
    run_snapshot_by_id: HashMap<String, journal::OrchestratorRunStatusSnapshot>,
    bindings_by_session: HashMap<String, SessionAgentBinding>,
    agents_by_id: HashMap<String, AgentRecord>,
    default_agent_id: Option<String>,
    effective_model_profile: Option<String>,
}

/// Handles `GET /console/v1/sessions`: builds, filters, sorts, and pages the
/// enriched session catalog for the authenticated console context.
///
/// # Errors
/// Returns an error response when console authorization fails, when the
/// cursor is not an unsigned integer, or when any of the catalog data sources
/// (sessions, approvals, workspace, project context, agents) fail to load.
pub(crate) async fn console_sessions_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleSessionCatalogQuery>,
) -> Result<Json<SessionCatalogListEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let include_archived = query.include_archived.unwrap_or(false);
    let limit =
        query.limit.unwrap_or(DEFAULT_SESSION_CATALOG_LIMIT).clamp(1, MAX_SESSION_CATALOG_LIMIT);
    let cursor = parse_session_catalog_cursor(query.cursor.as_deref())?;
    let search = normalize_session_catalog_search(query.q.as_deref());
    let title_source = normalize_session_catalog_title_source(query.title_source.as_deref());
    let branch_state = normalize_catalog_token(query.branch_state.as_deref());
    let agent_id_filter = normalize_catalog_token(query.agent_id.as_deref());
    let model_profile_filter = normalize_catalog_token(query.model_profile.as_deref());
    let title_state_filter = normalize_catalog_token(query.title_state.as_deref());
    let sort = normalize_session_catalog_sort(query.sort.as_deref());

    let base_sessions = load_scoped_sessions(
        &state,
        session.context.principal.as_str(),
        session.context.device_id.as_str(),
        session.context.channel.as_deref(),
        include_archived || query.archived.unwrap_or(false),
    )
    .await
    .map_err(runtime_status_response)?;
    let catalog_context =
        load_session_catalog_context(&state, &session.context, &base_sessions).await?;

    let mut catalog = Vec::with_capacity(base_sessions.len());
    for base in base_sessions {
        catalog.push(build_session_catalog_record(&state, &catalog_context, base, None).await?);
    }

    // Summary counts are computed before the query filters below apply so the
    // console can show catalog-wide totals alongside a filtered page.
    let summary = SessionCatalogSummary {
        active_sessions: catalog.iter().filter(|record| !record.archived).count(),
        archived_sessions: catalog.iter().filter(|record| record.archived).count(),
        sessions_with_pending_approvals: catalog
            .iter()
            .filter(|record| record.pending_approvals > 0)
            .count(),
        sessions_with_active_runs: catalog
            .iter()
            .filter(|record| {
                record
                    .last_run_state
                    .as_deref()
                    .is_some_and(|state| state == "accepted" || state == "in_progress")
            })
            .count(),
        sessions_with_context_files: catalog
            .iter()
            .filter(|record| record.has_context_files)
            .count(),
    };

    if let Some(archived_filter) = query.archived {
        catalog.retain(|record| record.archived == archived_filter);
    } else if !include_archived {
        catalog.retain(|record| !record.archived);
    }
    if let Some(expected_title_source) = title_source.as_deref() {
        catalog.retain(|record| record.title_source.eq_ignore_ascii_case(expected_title_source));
    }
    if let Some(has_pending_approvals) = query.has_pending_approvals {
        catalog.retain(|record| (record.pending_approvals > 0) == has_pending_approvals);
    }
    if let Some(expected_branch_state) = branch_state.as_deref() {
        catalog.retain(|record| record.branch_state.eq_ignore_ascii_case(expected_branch_state));
    }
    if let Some(has_context_files) = query.has_context_files {
        catalog.retain(|record| record.has_context_files == has_context_files);
    }
    if let Some(agent_id_filter) = agent_id_filter.as_deref() {
        catalog.retain(|record| {
            record
                .agent_id
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case(agent_id_filter))
        });
    }
    if let Some(model_profile_filter) = model_profile_filter.as_deref() {
        catalog.retain(|record| {
            record
                .model_profile
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case(model_profile_filter))
        });
    }
    if let Some(title_state_filter) = title_state_filter.as_deref() {
        catalog.retain(|record| {
            record.title_generation_state.eq_ignore_ascii_case(title_state_filter)
        });
    }
    if let Some(search) = search.as_deref() {
        catalog.retain(|record| session_catalog_record_matches(record, search));
    }

    catalog.sort_by(|left, right| compare_session_catalog_records(left, right, sort.as_str()));

    // The cursor is a plain offset into the filtered, sorted ordering; it is
    // cheap but not stable across concurrent catalog changes.
    let next_cursor =
        (cursor.saturating_add(limit) < catalog.len()).then(|| (cursor + limit).to_string());
    let sessions = catalog.into_iter().skip(cursor).take(limit).collect::<Vec<_>>();
    let page = build_page_info(limit, sessions.len(), next_cursor.clone());

    Ok(Json(SessionCatalogListEnvelope {
        contract: contract_descriptor(),
        sessions,
        summary,
        query: SessionCatalogQueryEcho {
            limit,
            cursor,
            q: search,
            include_archived,
            archived: query.archived,
            sort,
            title_source,
            has_pending_approvals: query.has_pending_approvals,
            branch_state,
            has_context_files: query.has_context_files,
            agent_id: agent_id_filter,
            model_profile: model_profile_filter,
            title_state: title_state_filter,
        },
        page,
    }))
}

/// Handles `GET /console/v1/sessions/{session_id}`: returns one enriched
/// catalog record including artifact details.
///
/// # Errors
/// Returns an error response when console authorization fails, when the
/// session id is not a canonical ULID, or when the session is missing from
/// the caller's scope.
pub(crate) async fn console_session_detail_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<SessionCatalogDetailEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let record = load_session_catalog_record(&state, &session.context, session_id.as_str()).await?;
    Ok(Json(SessionCatalogDetailEnvelope { contract: contract_descriptor(), session: record }))
}

/// Handles `GET /console/v1/sessions/{session_id}/snapshot`: returns a
/// redacted, decision-ready public snapshot for one session.
///
/// # Errors
/// Returns an error response when console authorization fails, when the
/// session id is not a canonical ULID, when the session is missing from the
/// caller's scope, or when session, queue, approval, run, or binding state
/// cannot be loaded.
pub(crate) async fn console_session_snapshot_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<SessionPublicSnapshotEnvelope>, Response> {
    let console_session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let base_sessions = load_scoped_sessions(
        &state,
        console_session.context.principal.as_str(),
        console_session.context.device_id.as_str(),
        console_session.context.channel.as_deref(),
        true,
    )
    .await
    .map_err(runtime_status_response)?;
    let base_session =
        base_sessions.iter().find(|record| record.session_id == session_id).cloned().ok_or_else(
            || runtime_status_response(tonic::Status::not_found("session was not found")),
        )?;
    let catalog_context =
        load_session_catalog_context(&state, &console_session.context, &base_sessions).await?;
    let detail_context = load_session_detail_context(
        &state,
        &console_session.context,
        base_session.session_id.as_str(),
    )
    .await?;
    let catalog_record = build_session_catalog_record(
        &state,
        &catalog_context,
        base_session.clone(),
        Some(detail_context.clone()),
    )
    .await?;
    let queue_context =
        load_session_public_queue_context(&state, &console_session.context, &catalog_record)
            .await?;
    let snapshot = build_session_public_snapshot(
        &state,
        &console_session.context,
        &base_sessions,
        &catalog_context,
        &detail_context,
        &catalog_record,
        queue_context,
    )
    .await?;
    Ok(Json(SessionPublicSnapshotEnvelope { contract: contract_descriptor(), snapshot }))
}

/// Handles `GET /console/v1/sessions/{session_id}/project-context`: returns
/// the current project-context preview without mutating anything.
///
/// # Errors
/// Returns an error response when console authorization fails, when the
/// session id is invalid or out of scope, or when the preview cannot be
/// computed.
pub(crate) async fn console_session_project_context_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<SessionProjectContextEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let _session_record =
        load_scoped_session(&state, &session.context, session_id.as_str()).await?;
    let preview = crate::application::project_context::preview_project_context(
        &state.runtime,
        &session.context,
        session_id.as_str(),
        "",
        false,
    )
    .await
    .map_err(runtime_status_response)?;
    let envelope = build_session_project_context_envelope(
        &state,
        &session.context,
        session_id.as_str(),
        preview,
        "inspect",
        None,
    )
    .await?;
    Ok(Json(envelope))
}

/// Handles `POST /console/v1/sessions/{session_id}/project-context/refresh`:
/// re-discovers project context files for the session.
///
/// # Errors
/// Returns an error response when console authorization or CSRF validation
/// fails, when the session id is invalid or out of scope, or when the refresh
/// fails.
pub(crate) async fn console_session_project_context_refresh_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<SessionProjectContextEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let _session_record =
        load_scoped_session(&state, &session.context, session_id.as_str()).await?;
    let preview = crate::application::project_context::refresh_project_context(
        &state.runtime,
        &session.context,
        session_id.as_str(),
    )
    .await
    .map_err(runtime_status_response)?;
    let envelope = build_session_project_context_envelope(
        &state,
        &session.context,
        session_id.as_str(),
        preview,
        "refresh",
        None,
    )
    .await?;
    Ok(Json(envelope))
}

/// Handles `POST .../project-context/entries/{entry_id}/disable`: excludes
/// one context entry from prompt assembly for this session.
///
/// # Errors
/// Returns an error response when console authorization or CSRF validation
/// fails, when the session id is invalid or out of scope, or when the entry
/// cannot be disabled.
pub(crate) async fn console_session_project_context_disable_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((session_id, entry_id)): Path<(String, String)>,
) -> Result<Json<SessionProjectContextEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let _session_record =
        load_scoped_session(&state, &session.context, session_id.as_str()).await?;
    let preview = crate::application::project_context::disable_project_context_entry(
        &state.runtime,
        &session.context,
        session_id.as_str(),
        entry_id.as_str(),
    )
    .await
    .map_err(runtime_status_response)?;
    let envelope = build_session_project_context_envelope(
        &state,
        &session.context,
        session_id.as_str(),
        preview,
        "disable",
        None,
    )
    .await?;
    Ok(Json(envelope))
}

/// Handles `POST .../project-context/entries/{entry_id}/enable`: re-includes
/// a previously disabled context entry.
///
/// # Errors
/// Returns an error response when console authorization or CSRF validation
/// fails, when the session id is invalid or out of scope, or when the entry
/// cannot be enabled.
pub(crate) async fn console_session_project_context_enable_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((session_id, entry_id)): Path<(String, String)>,
) -> Result<Json<SessionProjectContextEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let _session_record =
        load_scoped_session(&state, &session.context, session_id.as_str()).await?;
    let preview = crate::application::project_context::enable_project_context_entry(
        &state.runtime,
        &session.context,
        session_id.as_str(),
        entry_id.as_str(),
    )
    .await
    .map_err(runtime_status_response)?;
    let envelope = build_session_project_context_envelope(
        &state,
        &session.context,
        session_id.as_str(),
        preview,
        "enable",
        None,
    )
    .await?;
    Ok(Json(envelope))
}

/// Handles `POST .../project-context/entries/{entry_id}/approve`: grants the
/// approval a gated context entry needs before it becomes active.
///
/// # Errors
/// Returns an error response when console authorization or CSRF validation
/// fails, when the session id is invalid or out of scope, or when the entry
/// cannot be approved.
pub(crate) async fn console_session_project_context_approve_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((session_id, entry_id)): Path<(String, String)>,
) -> Result<Json<SessionProjectContextEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let _session_record =
        load_scoped_session(&state, &session.context, session_id.as_str()).await?;
    let preview = crate::application::project_context::approve_project_context_entry(
        &state.runtime,
        &session.context,
        session_id.as_str(),
        entry_id.as_str(),
    )
    .await
    .map_err(runtime_status_response)?;
    let envelope = build_session_project_context_envelope(
        &state,
        &session.context,
        session_id.as_str(),
        preview,
        "approve",
        None,
    )
    .await?;
    Ok(Json(envelope))
}

/// Handles `POST .../project-context/scaffold`: creates a starter context
/// file for the session workspace and refreshes the preview to include it.
///
/// # Errors
/// Returns an error response when console authorization or CSRF validation
/// fails, when the session id is invalid or out of scope, or when scaffolding
/// or the follow-up refresh fails.
pub(crate) async fn console_session_project_context_scaffold_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleSessionProjectContextScaffoldRequest>,
) -> Result<Json<SessionProjectContextEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let _session_record =
        load_scoped_session(&state, &session.context, session_id.as_str()).await?;
    let scaffold = crate::application::project_context::scaffold_project_context_file(
        &state.runtime,
        &session.context,
        session_id.as_str(),
        payload.project_name.as_deref(),
        payload.force.unwrap_or(false),
    )
    .await
    .map_err(runtime_status_response)?;
    let preview = crate::application::project_context::refresh_project_context(
        &state.runtime,
        &session.context,
        session_id.as_str(),
    )
    .await
    .map_err(runtime_status_response)?;
    let envelope = build_session_project_context_envelope(
        &state,
        &session.context,
        session_id.as_str(),
        preview,
        "scaffold",
        Some(scaffold),
    )
    .await?;
    Ok(Json(envelope))
}

/// Handles `POST /console/v1/sessions/{session_id}/archive`: archives the
/// session via the orchestrator cleanup path and returns the updated record.
///
/// # Errors
/// Returns an error response when console authorization or CSRF validation
/// fails, when the session id is not a canonical ULID, or when cleanup fails.
pub(crate) async fn console_session_archive_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<SessionCatalogMutationEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let outcome = state
        .runtime
        .cleanup_orchestrator_session(journal::OrchestratorSessionCleanupRequest {
            session_id: Some(session_id),
            session_key: None,
            principal: session.context.principal.clone(),
            device_id: session.context.device_id.clone(),
            channel: session.context.channel.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    let scoped_sessions = vec![outcome.session.clone()];
    let catalog_context =
        load_session_catalog_context(&state, &session.context, &scoped_sessions).await?;
    let record =
        build_session_catalog_record(&state, &catalog_context, outcome.session, None).await?;
    Ok(Json(SessionCatalogMutationEnvelope {
        contract: contract_descriptor(),
        session: record,
        action: "archived",
    }))
}

/// Handles `POST /console/v1/sessions/{session_id}/quick-controls`: applies
/// agent binding and model/thinking/trace/verbose overrides, or clears them
/// all when `reset_to_default` is set.
///
/// # Errors
/// Returns an error response when console authorization or CSRF validation
/// fails, when the session id is invalid or out of scope, when the payload
/// contains no change, or when persisting the overrides fails.
pub(crate) async fn console_session_quick_controls_update_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleSessionQuickControlsUpdateRequest>,
) -> Result<Json<SessionCatalogMutationEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let existing_session =
        load_scoped_session(&state, &session.context, session_id.as_str()).await?;
    let reset_to_default = payload.reset_to_default.unwrap_or(false);
    let requested_agent_id = payload.agent_id.map(|value| value.and_then(trim_to_option));
    let requested_model_profile = payload.model_profile.map(|value| value.and_then(trim_to_option));
    let requested_thinking = payload.thinking;
    let requested_trace = payload.trace;
    let requested_verbose = payload.verbose;
    if !reset_to_default
        && requested_agent_id.is_none()
        && requested_model_profile.is_none()
        && requested_thinking.is_none()
        && requested_trace.is_none()
        && requested_verbose.is_none()
    {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "quick controls update request must include at least one override change",
        )));
    }

    if reset_to_default {
        state
            .runtime
            .unbind_agent_for_context(AgentUnbindRequest {
                principal: session.context.principal.clone(),
                channel: session.context.channel.clone(),
                session_id: session_id.clone(),
            })
            .await
            .map_err(runtime_status_response)?;
    } else if let Some(agent_id) = requested_agent_id.clone() {
        match agent_id {
            Some(agent_id) => {
                state
                    .runtime
                    .bind_agent_for_context(AgentBindingRequest {
                        agent_id,
                        principal: session.context.principal.clone(),
                        channel: session.context.channel.clone(),
                        session_id: session_id.clone(),
                    })
                    .await
                    .map_err(runtime_status_response)?;
            }
            None => {
                state
                    .runtime
                    .unbind_agent_for_context(AgentUnbindRequest {
                        principal: session.context.principal.clone(),
                        channel: session.context.channel.clone(),
                        session_id: session_id.clone(),
                    })
                    .await
                    .map_err(runtime_status_response)?;
            }
        }
    }

    let updated_session = state
        .runtime
        .update_orchestrator_session_quick_controls(
            journal::OrchestratorSessionQuickControlsUpdateRequest {
                session_id: session_id.clone(),
                principal: session.context.principal.clone(),
                device_id: session.context.device_id.clone(),
                channel: session.context.channel.clone(),
                // reset_to_default clears every override with an explicit
                // Some(None); otherwise only fields present in the payload
                // change and omitted fields stay None (untouched).
                model_profile_override: if reset_to_default {
                    Some(None)
                } else {
                    requested_model_profile.clone()
                },
                thinking_override: if reset_to_default { Some(None) } else { requested_thinking },
                trace_override: if reset_to_default { Some(None) } else { requested_trace },
                verbose_override: if reset_to_default { Some(None) } else { requested_verbose },
            },
        )
        .await
        .map_err(runtime_status_response)?;

    // Reload the scoped set so family metadata and the returned record
    // reflect the mutation; fall back to the direct update result if the
    // record is missing from the listing.
    let base_sessions = load_scoped_sessions(
        &state,
        session.context.principal.as_str(),
        session.context.device_id.as_str(),
        session.context.channel.as_deref(),
        true,
    )
    .await
    .map_err(runtime_status_response)?;
    let refreshed_session = base_sessions
        .iter()
        .find(|record| record.session_id == updated_session.session_id)
        .cloned()
        .unwrap_or(updated_session);
    let catalog_context =
        load_session_catalog_context(&state, &session.context, &base_sessions).await?;
    let detail_context = load_session_detail_context(
        &state,
        &session.context,
        refreshed_session.session_id.as_str(),
    )
    .await?;
    let record = build_session_catalog_record(
        &state,
        &catalog_context,
        refreshed_session,
        Some(detail_context),
    )
    .await?;
    let _ = crate::gateway::record_agent_journal_event(
        &state.runtime,
        &session.context,
        json!({
            "event": "session.quick_controls.updated",
            "session_id": session_id,
            "reset_to_default": reset_to_default,
            "requested_agent_id": requested_agent_id,
            "requested_model_profile": requested_model_profile,
            "requested_thinking": requested_thinking,
            "requested_trace": requested_trace,
            "requested_verbose": requested_verbose,
            "previous_model_profile_override": existing_session.model_profile_override,
            "previous_thinking_override": existing_session.thinking_override,
            "previous_trace_override": existing_session.trace_override,
            "previous_verbose_override": existing_session.verbose_override,
            "quick_controls": record.quick_controls,
        }),
    )
    .await;
    Ok(Json(SessionCatalogMutationEnvelope {
        contract: contract_descriptor(),
        session: record,
        action: if reset_to_default { "quick_controls_reset" } else { "quick_controls_updated" },
    }))
}

/// Handles `POST /console/v1/sessions/runs/{run_id}/abort`: requests
/// orchestrator cancellation for a run owned by the caller's context and
/// cleans up its resources.
///
/// # Errors
/// Returns an error response when console authorization or CSRF validation
/// fails, when the run id is invalid or unknown, or when the run belongs to a
/// different console context.
pub(crate) async fn console_session_run_abort_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    payload: Option<Json<RunCancelRequest>>,
) -> Result<Json<SessionCatalogRunAbortEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    let run = state
        .runtime
        .orchestrator_run_status_snapshot(run_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "orchestrator run not found: {run_id}"
            )))
        })?;
    if !super::chat::run_matches_console_context(&run, &session.context) {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "chat run does not belong to the authenticated console session context",
        )));
    }
    let reason = payload
        .and_then(|body| body.0.reason)
        .and_then(trim_to_option)
        .unwrap_or_else(|| "console_session_abort".to_owned());
    let response = state
        .runtime
        .request_orchestrator_cancel(journal::OrchestratorCancelRequest {
            run_id: run_id.clone(),
            reason: reason.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    gateway::cleanup_run_resources(
        &state.runtime,
        response.run_id.as_str(),
        response.reason.as_str(),
    )
    .await;
    Ok(Json(SessionCatalogRunAbortEnvelope {
        contract: contract_descriptor(),
        run_id,
        cancel_requested: response.cancel_requested,
        reason: response.reason,
    }))
}

/// Loads every session in the caller's scope by paging the journal to
/// exhaustion; filtering and sorting happen in memory afterwards.
async fn load_scoped_sessions(
    state: &AppState,
    principal: &str,
    device_id: &str,
    channel: Option<&str>,
    include_archived: bool,
) -> Result<Vec<journal::OrchestratorSessionRecord>, tonic::Status> {
    let mut sessions = Vec::new();
    let mut cursor = None::<String>;

    loop {
        let (mut page, next_after) = state
            .runtime
            .list_orchestrator_sessions(gateway::ListOrchestratorSessionsRequest {
                after_session_key: cursor.clone(),
                principal: principal.to_owned(),
                device_id: device_id.to_owned(),
                channel: channel.map(str::to_owned),
                include_archived,
                requested_limit: Some(SESSION_CATALOG_FETCH_PAGE),
                search_query: None,
            })
            .await?;
        sessions.append(&mut page);
        let Some(next_after) = next_after else {
            break;
        };
        cursor = Some(next_after);
    }

    Ok(sessions)
}

/// Loads all approval records for the principal by paging to exhaustion;
/// pending-only filtering happens at the call site.
async fn load_scoped_pending_approvals(
    state: &AppState,
    principal: &str,
) -> Result<Vec<journal::ApprovalRecord>, tonic::Status> {
    let mut approvals = Vec::new();
    let mut cursor = None::<String>;

    loop {
        let (mut page, next_after) = state
            .runtime
            .list_approval_records(
                cursor.clone(),
                Some(SESSION_CATALOG_APPROVAL_PAGE),
                None,
                None,
                None,
                Some(principal.to_owned()),
                None,
                None,
            )
            .await?;
        approvals.append(&mut page);
        let Some(next_after) = next_after else {
            break;
        };
        cursor = Some(next_after);
    }

    Ok(approvals)
}

/// Loads one session and enforces that it belongs to the authenticated
/// console context (principal, device, and channel must all match).
///
/// # Errors
/// Returns not-found when the session does not exist and permission-denied
/// when it exists but is owned by a different context.
async fn load_scoped_session(
    state: &AppState,
    context: &gateway::RequestContext,
    session_id: &str,
) -> Result<journal::OrchestratorSessionRecord, Response> {
    let session = state
        .runtime
        .orchestrator_session_by_id(session_id.to_owned())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found("session was not found"))
        })?;
    if session.principal != context.principal
        || session.device_id != context.device_id
        || session.channel.as_deref() != context.channel.as_deref()
    {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "session does not belong to the authenticated console session context",
        )));
    }
    Ok(session)
}

/// Builds the fully enriched detail record for one session. Loads the whole
/// scoped set first because family metadata needs sibling sessions; not-found
/// covers both missing and out-of-scope sessions.
async fn load_session_catalog_record(
    state: &AppState,
    context: &gateway::RequestContext,
    session_id: &str,
) -> Result<SessionCatalogRecord, Response> {
    let base_sessions = load_scoped_sessions(
        state,
        context.principal.as_str(),
        context.device_id.as_str(),
        context.channel.as_deref(),
        true,
    )
    .await
    .map_err(runtime_status_response)?;
    let base =
        base_sessions.iter().find(|record| record.session_id == session_id).cloned().ok_or_else(
            || runtime_status_response(tonic::Status::not_found("session was not found")),
        )?;
    let catalog_context = load_session_catalog_context(state, context, &base_sessions).await?;
    let detail_context =
        load_session_detail_context(state, context, base.session_id.as_str()).await?;
    build_session_catalog_record(state, &catalog_context, base, Some(detail_context)).await
}

async fn load_session_public_queue_context(
    state: &AppState,
    context: &gateway::RequestContext,
    record: &SessionCatalogRecord,
) -> Result<SessionPublicQueueContext, Response> {
    let queued_inputs = state
        .runtime
        .list_orchestrator_queued_inputs(record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let control = state
        .runtime
        .get_orchestrator_session_queue_control(record.session_id.clone())
        .await
        .map_err(runtime_status_response)?
        .unwrap_or_else(|| default_public_session_queue_control(record.session_id.clone()));
    let policy = SessionQueuePolicy::from_config(
        &state.runtime.config.session_queue_policy,
        record.session_id.as_str(),
        context.channel.as_deref(),
        None,
    );
    let (active_run_stream, pending_approval, active_run_id) =
        super::chat::active_session_queue_boundary(state, record.session_id.as_str());
    let safe_boundary = crate::application::session_queue::SessionQueueSafeBoundary::active(
        active_run_stream,
        pending_approval,
    );
    Ok(SessionPublicQueueContext { queued_inputs, control, policy, safe_boundary, active_run_id })
}

fn default_public_session_queue_control(
    session_id: String,
) -> journal::OrchestratorSessionQueueControlRecord {
    journal::OrchestratorSessionQueueControlRecord {
        session_id,
        paused: false,
        pause_reason: None,
        updated_at_unix_ms: 0,
    }
}

async fn build_session_public_snapshot(
    state: &AppState,
    context: &gateway::RequestContext,
    base_sessions: &[journal::OrchestratorSessionRecord],
    catalog_context: &SessionCatalogContext,
    detail_context: &SessionDetailContext,
    record: &SessionCatalogRecord,
    queue_context: SessionPublicQueueContext,
) -> Result<SessionPublicSnapshot, Response> {
    let last_run_snapshot = record
        .last_run_id
        .as_ref()
        .and_then(|run_id| catalog_context.run_snapshot_by_id.get(run_id))
        .cloned();
    let active_run_id = queue_context
        .active_run_id
        .clone()
        .or_else(|| active_run_id_from_last_run(record, last_run_snapshot.as_ref()));
    let active_run_snapshot =
        load_snapshot_run(state, active_run_id.as_deref(), last_run_snapshot.as_ref()).await?;
    let run_for_usage = active_run_snapshot.as_ref().or(last_run_snapshot.as_ref());
    let queue = build_session_snapshot_queue(queue_context);
    let active_run = build_session_snapshot_run(
        active_run_id.as_deref(),
        active_run_snapshot.as_ref(),
        record.last_run_state.as_deref(),
    );
    let active_run_state = active_run.as_ref().map(|run| run.state.as_str());
    let pending_approval = record.pending_approvals > 0 || queue.safe_boundary.pending_approval;
    let lifecycle = derive_session_snapshot_lifecycle(SessionSnapshotLifecycleInputs {
        archived: record.archived,
        archived_at_unix_ms: record.archived_at_unix_ms,
        updated_at_unix_ms: record.updated_at_unix_ms,
        last_run_state: record.last_run_state.as_deref(),
        active_run_state,
        active_run_stream: queue.safe_boundary.active_run_stream,
        pending_approval,
        pending_depth: queue.pending_depth,
        queue_busy_state: queue.busy_state.as_str(),
    });
    let compaction = build_session_snapshot_compaction(
        record,
        detail_context,
        queue.safe_boundary.active_run_stream,
    );
    let suspend_resume = SessionSnapshotSuspendResume {
        paused: queue.paused,
        can_pause: !record.archived && !queue.paused,
        can_resume: !record.archived && queue.paused,
        pause_reason: queue.pause_reason.clone(),
    };
    let approvals = SessionSnapshotApprovals {
        pending: pending_approval,
        pending_count: record.pending_approvals,
        active_run_pending_approval: queue.safe_boundary.pending_approval,
    };
    let usage = build_session_snapshot_usage(run_for_usage);
    let safe_operations = derive_session_safe_operations(SessionSafeOperationInputs {
        archived: record.archived,
        active_run_id_present: active_run_id.is_some(),
        active_run_state,
        active_run_stream: queue.safe_boundary.active_run_stream,
        queue_paused: queue.paused,
        pending_depth: queue.pending_depth,
        can_compact: compaction.can_preview,
        can_repair_binding: record.quick_controls.reset_to_default_available
            || record.quick_controls.agent.value.is_none(),
    });
    let subagents = build_session_snapshot_subagents(state, context, record, base_sessions).await?;
    let last_error = active_run_snapshot
        .as_ref()
        .and_then(|run| run.last_error.as_deref())
        .or_else(|| last_run_snapshot.as_ref().and_then(|run| run.last_error.as_deref()))
        .and_then(|value| normalize_catalog_text(value, SESSION_CATALOG_PREVIEW_LEN));

    Ok(SessionPublicSnapshot {
        schema_version: SESSION_PUBLIC_SNAPSHOT_SCHEMA_VERSION,
        identity: SessionSnapshotIdentity {
            session_id: record.session_id.clone(),
            session_key: record.session_key.clone(),
            session_label: record.session_label.clone(),
            title: record.title.clone(),
            branch_state: record.branch_state.clone(),
            owner: SessionSnapshotOwner {
                principal: SESSION_PUBLIC_SNAPSHOT_REDACTED,
                device_id: SESSION_PUBLIC_SNAPSHOT_REDACTED,
                channel: record.channel.clone(),
                redaction_level: "owner_and_path_metadata",
            },
            created_at_unix_ms: record.created_at_unix_ms,
            updated_at_unix_ms: record.updated_at_unix_ms,
        },
        lifecycle,
        active_run,
        queue,
        binding: SessionSnapshotBinding {
            agent: record.quick_controls.agent.clone(),
            model: record.quick_controls.model.clone(),
            thinking: record.quick_controls.thinking.clone(),
            trace: record.quick_controls.trace.clone(),
            verbose: record.quick_controls.verbose.clone(),
            reset_to_default_available: record.quick_controls.reset_to_default_available,
        },
        compaction,
        suspend_resume,
        approvals,
        usage,
        safe_operations,
        subagents,
        last_error,
    })
}

async fn load_snapshot_run(
    state: &AppState,
    run_id: Option<&str>,
    cached: Option<&journal::OrchestratorRunStatusSnapshot>,
) -> Result<Option<journal::OrchestratorRunStatusSnapshot>, Response> {
    let Some(run_id) = run_id else {
        return Ok(None);
    };
    if cached.is_some_and(|run| run.run_id == run_id) {
        return Ok(cached.cloned());
    }
    state
        .runtime
        .orchestrator_run_status_snapshot(run_id.to_owned())
        .await
        .map_err(runtime_status_response)
}

fn active_run_id_from_last_run(
    record: &SessionCatalogRecord,
    last_run: Option<&journal::OrchestratorRunStatusSnapshot>,
) -> Option<String> {
    let state = last_run.map(|run| run.state.as_str()).or(record.last_run_state.as_deref())?;
    run_state_is_active(state).then(|| record.last_run_id.clone()).flatten()
}

fn build_session_snapshot_queue(context: SessionPublicQueueContext) -> SessionSnapshotQueue {
    let pending_depth = pending_queue_depth(
        context.queued_inputs.as_slice(),
        Some(context.policy.coalescing_group.as_str()),
    );
    let analysis = analyze_session_queue(
        context.queued_inputs.as_slice(),
        &context.policy,
        &context.safe_boundary,
        context.control.paused,
        crate::gateway::current_unix_ms(),
    );
    let metrics = analysis.metrics.snapshot_json();
    let analysis_json = analysis.snapshot_json();
    SessionSnapshotQueue {
        paused: context.control.paused,
        pause_reason: context
            .control
            .pause_reason
            .as_deref()
            .and_then(|value| normalize_catalog_text(value, SESSION_CATALOG_PREVIEW_LEN)),
        control_updated_at_unix_ms: context.control.updated_at_unix_ms,
        active_run_id: context.active_run_id,
        pending_depth,
        total_count: analysis.metrics.total_count,
        terminal_count: analysis.metrics.terminal_count,
        busy_state: analysis.busy_state.as_str().to_owned(),
        recommendation: analysis.recommendation.clone(),
        can_accept_followups: context.safe_boundary.can_steer()
            || (!context.control.paused && pending_depth < context.policy.cap),
        safe_boundary: context.safe_boundary,
        policy: context.policy.snapshot_json(),
        metrics,
        analysis: analysis_json,
    }
}

fn build_session_snapshot_run(
    active_run_id: Option<&str>,
    run: Option<&journal::OrchestratorRunStatusSnapshot>,
    fallback_state: Option<&str>,
) -> Option<SessionSnapshotRun> {
    let run_id =
        active_run_id.map(str::to_owned).or_else(|| run.map(|snapshot| snapshot.run_id.clone()))?;
    Some(SessionSnapshotRun {
        run_id,
        state: run
            .map(|snapshot| snapshot.state.clone())
            .or_else(|| fallback_state.map(str::to_owned))
            .unwrap_or_else(|| "unknown".to_owned()),
        cancel_requested: run.is_some_and(|snapshot| snapshot.cancel_requested),
        cancel_reason: run
            .and_then(|snapshot| snapshot.cancel_reason.as_deref())
            .and_then(|value| normalize_catalog_text(value, SESSION_CATALOG_PREVIEW_LEN)),
        started_at_unix_ms: run.map(|snapshot| snapshot.started_at_unix_ms),
        updated_at_unix_ms: run.map(|snapshot| snapshot.updated_at_unix_ms),
        completed_at_unix_ms: run.and_then(|snapshot| snapshot.completed_at_unix_ms),
        origin_kind: run.map(|snapshot| snapshot.origin_kind.clone()).unwrap_or_default(),
        origin_run_id: run.and_then(|snapshot| snapshot.origin_run_id.clone()),
        parent_run_id: run.and_then(|snapshot| snapshot.parent_run_id.clone()),
    })
}

struct SessionSnapshotLifecycleInputs<'a> {
    archived: bool,
    archived_at_unix_ms: Option<i64>,
    updated_at_unix_ms: i64,
    last_run_state: Option<&'a str>,
    active_run_state: Option<&'a str>,
    active_run_stream: bool,
    pending_approval: bool,
    pending_depth: usize,
    queue_busy_state: &'a str,
}

fn derive_session_snapshot_lifecycle(
    input: SessionSnapshotLifecycleInputs<'_>,
) -> SessionSnapshotLifecycle {
    let run_state = input.active_run_state.or(input.last_run_state).map(str::to_owned);
    let (state, reason) = if input.archived {
        ("archived", "session_archived")
    } else if input.pending_approval {
        ("approval_pending", "approval_pending")
    } else if input.active_run_stream || input.active_run_state.is_some_and(run_state_is_active) {
        ("running", "active_run")
    } else if input.pending_depth > 0 {
        ("queued", "queued_inputs_pending")
    } else if input.active_run_state.or(input.last_run_state).is_some_and(run_state_is_failed) {
        ("failed", "last_run_failed")
    } else {
        ("idle", "no_active_run_or_queue")
    };
    SessionSnapshotLifecycle {
        state: state.to_owned(),
        reason: reason.to_owned(),
        run_state,
        queue_busy_state: input.queue_busy_state.to_owned(),
        updated_at_unix_ms: input.updated_at_unix_ms,
        archived_at_unix_ms: input.archived_at_unix_ms,
    }
}

fn build_session_snapshot_compaction(
    record: &SessionCatalogRecord,
    detail_context: &SessionDetailContext,
    active_run_stream: bool,
) -> SessionSnapshotCompaction {
    let can_preview = !record.archived && !active_run_stream;
    let state = if record.archived {
        "archived"
    } else if active_run_stream {
        "blocked_active_run"
    } else if detail_context.compaction_artifact_count > 0 {
        "available"
    } else {
        "not_requested"
    };
    SessionSnapshotCompaction {
        state: state.to_owned(),
        can_preview,
        can_apply: can_preview,
        artifact_count: detail_context.compaction_artifact_count,
        latest_artifact_id: detail_context.latest_compaction_artifact_id.clone(),
    }
}

fn build_session_snapshot_usage(
    run: Option<&journal::OrchestratorRunStatusSnapshot>,
) -> SessionSnapshotUsage {
    let tokens = run
        .map(|snapshot| SessionSnapshotTokenCounters {
            prompt: Some(snapshot.prompt_tokens),
            completion: Some(snapshot.completion_tokens),
            total: Some(snapshot.total_tokens),
            source: "run_snapshot".to_owned(),
        })
        .unwrap_or_else(|| SessionSnapshotTokenCounters {
            prompt: None,
            completion: None,
            total: None,
            source: "unavailable".to_owned(),
        });
    SessionSnapshotUsage {
        tokens,
        cost: SessionSnapshotCostCounters {
            estimated_usd: None,
            currency: None,
            source: "unavailable".to_owned(),
        },
    }
}

struct SessionSafeOperationInputs<'a> {
    archived: bool,
    active_run_id_present: bool,
    active_run_state: Option<&'a str>,
    active_run_stream: bool,
    queue_paused: bool,
    pending_depth: usize,
    can_compact: bool,
    can_repair_binding: bool,
}

fn derive_session_safe_operations(
    input: SessionSafeOperationInputs<'_>,
) -> SessionSnapshotSafeOperations {
    let active_run_blocks_start = input.active_run_stream
        || input.active_run_state.is_some_and(run_state_is_active)
        || input.active_run_id_present;
    let can_start_run = !input.archived
        && !input.queue_paused
        && !active_run_blocks_start
        && input.pending_depth == 0;
    let can_cancel = !input.archived
        && input.active_run_id_present
        && input.active_run_state.is_none_or(|state| !run_state_is_terminal(state));
    let can_fork = !input.archived;
    let can_pause_queue = !input.archived && !input.queue_paused;
    let can_resume_queue = !input.archived && input.queue_paused;
    let mut blocking_reasons = Vec::new();
    if input.archived {
        blocking_reasons.push("session_archived".to_owned());
    }
    if input.queue_paused {
        blocking_reasons.push("queue_paused".to_owned());
    }
    if active_run_blocks_start {
        blocking_reasons.push("active_run_present".to_owned());
    }
    if input.pending_depth > 0 {
        blocking_reasons.push("queued_inputs_pending".to_owned());
    }
    SessionSnapshotSafeOperations {
        can_start_run,
        can_cancel,
        can_fork,
        can_compact: input.can_compact,
        can_repair_binding: input.can_repair_binding,
        can_pause_queue,
        can_resume_queue,
        blocking_reasons,
    }
}

async fn build_session_snapshot_subagents(
    state: &AppState,
    context: &gateway::RequestContext,
    record: &SessionCatalogRecord,
    base_sessions: &[journal::OrchestratorSessionRecord],
) -> Result<SessionSnapshotSubagents, Response> {
    let mut child_session_ids = base_sessions
        .iter()
        .filter(|candidate| {
            candidate.parent_session_id.as_deref() == Some(record.session_id.as_str())
        })
        .map(|candidate| candidate.session_id.clone())
        .collect::<Vec<_>>();
    child_session_ids.sort();
    let records = load_session_subagent_records(state, context, record.session_id.as_str()).await?;
    let stale_link_count = records
        .iter()
        .filter(|subagent| {
            subagent.stale_link_repair.status == crate::delegation::SubagentLinkStatus::Stale
        })
        .count();
    Ok(SessionSnapshotSubagents {
        parent_session_id: record.parent_session_id.clone(),
        child_count: child_session_ids.len(),
        child_session_ids,
        subagent_count: records.len(),
        stale_link_count,
        records,
    })
}

#[allow(clippy::result_large_err)]
pub(crate) async fn load_session_subagent_records(
    state: &AppState,
    context: &gateway::RequestContext,
    session_id: &str,
) -> Result<Vec<crate::delegation::SubagentSessionRecord>, Response> {
    let tasks = state
        .runtime
        .list_orchestrator_background_tasks(journal::OrchestratorBackgroundTaskListFilter {
            owner_principal: Some(context.principal.clone()),
            device_id: Some(context.device_id.clone()),
            channel: context.channel.clone(),
            session_id: Some(session_id.to_owned()),
            include_completed: true,
            limit: 64,
        })
        .await
        .map_err(runtime_status_response)?;
    let mut records = Vec::new();
    for task in tasks.into_iter().filter(|task| task.delegation.is_some()) {
        let run = if let Some(run_id) = task.target_run_id.as_ref() {
            state
                .runtime
                .orchestrator_run_status_snapshot(run_id.clone())
                .await
                .map_err(runtime_status_response)?
        } else {
            None
        };
        records.push(build_subagent_session_record_from_task(&task, run.as_ref())?);
    }
    records.sort_by(|left, right| {
        right
            .created_at_unix_ms
            .cmp(&left.created_at_unix_ms)
            .then_with(|| left.task_id.cmp(&right.task_id))
    });
    Ok(records)
}

#[allow(clippy::result_large_err)]
fn build_subagent_session_record_from_task(
    task: &journal::OrchestratorBackgroundTaskRecord,
    run: Option<&journal::OrchestratorRunStatusSnapshot>,
) -> Result<crate::delegation::SubagentSessionRecord, Response> {
    let delegation = task.delegation.clone().ok_or_else(|| {
        runtime_status_response(tonic::Status::failed_precondition(
            "subagent record requires delegation metadata",
        ))
    })?;
    let scope = build_subagent_task_scope(task, &delegation)?;
    let child_state = run.map(|snapshot| snapshot.state.as_str()).unwrap_or(task.state.as_str());
    let terminal = subagent_task_terminal(task, run);
    let merge_preview = subagent_merge_preview_json(task, run);
    Ok(crate::delegation::build_subagent_session_record(
        crate::delegation::SubagentSessionRecordBuildRequest {
            task_id: task.task_id.clone(),
            parent_run_id: task.parent_run_id.clone(),
            child_run_id: task.target_run_id.clone(),
            child_session_id: task.session_id.clone(),
            scope,
            delegation,
            status: child_state.to_owned(),
            child_run_exists: run.is_some(),
            task_terminal: terminal,
            artifacts: merge_preview
                .get("changed_artifacts")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            evidence_refs: merge_preview
                .get("evidence_refs")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            verification_state: subagent_verification_state(child_state, terminal, &merge_preview),
            created_at_unix_ms: task.created_at_unix_ms,
            updated_at_unix_ms: task.updated_at_unix_ms,
        },
    ))
}

#[allow(clippy::result_large_err)]
fn build_subagent_task_scope(
    task: &journal::OrchestratorBackgroundTaskRecord,
    delegation: &crate::delegation::DelegationSnapshot,
) -> Result<crate::delegation::DelegatedRunScope, Response> {
    let mut context_refs = Vec::new();
    if let Some(parent_run_id) = task.parent_run_id.as_deref() {
        context_refs.push(crate::delegation::DelegatedReferenceInput {
            ref_id: parent_run_id.to_owned(),
            reason: "parent run summary and progress refs".to_owned(),
            sensitivity: "internal".to_owned(),
        });
    }
    let memory_refs =
        if delegation.memory_scope == crate::delegation::DelegationMemoryScopeKind::None {
            Vec::new()
        } else {
            vec![crate::delegation::DelegatedReferenceInput {
                ref_id: task.session_id.clone(),
                reason: "delegated memory recall scope".to_owned(),
                sensitivity: "internal".to_owned(),
            }]
        };
    crate::delegation::build_delegated_scope(crate::delegation::DelegatedScopeBuildRequest {
        objective: subagent_task_objective(task),
        delegation: delegation.clone(),
        parent_tool_allowlist: delegation.tool_allowlist.clone(),
        parent_skill_allowlist: delegation.skill_allowlist.clone(),
        context_refs,
        memory_refs,
        artifact_refs: Vec::new(),
    })
    .map_err(runtime_status_response)
}

fn subagent_task_objective(task: &journal::OrchestratorBackgroundTaskRecord) -> String {
    task.input_text
        .as_deref()
        .and_then(|value| normalize_catalog_text(value, 512))
        .unwrap_or_else(|| format!("Delegated task {} ({})", task.task_id, task.task_kind))
}

fn subagent_task_terminal(
    task: &journal::OrchestratorBackgroundTaskRecord,
    run: Option<&journal::OrchestratorRunStatusSnapshot>,
) -> bool {
    palyra_common::runtime_contracts::AuxiliaryTaskState::from_str(task.state.as_str())
        .is_some_and(palyra_common::runtime_contracts::AuxiliaryTaskState::is_terminal)
        || run.is_some_and(|snapshot| subagent_run_state_terminal(snapshot.state.as_str()))
}

fn subagent_run_state_terminal(state: &str) -> bool {
    matches!(state, "done" | "failed" | "cancelled" | "canceled" | "timed_out" | "rejected")
}

fn subagent_merge_preview_json(
    task: &journal::OrchestratorBackgroundTaskRecord,
    run: Option<&journal::OrchestratorRunStatusSnapshot>,
) -> Value {
    let result_json = task.result_json.as_deref().and_then(parse_json_object);
    let merge_result = run
        .and_then(|snapshot| snapshot.merge_result.as_ref())
        .and_then(|merge| serde_json::to_value(merge).ok())
        .or_else(|| result_json.as_ref().and_then(|value| value.get("merge_result").cloned()));
    let Some(merge_result) = merge_result else {
        return json!({
            "ready": false,
            "reason": "merge preview is not available until the child run reaches a merge checkpoint",
        });
    };
    crate::delegation::redact_subagent_operator_value(json!({
        "ready": true,
        "summary": merge_result
            .get("summary_text")
            .and_then(Value::as_str)
            .and_then(|value| normalize_catalog_text(value, SESSION_CATALOG_PREVIEW_LEN))
            .unwrap_or_else(|| "no summary".to_owned()),
        "evidence_refs": merge_result.get("provenance").cloned().unwrap_or_else(|| json!([])),
        "changed_artifacts": merge_result
            .get("artifact_references")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "approval_required": merge_result
            .get("approval_required")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        "warnings": merge_result.get("warnings").cloned().unwrap_or_else(|| json!([])),
    }))
}

fn subagent_verification_state(child_state: &str, terminal: bool, merge_preview: &Value) -> String {
    if merge_preview.get("ready").and_then(Value::as_bool).unwrap_or(false)
        && terminal
        && !matches!(child_state, "failed" | "cancelled" | "canceled" | "timed_out" | "rejected")
    {
        "verified".to_owned()
    } else if terminal {
        "terminal_without_verified_merge".to_owned()
    } else {
        "pending".to_owned()
    }
}

fn parse_json_object(value: &str) -> Option<Value> {
    serde_json::from_str::<Value>(value).ok().filter(Value::is_object)
}

fn run_state_is_active(state: &str) -> bool {
    crate::orchestrator::RunLifecycleState::from_str(state)
        .is_some_and(|state| !state.is_terminal())
}

fn run_state_is_failed(state: &str) -> bool {
    state == crate::orchestrator::RunLifecycleState::Failed.as_str()
}

fn run_state_is_terminal(state: &str) -> bool {
    crate::orchestrator::RunLifecycleState::from_str(state)
        .is_some_and(crate::orchestrator::RunLifecycleState::is_terminal)
}

async fn build_session_project_context_envelope(
    state: &AppState,
    context: &gateway::RequestContext,
    session_id: &str,
    preview: crate::application::project_context::ProjectContextPreviewEnvelope,
    action: &'static str,
    scaffold: Option<crate::application::project_context::ProjectContextScaffoldOutcome>,
) -> Result<SessionProjectContextEnvelope, Response> {
    let session = load_session_catalog_record(state, context, session_id).await?;
    Ok(SessionProjectContextEnvelope {
        contract: contract_descriptor(),
        session,
        preview,
        action,
        scaffold,
    })
}

/// Gathers the cross-session lookup tables (approvals, workspace files,
/// project context, agent bindings, families) used to enrich every catalog
/// record without per-record refetching.
async fn load_session_catalog_context(
    state: &AppState,
    context: &gateway::RequestContext,
    base_sessions: &[journal::OrchestratorSessionRecord],
) -> Result<SessionCatalogContext, Response> {
    let approvals = load_scoped_pending_approvals(state, context.principal.as_str())
        .await
        .map_err(runtime_status_response)?;
    let mut pending_approvals_by_session = HashMap::<String, usize>::new();
    for record in approvals.into_iter().filter(|record| record.decision.is_none()) {
        *pending_approvals_by_session.entry(record.session_id).or_insert(0) += 1;
    }

    let workspace_by_session = load_session_workspace_summaries(state, context).await?;
    let project_context_by_session =
        crate::application::project_context_summary::load_project_context_summaries(
            &state.runtime,
            context,
            base_sessions,
        )
        .await
        .map_err(runtime_status_response)?;
    let (bindings_by_session, agents_by_id, default_agent_id) =
        load_session_agent_metadata(state, context).await?;
    let family_by_session = build_session_family_metadata(base_sessions);
    let run_snapshot_by_id = load_session_catalog_run_snapshots(state, base_sessions).await?;
    let effective_model_profile = effective_model_profile_from_provider_snapshot(
        &state.runtime.model_provider_status_snapshot(),
    );

    Ok(SessionCatalogContext {
        pending_approvals_by_session,
        workspace_by_session,
        project_context_by_session,
        family_by_session,
        run_snapshot_by_id,
        bindings_by_session,
        agents_by_id,
        default_agent_id,
        effective_model_profile,
    })
}

async fn load_session_catalog_run_snapshots(
    state: &AppState,
    base_sessions: &[journal::OrchestratorSessionRecord],
) -> Result<HashMap<String, journal::OrchestratorRunStatusSnapshot>, Response> {
    let run_ids =
        base_sessions.iter().filter_map(|session| session.last_run_id.clone()).collect::<Vec<_>>();
    let snapshots = state
        .runtime
        .list_orchestrator_run_status_snapshots(run_ids)
        .await
        .map_err(runtime_status_response)?;
    Ok(snapshots.into_iter().map(|snapshot| (snapshot.run_id.clone(), snapshot)).collect())
}

/// Picks the model profile the runtime would actually use, preferring the
/// registry default over provider-specific model ids.
fn effective_model_profile_from_provider_snapshot(
    snapshot: &crate::model_provider::ProviderStatusSnapshot,
) -> Option<String> {
    [
        snapshot.registry.default_chat_model_id.as_deref(),
        snapshot.model_id.as_deref(),
        snapshot.openai_model.as_deref(),
        snapshot.anthropic_model.as_deref(),
    ]
    .into_iter()
    .find_map(normalize_effective_model_profile)
}

fn normalize_effective_model_profile(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim).filter(|value| !value.is_empty()).map(str::to_owned)
}

fn agent_default_model_profile(agent: &AgentRecord) -> Option<String> {
    normalize_effective_model_profile(Some(agent.default_model_profile.as_str()))
}

async fn load_session_workspace_summaries(
    state: &AppState,
    context: &gateway::RequestContext,
) -> Result<HashMap<String, SessionWorkspaceSummary>, Response> {
    let documents = state
        .runtime
        .list_workspace_documents(journal::WorkspaceDocumentListFilter {
            principal: context.principal.clone(),
            channel: context.channel.clone(),
            agent_id: None,
            prefix: None,
            include_deleted: false,
            limit: SESSION_CATALOG_WORKSPACE_PAGE,
        })
        .await
        .map_err(runtime_status_response)?;

    let mut touched_files = HashMap::<String, HashSet<String>>::new();

    for document in documents {
        let Some(session_id) = document.latest_session_id.clone() else {
            continue;
        };
        touched_files.entry(session_id).or_default().insert(document.path);
    }

    Ok(touched_files
        .into_iter()
        .map(|(session_id, touched)| {
            (
                session_id,
                SessionWorkspaceSummary {
                    touched_files: sorted_limited_paths(touched, SESSION_CATALOG_RECAP_ITEMS_LIMIT),
                },
            )
        })
        .collect())
}

async fn load_session_agent_metadata(
    state: &AppState,
    context: &gateway::RequestContext,
) -> Result<
    (HashMap<String, SessionAgentBinding>, HashMap<String, AgentRecord>, Option<String>),
    Response,
> {
    let bindings = state
        .runtime
        .list_agent_bindings(AgentBindingQuery {
            agent_id: None,
            principal: Some(context.principal.clone()),
            channel: context.channel.clone(),
            session_id: None,
            limit: Some(1_000),
        })
        .await
        .map_err(runtime_status_response)?;
    let mut agents = Vec::new();
    let mut after_agent_id = None::<String>;
    let mut default_agent_id = None::<String>;
    loop {
        let page = state
            .runtime
            .list_agents(after_agent_id.clone(), Some(100))
            .await
            .map_err(runtime_status_response)?;
        if default_agent_id.is_none() {
            default_agent_id = page.default_agent_id.clone();
        }
        agents.extend(page.agents);
        let Some(next_after) = page.next_after_agent_id else {
            break;
        };
        after_agent_id = Some(next_after);
    }
    Ok((
        bindings.into_iter().map(|binding| (binding.session_id.clone(), binding)).collect(),
        agents.into_iter().map(|agent| (agent.agent_id.clone(), agent)).collect(),
        default_agent_id,
    ))
}

/// Groups sessions into branch families keyed by root title and derives each
/// session's sequence number, parent title, and capped relatives list.
fn build_session_family_metadata(
    sessions: &[journal::OrchestratorSessionRecord],
) -> HashMap<String, SessionCatalogFamilyRecord> {
    let sessions_by_id = sessions
        .iter()
        .map(|session| (session.session_id.as_str(), session))
        .collect::<HashMap<_, _>>();
    let mut family_root_by_session = HashMap::<String, String>::new();
    for session in sessions {
        let mut visiting = HashSet::new();
        let _ = resolve_session_family_root(
            session.session_id.as_str(),
            &sessions_by_id,
            &mut family_root_by_session,
            &mut visiting,
        );
    }

    let mut members_by_root = HashMap::<String, Vec<&journal::OrchestratorSessionRecord>>::new();
    for session in sessions {
        let root = family_root_by_session
            .get(session.session_id.as_str())
            .cloned()
            .unwrap_or_else(|| session.title.clone());
        members_by_root.entry(root).or_default().push(session);
    }
    for members in members_by_root.values_mut() {
        members.sort_by(|left, right| {
            left.created_at_unix_ms
                .cmp(&right.created_at_unix_ms)
                .then_with(|| left.session_id.cmp(&right.session_id))
        });
    }

    sessions
        .iter()
        .map(|session| {
            let root_title = family_root_by_session
                .get(session.session_id.as_str())
                .cloned()
                .unwrap_or_else(|| session.title.clone());
            let members = members_by_root.get(root_title.as_str()).cloned().unwrap_or_default();
            let sequence = members
                .iter()
                .position(|entry| entry.session_id == session.session_id)
                .map(|index| index as u64 + 1)
                .unwrap_or(1);
            let parent_title = session.parent_session_id.as_deref().and_then(|parent_session_id| {
                sessions_by_id.get(parent_session_id).map(|parent| parent.title.clone())
            });
            let relatives = members
                .iter()
                .filter(|entry| entry.session_id != session.session_id)
                .map(|entry| SessionCatalogFamilyRelative {
                    session_id: entry.session_id.clone(),
                    title: entry.title.clone(),
                    branch_state: entry.branch_state.clone(),
                    relation: if session.parent_session_id.as_deref()
                        == Some(entry.session_id.as_str())
                    {
                        "parent".to_owned()
                    } else if entry.parent_session_id.as_deref()
                        == Some(session.session_id.as_str())
                    {
                        "child".to_owned()
                    } else {
                        "sibling".to_owned()
                    },
                })
                .take(SESSION_CATALOG_RELATIVES_LIMIT)
                .collect::<Vec<_>>();
            (
                session.session_id.clone(),
                SessionCatalogFamilyRecord {
                    root_title,
                    sequence,
                    family_size: members.len(),
                    parent_session_id: session.parent_session_id.clone(),
                    parent_title,
                    relatives,
                },
            )
        })
        .collect()
}

/// Walks `parent_session_id` links to the family root title with memoization.
/// Cyclic or self-referential lineage falls back to the current session title
/// so corrupted journal data cannot recurse indefinitely.
fn resolve_session_family_root<'a>(
    session_id: &str,
    sessions_by_id: &HashMap<&'a str, &'a journal::OrchestratorSessionRecord>,
    memo: &mut HashMap<String, String>,
    visiting: &mut HashSet<String>,
) -> Option<String> {
    if let Some(existing) = memo.get(session_id) {
        return Some(existing.clone());
    }
    let session = sessions_by_id.get(session_id).copied()?;
    if !visiting.insert(session_id.to_owned()) {
        return None;
    }
    let root = if let Some(parent_session_id) = session.parent_session_id.as_deref() {
        if parent_session_id == session.session_id {
            session.title.clone()
        } else {
            resolve_session_family_root(parent_session_id, sessions_by_id, memo, visiting)
                .unwrap_or_else(|| normalized_title_family_root(session.title.as_str()))
        }
    } else {
        normalized_title_family_root(session.title.as_str())
    };
    visiting.remove(session_id);
    memo.insert(session.session_id.clone(), root.clone());
    Some(root)
}

/// Normalizes a title into a family-root key. Titles ending in an all-digit
/// `#N` suffix collapse to the prefix so branch copies named `<root> #2`,
/// `<root> #3`, ... group under the same root as the original.
fn normalized_title_family_root(raw: &str) -> String {
    let normalized = normalize_catalog_text(raw, SESSION_CATALOG_TITLE_LEN)
        .unwrap_or_else(|| raw.trim().to_owned());
    let Some((prefix, suffix)) = normalized.rsplit_once('#') else {
        return normalized;
    };
    if suffix.trim().chars().all(|value| value.is_ascii_digit()) {
        normalize_catalog_text(prefix.trim(), SESSION_CATALOG_TITLE_LEN).unwrap_or(normalized)
    } else {
        normalized
    }
}

/// Loads the per-session artifact details (checkpoints and compaction
/// artifacts) that are only fetched for detail and mutation responses, not
/// for list pages.
async fn load_session_detail_context(
    state: &AppState,
    _context: &gateway::RequestContext,
    session_id: &str,
) -> Result<SessionDetailContext, Response> {
    let checkpoints = state
        .runtime
        .list_orchestrator_checkpoints(session_id.to_owned())
        .await
        .map_err(runtime_status_response)?;
    let compactions = state
        .runtime
        .list_orchestrator_compaction_artifacts(session_id.to_owned())
        .await
        .map_err(runtime_status_response)?;

    let artifact_count = checkpoints.len() + compactions.len();
    let mut recent_artifacts = Vec::new();
    recent_artifacts.extend(checkpoints.iter().take(SESSION_CATALOG_RECAP_ITEMS_LIMIT).map(
        |entry| SessionCatalogArtifactRecord {
            artifact_id: entry.checkpoint_id.clone(),
            kind: "checkpoint".to_owned(),
            label: entry.name.clone(),
        },
    ));
    let remaining = SESSION_CATALOG_RECAP_ITEMS_LIMIT.saturating_sub(recent_artifacts.len());
    recent_artifacts.extend(compactions.iter().take(remaining).map(|entry| {
        SessionCatalogArtifactRecord {
            artifact_id: entry.artifact_id.clone(),
            kind: "compaction".to_owned(),
            label: entry.summary_preview.clone(),
        }
    }));

    Ok(SessionDetailContext {
        recent_artifacts,
        artifact_count,
        compaction_artifact_count: compactions.len(),
        latest_compaction_artifact_id: compactions.first().map(|entry| entry.artifact_id.clone()),
    })
}

/// Assembles one wire-facing catalog record from the base session, the shared
/// catalog context, and optional detail data; all free-text fields are
/// redacted and truncated here before they reach the console.
async fn build_session_catalog_record(
    _state: &AppState,
    context: &SessionCatalogContext,
    session: journal::OrchestratorSessionRecord,
    detail_context: Option<SessionDetailContext>,
) -> Result<SessionCatalogRecord, Response> {
    let run_snapshot =
        session.last_run_id.as_ref().and_then(|run_id| context.run_snapshot_by_id.get(run_id));
    let pending_approvals =
        context.pending_approvals_by_session.get(session.session_id.as_str()).copied().unwrap_or(0);
    let workspace =
        context.workspace_by_session.get(session.session_id.as_str()).cloned().unwrap_or_default();
    let family =
        context.family_by_session.get(session.session_id.as_str()).cloned().unwrap_or_else(|| {
            SessionCatalogFamilyRecord {
                root_title: session.title.clone(),
                sequence: 1,
                family_size: 1,
                parent_session_id: session.parent_session_id.clone(),
                parent_title: None,
                relatives: Vec::new(),
            }
        });
    let detail_context = detail_context.unwrap_or_default();
    let project_context =
        context.project_context_by_session.get(session.session_id.as_str()).cloned();
    let active_project_context_paths = project_context
        .as_ref()
        .map(|preview| {
            preview
                .entries
                .iter()
                .filter(|entry| entry.active)
                .map(|entry| entry.path.clone())
                .take(SESSION_CATALOG_RECAP_ITEMS_LIMIT)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let quick_controls = build_session_quick_controls(context, &session);
    let agent_id = quick_controls.agent.value.clone();
    let model_profile = quick_controls.model.value.clone();
    let session_id = session.session_id.clone();
    let session_title = normalize_catalog_text(session.title.as_str(), SESSION_CATALOG_TITLE_LEN)
        .unwrap_or_else(|| session_id.clone());
    let preview = session
        .preview
        .as_deref()
        .and_then(|value| normalize_catalog_text(value, SESSION_CATALOG_PREVIEW_LEN));
    let last_intent = session
        .last_intent
        .as_deref()
        .and_then(|value| normalize_catalog_text(value, SESSION_CATALOG_PREVIEW_LEN));
    let last_summary = session
        .last_summary
        .as_deref()
        .and_then(|value| normalize_catalog_text(value, SESSION_CATALOG_PREVIEW_LEN));
    // Prefer the live run snapshot over the state persisted on the session
    // record, which can lag behind an in-flight run.
    let last_run_state =
        run_snapshot.map(|run| run.state.clone()).or_else(|| session.last_run_state.clone());
    let recap = SessionCatalogRecapRecord {
        touched_files: workspace.touched_files.clone(),
        active_context_files: active_project_context_paths.clone(),
        project_context: project_context.as_ref().map(build_session_project_context_record),
        recent_artifacts: detail_context.recent_artifacts.clone(),
        ctas: build_session_recap_ctas(
            pending_approvals,
            !workspace.touched_files.is_empty(),
            detail_context.artifact_count > 0,
        ),
    };

    Ok(SessionCatalogRecord {
        session_id,
        session_key: session.session_key,
        session_label: session.session_label,
        title: session_title,
        title_source: session.title_source,
        title_generation_state: session.title_generation_state,
        manual_title_locked: session.manual_title_locked,
        auto_title_updated_at_unix_ms: session.auto_title_updated_at_unix_ms,
        manual_title_updated_at_unix_ms: session.manual_title_updated_at_unix_ms,
        preview: preview.clone(),
        preview_state: preview_metadata_state(preview.as_deref()).to_owned(),
        last_intent: last_intent.clone(),
        last_intent_state: preview_metadata_state(last_intent.as_deref()).to_owned(),
        last_summary: last_summary.clone(),
        last_summary_state: preview_metadata_state(last_summary.as_deref()).to_owned(),
        branch_state: session.branch_state,
        parent_session_id: session.parent_session_id,
        principal: session.principal,
        device_id: session.device_id,
        channel: session.channel,
        created_at_unix_ms: session.created_at_unix_ms,
        updated_at_unix_ms: session.updated_at_unix_ms,
        last_run_id: session.last_run_id.clone(),
        last_run_state,
        last_run_started_at_unix_ms: run_snapshot.map(|run| run.started_at_unix_ms),
        prompt_tokens: run_snapshot.map(|run| run.prompt_tokens).unwrap_or(0),
        completion_tokens: run_snapshot.map(|run| run.completion_tokens).unwrap_or(0),
        total_tokens: run_snapshot.map(|run| run.total_tokens).unwrap_or(0),
        archived: session.archived_at_unix_ms.is_some(),
        archived_at_unix_ms: session.archived_at_unix_ms,
        pending_approvals,
        has_context_files: !active_project_context_paths.is_empty(),
        last_context_file: active_project_context_paths
            .first()
            .cloned()
            .or_else(|| workspace.touched_files.first().cloned()),
        agent_id,
        model_profile,
        artifact_count: detail_context.artifact_count,
        family,
        recap,
        quick_controls,
    })
}

fn build_session_project_context_record(
    preview: &crate::application::project_context::ProjectContextPreviewEnvelope,
) -> SessionProjectContextRecord {
    SessionProjectContextRecord {
        generated_at_unix_ms: preview.generated_at_unix_ms,
        active_entries: preview.active_entries,
        blocked_entries: preview.blocked_entries,
        approval_required_entries: preview.approval_required_entries,
        disabled_entries: preview.disabled_entries,
        active_estimated_tokens: preview.active_estimated_tokens,
        warnings: preview.warnings.clone(),
        focus_paths: preview
            .focus_paths
            .iter()
            .map(|entry| SessionProjectContextFocusRecord {
                path: entry.path.clone(),
                reason: entry.reason.clone(),
            })
            .collect(),
        entries: preview
            .entries
            .iter()
            .map(|entry| SessionProjectContextEntryRecord {
                entry_id: entry.entry_id.clone(),
                order: entry.order,
                path: entry.path.clone(),
                source_kind: entry.source_kind.clone(),
                source_label: entry.source_label.clone(),
                precedence_label: entry.precedence_label.clone(),
                depth: entry.depth,
                root: entry.root,
                active: entry.active,
                disabled: entry.disabled,
                approved: entry.approved,
                status: entry.status.clone(),
                content_hash: entry.content_hash.clone(),
                loaded_at_unix_ms: entry.loaded_at_unix_ms,
                modified_at_unix_ms: entry.modified_at_unix_ms,
                estimated_tokens: entry.estimated_tokens,
                discovery_reasons: entry.discovery_reasons.clone(),
                warnings: entry.warnings.clone(),
                preview_text: entry.preview_text.clone(),
            })
            .collect(),
    }
}

/// Derives the quick-controls block for one session, resolving each control
/// from (in order) session override, runtime provider state, the bound agent,
/// and the default agent; `source` on each control names which layer won.
fn build_session_quick_controls(
    context: &SessionCatalogContext,
    session: &journal::OrchestratorSessionRecord,
) -> SessionCatalogQuickControlsRecord {
    let session_id = session.session_id.as_str();
    let binding = context.bindings_by_session.get(session_id);
    let bound_agent = binding.and_then(|record| context.agents_by_id.get(record.agent_id.as_str()));
    let inherited_agent =
        context.default_agent_id.as_deref().and_then(|agent_id| context.agents_by_id.get(agent_id));

    let agent = match (binding, bound_agent, inherited_agent) {
        (Some(binding), Some(agent), inherited) => SessionCatalogQuickControlRecord {
            value: Some(binding.agent_id.clone()),
            display_value: agent.display_name.clone(),
            source: "session_binding".to_owned(),
            inherited_value: inherited.map(|entry| entry.agent_id.clone()),
            override_active: inherited.is_none_or(|entry| entry.agent_id != binding.agent_id),
        },
        // A binding can outlive its agent record; fall back to the raw agent
        // id as the display value instead of dropping the binding.
        (Some(binding), None, inherited) => SessionCatalogQuickControlRecord {
            value: Some(binding.agent_id.clone()),
            display_value: binding.agent_id.clone(),
            source: "session_binding".to_owned(),
            inherited_value: inherited.map(|entry| entry.agent_id.clone()),
            override_active: inherited.is_none_or(|entry| entry.agent_id != binding.agent_id),
        },
        (None, _, Some(agent)) => SessionCatalogQuickControlRecord {
            value: Some(agent.agent_id.clone()),
            display_value: agent.display_name.clone(),
            source: "default".to_owned(),
            inherited_value: Some(agent.agent_id.clone()),
            override_active: false,
        },
        _ => SessionCatalogQuickControlRecord {
            value: None,
            display_value: "Unassigned".to_owned(),
            source: "unassigned".to_owned(),
            inherited_value: None,
            override_active: false,
        },
    };

    let inherited_model = bound_agent
        .and_then(agent_default_model_profile)
        .or_else(|| inherited_agent.and_then(agent_default_model_profile));
    // The runtime profile only counts as a distinct layer when it differs
    // from the agent default; otherwise the agent default reports as source.
    let runtime_model_profile = context
        .effective_model_profile
        .as_ref()
        .filter(|runtime| inherited_model.as_ref() != Some(*runtime));
    let (model_value, model_display, model_source, model_override_active) =
        if let Some(model_profile_override) = session.model_profile_override.as_ref() {
            (
                Some(model_profile_override.clone()),
                model_profile_override.clone(),
                "session_override".to_owned(),
                inherited_model.as_ref().is_none_or(|entry| entry != model_profile_override),
            )
        } else if let Some(runtime_model_profile) = runtime_model_profile {
            (
                Some(runtime_model_profile.clone()),
                runtime_model_profile.clone(),
                "model_provider_runtime".to_owned(),
                false,
            )
        } else {
            match (
                bound_agent.and_then(agent_default_model_profile),
                inherited_agent.and_then(agent_default_model_profile),
            ) {
                (Some(agent_model_profile), inherited) => (
                    Some(agent_model_profile.clone()),
                    agent_model_profile.clone(),
                    "agent_default_model_profile".to_owned(),
                    inherited.as_ref().is_none_or(|entry| entry != &agent_model_profile),
                ),
                (None, Some(agent_model_profile)) => (
                    Some(agent_model_profile.clone()),
                    agent_model_profile,
                    "default_agent_model_profile".to_owned(),
                    false,
                ),
                _ => (None, "Inherited default".to_owned(), "unassigned".to_owned(), false),
            }
        };

    // Surface defaults inherited when no session override exists: thinking is
    // on by default, trace and verbose are opt-in. An override equal to the
    // inherited value is not reported as active.
    let thinking_inherited = true;
    let trace_inherited = false;
    let verbose_inherited = false;
    let thinking_override_active =
        session.thinking_override.is_some_and(|value| value != thinking_inherited);
    let trace_override_active =
        session.trace_override.is_some_and(|value| value != trace_inherited);
    let verbose_override_active =
        session.verbose_override.is_some_and(|value| value != verbose_inherited);

    SessionCatalogQuickControlsRecord {
        agent,
        model: SessionCatalogQuickControlRecord {
            value: model_value,
            display_value: model_display,
            source: model_source,
            inherited_value: inherited_model.clone(),
            override_active: model_override_active,
        },
        thinking: SessionCatalogToggleControlRecord {
            value: session.thinking_override.unwrap_or(thinking_inherited),
            source: if session.thinking_override.is_some() {
                "session_override".to_owned()
            } else {
                "surface_default".to_owned()
            },
            inherited_value: thinking_inherited,
            override_active: thinking_override_active,
        },
        trace: SessionCatalogToggleControlRecord {
            value: session.trace_override.unwrap_or(trace_inherited),
            source: if session.trace_override.is_some() {
                "session_override".to_owned()
            } else {
                "surface_default".to_owned()
            },
            inherited_value: trace_inherited,
            override_active: trace_override_active,
        },
        verbose: SessionCatalogToggleControlRecord {
            value: session.verbose_override.unwrap_or(verbose_inherited),
            source: if session.verbose_override.is_some() {
                "session_override".to_owned()
            } else {
                "surface_default".to_owned()
            },
            inherited_value: verbose_inherited,
            override_active: verbose_override_active,
        },
        reset_to_default_available: binding.is_some()
            || session.model_profile_override.is_some()
            || session.thinking_override.is_some()
            || session.trace_override.is_some()
            || session.verbose_override.is_some(),
    }
}

fn build_session_recap_ctas(
    pending_approvals: usize,
    has_workspace_context: bool,
    has_artifacts: bool,
) -> Vec<String> {
    let mut ctas = vec!["resume".to_owned(), "open_run_inspector".to_owned(), "branch".to_owned()];
    if pending_approvals > 0 {
        ctas.push("open_approvals".to_owned());
    }
    if has_workspace_context {
        ctas.push("open_workspace".to_owned());
    }
    if has_artifacts {
        ctas.push("open_artifacts".to_owned());
    }
    ctas
}

fn sorted_limited_paths(paths: HashSet<String>, limit: usize) -> Vec<String> {
    let mut values = paths.into_iter().collect::<Vec<_>>();
    values.sort();
    values.truncate(limit);
    values
}

/// Redacts, whitespace-collapses, and truncates operator-visible catalog
/// text, returning `None` when nothing displayable remains. Redaction runs
/// before truncation so sensitive fragments embedded in runtime messages can
/// never survive into the console via a truncation boundary.
fn normalize_catalog_text(raw: &str, max_chars: usize) -> Option<String> {
    let normalized = palyra_common::redaction::redact_url_segments_in_text(
        palyra_common::redaction::redact_auth_error(raw).as_str(),
    )
    .replace(['\r', '\n'], " ");
    let trimmed = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    if trimmed.is_empty() {
        return None;
    }
    let mut truncated = trimmed.chars().take(max_chars.saturating_add(1)).collect::<String>();
    if truncated.chars().count() > max_chars {
        truncated = truncated.chars().take(max_chars).collect::<String>();
        truncated.push_str("...");
    }
    Some(truncated)
}

/// Parses the list cursor (an offset into the filtered ordering); a missing
/// or blank cursor starts at the beginning.
///
/// # Errors
/// Returns an invalid-argument response when the cursor is not an unsigned
/// integer.
#[allow(clippy::result_large_err)]
fn parse_session_catalog_cursor(raw: Option<&str>) -> Result<usize, Response> {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(0);
    };
    raw.parse::<usize>().map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "cursor must be an unsigned integer offset",
        ))
    })
}

fn normalize_session_catalog_search(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim).filter(|value| !value.is_empty()).map(|value| value.to_ascii_lowercase())
}

fn normalize_session_catalog_title_source(raw: Option<&str>) -> Option<String> {
    normalize_catalog_token(raw)
}

fn normalize_catalog_token(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim).filter(|value| !value.is_empty()).map(|value| value.to_ascii_lowercase())
}

/// Coerces the requested sort to a supported token; unknown values silently
/// fall back to `updated_desc` rather than erroring, matching the console's
/// permissive query handling.
fn normalize_session_catalog_sort(raw: Option<&str>) -> String {
    match raw.map(str::trim).filter(|value| !value.is_empty()) {
        Some("updated_asc") => "updated_asc".to_owned(),
        Some("created_desc") => "created_desc".to_owned(),
        Some("created_asc") => "created_asc".to_owned(),
        Some("title_asc") => "title_asc".to_owned(),
        Some("title_desc") => "title_desc".to_owned(),
        _ => "updated_desc".to_owned(),
    }
}

fn compare_session_catalog_records(
    left: &SessionCatalogRecord,
    right: &SessionCatalogRecord,
    sort: &str,
) -> Ordering {
    let ordering = match sort {
        "updated_asc" => left.updated_at_unix_ms.cmp(&right.updated_at_unix_ms),
        "created_desc" => right.created_at_unix_ms.cmp(&left.created_at_unix_ms),
        "created_asc" => left.created_at_unix_ms.cmp(&right.created_at_unix_ms),
        "title_asc" => left.title.cmp(&right.title),
        "title_desc" => right.title.cmp(&left.title),
        _ => right.updated_at_unix_ms.cmp(&left.updated_at_unix_ms),
    };
    // Ties break on session_id so offset paging over equal sort keys stays
    // deterministic across requests.
    if ordering == Ordering::Equal {
        left.session_id.cmp(&right.session_id)
    } else {
        ordering
    }
}

/// Case-insensitive substring search over every operator-visible text field
/// of a record, including recap files, artifacts, and family relatives.
fn session_catalog_record_matches(record: &SessionCatalogRecord, search: &str) -> bool {
    [
        Some(record.session_key.as_str()),
        record.session_label.as_deref(),
        Some(record.title.as_str()),
        Some(record.family.root_title.as_str()),
        record.preview.as_deref(),
        record.last_intent.as_deref(),
        record.last_summary.as_deref(),
        record.last_run_state.as_deref(),
        record.last_context_file.as_deref(),
        record.agent_id.as_deref(),
        record.model_profile.as_deref(),
        Some(record.quick_controls.agent.display_value.as_str()),
        Some(record.quick_controls.model.display_value.as_str()),
    ]
    .into_iter()
    .flatten()
    .chain(record.recap.touched_files.iter().map(String::as_str))
    .chain(record.recap.active_context_files.iter().map(String::as_str))
    .chain(record.recap.recent_artifacts.iter().map(|artifact| artifact.label.as_str()))
    .chain(record.family.relatives.iter().map(|relative| relative.title.as_str()))
    .any(|value| value.to_ascii_lowercase().contains(search))
}

fn preview_metadata_state(value: Option<&str>) -> &'static str {
    if value.is_some() {
        "computed"
    } else {
        "missing"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_agent(default_model_profile: &str) -> AgentRecord {
        AgentRecord {
            agent_id: "agent-default".to_owned(),
            display_name: "Default Agent".to_owned(),
            agent_dir: ".".to_owned(),
            workspace_roots: Vec::new(),
            default_model_profile: default_model_profile.to_owned(),
            execution_backend_preference:
                crate::execution_backends::ExecutionBackendPreference::Automatic,
            default_tool_allowlist: Vec::new(),
            default_skill_allowlist: Vec::new(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        }
    }

    fn test_session(model_profile_override: Option<&str>) -> journal::OrchestratorSessionRecord {
        journal::OrchestratorSessionRecord {
            session_id: "session-1".to_owned(),
            session_key: "scn-S025-cron-smoke".to_owned(),
            session_label: None,
            principal: "user:test".to_owned(),
            device_id: "device-test".to_owned(),
            channel: Some("cli".to_owned()),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 2,
            last_run_id: None,
            archived_at_unix_ms: None,
            auto_title: None,
            auto_title_source: None,
            auto_title_generator_version: None,
            auto_title_updated_at_unix_ms: None,
            title_generation_state: "missing".to_owned(),
            manual_title_locked: false,
            manual_title_updated_at_unix_ms: None,
            model_profile_override: model_profile_override.map(str::to_owned),
            thinking_override: None,
            trace_override: None,
            verbose_override: None,
            title: "Cron smoke".to_owned(),
            title_source: "manual".to_owned(),
            title_generator_version: None,
            preview: None,
            last_intent: None,
            last_summary: None,
            match_snippet: None,
            branch_state: "root".to_owned(),
            parent_session_id: None,
            branch_origin_run_id: None,
            last_run_state: None,
        }
    }

    fn test_lineage_session(
        session_id: &str,
        title: &str,
        parent_session_id: Option<&str>,
        created_at_unix_ms: i64,
    ) -> journal::OrchestratorSessionRecord {
        let mut session = test_session(None);
        session.session_id = session_id.to_owned();
        session.session_key = session_id.to_owned();
        session.title = title.to_owned();
        session.created_at_unix_ms = created_at_unix_ms;
        session.updated_at_unix_ms = created_at_unix_ms;
        session.parent_session_id = parent_session_id.map(str::to_owned);
        session.branch_state =
            if parent_session_id.is_some() { "branched" } else { "root" }.to_owned();
        session
    }

    fn test_context(
        agent: AgentRecord,
        effective_model_profile: Option<&str>,
    ) -> SessionCatalogContext {
        SessionCatalogContext {
            pending_approvals_by_session: HashMap::new(),
            workspace_by_session: HashMap::new(),
            project_context_by_session: HashMap::new(),
            family_by_session: HashMap::new(),
            run_snapshot_by_id: HashMap::new(),
            bindings_by_session: HashMap::new(),
            agents_by_id: HashMap::from([(agent.agent_id.clone(), agent)]),
            default_agent_id: Some("agent-default".to_owned()),
            effective_model_profile: effective_model_profile.map(str::to_owned),
        }
    }

    #[test]
    fn public_snapshot_lifecycle_prioritizes_pending_approval() {
        let lifecycle = derive_session_snapshot_lifecycle(SessionSnapshotLifecycleInputs {
            archived: false,
            archived_at_unix_ms: None,
            updated_at_unix_ms: 42,
            last_run_state: Some("in_progress"),
            active_run_state: Some("in_progress"),
            active_run_stream: true,
            pending_approval: true,
            pending_depth: 0,
            queue_busy_state: "waiting_on_approval",
        });

        assert_eq!(lifecycle.state, "approval_pending");
        assert_eq!(lifecycle.reason, "approval_pending");
        assert_eq!(lifecycle.run_state.as_deref(), Some("in_progress"));
        assert_eq!(lifecycle.queue_busy_state, "waiting_on_approval");
    }

    #[test]
    fn public_snapshot_safe_operations_block_start_for_active_or_queued_work() {
        let active = derive_session_safe_operations(SessionSafeOperationInputs {
            archived: false,
            active_run_id_present: true,
            active_run_state: Some("in_progress"),
            active_run_stream: true,
            queue_paused: false,
            pending_depth: 0,
            can_compact: false,
            can_repair_binding: false,
        });
        assert!(!active.can_start_run);
        assert!(active.can_cancel);
        assert!(active.blocking_reasons.iter().any(|reason| reason == "active_run_present"));

        let queued = derive_session_safe_operations(SessionSafeOperationInputs {
            archived: false,
            active_run_id_present: false,
            active_run_state: None,
            active_run_stream: false,
            queue_paused: false,
            pending_depth: 2,
            can_compact: true,
            can_repair_binding: true,
        });
        assert!(!queued.can_start_run);
        assert!(!queued.can_cancel);
        assert!(queued.can_compact);
        assert!(queued.can_repair_binding);
        assert!(queued.blocking_reasons.iter().any(|reason| reason == "queued_inputs_pending"));
    }

    #[test]
    fn subagent_record_marks_terminal_missing_child_run_as_stale() {
        let task = journal::OrchestratorBackgroundTaskRecord {
            task_id: "task-1".to_owned(),
            task_kind: palyra_common::runtime_contracts::AuxiliaryTaskKind::DelegationPrompt
                .as_str()
                .to_owned(),
            session_id: "session-1".to_owned(),
            child_session_id: None,
            parent_run_id: Some("parent-run".to_owned()),
            target_run_id: Some("child-run".to_owned()),
            planned_child_run_id: None,
            queued_input_id: None,
            owner_principal: "user:test".to_owned(),
            device_id: "device-test".to_owned(),
            channel: Some("cli".to_owned()),
            state: palyra_common::runtime_contracts::AuxiliaryTaskState::Succeeded
                .as_str()
                .to_owned(),
            priority: 0,
            revision: 2,
            execution_generation: 1,
            attempt_count: 1,
            max_attempts: 3,
            budget_tokens: 1_000,
            delegation: Some(crate::delegation::DelegationSnapshot {
                profile_id: "research".to_owned(),
                display_name: "Research".to_owned(),
                description: None,
                template_id: None,
                role: crate::delegation::DelegationRole::Research,
                execution_mode: crate::delegation::DelegationExecutionMode::Parallel,
                group_id: "default".to_owned(),
                model_profile: "deterministic".to_owned(),
                tool_allowlist: vec!["palyra.http.fetch".to_owned()],
                skill_allowlist: Vec::new(),
                memory_scope: crate::delegation::DelegationMemoryScopeKind::ParentSession,
                budget_tokens: 1_000,
                max_attempts: 3,
                merge_contract: crate::delegation::DelegationMergeContract {
                    strategy: crate::delegation::DelegationMergeStrategy::Summarize,
                    approval_required: false,
                },
                runtime_limits: crate::delegation::DelegationRuntimeLimits::default(),
                agent_id: Some("agent-default".to_owned()),
            }),
            cancellation_context: None,
            not_before_unix_ms: None,
            expires_at_unix_ms: None,
            notification_target_json: None,
            input_text: Some("Fetch docs and summarize".to_owned()),
            payload_json: None,
            last_error: None,
            result_json: None,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 2,
            started_at_unix_ms: Some(1),
            completed_at_unix_ms: Some(2),
        };

        let record = build_subagent_session_record_from_task(&task, None)
            .expect("subagent projection should build");

        assert_eq!(record.child_session_id, "session-1");
        assert_eq!(
            record.transcript_ref.status,
            crate::delegation::SubagentTranscriptStatus::Stale
        );
        assert_eq!(record.stale_link_repair.status, crate::delegation::SubagentLinkStatus::Stale);
        assert!(!record.stale_link_repair.actions.is_empty());
    }

    #[test]
    fn session_quick_controls_prefer_runtime_model_over_legacy_agent_default() {
        let context = test_context(test_agent("deterministic"), Some("MiniMax-M3"));
        let controls = build_session_quick_controls(&context, &test_session(None));

        assert_eq!(controls.model.value.as_deref(), Some("MiniMax-M3"));
        assert_eq!(controls.model.display_value, "MiniMax-M3");
        assert_eq!(controls.model.source, "model_provider_runtime");
        assert_eq!(controls.model.inherited_value.as_deref(), Some("deterministic"));
        assert!(!controls.model.override_active);
    }

    #[test]
    fn session_quick_controls_use_runtime_model_when_agent_default_is_unset() {
        let context = test_context(test_agent(""), Some("MiniMax-M3"));
        let controls = build_session_quick_controls(&context, &test_session(None));

        assert_eq!(controls.model.value.as_deref(), Some("MiniMax-M3"));
        assert_eq!(controls.model.display_value, "MiniMax-M3");
        assert_eq!(controls.model.source, "model_provider_runtime");
        assert_eq!(controls.model.inherited_value, None);
        assert!(!controls.model.override_active);
    }

    #[test]
    fn session_quick_controls_keep_explicit_model_override() {
        let context = test_context(test_agent("deterministic"), Some("MiniMax-M3"));
        let controls = build_session_quick_controls(&context, &test_session(Some("gpt-4o-mini")));

        assert_eq!(controls.model.value.as_deref(), Some("gpt-4o-mini"));
        assert_eq!(controls.model.display_value, "gpt-4o-mini");
        assert_eq!(controls.model.source, "session_override");
        assert_eq!(controls.model.inherited_value.as_deref(), Some("deterministic"));
        assert!(controls.model.override_active);
    }

    #[test]
    fn session_family_metadata_handles_parent_cycles() {
        let sessions = vec![
            test_lineage_session("session-a", "Alpha", Some("session-b"), 1),
            test_lineage_session("session-b", "Beta", Some("session-a"), 2),
        ];

        let families = build_session_family_metadata(&sessions);
        let alpha = families.get("session-a").expect("alpha family should resolve");
        let beta = families.get("session-b").expect("beta family should resolve");

        assert_eq!(alpha.root_title, beta.root_title);
        assert_eq!(alpha.family_size, 2);
        assert_eq!(beta.family_size, 2);
        assert_eq!(alpha.relatives.len(), 1);
        assert_eq!(beta.relatives.len(), 1);
    }
}
