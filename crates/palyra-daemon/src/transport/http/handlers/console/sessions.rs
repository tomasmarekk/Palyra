use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use serde::{Deserialize, Serialize};

use crate::{
    agents::{AgentBindingQuery, AgentRecord, SessionAgentBinding},
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

#[derive(Debug, Serialize)]
pub(crate) struct SessionCatalogListEnvelope {
    contract: control_plane::ContractDescriptor,
    sessions: Vec<SessionCatalogRecord>,
    summary: SessionCatalogSummary,
    query: SessionCatalogQueryEcho,
    page: control_plane::PageInfo,
}

#[derive(Debug, Serialize)]
pub(crate) struct SessionCatalogDetailEnvelope {
    contract: control_plane::ContractDescriptor,
    session: SessionCatalogRecord,
}

#[derive(Debug, Serialize)]
pub(crate) struct SessionCatalogMutationEnvelope {
    contract: control_plane::ContractDescriptor,
    session: SessionCatalogRecord,
    action: &'static str,
}

#[derive(Debug, Serialize)]
pub(crate) struct SessionCatalogRunAbortEnvelope {
    contract: control_plane::ContractDescriptor,
    run_id: String,
    cancel_requested: bool,
    reason: String,
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

#[derive(Debug, Clone, Default)]
struct SessionWorkspaceSummary {
    touched_files: Vec<String>,
    active_context_files: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct SessionDetailContext {
    recent_artifacts: Vec<SessionCatalogArtifactRecord>,
    artifact_count: usize,
}

#[derive(Debug)]
struct SessionCatalogContext {
    pending_approvals_by_session: HashMap<String, usize>,
    workspace_by_session: HashMap<String, SessionWorkspaceSummary>,
    family_by_session: HashMap<String, SessionCatalogFamilyRecord>,
    bindings_by_session: HashMap<String, SessionAgentBinding>,
    agents_by_id: HashMap<String, AgentRecord>,
    default_agent_id: Option<String>,
}

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
    let base_sessions = load_scoped_sessions(
        &state,
        session.context.principal.as_str(),
        session.context.device_id.as_str(),
        session.context.channel.as_deref(),
        true,
    )
    .await
    .map_err(runtime_status_response)?;
    let base =
        base_sessions.iter().find(|record| record.session_id == session_id).cloned().ok_or_else(
            || runtime_status_response(tonic::Status::not_found("session was not found")),
        )?;
    let catalog_context =
        load_session_catalog_context(&state, &session.context, &base_sessions).await?;
    let detail_context =
        load_session_detail_context(&state, &session.context, base.session_id.as_str()).await?;
    let record =
        build_session_catalog_record(&state, &catalog_context, base, Some(detail_context)).await?;
    Ok(Json(SessionCatalogDetailEnvelope { contract: contract_descriptor(), session: record }))
}

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
    Ok(Json(SessionCatalogRunAbortEnvelope {
        contract: contract_descriptor(),
        run_id,
        cancel_requested: response.cancel_requested,
        reason: response.reason,
    }))
}

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
    let (bindings_by_session, agents_by_id, default_agent_id) =
        load_session_agent_metadata(state, context).await?;
    let family_by_session = build_session_family_metadata(base_sessions);

    Ok(SessionCatalogContext {
        pending_approvals_by_session,
        workspace_by_session,
        family_by_session,
        bindings_by_session,
        agents_by_id,
        default_agent_id,
    })
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
    let mut active_context_files = HashMap::<String, HashSet<String>>::new();

    for document in documents {
        let Some(session_id) = document.latest_session_id.clone() else {
            continue;
        };
        touched_files.entry(session_id.clone()).or_default().insert(document.path.clone());
        if document.pinned || document.kind.eq_ignore_ascii_case("context") {
            active_context_files.entry(session_id).or_default().insert(document.path);
        }
    }

    let session_ids =
        touched_files.keys().chain(active_context_files.keys()).cloned().collect::<HashSet<_>>();

    Ok(session_ids
        .into_iter()
        .map(|session_id| {
            let touched = sorted_limited_paths(
                touched_files.remove(session_id.as_str()).unwrap_or_default(),
                SESSION_CATALOG_RECAP_ITEMS_LIMIT,
            );
            let mut active = sorted_limited_paths(
                active_context_files.remove(session_id.as_str()).unwrap_or_default(),
                SESSION_CATALOG_RECAP_ITEMS_LIMIT,
            );
            if active.is_empty() {
                active = touched.iter().take(SESSION_CATALOG_RECAP_ITEMS_LIMIT).cloned().collect();
            }
            (
                session_id,
                SessionWorkspaceSummary { touched_files: touched, active_context_files: active },
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

fn build_session_family_metadata(
    sessions: &[journal::OrchestratorSessionRecord],
) -> HashMap<String, SessionCatalogFamilyRecord> {
    let sessions_by_id = sessions
        .iter()
        .map(|session| (session.session_id.as_str(), session))
        .collect::<HashMap<_, _>>();
    let mut family_root_by_session = HashMap::<String, String>::new();
    for session in sessions {
        let _ = resolve_session_family_root(
            session.session_id.as_str(),
            &sessions_by_id,
            &mut family_root_by_session,
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
                    relation: if Some(entry.session_id.clone()) == session.parent_session_id {
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

fn resolve_session_family_root<'a>(
    session_id: &str,
    sessions_by_id: &HashMap<&'a str, &'a journal::OrchestratorSessionRecord>,
    memo: &mut HashMap<String, String>,
) -> Option<String> {
    if let Some(existing) = memo.get(session_id) {
        return Some(existing.clone());
    }
    let session = sessions_by_id.get(session_id).copied()?;
    let root = if let Some(parent_session_id) = session.parent_session_id.as_deref() {
        if parent_session_id == session.session_id {
            session.title.clone()
        } else {
            resolve_session_family_root(parent_session_id, sessions_by_id, memo)
                .unwrap_or_else(|| normalized_title_family_root(session.title.as_str()))
        }
    } else {
        normalized_title_family_root(session.title.as_str())
    };
    memo.insert(session.session_id.clone(), root.clone());
    Some(root)
}

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

    Ok(SessionDetailContext { recent_artifacts, artifact_count })
}

async fn build_session_catalog_record(
    state: &AppState,
    context: &SessionCatalogContext,
    session: journal::OrchestratorSessionRecord,
    detail_context: Option<SessionDetailContext>,
) -> Result<SessionCatalogRecord, Response> {
    let run_snapshot = if let Some(last_run_id) = session.last_run_id.as_ref() {
        state
            .runtime
            .orchestrator_run_status_snapshot(last_run_id.clone())
            .await
            .map_err(runtime_status_response)?
    } else {
        None
    };
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
    let quick_controls = build_session_quick_controls(context, session.session_id.as_str());
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
    let last_run_state =
        run_snapshot.as_ref().map(|run| run.state.clone()).or(session.last_run_state.clone());
    let recap = SessionCatalogRecapRecord {
        touched_files: workspace.touched_files.clone(),
        active_context_files: workspace.active_context_files.clone(),
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
        last_run_started_at_unix_ms: run_snapshot.as_ref().map(|run| run.started_at_unix_ms),
        prompt_tokens: run_snapshot.as_ref().map(|run| run.prompt_tokens).unwrap_or(0),
        completion_tokens: run_snapshot.as_ref().map(|run| run.completion_tokens).unwrap_or(0),
        total_tokens: run_snapshot.as_ref().map(|run| run.total_tokens).unwrap_or(0),
        archived: session.archived_at_unix_ms.is_some(),
        archived_at_unix_ms: session.archived_at_unix_ms,
        pending_approvals,
        has_context_files: !workspace.active_context_files.is_empty(),
        last_context_file: workspace
            .active_context_files
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

fn build_session_quick_controls(
    context: &SessionCatalogContext,
    session_id: &str,
) -> SessionCatalogQuickControlsRecord {
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
            override_active: inherited
                .map(|entry| entry.agent_id != binding.agent_id)
                .unwrap_or(true),
        },
        (Some(binding), None, inherited) => SessionCatalogQuickControlRecord {
            value: Some(binding.agent_id.clone()),
            display_value: binding.agent_id.clone(),
            source: "session_binding".to_owned(),
            inherited_value: inherited.map(|entry| entry.agent_id.clone()),
            override_active: inherited
                .map(|entry| entry.agent_id != binding.agent_id)
                .unwrap_or(true),
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

    let (model_value, model_display, model_source, inherited_model, model_override_active) =
        match (bound_agent, inherited_agent) {
            (Some(agent), inherited) => (
                Some(agent.default_model_profile.clone()),
                agent.default_model_profile.clone(),
                "agent_default_model_profile".to_owned(),
                inherited.map(|entry| entry.default_model_profile.clone()),
                inherited
                    .map(|entry| entry.default_model_profile != agent.default_model_profile)
                    .unwrap_or(true),
            ),
            (None, Some(agent)) => (
                Some(agent.default_model_profile.clone()),
                agent.default_model_profile.clone(),
                "default_agent_model_profile".to_owned(),
                Some(agent.default_model_profile.clone()),
                false,
            ),
            _ => (None, "Inherited default".to_owned(), "unassigned".to_owned(), None, false),
        };

    SessionCatalogQuickControlsRecord {
        agent,
        model: SessionCatalogQuickControlRecord {
            value: model_value,
            display_value: model_display,
            source: model_source,
            inherited_value: inherited_model,
            override_active: model_override_active,
        },
        thinking: SessionCatalogToggleControlRecord {
            value: true,
            source: "surface_default".to_owned(),
            inherited_value: true,
            override_active: false,
        },
        trace: SessionCatalogToggleControlRecord {
            value: false,
            source: "surface_default".to_owned(),
            inherited_value: false,
            override_active: false,
        },
        verbose: SessionCatalogToggleControlRecord {
            value: false,
            source: "surface_default".to_owned(),
            inherited_value: false,
            override_active: false,
        },
        reset_to_default_available: binding.is_some(),
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
    if ordering == Ordering::Equal {
        left.session_id.cmp(&right.session_id)
    } else {
        ordering
    }
}

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
