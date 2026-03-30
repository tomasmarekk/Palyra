use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::*;

const DEFAULT_SESSION_CATALOG_LIMIT: usize = 25;
const MAX_SESSION_CATALOG_LIMIT: usize = 100;
const SESSION_CATALOG_FETCH_PAGE: usize = 128;
const SESSION_CATALOG_TAPE_LIMIT: usize = 32;
const SESSION_CATALOG_APPROVAL_PAGE: usize = 256;
const SESSION_CATALOG_PREVIEW_LEN: usize = 180;
const SESSION_CATALOG_TITLE_LEN: usize = 72;

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
}

#[derive(Debug, Serialize)]
struct SessionCatalogSummary {
    active_sessions: usize,
    archived_sessions: usize,
    sessions_with_pending_approvals: usize,
    sessions_with_active_runs: usize,
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
}

#[derive(Debug, Clone, Serialize)]
struct SessionCatalogRecord {
    session_id: String,
    session_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_label: Option<String>,
    title: String,
    title_source: String,
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
}

#[derive(Debug, Clone, Default)]
struct SessionTapeSummary {
    first_user_message: Option<String>,
    latest_user_message: Option<String>,
    latest_assistant_message: Option<String>,
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
    let approvals = load_scoped_pending_approvals(&state, session.context.principal.as_str())
        .await
        .map_err(runtime_status_response)?;

    let mut catalog = Vec::with_capacity(base_sessions.len());
    for base in base_sessions {
        let pending_approvals = approvals
            .iter()
            .filter(|record| record.session_id == base.session_id && record.decision.is_none())
            .count();
        catalog.push(build_session_catalog_record(&state, base, pending_approvals).await?);
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
    };

    if let Some(archived_filter) = query.archived {
        catalog.retain(|record| record.archived == archived_filter);
    } else if !include_archived {
        catalog.retain(|record| !record.archived);
    }

    if let Some(expected_title_source) = title_source.as_deref() {
        catalog.retain(|record| record.title_source == expected_title_source);
    }
    if let Some(has_pending_approvals) = query.has_pending_approvals {
        catalog.retain(|record| (record.pending_approvals > 0) == has_pending_approvals);
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
    let base = load_scoped_session_by_id(
        &state,
        session.context.principal.as_str(),
        session.context.device_id.as_str(),
        session.context.channel.as_deref(),
        session_id.as_str(),
    )
    .await
    .map_err(runtime_status_response)?
    .ok_or_else(|| runtime_status_response(tonic::Status::not_found("session was not found")))?;
    let approvals = load_scoped_pending_approvals(&state, session.context.principal.as_str())
        .await
        .map_err(runtime_status_response)?;
    let pending_approvals = approvals
        .iter()
        .filter(|record| record.session_id == base.session_id && record.decision.is_none())
        .count();
    let record = build_session_catalog_record(&state, base, pending_approvals).await?;
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
    let approvals = load_scoped_pending_approvals(&state, session.context.principal.as_str())
        .await
        .map_err(runtime_status_response)?;
    let pending_approvals = approvals
        .iter()
        .filter(|record| {
            record.session_id == outcome.session.session_id && record.decision.is_none()
        })
        .count();
    let record = build_session_catalog_record(&state, outcome.session, pending_approvals).await?;
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
    let _session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
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
            .list_orchestrator_sessions(
                cursor.clone(),
                principal.to_owned(),
                device_id.to_owned(),
                channel.map(str::to_owned),
                include_archived,
                Some(SESSION_CATALOG_FETCH_PAGE),
            )
            .await?;
        sessions.append(&mut page);
        let Some(next_after) = next_after else {
            break;
        };
        cursor = Some(next_after);
    }

    Ok(sessions)
}

async fn load_scoped_session_by_id(
    state: &AppState,
    principal: &str,
    device_id: &str,
    channel: Option<&str>,
    session_id: &str,
) -> Result<Option<journal::OrchestratorSessionRecord>, tonic::Status> {
    let sessions = load_scoped_sessions(state, principal, device_id, channel, true).await?;
    Ok(sessions.into_iter().find(|record| record.session_id == session_id))
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

async fn build_session_catalog_record(
    state: &AppState,
    session: journal::OrchestratorSessionRecord,
    pending_approvals: usize,
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
    let tape_summary = if let Some(last_run_id) = session.last_run_id.as_ref() {
        match state
            .runtime
            .orchestrator_tape_snapshot(last_run_id.clone(), None, Some(SESSION_CATALOG_TAPE_LIMIT))
            .await
        {
            Ok(snapshot) => summarize_session_tape(snapshot.events.as_slice()),
            Err(status) if status.code() == tonic::Code::NotFound => SessionTapeSummary::default(),
            Err(status) => return Err(runtime_status_response(status)),
        }
    } else {
        SessionTapeSummary::default()
    };

    let last_intent = tape_summary.latest_user_message.clone();
    let last_summary = tape_summary.latest_assistant_message.clone();
    let preview = last_summary
        .clone()
        .or_else(|| last_intent.clone())
        .or_else(|| tape_summary.first_user_message.clone());
    let (title, title_source) = if let Some(label) = session
        .session_label
        .as_deref()
        .and_then(|value| normalize_catalog_text(value, SESSION_CATALOG_TITLE_LEN))
    {
        (label, "label".to_owned())
    } else if let Some(derived) = tape_summary
        .first_user_message
        .as_deref()
        .and_then(|value| normalize_catalog_text(value, SESSION_CATALOG_TITLE_LEN))
    {
        (derived, "derived_intent".to_owned())
    } else {
        (
            normalize_catalog_text(session.session_key.as_str(), SESSION_CATALOG_TITLE_LEN)
                .unwrap_or_else(|| session.session_id.clone()),
            "session_key".to_owned(),
        )
    };

    Ok(SessionCatalogRecord {
        session_id: session.session_id,
        session_key: session.session_key,
        session_label: session.session_label,
        title,
        title_source,
        preview: preview
            .as_deref()
            .and_then(|value| normalize_catalog_text(value, SESSION_CATALOG_PREVIEW_LEN)),
        preview_state: preview_metadata_state(preview.as_deref()).to_owned(),
        last_intent: last_intent
            .as_deref()
            .and_then(|value| normalize_catalog_text(value, SESSION_CATALOG_PREVIEW_LEN)),
        last_intent_state: preview_metadata_state(last_intent.as_deref()).to_owned(),
        last_summary: last_summary
            .as_deref()
            .and_then(|value| normalize_catalog_text(value, SESSION_CATALOG_PREVIEW_LEN)),
        last_summary_state: preview_metadata_state(last_summary.as_deref()).to_owned(),
        branch_state: "missing".to_owned(),
        parent_session_id: None,
        principal: session.principal,
        device_id: session.device_id,
        channel: session.channel,
        created_at_unix_ms: session.created_at_unix_ms,
        updated_at_unix_ms: session.updated_at_unix_ms,
        last_run_id: session.last_run_id.clone(),
        last_run_state: run_snapshot.as_ref().map(|run| run.state.clone()),
        last_run_started_at_unix_ms: run_snapshot.as_ref().map(|run| run.started_at_unix_ms),
        prompt_tokens: run_snapshot.as_ref().map(|run| run.prompt_tokens).unwrap_or(0),
        completion_tokens: run_snapshot.as_ref().map(|run| run.completion_tokens).unwrap_or(0),
        total_tokens: run_snapshot.as_ref().map(|run| run.total_tokens).unwrap_or(0),
        archived: session.archived_at_unix_ms.is_some(),
        archived_at_unix_ms: session.archived_at_unix_ms,
        pending_approvals,
    })
}

fn summarize_session_tape(events: &[journal::OrchestratorTapeRecord]) -> SessionTapeSummary {
    let mut summary = SessionTapeSummary::default();

    for event in events {
        let payload = match serde_json::from_str::<Value>(event.payload_json.as_str()) {
            Ok(value) => value,
            Err(_) => continue,
        };
        match event.event_type.as_str() {
            "message.received" => {
                let Some(text) = payload
                    .get("text")
                    .and_then(Value::as_str)
                    .and_then(|value| normalize_catalog_text(value, SESSION_CATALOG_PREVIEW_LEN))
                else {
                    continue;
                };
                if summary.first_user_message.is_none() {
                    summary.first_user_message = Some(text.clone());
                }
                summary.latest_user_message = Some(text);
            }
            "message.replied" => {
                let Some(text) = payload
                    .get("reply_text")
                    .and_then(Value::as_str)
                    .and_then(|value| normalize_catalog_text(value, SESSION_CATALOG_PREVIEW_LEN))
                else {
                    continue;
                };
                summary.latest_assistant_message = Some(text);
            }
            _ => {}
        }
    }

    summary
}

fn normalize_catalog_text(raw: &str, max_chars: usize) -> Option<String> {
    let normalized = raw.replace(['\r', '\n'], " ");
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
    match raw.map(str::trim).filter(|value| !value.is_empty()) {
        Some("label") => Some("label".to_owned()),
        Some("derived_intent") => Some("derived_intent".to_owned()),
        Some("session_key") => Some("session_key".to_owned()),
        _ => None,
    }
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
        record.preview.as_deref(),
        record.last_intent.as_deref(),
        record.last_summary.as_deref(),
        record.last_run_state.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(|value| value.to_ascii_lowercase().contains(search))
}

fn preview_metadata_state(value: Option<&str>) -> &'static str {
    if value.is_some() {
        "computed"
    } else {
        "missing"
    }
}
