//! Mobile companion console endpoints.
//!
//! The mobile surface is a constrained view over approvals, recent sessions,
//! safe URL handoff, and voice-note capture. It reuses console session auth
//! while returning mobile-specific envelopes for cacheable companion clients.

use std::collections::{BTreeMap, HashMap};

use palyra_common::runtime_contracts::{AuxiliaryTaskKind, AuxiliaryTaskState};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;

use super::{
    auth::create_console_browser_handoff,
    diagnostics::{authorize_console_session, build_page_info, contract_descriptor},
};
use crate::*;

const MOBILE_APPROVAL_LIMIT_DEFAULT: usize = 20;
const MOBILE_APPROVAL_LIMIT_MAX: usize = 50;
const MOBILE_SESSION_LIMIT_DEFAULT: usize = 10;
const MOBILE_SESSION_LIMIT_MAX: usize = 25;
const MOBILE_INBOX_ALERT_LIMIT: usize = 24;
const MOBILE_NOTIFICATION_POLL_INTERVAL_MS: u64 = 45_000;
const MOBILE_VOICE_NOTE_MAX_TEXT_BYTES: usize = 8 * 1024;
const MOBILE_VOICE_NOTE_MAX_DURATION_MS: u64 = 2 * 60 * 1000;

/// Query parameters for mobile approval list pagination.
#[derive(Debug, Deserialize)]
pub(crate) struct MobileApprovalsQuery {
    #[serde(default)]
    after_approval_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    include_resolved: Option<bool>,
}

/// Query parameters for mobile session list pagination and search.
#[derive(Debug, Deserialize)]
pub(crate) struct MobileSessionsQuery {
    #[serde(default)]
    after_session_key: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    include_archived: Option<bool>,
    #[serde(default)]
    q: Option<String>,
}

/// Request body for creating a mobile voice-note task.
#[derive(Debug, Deserialize)]
pub(crate) struct MobileVoiceNoteRequest {
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    create_session_label: Option<String>,
    transcript_text: String,
    transcript_reviewed: bool,
    #[serde(default)]
    duration_ms: Option<u64>,
    #[serde(default)]
    draft_id: Option<String>,
    #[serde(default)]
    notification_target: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
struct MobileSessionView {
    session: journal::OrchestratorSessionRecord,
    recap: control_plane::MobileSessionRecap,
    handoff: control_plane::MobileHandoffTarget,
}

/// Returns mobile capability, pairing, notification, and cache policy.
///
/// # Errors
/// Returns an error response when console session authorization fails.
pub(crate) async fn console_mobile_bootstrap_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<control_plane::MobileBootstrapEnvelope>, Response> {
    authorize_console_session(&state, &headers, false)?;
    Ok(Json(control_plane::MobileBootstrapEnvelope {
        contract: contract_descriptor(),
        release_scope: control_plane::MobileReleaseScope {
            approvals_inbox: true,
            polling_notifications: true,
            recent_sessions: true,
            safe_url_open: true,
            voice_note: true,
        },
        notifications: control_plane::MobileNotificationPolicy {
            delivery_mode: "polling".to_owned(),
            quiet_hours_supported: true,
            grouping_supported: true,
            priority_supported: true,
            default_poll_interval_ms: MOBILE_NOTIFICATION_POLL_INTERVAL_MS,
            max_alerts_per_poll: MOBILE_INBOX_ALERT_LIMIT,
        },
        pairing: control_plane::MobilePairingPolicy {
            auth_flow:
                "admin token login followed by approval-bound mobile enrollment and cookie session bootstrap"
                    .to_owned(),
            trust_model: "reuse existing node trust model, local mediation semantics, and explicit revoke/recovery flows"
                .to_owned(),
            revoke_supported: true,
            recovery_supported: true,
            offline_state_visible: true,
        },
        handoff: control_plane::MobileHandoffPolicy {
            contract: "cross_surface_handoff.v1".to_owned(),
            safe_url_open_requires_mediation: true,
            heavy_surface_handoff_supported: true,
            browser_automation_exposed: false,
        },
        store: control_plane::MobileLocalStoreContract {
            approvals_cache_key: "mobile.approvals.cache".to_owned(),
            sessions_cache_key: "mobile.sessions.cache".to_owned(),
            inbox_cache_key: "mobile.inbox.cache".to_owned(),
            outbox_queue_key: "mobile.voice-note.outbox".to_owned(),
            revoke_marker_key: "mobile.auth.revoked".to_owned(),
        },
        rollout: control_plane::MobileRolloutStatus {
            mobile_companion_enabled: true,
            approvals_enabled: true,
            notifications_enabled: true,
            recent_sessions_enabled: true,
            safe_url_open_enabled: true,
            voice_notes_enabled: true,
        },
        locales: vec!["en".to_owned(), "qps-ploc".to_owned()],
        default_locale: "en".to_owned(),
    }))
}

/// Returns the mobile inbox summary with approval and task alerts.
///
/// # Errors
/// Returns an error response when session authorization, approval lookup,
/// background-task lookup, or session lookup fails.
pub(crate) async fn console_mobile_inbox_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<control_plane::MobileInboxEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let approvals = load_mobile_approvals(
        &state,
        session.context.principal.as_str(),
        None,
        MOBILE_INBOX_ALERT_LIMIT,
        false,
    )
    .await?;
    let tasks = state
        .runtime
        .list_orchestrator_background_tasks(journal::OrchestratorBackgroundTaskListFilter {
            owner_principal: Some(session.context.principal.clone()),
            device_id: None,
            channel: None,
            session_id: None,
            include_completed: true,
            limit: MOBILE_INBOX_ALERT_LIMIT,
        })
        .await
        .map_err(runtime_status_response)?;
    let session_lookup =
        load_mobile_session_lookup(&state, session.context.principal.as_str()).await?;

    let mut alerts = build_mobile_approval_alerts(approvals.as_slice());
    alerts.extend(build_mobile_task_alerts(tasks.as_slice(), &session_lookup));
    alerts.sort_by(|left, right| {
        inbox_priority_weight(&left.priority)
            .cmp(&inbox_priority_weight(&right.priority))
            .then_with(|| right.created_at_unix_ms.cmp(&left.created_at_unix_ms))
    });
    alerts.truncate(MOBILE_INBOX_ALERT_LIMIT);

    let failed_tasks = tasks
        .iter()
        .filter(|task| {
            matches!(
                AuxiliaryTaskState::from_str(task.state.as_str()),
                Some(AuxiliaryTaskState::Failed | AuxiliaryTaskState::Expired)
            )
        })
        .count();
    let completed_tasks = tasks
        .iter()
        .filter(|task| {
            matches!(
                AuxiliaryTaskState::from_str(task.state.as_str()),
                Some(AuxiliaryTaskState::Succeeded | AuxiliaryTaskState::Cancelled)
            )
        })
        .count();
    let active_tasks = tasks
        .iter()
        .filter(|task| {
            matches!(
                AuxiliaryTaskState::from_str(task.state.as_str()),
                Some(
                    AuxiliaryTaskState::Queued
                        | AuxiliaryTaskState::Running
                        | AuxiliaryTaskState::Paused
                )
            )
        })
        .count();

    Ok(Json(control_plane::MobileInboxEnvelope {
        contract: contract_descriptor(),
        delivery_mode: "polling".to_owned(),
        quiet_hours_respected: true,
        summary: control_plane::MobileInboxSummary {
            pending_approvals: approvals.len(),
            active_tasks,
            completed_tasks,
            failed_tasks,
        },
        alerts,
    }))
}

/// Lists approvals for the mobile companion.
///
/// # Errors
/// Returns an error response when session authorization or approval lookup
/// fails.
pub(crate) async fn console_mobile_approvals_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<MobileApprovalsQuery>,
) -> Result<Json<control_plane::MobileApprovalsEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let limit =
        query.limit.unwrap_or(MOBILE_APPROVAL_LIMIT_DEFAULT).clamp(1, MOBILE_APPROVAL_LIMIT_MAX);
    let approvals = load_mobile_approvals(
        &state,
        session.context.principal.as_str(),
        query.after_approval_id.clone(),
        limit,
        query.include_resolved.unwrap_or(false),
    )
    .await?;
    let handoff_recommended =
        approvals.iter().filter(|approval| approval_needs_handoff(approval)).count();
    Ok(Json(control_plane::MobileApprovalsEnvelope {
        contract: contract_descriptor(),
        approvals: approvals.iter().map(serialize_value).collect::<Result<Vec<_>, _>>()?,
        summary: control_plane::MobileApprovalInboxSummary {
            pending: approvals.len(),
            ready_on_device: approvals.len().saturating_sub(handoff_recommended),
            handoff_recommended,
        },
        page: build_page_info(
            limit,
            approvals.len(),
            approvals.last().map(|value| value.approval_id.clone()),
        ),
    }))
}

/// Returns one approval record for the mobile companion.
///
/// # Errors
/// Returns an error response when session authorization or approval lookup
/// fails.
pub(crate) async fn console_mobile_approval_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(approval_id): Path<String>,
) -> Result<Json<control_plane::MobileApprovalDetailEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let approval =
        load_mobile_approval(&state, session.context.principal.as_str(), approval_id.as_str())
            .await?;
    Ok(Json(control_plane::MobileApprovalDetailEnvelope {
        contract: contract_descriptor(),
        approval: serialize_value(&approval)?,
        explainability: approval_explainability(&approval),
    }))
}

/// Records an approval decision from the mobile companion.
///
/// # Errors
/// Returns an error response when session authorization, approval lookup,
/// decision validation, or approval mutation fails.
pub(crate) async fn console_mobile_approval_decision_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(approval_id): Path<String>,
    Json(payload): Json<control_plane::ApprovalDecisionRequest>,
) -> Result<Json<control_plane::ApprovalDecisionEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let _approval =
        load_mobile_approval(&state, session.context.principal.as_str(), approval_id.as_str())
            .await?;
    let decision_scope = parse_mobile_decision_scope(payload.decision_scope.as_deref())?;
    let resolved = state
        .runtime
        .resolve_approval_record(journal::ApprovalResolveRequest {
            approval_id: approval_id.clone(),
            decision: if payload.approved {
                journal::ApprovalDecision::Allow
            } else {
                journal::ApprovalDecision::Deny
            },
            decision_scope,
            decision_reason: payload.reason.clone().unwrap_or_else(|| {
                if payload.approved {
                    "approved_by_mobile_companion".to_owned()
                } else {
                    "denied_by_mobile_companion".to_owned()
                }
            }),
            decision_scope_ttl_ms: payload.decision_scope_ttl_ms,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(control_plane::ApprovalDecisionEnvelope {
        approval: serialize_value(&resolved)?,
        dm_pairing: None,
    }))
}

/// Lists recent sessions for the mobile companion.
///
/// # Errors
/// Returns an error response when session authorization or session listing
/// fails.
pub(crate) async fn console_mobile_sessions_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<MobileSessionsQuery>,
) -> Result<Json<control_plane::MobileSessionsEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let limit =
        query.limit.unwrap_or(MOBILE_SESSION_LIMIT_DEFAULT).clamp(1, MOBILE_SESSION_LIMIT_MAX);
    let (sessions, next_cursor) = state
        .runtime
        .list_orchestrator_sessions_for_principal(
            gateway::ListPrincipalOrchestratorSessionsRequest {
                after_session_key: query.after_session_key.clone(),
                principal: session.context.principal.clone(),
                include_archived: query.include_archived.unwrap_or(false),
                requested_limit: Some(limit),
                search_query: query.q.clone(),
            },
        )
        .await
        .map_err(runtime_status_response)?;
    let pending_by_session =
        load_pending_approval_counts(&state, session.context.principal.as_str()).await?;
    let items = sessions
        .iter()
        .map(|record| {
            build_mobile_session_view(
                record,
                *pending_by_session.get(record.session_id.as_str()).unwrap_or(&0),
            )
        })
        .collect::<Vec<_>>();
    let mut session_summaries = Vec::with_capacity(items.len());
    for item in items {
        session_summaries.push(control_plane::MobileSessionSummary {
            session: serialize_value(&item.session)?,
            recap: item.recap,
            handoff: item.handoff,
        });
    }
    Ok(Json(control_plane::MobileSessionsEnvelope {
        contract: contract_descriptor(),
        sessions: session_summaries,
        page: build_page_info(limit, sessions.len(), next_cursor),
    }))
}

/// Returns one recent session view for the mobile companion.
///
/// # Errors
/// Returns an error response when session authorization, id validation, session
/// lookup, or transcript loading fails.
pub(crate) async fn console_mobile_session_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<control_plane::MobileSessionDetailEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let record =
        load_mobile_session(&state, session.context.principal.as_str(), session_id.as_str())
            .await?;
    let pending_by_session =
        load_pending_approval_counts(&state, session.context.principal.as_str()).await?;
    let view = build_mobile_session_view(
        &record,
        *pending_by_session.get(record.session_id.as_str()).unwrap_or(&0),
    );
    Ok(Json(control_plane::MobileSessionDetailEnvelope {
        contract: contract_descriptor(),
        session: serialize_value(&view.session)?,
        recap: view.recap,
        actions: vec![
            "resume_session".to_owned(),
            "handoff_web".to_owned(),
            "open_approvals".to_owned(),
        ],
    }))
}

/// Creates a mediated safe-URL handoff for mobile clients.
///
/// # Errors
/// Returns an error response when session authorization, URL validation, or
/// browser handoff creation fails.
pub(crate) async fn console_mobile_safe_url_open_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::MobileSafeUrlOpenRequest>,
) -> Result<Json<control_plane::MobileSafeUrlOpenEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let target = payload.target.trim();
    if target.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "target cannot be empty",
        )));
    }
    if target.starts_with('/') {
        let handoff = create_console_browser_handoff(&state, &session.context, Some(target))?;
        return Ok(Json(control_plane::MobileSafeUrlOpenEnvelope {
            contract: contract_descriptor(),
            action: "handoff_console".to_owned(),
            target: target.to_owned(),
            normalized_url: None,
            handoff_url: Some(handoff.handoff_url),
            reason: None,
        }));
    }

    let url = Url::parse(target).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "mobile safe URL open only accepts relative console paths or absolute HTTPS URLs",
        ))
    })?;
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        return Ok(Json(control_plane::MobileSafeUrlOpenEnvelope {
            contract: contract_descriptor(),
            action: "blocked".to_owned(),
            target: target.to_owned(),
            normalized_url: None,
            handoff_url: None,
            reason: Some("mobile safe URL open only allows clean HTTPS targets".to_owned()),
        }));
    }
    if url.fragment().is_some() {
        return Ok(Json(control_plane::MobileSafeUrlOpenEnvelope {
            contract: contract_descriptor(),
            action: "blocked".to_owned(),
            target: target.to_owned(),
            normalized_url: None,
            handoff_url: None,
            reason: Some(
                "URLs with fragments are blocked to avoid hidden mobile actions".to_owned(),
            ),
        }));
    }
    Ok(Json(control_plane::MobileSafeUrlOpenEnvelope {
        contract: contract_descriptor(),
        action: "open_external".to_owned(),
        target: target.to_owned(),
        normalized_url: Some(url.to_string()),
        handoff_url: None,
        reason: None,
    }))
}

/// Creates a voice-note background task from reviewed transcript text.
///
/// # Errors
/// Returns an error response when session authorization, transcript validation,
/// target session resolution, task creation, or event recording fails.
pub(crate) async fn console_mobile_voice_note_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<MobileVoiceNoteRequest>,
) -> Result<Json<control_plane::MobileVoiceNoteEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let transcript_text = trim_to_option(payload.transcript_text.clone()).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("transcript_text cannot be empty"))
    })?;
    if !payload.transcript_reviewed {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "mobile voice notes require transcript review before send",
        )));
    }
    if transcript_text.len() > MOBILE_VOICE_NOTE_MAX_TEXT_BYTES {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "transcript_text exceeds the mobile voice note size limit",
        )));
    }
    if payload.duration_ms.unwrap_or_default() > MOBILE_VOICE_NOTE_MAX_DURATION_MS {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "mobile voice note exceeds the maximum supported duration",
        )));
    }

    let mut queued_for_existing_session = false;
    let target_session = if let Some(session_id) =
        payload.session_id.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        queued_for_existing_session = true;
        load_mobile_execution_session(&state, &session.context, session_id).await?
    } else {
        state
            .runtime
            .resolve_orchestrator_session(journal::OrchestratorSessionResolveRequest {
                session_id: None,
                session_key: None,
                session_label: payload
                    .create_session_label
                    .clone()
                    .and_then(trim_to_option)
                    .or_else(|| Some("Mobile voice note".to_owned())),
                principal: session.context.principal.clone(),
                device_id: session.context.device_id.clone(),
                channel: session.context.channel.clone(),
                require_existing: false,
                reset_session: false,
            })
            .await
            .map_err(runtime_status_response)?
            .session
    };

    let task = state
        .runtime
        .create_orchestrator_background_task(journal::OrchestratorBackgroundTaskCreateRequest {
            task_id: Ulid::new().to_string(),
            task_kind: AuxiliaryTaskKind::BackgroundPrompt.as_str().to_owned(),
            session_id: target_session.session_id.clone(),
            parent_run_id: target_session.last_run_id.clone(),
            target_run_id: None,
            queued_input_id: None,
            owner_principal: session.context.principal.clone(),
            device_id: session.context.device_id.clone(),
            channel: session.context.channel.clone(),
            state: AuxiliaryTaskState::Queued.as_str().to_owned(),
            priority: 1,
            max_attempts: 3,
            budget_tokens: crate::orchestrator::estimate_token_count(transcript_text.as_str()),
            delegation: None,
            not_before_unix_ms: None,
            expires_at_unix_ms: None,
            notification_target_json: payload
                .notification_target
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| {
                    runtime_status_response(tonic::Status::internal(format!(
                        "failed to encode voice note notification target: {error}"
                    )))
                })?,
            input_text: Some(transcript_text),
            payload_json: Some(
                serde_json::json!({
                    "entry_point": "mobile_voice_note",
                    "source_surface": "mobile",
                    "transcript_reviewed": true,
                    "duration_ms": payload.duration_ms,
                    "draft_id": payload.draft_id,
                    "created_by_mobile_device_id": session.context.device_id,
                })
                .to_string(),
            ),
        })
        .await
        .map_err(runtime_status_response)?;

    Ok(Json(control_plane::MobileVoiceNoteEnvelope {
        contract: contract_descriptor(),
        session: serialize_value(&target_session)?,
        task: serialize_value(&task)?,
        queued_for_existing_session,
    }))
}

async fn load_mobile_approvals(
    state: &AppState,
    principal: &str,
    after_approval_id: Option<String>,
    limit: usize,
    include_resolved: bool,
) -> Result<Vec<journal::ApprovalRecord>, Response> {
    let mut collected = Vec::new();
    let mut cursor = after_approval_id;
    let page_size = limit.max(16).clamp(16, MOBILE_APPROVAL_LIMIT_MAX);
    while collected.len() < limit {
        let (page, next_after) = state
            .runtime
            .list_approval_records(
                cursor.clone(),
                Some(page_size),
                None,
                None,
                None,
                Some(principal.to_owned()),
                None,
                None,
            )
            .await
            .map_err(runtime_status_response)?;
        if page.is_empty() {
            break;
        }
        for approval in page {
            if include_resolved || approval.decision.is_none() {
                collected.push(approval);
                if collected.len() == limit {
                    break;
                }
            }
        }
        let Some(next_after) = next_after else {
            break;
        };
        cursor = Some(next_after);
    }
    Ok(collected)
}

async fn load_mobile_approval(
    state: &AppState,
    principal: &str,
    approval_id: &str,
) -> Result<journal::ApprovalRecord, Response> {
    validate_canonical_id(approval_id).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "approval_id must be a canonical ULID",
        ))
    })?;
    let approval = state
        .runtime
        .approval_record(approval_id.to_owned())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found("approval record not found"))
        })?;
    if approval.principal != principal {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "approval does not belong to the authenticated mobile operator",
        )));
    }
    Ok(approval)
}

async fn load_pending_approval_counts(
    state: &AppState,
    principal: &str,
) -> Result<HashMap<String, usize>, Response> {
    let approvals = load_mobile_approvals(state, principal, None, 256, false).await?;
    let mut counts = HashMap::<String, usize>::new();
    for approval in approvals {
        *counts.entry(approval.session_id).or_insert(0) += 1;
    }
    Ok(counts)
}

async fn load_mobile_session_lookup(
    state: &AppState,
    principal: &str,
) -> Result<HashMap<String, journal::OrchestratorSessionRecord>, Response> {
    let mut records = HashMap::new();
    let mut cursor = None::<String>;
    loop {
        let (page, next_cursor) = state
            .runtime
            .list_orchestrator_sessions_for_principal(
                gateway::ListPrincipalOrchestratorSessionsRequest {
                    after_session_key: cursor.clone(),
                    principal: principal.to_owned(),
                    include_archived: true,
                    requested_limit: Some(MOBILE_SESSION_LIMIT_MAX),
                    search_query: None,
                },
            )
            .await
            .map_err(runtime_status_response)?;
        if page.is_empty() {
            break;
        }
        for record in page {
            records.insert(record.session_id.clone(), record);
        }
        let Some(next_cursor) = next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    Ok(records)
}

async fn load_mobile_session(
    state: &AppState,
    principal: &str,
    session_id: &str,
) -> Result<journal::OrchestratorSessionRecord, Response> {
    validate_canonical_id(session_id).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let session = state
        .runtime
        .orchestrator_session_by_id(session_id.to_owned())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found("session was not found"))
        })?;
    if session.principal != principal {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "session does not belong to the authenticated mobile operator",
        )));
    }
    Ok(session)
}

async fn load_mobile_execution_session(
    state: &AppState,
    context: &gateway::RequestContext,
    session_id: &str,
) -> Result<journal::OrchestratorSessionRecord, Response> {
    validate_canonical_id(session_id).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    Ok(state
        .runtime
        .resolve_orchestrator_session(journal::OrchestratorSessionResolveRequest {
            session_id: Some(session_id.to_owned()),
            session_key: None,
            session_label: None,
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
            require_existing: true,
            reset_session: false,
        })
        .await
        .map_err(runtime_status_response)?
        .session)
}

fn build_mobile_session_view(
    session: &journal::OrchestratorSessionRecord,
    pending_approvals: usize,
) -> MobileSessionView {
    let handoff_path = build_handoff_path(
        "/chat",
        &[
            ("sessionId", Some(session.session_id.as_str())),
            ("source", Some("mobile")),
            ("intent", Some("resume-session")),
        ],
    );
    MobileSessionView {
        session: session.clone(),
        recap: control_plane::MobileSessionRecap {
            title: session.title.clone(),
            preview: session.preview.clone(),
            last_summary: session.last_summary.clone(),
            last_intent: session.last_intent.clone(),
            last_run_state: session.last_run_state.clone(),
            pending_approvals,
            handoff_recommended: pending_approvals > 0
                || matches!(session.last_run_state.as_deref(), Some("accepted" | "in_progress")),
        },
        handoff: control_plane::MobileHandoffTarget {
            path: handoff_path,
            intent: Some("resume-session".to_owned()),
            requires_full_console: pending_approvals > 0,
        },
    }
}

fn build_mobile_approval_alerts(
    approvals: &[journal::ApprovalRecord],
) -> Vec<control_plane::MobileInboxItem> {
    let mut grouped = BTreeMap::<String, Vec<&journal::ApprovalRecord>>::new();
    for approval in approvals.iter().filter(|approval| approval.decision.is_none()) {
        grouped.entry(approval.session_id.clone()).or_default().push(approval);
    }
    grouped
        .into_iter()
        .map(|(session_id, approvals)| {
            let first = approvals[0];
            let max_priority = approvals
                .iter()
                .map(|approval| risk_to_inbox_priority(approval.prompt.risk_level))
                .min_by_key(inbox_priority_weight)
                .unwrap_or(control_plane::MobileInboxPriority::Medium);
            control_plane::MobileInboxItem {
                alert_id: format!("approval-group:{session_id}"),
                kind: control_plane::MobileInboxItemKind::Approval,
                priority: max_priority,
                group_key: format!("approval:{session_id}"),
                title: if approvals.len() == 1 {
                    first.prompt.title.clone()
                } else {
                    format!("{} approvals are waiting", approvals.len())
                },
                body: first.prompt.summary.clone(),
                session_id: Some(session_id.clone()),
                run_id: Some(first.run_id.clone()),
                approval_id: Some(first.approval_id.clone()),
                task_id: None,
                created_at_unix_ms: approvals
                    .iter()
                    .map(|approval| approval.requested_at_unix_ms)
                    .max()
                    .unwrap_or(first.requested_at_unix_ms),
                handoff: Some(control_plane::MobileHandoffTarget {
                    path: build_handoff_path(
                        "/control/approvals",
                        &[
                            ("sessionId", Some(session_id.as_str())),
                            ("source", Some("mobile")),
                            ("intent", Some("approve")),
                        ],
                    ),
                    intent: Some("approve".to_owned()),
                    requires_full_console: approvals
                        .iter()
                        .any(|approval| approval_needs_handoff(approval)),
                }),
            }
        })
        .collect()
}

fn build_mobile_task_alerts(
    tasks: &[journal::OrchestratorBackgroundTaskRecord],
    sessions: &HashMap<String, journal::OrchestratorSessionRecord>,
) -> Vec<control_plane::MobileInboxItem> {
    tasks
        .iter()
        .filter_map(|task| {
            let (kind, priority, title, body) =
                match AuxiliaryTaskState::from_str(task.state.as_str()) {
                    Some(AuxiliaryTaskState::Failed) => (
                        control_plane::MobileInboxItemKind::Support,
                        control_plane::MobileInboxPriority::High,
                        "Background task failed".to_owned(),
                        task.last_error.clone().unwrap_or_else(|| {
                            "Inspect the session in the full console.".to_owned()
                        }),
                    ),
                    Some(AuxiliaryTaskState::Expired) => (
                        control_plane::MobileInboxItemKind::Support,
                        control_plane::MobileInboxPriority::High,
                        "Background task expired".to_owned(),
                        "The queued task expired before it could run.".to_owned(),
                    ),
                    Some(AuxiliaryTaskState::Queued) => (
                        control_plane::MobileInboxItemKind::RunUpdate,
                        control_plane::MobileInboxPriority::Medium,
                        "Background task queued".to_owned(),
                        task.input_text
                            .clone()
                            .unwrap_or_else(|| "Queued from another surface.".to_owned()),
                    ),
                    Some(AuxiliaryTaskState::Running) => (
                        control_plane::MobileInboxItemKind::RunUpdate,
                        control_plane::MobileInboxPriority::Medium,
                        "Background task running".to_owned(),
                        "Work is still in progress.".to_owned(),
                    ),
                    Some(AuxiliaryTaskState::Succeeded) => (
                        control_plane::MobileInboxItemKind::RunUpdate,
                        control_plane::MobileInboxPriority::Low,
                        "Background task completed".to_owned(),
                        "The task finished successfully.".to_owned(),
                    ),
                    _ => return None,
                };
            let handoff = sessions.get(task.session_id.as_str()).map(|session| {
                control_plane::MobileHandoffTarget {
                    path: build_handoff_path(
                        "/chat",
                        &[
                            ("sessionId", Some(session.session_id.as_str())),
                            ("source", Some("mobile")),
                            ("intent", Some("resume-session")),
                        ],
                    ),
                    intent: Some("resume-session".to_owned()),
                    requires_full_console: matches!(
                        AuxiliaryTaskState::from_str(task.state.as_str()),
                        Some(AuxiliaryTaskState::Failed | AuxiliaryTaskState::Expired)
                    ),
                }
            });
            Some(control_plane::MobileInboxItem {
                alert_id: format!("task:{}", task.task_id),
                kind,
                priority,
                group_key: format!("task-state:{}", task.state),
                title,
                body,
                session_id: Some(task.session_id.clone()),
                run_id: task.target_run_id.clone().or(task.parent_run_id.clone()),
                approval_id: None,
                task_id: Some(task.task_id.clone()),
                created_at_unix_ms: task.updated_at_unix_ms,
                handoff,
            })
        })
        .collect()
}

fn approval_explainability(
    approval: &journal::ApprovalRecord,
) -> control_plane::MobileApprovalExplainability {
    control_plane::MobileApprovalExplainability {
        evaluation_summary: approval.policy_snapshot.evaluation_summary.clone(),
        policy_explanation: approval.prompt.policy_explanation.clone(),
        recommended_surface: if approval_needs_handoff(approval) {
            "handoff".to_owned()
        } else {
            "mobile".to_owned()
        },
        web_handoff_path: Some(build_handoff_path(
            "/control/approvals",
            &[
                ("sessionId", Some(approval.session_id.as_str())),
                ("source", Some("mobile")),
                ("intent", Some("approve")),
            ],
        )),
    }
}

fn approval_needs_handoff(approval: &journal::ApprovalRecord) -> bool {
    matches!(
        approval.subject_type,
        journal::ApprovalSubjectType::DevicePairing | journal::ApprovalSubjectType::BrowserAction
    ) || matches!(approval.prompt.risk_level, journal::ApprovalRiskLevel::Critical)
        || approval.prompt.options.len() > 2
}

fn risk_to_inbox_priority(risk: journal::ApprovalRiskLevel) -> control_plane::MobileInboxPriority {
    match risk {
        journal::ApprovalRiskLevel::Critical => control_plane::MobileInboxPriority::Critical,
        journal::ApprovalRiskLevel::High => control_plane::MobileInboxPriority::High,
        journal::ApprovalRiskLevel::Medium => control_plane::MobileInboxPriority::Medium,
        journal::ApprovalRiskLevel::Low => control_plane::MobileInboxPriority::Low,
    }
}

fn inbox_priority_weight(priority: &control_plane::MobileInboxPriority) -> u8 {
    match priority {
        control_plane::MobileInboxPriority::Critical => 0,
        control_plane::MobileInboxPriority::High => 1,
        control_plane::MobileInboxPriority::Medium => 2,
        control_plane::MobileInboxPriority::Low => 3,
    }
}

fn build_handoff_path(base_path: &str, params: &[(&str, Option<&str>)]) -> String {
    let mut query = String::new();
    for (key, value) in params {
        if let Some(value) = value.filter(|value| !value.is_empty()) {
            if !query.is_empty() {
                query.push('&');
            }
            query.push_str(key);
            query.push('=');
            query.push_str(value);
        }
    }
    if query.is_empty() {
        base_path.to_owned()
    } else {
        format!("{base_path}?{query}")
    }
}
#[allow(clippy::result_large_err)]
fn parse_mobile_decision_scope(
    value: Option<&str>,
) -> Result<journal::ApprovalDecisionScope, Response> {
    let Some(raw) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(journal::ApprovalDecisionScope::Once);
    };
    match raw.to_ascii_lowercase().as_str() {
        "once" => Ok(journal::ApprovalDecisionScope::Once),
        "session" => Ok(journal::ApprovalDecisionScope::Session),
        "timeboxed" => Ok(journal::ApprovalDecisionScope::Timeboxed),
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "decision_scope must be one of once|session|timeboxed",
        ))),
    }
}
#[allow(clippy::result_large_err)]
fn serialize_value(value: &impl Serialize) -> Result<Value, Response> {
    serde_json::to_value(value).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize mobile response payload: {error}"
        )))
    })
}
