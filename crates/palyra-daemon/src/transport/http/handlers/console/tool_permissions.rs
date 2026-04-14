use std::collections::BTreeSet;

use serde::Serialize;
use ulid::Ulid;

use crate::{
    agents::AgentBindingQuery,
    journal::{ApprovalRecord, ApprovalSubjectType, JournalAppendRequest},
    tool_posture::{
        build_tool_friction_metrics, build_tool_recommendation, derive_scope_chain,
        evaluate_effective_tool_posture, normalize_scope_id, recent_tool_approvals,
        tool_catalog_entry, tool_posture_preset, EffectiveToolPosture, ToolCatalogEntry,
        ToolFrictionMetrics, ToolPostureAuditEventRecord, ToolPostureOverrideClearRequest,
        ToolPostureOverrideRecord, ToolPostureOverrideUpsertRequest, ToolPostureRecommendation,
        ToolPostureRecommendationAction, ToolPostureRecommendationActionRecord,
        ToolPostureRecommendationActionRequest, ToolPostureScopeKind, ToolPostureScopeRef,
        ToolPostureScopeResetRequest, ToolPostureState, TOOL_CATALOG,
        TOOL_POSTURE_ANALYTICS_WINDOW_MS, TOOL_POSTURE_PRESETS,
    },
    *,
};

const TOOL_PERMISSIONS_APPROVAL_PAGE_SIZE: usize = 500;
const TOOL_PERMISSION_AUDIT_HISTORY_LIMIT: usize = 20;

type ToolPermissionsStatusResult<T> = Result<T, tonic::Status>;

#[derive(Debug, Serialize)]
pub(crate) struct ToolPermissionsScopeEnvelope {
    active: ToolPostureScopeRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    workspace: Option<ToolPostureScopeRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    agent: Option<ToolPostureScopeRef>,
    chain: Vec<ToolPostureScopeRef>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ToolPermissionsSummary {
    total_tools: usize,
    locked_tools: usize,
    high_friction_tools: usize,
    approval_requests_14d: u64,
    pending_approvals_14d: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ToolPermissionRecord {
    tool_name: String,
    title: String,
    description: String,
    category: String,
    risk_level: crate::journal::ApprovalRiskLevel,
    effective_posture: EffectiveToolPosture,
    friction: ToolFrictionMetrics,
    recent_approvals: Vec<ApprovalRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_change: Option<ToolPostureAuditEventRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recommendation: Option<ToolPostureRecommendation>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ToolPermissionsEnvelope {
    contract: control_plane::ContractDescriptor,
    generated_at_unix_ms: i64,
    scope: ToolPermissionsScopeEnvelope,
    summary: ToolPermissionsSummary,
    categories: Vec<String>,
    presets: Vec<crate::tool_posture::ToolPosturePresetDefinition>,
    tools: Vec<ToolPermissionRecord>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ToolPermissionDetailEnvelope {
    contract: control_plane::ContractDescriptor,
    generated_at_unix_ms: i64,
    scope: ToolPermissionsScopeEnvelope,
    tool: ToolPermissionRecord,
    change_history: Vec<ToolPostureAuditEventRecord>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ToolPermissionPresetDiffEntry {
    tool_name: String,
    title: String,
    current_state: ToolPostureState,
    proposed_state: ToolPostureState,
    changed: bool,
    locked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    lock_reason: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ToolPermissionPresetPreviewEnvelope {
    contract: control_plane::ContractDescriptor,
    generated_at_unix_ms: i64,
    scope: ToolPermissionsScopeEnvelope,
    preset: crate::tool_posture::ToolPosturePresetDefinition,
    preview: Vec<ToolPermissionPresetDiffEntry>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ToolPermissionMutationEnvelope {
    contract: control_plane::ContractDescriptor,
    generated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    override_record: Option<ToolPostureOverrideRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recommendation_action: Option<ToolPostureRecommendationActionRecord>,
    detail: ToolPermissionDetailEnvelope,
}

#[derive(Debug, Serialize)]
pub(crate) struct ToolPermissionScopeResetEnvelope {
    contract: control_plane::ContractDescriptor,
    generated_at_unix_ms: i64,
    scope: ToolPermissionsScopeEnvelope,
    removed: Vec<ToolPostureOverrideRecord>,
}

#[derive(Debug, Clone)]
struct ResolvedToolPermissionsScope {
    active: ToolPostureScopeRef,
    workspace: Option<ToolPostureScopeRef>,
    agent: Option<ToolPostureScopeRef>,
    chain: Vec<ToolPostureScopeRef>,
}

#[derive(Debug, Clone)]
struct ToolPermissionsData {
    scope: ResolvedToolPermissionsScope,
    overrides: Vec<ToolPostureOverrideRecord>,
    recommendation_actions: Vec<ToolPostureRecommendationActionRecord>,
    audit_events: Vec<ToolPostureAuditEventRecord>,
    approvals: Vec<ApprovalRecord>,
    config: crate::gateway::GatewayRuntimeConfigSnapshot,
}

#[rustfmt::skip]
struct ToolPermissionsJournalEvent<'a> {
    event_name: &'a str, scope_kind: ToolPostureScopeKind, scope_id: &'a str,
    tool_name: Option<&'a str>, previous_state: Option<ToolPostureState>, new_state: Option<ToolPostureState>,
    source: &'a str, reason: Option<&'a str>, recommendation_id: Option<&'a str>, preset_id: Option<&'a str>,
}

pub(crate) async fn console_tool_permissions_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleToolPermissionsQuery>,
) -> Result<Json<ToolPermissionsEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let data = load_tool_permissions_data(&state, &query).await?;
    let mut tools = build_tool_permission_records(&data);
    apply_tool_permissions_filters(&mut tools, &query);
    Ok(Json(ToolPermissionsEnvelope {
        contract: contract_descriptor(),
        generated_at_unix_ms: gateway::current_unix_ms(),
        scope: scope_envelope(&data.scope),
        summary: build_tool_permissions_summary(tools.as_slice()),
        categories: tool_permission_categories(),
        presets: TOOL_POSTURE_PRESETS.to_vec(),
        tools,
    }))
}

pub(crate) async fn console_tool_permission_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(tool_name): Path<String>,
    Query(query): Query<ConsoleToolPermissionsQuery>,
) -> Result<Json<ToolPermissionDetailEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let data = load_tool_permissions_data(&state, &query).await?;
    Ok(Json(
        build_tool_permission_detail(&data, tool_name.as_str()).map_err(runtime_status_response)?,
    ))
}

pub(crate) async fn console_tool_permission_override_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(tool_name): Path<String>,
    Json(payload): Json<ConsoleToolPostureOverrideRequest>,
) -> Result<Json<ToolPermissionMutationEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_known_tool(tool_name.as_str()).map_err(runtime_status_response)?;
    let scope_kind = parse_tool_posture_scope_kind(payload.scope_kind.as_str())
        .map_err(runtime_status_response)?;
    let scope_id = normalize_scope_id(scope_kind, payload.scope_id.as_deref())
        .map_err(tool_posture_registry_error_response)?;
    let state_value =
        parse_tool_posture_state(payload.state.as_str()).map_err(runtime_status_response)?;
    let override_record = state
        .runtime
        .upsert_tool_posture_override(ToolPostureOverrideUpsertRequest {
            tool_name: tool_name.clone(),
            scope_kind,
            scope_id: scope_id.clone(),
            state: state_value,
            reason: payload.reason.clone(),
            actor_principal: session.context.principal.clone(),
            source: "manual".to_owned(),
            expires_at_unix_ms: payload.expires_at_unix_ms,
            now_unix_ms: gateway::current_unix_ms(),
        })
        .map_err(runtime_status_response)?;
    #[rustfmt::skip]
    let event = ToolPermissionsJournalEvent { event_name: "tool_posture.override_set", scope_kind, scope_id: scope_id.as_str(), tool_name: Some(tool_name.as_str()), previous_state: None, new_state: Some(state_value), source: "manual", reason: payload.reason.as_deref(), recommendation_id: None, preset_id: None };
    record_tool_permissions_journal_event(&state.runtime, &session.context, event)
        .await
        .map_err(runtime_status_response)?;
    let detail =
        build_tool_permission_detail_for_scope(&state, tool_name.as_str(), scope_kind, &scope_id)
            .await?;
    Ok(Json(ToolPermissionMutationEnvelope {
        contract: contract_descriptor(),
        generated_at_unix_ms: gateway::current_unix_ms(),
        override_record: Some(override_record),
        recommendation_action: None,
        detail,
    }))
}

pub(crate) async fn console_tool_permission_reset_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(tool_name): Path<String>,
    Json(payload): Json<ConsoleToolPostureResetRequest>,
) -> Result<Json<ToolPermissionMutationEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_known_tool(tool_name.as_str()).map_err(runtime_status_response)?;
    let scope_kind = parse_tool_posture_scope_kind(payload.scope_kind.as_str())
        .map_err(runtime_status_response)?;
    let scope_id = normalize_scope_id(scope_kind, payload.scope_id.as_deref())
        .map_err(tool_posture_registry_error_response)?;
    let removed = state
        .runtime
        .clear_tool_posture_override(ToolPostureOverrideClearRequest {
            tool_name: tool_name.clone(),
            scope_kind,
            scope_id: scope_id.clone(),
            actor_principal: session.context.principal.clone(),
            source: "manual_reset".to_owned(),
            reason: payload.reason.clone(),
            now_unix_ms: gateway::current_unix_ms(),
        })
        .map_err(runtime_status_response)?;
    if removed {
        #[rustfmt::skip]
        let event = ToolPermissionsJournalEvent { event_name: "tool_posture.override_cleared", scope_kind, scope_id: scope_id.as_str(), tool_name: Some(tool_name.as_str()), previous_state: None, new_state: None, source: "manual_reset", reason: payload.reason.as_deref(), recommendation_id: None, preset_id: None };
        record_tool_permissions_journal_event(&state.runtime, &session.context, event)
            .await
            .map_err(runtime_status_response)?;
    }
    let detail =
        build_tool_permission_detail_for_scope(&state, tool_name.as_str(), scope_kind, &scope_id)
            .await?;
    Ok(Json(ToolPermissionMutationEnvelope {
        contract: contract_descriptor(),
        generated_at_unix_ms: gateway::current_unix_ms(),
        override_record: None,
        recommendation_action: None,
        detail,
    }))
}

pub(crate) async fn console_tool_permission_scope_reset_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleToolPostureScopeResetRequest>,
) -> Result<Json<ToolPermissionScopeResetEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let scope_kind = parse_tool_posture_scope_kind(payload.scope_kind.as_str())
        .map_err(runtime_status_response)?;
    let scope_id = normalize_scope_id(scope_kind, payload.scope_id.as_deref())
        .map_err(tool_posture_registry_error_response)?;
    let removed = state
        .runtime
        .reset_tool_posture_scope(ToolPostureScopeResetRequest {
            scope_kind,
            scope_id: scope_id.clone(),
            actor_principal: session.context.principal.clone(),
            source: "manual_scope_reset".to_owned(),
            reason: payload.reason.clone(),
            now_unix_ms: gateway::current_unix_ms(),
        })
        .map_err(runtime_status_response)?;
    for record in removed.as_slice() {
        #[rustfmt::skip]
        let event = ToolPermissionsJournalEvent { event_name: "tool_posture.scope_reset", scope_kind, scope_id: scope_id.as_str(), tool_name: Some(record.tool_name.as_str()), previous_state: Some(record.state), new_state: None, source: "manual_scope_reset", reason: payload.reason.as_deref(), recommendation_id: None, preset_id: None };
        record_tool_permissions_journal_event(&state.runtime, &session.context, event)
            .await
            .map_err(runtime_status_response)?;
    }
    let data = load_tool_permissions_data(
        &state,
        &ConsoleToolPermissionsQuery {
            scope_kind: Some(scope_kind.as_str().to_owned()),
            scope_id: Some(scope_id),
            q: None,
            category: None,
            state: None,
            locked_only: None,
            high_friction_only: None,
        },
    )
    .await?;
    Ok(Json(ToolPermissionScopeResetEnvelope {
        contract: contract_descriptor(),
        generated_at_unix_ms: gateway::current_unix_ms(),
        scope: scope_envelope(&data.scope),
        removed,
    }))
}

pub(crate) async fn console_tool_permissions_preset_preview_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleToolPosturePresetPreviewRequest>,
) -> Result<Json<ToolPermissionPresetPreviewEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let scope_kind = parse_tool_posture_scope_kind(payload.scope_kind.as_str())
        .map_err(runtime_status_response)?;
    let scope_id = normalize_scope_id(scope_kind, payload.scope_id.as_deref())
        .map_err(tool_posture_registry_error_response)?;
    let data = load_tool_permissions_data(
        &state,
        &ConsoleToolPermissionsQuery {
            scope_kind: Some(scope_kind.as_str().to_owned()),
            scope_id: Some(scope_id),
            q: None,
            category: None,
            state: None,
            locked_only: None,
            high_friction_only: None,
        },
    )
    .await?;
    let preset = tool_posture_preset(payload.preset_id.as_str()).ok_or_else(|| {
        runtime_status_response(tonic::Status::not_found(format!(
            "tool posture preset not found: {}",
            payload.preset_id
        )))
    })?;
    Ok(Json(ToolPermissionPresetPreviewEnvelope {
        contract: contract_descriptor(),
        generated_at_unix_ms: gateway::current_unix_ms(),
        scope: scope_envelope(&data.scope),
        preset: *preset,
        preview: build_preset_preview_entries(&data, preset),
    }))
}

pub(crate) async fn console_tool_permissions_preset_apply_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleToolPosturePresetApplyRequest>,
) -> Result<Json<ToolPermissionPresetPreviewEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let scope_kind = parse_tool_posture_scope_kind(payload.scope_kind.as_str())
        .map_err(runtime_status_response)?;
    let scope_id = normalize_scope_id(scope_kind, payload.scope_id.as_deref())
        .map_err(tool_posture_registry_error_response)?;
    let preset = tool_posture_preset(payload.preset_id.as_str()).ok_or_else(|| {
        runtime_status_response(tonic::Status::not_found(format!(
            "tool posture preset not found: {}",
            payload.preset_id
        )))
    })?;
    let data_before = load_tool_permissions_data(
        &state,
        &ConsoleToolPermissionsQuery {
            scope_kind: Some(scope_kind.as_str().to_owned()),
            scope_id: Some(scope_id.clone()),
            q: None,
            category: None,
            state: None,
            locked_only: None,
            high_friction_only: None,
        },
    )
    .await?;
    for assignment in preset.assignments {
        let Some(entry) = TOOL_CATALOG.iter().find(|entry| entry.tool_name == assignment.tool_name)
        else {
            continue;
        };
        let posture = evaluate_effective_tool_posture(
            &data_before.config,
            data_before.overrides.as_slice(),
            data_before.scope.chain.as_slice(),
            entry.tool_name,
        );
        if !posture.editable || posture.effective_state == assignment.state {
            continue;
        }
        state
            .runtime
            .upsert_tool_posture_override(ToolPostureOverrideUpsertRequest {
                tool_name: entry.tool_name.to_owned(),
                scope_kind,
                scope_id: scope_id.clone(),
                state: assignment.state,
                reason: payload.reason.clone(),
                actor_principal: session.context.principal.clone(),
                source: format!("preset:{}", preset.preset_id),
                expires_at_unix_ms: None,
                now_unix_ms: gateway::current_unix_ms(),
            })
            .map_err(runtime_status_response)?;
        let source = format!("preset:{}", preset.preset_id);
        #[rustfmt::skip]
        let event = ToolPermissionsJournalEvent { event_name: "tool_posture.preset_applied", scope_kind, scope_id: scope_id.as_str(), tool_name: Some(entry.tool_name), previous_state: Some(posture.effective_state), new_state: Some(assignment.state), source: source.as_str(), reason: payload.reason.as_deref(), recommendation_id: None, preset_id: Some(preset.preset_id) };
        record_tool_permissions_journal_event(&state.runtime, &session.context, event)
            .await
            .map_err(runtime_status_response)?;
    }
    let data_after = load_tool_permissions_data(
        &state,
        &ConsoleToolPermissionsQuery {
            scope_kind: Some(scope_kind.as_str().to_owned()),
            scope_id: Some(scope_id),
            q: None,
            category: None,
            state: None,
            locked_only: None,
            high_friction_only: None,
        },
    )
    .await?;
    Ok(Json(ToolPermissionPresetPreviewEnvelope {
        contract: contract_descriptor(),
        generated_at_unix_ms: gateway::current_unix_ms(),
        scope: scope_envelope(&data_after.scope),
        preset: *preset,
        preview: build_preset_preview_entries(&data_after, preset),
    }))
}

pub(crate) async fn console_tool_permissions_recommendation_action_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleToolPostureRecommendationActionRequest>,
) -> Result<Json<ToolPermissionMutationEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_known_tool(payload.tool_name.as_str()).map_err(runtime_status_response)?;
    let scope_kind = parse_tool_posture_scope_kind(payload.scope_kind.as_str())
        .map_err(runtime_status_response)?;
    let scope_id = normalize_scope_id(scope_kind, payload.scope_id.as_deref())
        .map_err(tool_posture_registry_error_response)?;
    let action = parse_tool_posture_recommendation_action(payload.action.as_str())
        .map_err(runtime_status_response)?;
    let data = load_tool_permissions_data(
        &state,
        &ConsoleToolPermissionsQuery {
            scope_kind: Some(scope_kind.as_str().to_owned()),
            scope_id: Some(scope_id.clone()),
            q: None,
            category: None,
            state: None,
            locked_only: None,
            high_friction_only: None,
        },
    )
    .await?;
    let record = build_tool_permission_record(
        &data,
        tool_catalog_entry(payload.tool_name.as_str()).expect("checked"),
    );
    let recommendation = record
        .recommendation
        .clone()
        .filter(|recommendation| recommendation.recommendation_id == payload.recommendation_id)
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::failed_precondition(format!(
                "recommendation is no longer applicable: {}",
                payload.recommendation_id
            )))
        })?;
    let recommendation_action = state
        .runtime
        .record_tool_posture_recommendation_action(ToolPostureRecommendationActionRequest {
            recommendation_id: payload.recommendation_id.clone(),
            scope_kind,
            scope_id: scope_id.clone(),
            action,
            actor_principal: session.context.principal.clone(),
            now_unix_ms: gateway::current_unix_ms(),
        })
        .map_err(runtime_status_response)?;
    if matches!(action, ToolPostureRecommendationAction::Accepted) {
        state
            .runtime
            .upsert_tool_posture_override(ToolPostureOverrideUpsertRequest {
                tool_name: payload.tool_name.clone(),
                scope_kind,
                scope_id: scope_id.clone(),
                state: recommendation.recommended_state,
                reason: Some(recommendation.reason.clone()),
                actor_principal: session.context.principal.clone(),
                source: "recommendation".to_owned(),
                expires_at_unix_ms: None,
                now_unix_ms: gateway::current_unix_ms(),
            })
            .map_err(runtime_status_response)?;
        #[rustfmt::skip]
        let event = ToolPermissionsJournalEvent { event_name: "tool_posture.recommendation_accepted", scope_kind, scope_id: scope_id.as_str(), tool_name: Some(payload.tool_name.as_str()), previous_state: Some(record.effective_posture.effective_state), new_state: Some(recommendation.recommended_state), source: "recommendation", reason: Some(recommendation.reason.as_str()), recommendation_id: Some(payload.recommendation_id.as_str()), preset_id: None };
        record_tool_permissions_journal_event(&state.runtime, &session.context, event)
            .await
            .map_err(runtime_status_response)?;
    }
    let detail = build_tool_permission_detail_for_scope(
        &state,
        payload.tool_name.as_str(),
        scope_kind,
        &scope_id,
    )
    .await?;
    Ok(Json(ToolPermissionMutationEnvelope {
        contract: contract_descriptor(),
        generated_at_unix_ms: gateway::current_unix_ms(),
        override_record: None,
        recommendation_action: Some(recommendation_action),
        detail,
    }))
}

async fn load_tool_permissions_data(
    state: &AppState,
    query: &ConsoleToolPermissionsQuery,
) -> Result<ToolPermissionsData, Response> {
    let scope = resolve_tool_permissions_scope(state, query).await?;
    let approvals = list_recent_tool_approvals_window(&state.runtime).await?;
    Ok(ToolPermissionsData {
        scope,
        overrides: state.runtime.list_tool_posture_overrides().map_err(runtime_status_response)?,
        recommendation_actions: state
            .runtime
            .list_tool_posture_recommendation_actions()
            .map_err(runtime_status_response)?,
        audit_events: state
            .runtime
            .list_tool_posture_audit_events()
            .map_err(runtime_status_response)?,
        approvals,
        config: state.runtime.runtime_config_snapshot(),
    })
}

async fn resolve_tool_permissions_scope(
    state: &AppState,
    query: &ConsoleToolPermissionsQuery,
) -> Result<ResolvedToolPermissionsScope, Response> {
    let scope_kind = parse_tool_posture_scope_kind(query.scope_kind.as_deref().unwrap_or("global"))
        .map_err(runtime_status_response)?;
    let scope_id = normalize_scope_id(scope_kind, query.scope_id.as_deref())
        .map_err(tool_posture_registry_error_response)?;
    let active = ToolPostureScopeRef {
        kind: scope_kind,
        scope_id: scope_id.clone(),
        label: tool_posture_scope_label(scope_kind, scope_id.as_str()),
    };
    let (workspace, agent) = if matches!(scope_kind, ToolPostureScopeKind::Session) {
        let bindings = state
            .runtime
            .list_agent_bindings(AgentBindingQuery {
                agent_id: None,
                principal: None,
                channel: None,
                session_id: Some(scope_id.clone()),
                limit: Some(1),
            })
            .await
            .map_err(runtime_status_response)?;
        let binding = bindings.first().cloned();
        let workspace = binding
            .as_ref()
            .and_then(|binding| binding.principal.strip_prefix("workspace:"))
            .map(|workspace_id| ToolPostureScopeRef {
                kind: ToolPostureScopeKind::Workspace,
                scope_id: workspace_id.to_owned(),
                label: tool_posture_scope_label(ToolPostureScopeKind::Workspace, workspace_id),
            });
        let agent = binding.as_ref().map(|binding| ToolPostureScopeRef {
            kind: ToolPostureScopeKind::Agent,
            scope_id: binding.agent_id.clone(),
            label: tool_posture_scope_label(ToolPostureScopeKind::Agent, binding.agent_id.as_str()),
        });
        (workspace, agent)
    } else {
        (None, None)
    };
    Ok(ResolvedToolPermissionsScope {
        chain: derive_scope_chain(active.clone(), workspace.clone(), agent.clone()),
        active,
        workspace,
        agent,
    })
}

async fn list_recent_tool_approvals_window(
    runtime: &std::sync::Arc<gateway::GatewayRuntimeState>,
) -> Result<Vec<ApprovalRecord>, Response> {
    let mut approvals = Vec::new();
    let mut after_approval_id = None;
    let since_unix_ms = Some(gateway::current_unix_ms() - TOOL_POSTURE_ANALYTICS_WINDOW_MS);
    loop {
        let (page, next_after) = runtime
            .list_approval_records(
                after_approval_id.clone(),
                Some(TOOL_PERMISSIONS_APPROVAL_PAGE_SIZE),
                since_unix_ms,
                None,
                None,
                None,
                None,
                Some(ApprovalSubjectType::Tool),
            )
            .await
            .map_err(runtime_status_response)?;
        approvals.extend(page);
        if next_after.is_none() {
            break;
        }
        after_approval_id = next_after;
    }
    Ok(approvals)
}

fn build_tool_permission_records(data: &ToolPermissionsData) -> Vec<ToolPermissionRecord> {
    TOOL_CATALOG.iter().map(|catalog| build_tool_permission_record(data, catalog)).collect()
}

fn build_tool_permission_record(
    data: &ToolPermissionsData,
    catalog: &ToolCatalogEntry,
) -> ToolPermissionRecord {
    let effective_posture = evaluate_effective_tool_posture(
        &data.config,
        data.overrides.as_slice(),
        data.scope.chain.as_slice(),
        catalog.tool_name,
    );
    let friction = build_tool_friction_metrics(data.approvals.as_slice(), catalog.tool_name);
    let last_change = data
        .audit_events
        .iter()
        .find(|event| {
            event.tool_name.as_deref() == Some(catalog.tool_name)
                && scope_in_chain(
                    data.scope.chain.as_slice(),
                    event.scope_kind,
                    event.scope_id.as_str(),
                )
        })
        .cloned();
    let recommendation_action = data
        .recommendation_actions
        .iter()
        .find(|action| {
            action.recommendation_id
                == crate::tool_posture::tool_recommendation_id(
                    data.scope.active.kind,
                    data.scope.active.scope_id.as_str(),
                    catalog.tool_name,
                )
                && action.scope_kind == data.scope.active.kind
                && action.scope_id == data.scope.active.scope_id
        })
        .map(|record| record.action);
    let recommendation = build_tool_recommendation(
        catalog.tool_name,
        catalog,
        &data.scope.active,
        &effective_posture,
        &friction,
        recommendation_action,
    );
    ToolPermissionRecord {
        tool_name: catalog.tool_name.to_owned(),
        title: catalog.title.to_owned(),
        description: catalog.description.to_owned(),
        category: catalog.category.to_owned(),
        risk_level: catalog.risk_level,
        effective_posture,
        friction,
        recent_approvals: recent_tool_approvals(data.approvals.as_slice(), catalog.tool_name, 5)
            .into_iter()
            .cloned()
            .collect(),
        last_change,
        recommendation,
    }
}

fn build_tool_permission_detail(
    data: &ToolPermissionsData,
    tool_name: &str,
) -> ToolPermissionsStatusResult<ToolPermissionDetailEnvelope> {
    let catalog = tool_catalog_entry(tool_name).ok_or_else(|| {
        tonic::Status::not_found(format!("tool posture record not found: {tool_name}"))
    })?;
    let tool = build_tool_permission_record(data, catalog);
    let change_history = data
        .audit_events
        .iter()
        .filter(|event| {
            event.tool_name.as_deref() == Some(tool_name)
                && scope_in_chain(
                    data.scope.chain.as_slice(),
                    event.scope_kind,
                    event.scope_id.as_str(),
                )
        })
        .take(TOOL_PERMISSION_AUDIT_HISTORY_LIMIT)
        .cloned()
        .collect();
    Ok(ToolPermissionDetailEnvelope {
        contract: contract_descriptor(),
        generated_at_unix_ms: gateway::current_unix_ms(),
        scope: scope_envelope(&data.scope),
        tool,
        change_history,
    })
}

async fn build_tool_permission_detail_for_scope(
    state: &AppState,
    tool_name: &str,
    scope_kind: ToolPostureScopeKind,
    scope_id: &str,
) -> Result<ToolPermissionDetailEnvelope, Response> {
    let data = load_tool_permissions_data(
        state,
        &ConsoleToolPermissionsQuery {
            scope_kind: Some(scope_kind.as_str().to_owned()),
            scope_id: Some(scope_id.to_owned()),
            q: None,
            category: None,
            state: None,
            locked_only: None,
            high_friction_only: None,
        },
    )
    .await?;
    build_tool_permission_detail(&data, tool_name).map_err(runtime_status_response)
}

fn apply_tool_permissions_filters(
    tools: &mut Vec<ToolPermissionRecord>,
    query: &ConsoleToolPermissionsQuery,
) {
    let search = query.q.as_deref().map(str::trim).filter(|value| !value.is_empty());
    let category = query.category.as_deref().map(str::trim).filter(|value| !value.is_empty());
    let state = query.state.as_deref().and_then(|value| parse_tool_posture_state(value).ok());
    tools.retain(|tool| {
        let matches_search = search.is_none_or(|search| {
            let search = search.to_ascii_lowercase();
            tool.tool_name.to_ascii_lowercase().contains(search.as_str())
                || tool.title.to_ascii_lowercase().contains(search.as_str())
                || tool.description.to_ascii_lowercase().contains(search.as_str())
                || tool.category.to_ascii_lowercase().contains(search.as_str())
        });
        let matches_category =
            category.is_none_or(|category| tool.category.eq_ignore_ascii_case(category));
        let matches_state =
            state.is_none_or(|state| tool.effective_posture.effective_state == state);
        let matches_locked =
            !query.locked_only.unwrap_or(false) || tool.effective_posture.lock_reason.is_some();
        let matches_friction = !query.high_friction_only.unwrap_or(false)
            || tool.friction.approved_14d >= 3
            || tool.friction.pending_14d > 0;
        matches_search && matches_category && matches_state && matches_locked && matches_friction
    });
}

fn build_tool_permissions_summary(tools: &[ToolPermissionRecord]) -> ToolPermissionsSummary {
    ToolPermissionsSummary {
        total_tools: tools.len(),
        locked_tools: tools
            .iter()
            .filter(|tool| tool.effective_posture.lock_reason.is_some())
            .count(),
        high_friction_tools: tools
            .iter()
            .filter(|tool| tool.friction.approved_14d >= 3 || tool.friction.pending_14d > 0)
            .count(),
        approval_requests_14d: tools.iter().map(|tool| tool.friction.requested_14d).sum(),
        pending_approvals_14d: tools.iter().map(|tool| tool.friction.pending_14d).sum(),
    }
}

fn tool_permission_categories() -> Vec<String> {
    let mut categories = BTreeSet::new();
    for entry in TOOL_CATALOG {
        categories.insert(entry.category.to_owned());
    }
    categories.into_iter().collect()
}

fn build_preset_preview_entries(
    data: &ToolPermissionsData,
    preset: &crate::tool_posture::ToolPosturePresetDefinition,
) -> Vec<ToolPermissionPresetDiffEntry> {
    preset
        .assignments
        .iter()
        .filter_map(|assignment| {
            tool_catalog_entry(assignment.tool_name).map(|catalog| (assignment, catalog))
        })
        .map(|(assignment, catalog)| {
            let posture = evaluate_effective_tool_posture(
                &data.config,
                data.overrides.as_slice(),
                data.scope.chain.as_slice(),
                assignment.tool_name,
            );
            ToolPermissionPresetDiffEntry {
                tool_name: assignment.tool_name.to_owned(),
                title: catalog.title.to_owned(),
                current_state: posture.effective_state,
                proposed_state: assignment.state,
                changed: posture.effective_state != assignment.state,
                locked: !posture.editable,
                lock_reason: posture.lock_reason,
            }
        })
        .collect()
}

fn scope_envelope(scope: &ResolvedToolPermissionsScope) -> ToolPermissionsScopeEnvelope {
    ToolPermissionsScopeEnvelope {
        active: scope.active.clone(),
        workspace: scope.workspace.clone(),
        agent: scope.agent.clone(),
        chain: scope.chain.clone(),
    }
}

fn scope_in_chain(
    chain: &[ToolPostureScopeRef],
    scope_kind: ToolPostureScopeKind,
    scope_id: &str,
) -> bool {
    chain.iter().any(|scope| scope.kind == scope_kind && scope.scope_id == scope_id)
}

fn ensure_known_tool(tool_name: &str) -> ToolPermissionsStatusResult<()> {
    tool_catalog_entry(tool_name).map(|_| ()).ok_or_else(|| {
        tonic::Status::not_found(format!("tool posture record not found: {tool_name}"))
    })
}

fn parse_tool_posture_scope_kind(value: &str) -> ToolPermissionsStatusResult<ToolPostureScopeKind> {
    match value.trim().to_ascii_lowercase().as_str() {
        "global" => Ok(ToolPostureScopeKind::Global),
        "workspace" => Ok(ToolPostureScopeKind::Workspace),
        "agent" => Ok(ToolPostureScopeKind::Agent),
        "session" => Ok(ToolPostureScopeKind::Session),
        _ => Err(tonic::Status::invalid_argument(
            "scope_kind must be one of global|workspace|agent|session",
        )),
    }
}

fn parse_tool_posture_state(value: &str) -> ToolPermissionsStatusResult<ToolPostureState> {
    match value.trim().to_ascii_lowercase().as_str() {
        "always_allow" => Ok(ToolPostureState::AlwaysAllow),
        "ask_each_time" => Ok(ToolPostureState::AskEachTime),
        "disabled" => Ok(ToolPostureState::Disabled),
        _ => Err(tonic::Status::invalid_argument(
            "state must be one of always_allow|ask_each_time|disabled",
        )),
    }
}

fn parse_tool_posture_recommendation_action(
    value: &str,
) -> ToolPermissionsStatusResult<ToolPostureRecommendationAction> {
    match value.trim().to_ascii_lowercase().as_str() {
        "accepted" => Ok(ToolPostureRecommendationAction::Accepted),
        "dismissed" => Ok(ToolPostureRecommendationAction::Dismissed),
        "deferred" => Ok(ToolPostureRecommendationAction::Deferred),
        _ => Err(tonic::Status::invalid_argument(
            "action must be one of accepted|dismissed|deferred",
        )),
    }
}

fn tool_posture_scope_label(scope_kind: ToolPostureScopeKind, scope_id: &str) -> String {
    match scope_kind {
        ToolPostureScopeKind::Global => "Global default".to_owned(),
        ToolPostureScopeKind::Workspace => format!("Workspace {scope_id}"),
        ToolPostureScopeKind::Agent => format!("Agent {scope_id}"),
        ToolPostureScopeKind::Session => format!("Session {scope_id}"),
    }
}

fn tool_posture_registry_error_response(
    error: crate::tool_posture::ToolPostureRegistryError,
) -> Response {
    runtime_status_response(tonic::Status::invalid_argument(error.to_string()))
}

async fn record_tool_permissions_journal_event(
    runtime: &std::sync::Arc<gateway::GatewayRuntimeState>,
    context: &crate::transport::grpc::auth::RequestContext,
    event: ToolPermissionsJournalEvent<'_>,
) -> Result<(), tonic::Status> {
    #[rustfmt::skip]
    let ToolPermissionsJournalEvent { event_name, scope_kind, scope_id, tool_name, previous_state, new_state, source, reason, recommendation_id, preset_id } = event;
    let synthetic_session_id = format!("tool-posture:{}:{scope_id}", scope_kind.as_str());
    runtime
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: synthetic_session_id,
            run_id: format!("tool-posture-{}", Ulid::new()),
            kind: gateway::proto::palyra::common::v1::journal_event::EventKind::ToolExecuted as i32,
            actor: gateway::proto::palyra::common::v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: gateway::current_unix_ms(),
            payload_json: json!({
                "event": event_name,
                "scope_kind": scope_kind.as_str(),
                "scope_id": scope_id,
                "tool_name": tool_name,
                "previous_state": previous_state.map(ToolPostureState::as_str),
                "new_state": new_state.map(ToolPostureState::as_str),
                "source": source,
                "reason": reason,
                "recommendation_id": recommendation_id,
                "preset_id": preset_id,
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}
