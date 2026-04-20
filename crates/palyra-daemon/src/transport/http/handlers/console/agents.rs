use std::borrow::Cow;

use serde::{de, Deserialize, Deserializer};

use crate::{
    agents::{AgentCreateRequest, AgentRecord},
    application::service_authorization::authorize_agent_management_action,
    execution_backends::{
        build_execution_backend_inventory_with_worker_state,
        parse_optional_execution_backend_preference, resolve_execution_backend,
        validate_execution_backend_selection,
    },
    gateway::{normalize_agent_identifier, record_agent_journal_event},
    *,
};

const CONSOLE_MAX_AGENT_ID_QUERY_BYTES: usize = 64;

#[derive(Debug)]
pub(crate) struct BoundedConsoleAgentIdentifier(String);

impl BoundedConsoleAgentIdentifier {
    fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<'de> Deserialize<'de> for BoundedConsoleAgentIdentifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Cow::<'de, str>::deserialize(deserializer)?;
        if value.len() > CONSOLE_MAX_AGENT_ID_QUERY_BYTES {
            return Err(de::Error::custom(format!(
                "agent identifier cannot exceed {CONSOLE_MAX_AGENT_ID_QUERY_BYTES} bytes"
            )));
        }
        Ok(Self(value.into_owned()))
    }
}

#[derive(Debug, Default)]
struct ConsoleAgentsListQuery {
    after_agent_id: Option<String>,
    limit: Option<usize>,
}

pub(crate) async fn console_agents_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: axum::http::Uri,
) -> Result<Json<control_plane::AgentListEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    authorize_console_agent_action(&state, session.context.principal.as_str(), "agent.list")?;

    let query = parse_console_agents_list_query(&state, &uri)?;
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let page = state
        .runtime
        .list_agents(query.after_agent_id, Some(limit))
        .await
        .map_err(runtime_status_response)?;
    let inventory = backend_inventory(&state).map_err(runtime_status_response)?;

    Ok(Json(control_plane::AgentListEnvelope {
        contract: contract_descriptor(),
        agents: page.agents.iter().map(control_plane_agent_from_runtime).collect(),
        execution_backends: inventory
            .iter()
            .map(control_plane_execution_backend_inventory)
            .collect(),
        default_agent_id: page.default_agent_id,
        page: build_page_info(limit, page.agents.len(), page.next_after_agent_id),
    }))
}

pub(crate) async fn console_agent_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(agent_id): Path<BoundedConsoleAgentIdentifier>,
) -> Result<Json<control_plane::AgentEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    authorize_console_agent_action(&state, session.context.principal.as_str(), "agent.get")?;

    let agent_id = normalize_console_agent_id(&state, agent_id.as_str(), "agent_id")?;
    let (agent, is_default) =
        state.runtime.get_agent(agent_id).await.map_err(runtime_status_response)?;
    let inventory = backend_inventory(&state).map_err(runtime_status_response)?;
    let resolution = resolve_execution_backend(agent.execution_backend_preference, &inventory);

    Ok(Json(control_plane::AgentEnvelope {
        contract: contract_descriptor(),
        agent: control_plane_agent_from_runtime(&agent),
        is_default,
        execution_backends: inventory
            .iter()
            .map(control_plane_execution_backend_inventory)
            .collect(),
        resolved_execution_backend: resolution.resolved.as_str().to_owned(),
        execution_backend_fallback_used: resolution.fallback_used,
        execution_backend_reason_code: resolution.reason_code,
        execution_backend_approval_required: resolution.approval_required,
        execution_backend_reason: resolution.reason,
    }))
}

pub(crate) async fn console_agent_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::AgentCreateRequest>,
) -> Result<Json<control_plane::AgentCreateEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    authorize_console_agent_action(&state, session.context.principal.as_str(), "agent.create")?;
    let inventory = backend_inventory(&state).map_err(runtime_status_response)?;
    let execution_backend_preference = parse_optional_execution_backend_preference(
        payload.execution_backend_preference.as_deref(),
        "execution_backend_preference",
    )
    .map_err(|message| runtime_status_response(tonic::Status::invalid_argument(message)))?;
    if let Some(preference) = execution_backend_preference {
        validate_execution_backend_selection(preference, &inventory).map_err(|message| {
            runtime_status_response(tonic::Status::failed_precondition(message))
        })?;
    }

    let outcome = state
        .runtime
        .create_agent(AgentCreateRequest {
            agent_id: payload.agent_id,
            display_name: payload.display_name,
            agent_dir: payload.agent_dir.filter(|value| !value.trim().is_empty()),
            workspace_roots: payload.workspace_roots,
            default_model_profile: payload
                .default_model_profile
                .filter(|value| !value.trim().is_empty()),
            execution_backend_preference,
            default_tool_allowlist: payload.default_tool_allowlist,
            default_skill_allowlist: payload.default_skill_allowlist,
            set_default: payload.set_default,
            allow_absolute_paths: payload.allow_absolute_paths,
        })
        .await
        .map_err(runtime_status_response)?;

    let _ = record_agent_journal_event(
        &state.runtime,
        &session.context,
        json!({
            "event": "agent.created",
            "agent_id": outcome.agent.agent_id,
            "display_name": outcome.agent.display_name,
            "agent_dir": outcome.agent.agent_dir,
            "workspace_roots": outcome.agent.workspace_roots,
            "default_model_profile": outcome.agent.default_model_profile,
            "default_changed": outcome.default_changed,
            "default_agent_id": outcome.default_agent_id,
        }),
    )
    .await;
    if outcome.default_changed {
        let _ = record_agent_journal_event(
            &state.runtime,
            &session.context,
            json!({
                "event": "agent.default_changed",
                "previous_default_agent_id": outcome.previous_default_agent_id,
                "default_agent_id": outcome.default_agent_id,
            }),
        )
        .await;
    }

    let resolution =
        resolve_execution_backend(outcome.agent.execution_backend_preference, &inventory);
    Ok(Json(control_plane::AgentCreateEnvelope {
        contract: contract_descriptor(),
        agent: control_plane_agent_from_runtime(&outcome.agent),
        default_changed: outcome.default_changed,
        execution_backends: inventory
            .iter()
            .map(control_plane_execution_backend_inventory)
            .collect(),
        resolved_execution_backend: resolution.resolved.as_str().to_owned(),
        execution_backend_fallback_used: resolution.fallback_used,
        execution_backend_reason_code: resolution.reason_code,
        execution_backend_approval_required: resolution.approval_required,
        execution_backend_reason: resolution.reason,
        default_agent_id: outcome.default_agent_id,
    }))
}

pub(crate) async fn console_agent_set_default_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(agent_id): Path<BoundedConsoleAgentIdentifier>,
) -> Result<Json<control_plane::AgentSetDefaultEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    authorize_console_agent_action(
        &state,
        session.context.principal.as_str(),
        "agent.set_default",
    )?;

    let agent_id = normalize_console_agent_id(&state, agent_id.as_str(), "agent_id")?;
    let outcome =
        state.runtime.set_default_agent(agent_id).await.map_err(runtime_status_response)?;

    let _ = record_agent_journal_event(
        &state.runtime,
        &session.context,
        json!({
            "event": "agent.default_changed",
            "previous_default_agent_id": outcome.previous_default_agent_id,
            "default_agent_id": outcome.default_agent_id,
        }),
    )
    .await;

    Ok(Json(control_plane::AgentSetDefaultEnvelope {
        contract: contract_descriptor(),
        previous_default_agent_id: outcome.previous_default_agent_id,
        default_agent_id: outcome.default_agent_id,
    }))
}

fn control_plane_agent_from_runtime(agent: &AgentRecord) -> control_plane::AgentRecord {
    control_plane::AgentRecord {
        agent_id: agent.agent_id.clone(),
        display_name: agent.display_name.clone(),
        agent_dir: agent.agent_dir.clone(),
        workspace_roots: agent.workspace_roots.clone(),
        default_model_profile: agent.default_model_profile.clone(),
        execution_backend_preference: agent.execution_backend_preference.as_str().to_owned(),
        default_tool_allowlist: agent.default_tool_allowlist.clone(),
        default_skill_allowlist: agent.default_skill_allowlist.clone(),
        created_at_unix_ms: agent.created_at_unix_ms,
        updated_at_unix_ms: agent.updated_at_unix_ms,
    }
}

fn control_plane_execution_backend_inventory(
    backend: &crate::execution_backends::ExecutionBackendInventoryRecord,
) -> control_plane::ExecutionBackendInventoryRecord {
    control_plane::ExecutionBackendInventoryRecord {
        backend_id: backend.backend_id.clone(),
        label: backend.label.clone(),
        state: backend.state.as_str().to_owned(),
        selectable: backend.selectable,
        selected_by_default: backend.selected_by_default,
        description: backend.description.clone(),
        operator_summary: backend.operator_summary.clone(),
        executor_label: backend.executor_label.clone(),
        rollout_flag: backend.rollout_flag.clone(),
        rollout_enabled: backend.rollout_enabled,
        capabilities: backend.capabilities.clone(),
        tradeoffs: backend.tradeoffs.clone(),
        requires_attestation: backend.requires_attestation,
        requires_egress_proxy: backend.requires_egress_proxy,
        workspace_scope_mode: backend.workspace_scope_mode.clone(),
        artifact_transport: backend.artifact_transport.clone(),
        cleanup_strategy: backend.cleanup_strategy.clone(),
        active_node_count: backend.active_node_count,
        total_node_count: backend.total_node_count,
    }
}

fn backend_inventory(
    state: &AppState,
) -> Result<Vec<crate::execution_backends::ExecutionBackendInventoryRecord>, tonic::Status> {
    let now_unix_ms = crate::gateway::current_unix_ms_status()?;
    let nodes = state.node_runtime.nodes()?;
    Ok(build_execution_backend_inventory_with_worker_state(
        &state.runtime.config.tool_call.process_runner,
        nodes.as_slice(),
        now_unix_ms,
        &state.runtime.config.feature_rollouts,
        &state.runtime.config.networked_workers,
        state.runtime.worker_fleet_snapshot(),
        &state.runtime.worker_fleet_policy(),
    ))
}

#[allow(clippy::result_large_err)]
fn authorize_console_agent_action(
    state: &AppState,
    principal: &str,
    action: &'static str,
) -> Result<(), Response> {
    authorize_agent_management_action(principal, action, "agent:registry").map_err(|error| {
        state.runtime.record_denied();
        runtime_status_response(error)
    })
}

#[allow(clippy::result_large_err)]
fn normalize_console_agent_id(
    state: &AppState,
    raw: &str,
    field_name: &'static str,
) -> Result<String, Response> {
    normalize_agent_identifier(raw, field_name).map_err(|error| {
        state
            .runtime
            .counters
            .agent_validation_failures
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        runtime_status_response(error)
    })
}

#[allow(clippy::result_large_err)]
fn parse_console_agents_list_query(
    state: &AppState,
    uri: &axum::http::Uri,
) -> Result<ConsoleAgentsListQuery, Response> {
    let mut parsed = ConsoleAgentsListQuery::default();
    let Some(query) = uri.query() else {
        return Ok(parsed);
    };

    for segment in query.split('&') {
        if segment.is_empty() {
            continue;
        }
        let (key, raw_value) = segment.split_once('=').unwrap_or((segment, ""));
        match key {
            "after_agent_id" => {
                if raw_value.len() > CONSOLE_MAX_AGENT_ID_QUERY_BYTES {
                    return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
                        "after_agent_id cannot exceed {CONSOLE_MAX_AGENT_ID_QUERY_BYTES} bytes"
                    ))));
                }
                parsed.after_agent_id =
                    Some(normalize_console_agent_id(state, raw_value, "after_agent_id")?);
            }
            "limit" => {
                let limit = raw_value.parse::<usize>().map_err(|_| {
                    runtime_status_response(tonic::Status::invalid_argument(
                        "limit must be an unsigned integer",
                    ))
                })?;
                parsed.limit = Some(limit);
            }
            _ => {}
        }
    }

    Ok(parsed)
}
