//! Console agent registry handlers for the `/console/v1/agents` route family.
//!
//! Exposes list/get/create/set-default over the runtime agent registry and
//! decorates every envelope with the execution-backend inventory so the web
//! console can render backend selection state. Response shapes are part of
//! the `/console/v1` wire contract consumed by `apps/web`.

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

/// Upper bound for agent identifiers accepted from path and query input.
const CONSOLE_MAX_AGENT_ID_QUERY_BYTES: usize = 64;

/// Agent identifier extracted from a request path.
///
/// The length cap is enforced at deserialization time so oversized input is
/// rejected before any normalization or registry lookup runs.
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

/// Handles `GET /console/v1/agents`: lists registered agents with keyset
/// paging plus the execution-backend inventory.
///
/// # Errors
/// Returns an error response when console authorization fails, when the query
/// string is invalid, or when the runtime cannot list agents or build the
/// backend inventory.
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

/// Handles `GET /console/v1/agents/{agent_id}`: returns one agent with its
/// resolved execution-backend selection.
///
/// # Errors
/// Returns an error response when console authorization fails, when the agent
/// id is invalid or unknown, or when the backend inventory cannot be built.
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

/// Handles `POST /console/v1/agents`: creates an agent and records audit
/// journal events for the creation and any default-agent change.
///
/// # Errors
/// Returns an error response when console authorization or CSRF validation
/// fails, when the execution-backend preference is invalid or not selectable,
/// or when the runtime rejects the create request.
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

    // Journal writes are best-effort: the agent is already persisted, and a
    // failed audit entry must not turn a successful create into an error.
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

/// Handles `POST /console/v1/agents/{agent_id}/set-default`: switches the
/// default agent and records a best-effort journal event.
///
/// # Errors
/// Returns an error response when console authorization or CSRF validation
/// fails, when the agent id is invalid, or when the runtime rejects the
/// default change.
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

/// Builds the execution-backend inventory from the current node and worker
/// fleet state so envelopes reflect live backend availability.
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

/// Authorizes one agent-management action for the console principal.
///
/// # Errors
/// Returns the mapped authorization failure; the runtime denied counter is
/// incremented as a side effect so access metrics stay accurate.
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

/// Normalizes a caller-supplied agent identifier.
///
/// # Errors
/// Returns the validation failure response; the agent-validation-failure
/// counter is incremented as a side effect so rejected input stays visible
/// in diagnostics.
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

/// Parses the supported list-query parameters by hand from the raw query
/// string so the byte-length cap applies before any decoding or
/// normalization; unknown keys are ignored.
///
/// # Errors
/// Returns an invalid-argument response when `after_agent_id` exceeds the
/// byte cap or fails normalization, or when `limit` is not an unsigned
/// integer.
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
