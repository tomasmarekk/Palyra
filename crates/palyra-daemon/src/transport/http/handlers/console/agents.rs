use std::borrow::Cow;

use serde::{de, Deserialize, Deserializer};

use crate::{
    agents::{AgentCreateRequest, AgentRecord},
    application::service_authorization::authorize_agent_management_action,
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

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleAgentsQuery {
    pub(crate) after_agent_id: Option<BoundedConsoleAgentIdentifier>,
    pub(crate) limit: Option<usize>,
}

pub(crate) async fn console_agents_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleAgentsQuery>,
) -> Result<Json<control_plane::AgentListEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    authorize_console_agent_action(&state, session.context.principal.as_str(), "agent.list")?;

    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let after_agent_id = query
        .after_agent_id
        .map(|value| normalize_console_agent_id(&state, value.as_str(), "after_agent_id"))
        .transpose()?;
    let page = state
        .runtime
        .list_agents(after_agent_id, Some(limit))
        .await
        .map_err(runtime_status_response)?;

    Ok(Json(control_plane::AgentListEnvelope {
        contract: contract_descriptor(),
        agents: page.agents.iter().map(control_plane_agent_from_runtime).collect(),
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

    Ok(Json(control_plane::AgentEnvelope {
        contract: contract_descriptor(),
        agent: control_plane_agent_from_runtime(&agent),
        is_default,
    }))
}

pub(crate) async fn console_agent_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::AgentCreateRequest>,
) -> Result<Json<control_plane::AgentCreateEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    authorize_console_agent_action(&state, session.context.principal.as_str(), "agent.create")?;

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

    Ok(Json(control_plane::AgentCreateEnvelope {
        contract: contract_descriptor(),
        agent: control_plane_agent_from_runtime(&outcome.agent),
        default_changed: outcome.default_changed,
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
        default_tool_allowlist: agent.default_tool_allowlist.clone(),
        default_skill_allowlist: agent.default_skill_allowlist.clone(),
        created_at_unix_ms: agent.created_at_unix_ms,
        updated_at_unix_ms: agent.updated_at_unix_ms,
    }
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
