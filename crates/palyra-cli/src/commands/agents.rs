//! `palyra agents`: agent registry CRUD, context bindings, default selection,
//! and identity resolution against the gateway runtime API.
//! Model routing is read from the local models status so output can label
//! whether the registry `default_model_profile` is still authoritative.

use std::path::Path;

use anyhow::bail;

use super::models::{load_models_status, ModelsStatusPayload};
use crate::*;

/// Runs a `palyra agents` subcommand on a dedicated Tokio runtime.
///
/// # Errors
/// Returns an error when the gateway connection cannot be resolved or the
/// runtime call fails.
pub(crate) fn run_agents(command: AgentsCommand) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for agents command"))?;
    let connection = root_context.resolve_grpc_connection(
        app::ConnectionOverrides::default(),
        app::ConnectionDefaults::ADMIN,
    )?;
    let runtime = build_runtime()?;
    runtime.block_on(run_agents_async(command, connection))
}

/// Executes an agents subcommand against an already-resolved gateway connection.
///
/// # Errors
/// Returns an error when argument normalization or the gateway runtime call
/// fails, or when `agents delete` is invoked without `--yes`/`--dry-run`.
pub(crate) async fn run_agents_async(
    command: AgentsCommand,
    connection: AgentConnection,
) -> Result<()> {
    let json = match &command {
        AgentsCommand::List { json, .. }
        | AgentsCommand::Bindings { json, .. }
        | AgentsCommand::Show { json, .. }
        | AgentsCommand::Bind { json, .. }
        | AgentsCommand::Unbind { json, .. }
        | AgentsCommand::SetDefault { json, .. }
        | AgentsCommand::Create { json, .. }
        | AgentsCommand::Delete { json, .. }
        | AgentsCommand::Identity { json, .. } => output::preferred_json(*json),
    };
    let mut client = client::runtime::GatewayRuntimeClient::connect(connection.clone()).await?;

    match command {
        AgentsCommand::List { after, limit, json: _, ndjson } => {
            let ndjson = output::preferred_ndjson(json, ndjson);
            let model_routing = load_agent_model_routing_snapshot();
            let response = client.list_agents(after, limit).await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "agents": response.agents.iter().map(|agent| agent_to_json_with_model_routing(agent, &model_routing)).collect::<Vec<_>>(),
                        "default_agent_id": empty_to_none(response.default_agent_id),
                        "next_after_agent_id": empty_to_none(response.next_after_agent_id),
                        "model_routing": model_routing.to_json(),
                        "execution_backends": response.execution_backends.iter().map(execution_backend_inventory_to_json).collect::<Vec<_>>(),
                    }))?
                );
            } else if ndjson {
                for agent in &response.agents {
                    println!(
                        "{}",
                        serde_json::to_string(&json!({
                            "type": "agent",
                            "agent": agent_to_json_with_model_routing(agent, &model_routing),
                            "is_default": response.default_agent_id == agent.agent_id,
                        }))?
                    );
                }
            } else {
                println!(
                    "agents.list count={} default={} next_after={}",
                    response.agents.len(),
                    text_or_none(response.default_agent_id.as_str()),
                    text_or_none(response.next_after_agent_id.as_str())
                );
                println!(
                    "agents.model_routing status={} effective_model_profile={} source={}{}",
                    model_routing.status,
                    model_routing.effective_model_profile.as_deref().unwrap_or("none"),
                    model_routing.source,
                    model_routing
                        .error
                        .as_ref()
                        .map(|error| format!(" error={error}"))
                        .unwrap_or_default()
                );
                print_execution_backend_inventory(response.execution_backends.as_slice());
                for agent in &response.agents {
                    println!(
                        "agent id={} name={} dir={} workspaces={} model_profile={} effective_model_profile={} backend_preference={}",
                        agent.agent_id,
                        agent.display_name,
                        agent.agent_dir,
                        agent.workspace_roots.len(),
                        agent.default_model_profile,
                        model_routing
                            .effective_model_profile
                            .as_deref()
                            .unwrap_or(agent.default_model_profile.as_str()),
                        text_or_none(agent.execution_backend_preference.as_str())
                    );
                }
            }
        }
        AgentsCommand::Bindings {
            agent_id,
            principal,
            channel,
            session_id,
            limit,
            json: _,
            ndjson,
        } => {
            let ndjson = output::preferred_ndjson(json, ndjson);
            let response = client
                .list_agent_bindings(AgentBindingsQueryInput {
                    agent_id: agent_id
                        .map(|value| normalize_agent_id_cli(value.as_str()))
                        .transpose()?
                        .unwrap_or_default(),
                    principal: principal.unwrap_or_default(),
                    channel: channel.unwrap_or_default(),
                    session_id: resolve_optional_canonical_id(session_id)?,
                    limit: limit.unwrap_or(250),
                })
                .await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "bindings": response.bindings.iter().map(agent_binding_to_json).collect::<Vec<_>>(),
                    }))?
                );
            } else if ndjson {
                for binding in &response.bindings {
                    println!(
                        "{}",
                        serde_json::to_string(&json!({
                            "type": "agent_binding",
                            "binding": agent_binding_to_json(binding),
                        }))?
                    );
                }
            } else {
                println!("agents.bindings count={}", response.bindings.len());
                for binding in &response.bindings {
                    println!(
                        "binding agent_id={} principal={} channel={} updated_at_unix_ms={}",
                        binding.agent_id,
                        binding.principal,
                        text_or_none(binding.channel.as_str()),
                        binding.updated_at_unix_ms
                    );
                }
            }
        }
        AgentsCommand::Show { agent_id, json: _ } => {
            let response = client.get_agent(normalize_agent_id_cli(agent_id.as_str())?).await?;
            let agent = response.agent.context("GetAgent returned empty agent payload")?;
            let model_routing = load_agent_model_routing_snapshot();
            let cli_launch_cwd = std::env::current_dir().ok();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "agent": agent_show_to_json_with_model_routing(
                            &agent,
                            &model_routing,
                            cli_launch_cwd.as_deref(),
                        ),
                        "is_default": response.is_default,
                        "model_routing": model_routing.to_json(),
                        "resolved_execution_backend": empty_to_none(response.resolved_execution_backend),
                        "execution_backend_fallback_used": response.execution_backend_fallback_used,
                        "execution_backend_reason": response.execution_backend_reason,
                        "execution_backends": response.execution_backends.iter().map(execution_backend_inventory_to_json).collect::<Vec<_>>(),
                    }))?
                );
            } else {
                println!(
                    "agents.show id={} name={} dir={} default={} model_profile={} effective_model_profile={} backend_preference={} resolved_backend={} fallback={}",
                    agent.agent_id,
                    agent.display_name,
                    agent.agent_dir,
                    response.is_default,
                    agent.default_model_profile,
                    model_routing
                        .effective_model_profile
                        .as_deref()
                        .unwrap_or(agent.default_model_profile.as_str()),
                    text_or_none(agent.execution_backend_preference.as_str()),
                    text_or_none(response.resolved_execution_backend.as_str()),
                    response.execution_backend_fallback_used
                );
                println!("agents.show.backend_reason {}", response.execution_backend_reason);
                println!(
                    "agents.show.workspace_binding mode=cli_launch_cwd_then_agent_workspace_roots cli_launch_workspace_root={} configured_workspace_roots={}",
                    cli_launch_workspace_root_text(cli_launch_cwd.as_deref()),
                    agent_workspace_roots_text(agent.workspace_roots.as_slice())
                );
                print_execution_backend_inventory(response.execution_backends.as_slice());
            }
        }
        AgentsCommand::Bind { agent_id, principal, channel, session_id, json: _ } => {
            let response = client
                .bind_agent_for_context(gateway_v1::BindAgentForContextRequest {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    agent_id: normalize_agent_id_cli(agent_id.as_str())?,
                    principal: principal.unwrap_or_else(|| connection.principal.clone()),
                    channel: channel.unwrap_or_else(|| connection.channel.clone()),
                    session_id: Some(common_v1::CanonicalId {
                        ulid: resolve_or_generate_canonical_id(Some(session_id))?,
                    }),
                })
                .await?;
            let binding = response.binding.context("BindAgentForContext returned empty binding")?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "binding": agent_binding_to_json(&binding),
                        "created": response.created,
                    }))?
                );
            } else {
                println!(
                    "agents.bind agent_id={} principal={} channel={} created={}",
                    binding.agent_id,
                    binding.principal,
                    text_or_none(binding.channel.as_str()),
                    response.created
                );
            }
        }
        AgentsCommand::Unbind { principal, channel, session_id, json: _ } => {
            let response = client
                .unbind_agent_for_context(gateway_v1::UnbindAgentForContextRequest {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    principal: principal.unwrap_or_else(|| connection.principal.clone()),
                    channel: channel.unwrap_or_else(|| connection.channel.clone()),
                    session_id: Some(common_v1::CanonicalId {
                        ulid: resolve_or_generate_canonical_id(Some(session_id))?,
                    }),
                })
                .await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "removed": response.removed,
                        "removed_agent_id": empty_to_none(response.removed_agent_id),
                    }))?
                );
            } else {
                println!(
                    "agents.unbind removed={} removed_agent_id={}",
                    response.removed,
                    text_or_none(response.removed_agent_id.as_str())
                );
            }
        }
        AgentsCommand::SetDefault { agent_id, json: _ } => {
            let response =
                client.set_default_agent(normalize_agent_id_cli(agent_id.as_str())?).await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "previous_agent_id": empty_to_none(response.previous_agent_id),
                        "default_agent_id": response.default_agent_id,
                    }))?
                );
            } else {
                println!(
                    "agents.set_default previous={} default={}",
                    text_or_none(response.previous_agent_id.as_str()),
                    response.default_agent_id
                );
            }
        }
        AgentsCommand::Create {
            agent_id,
            display_name,
            agent_dir,
            workspace_root,
            model_profile,
            execution_backend,
            tool_allow,
            skill_allow,
            set_default,
            allow_absolute_paths,
            json: _,
        } => {
            reject_unflagged_absolute_agent_dir(agent_dir.as_deref(), allow_absolute_paths)?;
            let (workspace_roots, allow_absolute_paths) =
                prepare_workspace_roots_for_create(workspace_root, allow_absolute_paths)?;
            let response = client
                .create_agent(gateway_v1::CreateAgentRequest {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    agent_id: normalize_agent_id_cli(agent_id.as_str())?,
                    display_name,
                    agent_dir: agent_dir.unwrap_or_default(),
                    workspace_roots,
                    default_model_profile: model_profile.unwrap_or_default(),
                    execution_backend_preference: execution_backend
                        .and_then(normalize_optional_text_arg)
                        .unwrap_or_default(),
                    default_tool_allowlist: tool_allow,
                    default_skill_allowlist: skill_allow,
                    set_default,
                    allow_absolute_paths,
                })
                .await?;
            let agent = response.agent.context("CreateAgent returned empty agent payload")?;
            if json {
                let model_routing = load_agent_model_routing_snapshot();
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "agent": agent_to_json_with_model_routing(&agent, &model_routing),
                        "default_changed": response.default_changed,
                        "default_agent_id": empty_to_none(response.default_agent_id),
                        "model_routing": model_routing.to_json(),
                        "resolved_execution_backend": empty_to_none(response.resolved_execution_backend),
                        "execution_backend_fallback_used": response.execution_backend_fallback_used,
                        "execution_backend_reason": response.execution_backend_reason,
                        "execution_backends": response.execution_backends.iter().map(execution_backend_inventory_to_json).collect::<Vec<_>>(),
                    }))?
                );
            } else {
                println!(
                    "agents.create id={} name={} default_changed={} default={} dir={} backend_preference={} resolved_backend={} fallback={}",
                    agent.agent_id,
                    agent.display_name,
                    response.default_changed,
                    text_or_none(response.default_agent_id.as_str()),
                    agent.agent_dir,
                    text_or_none(agent.execution_backend_preference.as_str()),
                    text_or_none(response.resolved_execution_backend.as_str()),
                    response.execution_backend_fallback_used
                );
                println!("agents.create.backend_reason {}", response.execution_backend_reason);
                print_execution_backend_inventory(response.execution_backends.as_slice());
            }
        }
        AgentsCommand::Delete { agent_id, dry_run, yes, json: _ } => {
            let normalized_agent_id = normalize_agent_id_cli(agent_id.as_str())?;
            let agent = client
                .get_agent(normalized_agent_id.clone())
                .await?
                .agent
                .context("GetAgent returned empty agent payload")?;
            let bindings = client
                .list_agent_bindings(AgentBindingsQueryInput {
                    agent_id: normalized_agent_id.clone(),
                    principal: String::new(),
                    channel: String::new(),
                    session_id: None,
                    limit: 1_000,
                })
                .await?
                .bindings;
            if dry_run {
                if json {
                    let model_routing = load_agent_model_routing_snapshot();
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "agent": agent_to_json_with_model_routing(&agent, &model_routing),
                            "would_delete": true,
                            "binding_count": bindings.len(),
                            "bindings": bindings.iter().map(agent_binding_to_json).collect::<Vec<_>>(),
                            "agent_dir_retained": true,
                        }))?
                    );
                } else {
                    println!(
                        "agents.delete.dry_run agent_id={} binding_count={} agent_dir={} agent_dir_retained=true",
                        agent.agent_id,
                        bindings.len(),
                        agent.agent_dir
                    );
                }
            } else {
                if !yes {
                    anyhow::bail!(
                        "agents delete requires --yes; use --dry-run to inspect the impact first"
                    );
                }
                let response = client.delete_agent(normalized_agent_id).await?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "deleted": response.deleted,
                            "deleted_agent_id": response.deleted_agent_id,
                            "removed_bindings_count": response.removed_bindings_count,
                            "previous_default_agent_id": empty_to_none(response.previous_default_agent_id),
                            "default_agent_id": empty_to_none(response.default_agent_id),
                            "agent_dir": response.agent_dir,
                            "agent_dir_retained": true,
                        }))?
                    );
                } else {
                    println!(
                        "agents.delete deleted={} agent_id={} removed_bindings={} previous_default={} default={} agent_dir={} agent_dir_retained=true",
                        response.deleted,
                        response.deleted_agent_id,
                        response.removed_bindings_count,
                        text_or_none(response.previous_default_agent_id.as_str()),
                        text_or_none(response.default_agent_id.as_str()),
                        response.agent_dir
                    );
                }
            }
        }
        AgentsCommand::Identity {
            principal,
            channel,
            session_id,
            preferred_agent_id,
            persist_binding,
            json: _,
        } => {
            let response = client
                .resolve_agent_for_context(AgentContextResolveInput {
                    principal: principal.unwrap_or_else(|| connection.principal.clone()),
                    channel: channel.unwrap_or_else(|| connection.channel.clone()),
                    session_id: resolve_optional_canonical_id(session_id)?,
                    preferred_agent_id: preferred_agent_id
                        .map(|value| normalize_agent_id_cli(value.as_str()))
                        .transpose()?
                        .unwrap_or_default(),
                    persist_session_binding: persist_binding,
                })
                .await?;
            let agent =
                response.agent.context("ResolveAgentForContext returned empty agent payload")?;
            let model_routing = load_agent_model_routing_snapshot();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "agent": agent_to_json_with_model_routing(&agent, &model_routing),
                        "source": agent_resolution_source_label(response.source),
                        "binding_created": response.binding_created,
                        "is_default": response.is_default,
                        "model_routing": model_routing.to_json(),
                        "resolved_execution_backend": empty_to_none(response.resolved_execution_backend),
                        "execution_backend_fallback_used": response.execution_backend_fallback_used,
                        "execution_backend_reason": response.execution_backend_reason,
                        "execution_backends": response.execution_backends.iter().map(execution_backend_inventory_to_json).collect::<Vec<_>>(),
                    }))?
                );
            } else {
                println!(
                    "agents.identity agent_id={} source={} binding_created={} default={} effective_model_profile={} backend_preference={} resolved_backend={} fallback={}",
                    agent.agent_id,
                    agent_resolution_source_label(response.source),
                    response.binding_created,
                    response.is_default,
                    model_routing
                        .effective_model_profile
                        .as_deref()
                        .unwrap_or(agent.default_model_profile.as_str()),
                    text_or_none(agent.execution_backend_preference.as_str()),
                    text_or_none(response.resolved_execution_backend.as_str()),
                    response.execution_backend_fallback_used
                );
                println!("agents.identity.backend_reason {}", response.execution_backend_reason);
                print_execution_backend_inventory(response.execution_backends.as_slice());
            }
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}

#[derive(Debug, Clone)]
struct AgentModelRoutingSnapshot {
    status: String,
    effective_model_profile: Option<String>,
    source: String,
    provider_id: Option<String>,
    provider_kind: Option<String>,
    error: Option<String>,
}

impl AgentModelRoutingSnapshot {
    fn to_json(&self) -> Value {
        json!({
            "status": self.status,
            "effective_model_profile": self.effective_model_profile,
            "source": self.source,
            "provider_id": self.provider_id,
            "provider_kind": self.provider_kind,
            "error": self.error,
        })
    }
}

fn load_agent_model_routing_snapshot() -> AgentModelRoutingSnapshot {
    match load_models_status(None) {
        Ok(status) => agent_model_routing_snapshot_from_status(&status),
        Err(error) => AgentModelRoutingSnapshot {
            status: "unavailable".to_owned(),
            effective_model_profile: None,
            source: "models_status_error".to_owned(),
            provider_id: None,
            provider_kind: None,
            error: Some(sanitize_diagnostic_error(format!("{error:#}").as_str())),
        },
    }
}

fn agent_model_routing_snapshot_from_status(
    status: &ModelsStatusPayload,
) -> AgentModelRoutingSnapshot {
    let (effective_model_profile, source) =
        if let Some(model) = status.default_chat_model_id.clone() {
            (Some(model), "model_provider.default_chat_model_id")
        } else if let Some(model) = status.text_model.clone() {
            (Some(model), "model_provider.text_model")
        } else {
            (None, "model_provider.unconfigured")
        };
    AgentModelRoutingSnapshot {
        status: if effective_model_profile.is_some() { "available" } else { "missing" }.to_owned(),
        effective_model_profile,
        source: source.to_owned(),
        provider_id: Some(status.provider_id.clone()),
        provider_kind: Some(status.provider_kind.clone()),
        error: None,
    }
}

fn agent_to_json_with_model_routing(
    agent: &gateway_v1::Agent,
    routing: &AgentModelRoutingSnapshot,
) -> Value {
    let effective_model_profile =
        routing.effective_model_profile.as_deref().unwrap_or(agent.default_model_profile.as_str());
    let default_is_authoritative = routing
        .effective_model_profile
        .as_deref()
        .is_none_or(|model| model.eq_ignore_ascii_case(agent.default_model_profile.as_str()));
    json!({
        "agent_id": agent.agent_id,
        "display_name": agent.display_name,
        "agent_dir": agent.agent_dir,
        "workspace_roots": agent.workspace_roots,
        "default_model_profile": agent.default_model_profile,
        "default_model_profile_source": "agent_registry",
        "default_model_profile_authoritative": default_is_authoritative,
        "effective_model_profile": effective_model_profile,
        "effective_model_profile_source": routing.source,
        "model_profile_authority": if default_is_authoritative { "agent_registry" } else { "model_provider_runtime" },
        "model_profile_note": if default_is_authoritative {
            Value::Null
        } else {
            Value::String(
                "default_model_profile is legacy agent registry metadata; runtime chat routing uses effective_model_profile"
                    .to_owned(),
            )
        },
        "model_routing_status": routing.status,
        "model_routing_error": routing.error,
        "execution_backend_preference": empty_to_none(agent.execution_backend_preference.clone()),
        "default_tool_allowlist": agent.default_tool_allowlist,
        "default_skill_allowlist": agent.default_skill_allowlist,
        "created_at_unix_ms": agent.created_at_unix_ms,
        "updated_at_unix_ms": agent.updated_at_unix_ms,
    })
}

fn agent_show_to_json_with_model_routing(
    agent: &gateway_v1::Agent,
    routing: &AgentModelRoutingSnapshot,
    cli_launch_cwd: Option<&Path>,
) -> Value {
    let mut payload = agent_to_json_with_model_routing(agent, routing);
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "agent_run_workspace_binding".to_owned(),
            agent_run_workspace_binding_to_json_with_launch_cwd(agent, cli_launch_cwd),
        );
    }
    payload
}

fn agent_run_workspace_binding_to_json_with_launch_cwd(
    agent: &gateway_v1::Agent,
    cli_launch_cwd: Option<&Path>,
) -> Value {
    let cli_launch_workspace_root = cli_launch_workspace_root_text(cli_launch_cwd);
    let effective_first_workspace_root = if cli_launch_workspace_root == "unknown" {
        agent.workspace_roots.first().cloned().map(Value::String).unwrap_or(Value::Null)
    } else {
        Value::String(cli_launch_workspace_root.clone())
    };

    json!({
        "mode": "cli_launch_cwd_then_agent_workspace_roots",
        "cli_launch_workspace_root": cli_launch_workspace_root,
        "configured_workspace_roots": agent.workspace_roots.clone(),
        "effective_first_workspace_root": effective_first_workspace_root,
    })
}

fn cli_launch_workspace_root_text(cli_launch_cwd: Option<&Path>) -> String {
    cli_launch_cwd
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn agent_workspace_roots_text(workspace_roots: &[String]) -> String {
    if workspace_roots.is_empty() {
        "none".to_owned()
    } else {
        workspace_roots.join(",")
    }
}

// Session ids are redacted (presence-only) so binding listings cannot be used
// to hijack or correlate live sessions.
fn agent_binding_to_json(binding: &gateway_v1::AgentBinding) -> Value {
    json!({
        "agent_id": binding.agent_id,
        "principal": binding.principal,
        "channel": empty_to_none(binding.channel.clone()),
        "session_id": if binding.session_id.is_some() { Value::String(REDACTED.to_owned()) } else { Value::Null },
        "updated_at_unix_ms": binding.updated_at_unix_ms,
    })
}

fn execution_backend_inventory_to_json(backend: &gateway_v1::ExecutionBackendInventory) -> Value {
    json!({
        "backend_id": backend.backend_id,
        "label": backend.label,
        "state": backend.state,
        "selectable": backend.selectable,
        "selected_by_default": backend.selected_by_default,
        "description": backend.description,
        "operator_summary": backend.operator_summary,
        "executor_label": empty_to_none(backend.executor_label.clone()),
        "rollout_flag": empty_to_none(backend.rollout_flag.clone()),
        "rollout_enabled": backend.rollout_enabled,
        "capabilities": backend.capabilities,
        "tradeoffs": backend.tradeoffs,
        "active_node_count": backend.active_node_count,
        "total_node_count": backend.total_node_count,
    })
}

fn reject_unflagged_absolute_agent_dir(
    agent_dir: Option<&str>,
    allow_absolute_paths: bool,
) -> Result<()> {
    let Some(agent_dir) = agent_dir.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    if is_cli_absolute_path(agent_dir) && !allow_absolute_paths {
        bail!("absolute agent directory `{agent_dir}` requires --allow-absolute-paths");
    }
    Ok(())
}

fn prepare_workspace_roots_for_create(
    workspace_roots: Vec<String>,
    allow_absolute_paths: bool,
) -> Result<(Vec<String>, bool)> {
    let mut normalized = Vec::with_capacity(workspace_roots.len());
    for raw in workspace_roots {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            bail!("workspace root cannot be empty");
        }
        if is_cli_absolute_path(trimmed) && !allow_absolute_paths {
            bail!("absolute workspace root `{trimmed}` requires --allow-absolute-paths");
        }
        normalized.push(trimmed.to_owned());
    }
    Ok((normalized, allow_absolute_paths))
}

// Checks Windows drive-letter and UNC forms explicitly because the daemon may
// run on a different OS than this CLI; Path::is_absolute alone only reflects
// the local platform's path rules.
fn is_cli_absolute_path(value: &str) -> bool {
    Path::new(value).is_absolute()
        || value.starts_with(r"\\")
        || value.as_bytes().get(1).is_some_and(|byte| *byte == b':')
}

fn print_execution_backend_inventory(backends: &[gateway_v1::ExecutionBackendInventory]) {
    if backends.is_empty() {
        return;
    }
    for backend in backends {
        println!(
            "backend id={} state={} selectable={} rollout_enabled={} active_nodes={}/{}",
            backend.backend_id,
            text_or_none(backend.state.as_str()),
            backend.selectable,
            backend.rollout_enabled,
            backend.active_node_count,
            backend.total_node_count
        );
    }
}

fn text_or_none(value: &str) -> &str {
    if value.trim().is_empty() {
        "none"
    } else {
        value
    }
}

fn agent_resolution_source_label(raw: i32) -> &'static str {
    match gateway_v1::AgentResolutionSource::try_from(raw)
        .unwrap_or(gateway_v1::AgentResolutionSource::Unspecified)
    {
        gateway_v1::AgentResolutionSource::SessionBinding => "session_binding",
        gateway_v1::AgentResolutionSource::Default => "default",
        gateway_v1::AgentResolutionSource::Fallback => "fallback",
        gateway_v1::AgentResolutionSource::Unspecified => "unspecified",
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use tempfile::tempdir;

    fn sample_models_status() -> ModelsStatusPayload {
        ModelsStatusPayload {
            path: "palyra.toml".to_owned(),
            provider_id: "minimax-primary".to_owned(),
            provider_display_name: "MiniMax".to_owned(),
            protocol_compatibility: "anthropic_compatible".to_owned(),
            provider_kind: "anthropic".to_owned(),
            auth_provider_kind: Some("minimax".to_owned()),
            endpoint_base_url: Some("https://api.minimax.io".to_owned()),
            openai_base_url: None,
            text_model: Some("MiniMax-M2.7".to_owned()),
            embeddings_model: None,
            embeddings_dims: None,
            auth_profile_id: None,
            api_key_configured: true,
            default_chat_model_id: Some("MiniMax-M2.7".to_owned()),
            default_embeddings_model_id: None,
            failover_enabled: true,
            response_cache_enabled: true,
            registry_provider_count: 1,
            registry_model_count: 1,
            registry_valid: true,
            validation_issues: Vec::new(),
            migrated: false,
        }
    }

    fn sample_agent(default_model_profile: &str) -> gateway_v1::Agent {
        gateway_v1::Agent {
            agent_id: "local-default".to_owned(),
            display_name: "LocalDefaultAgent".to_owned(),
            default_model_profile: default_model_profile.to_owned(),
            ..gateway_v1::Agent::default()
        }
    }

    #[test]
    fn agent_json_labels_registry_model_profile_as_non_authoritative_when_routing_differs() {
        let routing = agent_model_routing_snapshot_from_status(&sample_models_status());
        let payload = agent_to_json_with_model_routing(&sample_agent("deterministic"), &routing);

        assert_eq!(payload["default_model_profile"], "deterministic");
        assert_eq!(payload["effective_model_profile"], "MiniMax-M2.7");
        assert_eq!(
            payload["effective_model_profile_source"],
            "model_provider.default_chat_model_id"
        );
        assert_eq!(payload["default_model_profile_authoritative"], false);
        assert_eq!(payload["model_profile_authority"], "model_provider_runtime");
    }

    #[test]
    fn agent_json_keeps_registry_profile_authoritative_when_it_matches_routing() {
        let routing = agent_model_routing_snapshot_from_status(&sample_models_status());
        let payload = agent_to_json_with_model_routing(&sample_agent("MiniMax-M2.7"), &routing);

        assert_eq!(payload["default_model_profile_authoritative"], true);
        assert_eq!(payload["model_profile_authority"], "agent_registry");
        assert!(payload["model_profile_note"].is_null());
    }

    #[test]
    fn agent_show_json_reports_cli_launch_workspace_binding() {
        let routing = agent_model_routing_snapshot_from_status(&sample_models_status());
        let mut agent = sample_agent("MiniMax-M2.7");
        agent.workspace_roots = vec!["C:/palyra/state/workspace".to_owned()];

        let payload = agent_show_to_json_with_model_routing(
            &agent,
            &routing,
            Some(Path::new("/tmp/current-project")),
        );

        let binding = &payload["agent_run_workspace_binding"];
        assert_eq!(binding["mode"], "cli_launch_cwd_then_agent_workspace_roots");
        assert_eq!(binding["cli_launch_workspace_root"], "/tmp/current-project");
        assert_eq!(binding["configured_workspace_roots"][0], "C:/palyra/state/workspace");
        assert_eq!(binding["effective_first_workspace_root"], "/tmp/current-project");
    }

    #[test]
    fn prepare_workspace_roots_preserves_relative_values_for_daemon_containment() -> Result<()> {
        let (workspace_roots, allow_absolute_paths) =
            prepare_workspace_roots_for_create(vec![" workspace ".to_owned()], false)?;

        assert_eq!(workspace_roots, vec!["workspace".to_owned()]);
        assert!(
            !allow_absolute_paths,
            "relative workspace roots must not implicitly permit absolute daemon roots"
        );
        Ok(())
    }

    #[test]
    fn prepare_workspace_roots_keeps_explicit_absolute_flag_only() -> Result<()> {
        let (workspace_roots, allow_absolute_paths) =
            prepare_workspace_roots_for_create(vec!["workspace".to_owned()], true)?;

        assert_eq!(workspace_roots, vec!["workspace".to_owned()]);
        assert!(allow_absolute_paths);
        Ok(())
    }

    #[test]
    fn prepare_workspace_roots_requires_flag_for_user_supplied_absolute_values() -> Result<()> {
        let temp = tempdir()?;
        let absolute_root = temp.path().to_string_lossy().into_owned();

        let error = prepare_workspace_roots_for_create(vec![absolute_root], false)
            .expect_err("absolute root without flag should fail");

        assert!(format!("{error:#}").contains("--allow-absolute-paths"));
        Ok(())
    }
}
