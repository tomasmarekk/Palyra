use crate::*;

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
            let response = client.list_agents(after, limit).await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "agents": response.agents.iter().map(agent_to_json).collect::<Vec<_>>(),
                        "default_agent_id": empty_to_none(response.default_agent_id),
                        "next_after_agent_id": empty_to_none(response.next_after_agent_id),
                    }))?
                );
            } else if ndjson {
                for agent in &response.agents {
                    println!(
                        "{}",
                        serde_json::to_string(&json!({
                            "type": "agent",
                            "agent": agent_to_json(agent),
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
                for agent in &response.agents {
                    println!(
                        "agent id={} name={} dir={} workspaces={} model_profile={}",
                        agent.agent_id,
                        agent.display_name,
                        agent.agent_dir,
                        agent.workspace_roots.len(),
                        agent.default_model_profile
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
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "agent": agent_to_json(&agent),
                        "is_default": response.is_default,
                    }))?
                );
            } else {
                println!(
                    "agents.show id={} name={} dir={} default={} model_profile={}",
                    agent.agent_id,
                    agent.display_name,
                    agent.agent_dir,
                    response.is_default,
                    agent.default_model_profile
                );
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
            tool_allow,
            skill_allow,
            set_default,
            allow_absolute_paths,
            json: _,
        } => {
            let response = client
                .create_agent(gateway_v1::CreateAgentRequest {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    agent_id: normalize_agent_id_cli(agent_id.as_str())?,
                    display_name,
                    agent_dir: agent_dir.unwrap_or_default(),
                    workspace_roots: workspace_root,
                    default_model_profile: model_profile.unwrap_or_default(),
                    default_tool_allowlist: tool_allow,
                    default_skill_allowlist: skill_allow,
                    set_default,
                    allow_absolute_paths,
                })
                .await?;
            let agent = response.agent.context("CreateAgent returned empty agent payload")?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "agent": agent_to_json(&agent),
                        "default_changed": response.default_changed,
                        "default_agent_id": empty_to_none(response.default_agent_id),
                    }))?
                );
            } else {
                println!(
                    "agents.create id={} name={} default_changed={} default={} dir={}",
                    agent.agent_id,
                    agent.display_name,
                    response.default_changed,
                    text_or_none(response.default_agent_id.as_str()),
                    agent.agent_dir
                );
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
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "agent": agent_to_json(&agent),
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
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "agent": agent_to_json(&agent),
                        "source": agent_resolution_source_label(response.source),
                        "binding_created": response.binding_created,
                        "is_default": response.is_default,
                    }))?
                );
            } else {
                println!(
                    "agents.identity agent_id={} source={} binding_created={} default={}",
                    agent.agent_id,
                    agent_resolution_source_label(response.source),
                    response.binding_created,
                    response.is_default
                );
            }
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}

fn agent_binding_to_json(binding: &gateway_v1::AgentBinding) -> Value {
    json!({
        "agent_id": binding.agent_id,
        "principal": binding.principal,
        "channel": empty_to_none(binding.channel.clone()),
        "session_id": if binding.session_id.is_some() { Value::String(REDACTED.to_owned()) } else { Value::Null },
        "updated_at_unix_ms": binding.updated_at_unix_ms,
    })
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
