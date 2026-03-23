use crate::*;

pub(crate) fn run_agents(command: AgentsCommand) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for agents command"))?;
    let connection = root_context
        .resolve_grpc_connection(app::ConnectionOverrides::default(), app::ConnectionDefaults::ADMIN)?;
    let runtime = build_runtime()?;
    runtime.block_on(run_agents_async(command, connection))
}

pub(crate) async fn run_agents_async(
    command: AgentsCommand,
    connection: AgentConnection,
) -> Result<()> {
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(
        connection.grpc_url.clone(),
    )
    .await
    .with_context(|| format!("failed to connect gateway gRPC endpoint {}", connection.grpc_url))?;

    match command {
        AgentsCommand::List { after, limit, json, ndjson } => {
            let json = output::preferred_json(json);
            let ndjson = output::preferred_ndjson(json, ndjson);
            let mut request = Request::new(gateway_v1::ListAgentsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                limit: limit.unwrap_or(100),
                after_agent_id: after.unwrap_or_default(),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .list_agents(request)
                .await
                .context("failed to call ListAgents")?
                .into_inner();
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
                    let line = json!({
                        "type": "agent",
                        "agent": agent_to_json(agent),
                        "is_default": response.default_agent_id == agent.agent_id,
                    });
                    println!("{}", serde_json::to_string(&line)?);
                }
            } else {
                println!(
                    "agents.list count={} default={} next_after={}",
                    response.agents.len(),
                    if response.default_agent_id.is_empty() {
                        "none"
                    } else {
                        response.default_agent_id.as_str()
                    },
                    if response.next_after_agent_id.is_empty() {
                        "none"
                    } else {
                        response.next_after_agent_id.as_str()
                    }
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
        AgentsCommand::Show { agent_id, json } => {
            let json = output::preferred_json(json);
            let mut request = Request::new(gateway_v1::GetAgentRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                agent_id: normalize_agent_id_cli(agent_id.as_str())?,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response =
                client.get_agent(request).await.context("failed to call GetAgent")?.into_inner();
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
        AgentsCommand::SetDefault { agent_id, json } => {
            let json = output::preferred_json(json);
            let mut request = Request::new(gateway_v1::SetDefaultAgentRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                agent_id: normalize_agent_id_cli(agent_id.as_str())?,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .set_default_agent(request)
                .await
                .context("failed to call SetDefaultAgent")?
                .into_inner();
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
                    if response.previous_agent_id.is_empty() {
                        "none"
                    } else {
                        response.previous_agent_id.as_str()
                    },
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
            json,
        } => {
            let json = output::preferred_json(json);
            let mut request = Request::new(gateway_v1::CreateAgentRequest {
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
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .create_agent(request)
                .await
                .context("failed to call CreateAgent")?
                .into_inner();
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
                    if response.default_agent_id.is_empty() {
                        "none"
                    } else {
                        response.default_agent_id.as_str()
                    },
                    agent.agent_dir
                );
            }
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}
