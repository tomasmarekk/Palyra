use crate::*;

fn interactive_session_started_message(
    session: Option<&gateway_v1::SessionSummary>,
    hinted_session_key: Option<&str>,
    hinted_session_label: Option<&str>,
) -> String {
    let session_key =
        session.map(|value| value.session_key.as_str()).or(hinted_session_key).unwrap_or("").trim();
    let session_label = session
        .map(|value| value.session_label.as_str())
        .or(hinted_session_label)
        .unwrap_or("")
        .trim();
    let mut parts =
        vec!["agent.interactive=session_started".to_owned(), "exit_hint=/exit".to_owned()];
    if !session_key.is_empty() {
        parts.push(format!("session_key={session_key}"));
    }
    if !session_label.is_empty() {
        parts.push(format!("session_label={session_label}"));
    }
    parts.push("help_hint=/help".to_owned());
    parts.join(" ")
}

pub(crate) fn run_agent(command: AgentCommand) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for agent command"))?;
    match command {
        AgentCommand::Run {
            grpc_url,
            token,
            principal,
            device_id,
            channel,
            session_id,
            session_key,
            session_label,
            require_existing,
            reset_session,
            run_id,
            prompt,
            prompt_stdin,
            allow_sensitive_tools,
            ndjson,
        } => {
            let input_prompt = resolve_prompt_input(prompt, prompt_stdin)?;
            let connection = root_context.resolve_grpc_connection(
                app::ConnectionOverrides {
                    grpc_url,
                    token,
                    principal,
                    device_id,
                    channel,
                    daemon_url: None,
                },
                app::ConnectionDefaults::USER,
            )?;
            let request = build_agent_run_input(AgentRunInputArgs {
                session_id: resolve_optional_canonical_id(session_id)?,
                session_key,
                session_label,
                require_existing,
                reset_session,
                run_id,
                prompt: input_prompt,
                allow_sensitive_tools,
                origin_kind: None,
                origin_run_id: None,
                parameter_delta_json: None,
            })?;
            execute_agent_stream(connection, request, output::preferred_ndjson(false, ndjson))
        }
        AgentCommand::Interactive {
            grpc_url,
            token,
            principal,
            device_id,
            channel,
            session_id,
            session_key,
            session_label,
            require_existing,
            allow_sensitive_tools,
            ndjson,
        } => {
            let connection = root_context.resolve_grpc_connection(
                app::ConnectionOverrides {
                    grpc_url,
                    token,
                    principal,
                    device_id,
                    channel,
                    daemon_url: None,
                },
                app::ConnectionDefaults::USER,
            )?;
            let runtime = build_runtime()?;
            runtime.block_on(run_agent_interactive_async(
                connection,
                session_id,
                session_key,
                session_label,
                require_existing,
                allow_sensitive_tools,
                ndjson,
            ))
        }
        AgentCommand::AcpShim { command } => commands::acp::run_legacy_agent_acp_shim(command),
        AgentCommand::Acp { command } => commands::acp::run_legacy_agent_acp(command),
    }
}

async fn run_agent_interactive_async(
    connection: AgentConnection,
    session_id: Option<String>,
    session_key: Option<String>,
    session_label: Option<String>,
    require_existing: bool,
    allow_sensitive_tools: bool,
    ndjson: bool,
) -> Result<()> {
    let runtime = client::operator::OperatorRuntime::new(connection.clone());
    let mut session = None::<gateway_v1::SessionSummary>;
    let initial_session_id = session_id;
    let initial_session_key = session_key;
    let initial_session_label = session_label;
    let started_message = interactive_session_started_message(
        None,
        initial_session_key.as_deref(),
        initial_session_label.as_deref(),
    );
    eprintln!("{started_message}");
    std::io::stderr().flush().context("stderr flush failed")?;

    let stdin = std::io::stdin();
    let mut last_run_id = None::<String>;
    for line in stdin.lock().lines() {
        let prompt = line.context("failed to read interactive prompt from stdin")?;
        let prompt = prompt.trim();
        if prompt.is_empty() {
            continue;
        }
        if prompt.eq_ignore_ascii_case("/exit") {
            break;
        }
        if prompt.eq_ignore_ascii_case("/help") {
            eprintln!("agent.interactive.commands /help /session /reset /abort [run_id] /exit");
            std::io::stderr().flush().context("stderr flush failed")?;
            continue;
        }
        if prompt.eq_ignore_ascii_case("/session") {
            let resolved_session = ensure_interactive_session(
                &runtime,
                &mut session,
                initial_session_id.as_ref(),
                initial_session_key.as_ref(),
                initial_session_label.as_ref(),
                require_existing,
                false,
            )
            .await?;
            eprintln!(
                "agent.interactive.session key={} label={} updated_at_unix_ms={} last_run_id={}",
                redacted_text_presence(resolved_session.session_key.as_str()),
                redacted_text_presence(resolved_session.session_label.as_str()),
                resolved_session.updated_at_unix_ms,
                redacted_identifier_presence(resolved_session.last_run_id.as_ref())
            );
            std::io::stderr().flush().context("stderr flush failed")?;
            continue;
        }
        if prompt.eq_ignore_ascii_case("/reset") {
            let _ = ensure_interactive_session(
                &runtime,
                &mut session,
                initial_session_id.as_ref(),
                initial_session_key.as_ref(),
                initial_session_label.as_ref(),
                require_existing,
                true,
            )
            .await?;
            eprintln!("agent.interactive.reset session_reset=true");
            std::io::stderr().flush().context("stderr flush failed")?;
            continue;
        }
        if let Some(run_id) = prompt.strip_prefix("/abort").map(str::trim) {
            let run_id = if run_id.is_empty() {
                last_run_id
                    .clone()
                    .context("/abort without explicit run_id requires a previous run")?
            } else {
                resolve_or_generate_canonical_id(Some(run_id.to_owned()))?
            };
            let response =
                runtime.abort_run(run_id.clone(), Some("interactive_abort".to_owned())).await?;
            eprintln!(
                "agent.interactive.abort run_id={} cancel_requested={} reason={}",
                if response.run_id.is_some() || !run_id.is_empty() { REDACTED } else { "none" },
                response.cancel_requested,
                redacted_text_presence(response.reason.as_str())
            );
            std::io::stderr().flush().context("stderr flush failed")?;
            continue;
        }

        let resolved_session = ensure_interactive_session(
            &runtime,
            &mut session,
            initial_session_id.as_ref(),
            initial_session_key.as_ref(),
            initial_session_label.as_ref(),
            require_existing,
            false,
        )
        .await?;
        let request = build_agent_run_input(AgentRunInputArgs {
            session_id: resolved_session.session_id.clone(),
            session_key: None,
            session_label: None,
            require_existing: true,
            reset_session: false,
            run_id: None,
            prompt: prompt.to_owned(),
            allow_sensitive_tools,
            origin_kind: None,
            origin_run_id: None,
            parameter_delta_json: None,
        })?;
        last_run_id = Some(request.run_id.clone());
        execute_agent_stream(connection.clone(), request, ndjson)?;
    }
    Ok(())
}

fn redacted_identifier_presence(value: Option<&common_v1::CanonicalId>) -> &'static str {
    if value.is_some() {
        REDACTED
    } else {
        "none"
    }
}

fn redacted_text_presence(value: &str) -> &'static str {
    if value.trim().is_empty() {
        "none"
    } else {
        REDACTED
    }
}

async fn ensure_interactive_session(
    runtime: &client::operator::OperatorRuntime,
    session: &mut Option<gateway_v1::SessionSummary>,
    initial_session_id: Option<&String>,
    initial_session_key: Option<&String>,
    initial_session_label: Option<&String>,
    require_existing: bool,
    reset_session: bool,
) -> Result<gateway_v1::SessionSummary> {
    let request = if let Some(existing_session) = session.as_ref() {
        SessionResolveInput {
            session_id: existing_session.session_id.clone(),
            session_key: String::new(),
            session_label: String::new(),
            require_existing: true,
            reset_session,
        }
    } else {
        SessionResolveInput {
            session_id: resolve_optional_canonical_id(initial_session_id.cloned())?,
            session_key: initial_session_key.cloned().unwrap_or_default(),
            session_label: initial_session_label.cloned().unwrap_or_default(),
            require_existing,
            reset_session,
        }
    };
    let resolved_session = runtime
        .resolve_session(request)
        .await?
        .session
        .context("ResolveSession returned empty session payload")?;
    *session = Some(resolved_session.clone());
    Ok(resolved_session)
}

#[cfg(test)]
mod tests {
    use super::interactive_session_started_message;
    use crate::proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1};

    #[test]
    fn interactive_session_started_message_omits_session_identifier() {
        let banner = interactive_session_started_message(
            Some(&gateway_v1::SessionSummary {
                session_id: Some(common_v1::CanonicalId {
                    ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
                }),
                session_key: "ops:triage".to_owned(),
                session_label: "Ops Triage".to_owned(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
                last_run_id: None,
                archived_at_unix_ms: 0,
                ..Default::default()
            }),
            None,
            None,
        );
        assert!(banner.contains("agent.interactive=session_started"));
        assert!(banner.contains("exit_hint=/exit"));
        assert!(banner.contains("session_key=ops:triage"));
        assert!(!banner.contains("session_id="));
    }
}
