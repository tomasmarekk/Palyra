//! `palyra agent` command: one-shot runs and the interactive terminal loop.
//!
//! Streams gateway run events to stdout, mediates tool-approval decisions, and
//! lets stdin input interrupt an active interactive run by queueing the new
//! prompt for replay once the current run stops.

use std::collections::VecDeque;

use tokio::sync::mpsc;

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

/// Dispatches `palyra agent` subcommands (run, interactive, ACP shims).
///
/// # Errors
/// Returns an error when the CLI root context is unavailable, approval flags
/// conflict, the gateway connection cannot be resolved, or the run stream
/// fails before reaching a successful terminal status.
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
            interrupt_active_run,
            approval_mode,
            ndjson,
        } => {
            ensure_agent_run_approval_flags(allow_sensitive_tools, approval_mode, prompt_stdin)?;
            let input_prompt = resolve_prompt_input(prompt, prompt_stdin)?;
            let parameter_delta_json = cli_launch_parameter_delta_json(input_prompt.as_str())?;
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
                interrupt_active_run,
                approval_mode: approval_mode.into(),
                origin_kind: None,
                origin_run_id: None,
                parameter_delta_json,
            })?;
            let run_id = request.run_id.clone();
            let outcome =
                execute_agent_stream(connection, request, output::preferred_ndjson(false, ndjson))?;
            if outcome.completed() && root_context.config_path().is_some() {
                commands::onboarding::record_cli_first_success(
                    root_context.state_root(),
                    run_id.as_str(),
                )?;
            }
            Ok(())
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
            let onboarding_state_root = root_context
                .config_path()
                .is_some()
                .then(|| root_context.state_root().to_path_buf());
            runtime.block_on(run_agent_interactive_async(
                connection,
                AgentInteractiveOptions {
                    onboarding_state_root,
                    session_id,
                    session_key,
                    session_label,
                    require_existing,
                    allow_sensitive_tools,
                    ndjson,
                },
            ))
        }
        AgentCommand::AcpShim { command } => commands::acp::run_legacy_agent_acp_shim(command),
        AgentCommand::Acp { command } => commands::acp::run_legacy_agent_acp(command),
    }
}

/// Rejects `agent run` flag combinations that would bypass or deadlock tool approval.
///
/// # Errors
/// Returns an error when `--allow-sensitive-tools` contradicts the requested
/// approval mode, or when `--approval-mode prompt` competes with
/// `--prompt-stdin` for stdin.
pub(crate) fn ensure_agent_run_approval_flags(
    allow_sensitive_tools: bool,
    approval_mode: AgentApprovalModeArg,
    prompt_stdin: bool,
) -> Result<()> {
    if allow_sensitive_tools && approval_mode == AgentApprovalModeArg::Deny {
        anyhow::bail!(
            "--allow-sensitive-tools cannot be combined with --approval-mode deny; remove --allow-sensitive-tools to deny sensitive tool requests, or use --approval-mode allow-once after reviewing the requested tool risk"
        );
    }
    if allow_sensitive_tools && approval_mode == AgentApprovalModeArg::Prompt {
        anyhow::bail!(
            "--allow-sensitive-tools cannot be combined with --approval-mode prompt because it auto-approves sensitive tool requests before an interactive prompt can be shown; remove --allow-sensitive-tools for manual approval prompts, or use --approval-mode allow-once after reviewing the requested tool risk"
        );
    }
    if prompt_stdin && approval_mode == AgentApprovalModeArg::Prompt {
        anyhow::bail!(
            "--approval-mode prompt cannot be combined with --prompt-stdin because stdin is consumed by the task prompt and cannot also receive tool approval decisions; use --approval-mode allow-once for unattended CLI runs, pass the task with --prompt from an interactive terminal, or use `palyra agent interactive`"
        );
    }
    Ok(())
}

struct AgentInteractiveOptions {
    onboarding_state_root: Option<std::path::PathBuf>,
    session_id: Option<String>,
    session_key: Option<String>,
    session_label: Option<String>,
    require_existing: bool,
    allow_sensitive_tools: bool,
    ndjson: bool,
}

async fn run_agent_interactive_async(
    connection: AgentConnection,
    options: AgentInteractiveOptions,
) -> Result<()> {
    let AgentInteractiveOptions {
        onboarding_state_root,
        session_id,
        session_key,
        session_label,
        require_existing,
        allow_sensitive_tools,
        ndjson,
    } = options;
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

    let mut last_run_id = None::<String>;
    let mut input_rx = spawn_interactive_stdin_reader();
    let mut pending_prompts = VecDeque::<String>::new();
    loop {
        let prompt = match pending_prompts.pop_front() {
            Some(prompt) => prompt,
            None => match read_next_interactive_prompt(&mut input_rx).await? {
                Some(prompt) => prompt,
                None => break,
            },
        };
        let prompt = prompt.as_str();
        if prompt.eq_ignore_ascii_case("/exit") {
            break;
        }
        if prompt.eq_ignore_ascii_case("/help") {
            eprintln!(
                "agent.interactive.commands /help /session /reset /abort [run_id] /exit | context refs: @file:PATH @folder:PATH @diff[:PATH] @staged[:PATH] @url:URL @memory:\"query\" escape with @@"
            );
            std::io::stderr().flush().context("stderr flush failed")?;
            continue;
        }
        if prompt.eq_ignore_ascii_case("/session") {
            let resolved_session = ensure_interactive_session(
                &runtime,
                &mut session,
                initial_session_id.as_deref(),
                initial_session_key.as_deref(),
                initial_session_label.as_deref(),
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
                initial_session_id.as_deref(),
                initial_session_key.as_deref(),
                initial_session_label.as_deref(),
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
            initial_session_id.as_deref(),
            initial_session_key.as_deref(),
            initial_session_label.as_deref(),
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
            interrupt_active_run: false,
            approval_mode: AgentApprovalMode::Prompt,
            origin_kind: None,
            origin_run_id: None,
            parameter_delta_json: cli_launch_parameter_delta_json(prompt)?,
        })?;
        last_run_id = Some(request.run_id.clone());
        let run_id = request.run_id.clone();
        let outcome = execute_interactive_agent_stream(
            &runtime,
            request,
            ndjson,
            &mut input_rx,
            &mut pending_prompts,
        )
        .await?;
        if let Some(state_root) =
            onboarding_state_root.as_ref().filter(|_| outcome.stream.completed())
        {
            commands::onboarding::record_cli_first_success(state_root.as_path(), run_id.as_str())?;
        }
        if outcome.exit_requested {
            break;
        }
    }
    Ok(())
}

// Allowlist of path-only env vars safe to forward in the launch context;
// anything else (tokens, credentials) must never be serialized into it.
const CLI_CONTEXT_SAFE_PATH_ENV_KEYS: &[&str] = &["PALYRA_E2E_HOME", "PALYRA_E2E_OS_ROOT"];

fn cli_launch_parameter_delta_json(prompt: &str) -> Result<Option<String>> {
    let cwd = std::env::current_dir().context("failed to resolve CLI current working directory")?;
    cli_launch_parameter_delta_json_for_cwd(cwd.as_path(), prompt)
}

// The prompt is accepted but deliberately unused: workspace roots must never
// be inferred from prompt contents (pinned by the tests below).
fn cli_launch_parameter_delta_json_for_cwd(
    cwd: &std::path::Path,
    _prompt: &str,
) -> Result<Option<String>> {
    let parameter_delta = serde_json::json!({
        "cli_context": {
            "launch_cwd": cwd.to_string_lossy(),
            "workspace_roots": Vec::<String>::new(),
            "env": cli_launch_safe_path_env(),
        }
    });
    serde_json::to_string(&parameter_delta)
        .map(Some)
        .context("failed to serialize CLI launch context")
}

fn cli_launch_safe_path_env() -> serde_json::Map<String, serde_json::Value> {
    let mut env = serde_json::Map::new();
    for key in CLI_CONTEXT_SAFE_PATH_ENV_KEYS {
        if let Some(value) = std::env::var_os(key).filter(|value| !value.is_empty()) {
            env.insert((*key).to_owned(), value.to_string_lossy().into_owned().into());
        }
    }
    env
}

// Stdin reads block, so they run on a dedicated thread; the async loop can
// then select between user input and run-stream events without stalling.
fn spawn_interactive_stdin_reader() -> mpsc::UnboundedReceiver<Result<String, String>> {
    let (tx, rx) = mpsc::unbounded_channel();
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            let line = line.map_err(|error| error.to_string());
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    rx
}

fn normalize_interactive_prompt_line(line: &str) -> Option<String> {
    let prompt = line.trim();
    (!prompt.is_empty()).then(|| prompt.to_owned())
}

async fn read_next_interactive_prompt(
    input_rx: &mut mpsc::UnboundedReceiver<Result<String, String>>,
) -> Result<Option<String>> {
    while let Some(line) = input_rx.recv().await {
        let line =
            line.map_err(|error| anyhow!("failed to read interactive prompt from stdin: {error}"))?;
        if let Some(prompt) = normalize_interactive_prompt_line(line.as_str()) {
            return Ok(Some(prompt));
        }
    }
    Ok(None)
}

fn interactive_interrupt_message(cancel_requested: bool, queued_prompt_count: usize) -> String {
    format!(
        "agent.interactive.interrupt run_id={} cancel_requested={} queued_prompt_count={} reason=interactive_interrupt",
        REDACTED, cancel_requested, queued_prompt_count
    )
}

fn interactive_queue_message(queued_prompt_count: usize) -> String {
    format!(
        "agent.interactive.queue active_run_cancel_pending=true queued_prompt_count={queued_prompt_count}"
    )
}

fn interactive_exit_after_interrupt_message(cancel_requested: bool) -> String {
    format!(
        "agent.interactive.exit active_run_cancel_requested={} exit_after_current_run=true",
        cancel_requested
    )
}

/// Stream outcome plus whether the user requested `/exit` during the run.
struct InteractiveAgentStreamOutcome {
    stream: AgentStreamOutcome,
    exit_requested: bool,
}

async fn execute_interactive_agent_stream(
    runtime: &client::operator::OperatorRuntime,
    request: AgentRunInput,
    ndjson: bool,
    input_rx: &mut mpsc::UnboundedReceiver<Result<String, String>>,
    pending_prompts: &mut VecDeque<String>,
) -> Result<InteractiveAgentStreamOutcome> {
    let run_id = request.run_id.clone();
    let mut stream = runtime.start_run_stream(request).await.map_err(|error| {
        enrich_agent_principal_auth_error(error, runtime.connection().principal.as_str())
    })?;
    let mut text_emitter = AgentTextEmitter::default();
    let mut failed_message = None::<String>;
    let mut needs_continuation_message = None::<String>;
    let mut completed = false;
    let mut cancelled = false;
    let mut saw_terminal_status = false;
    let mut approval_mode = AgentApprovalMode::Prompt;
    let mut abort_requested = false;
    let mut exit_requested = false;
    let mut input_closed = false;

    loop {
        // Stdin stays live during an active run: slash commands act
        // immediately, while any other input interrupts the run and queues the
        // prompt for replay once the stream stops.
        tokio::select! {
            maybe_prompt = read_next_interactive_prompt(input_rx), if !input_closed => {
                match maybe_prompt? {
                    Some(prompt) => {
                        if prompt.eq_ignore_ascii_case("/help") {
                            eprintln!(
                                "agent.interactive.commands /help /session /reset /abort [run_id] /exit | active run input interrupts the current run and queues the new prompt"
                            );
                            std::io::stderr().flush().context("stderr flush failed")?;
                            continue;
                        }
                        if prompt.eq_ignore_ascii_case("/session") {
                            eprintln!(
                                "agent.interactive.session active_run=true run_id={} queued_prompt_count={}",
                                REDACTED,
                                pending_prompts.len()
                            );
                            std::io::stderr().flush().context("stderr flush failed")?;
                            continue;
                        }
                        if prompt.eq_ignore_ascii_case("/reset") {
                            eprintln!("agent.interactive.reset active_run=true deferred=false message=reset_requires_idle_session");
                            std::io::stderr().flush().context("stderr flush failed")?;
                            continue;
                        }
                        if let Some(target_run_id) = prompt.strip_prefix("/abort").map(str::trim) {
                            let target_run_id = if target_run_id.is_empty() {
                                run_id.clone()
                            } else {
                                resolve_or_generate_canonical_id(Some(target_run_id.to_owned()))?
                            };
                            let response = runtime
                                .abort_run(target_run_id, Some("interactive_abort".to_owned()))
                                .await?;
                            if response.run_id.as_ref().is_some_and(|value| value.ulid == run_id) {
                                abort_requested = true;
                            }
                            eprintln!(
                                "agent.interactive.abort run_id={} cancel_requested={} reason={}",
                                REDACTED,
                                response.cancel_requested,
                                redacted_text_presence(response.reason.as_str())
                            );
                            std::io::stderr().flush().context("stderr flush failed")?;
                            continue;
                        }
                        if prompt.eq_ignore_ascii_case("/exit") {
                            let response = runtime
                                .abort_run(run_id.clone(), Some("interactive_exit".to_owned()))
                                .await?;
                            abort_requested = true;
                            exit_requested = true;
                            eprintln!("{}", interactive_exit_after_interrupt_message(response.cancel_requested));
                            std::io::stderr().flush().context("stderr flush failed")?;
                            continue;
                        }

                        pending_prompts.push_back(prompt);
                        if abort_requested {
                            eprintln!("{}", interactive_queue_message(pending_prompts.len()));
                        } else {
                            let response = runtime
                                .abort_run(run_id.clone(), Some("interactive_interrupt".to_owned()))
                                .await?;
                            abort_requested = true;
                            eprintln!(
                                "{}",
                                interactive_interrupt_message(
                                    response.cancel_requested,
                                    pending_prompts.len()
                                )
                            );
                        }
                        std::io::stderr().flush().context("stderr flush failed")?;
                    }
                    None => {
                        input_closed = true;
                    }
                }
            }
            maybe_event = stream.next_event() => {
                let Some(event) = maybe_event? else {
                    break;
                };
                let reached_terminal_status = matches!(
                    event.body.as_ref(),
                    Some(common_v1::run_stream_event::Body::Status(status))
                        if is_terminal_stream_status(status.kind)
                );
                if let Some(common_v1::run_stream_event::Body::Status(status)) = event.body.as_ref() {
                    if status.kind == common_v1::stream_status::StatusKind::Failed as i32 {
                        let message = sanitize_agent_failure_message(status.message.as_str());
                        if is_agent_cancellation_message(message.as_str()) {
                            cancelled = true;
                        } else if is_agent_needs_continuation_message(message.as_str()) {
                            needs_continuation_message = Some(message);
                        } else {
                            failed_message = Some(message);
                        }
                    } else if status.kind == common_v1::stream_status::StatusKind::Done as i32 {
                        completed = true;
                    }
                }
                if ndjson {
                    emit_acp_event_ndjson(&event)?;
                } else {
                    text_emitter.emit(&event)?;
                }
                std::io::stdout().flush().context("stdout flush failed")?;
                if let Some(common_v1::run_stream_event::Body::ToolApprovalRequest(approval)) =
                    event.body.as_ref()
                {
                    let decision = prompt_tool_approval_decision_from_interactive_input(
                        approval,
                        &mut approval_mode,
                        input_rx,
                    )
                    .await?;
                    stream.send_tool_approval_decision(
                        approval,
                        decision.approved,
                        decision.reason,
                        common_v1::ApprovalDecisionScope::Once as i32,
                        0,
                    )?;
                }
                if reached_terminal_status {
                    saw_terminal_status = true;
                    break;
                }
            }
        }
    }

    if !ndjson {
        text_emitter.finish()?;
    }
    let outcome = AgentStreamOutcome {
        completed,
        cancelled,
        needs_continuation_message,
        failed_message,
        continuation_request: None,
    };
    // An interrupted run is expected to end without a successful terminal
    // status; the queued prompt (or exit) decides what happens next, so the
    // terminal-status and success checks are intentionally skipped.
    if abort_requested {
        return Ok(InteractiveAgentStreamOutcome { stream: outcome, exit_requested });
    }
    if !saw_terminal_status {
        anyhow::bail!(
            "agent run stream ended before a terminal status was received; the run may still be in progress"
        );
    }
    outcome.ensure_success()?;
    Ok(InteractiveAgentStreamOutcome { stream: outcome, exit_requested })
}

async fn prompt_tool_approval_decision_from_interactive_input(
    approval: &common_v1::ToolApprovalRequest,
    mode: &mut AgentApprovalMode,
    input_rx: &mut mpsc::UnboundedReceiver<Result<String, String>>,
) -> Result<ToolApprovalDecision> {
    match *mode {
        AgentApprovalMode::Prompt => {}
        AgentApprovalMode::Deny | AgentApprovalMode::AllowOnce => {
            return prompt_tool_approval_decision_with_mode_state(approval, mode);
        }
    }

    if !std::io::stdin().is_terminal() {
        eprintln!("agent.approval.non_interactive_hint {}", approval_required_cli_hint());
        return Ok(ToolApprovalDecision {
            approved: false,
            reason: "approval_required_non_interactive_cli".to_owned(),
        });
    }

    eprintln!("{}", tool_approval_prompt_line(approval));
    eprint!("{}", tool_approval_terminal_prompt_text());
    std::io::stderr().flush().context("stderr flush failed")?;
    let input = read_next_interactive_prompt(input_rx).await?.unwrap_or_default();
    let approved = matches!(input.trim().to_ascii_lowercase().as_str(), "y" | "yes");
    Ok(ToolApprovalDecision {
        approved,
        reason: if approved {
            "approved_by_cli_terminal".to_owned()
        } else {
            "denied_by_cli_terminal".to_owned()
        },
    })
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
    initial_session_id: Option<&str>,
    initial_session_key: Option<&str>,
    initial_session_label: Option<&str>,
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
            session_id: resolve_optional_canonical_id(initial_session_id.map(str::to_owned))?,
            session_key: initial_session_key.map(str::to_owned).unwrap_or_default(),
            session_label: initial_session_label.map(str::to_owned).unwrap_or_default(),
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
    use super::{
        cli_launch_parameter_delta_json_for_cwd, ensure_agent_run_approval_flags,
        interactive_interrupt_message, interactive_session_started_message,
        normalize_interactive_prompt_line,
    };
    use crate::args::AgentApprovalModeArg;
    use crate::proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1};
    use serde_json::Value;
    use std::{
        ffi::OsString,
        fs,
        sync::{Mutex, OnceLock},
    };

    static CLI_CONTEXT_ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    struct ScopedEnvVar {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl ScopedEnvVar {
        fn set(key: &'static str, value: impl Into<OsString>) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value.into());
            Self { key, previous }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            match self.previous.as_ref() {
                Some(previous) => std::env::set_var(self.key, previous),
                None => std::env::remove_var(self.key),
            }
        }
    }

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

    #[test]
    fn interactive_prompt_lines_trim_and_skip_empty_input() {
        assert_eq!(
            normalize_interactive_prompt_line("  redirect active run  "),
            Some("redirect active run".to_owned())
        );
        assert_eq!(normalize_interactive_prompt_line(" \t "), None);
    }

    #[test]
    fn interactive_interrupt_message_is_visible_and_redacted() {
        let message = interactive_interrupt_message(true, 2);

        assert!(message.contains("agent.interactive.interrupt"));
        assert!(message.contains("cancel_requested=true"));
        assert!(message.contains("queued_prompt_count=2"));
        assert!(message.contains("run_id=<redacted>"));
        assert!(!message.contains("redirect active run"));
    }

    #[test]
    fn allow_sensitive_tools_conflicts_with_approval_deny() {
        let error = ensure_agent_run_approval_flags(true, AgentApprovalModeArg::Deny, false)
            .expect_err("contradictory approval flags should fail before a run starts");

        assert!(error.to_string().contains("--allow-sensitive-tools"), "{error}");
        assert!(error.to_string().contains("--approval-mode deny"), "{error}");
    }

    #[test]
    fn allow_sensitive_tools_conflicts_with_approval_prompt() {
        let error = ensure_agent_run_approval_flags(true, AgentApprovalModeArg::Prompt, false)
            .expect_err("prompt approval mode must not be silently bypassed by auto-approval");

        assert!(error.to_string().contains("--allow-sensitive-tools"), "{error}");
        assert!(error.to_string().contains("--approval-mode prompt"), "{error}");
        assert!(error.to_string().contains("manual approval prompts"), "{error}");
    }

    #[test]
    fn allow_sensitive_tools_can_pair_with_allow_once() {
        ensure_agent_run_approval_flags(true, AgentApprovalModeArg::AllowOnce, false)
            .expect("allow-once remains valid with explicit sensitive-tool opt-in");
    }

    #[test]
    fn prompt_approval_mode_rejects_prompt_stdin() {
        let error = ensure_agent_run_approval_flags(false, AgentApprovalModeArg::Prompt, true)
            .expect_err("prompt approval mode needs stdin for approval decisions");

        assert!(error.to_string().contains("--approval-mode prompt"), "{error}");
        assert!(error.to_string().contains("--prompt-stdin"), "{error}");
        assert!(error.to_string().contains("palyra agent interactive"), "{error}");
    }

    #[test]
    fn cli_launch_context_encodes_current_working_directory() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let parameter_delta = cli_launch_parameter_delta_json_for_cwd(tempdir.path(), "")
            .expect("launch context should serialize")
            .expect("launch context should be present");
        let value =
            serde_json::from_str::<Value>(parameter_delta.as_str()).expect("JSON should parse");

        assert_eq!(
            value
                .get("cli_context")
                .and_then(|context| context.get("launch_cwd"))
                .and_then(Value::as_str),
            Some(tempdir.path().to_string_lossy().as_ref())
        );
    }

    #[test]
    fn cli_launch_context_does_not_infer_workspace_roots_from_prompt_paths() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let project = tempdir.path().join("todo-app");
        fs::create_dir_all(project.as_path()).expect("project directory should exist");
        let prompt = format!("In project `{}` create a Todo app.", project.display());
        let parameter_delta =
            cli_launch_parameter_delta_json_for_cwd(tempdir.path(), prompt.as_str())
                .expect("launch context should serialize")
                .expect("launch context should be present");
        let value =
            serde_json::from_str::<Value>(parameter_delta.as_str()).expect("JSON should parse");
        let roots = value
            .get("cli_context")
            .and_then(|context| context.get("workspace_roots"))
            .and_then(Value::as_array)
            .expect("workspace roots should be encoded");

        assert!(
            roots.is_empty(),
            "harness must not infer workspace roots from prompt paths: {roots:?}"
        );
    }

    #[test]
    fn cli_launch_context_exports_only_safe_path_env_values() {
        let _guard = CLI_CONTEXT_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("CLI context env lock should not be poisoned");
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let e2e_home = tempdir.path().join("home");
        let e2e_os_root = tempdir.path().join("os-root");
        let _home = ScopedEnvVar::set("PALYRA_E2E_HOME", e2e_home.as_os_str());
        let _os_root = ScopedEnvVar::set("PALYRA_E2E_OS_ROOT", e2e_os_root.as_os_str());
        let _admin_token = ScopedEnvVar::set("PALYRA_ADMIN_TOKEN", "secret");
        let parameter_delta = cli_launch_parameter_delta_json_for_cwd(tempdir.path(), "")
            .expect("launch context should serialize")
            .expect("launch context should be present");
        let value =
            serde_json::from_str::<Value>(parameter_delta.as_str()).expect("JSON should parse");

        let env = value
            .get("cli_context")
            .and_then(|context| context.get("env"))
            .and_then(Value::as_object)
            .expect("safe env should be encoded");

        assert_eq!(
            env.get("PALYRA_E2E_HOME").and_then(Value::as_str),
            Some(e2e_home.to_string_lossy().as_ref())
        );
        assert_eq!(
            env.get("PALYRA_E2E_OS_ROOT").and_then(Value::as_str),
            Some(e2e_os_root.to_string_lossy().as_ref())
        );
        assert!(
            !env.contains_key("PALYRA_ADMIN_TOKEN"),
            "runtime tokens must not be serialized into launch context"
        );
    }
}
