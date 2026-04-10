use std::{
    io::{self, Stdout},
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use crossterm::{
    event::{self, Event as CEvent, KeyCode, KeyEvent, KeyModifiers},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
    Frame, Terminal,
};

use crate::{
    client::operator::{ManagedRunStream, OperatorRuntime},
    commands::models::ModelsListPayload,
    *,
};

#[derive(Clone)]
pub(crate) struct LaunchOptions {
    pub(crate) connection: AgentConnection,
    pub(crate) session_id: Option<common_v1::CanonicalId>,
    pub(crate) session_key: Option<String>,
    pub(crate) session_label: Option<String>,
    pub(crate) require_existing: bool,
    pub(crate) allow_sensitive_tools: bool,
    pub(crate) include_archived_sessions: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Focus {
    Transcript,
    Input,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Chat,
    Help,
    Picker(PickerKind),
    Settings,
    Approval,
    ShellConfirm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PickerKind {
    Agent,
    Session,
    Model,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettingsItem {
    ShowTools,
    ShowThinking,
    LocalShell,
}

#[derive(Debug, Clone)]
struct PickerItem {
    id: String,
    title: String,
    detail: String,
}

#[derive(Debug, Clone)]
struct PickerState {
    kind: PickerKind,
    title: String,
    items: Vec<PickerItem>,
    selected: usize,
}

#[derive(Debug, Clone)]
enum EntryKind {
    User,
    Assistant,
    Tool,
    Thinking,
    System,
    Shell,
}

#[derive(Debug, Clone)]
struct TranscriptEntry {
    kind: EntryKind,
    title: String,
    body: String,
}

#[derive(Debug, Clone)]
struct ShellResult {
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
}

struct App {
    runtime: OperatorRuntime,
    session: gateway_v1::SessionSummary,
    current_agent: Option<gateway_v1::Agent>,
    current_agent_source: &'static str,
    models: Option<ModelsListPayload>,
    input: String,
    transcript: Vec<TranscriptEntry>,
    active_stream: Option<ManagedRunStream>,
    pending_approval: Option<common_v1::ToolApprovalRequest>,
    pending_shell_command: Option<String>,
    pending_picker: Option<PickerState>,
    focus: Focus,
    mode: Mode,
    show_tools: bool,
    show_thinking: bool,
    local_shell_enabled: bool,
    allow_sensitive_tools: bool,
    include_archived_sessions: bool,
    last_run_id: Option<String>,
    selected_objective_id: Option<String>,
    scroll_offset: u16,
    status_line: String,
    settings_selected: usize,
}

const BUILT_IN_DELEGATION_PROFILES: &[&str] =
    &["research", "synthesis", "review", "patching", "triage"];
const BUILT_IN_DELEGATION_TEMPLATES: &[&str] =
    &["compare_variants", "research_then_synthesize", "review_and_patch", "multi_source_triage"];

pub(crate) fn run(options: LaunchOptions) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(async move {
        let mut app = App::bootstrap(options).await?;
        let mut terminal = setup_terminal()?;
        let result = run_loop(&mut terminal, &mut app).await;
        restore_terminal(&mut terminal)?;
        result
    })
}

async fn run_loop(terminal: &mut Terminal<CrosstermBackend<Stdout>>, app: &mut App) -> Result<()> {
    loop {
        app.drain_stream_events().await?;
        terminal.draw(|frame| render(frame, app))?;
        if app.should_exit() {
            return Ok(());
        }
        if event::poll(Duration::from_millis(50)).context("failed to poll terminal events")? {
            let event = event::read().context("failed to read terminal event")?;
            if let CEvent::Key(key) = event {
                if handle_key(app, key).await? {
                    return Ok(());
                }
            }
        }
    }
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode().context("failed to enable raw terminal mode")?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen).context("failed to enter alternate screen")?;
    Terminal::new(CrosstermBackend::new(stdout)).context("failed to initialize terminal backend")
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
    disable_raw_mode().context("failed to disable raw terminal mode")?;
    terminal
        .backend_mut()
        .execute(LeaveAlternateScreen)
        .context("failed to leave alternate screen")?;
    terminal.show_cursor().context("failed to restore terminal cursor")
}

impl App {
    async fn bootstrap(options: LaunchOptions) -> Result<Self> {
        let runtime = OperatorRuntime::new(options.connection.clone());
        let response = runtime
            .resolve_session(SessionResolveInput {
                session_id: options.session_id,
                session_key: options.session_key.unwrap_or_default(),
                session_label: options.session_label.unwrap_or_default(),
                require_existing: options.require_existing,
                reset_session: false,
            })
            .await?;
        let session = response
            .session
            .context("ResolveSession returned empty session payload for tui bootstrap")?;
        let mut app = Self {
            runtime,
            session,
            current_agent: None,
            current_agent_source: "unresolved",
            models: None,
            input: String::new(),
            transcript: Vec::new(),
            active_stream: None,
            pending_approval: None,
            pending_shell_command: None,
            pending_picker: None,
            focus: Focus::Input,
            mode: Mode::Chat,
            show_tools: true,
            show_thinking: true,
            local_shell_enabled: false,
            allow_sensitive_tools: options.allow_sensitive_tools,
            include_archived_sessions: options.include_archived_sessions,
            last_run_id: None,
            selected_objective_id: None,
            scroll_offset: 0,
            status_line: "Connected".to_owned(),
            settings_selected: 0,
        };
        app.refresh_agent_identity(None, false).await?;
        match app.runtime.list_models(None) {
            Ok(models) => app.models = Some(models),
            Err(error) => {
                app.status_line = sanitize_terminal_text(
                    format!("Connected; model catalog unavailable: {error}").as_str(),
                )
            }
        }
        app.push_entry(EntryKind::System, "Session", "Connected.");
        Ok(app)
    }

    fn should_exit(&self) -> bool {
        matches!(self.mode, Mode::Chat) && self.status_line == "__exit__"
    }

    async fn drain_stream_events(&mut self) -> Result<()> {
        loop {
            let next = {
                let Some(stream) = self.active_stream.as_mut() else {
                    break;
                };
                tokio::time::timeout(Duration::from_millis(1), stream.next_event()).await
            };
            match next {
                Ok(Ok(Some(event))) => self.handle_stream_event(event)?,
                Ok(Ok(None)) => {
                    self.active_stream = None;
                    self.status_line = "Run completed".to_owned();
                    break;
                }
                Ok(Err(error)) => {
                    self.active_stream = None;
                    self.status_line =
                        sanitize_terminal_text(format!("Run failed: {error}").as_str());
                    self.push_entry(EntryKind::System, "Run error", error.to_string());
                    break;
                }
                Err(_) => break,
            }
        }
        Ok(())
    }

    fn handle_stream_event(&mut self, event: common_v1::RunStreamEvent) -> Result<()> {
        let run_id =
            event.run_id.as_ref().map(|value| value.ulid.as_str()).unwrap_or("unknown").to_owned();
        match event.body {
            Some(common_v1::run_stream_event::Body::ModelToken(token)) => {
                self.append_assistant_token(run_id.as_str(), token.token.as_str());
                if token.is_final {
                    self.status_line = "Assistant response completed".to_owned();
                }
            }
            Some(common_v1::run_stream_event::Body::Status(status)) => {
                self.status_line = sanitize_terminal_text(status.message.as_str());
                if self.show_thinking {
                    self.push_entry(
                        EntryKind::Thinking,
                        format!("Status ({})", stream_status_kind_to_text(status.kind)),
                        status.message,
                    );
                }
            }
            Some(common_v1::run_stream_event::Body::ToolProposal(proposal)) => {
                if self.show_tools {
                    self.push_entry(
                        EntryKind::Tool,
                        format!("Tool proposal: {}", proposal.tool_name),
                        format!(
                            "proposal_id={} approval_required={}",
                            proposal
                                .proposal_id
                                .as_ref()
                                .map(|value| value.ulid.as_str())
                                .unwrap_or("unknown"),
                            proposal.approval_required
                        ),
                    );
                }
            }
            Some(common_v1::run_stream_event::Body::ToolDecision(decision)) => {
                if self.show_tools {
                    self.push_entry(
                        EntryKind::Tool,
                        format!("Tool decision: {}", tool_decision_kind_to_text(decision.kind)),
                        decision.reason,
                    );
                }
            }
            Some(common_v1::run_stream_event::Body::ToolApprovalRequest(mut approval)) => {
                approval.tool_name = sanitize_terminal_text(approval.tool_name.as_str());
                approval.request_summary =
                    sanitize_terminal_text(approval.request_summary.as_str());
                self.status_line = sanitize_terminal_text(
                    format!(
                        "Approval required for {}",
                        if approval.tool_name.trim().is_empty() {
                            "tool"
                        } else {
                            approval.tool_name.as_str()
                        }
                    )
                    .as_str(),
                );
                if self.show_tools {
                    self.push_entry(
                        EntryKind::Tool,
                        format!("Approval requested: {}", approval.tool_name),
                        approval.request_summary.clone(),
                    );
                }
                self.pending_approval = Some(approval);
                self.mode = Mode::Approval;
            }
            Some(common_v1::run_stream_event::Body::ToolApprovalResponse(response)) => {
                if self.show_tools {
                    self.push_entry(
                        EntryKind::Tool,
                        "Approval response",
                        format!(
                            "approved={} scope={} reason={}",
                            response.approved,
                            approval_scope_to_text(response.decision_scope),
                            response.reason
                        ),
                    );
                }
            }
            Some(common_v1::run_stream_event::Body::ToolResult(result)) => {
                if self.show_tools {
                    self.push_entry(
                        EntryKind::Tool,
                        format!(
                            "Tool result ({})",
                            if result.success { "success" } else { "error" }
                        ),
                        if result.error.trim().is_empty() {
                            truncate_text(
                                String::from_utf8_lossy(result.output_json.as_slice()).to_string(),
                                600,
                            )
                        } else {
                            result.error
                        },
                    );
                }
            }
            Some(common_v1::run_stream_event::Body::ToolAttestation(attestation)) => {
                if self.show_tools {
                    self.push_entry(
                        EntryKind::Tool,
                        "Tool attestation",
                        format!(
                            "executor={} timed_out={} proposal_id={}",
                            attestation.executor,
                            attestation.timed_out,
                            attestation
                                .proposal_id
                                .as_ref()
                                .map(|value| value.ulid.as_str())
                                .unwrap_or("unknown")
                        ),
                    );
                }
            }
            Some(common_v1::run_stream_event::Body::A2uiUpdate(update)) => {
                self.push_entry(
                    EntryKind::System,
                    "A2UI update",
                    format!("surface={} version={}", update.surface, update.v),
                );
            }
            Some(common_v1::run_stream_event::Body::JournalEvent(journal_event)) => {
                self.push_entry(
                    EntryKind::System,
                    "Journal event",
                    format!(
                        "{} ({})",
                        journal_event.kind,
                        journal_event
                            .event_id
                            .as_ref()
                            .map(|value| value.ulid.as_str())
                            .unwrap_or("unknown")
                    ),
                );
            }
            None => {
                self.push_entry(EntryKind::System, "Event", "Received empty run-stream event");
            }
        }
        Ok(())
    }

    fn append_assistant_token(&mut self, run_id: &str, token: &str) {
        let title = format!("Assistant ({})", shorten_id(run_id));
        let token = sanitize_terminal_text(token);
        if let Some(last) = self.transcript.last_mut() {
            if matches!(last.kind, EntryKind::Assistant) && last.title == title {
                last.body.push_str(token.as_str());
                return;
            }
        }
        self.transcript.push(TranscriptEntry { kind: EntryKind::Assistant, title, body: token });
    }

    fn push_entry<T: AsRef<str>, U: AsRef<str>>(&mut self, kind: EntryKind, title: T, body: U) {
        self.transcript.push(TranscriptEntry {
            kind,
            title: sanitize_terminal_text(title.as_ref()),
            body: sanitize_terminal_text(body.as_ref()),
        });
    }

    async fn start_prompt_run(
        &mut self,
        prompt: String,
        origin_kind: Option<String>,
        origin_run_id: Option<String>,
        parameter_delta_json: Option<String>,
    ) -> Result<()> {
        let request = build_agent_run_input(AgentRunInputArgs {
            session_id: self.session.session_id.clone(),
            session_key: None,
            session_label: None,
            require_existing: true,
            reset_session: false,
            run_id: None,
            prompt,
            allow_sensitive_tools: self.allow_sensitive_tools,
            origin_kind,
            origin_run_id,
            parameter_delta_json,
        })?;
        let stream = self.runtime.start_run_stream(request).await?;
        self.last_run_id = Some(stream.run_id().to_owned());
        self.active_stream = Some(stream);
        self.scroll_offset = 0;
        Ok(())
    }

    async fn submit_input(&mut self) -> Result<()> {
        let value = self.input.trim().to_owned();
        self.input.clear();
        if value.is_empty() {
            return Ok(());
        }
        if self.active_stream.is_some() {
            self.status_line = "A run is already in progress".to_owned();
            return Ok(());
        }
        if let Some(shell_command) = value.strip_prefix('!') {
            return self.handle_shell_request(shell_command.trim().to_owned()).await;
        }
        if let Some(command) = value.strip_prefix('/') {
            return self.handle_slash_command(command).await;
        }
        self.push_entry(EntryKind::User, "You", value.clone());
        self.status_line = "Running prompt".to_owned();
        self.start_prompt_run(value, None, None, None).await
    }

    async fn handle_shell_request(&mut self, command: String) -> Result<()> {
        if command.is_empty() {
            self.status_line = "Shell command is empty".to_owned();
            return Ok(());
        }
        if !self.local_shell_enabled {
            self.pending_shell_command = Some(command);
            self.mode = Mode::ShellConfirm;
            self.status_line = "Local shell requires explicit opt-in".to_owned();
            return Ok(());
        }
        let result = run_local_shell(command.clone()).await?;
        self.push_entry(
            EntryKind::Shell,
            format!("Shell: {}", command),
            format_shell_result(&result),
        );
        self.status_line = format!(
            "Shell finished with {}",
            result.exit_code.map(|value| value.to_string()).unwrap_or_else(|| "unknown".to_owned())
        );
        Ok(())
    }

    async fn handle_slash_command(&mut self, command: &str) -> Result<()> {
        let mut parts = command.split_whitespace();
        let Some(name) = parts.next() else {
            return Ok(());
        };
        match name {
            "help" => self.mode = Mode::Help,
            "status" => {
                self.push_entry(EntryKind::System, "Status", self.status_summary());
                self.status_line = "Status refreshed".to_owned();
            }
            "new" => {
                let label = normalize_optional_text(parts.collect::<Vec<_>>().join(" "));
                self.create_session(label).await?;
            }
            "agent" => {
                if let Some(agent_id) = parts.next() {
                    self.switch_agent(agent_id.to_owned()).await?;
                } else {
                    self.open_picker(PickerKind::Agent).await?;
                }
            }
            "session" => {
                if let Some(reference) = parts.next() {
                    self.switch_session(reference.to_owned()).await?;
                } else {
                    self.open_picker(PickerKind::Session).await?;
                }
            }
            "objective" => {
                let arguments = parts.map(ToOwned::to_owned).collect::<Vec<_>>();
                self.handle_objective_command(None, arguments).await?;
            }
            "heartbeat" => {
                let arguments = parts.map(ToOwned::to_owned).collect::<Vec<_>>();
                self.handle_objective_command(Some("heartbeat"), arguments).await?;
            }
            "standing-order" => {
                let arguments = parts.map(ToOwned::to_owned).collect::<Vec<_>>();
                self.handle_objective_command(Some("standing_order"), arguments).await?;
            }
            "program" => {
                let arguments = parts.map(ToOwned::to_owned).collect::<Vec<_>>();
                self.handle_objective_command(Some("program"), arguments).await?;
            }
            "history" => {
                let query = normalize_optional_text(parts.collect::<Vec<_>>().join(" "));
                self.open_session_history_picker(query).await?;
            }
            "resume" => {
                if let Some(reference) = parts.next() {
                    self.switch_session(reference.to_owned()).await?;
                } else {
                    self.open_session_history_picker(None).await?;
                }
            }
            "model" => {
                if let Some(model_id) = parts.next() {
                    self.set_model(model_id.to_owned()).await?;
                } else {
                    self.open_picker(PickerKind::Model).await?;
                }
            }
            "reset" => self.reset_session().await?,
            "retry" => self.retry_last_turn().await?,
            "branch" => {
                let label = normalize_optional_text(parts.collect::<Vec<_>>().join(" "));
                self.branch_from_current_session(label).await?;
            }
            "queue" => {
                let queued_text = normalize_optional_text(parts.collect::<Vec<_>>().join(" "));
                self.queue_follow_up(queued_text).await?;
            }
            "delegate" => {
                let arguments = parts.map(ToOwned::to_owned).collect::<Vec<_>>();
                self.delegate_background_run(arguments).await?;
            }
            "checkpoint" => {
                let arguments = parts.map(ToOwned::to_owned).collect::<Vec<_>>();
                self.handle_checkpoint_command(arguments).await?;
            }
            "background" => {
                let arguments = parts.map(ToOwned::to_owned).collect::<Vec<_>>();
                self.handle_background_command(arguments).await?;
            }
            "abort" => {
                let explicit = parts.next().map(ToOwned::to_owned);
                self.abort_run(explicit).await?;
            }
            "usage" => {
                self.push_entry(
                    EntryKind::System,
                    "Usage",
                    "Detailed usage remains available in the web console and the `palyra usage` CLI surfaces.",
                );
                self.status_line = "Usage summary is not embedded in the TUI yet".to_owned();
            }
            "compact" => {
                let arguments = parts.map(ToOwned::to_owned).collect::<Vec<_>>();
                self.handle_compaction_command(arguments).await?;
            }
            "attach" => {
                self.push_entry(
                    EntryKind::System,
                    "Attachments",
                    "Attachment upload is currently available in the web chat composer. The TUI keeps the same `/attach` terminology but does not upload files yet.",
                );
                self.status_line = "Attachment upload is currently web-only".to_owned();
            }
            "settings" => self.mode = Mode::Settings,
            "tools" => self.show_tools = parse_toggle(parts.next(), self.show_tools)?,
            "thinking" => self.show_thinking = parse_toggle(parts.next(), self.show_thinking)?,
            "shell" => {
                let enabled = parse_toggle(parts.next(), self.local_shell_enabled)?;
                if enabled && !self.local_shell_enabled {
                    self.mode = Mode::ShellConfirm;
                    self.pending_shell_command = None;
                    self.status_line = "Confirm local shell opt-in".to_owned();
                } else {
                    self.local_shell_enabled = enabled;
                    self.status_line = if enabled {
                        "Local shell enabled".to_owned()
                    } else {
                        "Local shell disabled".to_owned()
                    };
                }
            }
            "exit" | "quit" => self.status_line = "__exit__".to_owned(),
            other => {
                self.status_line = format!("Unknown slash command: /{other}");
            }
        }
        Ok(())
    }

    async fn switch_agent(&mut self, agent_id: String) -> Result<()> {
        let response = self
            .runtime
            .resolve_agent_for_context(AgentContextResolveInput {
                principal: self.runtime.connection().principal.clone(),
                channel: self.runtime.connection().channel.clone(),
                session_id: self.session.session_id.clone(),
                preferred_agent_id: normalize_agent_id_cli(agent_id.as_str())?,
                persist_session_binding: true,
            })
            .await?;
        let agent = response
            .agent
            .context("ResolveAgentForContext returned empty agent payload for tui switch")?;
        let source = agent_resolution_source_label(response.source);
        self.current_agent = Some(agent.clone());
        self.current_agent_source = source;
        self.push_entry(
            EntryKind::System,
            "Agent",
            format!("Switched agent to {} ({source}).", agent.agent_id),
        );
        self.status_line = format!("Agent switched to {}", agent.display_name);
        self.mode = Mode::Chat;
        Ok(())
    }

    async fn switch_session(&mut self, reference: String) -> Result<()> {
        if self.active_stream.is_some() {
            self.status_line = "Cannot switch sessions while a run is active".to_owned();
            return Ok(());
        }
        let request = if looks_like_canonical_ulid(reference.as_str()) {
            SessionResolveInput {
                session_id: Some(resolve_required_canonical_id(reference)?),
                session_key: String::new(),
                session_label: String::new(),
                require_existing: true,
                reset_session: false,
            }
        } else {
            SessionResolveInput {
                session_id: None,
                session_key: reference,
                session_label: String::new(),
                require_existing: true,
                reset_session: false,
            }
        };
        let response = self.runtime.resolve_session(request).await?;
        let session = response
            .session
            .context("ResolveSession returned empty session payload for tui switch")?;
        self.session = session;
        self.transcript.clear();
        self.push_entry(EntryKind::System, "Session", "Session switched.");
        self.refresh_agent_identity(None, false).await?;
        self.status_line = "Session switched".to_owned();
        self.mode = Mode::Chat;
        Ok(())
    }

    async fn reset_session(&mut self) -> Result<()> {
        if self.active_stream.is_some() {
            self.status_line =
                "Cannot reset an active session while a run is in progress".to_owned();
            return Ok(());
        }
        let response = self
            .runtime
            .resolve_session(SessionResolveInput {
                session_id: self.session.session_id.clone(),
                session_key: String::new(),
                session_label: String::new(),
                require_existing: true,
                reset_session: true,
            })
            .await?;
        self.session = response
            .session
            .context("ResolveSession returned empty session payload for tui reset")?;
        self.transcript.clear();
        self.push_entry(EntryKind::System, "Session", "Session history reset.");
        self.refresh_agent_identity(None, false).await?;
        self.status_line = "Session reset".to_owned();
        Ok(())
    }

    async fn abort_run(&mut self, explicit_run_id: Option<String>) -> Result<()> {
        let run_id = if let Some(run_id) = explicit_run_id {
            resolve_or_generate_canonical_id(Some(run_id))?
        } else {
            self.last_run_id
                .clone()
                .context("/abort without explicit run_id requires a previous run")?
        };
        let response = self.runtime.abort_run(run_id.clone(), Some("tui_abort".to_owned())).await?;
        self.push_entry(
            EntryKind::System,
            "Abort",
            format!(
                "cancel_requested={} run_id={}",
                response.cancel_requested,
                redacted_optional_identifier_for_output(
                    response
                        .run_id
                        .as_ref()
                        .map(|value| value.ulid.as_str())
                        .or(Some(run_id.as_str())),
                )
            ),
        );
        self.status_line = "Abort requested".to_owned();
        Ok(())
    }

    async fn create_session(&mut self, session_label: Option<String>) -> Result<()> {
        if self.active_stream.is_some() {
            self.status_line = "Cannot create a new session while a run is active".to_owned();
            return Ok(());
        }
        let response = self
            .runtime
            .resolve_session(SessionResolveInput {
                session_id: None,
                session_key: String::new(),
                session_label: session_label.unwrap_or_default(),
                require_existing: false,
                reset_session: false,
            })
            .await?;
        self.session = response
            .session
            .context("ResolveSession returned empty session payload for tui create")?;
        self.transcript.clear();
        self.last_run_id = None;
        self.push_entry(EntryKind::System, "Session", "Created a new session.");
        self.refresh_agent_identity(None, false).await?;
        self.status_line = "Session created".to_owned();
        Ok(())
    }

    async fn retry_last_turn(&mut self) -> Result<()> {
        if self.active_stream.is_some() {
            self.status_line = "Cannot retry while a run is active".to_owned();
            return Ok(());
        }
        let session_id = self
            .session
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .context("active TUI session is missing a session_id")?;
        let context = client::control_plane::connect_admin_console(app::ConnectionOverrides {
            grpc_url: Some(self.runtime.connection().grpc_url.clone()),
            daemon_url: None,
            token: self.runtime.connection().token.clone(),
            principal: Some(self.runtime.connection().principal.clone()),
            device_id: Some(self.runtime.connection().device_id.clone()),
            channel: Some(self.runtime.connection().channel.clone()),
        })
        .await?;
        let payload = context
            .client
            .post_json_value(
                format!(
                    "console/v1/chat/sessions/{}/retry",
                    percent_encode_component(session_id.as_str())
                ),
                &serde_json::json!({}),
            )
            .await?;
        let prompt = payload
            .pointer("/text")
            .and_then(serde_json::Value::as_str)
            .context("retry response is missing text")?
            .to_owned();
        let origin_kind = payload
            .pointer("/origin_kind")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
        let origin_run_id = payload
            .pointer("/origin_run_id")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
        let parameter_delta_json = payload
            .pointer("/parameter_delta")
            .filter(|value| !value.is_null())
            .map(serde_json::to_string)
            .transpose()?;
        self.push_entry(EntryKind::System, "Retry", "Replaying the latest turn as a new run.");
        self.status_line = "Retrying latest turn".to_owned();
        self.start_prompt_run(prompt, origin_kind, origin_run_id, parameter_delta_json).await
    }

    async fn branch_from_current_session(&mut self, session_label: Option<String>) -> Result<()> {
        if self.active_stream.is_some() {
            self.status_line = "Cannot branch while a run is active".to_owned();
            return Ok(());
        }
        let session_id = self
            .session
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .context("active TUI session is missing a session_id")?;
        let context = client::control_plane::connect_admin_console(app::ConnectionOverrides {
            grpc_url: Some(self.runtime.connection().grpc_url.clone()),
            daemon_url: None,
            token: self.runtime.connection().token.clone(),
            principal: Some(self.runtime.connection().principal.clone()),
            device_id: Some(self.runtime.connection().device_id.clone()),
            channel: Some(self.runtime.connection().channel.clone()),
        })
        .await?;
        let payload = context
            .client
            .post_json_value(
                format!(
                    "console/v1/chat/sessions/{}/branch",
                    percent_encode_component(session_id.as_str())
                ),
                &serde_json::json!({ "session_label": session_label }),
            )
            .await?;
        let next_session_id = payload
            .pointer("/session/session_id")
            .and_then(serde_json::Value::as_str)
            .context("branch response is missing child session_id")?
            .to_owned();
        self.switch_session(next_session_id).await?;
        self.push_entry(
            EntryKind::System,
            "Branch",
            "Created a new active branch from the latest terminal run.",
        );
        self.status_line = "Branched into a new session".to_owned();
        Ok(())
    }

    async fn queue_follow_up(&mut self, queued_text: Option<String>) -> Result<()> {
        let text = match queued_text {
            Some(text) => text,
            None => {
                self.status_line = "Usage: /queue <follow-up text>".to_owned();
                return Ok(());
            }
        };
        let Some(run_id) = self.active_stream.as_ref().map(|stream| stream.run_id().to_owned())
        else {
            self.status_line = "Queued follow-up requires an active run".to_owned();
            return Ok(());
        };
        let context = client::control_plane::connect_admin_console(app::ConnectionOverrides {
            grpc_url: Some(self.runtime.connection().grpc_url.clone()),
            daemon_url: None,
            token: self.runtime.connection().token.clone(),
            principal: Some(self.runtime.connection().principal.clone()),
            device_id: Some(self.runtime.connection().device_id.clone()),
            channel: Some(self.runtime.connection().channel.clone()),
        })
        .await?;
        let payload = context
            .client
            .post_json_value(
                format!("console/v1/chat/runs/{}/queue", percent_encode_component(run_id.as_str())),
                &serde_json::json!({ "text": text }),
            )
            .await?;
        let queued_input_id = payload
            .pointer("/queued_input/queued_input_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        self.push_entry(
            EntryKind::System,
            "Queue",
            format!("Queued follow-up {} for the active run.", shorten_id(queued_input_id)),
        );
        self.status_line = "Queued follow-up recorded".to_owned();
        Ok(())
    }

    async fn connect_admin_console(&self) -> Result<client::control_plane::AdminConsoleContext> {
        client::control_plane::connect_admin_console(app::ConnectionOverrides {
            grpc_url: Some(self.runtime.connection().grpc_url.clone()),
            daemon_url: None,
            token: self.runtime.connection().token.clone(),
            principal: Some(self.runtime.connection().principal.clone()),
            device_id: Some(self.runtime.connection().device_id.clone()),
            channel: Some(self.runtime.connection().channel.clone()),
        })
        .await
    }

    fn active_session_id(&self) -> Result<String> {
        self.session
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .context("active TUI session is missing a session_id")
    }

    async fn handle_objective_command(
        &mut self,
        fixed_kind: Option<&'static str>,
        arguments: Vec<String>,
    ) -> Result<()> {
        let context = self.connect_admin_console().await?;
        let Some(command) = arguments.first().map(String::as_str) else {
            let label = fixed_kind.unwrap_or("objective");
            self.status_line =
                format!("Usage: /{label} list|show|select|fire|pause|resume|archive|create");
            return Ok(());
        };
        match command {
            "list" => {
                let payload = crate::commands::objectives::list_objectives_value(
                    &context.client,
                    None,
                    Some(10),
                    fixed_kind,
                    None,
                )
                .await?;
                let objectives = payload
                    .pointer("/objectives")
                    .and_then(serde_json::Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let mut lines = Vec::new();
                if objectives.is_empty() {
                    lines.push("No objectives found.".to_owned());
                } else {
                    for objective in objectives.iter().take(10) {
                        let objective_id = objective
                            .pointer("/objective_id")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("unknown");
                        let kind = objective
                            .pointer("/kind")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("objective");
                        let state = objective
                            .pointer("/state")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("unknown");
                        let name = objective
                            .pointer("/name")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("Untitled");
                        let focus = objective
                            .pointer("/current_focus")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("No focus recorded.");
                        lines.push(format!("{objective_id} [{kind}/{state}] {name}"));
                        lines.push(format!("  focus: {focus}"));
                    }
                }
                self.push_entry(EntryKind::System, "Objectives", lines.join("\n"));
                self.status_line = "Objective list refreshed".to_owned();
            }
            "select" => {
                let objective_id =
                    arguments.get(1).cloned().context("Usage: /objective select <objective_id>")?;
                let payload = crate::commands::objectives::get_objective_value(
                    &context.client,
                    objective_id.as_str(),
                )
                .await?;
                let resolved_id = payload
                    .pointer("/objective/objective_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(objective_id.as_str())
                    .to_owned();
                self.selected_objective_id = Some(resolved_id.clone());
                self.push_entry(
                    EntryKind::System,
                    "Objective selected",
                    format!("Selected objective {resolved_id}."),
                );
                self.status_line = format!("Objective selected: {resolved_id}");
            }
            "show" => {
                let objective_id = resolve_tui_objective_reference(
                    arguments.get(1).cloned(),
                    self.selected_objective_id.as_ref(),
                )?;
                let payload = crate::commands::objectives::get_objective_summary_value(
                    &context.client,
                    objective_id.as_str(),
                )
                .await?;
                let markdown = payload
                    .pointer("/summary_markdown")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("Objective summary is unavailable.");
                self.selected_objective_id = Some(objective_id.clone());
                self.push_entry(EntryKind::System, "Objective summary", markdown.to_owned());
                self.status_line = format!("Objective summary loaded: {objective_id}");
            }
            "fire" | "pause" | "resume" | "archive" => {
                let objective_id = resolve_tui_objective_reference(
                    arguments.get(1).cloned(),
                    self.selected_objective_id.as_ref(),
                )?;
                let payload = crate::commands::objectives::objective_lifecycle_value(
                    &context.client,
                    objective_id.as_str(),
                    command,
                    None,
                )
                .await?;
                let objective = payload.pointer("/objective").cloned().unwrap_or(payload);
                let state = objective
                    .pointer("/state")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown");
                let name = objective
                    .pointer("/name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("Untitled objective");
                self.selected_objective_id = Some(objective_id.clone());
                self.push_entry(
                    EntryKind::System,
                    "Objective lifecycle",
                    format!("{objective_id} [{state}] {name}"),
                );
                self.status_line = format!("Objective {command}: {objective_id}");
            }
            "create" => {
                let spec = parse_tui_objective_create_spec(fixed_kind, arguments.as_slice())?;
                let payload = crate::commands::objectives::upsert_objective_value(
                    &context.client,
                    &serde_json::Map::from_iter([
                        ("kind".to_owned(), serde_json::Value::String(spec.kind.to_owned())),
                        ("name".to_owned(), serde_json::Value::String(spec.name.clone())),
                        ("prompt".to_owned(), serde_json::Value::String(spec.prompt.clone())),
                        ("enabled".to_owned(), serde_json::Value::Bool(true)),
                        (
                            "delivery_mode".to_owned(),
                            serde_json::Value::String("same_channel".to_owned()),
                        ),
                        (
                            "approval_mode".to_owned(),
                            serde_json::Value::String(
                                if spec.kind == "standing_order" || spec.kind == "program" {
                                    "before_first_run"
                                } else {
                                    "none"
                                }
                                .to_owned(),
                            ),
                        ),
                        (
                            "template_id".to_owned(),
                            if spec.kind == "heartbeat" {
                                serde_json::Value::String("heartbeat".to_owned())
                            } else {
                                serde_json::Value::Null
                            },
                        ),
                        (
                            "natural_language_schedule".to_owned(),
                            if spec.kind == "heartbeat" {
                                serde_json::Value::String("every weekday at 9".to_owned())
                            } else {
                                serde_json::Value::Null
                            },
                        ),
                    ]),
                )
                .await?;
                let objective = payload.pointer("/objective").cloned().unwrap_or(payload);
                let objective_id = objective
                    .pointer("/objective_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned();
                let state = objective
                    .pointer("/state")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown");
                self.selected_objective_id = Some(objective_id.clone());
                self.push_entry(
                    EntryKind::System,
                    "Objective created",
                    format!("{objective_id} [{state}] {}", spec.name),
                );
                self.status_line = format!("Objective created: {objective_id}");
            }
            other => {
                self.status_line = format!("Unknown objective subcommand: {other}");
            }
        }
        Ok(())
    }

    async fn handle_compaction_command(&mut self, arguments: Vec<String>) -> Result<()> {
        let session_id = self.active_session_id()?;
        let context = self.connect_admin_console().await?;
        let spec = parse_tui_compaction_arguments(arguments.as_slice())?;
        if spec.history {
            let payload = context
                .client
                .get_json_value(format!(
                    "console/v1/chat/sessions/{}/transcript",
                    percent_encode_component(session_id.as_str())
                ))
                .await?;
            let compactions = payload
                .pointer("/compactions")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            let checkpoints = payload
                .pointer("/checkpoints")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            let mut lines = Vec::new();
            if compactions.is_empty() {
                lines.push("Compactions: none".to_owned());
            } else {
                lines.push("Compactions:".to_owned());
                for artifact in compactions.iter().rev().take(3) {
                    let artifact_id = artifact
                        .pointer("/artifact_id")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown");
                    let summary = artifact
                        .pointer("/summary_json")
                        .and_then(serde_json::Value::as_str)
                        .and_then(parse_tui_json_string);
                    let lifecycle = summary
                        .as_ref()
                        .and_then(|value| value.pointer("/lifecycle_state"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("stored");
                    let review_count = summary
                        .as_ref()
                        .and_then(|value| value.pointer("/planner/review_candidate_count"))
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or_default();
                    lines.push(format!(
                        "- {} {} review={} {}",
                        shorten_id(artifact_id),
                        lifecycle,
                        review_count,
                        artifact
                            .pointer("/summary_preview")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("No compaction preview.")
                    ));
                }
            }
            if checkpoints.is_empty() {
                lines.push("Checkpoints: none".to_owned());
            } else {
                lines.push("Checkpoints:".to_owned());
                for checkpoint in checkpoints.iter().rev().take(3) {
                    let checkpoint_id = checkpoint
                        .pointer("/checkpoint_id")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown");
                    lines.push(format!(
                        "- {} {} restores={} {}",
                        shorten_id(checkpoint_id),
                        checkpoint
                            .pointer("/name")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("Unnamed checkpoint"),
                        checkpoint
                            .pointer("/restore_count")
                            .and_then(serde_json::Value::as_u64)
                            .unwrap_or_default(),
                        checkpoint
                            .pointer("/note")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("No note recorded.")
                    ));
                }
            }
            self.push_entry(EntryKind::System, "Compaction history", lines.join("\n"));
            self.status_line = "Compaction history loaded".to_owned();
            return Ok(());
        }

        let path = if spec.apply {
            format!(
                "console/v1/chat/sessions/{}/compactions",
                percent_encode_component(session_id.as_str())
            )
        } else {
            format!(
                "console/v1/chat/sessions/{}/compactions/preview",
                percent_encode_component(session_id.as_str())
            )
        };
        let payload = context
            .client
            .post_json_value(
                path,
                &serde_json::json!({
                    "accept_candidate_ids": spec.accept_candidate_ids,
                    "reject_candidate_ids": spec.reject_candidate_ids,
                }),
            )
            .await?;
        let preview = payload.pointer("/preview").cloned().unwrap_or_else(|| serde_json::json!({}));
        let summary_preview = preview
            .pointer("/summary_preview")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("No compaction preview available.");
        let token_delta =
            preview.pointer("/token_delta").and_then(serde_json::Value::as_u64).unwrap_or_default();
        let review_count = preview
            .pointer("/summary/planner/review_candidate_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default();
        let write_count = preview
            .pointer("/summary/writes")
            .and_then(serde_json::Value::as_array)
            .map(|writes| writes.len())
            .unwrap_or_default();
        if spec.apply {
            let artifact_id = payload
                .pointer("/artifact/artifact_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            let checkpoint_id = payload
                .pointer("/checkpoint/checkpoint_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            self.push_entry(
                EntryKind::System,
                "Compaction",
                format!(
                    "Created compaction artifact {} and checkpoint {} with estimated token delta {}.\nPlanned writes: {} · Review candidates: {}\n{}",
                    shorten_id(artifact_id),
                    shorten_id(checkpoint_id),
                    token_delta,
                    write_count,
                    review_count,
                    summary_preview
                ),
            );
            self.status_line = "Compaction artifact created".to_owned();
        } else {
            self.push_entry(
                EntryKind::System,
                "Compaction preview",
                format!(
                    "Estimated token delta {}.\nPlanned writes: {} · Review candidates: {}\n{}",
                    token_delta, write_count, review_count, summary_preview
                ),
            );
            self.status_line = "Compaction preview loaded".to_owned();
        }
        Ok(())
    }

    async fn handle_checkpoint_command(&mut self, arguments: Vec<String>) -> Result<()> {
        let Some(action) = arguments.first().map(String::as_str) else {
            self.status_line =
                "Usage: /checkpoint save <name> | list | restore <checkpoint_id>".to_owned();
            return Ok(());
        };
        let context = self.connect_admin_console().await?;
        match action {
            "save" => {
                let name = normalize_optional_text(
                    arguments.iter().skip(1).cloned().collect::<Vec<_>>().join(" "),
                );
                let Some(name) = name else {
                    self.status_line = "Usage: /checkpoint save <name>".to_owned();
                    return Ok(());
                };
                let session_id = self.active_session_id()?;
                let payload = context
                    .client
                    .post_json_value(
                        format!(
                            "console/v1/chat/sessions/{}/checkpoints",
                            percent_encode_component(session_id.as_str())
                        ),
                        &serde_json::json!({
                            "name": name,
                            "tags": Vec::<String>::new(),
                        }),
                    )
                    .await?;
                let checkpoint_id = payload
                    .pointer("/checkpoint/checkpoint_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown");
                self.push_entry(
                    EntryKind::System,
                    "Checkpoint",
                    format!("Saved checkpoint {}.", shorten_id(checkpoint_id)),
                );
                self.status_line = "Checkpoint created".to_owned();
            }
            "list" => {
                let session_id = self.active_session_id()?;
                let payload = context
                    .client
                    .get_json_value(format!(
                        "console/v1/chat/sessions/{}/transcript",
                        percent_encode_component(session_id.as_str())
                    ))
                    .await?;
                let checkpoints = payload
                    .pointer("/checkpoints")
                    .and_then(serde_json::Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                if checkpoints.is_empty() {
                    self.push_entry(
                        EntryKind::System,
                        "Checkpoint",
                        "No checkpoints stored for this session.",
                    );
                } else {
                    let body = checkpoints
                        .iter()
                        .map(|checkpoint| {
                            format!(
                                "{} · {}",
                                checkpoint
                                    .pointer("/checkpoint_id")
                                    .and_then(serde_json::Value::as_str)
                                    .map(shorten_id)
                                    .unwrap_or_else(|| "unknown".to_owned()),
                                checkpoint
                                    .pointer("/name")
                                    .and_then(serde_json::Value::as_str)
                                    .unwrap_or("unnamed")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    self.push_entry(EntryKind::System, "Checkpoint list", body);
                }
                self.status_line = "Checkpoint list refreshed".to_owned();
            }
            "restore" => {
                let Some(checkpoint_id) = arguments.get(1) else {
                    self.status_line = "Usage: /checkpoint restore <checkpoint_id>".to_owned();
                    return Ok(());
                };
                let payload = context
                    .client
                    .post_json_value(
                        format!(
                            "console/v1/chat/checkpoints/{}/restore",
                            percent_encode_component(checkpoint_id.as_str())
                        ),
                        &serde_json::json!({}),
                    )
                    .await?;
                let restored_session_id = payload
                    .pointer("/session/session_id")
                    .and_then(serde_json::Value::as_str)
                    .context("checkpoint restore response is missing session_id")?
                    .to_owned();
                self.switch_session(restored_session_id).await?;
                self.push_entry(
                    EntryKind::System,
                    "Checkpoint restore",
                    format!("Restored checkpoint {} as a new branch.", shorten_id(checkpoint_id)),
                );
                self.status_line = "Checkpoint restored into a new session".to_owned();
            }
            _ => {
                self.status_line =
                    "Usage: /checkpoint save <name> | list | restore <checkpoint_id>".to_owned();
            }
        }
        Ok(())
    }

    async fn handle_background_command(&mut self, arguments: Vec<String>) -> Result<()> {
        let Some(action) = arguments.first().map(String::as_str) else {
            self.status_line =
                "Usage: /background list | add <text> | show|pause|resume|retry|cancel <task_id>"
                    .to_owned();
            return Ok(());
        };
        let context = self.connect_admin_console().await?;
        match action {
            "list" => {
                let session_id = self.active_session_id()?;
                let payload = context
                    .client
                    .get_json_value(format!(
                        "console/v1/chat/background-tasks?session_id={}&include_completed=true",
                        percent_encode_component(session_id.as_str())
                    ))
                    .await?;
                let tasks = payload
                    .pointer("/tasks")
                    .and_then(serde_json::Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                if tasks.is_empty() {
                    self.push_entry(
                        EntryKind::System,
                        "Background",
                        "No background tasks recorded.",
                    );
                } else {
                    let body = tasks
                        .iter()
                        .map(|task| {
                            format!(
                                "{} · {} · {}",
                                task.pointer("/task_id")
                                    .and_then(serde_json::Value::as_str)
                                    .map(shorten_id)
                                    .unwrap_or_else(|| "unknown".to_owned()),
                                task.pointer("/state")
                                    .and_then(serde_json::Value::as_str)
                                    .unwrap_or("unknown"),
                                task.pointer("/input_text")
                                    .and_then(serde_json::Value::as_str)
                                    .unwrap_or("")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    self.push_entry(EntryKind::System, "Background list", body);
                }
                self.status_line = "Background tasks refreshed".to_owned();
            }
            "add" => {
                let text = normalize_optional_text(
                    arguments.iter().skip(1).cloned().collect::<Vec<_>>().join(" "),
                );
                let Some(text) = text else {
                    self.status_line = "Usage: /background add <text>".to_owned();
                    return Ok(());
                };
                let session_id = self.active_session_id()?;
                let payload = context
                    .client
                    .post_json_value(
                        format!(
                            "console/v1/chat/sessions/{}/background-tasks",
                            percent_encode_component(session_id.as_str())
                        ),
                        &serde_json::json!({ "text": text }),
                    )
                    .await?;
                let task_id = payload
                    .pointer("/task/task_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown");
                self.push_entry(
                    EntryKind::System,
                    "Background task",
                    format!("Queued background task {}.", shorten_id(task_id)),
                );
                self.status_line = "Background task queued".to_owned();
            }
            "show" | "pause" | "resume" | "retry" | "cancel" => {
                let Some(task_id) = arguments.get(1) else {
                    self.status_line = format!("Usage: /background {action} <task_id>");
                    return Ok(());
                };
                if action == "show" {
                    let payload = context
                        .client
                        .get_json_value(format!(
                            "console/v1/chat/background-tasks/{}",
                            percent_encode_component(task_id.as_str())
                        ))
                        .await?;
                    self.push_entry(
                        EntryKind::System,
                        "Background task",
                        serde_json::to_string_pretty(&payload)?,
                    );
                    self.status_line = "Background task detail loaded".to_owned();
                } else {
                    let payload = context
                        .client
                        .post_json_value(
                            format!(
                                "console/v1/chat/background-tasks/{}/{}",
                                percent_encode_component(task_id.as_str()),
                                action
                            ),
                            &serde_json::json!({}),
                        )
                        .await?;
                    let state = payload
                        .pointer("/task/state")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown");
                    self.push_entry(
                        EntryKind::System,
                        "Background task",
                        format!("Action {action} applied to {} -> {state}.", shorten_id(task_id)),
                    );
                    self.status_line = format!("Background task {action} queued");
                }
            }
            _ => {
                self.status_line =
                    "Usage: /background list | add <text> | show|pause|resume|retry|cancel <task_id>"
                        .to_owned();
            }
        }
        Ok(())
    }

    async fn delegate_background_run(&mut self, arguments: Vec<String>) -> Result<()> {
        let Some(selector) = arguments.first().map(String::as_str) else {
            self.status_line = format!(
                "Usage: /delegate <profile-or-template> <text> (profiles: {} | templates: {})",
                BUILT_IN_DELEGATION_PROFILES.join(", "),
                BUILT_IN_DELEGATION_TEMPLATES.join(", ")
            );
            return Ok(());
        };
        let text = normalize_optional_text(
            arguments.iter().skip(1).cloned().collect::<Vec<_>>().join(" "),
        );
        let Some(text) = text else {
            self.status_line = "Usage: /delegate <profile-or-template> <text>".to_owned();
            return Ok(());
        };
        let selector = selector.trim().to_ascii_lowercase();
        let delegation = if BUILT_IN_DELEGATION_TEMPLATES.contains(&selector.as_str()) {
            serde_json::json!({ "template_id": selector })
        } else if BUILT_IN_DELEGATION_PROFILES.contains(&selector.as_str()) {
            serde_json::json!({ "profile_id": selector })
        } else {
            self.status_line = format!(
                "Unknown delegation selector '{}'. Profiles: {}. Templates: {}.",
                selector,
                BUILT_IN_DELEGATION_PROFILES.join(", "),
                BUILT_IN_DELEGATION_TEMPLATES.join(", ")
            );
            return Ok(());
        };
        let session_id = self.active_session_id()?;
        let context = self.connect_admin_console().await?;
        let payload = context
            .client
            .post_json_value(
                format!(
                    "console/v1/chat/sessions/{}/background-tasks",
                    percent_encode_component(session_id.as_str())
                ),
                &serde_json::json!({
                    "text": text,
                    "delegation": delegation,
                }),
            )
            .await?;
        let task_id = payload
            .pointer("/task/task_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        self.push_entry(
            EntryKind::System,
            "Delegation",
            format!("Queued delegated child task {} via {}.", shorten_id(task_id), selector),
        );
        self.status_line = "Delegated background task queued".to_owned();
        Ok(())
    }

    async fn open_picker(&mut self, kind: PickerKind) -> Result<()> {
        if matches!(kind, PickerKind::Session) {
            return self.open_session_history_picker(None).await;
        }
        let picker = match kind {
            PickerKind::Agent => {
                let response = self.runtime.list_agents(None, Some(100)).await?;
                let selected_agent =
                    self.current_agent.as_ref().map(|agent| agent.agent_id.as_str());
                let items = response
                    .agents
                    .into_iter()
                    .map(|agent| PickerItem {
                        id: agent.agent_id.clone(),
                        title: format!(
                            "{}{}",
                            agent.display_name,
                            if response.default_agent_id == agent.agent_id {
                                " [default]"
                            } else {
                                ""
                            }
                        ),
                        detail: format!(
                            "{} | model={} | workspaces={}",
                            agent.agent_id,
                            agent.default_model_profile,
                            agent.workspace_roots.len()
                        ),
                    })
                    .collect::<Vec<_>>();
                PickerState {
                    kind,
                    title: "Agent picker".to_owned(),
                    selected: selected_agent
                        .and_then(|id| items.iter().position(|item| item.id == id))
                        .unwrap_or(0),
                    items,
                }
            }
            PickerKind::Session => unreachable!(),
            PickerKind::Model => {
                let models = self.runtime.list_models(None)?;
                let current_model = models.status.text_model.clone();
                let items = models
                    .models
                    .iter()
                    .filter(|entry| entry.target == "text")
                    .map(|entry| PickerItem {
                        id: entry.id.clone(),
                        title: entry.id.clone(),
                        detail: format!(
                            "configured={} preferred={} source={}",
                            entry.configured, entry.preferred, entry.source
                        ),
                    })
                    .collect::<Vec<_>>();
                self.models = Some(models);
                PickerState {
                    kind,
                    title: "Model picker".to_owned(),
                    selected: current_model
                        .as_deref()
                        .and_then(|id| items.iter().position(|item| item.id == id))
                        .unwrap_or(0),
                    items,
                }
            }
        };
        self.mode = Mode::Picker(kind);
        self.pending_picker = Some(picker);
        Ok(())
    }

    async fn open_session_history_picker(&mut self, query: Option<String>) -> Result<()> {
        let response = self
            .runtime
            .list_session_catalog(vec![
                ("limit", Some("100".to_owned())),
                ("sort", Some("updated_desc".to_owned())),
                ("q", query.clone()),
                ("include_archived", self.include_archived_sessions.then(|| "true".to_owned())),
            ])
            .await?;
        let current_session_id =
            self.session.session_id.as_ref().map(|value| value.ulid.as_str()).unwrap_or_default();
        let items = response
            .sessions
            .into_iter()
            .map(|session| PickerItem {
                id: session.session_id.clone(),
                title: session.title,
                detail: format!(
                    "{} | updated={} | {}",
                    if session.archived { "archived" } else { session.title_source.as_str() },
                    session.updated_at_unix_ms,
                    session.preview.unwrap_or_else(|| "no preview".to_owned())
                ),
            })
            .collect::<Vec<_>>();
        let selected = items.iter().position(|item| item.id == current_session_id).unwrap_or(0);
        self.mode = Mode::Picker(PickerKind::Session);
        self.pending_picker = Some(PickerState {
            kind: PickerKind::Session,
            title: match query.as_deref() {
                Some(value) if !value.trim().is_empty() => format!("Session history: {value}"),
                _ => "Session history".to_owned(),
            },
            items,
            selected,
        });
        self.status_line = "Session history ready".to_owned();
        Ok(())
    }

    async fn apply_picker_selection(&mut self) -> Result<()> {
        let Some(picker) = self.pending_picker.clone() else {
            self.mode = Mode::Chat;
            return Ok(());
        };
        let Some(item) = picker.items.get(picker.selected) else {
            self.mode = Mode::Chat;
            return Ok(());
        };
        match picker.kind {
            PickerKind::Agent => self.switch_agent(item.id.clone()).await?,
            PickerKind::Session => self.switch_session(item.id.clone()).await?,
            PickerKind::Model => self.set_model(item.id.clone()).await?,
        }
        self.pending_picker = None;
        self.mode = Mode::Chat;
        Ok(())
    }

    async fn set_model(&mut self, model_id: String) -> Result<()> {
        self.runtime.set_text_model(None, 1, model_id.clone())?;
        self.models = Some(self.runtime.list_models(None)?);
        self.push_entry(
            EntryKind::System,
            "Model",
            format!("Configured default text model to {model_id}."),
        );
        self.status_line = format!("Model set to {model_id}");
        self.mode = Mode::Chat;
        Ok(())
    }

    async fn refresh_agent_identity(
        &mut self,
        preferred_agent_id: Option<String>,
        persist_binding: bool,
    ) -> Result<()> {
        let response = self
            .runtime
            .resolve_agent_for_context(AgentContextResolveInput {
                principal: self.runtime.connection().principal.clone(),
                channel: self.runtime.connection().channel.clone(),
                session_id: self.session.session_id.clone(),
                preferred_agent_id: preferred_agent_id.unwrap_or_default(),
                persist_session_binding: persist_binding,
            })
            .await?;
        self.current_agent = response.agent;
        self.current_agent_source = agent_resolution_source_label(response.source);
        Ok(())
    }

    async fn reload_runtime_state(&mut self) -> Result<()> {
        self.refresh_agent_identity(None, false).await?;
        self.models = Some(self.runtime.list_models(None)?);
        self.status_line = "Runtime state reloaded".to_owned();
        Ok(())
    }

    fn status_summary(&self) -> String {
        format!(
            "session={} branch={} agent={} source={} model={} tools={} thinking={} shell={} active_run={}",
            display_session_identity(&self.session),
            if self.session.branch_state.trim().is_empty() {
                "none"
            } else {
                self.session.branch_state.as_str()
            },
            self.current_agent.as_ref().map(|agent| agent.agent_id.as_str()).unwrap_or("none"),
            self.current_agent_source,
            self.models
                .as_ref()
                .and_then(|models| models.status.text_model.as_deref())
                .unwrap_or("none"),
            self.show_tools,
            self.show_thinking,
            self.local_shell_enabled,
            self.active_stream
                .as_ref()
                .map(|stream| stream.run_id())
                .or(self.last_run_id.as_deref())
                .unwrap_or("none")
        )
    }
}

async fn handle_key(app: &mut App, key: KeyEvent) -> Result<bool> {
    if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c')) {
        return Ok(true);
    }
    match app.mode {
        Mode::Help => match key.code {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('?') => app.mode = Mode::Chat,
            _ => {}
        },
        Mode::Approval => handle_approval_key(app, key)?,
        Mode::ShellConfirm => handle_shell_confirm_key(app, key).await?,
        Mode::Settings => handle_settings_key(app, key),
        Mode::Picker(_) => handle_picker_key(app, key).await?,
        Mode::Chat => handle_chat_key(app, key).await?,
    }
    Ok(false)
}

async fn handle_chat_key(app: &mut App, key: KeyEvent) -> Result<()> {
    match key.code {
        KeyCode::Tab => {
            app.focus = match app.focus {
                Focus::Transcript => Focus::Input,
                Focus::Input => Focus::Transcript,
            };
        }
        KeyCode::Char('?') => app.mode = Mode::Help,
        KeyCode::F(2) => app.open_picker(PickerKind::Agent).await?,
        KeyCode::F(3) => app.open_picker(PickerKind::Session).await?,
        KeyCode::F(4) => app.open_picker(PickerKind::Model).await?,
        KeyCode::F(5) => app.mode = Mode::Settings,
        KeyCode::Char('r') if key.modifiers == KeyModifiers::CONTROL => {
            app.reload_runtime_state().await?;
        }
        KeyCode::Enter if matches!(app.focus, Focus::Input) => app.submit_input().await?,
        KeyCode::Backspace if matches!(app.focus, Focus::Input) => {
            app.input.pop();
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if matches!(app.focus, Focus::Input) {
                app.input.clear();
            }
        }
        KeyCode::Char(ch) if matches!(app.focus, Focus::Input) && key.modifiers.is_empty() => {
            app.input.push(ch);
        }
        KeyCode::Up => {
            if matches!(app.focus, Focus::Transcript) {
                app.scroll_offset = app.scroll_offset.saturating_add(1);
            }
        }
        KeyCode::Down => {
            if matches!(app.focus, Focus::Transcript) {
                app.scroll_offset = app.scroll_offset.saturating_sub(1);
            }
        }
        KeyCode::PageUp => app.scroll_offset = app.scroll_offset.saturating_add(8),
        KeyCode::PageDown => app.scroll_offset = app.scroll_offset.saturating_sub(8),
        KeyCode::Char('q') if app.input.is_empty() => app.status_line = "__exit__".to_owned(),
        _ => {}
    }
    Ok(())
}

fn handle_approval_key(app: &mut App, key: KeyEvent) -> Result<()> {
    let Some(approval) = app.pending_approval.clone() else {
        app.mode = Mode::Chat;
        return Ok(());
    };
    match key.code {
        KeyCode::Char('y') | KeyCode::Enter => {
            if let Some(stream) = app.active_stream.as_ref() {
                stream.send_tool_approval_decision(
                    &approval,
                    true,
                    "approved_by_tui".to_owned(),
                    common_v1::ApprovalDecisionScope::Once as i32,
                    0,
                )?;
            }
            app.push_entry(
                EntryKind::Tool,
                "Approval response",
                format!("Approved {}", approval.tool_name),
            );
            app.pending_approval = None;
            app.mode = Mode::Chat;
            app.status_line = "Approval granted once".to_owned();
        }
        KeyCode::Char('n') | KeyCode::Esc => {
            if let Some(stream) = app.active_stream.as_ref() {
                stream.send_tool_approval_decision(
                    &approval,
                    false,
                    "denied_by_tui".to_owned(),
                    common_v1::ApprovalDecisionScope::Once as i32,
                    0,
                )?;
            }
            app.push_entry(
                EntryKind::Tool,
                "Approval response",
                format!("Denied {}", approval.tool_name),
            );
            app.pending_approval = None;
            app.mode = Mode::Chat;
            app.status_line = "Approval denied".to_owned();
        }
        _ => {}
    }
    Ok(())
}

async fn handle_shell_confirm_key(app: &mut App, key: KeyEvent) -> Result<()> {
    match key.code {
        KeyCode::Char('y') | KeyCode::Enter => {
            app.local_shell_enabled = true;
            app.mode = Mode::Chat;
            app.status_line = "Local shell enabled for this TUI session".to_owned();
            if let Some(command) = app.pending_shell_command.take() {
                app.handle_shell_request(command).await?;
            }
        }
        KeyCode::Char('n') | KeyCode::Esc => {
            app.pending_shell_command = None;
            app.mode = Mode::Chat;
            app.status_line = "Local shell remains disabled".to_owned();
        }
        _ => {}
    }
    Ok(())
}

fn handle_settings_key(app: &mut App, key: KeyEvent) {
    let items = settings_items();
    match key.code {
        KeyCode::Esc => app.mode = Mode::Chat,
        KeyCode::Up => app.settings_selected = app.settings_selected.saturating_sub(1),
        KeyCode::Down => {
            app.settings_selected = (app.settings_selected + 1).min(items.len().saturating_sub(1));
        }
        KeyCode::Enter | KeyCode::Char(' ') => match items[app.settings_selected] {
            SettingsItem::ShowTools => app.show_tools = !app.show_tools,
            SettingsItem::ShowThinking => app.show_thinking = !app.show_thinking,
            SettingsItem::LocalShell => {
                if app.local_shell_enabled {
                    app.local_shell_enabled = false;
                    app.status_line = "Local shell disabled".to_owned();
                } else {
                    app.mode = Mode::ShellConfirm;
                    app.pending_shell_command = None;
                    app.status_line = "Confirm local shell opt-in".to_owned();
                }
            }
        },
        _ => {}
    }
}

async fn handle_picker_key(app: &mut App, key: KeyEvent) -> Result<()> {
    let Some(picker) = app.pending_picker.as_mut() else {
        app.mode = Mode::Chat;
        return Ok(());
    };
    match key.code {
        KeyCode::Esc => {
            app.pending_picker = None;
            app.mode = Mode::Chat;
        }
        KeyCode::Up => picker.selected = picker.selected.saturating_sub(1),
        KeyCode::Down => {
            picker.selected = (picker.selected + 1).min(picker.items.len().saturating_sub(1));
        }
        KeyCode::Enter => app.apply_picker_selection().await?,
        _ => {}
    }
    Ok(())
}

fn render(frame: &mut Frame<'_>, app: &App) {
    let areas = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Min(8),
            Constraint::Length(3),
            Constraint::Length(1),
        ])
        .split(frame.area());
    let header = areas[0];
    let main = areas[1];
    let input = areas[2];
    let footer = areas[3];

    render_header(frame, header, app);
    render_transcript(frame, main, app);
    render_input(frame, input, app);
    render_footer(frame, footer, app);

    match app.mode {
        Mode::Help => render_help_popup(frame, frame.area()),
        Mode::Approval => render_approval_popup(frame, frame.area(), app),
        Mode::Picker(_) => render_picker_popup(frame, frame.area(), app),
        Mode::Settings => render_settings_popup(frame, frame.area(), app),
        Mode::ShellConfirm => render_shell_confirm_popup(frame, frame.area()),
        Mode::Chat => {}
    }
}

fn render_header(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1)])
        .split(area);
    let top = rows[0];
    let bottom = rows[1];
    let connection_line = Line::from(vec![
        Span::styled("Gateway ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::raw(app.runtime.connection().grpc_url.as_str()),
        Span::raw("  "),
        Span::styled("Session ", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Span::raw(display_session_identity(&app.session)),
    ]);
    let status_line = Line::from(vec![
        Span::styled("Agent ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
        Span::raw(
            app.current_agent.as_ref().map(|agent| agent.agent_id.as_str()).unwrap_or("none"),
        ),
        Span::raw("  "),
        Span::styled("Model ", Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD)),
        Span::raw(
            app.models
                .as_ref()
                .and_then(|models| models.status.text_model.as_deref())
                .unwrap_or("none"),
        ),
        Span::raw("  "),
        Span::styled("Status ", Style::default().fg(Color::Blue).add_modifier(Modifier::BOLD)),
        Span::raw(sanitize_terminal_text(app.status_line.as_str())),
    ]);
    frame.render_widget(Paragraph::new(connection_line), top);
    frame.render_widget(Paragraph::new(status_line), bottom);
}

fn render_transcript(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let mut lines = Vec::new();
    for entry in &app.transcript {
        if matches!(entry.kind, EntryKind::Tool) && !app.show_tools {
            continue;
        }
        if matches!(entry.kind, EntryKind::Thinking) && !app.show_thinking {
            continue;
        }
        let style = entry_style(&entry.kind);
        lines.push(Line::from(Span::styled(
            format!("[{}]", entry.title),
            style.add_modifier(Modifier::BOLD),
        )));
        for chunk in entry.body.lines() {
            lines.push(Line::from(Span::styled(format!("  {}", chunk), style)));
        }
        lines.push(Line::default());
    }
    let transcript = Paragraph::new(Text::from(lines))
        .block(Block::default().borders(Borders::ALL).title(
            if matches!(app.focus, Focus::Transcript) {
                "Transcript [focus]"
            } else {
                "Transcript"
            },
        ))
        .scroll((app.scroll_offset, 0))
        .wrap(Wrap { trim: false });
    frame.render_widget(transcript, area);
}

fn render_input(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(if matches!(app.focus, Focus::Input) { "Input [focus]" } else { "Input" });
    let inner = block.inner(area);
    frame.render_widget(block, area);
    frame.render_widget(Paragraph::new(app.input.as_str()).wrap(Wrap { trim: false }), inner);
    if matches!(app.focus, Focus::Input) && matches!(app.mode, Mode::Chat) {
        frame.set_cursor_position((inner.x + app.input.chars().count() as u16, inner.y));
    }
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let help = format!(
        "Enter send  Tab focus  F2/F3/F4 pickers  F5 settings  Ctrl+R reload  ? help  / commands  ! shell({})",
        if app.local_shell_enabled { "on" } else { "off" }
    );
    frame.render_widget(Paragraph::new(help), area);
}

#[derive(Debug, Default)]
struct TuiCompactionCommand {
    apply: bool,
    history: bool,
    accept_candidate_ids: Vec<String>,
    reject_candidate_ids: Vec<String>,
}

fn parse_tui_compaction_arguments(arguments: &[String]) -> Result<TuiCompactionCommand> {
    let mut command = TuiCompactionCommand::default();
    let mut index = 0usize;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "preview" => {}
            "apply" => command.apply = true,
            "history" => command.history = true,
            "--accept" => {
                let candidate_id = arguments
                    .get(index + 1)
                    .cloned()
                    .context("Usage: /compact [preview|apply|history] [--accept <candidate_id>] [--reject <candidate_id>]")?;
                command.accept_candidate_ids.push(candidate_id);
                index += 1;
            }
            "--reject" => {
                let candidate_id = arguments
                    .get(index + 1)
                    .cloned()
                    .context("Usage: /compact [preview|apply|history] [--accept <candidate_id>] [--reject <candidate_id>]")?;
                command.reject_candidate_ids.push(candidate_id);
                index += 1;
            }
            other => anyhow::bail!(
                "unknown /compact argument `{other}`; use preview, apply, history, --accept, or --reject"
            ),
        }
        index += 1;
    }
    if command.history
        && (command.apply
            || !command.accept_candidate_ids.is_empty()
            || !command.reject_candidate_ids.is_empty())
    {
        anyhow::bail!("history cannot be combined with apply or candidate review flags");
    }
    Ok(command)
}

fn parse_tui_json_string(value: &str) -> Option<serde_json::Value> {
    serde_json::from_str::<serde_json::Value>(value).ok()
}

struct TuiObjectiveCreateSpec {
    kind: &'static str,
    name: String,
    prompt: String,
}

fn resolve_tui_objective_reference(
    explicit: Option<String>,
    selected: Option<&String>,
) -> Result<String> {
    explicit
        .or_else(|| selected.cloned())
        .context("Select an objective first or pass an explicit objective_id")
}

fn parse_tui_objective_create_spec(
    fixed_kind: Option<&'static str>,
    arguments: &[String],
) -> Result<TuiObjectiveCreateSpec> {
    let create_arguments =
        arguments.get(1..).context("Usage: /objective create <kind> <name> :: <prompt>")?;
    let joined = create_arguments.join(" ");
    let (head, prompt) =
        joined.split_once("::").context("Usage: /objective create <kind> <name> :: <prompt>")?;
    let prompt = prompt.trim();
    if prompt.is_empty() {
        anyhow::bail!("objective prompt cannot be empty");
    }
    let mut head_parts = head.split_whitespace();
    let kind = match fixed_kind {
        Some(kind) => kind,
        None => parse_tui_objective_kind(
            head_parts.next().context("Usage: /objective create <kind> <name> :: <prompt>")?,
        )?,
    };
    let name = head_parts.collect::<Vec<_>>().join(" ").trim().to_owned();
    if name.is_empty() {
        anyhow::bail!("objective name cannot be empty");
    }
    Ok(TuiObjectiveCreateSpec { kind, name, prompt: prompt.to_owned() })
}

fn parse_tui_objective_kind(value: &str) -> Result<&'static str> {
    match value.to_ascii_lowercase().as_str() {
        "objective" => Ok("objective"),
        "heartbeat" => Ok("heartbeat"),
        "standing-order" | "standing_order" => Ok("standing_order"),
        "program" => Ok("program"),
        other => anyhow::bail!(
            "unknown objective kind `{other}`; use objective, heartbeat, standing-order, or program"
        ),
    }
}

fn render_help_popup(frame: &mut Frame<'_>, area: Rect) {
    let popup = centered_rect(72, 14, area);
    let text = Text::from(vec![
        Line::from("Slash commands"),
        Line::from(
            "  /help /status /new [label] /agent [/id] /session [/id-or-key] /history [query]",
        ),
        Line::from(
            "  /resume [/id-or-key] /model [/id] /reset /retry /branch [label] /queue <text>",
        ),
        Line::from(
            "  /objective list|show|select|fire|pause|resume|archive|create  /heartbeat ...  /standing-order ...",
        ),
        Line::from(
            "  /delegate <profile-or-template> <text>  /checkpoint save|list|restore",
        ),
        Line::from(
            "  /background list|add|show|pause|resume|retry|cancel  /abort [run_id] /usage /compact [preview|apply|history] /attach /settings",
        ),
        Line::from("  /tools on|off /thinking on|off /shell on|off /exit"),
        Line::default(),
        Line::from("Context references"),
        Line::from(
            "  @file:PATH @folder:PATH @diff[:PATH] @staged[:PATH] @url:URL @memory:\"query\"",
        ),
        Line::from("  Escape a literal at-sign with @@"),
        Line::default(),
        Line::from("Pickers"),
        Line::from("  F2 agent  F3 session  F4 model"),
        Line::from("  F5 settings  Ctrl+R reload runtime state"),
        Line::default(),
        Line::from("Controls"),
        Line::from("  Tab focus  Enter submit/select  Esc close overlay  q quit"),
        Line::default(),
        Line::from(
            "Retry, branch, delegation, compaction, checkpoints, and background tasks reuse the console HTTP contracts.",
        ),
    ]);
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(text)
            .block(Block::default().borders(Borders::ALL).title("Help"))
            .alignment(Alignment::Left)
            .wrap(Wrap { trim: false }),
        popup,
    );
}

fn percent_encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(*byte));
            }
            other => {
                encoded.push('%');
                encoded.push_str(format!("{other:02X}").as_str());
            }
        }
    }
    encoded
}

fn render_approval_popup(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let popup = centered_rect(72, 10, area);
    let body = if let Some(approval) = app.pending_approval.as_ref() {
        Text::from(vec![
            Line::from(vec![
                Span::styled(
                    "Tool ",
                    Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                ),
                Span::raw(sanitize_terminal_text(approval.tool_name.as_str())),
            ]),
            Line::default(),
            Line::from(sanitize_terminal_text(approval.request_summary.as_str())),
            Line::default(),
            Line::from("y / Enter = allow once"),
            Line::from("n / Esc   = deny"),
        ])
    } else {
        Text::from("Approval request is no longer available.")
    };
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(body)
            .block(Block::default().borders(Borders::ALL).title("Approval Required"))
            .wrap(Wrap { trim: false }),
        popup,
    );
}

fn render_picker_popup(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let popup = centered_rect(88, 18, area);
    frame.render_widget(Clear, popup);
    if let Some(picker) = app.pending_picker.as_ref() {
        let mut lines = Vec::new();
        for (index, item) in picker.items.iter().enumerate() {
            let prefix = if index == picker.selected { ">" } else { " " };
            lines.push(Line::from(Span::styled(
                format!("{prefix} {}", item.title),
                if index == picker.selected {
                    Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                },
            )));
            lines.push(Line::from(format!("  {}", item.detail)));
            lines.push(Line::default());
        }
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(Block::default().borders(Borders::ALL).title(picker.title.as_str()))
                .wrap(Wrap { trim: false }),
            popup,
        );
    }
}

fn render_settings_popup(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let popup = centered_rect(56, 10, area);
    let items = settings_items();
    let mut lines = Vec::new();
    for (index, item) in items.iter().enumerate() {
        let selected = index == app.settings_selected;
        let prefix = if selected { ">" } else { " " };
        let (label, enabled) = match item {
            SettingsItem::ShowTools => ("Show tool cards", app.show_tools),
            SettingsItem::ShowThinking => ("Show thinking/status lines", app.show_thinking),
            SettingsItem::LocalShell => ("Enable local shell", app.local_shell_enabled),
        };
        lines.push(Line::from(Span::styled(
            format!("{prefix} [{enabled}] {label}"),
            if selected {
                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            },
        )));
    }
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(Block::default().borders(Borders::ALL).title("Settings"))
            .wrap(Wrap { trim: false }),
        popup,
    );
}

fn render_shell_confirm_popup(frame: &mut Frame<'_>, area: Rect) {
    let popup = centered_rect(64, 8, area);
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(Text::from(vec![
            Line::from("Local shell is disabled by default."),
            Line::default(),
            Line::from("Press y / Enter to enable it for this TUI session."),
            Line::from("Press n / Esc to keep it disabled."),
        ]))
        .block(Block::default().borders(Borders::ALL).title("Local Shell Opt-In"))
        .wrap(Wrap { trim: false }),
        popup,
    );
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(50),
            Constraint::Length(width.min(area.width.saturating_sub(2))),
            Constraint::Percentage(50),
        ])
        .split(area);
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(50),
            Constraint::Length(height.min(area.height.saturating_sub(2))),
            Constraint::Percentage(50),
        ])
        .split(horizontal[1]);
    vertical[1]
}

fn settings_items() -> [SettingsItem; 3] {
    [SettingsItem::ShowTools, SettingsItem::ShowThinking, SettingsItem::LocalShell]
}

fn entry_style(kind: &EntryKind) -> Style {
    match kind {
        EntryKind::User => Style::default().fg(Color::Cyan),
        EntryKind::Assistant => Style::default().fg(Color::White),
        EntryKind::Tool => Style::default().fg(Color::Yellow),
        EntryKind::Thinking => Style::default().fg(Color::DarkGray),
        EntryKind::System => Style::default().fg(Color::Green),
        EntryKind::Shell => Style::default().fg(Color::Magenta),
    }
}

fn display_session_identity(session: &gateway_v1::SessionSummary) -> String {
    if !session.title.trim().is_empty() {
        return session.title.clone();
    }
    if !session.session_label.trim().is_empty() {
        return session.session_label.clone();
    }
    if !session.session_key.trim().is_empty() {
        return session.session_key.clone();
    }
    if session.session_id.is_some() {
        "session".to_owned()
    } else {
        "unknown session".to_owned()
    }
}

fn normalize_optional_text(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn shorten_id(value: &str) -> String {
    redacted_identifier_for_output(value)
}

fn parse_toggle(value: Option<&str>, current: bool) -> Result<bool> {
    match value.unwrap_or("toggle") {
        "toggle" => Ok(!current),
        "on" | "true" | "yes" => Ok(true),
        "off" | "false" | "no" => Ok(false),
        other => Err(anyhow!("unsupported toggle value: {other}")),
    }
}

fn looks_like_canonical_ulid(value: &str) -> bool {
    value.len() == 26 && value.chars().all(|ch| ch.is_ascii_alphanumeric())
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

async fn run_local_shell(command: String) -> Result<ShellResult> {
    tokio::task::spawn_blocking(move || {
        #[cfg(windows)]
        let output = {
            let shell = std::env::var("ComSpec").unwrap_or_else(|_| "cmd.exe".to_owned());
            std::process::Command::new(shell).arg("/C").arg(command.as_str()).output()
        };
        #[cfg(not(windows))]
        let output = std::process::Command::new("sh").arg("-lc").arg(command.as_str()).output();

        let output = output.context("failed to execute local shell command")?;
        Ok::<ShellResult, anyhow::Error>(ShellResult {
            exit_code: output.status.code(),
            stdout: truncate_text(
                String::from_utf8_lossy(output.stdout.as_slice()).to_string(),
                1_500,
            ),
            stderr: truncate_text(
                String::from_utf8_lossy(output.stderr.as_slice()).to_string(),
                1_500,
            ),
        })
    })
    .await
    .context("local shell worker failed")?
}

fn truncate_text(mut value: String, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value;
    }
    value = value.chars().take(limit).collect::<String>();
    value.push_str("...");
    value
}

fn sanitize_terminal_text(value: &str) -> String {
    let mut sanitized = String::with_capacity(value.len());
    let mut just_saw_carriage_return = false;
    for ch in value.chars() {
        match ch {
            '\n' => {
                if !just_saw_carriage_return {
                    sanitized.push('\n');
                }
                just_saw_carriage_return = false;
            }
            '\r' => {
                if !sanitized.ends_with('\n') {
                    sanitized.push('\n');
                }
                just_saw_carriage_return = true;
            }
            '\u{1b}' => {
                sanitized.push_str("<ESC>");
                just_saw_carriage_return = false;
            }
            ch if ch.is_control() => {
                sanitized.push_str(format!("<U+{:04X}>", ch as u32).as_str());
                just_saw_carriage_return = false;
            }
            ch => {
                sanitized.push(ch);
                just_saw_carriage_return = false;
            }
        }
    }
    sanitized
}

fn format_shell_result(result: &ShellResult) -> String {
    let mut body = format!(
        "exit_code={}\n",
        result.exit_code.map(|value| value.to_string()).unwrap_or_else(|| "unknown".to_owned())
    );
    if !result.stdout.trim().is_empty() {
        body.push_str("stdout:\n");
        body.push_str(result.stdout.trim());
        body.push('\n');
    }
    if !result.stderr.trim().is_empty() {
        body.push_str("stderr:\n");
        body.push_str(result.stderr.trim());
    }
    if body.trim().is_empty() {
        "no output".to_owned()
    } else {
        body.trim_end().to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        display_session_identity, parse_toggle, parse_tui_objective_create_spec,
        parse_tui_objective_kind, sanitize_terminal_text, App, Focus, Mode,
    };
    use crate::proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1};

    fn test_app() -> App {
        App {
            runtime: crate::client::operator::OperatorRuntime::new(crate::AgentConnection {
                grpc_url: "http://127.0.0.1:7142".to_owned(),
                token: None,
                principal: "tester".to_owned(),
                device_id: "dev-1".to_owned(),
                channel: "cli".to_owned(),
                trace_id: "trace-1".to_owned(),
            }),
            session: gateway_v1::SessionSummary::default(),
            current_agent: None,
            current_agent_source: "test",
            models: None,
            input: String::new(),
            transcript: Vec::new(),
            active_stream: None,
            pending_approval: None,
            pending_shell_command: None,
            pending_picker: None,
            focus: Focus::Input,
            mode: Mode::Chat,
            show_tools: true,
            show_thinking: true,
            local_shell_enabled: false,
            allow_sensitive_tools: false,
            include_archived_sessions: false,
            last_run_id: None,
            selected_objective_id: None,
            scroll_offset: 0,
            status_line: String::new(),
            settings_selected: 0,
        }
    }

    #[test]
    fn sanitize_terminal_text_replaces_control_characters() {
        let sanitized = sanitize_terminal_text("ok\x1b[31mwarn\x07\r\nnext\tline");
        assert_eq!(sanitized, "ok<ESC>[31mwarn<U+0007>\nnext<U+0009>line");
    }

    #[test]
    fn parse_toggle_accepts_explicit_values() {
        assert!(parse_toggle(Some("on"), false).expect("on should parse"));
        assert!(!parse_toggle(Some("off"), true).expect("off should parse"));
    }

    #[test]
    fn display_session_identity_prefers_label() {
        let summary = gateway_v1::SessionSummary {
            session_id: Some(common_v1::CanonicalId {
                ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            }),
            session_key: "ops:triage".to_owned(),
            session_label: "Ops Triage".to_owned(),
            title: "Auto title".to_owned(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
            last_run_id: None,
            archived_at_unix_ms: 0,
            ..Default::default()
        };
        let display = display_session_identity(&summary);
        assert_eq!(display, "Auto title");
    }

    #[test]
    fn handle_stream_event_sanitizes_status_and_assistant_output() {
        let mut app = test_app();
        app.handle_stream_event(common_v1::RunStreamEvent {
            run_id: Some(common_v1::CanonicalId { ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned() }),
            body: Some(common_v1::run_stream_event::Body::Status(common_v1::StreamStatus {
                kind: 0,
                message: "phase\x1b[2J\r\nnext".to_owned(),
            })),
            ..Default::default()
        })
        .expect("status event should be accepted");
        app.handle_stream_event(common_v1::RunStreamEvent {
            run_id: Some(common_v1::CanonicalId { ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned() }),
            body: Some(common_v1::run_stream_event::Body::ModelToken(common_v1::ModelToken {
                token: "hello\x1b]52;c;ZXZpbA==\x07".to_owned(),
                is_final: false,
            })),
            ..Default::default()
        })
        .expect("token event should be accepted");

        assert_eq!(app.status_line, "phase<ESC>[2J\nnext");
        assert_eq!(app.transcript.len(), 2);
        assert_eq!(app.transcript[0].body, "phase<ESC>[2J\nnext");
        assert_eq!(app.transcript[1].body, "hello<ESC>]52;c;ZXZpbA==<U+0007>");
        assert!(app
            .transcript
            .iter()
            .flat_map(|entry| entry.title.chars().chain(entry.body.chars()))
            .all(|ch| !ch.is_control() || ch == '\n'));
    }

    #[test]
    fn handle_stream_event_sanitizes_approval_requests_before_storage() {
        let mut app = test_app();
        app.handle_stream_event(common_v1::RunStreamEvent {
            run_id: Some(common_v1::CanonicalId { ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned() }),
            body: Some(common_v1::run_stream_event::Body::ToolApprovalRequest(
                common_v1::ToolApprovalRequest {
                    tool_name: "shell\x1b[31m".to_owned(),
                    request_summary: "run\x07 dangerous\rcommand".to_owned(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        })
        .expect("approval event should be accepted");

        let approval = app.pending_approval.as_ref().expect("approval should be stored");
        assert_eq!(app.status_line, "Approval required for shell<ESC>[31m");
        assert_eq!(approval.tool_name, "shell<ESC>[31m");
        assert_eq!(approval.request_summary, "run<U+0007> dangerous\ncommand");
        assert_eq!(app.transcript.len(), 1);
        assert_eq!(app.transcript[0].title, "Approval requested: shell<ESC>[31m");
        assert_eq!(app.transcript[0].body, "run<U+0007> dangerous\ncommand");
    }

    #[test]
    fn parse_tui_objective_kind_accepts_product_aliases() {
        assert_eq!(parse_tui_objective_kind("objective").unwrap(), "objective");
        assert_eq!(parse_tui_objective_kind("heartbeat").unwrap(), "heartbeat");
        assert_eq!(parse_tui_objective_kind("standing-order").unwrap(), "standing_order");
        assert_eq!(parse_tui_objective_kind("program").unwrap(), "program");
    }

    #[test]
    fn parse_tui_objective_create_spec_supports_inline_prompt_separator() {
        let spec = parse_tui_objective_create_spec(
            None,
            &[
                "create".to_owned(),
                "heartbeat".to_owned(),
                "Ops".to_owned(),
                "status".to_owned(),
                "::".to_owned(),
                "Summarize".to_owned(),
                "current".to_owned(),
                "state".to_owned(),
            ],
        )
        .unwrap();
        assert_eq!(spec.kind, "heartbeat");
        assert_eq!(spec.name, "Ops status");
        assert_eq!(spec.prompt, "Summarize current state");
    }
}
