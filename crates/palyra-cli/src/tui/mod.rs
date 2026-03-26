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
    pub(crate) session_id: Option<String>,
    pub(crate) session_key: Option<String>,
    pub(crate) session_label: Option<String>,
    pub(crate) require_existing: bool,
    pub(crate) allow_sensitive_tools: bool,
    pub(crate) include_archived_sessions: bool,
}

impl std::fmt::Debug for LaunchOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaunchOptions")
            .field("connection", &"<redacted>")
            .field("session_id", &self.session_id.is_some())
            .field("session_key", &self.session_key.is_some())
            .field("session_label", &self.session_label.is_some())
            .field("require_existing", &self.require_existing)
            .field("allow_sensitive_tools", &self.allow_sensitive_tools)
            .field("include_archived_sessions", &self.include_archived_sessions)
            .finish()
    }
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
    scroll_offset: u16,
    status_line: String,
    settings_selected: usize,
}

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
            .resolve_session(gateway_v1::ResolveSessionRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: options
                    .session_id
                    .map(|value| resolve_or_generate_canonical_id(Some(value)))
                    .transpose()?
                    .map(|ulid| common_v1::CanonicalId { ulid }),
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
            scroll_offset: 0,
            status_line: "Connected".to_owned(),
            settings_selected: 0,
        };
        app.refresh_agent_identity(None, false).await?;
        match app.runtime.list_models(None) {
            Ok(models) => app.models = Some(models),
            Err(error) => {
                app.status_line = format!("Connected; model catalog unavailable: {error}")
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
                    self.status_line = format!("Run failed: {error}");
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
                self.status_line = status.message.clone();
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
            Some(common_v1::run_stream_event::Body::ToolApprovalRequest(approval)) => {
                self.status_line = format!(
                    "Approval required for {}",
                    if approval.tool_name.trim().is_empty() {
                        "tool"
                    } else {
                        approval.tool_name.as_str()
                    }
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
        if let Some(last) = self.transcript.last_mut() {
            if matches!(last.kind, EntryKind::Assistant) && last.title == title {
                last.body.push_str(token);
                return;
            }
        }
        self.transcript.push(TranscriptEntry {
            kind: EntryKind::Assistant,
            title,
            body: token.to_owned(),
        });
    }

    fn push_entry<T: Into<String>, U: Into<String>>(&mut self, kind: EntryKind, title: T, body: U) {
        self.transcript.push(TranscriptEntry { kind, title: title.into(), body: body.into() });
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
        let request = build_agent_run_input(AgentRunInputArgs {
            session_id: self.session.session_id.clone(),
            session_key: None,
            session_label: None,
            require_existing: true,
            reset_session: false,
            run_id: None,
            prompt: value,
            allow_sensitive_tools: self.allow_sensitive_tools,
        })?;
        let stream = self.runtime.start_run_stream(request).await?;
        self.last_run_id = Some(stream.run_id().to_owned());
        self.active_stream = Some(stream);
        self.scroll_offset = 0;
        Ok(())
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
            "model" => {
                if let Some(model_id) = parts.next() {
                    self.set_model(model_id.to_owned()).await?;
                } else {
                    self.open_picker(PickerKind::Model).await?;
                }
            }
            "reset" => self.reset_session().await?,
            "abort" => {
                let explicit = parts.next().map(ToOwned::to_owned);
                self.abort_run(explicit).await?;
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
            .resolve_agent_for_context(gateway_v1::ResolveAgentForContextRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
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
            gateway_v1::ResolveSessionRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId {
                    ulid: resolve_or_generate_canonical_id(Some(reference))?,
                }),
                session_key: String::new(),
                session_label: String::new(),
                require_existing: true,
                reset_session: false,
            }
        } else {
            gateway_v1::ResolveSessionRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
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
            .resolve_session(gateway_v1::ResolveSessionRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
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

    async fn open_picker(&mut self, kind: PickerKind) -> Result<()> {
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
            PickerKind::Session => {
                let response = self
                    .runtime
                    .list_sessions(None, self.include_archived_sessions, Some(100))
                    .await?;
                let items = response
                    .sessions
                    .into_iter()
                    .map(|session| {
                        let session_id = session
                            .session_id
                            .as_ref()
                            .map(|value| value.ulid.clone())
                            .unwrap_or_default();
                        PickerItem {
                            id: session_id.clone(),
                            title: display_session_identity(&session),
                            detail: format!(
                                "updated={} | archived={}",
                                session.updated_at_unix_ms,
                                session.archived_at_unix_ms > 0
                            ),
                        }
                    })
                    .collect::<Vec<_>>();
                PickerState { kind, title: "Session picker".to_owned(), selected: 0, items }
            }
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
            .resolve_agent_for_context(gateway_v1::ResolveAgentForContextRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
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
            "session={} agent={} source={} model={} tools={} thinking={} shell={}",
            display_session_identity(&self.session),
            self.current_agent.as_ref().map(|agent| agent.agent_id.as_str()).unwrap_or("none"),
            self.current_agent_source,
            self.models
                .as_ref()
                .and_then(|models| models.status.text_model.as_deref())
                .unwrap_or("none"),
            self.show_tools,
            self.show_thinking,
            self.local_shell_enabled
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
        Span::raw(app.status_line.as_str()),
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

fn render_help_popup(frame: &mut Frame<'_>, area: Rect) {
    let popup = centered_rect(72, 14, area);
    let text = Text::from(vec![
        Line::from("Slash commands"),
        Line::from("  /help /status /agent [/id] /session [/id-or-key] /model [/id]"),
        Line::from("  /reset /abort [run_id] /settings /tools on|off /thinking on|off"),
        Line::from("  /shell on|off /exit"),
        Line::default(),
        Line::from("Pickers"),
        Line::from("  F2 agent  F3 session  F4 model"),
        Line::from("  F5 settings  Ctrl+R reload runtime state"),
        Line::default(),
        Line::from("Controls"),
        Line::from("  Tab focus  Enter submit/select  Esc close overlay  q quit"),
        Line::default(),
        Line::from("Tool output and thinking visibility can be toggled without restart."),
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

fn render_approval_popup(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let popup = centered_rect(72, 10, area);
    let body = if let Some(approval) = app.pending_approval.as_ref() {
        Text::from(vec![
            Line::from(vec![
                Span::styled(
                    "Tool ",
                    Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                ),
                Span::raw(approval.tool_name.as_str()),
            ]),
            Line::default(),
            Line::from(approval.request_summary.as_str()),
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
    if !session.session_label.trim().is_empty() {
        return "labeled session".to_owned();
    }
    if !session.session_key.trim().is_empty() {
        return "keyed session".to_owned();
    }
    if session.session_id.is_some() {
        "session".to_owned()
    } else {
        "unknown session".to_owned()
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
    use super::{display_session_identity, parse_toggle};
    use crate::proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1};

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
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
            last_run_id: None,
            archived_at_unix_ms: 0,
        };
        let display = display_session_identity(&summary);
        assert_eq!(display, "labeled session");
    }
}
