use super::*;

#[derive(Debug, Default)]
pub(super) struct TuiCompactionCommand {
    pub(super) apply: bool,
    pub(super) history: bool,
    pub(super) accept_candidate_ids: Vec<String>,
    pub(super) reject_candidate_ids: Vec<String>,
}

pub(super) fn parse_tui_compaction_arguments(arguments: &[String]) -> Result<TuiCompactionCommand> {
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

pub(super) fn parse_tui_json_string(value: &str) -> Option<serde_json::Value> {
    serde_json::from_str::<serde_json::Value>(value).ok()
}

pub(super) fn estimate_text_tokens(text: &str) -> u64 {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return 0;
    }
    (trimmed.len() as u64).div_ceil(4)
}

pub(super) fn format_approx_tokens(value: u64) -> String {
    if value >= 1_000 {
        if value >= 10_000 {
            format!("{}k tok", value / 1_000)
        } else {
            format!("{:.1}k tok", (value as f64) / 1_000.0)
        }
    } else {
        format!("{value} tok")
    }
}

pub(super) fn format_size_bytes(value: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let value_f64 = value as f64;
    if value_f64 >= GIB {
        format!("{:.1} GiB", value_f64 / GIB)
    } else if value_f64 >= MIB {
        format!("{:.1} MiB", value_f64 / MIB)
    } else if value_f64 >= KIB {
        format!("{:.1} KiB", value_f64 / KIB)
    } else {
        format!("{value} B")
    }
}

pub(super) fn format_duration_ms(value: i64) -> String {
    let value = value.max(0);
    if value >= 60_000 {
        let minutes = value / 60_000;
        let seconds = (value % 60_000) / 1_000;
        format!("{minutes}m {seconds:02}s")
    } else if value >= 1_000 {
        format!("{:.1}s", (value as f64) / 1_000.0)
    } else {
        format!("{value}ms")
    }
}

pub(super) fn format_cost_usd(value: f64) -> String {
    if value >= 1.0 {
        format!("${value:.2}")
    } else {
        format!("${value:.4}")
    }
}

pub(super) fn guess_content_type(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "bmp" => "image/bmp",
        "txt" | "md" | "rs" | "toml" | "json" | "yaml" | "yml" | "ts" | "tsx" | "js" | "jsx"
        | "py" | "sh" | "ps1" => "text/plain",
        "pdf" => "application/pdf",
        "csv" => "text/csv",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "ogg" => "audio/ogg",
        "mp4" => "video/mp4",
        "mov" => "video/quicktime",
        "webm" => "video/webm",
        _ => "application/octet-stream",
    }
}

pub(super) fn attachment_kind_label(content_type: &str) -> &'static str {
    if content_type.starts_with("image/") {
        "image"
    } else if content_type.starts_with("audio/") {
        "audio"
    } else if content_type.starts_with("video/") {
        "video"
    } else {
        "file"
    }
}

pub(super) fn attachment_kind_to_proto(
    kind: &str,
) -> common_v1::message_attachment::AttachmentKind {
    match kind.to_ascii_lowercase().as_str() {
        "image" => common_v1::message_attachment::AttachmentKind::Image,
        "audio" => common_v1::message_attachment::AttachmentKind::Audio,
        "video" => common_v1::message_attachment::AttachmentKind::Video,
        "file" => common_v1::message_attachment::AttachmentKind::File,
        _ => common_v1::message_attachment::AttachmentKind::Unspecified,
    }
}

pub(super) fn read_json_optional_i64(value: &serde_json::Value, pointer: &str) -> Option<i64> {
    value.pointer(pointer).and_then(serde_json::Value::as_i64)
}

pub(super) fn read_json_u64(value: &serde_json::Value, pointer: &str) -> u64 {
    value.pointer(pointer).and_then(serde_json::Value::as_u64).unwrap_or_default()
}

pub(super) fn read_json_f64(value: &serde_json::Value, pointer: &str) -> Option<f64> {
    value.pointer(pointer).and_then(|entry| {
        entry
            .as_f64()
            .or_else(|| entry.as_i64().map(|number| number as f64))
            .or_else(|| entry.as_u64().map(|number| number as f64))
    })
}

pub(super) fn background_task_is_active(state: &str) -> bool {
    !matches!(
        state.trim().to_ascii_lowercase().as_str(),
        "" | "completed" | "failed" | "cancelled" | "canceled" | "succeeded"
    )
}

pub(super) fn map_session_catalog_record(session: SessionCatalogRecord) -> TuiSlashSessionRecord {
    TuiSlashSessionRecord {
        session_id: session.session_id,
        title: session.title,
        session_key: session.session_key,
        archived: session.archived,
        preview: session.preview.unwrap_or_default(),
        root_title: session.family.root_title,
        last_summary: session.last_summary.unwrap_or_default(),
        branch_state: session.branch_state,
        family_sequence: session.family.sequence,
        family_size: session.family.family_size,
        parent_session_id: session.family.parent_session_id,
        parent_title: session.family.parent_title,
        pending_approvals: session.pending_approvals,
        artifact_count: session.artifact_count,
        active_context_files: session.recap.active_context_files.len(),
        relatives: session
            .family
            .relatives
            .into_iter()
            .map(|relative| TuiSlashSessionRelative {
                session_id: relative.session_id,
                title: relative.title,
                branch_state: relative.branch_state,
                relation: relative.relation,
            })
            .collect(),
    }
}

pub(super) struct TuiObjectiveCreateSpec {
    pub(super) kind: &'static str,
    pub(super) name: String,
    pub(super) prompt: String,
}

pub(super) fn resolve_tui_objective_reference(
    explicit: Option<String>,
    selected: Option<&String>,
) -> Result<String> {
    explicit
        .or_else(|| selected.cloned())
        .context("Select an objective first or pass an explicit objective_id")
}

pub(super) fn parse_tui_objective_create_spec(
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

pub(super) fn parse_tui_objective_kind(value: &str) -> Result<&'static str> {
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

pub(super) fn render_help_popup(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let mut lines = vec![Line::from("Slash commands")];
    for line in render_shared_chat_command_synopsis_lines(SharedChatCommandSurface::Tui, 84) {
        lines.push(Line::from(line));
    }
    lines.extend([
        Line::default(),
        Line::from("Recommended now"),
        Line::from(discoverability_tip_line(app, 88)),
        Line::default(),
        Line::from("Multiline composer"),
        Line::from("  Enter send  Alt+Enter or Ctrl+J newline"),
        Line::from("  Ctrl+A select all  Ctrl+O seed /attach  Tab switch focus"),
    ]);
    lines.extend([
        Line::default(),
        Line::from("Daily-driver examples"),
        Line::from("  /attach ./logs/failure.txt   /resume parent   /status detail"),
        Line::from("  /workspace   /workspace show 1   /workspace handoff open"),
        Line::from("  /rollback diff <checkpoint>   /rollback restore <checkpoint> --confirm"),
        Line::default(),
        Line::from("Context references"),
        Line::from(
            "  @file:PATH @folder:PATH @diff[:PATH] @staged[:PATH] @url:URL @memory:\"query\"",
        ),
        Line::from("  Escape a literal at-sign with @@"),
        Line::default(),
        Line::from("Pickers"),
        Line::from("  F2 agent  F3 session  F4 model"),
        Line::from("  F5 settings  Ctrl+R reload runtime state  F8 toggle tips"),
        Line::default(),
        Line::from("Controls"),
        Line::from("  Tab focus or accept slash suggestion  Up/Down navigate palette or composer"),
        Line::from("  q quit when the composer is empty  Esc close overlay  /status detail expands context"),
        Line::default(),
        Line::from(
            "Retry, undo, attachments, recap family navigation, workspace explorer, rollback, and background tasks all reuse the console HTTP contracts.",
        ),
        Line::from("Voice remains desktop-first until the CLI path is fully stable."),
    ]);
    let popup_height = area.height.saturating_sub(2).min(lines.len() as u16 + 2).max(14);
    let popup = centered_rect(88, popup_height, area);
    let text = Text::from(lines);
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(text)
            .block(Block::default().borders(Borders::ALL).title("Help"))
            .alignment(Alignment::Left)
            .wrap(Wrap { trim: false }),
        popup,
    );
}

pub(super) fn percent_encode_component(value: &str) -> String {
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

pub(super) fn render_approval_popup(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let popup = centered_rect(88, 14, area);
    let body = if let Some(approval) = app.pending_approval.as_ref() {
        let mut lines = vec![
            Line::from(vec![
                Span::styled(
                    "Tool ",
                    Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                ),
                Span::raw(sanitize_terminal_text(approval.tool_name.as_str())),
            ]),
            Line::default(),
            Line::from(sanitize_terminal_text(approval.request_summary.as_str())),
        ];
        if let Some(prompt) = approval.prompt.as_ref() {
            lines.push(Line::from(text::approval_risk(
                app.locale,
                approval_risk_level_to_text(prompt.risk_level),
            )));
            if !prompt.policy_explanation.trim().is_empty() {
                lines.push(Line::from(text::approval_policy(
                    app.locale,
                    prompt.policy_explanation.as_str(),
                )));
            }
            if !prompt.summary.trim().is_empty() {
                lines.push(Line::from(prompt.summary.clone()));
            }
        }
        lines.push(Line::default());
        lines.push(Line::from(text::approval_manage_posture_hint(app.locale)));
        lines.push(Line::default());
        lines.push(Line::from(text::approval_allow_once_hint(app.locale)));
        lines.push(Line::from(text::approval_deny_hint(app.locale)));
        Text::from(lines)
    } else {
        Text::from(text::approval_request_unavailable(app.locale))
    };
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(body)
            .block(Block::default().borders(Borders::ALL).title("Approval Required"))
            .wrap(Wrap { trim: false }),
        popup,
    );
}

pub(super) fn approval_risk_level_to_text(raw: i32) -> &'static str {
    match common_v1::ApprovalRiskLevel::try_from(raw)
        .unwrap_or(common_v1::ApprovalRiskLevel::Unspecified)
    {
        common_v1::ApprovalRiskLevel::Low => "low",
        common_v1::ApprovalRiskLevel::Medium => "medium",
        common_v1::ApprovalRiskLevel::High => "high",
        common_v1::ApprovalRiskLevel::Critical => "critical",
        common_v1::ApprovalRiskLevel::Unspecified => "unspecified",
    }
}

pub(super) fn render_picker_popup(frame: &mut Frame<'_>, area: Rect, app: &App) {
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

pub(super) fn render_slash_palette_popup(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(palette) = app.pending_slash_palette.as_ref() else {
        return;
    };
    let preview = preview_for_selection(palette, app.slash_palette_selected);
    let preview_lines = if let Some(preview) = preview {
        vec![
            Line::from(vec![
                Span::styled(
                    preview.badge,
                    Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                Span::raw(preview.title),
            ]),
            Line::from(preview.subtitle),
            Line::from(preview.detail),
            Line::from(format!("Example: {}", preview.example)),
            Line::default(),
        ]
    } else {
        vec![
            Line::from("Slash command palette"),
            Line::from("Type a slash command, then use Up/Down and Tab."),
            Line::default(),
        ]
    };
    let mut lines = preview_lines;
    if palette.suggestions.is_empty() {
        lines.push(Line::from("No suggestions for the current token."));
    } else {
        for (index, suggestion) in palette.suggestions.iter().enumerate() {
            let selected = index == app.slash_palette_selected;
            let prefix = if selected { ">" } else { " " };
            lines.push(Line::from(Span::styled(
                format!("{prefix} [{}] {}", suggestion.badge, suggestion.title),
                if selected {
                    Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                },
            )));
            lines.push(Line::from(format!("  {}", suggestion.subtitle)));
            lines.push(Line::from(format!("  {}", suggestion.detail)));
            lines.push(Line::default());
        }
    }
    let popup = centered_rect(92, 18, area);
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(Block::default().borders(Borders::ALL).title("Slash palette"))
            .wrap(Wrap { trim: false }),
        popup,
    );
}

pub(super) fn render_settings_popup(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let popup = centered_rect(56, 12, area);
    let items = settings_items();
    let mut lines = Vec::new();
    for (index, item) in items.iter().enumerate() {
        let selected = index == app.settings_selected;
        let prefix = if selected { ">" } else { " " };
        let (label, enabled) = match item {
            SettingsItem::ShowTools => ("Show tool cards", app.show_tools),
            SettingsItem::ShowThinking => ("Show thinking/status lines", app.show_thinking),
            SettingsItem::ShowVerbose => ("Show verbose/system lines", app.show_verbose),
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

pub(super) fn render_shell_confirm_popup(frame: &mut Frame<'_>, area: Rect) {
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

pub(super) fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
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

pub(super) fn settings_items() -> [SettingsItem; 4] {
    [
        SettingsItem::ShowTools,
        SettingsItem::ShowThinking,
        SettingsItem::ShowVerbose,
        SettingsItem::LocalShell,
    ]
}

pub(super) fn entry_style(kind: &EntryKind) -> Style {
    match kind {
        EntryKind::User => Style::default().fg(Color::Cyan),
        EntryKind::Assistant => Style::default().fg(Color::White),
        EntryKind::Tool => Style::default().fg(Color::Yellow),
        EntryKind::Thinking => Style::default().fg(Color::DarkGray),
        EntryKind::System => Style::default().fg(Color::Green),
        EntryKind::Shell => Style::default().fg(Color::Magenta),
    }
}

pub(super) fn display_session_identity(session: &gateway_v1::SessionSummary) -> String {
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

pub(super) fn normalize_owned_optional_text(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

pub(super) fn shorten_id(value: &str) -> String {
    redacted_identifier_for_output(value)
}

pub(super) fn parse_toggle(value: Option<&str>, current: bool) -> Result<bool> {
    match value.unwrap_or("toggle") {
        "toggle" => Ok(!current),
        "on" | "true" | "yes" => Ok(true),
        "off" | "false" | "no" => Ok(false),
        other => Err(anyhow!("unsupported toggle value: {other}")),
    }
}

pub(super) fn quick_control_reset_requested(value: &str) -> bool {
    matches!(value.trim().to_ascii_lowercase().as_str(), "default" | "reset" | "inherit")
}

pub(super) fn looks_like_canonical_ulid(value: &str) -> bool {
    value.len() == 26 && value.chars().all(|ch| ch.is_ascii_alphanumeric())
}

pub(super) fn session_reference_equals(candidate: &str, reference: &str) -> bool {
    !candidate.trim().is_empty() && candidate.eq_ignore_ascii_case(reference)
}

pub(super) fn session_reference_matches(session: &TuiSlashSessionRecord, reference: &str) -> bool {
    session_reference_equals(session.session_id.as_str(), reference)
        || session_reference_equals(session.session_key.as_str(), reference)
        || session_reference_equals(session.title.as_str(), reference)
        || session_reference_equals(session.root_title.as_str(), reference)
}

pub(super) fn describe_branch_state_label(branch_state: &str) -> String {
    match branch_state {
        "root" => "root".to_owned(),
        "active_branch" | "branched" => "branch".to_owned(),
        "branch_source" => "branch source".to_owned(),
        "missing" => "no lineage".to_owned(),
        other => other.replace('_', " "),
    }
}

pub(super) fn agent_resolution_source_label(raw: i32) -> &'static str {
    match gateway_v1::AgentResolutionSource::try_from(raw)
        .unwrap_or(gateway_v1::AgentResolutionSource::Unspecified)
    {
        gateway_v1::AgentResolutionSource::SessionBinding => "session_binding",
        gateway_v1::AgentResolutionSource::Default => "default",
        gateway_v1::AgentResolutionSource::Fallback => "fallback",
        gateway_v1::AgentResolutionSource::Unspecified => "unspecified",
    }
}

pub(super) async fn run_local_shell(command: String) -> Result<ShellResult> {
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

pub(super) fn truncate_text(mut value: String, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value;
    }
    value = value.chars().take(limit).collect::<String>();
    value.push_str("...");
    value
}

pub(super) fn sanitize_terminal_text(value: &str) -> String {
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

pub(super) fn format_shell_result(result: &ShellResult) -> String {
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
