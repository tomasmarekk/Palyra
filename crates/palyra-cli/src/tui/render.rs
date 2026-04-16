use super::*;

pub(super) fn render(frame: &mut Frame<'_>, app: &App) {
    let composer_view =
        app.composer.render(MAX_COMPOSER_VISIBLE_LINES, matches!(app.focus, Focus::Input));
    let input_height = estimate_input_height(app, &composer_view);
    let footer_height = if app.show_tips { 3 } else { 2 };
    let areas = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(input_height),
            Constraint::Length(footer_height),
        ])
        .split(frame.area());
    let header = areas[0];
    let main = areas[1];
    let input = areas[2];
    let footer = areas[3];

    render_header(frame, header, app);
    render_transcript(frame, main, app);
    render_input(frame, input, app, &composer_view);
    render_footer(frame, footer, app);

    match app.mode {
        Mode::Help => render_help_popup(frame, frame.area(), app),
        Mode::Approval => render_approval_popup(frame, frame.area(), app),
        Mode::Picker(_) => render_picker_popup(frame, frame.area(), app),
        Mode::Settings => render_settings_popup(frame, frame.area(), app),
        Mode::ShellConfirm => render_shell_confirm_popup(frame, frame.area()),
        Mode::Chat => {
            if app.pending_slash_palette.is_some() {
                render_slash_palette_popup(frame, frame.area(), app);
            }
        }
    }
}

pub(super) fn render_header(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let profile = app::current_root_context().and_then(|context| context.active_profile_context());
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1), Constraint::Length(1)])
        .split(area);
    let top = rows[0];
    let bottom = rows[1];
    let banner = rows[2];
    let connection_line = Line::from(vec![
        Span::styled("Gateway ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::raw(app.runtime.connection().grpc_url.as_str()),
        Span::raw("  "),
        Span::styled("Session ", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Span::raw(display_session_identity(&app.session)),
    ]);
    let status_line = Line::from(vec![
        Span::styled("Agent ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
        Span::raw(app.current_session_agent_display()),
        Span::raw("  "),
        Span::styled("Model ", Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD)),
        Span::raw(app.current_session_model_display()),
        Span::raw("  "),
        Span::styled("Status ", Style::default().fg(Color::Blue).add_modifier(Modifier::BOLD)),
        Span::raw(sanitize_terminal_text(app.status_line.as_str())),
    ]);
    let profile_line = if let Some(profile) = profile {
        Line::from(vec![
            Span::styled(
                "Profile ",
                Style::default().fg(Color::LightGreen).add_modifier(Modifier::BOLD),
            ),
            Span::raw(profile.label),
            Span::raw("  "),
            Span::styled(
                "Env ",
                Style::default().fg(Color::LightBlue).add_modifier(Modifier::BOLD),
            ),
            Span::raw(profile.environment),
            Span::raw("  "),
            Span::styled(
                "Risk ",
                Style::default().fg(Color::LightRed).add_modifier(Modifier::BOLD),
            ),
            Span::raw(profile.risk_level),
            Span::raw("  "),
            Span::styled(
                "Strict ",
                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
            ),
            Span::raw(if profile.strict_mode { "on" } else { "off" }),
        ])
    } else {
        Line::from(vec![
            Span::styled(
                "Profile ",
                Style::default().fg(Color::DarkGray).add_modifier(Modifier::BOLD),
            ),
            Span::raw("none"),
        ])
    };
    frame.render_widget(Paragraph::new(connection_line), top);
    frame.render_widget(Paragraph::new(status_line), bottom);
    frame.render_widget(Paragraph::new(profile_line), banner);
}

pub(super) fn render_transcript(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let mut lines = Vec::new();
    for entry in &app.transcript {
        if matches!(entry.kind, EntryKind::Tool) && !app.show_tools {
            continue;
        }
        if matches!(entry.kind, EntryKind::Thinking) && !app.show_thinking {
            continue;
        }
        if matches!(entry.kind, EntryKind::System) && !app.show_verbose {
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

pub(super) fn render_input(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    composer_view: &TuiComposerView,
) {
    let budget = app.context_budget_summary();
    let workspace_checkpoint_count = app.slash_entity_catalog.workspace_checkpoints.len();
    let block =
        Block::default().borders(Borders::ALL).title(if matches!(app.focus, Focus::Input) {
            format!(
                "Composer [focus · {} line{} · ws {}]",
                composer_view.total_lines,
                if composer_view.total_lines == 1 { "" } else { "s" },
                workspace_checkpoint_count
            )
        } else {
            format!(
                "Composer [{} line{} · ws {}]",
                composer_view.total_lines,
                if composer_view.total_lines == 1 { "" } else { "s" },
                workspace_checkpoint_count
            )
        });
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let mut lines = vec![Line::from(vec![
        Span::styled("Budget ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::styled(
            budget.label.clone(),
            match budget.tone {
                StatusTone::Default => Style::default(),
                StatusTone::Warning => Style::default().fg(Color::Yellow),
                StatusTone::Danger => Style::default().fg(Color::LightRed),
            },
        ),
        Span::raw("  "),
        Span::styled("Context ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
        Span::raw(
            app.current_session_catalog
                .as_ref()
                .map(|session| session.recap.active_context_files.len().to_string())
                .unwrap_or_else(|| "0".to_owned()),
        ),
        Span::raw("  "),
        Span::styled("Approvals ", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Span::raw(
            app.current_session_catalog
                .as_ref()
                .map(|session| session.pending_approvals.to_string())
                .unwrap_or_else(|| "0".to_owned()),
        ),
        Span::raw("  "),
        Span::styled("Attach ", Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD)),
        Span::raw(app.pending_attachments.len().to_string()),
    ])];
    if !app.pending_attachments.is_empty() {
        let preview = app
            .pending_attachments
            .iter()
            .take(2)
            .enumerate()
            .map(|(index, attachment)| {
                format!(
                    "{}={} · {} · {}",
                    index + 1,
                    attachment.filename,
                    format_size_bytes(attachment.size_bytes),
                    format_approx_tokens(attachment.budget_tokens)
                )
            })
            .collect::<Vec<_>>()
            .join("  ");
        lines.push(Line::from(format!(
            "Pending attachments: {}{}",
            preview,
            if app.pending_attachments.len() > 2 {
                format!("  +{}", app.pending_attachments.len() - 2)
            } else {
                String::new()
            }
        )));
    }
    lines.extend(composer_view.lines.clone());
    if let Some(warning) = budget.warning.as_ref() {
        lines.push(Line::from(Span::styled(
            warning.clone(),
            match budget.tone {
                StatusTone::Danger => Style::default().fg(Color::LightRed),
                StatusTone::Warning => Style::default().fg(Color::Yellow),
                StatusTone::Default => Style::default(),
            },
        )));
    }
    frame.render_widget(Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }), inner);
    if matches!(app.focus, Focus::Input) && matches!(app.mode, Mode::Chat) {
        let cursor_y = inner
            .y
            .saturating_add(1)
            .saturating_add(u16::from(!app.pending_attachments.is_empty()))
            .saturating_add(composer_view.cursor_y);
        frame.set_cursor_position((inner.x + composer_view.cursor_x, cursor_y));
    }
}

pub(super) fn render_footer(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            std::iter::repeat_n(Constraint::Length(1), area.height as usize).collect::<Vec<_>>(),
        )
        .split(area);
    let width = area.width as usize;
    if let Some(row) = rows.first().copied() {
        frame.render_widget(Paragraph::new(compact_status_bar_line(app, width)), row);
    }
    if let Some(row) = rows.get(1).copied() {
        frame.render_widget(Paragraph::new(compact_shortcut_line(app, width)), row);
    }
    if app.show_tips {
        if let Some(row) = rows.get(2).copied() {
            frame.render_widget(Paragraph::new(discoverability_tip_line(app, width)), row);
        }
    }
}

pub(super) fn strict_profile_blocks_local_shell() -> bool {
    app::current_root_context()
        .map(|context| context.strict_profile_mode() && !context.allow_strict_profile_actions)
        .unwrap_or(false)
}

pub(super) fn estimate_input_height(app: &App, composer_view: &TuiComposerView) -> u16 {
    let mut height = 2 + composer_view.lines.len() as u16 + 1;
    if !app.pending_attachments.is_empty() {
        height += 1;
    }
    if app.context_budget_summary().warning.is_some() {
        height += 1;
    }
    height.clamp(4, 10)
}

pub(super) fn compact_status_bar_line(app: &App, width: usize) -> String {
    let budget = app.context_budget_summary();
    let context_fill = (budget.ratio * 100.0).round().clamp(0.0, 999.0);
    let run_duration = current_run_duration_ms(app).map(format_duration_ms);
    let connection_posture = connection_posture_label(app.runtime.connection().grpc_url.as_str());
    let checkpoint_count = app.slash_entity_catalog.workspace_checkpoints.len();
    let workspace_artifact_count = app.slash_entity_catalog.workspace_artifacts.len().max(
        app.current_session_catalog
            .as_ref()
            .map(|session| session.artifact_count)
            .unwrap_or_default(),
    );
    let segments = vec![
        format!("ctx {:>3.0}%", context_fill),
        format!("tok {}", format_approx_tokens(app.session_runtime.latest_run_total_tokens)),
        format!("budget {}", budget.label),
        app.session_runtime
            .estimated_cost_usd
            .map(format_cost_usd)
            .map(|value| format!("cost {value}"))
            .unwrap_or_else(|| "cost n/a".to_owned()),
        run_duration.map(|value| format!("run {value}")).unwrap_or_else(|| "run idle".to_owned()),
        format!(
            "approvals {}",
            app.current_session_catalog
                .as_ref()
                .map(|session| session.pending_approvals)
                .unwrap_or_default()
        ),
        format!("bg {}", app.session_runtime.active_background_task_count),
        format!("ws {workspace_artifact_count}/{checkpoint_count}"),
        format!("agent {}", app.current_session_agent_display()),
        format!("model {}", app.current_session_model_display()),
        format!("conn {}", connection_posture),
    ];
    fit_segments_to_width(width, segments)
}

pub(super) fn compact_shortcut_line(app: &App, width: usize) -> String {
    let slash_hint =
        if app.pending_slash_palette.is_some() { "slash Up/Down + Tab" } else { "/ commands" };
    let line = format!(
        "Enter send · Alt+Enter/Ctrl+J newline · Ctrl+O attach · /status detail · /workspace · /rollback diff · F8 tips · {slash_hint}"
    );
    truncate_text(line, width.max(8))
}

pub(super) fn discoverability_tip_line(app: &App, width: usize) -> String {
    let raw = if app.pending_approval.is_some() {
        "Tip: a pending approval is waiting; use y / n or Esc to resolve it."
    } else if !app.pending_attachments.is_empty() {
        "Tip: attachments are queued for the next turn; use /attach remove <index> before sending."
    } else if app
        .current_session_catalog
        .as_ref()
        .map(|session| session.family.family_size > 1)
        .unwrap_or(false)
    {
        "Tip: use /resume parent, /resume sibling, /resume child, or /history <family> to move around the current title family."
    } else if !app.slash_entity_catalog.workspace_checkpoints.is_empty() {
        "Tip: /rollback diff <checkpoint-id> previews recoverable workspace changes before restore."
    } else if !app.slash_entity_catalog.checkpoints.is_empty() {
        "Tip: /undo restores the latest safe conversation checkpoint, and /checkpoint list shows the rest."
    } else {
        "Tip: Alt+Enter/Ctrl+J adds a newline, Ctrl+O primes attachment upload, and F8 hides or re-shows these hints. Voice remains desktop-first."
    };
    truncate_text(raw.to_owned(), width.max(8))
}

pub(super) fn current_run_duration_ms(app: &App) -> Option<i64> {
    let started_at = app.session_runtime.latest_started_at_unix_ms?;
    let finished_at = if app.active_stream.is_some() {
        now_unix_ms_i64().ok()
    } else {
        app.session_runtime.latest_completed_at_unix_ms
    };
    Some(finished_at.unwrap_or(started_at) - started_at)
}

pub(super) fn connection_posture_label(grpc_url: &str) -> &'static str {
    let normalized = grpc_url.to_ascii_lowercase();
    if normalized.contains("127.0.0.1")
        || normalized.contains("localhost")
        || normalized.contains("[::1]")
    {
        "local"
    } else {
        "remote"
    }
}

pub(super) fn fit_segments_to_width(width: usize, segments: Vec<String>) -> String {
    let mut line = String::new();
    for segment in segments.into_iter().filter(|segment| !segment.trim().is_empty()) {
        let candidate = if line.is_empty() { segment } else { format!("{line}  {segment}") };
        if candidate.chars().count() > width {
            break;
        }
        line = candidate;
    }
    if line.is_empty() {
        String::new()
    } else {
        line
    }
}
