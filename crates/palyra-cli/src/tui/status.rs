//! Session status, context-budget, and recap summaries for the TUI.
//!
//! Aggregates usage and transcript metadata from the console API into
//! `SessionRuntimeSnapshot` and renders the `/status` summary blocks shown
//! in the header, footer, and status-detail popup.

use super::*;

impl App {
    /// Refreshes [`SessionRuntimeSnapshot`] from the usage and transcript APIs.
    ///
    /// Partial data is still applied so the UI degrades gracefully.
    ///
    /// # Errors
    /// Returns an error describing whichever of the two fetches failed; the
    /// snapshot built from the surviving source is applied regardless.
    pub(super) async fn refresh_session_runtime_snapshot(&mut self) -> Result<()> {
        let session_id = self.active_session_id()?;
        let context = self.connect_admin_console().await?;
        let mut snapshot = SessionRuntimeSnapshot {
            session_total_tokens: self
                .current_session_catalog
                .as_ref()
                .map(|session| session.total_tokens)
                .unwrap_or_default(),
            latest_started_at_unix_ms: self
                .current_session_catalog
                .as_ref()
                .and_then(|session| session.last_run_started_at_unix_ms),
            ..SessionRuntimeSnapshot::default()
        };
        let usage_path = format!(
            "console/v1/usage/sessions/{}?run_limit=8",
            percent_encode_component(session_id.as_str())
        );
        let usage_result = context.client.get_json_value(usage_path).await;
        let transcript_path = format!(
            "console/v1/chat/sessions/{}/transcript",
            percent_encode_component(session_id.as_str())
        );
        let transcript_result = context.client.get_json_value(transcript_path).await;

        if let Ok(usage) = usage_result.as_ref() {
            // Session, totals, and catalog counters can trail each other while
            // a run streams; take the max so the display never goes backwards.
            snapshot.session_total_tokens = read_json_u64(usage, "/session/total_tokens")
                .max(read_json_u64(usage, "/totals/total_tokens"))
                .max(snapshot.session_total_tokens);
            snapshot.session_runs = read_json_u64(usage, "/session/runs") as usize;
            snapshot.average_latency_ms = read_json_f64(usage, "/session/average_latency_ms")
                .or_else(|| read_json_f64(usage, "/totals/average_latency_ms"));
            snapshot.estimated_cost_usd = read_json_f64(usage, "/session/estimated_cost_usd")
                .or_else(|| read_json_f64(usage, "/totals/estimated_cost_usd"));
            snapshot.latest_started_at_unix_ms =
                read_json_optional_i64(usage, "/session/latest_started_at_unix_ms")
                    .or(snapshot.latest_started_at_unix_ms);
            if let Some(latest_run) = usage
                .pointer("/runs")
                .and_then(serde_json::Value::as_array)
                .and_then(|runs| runs.first())
            {
                snapshot.latest_run_total_tokens = read_json_u64(latest_run, "/total_tokens");
                snapshot.latest_started_at_unix_ms =
                    read_json_optional_i64(latest_run, "/started_at_unix_ms")
                        .or(snapshot.latest_started_at_unix_ms);
                snapshot.latest_completed_at_unix_ms =
                    read_json_optional_i64(latest_run, "/completed_at_unix_ms");
            }
        }

        if let Ok(transcript) = transcript_result.as_ref() {
            snapshot.attachment_count = transcript
                .pointer("/attachments")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
                .unwrap_or_default();
            let tasks = transcript
                .pointer("/background_tasks")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            snapshot.background_task_count = tasks.len();
            snapshot.active_background_task_count = tasks
                .iter()
                .filter(|task| background_task_is_active(read_json_string(task, "/state").as_str()))
                .count();
            if snapshot.latest_completed_at_unix_ms.is_none() {
                snapshot.latest_completed_at_unix_ms = transcript
                    .pointer("/runs")
                    .and_then(serde_json::Value::as_array)
                    .and_then(|runs| runs.first())
                    .and_then(|run| read_json_optional_i64(run, "/completed_at_unix_ms"));
            }
        }

        self.session_runtime = snapshot;
        match (usage_result.err(), transcript_result.err()) {
            (Some(usage_error), Some(transcript_error)) => Err(anyhow!(
                "usage metadata unavailable: {usage_error}; transcript metadata unavailable: {transcript_error}"
            )),
            (Some(error), None) => Err(anyhow!("usage metadata unavailable: {error}")),
            (None, Some(error)) => Err(anyhow!("transcript metadata unavailable: {error}")),
            (None, None) => Ok(()),
        }
    }

    /// Estimates the context budget from session tokens, the current draft,
    /// project context, and queued attachments.
    pub(super) fn context_budget_summary(&self) -> ContextBudgetSummary {
        let baseline_tokens = self
            .current_session_catalog
            .as_ref()
            .map(|session| session.total_tokens)
            .unwrap_or_default()
            .max(self.session_runtime.session_total_tokens);
        let draft_tokens = estimate_text_tokens(self.composer.text());
        let project_context_tokens = self
            .current_session_catalog
            .as_ref()
            .and_then(|session| session.recap.project_context.as_ref())
            .map(|project_context| project_context.active_estimated_tokens as u64)
            .unwrap_or_default();
        let attachment_tokens =
            self.pending_attachments.iter().map(|attachment| attachment.budget_tokens).sum::<u64>();
        let estimated_total_tokens =
            baseline_tokens + draft_tokens + project_context_tokens + attachment_tokens;
        let ratio = if CONTEXT_BUDGET_HARD_LIMIT == 0 {
            0.0
        } else {
            (estimated_total_tokens as f64) / (CONTEXT_BUDGET_HARD_LIMIT as f64)
        };
        let tone = if estimated_total_tokens >= CONTEXT_BUDGET_HARD_LIMIT {
            StatusTone::Danger
        } else if estimated_total_tokens >= CONTEXT_BUDGET_SOFT_LIMIT {
            StatusTone::Warning
        } else {
            StatusTone::Default
        };
        let warning = match tone {
            StatusTone::Danger => Some(
                "Estimated context is above the safe working budget. Branch or compact before another heavy turn."
                    .to_owned(),
            ),
            StatusTone::Warning => Some(
                "Estimated context is approaching the budget. Keep the next turn compact."
                    .to_owned(),
            ),
            StatusTone::Default => None,
        };
        ContextBudgetSummary {
            draft_tokens,
            project_context_tokens,
            attachment_tokens,
            estimated_total_tokens,
            limit_tokens: CONTEXT_BUDGET_HARD_LIMIT,
            ratio,
            tone,
            label: format!(
                "{} / {} est.",
                format_approx_tokens(estimated_total_tokens),
                format_approx_tokens(CONTEXT_BUDGET_HARD_LIMIT)
            ),
            warning,
        }
    }

    /// Appends a "Recap" transcript entry for the active session, using the
    /// catalog entry when loaded and `fallback` otherwise.
    pub(super) fn push_session_recap(&mut self, fallback: Option<TuiSlashSessionRecord>) {
        if let Some(session) = self.current_session_catalog.as_ref() {
            let mut lines = Vec::new();
            let workspace_checkpoint_count = self.slash_entity_catalog.workspace_checkpoints.len();
            let summary =
                session.last_summary.clone().or_else(|| session.preview.clone()).unwrap_or_else(
                    || "No stored recap is available for this session yet.".to_owned(),
                );
            lines.push(summary);
            lines.push(format!(
                "Approvals {} · Artifacts {} · Context {} · Rollback {} · Family {}/{}",
                session.pending_approvals,
                session.artifact_count,
                session.recap.active_context_files.len(),
                workspace_checkpoint_count,
                session.family.sequence,
                session.family.family_size
            ));
            if !session.recap.recent_artifacts.is_empty() {
                lines.push(format!(
                    "Recent artifacts: {}",
                    session
                        .recap
                        .recent_artifacts
                        .iter()
                        .take(3)
                        .map(|artifact| artifact.label.as_str())
                        .collect::<Vec<_>>()
                        .join(" · ")
                ));
            }
            if !session.recap.touched_files.is_empty() {
                lines.push(format!(
                    "Touched: {}",
                    session
                        .recap
                        .touched_files
                        .iter()
                        .take(3)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(" · ")
                ));
            }
            if let Some(workspace_checkpoint) =
                self.slash_entity_catalog.workspace_checkpoints.first()
            {
                lines.push(format!(
                    "Workspace: /workspace · /workspace show <artifact> · /rollback diff {}",
                    workspace_checkpoint.checkpoint_id
                ));
            } else if session.artifact_count > 0 {
                lines.push("Workspace: /workspace lists changed files and `/workspace handoff` jumps into the web inspector.".to_owned());
            }
            if !session.recap.ctas.is_empty() {
                lines.push(format!("Quick actions: {}", session.recap.ctas.join(" · ")));
            }
            if session.family.parent_session_id.is_some() || !session.family.relatives.is_empty() {
                let mut family_actions = Vec::new();
                if session.family.parent_session_id.is_some() {
                    family_actions.push("/resume parent".to_owned());
                }
                if session.family.relatives.iter().any(|relative| relative.relation == "sibling") {
                    family_actions.push("/resume sibling".to_owned());
                }
                if session.family.relatives.iter().any(|relative| relative.relation == "child") {
                    family_actions.push("/resume child".to_owned());
                }
                family_actions.push(format!("/history {}", session.family.root_title));
                lines.push(format!("Family: {}", family_actions.join(" · ")));
            }
            self.push_entry(EntryKind::System, "Recap", lines.join("\n"));
            return;
        }
        if let Some(recap_session) = fallback {
            let recap = if !recap_session.last_summary.is_empty() {
                recap_session.last_summary
            } else if !recap_session.preview.is_empty() {
                recap_session.preview
            } else if recap_session.root_title != recap_session.title {
                recap_session.root_title
            } else {
                "No stored recap is available for this session yet.".to_owned()
            };
            self.push_entry(EntryKind::System, "Recap", recap);
        }
    }

    /// Mirrors the session quick-control toggles into the local view flags.
    pub(super) fn sync_session_quick_controls_from_catalog(&mut self) {
        let Some(session) = self.current_session_catalog.as_ref() else {
            return;
        };
        self.show_tools = session.quick_controls.trace.value;
        self.show_thinking = session.quick_controls.thinking.value;
        self.show_verbose = session.quick_controls.verbose.value;
    }

    /// Display label for the effective session model (override or inherited).
    pub(super) fn current_session_model_display(&self) -> String {
        let quick_control_display = self
            .current_session_catalog
            .as_ref()
            .map(|session| session.quick_controls.model.display_value.as_str());
        let override_active = self.current_session_model_override_active();
        model_display_from_sources(
            override_active,
            quick_control_display,
            effective_runtime_model_display(self.models.as_ref()),
        )
        .unwrap_or_else(|| "none".to_owned())
    }

    /// Display label for the effective session agent (override or resolved).
    pub(super) fn current_session_agent_display(&self) -> String {
        self.current_session_catalog
            .as_ref()
            .map(|session| session.quick_controls.agent.display_value.trim())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| self.current_agent.as_ref().map(|agent| agent.agent_id.clone()))
            .unwrap_or_else(|| "none".to_owned())
    }

    /// Whether the session carries an explicit model override.
    pub(super) fn current_session_model_override_active(&self) -> bool {
        self.current_session_catalog
            .as_ref()
            .is_some_and(|session| session.quick_controls.model.override_active)
    }

    /// Whether the session carries an explicit agent override.
    pub(super) fn current_session_agent_override_active(&self) -> bool {
        self.current_session_catalog
            .as_ref()
            .is_some_and(|session| session.quick_controls.agent.override_active)
    }

    /// Re-resolves the agent bound to this session context.
    ///
    /// # Errors
    /// Returns an error when the gateway agent resolution call fails.
    pub(super) async fn refresh_agent_identity(
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

    /// Reloads agent, model, and catalog state (Ctrl+R).
    ///
    /// # Errors
    /// Returns an error when agent, model, or catalog refreshes fail; the
    /// runtime-snapshot refresh is best-effort and never fails the reload.
    pub(super) async fn reload_runtime_state(&mut self) -> Result<()> {
        self.refresh_agent_identity(None, false).await?;
        self.models = Some(self.runtime.list_models(None)?);
        self.refresh_slash_entity_catalogs().await?;
        let _ = self.refresh_session_runtime_snapshot().await;
        self.sync_slash_palette();
        self.status_line = "Runtime state reloaded".to_owned();
        Ok(())
    }

    /// Drops cached browser profiles/sessions when browserd is unavailable.
    pub(super) fn clear_browser_catalog(&mut self) {
        self.slash_entity_catalog.browser_profiles.clear();
        self.slash_entity_catalog.browser_sessions.clear();
    }

    /// Multi-line `/status` summary block (session, budget, run, profile).
    pub(super) fn status_summary(&self) -> String {
        let handoff = build_console_handoff_path(&TuiCrossSurfaceHandoff {
            section: "chat".to_owned(),
            session_id: self.active_session_id().ok(),
            run_id: self.last_run_id.clone(),
            objective_id: self.selected_objective_id.clone(),
            source: Some("tui".to_owned()),
            ..TuiCrossSurfaceHandoff::default()
        });
        let profile =
            app::current_root_context().and_then(|context| context.active_profile_context());
        let budget = self.context_budget_summary();
        let workspace_artifacts = self.slash_entity_catalog.workspace_artifacts.len().max(
            self.current_session_catalog
                .as_ref()
                .map(|session| session.artifact_count)
                .unwrap_or_default(),
        );
        let workspace_checkpoints = self.slash_entity_catalog.workspace_checkpoints.len();
        let duration = current_run_duration_ms(self)
            .map(format_duration_ms)
            .unwrap_or_else(|| "idle".to_owned());
        [
            format!(
                "Session {}\nBranch {} · Agent {} ({}) [{}] · Model {} [{}]",
                display_session_identity(&self.session),
                if self.session.branch_state.trim().is_empty() {
                    "none"
                } else {
                    self.session.branch_state.as_str()
                },
                self.current_session_agent_display(),
                self.current_agent_source,
                if self.current_session_agent_override_active() {
                    "session"
                } else {
                    "inherited"
                },
                self.current_session_model_display(),
                if self.current_session_model_override_active() {
                    "session"
                } else {
                    "inherited"
                },
            ),
            format!(
                "Budget {} · Attach {} · Context {} · Approvals {} · Background {} · Workspace {}/{}",
                budget.label,
                self.pending_attachments.len(),
                self.current_session_catalog
                    .as_ref()
                    .map(|session| session.recap.active_context_files.len())
                    .unwrap_or_default(),
                self.current_session_catalog
                    .as_ref()
                    .map(|session| session.pending_approvals)
                    .unwrap_or_default(),
                self.session_runtime.active_background_task_count,
                workspace_artifacts,
                workspace_checkpoints,
            ),
            format!(
                "Run {} · Tokens {} · Cost {} · Connection {}",
                duration,
                format_approx_tokens(self.session_runtime.latest_run_total_tokens),
                self.session_runtime
                    .estimated_cost_usd
                    .map(format_cost_usd)
                    .unwrap_or_else(|| "n/a".to_owned()),
                connection_posture_label(self.runtime.connection().grpc_url.as_str()),
            ),
            format!(
                "{} · Tools {} · Thinking {} · Verbose {} · Shell {}",
                profile_header_summary(profile.as_ref()),
                self.show_tools,
                self.show_thinking,
                self.show_verbose,
                self.local_shell_enabled,
            ),
            format!("Handoff {handoff}"),
        ]
        .join("\n")
    }

    /// [`Self::status_summary`] extended with context/workspace/attachment detail
    /// for `/status detail`.
    pub(super) fn status_detail_summary(&self) -> String {
        let budget = self.context_budget_summary();
        let workspace_artifacts = self.slash_entity_catalog.workspace_artifacts.len().max(
            self.current_session_catalog
                .as_ref()
                .map(|session| session.artifact_count)
                .unwrap_or_default(),
        );
        let workspace_checkpoints = self.slash_entity_catalog.workspace_checkpoints.len();
        let latest_workspace_artifact = self
            .slash_entity_catalog
            .workspace_artifacts
            .first()
            .map(|artifact| artifact.display_path.as_str())
            .unwrap_or("none");
        let latest_workspace_checkpoint = self
            .slash_entity_catalog
            .workspace_checkpoints
            .first()
            .map(|checkpoint| checkpoint.checkpoint_id.as_str())
            .unwrap_or("none");
        [
            self.status_summary(),
            format!(
                "Context detail total={} / {} · draft={} · project_context={} · attachments={}",
                format_approx_tokens(budget.estimated_total_tokens),
                format_approx_tokens(budget.limit_tokens),
                format_approx_tokens(budget.draft_tokens),
                format_approx_tokens(budget.project_context_tokens),
                format_approx_tokens(budget.attachment_tokens),
            ),
            format!(
                "Workspace detail artifacts={} · checkpoints={} · latest artifact={} · latest rollback={}",
                workspace_artifacts,
                workspace_checkpoints,
                latest_workspace_artifact,
                if latest_workspace_checkpoint == "none" {
                    "none".to_owned()
                } else {
                    shorten_id(latest_workspace_checkpoint)
                },
            ),
            format!(
                "Session attachments stored={} · background queued={} active={}",
                self.session_runtime.attachment_count,
                self.session_runtime.background_task_count,
                self.session_runtime.active_background_task_count,
            ),
        ]
        .join("\n")
    }
}

/// Runtime-level model label: the default chat model, then the text model.
pub(super) fn effective_runtime_model_display(models: Option<&ModelsListPayload>) -> Option<&str> {
    models.and_then(|models| {
        models
            .status
            .default_chat_model_id
            .as_deref()
            .or(models.status.text_model.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    })
}

/// Picks the model label to display: an active session override wins;
/// otherwise the runtime default is preferred over the legacy quick-control
/// value.
pub(super) fn model_display_from_sources(
    override_active: bool,
    quick_control_display: Option<&str>,
    runtime_model_display: Option<&str>,
) -> Option<String> {
    let quick_control_display =
        quick_control_display.map(str::trim).filter(|value| !value.is_empty());
    if override_active {
        return quick_control_display.map(ToOwned::to_owned);
    }
    runtime_model_display
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or(quick_control_display)
        .map(ToOwned::to_owned)
}

/// Whether a browser-catalog refresh failure is expected when browserd is
/// disabled or unreachable, so the TUI degrades instead of erroring.
///
/// Console errors arrive untyped over HTTP, so message substrings are the
/// only signal available here.
pub(super) fn browser_catalog_optional_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("browser service is disabled")
        || message.contains("tool_call.browser_service.enabled=false")
        || message.contains("failed to connect to browser service")
        || message.contains("browser service connect failed")
        || message.contains("browserd is unavailable")
}
