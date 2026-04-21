use super::*;

pub(super) fn build_command_name_suggestions(query: &str) -> Vec<TuiSlashSuggestion> {
    let normalized_query = query.trim().to_ascii_lowercase();
    shared_chat_commands_for_surface(SharedChatCommandSurface::Tui)
        .into_iter()
        .filter(|command| {
            if normalized_query.is_empty() {
                return true;
            }
            command.name.contains(normalized_query.as_str())
                || command.aliases.iter().any(|alias| alias.contains(normalized_query.as_str()))
                || command
                    .keywords
                    .iter()
                    .any(|keyword| keyword.contains(normalized_query.as_str()))
        })
        .take(8)
        .map(|command| {
            let replacement = if command.synopsis.contains(' ') {
                format!("/{} ", command.name)
            } else {
                format!("/{}", command.name)
            };
            TuiSlashSuggestion {
                title: command.synopsis.clone(),
                subtitle: command.description.clone(),
                detail: command.example.clone(),
                example: command.example.clone(),
                replacement,
                badge: command.category.clone(),
            }
        })
        .collect()
}

pub(super) fn build_entity_suggestions(
    command: &'static SharedChatCommandDefinition,
    catalog: &TuiSlashEntityCatalog,
    normalized_rest: &str,
    active_token: &str,
    streaming: bool,
    delegation_profiles: &[&str],
    delegation_templates: &[&str],
) -> Vec<TuiSlashSuggestion> {
    match command.name.as_str() {
        "resume" | "history" => build_session_suggestions(command, catalog, active_token),
        "objective" => build_objective_suggestions(command, catalog, active_token),
        "profile" => build_profile_suggestions(command, catalog, active_token),
        "browser" => {
            let mut suggestions = build_browser_profile_suggestions(command, catalog, active_token);
            suggestions.extend(build_browser_session_suggestions(command, catalog, active_token));
            suggestions.truncate(8);
            suggestions
        }
        "delegate" => build_delegation_suggestions(
            command,
            active_token,
            delegation_profiles,
            delegation_templates,
        ),
        "checkpoint" => {
            build_checkpoint_suggestions(command, catalog, normalized_rest, active_token)
        }
        "rollback" => {
            build_workspace_rollback_suggestions(command, catalog, normalized_rest, active_token)
        }
        "undo" => build_undo_suggestions(command, catalog, active_token),
        "workspace" => build_workspace_suggestions(command, catalog, normalized_rest, active_token),
        "interrupt" => build_interrupt_suggestions(command, active_token, streaming),
        "doctor" => build_doctor_suggestions(command, active_token),
        "compact" => {
            build_static_suggestions(command, active_token, &["preview", "apply", "history"])
        }
        "tools" | "thinking" | "verbose" => {
            build_static_suggestions(command, active_token, &["on", "off", "default"])
        }
        "shell" => build_static_suggestions(command, active_token, &["on", "off"]),
        "export" => build_static_suggestions(command, active_token, &["json", "markdown"]),
        _ => Vec::new(),
    }
}

pub(super) fn build_session_suggestions(
    command: &SharedChatCommandDefinition,
    catalog: &TuiSlashEntityCatalog,
    active_token: &str,
) -> Vec<TuiSlashSuggestion> {
    let query = active_token.trim().to_ascii_lowercase();
    catalog
        .sessions
        .iter()
        .filter(|session| {
            query.is_empty()
                || session.session_id.to_ascii_lowercase().contains(query.as_str())
                || session.title.to_ascii_lowercase().contains(query.as_str())
                || session.session_key.to_ascii_lowercase().contains(query.as_str())
                || session.root_title.to_ascii_lowercase().contains(query.as_str())
                || session.last_summary.to_ascii_lowercase().contains(query.as_str())
                || session
                    .parent_title
                    .as_ref()
                    .map(|title| title.to_ascii_lowercase().contains(query.as_str()))
                    .unwrap_or(false)
                || session.relatives.iter().any(|relative| {
                    relative.title.to_ascii_lowercase().contains(query.as_str())
                        || relative.relation.to_ascii_lowercase().contains(query.as_str())
                })
        })
        .take(6)
        .map(|session| {
            let mut detail_parts = Vec::new();
            if !session.last_summary.is_empty() {
                detail_parts.push(session.last_summary.clone());
            } else if !session.preview.is_empty() {
                detail_parts.push(session.preview.clone());
            } else if !session.root_title.is_empty() && session.root_title != session.title {
                detail_parts.push(format!("Family root: {}", session.root_title));
            } else {
                detail_parts.push("Resume this session context.".to_owned());
            }
            detail_parts.push(format!(
                "approvals {} · artifacts {} · context {}",
                session.pending_approvals, session.artifact_count, session.active_context_files
            ));
            if let Some(parent_session_id) = session.parent_session_id.as_ref() {
                detail_parts.push(format!(
                    "parent {}",
                    session
                        .parent_title
                        .clone()
                        .unwrap_or_else(|| shorten_entity_id(parent_session_id.as_str()))
                ));
            }
            if !session.relatives.is_empty() {
                let family_preview = session
                    .relatives
                    .iter()
                    .take(2)
                    .map(|relative| {
                        let lineage_state = if relative.branch_state.is_empty() {
                            shorten_entity_id(relative.session_id.as_str())
                        } else {
                            relative.branch_state.clone()
                        };
                        format!("{} {} ({lineage_state})", relative.relation, relative.title)
                    })
                    .collect::<Vec<_>>()
                    .join(" · ");
                detail_parts.push(format!("family {family_preview}"));
            }
            TuiSlashSuggestion {
                title: session.title.clone(),
                subtitle: if session.session_key.is_empty() {
                    session.session_id.clone()
                } else {
                    match session.family_size {
                        0 | 1 => session.session_key.clone(),
                        size => format!(
                            "{} · family {}/{}",
                            session.session_key, session.family_sequence, size
                        ),
                    }
                },
                detail: detail_parts.join(" · "),
                example: format!("/{} {}", command.name, session.session_id),
                replacement: if command.name == "history" {
                    format!("/{} {}", command.name, session.title)
                } else {
                    format!("/{} {}", command.name, session.session_id)
                },
                badge: if session.archived {
                    "archived".to_owned()
                } else if session.branch_state.eq_ignore_ascii_case("active_branch") {
                    "branch".to_owned()
                } else {
                    "session".to_owned()
                },
            }
        })
        .collect()
}

pub(super) fn build_objective_suggestions(
    command: &SharedChatCommandDefinition,
    catalog: &TuiSlashEntityCatalog,
    active_token: &str,
) -> Vec<TuiSlashSuggestion> {
    let query = active_token.trim().to_ascii_lowercase();
    catalog
        .objectives
        .iter()
        .filter(|objective| {
            query.is_empty()
                || objective.objective_id.to_ascii_lowercase().contains(query.as_str())
                || objective.name.to_ascii_lowercase().contains(query.as_str())
                || objective.kind.to_ascii_lowercase().contains(query.as_str())
        })
        .take(6)
        .map(|objective| TuiSlashSuggestion {
            title: format!("{} · {}", objective.kind.replace('_', " "), objective.name),
            subtitle: objective.objective_id.clone(),
            detail: if objective.focus.is_empty() {
                "No focus recorded.".to_owned()
            } else {
                objective.focus.clone()
            },
            example: format!("/{} {}", command.name, objective.objective_id),
            replacement: format!("/{} {}", command.name, objective.objective_id),
            badge: objective.kind.clone(),
        })
        .collect()
}

pub(super) fn build_profile_suggestions(
    command: &SharedChatCommandDefinition,
    catalog: &TuiSlashEntityCatalog,
    active_token: &str,
) -> Vec<TuiSlashSuggestion> {
    let query = active_token.trim().to_ascii_lowercase();
    catalog
        .auth_profiles
        .iter()
        .filter(|profile| {
            query.is_empty()
                || profile.profile_id.to_ascii_lowercase().contains(query.as_str())
                || profile.profile_name.to_ascii_lowercase().contains(query.as_str())
                || profile.provider_kind.to_ascii_lowercase().contains(query.as_str())
        })
        .take(6)
        .map(|profile| TuiSlashSuggestion {
            title: profile.profile_name.clone(),
            subtitle: format!("{} · {}", profile.provider_kind, profile.scope_kind),
            detail: profile.profile_id.clone(),
            example: format!("/{} {}", command.name, profile.profile_id),
            replacement: format!("/{} {}", command.name, profile.profile_id),
            badge: "profile".to_owned(),
        })
        .collect()
}

pub(super) fn build_browser_profile_suggestions(
    command: &SharedChatCommandDefinition,
    catalog: &TuiSlashEntityCatalog,
    active_token: &str,
) -> Vec<TuiSlashSuggestion> {
    let query = active_token.trim().to_ascii_lowercase();
    catalog
        .browser_profiles
        .iter()
        .filter(|profile| {
            query.is_empty()
                || profile.profile_id.to_ascii_lowercase().contains(query.as_str())
                || profile.name.to_ascii_lowercase().contains(query.as_str())
        })
        .take(4)
        .map(|profile| TuiSlashSuggestion {
            title: profile.name.clone(),
            subtitle: profile.profile_id.clone(),
            detail: format!(
                "{} · {}",
                if profile.persistence_enabled { "persistent" } else { "ephemeral" },
                if profile.private_profile { "private" } else { "shared" }
            ),
            example: format!("/{} {}", command.name, profile.profile_id),
            replacement: format!("/{} {}", command.name, profile.profile_id),
            badge: "browser profile".to_owned(),
        })
        .collect()
}

pub(super) fn build_browser_session_suggestions(
    command: &SharedChatCommandDefinition,
    catalog: &TuiSlashEntityCatalog,
    active_token: &str,
) -> Vec<TuiSlashSuggestion> {
    let query = active_token.trim().to_ascii_lowercase();
    catalog
        .browser_sessions
        .iter()
        .filter(|session| {
            query.is_empty()
                || session.session_id.to_ascii_lowercase().contains(query.as_str())
                || session.title.to_ascii_lowercase().contains(query.as_str())
        })
        .take(4)
        .map(|session| TuiSlashSuggestion {
            title: session.title.clone(),
            subtitle: session.session_id.clone(),
            detail: "Inspect browser session detail in the transcript.".to_owned(),
            example: format!("/{} {}", command.name, session.session_id),
            replacement: format!("/{} {}", command.name, session.session_id),
            badge: "browser session".to_owned(),
        })
        .collect()
}

pub(super) fn build_delegation_suggestions(
    command: &SharedChatCommandDefinition,
    active_token: &str,
    delegation_profiles: &[&str],
    delegation_templates: &[&str],
) -> Vec<TuiSlashSuggestion> {
    let query = active_token.trim().to_ascii_lowercase();
    delegation_templates
        .iter()
        .map(|template| (*template, "template"))
        .chain(delegation_profiles.iter().map(|profile| (*profile, "profile")))
        .filter(|(value, _)| query.is_empty() || value.contains(&query))
        .take(8)
        .map(|(value, badge)| TuiSlashSuggestion {
            title: value.to_owned(),
            subtitle: badge.to_owned(),
            detail: "Complete the command with a delegated task prompt.".to_owned(),
            example: format!("/{} {} Summarize the latest operator findings.", command.name, value),
            replacement: format!("/{} {} ", command.name, value),
            badge: badge.to_owned(),
        })
        .collect()
}

pub(super) fn build_checkpoint_suggestions(
    command: &SharedChatCommandDefinition,
    catalog: &TuiSlashEntityCatalog,
    normalized_rest: &str,
    active_token: &str,
) -> Vec<TuiSlashSuggestion> {
    if !normalized_rest.starts_with("restore") {
        return build_static_suggestions(command, active_token, &["list", "restore", "save"]);
    }
    let query = active_token.trim().to_ascii_lowercase();
    catalog
        .checkpoints
        .iter()
        .filter(|checkpoint| {
            query.is_empty()
                || checkpoint.checkpoint_id.to_ascii_lowercase().contains(query.as_str())
                || checkpoint.name.to_ascii_lowercase().contains(query.as_str())
        })
        .take(6)
        .map(|checkpoint| TuiSlashSuggestion {
            title: checkpoint.name.clone(),
            subtitle: checkpoint.checkpoint_id.clone(),
            detail: if checkpoint.note.is_empty() {
                "Restore checkpoint into a new branch.".to_owned()
            } else {
                checkpoint.note.clone()
            },
            example: format!("/{} restore {}", command.name, checkpoint.checkpoint_id),
            replacement: format!("/{} restore {}", command.name, checkpoint.checkpoint_id),
            badge: if checkpoint_has_tag(checkpoint, "undo_safe") {
                "undo-safe".to_owned()
            } else {
                "checkpoint".to_owned()
            },
        })
        .collect()
}

pub(super) fn build_workspace_rollback_suggestions(
    command: &SharedChatCommandDefinition,
    catalog: &TuiSlashEntityCatalog,
    normalized_rest: &str,
    active_token: &str,
) -> Vec<TuiSlashSuggestion> {
    if normalized_rest.starts_with("diff") {
        return build_workspace_checkpoint_suggestions(command, catalog, active_token, "diff");
    }
    if normalized_rest.starts_with("restore") {
        return build_workspace_checkpoint_suggestions(command, catalog, active_token, "restore");
    }
    vec![
        TuiSlashSuggestion {
            title: "Workspace rollback summary".to_owned(),
            subtitle: command.synopsis.clone(),
            detail: "List workspace checkpoints and restore guidance for the latest run."
                .to_owned(),
            example: format!("/{}", command.name),
            replacement: format!("/{}", command.name),
            badge: command.category.clone(),
        },
        TuiSlashSuggestion {
            title: "Preview rollback diff".to_owned(),
            subtitle: "diff".to_owned(),
            detail: "Compare the current run against a workspace checkpoint before restoring."
                .to_owned(),
            example: format!("/{} diff ", command.name),
            replacement: format!("/{} diff ", command.name),
            badge: "diff".to_owned(),
        },
        TuiSlashSuggestion {
            title: "Restore workspace checkpoint".to_owned(),
            subtitle: "restore".to_owned(),
            detail: "Require `--confirm` before mutating the tracked workspace.".to_owned(),
            example: format!("/{} restore ", command.name),
            replacement: format!("/{} restore ", command.name),
            badge: "restore".to_owned(),
        },
    ]
}

pub(super) fn build_workspace_suggestions(
    command: &SharedChatCommandDefinition,
    catalog: &TuiSlashEntityCatalog,
    normalized_rest: &str,
    active_token: &str,
) -> Vec<TuiSlashSuggestion> {
    if normalized_rest.starts_with("show") {
        return build_workspace_artifact_suggestions(command, catalog, active_token, "show");
    }
    if normalized_rest.starts_with("open") {
        return build_workspace_artifact_suggestions(command, catalog, active_token, "open");
    }
    if normalized_rest.starts_with("handoff") {
        return vec![
            TuiSlashSuggestion {
                title: "Workspace handoff path".to_owned(),
                subtitle: "handoff".to_owned(),
                detail: "Print the matching web workspace handoff path for the current run."
                    .to_owned(),
                example: format!("/{} handoff", command.name),
                replacement: format!("/{} handoff", command.name),
                badge: command.category.clone(),
            },
            TuiSlashSuggestion {
                title: "Open workspace handoff".to_owned(),
                subtitle: "handoff open".to_owned(),
                detail: "Open the workspace handoff directly in the default browser.".to_owned(),
                example: format!("/{} handoff open", command.name),
                replacement: format!("/{} handoff open", command.name),
                badge: "browser".to_owned(),
            },
        ];
    }
    vec![
        TuiSlashSuggestion {
            title: "Workspace summary".to_owned(),
            subtitle: command.synopsis.clone(),
            detail: "List changed paths, checkpoints, and handoff options for the latest run."
                .to_owned(),
            example: format!("/{}", command.name),
            replacement: format!("/{}", command.name),
            badge: command.category.clone(),
        },
        TuiSlashSuggestion {
            title: "Changed workspace paths".to_owned(),
            subtitle: "changed".to_owned(),
            detail: "Show the latest changed files without opening artifact detail.".to_owned(),
            example: format!("/{} changed", command.name),
            replacement: format!("/{} changed", command.name),
            badge: "workspace".to_owned(),
        },
        TuiSlashSuggestion {
            title: "Inspect workspace artifact".to_owned(),
            subtitle: "show".to_owned(),
            detail: "Resolve an artifact id or list index to preview content in the transcript."
                .to_owned(),
            example: format!("/{} show ", command.name),
            replacement: format!("/{} show ", command.name),
            badge: "artifact".to_owned(),
        },
        TuiSlashSuggestion {
            title: "Open workspace artifact externally".to_owned(),
            subtitle: "open".to_owned(),
            detail: "Open a changed file with the platform default application.".to_owned(),
            example: format!("/{} open ", command.name),
            replacement: format!("/{} open ", command.name),
            badge: "external".to_owned(),
        },
        TuiSlashSuggestion {
            title: "Workspace handoff".to_owned(),
            subtitle: "handoff".to_owned(),
            detail: "Send the current workspace context to the web console for richer preview."
                .to_owned(),
            example: format!("/{} handoff", command.name),
            replacement: format!("/{} handoff", command.name),
            badge: "handoff".to_owned(),
        },
    ]
}

pub(super) fn build_workspace_artifact_suggestions(
    command: &SharedChatCommandDefinition,
    catalog: &TuiSlashEntityCatalog,
    active_token: &str,
    action: &str,
) -> Vec<TuiSlashSuggestion> {
    let query = active_token.trim().to_ascii_lowercase();
    catalog
        .workspace_artifacts
        .iter()
        .filter(|artifact| {
            query.is_empty()
                || artifact.artifact_id.to_ascii_lowercase().contains(query.as_str())
                || artifact.display_path.to_ascii_lowercase().contains(query.as_str())
                || artifact.path.to_ascii_lowercase().contains(query.as_str())
                || artifact.change_kind.to_ascii_lowercase().contains(query.as_str())
        })
        .take(6)
        .map(|artifact| TuiSlashSuggestion {
            title: artifact.display_path.clone(),
            subtitle: artifact.artifact_id.clone(),
            detail: format!(
                "{} · {} · cp {} · {}{}",
                artifact.change_kind,
                artifact.preview_kind,
                shorten_entity_id(artifact.latest_checkpoint_id.as_str()),
                artifact
                    .size_bytes
                    .map(|value| format!("{value} bytes"))
                    .unwrap_or_else(|| "size unknown".to_owned()),
                if artifact.deleted { " · deleted" } else { "" }
            ),
            example: format!("/{} {} {}", command.name, action, artifact.artifact_id),
            replacement: format!("/{} {} {}", command.name, action, artifact.artifact_id),
            badge: if artifact.deleted { "deleted".to_owned() } else { "artifact".to_owned() },
        })
        .collect()
}

pub(super) fn build_workspace_checkpoint_suggestions(
    command: &SharedChatCommandDefinition,
    catalog: &TuiSlashEntityCatalog,
    active_token: &str,
    action: &str,
) -> Vec<TuiSlashSuggestion> {
    let query = active_token.trim().to_ascii_lowercase();
    let mut checkpoints = catalog.workspace_checkpoints.iter().collect::<Vec<_>>();
    checkpoints.sort_by_key(|checkpoint| std::cmp::Reverse(checkpoint.created_at_unix_ms));
    checkpoints
        .into_iter()
        .filter(|checkpoint| {
            query.is_empty()
                || checkpoint.checkpoint_id.to_ascii_lowercase().contains(query.as_str())
                || checkpoint.source_label.to_ascii_lowercase().contains(query.as_str())
                || checkpoint.summary_text.to_ascii_lowercase().contains(query.as_str())
        })
        .take(6)
        .map(|checkpoint| TuiSlashSuggestion {
            title: format!(
                "{} · {}",
                workspace_checkpoint_stage_label(checkpoint.checkpoint_stage.as_str()),
                checkpoint.source_label
            ),
            subtitle: checkpoint.checkpoint_id.clone(),
            detail: format!(
                "{} · {} · {} · paired {} · restores {}",
                if checkpoint.summary_text.is_empty() {
                    "Workspace checkpoint".to_owned()
                } else {
                    checkpoint.summary_text.clone()
                },
                if checkpoint.risk_level.is_empty() {
                    "risk unknown"
                } else {
                    checkpoint.risk_level.as_str()
                },
                if checkpoint.review_posture.is_empty() {
                    "review unknown"
                } else {
                    checkpoint.review_posture.as_str()
                },
                checkpoint
                    .paired_checkpoint_id
                    .as_deref()
                    .map(shorten_palette_id)
                    .unwrap_or_else(|| "none".to_owned()),
                checkpoint.restore_count
            ),
            example: if action == "restore" {
                format!("/{} restore {} --confirm", command.name, checkpoint.checkpoint_id)
            } else {
                format!("/{} diff {}", command.name, checkpoint.checkpoint_id)
            },
            replacement: if action == "restore" {
                format!("/{} restore {} --confirm", command.name, checkpoint.checkpoint_id)
            } else {
                format!("/{} diff {}", command.name, checkpoint.checkpoint_id)
            },
            badge: "workspace checkpoint".to_owned(),
        })
        .collect()
}

fn workspace_checkpoint_stage_label(stage: &str) -> &'static str {
    match stage {
        "preflight" => "preflight",
        "post_change" => "post-change",
        _ => "checkpoint",
    }
}

fn shorten_palette_id(value: &str) -> String {
    value.chars().take(8).collect()
}

pub(super) fn build_undo_suggestions(
    command: &SharedChatCommandDefinition,
    catalog: &TuiSlashEntityCatalog,
    active_token: &str,
) -> Vec<TuiSlashSuggestion> {
    let mut suggestions = Vec::new();
    if active_token.trim().is_empty() {
        if let Some(checkpoint) = select_undo_checkpoint(catalog.checkpoints.as_slice()) {
            suggestions.push(TuiSlashSuggestion {
                title: "Undo last turn".to_owned(),
                subtitle: checkpoint.name.clone(),
                detail: if checkpoint.note.is_empty() {
                    "Restore the latest safe checkpoint.".to_owned()
                } else {
                    checkpoint.note.clone()
                },
                example: format!("/{}", command.name),
                replacement: format!("/{}", command.name),
                badge: if checkpoint_has_tag(checkpoint, "undo_safe") {
                    "undo-safe".to_owned()
                } else {
                    "checkpoint".to_owned()
                },
            });
        }
    }
    let query = active_token.trim().to_ascii_lowercase();
    suggestions.extend(
        catalog
            .checkpoints
            .iter()
            .filter(|checkpoint| {
                query.is_empty()
                    || checkpoint.checkpoint_id.to_ascii_lowercase().contains(query.as_str())
                    || checkpoint.name.to_ascii_lowercase().contains(query.as_str())
            })
            .take(6)
            .map(|checkpoint| TuiSlashSuggestion {
                title: checkpoint.name.clone(),
                subtitle: checkpoint.checkpoint_id.clone(),
                detail: if checkpoint.note.is_empty() {
                    "Restore this checkpoint as the new active branch.".to_owned()
                } else {
                    checkpoint.note.clone()
                },
                example: format!("/{} {}", command.name, checkpoint.checkpoint_id),
                replacement: format!("/{} {}", command.name, checkpoint.checkpoint_id),
                badge: if checkpoint_has_tag(checkpoint, "undo_safe") {
                    "undo-safe".to_owned()
                } else {
                    "checkpoint".to_owned()
                },
            }),
    );
    suggestions.truncate(8);
    suggestions
}

pub(super) fn build_interrupt_suggestions(
    command: &SharedChatCommandDefinition,
    active_token: &str,
    streaming: bool,
) -> Vec<TuiSlashSuggestion> {
    ["soft", "force"]
        .into_iter()
        .filter(|candidate| active_token.is_empty() || candidate.contains(active_token))
        .map(|candidate| TuiSlashSuggestion {
            title: if candidate == "soft" {
                "Soft interrupt".to_owned()
            } else {
                "Force interrupt".to_owned()
            },
            subtitle: if streaming {
                "Current run is active".to_owned()
            } else {
                "Prepare redirect before the next send".to_owned()
            },
            detail: if candidate == "soft" {
                "Wait for the runtime to honor a normal cancellation request.".to_owned()
            } else {
                "Escalate wording only when a normal interrupt already failed.".to_owned()
            },
            example: format!("/{} {} Summarize the failures instead.", command.name, candidate),
            replacement: format!("/{} {} ", command.name, candidate),
            badge: candidate.to_owned(),
        })
        .collect()
}

pub(super) fn build_doctor_suggestions(
    command: &SharedChatCommandDefinition,
    active_token: &str,
) -> Vec<TuiSlashSuggestion> {
    [
        ("jobs", "Recent jobs", "List the latest doctor recovery jobs."),
        ("run", "Dry-run doctor", "Queue a non-mutating doctor recovery pass."),
        ("repair", "Repair doctor", "Queue a repair-enabled doctor recovery pass."),
    ]
    .into_iter()
    .filter(|(value, title, _)| {
        active_token.is_empty()
            || value.contains(active_token)
            || title.to_ascii_lowercase().contains(active_token)
    })
    .map(|(value, title, detail)| TuiSlashSuggestion {
        title: title.to_owned(),
        subtitle: value.to_owned(),
        detail: detail.to_owned(),
        example: format!("/{} {}", command.name, value),
        replacement: format!("/{} {}", command.name, value),
        badge: "doctor".to_owned(),
    })
    .collect()
}

pub(super) fn build_static_suggestions(
    command: &SharedChatCommandDefinition,
    active_token: &str,
    values: &[&str],
) -> Vec<TuiSlashSuggestion> {
    values
        .iter()
        .copied()
        .filter(|value| active_token.is_empty() || value.contains(active_token))
        .map(|value| TuiSlashSuggestion {
            title: format!("{} {}", command.name, value),
            subtitle: command.synopsis.clone(),
            detail: command.description.clone(),
            example: format!("/{} {}", command.name, value),
            replacement: format!("/{} {}", command.name, value),
            badge: command.category.clone(),
        })
        .collect()
}

pub(super) fn shorten_entity_id(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() <= 12 {
        trimmed.to_owned()
    } else {
        format!("{}…{}", &trimmed[..6], &trimmed[trimmed.len().saturating_sub(4)..])
    }
}
