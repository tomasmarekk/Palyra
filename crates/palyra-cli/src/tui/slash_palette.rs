use serde_json::Value;

use crate::shared_chat_commands::{
    find_shared_chat_command, resolve_shared_chat_command_name, shared_chat_commands_for_surface,
    SharedChatCommandDefinition, SharedChatCommandSurface,
};

#[derive(Debug, Clone, Default)]
pub(crate) struct TuiSlashEntityCatalog {
    pub(crate) sessions: Vec<TuiSlashSessionRecord>,
    pub(crate) objectives: Vec<TuiSlashObjectiveRecord>,
    pub(crate) auth_profiles: Vec<TuiSlashAuthProfileRecord>,
    pub(crate) browser_profiles: Vec<TuiSlashBrowserProfileRecord>,
    pub(crate) browser_sessions: Vec<TuiSlashBrowserSessionRecord>,
    pub(crate) checkpoints: Vec<TuiSlashCheckpointRecord>,
}

#[derive(Debug, Clone)]
pub(crate) struct TuiSlashSessionRecord {
    pub(crate) session_id: String,
    pub(crate) title: String,
    pub(crate) session_key: String,
    pub(crate) archived: bool,
    pub(crate) preview: String,
}

#[derive(Debug, Clone)]
pub(crate) struct TuiSlashObjectiveRecord {
    pub(crate) objective_id: String,
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) focus: String,
}

#[derive(Debug, Clone)]
pub(crate) struct TuiSlashAuthProfileRecord {
    pub(crate) profile_id: String,
    pub(crate) profile_name: String,
    pub(crate) provider_kind: String,
    pub(crate) scope_kind: String,
}

#[derive(Debug, Clone)]
pub(crate) struct TuiSlashBrowserProfileRecord {
    pub(crate) profile_id: String,
    pub(crate) name: String,
    pub(crate) persistence_enabled: bool,
    pub(crate) private_profile: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct TuiSlashBrowserSessionRecord {
    pub(crate) session_id: String,
    pub(crate) title: String,
}

#[derive(Debug, Clone)]
pub(crate) struct TuiSlashCheckpointRecord {
    pub(crate) checkpoint_id: String,
    pub(crate) name: String,
    pub(crate) note: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) tags: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TuiUxMetrics {
    pub(crate) slash_commands: u64,
    pub(crate) palette_accepts: u64,
    pub(crate) keyboard_accepts: u64,
    pub(crate) undo: u64,
    pub(crate) interrupt: u64,
    pub(crate) errors: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TuiUxMetricKey {
    SlashCommands,
    PaletteAccepts,
    KeyboardAccepts,
    Undo,
    Interrupt,
    Errors,
}

impl TuiUxMetrics {
    pub(crate) fn record(&mut self, key: TuiUxMetricKey) {
        match key {
            TuiUxMetricKey::SlashCommands => self.slash_commands += 1,
            TuiUxMetricKey::PaletteAccepts => self.palette_accepts += 1,
            TuiUxMetricKey::KeyboardAccepts => self.keyboard_accepts += 1,
            TuiUxMetricKey::Undo => self.undo += 1,
            TuiUxMetricKey::Interrupt => self.interrupt += 1,
            TuiUxMetricKey::Errors => self.errors += 1,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TuiSlashSuggestion {
    pub(crate) title: String,
    pub(crate) subtitle: String,
    pub(crate) detail: String,
    pub(crate) example: String,
    pub(crate) replacement: String,
    pub(crate) badge: String,
}

#[derive(Debug, Clone)]
pub(crate) struct TuiSlashPaletteState {
    pub(crate) active_command: Option<&'static SharedChatCommandDefinition>,
    pub(crate) suggestions: Vec<TuiSlashSuggestion>,
}

#[derive(Debug, Clone)]
pub(crate) struct TuiSlashPreview {
    pub(crate) title: String,
    pub(crate) subtitle: String,
    pub(crate) detail: String,
    pub(crate) example: String,
    pub(crate) badge: String,
}

pub(crate) struct BuildTuiSlashPaletteArgs<'a> {
    pub(crate) input: &'a str,
    pub(crate) catalog: &'a TuiSlashEntityCatalog,
    pub(crate) streaming: bool,
    pub(crate) delegation_profiles: &'a [&'a str],
    pub(crate) delegation_templates: &'a [&'a str],
}

pub(crate) fn build_tui_slash_palette(
    args: BuildTuiSlashPaletteArgs<'_>,
) -> Option<TuiSlashPaletteState> {
    let trimmed = args.input.trim_start();
    if !trimmed.starts_with('/') {
        return None;
    }

    let body = trimmed.trim_start_matches('/');
    let has_trailing_whitespace = trimmed.chars().last().map(char::is_whitespace).unwrap_or(false);
    let raw_parts = body.split_whitespace().collect::<Vec<_>>();
    let raw_command_token = raw_parts.first().copied().unwrap_or_default();
    let normalized_command_token = raw_command_token.trim().to_ascii_lowercase();
    let normalized_command_name = resolve_shared_chat_command_name(
        normalized_command_token.as_str(),
        SharedChatCommandSurface::Tui,
    )
    .unwrap_or(normalized_command_token.as_str());
    let active_command = if normalized_command_name.is_empty() {
        None
    } else {
        find_shared_chat_command(normalized_command_name, SharedChatCommandSurface::Tui)
    };

    if raw_command_token.trim().is_empty() || active_command.is_none() {
        return Some(TuiSlashPaletteState {
            active_command,
            suggestions: build_command_name_suggestions(normalized_command_token.as_str()),
        });
    }

    let normalized_rest = raw_parts.iter().skip(1).copied().collect::<Vec<_>>().join(" ");
    let active_token = if has_trailing_whitespace || normalized_rest.trim().is_empty() {
        String::new()
    } else {
        normalized_rest.split_whitespace().last().unwrap_or_default().trim().to_ascii_lowercase()
    };
    Some(TuiSlashPaletteState {
        active_command,
        suggestions: build_entity_suggestions(
            active_command
                .expect("active command must be present when building entity suggestions"),
            args.catalog,
            normalized_rest.trim(),
            active_token.as_str(),
            args.streaming,
            args.delegation_profiles,
            args.delegation_templates,
        ),
    })
}

pub(crate) fn preview_for_selection(
    palette: &TuiSlashPaletteState,
    selected: usize,
) -> Option<TuiSlashPreview> {
    if let Some(suggestion) = palette.suggestions.get(selected) {
        return Some(TuiSlashPreview {
            title: suggestion.title.clone(),
            subtitle: suggestion.subtitle.clone(),
            detail: suggestion.detail.clone(),
            example: suggestion.example.clone(),
            badge: suggestion.badge.clone(),
        });
    }
    palette.active_command.map(|command| TuiSlashPreview {
        title: command.synopsis.clone(),
        subtitle: command.description.clone(),
        detail: command.example.clone(),
        example: command.example.clone(),
        badge: command.category.clone(),
    })
}

pub(crate) fn select_undo_checkpoint(
    checkpoints: &[TuiSlashCheckpointRecord],
) -> Option<&TuiSlashCheckpointRecord> {
    let undo_checkpoint = checkpoints
        .iter()
        .filter(|checkpoint| checkpoint_has_tag(checkpoint, "undo_safe"))
        .max_by_key(|checkpoint| checkpoint.created_at_unix_ms);
    if undo_checkpoint.is_some() {
        return undo_checkpoint;
    }
    checkpoints.iter().max_by_key(|checkpoint| checkpoint.created_at_unix_ms)
}

pub(crate) fn checkpoint_has_tag(checkpoint: &TuiSlashCheckpointRecord, tag: &str) -> bool {
    let normalized = tag.trim().to_ascii_lowercase();
    !normalized.is_empty()
        && checkpoint.tags.iter().any(|entry| entry.trim().to_ascii_lowercase() == normalized)
}

pub(crate) fn read_json_string(value: &Value, pointer: &str) -> String {
    value.pointer(pointer).and_then(Value::as_str).unwrap_or_default().to_owned()
}

pub(crate) fn read_json_i64(value: &Value, pointer: &str) -> i64 {
    value.pointer(pointer).and_then(Value::as_i64).unwrap_or_default()
}

pub(crate) fn read_json_bool(value: &Value, pointer: &str) -> bool {
    value.pointer(pointer).and_then(Value::as_bool).unwrap_or(false)
}

pub(crate) fn read_json_tags(value: &Value, pointer: &str) -> Vec<String> {
    value
        .pointer(pointer)
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry.as_str().map(ToOwned::to_owned))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn build_command_name_suggestions(query: &str) -> Vec<TuiSlashSuggestion> {
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

fn build_entity_suggestions(
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
        "undo" => build_undo_suggestions(command, catalog, active_token),
        "interrupt" => build_interrupt_suggestions(command, active_token, streaming),
        "doctor" => build_doctor_suggestions(command, active_token),
        "compact" => {
            build_static_suggestions(command, active_token, &["preview", "apply", "history"])
        }
        "export" => build_static_suggestions(command, active_token, &["json", "markdown"]),
        _ => Vec::new(),
    }
}

fn build_session_suggestions(
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
        })
        .take(6)
        .map(|session| TuiSlashSuggestion {
            title: session.title.clone(),
            subtitle: if session.session_key.is_empty() {
                session.session_id.clone()
            } else {
                session.session_key.clone()
            },
            detail: if session.preview.is_empty() {
                "Resume this session context.".to_owned()
            } else {
                session.preview.clone()
            },
            example: format!("/{} {}", command.name, session.session_id),
            replacement: if command.name == "history" {
                format!("/{} {}", command.name, session.title)
            } else {
                format!("/{} {}", command.name, session.session_id)
            },
            badge: if session.archived { "archived".to_owned() } else { "session".to_owned() },
        })
        .collect()
}

fn build_objective_suggestions(
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

fn build_profile_suggestions(
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

fn build_browser_profile_suggestions(
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

fn build_browser_session_suggestions(
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

fn build_delegation_suggestions(
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

fn build_checkpoint_suggestions(
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

fn build_undo_suggestions(
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

fn build_interrupt_suggestions(
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

fn build_doctor_suggestions(
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

fn build_static_suggestions(
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

#[cfg(test)]
mod tests {
    use super::{
        build_tui_slash_palette, checkpoint_has_tag, preview_for_selection, read_json_tags,
        select_undo_checkpoint, BuildTuiSlashPaletteArgs, TuiSlashCheckpointRecord,
        TuiSlashEntityCatalog, TuiSlashSessionRecord,
    };
    use serde_json::json;

    #[test]
    fn slash_palette_offers_command_matches_for_partial_input() {
        let palette = build_tui_slash_palette(BuildTuiSlashPaletteArgs {
            input: "/int",
            catalog: &TuiSlashEntityCatalog::default(),
            streaming: false,
            delegation_profiles: &[],
            delegation_templates: &[],
        })
        .expect("slash palette should be available");
        assert!(palette
            .suggestions
            .iter()
            .any(|suggestion| suggestion.replacement.starts_with("/interrupt")));
    }

    #[test]
    fn slash_palette_uses_real_session_catalog_for_resume() {
        let palette = build_tui_slash_palette(BuildTuiSlashPaletteArgs {
            input: "/resume ops",
            catalog: &TuiSlashEntityCatalog {
                sessions: vec![TuiSlashSessionRecord {
                    session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
                    title: "Ops Triage".to_owned(),
                    session_key: "ops:triage".to_owned(),
                    archived: false,
                    preview: "Investigate deploy failures".to_owned(),
                }],
                ..TuiSlashEntityCatalog::default()
            },
            streaming: false,
            delegation_profiles: &[],
            delegation_templates: &[],
        })
        .expect("resume palette should be available");
        assert_eq!(palette.suggestions.len(), 1);
        assert_eq!(palette.suggestions[0].replacement, "/resume 01ARZ3NDEKTSV4RRFFQ69G5FAW");
    }

    #[test]
    fn undo_selection_prefers_undo_safe_checkpoint() {
        let checkpoints = vec![
            TuiSlashCheckpointRecord {
                checkpoint_id: "older".to_owned(),
                name: "Older".to_owned(),
                note: String::new(),
                created_at_unix_ms: 10,
                tags: vec!["manual".to_owned()],
            },
            TuiSlashCheckpointRecord {
                checkpoint_id: "latest".to_owned(),
                name: "Latest".to_owned(),
                note: String::new(),
                created_at_unix_ms: 20,
                tags: vec!["undo_safe".to_owned()],
            },
        ];
        assert_eq!(
            select_undo_checkpoint(checkpoints.as_slice())
                .map(|checkpoint| checkpoint.checkpoint_id.as_str()),
            Some("latest")
        );
    }

    #[test]
    fn checkpoint_tag_matching_is_case_insensitive() {
        let checkpoint = TuiSlashCheckpointRecord {
            checkpoint_id: "latest".to_owned(),
            name: "Latest".to_owned(),
            note: String::new(),
            created_at_unix_ms: 20,
            tags: vec!["Undo_Safe".to_owned()],
        };
        assert!(checkpoint_has_tag(&checkpoint, "undo_safe"));
    }

    #[test]
    fn preview_falls_back_to_active_command_when_no_suggestions_exist() {
        let palette = build_tui_slash_palette(BuildTuiSlashPaletteArgs {
            input: "/help",
            catalog: &TuiSlashEntityCatalog::default(),
            streaming: false,
            delegation_profiles: &[],
            delegation_templates: &[],
        })
        .expect("help palette should be available");
        let preview = preview_for_selection(&palette, 0).expect("preview should resolve");
        assert!(preview.title.starts_with("/help"));
    }

    #[test]
    fn json_tag_reader_extracts_string_values_only() {
        let value = json!({ "tags": ["undo_safe", 4, "manual"] });
        assert_eq!(
            read_json_tags(&value, "/tags"),
            vec!["undo_safe".to_owned(), "manual".to_owned()]
        );
    }
}
