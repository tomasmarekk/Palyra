//! Slash-command palette state and entity catalogs for the TUI composer.
//!
//! Resolves the typed `/command` against the shared chat-command registry and
//! produces ranked suggestions plus a selection preview; per-command
//! suggestion construction lives in the `builders` submodule.

use serde_json::Value;

use crate::shared_chat_commands::{
    find_shared_chat_command, resolve_shared_chat_command_name, shared_chat_commands_for_surface,
    SharedChatCommandDefinition, SharedChatCommandSurface,
};

#[path = "slash_palette_builders.rs"]
mod builders;

use builders::*;

/// Cached entity catalogs that feed slash-palette argument suggestions.
#[derive(Debug, Clone, Default)]
pub(crate) struct TuiSlashEntityCatalog {
    pub(crate) sessions: Vec<TuiSlashSessionRecord>,
    pub(crate) objectives: Vec<TuiSlashObjectiveRecord>,
    pub(crate) auth_profiles: Vec<TuiSlashAuthProfileRecord>,
    pub(crate) browser_profiles: Vec<TuiSlashBrowserProfileRecord>,
    pub(crate) browser_sessions: Vec<TuiSlashBrowserSessionRecord>,
    pub(crate) checkpoints: Vec<TuiSlashCheckpointRecord>,
    pub(crate) workspace_artifacts: Vec<TuiSlashWorkspaceArtifactRecord>,
    pub(crate) workspace_checkpoints: Vec<TuiSlashWorkspaceCheckpointRecord>,
}

/// Family relation (parent, sibling, child) of a catalogued session.
#[derive(Debug, Clone)]
pub(crate) struct TuiSlashSessionRelative {
    pub(crate) session_id: String,
    pub(crate) title: String,
    pub(crate) branch_state: String,
    pub(crate) relation: String,
}

/// Session catalog entry surfaced by `/resume`, `/history`, and recaps.
#[derive(Debug, Clone)]
pub(crate) struct TuiSlashSessionRecord {
    pub(crate) session_id: String,
    pub(crate) title: String,
    pub(crate) session_key: String,
    pub(crate) archived: bool,
    pub(crate) preview: String,
    pub(crate) root_title: String,
    pub(crate) last_summary: String,
    pub(crate) branch_state: String,
    pub(crate) family_sequence: u64,
    pub(crate) family_size: usize,
    pub(crate) parent_session_id: Option<String>,
    pub(crate) parent_title: Option<String>,
    pub(crate) pending_approvals: usize,
    pub(crate) artifact_count: usize,
    pub(crate) active_context_files: usize,
    pub(crate) relatives: Vec<TuiSlashSessionRelative>,
}

/// Objective catalog entry surfaced by `/objective` suggestions.
#[derive(Debug, Clone)]
pub(crate) struct TuiSlashObjectiveRecord {
    pub(crate) objective_id: String,
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) focus: String,
}

/// Auth profile catalog entry surfaced by `/profile` suggestions.
#[derive(Debug, Clone)]
pub(crate) struct TuiSlashAuthProfileRecord {
    pub(crate) profile_id: String,
    pub(crate) profile_name: String,
    pub(crate) provider_kind: String,
    pub(crate) scope_kind: String,
}

/// Browser profile catalog entry surfaced by `/browser` suggestions.
#[derive(Debug, Clone)]
pub(crate) struct TuiSlashBrowserProfileRecord {
    pub(crate) profile_id: String,
    pub(crate) name: String,
    pub(crate) persistence_enabled: bool,
    pub(crate) private_profile: bool,
}

/// Browser session catalog entry surfaced by `/browser` suggestions.
#[derive(Debug, Clone)]
pub(crate) struct TuiSlashBrowserSessionRecord {
    pub(crate) session_id: String,
    pub(crate) title: String,
}

/// Conversation checkpoint entry used by `/checkpoint` and `/undo`.
#[derive(Debug, Clone)]
pub(crate) struct TuiSlashCheckpointRecord {
    pub(crate) checkpoint_id: String,
    pub(crate) name: String,
    pub(crate) note: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) tags: Vec<String>,
}

/// Workspace artifact entry used by `/workspace show|open` suggestions.
#[derive(Debug, Clone)]
pub(crate) struct TuiSlashWorkspaceArtifactRecord {
    pub(crate) artifact_id: String,
    pub(crate) path: String,
    pub(crate) display_path: String,
    pub(crate) change_kind: String,
    pub(crate) latest_checkpoint_id: String,
    pub(crate) preview_kind: String,
    pub(crate) size_bytes: Option<u64>,
    pub(crate) deleted: bool,
}

/// Workspace checkpoint entry used by `/rollback diff|restore` suggestions.
#[derive(Debug, Clone)]
pub(crate) struct TuiSlashWorkspaceCheckpointRecord {
    pub(crate) checkpoint_id: String,
    pub(crate) source_label: String,
    pub(crate) checkpoint_stage: String,
    pub(crate) paired_checkpoint_id: Option<String>,
    pub(crate) risk_level: String,
    pub(crate) review_posture: String,
    pub(crate) summary_text: String,
    pub(crate) restore_count: u64,
    pub(crate) created_at_unix_ms: i64,
}

/// In-session interaction counters reported by `/usage`.
#[derive(Debug, Clone, Default)]
pub(crate) struct TuiUxMetrics {
    pub(crate) slash_commands: u64,
    pub(crate) palette_accepts: u64,
    pub(crate) keyboard_accepts: u64,
    pub(crate) undo: u64,
    pub(crate) interrupt: u64,
    pub(crate) errors: u64,
}

/// Selector for the [`TuiUxMetrics`] counter to bump.
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
    /// Increments the counter selected by `key`.
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

/// One palette row; `replacement` is the full composer text to insert when
/// the suggestion is accepted.
#[derive(Debug, Clone)]
pub(crate) struct TuiSlashSuggestion {
    pub(crate) title: String,
    pub(crate) subtitle: String,
    pub(crate) detail: String,
    pub(crate) example: String,
    pub(crate) replacement: String,
    pub(crate) badge: String,
}

/// Current palette contents: the resolved command (if any) and its rows.
#[derive(Debug, Clone)]
pub(crate) struct TuiSlashPaletteState {
    pub(crate) active_command: Option<&'static SharedChatCommandDefinition>,
    pub(crate) suggestions: Vec<TuiSlashSuggestion>,
}

/// Preview block rendered above the suggestion list for the selected row.
#[derive(Debug, Clone)]
pub(crate) struct TuiSlashPreview {
    pub(crate) title: String,
    pub(crate) subtitle: String,
    pub(crate) detail: String,
    pub(crate) example: String,
    pub(crate) badge: String,
}

/// Inputs for [`build_tui_slash_palette`].
pub(crate) struct BuildTuiSlashPaletteArgs<'a> {
    pub(crate) input: &'a str,
    pub(crate) catalog: &'a TuiSlashEntityCatalog,
    pub(crate) streaming: bool,
    pub(crate) delegation_profiles: &'a [&'a str],
    pub(crate) delegation_templates: &'a [&'a str],
}

/// Builds palette state for the composer input, or `None` when the input is
/// not a slash command. Unresolved command tokens yield command-name matches;
/// resolved commands yield argument suggestions for the trailing token.
pub(crate) fn build_tui_slash_palette(
    args: BuildTuiSlashPaletteArgs<'_>,
) -> Option<TuiSlashPaletteState> {
    let trimmed = args.input.trim_start();
    if !trimmed.starts_with('/') {
        return None;
    }

    let body = trimmed.trim_start_matches('/');
    let has_trailing_whitespace = trimmed.chars().last().is_some_and(char::is_whitespace);
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
    // Trailing whitespace means the user finished the previous token, so the
    // palette should suggest fresh values instead of filtering by it.
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

/// Preview for the suggestion at `selected`, falling back to the active
/// command's own synopsis when no suggestion row exists.
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

/// Picks the checkpoint `/undo` should restore: the newest `undo_safe`-tagged
/// checkpoint, falling back to the newest checkpoint of any kind.
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

/// Case-insensitive tag membership test for a checkpoint record.
pub(crate) fn checkpoint_has_tag(checkpoint: &TuiSlashCheckpointRecord, tag: &str) -> bool {
    let normalized = tag.trim().to_ascii_lowercase();
    !normalized.is_empty()
        && checkpoint.tags.iter().any(|entry| entry.trim().to_ascii_lowercase() == normalized)
}

/// Reads a string at a JSON pointer, defaulting to empty when absent.
pub(crate) fn read_json_string(value: &Value, pointer: &str) -> String {
    value.pointer(pointer).and_then(Value::as_str).unwrap_or_default().to_owned()
}

/// Reads an i64 at a JSON pointer, defaulting to zero when absent.
pub(crate) fn read_json_i64(value: &Value, pointer: &str) -> i64 {
    value.pointer(pointer).and_then(Value::as_i64).unwrap_or_default()
}

/// Reads a bool at a JSON pointer, defaulting to false when absent.
pub(crate) fn read_json_bool(value: &Value, pointer: &str) -> bool {
    value.pointer(pointer).and_then(Value::as_bool).unwrap_or(false)
}

/// Reads a string array at a JSON pointer, skipping non-string entries.
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

#[cfg(test)]
mod tests {
    use super::{
        build_tui_slash_palette, checkpoint_has_tag, preview_for_selection, read_json_tags,
        select_undo_checkpoint, BuildTuiSlashPaletteArgs, TuiSlashCheckpointRecord,
        TuiSlashEntityCatalog, TuiSlashSessionRecord, TuiSlashWorkspaceArtifactRecord,
        TuiSlashWorkspaceCheckpointRecord,
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
                    root_title: "Ops Triage".to_owned(),
                    last_summary: "Investigate deploy failures".to_owned(),
                    branch_state: "root".to_owned(),
                    family_sequence: 1,
                    family_size: 1,
                    parent_session_id: None,
                    parent_title: None,
                    pending_approvals: 0,
                    artifact_count: 0,
                    active_context_files: 0,
                    relatives: Vec::new(),
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

    #[test]
    fn workspace_palette_offers_artifact_show_suggestions() {
        let palette = build_tui_slash_palette(BuildTuiSlashPaletteArgs {
            input: "/workspace show src",
            catalog: &TuiSlashEntityCatalog {
                workspace_artifacts: vec![TuiSlashWorkspaceArtifactRecord {
                    artifact_id: "artifact-1".to_owned(),
                    path: "src/lib.rs".to_owned(),
                    display_path: "src/lib.rs".to_owned(),
                    change_kind: "modified".to_owned(),
                    latest_checkpoint_id: "checkpoint-1".to_owned(),
                    preview_kind: "text".to_owned(),
                    size_bytes: Some(1024),
                    deleted: false,
                }],
                ..TuiSlashEntityCatalog::default()
            },
            streaming: false,
            delegation_profiles: &[],
            delegation_templates: &[],
        })
        .expect("workspace palette should be available");
        assert_eq!(palette.suggestions.len(), 1);
        assert_eq!(palette.suggestions[0].replacement, "/workspace show artifact-1");
    }

    #[test]
    fn rollback_palette_offers_workspace_restore_confirmation_suggestions() {
        let palette = build_tui_slash_palette(BuildTuiSlashPaletteArgs {
            input: "/rollback restore work",
            catalog: &TuiSlashEntityCatalog {
                workspace_checkpoints: vec![TuiSlashWorkspaceCheckpointRecord {
                    checkpoint_id: "workspace-1".to_owned(),
                    source_label: "filesystem_write".to_owned(),
                    checkpoint_stage: "post_change".to_owned(),
                    paired_checkpoint_id: Some("workspace-0".to_owned()),
                    risk_level: "low".to_owned(),
                    review_posture: "standard".to_owned(),
                    summary_text: "src/lib.rs changed".to_owned(),
                    restore_count: 0,
                    created_at_unix_ms: 42,
                }],
                ..TuiSlashEntityCatalog::default()
            },
            streaming: false,
            delegation_profiles: &[],
            delegation_templates: &[],
        })
        .expect("rollback palette should be available");
        assert_eq!(palette.suggestions.len(), 1);
        assert_eq!(palette.suggestions[0].replacement, "/rollback restore workspace-1 --confirm");
    }
}
