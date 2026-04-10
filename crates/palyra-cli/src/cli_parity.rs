use std::collections::BTreeMap;

use anyhow::Result;
use clap::{Command as ClapCommand, CommandFactory};
use serde::{Deserialize, Serialize};

use crate::cli::Cli;
use crate::shared_chat_commands::{
    shared_chat_commands, SharedChatCommandExecution, SharedChatCommandSurface,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CliParityStatus {
    Done,
    Partial,
    IntentionalDeviation,
    CapabilityGated,
}

impl CliParityStatus {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Done => "done",
            Self::Partial => "partial",
            Self::IntentionalDeviation => "intentional_deviation",
            Self::CapabilityGated => "capability_gated",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliParitySnapshotSpec {
    pub path: String,
    #[serde(default)]
    pub file: Option<String>,
    #[serde(default)]
    pub unix_file: Option<String>,
    #[serde(default)]
    pub windows_file: Option<String>,
}

impl CliParitySnapshotSpec {
    pub fn expected_file(&self) -> Option<&str> {
        #[cfg(windows)]
        {
            self.windows_file.as_deref().or(self.file.as_deref()).or(self.unix_file.as_deref())
        }

        #[cfg(not(windows))]
        {
            self.unix_file.as_deref().or(self.file.as_deref()).or(self.windows_file.as_deref())
        }
    }

    pub fn display_label(&self) -> Option<String> {
        match (self.file.as_deref(), self.unix_file.as_deref(), self.windows_file.as_deref()) {
            (Some(file), None, None) => Some(file.to_owned()),
            (Some(file), Some(unix_file), Some(windows_file))
                if file == unix_file && file == windows_file =>
            {
                Some(file.to_owned())
            }
            (Some(file), Some(unix_file), None) if file == unix_file => Some(file.to_owned()),
            (Some(file), None, Some(windows_file)) if file == windows_file => Some(file.to_owned()),
            (Some(file), Some(unix_file), Some(windows_file)) => {
                Some(format!("{file} (unix: {unix_file}, windows: {windows_file})"))
            }
            (None, Some(unix_file), Some(windows_file)) if unix_file == windows_file => {
                Some(unix_file.to_owned())
            }
            (None, Some(unix_file), Some(windows_file)) => {
                Some(format!("unix: {unix_file}; windows: {windows_file}"))
            }
            (None, Some(unix_file), None) => Some(unix_file.to_owned()),
            (None, None, Some(windows_file)) => Some(windows_file.to_owned()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliParityMatrixEntry {
    pub path: String,
    pub category: String,
    pub status: CliParityStatus,
    pub summary: String,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default)]
    pub required_aliases: Vec<String>,
    #[serde(default)]
    pub required_flags: Vec<String>,
    #[serde(default)]
    pub snapshot: Option<CliParitySnapshotSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliParityMatrix {
    pub version: u32,
    pub entries: Vec<CliParityMatrixEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CliParityVerificationStatus {
    Verified,
    MissingCommand,
    MissingAliases,
    MissingFlags,
    MissingAliasesAndFlags,
}

impl CliParityVerificationStatus {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::MissingCommand => "missing_command",
            Self::MissingAliases => "missing_aliases",
            Self::MissingFlags => "missing_flags",
            Self::MissingAliasesAndFlags => "missing_aliases_and_flags",
        }
    }

    pub fn is_verified(self) -> bool {
        matches!(self, Self::Verified)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliParityReportEntry {
    pub path: String,
    pub category: String,
    pub status: CliParityStatus,
    pub summary: String,
    pub notes: Option<String>,
    pub required_aliases: Vec<String>,
    pub required_flags: Vec<String>,
    pub actual_aliases: Vec<String>,
    pub actual_flags: Vec<String>,
    pub snapshot: Option<CliParitySnapshotSpec>,
    pub verification_status: CliParityVerificationStatus,
    pub missing_aliases: Vec<String>,
    pub missing_flags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliParitySummary {
    pub total_entries: usize,
    pub verified_entries: usize,
    pub regression_entries: usize,
    pub help_snapshot_entries: usize,
    pub status_counts: BTreeMap<String, usize>,
    pub verification_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliParityReport {
    pub version: u32,
    pub summary: CliParitySummary,
    pub entries: Vec<CliParityReportEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedChatCommandParityEntry {
    pub name: String,
    pub synopsis: String,
    pub category: String,
    pub execution: SharedChatCommandExecution,
    pub surfaces: Vec<String>,
    pub aliases: Vec<String>,
    pub capability_tags: Vec<String>,
    pub entity_targets: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedChatCommandParitySummary {
    pub total_commands: usize,
    pub shared_commands: usize,
    pub web_commands: usize,
    pub tui_commands: usize,
    pub web_only_commands: usize,
    pub tui_only_commands: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedChatCommandParityReport {
    pub summary: SharedChatCommandParitySummary,
    pub entries: Vec<SharedChatCommandParityEntry>,
}

pub fn build_cli_root_command() -> ClapCommand {
    build_cli_root_command_inner()
}

pub fn build_cli_parity_report(matrix: &CliParityMatrix, root: &ClapCommand) -> CliParityReport {
    let mut status_counts = BTreeMap::new();
    let mut verification_counts = BTreeMap::new();
    let mut verified_entries = 0_usize;
    let mut regression_entries = 0_usize;
    let mut help_snapshot_entries = 0_usize;
    let mut entries = Vec::with_capacity(matrix.entries.len());

    for spec in &matrix.entries {
        *status_counts.entry(spec.status.as_label().to_owned()).or_insert(0) += 1;
        if spec.snapshot.is_some() {
            help_snapshot_entries = help_snapshot_entries.saturating_add(1);
        }

        let report_entry = match find_command(root, spec.path.as_str()) {
            Some(command) => {
                let mut actual_aliases =
                    command.get_all_aliases().map(str::to_owned).collect::<Vec<_>>();
                actual_aliases.sort();
                actual_aliases.dedup();
                let mut actual_flags = command
                    .get_arguments()
                    .filter_map(|arg| arg.get_long().map(str::to_owned))
                    .collect::<Vec<_>>();
                actual_flags.sort();
                actual_flags.dedup();
                let missing_aliases = missing_items(&spec.required_aliases, &actual_aliases);
                let missing_flags = missing_items(&spec.required_flags, &actual_flags);
                let verification_status =
                    classify_verification_status(false, &missing_aliases, &missing_flags);
                if verification_status.is_verified() {
                    verified_entries = verified_entries.saturating_add(1);
                } else {
                    regression_entries = regression_entries.saturating_add(1);
                }
                CliParityReportEntry {
                    path: spec.path.clone(),
                    category: spec.category.clone(),
                    status: spec.status,
                    summary: spec.summary.clone(),
                    notes: spec.notes.clone(),
                    required_aliases: spec.required_aliases.clone(),
                    required_flags: spec.required_flags.clone(),
                    actual_aliases,
                    actual_flags,
                    snapshot: spec.snapshot.clone(),
                    verification_status,
                    missing_aliases,
                    missing_flags,
                }
            }
            None => {
                regression_entries = regression_entries.saturating_add(1);
                CliParityReportEntry {
                    path: spec.path.clone(),
                    category: spec.category.clone(),
                    status: spec.status,
                    summary: spec.summary.clone(),
                    notes: spec.notes.clone(),
                    required_aliases: spec.required_aliases.clone(),
                    required_flags: spec.required_flags.clone(),
                    actual_aliases: Vec::new(),
                    actual_flags: Vec::new(),
                    snapshot: spec.snapshot.clone(),
                    verification_status: CliParityVerificationStatus::MissingCommand,
                    missing_aliases: spec.required_aliases.clone(),
                    missing_flags: spec.required_flags.clone(),
                }
            }
        };

        *verification_counts
            .entry(report_entry.verification_status.as_label().to_owned())
            .or_insert(0) += 1;
        entries.push(report_entry);
    }

    CliParityReport {
        version: matrix.version,
        summary: CliParitySummary {
            total_entries: matrix.entries.len(),
            verified_entries,
            regression_entries,
            help_snapshot_entries,
            status_counts,
            verification_counts,
        },
        entries,
    }
}

pub fn render_cli_parity_report_markdown(report: &CliParityReport) -> String {
    let mut lines = Vec::new();
    lines.push("# CLI Parity Acceptance Matrix".to_owned());
    lines.push(String::new());
    lines.push(format!("Version: `{}`", report.version));
    lines.push(String::new());
    lines.push("This report is generated from the committed CLI parity matrix plus the current `clap` command tree.".to_owned());
    lines.push("It distinguishes expected parity posture (`done` / `partial` / `intentional_deviation` / `capability_gated`) from validation status against the live CLI surface.".to_owned());
    lines.push(String::new());
    lines.push("## Summary".to_owned());
    lines.push(String::new());
    lines.push(format!("- Total entries: `{}`", report.summary.total_entries));
    lines.push(format!("- Verified entries: `{}`", report.summary.verified_entries));
    lines.push(format!("- Regression entries: `{}`", report.summary.regression_entries));
    lines.push(format!(
        "- Help snapshot coverage: `{}` entries",
        report.summary.help_snapshot_entries
    ));
    lines.push(String::new());
    lines.push("### Expected parity status counts".to_owned());
    lines.push(String::new());
    for (label, count) in &report.summary.status_counts {
        lines.push(format!("- `{label}`: `{count}`"));
    }
    lines.push(String::new());
    lines.push("### Validation status counts".to_owned());
    lines.push(String::new());
    for (label, count) in &report.summary.verification_counts {
        lines.push(format!("- `{label}`: `{count}`"));
    }
    lines.push(String::new());
    lines.push("## Entries".to_owned());
    lines.push(String::new());
    lines.push(
        "| Path | Category | Expected | Validation | Snapshot | Aliases | Flags | Notes |"
            .to_owned(),
    );
    lines.push("| --- | --- | --- | --- | --- | --- | --- | --- |".to_owned());
    for entry in &report.entries {
        let snapshot = entry
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.display_label().map(|value| format!("`{value}`")))
            .unwrap_or_else(|| "-".to_owned());
        let aliases = if entry.required_aliases.is_empty() {
            "-".to_owned()
        } else {
            entry
                .required_aliases
                .iter()
                .map(|alias| format!("`{alias}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let flags = if entry.required_flags.is_empty() {
            "-".to_owned()
        } else {
            entry
                .required_flags
                .iter()
                .map(|flag| format!("`--{flag}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let notes = render_entry_notes(entry);
        lines.push(format!(
            "| `{}` | `{}` | `{}` | `{}` | {} | {} | {} | {} |",
            entry.path,
            entry.category,
            entry.status.as_label(),
            entry.verification_status.as_label(),
            snapshot,
            aliases,
            flags,
            notes
        ));
    }
    lines.push(String::new());
    lines.join("\n")
}

pub fn validate_cli_parity_report(report: &CliParityReport) -> Result<()> {
    let regressions = report
        .entries
        .iter()
        .filter(|entry| !entry.verification_status.is_verified())
        .map(render_regression)
        .collect::<Vec<_>>();
    if regressions.is_empty() {
        return Ok(());
    }

    anyhow::bail!("CLI parity regressions detected:\n{}", regressions.join("\n"));
}

pub fn build_shared_chat_command_parity_report() -> SharedChatCommandParityReport {
    let mut shared_commands = 0_usize;
    let mut web_only_commands = 0_usize;
    let mut tui_only_commands = 0_usize;
    let mut web_commands = 0_usize;
    let mut tui_commands = 0_usize;
    let mut entries = Vec::new();

    for command in shared_chat_commands() {
        let surfaces = command
            .surfaces
            .iter()
            .map(|surface| surface.as_label().to_owned())
            .collect::<Vec<_>>();
        let has_web = command.surfaces.contains(&SharedChatCommandSurface::Web);
        let has_tui = command.surfaces.contains(&SharedChatCommandSurface::Tui);
        if has_web {
            web_commands = web_commands.saturating_add(1);
        }
        if has_tui {
            tui_commands = tui_commands.saturating_add(1);
        }
        match (has_web, has_tui) {
            (true, true) => shared_commands = shared_commands.saturating_add(1),
            (true, false) => web_only_commands = web_only_commands.saturating_add(1),
            (false, true) => tui_only_commands = tui_only_commands.saturating_add(1),
            (false, false) => {}
        }
        entries.push(SharedChatCommandParityEntry {
            name: command.name.clone(),
            synopsis: command.synopsis.clone(),
            category: command.category.clone(),
            execution: command.execution,
            surfaces,
            aliases: command.aliases.clone(),
            capability_tags: command.capability_tags.clone(),
            entity_targets: command.entity_targets.clone(),
        });
    }

    SharedChatCommandParityReport {
        summary: SharedChatCommandParitySummary {
            total_commands: entries.len(),
            shared_commands,
            web_commands,
            tui_commands,
            web_only_commands,
            tui_only_commands,
        },
        entries,
    }
}

pub fn render_shared_chat_command_parity_markdown(
    report: &SharedChatCommandParityReport,
) -> String {
    let mut lines = Vec::new();
    lines.push("# Shared Chat Command Registry".to_owned());
    lines.push(String::new());
    lines.push("This report is generated from the shared slash-command registry consumed by the web chat composer and the TUI.".to_owned());
    lines.push(String::new());
    lines.push("## Summary".to_owned());
    lines.push(String::new());
    lines.push(format!("- Total commands: `{}`", report.summary.total_commands));
    lines.push(format!("- Shared across web and TUI: `{}`", report.summary.shared_commands));
    lines.push(format!("- Web-visible commands: `{}`", report.summary.web_commands));
    lines.push(format!("- TUI-visible commands: `{}`", report.summary.tui_commands));
    lines.push(format!("- Web-only commands: `{}`", report.summary.web_only_commands));
    lines.push(format!("- TUI-only commands: `{}`", report.summary.tui_only_commands));
    lines.push(String::new());
    lines.push("## Entries".to_owned());
    lines.push(String::new());
    lines.push("| Command | Synopsis | Category | Execution | Surfaces | Aliases | Capability tags | Entity targets |".to_owned());
    lines.push("| --- | --- | --- | --- | --- | --- | --- | --- |".to_owned());
    for entry in &report.entries {
        let aliases = if entry.aliases.is_empty() {
            "-".to_owned()
        } else {
            entry.aliases.iter().map(|value| format!("`/{value}`")).collect::<Vec<_>>().join(", ")
        };
        let capability_tags = if entry.capability_tags.is_empty() {
            "-".to_owned()
        } else {
            entry
                .capability_tags
                .iter()
                .map(|value| format!("`{value}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let entity_targets = if entry.entity_targets.is_empty() {
            "-".to_owned()
        } else {
            entry
                .entity_targets
                .iter()
                .map(|value| format!("`{value}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        lines.push(format!(
            "| `/{}` | `{}` | `{}` | `{}` | {} | {} | {} | {} |",
            entry.name,
            entry.synopsis,
            entry.category,
            entry.execution.as_label(),
            entry
                .surfaces
                .iter()
                .map(|surface| format!("`{surface}`"))
                .collect::<Vec<_>>()
                .join(", "),
            aliases,
            capability_tags,
            entity_targets
        ));
    }
    lines.push(String::new());
    lines.join("\n")
}

fn find_command<'a>(root: &'a ClapCommand, path: &str) -> Option<&'a ClapCommand> {
    if path == "palyra" {
        return Some(root);
    }

    let mut current = root;
    for segment in path.split(' ') {
        current = current.get_subcommands().find(|candidate| {
            candidate.get_name() == segment
                || candidate.get_all_aliases().any(|alias| alias == segment)
        })?;
    }
    Some(current)
}

fn missing_items(expected: &[String], actual: &[String]) -> Vec<String> {
    expected
        .iter()
        .filter(|value| !actual.iter().any(|candidate| candidate == *value))
        .cloned()
        .collect()
}

fn classify_verification_status(
    command_missing: bool,
    missing_aliases: &[String],
    missing_flags: &[String],
) -> CliParityVerificationStatus {
    if command_missing {
        return CliParityVerificationStatus::MissingCommand;
    }
    match (missing_aliases.is_empty(), missing_flags.is_empty()) {
        (true, true) => CliParityVerificationStatus::Verified,
        (false, true) => CliParityVerificationStatus::MissingAliases,
        (true, false) => CliParityVerificationStatus::MissingFlags,
        (false, false) => CliParityVerificationStatus::MissingAliasesAndFlags,
    }
}

fn render_entry_notes(entry: &CliParityReportEntry) -> String {
    let mut parts = Vec::new();
    if let Some(notes) = &entry.notes {
        parts.push(notes.clone());
    }
    if !entry.missing_aliases.is_empty() {
        parts.push(format!(
            "missing aliases: {}",
            entry
                .missing_aliases
                .iter()
                .map(|alias| format!("`{alias}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !entry.missing_flags.is_empty() {
        parts.push(format!(
            "missing flags: {}",
            entry
                .missing_flags
                .iter()
                .map(|flag| format!("`--{flag}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if parts.is_empty() {
        "-".to_owned()
    } else {
        parts.join("; ")
    }
}

fn render_regression(entry: &CliParityReportEntry) -> String {
    match entry.verification_status {
        CliParityVerificationStatus::MissingCommand => {
            format!("- {}: command path is missing from the CLI tree", entry.path)
        }
        CliParityVerificationStatus::MissingAliases => {
            format!("- {}: missing aliases {}", entry.path, entry.missing_aliases.join(", "))
        }
        CliParityVerificationStatus::MissingFlags => format!(
            "- {}: missing flags {}",
            entry.path,
            entry
                .missing_flags
                .iter()
                .map(|flag| format!("--{flag}"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        CliParityVerificationStatus::MissingAliasesAndFlags => format!(
            "- {}: missing aliases [{}]; missing flags [{}]",
            entry.path,
            entry.missing_aliases.join(", "),
            entry
                .missing_flags
                .iter()
                .map(|flag| format!("--{flag}"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        CliParityVerificationStatus::Verified => {
            format!("- {}: verified", entry.path)
        }
    }
}

#[cfg(windows)]
fn build_cli_root_command_inner() -> ClapCommand {
    const CLI_HELP_STACK_SIZE_BYTES: usize = 8 * 1024 * 1024;

    std::thread::Builder::new()
        .name("palyra-cli-parity".to_owned())
        .stack_size(CLI_HELP_STACK_SIZE_BYTES)
        .spawn(|| {
            let mut command = Cli::command();
            command.build();
            command
        })
        .expect("failed to spawn CLI parity helper thread")
        .join()
        .expect("CLI parity helper thread panicked")
}

#[cfg(not(windows))]
fn build_cli_root_command_inner() -> ClapCommand {
    let mut command = Cli::command();
    command.build();
    command
}
