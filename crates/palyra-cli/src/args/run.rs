//! Arguments for `palyra run`: run-level export and audit helpers.

use clap::{Subcommand, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RunControlCommandArg {
    Cancel,
    Pause,
    Redirect,
    Resume,
    Steer,
    Yield,
}

impl RunControlCommandArg {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Cancel => "cancel",
            Self::Pause => "pause",
            Self::Redirect => "redirect",
            Self::Resume => "resume",
            Self::Steer => "steer",
            Self::Yield => "yield",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RunControlActivePhaseArg {
    ProviderStream,
    ToolExecution,
    ApprovalPending,
    Queue,
    BackgroundTask,
    Idle,
}

impl RunControlActivePhaseArg {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderStream => "provider_stream",
            Self::ToolExecution => "tool_execution",
            Self::ApprovalPending => "approval_pending",
            Self::Queue => "queue",
            Self::BackgroundTask => "background_task",
            Self::Idle => "idle",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RunExportFormatArg {
    PalyraAttested,
    Sharegpt,
    Atropos,
}

impl RunExportFormatArg {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PalyraAttested => "palyra-attested",
            Self::Sharegpt => "sharegpt",
            Self::Atropos => "atropos",
        }
    }
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum RunCommand {
    #[command(about = "Wait for a daemon run to finish")]
    Wait {
        run_id: String,
        #[arg(long, default_value_t = 30_000)]
        timeout_ms: u64,
        #[arg(long, default_value_t = false)]
        return_on_waiting: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Apply a unified control command to a daemon run")]
    Control {
        run_id: String,
        #[arg(long, value_enum)]
        command: RunControlCommandArg,
        #[arg(long, value_enum)]
        active_phase: Option<RunControlActivePhaseArg>,
        #[arg(long)]
        instruction: Option<String>,
        #[arg(long)]
        queued_input_id: Option<String>,
        #[arg(long)]
        priority_lane: Option<String>,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Export a redacted run trajectory")]
    Export {
        #[arg(long)]
        run_id: String,
        #[arg(long)]
        output: String,
        #[arg(long, value_enum, default_value_t = RunExportFormatArg::PalyraAttested)]
        format: RunExportFormatArg,
        #[arg(
            long,
            default_value_t = true,
            action = clap::ArgAction::Set,
            value_parser = clap::value_parser!(bool)
        )]
        redacted: bool,
        #[arg(long)]
        journal_db: Option<String>,
        #[arg(long, default_value_t = 128)]
        max_events: usize,
        #[arg(long, default_value_t = false)]
        trajectory: bool,
    },
    #[command(about = "Replay a trajectory JSONL offline")]
    Replay {
        input: String,
        #[arg(long)]
        golden: Option<String>,
        #[arg(long)]
        diff_output: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
