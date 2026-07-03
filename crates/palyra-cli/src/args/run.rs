//! Arguments for `palyra run`: run-level export and audit helpers.

use clap::{Subcommand, ValueEnum};

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
