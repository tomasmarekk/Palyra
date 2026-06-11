//! Arguments for `palyra system`: runtime heartbeat, subsystem presence,
//! operator insights, and system-event listing/emission. Help text is pinned by
//! snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::{Subcommand, ValueEnum};

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum SystemCommand {
    #[command(visible_alias = "status")]
    Heartbeat {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Presence {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Inspect aggregated operator insights and hotspots")]
    Insights {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(visible_alias = "events")]
    Event {
        #[command(subcommand)]
        command: SystemEventCommand,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum SystemEventCommand {
    List {
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Emit {
        event: String,
        #[arg(long)]
        message: Option<String>,
        #[arg(long, value_enum, default_value_t = SystemEventSeverityArg::Info)]
        severity: SystemEventSeverityArg,
        #[arg(long)]
        tag: Vec<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SystemEventSeverityArg {
    Info,
    Warn,
    Error,
}
