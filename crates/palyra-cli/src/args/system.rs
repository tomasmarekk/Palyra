//! Arguments for `palyra system`: runtime heartbeat, subsystem presence,
//! operator insights, and system-event listing/emission. Help text is pinned by
//! snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::{Subcommand, ValueEnum};

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum SystemCommand {
    #[command(visible_alias = "status", about = "Show runtime heartbeat state")]
    Heartbeat {
        #[arg(long, default_value_t = false, help = "Print heartbeat state as JSON")]
        json: bool,
    },
    #[command(about = "Show subsystem presence and availability")]
    Presence {
        #[arg(long, default_value_t = false, help = "Print subsystem presence as JSON")]
        json: bool,
    },
    #[command(about = "Inspect aggregated operator insights and hotspots")]
    Insights {
        #[arg(long, default_value_t = false, help = "Print operator insights as JSON")]
        json: bool,
    },
    #[command(visible_alias = "events", about = "List or emit system events")]
    Event {
        #[command(subcommand)]
        command: SystemEventCommand,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum SystemEventCommand {
    #[command(about = "List recent system events")]
    List {
        #[arg(long, help = "Maximum number of events to return")]
        limit: Option<usize>,
        #[arg(long, default_value_t = false, help = "Print system events as JSON")]
        json: bool,
    },
    #[command(about = "Emit a synthetic system event")]
    Emit {
        #[arg(help = "Event type or name to emit")]
        event: String,
        #[arg(long, help = "Human-readable event message")]
        message: Option<String>,
        #[arg(long, value_enum, default_value_t = SystemEventSeverityArg::Info, help = "Severity to attach to the event")]
        severity: SystemEventSeverityArg,
        #[arg(long, help = "Tag to attach to the event; repeat for multiple tags")]
        tag: Vec<String>,
        #[arg(long, default_value_t = false, help = "Print the emitted event as JSON")]
        json: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SystemEventSeverityArg {
    Info,
    Warn,
    Error,
}
