//! Arguments for `palyra state`: offline SQLite journal doctor, repair, and checkpointing.
//! Help text is pinned by snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::Subcommand;

use super::JournalCheckpointModeArg;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum StateCommand {
    Doctor {
        #[arg(long)]
        db_path: Option<String>,
        #[arg(long)]
        fast_window: Option<usize>,
        #[arg(long, default_value_t = false)]
        full: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    VerifyHashChain {
        #[arg(long)]
        db_path: Option<String>,
        #[arg(long, default_value_t = false)]
        full: bool,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Repair {
        #[arg(long)]
        db_path: Option<String>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        fts_only: bool,
        #[arg(long, default_value = "cli")]
        actor_principal: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Checkpoint {
        #[arg(long)]
        db_path: Option<String>,
        #[arg(long, value_enum, default_value_t = JournalCheckpointModeArg::Truncate)]
        mode: JournalCheckpointModeArg,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    SidecarsPrepare {
        #[arg(long)]
        db_path: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
