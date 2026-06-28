//! Arguments for `palyra workers`: networked-worker diagnostics and cleanup
//! actions over the console API.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum WorkersCommand {
    List {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Doctor {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Leases {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Cleanup {
        worker_id: String,
        #[arg(long, default_value_t = false)]
        removed_workspace_scope: bool,
        #[arg(long, default_value_t = false)]
        removed_artifacts: bool,
        #[arg(long, default_value_t = false)]
        removed_logs: bool,
        #[arg(long)]
        failure_reason: Option<String>,
        #[arg(long, default_value_t = false)]
        confirm: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
