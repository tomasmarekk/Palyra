use clap::{Subcommand, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum JournalCheckpointModeArg {
    Passive,
    Full,
    Restart,
    Truncate,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum DaemonCommand {
    Status {
        #[arg(long)]
        url: Option<String>,
    },
    AdminStatus {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        device_id: Option<String>,
        #[arg(long)]
        channel: Option<String>,
    },
    JournalRecent {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        device_id: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
    },
    JournalVacuum {
        #[arg(long)]
        db_path: Option<String>,
    },
    JournalCheckpoint {
        #[arg(long)]
        db_path: Option<String>,
        #[arg(long, value_enum, default_value_t = JournalCheckpointModeArg::Truncate)]
        mode: JournalCheckpointModeArg,
        #[arg(long, default_value_t = false)]
        sign: bool,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        identity_store_dir: Option<String>,
        #[arg(long)]
        attestation_out: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    RunStatus {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        device_id: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        run_id: String,
    },
    RunTape {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        device_id: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        run_id: String,
        #[arg(long)]
        after_seq: Option<i64>,
        #[arg(long)]
        limit: Option<usize>,
    },
    RunCancel {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        device_id: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        run_id: String,
        #[arg(long)]
        reason: Option<String>,
    },
    DashboardUrl {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        verify_remote: bool,
        #[arg(long)]
        identity_store_dir: Option<String>,
        #[arg(long, default_value_t = false)]
        open: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
