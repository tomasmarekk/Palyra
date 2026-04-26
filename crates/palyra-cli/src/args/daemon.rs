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
    Run {
        #[arg(long)]
        bin_path: Option<String>,
    },
    Health {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        grpc_url: Option<String>,
    },
    Probe {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        grpc_url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        device_id: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        verify_remote: bool,
        #[arg(long)]
        identity_store_dir: Option<String>,
    },
    Discover {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        verify_remote: bool,
        #[arg(long)]
        identity_store_dir: Option<String>,
    },
    Call {
        method: String,
        #[arg(long)]
        params: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        grpc_url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        device_id: Option<String>,
        #[arg(long)]
        channel: Option<String>,
    },
    UsageCost {
        #[arg(long)]
        db_path: Option<String>,
        #[arg(long, default_value_t = 30)]
        days: u32,
    },
    Install {
        #[arg(long)]
        service_name: Option<String>,
        #[arg(long)]
        bin_path: Option<String>,
        #[arg(long)]
        log_dir: Option<String>,
        #[arg(long, default_value_t = false)]
        start: bool,
    },
    Start,
    Stop,
    Restart,
    Uninstall,
    Logs {
        #[arg(long)]
        db_path: Option<String>,
        #[arg(long, default_value_t = 50)]
        lines: usize,
        #[arg(long, default_value_t = false)]
        follow: bool,
        #[arg(long, default_value_t = 1000)]
        poll_interval_ms: u64,
    },
    Status {
        #[arg(long)]
        url: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
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
        #[arg(long, default_value_t = false)]
        json: bool,
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
        #[arg(long, default_value_t = false)]
        json: bool,
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
        #[arg(long, default_value_t = false)]
        json: bool,
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
