use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum DevicesCommand {
    List {
        #[arg(long, default_value_t = false, conflicts_with = "ndjson")]
        json: bool,
        #[arg(long, default_value_t = false, conflicts_with = "json")]
        ndjson: bool,
    },
    Show {
        device_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Rotate {
        device_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Revoke {
        device_id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Remove {
        device_id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Clear {
        #[arg(long, default_value_t = true)]
        revoked_only: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
