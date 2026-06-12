//! Arguments for `palyra devices`: paired operator device listing, key
//! rotation, revocation, removal, and cleanup. Help text is pinned by snapshot
//! tests; see the doc-comment rules in `mod.rs`.

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
        #[arg(
            long,
            default_value_t = true,
            action = clap::ArgAction::Set,
            value_parser = clap::value_parser!(bool)
        )]
        revoked_only: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
