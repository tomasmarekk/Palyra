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
        // AIDEV-NOTE: `bool` with `default_value_t = true` derives clap's SetTrue
        // action, so this flag parses as true whether or not it is passed;
        // clearing non-revoked devices is currently unreachable from the CLI.
        // Exposing the false case is a behavior change (ArgAction::Set or a
        // --no-* flag) and would re-pin help snapshots.
        #[arg(long, default_value_t = true)]
        revoked_only: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
