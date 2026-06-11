//! Arguments for `palyra nodes`: remote node inventory, pairing-request
//! approval/rejection, and capability invocation over node RPC. Help text is
//! pinned by snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum NodesCommand {
    List {
        #[arg(long, default_value_t = false, conflicts_with = "ndjson")]
        json: bool,
        #[arg(long, default_value_t = false, conflicts_with = "json")]
        ndjson: bool,
    },
    Pending {
        #[arg(long, default_value_t = false, conflicts_with = "ndjson")]
        json: bool,
        #[arg(long, default_value_t = false, conflicts_with = "json")]
        ndjson: bool,
    },
    Approve {
        request_id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Reject {
        request_id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Describe {
        device_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Status {
        device_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Invoke {
        device_id: String,
        capability: String,
        #[arg(long)]
        input_json: Option<String>,
        #[arg(long, default_value_t = false)]
        input_stdin: bool,
        #[arg(long)]
        max_payload_bytes: Option<u64>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
