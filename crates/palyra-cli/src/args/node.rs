//! Arguments for `palyra node`: local node runtime pairing, service
//! install/lifecycle, and status. Pairing codes are read from stdin by default;
//! passing one on argv requires the explicit insecure-arg acknowledgement flag.
//! Help text is pinned by snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::Subcommand;

use super::PairingMethodArg;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum NodeCommand {
    Run {
        #[arg(long)]
        grpc_url: Option<String>,
        #[arg(long)]
        gateway_ca_file: Option<String>,
        #[arg(long)]
        device_id: Option<String>,
        #[arg(long, value_enum)]
        method: Option<PairingMethodArg>,
        #[arg(
            long,
            conflicts_with = "pairing_code_stdin",
            requires = "allow_insecure_pairing_code_arg",
            help = "Read the node pairing code from argv after acknowledging process-list exposure"
        )]
        pairing_code: Option<String>,
        #[arg(long, default_value_t = false, conflicts_with = "pairing_code")]
        pairing_code_stdin: bool,
        #[arg(
            long,
            default_value_t = false,
            help = "Allow --pairing-code despite process-list exposure; prefer --pairing-code-stdin"
        )]
        allow_insecure_pairing_code_arg: bool,
        #[arg(long)]
        poll_interval_ms: Option<u64>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Status {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Install {
        #[arg(long)]
        grpc_url: Option<String>,
        #[arg(long)]
        gateway_ca_file: Option<String>,
        #[arg(long)]
        device_id: Option<String>,
        #[arg(long, value_enum)]
        method: Option<PairingMethodArg>,
        #[arg(
            long,
            conflicts_with = "pairing_code_stdin",
            requires = "allow_insecure_pairing_code_arg",
            help = "Read the node pairing code from argv after acknowledging process-list exposure"
        )]
        pairing_code: Option<String>,
        #[arg(long, default_value_t = false, conflicts_with = "pairing_code")]
        pairing_code_stdin: bool,
        #[arg(
            long,
            default_value_t = false,
            help = "Allow --pairing-code despite process-list exposure; prefer --pairing-code-stdin"
        )]
        allow_insecure_pairing_code_arg: bool,
        #[arg(long, default_value_t = false)]
        start: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Start {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Stop {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Restart {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Uninstall {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
