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
        #[arg(long)]
        pairing_code: Option<String>,
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
        #[arg(long)]
        pairing_code: Option<String>,
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
