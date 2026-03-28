use clap::{Subcommand, ValueEnum};

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum PairingCommand {
    List {
        #[arg(long)]
        client_kind: Option<String>,
        #[arg(long, value_enum)]
        state: Option<PairingStateArg>,
        #[arg(long, default_value_t = false, conflicts_with = "ndjson")]
        json: bool,
        #[arg(long, default_value_t = false, conflicts_with = "json")]
        ndjson: bool,
    },
    Code {
        #[arg(long, value_enum, default_value_t = PairingMethodArg::Pin)]
        method: PairingMethodArg,
        #[arg(long)]
        issued_by: Option<String>,
        #[arg(long)]
        ttl_ms: Option<u64>,
        #[arg(long, default_value_t = false)]
        json: bool,
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
    #[command(hide = true)]
    Pair {
        #[arg(long)]
        device_id: String,
        #[arg(long, value_enum, default_value_t = PairingClientKindArg::Node)]
        client_kind: PairingClientKindArg,
        #[arg(long, value_enum, default_value_t = PairingMethodArg::Pin)]
        method: PairingMethodArg,
        #[arg(
            long,
            hide = true,
            conflicts_with = "proof_stdin",
            requires = "allow_insecure_proof_arg"
        )]
        proof: Option<String>,
        #[arg(long, default_value_t = false)]
        proof_stdin: bool,
        #[arg(long, default_value_t = false)]
        allow_insecure_proof_arg: bool,
        #[arg(long)]
        store_dir: Option<String>,
        #[arg(long, default_value_t = false)]
        approve: bool,
        #[arg(long, default_value_t = false)]
        simulate_rotation: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PairingClientKindArg {
    Cli,
    Desktop,
    Node,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PairingMethodArg {
    Pin,
    Qr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PairingStateArg {
    PendingApproval,
    Approved,
    Rejected,
    Completed,
    Expired,
}

impl PairingMethodArg {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pin => "pin",
            Self::Qr => "qr",
        }
    }
}

impl PairingStateArg {
    #[must_use]
    pub const fn as_contract_str(self) -> &'static str {
        match self {
            Self::PendingApproval => "pending_approval",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Completed => "completed",
            Self::Expired => "expired",
        }
    }
}
