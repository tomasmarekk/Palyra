//! Arguments for `palyra protocol`: protocol version reporting plus contract
//! and id validation. Help text is pinned by snapshot tests; see the
//! doc-comment rules in `mod.rs`.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ProtocolCommand {
    #[command(about = "Show protocol and schema contract versions")]
    Version {
        #[arg(long, default_value_t = false, help = "Print protocol version data as JSON")]
        json: bool,
    },
    #[command(about = "Validate committed protocol contract artifacts")]
    Validate {
        #[arg(long, default_value_t = false, help = "Print protocol validation results as JSON")]
        json: bool,
    },
    #[command(about = "Fetch the live runtime method and scope registry")]
    Methods {
        #[arg(long, default_value_t = false, help = "Print method registry as JSON")]
        json: bool,
    },
    #[command(about = "Validate a protocol identifier string")]
    ValidateId {
        #[arg(long, help = "Protocol identifier to validate")]
        id: String,
        #[arg(long, default_value_t = false, help = "Print identifier validation results as JSON")]
        json: bool,
    },
}
