//! Arguments for `palyra protocol`: protocol version reporting plus contract
//! and id validation. Help text is pinned by snapshot tests; see the
//! doc-comment rules in `mod.rs`.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ProtocolCommand {
    Version {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Validate {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    ValidateId {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
