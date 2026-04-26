use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ProtocolCommand {
    Version {
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
