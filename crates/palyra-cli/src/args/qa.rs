//! Arguments for `palyra qa`: QA Lab scenario validation and discovery.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum QaCommand {
    Validate {
        #[arg(long, default_value = "qa/scenarios")]
        path: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
