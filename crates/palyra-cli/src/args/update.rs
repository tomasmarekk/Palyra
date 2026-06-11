//! Arguments for `palyra update`: packaged update checks and application.
//! `--dry-run` and `--yes` are mutually exclusive so a preview can never double
//! as confirmation. Help text is pinned by snapshot tests; see the doc-comment
//! rules in `mod.rs`.

use clap::Args;

#[derive(Debug, Clone, Args, PartialEq, Eq)]
pub struct UpdateCommand {
    #[arg(long)]
    pub install_root: Option<String>,
    #[arg(long)]
    pub archive: Option<String>,
    #[arg(long, default_value_t = false)]
    pub check: bool,
    #[arg(long, default_value_t = false, conflicts_with = "yes")]
    pub dry_run: bool,
    #[arg(long, default_value_t = false, conflicts_with = "dry_run")]
    pub yes: bool,
    #[arg(long, default_value_t = false)]
    pub skip_service_restart: bool,
    #[arg(long, default_value_t = false)]
    pub json: bool,
}
