//! Arguments for `palyra reset`: destructive local recovery with explicit scope
//! selection. `--dry-run` and `--yes` are mutually exclusive so a preview can
//! never double as confirmation. Help text is pinned by snapshot tests; see the
//! doc-comment rules in `mod.rs`.

use clap::{Args, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ResetScopeArg {
    Config,
    State,
    Service,
    Workspace,
}

#[derive(Debug, Clone, Args, PartialEq, Eq)]
pub struct ResetCommand {
    #[arg(long = "scope", value_enum, required = true)]
    pub scopes: Vec<ResetScopeArg>,
    #[arg(long)]
    pub config_path: Option<String>,
    #[arg(long)]
    pub workspace_root: Option<String>,
    #[arg(long, default_value_t = false, conflicts_with = "yes")]
    pub dry_run: bool,
    #[arg(long, default_value_t = false, conflicts_with = "dry_run")]
    pub yes: bool,
}
