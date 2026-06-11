//! Arguments for `palyra uninstall`: installer-aware package removal with
//! optional state cleanup. `--dry-run` and `--yes` are mutually exclusive so a
//! preview can never double as confirmation. Help text is pinned by snapshot
//! tests; see the doc-comment rules in `mod.rs`.

use clap::Args;

#[derive(Debug, Clone, Args, PartialEq, Eq)]
pub struct UninstallCommand {
    #[arg(long)]
    pub install_root: Option<String>,
    #[arg(long, default_value_t = false)]
    pub remove_state: bool,
    #[arg(long, default_value_t = false, conflicts_with = "dry_run")]
    pub yes: bool,
    #[arg(long, default_value_t = false, conflicts_with = "yes")]
    pub dry_run: bool,
}
