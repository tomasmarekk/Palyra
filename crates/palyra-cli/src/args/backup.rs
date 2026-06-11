//! Arguments for `palyra backup`: portable operator backup creation and archive
//! verification. Help text is pinned by snapshot tests; see the doc-comment
//! rules in `mod.rs`.

use clap::{Subcommand, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BackupComponentArg {
    Config,
    State,
    Workspace,
    SupportBundle,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum BackupCommand {
    Create {
        #[arg(long)]
        output: Option<String>,
        #[arg(long)]
        config_path: Option<String>,
        #[arg(long)]
        state_root: Option<String>,
        #[arg(long)]
        workspace_root: Option<String>,
        #[arg(long = "include", value_enum)]
        include: Vec<BackupComponentArg>,
        #[arg(long, default_value_t = false)]
        include_workspace: bool,
        #[arg(long, default_value_t = false)]
        include_support_bundle: bool,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Verify {
        #[arg(long)]
        archive: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
