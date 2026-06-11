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
    #[command(about = "Create a portable backup archive")]
    Create {
        #[arg(long, help = "Write the backup archive to this path")]
        output: Option<String>,
        #[arg(long, help = "Read configuration from this palyra.toml path")]
        config_path: Option<String>,
        #[arg(long, help = "Read runtime state from this state root")]
        state_root: Option<String>,
        #[arg(long, help = "Include this workspace root when workspace backup is enabled")]
        workspace_root: Option<String>,
        #[arg(long = "include", value_enum, help = "Include this backup component")]
        include: Vec<BackupComponentArg>,
        #[arg(long, default_value_t = false, help = "Include the configured workspace files")]
        include_workspace: bool,
        #[arg(long, default_value_t = false, help = "Include a support bundle in the archive")]
        include_support_bundle: bool,
        #[arg(long, default_value_t = false, help = "Overwrite an existing output archive")]
        force: bool,
        #[arg(long, default_value_t = false, help = "Print backup creation results as JSON")]
        json: bool,
    },
    #[command(about = "Verify a portable backup archive")]
    Verify {
        #[arg(long, help = "Path to the backup archive to verify")]
        archive: String,
        #[arg(long, default_value_t = false, help = "Print backup verification results as JSON")]
        json: bool,
    },
}
