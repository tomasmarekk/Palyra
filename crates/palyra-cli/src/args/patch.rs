//! Arguments for `palyra patch`: applying workspace patches through the guarded
//! patch engine. Help text is pinned by snapshot tests; see the doc-comment
//! rules in `mod.rs`.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum PatchCommand {
    Apply {
        #[arg(long)]
        workspace_root: Option<String>,
        #[arg(long, default_value_t = false)]
        stdin: bool,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
