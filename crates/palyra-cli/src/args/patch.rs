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
    Bundles {
        #[command(subcommand)]
        command: PatchBundleCommand,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum PatchBundleCommand {
    List {
        #[arg(long)]
        store: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Show {
        id: String,
        #[arg(long)]
        store: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Approve {
        id: String,
        #[arg(long)]
        store: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Apply {
        id: String,
        #[arg(long)]
        workspace_root: Option<String>,
        #[arg(long)]
        store: Option<String>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Discard {
        id: String,
        #[arg(long)]
        store: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
