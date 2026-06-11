//! Arguments for `palyra hooks`: event-driven automation bindings over trusted
//! plugins (list/info/check/bind/enable/disable/remove). Help text is pinned by
//! snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum HooksCommand {
    List {
        #[arg(long)]
        hook_id: Option<String>,
        #[arg(long)]
        plugin_id: Option<String>,
        #[arg(long)]
        event: Option<String>,
        #[arg(long, default_value_t = false)]
        enabled_only: bool,
        #[arg(long, default_value_t = false)]
        ready_only: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Info {
        hook_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Check {
        hook_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(visible_alias = "install")]
    Bind {
        hook_id: String,
        #[arg(long)]
        event: String,
        #[arg(long)]
        plugin_id: String,
        #[arg(long)]
        display_name: Option<String>,
        #[arg(long)]
        notes: Option<String>,
        #[arg(long)]
        owner_principal: Option<String>,
        #[arg(long, default_value_t = false)]
        disabled: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Enable {
        hook_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Disable {
        hook_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Remove {
        hook_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
