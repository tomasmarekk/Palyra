//! Arguments for `palyra config`: inspecting, validating, editing, migrating,
//! and recovering the local `palyra.toml` configuration. Help text is pinned by
//! snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ConfigCommand {
    Status {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Path {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Validate {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(visible_alias = "show")]
    List {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        show_secrets: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Get {
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        key: String,
        #[arg(long, default_value_t = false)]
        show_secrets: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Set {
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        key: String,
        #[arg(
            long,
            value_name = "TOML_LITERAL",
            help = "TOML literal to write; simple strings may be passed without TOML quotes"
        )]
        value: String,
        #[arg(long, default_value_t = 5)]
        backups: usize,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Unset {
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        key: String,
        #[arg(long, default_value_t = 5)]
        backups: usize,
    },
    Migrate {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = 5)]
        backups: usize,
    },
    Recover {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = 1)]
        backup: usize,
        #[arg(long, default_value_t = 5)]
        backups: usize,
    },
}
