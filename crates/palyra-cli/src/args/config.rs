//! Arguments for `palyra config`: inspecting, validating, editing, migrating,
//! and recovering the local `palyra.toml` configuration. Help text is pinned by
//! snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ConfigCommand {
    #[command(about = "Show effective configuration status")]
    Status {
        #[arg(long, help = "Inspect this palyra.toml path")]
        path: Option<String>,
        #[arg(long, default_value_t = false, help = "Print config status as JSON")]
        json: bool,
    },
    #[command(about = "Resolve the active configuration file path")]
    Path {
        #[arg(long, help = "Resolve from this explicit palyra.toml path")]
        path: Option<String>,
        #[arg(long, default_value_t = false, help = "Print resolved config path as JSON")]
        json: bool,
    },
    #[command(about = "Validate the local configuration file")]
    Validate {
        #[arg(long, help = "Validate this palyra.toml path")]
        path: Option<String>,
        #[arg(long, default_value_t = false, help = "Print validation results as JSON")]
        json: bool,
    },
    #[command(visible_alias = "show", about = "List effective configuration values")]
    List {
        #[arg(long, help = "List values from this palyra.toml path")]
        path: Option<String>,
        #[arg(
            long,
            default_value_t = false,
            help = "Include secret values instead of redacting them"
        )]
        show_secrets: bool,
        #[arg(long, default_value_t = false, help = "Print configuration values as JSON")]
        json: bool,
    },
    #[command(about = "Read one configuration key")]
    Get {
        #[arg(long, help = "Read from this palyra.toml path")]
        path: Option<String>,
        #[arg(long, help = "Dotted configuration key to read")]
        key: String,
        #[arg(
            long,
            default_value_t = false,
            help = "Reveal secret values instead of redacting them"
        )]
        show_secrets: bool,
        #[arg(long, default_value_t = false, help = "Print the key value as JSON")]
        json: bool,
    },
    #[command(about = "Set one configuration key")]
    Set {
        #[arg(long, help = "Edit this palyra.toml path")]
        path: Option<String>,
        #[arg(long, help = "Dotted configuration key to set")]
        key: String,
        #[arg(
            long,
            value_name = "TOML_LITERAL",
            help = "TOML literal to write; simple strings may be passed without TOML quotes"
        )]
        value: String,
        #[arg(long, default_value_t = 5, help = "Number of backup files to retain")]
        backups: usize,
        #[arg(long, default_value_t = false, help = "Print the updated key as JSON")]
        json: bool,
    },
    #[command(about = "Remove one configuration key")]
    Unset {
        #[arg(long, help = "Edit this palyra.toml path")]
        path: Option<String>,
        #[arg(long, help = "Dotted configuration key to remove")]
        key: String,
        #[arg(long, default_value_t = 5, help = "Number of backup files to retain")]
        backups: usize,
    },
    #[command(about = "Migrate configuration to the current schema")]
    Migrate {
        #[arg(long, help = "Migrate this palyra.toml path")]
        path: Option<String>,
        #[arg(long, default_value_t = 5, help = "Number of backup files to retain")]
        backups: usize,
    },
    #[command(about = "Recover configuration from a retained backup")]
    Recover {
        #[arg(long, help = "Recover this palyra.toml path")]
        path: Option<String>,
        #[arg(long, default_value_t = 1, help = "Backup generation to restore")]
        backup: usize,
        #[arg(long, default_value_t = 5, help = "Number of backup files to retain")]
        backups: usize,
    },
}
