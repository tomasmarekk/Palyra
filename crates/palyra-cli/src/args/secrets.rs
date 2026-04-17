use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum SecretsCommand {
    Set {
        scope: String,
        key: String,
        #[arg(long, default_value_t = false)]
        value_stdin: bool,
    },
    Get {
        scope: String,
        key: String,
        #[arg(long, default_value_t = false)]
        reveal: bool,
    },
    List {
        scope: String,
    },
    Delete {
        scope: String,
        key: String,
    },
    Audit {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        offline: bool,
        #[arg(long, default_value_t = false)]
        strict: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Apply {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        offline: bool,
        #[arg(long, default_value_t = false)]
        strict: bool,
        #[arg(long, default_value_t = false)]
        runtime: bool,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Inventory {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Explain {
        secret_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Plan {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Configure {
        #[command(subcommand)]
        command: SecretsConfigureCommand,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum SecretsConfigureCommand {
    OpenaiApiKey {
        scope: String,
        key: String,
        #[arg(long, default_value_t = false)]
        value_stdin: bool,
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = 5)]
        backups: usize,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    BrowserStateKey {
        scope: String,
        key: String,
        #[arg(long, default_value_t = false)]
        value_stdin: bool,
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = 5)]
        backups: usize,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
