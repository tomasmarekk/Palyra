use clap::Subcommand;

const VAULT_SCOPE_HELP: &str =
    "Secret scope: global | principal:<id> | channel:<name>:<account_id> | skill:<skill_id>";

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum SecretsCommand {
    Set {
        #[arg(help = VAULT_SCOPE_HELP)]
        scope: String,
        key: String,
        #[arg(long, default_value_t = false)]
        value_stdin: bool,
    },
    Get {
        #[arg(help = VAULT_SCOPE_HELP)]
        scope: String,
        key: String,
        #[arg(long, default_value_t = false)]
        reveal: bool,
    },
    List {
        #[arg(help = VAULT_SCOPE_HELP)]
        scope: String,
    },
    Delete {
        #[arg(help = VAULT_SCOPE_HELP)]
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
        #[arg(help = VAULT_SCOPE_HELP)]
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
        #[arg(help = VAULT_SCOPE_HELP)]
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
