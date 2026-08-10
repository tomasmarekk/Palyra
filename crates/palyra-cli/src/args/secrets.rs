//! Arguments for `palyra secrets`: scoped vault secret CRUD, configuration
//! audits/plans, and guided secret configuration. Secret values are never
//! accepted on argv; `--value-stdin` selects stdin input. Help text is pinned
//! by snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::Subcommand;

const VAULT_SCOPE_HELP: &str =
    "Secret scope: global | principal:<id> | channel:<name>:<account_id> | skill:<skill_id>";
const VALUE_STDIN_HELP: &str =
    "Read the secret value from stdin; secret values are never accepted as argv";

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum SecretsCommand {
    Set {
        #[arg(help = VAULT_SCOPE_HELP)]
        scope: String,
        key: String,
        #[arg(long, default_value_t = false, help = VALUE_STDIN_HELP)]
        value_stdin: bool,
    },
    Get {
        #[arg(help = VAULT_SCOPE_HELP)]
        scope: String,
        key: String,
        #[arg(long, default_value_t = false)]
        reveal: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    List {
        #[arg(default_value = "global", help = VAULT_SCOPE_HELP)]
        scope: String,
        #[arg(long, default_value_t = false)]
        json: bool,
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
    #[command(about = "Inventory configured secret references and local vault keys")]
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
        #[arg(long, default_value_t = false, help = VALUE_STDIN_HELP)]
        value_stdin: bool,
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = 0)]
        backups: usize,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    BrowserStateKey {
        #[arg(help = VAULT_SCOPE_HELP)]
        scope: String,
        key: String,
        #[arg(long, default_value_t = false, help = VALUE_STDIN_HELP)]
        value_stdin: bool,
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = 0)]
        backups: usize,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
