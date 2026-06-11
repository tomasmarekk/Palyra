//! Arguments for `palyra webhooks`: webhook-backed integration management with
//! vault-referenced signing secrets and event/source allowlists. Help text is
//! pinned by snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum WebhooksCommand {
    List {
        #[arg(long)]
        provider: Option<String>,
        #[arg(long)]
        enabled: Option<bool>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Show {
        integration_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Add {
        integration_id: String,
        provider: String,
        #[arg(long)]
        display_name: Option<String>,
        #[arg(long = "secret-ref")]
        secret_vault_ref: String,
        #[arg(long = "allow-event")]
        allowed_events: Vec<String>,
        #[arg(long = "allow-source")]
        allowed_sources: Vec<String>,
        #[arg(long, default_value_t = false)]
        disabled: bool,
        #[arg(long, default_value_t = false)]
        require_signature: bool,
        #[arg(long)]
        max_payload_bytes: Option<u64>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Enable {
        integration_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Disable {
        integration_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Remove {
        integration_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(visible_alias = "verify")]
    Test {
        integration_id: String,
        #[arg(long, default_value_t = false, conflicts_with = "payload_file")]
        payload_stdin: bool,
        #[arg(long, conflicts_with = "payload_stdin")]
        payload_file: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
