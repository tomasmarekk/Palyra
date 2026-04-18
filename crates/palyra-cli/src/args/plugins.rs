use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum PluginsCommand {
    List {
        #[arg(long)]
        plugin_id: Option<String>,
        #[arg(long)]
        skill_id: Option<String>,
        #[arg(long, default_value_t = false)]
        enabled_only: bool,
        #[arg(long, default_value_t = false)]
        ready_only: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(visible_alias = "info")]
    Inspect {
        plugin_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Discover {
        #[arg(long)]
        plugin_id: Option<String>,
        #[arg(long)]
        skill_id: Option<String>,
        #[arg(long, default_value_t = false)]
        enabled_only: bool,
        #[arg(long, default_value_t = false)]
        ready_only: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Check {
        plugin_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Explain {
        plugin_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Doctor {
        #[arg(long)]
        plugin_id: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(visible_alias = "bind")]
    Install {
        plugin_id: String,
        #[arg(long)]
        skill_id: Option<String>,
        #[arg(long)]
        skill_version: Option<String>,
        #[arg(long = "artifact", alias = "artifact-path")]
        artifact_path: Option<String>,
        #[arg(long)]
        tool_id: Option<String>,
        #[arg(long)]
        module_path: Option<String>,
        #[arg(long)]
        entrypoint: Option<String>,
        #[arg(long = "cap-http-host")]
        capability_http_hosts: Vec<String>,
        #[arg(long = "cap-secret")]
        capability_secrets: Vec<String>,
        #[arg(long = "cap-storage-prefix")]
        capability_storage_prefixes: Vec<String>,
        #[arg(long = "cap-channel")]
        capability_channels: Vec<String>,
        #[arg(long)]
        display_name: Option<String>,
        #[arg(long)]
        notes: Option<String>,
        #[arg(long)]
        owner_principal: Option<String>,
        #[arg(long = "tag")]
        tags: Vec<String>,
        #[arg(long)]
        config_json: Option<String>,
        #[arg(long, default_value_t = false)]
        clear_config: bool,
        #[arg(long, default_value_t = false)]
        disabled: bool,
        #[arg(long, default_value_t = false)]
        allow_tofu: bool,
        #[arg(long, default_value_t = false)]
        allow_untrusted: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Update {
        plugin_id: String,
        #[arg(long)]
        skill_id: Option<String>,
        #[arg(long)]
        skill_version: Option<String>,
        #[arg(long = "artifact", alias = "artifact-path")]
        artifact_path: Option<String>,
        #[arg(long)]
        tool_id: Option<String>,
        #[arg(long)]
        module_path: Option<String>,
        #[arg(long)]
        entrypoint: Option<String>,
        #[arg(long = "cap-http-host")]
        capability_http_hosts: Vec<String>,
        #[arg(long = "cap-secret")]
        capability_secrets: Vec<String>,
        #[arg(long = "cap-storage-prefix")]
        capability_storage_prefixes: Vec<String>,
        #[arg(long = "cap-channel")]
        capability_channels: Vec<String>,
        #[arg(long)]
        display_name: Option<String>,
        #[arg(long)]
        notes: Option<String>,
        #[arg(long)]
        owner_principal: Option<String>,
        #[arg(long = "tag")]
        tags: Vec<String>,
        #[arg(long)]
        config_json: Option<String>,
        #[arg(long, default_value_t = false)]
        clear_config: bool,
        #[arg(long, default_value_t = false)]
        disabled: bool,
        #[arg(long, default_value_t = false)]
        allow_tofu: bool,
        #[arg(long, default_value_t = false)]
        allow_untrusted: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Enable {
        plugin_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Disable {
        plugin_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Remove {
        plugin_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
