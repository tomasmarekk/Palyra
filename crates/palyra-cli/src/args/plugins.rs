//! Arguments for `palyra plugins`: trusted WASM plugin discovery, binding
//! (install/update with capability grants), and lifecycle control. Plugin
//! config JSON may carry secrets, so file and stdin input variants exist
//! alongside the inline form. Help text is pinned by snapshot tests; see the
//! doc-comment rules in `mod.rs`.

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
    Health {
        #[arg(long)]
        plugin_id: Option<String>,
        #[arg(long)]
        skill_id: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Validate {
        #[arg(long = "artifact", alias = "artifact-path")]
        artifact_path: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    DryRun {
        #[arg(long = "artifact", alias = "artifact-path")]
        artifact_path: String,
        #[arg(long)]
        hook_event: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Permissions {
        #[arg(long = "artifact", alias = "artifact-path")]
        artifact_path: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Test {
        #[arg(long = "artifact", alias = "artifact-path")]
        artifact_path: String,
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
        #[arg(
            long,
            conflicts_with_all = ["config_json_file", "config_json_stdin"],
            help = "Inline JSON object for non-secret config. Use --config-json-file or --config-json-stdin for secret-bearing config."
        )]
        config_json: Option<String>,
        #[arg(
            long = "config-json-file",
            conflicts_with_all = ["config_json", "config_json_stdin"],
            help = "Read plugin config JSON object from a local file instead of argv."
        )]
        config_json_file: Option<String>,
        #[arg(
            long = "config-json-stdin",
            conflicts_with_all = ["config_json", "config_json_file"],
            default_value_t = false,
            help = "Read plugin config JSON object from stdin instead of argv."
        )]
        config_json_stdin: bool,
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
        #[arg(
            long,
            conflicts_with_all = ["config_json_file", "config_json_stdin"],
            help = "Inline JSON object for non-secret config. Use --config-json-file or --config-json-stdin for secret-bearing config."
        )]
        config_json: Option<String>,
        #[arg(
            long = "config-json-file",
            conflicts_with_all = ["config_json", "config_json_stdin"],
            help = "Read plugin config JSON object from a local file instead of argv."
        )]
        config_json_file: Option<String>,
        #[arg(
            long = "config-json-stdin",
            conflicts_with_all = ["config_json", "config_json_file"],
            default_value_t = false,
            help = "Read plugin config JSON object from stdin instead of argv."
        )]
        config_json_stdin: bool,
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
