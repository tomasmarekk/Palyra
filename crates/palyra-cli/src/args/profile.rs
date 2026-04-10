use clap::{Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};

const PROFILE_AFTER_HELP: &str = "\
Examples:
  palyra profile list
  palyra profile create staging --mode remote --set-default
  palyra profile clone prod staging --json
  palyra profile export prod --output ./artifacts/prod-profile.json
  palyra profile export prod --mode encrypted --password-stdin --output ./artifacts/prod-profile.enc
  palyra profile import --input ./artifacts/staging-profile.enc --password-stdin --json
  palyra profile show
  palyra profile use prod
  palyra profile rename stage staging
  palyra profile delete old-sandbox --yes

Discoverability:
  `profile show` without a name resolves the active profile.
  `profile create`, `profile clone`, and `profile import` assign isolated per-profile state roots.
  `profile clone` and `profile import` also write copied config snapshots into isolated per-profile config paths when source config is available.";

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileModeArg {
    Local,
    Remote,
    Custom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileRiskLevelArg {
    Low,
    Elevated,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileExportModeArg {
    Redacted,
    Encrypted,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
#[command(after_long_help = PROFILE_AFTER_HELP)]
pub enum ProfileCommand {
    List {
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(long, default_value_t = false)]
        ndjson: bool,
    },
    Show {
        name: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Create {
        name: String,
        #[arg(long, value_enum, default_value_t = ProfileModeArg::Local)]
        mode: ProfileModeArg,
        #[arg(long)]
        label: Option<String>,
        #[arg(long)]
        environment: Option<String>,
        #[arg(long)]
        color: Option<String>,
        #[arg(long, value_enum)]
        risk_level: Option<ProfileRiskLevelArg>,
        #[arg(long, default_value_t = false)]
        strict_mode: bool,
        #[arg(long)]
        config_path: Option<String>,
        #[arg(long)]
        state_root: Option<String>,
        #[arg(long)]
        daemon_url: Option<String>,
        #[arg(long)]
        grpc_url: Option<String>,
        #[arg(long)]
        admin_token_env: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        device_id: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        set_default: bool,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Clone {
        name: String,
        new_name: String,
        #[arg(long)]
        label: Option<String>,
        #[arg(long)]
        environment: Option<String>,
        #[arg(long)]
        color: Option<String>,
        #[arg(long, value_enum)]
        risk_level: Option<ProfileRiskLevelArg>,
        #[arg(long, default_value_t = false)]
        strict_mode: bool,
        #[arg(long, default_value_t = false)]
        set_default: bool,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Export {
        name: Option<String>,
        #[arg(long)]
        output: String,
        #[arg(long, value_enum, default_value_t = ProfileExportModeArg::Redacted)]
        mode: ProfileExportModeArg,
        #[arg(long, default_value_t = false)]
        password_stdin: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Import {
        #[arg(long)]
        input: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long, default_value_t = false)]
        password_stdin: bool,
        #[arg(long, default_value_t = false)]
        set_default: bool,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Use {
        name: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Rename {
        name: String,
        new_name: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Delete {
        name: String,
        #[arg(long, default_value_t = false)]
        yes: bool,
        #[arg(long, default_value_t = false)]
        delete_state_root: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
