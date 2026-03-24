use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};

mod agent;
mod agents;
mod approvals;
mod auth;
mod browser;
mod channels;
mod completion;
mod config;
mod configure;
mod cron;
mod daemon;
mod init;
mod memory;
mod models;
mod onboarding;
#[cfg(not(windows))]
mod pairing;
mod patch;
mod policy;
mod protocol;
mod secrets;
mod security;
mod skills;
mod support_bundle;

pub use agent::AgentCommand;
pub use agents::AgentsCommand;
pub use approvals::{
    ApprovalDecisionArg, ApprovalDecisionScopeArg, ApprovalExportFormatArg,
    ApprovalResolveDecisionArg, ApprovalSubjectTypeArg, ApprovalsCommand,
};
pub use auth::{
    AuthCommand, AuthCredentialArg, AuthOpenAiCommand, AuthProfilesCommand, AuthProviderArg,
    AuthScopeArg,
};
pub use browser::BrowserCommand;
pub use channels::{ChannelsCommand, ChannelsDiscordCommand, ChannelsRouterCommand};
pub use completion::CompletionShell;
pub use config::ConfigCommand;
pub use configure::ConfigureSectionArg;
pub use cron::{CronCommand, CronConcurrencyPolicyArg, CronMisfirePolicyArg, CronScheduleTypeArg};
pub use daemon::{DaemonCommand, JournalCheckpointModeArg};
pub use init::{InitModeArg, InitTlsScaffoldArg};
pub use memory::{MemoryCommand, MemoryScopeArg, MemorySourceArg};
pub use models::ModelsCommand;
pub use onboarding::{
    GatewayBindProfileArg, OnboardingAuthMethodArg, OnboardingCommand, OnboardingFlowArg,
    RemoteVerificationModeArg, SetupWizardOverridesArg, WizardOverridesArg,
};
#[cfg(not(windows))]
pub use pairing::{PairingClientKindArg, PairingCommand, PairingMethodArg};
pub use patch::PatchCommand;
pub use policy::PolicyCommand;
pub use protocol::ProtocolCommand;
pub use secrets::{SecretsCommand, SecretsConfigureCommand};
pub use security::SecurityCommand;
pub use skills::{SkillsCommand, SkillsPackageCommand};
pub use support_bundle::SupportBundleCommand;

const ROOT_AFTER_HELP: &str = "\
Examples:
  palyra setup --mode local
  palyra gateway status
  palyra dashboard --open
  palyra --profile staging agents list --json
  palyra --config ./palyra.toml --output-format json status --admin

Canonical command map:
  setup      Preferred bootstrap/init workflow (`init` remains as a compatibility alias)
  configure  Guided reconfiguration workflow for an existing installation
  gateway    Preferred runtime/admin family (`daemon` remains as a compatibility alias)
  dashboard  Thin operator shortcut for dashboard URL discovery/open workflows
  onboarding Operator onboarding workflows (`onboard` remains as a compatibility alias)";

const SETUP_AFTER_HELP: &str = "\
Examples:
  palyra setup --mode local
  palyra setup --mode local --wizard
  palyra setup --mode remote --path ./config/palyra.toml --force

Discoverability:
  Use `palyra onboarding wizard --flow quickstart` for full guided onboarding.
  Use `palyra gateway status` after setup to verify runtime health.";

const ONBOARDING_AFTER_HELP: &str = "\
Examples:
  palyra onboarding wizard
  palyra onboarding wizard --flow manual
  palyra onboarding wizard --flow remote --non-interactive --accept-risk --remote-base-url https://dashboard.example.com/

Discoverability:
  Use `palyra setup --wizard` for bootstrap-first routing into the onboarding wizard.";

const CONFIGURE_AFTER_HELP: &str = "\
Examples:
  palyra configure
  palyra configure --section workspace --section auth-model
  palyra configure --non-interactive --section gateway --bind-profile public-tls --accept-risk

Discoverability:
  `configure` reuses the onboarding wizard engine to safely edit an existing installation.";

const GATEWAY_AFTER_HELP: &str = "\
Examples:
  palyra gateway status
  palyra gateway admin-status --token <token>
  palyra gateway dashboard-url --verify-remote --open

Discoverability:
  `palyra dashboard` is the thin shortcut for dashboard URL workflows.";

const DASHBOARD_AFTER_HELP: &str = "\
Examples:
  palyra dashboard
  palyra dashboard --open
  palyra dashboard --path ./palyra.toml --verify-remote --json";

const COMPLETION_AFTER_HELP: &str = "\
Examples:
  palyra completion --shell powershell
  palyra completion --shell bash > palyra.bash";

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OutputFormatArg {
    #[default]
    Text,
    Json,
    Ndjson,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LogLevelArg {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

#[derive(Debug, Clone, Args, PartialEq, Eq, Default)]
#[command(next_help_heading = "Global Options")]
pub struct RootOptions {
    #[arg(long, global = true)]
    pub profile: Option<String>,
    #[arg(long = "config", global = true)]
    pub config_path: Option<String>,
    #[arg(long, global = true)]
    pub state_root: Option<String>,
    #[arg(
        short = 'v',
        long,
        action = clap::ArgAction::Count,
        global = true,
        help = "Increase logging verbosity (-v => debug, -vv => trace)"
    )]
    pub verbose: u8,
    #[arg(long, value_enum, default_value_t = LogLevelArg::Info, global = true)]
    pub log_level: LogLevelArg,
    #[arg(long = "output-format", value_enum, default_value_t = OutputFormatArg::Text, global = true)]
    pub output_format: OutputFormatArg,
    #[arg(long, default_value_t = false, global = true)]
    pub plain: bool,
    #[arg(long, default_value_t = false, global = true)]
    pub no_color: bool,
}

#[derive(Debug, Parser)]
#[command(
    name = "palyra",
    version,
    about = "Palyra operator CLI",
    long_about = "Palyra operator CLI for secure local and remote runtime management.",
    propagate_version = true,
    disable_version_flag = true,
    arg_required_else_help = true,
    disable_help_subcommand = true,
    after_help = ROOT_AFTER_HELP,
)]
pub struct Cli {
    #[command(flatten)]
    pub root: RootOptions,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum Command {
    #[command(about = "Show CLI build metadata")]
    Version,
    #[command(
        visible_alias = "init",
        about = "Bootstrap a Palyra installation",
        after_long_help = SETUP_AFTER_HELP
    )]
    Setup {
        #[arg(long, value_enum, default_value_t = InitModeArg::Local)]
        mode: InitModeArg,
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[arg(long, value_enum, default_value_t = InitTlsScaffoldArg::BringYourOwn)]
        tls_scaffold: InitTlsScaffoldArg,
        #[arg(long, default_value_t = false)]
        wizard: bool,
        #[command(flatten)]
        wizard_options: SetupWizardOverridesArg,
    },
    Doctor {
        #[arg(long, default_value_t = false)]
        strict: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Show transport and admin status across HTTP/gRPC surfaces")]
    Status {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        grpc_url: Option<String>,
        #[arg(long, default_value_t = false)]
        admin: bool,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        device_id: Option<String>,
        #[arg(long)]
        channel: Option<String>,
    },
    Agent {
        #[command(subcommand)]
        command: AgentCommand,
    },
    Agents {
        #[command(subcommand)]
        command: AgentsCommand,
    },
    Cron {
        #[command(subcommand)]
        command: CronCommand,
    },
    Memory {
        #[command(subcommand)]
        command: MemoryCommand,
    },
    Approvals {
        #[command(subcommand)]
        command: ApprovalsCommand,
    },
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
    Channels {
        #[command(subcommand)]
        command: ChannelsCommand,
    },
    Browser {
        #[command(subcommand)]
        command: BrowserCommand,
    },
    #[command(
        about = "Generate shell completion scripts",
        after_long_help = COMPLETION_AFTER_HELP
    )]
    Completion {
        #[arg(long, value_enum)]
        shell: CompletionShell,
    },
    #[command(visible_alias = "onboard", after_long_help = ONBOARDING_AFTER_HELP)]
    Onboarding {
        #[command(subcommand)]
        command: OnboardingCommand,
    },
    #[command(
        about = "Safely reconfigure an existing installation",
        after_long_help = CONFIGURE_AFTER_HELP
    )]
    Configure {
        #[arg(long)]
        path: Option<String>,
        #[arg(long = "section", value_enum)]
        sections: Vec<ConfigureSectionArg>,
        #[arg(long, default_value_t = false)]
        non_interactive: bool,
        #[arg(long, default_value_t = false)]
        accept_risk: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(long)]
        workspace_root: Option<String>,
        #[arg(long, value_enum)]
        auth_method: Option<OnboardingAuthMethodArg>,
        #[arg(long)]
        api_key_env: Option<String>,
        #[arg(long, default_value_t = false)]
        api_key_stdin: bool,
        #[arg(long, default_value_t = false)]
        api_key_prompt: bool,
        #[arg(long, value_enum)]
        bind_profile: Option<GatewayBindProfileArg>,
        #[arg(long)]
        daemon_port: Option<u16>,
        #[arg(long)]
        grpc_port: Option<u16>,
        #[arg(long)]
        quic_port: Option<u16>,
        #[arg(long, value_enum)]
        tls_scaffold: Option<InitTlsScaffoldArg>,
        #[arg(long)]
        tls_cert_path: Option<String>,
        #[arg(long)]
        tls_key_path: Option<String>,
        #[arg(long)]
        remote_base_url: Option<String>,
        #[arg(long)]
        admin_token_env: Option<String>,
        #[arg(long, default_value_t = false)]
        admin_token_stdin: bool,
        #[arg(long, default_value_t = false)]
        admin_token_prompt: bool,
        #[arg(long, value_enum)]
        remote_verification: Option<RemoteVerificationModeArg>,
        #[arg(long)]
        pinned_server_cert_sha256: Option<String>,
        #[arg(long)]
        pinned_gateway_ca_sha256: Option<String>,
        #[arg(long)]
        ssh_target: Option<String>,
        #[arg(long, default_value_t = false)]
        skip_health: bool,
        #[arg(long, default_value_t = false)]
        skip_channels: bool,
        #[arg(long, default_value_t = false)]
        skip_skills: bool,
    },
    #[command(
        visible_alias = "daemon",
        about = "Gateway and runtime diagnostics surface",
        after_long_help = GATEWAY_AFTER_HELP
    )]
    Gateway {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    #[command(
        about = "Resolve or open the operator dashboard URL",
        after_long_help = DASHBOARD_AFTER_HELP
    )]
    Dashboard {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        verify_remote: bool,
        #[arg(long)]
        identity_store_dir: Option<String>,
        #[arg(long, default_value_t = false)]
        open: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    SupportBundle {
        #[command(subcommand)]
        command: SupportBundleCommand,
    },
    Policy {
        #[command(subcommand)]
        command: PolicyCommand,
    },
    Protocol {
        #[command(subcommand)]
        command: ProtocolCommand,
    },
    Config {
        #[command(subcommand)]
        command: Option<ConfigCommand>,
    },
    Models {
        #[command(subcommand)]
        command: ModelsCommand,
    },
    Patch {
        #[command(subcommand)]
        command: PatchCommand,
    },
    #[command(visible_alias = "skill")]
    Skills {
        #[command(subcommand)]
        command: SkillsCommand,
    },
    Secrets {
        #[command(subcommand)]
        command: SecretsCommand,
    },
    Security {
        #[command(subcommand)]
        command: SecurityCommand,
    },
    Tunnel {
        #[arg(long)]
        ssh: String,
        #[arg(long, default_value_t = 7142)]
        remote_port: u16,
        #[arg(long, default_value_t = 7142)]
        local_port: u16,
        #[arg(long, default_value_t = false)]
        open: bool,
        #[arg(long)]
        identity_file: Option<String>,
    },
    #[cfg(not(windows))]
    Pairing {
        #[command(subcommand)]
        command: PairingCommand,
    },
}

#[cfg(test)]
mod tests;
