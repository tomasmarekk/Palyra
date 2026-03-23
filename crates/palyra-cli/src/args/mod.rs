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
mod cron;
mod daemon;
mod init;
mod memory;
mod onboarding;
#[cfg(not(windows))]
mod pairing;
mod patch;
mod policy;
mod protocol;
mod secrets;
mod skills;
mod support_bundle;

pub use agent::AgentCommand;
pub use agents::AgentsCommand;
pub use approvals::{ApprovalDecisionArg, ApprovalExportFormatArg, ApprovalsCommand};
pub use auth::{
    AuthCommand, AuthCredentialArg, AuthProfilesCommand, AuthProviderArg, AuthScopeArg,
};
pub use browser::BrowserCommand;
pub use channels::{ChannelsCommand, ChannelsDiscordCommand, ChannelsRouterCommand};
pub use completion::CompletionShell;
pub use config::ConfigCommand;
pub use cron::{CronCommand, CronConcurrencyPolicyArg, CronMisfirePolicyArg, CronScheduleTypeArg};
pub use daemon::{DaemonCommand, JournalCheckpointModeArg};
pub use init::{InitModeArg, InitTlsScaffoldArg};
pub use memory::{MemoryCommand, MemoryScopeArg, MemorySourceArg};
pub use onboarding::OnboardingCommand;
#[cfg(not(windows))]
pub use pairing::{PairingClientKindArg, PairingCommand, PairingMethodArg};
pub use patch::PatchCommand;
pub use policy::PolicyCommand;
pub use protocol::ProtocolCommand;
pub use secrets::SecretsCommand;
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
  gateway    Preferred runtime/admin family (`daemon` remains as a compatibility alias)
  dashboard  Thin operator shortcut for dashboard URL discovery/open workflows
  onboarding Operator onboarding workflows (`onboard` remains as a compatibility alias)";

const SETUP_AFTER_HELP: &str = "\
Examples:
  palyra setup --mode local
  palyra setup --mode remote --path ./config/palyra.toml --force

Discoverability:
  Use `palyra gateway status` after setup to verify runtime health.";

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
    #[command(visible_alias = "onboard")]
    Onboarding {
        #[command(subcommand)]
        command: OnboardingCommand,
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
        command: ConfigCommand,
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
