use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};

mod acp;
mod agent;
mod agents;
mod approvals;
mod auth;
mod backup;
mod browser;
mod channels;
mod completion;
mod config;
mod configure;
mod cron;
mod daemon;
mod devices;
mod docs;
mod hooks;
mod init;
mod memory;
mod message;
mod models;
mod node;
mod nodes;
mod onboarding;
mod pairing;
mod patch;
mod plugins;
mod policy;
mod protocol;
mod reset;
mod sandbox;
mod secrets;
mod security;
mod sessions;
mod skills;
mod support_bundle;
mod system;
mod tui;
mod uninstall;
mod update;
mod webhooks;

pub use acp::{
    AcpBridgeArgs, AcpCommand, AcpConnectionArgs, AcpSessionDefaultsArgs, AcpShimArgs,
    AcpSubcommand,
};
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
pub use backup::{BackupCommand, BackupComponentArg};
pub use browser::{
    BrowserCommand, BrowserPermissionsCommand, BrowserProfilesCommand, BrowserSessionCommand,
    BrowserTabsCommand,
};
pub use channels::{
    ChannelProviderArg, ChannelResolveEntityArg, ChannelsCommand, ChannelsDiscordCommand,
    ChannelsRouterCommand,
};
pub use completion::CompletionShell;
pub use config::ConfigCommand;
pub use configure::ConfigureSectionArg;
pub use cron::{CronCommand, CronConcurrencyPolicyArg, CronMisfirePolicyArg, CronScheduleTypeArg};
pub use daemon::{DaemonCommand, JournalCheckpointModeArg};
pub use devices::DevicesCommand;
pub use docs::DocsCommand;
pub use hooks::HooksCommand;
pub use init::{InitModeArg, InitTlsScaffoldArg};
pub use memory::{MemoryCommand, MemoryScopeArg, MemorySourceArg};
pub use message::MessageCommand;
pub use models::ModelsCommand;
pub use node::NodeCommand;
pub use nodes::NodesCommand;
pub use onboarding::{
    GatewayBindProfileArg, OnboardingAuthMethodArg, OnboardingCommand, OnboardingFlowArg,
    RemoteVerificationModeArg, SetupWizardOverridesArg, WizardOverridesArg,
};
pub use pairing::{PairingClientKindArg, PairingCommand, PairingMethodArg, PairingStateArg};
pub use patch::PatchCommand;
pub use plugins::PluginsCommand;
pub use policy::PolicyCommand;
pub use protocol::ProtocolCommand;
pub use reset::{ResetCommand, ResetScopeArg};
pub use sandbox::{SandboxCommand, SandboxRuntimeArg};
pub use secrets::{SecretsCommand, SecretsConfigureCommand};
pub use security::SecurityCommand;
pub use sessions::SessionsCommand;
pub use skills::{SkillsCommand, SkillsPackageCommand};
pub use support_bundle::SupportBundleCommand;
pub use system::{SystemCommand, SystemEventCommand, SystemEventSeverityArg};
pub use tui::TuiCommand;
pub use uninstall::UninstallCommand;
pub use update::UpdateCommand;
pub use webhooks::WebhooksCommand;

const ROOT_AFTER_HELP: &str = "\
Examples:
  palyra setup --mode local
  palyra acp --session-key ops:triage
  palyra docs search acp
  palyra gateway status
  palyra dashboard --open
  palyra backup create --output ./artifacts/palyra-backup.zip
  palyra system heartbeat
  palyra sandbox explain --runtime process-runner
  palyra update --check
  palyra --profile staging agents list --json
  palyra --config ./palyra.toml --output-format json status --admin

Canonical command map:
  setup      Preferred bootstrap/init workflow (`init` remains as a compatibility alias)
  configure  Guided reconfiguration workflow for an existing installation
  acp        Preferred ACP stdio bridge entry point (`agent acp` remains compatible)
  docs       Local committed docs/help discovery surface
  gateway    Preferred runtime/admin family (`daemon` remains as a compatibility alias)
  dashboard  Thin operator shortcut for dashboard URL discovery/open workflows
  backup     Portable lifecycle backup/create verification surface
  system     Runtime heartbeat, presence, and recent system-event observability
  sandbox    Effective isolation/runtime policy explain surface for process and WASM tooling
  plugins    Trusted WASM plugin binding and lifecycle surface
  hooks      Event-driven automation bindings over trusted plugins
  reset      Destructive local recovery surface with explicit scope selection
  uninstall  Installer-aware package removal surface
  update     Package update/check orchestration surface
  onboarding Operator onboarding workflows (`onboard` remains as a compatibility alias)
  webhooks   Webhook-backed integration management surface";

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
  palyra gateway run
  palyra gateway health
  palyra gateway probe
  palyra gateway discover --verify-remote
  palyra gateway call health
  palyra gateway usage-cost --days 7
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

const BACKUP_AFTER_HELP: &str = "\
Examples:
  palyra backup create --output ./artifacts/palyra-backup.zip
  palyra backup create --include workspace --workspace-root ./workspace --include-support-bundle
  palyra backup verify --archive ./artifacts/palyra-backup.zip";

const RESET_AFTER_HELP: &str = "\
Examples:
  palyra reset --scope service --dry-run
  palyra reset --scope state --scope service --yes
  palyra reset --scope config --config-path ./palyra.toml --dry-run";

const UNINSTALL_AFTER_HELP: &str = "\
Examples:
  palyra uninstall --install-root ./install --dry-run
  palyra uninstall --install-root ./install --remove-state --yes";

const UPDATE_AFTER_HELP: &str = "\
Examples:
  palyra update --check
  palyra update --install-root ./install --archive ./artifacts/palyra-headless.zip --dry-run
  palyra update --install-root ./install --archive ./artifacts/palyra-headless.zip --yes --skip-service-restart";

const HEALTH_AFTER_HELP: &str = "\
Examples:
  palyra health
  palyra health --output-format json
  palyra gateway health --url http://127.0.0.1:7142";

const LOGS_AFTER_HELP: &str = "\
Examples:
  palyra logs
  palyra logs --lines 100
  palyra logs --follow";

const COMPLETION_AFTER_HELP: &str = "\
Examples:
  palyra completion --shell powershell
  palyra completion --shell bash > palyra.bash";

const WEBHOOKS_AFTER_HELP: &str = "\
Examples:
  palyra webhooks list
  palyra webhooks add github_repo_a github --secret-ref global/github_repo_a --allow-event push --allow-source github.repo_a
  palyra webhooks test github_repo_a --payload-stdin

Discoverability:
  `webhooks` manages secret-aware webhook integrations without exposing a public ingress surface by default.";

const ACP_AFTER_HELP: &str = "\
Examples:
  palyra acp
  palyra acp --session-key ops:triage --session-label \"Ops Triage\"
  palyra acp --require-existing
  palyra acp shim --session-id 01ARZ3NDEKTSV4RRFFQ69G5FAW --prompt \"hello\"
  palyra acp shim --ndjson-stdin

Discoverability:
  `acp` is the preferred ACP bridge entry point. `palyra agent acp` and `palyra agent acp-shim` remain compatible.
  CLI defaults for `session_key`, `session_label`, `require_existing`, and `reset_session` seed bridge behavior; `_meta` prompt overrides still win when present.";

const DOCS_AFTER_HELP: &str = "\
Examples:
  palyra docs list
  palyra docs search acp
  palyra docs show cli-v1-acp-shim
  palyra docs show docs/architecture/browser-service-v1.md

Discoverability:
  `docs` indexes committed `docs/` markdown plus CLI help snapshots for local, offline lookup.";

const BROWSER_AFTER_HELP: &str = "\
Examples:
  palyra browser status
  palyra browser start --wait-ms 15000
  palyra browser profiles list
  palyra browser session create --allow-domain docs.palyra.dev
  palyra browser navigate <session-id> --url https://example.com/
  palyra browser snapshot <session-id> --include-visible-text --output ./snapshot.json
  palyra browser screenshot <session-id> --output ./page.png
  palyra browser trace <session-id> --output ./trace.json

Discoverability:
  Session list/show/inspect talks directly to browserd. Mutating actions go through the control plane so policy and audit hooks stay intact.";

const SYSTEM_AFTER_HELP: &str = "\
Examples:
  palyra system heartbeat
  palyra system presence --json
  palyra system event --limit 50

Discoverability:
  `system` is the top-level operator view over runtime heartbeat, subsystem presence, and recent journal events.";

const SANDBOX_AFTER_HELP: &str = "\
Examples:
  palyra sandbox list
  palyra sandbox explain --runtime process-runner
  palyra sandbox explain --runtime wasm-runtime --json

Discoverability:
  `sandbox` reads the effective runtime policy snapshot from admin diagnostics. Use `palyra policy explain` for per-action Cedar decisions.";

const TUI_AFTER_HELP: &str = "\
Examples:
  palyra tui
  palyra tui --session-key ops:triage
  palyra tui --allow-sensitive-tools --include-archived-sessions

Keys:
  Tab switches focus, F2/F3/F4 open agent/session/model pickers, F5 opens settings, Ctrl+R reloads data.
  Enter sends input, `/` starts slash commands, `!` enters the local shell flow with explicit opt-in.";

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
    #[command(
        about = "Run a narrow liveness/readiness probe across HTTP and gRPC gateway surfaces",
        after_long_help = HEALTH_AFTER_HELP
    )]
    Health {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        grpc_url: Option<String>,
    },
    #[command(
        about = "Tail local gateway journal diagnostics",
        after_long_help = LOGS_AFTER_HELP
    )]
    Logs {
        #[arg(long)]
        db_path: Option<String>,
        #[arg(long, default_value_t = 50)]
        lines: usize,
        #[arg(long, default_value_t = false)]
        follow: bool,
        #[arg(long, default_value_t = 1000)]
        poll_interval_ms: u64,
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
    #[command(
        about = "Run the ACP stdio bridge and legacy compatibility shim",
        after_long_help = ACP_AFTER_HELP
    )]
    Acp {
        #[command(flatten)]
        command: AcpCommand,
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
    Message {
        #[command(subcommand)]
        command: MessageCommand,
    },
    Approvals {
        #[command(subcommand)]
        command: ApprovalsCommand,
    },
    Sessions {
        #[command(subcommand)]
        command: SessionsCommand,
    },
    #[command(
        about = "Launch the terminal operator client",
        after_long_help = TUI_AFTER_HELP
    )]
    Tui {
        #[command(flatten)]
        command: TuiCommand,
    },
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
    Channels {
        #[command(subcommand)]
        command: ChannelsCommand,
    },
    #[command(about = "Manage webhook-backed integrations", after_long_help = WEBHOOKS_AFTER_HELP)]
    Webhooks {
        #[command(subcommand)]
        command: WebhooksCommand,
    },
    #[command(
        about = "Discover committed docs and CLI help snapshots locally",
        after_long_help = DOCS_AFTER_HELP
    )]
    Docs {
        #[command(subcommand)]
        command: DocsCommand,
    },
    Plugins {
        #[command(subcommand)]
        command: PluginsCommand,
    },
    Hooks {
        #[command(subcommand)]
        command: HooksCommand,
    },
    Devices {
        #[command(subcommand)]
        command: DevicesCommand,
    },
    Node {
        #[command(subcommand)]
        command: NodeCommand,
    },
    Nodes {
        #[command(subcommand)]
        command: NodesCommand,
    },
    #[command(
        about = "Operate the browser service and browser-backed automation sessions",
        after_long_help = BROWSER_AFTER_HELP
    )]
    Browser {
        #[command(subcommand)]
        command: BrowserCommand,
    },
    #[command(
        about = "Inspect runtime heartbeat, subsystem presence, and recent system events",
        after_long_help = SYSTEM_AFTER_HELP
    )]
    System {
        #[command(subcommand)]
        command: SystemCommand,
    },
    #[command(
        about = "Inspect effective process-runner and WASM sandbox policy surfaces",
        after_long_help = SANDBOX_AFTER_HELP
    )]
    Sandbox {
        #[command(subcommand)]
        command: SandboxCommand,
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
    #[command(
        about = "Create or verify portable operator backups",
        after_long_help = BACKUP_AFTER_HELP
    )]
    Backup {
        #[command(subcommand)]
        command: BackupCommand,
    },
    #[command(
        about = "Reset selected local runtime scopes",
        after_long_help = RESET_AFTER_HELP
    )]
    Reset {
        #[command(flatten)]
        command: ResetCommand,
    },
    #[command(
        about = "Remove an installed Palyra package and optional state",
        after_long_help = UNINSTALL_AFTER_HELP
    )]
    Uninstall {
        #[command(flatten)]
        command: UninstallCommand,
    },
    #[command(
        about = "Check or apply a packaged Palyra update",
        after_long_help = UPDATE_AFTER_HELP
    )]
    Update {
        #[command(flatten)]
        command: UpdateCommand,
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
    Pairing {
        #[command(subcommand)]
        command: PairingCommand,
    },
}

#[cfg(test)]
mod tests;
