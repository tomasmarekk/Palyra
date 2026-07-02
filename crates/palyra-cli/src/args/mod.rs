//! Clap argument surface for the `palyra` operator CLI: the root parser, global
//! options, and the top-level `Command` tree that fans out into the per-family
//! modules in this directory.
//!
//! NOTE: clap derive renders `///` doc comments on parser structs, fields,
//! and variants as help text, and help output is pinned byte-for-byte by the
//! `help_snapshots` and `cli_parity` test suites. Never add, change, or remove
//! `///` docs on clap items anywhere under `args/`; use plain `//` comments
//! instead. Defaults, value parsers, possible values, and the `*_AFTER_HELP`
//! strings below are part of the same pinned contract.

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
mod commitments;
mod completion;
mod config;
mod configure;
mod cron;
mod daemon;
mod deployment;
mod devices;
mod docs;
mod eval;
mod extension;
mod flows;
mod hooks;
mod ids;
mod init;
mod jobs;
mod mcp;
mod memory;
mod message;
mod models;
mod node;
mod nodes;
mod objectives;
mod onboarding;
mod pairing;
mod patch;
mod plugins;
mod policy;
mod profile;
mod protocol;
mod qa;
mod reset;
mod routines;
mod run;
mod sandbox;
mod secrets;
mod security;
mod sessions;
mod skills;
mod state;
mod support_bundle;
mod system;
mod tasks;
mod tui;
mod uninstall;
mod update;
mod webhooks;
mod workers;

pub use acp::{
    AcpBridgeArgs, AcpCommand, AcpConnectionArgs, AcpSessionDefaultsArgs, AcpShimArgs,
    AcpSubcommand,
};
pub use agent::{AgentApprovalModeArg, AgentAutoResumeArg, AgentCommand};
pub use agents::AgentsCommand;
pub use approvals::{
    ApprovalDecisionArg, ApprovalDecisionScopeArg, ApprovalExportFormatArg,
    ApprovalResolveDecisionArg, ApprovalSubjectTypeArg, ApprovalsCommand,
};
pub use auth::{
    AuthAccessCommand, AuthAnthropicCommand, AuthCommand, AuthCredentialArg, AuthOpenAiCommand,
    AuthProfilesCommand, AuthProviderArg, AuthScopeArg, AuthXaiCommand, WorkspaceRoleArg,
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
pub use commitments::CommitmentsCommand;
pub use completion::CompletionShell;
pub use config::ConfigCommand;
pub use configure::ConfigureSectionArg;
pub use cron::{CronCommand, CronConcurrencyPolicyArg, CronMisfirePolicyArg, CronScheduleTypeArg};
pub use daemon::{DaemonCommand, JournalCheckpointModeArg};
pub use deployment::{DeploymentCommand, DeploymentProfileArg};
pub use devices::DevicesCommand;
pub use docs::DocsCommand;
pub use eval::{EvalBundleCommand, EvalCommand};
pub use extension::ExtensionCommand;
pub use flows::{FlowStateArg, FlowsCommand};
pub use hooks::HooksCommand;
pub use ids::RequiredCommandIdArg;
pub use init::{InitModeArg, InitTlsScaffoldArg};
pub use jobs::JobsCommand;
pub use mcp::{McpCommand, McpSubcommand};
pub use memory::{
    MemoryCommand, MemoryLearningCommand, MemoryScopeArg, MemorySourceArg, MemoryWorkspaceCommand,
};
pub use message::MessageCommand;
pub use models::ModelsCommand;
pub use node::NodeCommand;
pub use nodes::NodesCommand;
pub use objectives::{
    ObjectiveKindArg, ObjectivePriorityArg, ObjectiveScheduleTypeArg, ObjectiveStateArg,
    ObjectiveUpsertCommandArgs, ObjectivesCommand,
};
pub use onboarding::{
    GatewayBindProfileArg, OnboardingAuthMethodArg, OnboardingCommand, OnboardingFlowArg,
    RemoteVerificationModeArg, SetupWizardOverridesArg, WizardOverridesArg,
};
pub use pairing::{PairingClientKindArg, PairingCommand, PairingMethodArg, PairingStateArg};
pub use patch::{PatchBundleCommand, PatchCommand};
pub use plugins::PluginsCommand;
pub use policy::PolicyCommand;
pub use profile::{ProfileCommand, ProfileExportModeArg, ProfileModeArg, ProfileRiskLevelArg};
pub use protocol::ProtocolCommand;
pub use qa::QaCommand;
pub use reset::{ResetCommand, ResetScopeArg};
pub use routines::{
    RoutineApprovalModeArg, RoutineDeliveryModeArg, RoutineExecutionPostureArg,
    RoutinePreviewTimezoneArg, RoutineRunModeArg, RoutineSilentPolicyArg, RoutineTriggerKindArg,
    RoutineUpsertCommand, RoutinesCommand,
};
pub use run::{RunCommand, RunExportFormatArg};
pub use sandbox::{SandboxCommand, SandboxRuntimeArg};
pub use secrets::{SecretsCommand, SecretsConfigureCommand};
pub use security::SecurityCommand;
pub use sessions::SessionsCommand;
pub use skills::{SkillsCommand, SkillsPackageCommand, SkillsProcedureCommand};
pub use state::StateCommand;
pub use support_bundle::SupportBundleCommand;
pub use system::{SystemCommand, SystemEventCommand, SystemEventSeverityArg};
pub use tasks::{TasksCommand, WorkboardCommand};
pub use tui::TuiCommand;
pub use uninstall::UninstallCommand;
pub use update::UpdateCommand;
pub use webhooks::WebhooksCommand;
pub use workers::WorkersCommand;

const ROOT_AFTER_HELP: &str = "\
Examples:
  palyra setup --mode local
  palyra setup --deployment-profile single-vm --path ./config/palyra.toml
  palyra deployment profiles --json
  palyra acp --session-key ops:triage
  palyra docs search gateway
  palyra gateway status
  palyra dashboard --open
  palyra backup create --output ./artifacts/palyra-backup.zip
  palyra profile list --json
  palyra objectives list --kind heartbeat --json
  palyra system heartbeat
  palyra sandbox explain --runtime process-runner
  palyra update --check
  palyra --profile staging agents list --json
  palyra --profile prod --expect-profile prod gateway status
  palyra --profile prod --allow-strict-profile-actions reset --scope state --yes
  palyra --config ./palyra.toml --output-format json status --admin

Canonical command map:
  setup      Preferred bootstrap/init workflow (`init` remains as a compatibility alias)
  configure  Guided reconfiguration workflow for an existing installation
  acp        Preferred ACP stdio bridge entry point (`agent acp` remains compatible)
  docs       Local CLI help snapshot discovery surface
  gateway    Preferred runtime/admin family (`daemon` remains as a compatibility alias)
  dashboard  Thin operator shortcut for dashboard URL discovery/open workflows
  objectives Durable objective, heartbeat, standing-order, and program surface
  routines   Unified automation surface for schedules, hooks, webhooks, and system events
  backup     Portable lifecycle backup/create verification surface
  system     Runtime heartbeat, presence, and recent system-event observability
  sandbox    Effective isolation/runtime policy explain surface for process and WASM tooling
  plugins    Trusted WASM plugin binding and lifecycle surface
  hooks      Event-driven automation bindings over trusted plugins
  extension  Unified package preflight, grants, and lifecycle diagnostics
  reset      Destructive local recovery surface with explicit scope selection
  uninstall  Installer-aware package removal surface
  update     Package update/check orchestration surface
  onboarding Operator onboarding workflows (`onboard` stays as the shorthand alias)
  profile    First-class CLI profile lifecycle and environment selection
  qa         QA Lab scenario manifest validation
  webhooks   Webhook-backed integration management surface";

const SETUP_AFTER_HELP: &str = "\
Examples:
  palyra setup --mode local
  palyra setup --deployment-profile single-vm --path ./config/palyra.toml
  palyra setup --deployment-profile worker-enabled --wizard --non-interactive --accept-risk
  palyra setup --mode local --wizard
  palyra setup --mode remote --path ./config/palyra.toml --force

Discoverability:
  Use `palyra onboard wizard --flow quickstart` for the guided onboarding family.
  Use `palyra gateway status` after setup to verify runtime health.";

const ONBOARDING_AFTER_HELP: &str = "\
Examples:
  palyra onboard wizard
  palyra onboard wizard --flow manual
  palyra onboard wizard --flow remote --non-interactive --accept-risk --remote-base-url https://dashboard.example.com/

Discoverability:
  Use `palyra setup --wizard` for bootstrap-first routing, or `palyra onboarding wizard` if you want the explicit long-form family name.";

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

const QA_AFTER_HELP: &str = "\
Examples:
  palyra qa validate
  palyra qa validate --path qa/scenarios/text_run_basic.yaml
  palyra qa validate --path qa/scenarios --json

Discoverability:
  `qa validate` checks QA Lab scenario manifests before runner or replay tooling consumes them.";

const DEPLOYMENT_AFTER_HELP: &str = "\
Examples:
  palyra deployment profiles --json
  palyra deployment manifest --deployment-profile worker-enabled --output ./artifacts/worker-profile.json
  palyra deployment preflight --deployment-profile single-vm --path ./palyra.toml
  palyra deployment recipe --deployment-profile worker-enabled --output-dir ./artifacts/deploy
  palyra deployment upgrade-smoke --deployment-profile worker-enabled --path ./palyra.toml
  palyra deployment promotion-check --deployment-profile worker-enabled
  palyra deployment rollback-plan --deployment-profile worker-enabled --output ./artifacts/rollback.json";

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
  palyra completion powershell
  palyra completion --shell powershell
  palyra completion --shell bash > palyra.bash";

const WEBHOOKS_AFTER_HELP: &str = "\
Examples:
  palyra webhooks list
  palyra webhooks add github_repo_a github --secret-ref global/github_repo_a --allow-event push --allow-source github.repo_a
  palyra webhooks test github_repo_a --payload-stdin

Discoverability:
  `webhooks` manages secret-aware webhook integrations without exposing a public ingress surface by default.";

const ROUTINES_AFTER_HELP: &str = "\
Examples:
  palyra routines list
  palyra routines create-from-template --template-id heartbeat
  palyra routines upsert --name \"Daily report\" --prompt \"Summarize incidents\" --trigger-kind schedule --natural-language-schedule \"every weekday at 9\"
  palyra routines schedule-preview \"every weekday at 9\"

Discoverability:
  `routines` is the first-class automation surface. Use `cron` only for schedule-only compatibility flows.";

const CRON_AFTER_HELP: &str = "\
Examples:
  palyra cron list
  palyra cron add --name \"Health summary\" --prompt \"Summarize status\" --schedule-type cron --schedule \"*/5 * * * *\"
  palyra cron run-now --id 01ARZ3NDEKTSV4RRFFQ69G5FB0

Compatibility:
  `cron` remains available for schedule-only automation. Internally it is backed by unified routines; use `palyra routines` for delivery policies, quiet hours, approvals, templates, and event-driven triggers.";

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

const MCP_AFTER_HELP: &str = "\
Examples:
  palyra mcp serve --read-only
  palyra mcp serve --session-key ops:triage
  palyra mcp serve --allow-sensitive-tools

Discoverability:
  `mcp serve` exposes Palyra as a stdio MCP server facade over the same connection, access control, and approval model used by the rest of the CLI.
  It does not import external MCP servers or register external MCP client tools such as `ticket.read` into Palyra agent runs.";

const DOCS_AFTER_HELP: &str = "\
Examples:
  palyra docs list
  palyra docs search gateway
  palyra docs search browser
  palyra docs show help/docs-help

Discoverability:
  `docs` indexes committed CLI help snapshots in source checkouts and bundled help snapshots from portable installs for local, offline lookup.";

const BROWSER_AFTER_HELP: &str = "\
Examples:
  palyra browser status
  palyra browser start --wait-ms 15000
  palyra browser profiles list
  palyra browser session create --allow-domain docs.palyra.dev
  palyra browser navigate <session-id> --url https://example.com/
  palyra browser snapshot <session-id> --include-visible-text --output ./snapshot.json
  palyra browser screenshot <session-id> --output ./page.png
  palyra browser upload <session-id> --selector 'input[type=file]' --file ./input.csv
  palyra browser downloads <session-id> --output ./download.csv
  palyra browser trace <session-id> --output ./trace.json

Discoverability:
  Session list/show/inspect and local artifact transfers talk directly to browserd. Control-plane-backed actions keep policy and audit hooks intact.";

const SYSTEM_AFTER_HELP: &str = "\
Examples:
  palyra system heartbeat
  palyra system presence --json
  palyra system insights --json
  palyra system event list --limit 50

Discoverability:
  `system` is the top-level operator view over runtime heartbeat, subsystem presence, operator insights, and recent journal events.";

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
  Enter sends input, `/` starts slash commands, `!` enters the local shell flow with explicit opt-in.
  Ctrl+C exits immediately; `/exit` and `/quit` exit from the composer. `q` exits only when the composer is empty.";

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
    #[arg(long, global = true, help = "Use the named CLI profile from the local profile registry")]
    pub profile: Option<String>,
    #[arg(
        long = "expect-profile",
        global = true,
        help = "Require the resolved active profile name to match this value"
    )]
    pub expect_profile: Option<String>,
    #[arg(long = "config", global = true, help = "Use this palyra.toml config file")]
    pub config_path: Option<String>,
    #[arg(long, global = true, help = "Use this runtime state root directory")]
    pub state_root: Option<String>,
    #[arg(
        short = 'v',
        long,
        action = clap::ArgAction::Count,
        global = true,
        help = "Increase logging verbosity (-v => debug, -vv => trace)"
    )]
    pub verbose: u8,
    #[arg(
        long,
        value_enum,
        default_value_t = LogLevelArg::Info,
        global = true,
        help = "Set diagnostic log verbosity"
    )]
    pub log_level: LogLevelArg,
    #[arg(
        long = "output-format",
        value_enum,
        default_value_t = OutputFormatArg::Text,
        global = true,
        help = "Select text, JSON, or NDJSON output for automation"
    )]
    pub output_format: OutputFormatArg,
    #[arg(long, default_value_t = false, global = true, help = "Disable styled terminal output")]
    pub plain: bool,
    #[arg(long, default_value_t = false, global = true, help = "Disable ANSI color output")]
    pub no_color: bool,
    #[arg(
        long,
        default_value_t = false,
        global = true,
        help = "Allow commands to continue when the selected profile differs from context"
    )]
    pub allow_profile_mismatch: bool,
    #[arg(
        long,
        default_value_t = false,
        global = true,
        help = "Permit strict profile-scoped actions after explicit profile checks"
    )]
    pub allow_strict_profile_actions: bool,
}

#[derive(Debug, Parser)]
#[command(
    name = "palyra",
    version,
    about = "Palyra operator CLI",
    long_about = "Palyra operator CLI for secure local and remote runtime management.",
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
    #[command(about = "Run diagnostics, repair previews, and rollback workflows")]
    Doctor {
        #[arg(long, default_value_t = false)]
        strict: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(long, default_value_t = false)]
        repair: bool,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[arg(long = "only")]
        only: Vec<String>,
        #[arg(long = "skip")]
        skip: Vec<String>,
        #[arg(long)]
        rollback_run: Option<String>,
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
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(
        about = "Tail local gateway journal diagnostics",
        after_long_help = LOGS_AFTER_HELP
    )]
    Logs {
        #[arg(long, help = "Read journal entries from this SQLite database path")]
        db_path: Option<String>,
        #[arg(long, default_value_t = 50, help = "Number of recent log lines to print")]
        lines: usize,
        #[arg(long, default_value_t = false, help = "Keep polling and print new log entries")]
        follow: bool,
        #[arg(
            long,
            default_value_t = 1000,
            help = "Polling interval in milliseconds when following"
        )]
        poll_interval_ms: u64,
        #[arg(long, default_value_t = false, help = "Print log entries as JSON")]
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
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(
        about = "Run the ACP stdio bridge and legacy compatibility shim",
        after_long_help = ACP_AFTER_HELP
    )]
    Acp {
        #[command(flatten)]
        command: AcpCommand,
    },
    #[command(about = "Run the MCP stdio facade", after_long_help = MCP_AFTER_HELP)]
    Mcp {
        #[command(flatten)]
        command: McpCommand,
    },
    #[command(about = "Run one-off or interactive agent sessions")]
    Agent {
        #[command(subcommand)]
        command: AgentCommand,
    },
    #[command(about = "Manage daemon agent registry, bindings, and defaults")]
    Agents {
        #[command(subcommand)]
        command: AgentsCommand,
    },
    #[command(
        about = "Manage unified automation routines across schedules and event triggers",
        after_long_help = ROUTINES_AFTER_HELP
    )]
    Routines {
        #[command(subcommand)]
        command: RoutinesCommand,
    },
    #[command(about = "Manage durable objectives, heartbeats, standing orders, and programs")]
    Objectives {
        #[command(subcommand)]
        command: ObjectivesCommand,
    },
    #[command(about = "Inspect and control durable orchestration flows")]
    Flows {
        #[command(subcommand)]
        command: FlowsCommand,
    },
    #[command(about = "Inspect unified runtime tasks and WorkBoard items")]
    Tasks {
        #[command(subcommand)]
        command: TasksCommand,
    },
    #[command(about = "Inspect and control durable long-running tool jobs")]
    Jobs {
        #[command(subcommand)]
        command: JobsCommand,
    },
    #[command(about = "Extract, review, and schedule user commitments")]
    Commitments {
        #[command(subcommand)]
        command: CommitmentsCommand,
    },
    #[command(
        about = "Manage schedule-only cron workflows through the routines compatibility layer",
        after_long_help = CRON_AFTER_HELP
    )]
    Cron {
        #[command(subcommand)]
        command: CronCommand,
    },
    #[command(about = "Inspect and manage session, workspace, and learning memory")]
    Memory {
        #[command(subcommand)]
        command: MemoryCommand,
    },
    #[command(about = "Inspect and manage routed channel messages")]
    Message {
        #[command(subcommand)]
        command: MessageCommand,
    },
    #[command(about = "Review and resolve pending tool approvals")]
    Approvals {
        #[command(subcommand)]
        command: ApprovalsCommand,
    },
    #[command(about = "List, inspect, and manage chat sessions")]
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
    #[command(about = "Manage auth profiles, provider access, and OAuth/API-key flows")]
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
    #[command(about = "Manage connector channels and routing")]
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
        about = "Discover local CLI help snapshots from source checkouts or portable installs",
        after_long_help = DOCS_AFTER_HELP
    )]
    Docs {
        #[command(subcommand)]
        command: DocsCommand,
    },
    #[command(about = "Manage installed plugin packages and discovery")]
    Plugins {
        #[command(subcommand)]
        command: PluginsCommand,
    },
    #[command(about = "Manage local hooks and bindings")]
    Hooks {
        #[command(subcommand)]
        command: HooksCommand,
    },
    #[command(about = "Manage extension integration surfaces")]
    Extension {
        #[command(subcommand)]
        command: ExtensionCommand,
    },
    #[command(about = "Manage CLI connection profiles and active environment selection")]
    Profile {
        #[command(subcommand)]
        command: ProfileCommand,
    },
    #[command(about = "List and manage paired operator devices")]
    Devices {
        #[command(subcommand)]
        command: DevicesCommand,
    },
    #[command(about = "Manage the local node runtime")]
    Node {
        #[command(subcommand)]
        command: NodeCommand,
    },
    #[command(about = "Inspect and invoke remote node RPC surfaces")]
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
        about = "Inspect runtime heartbeat, subsystem presence, operator insights, and recent system events",
        after_long_help = SYSTEM_AFTER_HELP
    )]
    System {
        #[command(subcommand)]
        command: SystemCommand,
    },
    #[command(about = "Inspect and repair local durable state")]
    State {
        #[command(subcommand)]
        command: StateCommand,
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
        #[arg(
            value_enum,
            value_name = "SHELL",
            required_unless_present = "shell_flag",
            conflicts_with = "shell_flag"
        )]
        shell: Option<CompletionShell>,
        #[arg(long = "shell", value_enum, value_name = "SHELL", conflicts_with = "shell")]
        shell_flag: Option<CompletionShell>,
    },
    #[command(
        visible_alias = "onboard",
        about = "Run guided onboarding workflows",
        after_long_help = ONBOARDING_AFTER_HELP
    )]
    Onboarding {
        #[command(subcommand)]
        command: OnboardingCommand,
    },
    #[command(
        about = "Inspect deployment profiles, run preflights, and generate deploy recipes",
        after_long_help = DEPLOYMENT_AFTER_HELP
    )]
    Deployment {
        #[command(subcommand)]
        command: DeploymentCommand,
    },
    #[command(
        about = "Safely reconfigure an existing installation",
        after_long_help = CONFIGURE_AFTER_HELP
    )]
    Configure {
        #[arg(long, help = "Edit this palyra.toml path")]
        path: Option<String>,
        #[arg(
            long = "section",
            value_enum,
            help = "Limit reconfiguration to this section; repeat for multiple sections"
        )]
        sections: Vec<ConfigureSectionArg>,
        #[arg(long = "deployment-profile", value_enum, help = "Set the deployment profile")]
        deployment_profile: Option<DeploymentProfileArg>,
        #[arg(long, default_value_t = false, help = "Run without interactive prompts")]
        non_interactive: bool,
        #[arg(
            long,
            default_value_t = false,
            help = "Accept risk gates required by the selected changes"
        )]
        accept_risk: bool,
        #[arg(long, default_value_t = false, help = "Print reconfiguration results as JSON")]
        json: bool,
        #[arg(long, help = "Set the workspace root")]
        workspace_root: Option<String>,
        #[arg(long, value_enum, help = "Choose how model-provider credentials are configured")]
        auth_method: Option<OnboardingAuthMethodArg>,
        #[arg(long, help = "Read the model-provider API key from this environment variable name")]
        api_key_env: Option<String>,
        #[arg(
            long,
            default_value_t = false,
            help = "Read one model-provider API key from stdin; requires --non-interactive for scripted configure runs"
        )]
        api_key_stdin: bool,
        #[arg(
            long,
            default_value_t = false,
            help = "Prompt securely for the model-provider API key"
        )]
        api_key_prompt: bool,
        #[arg(long, value_enum, help = "Select loopback-only or public TLS gateway binding")]
        bind_profile: Option<GatewayBindProfileArg>,
        #[arg(long, help = "Set the daemon HTTP port")]
        daemon_port: Option<u16>,
        #[arg(long, help = "Set the daemon gRPC port")]
        grpc_port: Option<u16>,
        #[arg(long, help = "Set the daemon QUIC port")]
        quic_port: Option<u16>,
        #[arg(long, value_enum, help = "Choose TLS scaffold handling for gateway setup")]
        tls_scaffold: Option<InitTlsScaffoldArg>,
        #[arg(long, help = "Use this TLS certificate path for public gateway binding")]
        tls_cert_path: Option<String>,
        #[arg(long, help = "Use this TLS private-key path for public gateway binding")]
        tls_key_path: Option<String>,
        #[arg(long, help = "Record this remote dashboard or gateway base URL")]
        remote_base_url: Option<String>,
        #[arg(long, help = "Read the admin token from this environment variable name")]
        admin_token_env: Option<String>,
        #[arg(
            long,
            default_value_t = false,
            help = "Read one admin token from stdin; requires --non-interactive for scripted configure runs"
        )]
        admin_token_stdin: bool,
        #[arg(long, default_value_t = false, help = "Prompt securely for the admin token")]
        admin_token_prompt: bool,
        #[arg(long, value_enum, help = "Choose how remote gateway identity is verified")]
        remote_verification: Option<RemoteVerificationModeArg>,
        #[arg(long, help = "Pin the remote server certificate SHA-256 digest")]
        pinned_server_cert_sha256: Option<String>,
        #[arg(long, help = "Pin the gateway CA SHA-256 digest")]
        pinned_gateway_ca_sha256: Option<String>,
        #[arg(long, help = "Record the SSH target used for remote administration")]
        ssh_target: Option<String>,
        #[arg(long, default_value_t = false, help = "Skip post-reconfiguration health checks")]
        skip_health: bool,
        #[arg(long, default_value_t = false, help = "Skip channel reconfiguration")]
        skip_channels: bool,
        #[arg(long, default_value_t = false, help = "Skip skill reconfiguration")]
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
    // Hidden compatibility spelling of `dashboard`; both variants must keep
    // identical fields so either form resolves the same way.
    #[command(name = "dashboard-url", hide = true)]
    DashboardUrl {
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
    #[command(about = "Export, import, and replay support bundles")]
    SupportBundle {
        #[command(subcommand)]
        command: SupportBundleCommand,
    },
    #[command(about = "Explain policy decisions and authorization posture")]
    Policy {
        #[command(subcommand)]
        command: PolicyCommand,
    },
    #[command(about = "Validate schema and protocol contract artifacts")]
    Protocol {
        #[command(subcommand)]
        command: ProtocolCommand,
    },
    #[command(about = "Create local eval bundles and replay metadata")]
    Eval {
        #[command(subcommand)]
        command: EvalCommand,
    },
    #[command(
        about = "Validate QA Lab scenario manifests",
        after_long_help = QA_AFTER_HELP
    )]
    Qa {
        #[command(subcommand)]
        command: QaCommand,
    },
    #[command(about = "Inspect and validate local configuration")]
    Config {
        #[command(subcommand)]
        command: Option<ConfigCommand>,
    },
    #[command(about = "Inspect networked workers, leases, and cleanup evidence")]
    Workers {
        #[command(subcommand)]
        command: WorkersCommand,
    },
    #[command(about = "Export run trajectories for audit and evals")]
    Run {
        #[command(subcommand)]
        command: RunCommand,
    },
    #[command(about = "Inspect and configure model providers and defaults")]
    Models {
        #[command(subcommand)]
        command: ModelsCommand,
    },
    #[command(about = "Apply workspace patches through the guarded patch engine")]
    Patch {
        #[command(subcommand)]
        command: PatchCommand,
    },
    #[command(
        visible_alias = "skill",
        about = "Manage skill packages, trust, and lifecycle gates"
    )]
    Skills {
        #[command(subcommand)]
        command: SkillsCommand,
    },
    #[command(about = "Manage local secrets and vault-backed credentials")]
    Secrets {
        #[command(subcommand)]
        command: SecretsCommand,
    },
    #[command(about = "Run security audits and posture checks")]
    Security {
        #[command(subcommand)]
        command: SecurityCommand,
    },
    #[command(about = "Open an SSH tunnel for remote daemon access")]
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
    #[command(about = "Manage device pairing and trust bootstrap")]
    Pairing {
        #[command(subcommand)]
        command: PairingCommand,
    },
}

#[cfg(test)]
mod tests;
