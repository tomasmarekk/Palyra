//! Arguments for `palyra onboarding` (alias `onboard`) wizard runs and status.
//!
//! `WizardOverridesArg` and `SetupWizardOverridesArg` differ deliberately:
//! `setup` exposes `--tls-scaffold` as a top-level flag, so its wizard override
//! set omits that field to avoid a duplicate argument. Help text is pinned by
//! snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::{Args, Subcommand, ValueEnum};

use super::{DeploymentProfileArg, InitTlsScaffoldArg};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OnboardingFlowArg {
    Quickstart,
    Manual,
    Remote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OnboardingAuthMethodArg {
    ApiKey,
    AnthropicApiKey,
    MinimaxApiKey,
    Skip,
    ExistingConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum GatewayBindProfileArg {
    LoopbackOnly,
    PublicTls,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RemoteVerificationModeArg {
    None,
    ServerCert,
    GatewayCa,
}

#[derive(Debug, Clone, Args, PartialEq, Eq)]
pub struct WizardOverridesArg {
    #[arg(long, value_enum)]
    pub flow: Option<OnboardingFlowArg>,
    #[arg(long, default_value_t = false)]
    pub non_interactive: bool,
    #[arg(long, default_value_t = false)]
    pub accept_risk: bool,
    #[arg(long, default_value_t = false)]
    pub json: bool,
    #[arg(long)]
    pub workspace_root: Option<String>,
    #[arg(long, value_enum)]
    pub auth_method: Option<OnboardingAuthMethodArg>,
    #[arg(long)]
    pub api_key_env: Option<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Read one model-provider API key from stdin; requires --non-interactive for scripted wizard runs"
    )]
    pub api_key_stdin: bool,
    #[arg(long, default_value_t = false)]
    pub api_key_prompt: bool,
    #[arg(long, value_enum)]
    pub deployment_profile: Option<DeploymentProfileArg>,
    #[arg(long, value_enum)]
    pub bind_profile: Option<GatewayBindProfileArg>,
    #[arg(long)]
    pub daemon_port: Option<u16>,
    #[arg(long)]
    pub grpc_port: Option<u16>,
    #[arg(long)]
    pub quic_port: Option<u16>,
    #[arg(long, value_enum)]
    pub tls_scaffold: Option<InitTlsScaffoldArg>,
    #[arg(long)]
    pub tls_cert_path: Option<String>,
    #[arg(long)]
    pub tls_key_path: Option<String>,
    #[arg(long)]
    pub remote_base_url: Option<String>,
    #[arg(long)]
    pub admin_token_env: Option<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Read one admin token from stdin; requires --non-interactive for scripted wizard runs"
    )]
    pub admin_token_stdin: bool,
    #[arg(long, default_value_t = false)]
    pub admin_token_prompt: bool,
    #[arg(long, value_enum)]
    pub remote_verification: Option<RemoteVerificationModeArg>,
    #[arg(long)]
    pub pinned_server_cert_sha256: Option<String>,
    #[arg(long)]
    pub pinned_gateway_ca_sha256: Option<String>,
    #[arg(long)]
    pub ssh_target: Option<String>,
    #[arg(long, default_value_t = false)]
    pub skip_health: bool,
    #[arg(long, default_value_t = false)]
    pub skip_channels: bool,
    #[arg(long, default_value_t = false)]
    pub skip_skills: bool,
}

#[derive(Debug, Clone, Args, PartialEq, Eq)]
pub struct SetupWizardOverridesArg {
    #[arg(long, value_enum)]
    pub flow: Option<OnboardingFlowArg>,
    #[arg(long, default_value_t = false)]
    pub non_interactive: bool,
    #[arg(long, default_value_t = false)]
    pub accept_risk: bool,
    #[arg(long, default_value_t = false)]
    pub json: bool,
    #[arg(long)]
    pub workspace_root: Option<String>,
    #[arg(long, value_enum)]
    pub auth_method: Option<OnboardingAuthMethodArg>,
    #[arg(long)]
    pub api_key_env: Option<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Read one model-provider API key from stdin; requires --non-interactive for scripted wizard runs"
    )]
    pub api_key_stdin: bool,
    #[arg(long, default_value_t = false)]
    pub api_key_prompt: bool,
    #[arg(long, value_enum)]
    pub deployment_profile: Option<DeploymentProfileArg>,
    #[arg(long, value_enum)]
    pub bind_profile: Option<GatewayBindProfileArg>,
    #[arg(long)]
    pub daemon_port: Option<u16>,
    #[arg(long)]
    pub grpc_port: Option<u16>,
    #[arg(long)]
    pub quic_port: Option<u16>,
    #[arg(long)]
    pub tls_cert_path: Option<String>,
    #[arg(long)]
    pub tls_key_path: Option<String>,
    #[arg(long)]
    pub remote_base_url: Option<String>,
    #[arg(long)]
    pub admin_token_env: Option<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Read one admin token from stdin; requires --non-interactive for scripted wizard runs"
    )]
    pub admin_token_stdin: bool,
    #[arg(long, default_value_t = false)]
    pub admin_token_prompt: bool,
    #[arg(long, value_enum)]
    pub remote_verification: Option<RemoteVerificationModeArg>,
    #[arg(long)]
    pub pinned_server_cert_sha256: Option<String>,
    #[arg(long)]
    pub pinned_gateway_ca_sha256: Option<String>,
    #[arg(long)]
    pub ssh_target: Option<String>,
    #[arg(long, default_value_t = false)]
    pub skip_health: bool,
    #[arg(long, default_value_t = false)]
    pub skip_channels: bool,
    #[arg(long, default_value_t = false)]
    pub skip_skills: bool,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum OnboardingCommand {
    Wizard {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[command(flatten)]
        options: Box<WizardOverridesArg>,
    },
    Status {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, value_enum)]
        flow: Option<OnboardingFlowArg>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
