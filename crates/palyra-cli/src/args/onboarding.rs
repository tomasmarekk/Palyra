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
    #[arg(long, value_enum, help = "Select quickstart, manual, or remote onboarding")]
    pub flow: Option<OnboardingFlowArg>,
    #[arg(long, default_value_t = false, help = "Run without interactive prompts")]
    pub non_interactive: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Accept the risk gates required by remote or public exposure"
    )]
    pub accept_risk: bool,
    #[arg(long, default_value_t = false, help = "Print the onboarding result as JSON")]
    pub json: bool,
    #[arg(long, help = "Use this workspace root in generated configuration")]
    pub workspace_root: Option<String>,
    #[arg(long, value_enum, help = "Choose how model-provider credentials are configured")]
    pub auth_method: Option<OnboardingAuthMethodArg>,
    #[arg(long, help = "Read the model-provider API key from this environment variable name")]
    pub api_key_env: Option<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Read one model-provider API key from stdin; requires --non-interactive for scripted wizard runs"
    )]
    pub api_key_stdin: bool,
    #[arg(long, default_value_t = false, help = "Prompt securely for the model-provider API key")]
    pub api_key_prompt: bool,
    #[arg(long, value_enum, help = "Select the generated deployment profile")]
    pub deployment_profile: Option<DeploymentProfileArg>,
    #[arg(long, value_enum, help = "Select loopback-only or public TLS gateway binding")]
    pub bind_profile: Option<GatewayBindProfileArg>,
    #[arg(long, help = "Set the daemon HTTP port")]
    pub daemon_port: Option<u16>,
    #[arg(long, help = "Set the daemon gRPC port")]
    pub grpc_port: Option<u16>,
    #[arg(long, help = "Set the daemon QUIC port")]
    pub quic_port: Option<u16>,
    #[arg(long, value_enum, help = "Choose TLS scaffold handling for gateway setup")]
    pub tls_scaffold: Option<InitTlsScaffoldArg>,
    #[arg(long, help = "Use this TLS certificate path for public gateway binding")]
    pub tls_cert_path: Option<String>,
    #[arg(long, help = "Use this TLS private-key path for public gateway binding")]
    pub tls_key_path: Option<String>,
    #[arg(long, help = "Record this remote dashboard or gateway base URL")]
    pub remote_base_url: Option<String>,
    #[arg(long, help = "Read the admin token from this environment variable name")]
    pub admin_token_env: Option<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Read one admin token from stdin; requires --non-interactive for scripted wizard runs"
    )]
    pub admin_token_stdin: bool,
    #[arg(long, default_value_t = false, help = "Prompt securely for the admin token")]
    pub admin_token_prompt: bool,
    #[arg(long, value_enum, help = "Choose how remote gateway identity is verified")]
    pub remote_verification: Option<RemoteVerificationModeArg>,
    #[arg(long, help = "Pin the remote server certificate SHA-256 digest")]
    pub pinned_server_cert_sha256: Option<String>,
    #[arg(long, help = "Pin the gateway CA SHA-256 digest")]
    pub pinned_gateway_ca_sha256: Option<String>,
    #[arg(long, help = "Record the SSH target used for remote administration")]
    pub ssh_target: Option<String>,
    #[arg(long, default_value_t = false, help = "Skip post-onboarding health checks")]
    pub skip_health: bool,
    #[arg(long, default_value_t = false, help = "Skip default channel setup")]
    pub skip_channels: bool,
    #[arg(long, default_value_t = false, help = "Skip default skill bootstrap")]
    pub skip_skills: bool,
}

#[derive(Debug, Clone, Args, PartialEq, Eq)]
pub struct SetupWizardOverridesArg {
    #[arg(long, value_enum, help = "Select quickstart, manual, or remote onboarding")]
    pub flow: Option<OnboardingFlowArg>,
    #[arg(long, default_value_t = false, help = "Run without interactive prompts")]
    pub non_interactive: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Accept the risk gates required by remote or public exposure"
    )]
    pub accept_risk: bool,
    #[arg(long, default_value_t = false, help = "Print the onboarding result as JSON")]
    pub json: bool,
    #[arg(long, help = "Use this workspace root in generated configuration")]
    pub workspace_root: Option<String>,
    #[arg(long, value_enum, help = "Choose how model-provider credentials are configured")]
    pub auth_method: Option<OnboardingAuthMethodArg>,
    #[arg(long, help = "Read the model-provider API key from this environment variable name")]
    pub api_key_env: Option<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Read one model-provider API key from stdin; requires --non-interactive for scripted wizard runs"
    )]
    pub api_key_stdin: bool,
    #[arg(long, default_value_t = false, help = "Prompt securely for the model-provider API key")]
    pub api_key_prompt: bool,
    #[arg(long, value_enum, help = "Select the generated deployment profile")]
    pub deployment_profile: Option<DeploymentProfileArg>,
    #[arg(long, value_enum, help = "Select loopback-only or public TLS gateway binding")]
    pub bind_profile: Option<GatewayBindProfileArg>,
    #[arg(long, help = "Set the daemon HTTP port")]
    pub daemon_port: Option<u16>,
    #[arg(long, help = "Set the daemon gRPC port")]
    pub grpc_port: Option<u16>,
    #[arg(long, help = "Set the daemon QUIC port")]
    pub quic_port: Option<u16>,
    #[arg(long, help = "Use this TLS certificate path for public gateway binding")]
    pub tls_cert_path: Option<String>,
    #[arg(long, help = "Use this TLS private-key path for public gateway binding")]
    pub tls_key_path: Option<String>,
    #[arg(long, help = "Record this remote dashboard or gateway base URL")]
    pub remote_base_url: Option<String>,
    #[arg(long, help = "Read the admin token from this environment variable name")]
    pub admin_token_env: Option<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Read one admin token from stdin; requires --non-interactive for scripted wizard runs"
    )]
    pub admin_token_stdin: bool,
    #[arg(long, default_value_t = false, help = "Prompt securely for the admin token")]
    pub admin_token_prompt: bool,
    #[arg(long, value_enum, help = "Choose how remote gateway identity is verified")]
    pub remote_verification: Option<RemoteVerificationModeArg>,
    #[arg(long, help = "Pin the remote server certificate SHA-256 digest")]
    pub pinned_server_cert_sha256: Option<String>,
    #[arg(long, help = "Pin the gateway CA SHA-256 digest")]
    pub pinned_gateway_ca_sha256: Option<String>,
    #[arg(long, help = "Record the SSH target used for remote administration")]
    pub ssh_target: Option<String>,
    #[arg(long, default_value_t = false, help = "Skip post-onboarding health checks")]
    pub skip_health: bool,
    #[arg(long, default_value_t = false, help = "Skip default channel setup")]
    pub skip_channels: bool,
    #[arg(long, default_value_t = false, help = "Skip default skill bootstrap")]
    pub skip_skills: bool,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum OnboardingCommand {
    #[command(about = "Run the guided onboarding wizard")]
    Wizard {
        #[arg(long, help = "Write or read this palyra.toml path")]
        path: Option<String>,
        #[arg(
            long,
            default_value_t = false,
            help = "Overwrite existing onboarding output where allowed"
        )]
        force: bool,
        #[command(flatten)]
        options: Box<WizardOverridesArg>,
    },
    #[command(about = "Inspect onboarding state and detected setup posture")]
    Status {
        #[arg(long, help = "Read this palyra.toml path")]
        path: Option<String>,
        #[arg(long, value_enum, help = "Evaluate status for this onboarding flow")]
        flow: Option<OnboardingFlowArg>,
        #[arg(long, default_value_t = false, help = "Print onboarding status as JSON")]
        json: bool,
    },
}
