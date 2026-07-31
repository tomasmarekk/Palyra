//! Arguments for `palyra mcp`: external MCP registry management plus the
//! stdio MCP server facade. `serve` exposes Palyra to external MCP clients;
//! registry subcommands only edit local config; live import is unavailable.

use clap::{Args, Subcommand, ValueEnum};

use super::{AcpConnectionArgs, AcpSessionDefaultsArgs};

const MCP_SERVE_AFTER_HELP: &str = "\
Scope:
  `palyra mcp serve` exposes Palyra as a stdio MCP server for MCP clients.
  It does not import external MCP servers or register external MCP client tools such as `ticket.read` into Palyra agent runs.";

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum McpSubcommand {
    #[command(about = "Run the stdio MCP server", after_long_help = MCP_SERVE_AFTER_HELP)]
    Serve {
        #[command(flatten)]
        connection: AcpConnectionArgs,
        #[command(flatten)]
        session_defaults: AcpSessionDefaultsArgs,
        #[arg(long, default_value_t = false)]
        read_only: bool,
        #[arg(long, default_value_t = false)]
        allow_sensitive_tools: bool,
    },
    #[command(about = "Show external MCP runtime supervisor status")]
    Status(McpStatusArgs),
    #[command(about = "Run external MCP runtime doctor checks")]
    Doctor(McpDoctorArgs),
    #[command(about = "Show external MCP runtime probe status")]
    Probe(McpProbeArgs),
    #[command(about = "Show external MCP tool catalog availability")]
    Tools(McpToolsArgs),
    #[command(about = "Reload external MCP runtime configuration")]
    Reload(McpReloadArgs),
    #[command(about = "List configured external MCP servers")]
    List {
        #[arg(long, help = "Read this palyra.toml path")]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Show one configured external MCP server")]
    Show {
        id: String,
        #[arg(long, help = "Read this palyra.toml path")]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Store an OAuth grant for one external MCP server")]
    Login(McpLoginArgs),
    #[command(about = "Revoke an OAuth grant for one external MCP server")]
    Logout(McpLogoutArgs),
    #[command(about = "Add one external MCP server to local config")]
    Add(McpRegistryMutateArgs),
    #[command(about = "Update one external MCP server in local config")]
    Set(McpRegistryMutateArgs),
    #[command(about = "Enable one external MCP server")]
    Enable(McpRegistryToggleArgs),
    #[command(about = "Disable one external MCP server")]
    Disable(McpRegistryToggleArgs),
    #[command(about = "Remove one external MCP server")]
    Remove(McpRegistryToggleArgs),
}

#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct McpCommand {
    #[command(subcommand)]
    pub subcommand: McpSubcommand,
}

#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct McpRuntimeConnectionArgs {
    #[arg(long, help = "Admin HTTP base URL")]
    pub url: Option<String>,
    #[arg(long, help = "Admin token override")]
    pub token: Option<String>,
    #[arg(long, help = "Console principal for the diagnostics session")]
    pub principal: Option<String>,
    #[arg(long, help = "Console device id for the diagnostics session")]
    pub device_id: Option<String>,
    #[arg(long, help = "Console channel for the diagnostics session")]
    pub channel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct McpStatusArgs {
    #[command(flatten)]
    pub connection: McpRuntimeConnectionArgs,
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct McpDoctorArgs {
    #[arg(help = "Limit doctor output to one external MCP server")]
    pub id: Option<String>,
    #[command(flatten)]
    pub connection: McpRuntimeConnectionArgs,
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct McpProbeArgs {
    #[arg(help = "Limit probe output to one external MCP server")]
    pub id: Option<String>,
    #[command(flatten)]
    pub connection: McpRuntimeConnectionArgs,
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct McpToolsArgs {
    #[arg(help = "Limit tool availability output to one external MCP server")]
    pub id: Option<String>,
    #[command(flatten)]
    pub connection: McpRuntimeConnectionArgs,
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct McpReloadArgs {
    #[arg(long, help = "Reload this palyra.toml path; defaults to the active daemon config")]
    pub path: Option<String>,
    #[command(flatten)]
    pub connection: McpRuntimeConnectionArgs,
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    #[arg(long, default_value_t = false)]
    pub force: bool,
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct McpLoginArgs {
    pub id: String,
    #[arg(long, help = "Edit this palyra.toml path")]
    pub path: Option<String>,
    #[arg(long, default_value_t = false, help = "Read OAuth token JSON from stdin")]
    pub token_json_stdin: bool,
    #[arg(long = "scope", help = "Repeatable OAuth scope override")]
    pub scopes: Vec<String>,
    #[arg(long, help = "OAuth access token expiry as unix milliseconds")]
    pub expires_at_unix_ms: Option<i64>,
    #[arg(long, help = "Opaque provider rotation metadata")]
    pub rotation_id: Option<String>,
    #[arg(long, help = "Existing OAuth auth profile used for automatic access-token refresh")]
    pub auth_profile_id: Option<String>,
    #[arg(long, default_value_t = 5, help = "Number of backup files to retain")]
    pub backups: usize,
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct McpLogoutArgs {
    pub id: String,
    #[arg(long, help = "Edit this palyra.toml path")]
    pub path: Option<String>,
    #[arg(long, default_value_t = 5, help = "Number of backup files to retain")]
    pub backups: usize,
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct McpRegistryMutateArgs {
    pub id: String,
    #[arg(long, help = "Edit this palyra.toml path")]
    pub path: Option<String>,
    #[arg(long, value_enum)]
    pub transport: McpTransportArg,
    #[arg(long, help = "External MCP namespace; defaults to the server id")]
    pub namespace: Option<String>,
    #[arg(long, help = "Stdio command path when transport=stdio")]
    pub command: Option<String>,
    #[arg(long = "arg", help = "Repeatable stdio command argument")]
    pub args: Vec<String>,
    #[arg(long, help = "HTTP or SSE URL when transport=http|sse")]
    pub url: Option<String>,
    #[arg(
        long = "env-vault-ref",
        help = "Repeatable vault-backed env binding in NAME=scope/key form"
    )]
    pub env_vault_refs: Vec<String>,
    #[arg(long, value_enum, default_value_t = McpTrustLevelArg::External)]
    pub trust_level: McpTrustLevelArg,
    #[arg(long, value_enum, default_value_t = McpApprovalProfileArg::RequireApproval)]
    pub approval_profile: McpApprovalProfileArg,
    #[arg(long, value_enum, default_value_t = McpEgressPolicyArg::DenyAll)]
    pub egress_policy: McpEgressPolicyArg,
    #[arg(long = "egress-host", help = "Repeatable egress allowlist host")]
    pub egress_allowlist: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub oauth_required: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Allow host-owned MCP elicitation callbacks for this server"
    )]
    pub elicitation_enabled: bool,
    #[arg(long, value_enum, default_value_t = McpSamplingModeArg::Deny)]
    pub sampling_mode: McpSamplingModeArg,
    #[arg(long = "sampling-model-capability", help = "Repeatable sampling model capability")]
    pub sampling_model_capabilities: Vec<String>,
    #[arg(long, help = "Host model used for MCP sampling callbacks")]
    pub sampling_host_model_id: Option<String>,
    #[arg(long, default_value_t = 0, help = "Maximum output tokens per sampling callback")]
    pub sampling_max_output_tokens_per_request: u64,
    #[arg(long, default_value_t = 0, help = "Sampling budget window in seconds")]
    pub sampling_window_seconds: u64,
    #[arg(long, default_value_t = 0, help = "Maximum sampling callbacks per budget window")]
    pub sampling_max_requests_per_window: u64,
    #[arg(long, default_value_t = 0, help = "Maximum sampling output tokens per budget window")]
    pub sampling_max_output_tokens_per_window: u64,
    #[arg(long = "tool-allow", help = "Repeatable raw MCP tool allowlist entry")]
    pub tool_allowlist: Vec<String>,
    #[arg(long = "tool-deny", help = "Repeatable raw MCP tool denylist entry")]
    pub tool_denylist: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub enabled: bool,
    #[arg(long, default_value_t = 5, help = "Number of backup files to retain")]
    pub backups: usize,
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct McpRegistryToggleArgs {
    pub id: String,
    #[arg(long, help = "Edit this palyra.toml path")]
    pub path: Option<String>,
    #[arg(long, default_value_t = 5, help = "Number of backup files to retain")]
    pub backups: usize,
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum McpTransportArg {
    Stdio,
    Http,
    Sse,
}

impl McpTransportArg {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Stdio => "stdio",
            Self::Http => "http",
            Self::Sse => "sse",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum McpTrustLevelArg {
    Local,
    Workspace,
    External,
}

impl McpTrustLevelArg {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Workspace => "workspace",
            Self::External => "external",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum McpApprovalProfileArg {
    Safe,
    RequireApproval,
}

impl McpApprovalProfileArg {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Safe => "safe",
            Self::RequireApproval => "require_approval",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum McpEgressPolicyArg {
    DenyAll,
    Allowlist,
}

impl McpEgressPolicyArg {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::DenyAll => "deny_all",
            Self::Allowlist => "allowlist",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum McpSamplingModeArg {
    Deny,
    Allowlist,
}

impl McpSamplingModeArg {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Deny => "deny",
            Self::Allowlist => "allowlist",
        }
    }
}
