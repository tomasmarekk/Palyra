use clap::{Subcommand, ValueEnum};

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum AuthCommand {
    Profiles {
        #[command(subcommand)]
        command: AuthProfilesCommand,
    },
    Access {
        #[command(subcommand)]
        command: AuthAccessCommand,
    },
    Openai {
        #[command(subcommand)]
        command: AuthOpenAiCommand,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum AuthProfilesCommand {
    List {
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, value_enum)]
        provider: Option<AuthProviderArg>,
        #[arg(long)]
        provider_name: Option<String>,
        #[arg(long, value_enum)]
        scope: Option<AuthScopeArg>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Show {
        profile_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Set {
        profile_id: String,
        #[arg(long, value_enum)]
        provider: AuthProviderArg,
        #[arg(long)]
        provider_name: Option<String>,
        #[arg(long)]
        profile_name: String,
        #[arg(long, value_enum, default_value_t = AuthScopeArg::Global)]
        scope: AuthScopeArg,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long, value_enum)]
        credential: AuthCredentialArg,
        #[arg(long)]
        api_key_ref: Option<String>,
        #[arg(long)]
        access_token_ref: Option<String>,
        #[arg(long)]
        refresh_token_ref: Option<String>,
        #[arg(long)]
        token_endpoint: Option<String>,
        #[arg(long)]
        client_id: Option<String>,
        #[arg(long)]
        client_secret_ref: Option<String>,
        #[arg(long = "scope-value")]
        scope_value: Vec<String>,
        #[arg(long)]
        expires_at_unix_ms: Option<i64>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Delete {
        profile_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Health {
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long, default_value_t = false)]
        include_profiles: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Doctor {
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Audit {
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long, value_enum)]
        provider: Option<AuthProviderArg>,
        #[arg(long)]
        provider_name: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    CooldownClear {
        profile_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    OrderSet {
        #[arg(long, value_enum)]
        provider: Option<AuthProviderArg>,
        #[arg(long)]
        provider_name: Option<String>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(required = true)]
        profile_id: Vec<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    ExplainSelection {
        #[arg(long, value_enum)]
        provider: Option<AuthProviderArg>,
        #[arg(long)]
        provider_name: Option<String>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long = "profile-id")]
        profile_id: Vec<String>,
        #[arg(long = "credential", value_enum)]
        credential: Vec<AuthCredentialArg>,
        #[arg(long = "policy-denied-profile-id")]
        policy_denied_profile_id: Vec<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum AuthOpenAiCommand {
    Status {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    ApiKey {
        #[arg(long)]
        profile_id: Option<String>,
        #[arg(long, default_value = "OpenAI")]
        profile_name: String,
        #[arg(long, value_enum, default_value_t = AuthScopeArg::Global)]
        scope: AuthScopeArg,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long)]
        api_key_env: Option<String>,
        #[arg(long, default_value_t = false)]
        api_key_stdin: bool,
        #[arg(long, default_value_t = false)]
        api_key_prompt: bool,
        #[arg(long, default_value_t = false)]
        set_default: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    OauthStart {
        #[arg(long)]
        profile_id: Option<String>,
        #[arg(long)]
        profile_name: Option<String>,
        #[arg(long, value_enum, default_value_t = AuthScopeArg::Global)]
        scope: AuthScopeArg,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long)]
        client_id: String,
        #[arg(long)]
        client_secret_env: Option<String>,
        #[arg(long, default_value_t = false)]
        client_secret_stdin: bool,
        #[arg(long, default_value_t = false)]
        client_secret_prompt: bool,
        #[arg(long = "scope-value")]
        scope_value: Vec<String>,
        #[arg(long, default_value_t = false)]
        set_default: bool,
        #[arg(long, default_value_t = false)]
        open: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    OauthState {
        attempt_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Refresh {
        profile_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Reconnect {
        profile_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Revoke {
        profile_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    UseProfile {
        profile_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum AuthAccessCommand {
    Status {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Backfill {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Feature {
        feature_key: String,
        #[arg(action = clap::ArgAction::Set, value_parser = clap::value_parser!(bool))]
        enabled: bool,
        #[arg(long)]
        stage: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    TokenList {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    TokenCreate {
        #[arg(long)]
        label: String,
        #[arg(long)]
        principal: String,
        #[arg(long)]
        workspace_id: Option<String>,
        #[arg(long, value_enum, default_value_t = WorkspaceRoleArg::Operator)]
        role: WorkspaceRoleArg,
        #[arg(long = "scope")]
        scope: Vec<String>,
        #[arg(long)]
        expires_at_unix_ms: Option<i64>,
        #[arg(long)]
        rate_limit_per_minute: Option<u32>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    TokenRotate {
        token_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    TokenRevoke {
        token_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    WorkspaceCreate {
        #[arg(long)]
        team_name: String,
        #[arg(long)]
        workspace_name: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    InviteCreate {
        #[arg(long)]
        workspace_id: String,
        #[arg(long)]
        invited_identity: String,
        #[arg(long, value_enum, default_value_t = WorkspaceRoleArg::Operator)]
        role: WorkspaceRoleArg,
        #[arg(long)]
        expires_at_unix_ms: i64,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    InviteAccept {
        invitation_token: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    MembershipRole {
        #[arg(long)]
        workspace_id: String,
        #[arg(long)]
        member_principal: String,
        #[arg(long, value_enum)]
        role: WorkspaceRoleArg,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    MembershipRemove {
        #[arg(long)]
        workspace_id: String,
        #[arg(long)]
        member_principal: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    ShareUpsert {
        #[arg(long)]
        workspace_id: String,
        #[arg(long)]
        resource_kind: String,
        #[arg(long)]
        resource_id: String,
        #[arg(long)]
        access_level: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum AuthProviderArg {
    Openai,
    Anthropic,
    Telegram,
    Slack,
    Discord,
    Webhook,
    Custom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum AuthScopeArg {
    Global,
    Agent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum AuthCredentialArg {
    ApiKey,
    Oauth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum WorkspaceRoleArg {
    Owner,
    Admin,
    Operator,
}
