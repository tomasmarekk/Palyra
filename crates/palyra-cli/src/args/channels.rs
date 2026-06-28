//! Arguments for `palyra channels`: connector lifecycle and queue control plus
//! the Discord and router subfamilies. Connector credentials are read from
//! stdin or an interactive prompt; passing `--credential` on argv additionally
//! requires the explicit insecure-arg acknowledgement flag. Help text is pinned
//! by snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::{ArgGroup, Subcommand, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ChannelProviderArg {
    Discord,
    Slack,
    Telegram,
    Webhook,
    Echo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ChannelResolveEntityArg {
    Channel,
    Conversation,
    Thread,
    User,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ChannelsDiscordCommand {
    Setup {
        #[arg(long, default_value = "default")]
        account_id: String,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        verify_channel_id: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Status {
        #[arg(long, default_value = "default")]
        account_id: String,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    HealthRefresh {
        #[arg(long, default_value = "default")]
        account_id: String,
        #[arg(long)]
        verify_channel_id: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(visible_alias = "test-send")]
    Verify {
        #[arg(long, default_value = "default")]
        account_id: String,
        #[arg(long)]
        to: String,
        #[arg(long, default_value = "palyra discord test message")]
        text: String,
        #[arg(long, default_value_t = false)]
        confirm: bool,
        #[arg(long)]
        auto_reaction: Option<String>,
        #[arg(long)]
        thread_id: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ChannelsRouterCommand {
    Rules {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Warnings {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Preview {
        #[arg(long = "route-channel")]
        route_channel: String,
        #[arg(long)]
        text: String,
        #[arg(long)]
        conversation_id: Option<String>,
        #[arg(long)]
        sender_identity: Option<String>,
        #[arg(long)]
        sender_display: Option<String>,
        #[arg(long, default_value_t = true)]
        sender_verified: bool,
        #[arg(long, default_value_t = true)]
        is_direct_message: bool,
        #[arg(long, default_value_t = false)]
        requested_broadcast: bool,
        #[arg(long)]
        adapter_message_id: Option<String>,
        #[arg(long)]
        adapter_thread_id: Option<String>,
        #[arg(long)]
        max_payload_bytes: Option<u64>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Pairings {
        #[arg(long = "route-channel")]
        route_channel: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    MintPairingCode {
        #[arg(long = "route-channel")]
        route_channel: String,
        #[arg(long)]
        issued_by: Option<String>,
        #[arg(long)]
        ttl_ms: Option<u64>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ChannelsCommand {
    Add {
        #[arg(long, value_enum)]
        provider: ChannelProviderArg,
        #[arg(long, default_value = "default")]
        account_id: String,
        #[arg(long, default_value_t = false)]
        interactive: bool,
        #[arg(
            long,
            conflicts_with_all = ["credential_stdin", "credential_prompt"],
            requires = "allow_insecure_credential_arg",
            help = "Read the connector credential from argv after acknowledging process-list exposure"
        )]
        credential: Option<String>,
        #[arg(
            long,
            default_value_t = false,
            conflicts_with_all = ["credential", "credential_prompt"]
        )]
        credential_stdin: bool,
        #[arg(
            long,
            default_value_t = false,
            conflicts_with_all = ["credential", "credential_stdin"]
        )]
        credential_prompt: bool,
        #[arg(
            long,
            default_value_t = false,
            help = "Allow --credential despite process-list exposure; prefer --credential-stdin or --credential-prompt"
        )]
        allow_insecure_credential_arg: bool,
        #[arg(long, default_value = "local")]
        mode: String,
        #[arg(long, default_value = "dm_only")]
        inbound_scope: String,
        #[arg(long = "allow-from")]
        allow_from: Vec<String>,
        #[arg(long = "deny-from")]
        deny_from: Vec<String>,
        #[arg(long)]
        require_mention: Option<bool>,
        #[arg(long = "mention-pattern")]
        mention_patterns: Vec<String>,
        #[arg(long)]
        concurrency_limit: Option<u64>,
        #[arg(long)]
        direct_message_policy: Option<String>,
        #[arg(long)]
        broadcast_strategy: Option<String>,
        #[arg(long, default_value_t = false)]
        confirm_open_guild_channels: bool,
        #[arg(long)]
        verify_channel_id: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Login {
        #[arg(long, value_enum)]
        provider: ChannelProviderArg,
        #[arg(long, default_value = "default")]
        account_id: String,
        #[arg(
            long,
            conflicts_with_all = ["credential_stdin", "credential_prompt"],
            requires = "allow_insecure_credential_arg",
            help = "Read the connector credential from argv after acknowledging process-list exposure"
        )]
        credential: Option<String>,
        #[arg(
            long,
            default_value_t = false,
            conflicts_with_all = ["credential", "credential_prompt"]
        )]
        credential_stdin: bool,
        #[arg(
            long,
            default_value_t = false,
            conflicts_with_all = ["credential", "credential_stdin"]
        )]
        credential_prompt: bool,
        #[arg(
            long,
            default_value_t = false,
            help = "Allow --credential despite process-list exposure; prefer --credential-stdin or --credential-prompt"
        )]
        allow_insecure_credential_arg: bool,
        #[arg(long, default_value = "local")]
        mode: String,
        #[arg(long, default_value = "dm_only")]
        inbound_scope: String,
        #[arg(long = "allow-from")]
        allow_from: Vec<String>,
        #[arg(long = "deny-from")]
        deny_from: Vec<String>,
        #[arg(long)]
        require_mention: Option<bool>,
        #[arg(long = "mention-pattern")]
        mention_patterns: Vec<String>,
        #[arg(long)]
        concurrency_limit: Option<u64>,
        #[arg(long)]
        direct_message_policy: Option<String>,
        #[arg(long)]
        broadcast_strategy: Option<String>,
        #[arg(long, default_value_t = false)]
        confirm_open_guild_channels: bool,
        #[arg(long)]
        verify_channel_id: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Logout {
        #[arg(long, value_enum)]
        provider: ChannelProviderArg,
        #[arg(long, default_value = "default")]
        account_id: String,
        #[arg(long, default_value_t = false)]
        keep_credential: bool,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Remove {
        #[arg(long, value_enum)]
        provider: ChannelProviderArg,
        #[arg(long, default_value = "default")]
        account_id: String,
        #[arg(long, default_value_t = false)]
        keep_credential: bool,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Capabilities {
        connector_id: Option<String>,
        #[arg(long, value_enum)]
        provider: Option<ChannelProviderArg>,
        #[arg(long)]
        account_id: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Resolve {
        #[arg(long, value_enum)]
        provider: ChannelProviderArg,
        #[arg(long, default_value = "default")]
        account_id: String,
        #[arg(long, value_enum)]
        entity: ChannelResolveEntityArg,
        #[arg(long)]
        value: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Pairings {
        connector_id: Option<String>,
        #[arg(long, value_enum)]
        provider: Option<ChannelProviderArg>,
        #[arg(long)]
        account_id: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    PairingCode {
        connector_id: Option<String>,
        #[arg(long, value_enum)]
        provider: Option<ChannelProviderArg>,
        #[arg(long)]
        account_id: Option<String>,
        #[arg(long)]
        issued_by: Option<String>,
        #[arg(long)]
        ttl_ms: Option<u64>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Qr {
        connector_id: Option<String>,
        #[arg(long, value_enum)]
        provider: Option<ChannelProviderArg>,
        #[arg(long)]
        account_id: Option<String>,
        #[arg(long)]
        issued_by: Option<String>,
        #[arg(long)]
        ttl_ms: Option<u64>,
        #[arg(long)]
        artifact: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Discord {
        #[command(subcommand)]
        command: ChannelsDiscordCommand,
    },
    Router {
        #[command(subcommand)]
        command: ChannelsRouterCommand,
    },
    List {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Status {
        connector_id: Option<String>,
        #[arg(long, value_enum)]
        provider: Option<ChannelProviderArg>,
        #[arg(long)]
        account_id: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(group(ArgGroup::new("selector").required(true).args(["connector_id", "provider"])))]
    HealthRefresh {
        connector_id: Option<String>,
        #[arg(long, value_enum)]
        provider: Option<ChannelProviderArg>,
        #[arg(long)]
        account_id: Option<String>,
        #[arg(long)]
        verify_channel_id: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Enable {
        connector_id: String,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    QueuePause {
        connector_id: String,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    QueueResume {
        connector_id: String,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    QueueDrain {
        connector_id: String,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    DeadLetterReplay {
        connector_id: String,
        dead_letter_id: i64,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    DeadLetterDiscard {
        connector_id: String,
        dead_letter_id: i64,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Disable {
        connector_id: String,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Logs {
        connector_id: Option<String>,
        #[arg(long, value_enum)]
        provider: Option<ChannelProviderArg>,
        #[arg(long)]
        account_id: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    IngressList {
        connector_id: String,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    IngressShow {
        connector_id: String,
        ingress_event_id: i64,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    DeliveryList {
        connector_id: String,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    DeliveryShow {
        intent_id: String,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    DeliveryRetry {
        intent_id: String,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Test {
        connector_id: String,
        #[arg(long)]
        text: String,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        conversation_id: Option<String>,
        #[arg(long)]
        sender_id: Option<String>,
        #[arg(long)]
        sender_display: Option<String>,
        #[arg(long, default_value_t = false)]
        simulate_crash_once: bool,
        #[arg(long, default_value_t = true)]
        is_direct_message: bool,
        #[arg(long, default_value_t = false)]
        requested_broadcast: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
