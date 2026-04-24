use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::Instant,
};

use tokio::sync::Notify;

use super::state::{
    AdminRateLimitEntry, AppState, CanvasRateLimitEntry, CompatApiRateLimitEntry,
    ConfiguredSecretsState, ConsoleBrowserHandoff, ConsoleChatRunStream, ConsoleRelayToken,
    ConsoleSession, DeploymentRuntimeSnapshot, ReloadOperationsState, RemoteAdminAccessAttempt,
};
use crate::{
    access_control::AccessRegistry,
    channels,
    config::{BrowserServiceConfig, LoadedConfig},
    gateway::{self, GatewayAuthConfig, GatewayRuntimeState},
    node_runtime::NodeRuntimeState,
    objectives,
    realtime::{RealtimeEventRouter, RealtimeRateLimiter},
    routines, webhooks,
};
use palyra_identity::IdentityManager;
use palyra_vault::Vault;

pub(crate) struct AppStateBuildContext {
    pub(crate) runtime: Arc<GatewayRuntimeState>,
    pub(crate) node_runtime: Arc<NodeRuntimeState>,
    pub(crate) identity_manager: Arc<Mutex<IdentityManager>>,
    pub(crate) channels: Arc<channels::ChannelPlatform>,
    pub(crate) webhooks: Arc<webhooks::WebhookRegistry>,
    pub(crate) routines: Arc<routines::RoutineRegistry>,
    pub(crate) objectives: Arc<objectives::ObjectiveRegistry>,
    pub(crate) vault: Arc<Vault>,
    pub(crate) auth_runtime: Arc<gateway::AuthRuntimeState>,
    pub(crate) auth: GatewayAuthConfig,
    pub(crate) grpc_url: String,
    pub(crate) scheduler_wake: Arc<Notify>,
    pub(crate) access_registry: Arc<Mutex<AccessRegistry>>,
}

pub(crate) fn build_app_state(
    loaded: &LoadedConfig,
    dangerous_remote_bind_ack_env: bool,
    configured_secrets: ConfiguredSecretsState,
    context: AppStateBuildContext,
) -> AppState {
    let observability = Arc::clone(&context.runtime.observability);
    AppState {
        started_at: Instant::now(),
        loaded_config: Arc::new(Mutex::new(loaded.clone())),
        runtime: context.runtime,
        node_runtime: context.node_runtime,
        identity_manager: context.identity_manager,
        channels: context.channels,
        webhooks: context.webhooks,
        routines: context.routines,
        objectives: context.objectives,
        vault: context.vault,
        tool_allowed_tools: loaded.tool_call.allowed_tools.clone(),
        browser_service_config: build_browser_service_runtime_config(
            &loaded.tool_call.browser_service,
        ),
        auth_runtime: context.auth_runtime,
        auth: context.auth,
        admin_rate_limit: Arc::new(Mutex::new(HashMap::<IpAddr, AdminRateLimitEntry>::new())),
        canvas_rate_limit: Arc::new(Mutex::new(HashMap::<IpAddr, CanvasRateLimitEntry>::new())),
        compat_api_rate_limit: Arc::new(Mutex::new(
            HashMap::<String, CompatApiRateLimitEntry>::new(),
        )),
        cron_timezone_mode: loaded.cron.timezone,
        grpc_url: context.grpc_url,
        scheduler_wake: context.scheduler_wake,
        console_sessions: Arc::new(Mutex::new(HashMap::<String, ConsoleSession>::new())),
        console_browser_handoffs: Arc::new(Mutex::new(
            HashMap::<String, ConsoleBrowserHandoff>::new(),
        )),
        openai_oauth_attempts: Arc::new(Mutex::new(HashMap::new())),
        relay_tokens: Arc::new(Mutex::new(HashMap::<String, ConsoleRelayToken>::new())),
        console_chat_streams: Arc::new(Mutex::new(HashMap::<String, ConsoleChatRunStream>::new())),
        support_bundle_jobs: Arc::new(Mutex::new(HashMap::new())),
        doctor_jobs: Arc::new(Mutex::new(HashMap::new())),
        observability,
        configured_secrets: Arc::new(Mutex::new(configured_secrets)),
        reload_state: Arc::new(Mutex::new(ReloadOperationsState::default())),
        realtime_events: Arc::new(Mutex::new(RealtimeEventRouter::default())),
        realtime_rate_limit: Arc::new(Mutex::new(RealtimeRateLimiter::default())),
        deployment: build_deployment_runtime_snapshot(loaded, dangerous_remote_bind_ack_env),
        remote_admin_access: Arc::new(Mutex::new(None::<RemoteAdminAccessAttempt>)),
        access_registry: context.access_registry,
    }
}

pub(crate) fn build_browser_service_runtime_config(
    config: &BrowserServiceConfig,
) -> gateway::BrowserServiceRuntimeConfig {
    gateway::BrowserServiceRuntimeConfig {
        enabled: config.enabled,
        endpoint: config.endpoint.clone(),
        auth_token: config.auth_token.clone(),
        connect_timeout_ms: config.connect_timeout_ms,
        request_timeout_ms: config.request_timeout_ms,
        max_screenshot_bytes: usize::try_from(config.max_screenshot_bytes).unwrap_or(usize::MAX),
        max_title_bytes: usize::try_from(config.max_title_bytes).unwrap_or(usize::MAX),
    }
}

pub(crate) fn build_deployment_runtime_snapshot(
    loaded: &LoadedConfig,
    dangerous_remote_bind_ack_env: bool,
) -> DeploymentRuntimeSnapshot {
    DeploymentRuntimeSnapshot {
        profile: loaded.deployment.profile.clone(),
        mode: loaded.deployment.mode.as_str().to_owned(),
        bind_profile: loaded.gateway.bind_profile.as_str().to_owned(),
        admin_bind_addr: loaded.daemon.bind_addr.clone(),
        admin_port: loaded.daemon.port,
        grpc_bind_addr: loaded.gateway.grpc_bind_addr.clone(),
        grpc_port: loaded.gateway.grpc_port,
        quic_bind_addr: loaded.gateway.quic_bind_addr.clone(),
        quic_port: loaded.gateway.quic_port,
        quic_enabled: loaded.gateway.quic_enabled,
        gateway_tls_enabled: loaded.gateway.tls.enabled,
        admin_auth_required: loaded.admin.require_auth,
        dangerous_remote_bind_ack_config: loaded.deployment.dangerous_remote_bind_ack,
        dangerous_remote_bind_ack_env,
    }
}

pub(crate) fn loopback_grpc_url(socket: SocketAddr, tls_enabled: bool) -> String {
    let normalized = match socket {
        SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), v4.port())
        }
        SocketAddr::V6(v6) if v6.ip().is_unspecified() => {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), v6.port())
        }
        other => other,
    };
    let scheme = if tls_enabled { "https" } else { "http" };
    format!("{scheme}://{normalized}")
}
