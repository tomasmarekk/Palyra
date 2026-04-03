use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::Instant,
};

use palyra_common::redaction::{redact_url, REDACTED};
use palyra_control_plane as control_plane;
use palyra_identity::IdentityManager;
use palyra_vault::Vault;
use reqwest::Url;
use serde::Serialize;
use tokio::sync::{mpsc, Notify};

use crate::gateway::proto::palyra::common::v1 as common_v1;
use crate::{
    channels,
    cron::CronTimezoneMode,
    gateway::{self, GatewayAuthConfig, GatewayRuntimeState},
    node_runtime::NodeRuntimeState,
    observability::ObservabilityState,
    openai_auth::OpenAiOAuthAttemptStateRecord,
    routines, webhooks,
};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) started_at: Instant,
    pub(crate) runtime: Arc<GatewayRuntimeState>,
    pub(crate) node_runtime: Arc<NodeRuntimeState>,
    pub(crate) identity_manager: Arc<Mutex<IdentityManager>>,
    pub(crate) channels: Arc<channels::ChannelPlatform>,
    pub(crate) webhooks: Arc<webhooks::WebhookRegistry>,
    pub(crate) routines: Arc<routines::RoutineRegistry>,
    pub(crate) vault: Arc<Vault>,
    pub(crate) tool_allowed_tools: Vec<String>,
    pub(crate) browser_service_config: gateway::BrowserServiceRuntimeConfig,
    pub(crate) auth_runtime: Arc<gateway::AuthRuntimeState>,
    pub(crate) auth: GatewayAuthConfig,
    pub(crate) admin_rate_limit: Arc<Mutex<HashMap<IpAddr, AdminRateLimitEntry>>>,
    pub(crate) canvas_rate_limit: Arc<Mutex<HashMap<IpAddr, CanvasRateLimitEntry>>>,
    pub(crate) cron_timezone_mode: CronTimezoneMode,
    pub(crate) grpc_url: String,
    pub(crate) scheduler_wake: Arc<Notify>,
    pub(crate) console_sessions: Arc<Mutex<HashMap<String, ConsoleSession>>>,
    pub(crate) console_browser_handoffs: Arc<Mutex<HashMap<String, ConsoleBrowserHandoff>>>,
    pub(crate) openai_oauth_attempts: Arc<Mutex<HashMap<String, OpenAiOAuthAttempt>>>,
    pub(crate) relay_tokens: Arc<Mutex<HashMap<String, ConsoleRelayToken>>>,
    pub(crate) console_chat_streams: Arc<Mutex<HashMap<String, ConsoleChatRunStream>>>,
    pub(crate) support_bundle_jobs: Arc<Mutex<HashMap<String, control_plane::SupportBundleJob>>>,
    pub(crate) observability: Arc<ObservabilityState>,
    pub(crate) deployment: DeploymentRuntimeSnapshot,
    pub(crate) remote_admin_access: Arc<Mutex<Option<RemoteAdminAccessAttempt>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct DeploymentRuntimeSnapshot {
    pub(crate) mode: String,
    pub(crate) bind_profile: String,
    pub(crate) admin_bind_addr: String,
    pub(crate) admin_port: u16,
    pub(crate) grpc_bind_addr: String,
    pub(crate) grpc_port: u16,
    pub(crate) quic_bind_addr: String,
    pub(crate) quic_port: u16,
    pub(crate) quic_enabled: bool,
    pub(crate) gateway_tls_enabled: bool,
    pub(crate) admin_auth_required: bool,
    pub(crate) dangerous_remote_bind_ack_config: bool,
    pub(crate) dangerous_remote_bind_ack_env: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct ConsoleActionContext {
    pub(crate) principal: String,
    pub(crate) device_id: String,
    pub(crate) channel: Option<String>,
}

#[derive(Clone)]
pub(crate) struct OpenAiOAuthAttempt {
    pub(crate) attempt_id: String,
    pub(crate) expires_at_unix_ms: i64,
    pub(crate) redirect_uri: String,
    pub(crate) profile_id: String,
    pub(crate) profile_name: String,
    pub(crate) scope: control_plane::AuthProfileScope,
    pub(crate) client_id: String,
    pub(crate) client_secret: String,
    pub(crate) scopes: Vec<String>,
    pub(crate) token_endpoint: Url,
    pub(crate) code_verifier: String,
    pub(crate) set_default: bool,
    pub(crate) context: ConsoleActionContext,
    pub(crate) state: OpenAiOAuthAttemptStateRecord,
}

impl std::fmt::Debug for OpenAiOAuthAttempt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAiOAuthAttempt")
            .field("attempt_id", &self.attempt_id)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .field("redirect_uri", &redact_url(self.redirect_uri.as_str()))
            .field("profile_id", &self.profile_id)
            .field("profile_name", &self.profile_name)
            .field("scope", &self.scope)
            .field("client_id", &self.client_id)
            .field("client_secret", &REDACTED)
            .field("scopes", &self.scopes)
            .field("token_endpoint", &redact_url(self.token_endpoint.as_str()))
            .field("code_verifier", &REDACTED)
            .field("set_default", &self.set_default)
            .field("context", &self.context)
            .field("state", &self.state)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RemoteAdminAccessAttempt {
    pub(crate) observed_at_unix_ms: i64,
    pub(crate) remote_ip_fingerprint: String,
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) status_code: u16,
    pub(crate) outcome: String,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AdminRateLimitEntry {
    pub(crate) window_started_at: Instant,
    pub(crate) requests_in_window: u32,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CanvasRateLimitEntry {
    pub(crate) window_started_at: Instant,
    pub(crate) requests_in_window: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct ConsoleSession {
    pub(crate) session_token_hash_sha256: String,
    pub(crate) csrf_token: String,
    pub(crate) context: gateway::RequestContext,
    pub(crate) issued_at_unix_ms: i64,
    pub(crate) expires_at_unix_ms: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct ConsoleBrowserHandoff {
    pub(crate) token_hash_sha256: String,
    pub(crate) context: gateway::RequestContext,
    pub(crate) redirect_path: String,
    pub(crate) expires_at_unix_ms: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct ConsoleRelayToken {
    pub(crate) token_hash_sha256: String,
    pub(crate) principal: String,
    pub(crate) device_id: String,
    pub(crate) channel: Option<String>,
    pub(crate) session_id: String,
    pub(crate) extension_id: String,
    pub(crate) issued_at_unix_ms: i64,
    pub(crate) expires_at_unix_ms: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct ConsoleChatRunStream {
    pub(crate) session_id: String,
    pub(crate) request_sender: mpsc::Sender<common_v1::RunStreamRequest>,
    pub(crate) pending_approvals: Arc<Mutex<HashMap<String, String>>>,
}
