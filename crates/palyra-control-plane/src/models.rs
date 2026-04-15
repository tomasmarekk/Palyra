use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::contract::{ContractDescriptor, PageInfo};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleProfileContext {
    pub name: String,
    pub label: String,
    pub environment: String,
    pub color: String,
    pub risk_level: String,
    pub strict_mode: bool,
    pub mode: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleSession {
    pub principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<ConsoleProfileContext>,
    pub csrf_token: String,
    pub issued_at_unix_ms: i64,
    pub expires_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentBindAddresses {
    pub admin: String,
    pub grpc: String,
    pub quic: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentTlsSummary {
    pub gateway_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DangerousRemoteBindAckSummary {
    pub config: bool,
    pub env: bool,
    pub env_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingFlow {
    QuickStart,
    AdvancedSetup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingPostureState {
    NotStarted,
    InProgress,
    Blocked,
    Ready,
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingStepStatus {
    Todo,
    InProgress,
    Blocked,
    Done,
    Skipped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingActionKind {
    OpenConsolePath,
    RunCliCommand,
    OpenDesktopSection,
    ReadDocs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingStepAction {
    pub label: String,
    pub kind: OnboardingActionKind,
    pub surface: String,
    pub target: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingBlockedReason {
    pub code: String,
    pub detail: String,
    pub repair_hint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingStepView {
    pub step_id: String,
    pub title: String,
    pub summary: String,
    pub status: OnboardingStepStatus,
    #[serde(default)]
    pub optional: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked: Option<OnboardingBlockedReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<OnboardingStepAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OnboardingStepCounts {
    pub todo: usize,
    pub in_progress: usize,
    pub blocked: usize,
    pub done: usize,
    pub skipped: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingPostureEnvelope {
    pub contract: ContractDescriptor,
    pub flow: OnboardingFlow,
    pub flow_variant: String,
    pub status: OnboardingPostureState,
    pub config_path: String,
    pub resume_supported: bool,
    pub ready_for_first_success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recommended_step_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_success_hint: Option<String>,
    pub counts: OnboardingStepCounts,
    #[serde(default)]
    pub available_flows: Vec<OnboardingFlow>,
    #[serde(default)]
    pub steps: Vec<OnboardingStepView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteAdminAccessAttempt {
    pub observed_at_unix_ms: i64,
    pub remote_ip_fingerprint: String,
    pub method: String,
    pub path: String,
    pub status_code: u16,
    pub outcome: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentPostureSummary {
    pub contract: ContractDescriptor,
    pub mode: String,
    pub bind_profile: String,
    pub bind_addresses: DeploymentBindAddresses,
    pub tls: DeploymentTlsSummary,
    pub admin_auth_required: bool,
    pub dangerous_remote_bind_ack: DangerousRemoteBindAckSummary,
    pub remote_bind_detected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_remote_admin_access_attempt: Option<RemoteAdminAccessAttempt>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDecisionRequest {
    pub approved: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_scope_ttl_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalDecisionEnvelope {
    pub approval: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dm_pairing: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretMetadata {
    pub scope: String,
    pub key: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub value_bytes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretMetadataList {
    pub contract: ContractDescriptor,
    pub scope: String,
    #[serde(default)]
    pub secrets: Vec<SecretMetadata>,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretMetadataEnvelope {
    pub contract: ContractDescriptor,
    pub secret: SecretMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretRevealEnvelope {
    pub contract: ContractDescriptor,
    pub scope: String,
    pub key: String,
    pub value_bytes: u32,
    pub value_base64: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_utf8: Option<String>,
}

impl SecretRevealEnvelope {
    #[must_use]
    pub fn decode_value(&self) -> Option<Vec<u8>> {
        BASE64_STANDARD.decode(self.value_base64.as_bytes()).ok()
    }
}

fn decode_optional_base64(raw: Option<&str>) -> Option<Vec<u8>> {
    raw.and_then(|value| {
        if value.is_empty() {
            None
        } else {
            BASE64_STANDARD.decode(value.as_bytes()).ok()
        }
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigBackupRecord {
    pub index: usize,
    pub path: String,
    pub exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigDocumentSnapshot {
    pub contract: ContractDescriptor,
    pub source_path: String,
    pub config_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migrated_from_version: Option<u32>,
    pub redacted: bool,
    pub document_toml: String,
    #[serde(default)]
    pub backups: Vec<ConfigBackupRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigValidationEnvelope {
    pub contract: ContractDescriptor,
    pub source_path: String,
    pub valid: bool,
    pub config_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migrated_from_version: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigMutationEnvelope {
    pub contract: ContractDescriptor,
    pub operation: String,
    pub source_path: String,
    pub backups_retained: usize,
    pub config_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migrated_from_version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changed_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionBackendInventoryRecord {
    pub backend_id: String,
    pub label: String,
    pub state: String,
    pub selectable: bool,
    pub selected_by_default: bool,
    pub description: String,
    pub operator_summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollout_flag: Option<String>,
    pub rollout_enabled: bool,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub tradeoffs: Vec<String>,
    pub active_node_count: usize,
    pub total_node_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRecord {
    pub agent_id: String,
    pub display_name: String,
    pub agent_dir: String,
    #[serde(default)]
    pub workspace_roots: Vec<String>,
    pub default_model_profile: String,
    #[serde(default)]
    pub execution_backend_preference: String,
    #[serde(default)]
    pub default_tool_allowlist: Vec<String>,
    #[serde(default)]
    pub default_skill_allowlist: Vec<String>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentListEnvelope {
    pub contract: ContractDescriptor,
    #[serde(default)]
    pub agents: Vec<AgentRecord>,
    #[serde(default)]
    pub execution_backends: Vec<ExecutionBackendInventoryRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_agent_id: Option<String>,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEnvelope {
    pub contract: ContractDescriptor,
    pub agent: AgentRecord,
    pub is_default: bool,
    #[serde(default)]
    pub execution_backends: Vec<ExecutionBackendInventoryRecord>,
    pub resolved_execution_backend: String,
    pub execution_backend_fallback_used: bool,
    pub execution_backend_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCreateRequest {
    pub agent_id: String,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_dir: Option<String>,
    #[serde(default)]
    pub workspace_roots: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_model_profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_backend_preference: Option<String>,
    #[serde(default)]
    pub default_tool_allowlist: Vec<String>,
    #[serde(default)]
    pub default_skill_allowlist: Vec<String>,
    #[serde(default)]
    pub set_default: bool,
    #[serde(default)]
    pub allow_absolute_paths: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCreateEnvelope {
    pub contract: ContractDescriptor,
    pub agent: AgentRecord,
    pub default_changed: bool,
    #[serde(default)]
    pub execution_backends: Vec<ExecutionBackendInventoryRecord>,
    pub resolved_execution_backend: String,
    pub execution_backend_fallback_used: bool,
    pub execution_backend_reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_agent_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSetDefaultEnvelope {
    pub contract: ContractDescriptor,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_default_agent_id: Option<String>,
    pub default_agent_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthProfileProvider {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthProfileScope {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthCredentialView {
    ApiKey {
        api_key_vault_ref: String,
    },
    Oauth {
        access_token_vault_ref: String,
        refresh_token_vault_ref: String,
        token_endpoint: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        client_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        client_secret_vault_ref: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        scopes: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        expires_at_unix_ms: Option<i64>,
        #[serde(default)]
        refresh_state: Value,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthProfileView {
    pub profile_id: String,
    pub provider: AuthProfileProvider,
    pub profile_name: String,
    pub scope: AuthProfileScope,
    pub credential: AuthCredentialView,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthProfileListEnvelope {
    pub contract: ContractDescriptor,
    #[serde(default)]
    pub profiles: Vec<AuthProfileView>,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthProfileEnvelope {
    pub contract: ContractDescriptor,
    pub profile: AuthProfileView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthProfileDeleteEnvelope {
    pub contract: ContractDescriptor,
    pub profile_id: String,
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthHealthEnvelope {
    pub contract: ContractDescriptor,
    pub summary: Value,
    pub expiry_distribution: Value,
    #[serde(default)]
    pub profiles: Vec<Value>,
    pub refresh_metrics: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderAuthStateEnvelope {
    pub contract: ContractDescriptor,
    pub provider: String,
    pub oauth_supported: bool,
    pub bootstrap_supported: bool,
    pub callback_supported: bool,
    pub reconnect_supported: bool,
    pub revoke_supported: bool,
    pub default_selection_supported: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_profile_id: Option<String>,
    #[serde(default)]
    pub available_profile_ids: Vec<String>,
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderAuthActionEnvelope {
    pub contract: ContractDescriptor,
    pub provider: String,
    pub action: String,
    pub state: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairingCodeRecord {
    pub code: String,
    pub channel: String,
    pub issued_by: String,
    pub created_at_unix_ms: i64,
    pub expires_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairingPendingRecord {
    pub channel: String,
    pub sender_identity: String,
    pub code: String,
    pub requested_at_unix_ms: i64,
    pub expires_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairingGrantRecord {
    pub channel: String,
    pub sender_identity: String,
    pub approved_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairingChannelSnapshot {
    pub channel: String,
    #[serde(default)]
    pub pending: Vec<PairingPendingRecord>,
    #[serde(default)]
    pub paired: Vec<PairingGrantRecord>,
    #[serde(default)]
    pub active_codes: Vec<PairingCodeRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairingSummaryEnvelope {
    pub contract: ContractDescriptor,
    #[serde(default)]
    pub channels: Vec<PairingChannelSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCatalogFamilyRelative {
    pub session_id: String,
    pub title: String,
    pub branch_state: String,
    pub relation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCatalogFamilyRecord {
    pub root_title: String,
    pub sequence: u64,
    pub family_size: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_title: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relatives: Vec<SessionCatalogFamilyRelative>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCatalogArtifactRecord {
    pub artifact_id: String,
    pub kind: String,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCatalogRecapRecord {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub touched_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub active_context_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_artifacts: Vec<SessionCatalogArtifactRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ctas: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCatalogQuickControlRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    pub display_value: String,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherited_value: Option<String>,
    pub override_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCatalogToggleControlRecord {
    pub value: bool,
    pub source: String,
    pub inherited_value: bool,
    pub override_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCatalogQuickControlsRecord {
    pub agent: SessionCatalogQuickControlRecord,
    pub model: SessionCatalogQuickControlRecord,
    pub thinking: SessionCatalogToggleControlRecord,
    pub trace: SessionCatalogToggleControlRecord,
    pub verbose: SessionCatalogToggleControlRecord,
    pub reset_to_default_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCatalogRecord {
    pub session_id: String,
    pub session_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_label: Option<String>,
    pub title: String,
    pub title_source: String,
    pub title_generation_state: String,
    pub manual_title_locked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_title_updated_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manual_title_updated_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
    pub preview_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_intent: Option<String>,
    pub last_intent_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_summary: Option<String>,
    pub last_summary_state: String,
    pub branch_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch_origin_run_id: Option<String>,
    pub principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_started_at_unix_ms: Option<i64>,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub archived: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archived_at_unix_ms: Option<i64>,
    pub pending_approvals: usize,
    pub has_context_files: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_context_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_profile: Option<String>,
    pub artifact_count: usize,
    pub family: SessionCatalogFamilyRecord,
    pub recap: SessionCatalogRecapRecord,
    pub quick_controls: SessionCatalogQuickControlsRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCatalogSummary {
    pub active_sessions: usize,
    pub archived_sessions: usize,
    pub sessions_with_pending_approvals: usize,
    pub sessions_with_active_runs: usize,
    pub sessions_with_context_files: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCatalogQueryEcho {
    pub limit: usize,
    pub cursor: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub q: Option<String>,
    pub include_archived: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archived: Option<bool>,
    pub sort: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_pending_approvals: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_context_files: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title_state: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCatalogListEnvelope {
    pub contract: ContractDescriptor,
    #[serde(default)]
    pub sessions: Vec<SessionCatalogRecord>,
    pub summary: SessionCatalogSummary,
    pub query: SessionCatalogQueryEcho,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCatalogDetailEnvelope {
    pub contract: ContractDescriptor,
    pub session: SessionCatalogRecord,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogListQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub severity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contains: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogQueryEcho {
    pub limit: usize,
    pub direction: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub severity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contains: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogRecord {
    pub cursor: String,
    pub source: String,
    pub source_kind: String,
    pub severity: String,
    pub message: String,
    pub timestamp_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connector_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub structured_payload: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogListEnvelope {
    pub contract: ContractDescriptor,
    pub query: LogQueryEcho,
    #[serde(default)]
    pub records: Vec<LogRecord>,
    pub page: PageInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub newest_cursor: Option<String>,
    #[serde(default)]
    pub available_sources: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodePairingMethod {
    Pin,
    Qr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodePairingRequestState {
    PendingApproval,
    Approved,
    Rejected,
    Completed,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePairingCodeView {
    pub code: String,
    pub method: NodePairingMethod,
    pub issued_by: String,
    pub created_at_unix_ms: i64,
    pub expires_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePairingRequestView {
    pub request_id: String,
    pub session_id: String,
    pub device_id: String,
    pub client_kind: String,
    pub method: NodePairingMethod,
    pub code_issued_by: String,
    pub requested_at_unix_ms: i64,
    pub expires_at_unix_ms: i64,
    pub approval_id: String,
    pub state: NodePairingRequestState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_scope_ttl_ms: Option<i64>,
    pub identity_fingerprint: String,
    pub transcript_hash_hex: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_expires_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePairingListEnvelope {
    pub contract: ContractDescriptor,
    #[serde(default)]
    pub codes: Vec<NodePairingCodeView>,
    #[serde(default)]
    pub requests: Vec<NodePairingRequestView>,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePairingRequestEnvelope {
    pub contract: ContractDescriptor,
    pub request: NodePairingRequestView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePairingCodeEnvelope {
    pub contract: ContractDescriptor,
    pub code: NodePairingCodeView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceRecord {
    pub device_id: String,
    pub client_kind: String,
    pub status: String,
    pub paired_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub issued_by: String,
    pub approval_id: String,
    pub identity_fingerprint: String,
    pub transcript_hash_hex: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_certificate_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub certificate_fingerprint_history: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_certificate_expires_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revoked_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revoked_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub removed_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceListEnvelope {
    pub contract: ContractDescriptor,
    #[serde(default)]
    pub devices: Vec<DeviceRecord>,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceEnvelope {
    pub contract: ContractDescriptor,
    pub device: DeviceRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceClearEnvelope {
    pub contract: ContractDescriptor,
    pub deleted: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCapabilityView {
    pub name: String,
    pub available: bool,
    pub execution_mode: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub device_id: String,
    pub platform: String,
    #[serde(default)]
    pub capabilities: Vec<NodeCapabilityView>,
    pub registered_at_unix_ms: i64,
    pub last_seen_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeListEnvelope {
    pub contract: ContractDescriptor,
    #[serde(default)]
    pub nodes: Vec<NodeRecord>,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeEnvelope {
    pub contract: ContractDescriptor,
    pub node: NodeRecord,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeInvokeEnvelope {
    pub contract: ContractDescriptor,
    pub device_id: String,
    pub capability: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_json: Option<Value>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeCapabilityRequestState {
    Queued,
    Dispatched,
    AwaitingLocalMediation,
    Succeeded,
    Failed,
    TimedOut,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCapabilityRequestView {
    pub request_id: String,
    pub device_id: String,
    pub capability: String,
    pub state: NodeCapabilityRequestState,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dispatched_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_ms: Option<i64>,
    pub max_payload_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InventoryPresenceState {
    Ok,
    Stale,
    Degraded,
    Offline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InventoryTrustState {
    Trusted,
    Pending,
    Revoked,
    Removed,
    Legacy,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryCapabilitySummary {
    pub total: usize,
    pub available: usize,
    pub unavailable: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryActionAvailability {
    pub can_rotate: bool,
    pub can_revoke: bool,
    pub can_remove: bool,
    pub can_invoke: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryDeviceRecord {
    pub device_id: String,
    pub client_kind: String,
    pub device_status: String,
    pub trust_state: InventoryTrustState,
    pub presence_state: InventoryPresenceState,
    pub paired_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registered_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat_age_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_session_id: Option<String>,
    pub pending_pairings: usize,
    pub issued_by: String,
    pub approval_id: String,
    pub identity_fingerprint: String,
    pub transcript_hash_hex: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_certificate_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub certificate_fingerprint_history: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<NodeCapabilityView>,
    pub capability_summary: InventoryCapabilitySummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_certificate_expires_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revoked_reason: Option<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
    pub actions: InventoryActionAvailability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryInstanceRecord {
    pub instance_id: String,
    pub label: String,
    pub kind: String,
    pub presence_state: InventoryPresenceState,
    pub observed_at_unix_ms: i64,
    pub state_label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub capability_summary: InventoryCapabilitySummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventorySummary {
    pub devices: usize,
    pub trusted_devices: usize,
    pub pending_pairings: usize,
    pub ok_devices: usize,
    pub stale_devices: usize,
    pub degraded_devices: usize,
    pub offline_devices: usize,
    pub ok_instances: usize,
    pub stale_instances: usize,
    pub degraded_instances: usize,
    pub offline_instances: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryListEnvelope {
    pub contract: ContractDescriptor,
    pub generated_at_unix_ms: i64,
    pub summary: InventorySummary,
    #[serde(default)]
    pub devices: Vec<InventoryDeviceRecord>,
    #[serde(default)]
    pub pending_pairings: Vec<NodePairingRequestView>,
    #[serde(default)]
    pub instances: Vec<InventoryInstanceRecord>,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryDeviceDetailEnvelope {
    pub contract: ContractDescriptor,
    pub generated_at_unix_ms: i64,
    pub device: InventoryDeviceRecord,
    #[serde(default)]
    pub pairings: Vec<NodePairingRequestView>,
    #[serde(default)]
    pub capability_requests: Vec<NodeCapabilityRequestView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportBundleJobState {
    Queued,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupportBundleJob {
    pub job_id: String,
    pub state: SupportBundleJobState,
    pub requested_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_path: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub command_output: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupportBundleJobEnvelope {
    pub contract: ContractDescriptor,
    pub job: SupportBundleJob,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupportBundleJobListEnvelope {
    pub contract: ContractDescriptor,
    #[serde(default)]
    pub jobs: Vec<SupportBundleJob>,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorRecoveryJobState {
    Queued,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DoctorRecoveryJob {
    pub job_id: String,
    pub state: DoctorRecoveryJobState,
    pub requested_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub report: Option<Value>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub command_output: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DoctorRecoveryJobEnvelope {
    pub contract: ContractDescriptor,
    pub job: DoctorRecoveryJob,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DoctorRecoveryJobListEnvelope {
    pub contract: ContractDescriptor,
    #[serde(default)]
    pub jobs: Vec<DoctorRecoveryJob>,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityCatalog {
    pub contract: ContractDescriptor,
    pub version: String,
    pub generated_at_unix_ms: i64,
    #[serde(default)]
    pub capabilities: Vec<CapabilityEntry>,
    #[serde(default)]
    pub migration_notes: Vec<CapabilityMigrationNote>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityDashboardExposure {
    DirectAction,
    CliHandoff,
    InternalOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityEntry {
    pub id: String,
    pub domain: String,
    #[serde(default)]
    pub dashboard_section: String,
    pub title: String,
    pub owner: String,
    #[serde(default)]
    pub surfaces: Vec<String>,
    pub execution_mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dashboard_exposure: Option<CapabilityDashboardExposure>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cli_handoff_commands: Vec<String>,
    #[serde(default)]
    pub mutation_classes: Vec<String>,
    #[serde(default)]
    pub test_refs: Vec<String>,
    #[serde(default)]
    pub contract_paths: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityMigrationNote {
    pub id: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleLoginRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admin_token: Option<String>,
    pub principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleBrowserHandoffRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redirect_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleBrowserHandoffEnvelope {
    pub handoff_url: String,
    pub expires_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserProfilesQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSessionBudget {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_navigation_timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_session_lifetime_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_screenshot_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_response_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_action_timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_type_input_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_actions_per_session: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_actions_per_window: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_rate_window_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_action_log_entries: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_observe_snapshot_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_visible_text_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_network_log_entries: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_network_log_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserPermissionSetting {
    Unspecified,
    Deny,
    Allow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSessionPermissions {
    pub camera: BrowserPermissionSetting,
    pub microphone: BrowserPermissionSetting,
    pub location: BrowserPermissionSetting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserActionLogEntry {
    pub action_id: String,
    pub action_name: String,
    pub selector: String,
    pub success: bool,
    pub outcome: String,
    pub error: String,
    pub started_at_unix_ms: u64,
    pub completed_at_unix_ms: u64,
    pub attempts: u32,
    pub page_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserNetworkLogHeader {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserNetworkLogEntry {
    pub request_url: String,
    pub status_code: u32,
    pub timing_bucket: String,
    pub latency_ms: u64,
    pub captured_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<BrowserNetworkLogHeader>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserDiagnosticSeverity {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserConsoleEntry {
    pub severity: BrowserDiagnosticSeverity,
    pub kind: String,
    pub message: String,
    pub captured_at_unix_ms: u64,
    pub source: String,
    pub stack_trace: String,
    pub page_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserPageDiagnostics {
    pub page_url: String,
    pub page_title: String,
    pub console_entry_count: u32,
    pub warning_count: u32,
    pub error_count: u32,
    pub last_event_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserTabRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tab_id: Option<String>,
    pub url: String,
    pub title: String,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserProfileRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    pub principal: String,
    pub name: String,
    pub theme_color: String,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
    pub last_used_unix_ms: u64,
    pub persistence_enabled: bool,
    pub private_profile: bool,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserDownloadArtifactRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    pub source_url: String,
    pub file_name: String,
    pub mime_type: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub created_at_unix_ms: u64,
    pub quarantined: bool,
    pub quarantine_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserCreateProfileRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub theme_color: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistence_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_profile: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserRenameProfileRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserProfileScopeRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserProfileListEnvelope {
    pub contract: ContractDescriptor,
    pub principal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_profile_id: Option<String>,
    #[serde(default)]
    pub profiles: Vec<BrowserProfileRecord>,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserProfileEnvelope {
    pub contract: ContractDescriptor,
    pub profile: BrowserProfileRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserProfileDeleteEnvelope {
    pub contract: ContractDescriptor,
    pub principal: String,
    pub profile_id: String,
    pub deleted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_profile_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserDownloadArtifactsQuery {
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default)]
    pub quarantined_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserDownloadArtifactListEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    #[serde(default)]
    pub artifacts: Vec<BrowserDownloadArtifactRecord>,
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSessionCreateRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idle_ttl_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget: Option<BrowserSessionBudget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_private_targets: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_downloads: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub action_allowed_domains: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistence_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistence_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_profile: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSessionCreateEnvelope {
    pub contract: ContractDescriptor,
    pub principal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub created_at_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_budget: Option<BrowserSessionBudget>,
    pub downloads_enabled: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub action_allowed_domains: Vec<String>,
    pub persistence_enabled: bool,
    pub persistence_id: String,
    pub state_restored: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    pub private_profile: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSessionCloseEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub closed: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserNavigateRequest {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_redirects: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_redirects: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_private_targets: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserNavigateEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    pub final_url: String,
    pub status_code: u32,
    pub title: String,
    pub body_bytes: u64,
    pub latency_ms: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserClickRequest {
    pub selector: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_failure_screenshot: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_failure_screenshot_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserClickEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_log: Option<BrowserActionLogEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact: Option<BrowserDownloadArtifactRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_screenshot_mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_screenshot_base64: Option<String>,
}

impl BrowserClickEnvelope {
    #[must_use]
    pub fn decode_failure_screenshot(&self) -> Option<Vec<u8>> {
        decode_optional_base64(self.failure_screenshot_base64.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserTypeRequest {
    pub selector: String,
    #[serde(default)]
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clear_existing: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_failure_screenshot: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_failure_screenshot_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserTypeEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    pub typed_bytes: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_log: Option<BrowserActionLogEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_screenshot_mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_screenshot_base64: Option<String>,
}

impl BrowserTypeEnvelope {
    #[must_use]
    pub fn decode_failure_screenshot(&self) -> Option<Vec<u8>> {
        decode_optional_base64(self.failure_screenshot_base64.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserPressRequest {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_failure_screenshot: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_failure_screenshot_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserPressEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    pub key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_log: Option<BrowserActionLogEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_screenshot_mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_screenshot_base64: Option<String>,
}

impl BrowserPressEnvelope {
    #[must_use]
    pub fn decode_failure_screenshot(&self) -> Option<Vec<u8>> {
        decode_optional_base64(self.failure_screenshot_base64.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSelectRequest {
    pub selector: String,
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_failure_screenshot: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_failure_screenshot_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSelectEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    pub selected_value: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_log: Option<BrowserActionLogEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_screenshot_mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_screenshot_base64: Option<String>,
}

impl BrowserSelectEnvelope {
    #[must_use]
    pub fn decode_failure_screenshot(&self) -> Option<Vec<u8>> {
        decode_optional_base64(self.failure_screenshot_base64.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserHighlightRequest {
    pub selector: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_failure_screenshot: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_failure_screenshot_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserHighlightEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    pub selector: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_log: Option<BrowserActionLogEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_screenshot_mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_screenshot_base64: Option<String>,
}

impl BrowserHighlightEnvelope {
    #[must_use]
    pub fn decode_failure_screenshot(&self) -> Option<Vec<u8>> {
        decode_optional_base64(self.failure_screenshot_base64.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserScrollRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_x: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_y: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_failure_screenshot: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_failure_screenshot_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserScrollEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    pub scroll_x: i64,
    pub scroll_y: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_log: Option<BrowserActionLogEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_screenshot_mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_screenshot_base64: Option<String>,
}

impl BrowserScrollEnvelope {
    #[must_use]
    pub fn decode_failure_screenshot(&self) -> Option<Vec<u8>> {
        decode_optional_base64(self.failure_screenshot_base64.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserWaitForRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poll_interval_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_failure_screenshot: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_failure_screenshot_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserWaitForEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    pub waited_ms: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    pub matched_selector: String,
    pub matched_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_log: Option<BrowserActionLogEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_screenshot_mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_screenshot_base64: Option<String>,
}

impl BrowserWaitForEnvelope {
    #[must_use]
    pub fn decode_failure_screenshot(&self) -> Option<Vec<u8>> {
        decode_optional_base64(self.failure_screenshot_base64.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserTitleQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_title_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserTitleEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    pub title: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserScreenshotQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserScreenshotEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_base64: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

impl BrowserScreenshotEnvelope {
    #[must_use]
    pub fn decode_image(&self) -> Option<Vec<u8>> {
        decode_optional_base64(self.image_base64.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserPdfQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserPdfEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    pub size_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact: Option<BrowserDownloadArtifactRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pdf_base64: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

impl BrowserPdfEnvelope {
    #[must_use]
    pub fn decode_pdf(&self) -> Option<Vec<u8>> {
        decode_optional_base64(self.pdf_base64.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserObserveQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_dom_snapshot: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_accessibility_tree: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_visible_text: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_dom_snapshot_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_accessibility_tree_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_visible_text_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserObserveEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    pub dom_snapshot: String,
    pub accessibility_tree: String,
    pub visible_text: String,
    pub dom_truncated: bool,
    pub accessibility_tree_truncated: bool,
    pub visible_text_truncated: bool,
    pub page_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserNetworkLogQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_headers: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_payload_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserNetworkLogEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    #[serde(default)]
    pub entries: Vec<BrowserNetworkLogEntry>,
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserConsoleLogQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minimum_severity: Option<BrowserDiagnosticSeverity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_page_diagnostics: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_payload_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserConsoleLogEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    #[serde(default)]
    pub entries: Vec<BrowserConsoleEntry>,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_diagnostics: Option<BrowserPageDiagnostics>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserTabListEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    #[serde(default)]
    pub tabs: Vec<BrowserTabRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_tab_id: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserOpenTabRequest {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activate: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_redirects: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_redirects: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_private_targets: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserOpenTabEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tab: Option<BrowserTabRecord>,
    pub navigated: bool,
    pub status_code: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserTabMutationRequest {
    pub tab_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserTabCloseRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tab_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSwitchTabEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_tab: Option<BrowserTabRecord>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserCloseTabEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_tab_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_tab: Option<BrowserTabRecord>,
    pub tabs_remaining: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserPermissionsEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permissions: Option<BrowserSessionPermissions>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSetPermissionsRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub camera: Option<BrowserPermissionSetting>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub microphone: Option<BrowserPermissionSetting>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<BrowserPermissionSetting>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset_to_default: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserResetStateRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clear_cookies: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clear_storage: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset_tabs: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset_permissions: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserResetStateEnvelope {
    pub contract: ContractDescriptor,
    pub session_id: String,
    pub success: bool,
    pub cookies_cleared: u32,
    pub storage_entries_cleared: u32,
    pub tabs_closed: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permissions: Option<BrowserSessionPermissions>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigInspectRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default)]
    pub show_secrets: bool,
    #[serde(default)]
    pub backups: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigValidateRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigMutationRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default = "default_config_backups")]
    pub backups: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigRecoverRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default = "default_recover_backup")]
    pub backup: usize,
    #[serde(default = "default_config_backups")]
    pub backups: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretSetRequest {
    pub scope: String,
    pub key: String,
    pub value_base64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretRevealRequest {
    pub scope: String,
    pub key: String,
    #[serde(default)]
    pub reveal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretDeleteRequest {
    pub scope: String,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookIntegrationView {
    pub integration_id: String,
    pub provider: String,
    pub display_name: String,
    pub secret_vault_ref: String,
    pub secret_present: bool,
    #[serde(default)]
    pub allowed_events: Vec<String>,
    #[serde(default)]
    pub allowed_sources: Vec<String>,
    pub enabled: bool,
    pub signature_required: bool,
    pub max_payload_bytes: u64,
    pub status: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_test_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_test_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_test_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookIntegrationListEnvelope {
    pub contract: ContractDescriptor,
    #[serde(default)]
    pub integrations: Vec<WebhookIntegrationView>,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookIntegrationEnvelope {
    pub contract: ContractDescriptor,
    pub integration: WebhookIntegrationView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookIntegrationDeleteEnvelope {
    pub contract: ContractDescriptor,
    pub integration_id: String,
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookIntegrationUpsertRequest {
    pub integration_id: String,
    pub provider: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub secret_vault_ref: String,
    #[serde(default)]
    pub allowed_events: Vec<String>,
    #[serde(default)]
    pub allowed_sources: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature_required: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_payload_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookIntegrationEnabledRequest {
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookIntegrationTestRequest {
    pub payload_base64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookIntegrationTestResult {
    pub integration_id: String,
    pub valid: bool,
    pub outcome: String,
    pub message: String,
    pub payload_bytes: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    pub signature_present: bool,
    pub secret_present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookIntegrationTestEnvelope {
    pub contract: ContractDescriptor,
    pub integration: WebhookIntegrationView,
    pub result: WebhookIntegrationTestResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PluginBindingsQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plugin_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PluginCapabilityProfile {
    #[serde(default)]
    pub http_hosts: Vec<String>,
    #[serde(default)]
    pub secrets: Vec<String>,
    #[serde(default)]
    pub storage_prefixes: Vec<String>,
    #[serde(default)]
    pub channels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PluginOperatorMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_principal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginBindingView {
    pub plugin_id: String,
    pub enabled: bool,
    pub skill_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<String>,
    #[serde(default)]
    pub capability_profile: PluginCapabilityProfile,
    #[serde(default)]
    pub operator: PluginOperatorMetadata,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginBindingListEntry {
    pub binding: PluginBindingView,
    pub check: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginBindingListEnvelope {
    pub contract: ContractDescriptor,
    pub plugins_root: String,
    pub count: usize,
    #[serde(default)]
    pub entries: Vec<PluginBindingListEntry>,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginBindingEnvelope {
    pub contract: ContractDescriptor,
    pub binding: PluginBindingView,
    pub check: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub installed_skill: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginBindingDeleteEnvelope {
    pub contract: ContractDescriptor,
    pub deleted: bool,
    pub binding: PluginBindingView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginBindingUpsertRequest {
    pub plugin_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_tofu: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_untrusted: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capability_profile: Option<PluginCapabilityProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator: Option<PluginOperatorMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HookBindingsQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hook_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plugin_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HookOperatorMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_principal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookBindingView {
    pub hook_id: String,
    pub event: String,
    pub plugin_id: String,
    pub enabled: bool,
    #[serde(default)]
    pub operator: HookOperatorMetadata,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookBindingListEntry {
    pub binding: HookBindingView,
    pub check: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookBindingListEnvelope {
    pub contract: ContractDescriptor,
    pub hooks_root: String,
    pub count: usize,
    #[serde(default)]
    pub entries: Vec<HookBindingListEntry>,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookBindingEnvelope {
    pub contract: ContractDescriptor,
    pub binding: HookBindingView,
    pub check: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookBindingDeleteEnvelope {
    pub contract: ContractDescriptor,
    pub deleted: bool,
    pub binding: HookBindingView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookBindingUpsertRequest {
    pub hook_id: String,
    pub event: String,
    pub plugin_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator: Option<HookOperatorMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairingCodeMintRequest {
    pub channel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issued_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePairingCodeMintRequest {
    pub method: NodePairingMethod,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issued_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NodePairingListQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<NodePairingRequestState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePairingDecisionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceActionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DeviceClearRequest {
    #[serde(default)]
    pub revoked_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInvokeRequest {
    pub capability: String,
    #[serde(default)]
    pub input_json: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_payload_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupportBundleCreateRequest {
    #[serde(default = "default_support_bundle_backups")]
    pub retain_jobs: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DoctorRecoveryCreateRequest {
    #[serde(default = "default_doctor_recovery_jobs")]
    pub retain_jobs: usize,
    #[serde(default)]
    pub repair: bool,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub force: bool,
    #[serde(default)]
    pub only: Vec<String>,
    #[serde(default)]
    pub skip: Vec<String>,
    #[serde(default)]
    pub rollback_run: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderAuthActionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderApiKeyUpsertRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    pub profile_name: String,
    pub scope: AuthProfileScope,
    pub api_key: String,
    #[serde(default)]
    pub set_default: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenAiApiKeyUpsertRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    pub profile_name: String,
    pub scope: AuthProfileScope,
    pub api_key: String,
    #[serde(default)]
    pub set_default: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenAiOAuthBootstrapRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<AuthProfileScope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
    #[serde(default)]
    pub set_default: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenAiOAuthBootstrapEnvelope {
    pub contract: ContractDescriptor,
    pub provider: String,
    pub attempt_id: String,
    pub authorization_url: String,
    pub expires_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenAiOAuthCallbackStateEnvelope {
    pub contract: ContractDescriptor,
    pub provider: String,
    pub attempt_id: String,
    pub state: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<i64>,
}

const fn default_config_backups() -> usize {
    5
}

const fn default_recover_backup() -> usize {
    1
}

const fn default_support_bundle_backups() -> usize {
    16
}

const fn default_doctor_recovery_jobs() -> usize {
    16
}

#[cfg(test)]
mod tests {
    use super::ConsoleLoginRequest;

    #[test]
    fn console_login_request_omits_admin_token_when_not_provided() {
        let request = ConsoleLoginRequest {
            admin_token: None,
            principal: "admin:test".to_owned(),
            device_id: "device-1".to_owned(),
            channel: Some("cli".to_owned()),
        };
        let encoded = serde_json::to_value(&request).expect("console login request should encode");
        let principal = encoded.get("principal").and_then(serde_json::Value::as_str);
        assert!(encoded.get("admin_token").is_none());
        assert_eq!(principal, Some("admin:test"));
    }
}
