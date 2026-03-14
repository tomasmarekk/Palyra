use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::contract::{ContractDescriptor, PageInfo};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleSession {
    pub principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
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
pub struct AgentRecord {
    pub agent_id: String,
    pub display_name: String,
    pub agent_dir: String,
    #[serde(default)]
    pub workspace_roots: Vec<String>,
    pub default_model_profile: String,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_agent_id: Option<String>,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEnvelope {
    pub contract: ContractDescriptor,
    pub agent: AgentRecord,
    pub is_default: bool,
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
    pub admin_token: String,
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
pub struct PairingCodeMintRequest {
    pub channel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issued_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupportBundleCreateRequest {
    #[serde(default = "default_support_bundle_backups")]
    pub retain_jobs: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderAuthActionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
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
