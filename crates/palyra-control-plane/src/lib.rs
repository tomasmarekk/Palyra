use std::time::Duration;

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use reqwest::{Client, Method, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub const CONTROL_PLANE_CONTRACT_VERSION: &str = "control-plane.v1";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_SAFE_READ_RETRIES: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    Auth,
    Validation,
    Policy,
    NotFound,
    Conflict,
    Dependency,
    Availability,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationIssue {
    pub field: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub error: String,
    pub code: String,
    pub category: ErrorCategory,
    pub retryable: bool,
    #[serde(default)]
    pub redacted: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub validation_errors: Vec<ValidationIssue>,
}

impl ErrorEnvelope {
    #[must_use]
    pub fn message(&self) -> &str {
        self.error.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageInfo {
    pub limit: usize,
    pub returned: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractDescriptor {
    pub contract_version: String,
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityEntry {
    pub id: String,
    pub domain: String,
    pub title: String,
    pub owner: String,
    #[serde(default)]
    pub surfaces: Vec<String>,
    pub execution_mode: String,
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

#[derive(Debug, Clone)]
pub struct ControlPlaneClientConfig {
    pub base_url: String,
    pub request_timeout: Duration,
    pub safe_read_retries: usize,
}

impl ControlPlaneClientConfig {
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            request_timeout: Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
            safe_read_retries: DEFAULT_SAFE_READ_RETRIES,
        }
    }
}

#[derive(Debug, Error)]
pub enum ControlPlaneClientError {
    #[error("invalid control-plane base URL: {0}")]
    InvalidBaseUrl(String),
    #[error("HTTP client initialization failed: {0}")]
    ClientInit(String),
    #[error("request failed: {0}")]
    Transport(String),
    #[error("request failed with HTTP {status}: {message}")]
    Http { status: u16, message: String, envelope: Option<ErrorEnvelope> },
    #[error("response decoding failed: {0}")]
    Decode(String),
}

#[derive(Clone)]
pub struct ControlPlaneClient {
    base_url: Url,
    client: Client,
    csrf_token: Option<String>,
    safe_read_retries: usize,
}

impl ControlPlaneClient {
    pub fn new(config: ControlPlaneClientConfig) -> Result<Self, ControlPlaneClientError> {
        let client = Client::builder()
            .cookie_store(true)
            .timeout(config.request_timeout)
            .build()
            .map_err(|error| ControlPlaneClientError::ClientInit(error.to_string()))?;
        Self::with_client(config, client)
    }

    pub fn with_client(
        config: ControlPlaneClientConfig,
        client: Client,
    ) -> Result<Self, ControlPlaneClientError> {
        let mut base_url = Url::parse(config.base_url.as_str())
            .map_err(|error| ControlPlaneClientError::InvalidBaseUrl(error.to_string()))?;
        if !base_url.path().ends_with('/') {
            let normalized = format!("{}/", base_url.path().trim_end_matches('/'));
            base_url.set_path(normalized.as_str());
        }
        Ok(Self { base_url, client, csrf_token: None, safe_read_retries: config.safe_read_retries })
    }

    pub fn set_csrf_token(&mut self, csrf_token: Option<String>) {
        self.csrf_token = csrf_token;
    }

    pub async fn get_session(&mut self) -> Result<ConsoleSession, ControlPlaneClientError> {
        let session: ConsoleSession = self
            .request_json(Method::GET, "console/v1/auth/session", None::<&Value>, false)
            .await?;
        self.csrf_token = Some(session.csrf_token.clone());
        Ok(session)
    }

    pub async fn login(
        &mut self,
        request: &ConsoleLoginRequest,
    ) -> Result<ConsoleSession, ControlPlaneClientError> {
        let session: ConsoleSession =
            self.request_json(Method::POST, "console/v1/auth/login", Some(request), false).await?;
        self.csrf_token = Some(session.csrf_token.clone());
        Ok(session)
    }

    pub async fn get_diagnostics(&self) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/diagnostics", None::<&Value>, false).await
    }

    pub async fn get_deployment_posture(
        &self,
    ) -> Result<DeploymentPostureSummary, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/deployment/posture", None::<&Value>, false).await
    }

    pub async fn get_capability_catalog(
        &self,
    ) -> Result<CapabilityCatalog, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            "console/v1/control-plane/capabilities",
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn inspect_config(
        &self,
        request: &ConfigInspectRequest,
    ) -> Result<ConfigDocumentSnapshot, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/config/inspect", Some(request), false).await
    }

    pub async fn validate_config(
        &self,
        request: &ConfigValidateRequest,
    ) -> Result<ConfigValidationEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/config/validate", Some(request), false).await
    }

    pub async fn mutate_config(
        &self,
        request: &ConfigMutationRequest,
    ) -> Result<ConfigMutationEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/config/mutate", Some(request), true).await
    }

    pub async fn migrate_config(
        &self,
        request: &ConfigInspectRequest,
    ) -> Result<ConfigMutationEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/config/migrate", Some(request), true).await
    }

    pub async fn recover_config(
        &self,
        request: &ConfigRecoverRequest,
    ) -> Result<ConfigMutationEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/config/recover", Some(request), true).await
    }

    pub async fn list_secrets(
        &self,
        scope: &str,
    ) -> Result<SecretMetadataList, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/secrets?scope={}", urlencoding(scope)),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn get_secret_metadata(
        &self,
        scope: &str,
        key: &str,
    ) -> Result<SecretMetadataEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!(
                "console/v1/secrets/metadata?scope={}&key={}",
                urlencoding(scope),
                urlencoding(key)
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn set_secret(
        &self,
        request: &SecretSetRequest,
    ) -> Result<SecretMetadataEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/secrets", Some(request), true).await
    }

    pub async fn reveal_secret(
        &self,
        request: &SecretRevealRequest,
    ) -> Result<SecretRevealEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/secrets/reveal", Some(request), true).await
    }

    pub async fn delete_secret(
        &self,
        request: &SecretDeleteRequest,
    ) -> Result<SecretMetadataEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/secrets/delete", Some(request), true).await
    }

    pub async fn list_auth_profiles(
        &self,
        query: &str,
    ) -> Result<AuthProfileListEnvelope, ControlPlaneClientError> {
        let path = if query.trim().is_empty() {
            "console/v1/auth/profiles".to_owned()
        } else {
            format!("console/v1/auth/profiles?{query}")
        };
        self.request_json(Method::GET, path, None::<&Value>, false).await
    }

    pub async fn get_auth_profile(
        &self,
        profile_id: &str,
    ) -> Result<AuthProfileEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/auth/profiles/{}", urlencoding(profile_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn upsert_auth_profile(
        &self,
        profile: &AuthProfileView,
    ) -> Result<AuthProfileEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/auth/profiles", Some(profile), true).await
    }

    pub async fn delete_auth_profile(
        &self,
        profile_id: &str,
    ) -> Result<AuthProfileDeleteEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/auth/profiles/{}/delete", urlencoding(profile_id)),
            Some(&serde_json::json!({})),
            true,
        )
        .await
    }

    pub async fn get_auth_health(
        &self,
        include_profiles: bool,
        agent_id: Option<&str>,
    ) -> Result<AuthHealthEnvelope, ControlPlaneClientError> {
        let mut query = format!("include_profiles={include_profiles}");
        if let Some(agent_id) = agent_id.filter(|value| !value.trim().is_empty()) {
            query.push_str(format!("&agent_id={}", urlencoding(agent_id)).as_str());
        }
        self.request_json(
            Method::GET,
            format!("console/v1/auth/health?{query}"),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn get_openai_provider_state(
        &self,
    ) -> Result<ProviderAuthStateEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/auth/providers/openai", None::<&Value>, false)
            .await
    }

    pub async fn connect_openai_api_key(
        &self,
        request: &OpenAiApiKeyUpsertRequest,
    ) -> Result<ProviderAuthActionEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            "console/v1/auth/providers/openai/api-key",
            Some(request),
            true,
        )
        .await
    }

    pub async fn start_openai_oauth_bootstrap(
        &self,
        request: &OpenAiOAuthBootstrapRequest,
    ) -> Result<OpenAiOAuthBootstrapEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            "console/v1/auth/providers/openai/bootstrap",
            Some(request),
            true,
        )
        .await
    }

    pub async fn get_openai_oauth_callback_state(
        &self,
        attempt_id: &str,
    ) -> Result<OpenAiOAuthCallbackStateEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!(
                "console/v1/auth/providers/openai/callback-state?attempt_id={}",
                urlencoding(attempt_id)
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn run_openai_provider_action(
        &self,
        action: &str,
        request: &ProviderAuthActionRequest,
    ) -> Result<ProviderAuthActionEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/auth/providers/openai/{action}"),
            Some(request),
            true,
        )
        .await
    }

    pub async fn get_pairing_summary(
        &self,
    ) -> Result<PairingSummaryEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/pairing", None::<&Value>, false).await
    }

    pub async fn mint_pairing_code(
        &self,
        request: &PairingCodeMintRequest,
    ) -> Result<PairingSummaryEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/pairing/codes", Some(request), true).await
    }

    pub async fn list_support_bundle_jobs(
        &self,
    ) -> Result<SupportBundleJobListEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/support-bundle/jobs", None::<&Value>, false)
            .await
    }

    pub async fn create_support_bundle_job(
        &self,
        request: &SupportBundleCreateRequest,
    ) -> Result<SupportBundleJobEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/support-bundle/jobs", Some(request), true).await
    }

    pub async fn get_support_bundle_job(
        &self,
        job_id: &str,
    ) -> Result<SupportBundleJobEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/support-bundle/jobs/{}", urlencoding(job_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    async fn request_json<T, B>(
        &self,
        method: Method,
        path: impl AsRef<str>,
        body: Option<&B>,
        require_csrf: bool,
    ) -> Result<T, ControlPlaneClientError>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let relative = path.as_ref().trim_start_matches('/');
        let url = self
            .base_url
            .join(relative)
            .map_err(|error| ControlPlaneClientError::InvalidBaseUrl(error.to_string()))?;
        let mut attempts_remaining =
            if method == Method::GET { self.safe_read_retries + 1 } else { 1 };
        loop {
            let mut request = self.client.request(method.clone(), url.clone());
            if require_csrf {
                if let Some(token) = self.csrf_token.as_deref() {
                    request = request.header("x-palyra-csrf-token", token);
                }
            }
            if let Some(body) = body {
                request = request.json(body);
            }
            let response = request
                .send()
                .await
                .map_err(|error| ControlPlaneClientError::Transport(error.to_string()));
            match response {
                Ok(response) => {
                    if !response.status().is_success() {
                        let status = response.status().as_u16();
                        let body = response
                            .text()
                            .await
                            .map_err(|error| ControlPlaneClientError::Decode(error.to_string()))?;
                        let envelope = serde_json::from_str::<ErrorEnvelope>(body.as_str()).ok();
                        let message = envelope
                            .as_ref()
                            .map(|value| value.error.clone())
                            .unwrap_or_else(|| fallback_error_message(status, body.as_str()));
                        return Err(ControlPlaneClientError::Http { status, message, envelope });
                    }
                    return response
                        .json::<T>()
                        .await
                        .map_err(|error| ControlPlaneClientError::Decode(error.to_string()));
                }
                Err(error) => {
                    attempts_remaining = attempts_remaining.saturating_sub(1);
                    if attempts_remaining == 0 {
                        return Err(error);
                    }
                }
            }
        }
    }
}

fn fallback_error_message(status: u16, body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return format!("request failed with HTTP {status}");
    }
    if trimmed.len() > 256 {
        return format!("request failed with HTTP {status}");
    }
    trimmed.to_owned()
}

fn urlencoding(raw: &str) -> String {
    raw.bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![char::from(byte)]
            }
            _ => format!("%{byte:02X}").chars().collect::<Vec<_>>(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_reveal_decodes_base64() {
        let envelope = SecretRevealEnvelope {
            contract: ContractDescriptor {
                contract_version: CONTROL_PLANE_CONTRACT_VERSION.to_owned(),
            },
            scope: "global".to_owned(),
            key: "openai_api_key".to_owned(),
            value_bytes: 3,
            value_base64: BASE64_STANDARD.encode(b"abc"),
            value_utf8: Some("abc".to_owned()),
        };
        assert_eq!(envelope.decode_value().as_deref(), Some(b"abc".as_slice()));
    }

    #[test]
    fn urlencoding_escapes_reserved_bytes() {
        assert_eq!(urlencoding("global/openai key"), "global%2Fopenai%20key");
    }
}
