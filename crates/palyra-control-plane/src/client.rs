use std::time::Duration;

use reqwest::{Client, Method, Url};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::errors::{ControlPlaneClientError, ErrorEnvelope};
use crate::models::*;
use crate::transport::{fallback_error_message, urlencoding};

const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_SAFE_READ_RETRIES: usize = 1;

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

    pub async fn create_browser_handoff(
        &self,
        request: &ConsoleBrowserHandoffRequest,
    ) -> Result<ConsoleBrowserHandoffEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/auth/browser-handoff", Some(request), true)
            .await
    }

    pub async fn get_diagnostics(&self) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/diagnostics", None::<&Value>, false).await
    }

    pub async fn get_json_value(
        &self,
        path: impl AsRef<str>,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::GET, path, None::<&Value>, false).await
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
