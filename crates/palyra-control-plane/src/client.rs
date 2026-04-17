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

    pub async fn get_mobile_bootstrap(
        &self,
    ) -> Result<MobileBootstrapEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/mobile/bootstrap", None::<&Value>, false).await
    }

    pub async fn get_mobile_inbox(&self) -> Result<MobileInboxEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/mobile/inbox", None::<&Value>, false).await
    }

    pub async fn list_mobile_approvals(
        &self,
        limit: Option<usize>,
    ) -> Result<MobileApprovalsEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path(
                "console/v1/mobile/approvals",
                vec![("limit", limit.map(|value| value.to_string()))],
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn get_mobile_approval(
        &self,
        approval_id: &str,
    ) -> Result<MobileApprovalDetailEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/mobile/approvals/{}", urlencoding(approval_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn decide_mobile_approval(
        &self,
        approval_id: &str,
        request: &ApprovalDecisionRequest,
    ) -> Result<ApprovalDecisionEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/mobile/approvals/{}/decision", urlencoding(approval_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn list_mobile_sessions(
        &self,
        limit: Option<usize>,
    ) -> Result<MobileSessionsEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path(
                "console/v1/mobile/sessions",
                vec![("limit", limit.map(|value| value.to_string()))],
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn get_mobile_session(
        &self,
        session_id: &str,
    ) -> Result<MobileSessionDetailEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/mobile/sessions/{}", urlencoding(session_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn prepare_mobile_safe_url_open(
        &self,
        request: &MobileSafeUrlOpenRequest,
    ) -> Result<MobileSafeUrlOpenEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/mobile/safe-url-open", Some(request), true)
            .await
    }

    pub async fn create_mobile_voice_note(
        &self,
        request: &MobileVoiceNoteCreateRequest,
    ) -> Result<MobileVoiceNoteEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/mobile/voice-notes", Some(request), true).await
    }

    pub async fn list_browser_profiles(
        &self,
        query: &BrowserProfilesQuery,
    ) -> Result<BrowserProfileListEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path(
                "console/v1/browser/profiles",
                vec![("principal", query.principal.clone())],
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn create_browser_profile(
        &self,
        request: &BrowserCreateProfileRequest,
    ) -> Result<BrowserProfileEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/browser/profiles/create", Some(request), true)
            .await
    }

    pub async fn rename_browser_profile(
        &self,
        profile_id: &str,
        request: &BrowserRenameProfileRequest,
    ) -> Result<BrowserProfileEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/profiles/{}/rename", urlencoding(profile_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn delete_browser_profile(
        &self,
        profile_id: &str,
        request: &BrowserProfileScopeRequest,
    ) -> Result<BrowserProfileDeleteEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/profiles/{}/delete", urlencoding(profile_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn activate_browser_profile(
        &self,
        profile_id: &str,
        request: &BrowserProfileScopeRequest,
    ) -> Result<BrowserProfileEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/profiles/{}/activate", urlencoding(profile_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn list_browser_download_artifacts(
        &self,
        query: &BrowserDownloadArtifactsQuery,
    ) -> Result<BrowserDownloadArtifactListEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path(
                "console/v1/browser/downloads",
                vec![
                    ("session_id", Some(query.session_id.clone())),
                    ("limit", query.limit.map(|value| value.to_string())),
                    ("quarantined_only", query.quarantined_only.then(|| "true".to_owned())),
                ],
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn create_browser_session(
        &self,
        request: &BrowserSessionCreateRequest,
    ) -> Result<BrowserSessionCreateEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/browser/sessions", Some(request), true).await
    }

    pub async fn close_browser_session(
        &self,
        session_id: &str,
    ) -> Result<BrowserSessionCloseEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/sessions/{}/close", urlencoding(session_id)),
            None::<&Value>,
            true,
        )
        .await
    }

    pub async fn navigate_browser_session(
        &self,
        session_id: &str,
        request: &BrowserNavigateRequest,
    ) -> Result<BrowserNavigateEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/sessions/{}/navigate", urlencoding(session_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn click_browser_session(
        &self,
        session_id: &str,
        request: &BrowserClickRequest,
    ) -> Result<BrowserClickEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/sessions/{}/click", urlencoding(session_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn type_browser_session(
        &self,
        session_id: &str,
        request: &BrowserTypeRequest,
    ) -> Result<BrowserTypeEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/sessions/{}/type", urlencoding(session_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn press_browser_session(
        &self,
        session_id: &str,
        request: &BrowserPressRequest,
    ) -> Result<BrowserPressEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/sessions/{}/press", urlencoding(session_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn select_browser_session(
        &self,
        session_id: &str,
        request: &BrowserSelectRequest,
    ) -> Result<BrowserSelectEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/sessions/{}/select", urlencoding(session_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn highlight_browser_session(
        &self,
        session_id: &str,
        request: &BrowserHighlightRequest,
    ) -> Result<BrowserHighlightEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/sessions/{}/highlight", urlencoding(session_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn scroll_browser_session(
        &self,
        session_id: &str,
        request: &BrowserScrollRequest,
    ) -> Result<BrowserScrollEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/sessions/{}/scroll", urlencoding(session_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn wait_for_browser_session(
        &self,
        session_id: &str,
        request: &BrowserWaitForRequest,
    ) -> Result<BrowserWaitForEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/sessions/{}/wait-for", urlencoding(session_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn get_browser_title(
        &self,
        session_id: &str,
        query: &BrowserTitleQuery,
    ) -> Result<BrowserTitleEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path(
                format!("console/v1/browser/sessions/{}/title", urlencoding(session_id)).as_str(),
                vec![("max_title_bytes", query.max_title_bytes.map(|value| value.to_string()))],
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn get_browser_screenshot(
        &self,
        session_id: &str,
        query: &BrowserScreenshotQuery,
    ) -> Result<BrowserScreenshotEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path(
                format!("console/v1/browser/sessions/{}/screenshot", urlencoding(session_id))
                    .as_str(),
                vec![
                    ("max_bytes", query.max_bytes.map(|value| value.to_string())),
                    ("format", query.format.clone()),
                ],
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn get_browser_pdf(
        &self,
        session_id: &str,
        query: &BrowserPdfQuery,
    ) -> Result<BrowserPdfEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path(
                format!("console/v1/browser/sessions/{}/pdf", urlencoding(session_id)).as_str(),
                vec![("max_bytes", query.max_bytes.map(|value| value.to_string()))],
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn observe_browser_session(
        &self,
        session_id: &str,
        query: &BrowserObserveQuery,
    ) -> Result<BrowserObserveEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path(
                format!("console/v1/browser/sessions/{}/observe", urlencoding(session_id)).as_str(),
                vec![
                    (
                        "include_dom_snapshot",
                        query.include_dom_snapshot.map(|value| value.to_string()),
                    ),
                    (
                        "include_accessibility_tree",
                        query.include_accessibility_tree.map(|value| value.to_string()),
                    ),
                    (
                        "include_visible_text",
                        query.include_visible_text.map(|value| value.to_string()),
                    ),
                    (
                        "max_dom_snapshot_bytes",
                        query.max_dom_snapshot_bytes.map(|value| value.to_string()),
                    ),
                    (
                        "max_accessibility_tree_bytes",
                        query.max_accessibility_tree_bytes.map(|value| value.to_string()),
                    ),
                    (
                        "max_visible_text_bytes",
                        query.max_visible_text_bytes.map(|value| value.to_string()),
                    ),
                ],
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn get_browser_network_log(
        &self,
        session_id: &str,
        query: &BrowserNetworkLogQuery,
    ) -> Result<BrowserNetworkLogEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path(
                format!("console/v1/browser/sessions/{}/network-log", urlencoding(session_id))
                    .as_str(),
                vec![
                    ("limit", query.limit.map(|value| value.to_string())),
                    ("include_headers", query.include_headers.map(|value| value.to_string())),
                    ("max_payload_bytes", query.max_payload_bytes.map(|value| value.to_string())),
                ],
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn get_browser_console_log(
        &self,
        session_id: &str,
        query: &BrowserConsoleLogQuery,
    ) -> Result<BrowserConsoleLogEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path(
                format!("console/v1/browser/sessions/{}/console", urlencoding(session_id)).as_str(),
                vec![
                    ("limit", query.limit.map(|value| value.to_string())),
                    (
                        "minimum_severity",
                        query.minimum_severity.map(|value| {
                            serde_json::to_string(&value)
                                .unwrap_or_default()
                                .trim_matches('"')
                                .to_owned()
                        }),
                    ),
                    (
                        "include_page_diagnostics",
                        query.include_page_diagnostics.map(|value| value.to_string()),
                    ),
                    ("max_payload_bytes", query.max_payload_bytes.map(|value| value.to_string())),
                ],
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn list_browser_tabs(
        &self,
        session_id: &str,
    ) -> Result<BrowserTabListEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/browser/sessions/{}/tabs", urlencoding(session_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn open_browser_tab(
        &self,
        session_id: &str,
        request: &BrowserOpenTabRequest,
    ) -> Result<BrowserOpenTabEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/sessions/{}/tabs/open", urlencoding(session_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn switch_browser_tab(
        &self,
        session_id: &str,
        request: &BrowserTabMutationRequest,
    ) -> Result<BrowserSwitchTabEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/sessions/{}/tabs/switch", urlencoding(session_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn close_browser_tab(
        &self,
        session_id: &str,
        request: &BrowserTabCloseRequest,
    ) -> Result<BrowserCloseTabEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/sessions/{}/tabs/close", urlencoding(session_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn get_browser_permissions(
        &self,
        session_id: &str,
    ) -> Result<BrowserPermissionsEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/browser/sessions/{}/permissions", urlencoding(session_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn set_browser_permissions(
        &self,
        session_id: &str,
        request: &BrowserSetPermissionsRequest,
    ) -> Result<BrowserPermissionsEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/sessions/{}/permissions", urlencoding(session_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn reset_browser_state(
        &self,
        session_id: &str,
        request: &BrowserResetStateRequest,
    ) -> Result<BrowserResetStateEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/browser/sessions/{}/reset-state", urlencoding(session_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn get_diagnostics(&self) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/diagnostics", None::<&Value>, false).await
    }

    pub async fn list_session_catalog(
        &self,
        query: Vec<(&str, Option<String>)>,
    ) -> Result<SessionCatalogListEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path("console/v1/sessions", query),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn get_session_catalog_entry(
        &self,
        session_id: &str,
    ) -> Result<SessionCatalogDetailEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/sessions/{}", urlencoding(session_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn update_session_quick_controls(
        &self,
        session_id: &str,
        request: &SessionQuickControlsUpdateRequest,
    ) -> Result<SessionCatalogMutationEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/sessions/{}/quick-controls", urlencoding(session_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn get_json_value(
        &self,
        path: impl AsRef<str>,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::GET, path, None::<&Value>, false).await
    }

    pub async fn post_json_value<T: Serialize + ?Sized>(
        &self,
        path: impl AsRef<str>,
        request: &T,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, path, Some(request), true).await
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

    pub async fn get_onboarding_posture(
        &self,
        query: Vec<(&str, Option<String>)>,
    ) -> Result<OnboardingPostureEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path("console/v1/onboarding/posture", query),
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

    pub async fn plan_config_reload(
        &self,
        request: &ConfigReloadPlanRequest,
    ) -> Result<ConfigReloadPlanEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/config/reload/plan", Some(request), false).await
    }

    pub async fn apply_config_reload(
        &self,
        request: &ConfigReloadApplyRequest,
    ) -> Result<ConfigReloadApplyEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/config/reload/apply", Some(request), true).await
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

    pub async fn list_configured_secrets(
        &self,
    ) -> Result<ConfiguredSecretListEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/secrets/configured", None::<&Value>, false).await
    }

    pub async fn get_configured_secret(
        &self,
        secret_id: &str,
    ) -> Result<ConfiguredSecretEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/secrets/configured/detail?secret_id={}", urlencoding(secret_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn list_webhooks(
        &self,
        query: &str,
    ) -> Result<WebhookIntegrationListEnvelope, ControlPlaneClientError> {
        let path = if query.trim().is_empty() {
            "console/v1/webhooks".to_owned()
        } else {
            format!("console/v1/webhooks?{query}")
        };
        self.request_json(Method::GET, path, None::<&Value>, false).await
    }

    pub async fn get_webhook(
        &self,
        integration_id: &str,
    ) -> Result<WebhookIntegrationEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/webhooks/{}", urlencoding(integration_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn upsert_webhook(
        &self,
        request: &WebhookIntegrationUpsertRequest,
    ) -> Result<WebhookIntegrationEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/webhooks", Some(request), true).await
    }

    pub async fn set_webhook_enabled(
        &self,
        integration_id: &str,
        request: &WebhookIntegrationEnabledRequest,
    ) -> Result<WebhookIntegrationEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/webhooks/{}/enabled", urlencoding(integration_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn delete_webhook(
        &self,
        integration_id: &str,
    ) -> Result<WebhookIntegrationDeleteEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/webhooks/{}/delete", urlencoding(integration_id)),
            Some(&serde_json::json!({})),
            true,
        )
        .await
    }

    pub async fn test_webhook(
        &self,
        integration_id: &str,
        request: &WebhookIntegrationTestRequest,
    ) -> Result<WebhookIntegrationTestEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/webhooks/{}/test", urlencoding(integration_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn list_plugins(
        &self,
        query: &PluginBindingsQuery,
    ) -> Result<PluginBindingListEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path(
                "console/v1/plugins",
                vec![("plugin_id", query.plugin_id.clone()), ("skill_id", query.skill_id.clone())],
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn get_plugin(
        &self,
        plugin_id: &str,
    ) -> Result<PluginBindingEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/plugins/{}", urlencoding(plugin_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn upsert_plugin(
        &self,
        request: &PluginBindingUpsertRequest,
    ) -> Result<PluginBindingEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/plugins/install-or-bind", Some(request), true)
            .await
    }

    pub async fn check_plugin(
        &self,
        plugin_id: &str,
    ) -> Result<PluginBindingEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/plugins/{}/check", urlencoding(plugin_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn enable_plugin(
        &self,
        plugin_id: &str,
    ) -> Result<PluginBindingEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/plugins/{}/enable", urlencoding(plugin_id)),
            Some(&serde_json::json!({})),
            true,
        )
        .await
    }

    pub async fn disable_plugin(
        &self,
        plugin_id: &str,
    ) -> Result<PluginBindingEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/plugins/{}/disable", urlencoding(plugin_id)),
            Some(&serde_json::json!({})),
            true,
        )
        .await
    }

    pub async fn delete_plugin(
        &self,
        plugin_id: &str,
    ) -> Result<PluginBindingDeleteEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/plugins/{}/delete", urlencoding(plugin_id)),
            None::<&Value>,
            true,
        )
        .await
    }

    pub async fn list_hooks(
        &self,
        query: &HookBindingsQuery,
    ) -> Result<HookBindingListEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path(
                "console/v1/hooks",
                vec![
                    ("hook_id", query.hook_id.clone()),
                    ("plugin_id", query.plugin_id.clone()),
                    ("event", query.event.clone()),
                ],
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn get_hook(
        &self,
        hook_id: &str,
    ) -> Result<HookBindingEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/hooks/{}", urlencoding(hook_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn upsert_hook(
        &self,
        request: &HookBindingUpsertRequest,
    ) -> Result<HookBindingEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/hooks/bind", Some(request), true).await
    }

    pub async fn check_hook(
        &self,
        hook_id: &str,
    ) -> Result<HookBindingEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/hooks/{}/check", urlencoding(hook_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn enable_hook(
        &self,
        hook_id: &str,
    ) -> Result<HookBindingEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/hooks/{}/enable", urlencoding(hook_id)),
            Some(&serde_json::json!({})),
            true,
        )
        .await
    }

    pub async fn disable_hook(
        &self,
        hook_id: &str,
    ) -> Result<HookBindingEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/hooks/{}/disable", urlencoding(hook_id)),
            Some(&serde_json::json!({})),
            true,
        )
        .await
    }

    pub async fn delete_hook(
        &self,
        hook_id: &str,
    ) -> Result<HookBindingDeleteEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/hooks/{}/delete", urlencoding(hook_id)),
            None::<&Value>,
            true,
        )
        .await
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

    pub async fn decide_approval(
        &self,
        approval_id: &str,
        request: &ApprovalDecisionRequest,
    ) -> Result<ApprovalDecisionEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/approvals/{}/decision", urlencoding(approval_id)),
            Some(request),
            true,
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

    pub async fn get_access_snapshot(&self) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/access", None::<&Value>, false).await
    }

    pub async fn run_access_backfill(
        &self,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/access/backfill", Some(request), true).await
    }

    pub async fn set_access_feature_flag(
        &self,
        feature_key: &str,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/access/features/{}", urlencoding(feature_key)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn list_access_api_tokens(&self) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/access/api-tokens", None::<&Value>, false).await
    }

    pub async fn create_access_api_token(
        &self,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/access/api-tokens", Some(request), true).await
    }

    pub async fn rotate_access_api_token(
        &self,
        token_id: &str,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/access/api-tokens/{}/rotate", urlencoding(token_id)),
            Some(&serde_json::json!({})),
            true,
        )
        .await
    }

    pub async fn revoke_access_api_token(
        &self,
        token_id: &str,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/access/api-tokens/{}/revoke", urlencoding(token_id)),
            Some(&serde_json::json!({})),
            true,
        )
        .await
    }

    pub async fn create_access_workspace(
        &self,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/access/workspaces", Some(request), true).await
    }

    pub async fn create_access_invitation(
        &self,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/access/invitations", Some(request), true).await
    }

    pub async fn accept_access_invitation(
        &self,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/access/invitations/accept", Some(request), true)
            .await
    }

    pub async fn update_access_membership_role(
        &self,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/access/memberships/role", Some(request), true)
            .await
    }

    pub async fn remove_access_membership(
        &self,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/access/memberships/remove", Some(request), true)
            .await
    }

    pub async fn upsert_access_share(
        &self,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/access/shares", Some(request), true).await
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

    pub async fn list_node_pairing_requests(
        &self,
        query: Option<&NodePairingListQuery>,
    ) -> Result<NodePairingListEnvelope, ControlPlaneClientError> {
        let path = if let Some(query) = query {
            let mut pairs = Vec::new();
            if let Some(client_kind) = query.client_kind.as_deref() {
                pairs.push(format!("client_kind={}", urlencoding(client_kind)));
            }
            if let Some(state) = query.state {
                let state = serde_json::to_string(&state)
                    .unwrap_or_else(|_| "\"pending_approval\"".to_owned())
                    .trim_matches('"')
                    .to_owned();
                pairs.push(format!("state={}", urlencoding(state.as_str())));
            }
            if pairs.is_empty() {
                "console/v1/pairing/requests".to_owned()
            } else {
                format!("console/v1/pairing/requests?{}", pairs.join("&"))
            }
        } else {
            "console/v1/pairing/requests".to_owned()
        };
        self.request_json(Method::GET, path, None::<&Value>, false).await
    }

    pub async fn mint_node_pairing_code(
        &self,
        request: &NodePairingCodeMintRequest,
    ) -> Result<NodePairingCodeEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/pairing/requests/code", Some(request), true)
            .await
    }

    pub async fn approve_node_pairing_request(
        &self,
        request_id: &str,
        request: &NodePairingDecisionRequest,
    ) -> Result<NodePairingRequestEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/pairing/requests/{}/approve", urlencoding(request_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn reject_node_pairing_request(
        &self,
        request_id: &str,
        request: &NodePairingDecisionRequest,
    ) -> Result<NodePairingRequestEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/pairing/requests/{}/reject", urlencoding(request_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn list_logs(
        &self,
        query: &LogListQuery,
    ) -> Result<LogListEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path(
                "console/v1/logs",
                vec![
                    ("limit", query.limit.map(|value| value.to_string())),
                    ("cursor", query.cursor.clone()),
                    ("direction", query.direction.clone()),
                    ("source", query.source.clone()),
                    ("severity", query.severity.clone()),
                    ("contains", query.contains.clone()),
                    ("start_at_unix_ms", query.start_at_unix_ms.map(|value| value.to_string())),
                    ("end_at_unix_ms", query.end_at_unix_ms.map(|value| value.to_string())),
                ],
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn list_inventory(&self) -> Result<InventoryListEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/inventory", None::<&Value>, false).await
    }

    pub async fn get_inventory_device(
        &self,
        device_id: &str,
    ) -> Result<InventoryDeviceDetailEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/inventory/{}", urlencoding(device_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn list_devices(&self) -> Result<DeviceListEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/devices", None::<&Value>, false).await
    }

    pub async fn get_device(
        &self,
        device_id: &str,
    ) -> Result<DeviceEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/devices/{}", urlencoding(device_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn rotate_device(
        &self,
        device_id: &str,
    ) -> Result<DeviceEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/devices/{}/rotate", urlencoding(device_id)),
            None::<&Value>,
            true,
        )
        .await
    }

    pub async fn revoke_device(
        &self,
        device_id: &str,
        request: &DeviceActionRequest,
    ) -> Result<DeviceEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/devices/{}/revoke", urlencoding(device_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn remove_device(
        &self,
        device_id: &str,
        request: &DeviceActionRequest,
    ) -> Result<DeviceEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/devices/{}/remove", urlencoding(device_id)),
            Some(request),
            true,
        )
        .await
    }

    pub async fn clear_devices(
        &self,
        request: &DeviceClearRequest,
    ) -> Result<DeviceClearEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/devices/clear", Some(request), true).await
    }

    pub async fn list_nodes(&self) -> Result<NodeListEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/nodes", None::<&Value>, false).await
    }

    pub async fn list_pending_nodes(
        &self,
    ) -> Result<NodePairingListEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/nodes/pending", None::<&Value>, false).await
    }

    pub async fn get_node(&self, device_id: &str) -> Result<NodeEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/nodes/{}", urlencoding(device_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    pub async fn invoke_node(
        &self,
        device_id: &str,
        request: &NodeInvokeRequest,
    ) -> Result<NodeInvokeEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/nodes/{}/invoke", urlencoding(device_id)),
            Some(request),
            true,
        )
        .await
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

    pub async fn list_doctor_recovery_jobs(
        &self,
    ) -> Result<DoctorRecoveryJobListEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/doctor/jobs", None::<&Value>, false).await
    }

    pub async fn create_doctor_recovery_job(
        &self,
        request: &DoctorRecoveryCreateRequest,
    ) -> Result<DoctorRecoveryJobEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/doctor/jobs", Some(request), true).await
    }

    pub async fn get_doctor_recovery_job(
        &self,
        job_id: &str,
    ) -> Result<DoctorRecoveryJobEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/doctor/jobs/{}", urlencoding(job_id)),
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

fn build_query_path(path: &str, pairs: Vec<(&str, Option<String>)>) -> String {
    let query = pairs
        .into_iter()
        .filter_map(|(key, value)| {
            value
                .map(|candidate| candidate.trim().to_owned())
                .filter(|candidate| !candidate.is_empty())
                .map(|candidate| format!("{key}={}", urlencoding(candidate.as_str())))
        })
        .collect::<Vec<_>>()
        .join("&");
    if query.is_empty() {
        path.to_owned()
    } else {
        format!("{path}?{query}")
    }
}
