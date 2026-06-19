//! Async HTTP client for the `/console/v1` control-plane API.
//!
//! Wraps `reqwest` with cookie-based session state, CSRF-token propagation for
//! mutating endpoints, and bounded retries for safe (GET) reads. Each public
//! method maps one-to-one onto a daemon console endpoint and decodes into the
//! typed envelopes from `crate::models`.

use std::time::Duration;

use reqwest::{Client, Method, Url};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::errors::{ControlPlaneClientError, ErrorEnvelope};
use crate::models::*;
use crate::transport::{fallback_error_message, urlencoding};

const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_SAFE_READ_RETRIES: usize = 1;

/// Configuration for constructing a [`ControlPlaneClient`].
#[derive(Debug, Clone)]
pub struct ControlPlaneClientConfig {
    /// Base URL of the daemon console API, e.g. `http://127.0.0.1:8787/`.
    pub base_url: String,
    /// Per-request timeout applied to the underlying HTTP client.
    pub request_timeout: Duration,
    /// Extra attempts for GET requests after transport failures (HTTP errors are
    /// never retried).
    pub safe_read_retries: usize,
}

impl ControlPlaneClientConfig {
    /// Creates a config with the default timeout (10 s) and one safe-read retry.
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            request_timeout: Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
            safe_read_retries: DEFAULT_SAFE_READ_RETRIES,
        }
    }
}

/// Asynchronous client for the daemon's `/console/v1` API.
///
/// Holds the session cookie store plus the CSRF token captured by
/// [`get_session`](Self::get_session) or [`login`](Self::login). All request
/// methods share one error contract: [`ControlPlaneClientError::Transport`]
/// when the request cannot be sent, [`ControlPlaneClientError::Http`] for
/// non-success statuses (with the parsed [`ErrorEnvelope`] when available), and
/// [`ControlPlaneClientError::Decode`] when the response body does not match
/// the expected envelope. GET requests are retried immediately, without
/// backoff, up to `safe_read_retries` extra times on transport failures only.
// INTENTIONAL: no `Debug` derive — `csrf_token` is a session credential and a
// derived impl would leak it into logs.
#[derive(Clone)]
pub struct ControlPlaneClient {
    base_url: Url,
    client: Client,
    csrf_token: Option<String>,
    safe_read_retries: usize,
}

impl ControlPlaneClient {
    /// Creates a client with a cookie-enabled HTTP client and the configured timeout.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError::ClientInit`] if the HTTP client cannot
    /// be built, or [`ControlPlaneClientError::InvalidBaseUrl`] if the base URL
    /// does not parse.
    pub fn new(config: ControlPlaneClientConfig) -> Result<Self, ControlPlaneClientError> {
        let client = Client::builder()
            .cookie_store(true)
            .timeout(config.request_timeout)
            .build()
            .map_err(|error| ControlPlaneClientError::ClientInit(error.to_string()))?;
        Self::with_client(config, client)
    }

    /// Creates a client on top of a caller-provided `reqwest` client.
    ///
    /// The base URL path is normalized to end with `/` so endpoint paths join
    /// under it instead of replacing its final segment.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError::InvalidBaseUrl`] if the base URL does
    /// not parse.
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

    /// Overrides the CSRF token sent with mutating requests (`None` clears it).
    pub fn set_csrf_token(&mut self, csrf_token: Option<String>) {
        self.csrf_token = csrf_token;
    }

    /// Fetches the current session via `GET console/v1/auth/session` and stores
    /// its CSRF token for subsequent mutations.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn get_session(&mut self) -> Result<ConsoleSession, ControlPlaneClientError> {
        let session: ConsoleSession = self
            .request_json(Method::GET, "console/v1/auth/session", None::<&Value>, false)
            .await?;
        self.csrf_token = Some(session.csrf_token.clone());
        Ok(session)
    }

    /// Logs in via `POST console/v1/auth/login` and stores the session's CSRF
    /// token for subsequent mutations.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn login(
        &mut self,
        request: &ConsoleLoginRequest,
    ) -> Result<ConsoleSession, ControlPlaneClientError> {
        let session: ConsoleSession =
            self.request_json(Method::POST, "console/v1/auth/login", Some(request), false).await?;
        self.csrf_token = Some(session.csrf_token.clone());
        Ok(session)
    }

    /// Creates a one-shot browser handoff URL via `POST console/v1/auth/browser-handoff`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn create_browser_handoff(
        &self,
        request: &ConsoleBrowserHandoffRequest,
    ) -> Result<ConsoleBrowserHandoffEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/auth/browser-handoff", Some(request), true)
            .await
    }

    /// Fetches the mobile companion bootstrap contract via `GET console/v1/mobile/bootstrap`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn get_mobile_bootstrap(
        &self,
    ) -> Result<MobileBootstrapEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/mobile/bootstrap", None::<&Value>, false).await
    }

    /// Fetches the mobile inbox via `GET console/v1/mobile/inbox`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn get_mobile_inbox(&self) -> Result<MobileInboxEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/mobile/inbox", None::<&Value>, false).await
    }

    /// Lists pending mobile approvals via `GET console/v1/mobile/approvals`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Fetches one mobile approval with explainability.
    ///
    /// Calls `GET console/v1/mobile/approvals/{approval_id}`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Decides a mobile approval via `POST console/v1/mobile/approvals/{approval_id}/decision`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Lists recent sessions for the mobile companion via `GET console/v1/mobile/sessions`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Fetches one mobile session detail via `GET console/v1/mobile/sessions/{session_id}`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Prepares a mediated safe-URL open via `POST console/v1/mobile/safe-url-open`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn prepare_mobile_safe_url_open(
        &self,
        request: &MobileSafeUrlOpenRequest,
    ) -> Result<MobileSafeUrlOpenEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/mobile/safe-url-open", Some(request), true)
            .await
    }

    /// Creates a voice note via `POST console/v1/mobile/voice-notes`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn create_mobile_voice_note(
        &self,
        request: &MobileVoiceNoteCreateRequest,
    ) -> Result<MobileVoiceNoteEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/mobile/voice-notes", Some(request), true).await
    }

    /// Lists browser profiles via `GET console/v1/browser/profiles`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Creates a browser profile via `POST console/v1/browser/profiles/create`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn create_browser_profile(
        &self,
        request: &BrowserCreateProfileRequest,
    ) -> Result<BrowserProfileEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/browser/profiles/create", Some(request), true)
            .await
    }

    /// Renames a browser profile via `POST console/v1/browser/profiles/{profile_id}/rename`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Deletes a browser profile via `POST console/v1/browser/profiles/{profile_id}/delete`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Activates a browser profile via `POST console/v1/browser/profiles/{profile_id}/activate`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Lists download artifacts of a browser session via `GET console/v1/browser/downloads`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Creates a browser session via `POST console/v1/browser/sessions`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn create_browser_session(
        &self,
        request: &BrowserSessionCreateRequest,
    ) -> Result<BrowserSessionCreateEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/browser/sessions", Some(request), true).await
    }

    /// Closes a browser session via `POST console/v1/browser/sessions/{session_id}/close`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Navigates a browser session via `POST console/v1/browser/sessions/{session_id}/navigate`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Clicks an element in a browser session.
    ///
    /// Calls `POST console/v1/browser/sessions/{session_id}/click`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Types text into a browser session via `POST console/v1/browser/sessions/{session_id}/type`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Presses a key in a browser session.
    ///
    /// Calls `POST console/v1/browser/sessions/{session_id}/press`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Selects an option in a browser session.
    ///
    /// Calls `POST console/v1/browser/sessions/{session_id}/select`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Highlights an element in a browser session.
    ///
    /// Calls `POST console/v1/browser/sessions/{session_id}/highlight`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Scrolls a browser session via `POST console/v1/browser/sessions/{session_id}/scroll`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Waits for a selector or text in a browser session.
    ///
    /// Calls `POST console/v1/browser/sessions/{session_id}/wait-for`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Reads the page title of a browser session.
    ///
    /// Calls `GET console/v1/browser/sessions/{session_id}/title`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Captures a screenshot of a browser session.
    ///
    /// Calls `GET console/v1/browser/sessions/{session_id}/screenshot`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Renders a browser session page to PDF.
    ///
    /// Calls `GET console/v1/browser/sessions/{session_id}/pdf`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Observes DOM, accessibility, and visible text of a browser session.
    ///
    /// Calls `GET console/v1/browser/sessions/{session_id}/observe`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Reads the network log of a browser session.
    ///
    /// Calls `GET console/v1/browser/sessions/{session_id}/network-log`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Reads the console log of a browser session.
    ///
    /// Calls `GET console/v1/browser/sessions/{session_id}/console`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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
                        // Round-trip through serde_json so the query value always
                        // matches the enum's snake_case wire encoding.
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

    /// Lists tabs of a browser session via `GET console/v1/browser/sessions/{session_id}/tabs`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Opens a tab in a browser session.
    ///
    /// Calls `POST console/v1/browser/sessions/{session_id}/tabs/open`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Switches the active tab of a browser session.
    ///
    /// Calls `POST console/v1/browser/sessions/{session_id}/tabs/switch`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Closes a tab of a browser session.
    ///
    /// Calls `POST console/v1/browser/sessions/{session_id}/tabs/close`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Reads permissions of a browser session.
    ///
    /// Calls `GET console/v1/browser/sessions/{session_id}/permissions`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Updates permissions of a browser session.
    ///
    /// Calls `POST console/v1/browser/sessions/{session_id}/permissions`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Resets selected state of a browser session.
    ///
    /// Calls `POST console/v1/browser/sessions/{session_id}/reset-state`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Fetches raw daemon diagnostics via `GET console/v1/diagnostics`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn get_diagnostics(&self) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/diagnostics", None::<&Value>, false).await
    }

    /// Lists the session catalog via `GET console/v1/sessions`.
    ///
    /// Caller-supplied query pairs are appended; `None` or blank values are dropped.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Fetches one session catalog entry via `GET console/v1/sessions/{session_id}`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Updates a session's quick controls.
    ///
    /// Calls `POST console/v1/sessions/{session_id}/quick-controls`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Fetches an arbitrary console path and returns the raw JSON value.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn get_json_value(
        &self,
        path: impl AsRef<str>,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::GET, path, None::<&Value>, false).await
    }

    /// Posts a JSON body to an arbitrary console path and returns the raw JSON value.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn post_json_value<T: Serialize + ?Sized>(
        &self,
        path: impl AsRef<str>,
        request: &T,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, path, Some(request), true).await
    }

    /// Fetches the deployment posture summary via `GET console/v1/deployment/posture`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn get_deployment_posture(
        &self,
    ) -> Result<DeploymentPostureSummary, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/deployment/posture", None::<&Value>, false).await
    }

    /// Fetches the capability catalog via `GET console/v1/control-plane/capabilities`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Fetches onboarding posture via `GET console/v1/onboarding/posture`.
    ///
    /// Caller-supplied query pairs are appended; `None` or blank values are dropped.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Inspects the config document via `POST console/v1/config/inspect`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn inspect_config(
        &self,
        request: &ConfigInspectRequest,
    ) -> Result<ConfigDocumentSnapshot, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/config/inspect", Some(request), false).await
    }

    /// Validates the config document via `POST console/v1/config/validate`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn validate_config(
        &self,
        request: &ConfigValidateRequest,
    ) -> Result<ConfigValidationEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/config/validate", Some(request), false).await
    }

    /// Mutates one config key via `POST console/v1/config/mutate`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn mutate_config(
        &self,
        request: &ConfigMutationRequest,
    ) -> Result<ConfigMutationEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/config/mutate", Some(request), true).await
    }

    /// Migrates the config document to the current version via `POST console/v1/config/migrate`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn migrate_config(
        &self,
        request: &ConfigInspectRequest,
    ) -> Result<ConfigMutationEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/config/migrate", Some(request), true).await
    }

    /// Restores the config from a numbered backup via `POST console/v1/config/recover`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn recover_config(
        &self,
        request: &ConfigRecoverRequest,
    ) -> Result<ConfigMutationEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/config/recover", Some(request), true).await
    }

    /// Plans a config reload via `POST console/v1/config/reload/plan`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn plan_config_reload(
        &self,
        request: &ConfigReloadPlanRequest,
    ) -> Result<ConfigReloadPlanEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/config/reload/plan", Some(request), false).await
    }

    /// Applies a config reload plan via `POST console/v1/config/reload/apply`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn apply_config_reload(
        &self,
        request: &ConfigReloadApplyRequest,
    ) -> Result<ConfigReloadApplyEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/config/reload/apply", Some(request), true).await
    }

    /// Lists secret metadata in a scope via `GET console/v1/secrets`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Fetches metadata for one secret via `GET console/v1/secrets/metadata`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Stores a secret value via `POST console/v1/secrets`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn set_secret(
        &self,
        request: &SecretSetRequest,
    ) -> Result<SecretMetadataEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/secrets", Some(request), true).await
    }

    /// Reveals a secret value via `POST console/v1/secrets/reveal`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn reveal_secret(
        &self,
        request: &SecretRevealRequest,
    ) -> Result<SecretRevealEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/secrets/reveal", Some(request), true).await
    }

    /// Deletes a secret via `POST console/v1/secrets/delete`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn delete_secret(
        &self,
        request: &SecretDeleteRequest,
    ) -> Result<SecretMetadataEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/secrets/delete", Some(request), true).await
    }

    /// Lists secrets configured in the daemon config via `GET console/v1/secrets/configured`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn list_configured_secrets(
        &self,
    ) -> Result<ConfiguredSecretListEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/secrets/configured", None::<&Value>, false).await
    }

    /// Fetches one configured secret via `GET console/v1/secrets/configured/detail`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Lists webhook integrations via `GET console/v1/webhooks`.
    ///
    /// `query` is appended verbatim as the raw query string when non-blank.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Fetches one webhook integration via `GET console/v1/webhooks/{integration_id}`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Creates or updates a webhook integration via `POST console/v1/webhooks`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn upsert_webhook(
        &self,
        request: &WebhookIntegrationUpsertRequest,
    ) -> Result<WebhookIntegrationEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/webhooks", Some(request), true).await
    }

    /// Enables or disables a webhook integration.
    ///
    /// Calls `POST console/v1/webhooks/{integration_id}/enabled`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Deletes a webhook integration via `POST console/v1/webhooks/{integration_id}/delete`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Test-delivers a payload to a webhook integration.
    ///
    /// Calls `POST console/v1/webhooks/{integration_id}/test`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Lists plugin bindings via `GET console/v1/plugins`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Fetches one plugin binding via `GET console/v1/plugins/{plugin_id}`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Installs or binds a plugin via `POST console/v1/plugins/install-or-bind`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn upsert_plugin(
        &self,
        request: &PluginBindingUpsertRequest,
    ) -> Result<PluginBindingEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/plugins/install-or-bind", Some(request), true)
            .await
    }

    /// Re-checks a plugin binding via `GET console/v1/plugins/{plugin_id}/check`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Enables a plugin binding via `POST console/v1/plugins/{plugin_id}/enable`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Disables a plugin binding via `POST console/v1/plugins/{plugin_id}/disable`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Deletes a plugin binding via `POST console/v1/plugins/{plugin_id}/delete`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Lists hook bindings via `GET console/v1/hooks`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Fetches one hook binding via `GET console/v1/hooks/{hook_id}`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Creates or updates a hook binding via `POST console/v1/hooks/bind`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn upsert_hook(
        &self,
        request: &HookBindingUpsertRequest,
    ) -> Result<HookBindingEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/hooks/bind", Some(request), true).await
    }

    /// Re-checks a hook binding via `GET console/v1/hooks/{hook_id}/check`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Enables a hook binding via `POST console/v1/hooks/{hook_id}/enable`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Disables a hook binding via `POST console/v1/hooks/{hook_id}/disable`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Deletes a hook binding via `POST console/v1/hooks/{hook_id}/delete`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Lists auth profiles via `GET console/v1/auth/profiles`.
    ///
    /// `query` is appended verbatim as the raw query string when non-blank.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Fetches one auth profile via `GET console/v1/auth/profiles/{profile_id}`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Decides an approval via `POST console/v1/approvals/{approval_id}/decision`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Creates or updates an auth profile via `POST console/v1/auth/profiles`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn upsert_auth_profile(
        &self,
        profile: &AuthProfileView,
    ) -> Result<AuthProfileEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/auth/profiles", Some(profile), true).await
    }

    /// Deletes an auth profile via `POST console/v1/auth/profiles/{profile_id}/delete`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Fetches auth health via `GET console/v1/auth/health`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Fetches the raw auth doctor report via `GET console/v1/auth/doctor`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn get_auth_doctor(
        &self,
        agent_id: Option<&str>,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path(
                "console/v1/auth/doctor",
                vec![("agent_id", agent_id.map(str::to_owned))],
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    /// Fetches the raw auth audit report via `GET console/v1/auth/audit`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn get_auth_audit(
        &self,
        agent_id: Option<&str>,
        provider_kind: Option<&str>,
        provider_custom_name: Option<&str>,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            build_query_path(
                "console/v1/auth/audit",
                vec![
                    ("agent_id", agent_id.map(str::to_owned)),
                    ("provider_kind", provider_kind.map(str::to_owned)),
                    ("provider_custom_name", provider_custom_name.map(str::to_owned)),
                ],
            ),
            None::<&Value>,
            false,
        )
        .await
    }

    /// Clears an auth profile's cooldown.
    ///
    /// Calls `POST console/v1/auth/profiles/{profile_id}/cooldown/clear`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn clear_auth_profile_cooldown(
        &self,
        profile_id: &str,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/auth/profiles/{}/cooldown/clear", urlencoding(profile_id)),
            Some(&serde_json::json!({})),
            true,
        )
        .await
    }

    /// Sets the auth profile selection order via `POST console/v1/auth/profile-order`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn set_auth_profile_order<T: Serialize + ?Sized>(
        &self,
        request: &T,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/auth/profile-order", Some(request), true).await
    }

    /// Explains which auth profile would be selected via `POST console/v1/auth/selection/explain`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn explain_auth_profile_selection<T: Serialize + ?Sized>(
        &self,
        request: &T,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/auth/selection/explain", Some(request), false)
            .await
    }

    /// Tests model-provider connectivity via `POST console/v1/models/test-connection`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn test_model_provider_connection<T: Serialize + ?Sized>(
        &self,
        request: &T,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/models/test-connection", Some(request), true)
            .await
    }

    /// Discovers live model-provider models via `POST console/v1/models/discover`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn discover_model_provider_models<T: Serialize + ?Sized>(
        &self,
        request: &T,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/models/discover", Some(request), true).await
    }

    /// Fetches provider auth state via `GET console/v1/auth/providers/{provider}`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn get_provider_auth_state(
        &self,
        provider: &str,
    ) -> Result<ProviderAuthStateEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/auth/providers/{}", urlencoding(provider)),
            None::<&Value>,
            false,
        )
        .await
    }

    /// Stores a provider API key via `POST console/v1/auth/providers/{provider}/api-key`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn connect_provider_api_key(
        &self,
        provider: &str,
        request: &ProviderApiKeyUpsertRequest,
    ) -> Result<ProviderAuthActionEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/auth/providers/{}/api-key", urlencoding(provider)),
            Some(request),
            true,
        )
        .await
    }

    /// Stores provider OAuth tokens via `POST console/v1/auth/providers/{provider}/oauth-token`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn connect_provider_oauth_tokens(
        &self,
        provider: &str,
        request: &ProviderOAuthTokenUpsertRequest,
    ) -> Result<ProviderAuthActionEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/auth/providers/{}/oauth-token", urlencoding(provider)),
            Some(request),
            true,
        )
        .await
    }

    /// Runs a named provider auth action via `POST console/v1/auth/providers/{provider}/{action}`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn run_provider_auth_action(
        &self,
        provider: &str,
        action: &str,
        request: &ProviderAuthActionRequest,
    ) -> Result<ProviderAuthActionEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            format!("console/v1/auth/providers/{}/{}", urlencoding(provider), urlencoding(action)),
            Some(request),
            true,
        )
        .await
    }

    /// Fetches the OpenAI provider auth state via `GET console/v1/auth/providers/openai`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn get_openai_provider_state(
        &self,
    ) -> Result<ProviderAuthStateEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/auth/providers/openai", None::<&Value>, false)
            .await
    }

    /// Stores an OpenAI API key via `POST console/v1/auth/providers/openai/api-key`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Starts the OpenAI OAuth bootstrap flow.
    ///
    /// Calls `POST console/v1/auth/providers/openai/bootstrap`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Polls OpenAI OAuth callback state via `GET console/v1/auth/providers/openai/callback-state`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Restarts the OpenAI OAuth flow for an existing profile.
    ///
    /// Calls `POST console/v1/auth/providers/openai/reconnect`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn reconnect_openai_oauth(
        &self,
        request: &ProviderAuthActionRequest,
    ) -> Result<OpenAiOAuthBootstrapEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::POST,
            "console/v1/auth/providers/openai/reconnect",
            Some(request),
            true,
        )
        .await
    }

    /// Runs a named OpenAI provider action via `POST console/v1/auth/providers/openai/{action}`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Fetches the raw access-control snapshot via `GET console/v1/access`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn get_access_snapshot(&self) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/access", None::<&Value>, false).await
    }

    /// Runs an access-control backfill via `POST console/v1/access/backfill`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn run_access_backfill(
        &self,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/access/backfill", Some(request), true).await
    }

    /// Sets an access feature flag via `POST console/v1/access/features/{feature_key}`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Lists access API tokens via `GET console/v1/access/api-tokens`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn list_access_api_tokens(&self) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/access/api-tokens", None::<&Value>, false).await
    }

    /// Creates an access API token via `POST console/v1/access/api-tokens`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn create_access_api_token(
        &self,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/access/api-tokens", Some(request), true).await
    }

    /// Rotates an access API token via `POST console/v1/access/api-tokens/{token_id}/rotate`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Revokes an access API token via `POST console/v1/access/api-tokens/{token_id}/revoke`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Creates a workspace via `POST console/v1/access/workspaces`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn create_access_workspace(
        &self,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/access/workspaces", Some(request), true).await
    }

    /// Creates a workspace invitation via `POST console/v1/access/invitations`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn create_access_invitation(
        &self,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/access/invitations", Some(request), true).await
    }

    /// Accepts a workspace invitation via `POST console/v1/access/invitations/accept`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn accept_access_invitation(
        &self,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/access/invitations/accept", Some(request), true)
            .await
    }

    /// Updates a membership role via `POST console/v1/access/memberships/role`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn update_access_membership_role(
        &self,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/access/memberships/role", Some(request), true)
            .await
    }

    /// Removes a workspace membership via `POST console/v1/access/memberships/remove`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn remove_access_membership(
        &self,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/access/memberships/remove", Some(request), true)
            .await
    }

    /// Creates or updates an access share via `POST console/v1/access/shares`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn upsert_access_share(
        &self,
        request: &Value,
    ) -> Result<Value, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/access/shares", Some(request), true).await
    }

    /// Fetches the channel pairing summary via `GET console/v1/pairing`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn get_pairing_summary(
        &self,
    ) -> Result<PairingSummaryEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/pairing", None::<&Value>, false).await
    }

    /// Mints a channel pairing code via `POST console/v1/pairing/codes`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn mint_pairing_code(
        &self,
        request: &PairingCodeMintRequest,
    ) -> Result<PairingSummaryEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/pairing/codes", Some(request), true).await
    }

    /// Lists node pairing codes and requests via `GET console/v1/pairing/requests`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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
                // Serialize through serde_json so the query value matches the
                // enum's snake_case wire encoding; the fallback is unreachable
                // for this fieldless enum.
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

    /// Mints a node pairing code via `POST console/v1/pairing/requests/code`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn mint_node_pairing_code(
        &self,
        request: &NodePairingCodeMintRequest,
    ) -> Result<NodePairingCodeEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/pairing/requests/code", Some(request), true)
            .await
    }

    /// Approves a node pairing request via `POST console/v1/pairing/requests/{request_id}/approve`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Rejects a node pairing request via `POST console/v1/pairing/requests/{request_id}/reject`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Lists log records via `GET console/v1/logs`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Fetches the device and instance inventory via `GET console/v1/inventory`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn list_inventory(&self) -> Result<InventoryListEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/inventory", None::<&Value>, false).await
    }

    /// Fetches one inventory device with related activity.
    ///
    /// Calls `GET console/v1/inventory/{device_id}`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Lists paired devices via `GET console/v1/devices`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn list_devices(&self) -> Result<DeviceListEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/devices", None::<&Value>, false).await
    }

    /// Fetches one device via `GET console/v1/devices/{device_id}`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Rotates a device certificate via `POST console/v1/devices/{device_id}/rotate`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Revokes a device via `POST console/v1/devices/{device_id}/revoke`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Removes a device via `POST console/v1/devices/{device_id}/remove`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Clears devices via `POST console/v1/devices/clear`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn clear_devices(
        &self,
        request: &DeviceClearRequest,
    ) -> Result<DeviceClearEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/devices/clear", Some(request), true).await
    }

    /// Lists registered nodes via `GET console/v1/nodes`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn list_nodes(&self) -> Result<NodeListEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/nodes", None::<&Value>, false).await
    }

    /// Lists pending node pairings via `GET console/v1/nodes/pending`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn list_pending_nodes(
        &self,
    ) -> Result<NodePairingListEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/nodes/pending", None::<&Value>, false).await
    }

    /// Fetches one node via `GET console/v1/nodes/{device_id}`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn get_node(&self, device_id: &str) -> Result<NodeEnvelope, ControlPlaneClientError> {
        self.request_json(
            Method::GET,
            format!("console/v1/nodes/{}", urlencoding(device_id)),
            None::<&Value>,
            false,
        )
        .await
    }

    /// Invokes a capability on a node via `POST console/v1/nodes/{device_id}/invoke`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Lists support bundle jobs via `GET console/v1/support-bundle/jobs`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn list_support_bundle_jobs(
        &self,
    ) -> Result<SupportBundleJobListEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/support-bundle/jobs", None::<&Value>, false)
            .await
    }

    /// Creates a support bundle job via `POST console/v1/support-bundle/jobs`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn create_support_bundle_job(
        &self,
        request: &SupportBundleCreateRequest,
    ) -> Result<SupportBundleJobEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/support-bundle/jobs", Some(request), true).await
    }

    /// Fetches one support bundle job via `GET console/v1/support-bundle/jobs/{job_id}`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Lists doctor recovery jobs via `GET console/v1/doctor/jobs`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn list_doctor_recovery_jobs(
        &self,
    ) -> Result<DoctorRecoveryJobListEnvelope, ControlPlaneClientError> {
        self.request_json(Method::GET, "console/v1/doctor/jobs", None::<&Value>, false).await
    }

    /// Creates a doctor recovery job via `POST console/v1/doctor/jobs`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
    pub async fn create_doctor_recovery_job(
        &self,
        request: &DoctorRecoveryCreateRequest,
    ) -> Result<DoctorRecoveryJobEnvelope, ControlPlaneClientError> {
        self.request_json(Method::POST, "console/v1/doctor/jobs", Some(request), true).await
    }

    /// Fetches one doctor recovery job via `GET console/v1/doctor/jobs/{job_id}`.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError`] on transport, HTTP, or response-decode failure.
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

    /// Shared request path: joins `path` onto the base URL, attaches the CSRF
    /// header for mutations, retries GETs on transport errors, and maps
    /// non-success statuses to [`ControlPlaneClientError::Http`].
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
        // Only idempotent reads are retried; mutations get exactly one attempt.
        let mut attempts_remaining =
            if method == Method::GET { self.safe_read_retries + 1 } else { 1 };
        loop {
            let mut request = self.client.request(method.clone(), url.clone());
            if require_csrf {
                // A missing token is not a client-side error: the daemon enforces
                // CSRF and rejects unauthenticated mutations server-side.
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
                        // Prefer the daemon's structured envelope message; fall back
                        // to a bounded slice of the raw body only when the envelope
                        // shape is absent.
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

/// Joins non-empty query pairs onto `path`, percent-encoding values and
/// dropping `None` or blank entries entirely.
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
