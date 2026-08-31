//! Browser tool runtime: brokers `palyra.browser.*` tool calls to the
//! `palyra-browserd` gRPC `BrowserService` (sessions, navigation, DOM
//! actions, observation, tabs, permissions, downloads).
//!
//! Beyond plain RPC mapping this module owns the daemon-side guarantees:
//! JSON payload validation, workspace/OS-root scoping for `file://`
//! navigation, uploads, and saved artifacts, safety redaction of all exported
//! page content (titles, DOM, cookies, logs), engine-capability annotation
//! (Chromium vs simulated static-HTML), and normalization of failures into
//! attested [`ToolExecutionOutcome`]s carrying machine-readable recovery
//! hints. Entry points are [`execute_browser_tool`] (dispatched from
//! `crate::gateway`) and [`close_browser_session_for_run_cleanup`];
//! closed-session bookkeeping lives on [`GatewayRuntimeState`].

use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use palyra_common::{
    derive_browser_principal_token,
    redaction::{redact_header, redact_url},
    validate_canonical_id, CANONICAL_PROTOCOL_MAJOR,
};
use palyra_safety::{
    merge_scan_results, redact_text_for_export, ExportRedactionOutcome, SafetyContentKind,
    SafetyPhase, SafetyScanResult, SafetySourceKind, TrustLabel,
};
use reqwest::Url;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tonic::{Request, Status};
use ulid::Ulid;

use crate::{
    agents::{AgentResolutionSource, AgentResolveRequest},
    application::tool_runtime::{
        os_file::open_regular_file_for_read_under_roots,
        workspace_scope::{
            relative_path_already_targets_active_root, run_launch_context_path_env,
            session_active_workspace_root,
            workspace_roots_with_run_launch_context_for_agent_source, ActiveWorkspaceRoot,
        },
    },
    gateway::{
        current_unix_ms, truncate_with_ellipsis, GatewayRuntimeState, ToolRuntimeExecutionContext,
        BROWSER_CDP_INVOKE_TOOL_NAME, BROWSER_CLICK_TOOL_NAME, BROWSER_CONSOLE_LOG_TOOL_NAME,
        BROWSER_DIALOG_TOOL_NAME, BROWSER_DOWNLOADS_GET_TOOL_NAME,
        BROWSER_DOWNLOADS_LIST_TOOL_NAME, BROWSER_FILL_TOOL_NAME, BROWSER_HIGHLIGHT_TOOL_NAME,
        BROWSER_IMAGES_LIST_TOOL_NAME, BROWSER_NAVIGATE_TOOL_NAME, BROWSER_NETWORK_LOG_TOOL_NAME,
        BROWSER_OBSERVE_TOOL_NAME, BROWSER_PDF_TOOL_NAME, BROWSER_PERMISSIONS_GET_TOOL_NAME,
        BROWSER_PERMISSIONS_SET_TOOL_NAME, BROWSER_PRESS_TOOL_NAME, BROWSER_RELOAD_TOOL_NAME,
        BROWSER_RESET_STATE_TOOL_NAME, BROWSER_SCREENSHOT_TOOL_NAME, BROWSER_SCROLL_TOOL_NAME,
        BROWSER_SELECT_TOOL_NAME, BROWSER_SESSION_CLOSE_TOOL_NAME,
        BROWSER_SESSION_CREATE_TOOL_NAME, BROWSER_STORAGE_TOOL_NAME, BROWSER_TABS_CLOSE_TOOL_NAME,
        BROWSER_TABS_LIST_TOOL_NAME, BROWSER_TABS_OPEN_TOOL_NAME, BROWSER_TABS_SWITCH_TOOL_NAME,
        BROWSER_TITLE_TOOL_NAME, BROWSER_TYPE_TOOL_NAME, BROWSER_UPLOAD_TOOL_NAME,
        BROWSER_VIEWPORT_TOOL_NAME, BROWSER_VISION_TOOL_NAME, BROWSER_WAIT_FOR_TOOL_NAME,
        IMAGE_OBSERVE_TOOL_NAME, MAX_BROWSER_TOOL_INPUT_BYTES,
    },
    tool_protocol::{ToolAttestation, ToolExecutionOutcome},
    transport::grpc::proto::palyra::{browser::v1 as browser_v1, common::v1 as common_v1},
};

/// gRPC metadata key carrying the daemon-side caller principal to browserd.
const BROWSER_CALLER_PRINCIPAL_HEADER: &str = "x-palyra-principal";
// Recovery hints and runtime warnings ride along on tool outcomes so the
// calling agent can self-correct without operator help. Tests and fixtures
// pin the exact wording; treat these strings as frozen.
const BROWSER_SELECTOR_RECOVERY_HINT: &str = "selector_not_found: call palyra.browser.observe with include_dom_snapshot=true and include_accessibility_tree=true, choose a selector from observed ids, names, labels, roles, or visible text, then retry once with that grounded selector; do not keep guessing selectors";
const BROWSER_WAIT_FOR_INPUT_RECOVERY_HINT: &str = "wait_for_input_required: pass either selector or text; when unsure, call palyra.browser.observe first and wait for a visible text snippet or observed selector";
const BROWSER_WAIT_FOR_TIMEOUT_RECOVERY_HINT: &str = "wait_for_timeout: call palyra.browser.observe to inspect the current step/state before retrying with a grounded selector or visible text";
const BROWSER_SESSION_CLOSED_RECOVERY_HINT: &str = "browser_session_closed: create a new browser session with palyra.browser.session.create and retry the browser operation with the new session_id";
const BROWSER_RUNTIME_RECOVERY_HINT: &str = "browser_runtime_unavailable: inspect `palyra browser status`; if browserd was restarted, recreate the browser session and retry the browser operation";
const BROWSER_STATIC_HTML_RUNTIME_WARNING: &str = "simulated_browser_engine_static_html_only: this browserd engine fetches static HTML and does not execute JavaScript, module scripts, app hydration, or subresource-driven UI state; use a Chromium browserd engine before claiming JS UI validation";

fn browser_max_redirects_from_payload(payload: &serde_json::Map<String, Value>) -> u32 {
    payload
        .get("max_redirects")
        .and_then(Value::as_u64)
        .map(|value| u32::try_from(value).unwrap_or(u32::MAX))
        .unwrap_or(3)
}
const BROWSER_UNKNOWN_RUNTIME_WARNING: &str = "browser_runtime_capabilities_unknown: browserd did not report JavaScript/subresource capabilities; do not treat title, URL, or fetched HTML alone as JS UI validation";
const BROWSER_TOOL_INPUT_RECOVERY_HINT: &str = "browser_tool_input_error: inspect the error field, fix the browser tool input or session state, and retry";
/// Hard cap on upload file size; the file body travels inline in the gRPC
/// request, so this also bounds request memory.
const BROWSER_UPLOAD_MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;
/// Default and hard cap for download-artifact bytes fetched from browserd in
/// a single `downloads.get` call.
const BROWSER_DOWNLOAD_TOOL_DEFAULT_MAX_BYTES: u64 = 256 * 1024;
const BROWSER_DOWNLOAD_TOOL_MAX_BYTES: u64 = 512 * 1024;
const BROWSER_VIEWPORT_HEIGHT_TOLERANCE_PX: u32 = 80;
const BROWSER_OBSERVE_MAX_CAPTURE_SELECTORS: usize = 8;
const BROWSER_OBSERVE_MAX_COMPUTED_STYLE_PROPERTIES: usize = 16;
const BROWSER_IMAGES_LIST_DEFAULT_MAX_COUNT: usize = 20;
const BROWSER_IMAGES_LIST_MAX_COUNT: usize = 100;
const BROWSER_IMAGES_LIST_DEFAULT_DOM_BYTES: u64 = 128 * 1024;
const BROWSER_RESCUE_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.browser_rescue";
const BROWSER_VISION_UNSUPPORTED_ERROR: &str = "vision_not_available";
/// Env var listing extra OS roots (split like `PATH`) approved for browser
/// file IO outside agent workspaces.
const PALYRA_OS_FILE_ROOTS_ENV: &str = "PALYRA_OS_FILE_ROOTS";

/// JavaScript/DOM capability report for the browserd engine serving a call.
///
/// Attached to every tool outcome (success or failure) so agents cannot
/// mistake static-HTML fetches from the simulated engine for real Chromium
/// rendering when validating UI behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BrowserRuntimeCapabilities {
    source: &'static str,
    engine_mode: String,
    javascript_execution: Option<bool>,
    subresource_loading: Option<bool>,
    dom_interaction: Option<bool>,
    health_status: String,
    resilience_profile: String,
    automatic_reconnect: Option<bool>,
    degraded_sessions: u32,
    reconnecting_sessions: u32,
    blocked_sessions: u32,
    process_reconnect_count: u64,
    target_reconnect_count: u64,
    dialog_timeout_count: u64,
    warning: Option<&'static str>,
}

impl BrowserRuntimeCapabilities {
    /// Maps a browserd health response; engine modes other than `chromium`
    /// or `simulated` degrade to "unknown" with a validation warning.
    fn from_health(response: &browser_v1::BrowserHealthResponse) -> Self {
        let engine_mode = response.engine_mode.trim().to_ascii_lowercase();
        match engine_mode.as_str() {
            "chromium" => Self {
                source: "browserd.health",
                engine_mode,
                javascript_execution: Some(response.javascript_execution_enabled),
                subresource_loading: Some(response.subresource_loading_enabled),
                dom_interaction: Some(response.dom_interaction_enabled),
                health_status: response.status.clone(),
                resilience_profile: response.resilience_profile.clone(),
                automatic_reconnect: Some(response.automatic_reconnect_enabled),
                degraded_sessions: response.degraded_sessions,
                reconnecting_sessions: response.reconnecting_sessions,
                blocked_sessions: response.blocked_sessions,
                process_reconnect_count: response.process_reconnect_count,
                target_reconnect_count: response.target_reconnect_count,
                dialog_timeout_count: response.dialog_timeout_count,
                warning: None,
            },
            "simulated" => Self {
                source: "browserd.health",
                engine_mode,
                javascript_execution: Some(response.javascript_execution_enabled),
                subresource_loading: Some(response.subresource_loading_enabled),
                dom_interaction: Some(response.dom_interaction_enabled),
                health_status: response.status.clone(),
                resilience_profile: response.resilience_profile.clone(),
                automatic_reconnect: Some(response.automatic_reconnect_enabled),
                degraded_sessions: response.degraded_sessions,
                reconnecting_sessions: response.reconnecting_sessions,
                blocked_sessions: response.blocked_sessions,
                process_reconnect_count: response.process_reconnect_count,
                target_reconnect_count: response.target_reconnect_count,
                dialog_timeout_count: response.dialog_timeout_count,
                warning: Some(BROWSER_STATIC_HTML_RUNTIME_WARNING),
            },
            _ => Self::unknown("browserd.health", Some(BROWSER_UNKNOWN_RUNTIME_WARNING)),
        }
    }

    fn unavailable() -> Self {
        Self::unknown("browserd.health.unavailable", Some(BROWSER_UNKNOWN_RUNTIME_WARNING))
    }

    fn unknown(source: &'static str, warning: Option<&'static str>) -> Self {
        Self {
            source,
            engine_mode: "unknown".to_owned(),
            javascript_execution: None,
            subresource_loading: None,
            dom_interaction: None,
            health_status: "unknown".to_owned(),
            resilience_profile: "unknown".to_owned(),
            automatic_reconnect: None,
            degraded_sessions: 0,
            reconnecting_sessions: 0,
            blocked_sessions: 0,
            process_reconnect_count: 0,
            target_reconnect_count: 0,
            dialog_timeout_count: 0,
            warning,
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "source": self.source,
            "engine_mode": self.engine_mode,
            "javascript_execution": self.javascript_execution,
            "subresource_loading": self.subresource_loading,
            "dom_interaction": self.dom_interaction,
            "health_status": self.health_status,
            "resilience_profile": self.resilience_profile,
            "automatic_reconnect": self.automatic_reconnect,
            "degraded_sessions": self.degraded_sessions,
            "reconnecting_sessions": self.reconnecting_sessions,
            "blocked_sessions": self.blocked_sessions,
            "process_reconnect_count": self.process_reconnect_count,
            "target_reconnect_count": self.target_reconnect_count,
            "dialog_timeout_count": self.dialog_timeout_count,
            "warning": self.warning,
        })
    }
}

fn browser_text_entry_action_name(tool_name: &str) -> &'static str {
    if tool_name == BROWSER_FILL_TOOL_NAME {
        "fill"
    } else {
        "type"
    }
}

fn is_browser_rescue_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        BROWSER_VISION_TOOL_NAME
            | BROWSER_IMAGES_LIST_TOOL_NAME
            | BROWSER_DIALOG_TOOL_NAME
            | BROWSER_CDP_INVOKE_TOOL_NAME
    )
}

fn browser_rescue_rollout_disabled_output(tool_name: &str) -> Value {
    json!({
        "success": false,
        "error": "browser_rescue_disabled",
        "error_code": "browser_rescue_disabled",
        "tool_name": tool_name,
        "rollout": {
            "enabled": false,
            "config_path": BROWSER_RESCUE_ROLLOUT_CONFIG_PATH,
        },
        "next_action": "enable feature_rollouts.browser_rescue before using browser rescue tools",
    })
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BrowserRescueTriggerKind {
    ExplicitBrowserToolFailure,
    BrowserStateCorruption,
    PolicyDenied,
    NetworkEgressDenied,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct BrowserRescueTriggerDecision {
    attempt_rescue: bool,
    reason_code: &'static str,
    trace_event: &'static str,
    maturity: &'static str,
}

#[allow(dead_code)]
fn evaluate_browser_rescue_trigger(
    rollout_enabled: bool,
    trigger: BrowserRescueTriggerKind,
) -> BrowserRescueTriggerDecision {
    if !rollout_enabled {
        return BrowserRescueTriggerDecision {
            attempt_rescue: false,
            reason_code: "browser_rescue.rollout_disabled",
            trace_event: "browser.rescue.skipped",
            maturity: "preview",
        };
    }
    match trigger {
        BrowserRescueTriggerKind::ExplicitBrowserToolFailure
        | BrowserRescueTriggerKind::BrowserStateCorruption => BrowserRescueTriggerDecision {
            attempt_rescue: true,
            reason_code: "browser_rescue.explicit_trigger",
            trace_event: "browser.rescue.requested",
            maturity: "preview",
        },
        BrowserRescueTriggerKind::PolicyDenied => BrowserRescueTriggerDecision {
            attempt_rescue: false,
            reason_code: "browser_rescue.policy_denied_no_rescue",
            trace_event: "browser.rescue.skipped",
            maturity: "preview",
        },
        BrowserRescueTriggerKind::NetworkEgressDenied => BrowserRescueTriggerDecision {
            attempt_rescue: false,
            reason_code: "browser_rescue.egress_denied_no_rescue",
            trace_event: "browser.rescue.skipped",
            maturity: "preview",
        },
    }
}

#[allow(dead_code)]
fn browser_rescue_trace_payload(
    profile_id: &str,
    trigger: BrowserRescueTriggerKind,
    decision: &BrowserRescueTriggerDecision,
) -> Value {
    json!({
        "event_type": decision.trace_event,
        "profile_id": crate::sha256_hex(profile_id.as_bytes()),
        "rescue_kind": match trigger {
            BrowserRescueTriggerKind::ExplicitBrowserToolFailure => "explicit_browser_tool_failure",
            BrowserRescueTriggerKind::BrowserStateCorruption => "browser_state_corruption",
            BrowserRescueTriggerKind::PolicyDenied => "policy_denied",
            BrowserRescueTriggerKind::NetworkEgressDenied => "network_egress_denied",
        },
        "policy_decision": decision.reason_code,
        "attempt_rescue": decision.attempt_rescue,
        "maturity": decision.maturity,
        "raw_browser_payload_visible": false,
    })
}

fn browser_cdp_method_allowed(method: &str) -> bool {
    matches!(method, "Page.getLayoutMetrics" | "DOM.getDocument" | "Accessibility.getFullAXTree")
}

/// Whether `tool_name` operates on an already-open browser session.
///
/// `session.create` is exempt because it mints the session; `session.close`
/// is exempt so closing stays idempotent even after the daemon has already
/// recorded the session as closed.
fn browser_tool_requires_open_session(tool_name: &str) -> bool {
    matches!(
        tool_name,
        BROWSER_NAVIGATE_TOOL_NAME
            | BROWSER_RELOAD_TOOL_NAME
            | BROWSER_CLICK_TOOL_NAME
            | BROWSER_TYPE_TOOL_NAME
            | BROWSER_FILL_TOOL_NAME
            | BROWSER_UPLOAD_TOOL_NAME
            | BROWSER_PRESS_TOOL_NAME
            | BROWSER_SELECT_TOOL_NAME
            | BROWSER_VIEWPORT_TOOL_NAME
            | BROWSER_HIGHLIGHT_TOOL_NAME
            | BROWSER_SCROLL_TOOL_NAME
            | BROWSER_WAIT_FOR_TOOL_NAME
            | BROWSER_TITLE_TOOL_NAME
            | BROWSER_SCREENSHOT_TOOL_NAME
            | BROWSER_PDF_TOOL_NAME
            | BROWSER_OBSERVE_TOOL_NAME
            | BROWSER_VISION_TOOL_NAME
            | BROWSER_IMAGES_LIST_TOOL_NAME
            | BROWSER_DIALOG_TOOL_NAME
            | BROWSER_CDP_INVOKE_TOOL_NAME
            | BROWSER_STORAGE_TOOL_NAME
            | BROWSER_NETWORK_LOG_TOOL_NAME
            | BROWSER_CONSOLE_LOG_TOOL_NAME
            | BROWSER_RESET_STATE_TOOL_NAME
            | BROWSER_TABS_LIST_TOOL_NAME
            | BROWSER_TABS_OPEN_TOOL_NAME
            | BROWSER_TABS_SWITCH_TOOL_NAME
            | BROWSER_TABS_CLOSE_TOOL_NAME
            | BROWSER_PERMISSIONS_GET_TOOL_NAME
            | BROWSER_PERMISSIONS_SET_TOOL_NAME
            | BROWSER_DOWNLOADS_LIST_TOOL_NAME
            | BROWSER_DOWNLOADS_GET_TOOL_NAME
    )
}

/// Resolves the browserd private-target flag after URL-specific validation.
///
/// A `file://` target reaches this helper only after the caller has confined
/// the canonical file to an active workspace root. Network targets require the
/// explicit model-visible opt-in carried by the browser call.
fn browser_private_target_flag_for_validated_url(url: &str, explicitly_allowed: bool) -> bool {
    browser_url_uses_file_scheme(url) || explicitly_allowed
}

fn browser_private_targets_requested(payload: &serde_json::Map<String, Value>) -> bool {
    payload.get("allow_private_targets").and_then(Value::as_bool).unwrap_or(false)
}

fn browser_reload_private_target_flag_for_validated_url(
    url: &str,
    payload: &serde_json::Map<String, Value>,
) -> bool {
    browser_private_target_flag_for_validated_url(url, browser_private_targets_requested(payload))
}

fn browser_url_uses_file_scheme(raw_url: &str) -> bool {
    Url::parse(raw_url.trim())
        .map(|parsed| parsed.scheme().eq_ignore_ascii_case("file"))
        .unwrap_or(false)
}

fn browser_reload_expected_url_from_payload(
    payload: &serde_json::Map<String, Value>,
) -> Result<String, String> {
    let Some(expected_url) = payload.get("expected_url").and_then(Value::as_str).map(str::trim)
    else {
        return Err(
            "palyra.browser.reload requires expected_url to bind the reload destination".to_owned()
        );
    };
    if expected_url.is_empty() || expected_url.chars().any(char::is_control) {
        return Err(
            "palyra.browser.reload expected_url must be a non-empty URL without control characters"
                .to_owned(),
        );
    }
    Url::parse(expected_url)
        .map_err(|error| format!("palyra.browser.reload expected_url is invalid: {error}"))?;
    Ok(expected_url.to_owned())
}

/// Parses the optional `profile_id` field of `session.create`.
///
/// # Errors
/// Returns a tool-facing message when `profile_id` is present but not a
/// string or not a canonical id.
// NOTE: the second tuple element ("ignored profile id") is always None
// today -- non-canonical ids hard-fail instead of being ignored (pinned by
// browser_session_profile_id_rejects_friendly_labels). It survives only so
// the session.create output keeps its `ignored_profile_id` and
// `profile_id_warning` JSON fields; removing it changes the output shape.
fn browser_session_profile_id_from_payload(
    payload: &serde_json::Map<String, Value>,
) -> Result<(Option<common_v1::CanonicalId>, Option<String>), String> {
    let Some(value) = payload.get("profile_id") else {
        return Ok((None, None));
    };
    let Value::String(raw) = value else {
        return Err("palyra.browser.session.create field 'profile_id' must be a string".to_owned());
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok((None, None));
    }
    if validate_canonical_id(trimmed).is_ok() {
        return Ok((Some(common_v1::CanonicalId { ulid: trimmed.to_owned() }), None));
    }

    Err("palyra.browser.session.create field 'profile_id' must be a canonical id".to_owned())
}

/// Resolves `(persistence_enabled, persistence_id)` for `session.create`.
///
/// Explicitly enabled persistence is keyed by a daemon-derived id for the
/// current agent session, so callers cannot pick another session's namespace.
fn browser_session_persistence_from_payload(
    payload: &serde_json::Map<String, Value>,
    agent_session_id: &str,
) -> Result<(bool, String), String> {
    if payload.contains_key("persistence_id") {
        return Err(
            "palyra.browser.session.create field 'persistence_id' is reserved for the runtime"
                .to_owned(),
        );
    }
    if payload.get("private_profile").and_then(Value::as_bool).unwrap_or(false) {
        return Ok((false, String::new()));
    }
    let persistence_enabled =
        payload.get("persistence_enabled").and_then(Value::as_bool).unwrap_or(false);
    if !persistence_enabled {
        return Ok((false, String::new()));
    }
    Ok((true, default_browser_session_persistence_id(agent_session_id)))
}

/// Derives a deterministic, collision-resistant persistence id from the agent
/// session id without exposing the raw session id in browserd storage keys.
fn default_browser_session_persistence_id(agent_session_id: &str) -> String {
    const PREFIX: &str = "agent-session-sha256-";
    let mut digest = Sha256::new();
    digest.update(b"palyra.browser.session.persistence.v1\0");
    digest.update(agent_session_id.as_bytes());
    format!("{PREFIX}{}", hex::encode(digest.finalize()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BrowserNavigationUrl {
    model_url: String,
    transport_url: String,
}

impl BrowserNavigationUrl {
    fn same_destination(&self, other: &Self) -> bool {
        self.transport_url == other.transport_url
    }

    fn project_response_url(&self, response_url: &str) -> String {
        if browser_navigation_response_matches_transport(response_url, self.transport_url.as_str())
        {
            return self.model_url.clone();
        }
        redact_url(response_url)
    }
}

/// Confines `file://` navigation targets to the run's browser-readable workspace roots and
/// resolves the portable `file:///workspace/...` alias for browserd transport.
///
/// Non-file URLs pass through untouched. The target must canonicalize to a
/// regular file inside one of the resolved workspace roots, which blocks
/// symlink and `..` escapes before the URL ever reaches browserd.
///
/// # Errors
/// Returns a tool-facing message when the URL is malformed, the target does
/// not resolve to a regular file, or it lies outside every workspace root.
async fn resolve_browser_navigation_url(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    url: &str,
) -> Result<BrowserNavigationUrl, String> {
    let url = url.trim();
    if !browser_url_uses_file_scheme(url) {
        return Ok(BrowserNavigationUrl {
            model_url: url.to_owned(),
            transport_url: url.to_owned(),
        });
    }
    let parsed = Url::parse(url).map_err(|error| format!("invalid file URL: {error}"))?;
    validate_browser_file_url_shape(&parsed)?;
    let canonical_roots =
        resolve_browser_read_workspace_roots(runtime_state, context, BROWSER_NAVIGATE_TOOL_NAME)
            .await?;
    resolve_browser_file_navigation_url(&parsed, canonical_roots.as_slice())
}

fn resolve_browser_file_navigation_url(
    parsed: &Url,
    canonical_roots: &[PathBuf],
) -> Result<BrowserNavigationUrl, String> {
    validate_browser_file_url_shape(parsed)?;
    let transport_candidate = if let Some(relative_url) =
        browser_workspace_alias_relative_url(parsed)
    {
        let workspace_root = canonical_roots.first().ok_or_else(|| {
            "palyra.browser.navigate agent has no accessible workspace roots".to_owned()
        })?;
        let workspace_url = Url::from_directory_path(workspace_root).map_err(|()| {
            "palyra.browser.navigate workspace root cannot be represented as a file URL".to_owned()
        })?;
        // Prefixing with `./` prevents a colon in the relative payload from
        // being interpreted as a new URL scheme. Canonical scope validation
        // below remains authoritative for encoded separators and traversal.
        let mut resolved =
            workspace_url.join(format!("./{relative_url}").as_str()).map_err(|error| {
                format!("palyra.browser.navigate workspace file URL is invalid: {error}")
            })?;
        resolved.set_fragment(parsed.fragment());
        resolved
    } else {
        parsed.clone()
    };
    let file_path = browser_file_url_to_path(&transport_candidate)?;
    let canonical_target = fs::canonicalize(file_path.as_path()).map_err(|error| {
        format!("palyra.browser.navigate failed to resolve file URL target: {error}")
    })?;
    let metadata = fs::metadata(canonical_target.as_path()).map_err(|error| {
        format!("palyra.browser.navigate failed to inspect file URL target: {error}")
    })?;
    if !metadata.is_file() {
        return Err("palyra.browser.navigate file URL target is not a regular file".to_owned());
    }
    validate_browser_file_url_path_scope(
        parsed.as_str(),
        canonical_target.as_path(),
        canonical_roots,
    )?;

    let mut canonical_url = Url::from_file_path(canonical_target.as_path()).map_err(|()| {
        "palyra.browser.navigate canonical target cannot be represented as a file URL".to_owned()
    })?;
    canonical_url.set_fragment(parsed.fragment());
    Ok(BrowserNavigationUrl {
        model_url: parsed.to_string(),
        transport_url: canonical_url.to_string(),
    })
}

fn browser_workspace_alias_relative_url(parsed: &Url) -> Option<&str> {
    if parsed.host_str().is_some() {
        return None;
    }
    parsed.path().strip_prefix("/workspace/")
}

fn browser_navigation_response_matches_transport(response_url: &str, transport_url: &str) -> bool {
    if response_url == transport_url {
        return true;
    }
    let Ok(parsed) = Url::parse(response_url) else {
        return false;
    };
    if parsed.scheme() != "file" || validate_browser_file_url_shape(&parsed).is_err() {
        return false;
    }
    let Ok(path) = browser_file_url_to_path(&parsed) else {
        return false;
    };
    let Ok(canonical_target) = fs::canonicalize(path) else {
        return false;
    };
    let Ok(mut canonical_url) = Url::from_file_path(canonical_target) else {
        return false;
    };
    canonical_url.set_fragment(parsed.fragment());
    canonical_url.as_str() == transport_url
}

fn validate_browser_file_url_path_scope(
    url: &str,
    canonical_target: &Path,
    canonical_roots: &[PathBuf],
) -> Result<(), String> {
    if canonical_file_path_is_inside_workspace_roots(canonical_target, canonical_roots) {
        return Ok(());
    }
    Err(format!(
        "palyra.browser.navigate file:// URL {url} must point at a regular file inside the active agent workspace roots"
    ))
}

/// Rejects embedded credentials and query strings so nothing can be smuggled
/// past the path checks.
fn validate_browser_file_url_shape(parsed: &Url) -> Result<(), String> {
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("palyra.browser.navigate file URL credentials are not allowed".to_owned());
    }
    if parsed.query().is_some() {
        return Err("palyra.browser.navigate file URL query strings are not allowed".to_owned());
    }
    Ok(())
}

fn browser_file_url_to_path(parsed: &Url) -> Result<PathBuf, String> {
    validate_browser_file_url_shape(parsed)?;
    parsed.to_file_path().map_err(|_| "palyra.browser.navigate file URL path is invalid".to_owned())
}

/// Canonicalizes workspace roots, requiring each to be an existing directory.
///
/// # Errors
/// Returns a tool-facing message when a root cannot be canonicalized, is not
/// a directory, or the agent has no roots at all.
fn canonicalize_browser_workspace_roots(
    tool_name: &str,
    roots: &[PathBuf],
) -> Result<Vec<PathBuf>, String> {
    let mut canonical_roots = Vec::with_capacity(roots.len());
    for (index, root) in roots.iter().enumerate() {
        let canonical = fs::canonicalize(root).map_err(|error| {
            format!("{tool_name} failed to resolve workspace root {index}: {error}")
        })?;
        if !canonical.is_dir() {
            return Err(format!("{tool_name} workspace root {index} is not a directory"));
        }
        canonical_roots.push(canonical);
    }
    if canonical_roots.is_empty() {
        return Err(format!("{tool_name} agent has no accessible workspace roots"));
    }
    Ok(canonical_roots)
}

fn canonical_file_path_is_inside_workspace_roots(
    canonical_target: &Path,
    canonical_roots: &[PathBuf],
) -> bool {
    // Canonical path components must retain their identity because Windows can
    // host case-sensitive directories and shares.
    canonical_roots.iter().any(|root| canonical_target.starts_with(root))
}

/// Resolves only the configured agent roots for browser reads and navigation.
/// Run-launch roots are intentionally excluded because client-supplied launch
/// context cannot widen local-file authority.
///
/// # Errors
/// Returns a tool-facing message when agent resolution fails or the roots do
/// not canonicalize to existing directories.
async fn resolve_browser_read_workspace_roots(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    tool_name: &str,
) -> Result<Vec<PathBuf>, String> {
    let (workspace_roots, _) =
        browser_agent_workspace_root_inputs(runtime_state, context, tool_name).await?;
    canonicalize_browser_workspace_roots(tool_name, workspace_roots.as_slice())
}

/// Resolves workspace roots for browser artifact output. Launch-context roots
/// stay in this path so outputs land next to the operator's active project
/// when explicitly requested, without widening browser read/navigation scope.
async fn resolve_browser_output_workspace_roots(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    tool_name: &str,
) -> Result<Vec<PathBuf>, String> {
    let (workspace_roots, source) =
        browser_agent_workspace_root_inputs(runtime_state, context, tool_name).await?;
    let workspace_roots = workspace_roots_with_run_launch_context_for_agent_source(
        runtime_state,
        context.run_id,
        workspace_roots.as_slice(),
        source,
    )
    .await;
    canonicalize_browser_workspace_roots(tool_name, workspace_roots.as_slice())
}

async fn browser_agent_workspace_root_inputs(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    tool_name: &str,
) -> Result<(Vec<PathBuf>, AgentResolutionSource), String> {
    let agent_outcome = runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: context.principal.to_owned(),
            channel: context.channel.map(str::to_owned),
            session_id: Some(context.session_id.to_owned()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await
        .map_err(|error| {
            format!("{tool_name} failed to resolve agent workspace: {}", error.message())
        })?;
    let workspace_roots =
        agent_outcome.agent.workspace_roots.iter().map(PathBuf::from).collect::<Vec<_>>();
    Ok((workspace_roots, agent_outcome.source))
}

/// Extracts the `file_path` field for `upload`, rejecting empty values and
/// control characters.
fn browser_upload_path_from_payload(
    payload: &serde_json::Map<String, Value>,
) -> Result<&str, String> {
    let Some(file_path) = payload.get("file_path").and_then(Value::as_str).map(str::trim) else {
        return Err(format!(
            "{BROWSER_UPLOAD_TOOL_NAME} requires non-empty string field 'file_path'"
        ));
    };
    if file_path.is_empty() {
        return Err(format!(
            "{BROWSER_UPLOAD_TOOL_NAME} requires non-empty string field 'file_path'"
        ));
    }
    if file_path.chars().any(char::is_control) {
        return Err(format!(
            "{BROWSER_UPLOAD_TOOL_NAME} field 'file_path' contains unsupported characters"
        ));
    }
    Ok(file_path)
}

/// Resolves and reads an upload file, returning `(file_name, bytes)`.
///
/// The size cap is checked against metadata before reading so an oversized
/// file is rejected without ever being loaded into memory.
///
/// # Errors
/// Returns a tool-facing message when the path escapes the allowed roots, is
/// not a regular file, exceeds [`BROWSER_UPLOAD_MAX_FILE_BYTES`], or IO
/// fails.
async fn read_browser_upload_file(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    file_path: &str,
) -> Result<(String, Vec<u8>), String> {
    let workspace_roots =
        resolve_browser_read_workspace_roots(runtime_state, context, BROWSER_UPLOAD_TOOL_NAME)
            .await?;
    let user_owned_roots = browser_upload_approved_os_roots();
    let path_env = run_launch_context_path_env(runtime_state, context.run_id).await;
    let canonical = resolve_browser_upload_path(
        file_path,
        workspace_roots.as_slice(),
        user_owned_roots.as_slice(),
        &path_env,
    )?;
    let mut allowed_roots = workspace_roots;
    allowed_roots.extend(user_owned_roots);
    let (file, opened_path) = open_regular_file_for_read_under_roots(
        BROWSER_UPLOAD_TOOL_NAME,
        canonical.as_path(),
        file_path,
        allowed_roots.as_slice(),
    )?;
    let metadata = file.metadata().map_err(|error| {
        format!(
            "{BROWSER_UPLOAD_TOOL_NAME} failed to inspect upload file {}: {error}",
            opened_path.display()
        )
    })?;
    if metadata.len() > BROWSER_UPLOAD_MAX_FILE_BYTES {
        return Err(format!(
            "{BROWSER_UPLOAD_TOOL_NAME} upload file exceeds max bytes ({} > {BROWSER_UPLOAD_MAX_FILE_BYTES})",
            metadata.len()
        ));
    }
    let file_name = opened_path
        .file_name()
        .and_then(|value| value.to_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{BROWSER_UPLOAD_TOOL_NAME} upload path has no file name"))?
        .to_owned();
    let mut file_bytes = Vec::with_capacity(
        usize::try_from(metadata.len().min(BROWSER_UPLOAD_MAX_FILE_BYTES)).unwrap_or(0),
    );
    file.take(BROWSER_UPLOAD_MAX_FILE_BYTES.saturating_add(1))
        .read_to_end(&mut file_bytes)
        .map_err(|error| {
            format!(
                "{BROWSER_UPLOAD_TOOL_NAME} failed to read upload file {}: {error}",
                opened_path.display()
            )
        })?;
    if file_bytes.len() as u64 > BROWSER_UPLOAD_MAX_FILE_BYTES {
        return Err(format!(
            "{BROWSER_UPLOAD_TOOL_NAME} upload file exceeds max bytes while reading"
        ));
    }
    Ok((file_name, file_bytes))
}

/// Resolves an upload `file_path` to a canonical path inside the allowed
/// roots.
///
/// Resolution order: launch-env-prefixed paths expand first, then every
/// absolute result must fall inside an authorized workspace or approved
/// user-owned OS root. Launch env values are path aliases, not additional read
/// grants. Relative paths are confined to the first workspace root.
/// Uploads intentionally do not fall back to daemon process env because those
/// variables commonly point at credential files.
/// Protected OS locations are denied regardless of root membership.
fn resolve_browser_upload_path(
    file_path: &str,
    workspace_roots: &[PathBuf],
    user_owned_roots: &[PathBuf],
    path_env: &BTreeMap<String, PathBuf>,
) -> Result<PathBuf, String> {
    let requested = expand_browser_env_prefixed_path(
        BROWSER_UPLOAD_TOOL_NAME,
        "file_path",
        file_path,
        path_env,
        BrowserPathEnvFallback::LaunchOnly,
    )?
    .unwrap_or_else(|| PathBuf::from(file_path));
    let resolved = if requested.is_absolute() {
        requested
    } else {
        let relative = validate_browser_workspace_relative_path(requested.as_path())?;
        let Some(root) = workspace_roots.first() else {
            return Err(format!("{BROWSER_UPLOAD_TOOL_NAME} agent has no workspace root"));
        };
        root.join(relative)
    };
    let canonical = fs::canonicalize(resolved.as_path()).map_err(|error| {
        format!("{BROWSER_UPLOAD_TOOL_NAME} failed to resolve upload file {file_path}: {error}")
    })?;
    if browser_protected_os_path(canonical.as_path()) {
        return Err(format!(
            "{BROWSER_UPLOAD_TOOL_NAME} denied protected OS path {}",
            canonical.display()
        ));
    }
    if !workspace_roots.iter().chain(user_owned_roots).any(|root| canonical.starts_with(root)) {
        return Err(format!(
            "{BROWSER_UPLOAD_TOOL_NAME} upload file {} is outside agent workspace roots and approved user-owned OS roots; launch environment aliases do not grant file access",
            canonical.display()
        ));
    }
    Ok(canonical)
}

fn validate_browser_workspace_relative_path(path: &Path) -> Result<PathBuf, String> {
    validate_browser_workspace_relative_path_for_tool(BROWSER_UPLOAD_TOOL_NAME, "file_path", path)
}

/// Rejects rooted, `.`, and `..` components so a relative path can only
/// descend from the root it is later joined to.
fn validate_browser_workspace_relative_path_for_tool(
    tool_name: &str,
    field_name: &str,
    path: &Path,
) -> Result<PathBuf, String> {
    if path.as_os_str().is_empty() {
        return Err(format!("{tool_name} relative {field_name} must be non-empty"));
    }
    for component in path.components() {
        if matches!(
            component,
            Component::Prefix(_) | Component::RootDir | Component::CurDir | Component::ParentDir
        ) {
            return Err(format!(
                "{tool_name} relative {field_name} must not contain root, '.', or '..' components"
            ));
        }
    }
    Ok(path.to_path_buf())
}

/// Extracts the optional `output_path` field; `Ok(None)` means the caller
/// did not ask for the artifact to be written to disk.
fn browser_output_path_from_payload<'a>(
    payload: &'a serde_json::Map<String, Value>,
    tool_name: &str,
) -> Result<Option<&'a str>, String> {
    let Some(value) = payload.get("output_path") else {
        return Ok(None);
    };
    let Some(output_path) = value.as_str().map(str::trim) else {
        return Err(format!("{tool_name} field 'output_path' must be a string"));
    };
    if output_path.is_empty() {
        return Err(format!("{tool_name} field 'output_path' must be non-empty"));
    }
    if output_path.chars().any(char::is_control) {
        return Err(format!("{tool_name} field 'output_path' contains unsupported characters"));
    }
    Ok(Some(output_path))
}

/// Writes artifact `bytes` to the payload's `output_path`, if requested.
///
/// Returns `Ok(None)` when no `output_path` was supplied, otherwise a JSON
/// manifest (`path`, `mime_type`, `size_bytes`, `sha256`) describing the
/// written file. The target is scope-checked against workspace and approved
/// OS roots before any write happens.
///
/// # Errors
/// Returns a tool-facing message when scope resolution fails, the target is
/// outside every allowed root, or the write fails.
async fn save_browser_output_file_from_payload(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    payload: &serde_json::Map<String, Value>,
    tool_name: &str,
    mime_type: &str,
    bytes: &[u8],
) -> Result<Option<Value>, String> {
    let Some(output_path) = browser_output_path_from_payload(payload, tool_name)? else {
        return Ok(None);
    };
    let workspace_roots = resolve_browser_output_workspace_roots(runtime_state, context, tool_name)
        .await
        .map_err(|error| format!("{tool_name} failed to resolve output workspace: {error}"))?;
    let active_workspace_root = session_active_workspace_root(
        runtime_state,
        context.session_id,
        workspace_roots.as_slice(),
    )
    .await
    .map_err(|error| format!("{tool_name} failed to resolve active output workspace: {error}"))?;
    let path_env = run_launch_context_path_env(runtime_state, context.run_id).await;
    let target = resolve_browser_output_path(
        tool_name,
        output_path,
        workspace_roots.as_slice(),
        active_workspace_root.as_ref(),
        &path_env,
        browser_user_owned_os_roots().as_slice(),
    )?;
    write_browser_output_file(tool_name, output_path, target.as_path(), bytes)?;
    let canonical_target = fs::canonicalize(target.as_path()).unwrap_or(target);
    Ok(Some(json!({
        "path": canonical_target.to_string_lossy(),
        "mime_type": mime_type,
        "size_bytes": bytes.len(),
        "sha256": hex::encode(Sha256::digest(bytes)),
    })))
}

/// Resolves an `output_path` to a validated, writable target path.
///
/// Env-prefixed paths expand first. Absolute paths must land inside the
/// workspace, user-owned OS, or launch env roots. Relative paths prefer the
/// session's active workspace root (so artifacts land next to the work in
/// progress) and fall back to the first workspace root.
fn resolve_browser_output_path(
    tool_name: &str,
    output_path: &str,
    workspace_roots: &[PathBuf],
    active_workspace_root: Option<&ActiveWorkspaceRoot>,
    path_env: &BTreeMap<String, PathBuf>,
    user_owned_roots: &[PathBuf],
) -> Result<PathBuf, String> {
    let requested = expand_browser_env_prefixed_path(
        tool_name,
        "output_path",
        output_path,
        path_env,
        BrowserPathEnvFallback::ProcessEnv,
    )?
    .unwrap_or_else(|| PathBuf::from(output_path));
    let allowed_roots = if requested.is_absolute() {
        browser_output_absolute_path_allowed_roots(workspace_roots, user_owned_roots, path_env)
    } else {
        let relative = validate_browser_workspace_relative_path_for_tool(
            tool_name,
            "output_path",
            &requested,
        )?;
        let Some(root) =
            browser_relative_output_base_root(output_path, workspace_roots, active_workspace_root)
                .or_else(|| workspace_roots.first().cloned())
        else {
            return Err(format!("{tool_name} agent has no workspace root"));
        };
        return prepare_browser_output_target(
            tool_name,
            root.join(relative).as_path(),
            workspace_roots,
        );
    };
    prepare_browser_output_target(tool_name, requested.as_path(), allowed_roots.as_slice())
}

/// Union of roots an absolute browser output path may use, deduplicated.
fn browser_output_absolute_path_allowed_roots(
    workspace_roots: &[PathBuf],
    user_owned_roots: &[PathBuf],
    path_env: &BTreeMap<String, PathBuf>,
) -> Vec<PathBuf> {
    let mut roots =
        workspace_roots.iter().chain(user_owned_roots.iter()).cloned().collect::<Vec<_>>();
    for root in path_env.values() {
        if !roots.iter().any(|existing| existing == root) {
            roots.push(root.clone());
        }
    }
    roots
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BrowserPathEnvFallback {
    LaunchOnly,
    ProcessEnv,
}

/// Expands a `%VAR%`, `${VAR}`, or `$VAR` prefix into an absolute base path.
///
/// Run-launch-context variables take precedence over process env so harness
/// or CLI launches can hand the agent approved roots without baking machine
/// paths into tool inputs. Callers that read local bytes must use
/// [`BrowserPathEnvFallback::LaunchOnly`] to avoid expanding process-level
/// secret locator variables. Returns `Ok(None)` when `path` has no env prefix.
fn expand_browser_env_prefixed_path(
    tool_name: &str,
    field_name: &str,
    path: &str,
    path_env: &BTreeMap<String, PathBuf>,
    fallback: BrowserPathEnvFallback,
) -> Result<Option<PathBuf>, String> {
    let Some((key, suffix)) = browser_path_env_prefix(tool_name, field_name, path)? else {
        return Ok(None);
    };
    let base = if let Some(value) = path_env.get(key) {
        value.clone()
    } else {
        match fallback {
            BrowserPathEnvFallback::LaunchOnly => {
                return Err(format!(
                    "{tool_name} {field_name} references unset launch environment variable `{key}`"
                ));
            }
            BrowserPathEnvFallback::ProcessEnv => std::env::var_os(key)
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
                .ok_or_else(|| {
                    format!(
                        "{tool_name} {field_name} references unset environment variable `{key}`"
                    )
                })?,
        }
    };
    if !base.is_absolute() {
        return Err(format!(
            "{tool_name} {field_name} environment variable `{key}` must resolve to an absolute OS path"
        ));
    }
    append_browser_env_path_suffix(tool_name, field_name, base, suffix).map(Some)
}

/// Splits a leading `%VAR%`, `${VAR}`, or `$VAR` prefix into `(key, suffix)`.
fn browser_path_env_prefix<'a>(
    tool_name: &str,
    field_name: &str,
    path: &'a str,
) -> Result<Option<(&'a str, &'a str)>, String> {
    if let Some(rest) = path.strip_prefix('%') {
        let Some(end) = rest.find('%') else {
            return Err(format!("{tool_name} {field_name} has malformed %VAR% environment prefix"));
        };
        let key = &rest[..end];
        validate_browser_path_env_key(tool_name, field_name, key)?;
        return Ok(Some((key, &rest[end + 1..])));
    }
    if let Some(rest) = path.strip_prefix("${") {
        let Some(end) = rest.find('}') else {
            return Err(format!(
                "{tool_name} {field_name} has malformed ${{VAR}} environment prefix"
            ));
        };
        let key = &rest[..end];
        validate_browser_path_env_key(tool_name, field_name, key)?;
        return Ok(Some((key, &rest[end + 1..])));
    }
    if let Some(rest) = path.strip_prefix('$') {
        let key_len = rest
            .char_indices()
            .take_while(|(_, ch)| ch.is_ascii_alphanumeric() || *ch == '_')
            .map(|(index, ch)| index + ch.len_utf8())
            .last()
            .unwrap_or(0);
        if key_len == 0 {
            return Err(format!("{tool_name} {field_name} has malformed $VAR environment prefix"));
        }
        let key = &rest[..key_len];
        validate_browser_path_env_key(tool_name, field_name, key)?;
        return Ok(Some((key, &rest[key_len..])));
    }
    Ok(None)
}

fn validate_browser_path_env_key(
    tool_name: &str,
    field_name: &str,
    key: &str,
) -> Result<(), String> {
    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return Err(format!("{tool_name} {field_name} environment variable name is empty"));
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(format!(
            "{tool_name} {field_name} environment variable name must use ASCII letters, digits, or underscores"
        ));
    }
    Ok(())
}

/// Appends the post-prefix suffix to the expanded env base path.
///
/// Segments are re-validated here (no `.`, `..`, or drive-colon segments)
/// because the suffix bypassed the relative-path validator; this keeps an
/// env-prefixed path from escaping its expanded root.
fn append_browser_env_path_suffix(
    tool_name: &str,
    field_name: &str,
    mut base: PathBuf,
    suffix: &str,
) -> Result<PathBuf, String> {
    let relative_suffix = suffix.trim_start_matches(['/', '\\']);
    if relative_suffix.is_empty() {
        return Ok(base);
    }
    for segment in relative_suffix.split(['/', '\\']) {
        if segment.is_empty() {
            continue;
        }
        if segment == "." || segment == ".." || segment.contains(':') {
            return Err(format!(
                "{tool_name} {field_name} environment suffix must stay relative to the expanded root"
            ));
        }
        if segment.chars().any(char::is_control) {
            return Err(format!("{tool_name} {field_name} contains unsupported characters"));
        }
        base.push(segment);
    }
    Ok(base)
}

/// Picks the base root for a relative `output_path` when a session has an
/// active workspace.
///
/// Short relative paths resolve under the active workspace root itself;
/// paths that already start with the active root's relative prefix resolve
/// under the workspace root that owns it (so the prefix is not doubled).
/// `None` defers to the caller's first-workspace-root fallback.
fn browser_relative_output_base_root(
    output_path: &str,
    workspace_roots: &[PathBuf],
    active_workspace_root: Option<&ActiveWorkspaceRoot>,
) -> Option<PathBuf> {
    let active_workspace_root = active_workspace_root?;
    if relative_path_already_targets_active_root(output_path, active_workspace_root) {
        return workspace_root_for_active_relative_path(workspace_roots, active_workspace_root);
    }
    // Browser artifacts intentionally follow the active work directory. File
    // and patch tools use a stricter existence-based heuristic because their
    // relative paths describe source targets rather than generated evidence.
    Some(active_workspace_root.root.clone())
}

/// Finds the workspace root under which the active workspace's relative path
/// canonicalizes to the active root itself.
fn workspace_root_for_active_relative_path(
    workspace_roots: &[PathBuf],
    active_workspace_root: &ActiveWorkspaceRoot,
) -> Option<PathBuf> {
    let canonical_active = fs::canonicalize(active_workspace_root.root.as_path()).ok()?;
    let active_relative_path = Path::new(active_workspace_root.relative_path.as_str());
    workspace_roots.iter().find_map(|root| {
        let candidate = root.join(active_relative_path);
        let canonical_candidate = fs::canonicalize(candidate.as_path()).ok()?;
        if canonical_candidate == canonical_active {
            Some(root.clone())
        } else {
            None
        }
    })
}

/// Validates an output target and creates its parent directory, returning
/// the canonical path to write to.
///
/// Containment is checked twice on purpose: against the nearest existing
/// ancestor before `create_dir_all` (so no directories are ever created
/// outside the allowed roots), and against the canonicalized parent after
/// creation (so symlinked intermediates cannot redirect the write).
fn prepare_browser_output_target(
    tool_name: &str,
    target: &Path,
    allowed_roots: &[PathBuf],
) -> Result<PathBuf, String> {
    if browser_protected_os_path(target) {
        return Err(format!("{tool_name} denied protected output_path {}", target.display()));
    }
    let file_name = target
        .file_name()
        .ok_or_else(|| format!("{tool_name} output_path must include a file name"))?;
    let parent = target
        .parent()
        .ok_or_else(|| format!("{tool_name} output_path must include a parent directory"))?;
    let existing_parent = nearest_existing_parent(parent)
        .ok_or_else(|| format!("{tool_name} output_path has no existing parent directory"))?;
    let canonical_existing_parent =
        fs::canonicalize(existing_parent.as_path()).map_err(|error| {
            format!(
                "{tool_name} failed to resolve output_path parent {}: {error}",
                existing_parent.display()
            )
        })?;
    if !canonical_file_path_is_inside_workspace_roots(
        canonical_existing_parent.as_path(),
        allowed_roots,
    ) {
        return Err(format!(
            "{tool_name} output_path parent {} is outside agent workspace roots and approved user-owned OS roots",
            canonical_existing_parent.display()
        ));
    }
    fs::create_dir_all(parent).map_err(|error| {
        format!("{tool_name} failed to create output_path parent {}: {error}", parent.display())
    })?;
    let canonical_parent = fs::canonicalize(parent).map_err(|error| {
        format!("{tool_name} failed to resolve output_path parent {}: {error}", parent.display())
    })?;
    if !canonical_file_path_is_inside_workspace_roots(canonical_parent.as_path(), allowed_roots) {
        return Err(format!(
            "{tool_name} output_path parent {} is outside agent workspace roots and approved user-owned OS roots",
            canonical_parent.display()
        ));
    }
    let resolved = canonical_parent.join(file_name);
    reject_browser_output_final_symlink(tool_name, resolved.as_path())?;
    Ok(resolved)
}

fn reject_browser_output_final_symlink(tool_name: &str, target: &Path) -> Result<(), String> {
    match fs::symlink_metadata(target) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(format!(
            "{tool_name} output_path final component must not be a symlink: {}",
            target.display()
        )),
        Ok(metadata) if !metadata.is_file() => {
            Err(format!("{tool_name} output_path is not a regular file: {}", target.display()))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "{tool_name} failed to inspect output_path target {}: {error}",
            target.display()
        )),
    }
}

fn write_browser_output_file(
    tool_name: &str,
    output_path: &str,
    target: &Path,
    bytes: &[u8],
) -> Result<(), String> {
    reject_browser_output_final_symlink(tool_name, target)?;
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    configure_browser_output_no_follow(&mut options);
    let mut file = options.open(target).map_err(|error| {
        format!("{tool_name} failed to open output_path {output_path}: {error}")
    })?;
    file.write_all(bytes)
        .map_err(|error| format!("{tool_name} failed to write output_path {output_path}: {error}"))
}

#[cfg(unix)]
fn configure_browser_output_no_follow(options: &mut fs::OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(windows)]
fn configure_browser_output_no_follow(options: &mut fs::OpenOptions) {
    use std::os::windows::fs::OpenOptionsExt;
    options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_browser_output_no_follow(_options: &mut fs::OpenOptions) {}

/// Walks up from `path` to the closest ancestor that already exists.
fn nearest_existing_parent(path: &Path) -> Option<PathBuf> {
    let mut candidate = path;
    loop {
        if candidate.exists() {
            return Some(candidate.to_path_buf());
        }
        candidate = candidate.parent()?;
    }
}

/// User-owned OS roots where browser tools may save outputs outside agent
/// workspaces.
///
/// Explicitly configured roots (`PALYRA_OS_FILE_ROOTS`) replace the implicit
/// `USERPROFILE`/`HOME` roots so operators can narrow host filesystem access.
/// Temp directories are always allowed.
fn browser_user_owned_os_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(configured_roots) = configured_browser_user_os_roots() {
        for root in configured_roots {
            push_browser_canonical_root(&mut roots, root);
        }
    } else {
        for key in ["USERPROFILE", "HOME"] {
            if let Some(value) = std::env::var_os(key) {
                push_browser_canonical_root(&mut roots, PathBuf::from(value));
            }
        }
    }
    push_browser_canonical_root(&mut roots, std::env::temp_dir());
    #[cfg(windows)]
    push_browser_windows_drive_user_artifact_roots(&mut roots);
    #[cfg(unix)]
    {
        push_browser_canonical_root(&mut roots, PathBuf::from("/var/tmp"));
    }
    roots
}

fn configured_browser_user_os_roots() -> Option<Vec<PathBuf>> {
    let value = std::env::var_os(PALYRA_OS_FILE_ROOTS_ENV)?;
    let roots = std::env::split_paths(&value)
        .filter(|path| !path.as_os_str().is_empty())
        .collect::<Vec<_>>();
    if roots.is_empty() {
        None
    } else {
        Some(roots)
    }
}

/// Browser uploads admit only roots explicitly configured for OS-file access.
///
/// Unlike artifact outputs, uploads read bytes that a page can transmit, so
/// implicit profile and temp roots do not become browser input authority.
fn browser_upload_approved_os_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(configured_roots) = configured_browser_user_os_roots() {
        for root in configured_roots {
            push_browser_canonical_root(&mut roots, root);
        }
    }
    roots
}

#[cfg(windows)]
fn push_browser_windows_drive_user_artifact_roots(roots: &mut Vec<PathBuf>) {
    let Some(system_drive) = std::env::var_os("SystemDrive") else {
        return;
    };
    for candidate in
        browser_windows_drive_user_artifact_root_candidates(system_drive.to_string_lossy().as_ref())
    {
        push_browser_canonical_root(roots, candidate);
    }
}

// Mirrors the unix /var/tmp allowance and accepts the common harness/user
// exchange directory C:\downloads when it exists.
#[cfg(windows)]
fn browser_windows_drive_user_artifact_root_candidates(system_drive: &str) -> Vec<PathBuf> {
    let drive = system_drive.trim().trim_end_matches(['\\', '/']);
    let bytes = drive.as_bytes();
    if bytes.len() != 2 || bytes[1] != b':' || !bytes[0].is_ascii_alphabetic() {
        return Vec::new();
    }
    vec![
        PathBuf::from(format!("{drive}\\var\\tmp")),
        PathBuf::from(format!("{drive}\\downloads")),
        PathBuf::from(format!("{drive}\\Downloads")),
    ]
}

/// Adds `root` if it canonicalizes to an existing directory not already
/// listed; non-existent candidates are silently skipped.
fn push_browser_canonical_root(roots: &mut Vec<PathBuf>, root: PathBuf) {
    if let Ok(canonical) = fs::canonicalize(root.as_path()) {
        if canonical.is_dir() && !roots.iter().any(|existing| existing == &canonical) {
            roots.push(canonical);
        }
    }
}

/// Coarse deny-list of OS-critical locations browser tools must never touch,
/// even when an approved root would otherwise contain them.
///
/// Substring matching on the normalized path is deliberate on Windows so the
/// check applies to every drive letter.
fn browser_protected_os_path(path: &Path) -> bool {
    #[cfg(windows)]
    {
        let normalized = path.to_string_lossy().replace('\\', "/").to_ascii_lowercase();
        normalized.ends_with(":/")
            || normalized.contains(":/windows")
            || normalized.contains(":/program files")
            || normalized.contains(":/program files (x86)")
            || normalized.contains(":/system volume information")
    }
    #[cfg(not(windows))]
    {
        let normalized = path.to_string_lossy().replace('\\', "/");
        if normalized == "/" {
            return true;
        }
        for prefix in ["/etc", "/bin", "/sbin", "/usr", "/lib", "/lib64", "/System", "/Library"] {
            if normalized == prefix || normalized.starts_with(format!("{prefix}/").as_str()) {
                return true;
            }
        }
        false
    }
}

/// Executes one approved `palyra.browser.*` tool call against browserd.
///
/// Validates the JSON `input_json` payload, short-circuits sessions the
/// daemon has already recorded as closed, dials the browserd gRPC service,
/// dispatches the request for `tool_name`, and post-processes the result:
/// engine-capability annotation, missing-session normalization, recovery
/// hints, and an execution attestation. Every failure -- input validation,
/// transport, or backend-reported -- is encoded in the returned outcome
/// rather than panicking or bubbling an error.
pub(crate) async fn execute_browser_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    tool_name: &str,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let principal = context.principal;
    let channel = context.channel;
    let browser_service_config = runtime_state.browser_service_config_snapshot();
    if input_json.len() > MAX_BROWSER_TOOL_INPUT_BYTES {
        return browser_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.browser.* input exceeds {MAX_BROWSER_TOOL_INPUT_BYTES} bytes"),
        );
    }
    if !browser_service_config.enabled {
        return browser_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            "palyra.browser.* is disabled by runtime config (tool_call.browser_service.enabled=false)"
                .to_owned(),
        );
    }

    let payload = match serde_json::from_slice::<Value>(input_json) {
        Ok(Value::Object(map)) => map,
        Ok(_) => {
            return browser_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.browser.* requires JSON object input".to_owned(),
            );
        }
        Err(error) => {
            return browser_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.browser.* invalid JSON input: {error}"),
            );
        }
    };

    if is_browser_rescue_tool(tool_name)
        && !runtime_state.config.feature_rollouts.browser_rescue.enabled
    {
        let output = browser_rescue_rollout_disabled_output(tool_name);
        return browser_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
            format!("{tool_name} requires {BROWSER_RESCUE_ROLLOUT_CONFIG_PATH}=true"),
        );
    }

    // The daemon keeps its own ledger of sessions it saw close so calls on a
    // dead session fail fast with a stable `browser_session_closed` error
    // instead of a backend-specific one (and without an RPC round trip).
    if browser_tool_requires_open_session(tool_name) {
        let session_id = match parse_browser_tool_session_id(&payload) {
            Ok(value) => value,
            Err(error) => {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
        };
        if runtime_state.is_browser_session_closed(session_id.as_str()) {
            let error = browser_session_closed_error_message(tool_name, session_id.as_str());
            let output = json!({
                "success": false,
                "session_id": session_id,
                "error": "browser_session_closed",
            });
            return browser_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                error,
            );
        }
    }

    let browser_service_channel =
        match runtime_state.browser_service_channel(&browser_service_config) {
            Ok(value) => value,
            Err(error) => {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
        };
    let mut capability_client = browser_v1::browser_service_client::BrowserServiceClient::new(
        browser_service_channel.clone(),
    );
    // One extra health RPC per tool call is accepted so each outcome reports
    // the engine that actually served it (browserd may restart or change
    // engine mode between calls).
    let browser_runtime_capabilities = fetch_browser_runtime_capabilities(
        &mut capability_client,
        browser_service_config.auth_token.as_deref(),
    )
    .await;
    if browser_resilience_rollout_mismatch(
        runtime_state.config.feature_rollouts.browser_resilience.enabled,
        &browser_runtime_capabilities,
    ) {
        let output = json!({
            "success": false,
            "error": "browser_resilience_rollout_mismatch",
            "reason_code": "browser.resilience.rollout_mismatch",
            "browser_runtime": browser_runtime_capabilities.to_json(),
        });
        return browser_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
            "browserd automatic reconnect requires feature_rollouts.browser_resilience".to_owned(),
        );
    }
    let caller_principal_interceptor = match browser_caller_principal_interceptor(
        principal,
        browser_service_config.auth_token.as_deref(),
    ) {
        Ok(interceptor) => interceptor,
        Err(error) => {
            return browser_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };
    let mut client = browser_v1::browser_service_client::BrowserServiceClient::with_interceptor(
        browser_service_channel,
        caller_principal_interceptor,
    );

    // Every arm evaluates to (success, output_json, error); shared
    // post-processing below the match attaches capabilities, recovery hints,
    // and missing-session normalization.
    let outcome = match tool_name {
        BROWSER_SESSION_CREATE_TOOL_NAME => {
            let idle_ttl_ms = payload.get("idle_ttl_ms").and_then(Value::as_u64).unwrap_or(0);
            let (profile_id, ignored_profile_id) =
                match browser_session_profile_id_from_payload(&payload) {
                    Ok(value) => value,
                    Err(error) => {
                        return browser_tool_execution_outcome(
                            proposal_id,
                            input_json,
                            false,
                            b"{}".to_vec(),
                            error,
                        );
                    }
                };
            // Budget fields left at zero are sentinels: browserd substitutes
            // its own defaults and hard caps, and the response reports the
            // effective budget actually applied.
            let budget = payload.get("budget").and_then(Value::as_object).map(|value| {
                browser_v1::SessionBudget {
                    max_navigation_timeout_ms: value
                        .get("max_navigation_timeout_ms")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_session_lifetime_ms: value
                        .get("max_session_lifetime_ms")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_screenshot_bytes: value
                        .get("max_screenshot_bytes")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_response_bytes: value
                        .get("max_response_bytes")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_action_timeout_ms: value
                        .get("max_action_timeout_ms")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_type_input_bytes: value
                        .get("max_type_input_bytes")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_actions_per_session: value
                        .get("max_actions_per_session")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_actions_per_window: value
                        .get("max_actions_per_window")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    action_rate_window_ms: value
                        .get("action_rate_window_ms")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_action_log_entries: value
                        .get("max_action_log_entries")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_observe_snapshot_bytes: value
                        .get("max_observe_snapshot_bytes")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_visible_text_bytes: value
                        .get("max_visible_text_bytes")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_network_log_entries: value
                        .get("max_network_log_entries")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_network_log_bytes: value
                        .get("max_network_log_bytes")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                }
            });
            let (persistence_enabled, persistence_id) =
                match browser_session_persistence_from_payload(&payload, context.session_id) {
                    Ok(value) => value,
                    Err(error) => {
                        return browser_tool_execution_outcome(
                            proposal_id,
                            input_json,
                            false,
                            b"{}".to_vec(),
                            error,
                        );
                    }
                };
            let mut request = Request::new(browser_v1::CreateSessionRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                principal: principal.to_owned(),
                idle_ttl_ms,
                budget,
                allow_private_targets: browser_private_targets_requested(&payload),
                allow_downloads: payload
                    .get("allow_downloads")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                action_allowed_domains: payload
                    .get("action_allowed_domains")
                    .and_then(Value::as_array)
                    .map(|entries| {
                        entries
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .map(str::to_owned)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default(),
                persistence_enabled,
                persistence_id,
                profile_id,
                private_profile: payload
                    .get("private_profile")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                channel: channel.unwrap_or_default().to_owned(),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.create_session(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let session_id = response.session_id.map(|value| value.ulid);
                    let profile_id_warning = ignored_profile_id.as_ref().map(|value| {
                        format!(
                            "ignored non-canonical profile_id '{value}'; session was created without a browser profile"
                        )
                    });
                    let output = json!({
                        "session_id": session_id,
                        "created_at_unix_ms": response.created_at_unix_ms,
                        "effective_budget": response.effective_budget.map(|value| json!({
                            "max_navigation_timeout_ms": value.max_navigation_timeout_ms,
                            "max_session_lifetime_ms": value.max_session_lifetime_ms,
                            "max_screenshot_bytes": value.max_screenshot_bytes,
                            "max_response_bytes": value.max_response_bytes,
                            "max_action_timeout_ms": value.max_action_timeout_ms,
                            "max_type_input_bytes": value.max_type_input_bytes,
                            "max_actions_per_session": value.max_actions_per_session,
                            "max_actions_per_window": value.max_actions_per_window,
                            "action_rate_window_ms": value.action_rate_window_ms,
                            "max_action_log_entries": value.max_action_log_entries,
                            "max_observe_snapshot_bytes": value.max_observe_snapshot_bytes,
                            "max_visible_text_bytes": value.max_visible_text_bytes,
                            "max_network_log_entries": value.max_network_log_entries,
                            "max_network_log_bytes": value.max_network_log_bytes,
                        })),
                        "downloads_enabled": response.downloads_enabled,
                        "action_allowed_domains": response.action_allowed_domains,
                        "persistence_enabled": response.persistence_enabled,
                        "persistence_id": response.persistence_id,
                        "state_restored": response.state_restored,
                        "profile_id": response.profile_id.map(|value| value.ulid),
                        "ignored_profile_id": ignored_profile_id,
                        "profile_id_warning": profile_id_warning,
                        "private_profile": response.private_profile,
                    });
                    (
                        true,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        String::new(),
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.session.create failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_SESSION_CLOSE_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::CloseSessionRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.close_session(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    if response.closed {
                        // Feed the daemon-side ledger so later calls on this
                        // session short-circuit before reaching browserd.
                        runtime_state.record_closed_browser_session(session_id.as_str());
                    }
                    let output = json!({
                        "closed": response.closed,
                        "reason": response.reason,
                    });
                    (
                        response.closed,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.closed {
                            String::new()
                        } else {
                            "browser session was not closed".to_owned()
                        },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.session.close failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_NAVIGATE_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let Some(url) = payload.get("url").and_then(Value::as_str).map(str::trim) else {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.navigate requires non-empty string field 'url'".to_owned(),
                );
            };
            if url.is_empty() {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.navigate requires non-empty string field 'url'".to_owned(),
                );
            }
            let navigation_url =
                match resolve_browser_navigation_url(runtime_state, context, url).await {
                    Ok(value) => value,
                    Err(error) => {
                        return browser_tool_execution_outcome(
                            proposal_id,
                            input_json,
                            false,
                            b"{}".to_vec(),
                            error,
                        );
                    }
                };
            let allow_private_targets = browser_private_target_flag_for_validated_url(
                navigation_url.transport_url.as_str(),
                browser_private_targets_requested(&payload),
            );
            let mut request = Request::new(browser_v1::NavigateRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                url: navigation_url.transport_url.clone(),
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                allow_redirects: payload
                    .get("allow_redirects")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_redirects: browser_max_redirects_from_payload(&payload),
                allow_private_targets,
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.navigate(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let final_url =
                        navigation_url.project_response_url(response.final_url.as_str());
                    let output = json!({
                        "success": response.success,
                        "final_url": final_url,
                        "status_code": response.status_code,
                        "title": response.title,
                        "body_bytes": response.body_bytes,
                        "latency_ms": response.latency_ms,
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.navigate failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        // browserd has no native reload RPC: reload binds the model-visible
        // expected_url to the active tab URL, then re-navigates it with the
        // same file-URL and private-target checks as a fresh navigation.
        BROWSER_RELOAD_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let expected_url = match browser_reload_expected_url_from_payload(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut get_request = Request::new(browser_v1::GetSessionRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut get_request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            if let Err(error) =
                attach_browser_caller_principal_metadata(&mut get_request, principal)
            {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            let current_url = match client.get_session(get_request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    if !response.success {
                        let error = if response.error.trim().is_empty() {
                            "session_not_found".to_owned()
                        } else {
                            response.error
                        };
                        return browser_tool_execution_outcome(
                            proposal_id,
                            input_json,
                            false,
                            json!({"success": false, "session_id": session_id, "error": error.clone()})
                                .to_string()
                                .into_bytes(),
                            format!("palyra.browser.reload failed: {error}"),
                        );
                    }
                    let Some(summary) = response.session.and_then(|session| session.summary) else {
                        return browser_tool_execution_outcome(
                            proposal_id,
                            input_json,
                            false,
                            b"{}".to_vec(),
                            "palyra.browser.reload failed: session response did not include active tab state"
                                .to_owned(),
                        );
                    };
                    let active_url = summary.active_tab_url.trim().to_owned();
                    if active_url.is_empty() {
                        return browser_tool_execution_outcome(
                            proposal_id,
                            input_json,
                            false,
                            json!({
                                "success": false,
                                "session_id": session_id,
                                "error": "active_tab_url_missing"
                            })
                            .to_string()
                            .into_bytes(),
                            "palyra.browser.reload requires a previously navigated active tab"
                                .to_owned(),
                        );
                    }
                    active_url
                }
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        format!(
                            "palyra.browser.reload failed: {}",
                            sanitize_status_message(&error)
                        ),
                    );
                }
            };
            let expected_navigation_url =
                match resolve_browser_navigation_url(runtime_state, context, expected_url.as_str())
                    .await
                {
                    Ok(value) => value,
                    Err(error) => {
                        return browser_tool_execution_outcome(
                            proposal_id,
                            input_json,
                            false,
                            b"{}".to_vec(),
                            error,
                        );
                    }
                };
            let current_navigation_url =
                match resolve_browser_navigation_url(runtime_state, context, current_url.as_str())
                    .await
                {
                    Ok(value) => value,
                    Err(error) => {
                        return browser_tool_execution_outcome(
                            proposal_id,
                            input_json,
                            false,
                            b"{}".to_vec(),
                            error,
                        );
                    }
                };
            if !current_navigation_url.same_destination(&expected_navigation_url) {
                let output = json!({
                    "success": false,
                    "session_id": session_id,
                    "expected_url": expected_navigation_url.model_url,
                    "active_url": current_navigation_url.model_url,
                    "error": "active_tab_url_mismatch",
                });
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                    "palyra.browser.reload expected_url does not match the active tab URL"
                        .to_owned(),
                );
            }
            let allow_private_targets = browser_reload_private_target_flag_for_validated_url(
                current_navigation_url.transport_url.as_str(),
                &payload,
            );
            let requested_url = expected_navigation_url.model_url.clone();
            let mut request = Request::new(browser_v1::NavigateRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                url: current_navigation_url.transport_url,
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                allow_redirects: payload
                    .get("allow_redirects")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_redirects: browser_max_redirects_from_payload(&payload),
                allow_private_targets,
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.navigate(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let final_url =
                        expected_navigation_url.project_response_url(response.final_url.as_str());
                    let output = json!({
                        "success": response.success,
                        "reloaded": response.success,
                        "requested_url": requested_url,
                        "final_url": final_url,
                        "status_code": response.status_code,
                        "title": response.title,
                        "body_bytes": response.body_bytes,
                        "latency_ms": response.latency_ms,
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.reload failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_CLICK_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let Some(selector) = payload.get("selector").and_then(Value::as_str).map(str::trim)
            else {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.click requires non-empty string field 'selector'".to_owned(),
                );
            };
            if selector.is_empty() {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.click requires non-empty string field 'selector'".to_owned(),
                );
            }
            let mut request = Request::new(browser_v1::ClickRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                selector: selector.to_owned(),
                max_retries: payload.get("max_retries").and_then(Value::as_u64).unwrap_or(0) as u32,
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                capture_failure_screenshot: payload
                    .get("capture_failure_screenshot")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_failure_screenshot_bytes: payload
                    .get("max_failure_screenshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(browser_service_config.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.click(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "error": response.error,
                        "action_log": response.action_log.map(browser_action_log_to_json),
                        "failure_screenshot": browser_failure_screenshot_metadata(
                            response.failure_screenshot_mime_type.as_str(),
                            response.failure_screenshot_bytes.as_slice(),
                        ),
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64_omitted": true,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.click failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_TYPE_TOOL_NAME | BROWSER_FILL_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let Some(selector) = payload.get("selector").and_then(Value::as_str).map(str::trim)
            else {
                let action = browser_text_entry_action_name(tool_name);
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.{action} requires non-empty string field 'selector'"),
                );
            };
            if selector.is_empty() {
                let action = browser_text_entry_action_name(tool_name);
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.{action} requires non-empty string field 'selector'"),
                );
            }
            let text = payload.get("text").and_then(Value::as_str).unwrap_or_default();
            let clear_existing = tool_name == BROWSER_FILL_TOOL_NAME
                || payload.get("clear_existing").and_then(Value::as_bool).unwrap_or(false);
            let mut request = Request::new(browser_v1::TypeRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                selector: selector.to_owned(),
                text: text.to_owned(),
                clear_existing,
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                capture_failure_screenshot: payload
                    .get("capture_failure_screenshot")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_failure_screenshot_bytes: payload
                    .get("max_failure_screenshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(browser_service_config.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.r#type(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "typed_bytes": response.typed_bytes,
                        "error": response.error,
                        "action_log": response.action_log.map(browser_action_log_to_json),
                        "failure_screenshot": browser_failure_screenshot_metadata(
                            response.failure_screenshot_mime_type.as_str(),
                            response.failure_screenshot_bytes.as_slice(),
                        ),
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64_omitted": true,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.{} failed: {}",
                        browser_text_entry_action_name(tool_name),
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_UPLOAD_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let Some(selector) = payload.get("selector").and_then(Value::as_str).map(str::trim)
            else {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!(
                        "{BROWSER_UPLOAD_TOOL_NAME} requires non-empty string field 'selector'"
                    ),
                );
            };
            if selector.is_empty() {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!(
                        "{BROWSER_UPLOAD_TOOL_NAME} requires non-empty string field 'selector'"
                    ),
                );
            }
            let file_path = match browser_upload_path_from_payload(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let (file_name, file_bytes) =
                match read_browser_upload_file(runtime_state, context, file_path).await {
                    Ok(value) => value,
                    Err(error) => {
                        return browser_tool_execution_outcome(
                            proposal_id,
                            input_json,
                            false,
                            b"{}".to_vec(),
                            error,
                        );
                    }
                };
            let mut request = Request::new(browser_v1::SetFileInputRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                selector: selector.to_owned(),
                file_name,
                file_bytes,
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                capture_failure_screenshot: payload
                    .get("capture_failure_screenshot")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_failure_screenshot_bytes: payload
                    .get("max_failure_screenshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(browser_service_config.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.set_file_input(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let success = response.success;
                    let error = response.error.clone();
                    let output = json!({
                        "success": success,
                        "selector": selector,
                        "file_name": response.uploaded_file_name,
                        "uploaded_file_bytes": response.uploaded_file_bytes,
                        "error": response.error,
                        "action_log": response.action_log.map(browser_action_log_to_json),
                        "failure_screenshot": browser_failure_screenshot_metadata(
                            response.failure_screenshot_mime_type.as_str(),
                            response.failure_screenshot_bytes.as_slice(),
                        ),
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64_omitted": true,
                    });
                    (
                        success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if success { String::new() } else { error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.upload failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_PRESS_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let Some(raw_key) = payload.get("key").and_then(Value::as_str) else {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.press requires non-empty string field 'key'".to_owned(),
                );
            };
            let key = normalize_browser_press_key_input(raw_key);
            if key.is_empty() {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.press requires non-empty string field 'key'".to_owned(),
                );
            }
            let mut request = Request::new(browser_v1::PressRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                key,
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                capture_failure_screenshot: payload
                    .get("capture_failure_screenshot")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_failure_screenshot_bytes: payload
                    .get("max_failure_screenshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(browser_service_config.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.press(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "key": response.key,
                        "error": response.error,
                        "action_log": response.action_log.map(browser_action_log_to_json),
                        "failure_screenshot": browser_failure_screenshot_metadata(
                            response.failure_screenshot_mime_type.as_str(),
                            response.failure_screenshot_bytes.as_slice(),
                        ),
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64_omitted": true,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.press failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_SELECT_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let Some(selector) = payload.get("selector").and_then(Value::as_str).map(str::trim)
            else {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.select requires non-empty string field 'selector'".to_owned(),
                );
            };
            if selector.is_empty() {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.select requires non-empty string field 'selector'".to_owned(),
                );
            }
            let Some(value) = payload.get("value").and_then(Value::as_str).map(str::trim) else {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.select requires non-empty string field 'value'".to_owned(),
                );
            };
            if value.is_empty() {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.select requires non-empty string field 'value'".to_owned(),
                );
            }
            let mut request = Request::new(browser_v1::SelectRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                selector: selector.to_owned(),
                value: value.to_owned(),
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                capture_failure_screenshot: payload
                    .get("capture_failure_screenshot")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_failure_screenshot_bytes: payload
                    .get("max_failure_screenshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(browser_service_config.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.select(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "selected_value": response.selected_value,
                        "error": response.error,
                        "action_log": response.action_log.map(browser_action_log_to_json),
                        "failure_screenshot": browser_failure_screenshot_metadata(
                            response.failure_screenshot_mime_type.as_str(),
                            response.failure_screenshot_bytes.as_slice(),
                        ),
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64_omitted": true,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.select failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_VIEWPORT_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let Some(width) = payload
                .get("width")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
            else {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.viewport requires integer field 'width'".to_owned(),
                );
            };
            let Some(height) = payload
                .get("height")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
            else {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.viewport requires integer field 'height'".to_owned(),
                );
            };
            let device_scale_factor =
                payload.get("device_scale_factor").and_then(Value::as_f64).unwrap_or(0.0);
            let mobile = payload.get("mobile").and_then(Value::as_bool).unwrap_or(false);
            let mut request = Request::new(browser_v1::SetViewportRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                width,
                height,
                device_scale_factor,
                mobile,
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.set_viewport(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let response_width = response.width;
                    let response_height = response.height;
                    // Engines may silently clamp the viewport. Treat any
                    // mismatch as failure so responsive/mobile visual
                    // assertions are never made against the wrong geometry.
                    let mismatch_error = response.success.then(|| {
                        browser_viewport_metric_mismatch_error(
                            width,
                            height,
                            response_width,
                            response_height,
                        )
                    });
                    let error = mismatch_error.flatten().unwrap_or_else(|| response.error.clone());
                    let success = response.success && error.is_empty();
                    let mut output = json!({
                        "success": success,
                        "browser_service_success": response.success,
                        "width": response.width,
                        "height": response.height,
                        "requested_width": width,
                        "requested_height": height,
                        "device_scale_factor": response.device_scale_factor,
                        "mobile": response.mobile,
                        "requested_mobile": mobile,
                        "error": response.error,
                        "action_log": response.action_log.map(browser_action_log_to_json),
                    });
                    if !success && response.success {
                        output["error"] = json!(error);
                        output["viewport_error"] = json!(
                            "reported viewport differs from requested viewport; mobile or responsive visual assertions are unverified"
                        );
                    }
                    (
                        success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if success { String::new() } else { error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.viewport failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_HIGHLIGHT_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let Some(selector) = payload.get("selector").and_then(Value::as_str).map(str::trim)
            else {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.highlight requires non-empty string field 'selector'"
                        .to_owned(),
                );
            };
            if selector.is_empty() {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.highlight requires non-empty string field 'selector'"
                        .to_owned(),
                );
            }
            let mut request = Request::new(browser_v1::HighlightRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                selector: selector.to_owned(),
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                duration_ms: payload.get("duration_ms").and_then(Value::as_u64).unwrap_or(0),
                capture_failure_screenshot: payload
                    .get("capture_failure_screenshot")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_failure_screenshot_bytes: payload
                    .get("max_failure_screenshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(browser_service_config.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.highlight(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "selector": response.selector,
                        "error": response.error,
                        "action_log": response.action_log.map(browser_action_log_to_json),
                        "failure_screenshot": browser_failure_screenshot_metadata(
                            response.failure_screenshot_mime_type.as_str(),
                            response.failure_screenshot_bytes.as_slice(),
                        ),
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64_omitted": true,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.highlight failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_SCROLL_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::ScrollRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                delta_x: payload.get("delta_x").and_then(Value::as_i64).unwrap_or(0),
                delta_y: payload.get("delta_y").and_then(Value::as_i64).unwrap_or(0),
                capture_failure_screenshot: payload
                    .get("capture_failure_screenshot")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_failure_screenshot_bytes: payload
                    .get("max_failure_screenshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(browser_service_config.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.scroll(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "scroll_x": response.scroll_x,
                        "scroll_y": response.scroll_y,
                        "error": response.error,
                        "action_log": response.action_log.map(browser_action_log_to_json),
                        "failure_screenshot": browser_failure_screenshot_metadata(
                            response.failure_screenshot_mime_type.as_str(),
                            response.failure_screenshot_bytes.as_slice(),
                        ),
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64_omitted": true,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.scroll failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_WAIT_FOR_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::WaitForRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                selector: payload
                    .get("selector")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                text: payload.get("text").and_then(Value::as_str).unwrap_or_default().to_owned(),
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                poll_interval_ms: payload
                    .get("poll_interval_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                capture_failure_screenshot: payload
                    .get("capture_failure_screenshot")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_failure_screenshot_bytes: payload
                    .get("max_failure_screenshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(browser_service_config.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.wait_for(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "waited_ms": response.waited_ms,
                        "error": response.error,
                        "matched_selector": response.matched_selector,
                        "matched_text": response.matched_text,
                        "action_log": response.action_log.map(browser_action_log_to_json),
                        "failure_screenshot": browser_failure_screenshot_metadata(
                            response.failure_screenshot_mime_type.as_str(),
                            response.failure_screenshot_bytes.as_slice(),
                        ),
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64_omitted": true,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.wait_for failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_TITLE_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::GetTitleRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                max_title_bytes: payload
                    .get("max_title_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(browser_service_config.max_title_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.get_title(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let title_export = export_browser_text(
                        response.title.as_str(),
                        SafetyContentKind::BrowserTitle,
                    );
                    let output = json!({
                        "success": response.success,
                        "title": title_export.redacted_text,
                        "safety": browser_safety_json(&title_export.scan, title_export.redacted),
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.title failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_SCREENSHOT_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::ScreenshotRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                max_bytes: payload
                    .get("max_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(browser_service_config.max_screenshot_bytes as u64),
                format: payload.get("format").and_then(Value::as_str).unwrap_or("png").to_owned(),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.screenshot(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let image_sha256 = if response.success {
                        hex::encode(Sha256::digest(response.image_bytes.as_slice()))
                    } else {
                        String::new()
                    };
                    let mut success = response.success;
                    let mut error = response.error.clone();
                    // A failed save fails the call: never report an artifact
                    // that was not written (pdf and downloads.get likewise).
                    let saved_file = if response.success {
                        match save_browser_output_file_from_payload(
                            runtime_state,
                            context,
                            &payload,
                            BROWSER_SCREENSHOT_TOOL_NAME,
                            response.mime_type.as_str(),
                            response.image_bytes.as_slice(),
                        )
                        .await
                        {
                            Ok(saved_file) => saved_file,
                            Err(save_error) => {
                                success = false;
                                error = save_error;
                                None
                            }
                        }
                    } else {
                        None
                    };
                    let image_observation =
                        browser_screenshot_image_observation_hint(saved_file.as_ref());
                    let output = json!({
                        "success": success,
                        "mime_type": response.mime_type,
                        "size_bytes": response.image_bytes.len(),
                        "sha256": image_sha256,
                        "saved_file": saved_file,
                        "layout_metrics": response.layout_metrics.map(browser_layout_metrics_to_json),
                        "image_base64_omitted": true,
                        "image_observation": image_observation,
                        "error": error,
                    });
                    (
                        success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if success { String::new() } else { error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.screenshot failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_PDF_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::ExportPdfRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                max_bytes: payload.get("max_bytes").and_then(Value::as_u64).unwrap_or(0),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            if let Err(error) = attach_browser_caller_principal_metadata(&mut request, principal) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.export_pdf(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let mut success = response.success;
                    let mut error = response.error.clone();
                    let saved_file = if response.success {
                        match save_browser_output_file_from_payload(
                            runtime_state,
                            context,
                            &payload,
                            BROWSER_PDF_TOOL_NAME,
                            response.mime_type.as_str(),
                            response.pdf_bytes.as_slice(),
                        )
                        .await
                        {
                            Ok(saved_file) => saved_file,
                            Err(save_error) => {
                                success = false;
                                error = save_error;
                                None
                            }
                        }
                    } else {
                        None
                    };
                    let output = json!({
                        "success": success,
                        "mime_type": response.mime_type,
                        "size_bytes": response.size_bytes,
                        "sha256": response.sha256,
                        "artifact": response.artifact.map(browser_download_artifact_to_json),
                        "saved_file": saved_file,
                        "pdf_base64": STANDARD.encode(response.pdf_bytes.as_slice()),
                        "error": error,
                    });
                    (
                        success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if success { String::new() } else { error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.pdf failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_OBSERVE_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let capture_selectors = match parse_browser_observe_string_array(
                &payload,
                "capture_selectors",
                BROWSER_OBSERVE_MAX_CAPTURE_SELECTORS,
            ) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let computed_style_properties = match parse_browser_observe_string_array(
                &payload,
                "computed_style_properties",
                BROWSER_OBSERVE_MAX_COMPUTED_STYLE_PROPERTIES,
            ) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::ObserveRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                include_dom_snapshot: payload
                    .get("include_dom_snapshot")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                include_accessibility_tree: payload
                    .get("include_accessibility_tree")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                include_visible_text: browser_observe_include_visible_text(&payload),
                max_dom_snapshot_bytes: payload
                    .get("max_dom_snapshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                max_accessibility_tree_bytes: payload
                    .get("max_accessibility_tree_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                max_visible_text_bytes: payload
                    .get("max_visible_text_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                capture_selectors,
                computed_style_properties,
                max_capture_text_bytes: payload
                    .get("max_capture_text_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.observe(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let dom_export = export_browser_text(
                        response.dom_snapshot.as_str(),
                        SafetyContentKind::BrowserObservation,
                    );
                    let accessibility_export = export_browser_text(
                        response.accessibility_tree.as_str(),
                        SafetyContentKind::BrowserObservation,
                    );
                    let visible_text_export = export_browser_text(
                        response.visible_text.as_str(),
                        SafetyContentKind::BrowserObservation,
                    );
                    let (element_captures, capture_scans, capture_redacted) =
                        browser_element_captures_to_json(response.element_captures.as_slice());
                    let page_url = redact_url(response.page_url.as_str());
                    let mut observation_scans = vec![
                        dom_export.scan.clone(),
                        accessibility_export.scan.clone(),
                        visible_text_export.scan.clone(),
                    ];
                    observation_scans.extend(capture_scans);
                    let observation_scan = merge_scan_results(
                        SafetyPhase::Export,
                        SafetySourceKind::Browser,
                        SafetyContentKind::BrowserObservation,
                        observation_scans.as_slice(),
                    );
                    let output = json!({
                        "success": response.success,
                        "dom_snapshot": dom_export.redacted_text,
                        "accessibility_tree": accessibility_export.redacted_text,
                        "visible_text": visible_text_export.redacted_text,
                        "element_captures": element_captures,
                        "dom_truncated": response.dom_truncated,
                        "accessibility_tree_truncated": response.accessibility_tree_truncated,
                        "visible_text_truncated": response.visible_text_truncated,
                        "page_url": page_url,
                        "safety": browser_safety_json(
                            &observation_scan,
                            dom_export.redacted
                                || accessibility_export.redacted
                                || visible_text_export.redacted
                                || capture_redacted
                                || page_url != response.page_url,
                        ),
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.observe failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_VISION_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let Some(question) = payload.get("question").and_then(Value::as_str).map(str::trim)
            else {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!(
                        "{BROWSER_VISION_TOOL_NAME} requires non-empty string field 'question'"
                    ),
                );
            };
            if question.is_empty() {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!(
                        "{BROWSER_VISION_TOOL_NAME} requires non-empty string field 'question'"
                    ),
                );
            }
            let mut request = Request::new(browser_v1::ScreenshotRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
                max_bytes: payload
                    .get("max_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(browser_service_config.max_screenshot_bytes as u64),
                format: "png".to_owned(),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.screenshot(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let screenshot_metadata = browser_image_bytes_metadata(
                        response.mime_type.as_str(),
                        response.image_bytes.as_slice(),
                    );
                    let error_code = if response.success {
                        BROWSER_VISION_UNSUPPORTED_ERROR
                    } else {
                        "browser_screenshot_failed"
                    };
                    let error = if response.success {
                        "browser screenshot captured, but no OCR/vision bridge is configured"
                            .to_owned()
                    } else {
                        response.error.clone()
                    };
                    let output = json!({
                        "success": false,
                        "session_id": session_id,
                        "error": error_code,
                        "error_code": error_code,
                        "question_sha256": hex::encode(Sha256::digest(question.as_bytes())),
                        "screenshot": screenshot_metadata,
                        "layout_metrics": response.layout_metrics.map(browser_layout_metrics_to_json),
                        "vision_status": "unsupported",
                        "provider_handoff_available": false,
                        "raw_image_bytes_model_visible": false,
                        "image_base64_omitted": true,
                        "next_action": "save a screenshot with palyra.browser.screenshot output_path and use a configured OCR/vision runtime before claiming visual facts",
                    });
                    (
                        false,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        format!("{BROWSER_VISION_TOOL_NAME} failed: {error}"),
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.vision failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_IMAGES_LIST_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let max_count = browser_images_list_max_count(&payload);
            let mut request = Request::new(browser_v1::ObserveRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                include_dom_snapshot: true,
                include_accessibility_tree: false,
                include_visible_text: false,
                max_dom_snapshot_bytes: payload
                    .get("max_dom_snapshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(BROWSER_IMAGES_LIST_DEFAULT_DOM_BYTES),
                max_accessibility_tree_bytes: 0,
                max_visible_text_bytes: 0,
                capture_selectors: Vec::new(),
                computed_style_properties: Vec::new(),
                max_capture_text_bytes: 0,
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.observe(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let (images, image_count_truncated) = browser_image_tags_from_dom_snapshot(
                        response.dom_snapshot.as_str(),
                        max_count,
                    );
                    let image_count = images.len();
                    let output = json!({
                        "success": response.success,
                        "source": "browser.observe.dom_snapshot",
                        "page_url": redact_url(response.page_url.as_str()),
                        "images": images,
                        "image_count": image_count,
                        "image_count_truncated": image_count_truncated,
                        "dom_truncated": response.dom_truncated,
                        "artifact_refs_available": false,
                        "raw_image_bytes_model_visible": false,
                        "image_base64_omitted": true,
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.images.list failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_DIALOG_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let action = payload
                .get("action")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("inspect");
            if !matches!(action, "inspect" | "accept" | "dismiss" | "respond") {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!(
                        "{BROWSER_DIALOG_TOOL_NAME} action must be inspect, accept, dismiss, or respond"
                    ),
                );
            }
            let expected_generation =
                payload.get("expected_generation").and_then(Value::as_u64).unwrap_or(0);
            if action != "inspect" && expected_generation == 0 {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!(
                        "{BROWSER_DIALOG_TOOL_NAME} mutating actions require expected_generation from a prior inspect"
                    ),
                );
            }
            let proto_action = match action {
                "inspect" => browser_v1::BrowserDialogAction::Inspect,
                "accept" => browser_v1::BrowserDialogAction::Accept,
                "dismiss" => browser_v1::BrowserDialogAction::Dismiss,
                "respond" => browser_v1::BrowserDialogAction::Respond,
                _ => unreachable!("dialog action validated above"),
            };
            let mut request = Request::new(browser_v1::HandleDialogRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
                action: proto_action.into(),
                expected_generation,
                prompt_text: payload
                    .get("prompt_text")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.handle_dialog(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let dialog = response.event.map(|event| {
                        json!({
                            "generation": event.generation,
                            "tab_id": event.tab_id.map(|value| value.ulid).unwrap_or_default(),
                            "dialog_type": event.dialog_type,
                            "message": event.message,
                            "default_prompt": event.default_prompt,
                            "page_url": event.page_url,
                            "opened_at_unix_ms": event.opened_at_unix_ms,
                            "expires_at_unix_ms": event.expires_at_unix_ms,
                        })
                    });
                    let error = response.error.clone();
                    let output = json!({
                        "success": response.success,
                        "session_id": session_id,
                        "action": action,
                        "dialog_present": response.present,
                        "dialog": dialog,
                        "blocking_status": if response.present { "blocked" } else { "clear" },
                        "backend_support": response.backend_support,
                        "mutated_page": response.mutated_page,
                        "timed_out": response.timed_out,
                        "error_code": response.error_code,
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success {
                            String::new()
                        } else {
                            format!("{BROWSER_DIALOG_TOOL_NAME} failed: {error}")
                        },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "{BROWSER_DIALOG_TOOL_NAME} failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_CDP_INVOKE_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let Some(method) = payload.get("method").and_then(Value::as_str).map(str::trim) else {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!(
                        "{BROWSER_CDP_INVOKE_TOOL_NAME} requires non-empty string field 'method'"
                    ),
                );
            };
            if method.is_empty() {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!(
                        "{BROWSER_CDP_INVOKE_TOOL_NAME} requires non-empty string field 'method'"
                    ),
                );
            }
            let (error_code, next_action) = if browser_cdp_method_allowed(method) {
                (
                    "cdp_backend_unavailable",
                    "retry only after browserd exposes a bounded CDP invoke RPC for this read-only method",
                )
            } else {
                (
                    "cdp_method_denied",
                    "use palyra.browser.observe or another first-class browser tool instead of requesting a non-allowlisted CDP method",
                )
            };
            let output = json!({
                "success": false,
                "session_id": session_id,
                "method": method,
                "error": error_code,
                "error_code": error_code,
                "allowlisted": browser_cdp_method_allowed(method),
                "backend_support": false,
                "raw_protocol_result_model_visible": false,
                "next_action": next_action,
            });
            (
                false,
                serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                format!("{BROWSER_CDP_INVOKE_TOOL_NAME} failed: {error_code}"),
            )
        }
        BROWSER_STORAGE_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::InspectSessionRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                include_cookies: true,
                include_storage: true,
                include_action_log: false,
                include_network_log: false,
                include_page_snapshot: false,
                max_cookie_bytes: payload
                    .get("max_cookie_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                max_storage_bytes: payload
                    .get("max_storage_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                max_action_log_entries: 0,
                max_network_log_entries: 0,
                max_network_log_bytes: 0,
                max_dom_snapshot_bytes: 0,
                max_visible_text_bytes: 0,
                include_console_log: false,
                include_page_diagnostics: false,
                max_console_log_entries: 0,
                max_console_log_bytes: 0,
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            if let Err(error) = attach_browser_caller_principal_metadata(&mut request, principal) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.inspect_session(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let exported_cookies = response
                        .cookies
                        .into_iter()
                        .map(browser_cookie_domain_to_json)
                        .collect::<Vec<_>>();
                    let exported_storage = response
                        .storage
                        .into_iter()
                        .map(browser_storage_origin_to_json)
                        .collect::<Vec<_>>();
                    let mut exported_values = Vec::with_capacity(
                        exported_cookies.len().saturating_add(exported_storage.len()),
                    );
                    exported_values.extend(exported_cookies.iter().map(|entry| {
                        BrowserValueExport {
                            value: entry.value.clone(),
                            scan: entry.scan.clone(),
                            redacted: entry.redacted,
                        }
                    }));
                    exported_values.extend(exported_storage.iter().map(|entry| {
                        BrowserValueExport {
                            value: entry.value.clone(),
                            scan: entry.scan.clone(),
                            redacted: entry.redacted,
                        }
                    }));
                    let storage_scan = merge_browser_value_scans(
                        SafetyContentKind::BrowserObservation,
                        exported_values.as_slice(),
                    );
                    let output = json!({
                        "success": response.success,
                        "cookies": exported_cookies.iter().map(|entry| entry.value.clone()).collect::<Vec<_>>(),
                        "storage": exported_storage.iter().map(|entry| entry.value.clone()).collect::<Vec<_>>(),
                        "cookies_truncated": response.cookies_truncated,
                        "storage_truncated": response.storage_truncated,
                        "safety": browser_safety_json(
                            &storage_scan,
                            exported_values.iter().any(|entry| entry.redacted),
                        ),
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.storage failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_NETWORK_LOG_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let since_unix_ms = payload.get("since_unix_ms").and_then(Value::as_u64).unwrap_or(0);
            let mut request = Request::new(browser_v1::NetworkLogRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                limit: payload.get("limit").and_then(Value::as_u64).unwrap_or(0) as u32,
                include_headers: payload
                    .get("include_headers")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                max_payload_bytes: payload
                    .get("max_payload_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            if let Err(error) = attach_browser_caller_principal_metadata(&mut request, principal) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.network_log(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let original_entry_count = response.entries.len();
                    let entries =
                        filter_browser_network_log_entries_since(response.entries, since_unix_ms);
                    let filtered_before_since_count =
                        original_entry_count.saturating_sub(entries.len());
                    let exported_entries = entries
                        .into_iter()
                        .map(browser_network_log_entry_to_json)
                        .collect::<Vec<_>>();
                    let network_scan = merge_browser_value_scans(
                        SafetyContentKind::BrowserNetwork,
                        exported_entries.as_slice(),
                    );
                    let output = json!({
                        "success": response.success,
                        "entries": exported_entries.iter().map(|entry| entry.value.clone()).collect::<Vec<_>>(),
                        "truncated": response.truncated,
                        "since_unix_ms": since_unix_ms,
                        "filtered_before_since_count": filtered_before_since_count,
                        "safety": browser_safety_json(
                            &network_scan,
                            exported_entries.iter().any(|entry| entry.redacted),
                        ),
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.network_log failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_CONSOLE_LOG_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let minimum_severity =
                match parse_browser_diagnostic_severity(&payload, "minimum_severity") {
                    Ok(value) => value,
                    Err(error) => {
                        return browser_tool_execution_outcome(
                            proposal_id,
                            input_json,
                            false,
                            b"{}".to_vec(),
                            error,
                        );
                    }
                };
            let mut request = Request::new(browser_v1::ConsoleLogRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                limit: payload.get("limit").and_then(Value::as_u64).unwrap_or(0) as u32,
                minimum_severity,
                include_page_diagnostics: payload
                    .get("include_page_diagnostics")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                max_payload_bytes: payload
                    .get("max_payload_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            if let Err(error) = attach_browser_caller_principal_metadata(&mut request, principal) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.console_log(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let exported_entries = response
                        .entries
                        .into_iter()
                        .map(browser_console_entry_to_json)
                        .collect::<Vec<_>>();
                    let page_diagnostics =
                        response.page_diagnostics.map(browser_page_diagnostics_to_json);
                    let mut scans =
                        exported_entries.iter().map(|entry| entry.scan.clone()).collect::<Vec<_>>();
                    if let Some(diagnostics) = page_diagnostics.as_ref() {
                        scans.push(diagnostics.scan.clone());
                    }
                    let console_scan = if scans.is_empty() {
                        export_browser_text("", SafetyContentKind::BrowserConsole).scan
                    } else {
                        merge_scan_results(
                            SafetyPhase::Export,
                            SafetySourceKind::Browser,
                            SafetyContentKind::BrowserConsole,
                            scans.as_slice(),
                        )
                    };
                    let output = json!({
                        "success": response.success,
                        "entries": exported_entries.iter().map(|entry| entry.value.clone()).collect::<Vec<_>>(),
                        "truncated": response.truncated,
                        "page_diagnostics": page_diagnostics.as_ref().map(|value| value.value.clone()),
                        "safety": browser_safety_json(
                            &console_scan,
                            exported_entries.iter().any(|entry| entry.redacted)
                                || page_diagnostics.as_ref().is_some_and(|value| value.redacted),
                        ),
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.console_log failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_RESET_STATE_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::ResetStateRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                clear_cookies: payload
                    .get("clear_cookies")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                clear_storage: payload
                    .get("clear_storage")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                reset_tabs: payload.get("reset_tabs").and_then(Value::as_bool).unwrap_or(false),
                reset_permissions: payload
                    .get("reset_permissions")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            if let Err(error) = attach_browser_caller_principal_metadata(&mut request, principal) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.reset_state(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "cookies_cleared": response.cookies_cleared,
                        "storage_entries_cleared": response.storage_entries_cleared,
                        "tabs_closed": response.tabs_closed,
                        "permissions": response.permissions.map(browser_permissions_to_json),
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.reset_state failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_TABS_LIST_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::ListTabsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.list_tabs(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "tabs": response.tabs.into_iter().map(browser_tab_to_json).collect::<Vec<_>>(),
                        "active_tab_id": response.active_tab_id.map(|value| value.ulid),
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.tabs.list failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_TABS_OPEN_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let url = payload.get("url").and_then(Value::as_str).map(str::trim).unwrap_or_default();
            let navigation_url =
                match resolve_browser_navigation_url(runtime_state, context, url).await {
                    Ok(value) => value,
                    Err(error) => {
                        return browser_tool_execution_outcome(
                            proposal_id,
                            input_json,
                            false,
                            b"{}".to_vec(),
                            error,
                        );
                    }
                };
            let allow_private_targets = browser_private_target_flag_for_validated_url(
                navigation_url.transport_url.as_str(),
                browser_private_targets_requested(&payload),
            );
            let mut request = Request::new(browser_v1::OpenTabRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                url: navigation_url.transport_url.clone(),
                activate: payload.get("activate").and_then(Value::as_bool).unwrap_or(true),
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                allow_redirects: payload
                    .get("allow_redirects")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_redirects: browser_max_redirects_from_payload(&payload),
                allow_private_targets,
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.open_tab(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let tab = response.tab.map(|mut tab| {
                        tab.url = navigation_url.project_response_url(tab.url.as_str());
                        browser_tab_to_json(tab)
                    });
                    let output = json!({
                        "success": response.success,
                        "tab": tab,
                        "navigated": response.navigated,
                        "status_code": response.status_code,
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.tabs.open failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_TABS_SWITCH_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let tab_id = match parse_browser_tool_tab_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::SwitchTabRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                tab_id: Some(common_v1::CanonicalId { ulid: tab_id }),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.switch_tab(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "active_tab": response.active_tab.map(browser_tab_to_json),
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.tabs.switch failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_TABS_CLOSE_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let tab_id = match payload.get("tab_id") {
                Some(Value::String(raw)) => {
                    let trimmed = raw.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        match validate_canonical_id(trimmed) {
                            Ok(_) => Some(common_v1::CanonicalId { ulid: trimmed.to_owned() }),
                            Err(error) => {
                                return browser_tool_execution_outcome(
                                    proposal_id,
                                    input_json,
                                    false,
                                    b"{}".to_vec(),
                                    format!("palyra.browser.tabs.close tab_id is invalid: {error}"),
                                );
                            }
                        }
                    }
                }
                Some(_) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        "palyra.browser.tabs.close field 'tab_id' must be a string".to_owned(),
                    );
                }
                None => None,
            };
            let mut request = Request::new(browser_v1::CloseTabRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                tab_id,
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.close_tab(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "closed_tab_id": response.closed_tab_id.map(|value| value.ulid),
                        "active_tab": response.active_tab.map(browser_tab_to_json),
                        "tabs_remaining": response.tabs_remaining,
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.tabs.close failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_PERMISSIONS_GET_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::GetPermissionsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.get_permissions(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "permissions": response.permissions.map(browser_permissions_to_json),
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.permissions.get failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_PERMISSIONS_SET_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let camera = match parse_browser_permission_setting(&payload, "camera") {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let microphone = match parse_browser_permission_setting(&payload, "microphone") {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let location = match parse_browser_permission_setting(&payload, "location") {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::SetPermissionsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                camera,
                microphone,
                location,
                reset_to_default: payload
                    .get("reset_to_default")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.set_permissions(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "permissions": response.permissions.map(browser_permissions_to_json),
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.permissions.set failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_DOWNLOADS_LIST_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::ListDownloadArtifactsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                limit: payload.get("limit").and_then(Value::as_u64).unwrap_or(20) as u32,
                quarantined_only: payload
                    .get("quarantined_only")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            if let Err(error) = attach_browser_caller_principal_metadata(&mut request, principal) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.list_download_artifacts(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "artifacts": response.artifacts.into_iter().map(browser_download_artifact_to_json).collect::<Vec<_>>(),
                        "truncated": response.truncated,
                        "error": response.error,
                    });
                    let success =
                        output.get("error").and_then(Value::as_str).unwrap_or_default().is_empty();
                    (
                        success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if success {
                            String::new()
                        } else {
                            output
                                .get("error")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_owned()
                        },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.downloads.list failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_DOWNLOADS_GET_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let artifact_id = match parse_browser_download_artifact_id(&payload) {
                Ok(Some(value)) => Some(value),
                // No artifact_id means "the latest download": resolve it via
                // a limit-1 listing so agents can fetch what they just
                // triggered without scraping ids first.
                Ok(None) => {
                    let mut list_request = Request::new(browser_v1::ListDownloadArtifactsRequest {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
                        limit: 1,
                        quarantined_only: false,
                    });
                    if let Err(error) = attach_browser_auth_metadata(
                        &mut list_request,
                        browser_service_config.auth_token.as_deref(),
                    ) {
                        return browser_tool_execution_outcome(
                            proposal_id,
                            input_json,
                            false,
                            b"{}".to_vec(),
                            error,
                        );
                    }
                    if let Err(error) =
                        attach_browser_caller_principal_metadata(&mut list_request, principal)
                    {
                        return browser_tool_execution_outcome(
                            proposal_id,
                            input_json,
                            false,
                            b"{}".to_vec(),
                            error,
                        );
                    }
                    match client.list_download_artifacts(list_request).await {
                        Ok(response) => response
                            .into_inner()
                            .artifacts
                            .into_iter()
                            .next()
                            .and_then(|artifact| artifact.artifact_id),
                        Err(error) => {
                            return browser_tool_execution_outcome(
                                proposal_id,
                                input_json,
                                false,
                                b"{}".to_vec(),
                                format!(
                                    "palyra.browser.downloads.get failed to resolve latest artifact: {}",
                                    sanitize_status_message(&error)
                                ),
                            );
                        }
                    }
                }
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let Some(artifact_id) = artifact_id else {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.downloads.get found no download artifacts for the session"
                        .to_owned(),
                );
            };
            // Clamp rather than reject so an over-eager max_bytes still
            // succeeds at the hard cap; content_base64 is bounded either way.
            let max_bytes = payload
                .get("max_bytes")
                .and_then(Value::as_u64)
                .unwrap_or(BROWSER_DOWNLOAD_TOOL_DEFAULT_MAX_BYTES)
                .clamp(1, BROWSER_DOWNLOAD_TOOL_MAX_BYTES);
            let mut request = Request::new(browser_v1::GetDownloadArtifactRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                artifact_id: Some(artifact_id),
                max_bytes,
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                browser_service_config.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            if let Err(error) = attach_browser_caller_principal_metadata(&mut request, principal) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.get_download_artifact(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let mut success = response.success;
                    let mut error = response.error.clone();
                    let content_sha256 = hex::encode(Sha256::digest(response.content.as_slice()));
                    let artifact = response.artifact.map(browser_download_artifact_to_json);
                    let artifact_size_bytes = artifact
                        .as_ref()
                        .and_then(|value| value.get("size_bytes"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    let mime_type = artifact
                        .as_ref()
                        .and_then(|value| value.get("mime_type"))
                        .and_then(Value::as_str)
                        .unwrap_or("application/octet-stream");
                    let saved_file = if response.success {
                        match browser_output_path_from_payload(
                            &payload,
                            BROWSER_DOWNLOADS_GET_TOOL_NAME,
                        ) {
                            Ok(Some(_)) if response.content_truncated => {
                                success = false;
                                error = format!(
                                    "{BROWSER_DOWNLOADS_GET_TOOL_NAME} cannot write output_path from a truncated download preview (content_bytes={} artifact_size_bytes={} content_limit_bytes={}); increase max_bytes enough to fetch the full artifact, or use the CLI browser downloads save command without a preview cap",
                                    response.content.len(),
                                    artifact_size_bytes,
                                    response.content_limit_bytes
                                );
                                None
                            }
                            Ok(_) => match save_browser_output_file_from_payload(
                                runtime_state,
                                context,
                                &payload,
                                BROWSER_DOWNLOADS_GET_TOOL_NAME,
                                mime_type,
                                response.content.as_slice(),
                            )
                            .await
                            {
                                Ok(saved_file) => saved_file,
                                Err(save_error) => {
                                    success = false;
                                    error = save_error;
                                    None
                                }
                            },
                            Err(save_error) => {
                                success = false;
                                error = save_error;
                                None
                            }
                        }
                    } else {
                        None
                    };
                    let output = json!({
                        "success": success,
                        "error": error,
                        "artifact": artifact,
                        "saved_file": saved_file,
                        "content_base64": STANDARD.encode(response.content.as_slice()),
                        "content_bytes": response.content.len(),
                        "content_sha256": content_sha256,
                        "content_truncated": response.content_truncated,
                        "content_offset_bytes": response.content_offset_bytes,
                        "content_limit_bytes": response.content_limit_bytes,
                    });
                    (
                        success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if success { String::new() } else { error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.downloads.get failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        _ => (false, b"{}".to_vec(), "palyra.browser.* unsupported tool name".to_owned()),
    };

    let (success, mut output_json, error) = outcome;
    output_json =
        browser_output_with_runtime_capabilities(output_json, &browser_runtime_capabilities);

    browser_tool_execution_outcome(proposal_id, input_json, success, output_json, error)
}

/// Closes a browser session as part of run-termination cleanup.
///
/// Returns `Ok(true)` when browserd confirms the close, `Ok(false)` when
/// `session_id` is blank (nothing to clean up) or browserd reports the
/// session was not closed.
///
/// # Errors
/// Returns a message when the browser service is disabled, the session id is
/// not a canonical id, or the connect/close RPC fails.
pub(crate) async fn close_browser_session_for_run_cleanup(
    runtime_state: &Arc<GatewayRuntimeState>,
    session_id: &str,
) -> Result<bool, String> {
    let browser_service_config = runtime_state.browser_service_config_snapshot();
    if !browser_service_config.enabled {
        return Err(
            "palyra.browser.session cleanup skipped because browser service is disabled".to_owned()
        );
    }
    let session_id = session_id.trim();
    if session_id.is_empty() {
        return Ok(false);
    }
    validate_canonical_id(session_id).map_err(|error| {
        format!("palyra.browser.session cleanup session_id is invalid: {error}")
    })?;

    let channel = runtime_state.browser_service_channel(&browser_service_config)?;
    let mut client = browser_v1::browser_service_client::BrowserServiceClient::new(channel);
    let mut request = Request::new(browser_v1::CloseSessionRequest {
        v: CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.to_owned() }),
    });
    attach_browser_auth_metadata(&mut request, browser_service_config.auth_token.as_deref())?;

    client.close_session(request).await.map(|response| response.into_inner().closed).map_err(
        |error| {
            format!("palyra.browser.session cleanup failed: {}", sanitize_status_message(&error))
        },
    )
}

/// Best-effort capability probe: health-RPC failures degrade to an
/// "unavailable" report instead of an error, because capability annotation
/// must never block tool execution itself.
async fn fetch_browser_runtime_capabilities(
    client: &mut browser_v1::browser_service_client::BrowserServiceClient<
        tonic::transport::Channel,
    >,
    auth_token: Option<&str>,
) -> BrowserRuntimeCapabilities {
    let mut request =
        Request::new(browser_v1::BrowserHealthRequest { v: CANONICAL_PROTOCOL_MAJOR });
    if attach_browser_auth_metadata(&mut request, auth_token).is_err() {
        return BrowserRuntimeCapabilities::unavailable();
    }
    match client.health(request).await {
        Ok(response) => BrowserRuntimeCapabilities::from_health(&response.into_inner()),
        Err(_) => BrowserRuntimeCapabilities::unavailable(),
    }
}

fn browser_resilience_rollout_mismatch(
    rollout_enabled: bool,
    capabilities: &BrowserRuntimeCapabilities,
) -> bool {
    capabilities.automatic_reconnect == Some(true) && !rollout_enabled
}

/// Extracts and validates the mandatory `session_id` payload field.
fn parse_browser_tool_session_id(
    payload: &serde_json::Map<String, Value>,
) -> Result<String, String> {
    let Some(session_id) = payload.get("session_id").and_then(Value::as_str).map(str::trim) else {
        return Err("palyra.browser.* requires non-empty string field 'session_id'".to_owned());
    };
    if session_id.is_empty() {
        return Err("palyra.browser.* requires non-empty string field 'session_id'".to_owned());
    }
    validate_canonical_id(session_id)
        .map_err(|error| format!("palyra.browser.* session_id is invalid: {error}"))?;
    Ok(session_id.to_owned())
}

/// Parses the optional `artifact_id` for `downloads.get`; `Ok(None)` (missing
/// or blank) means "fetch the latest artifact".
fn parse_browser_download_artifact_id(
    payload: &serde_json::Map<String, Value>,
) -> Result<Option<common_v1::CanonicalId>, String> {
    let Some(value) = payload.get("artifact_id") else {
        return Ok(None);
    };
    let Some(artifact_id) = value.as_str().map(str::trim) else {
        return Err(format!(
            "{BROWSER_DOWNLOADS_GET_TOOL_NAME} field 'artifact_id' must be a string"
        ));
    };
    if artifact_id.is_empty() {
        return Ok(None);
    }
    validate_canonical_id(artifact_id).map_err(|error| {
        format!("{BROWSER_DOWNLOADS_GET_TOOL_NAME} artifact_id is invalid: {error}")
    })?;
    Ok(Some(common_v1::CanonicalId { ulid: artifact_id.to_owned() }))
}

fn browser_observe_include_visible_text(payload: &serde_json::Map<String, Value>) -> bool {
    payload.get("include_visible_text").and_then(Value::as_bool).unwrap_or(true)
}

/// Parses an observe string-array field, trimming entries, dropping blanks
/// and duplicates, and silently capping at `max_items` (the caps bound the
/// per-call work browserd is asked to do; excess entries are not an error).
fn parse_browser_observe_string_array(
    payload: &serde_json::Map<String, Value>,
    field: &str,
    max_items: usize,
) -> Result<Vec<String>, String> {
    let Some(value) = payload.get(field) else {
        return Ok(Vec::new());
    };
    let Some(entries) = value.as_array() else {
        return Err(format!("palyra.browser.observe field '{field}' must be an array of strings"));
    };
    let mut parsed = Vec::new();
    for entry in entries {
        let Some(raw) = entry.as_str() else {
            return Err(format!(
                "palyra.browser.observe field '{field}' must contain only strings"
            ));
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() || parsed.iter().any(|existing: &String| existing == trimmed) {
            continue;
        }
        parsed.push(trimmed.to_owned());
        if parsed.len() >= max_items {
            break;
        }
    }
    Ok(parsed)
}

/// Extracts and validates the mandatory `tab_id` payload field.
fn parse_browser_tool_tab_id(payload: &serde_json::Map<String, Value>) -> Result<String, String> {
    let Some(tab_id) = payload.get("tab_id").and_then(Value::as_str).map(str::trim) else {
        return Err("palyra.browser.tabs.* requires non-empty string field 'tab_id'".to_owned());
    };
    if tab_id.is_empty() {
        return Err("palyra.browser.tabs.* requires non-empty string field 'tab_id'".to_owned());
    }
    validate_canonical_id(tab_id)
        .map_err(|error| format!("palyra.browser.tabs.* tab_id is invalid: {error}"))?;
    Ok(tab_id.to_owned())
}

/// Parses a permission field as either the proto enum number (0..=2) or a
/// label (`allow`/`deny`/`unspecified`); missing means unspecified.
fn parse_browser_permission_setting(
    payload: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<i32, String> {
    let Some(value) = payload.get(field) else {
        return Ok(0);
    };
    match value {
        Value::Number(number) => number
            .as_i64()
            .filter(|candidate| (0..=2).contains(candidate))
            .map(|candidate| candidate as i32)
            .ok_or_else(|| {
                format!("palyra.browser.permissions.set field '{field}' must be 0, 1, or 2")
            }),
        Value::String(raw) => {
            let normalized = raw.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "" | "unspecified" => Ok(0),
                "deny" => Ok(1),
                "allow" => Ok(2),
                _ => Err(format!(
                    "palyra.browser.permissions.set field '{field}' must be one of: allow|deny|unspecified"
                )),
            }
        }
        _ => Err(format!(
            "palyra.browser.permissions.set field '{field}' must be a string or integer"
        )),
    }
}

/// Parses a console-log severity as either the proto enum number (0..=4) or
/// a label (`debug`/`info`/`warn`/`error`/`unspecified`).
fn parse_browser_diagnostic_severity(
    payload: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<i32, String> {
    let Some(value) = payload.get(field) else {
        return Ok(browser_v1::BrowserDiagnosticSeverity::Unspecified as i32);
    };
    match value {
        Value::Number(number) => number
            .as_i64()
            .filter(|candidate| (0..=4).contains(candidate))
            .map(|candidate| candidate as i32)
            .ok_or_else(|| {
                format!("palyra.browser.console_log field '{field}' must be 0, 1, 2, 3, or 4")
            }),
        Value::String(raw) => {
            let normalized = raw.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "" | "unspecified" => Ok(browser_v1::BrowserDiagnosticSeverity::Unspecified as i32),
                "debug" => Ok(browser_v1::BrowserDiagnosticSeverity::Debug as i32),
                "info" => Ok(browser_v1::BrowserDiagnosticSeverity::Info as i32),
                "warn" | "warning" => Ok(browser_v1::BrowserDiagnosticSeverity::Warn as i32),
                "error" => Ok(browser_v1::BrowserDiagnosticSeverity::Error as i32),
                _ => Err(format!(
                    "palyra.browser.console_log field '{field}' must be one of: debug|info|warn|error|unspecified"
                )),
            }
        }
        _ => Err(format!("palyra.browser.console_log field '{field}' must be a string or integer")),
    }
}

/// Attaches the configured browserd bearer token to a request; a no-op when
/// no token is configured. The token value itself never appears in errors.
fn attach_browser_auth_metadata<T>(
    request: &mut Request<T>,
    auth_token: Option<&str>,
) -> Result<(), String> {
    let Some(token) = auth_token.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let value = tonic::metadata::MetadataValue::try_from(format!("Bearer {token}"))
        .map_err(|error| format!("invalid browser service auth token metadata: {error}"))?;
    request.metadata_mut().insert("authorization", value);
    Ok(())
}

/// Forwards the daemon-side caller principal to browserd so sensitive reads
/// and destructive session mutations can be attributed and access-checked.
fn attach_browser_caller_principal_metadata<T>(
    request: &mut Request<T>,
    caller_principal: &str,
) -> Result<(), String> {
    let value = browser_caller_principal_metadata_value(caller_principal)?;
    request.metadata_mut().insert(BROWSER_CALLER_PRINCIPAL_HEADER, value);
    Ok(())
}

fn browser_caller_principal_metadata_value(
    caller_principal: &str,
) -> Result<tonic::metadata::MetadataValue<tonic::metadata::Ascii>, String> {
    let caller_principal = caller_principal.trim();
    if caller_principal.is_empty() {
        return Err("browser caller principal must not be empty".to_owned());
    }
    tonic::metadata::MetadataValue::try_from(caller_principal)
        .map_err(|error| format!("invalid browser caller principal metadata: {error}"))
}

fn browser_caller_principal_interceptor(
    caller_principal: &str,
    root_auth_token: Option<&str>,
) -> Result<impl tonic::service::Interceptor + Clone, String> {
    let metadata_value = browser_caller_principal_metadata_value(caller_principal)?;
    let caller_principal = metadata_value
        .to_str()
        .map_err(|error| format!("invalid browser caller principal metadata: {error}"))?;
    let authorization_value = root_auth_token
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|root_secret| {
            let credential =
                derive_browser_principal_token(root_secret.as_bytes(), caller_principal);
            tonic::metadata::MetadataValue::try_from(format!("Bearer {credential}"))
                .map_err(|error| format!("invalid browser service auth token metadata: {error}"))
        })
        .transpose()?;
    Ok(move |mut request: Request<()>| {
        request.metadata_mut().insert(BROWSER_CALLER_PRINCIPAL_HEADER, metadata_value.clone());
        if let Some(value) = authorization_value.as_ref() {
            request.metadata_mut().insert("authorization", value.clone());
        }
        Ok(request)
    })
}

/// Renders a gRPC status as a bounded, single-string error message (512
/// chars max); falls back to the full status when the message is empty.
fn sanitize_status_message(status: &Status) -> String {
    let message = status.message().trim();
    if message.is_empty() {
        return truncate_with_ellipsis(status.to_string(), 512);
    }
    truncate_with_ellipsis(message.to_owned(), 512)
}

fn browser_action_log_to_json(entry: browser_v1::BrowserActionLogEntry) -> Value {
    json!({
        "action_id": entry.action_id,
        "action_name": entry.action_name,
        "selector": entry.selector,
        "success": entry.success,
        "outcome": entry.outcome,
        "error": entry.error,
        "started_at_unix_ms": entry.started_at_unix_ms,
        "completed_at_unix_ms": entry.completed_at_unix_ms,
        "attempts": entry.attempts,
        "page_url": entry.page_url,
    })
}

fn browser_image_bytes_metadata(mime_type: &str, bytes: &[u8]) -> Value {
    json!({
        "available": !bytes.is_empty(),
        "mime_type": if mime_type.trim().is_empty() { Value::Null } else { json!(mime_type) },
        "size_bytes": bytes.len(),
        "sha256": if bytes.is_empty() {
            Value::Null
        } else {
            json!(hex::encode(Sha256::digest(bytes)))
        },
        "image_base64_omitted": true,
        "model_visible_bytes": false,
    })
}

fn browser_failure_screenshot_metadata(mime_type: &str, bytes: &[u8]) -> Value {
    let mut metadata = browser_image_bytes_metadata(mime_type, bytes);
    metadata["kind"] = json!("failure_screenshot");
    metadata
}

fn browser_images_list_max_count(payload: &serde_json::Map<String, Value>) -> usize {
    payload
        .get("max_count")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
        .unwrap_or(BROWSER_IMAGES_LIST_DEFAULT_MAX_COUNT)
        .min(BROWSER_IMAGES_LIST_MAX_COUNT)
}

fn browser_image_tags_from_dom_snapshot(
    dom_snapshot: &str,
    max_count: usize,
) -> (Vec<Value>, bool) {
    let max_count = max_count.max(1);
    let lowered = dom_snapshot.to_ascii_lowercase();
    let mut cursor = 0usize;
    let mut images = Vec::new();
    let mut truncated = false;

    while let Some(relative_start) = lowered[cursor..].find("<img") {
        let start = cursor + relative_start;
        let after_tag_name = lowered.as_bytes().get(start + 4).copied();
        if after_tag_name.is_some_and(|byte| byte.is_ascii_alphanumeric() || byte == b'-') {
            cursor = start.saturating_add(4);
            continue;
        }
        let Some(relative_end) = lowered[start..].find('>') else {
            break;
        };
        let end = start + relative_end + 1;
        if images.len() >= max_count {
            truncated = true;
            break;
        }
        images.push(browser_image_tag_metadata(&dom_snapshot[start..end], images.len()));
        cursor = end;
    }

    (images, truncated)
}

fn browser_image_tag_metadata(tag: &str, index: usize) -> Value {
    let src = browser_img_attr(tag, "src")
        .map(|value| browser_safe_image_src(value.as_str()))
        .unwrap_or(Value::Null);
    let srcset_present = browser_img_attr(tag, "srcset").is_some();
    json!({
        "index": index,
        "tag_name": "img",
        "src": src,
        "src_present": browser_img_attr(tag, "src").is_some(),
        "srcset": if srcset_present {
            json!({"present": true, "content_omitted": true})
        } else {
            Value::Null
        },
        "alt": browser_img_text_attr(tag, "alt"),
        "title": browser_img_text_attr(tag, "title"),
        "width_attr": browser_img_numeric_attr(tag, "width"),
        "height_attr": browser_img_numeric_attr(tag, "height"),
        "loading": browser_img_text_attr(tag, "loading"),
        "decoding": browser_img_text_attr(tag, "decoding"),
        "artifact_ref": Value::Null,
        "visibility": "unknown",
        "raw_bytes_model_visible": false,
    })
}

fn browser_img_attr(tag: &str, name: &str) -> Option<String> {
    let bytes = tag.as_bytes();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        while cursor < bytes.len() && !bytes[cursor].is_ascii_alphabetic() {
            cursor = cursor.saturating_add(1);
        }
        let name_start = cursor;
        while cursor < bytes.len()
            && (bytes[cursor].is_ascii_alphanumeric()
                || matches!(bytes[cursor], b'-' | b'_' | b':'))
        {
            cursor = cursor.saturating_add(1);
        }
        if name_start == cursor {
            cursor = cursor.saturating_add(1);
            continue;
        }
        let attr_name = &tag[name_start..cursor];
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor = cursor.saturating_add(1);
        }
        if cursor >= bytes.len() || bytes[cursor] != b'=' {
            continue;
        }
        cursor = cursor.saturating_add(1);
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor = cursor.saturating_add(1);
        }
        let value = if matches!(bytes.get(cursor), Some(b'"' | b'\'')) {
            let quote = bytes[cursor];
            cursor = cursor.saturating_add(1);
            let value_start = cursor;
            while cursor < bytes.len() && bytes[cursor] != quote {
                cursor = cursor.saturating_add(1);
            }
            tag[value_start..cursor].to_owned()
        } else {
            let value_start = cursor;
            while cursor < bytes.len()
                && !bytes[cursor].is_ascii_whitespace()
                && bytes[cursor] != b'>'
            {
                cursor = cursor.saturating_add(1);
            }
            tag[value_start..cursor].to_owned()
        };
        if attr_name.eq_ignore_ascii_case(name) {
            let value = value.trim();
            return (!value.is_empty()).then(|| value.to_owned());
        }
    }
    None
}

fn browser_img_text_attr(tag: &str, name: &str) -> Value {
    browser_img_attr(tag, name)
        .map(|value| {
            let exported =
                export_browser_text(value.as_str(), SafetyContentKind::BrowserObservation);
            json!(truncate_with_ellipsis(exported.redacted_text, 512))
        })
        .unwrap_or(Value::Null)
}

fn browser_img_numeric_attr(tag: &str, name: &str) -> Value {
    browser_img_attr(tag, name)
        .and_then(|value| value.trim().parse::<u32>().ok())
        .map_or(Value::Null, |value| json!(value))
}

fn browser_safe_image_src(src: &str) -> Value {
    let trimmed = src.trim();
    if trimmed.to_ascii_lowercase().starts_with("data:") {
        let metadata = trimmed.split_once(',').map(|(metadata, _)| metadata).unwrap_or("data:");
        return json!({
            "kind": "data_uri",
            "metadata": truncate_with_ellipsis(metadata.to_owned(), 128),
            "content_omitted": true,
        });
    }
    json!({
        "kind": "url",
        "value": truncate_with_ellipsis(redact_url(trimmed), 512),
    })
}

fn browser_layout_metrics_to_json(metrics: browser_v1::BrowserLayoutMetrics) -> Value {
    json!({
        "viewport_width": metrics.viewport_width,
        "viewport_height": metrics.viewport_height,
        "device_scale_factor": metrics.device_scale_factor,
        "document_scroll_width": metrics.document_scroll_width,
        "document_scroll_height": metrics.document_scroll_height,
        "document_client_width": metrics.document_client_width,
        "document_client_height": metrics.document_client_height,
        "horizontal_overflow": metrics.horizontal_overflow,
        "vertical_overflow": metrics.vertical_overflow,
    })
}

fn browser_screenshot_image_observation_hint(saved_file: Option<&Value>) -> Value {
    let path = saved_file
        .and_then(|file| file.get("path"))
        .and_then(Value::as_str)
        .filter(|path| !path.trim().is_empty());
    json!({
        "tool": IMAGE_OBSERVE_TOOL_NAME,
        "path": path,
        "available_without_saved_file": false,
        "message": if path.is_some() {
            "Call palyra.image.observe with this saved_file path when OCR or visual interpretation is needed."
        } else {
            "Pass output_path to palyra.browser.screenshot, then call palyra.image.observe with the saved_file path when OCR or visual interpretation is needed."
        },
    })
}

/// Builds the agent-facing error for a viewport that browserd reported
/// differently from what was requested; `None` when they match.
fn browser_viewport_metric_mismatch_error(
    requested_width: u32,
    requested_height: u32,
    actual_width: u32,
    actual_height: u32,
) -> Option<String> {
    if browser_viewport_dimensions_match(
        requested_width,
        requested_height,
        (actual_width, actual_height),
    ) {
        return None;
    }
    Some(format!(
        "palyra.browser.viewport reported viewport {actual_width}x{actual_height} after requesting {requested_width}x{requested_height}; mobile or responsive visual assertions are unverified"
    ))
}

fn browser_viewport_dimensions_match(
    requested_width: u32,
    requested_height: u32,
    actual: (u32, u32),
) -> bool {
    let (actual_width, actual_height) = actual;
    if actual_width != requested_width {
        return false;
    }
    if actual_height == requested_height {
        return true;
    }
    actual_height < requested_height
        && requested_height.saturating_sub(actual_height) <= BROWSER_VIEWPORT_HEIGHT_TOLERANCE_PX
}

/// Converts element captures to redacted JSON, returning
/// `(captures, per-text scans, any_redacted)` so the caller can fold the
/// scans into the observation-wide safety verdict.
fn browser_element_captures_to_json(
    captures: &[browser_v1::BrowserElementCapture],
) -> (Vec<Value>, Vec<SafetyScanResult>, bool) {
    let mut scans = Vec::new();
    let mut redacted = false;
    let captures = captures
        .iter()
        .map(|capture| {
            let selector =
                export_browser_capture_field(capture.selector.as_str(), &mut scans, &mut redacted);
            let tag_name =
                export_browser_capture_field(capture.tag_name.as_str(), &mut scans, &mut redacted);
            let id = export_browser_capture_field(capture.id.as_str(), &mut scans, &mut redacted);
            let class_name = export_browser_capture_field(
                capture.class_name.as_str(),
                &mut scans,
                &mut redacted,
            );
            let text =
                export_browser_capture_field(capture.text.as_str(), &mut scans, &mut redacted);
            let error =
                export_browser_capture_field(capture.error.as_str(), &mut scans, &mut redacted);
            let computed_styles = capture
                .computed_styles
                .iter()
                .map(|style| {
                    let name = export_browser_capture_field(
                        style.name.as_str(),
                        &mut scans,
                        &mut redacted,
                    );
                    let value = export_browser_capture_field(
                        style.value.as_str(),
                        &mut scans,
                        &mut redacted,
                    );
                    json!({
                        "name": name,
                        "value": value,
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "selector": selector,
                "found": capture.found,
                "bounding_rect": capture.bounding_rect.as_ref().map(browser_rect_to_json),
                "visible": capture.visible,
                "tag_name": tag_name,
                "id": id,
                "class_name": class_name,
                "text": text,
                "text_truncated": capture.text_truncated,
                "computed_styles": computed_styles,
                "error": error,
            })
        })
        .collect::<Vec<_>>();
    (captures, scans, redacted)
}

fn export_browser_capture_field(
    value: &str,
    scans: &mut Vec<SafetyScanResult>,
    redacted: &mut bool,
) -> String {
    let exported = export_browser_text(value, SafetyContentKind::BrowserObservation);
    *redacted |= exported.redacted;
    scans.push(exported.scan.clone());
    exported.redacted_text
}

fn browser_rect_to_json(rect: &browser_v1::BrowserRect) -> Value {
    json!({
        "x": rect.x,
        "y": rect.y,
        "width": rect.width,
        "height": rect.height,
        "top": rect.top,
        "right": rect.right,
        "bottom": rect.bottom,
        "left": rect.left,
    })
}

fn browser_console_severity_label(value: i32) -> &'static str {
    match browser_v1::BrowserDiagnosticSeverity::try_from(value)
        .unwrap_or(browser_v1::BrowserDiagnosticSeverity::Unspecified)
    {
        browser_v1::BrowserDiagnosticSeverity::Debug => "debug",
        browser_v1::BrowserDiagnosticSeverity::Info => "info",
        browser_v1::BrowserDiagnosticSeverity::Warn => "warn",
        browser_v1::BrowserDiagnosticSeverity::Error => "error",
        browser_v1::BrowserDiagnosticSeverity::Unspecified => "unspecified",
    }
}

/// A redacted JSON export plus the safety scan that produced it, kept
/// together so per-entry verdicts can be merged into a tool-level one.
struct BrowserValueExport {
    value: Value,
    scan: SafetyScanResult,
    redacted: bool,
}

/// Scans and redacts browser-sourced text for export; all browser content is
/// treated as externally untrusted regardless of the page it came from.
fn export_browser_text(text: &str, content_kind: SafetyContentKind) -> ExportRedactionOutcome {
    redact_text_for_export(
        text,
        SafetySourceKind::Browser,
        content_kind,
        TrustLabel::ExternalUntrusted,
    )
}

fn browser_safety_json(scan: &SafetyScanResult, redacted: bool) -> Value {
    json!({
        "trust_label": scan.trust_label.as_str(),
        "action": scan.recommended_action.as_str(),
        "findings": scan.finding_codes(),
        "redacted": redacted,
    })
}

/// Folds per-entry scans into one tool-level safety verdict; an empty set
/// scans the empty string to obtain a clean baseline result.
fn merge_browser_value_scans(
    content_kind: SafetyContentKind,
    values: &[BrowserValueExport],
) -> SafetyScanResult {
    if values.is_empty() {
        return export_browser_text("", content_kind).scan;
    }
    let scans = values.iter().map(|entry| entry.scan.clone()).collect::<Vec<_>>();
    merge_scan_results(
        SafetyPhase::Export,
        SafetySourceKind::Browser,
        content_kind,
        scans.as_slice(),
    )
}

const BROWSER_COOKIE_VALUE_WITHHELD: &str = "[WITHHELD_BROWSER_COOKIE_VALUE]";
const BROWSER_STORAGE_VALUE_WITHHELD: &str = "[WITHHELD_BROWSER_STORAGE_VALUE]";

fn browser_cookie_domain_to_json(domain: browser_v1::SessionCookieDomain) -> BrowserValueExport {
    let mut scan_input = format!("domain={}", domain.domain);
    let cookies = domain
        .cookies
        .into_iter()
        .map(|cookie| {
            let value_length_bytes = cookie.value.len();
            scan_input.push('\n');
            scan_input.push_str(cookie.name.as_str());
            json!({
                "name": cookie.name,
                "value": BROWSER_COOKIE_VALUE_WITHHELD,
                "value_withheld": true,
                "value_length_bytes": value_length_bytes,
            })
        })
        .collect::<Vec<_>>();
    let scan = export_browser_text(scan_input.as_str(), SafetyContentKind::BrowserObservation);
    let withheld = !cookies.is_empty();
    BrowserValueExport {
        value: json!({
            "domain": domain.domain,
            "cookies": cookies,
            "safety": browser_safety_json(&scan.scan, scan.redacted || withheld),
        }),
        scan: scan.scan,
        redacted: scan.redacted || withheld,
    }
}

fn browser_storage_origin_to_json(origin: browser_v1::SessionStorageOrigin) -> BrowserValueExport {
    let mut scan_input = format!("origin={}", origin.origin);
    let entries = origin
        .entries
        .into_iter()
        .map(|entry| {
            let value_length_bytes = entry.value.len();
            scan_input.push('\n');
            scan_input.push_str(entry.key.as_str());
            json!({
                "key": entry.key,
                "value": BROWSER_STORAGE_VALUE_WITHHELD,
                "value_withheld": true,
                "value_length_bytes": value_length_bytes,
            })
        })
        .collect::<Vec<_>>();
    let scan = export_browser_text(scan_input.as_str(), SafetyContentKind::BrowserObservation);
    let withheld = !entries.is_empty();
    BrowserValueExport {
        value: json!({
            "origin": origin.origin,
            "entries": entries,
            "safety": browser_safety_json(&scan.scan, scan.redacted || withheld),
        }),
        scan: scan.scan,
        redacted: scan.redacted || withheld,
    }
}

fn browser_console_entry_to_json(entry: browser_v1::BrowserConsoleEntry) -> BrowserValueExport {
    let message_export =
        export_browser_text(entry.message.as_str(), SafetyContentKind::BrowserConsole);
    let stack_export =
        export_browser_text(entry.stack_trace.as_str(), SafetyContentKind::BrowserConsole);
    let page_url = redact_url(entry.page_url.as_str());
    let combined_scan = export_browser_text(
        format!(
            "message={}\nstack_trace={}\npage_url={}",
            entry.message, entry.stack_trace, entry.page_url
        )
        .as_str(),
        SafetyContentKind::BrowserConsole,
    );
    BrowserValueExport {
        value: json!({
            "severity": browser_console_severity_label(entry.severity),
            "kind": entry.kind,
            "message": message_export.redacted_text,
            "captured_at_unix_ms": entry.captured_at_unix_ms,
            "source": entry.source,
            "stack_trace": stack_export.redacted_text,
            "page_url": page_url,
            "safety": browser_safety_json(&combined_scan.scan, combined_scan.redacted),
        }),
        scan: combined_scan.scan,
        redacted: message_export.redacted
            || stack_export.redacted
            || combined_scan.redacted
            || page_url != entry.page_url,
    }
}

fn browser_page_diagnostics_to_json(
    diagnostics: browser_v1::BrowserPageDiagnostics,
) -> BrowserValueExport {
    let title_export =
        export_browser_text(diagnostics.page_title.as_str(), SafetyContentKind::BrowserTitle);
    let page_url = redact_url(diagnostics.page_url.as_str());
    BrowserValueExport {
        value: json!({
            "page_url": page_url,
            "page_title": title_export.redacted_text,
            "console_entry_count": diagnostics.console_entry_count,
            "warning_count": diagnostics.warning_count,
            "error_count": diagnostics.error_count,
            "last_event_unix_ms": diagnostics.last_event_unix_ms,
            "safety": browser_safety_json(&title_export.scan, title_export.redacted),
        }),
        scan: title_export.scan,
        redacted: title_export.redacted || page_url != diagnostics.page_url,
    }
}

fn browser_network_log_entry_to_json(entry: browser_v1::NetworkLogEntry) -> BrowserValueExport {
    let request_url = redact_url(entry.request_url.as_str());
    let entry_id = browser_network_log_entry_id(&entry, request_url.as_str());
    let raw_scan_input = {
        let mut buffer = String::new();
        buffer.push_str("request_url=");
        buffer.push_str(entry.request_url.as_str());
        for header in &entry.headers {
            buffer.push('\n');
            buffer.push_str(header.name.as_str());
            buffer.push_str(": ");
            buffer.push_str(header.value.as_str());
        }
        buffer
    };
    let mut headers = entry
        .headers
        .into_iter()
        .map(|header| {
            let redacted_value = redact_header(header.name.as_str(), header.value.as_str());
            json!({ "name": header.name, "value": redacted_value })
        })
        .collect::<Vec<_>>();
    // Sort headers by name for deterministic output across engines and
    // replays; fixtures assert on stable ordering.
    headers.sort_by(|left, right| {
        let left_name = left.get("name").and_then(Value::as_str).unwrap_or_default();
        let right_name = right.get("name").and_then(Value::as_str).unwrap_or_default();
        left_name.cmp(right_name)
    });
    let scan = export_browser_text(raw_scan_input.as_str(), SafetyContentKind::BrowserNetwork);
    BrowserValueExport {
        value: json!({
            "entry_id": entry_id,
            "phase": "response",
            "request_url": request_url,
            "status_code": entry.status_code,
            "timing_bucket": entry.timing_bucket,
            "latency_ms": entry.latency_ms,
            "captured_at_unix_ms": entry.captured_at_unix_ms,
            "headers": headers,
            "safety": browser_safety_json(&scan.scan, scan.redacted),
        }),
        scan: scan.scan,
        redacted: scan.redacted || request_url != entry.request_url,
    }
}

fn browser_network_log_entry_id(
    entry: &browser_v1::NetworkLogEntry,
    redacted_request_url: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.browser.network_log.entry.v2");
    hasher.update(redacted_request_url.as_bytes());
    hasher.update(entry.status_code.to_be_bytes());
    hasher.update(entry.timing_bucket.as_bytes());
    hasher.update(entry.latency_ms.to_be_bytes());
    hasher.update(entry.captured_at_unix_ms.to_be_bytes());
    format!("net_{}", &hex::encode(hasher.finalize())[..16])
}

fn filter_browser_network_log_entries_since(
    entries: Vec<browser_v1::NetworkLogEntry>,
    since_unix_ms: u64,
) -> Vec<browser_v1::NetworkLogEntry> {
    if since_unix_ms == 0 {
        return entries;
    }
    entries.into_iter().filter(|entry| entry.captured_at_unix_ms >= since_unix_ms).collect()
}

fn browser_tab_to_json(tab: browser_v1::BrowserTab) -> Value {
    json!({
        "tab_id": tab.tab_id.map(|value| value.ulid),
        "url": tab.url,
        "title": tab.title,
        "active": tab.active,
    })
}

fn browser_download_artifact_to_json(artifact: browser_v1::DownloadArtifact) -> Value {
    json!({
        "artifact_id": artifact.artifact_id.map(|value| value.ulid),
        "session_id": artifact.session_id.map(|value| value.ulid),
        "profile_id": artifact.profile_id.map(|value| value.ulid),
        "source_url": artifact.source_url,
        "file_name": artifact.file_name,
        "mime_type": artifact.mime_type,
        "size_bytes": artifact.size_bytes,
        "sha256": artifact.sha256,
        "created_at_unix_ms": artifact.created_at_unix_ms,
        "quarantined": artifact.quarantined,
        "quarantine_reason": artifact.quarantine_reason,
    })
}

fn browser_permission_setting_label(value: i32) -> &'static str {
    match value {
        1 => "deny",
        2 => "allow",
        _ => "unspecified",
    }
}

fn browser_permissions_to_json(permissions: browser_v1::SessionPermissions) -> Value {
    json!({
        "camera": browser_permission_setting_label(permissions.camera),
        "microphone": browser_permission_setting_label(permissions.microphone),
        "location": browser_permission_setting_label(permissions.location),
    })
}

/// Trims a `press` key name, except that a single literal space is itself a
/// valid key press and must survive untrimmed (pinned by test).
fn normalize_browser_press_key_input(raw: &str) -> String {
    if raw == " " {
        " ".to_owned()
    } else {
        raw.trim().to_owned()
    }
}

fn browser_session_closed_error_message(tool_name: &str, session_id: &str) -> String {
    format!(
        "{tool_name} failed: browser session {session_id} is closed; create a new browser session before retrying"
    )
}

/// Recognizes only the daemon-authored closed-session message with a valid
/// session id; backend and selector text must not select this recovery path.
fn browser_session_closed_error(error: &str) -> bool {
    let Some((_, suffix)) = error.split_once("failed: browser session ") else {
        return false;
    };
    let Some((session_id, recovery)) = suffix.split_once(" is closed; ") else {
        return false;
    };
    validate_canonical_id(session_id).is_ok()
        && recovery == "create a new browser session before retrying"
}

/// Maps an error to the recovery hint for its failure class, most specific
/// first. Classification must stay aligned with [`browser_error_category`].
fn browser_recovery_hint(error: &str) -> Option<&'static str> {
    let normalized = error.to_ascii_lowercase();
    if browser_session_closed_error(error) {
        return Some(BROWSER_SESSION_CLOSED_RECOVERY_HINT);
    }
    if browser_runtime_unavailable_error(&normalized) {
        return Some(BROWSER_RUNTIME_RECOVERY_HINT);
    }
    if normalized.contains("selector") && normalized.contains("not found") {
        return Some(BROWSER_SELECTOR_RECOVERY_HINT);
    }
    if normalized.contains("wait_for requires non-empty selector")
        || normalized.contains("wait_for requires non-empty selector or non-empty text")
    {
        return Some(BROWSER_WAIT_FOR_INPUT_RECOVERY_HINT);
    }
    if normalized.contains("wait_for condition was not satisfied") {
        return Some(BROWSER_WAIT_FOR_TIMEOUT_RECOVERY_HINT);
    }
    None
}

/// Matches tonic/h2 transport-failure signatures that indicate browserd is
/// unreachable or restarted, as opposed to a tool-level failure.
fn browser_runtime_unavailable_error(normalized_error: &str) -> bool {
    let browser_service_transport_error = normalized_error.contains("browser service")
        && (normalized_error.contains("connection refused")
            || normalized_error.contains("connection reset")
            || normalized_error.contains("connection closed")
            || normalized_error.contains("transport error")
            || normalized_error.contains("h2 protocol error")
            || normalized_error.contains("tcp connect error")
            || normalized_error.contains("deadline has elapsed")
            || normalized_error.contains("timed out"));
    let browser_tool_transport_error = normalized_error.contains("palyra.browser.")
        && (normalized_error.contains("connection reset")
            || normalized_error.contains("connection refused")
            || normalized_error.contains("connection closed")
            || normalized_error.contains("transport error")
            || normalized_error.contains("h2 protocol error")
            || normalized_error.contains("broken pipe"));

    normalized_error.contains("failed to connect to browser service")
        || browser_service_transport_error
        || browser_tool_transport_error
}

/// Stable machine-readable category for a failure; mirrors the
/// classification order of [`browser_recovery_hint`] so hint and category
/// never disagree on the same error.
fn browser_error_category(error: &str) -> &'static str {
    let normalized = error.to_ascii_lowercase();
    if browser_session_closed_error(error) {
        "browser_session_closed"
    } else if browser_runtime_unavailable_error(&normalized) {
        "browser_runtime_unavailable"
    } else if normalized.contains("selector") && normalized.contains("not found") {
        "selector_not_found"
    } else if normalized.contains("wait_for requires non-empty selector")
        || normalized.contains("wait_for requires non-empty selector or non-empty text")
    {
        "wait_for_input_required"
    } else if normalized.contains("wait_for condition was not satisfied") {
        "wait_for_timeout"
    } else {
        "browser_tool_error"
    }
}

/// Extracts the leading `palyra.browser.<tool>` label from an error message,
/// falling back to the wildcard label.
fn browser_tool_label_from_error(error: &str) -> &str {
    error
        .split_whitespace()
        .next()
        .map(|value| value.trim_end_matches(':'))
        .filter(|value| value.starts_with("palyra.browser."))
        .unwrap_or("palyra.browser.*")
}

/// Builds a self-describing failure payload for arms that produced no output
/// of their own (`{}`), so agents always get category, hint, and executor.
fn browser_failure_diagnostic_output(error: &str, hint: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "success": false,
        "tool": browser_tool_label_from_error(error),
        "error": error,
        "error_category": browser_error_category(error),
        "recovery_hint": hint,
        "executor": "browser_broker",
        "sandbox_enforcement": "browser_service",
    }))
    .unwrap_or_else(|_| br#"{"success":false,"error":"browser tool failed"}"#.to_vec())
}

/// Appends `; recovery_hint=...` to a failure message unless the hint text
/// is already present (avoids doubling on re-processed errors).
fn browser_error_with_recovery_hint(error: String) -> String {
    let Some(hint) = browser_recovery_hint(error.as_str()) else {
        return error;
    };
    if error.contains(hint) {
        return error;
    }
    format!("{error}; recovery_hint={hint}")
}

/// Enriches a failure output with `error_category` and `recovery_hint`
/// fields; an empty `{}` output is replaced by the full diagnostic payload.
/// Non-object outputs pass through unchanged.
fn browser_output_with_recovery_hint(output_json: Vec<u8>, error: &str) -> Vec<u8> {
    let hint = browser_recovery_hint(error).unwrap_or(BROWSER_TOOL_INPUT_RECOVERY_HINT);
    let mut output = serde_json::from_slice::<Value>(output_json.as_slice())
        .unwrap_or_else(|_| json!({ "success": false, "error": error }));
    if let Some(object) = output.as_object_mut() {
        if object.is_empty() {
            return browser_failure_diagnostic_output(error, hint);
        }
        object.entry("success").or_insert(Value::Bool(false));
        object.entry("error").or_insert(Value::String(error.to_owned()));
        object.insert(
            "error_category".to_owned(),
            Value::String(browser_error_category(error).to_owned()),
        );
        object.insert("recovery_hint".to_owned(), Value::String(hint.to_owned()));
    }
    serde_json::to_vec(&output).unwrap_or(output_json)
}

/// Adds the `browser_runtime` capability report (and, for limited engines,
/// `browser_validation_warning`) to every object-shaped output, success or
/// failure; non-object outputs pass through unchanged.
fn browser_output_with_runtime_capabilities(
    output_json: Vec<u8>,
    capabilities: &BrowserRuntimeCapabilities,
) -> Vec<u8> {
    let mut output = match serde_json::from_slice::<Value>(output_json.as_slice()) {
        Ok(value) => value,
        Err(_) => return output_json,
    };
    let Some(object) = output.as_object_mut() else {
        return output_json;
    };
    object.insert("browser_runtime".to_owned(), capabilities.to_json());
    if let Some(warning) = capabilities.warning {
        object.insert("browser_validation_warning".to_owned(), Value::String(warning.to_owned()));
    }
    serde_json::to_vec(&output).unwrap_or(output_json)
}

/// Finalizes a browser tool result into an attested [`ToolExecutionOutcome`],
/// enriching failures with recovery hints and error categories first.
fn browser_tool_execution_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    let output_json =
        if success { output_json } else { browser_output_with_recovery_hint(output_json, &error) };
    let error = if success { error } else { browser_error_with_recovery_hint(error) };
    let executed_at_unix_ms = current_unix_ms();
    // Domain-separated digest with length-prefixed fields so adjacent values
    // cannot collide by shifting bytes across a field boundary.
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.browser.tool.attestation.v1");
    hasher.update((proposal_id.len() as u64).to_be_bytes());
    hasher.update(proposal_id.as_bytes());
    hasher.update((input_json.len() as u64).to_be_bytes());
    hasher.update(input_json);
    hasher.update([u8::from(success)]);
    hasher.update((output_json.len() as u64).to_be_bytes());
    hasher.update(output_json.as_slice());
    hasher.update((error.len() as u64).to_be_bytes());
    hasher.update(error.as_bytes());
    hasher.update(executed_at_unix_ms.to_be_bytes());
    let execution_sha256 = hex::encode(hasher.finalize());

    ToolExecutionOutcome {
        success,
        output_json,
        error,
        attestation: ToolAttestation {
            attestation_id: Ulid::generate().to_string(),
            execution_sha256,
            executed_at_unix_ms,
            timed_out: false,
            executor: "browser_broker".to_owned(),
            sandbox_enforcement: "browser_service".to_owned(),
            execution_manifest: None,
            mcp_transport_invocation: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Mutex, OnceLock};

    use super::{
        attach_browser_caller_principal_metadata, browser_caller_principal_interceptor,
        browser_cdp_method_allowed, browser_console_entry_to_json, browser_cookie_domain_to_json,
        browser_element_captures_to_json, browser_failure_screenshot_metadata,
        browser_file_url_to_path, browser_image_tags_from_dom_snapshot,
        browser_max_redirects_from_payload, browser_network_log_entry_to_json,
        browser_observe_include_visible_text, browser_output_with_runtime_capabilities,
        browser_private_target_flag_for_validated_url, browser_private_targets_requested,
        browser_reload_expected_url_from_payload,
        browser_reload_private_target_flag_for_validated_url,
        browser_rescue_rollout_disabled_output, browser_rescue_trace_payload,
        browser_resilience_rollout_mismatch, browser_screenshot_image_observation_hint,
        browser_session_closed_error_message, browser_session_persistence_from_payload,
        browser_session_profile_id_from_payload, browser_storage_origin_to_json,
        browser_tool_execution_outcome, browser_tool_requires_open_session,
        browser_user_owned_os_roots, browser_viewport_metric_mismatch_error,
        canonical_file_path_is_inside_workspace_roots, default_browser_session_persistence_id,
        evaluate_browser_rescue_trigger, filter_browser_network_log_entries_since,
        normalize_browser_press_key_input, parse_browser_download_artifact_id,
        parse_browser_observe_string_array, resolve_browser_file_navigation_url,
        resolve_browser_output_path, resolve_browser_upload_path,
        validate_browser_file_url_path_scope, validate_browser_workspace_relative_path,
        write_browser_output_file, BrowserRescueTriggerKind, BrowserRuntimeCapabilities,
        BROWSER_CALLER_PRINCIPAL_HEADER, BROWSER_COOKIE_VALUE_WITHHELD,
        BROWSER_STORAGE_VALUE_WITHHELD, PALYRA_OS_FILE_ROOTS_ENV,
    };
    use crate::application::tool_runtime::workspace_scope::ActiveWorkspaceRoot;
    use crate::gateway::{
        BROWSER_CDP_INVOKE_TOOL_NAME, BROWSER_CLICK_TOOL_NAME, BROWSER_DOWNLOADS_GET_TOOL_NAME,
        BROWSER_IMAGES_LIST_TOOL_NAME, BROWSER_NAVIGATE_TOOL_NAME, BROWSER_OBSERVE_TOOL_NAME,
        BROWSER_RELOAD_TOOL_NAME, BROWSER_SESSION_CLOSE_TOOL_NAME,
        BROWSER_SESSION_CREATE_TOOL_NAME, BROWSER_TABS_CLOSE_TOOL_NAME, BROWSER_VISION_TOOL_NAME,
        IMAGE_OBSERVE_TOOL_NAME,
    };
    use crate::transport::grpc::proto::palyra::browser::v1 as browser_v1;
    use palyra_common::{derive_browser_principal_token, CANONICAL_PROTOCOL_MAJOR};
    use serde_json::json;
    use tonic::Request;

    // Process env vars are global; tests that mutate them serialize here so
    // parallel test threads cannot observe each other's overrides.
    static BROWSER_ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn failure_screenshot_metadata_omits_base64_bytes() {
        let metadata = browser_failure_screenshot_metadata("image/png", b"abc");
        let serialized =
            serde_json::to_string(&metadata).expect("failure screenshot metadata should serialize");

        assert_eq!(metadata["available"], true);
        assert_eq!(metadata["size_bytes"], 3);
        assert_eq!(metadata["image_base64_omitted"], true);
        assert_eq!(metadata["model_visible_bytes"], false);
        assert!(metadata["sha256"].as_str().is_some_and(|hash| !hash.is_empty()));
        assert!(!serialized.contains("YWJj"));
        assert!(!serialized.contains("failure_screenshot_base64"));
    }

    #[test]
    fn image_list_metadata_redacts_urls_and_omits_data_uri_content() {
        let dom = r#"
            <main>
              <img src="https://example.test/a.png?token=secret-token" alt="Product">
              <IMG SRC='data:image/png;base64,QUJDREVGRw==' width="20" height="10" srcset="https://cdn.example.test/a 1x">
              <img src="/later.png">
            </main>
        "#;
        let (images, truncated) = browser_image_tags_from_dom_snapshot(dom, 2);
        let serialized = serde_json::to_string(&images).expect("image metadata should serialize");

        assert_eq!(images.len(), 2);
        assert!(truncated);
        assert_eq!(images[0]["alt"], "Product");
        assert_eq!(images[0]["src"]["kind"], "url");
        assert!(!images[0]["src"]["value"].as_str().unwrap_or_default().contains("secret-token"));
        assert_eq!(images[1]["src"]["kind"], "data_uri");
        assert_eq!(images[1]["src"]["content_omitted"], true);
        assert_eq!(images[1]["width_attr"], 20);
        assert_eq!(images[1]["height_attr"], 10);
        assert_eq!(images[1]["srcset"]["content_omitted"], true);
        assert!(!serialized.contains("QUJDREVGRw"));
    }

    #[test]
    fn browser_rescue_tools_fail_closed_behind_rollout_and_cdp_allowlist() {
        let output = browser_rescue_rollout_disabled_output(BROWSER_VISION_TOOL_NAME);

        assert_eq!(output["success"], false);
        assert_eq!(output["error_code"], "browser_rescue_disabled");
        assert_eq!(output["rollout"]["config_path"], "feature_rollouts.browser_rescue");
        assert!(browser_cdp_method_allowed("Page.getLayoutMetrics"));
        assert!(!browser_cdp_method_allowed("Runtime.evaluate"));
        assert!(browser_tool_requires_open_session(BROWSER_VISION_TOOL_NAME));
        assert!(browser_tool_requires_open_session(BROWSER_IMAGES_LIST_TOOL_NAME));
        assert!(browser_tool_requires_open_session(BROWSER_CDP_INVOKE_TOOL_NAME));

        let explicit = evaluate_browser_rescue_trigger(
            true,
            BrowserRescueTriggerKind::ExplicitBrowserToolFailure,
        );
        assert!(explicit.attempt_rescue);
        assert_eq!(explicit.trace_event, "browser.rescue.requested");

        let corruption =
            evaluate_browser_rescue_trigger(true, BrowserRescueTriggerKind::BrowserStateCorruption);
        let trace = browser_rescue_trace_payload(
            "profile-1",
            BrowserRescueTriggerKind::BrowserStateCorruption,
            &corruption,
        );
        assert!(corruption.attempt_rescue);
        assert_eq!(trace["rescue_kind"], "browser_state_corruption");
        assert_eq!(trace["raw_browser_payload_visible"], false);
        assert_ne!(trace["profile_id"], "profile-1");

        let policy_denied =
            evaluate_browser_rescue_trigger(true, BrowserRescueTriggerKind::PolicyDenied);
        assert!(!policy_denied.attempt_rescue);
        assert_eq!(policy_denied.reason_code, "browser_rescue.policy_denied_no_rescue");

        let egress_denied =
            evaluate_browser_rescue_trigger(true, BrowserRescueTriggerKind::NetworkEgressDenied);
        assert!(!egress_denied.attempt_rescue);
        assert_eq!(egress_denied.reason_code, "browser_rescue.egress_denied_no_rescue");
    }

    /// Sets an env var for the test's lifetime and restores the previous
    /// value (or removes the var) on drop.
    struct ScopedEnvVar {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl ScopedEnvVar {
        fn set(key: &'static str, value: &std::path::Path) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            match self.previous.as_ref() {
                Some(previous) => std::env::set_var(self.key, previous),
                None => std::env::remove_var(self.key),
            }
        }
    }

    #[test]
    fn console_log_export_redacts_sensitive_message_content() {
        let exported = browser_console_entry_to_json(browser_v1::BrowserConsoleEntry {
            v: CANONICAL_PROTOCOL_MAJOR,
            severity: browser_v1::BrowserDiagnosticSeverity::Error as i32,
            kind: "exception".to_owned(),
            message: "Authorization: Bearer super-secret-token-value".to_owned(),
            captured_at_unix_ms: 42,
            source: "runtime".to_owned(),
            stack_trace: "token=super-secret-token-value".to_owned(),
            page_url: "https://example.test/path?token=abc123".to_owned(),
        });
        assert_eq!(exported.value["message"], "Authorization: [REDACTED_SECRET]");
        assert_eq!(exported.value["safety"]["action"], "redact");
        assert!(exported.redacted);
    }

    #[test]
    fn browser_storage_exports_withhold_cookie_and_local_storage_values() {
        let cookies = browser_cookie_domain_to_json(browser_v1::SessionCookieDomain {
            v: CANONICAL_PROTOCOL_MAJOR,
            domain: "accounts.example.test".to_owned(),
            cookies: vec![
                browser_v1::SessionCookieEntry {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    name: "sid".to_owned(),
                    value: "opaque-random-browser-session-1234567890".to_owned(),
                },
                browser_v1::SessionCookieEntry {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    name: "__Secure-1PSID".to_owned(),
                    value: "securepsidvalue-1234567890abcdef".to_owned(),
                },
            ],
        });
        let storage = browser_storage_origin_to_json(browser_v1::SessionStorageOrigin {
            v: CANONICAL_PROTOCOL_MAJOR,
            origin: "https://accounts.example.test".to_owned(),
            entries: vec![browser_v1::SessionStorageEntry {
                v: CANONICAL_PROTOCOL_MAJOR,
                key: "sid".to_owned(),
                value: "local-storage-session-abcdef123456".to_owned(),
            }],
        });

        assert_eq!(cookies.value["cookies"][0]["value"], BROWSER_COOKIE_VALUE_WITHHELD);
        assert_eq!(cookies.value["cookies"][0]["value_withheld"], true);
        assert_eq!(storage.value["entries"][0]["value"], BROWSER_STORAGE_VALUE_WITHHELD);
        assert_eq!(storage.value["entries"][0]["value_withheld"], true);
        assert!(cookies.redacted);
        assert!(storage.redacted);

        let exported = serde_json::to_string(&json!({
            "cookies": cookies.value,
            "storage": storage.value,
        }))
        .expect("storage export should serialize");
        assert!(!exported.contains("opaque-random-browser-session"));
        assert!(!exported.contains("securepsidvalue"));
        assert!(!exported.contains("local-storage-session"));
    }

    #[test]
    fn network_log_export_redacts_sensitive_headers() {
        let entry = browser_v1::NetworkLogEntry {
            v: CANONICAL_PROTOCOL_MAJOR,
            request_url: "https://example.test/api?token=abc123".to_owned(),
            status_code: 200,
            timing_bucket: "fast".to_owned(),
            latency_ms: 17,
            captured_at_unix_ms: 7,
            headers: vec![browser_v1::NetworkLogHeader {
                v: CANONICAL_PROTOCOL_MAJOR,
                name: "Authorization".to_owned(),
                value: "Bearer super-secret-token-value".to_owned(),
            }],
        };
        let mut alternate_secret_entry = entry.clone();
        alternate_secret_entry.request_url =
            "https://example.test/api?token=different-secret".to_owned();
        let exported = browser_network_log_entry_to_json(entry);
        let alternate_secret_export = browser_network_log_entry_to_json(alternate_secret_entry);
        assert_eq!(exported.value["headers"][0]["value"], "<redacted>");
        assert_eq!(exported.value["phase"], "response");
        assert!(
            exported
                .value
                .get("entry_id")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|entry_id| entry_id.starts_with("net_")),
            "{}",
            exported.value
        );
        assert_eq!(
            exported.value["entry_id"], alternate_secret_export.value["entry_id"],
            "network entry IDs must not verify pre-redaction query secrets"
        );
        assert_eq!(exported.value["safety"]["action"], "redact");
        assert!(exported.redacted);
    }

    #[test]
    fn network_log_since_filter_drops_older_entries() {
        let entries = [10, 20, 30]
            .into_iter()
            .map(|captured_at_unix_ms| browser_v1::NetworkLogEntry {
                v: CANONICAL_PROTOCOL_MAJOR,
                request_url: format!("https://example.test/{captured_at_unix_ms}"),
                status_code: 200,
                timing_bucket: "fast".to_owned(),
                latency_ms: 1,
                captured_at_unix_ms,
                headers: Vec::new(),
            })
            .collect::<Vec<_>>();

        let filtered = filter_browser_network_log_entries_since(entries.clone(), 20);
        let urls = filtered.iter().map(|entry| entry.request_url.as_str()).collect::<Vec<_>>();

        assert_eq!(urls, vec!["https://example.test/20", "https://example.test/30"]);
        assert_eq!(filter_browser_network_log_entries_since(entries, 0).len(), 3);
    }

    #[test]
    fn browser_runtime_capabilities_mark_simulated_engine_as_static_html_only() {
        let capabilities =
            BrowserRuntimeCapabilities::from_health(&browser_v1::BrowserHealthResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                status: "ok".to_owned(),
                uptime_seconds: 12,
                active_sessions: 1,
                engine_mode: "simulated".to_owned(),
                javascript_execution_enabled: false,
                subresource_loading_enabled: false,
                dom_interaction_enabled: false,
                resilience_profile: "disabled".to_owned(),
                automatic_reconnect_enabled: false,
                healthy_sessions: 1,
                degraded_sessions: 0,
                reconnecting_sessions: 0,
                blocked_sessions: 0,
                process_reconnect_count: 0,
                target_reconnect_count: 0,
                dialog_timeout_count: 0,
            });

        assert_eq!(capabilities.engine_mode, "simulated");
        assert_eq!(capabilities.javascript_execution, Some(false));
        assert_eq!(capabilities.subresource_loading, Some(false));
        assert_eq!(capabilities.dom_interaction, Some(false));
        assert_eq!(capabilities.health_status, "ok");
        assert_eq!(capabilities.resilience_profile, "disabled");
        assert_eq!(capabilities.automatic_reconnect, Some(false));
        assert!(capabilities.warning.is_some_and(|warning| warning.contains("static_html_only")));
    }

    #[test]
    fn browser_resilience_requires_both_product_and_browserd_gates() {
        let capabilities =
            BrowserRuntimeCapabilities::from_health(&browser_v1::BrowserHealthResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                status: "ok".to_owned(),
                uptime_seconds: 12,
                active_sessions: 1,
                engine_mode: "chromium".to_owned(),
                javascript_execution_enabled: true,
                subresource_loading_enabled: true,
                dom_interaction_enabled: true,
                resilience_profile: "resilient".to_owned(),
                automatic_reconnect_enabled: true,
                healthy_sessions: 1,
                degraded_sessions: 0,
                reconnecting_sessions: 0,
                blocked_sessions: 0,
                process_reconnect_count: 0,
                target_reconnect_count: 0,
                dialog_timeout_count: 0,
            });

        assert!(browser_resilience_rollout_mismatch(false, &capabilities));
        assert!(!browser_resilience_rollout_mismatch(true, &capabilities));
        assert!(!browser_resilience_rollout_mismatch(
            false,
            &BrowserRuntimeCapabilities::unknown("test", None)
        ));
    }

    #[test]
    fn browser_screenshot_hint_points_to_image_observe_without_base64() {
        let saved_file = json!({
            "path": "C:/workspace/evidence.png",
            "mime_type": "image/png",
            "size_bytes": 12,
            "sha256": "abc",
        });

        let hint = browser_screenshot_image_observation_hint(Some(&saved_file));

        assert_eq!(hint["tool"], IMAGE_OBSERVE_TOOL_NAME);
        assert_eq!(hint["path"], "C:/workspace/evidence.png");
        assert_eq!(hint["available_without_saved_file"], false);
        assert!(hint["message"].as_str().is_some_and(|message| {
            message.contains("palyra.image.observe") && !message.contains("base64")
        }));
    }

    #[test]
    fn browser_output_with_runtime_capabilities_includes_validation_warning() {
        let capabilities =
            BrowserRuntimeCapabilities::from_health(&browser_v1::BrowserHealthResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                status: "ok".to_owned(),
                uptime_seconds: 12,
                active_sessions: 1,
                engine_mode: "simulated".to_owned(),
                javascript_execution_enabled: false,
                subresource_loading_enabled: false,
                dom_interaction_enabled: false,
                resilience_profile: "disabled".to_owned(),
                automatic_reconnect_enabled: false,
                healthy_sessions: 1,
                degraded_sessions: 0,
                reconnecting_sessions: 0,
                blocked_sessions: 0,
                process_reconnect_count: 0,
                target_reconnect_count: 0,
                dialog_timeout_count: 0,
            });
        let output = browser_output_with_runtime_capabilities(
            br#"{"success":true,"title":"S020 Vite Env Demo","status_code":200}"#.to_vec(),
            &capabilities,
        );
        let output: serde_json::Value =
            serde_json::from_slice(output.as_slice()).expect("output should parse");

        assert_eq!(output["browser_runtime"]["engine_mode"], "simulated");
        assert_eq!(output["browser_runtime"]["javascript_execution"], false);
        assert_eq!(output["browser_runtime"]["subresource_loading"], false);
        assert_eq!(output["browser_runtime"]["health_status"], "ok");
        assert_eq!(output["browser_runtime"]["resilience_profile"], "disabled");
        assert_eq!(output["browser_runtime"]["automatic_reconnect"], false);
        assert!(output["browser_validation_warning"]
            .as_str()
            .is_some_and(|warning| warning.contains("static_html_only")));
    }

    #[test]
    fn failed_browser_selector_actions_include_recovery_hint() {
        let outcome = browser_tool_execution_outcome(
            "proposal-1",
            br##"{"selector":"#card-number"}"##,
            false,
            br#"{"success":false,"error":"selector '#card-number' was not found"}"#.to_vec(),
            "selector '#card-number' was not found".to_owned(),
        );
        let output: serde_json::Value =
            serde_json::from_slice(outcome.output_json.as_slice()).expect("output should parse");

        assert!(output["recovery_hint"]
            .as_str()
            .is_some_and(|hint| hint.contains("palyra.browser.observe")));
        assert!(outcome.error.contains("recovery_hint=selector_not_found"));
    }

    #[test]
    fn invalid_wait_for_output_includes_recovery_hint() {
        let outcome = browser_tool_execution_outcome(
            "proposal-1",
            br#"{"session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV"}"#,
            false,
            b"{}".to_vec(),
            "palyra.browser.wait_for failed: wait_for requires non-empty selector or non-empty text"
                .to_owned(),
        );
        let output: serde_json::Value =
            serde_json::from_slice(outcome.output_json.as_slice()).expect("output should parse");

        assert_eq!(output["success"], false);
        assert!(output["recovery_hint"]
            .as_str()
            .is_some_and(|hint| hint.contains("pass either selector or text")));
        assert!(outcome.error.contains("recovery_hint=wait_for_input_required"));
    }

    #[test]
    fn failed_browser_runtime_transport_output_includes_diagnostics() {
        let outcome = browser_tool_execution_outcome(
            "proposal-1",
            br#"{"session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV"}"#,
            false,
            b"{}".to_vec(),
            "palyra.browser.observe failed: h2 protocol error: error reading a body from connection: connection reset"
                .to_owned(),
        );
        let output: serde_json::Value =
            serde_json::from_slice(outcome.output_json.as_slice()).expect("output should parse");

        assert_eq!(output["success"], false);
        assert_eq!(output["tool"], "palyra.browser.observe");
        assert_eq!(output["error_category"], "browser_runtime_unavailable");
        assert_eq!(output["executor"], "browser_broker");
        assert_eq!(output["sandbox_enforcement"], "browser_service");
        assert!(output["recovery_hint"]
            .as_str()
            .is_some_and(|hint| hint.contains("palyra browser status")));
        assert!(outcome.error.contains("recovery_hint=browser_runtime_unavailable"));
    }

    #[test]
    fn closed_browser_session_recovery_requires_daemon_authored_error() {
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let output_json = serde_json::to_vec(&json!({
            "success": false,
            "session_id": session_id,
            "error": "browser_session_closed",
        }))
        .expect("closed-session output should serialize");
        let outcome = browser_tool_execution_outcome(
            "proposal-1",
            br#"{"session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV"}"#,
            false,
            output_json,
            browser_session_closed_error_message("palyra.browser.tabs.list", session_id),
        );
        let output: serde_json::Value =
            serde_json::from_slice(outcome.output_json.as_slice()).expect("output should parse");

        assert_eq!(output["success"], false);
        assert_eq!(output["session_id"], session_id);
        assert_eq!(output["error"], "browser_session_closed");
        assert_eq!(output["error_category"], "browser_session_closed");
        assert!(output["recovery_hint"]
            .as_str()
            .is_some_and(|hint| hint.contains("palyra.browser.session.create")));
        assert!(outcome.error.contains("recovery_hint=browser_session_closed"));

        let injected_error = concat!(
            "selector 'session_not_found browser session ",
            "01ARZ3NDEKTSV4RRFFQ69G5FAV not found' was not found",
        );
        let injected_outcome = browser_tool_execution_outcome(
            "proposal-2",
            br#"{"session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV"}"#,
            false,
            br#"{"success":false,"error":"selector_not_found"}"#.to_vec(),
            injected_error.to_owned(),
        );
        let injected_output: serde_json::Value =
            serde_json::from_slice(injected_outcome.output_json.as_slice())
                .expect("selector failure output should parse");

        assert_eq!(injected_output["error_category"], "selector_not_found");
        assert!(injected_outcome.error.contains("recovery_hint=selector_not_found"));
        assert!(!injected_outcome.error.contains("recovery_hint=browser_session_closed"));
    }

    #[test]
    fn browser_action_tools_require_open_session_handles() {
        assert!(!browser_tool_requires_open_session(BROWSER_SESSION_CREATE_TOOL_NAME));
        assert!(!browser_tool_requires_open_session(BROWSER_SESSION_CLOSE_TOOL_NAME));
        assert!(browser_tool_requires_open_session(BROWSER_NAVIGATE_TOOL_NAME));
        assert!(browser_tool_requires_open_session(BROWSER_RELOAD_TOOL_NAME));
        assert!(browser_tool_requires_open_session(BROWSER_CLICK_TOOL_NAME));
        assert!(browser_tool_requires_open_session(BROWSER_OBSERVE_TOOL_NAME));
        assert!(browser_tool_requires_open_session(BROWSER_TABS_CLOSE_TOOL_NAME));
        assert!(browser_tool_requires_open_session(BROWSER_DOWNLOADS_GET_TOOL_NAME));
    }

    #[test]
    fn browser_press_key_input_preserves_literal_space() {
        assert_eq!(normalize_browser_press_key_input(" "), " ");
        assert_eq!(normalize_browser_press_key_input(" Space "), "Space");
        assert!(normalize_browser_press_key_input(" \t ").is_empty());
    }

    #[test]
    fn browser_max_redirects_saturates_large_payload_values() {
        let default_payload = json!({});
        let large_payload = json!({"max_redirects": u64::MAX});

        assert_eq!(
            browser_max_redirects_from_payload(
                default_payload.as_object().expect("payload should be an object")
            ),
            3
        );
        assert_eq!(
            browser_max_redirects_from_payload(
                large_payload.as_object().expect("payload should be an object")
            ),
            u32::MAX
        );
    }

    #[test]
    fn browser_reload_expected_url_requires_visible_destination() {
        let missing = json!({});
        let error = browser_reload_expected_url_from_payload(
            missing.as_object().expect("payload should be an object"),
        )
        .expect_err("reload expected_url should be required");
        assert!(error.contains("requires expected_url"));

        let control_character = json!({"expected_url": "https://example.test/\u{7}admin"});
        let error = browser_reload_expected_url_from_payload(
            control_character.as_object().expect("payload should be an object"),
        )
        .expect_err("control characters should be rejected");
        assert!(error.contains("without control characters"));

        let invalid_url = json!({"expected_url": "not a url"});
        let error = browser_reload_expected_url_from_payload(
            invalid_url.as_object().expect("payload should be an object"),
        )
        .expect_err("malformed URLs should be rejected");
        assert!(error.contains("expected_url is invalid"));

        let visible_destination =
            json!({"expected_url": "https://example.test/dashboard?nonce=destination-bound"});
        let expected_url = browser_reload_expected_url_from_payload(
            visible_destination.as_object().expect("payload should be an object"),
        )
        .expect("valid expected_url should parse");
        assert_eq!(expected_url, "https://example.test/dashboard?nonce=destination-bound");
    }

    #[test]
    fn browser_reload_private_target_flag_honors_model_visible_opt_in() {
        let private_reload = json!({"allow_private_targets": true});
        assert!(browser_reload_private_target_flag_for_validated_url(
            "http://127.0.0.1:4173/dashboard",
            private_reload.as_object().expect("payload should be an object"),
        ));

        let default_reload = json!({});
        assert!(!browser_reload_private_target_flag_for_validated_url(
            "http://127.0.0.1:4173/dashboard",
            default_reload.as_object().expect("payload should be an object"),
        ));
    }

    #[test]
    fn browser_session_profile_id_accepts_existing_canonical_ids() {
        let payload = json!({"profile_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV"});

        let (profile_id, ignored) = browser_session_profile_id_from_payload(
            payload.as_object().expect("payload must be an object"),
        )
        .expect("canonical profile id should parse");

        assert_eq!(
            profile_id.as_ref().map(|value| value.ulid.as_str()),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
        );
        assert_eq!(ignored, None);
    }

    #[test]
    fn browser_session_profile_id_rejects_friendly_labels() {
        let payload = json!({"profile_id": "friendly-browser-profile"});

        let error = browser_session_profile_id_from_payload(
            payload.as_object().expect("payload must be an object"),
        )
        .expect_err("friendly profile labels should fail session creation");

        assert!(error.contains("field 'profile_id' must be a canonical id"));
    }

    #[test]
    fn browser_session_profile_id_rejects_non_string_values() {
        let payload = json!({"profile_id": 42});

        let error = browser_session_profile_id_from_payload(
            payload.as_object().expect("payload must be an object"),
        )
        .expect_err("non-string profile id should fail");

        assert!(error.contains("field 'profile_id' must be a string"));
    }

    #[test]
    fn browser_session_create_defaults_to_ephemeral() {
        let payload = json!({});

        assert_eq!(
            browser_session_persistence_from_payload(
                payload.as_object().expect("payload must be an object"),
                "browser-recovery-export-20260527",
            )
            .expect("default ephemeral mode should be accepted"),
            (false, String::new())
        );
    }

    #[test]
    fn browser_session_create_derives_persistence_id_when_enabled() {
        let payload = json!({"persistence_enabled": true});

        let (enabled, persistence_id) = browser_session_persistence_from_payload(
            payload.as_object().expect("payload must be an object"),
            "browser-recovery-export-20260527",
        )
        .expect("explicit persistence should be accepted");

        assert!(enabled);
        assert!(persistence_id.starts_with("agent-session-sha256-"));
        assert_eq!(persistence_id.len(), "agent-session-sha256-".len() + 64);
        assert_eq!(
            persistence_id,
            default_browser_session_persistence_id("browser-recovery-export-20260527")
        );
        assert_ne!(
            default_browser_session_persistence_id("abc/def"),
            default_browser_session_persistence_id("abcdef")
        );
    }

    #[test]
    fn browser_session_create_respects_ephemeral_opt_outs() {
        let explicit_ephemeral = json!({"persistence_enabled": false});
        let private_profile = json!({"private_profile": true, "persistence_enabled": true});

        assert_eq!(
            browser_session_persistence_from_payload(
                explicit_ephemeral.as_object().expect("payload must be an object"),
                "agent-session"
            )
            .expect("explicit persistence disable should be accepted"),
            (false, String::new())
        );
        assert_eq!(
            browser_session_persistence_from_payload(
                private_profile.as_object().expect("payload must be an object"),
                "agent-session"
            )
            .expect("private profile should be accepted"),
            (false, String::new())
        );
    }

    #[test]
    fn browser_session_create_rejects_explicit_persistence_id() {
        let payload = json!({"persistence_id": "profile.recovery-1"});

        let error = browser_session_persistence_from_payload(
            payload.as_object().expect("payload must be an object"),
            "ignored-session",
        )
        .expect_err("caller-supplied persistence id should fail");

        assert!(error.contains("field 'persistence_id' is reserved"));
    }

    #[test]
    fn browser_download_artifact_id_parser_accepts_optional_canonical_id() {
        let missing = serde_json::Map::new();
        assert!(parse_browser_download_artifact_id(&missing)
            .expect("missing artifact id should be optional")
            .is_none());

        let empty = json!({"artifact_id": "  "});
        assert!(parse_browser_download_artifact_id(empty.as_object().expect("object payload"))
            .expect("empty artifact id should request latest artifact")
            .is_none());

        let explicit = json!({"artifact_id": "01ARZ3NDEKTSV4RRFFQ69G5FAY"});
        assert_eq!(
            parse_browser_download_artifact_id(explicit.as_object().expect("object payload"))
                .expect("canonical artifact id should parse")
                .expect("artifact id should be present")
                .ulid,
            "01ARZ3NDEKTSV4RRFFQ69G5FAY"
        );

        let invalid = json!({"artifact_id": "downloads/latest"});
        let error =
            parse_browser_download_artifact_id(invalid.as_object().expect("object payload"))
                .expect_err("non-canonical artifact id should fail");
        assert!(error.contains("artifact_id is invalid"));
    }

    #[test]
    fn browser_viewport_metric_mismatch_is_agent_facing_failure() {
        let matching = browser_viewport_metric_mismatch_error(375, 812, 375, 812);
        assert!(matching.is_none());
        let browser_chrome_delta = browser_viewport_metric_mismatch_error(375, 667, 375, 652);
        assert!(
            browser_chrome_delta.is_none(),
            "exact width with small visible-height delta should remain usable for responsive assertions"
        );

        let mismatch = browser_viewport_metric_mismatch_error(375, 812, 960, 2079)
            .expect("viewport mismatch should produce an explicit failure message");

        assert!(mismatch.contains("960x2079"));
        assert!(mismatch.contains("375x812"));
        assert!(mismatch.contains("visual assertions are unverified"));
    }

    #[test]
    fn browser_upload_relative_path_validation_confines_workspace_paths() {
        let relative =
            validate_browser_workspace_relative_path(std::path::Path::new("fixtures/upload.txt"))
                .expect("plain relative upload path should be accepted");
        assert_eq!(relative, std::path::PathBuf::from("fixtures/upload.txt"));

        for denied in ["", "../secret.txt", "./upload.txt", "/secret.txt"] {
            let error = validate_browser_workspace_relative_path(std::path::Path::new(denied))
                .expect_err("unsafe relative upload path should be denied");
            assert!(
                error.contains("relative file_path"),
                "unexpected validation error for {denied:?}: {error}"
            );
        }
    }

    #[test]
    fn browser_upload_path_expands_launch_env_prefix() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let os_root = temp.path().join("os-root");
        let downloads = os_root.join("downloads");
        std::fs::create_dir_all(downloads.as_path()).expect("downloads should exist");
        let upload = downloads.join("upload-input.csv");
        std::fs::write(upload.as_path(), "sku,qty\nE2E-WIDGET,1\n").expect("upload should exist");
        let canonical_root = os_root.canonicalize().expect("OS root should canonicalize");
        let canonical_upload = upload.canonicalize().expect("upload should canonicalize");
        let path_env = BTreeMap::from([("PALYRA_E2E_OS_ROOT".to_owned(), canonical_root.clone())]);

        let resolved = resolve_browser_upload_path(
            "$PALYRA_E2E_OS_ROOT/downloads/upload-input.csv",
            std::slice::from_ref(&canonical_root),
            &[],
            &path_env,
        )
        .expect("env-prefixed upload file should resolve inside an authorized workspace root");

        assert_eq!(resolved, canonical_upload);
    }

    #[test]
    fn browser_upload_path_rejects_process_env_prefix_without_launch_env_root() {
        let _guard = BROWSER_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("browser env lock should not be poisoned");
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let home = temp.path().join("home");
        let credentials_dir = home.join(".aws");
        std::fs::create_dir_all(credentials_dir.as_path())
            .expect("credential directory should exist");
        let credentials_file = credentials_dir.join("credentials");
        std::fs::write(credentials_file.as_path(), "aws_access_key_id=SECRET\n")
            .expect("credential file should exist");
        let _credential_locator =
            ScopedEnvVar::set("AWS_SHARED_CREDENTIALS_FILE", credentials_file.as_path());

        let error =
            resolve_browser_upload_path("$AWS_SHARED_CREDENTIALS_FILE", &[], &[], &BTreeMap::new())
                .expect_err("browser upload must not expand daemon process env locators");

        assert!(
            error.contains(
                "palyra.browser.upload file_path references unset launch environment variable `AWS_SHARED_CREDENTIALS_FILE`"
            ),
            "unexpected upload path error: {error}"
        );
    }

    #[test]
    fn browser_upload_path_rejects_launch_env_root_outside_workspace() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        let os_root = temp.path().join("os-root");
        let downloads = os_root.join("downloads");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should exist");
        std::fs::create_dir_all(downloads.as_path()).expect("downloads should exist");
        let upload = downloads.join("upload-input.csv");
        std::fs::write(upload.as_path(), "sku,qty\nE2E-WIDGET,1\n").expect("upload should exist");
        let canonical_workspace = workspace.canonicalize().expect("workspace should canonicalize");
        let canonical_root = os_root.canonicalize().expect("OS root should canonicalize");
        let canonical_upload = upload.canonicalize().expect("upload should canonicalize");
        let path_env = BTreeMap::from([("PALYRA_E2E_OS_ROOT".to_owned(), canonical_root)]);

        let error = resolve_browser_upload_path(
            canonical_upload.to_string_lossy().as_ref(),
            std::slice::from_ref(&canonical_workspace),
            &[],
            &path_env,
        )
        .expect_err("request-supplied launch env root must not grant browser upload access");

        assert!(error.contains("launch environment aliases do not grant file access"), "{error}");
    }

    #[test]
    fn browser_upload_path_rejects_absolute_path_inside_unapproved_launch_workspace() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let agent_workspace = temp.path().join("agent-workspace");
        let launch_workspace = temp.path().join("launch-workspace");
        std::fs::create_dir_all(agent_workspace.as_path()).expect("workspace should exist");
        std::fs::create_dir_all(launch_workspace.as_path()).expect("launch workspace should exist");
        let upload = launch_workspace.join("secret-upload.csv");
        std::fs::write(upload.as_path(), "secret").expect("upload should exist");
        let canonical_agent =
            agent_workspace.canonicalize().expect("workspace should canonicalize");

        let error = resolve_browser_upload_path(
            upload.to_string_lossy().as_ref(),
            std::slice::from_ref(&canonical_agent),
            &[],
            &BTreeMap::new(),
        )
        .expect_err("launch cwd must not implicitly authorize browser upload reads");

        assert!(error.contains("outside agent workspace roots"), "{error}");
    }

    #[test]
    fn browser_upload_path_rejects_home_directory_secret() {
        let _guard = BROWSER_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("browser env lock should not be poisoned");
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        let home = temp.path().join("home");
        let ssh_dir = home.join(".ssh");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should exist");
        std::fs::create_dir_all(ssh_dir.as_path()).expect("ssh dir should exist");
        let key_file = ssh_dir.join("id_rsa");
        std::fs::write(key_file.as_path(), "test-private-key").expect("key fixture should exist");
        let canonical_workspace = workspace.canonicalize().expect("workspace should canonicalize");
        let _home = ScopedEnvVar::set("HOME", home.as_path());
        let _userprofile = ScopedEnvVar::set("USERPROFILE", home.as_path());

        let error = resolve_browser_upload_path(
            key_file.to_string_lossy().as_ref(),
            std::slice::from_ref(&canonical_workspace),
            &[],
            &BTreeMap::new(),
        )
        .expect_err("implicit home roots must not authorize browser upload reads");

        assert!(error.contains("outside agent workspace roots"), "{error}");
    }

    #[test]
    fn browser_upload_path_accepts_explicitly_approved_os_root() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        let approved = temp.path().join("approved");
        let adjacent = temp.path().join("adjacent");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should exist");
        std::fs::create_dir_all(approved.as_path()).expect("approved root should exist");
        std::fs::create_dir_all(adjacent.as_path()).expect("adjacent root should exist");
        let upload = approved.join("upload-input.csv");
        let denied = adjacent.join("denied.csv");
        std::fs::write(upload.as_path(), "sku,qty\nE2E-WIDGET,1\n")
            .expect("approved upload should exist");
        std::fs::write(denied.as_path(), "secret\n").expect("adjacent upload should exist");
        let canonical_workspace = workspace.canonicalize().expect("workspace should canonicalize");
        let canonical_approved =
            approved.canonicalize().expect("approved root should canonicalize");
        let canonical_upload = upload.canonicalize().expect("upload should canonicalize");

        let resolved = resolve_browser_upload_path(
            upload.to_string_lossy().as_ref(),
            std::slice::from_ref(&canonical_workspace),
            std::slice::from_ref(&canonical_approved),
            &BTreeMap::new(),
        )
        .expect("explicitly approved OS file should be accepted");
        assert_eq!(resolved, canonical_upload);

        let error = resolve_browser_upload_path(
            denied.to_string_lossy().as_ref(),
            std::slice::from_ref(&canonical_workspace),
            std::slice::from_ref(&canonical_approved),
            &BTreeMap::new(),
        )
        .expect_err("adjacent OS file should remain denied");
        assert!(error.contains("approved user-owned OS roots"), "{error}");
    }

    #[test]
    fn browser_output_path_resolves_relative_artifact_inside_workspace() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should be created");
        let canonical_workspace = workspace.canonicalize().expect("workspace should canonicalize");

        let output = resolve_browser_output_path(
            "palyra.browser.screenshot",
            "artifacts/visual-smoke.png",
            std::slice::from_ref(&canonical_workspace),
            None,
            &BTreeMap::new(),
            &[],
        )
        .expect("relative browser output path should resolve");

        assert!(output.starts_with(canonical_workspace.as_path()));
        assert_eq!(output.file_name().and_then(|value| value.to_str()), Some("visual-smoke.png"));
        assert!(
            output.parent().is_some_and(|parent| parent.is_dir()),
            "output parent should be created"
        );
    }

    #[test]
    fn browser_output_file_write_overwrites_regular_file() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let target = temp.path().join("artifact.png");
        std::fs::write(target.as_path(), b"old bytes").expect("target should be created");

        write_browser_output_file(
            "palyra.browser.screenshot",
            "artifact.png",
            target.as_path(),
            b"new bytes",
        )
        .expect("regular browser output target should be writable");

        assert_eq!(
            std::fs::read(target.as_path()).expect("target should be readable"),
            b"new bytes"
        );
    }

    #[cfg(unix)]
    #[test]
    fn browser_output_path_rejects_final_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should be created");
        let outside_target = temp.path().join("outside-target.txt");
        std::fs::write(outside_target.as_path(), b"outside").expect("outside target should exist");
        let output_link = workspace.join("report.pdf");
        symlink(outside_target.as_path(), output_link.as_path())
            .expect("symlink should be created");
        let canonical_workspace = workspace.canonicalize().expect("workspace should canonicalize");

        let error = resolve_browser_output_path(
            BROWSER_DOWNLOADS_GET_TOOL_NAME,
            "report.pdf",
            std::slice::from_ref(&canonical_workspace),
            None,
            &BTreeMap::new(),
            &[],
        )
        .expect_err("browser output path must reject final symlinks");

        assert!(error.contains("final component must not be a symlink"), "{error}");
        assert_eq!(
            std::fs::read(outside_target.as_path()).expect("outside target should be readable"),
            b"outside",
            "resolver must not allow writing through the symlink"
        );
    }

    #[test]
    fn browser_output_path_prefers_launch_workspace_root_order() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let launch_workspace = temp.path().join("launch-workspace");
        let default_workspace = temp.path().join("default-workspace");
        std::fs::create_dir_all(launch_workspace.as_path())
            .expect("launch workspace should be created");
        std::fs::create_dir_all(default_workspace.as_path())
            .expect("default workspace should be created");
        let canonical_launch =
            launch_workspace.canonicalize().expect("launch workspace should canonicalize");
        let canonical_default =
            default_workspace.canonicalize().expect("default workspace should canonicalize");

        let output = resolve_browser_output_path(
            "palyra.browser.screenshot",
            "reports/visual-smoke.png",
            &[canonical_launch.clone(), canonical_default],
            None,
            &BTreeMap::new(),
            &[],
        )
        .expect("relative browser output path should resolve");

        assert!(output.starts_with(canonical_launch.as_path()));
    }

    #[test]
    fn browser_output_path_uses_active_workspace_for_short_relative_artifact() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let repo_launch = temp.path().join("repo-launch");
        let harness_root = temp.path().join("Palyra-TestHarness");
        let active_workspace = harness_root.join("scenario-workspaces").join("S006_save_bug");
        std::fs::create_dir_all(repo_launch.as_path()).expect("repo launch should exist");
        std::fs::create_dir_all(active_workspace.as_path()).expect("active workspace should exist");
        let canonical_repo = repo_launch.canonicalize().expect("repo should canonicalize");
        let canonical_harness = harness_root.canonicalize().expect("harness should canonicalize");
        let canonical_active =
            active_workspace.canonicalize().expect("active workspace should canonicalize");
        let active = ActiveWorkspaceRoot {
            root: canonical_active.clone(),
            relative_path: "scenario-workspaces/S006_save_bug".to_owned(),
        };

        let output = resolve_browser_output_path(
            "palyra.browser.screenshot",
            "evidence-after-fix.png",
            &[canonical_repo, canonical_harness],
            Some(&active),
            &BTreeMap::new(),
            &[],
        )
        .expect("short relative browser output path should resolve inside active workspace");

        assert!(output.starts_with(canonical_active.as_path()));
        assert_eq!(
            output.file_name().and_then(|value| value.to_str()),
            Some("evidence-after-fix.png")
        );
    }

    #[test]
    fn browser_output_path_keeps_active_relative_prefix_under_owning_workspace() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let repo_launch = temp.path().join("repo-launch");
        let harness_root = temp.path().join("Palyra-TestHarness");
        let active_workspace = harness_root.join("scenario-workspaces").join("S006_save_bug");
        std::fs::create_dir_all(repo_launch.as_path()).expect("repo launch should exist");
        std::fs::create_dir_all(active_workspace.as_path()).expect("active workspace should exist");
        let canonical_repo = repo_launch.canonicalize().expect("repo should canonicalize");
        let canonical_harness = harness_root.canonicalize().expect("harness should canonicalize");
        let canonical_active =
            active_workspace.canonicalize().expect("active workspace should canonicalize");
        let active = ActiveWorkspaceRoot {
            root: canonical_active.clone(),
            relative_path: "scenario-workspaces/S006_save_bug".to_owned(),
        };

        let output = resolve_browser_output_path(
            "palyra.browser.screenshot",
            "scenario-workspaces/S006_save_bug/evidence-after-fix.png",
            &[canonical_repo.clone(), canonical_harness],
            Some(&active),
            &BTreeMap::new(),
            &[],
        )
        .expect("prefixed relative browser output path should resolve inside active workspace");

        assert!(output.starts_with(canonical_active.as_path()));
        assert!(!output.starts_with(canonical_repo.as_path()));
        assert_eq!(
            output.file_name().and_then(|value| value.to_str()),
            Some("evidence-after-fix.png")
        );
    }

    #[test]
    fn browser_output_path_expands_launch_env_prefix() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let harness_home = temp.path().join("Palyra-TestHarness");
        std::fs::create_dir_all(harness_home.as_path()).expect("harness home should exist");
        let canonical_home = harness_home.canonicalize().expect("harness home should canonicalize");
        let path_env = BTreeMap::from([("PALYRA_E2E_HOME".to_owned(), canonical_home.clone())]);

        let output = resolve_browser_output_path(
            BROWSER_DOWNLOADS_GET_TOOL_NAME,
            "%PALYRA_E2E_HOME%/Desktop/palyra-orders-export.csv",
            &[],
            None,
            &path_env,
            &[],
        )
        .expect("env-prefixed browser output path should resolve inside launch env root");

        assert!(output.starts_with(canonical_home.as_path()));
        assert_eq!(
            output.parent().and_then(|parent| parent.file_name()).and_then(|value| value.to_str()),
            Some("Desktop")
        );
        assert_eq!(
            output.file_name().and_then(|value| value.to_str()),
            Some("palyra-orders-export.csv")
        );
        assert!(
            output.parent().is_some_and(|parent| parent.is_dir()),
            "env-prefixed output parent should be created"
        );
    }

    #[test]
    fn browser_user_owned_roots_replace_implicit_profile_roots_when_configured() {
        let _guard = BROWSER_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("browser env lock should not be poisoned");
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let configured_root = temp.path().join("configured-os-root");
        let implicit_home = temp.path().join("implicit-home");
        std::fs::create_dir_all(configured_root.as_path())
            .expect("configured root should be created");
        std::fs::create_dir_all(implicit_home.as_path()).expect("implicit home should be created");
        let _configured = ScopedEnvVar::set(PALYRA_OS_FILE_ROOTS_ENV, configured_root.as_path());
        let _userprofile = ScopedEnvVar::set("USERPROFILE", implicit_home.as_path());
        let _home = ScopedEnvVar::set("HOME", implicit_home.as_path());

        let roots = browser_user_owned_os_roots();
        let canonical_configured =
            configured_root.canonicalize().expect("root should canonicalize");
        let canonical_home = implicit_home.canonicalize().expect("home should canonicalize");

        assert!(
            roots.iter().any(|root| root == &canonical_configured),
            "configured OS root should be included: {roots:?}"
        );
        assert!(
            !roots.iter().any(|root| root == &canonical_home),
            "implicit USERPROFILE/HOME roots must not be included when PALYRA_OS_FILE_ROOTS is set: {roots:?}"
        );
    }

    #[test]
    fn browser_output_path_rejects_absolute_path_outside_configured_roots() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let configured_root = temp.path().join("configured-os-root");
        let outside_root = temp.path().join("Palyra-TestHarness");
        let outside_desktop = outside_root.join("Desktop");
        std::fs::create_dir_all(configured_root.as_path())
            .expect("configured root should be created");
        std::fs::create_dir_all(outside_desktop.as_path())
            .expect("outside desktop should be created");
        let canonical_configured =
            configured_root.canonicalize().expect("configured root should canonicalize");
        let outside_output = outside_desktop.join("palyra-orders-export.csv");

        let error = resolve_browser_output_path(
            BROWSER_DOWNLOADS_GET_TOOL_NAME,
            outside_output.to_string_lossy().as_ref(),
            &[],
            None,
            &BTreeMap::new(),
            std::slice::from_ref(&canonical_configured),
        )
        .expect_err("browser downloads should reject paths outside configured OS roots");

        assert!(error.contains("outside agent workspace roots"), "{error}");
    }

    #[test]
    fn browser_output_path_rejects_traversal_and_unapproved_absolute_paths() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should be created");
        std::fs::create_dir_all(outside.as_path()).expect("outside should be created");
        let canonical_workspace = workspace.canonicalize().expect("workspace should canonicalize");

        let traversal = resolve_browser_output_path(
            "palyra.browser.screenshot",
            "../outside/smoke.png",
            std::slice::from_ref(&canonical_workspace),
            None,
            &BTreeMap::new(),
            &[],
        )
        .expect_err("relative traversal should be rejected");
        assert!(traversal.contains("relative output_path"));

        let absolute_outside = outside.join("smoke.png");
        let outside_error = resolve_browser_output_path(
            "palyra.browser.screenshot",
            absolute_outside.to_string_lossy().as_ref(),
            &[canonical_workspace],
            None,
            &BTreeMap::new(),
            &[],
        )
        .expect_err("unapproved absolute path should be rejected");
        assert!(outside_error.contains("outside agent workspace roots"));
    }

    #[test]
    fn browser_console_request_metadata_includes_caller_principal() {
        let mut request = Request::new(());

        attach_browser_caller_principal_metadata(&mut request, " user:local ")
            .expect("principal metadata should attach");

        assert_eq!(
            request
                .metadata()
                .get(BROWSER_CALLER_PRINCIPAL_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("user:local")
        );
    }

    #[test]
    fn browser_client_interceptor_includes_caller_principal_on_every_request() {
        let mut interceptor =
            browser_caller_principal_interceptor(" user:local ", Some("root-browser-secret"))
                .expect("principal interceptor should construct");

        for _ in 0..2 {
            let request = tonic::service::Interceptor::call(&mut interceptor, Request::new(()))
                .expect("principal interceptor should accept a request");
            assert_eq!(
                request
                    .metadata()
                    .get(BROWSER_CALLER_PRINCIPAL_HEADER)
                    .and_then(|value| value.to_str().ok()),
                Some("user:local")
            );
            let expected = format!(
                "Bearer {}",
                derive_browser_principal_token(b"root-browser-secret", "user:local")
            );
            assert_eq!(
                request.metadata().get("authorization").and_then(|value| value.to_str().ok()),
                Some(expected.as_str())
            );
        }
    }

    #[test]
    fn browser_pdf_request_metadata_includes_caller_principal() {
        let mut request = Request::new(browser_v1::ExportPdfRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            session_id: None,
            max_bytes: 0,
        });

        attach_browser_caller_principal_metadata(&mut request, " user:local ")
            .expect("principal metadata should attach");

        assert_eq!(
            request
                .metadata()
                .get(BROWSER_CALLER_PRINCIPAL_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("user:local")
        );
    }

    #[test]
    fn browser_network_request_metadata_includes_caller_principal() {
        let mut request = Request::new(browser_v1::NetworkLogRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            session_id: None,
            limit: 0,
            include_headers: false,
            max_payload_bytes: 0,
        });

        attach_browser_caller_principal_metadata(&mut request, " user:local ")
            .expect("principal metadata should attach");

        assert_eq!(
            request
                .metadata()
                .get(BROWSER_CALLER_PRINCIPAL_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("user:local")
        );
    }

    #[test]
    fn browser_observe_includes_visible_text_by_default() {
        let default_payload = serde_json::Map::new();
        assert!(browser_observe_include_visible_text(&default_payload));

        let explicit_false = json!({"include_visible_text": false});
        assert!(!browser_observe_include_visible_text(
            explicit_false.as_object().expect("object payload")
        ));
    }

    #[test]
    fn browser_observe_string_array_parser_trims_dedupes_and_caps() {
        let payload = json!({
            "capture_selectors": ["  #hero  ", "", "#hero", ".nav", ".footer"]
        });

        let parsed = parse_browser_observe_string_array(
            payload.as_object().expect("payload should be an object"),
            "capture_selectors",
            2,
        )
        .expect("string array should parse");

        assert_eq!(parsed, vec!["#hero".to_owned(), ".nav".to_owned()]);
    }

    #[test]
    fn browser_observe_string_array_parser_rejects_non_strings() {
        let payload = json!({"capture_selectors": ["#hero", 42]});

        let error = parse_browser_observe_string_array(
            payload.as_object().expect("payload should be an object"),
            "capture_selectors",
            8,
        )
        .expect_err("non-string entry should fail");

        assert!(error.contains("capture_selectors"));
        assert!(error.contains("only strings"));
    }

    #[test]
    fn browser_element_captures_to_json_redacts_untrusted_text_and_styles() {
        let naked_token = "ghp_0123456789abcdefghijklmnopqrstuvwxyz";
        let (captures, scans, redacted) =
            browser_element_captures_to_json(&[browser_v1::BrowserElementCapture {
                v: CANONICAL_PROTOCOL_MAJOR,
                selector: format!("#{naked_token}"),
                found: true,
                bounding_rect: Some(browser_v1::BrowserRect {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    x: 1.0,
                    y: 2.0,
                    width: 3.0,
                    height: 4.0,
                    top: 2.0,
                    right: 4.0,
                    bottom: 6.0,
                    left: 1.0,
                }),
                visible: true,
                tag_name: format!("div-{naked_token}"),
                id: "token=super-secret-token-value".to_owned(),
                class_name: "Authorization: Bearer super-secret-token-value".to_owned(),
                text: "Authorization: Bearer super-secret-token-value".to_owned(),
                text_truncated: false,
                computed_styles: vec![browser_v1::BrowserComputedStyle {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    name: format!("content-{naked_token}"),
                    value: "token=super-secret-token-value".to_owned(),
                }],
                error: format!("capture failed for {naked_token}"),
            }]);

        assert!(redacted);
        assert!(!scans.is_empty());
        let serialized = serde_json::to_string(&captures).expect("captures should serialize");
        assert!(!serialized.contains(naked_token), "{serialized}");
        assert_eq!(captures[0]["bounding_rect"]["width"], 3.0);
        assert_eq!(captures[0]["id"], "token=[REDACTED_SECRET]");
        assert_eq!(captures[0]["class_name"], "Authorization: [REDACTED_SECRET]");
        assert_eq!(captures[0]["text"], "Authorization: [REDACTED_SECRET]");
        assert_eq!(captures[0]["computed_styles"][0]["value"], "token=[REDACTED_SECRET]");
    }

    #[test]
    fn browser_private_target_flag_requires_explicit_network_opt_in() {
        for url in [
            "http://localhost:8899/",
            "http://127.0.0.1:8899/",
            "http://[::1]:8899/",
            "http://192.168.1.10/",
        ] {
            assert!(!browser_private_target_flag_for_validated_url(url, false));
            assert!(browser_private_target_flag_for_validated_url(url, true));
        }
        assert!(!browser_private_target_flag_for_validated_url("https://example.com/", false));
        assert!(browser_private_target_flag_for_validated_url("https://example.com/", true));
        assert!(browser_private_target_flag_for_validated_url(
            "file:///workspace/index.html",
            false
        ));
        let opted_in = json!({"allow_private_targets": true});
        assert!(browser_private_targets_requested(
            opted_in.as_object().expect("fixture should be an object")
        ));
        let defaulted = json!({});
        assert!(!browser_private_targets_requested(
            defaulted.as_object().expect("fixture should be an object")
        ));
    }

    #[test]
    fn browser_file_url_scope_accepts_workspace_file_and_rejects_sibling() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should be created");
        std::fs::create_dir_all(outside.as_path()).expect("outside should be created");
        let fixture = workspace.join("table.html");
        let sibling = outside.join("secret.html");
        std::fs::write(fixture.as_path(), "<table></table>").expect("fixture should be written");
        std::fs::write(sibling.as_path(), "secret").expect("sibling should be written");

        let fixture_url = reqwest::Url::from_file_path(fixture.as_path())
            .expect("fixture file URL should be built");
        let target = browser_file_url_to_path(&fixture_url)
            .expect("workspace file URL should resolve to path")
            .canonicalize()
            .expect("target should canonicalize");
        let canonical_workspace = workspace.canonicalize().expect("workspace should canonicalize");
        assert!(canonical_file_path_is_inside_workspace_roots(
            target.as_path(),
            &[canonical_workspace]
        ));

        let sibling_url =
            reqwest::Url::from_file_path(sibling.as_path()).expect("sibling file URL");
        let sibling_target = browser_file_url_to_path(&sibling_url)
            .expect("sibling file URL should resolve to path")
            .canonicalize()
            .expect("sibling should canonicalize");
        let canonical_workspace = workspace.canonicalize().expect("workspace should canonicalize");
        assert!(!canonical_file_path_is_inside_workspace_roots(
            sibling_target.as_path(),
            &[canonical_workspace]
        ));
    }

    #[test]
    fn browser_workspace_file_alias_resolves_portably_and_preserves_public_url() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        let nested = workspace.join("nested");
        std::fs::create_dir_all(nested.as_path()).expect("workspace should be created");
        let fixture = nested.join("index.html");
        std::fs::write(fixture.as_path(), "<main>fixture</main>")
            .expect("fixture should be written");
        let canonical_workspace = workspace.canonicalize().expect("workspace should canonicalize");
        let alias = reqwest::Url::parse("file:///workspace/nested/index.html#result")
            .expect("workspace alias should parse");

        let resolved =
            resolve_browser_file_navigation_url(&alias, std::slice::from_ref(&canonical_workspace))
                .expect("workspace alias should resolve beneath the first root");
        let transport = reqwest::Url::parse(resolved.transport_url.as_str())
            .expect("transport URL should parse");
        let transport_path = browser_file_url_to_path(&transport)
            .expect("transport URL should resolve to a local path")
            .canonicalize()
            .expect("transport target should canonicalize");
        let mut absolute = reqwest::Url::from_file_path(fixture.as_path())
            .expect("absolute fixture URL should be built");
        absolute.set_fragment(Some("result"));
        let absolute = resolve_browser_file_navigation_url(
            &absolute,
            std::slice::from_ref(&canonical_workspace),
        )
        .expect("absolute fixture URL should resolve");

        assert_eq!(transport_path, fixture.canonicalize().expect("fixture should canonicalize"));
        assert_eq!(resolved.model_url, "file:///workspace/nested/index.html#result");
        assert!(resolved.same_destination(&absolute));
        assert_eq!(
            resolved.project_response_url(resolved.transport_url.as_str()),
            "file:///workspace/nested/index.html#result"
        );
    }

    #[test]
    fn browser_workspace_file_alias_cannot_traverse_outside_first_root() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should be created");
        std::fs::create_dir_all(outside.as_path()).expect("outside should be created");
        std::fs::write(outside.join("secret.html"), "secret").expect("sibling should be written");
        let canonical_workspace = workspace.canonicalize().expect("workspace should canonicalize");
        let alias = reqwest::Url::parse("file:///workspace/%2e%2e/outside/secret.html")
            .expect("encoded traversal alias should parse");

        let error =
            resolve_browser_file_navigation_url(&alias, std::slice::from_ref(&canonical_workspace))
                .expect_err("workspace alias traversal must remain outside authority");

        assert!(
            error.contains("active agent workspace roots")
                || error.contains("failed to resolve file URL target")
                || error.contains("file URL path is invalid"),
            "{error}"
        );
    }

    #[cfg(windows)]
    #[test]
    fn browser_path_scope_rejects_case_colliding_windows_root() {
        let target = std::path::PathBuf::from(r"\\?\C:\case\workspace\secret.html");
        let allowed_root = std::path::PathBuf::from(r"\\?\C:\case\Workspace");
        let matching_root = std::path::PathBuf::from(r"\\?\C:\case\workspace");

        assert!(
            !canonical_file_path_is_inside_workspace_roots(
                target.as_path(),
                std::slice::from_ref(&allowed_root)
            ),
            "case-colliding Windows roots must remain distinct"
        );
        assert!(canonical_file_path_is_inside_workspace_roots(target.as_path(), &[matching_root]));
    }

    #[test]
    fn browser_file_url_scope_rejects_run_launch_workspace_root() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let agent_workspace = temp.path().join("agent-workspace");
        let launch_workspace = temp.path().join("launch-workspace");
        std::fs::create_dir_all(agent_workspace.as_path()).expect("workspace should be created");
        std::fs::create_dir_all(launch_workspace.as_path())
            .expect("launch workspace should be created");
        let launch_file = launch_workspace.join("secret.html");
        std::fs::write(launch_file.as_path(), "secret").expect("launch file should be written");

        let launch_url =
            reqwest::Url::from_file_path(launch_file.as_path()).expect("launch file URL");
        let launch_target = browser_file_url_to_path(&launch_url)
            .expect("launch file URL should resolve to path")
            .canonicalize()
            .expect("launch file should canonicalize");
        let canonical_agent =
            agent_workspace.canonicalize().expect("workspace should canonicalize");
        let error = validate_browser_file_url_path_scope(
            launch_url.as_str(),
            launch_target.as_path(),
            std::slice::from_ref(&canonical_agent),
        )
        .expect_err("launch siblings must not widen browser file authority");

        assert!(error.contains("active agent workspace roots"), "{error}");
    }
}
