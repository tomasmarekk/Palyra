use std::{sync::Arc, time::Duration};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use palyra_common::{
    redaction::{redact_header, redact_url},
    validate_canonical_id, CANONICAL_PROTOCOL_MAJOR,
};
use palyra_safety::{
    merge_scan_results, redact_text_for_export, ExportRedactionOutcome, SafetyContentKind,
    SafetyPhase, SafetyScanResult, SafetySourceKind, TrustLabel,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tonic::{Request, Status};
use ulid::Ulid;

use crate::{
    gateway::{
        current_unix_ms, truncate_with_ellipsis, BrowserServiceRuntimeConfig, GatewayRuntimeState,
        BROWSER_CLICK_TOOL_NAME, BROWSER_CONSOLE_LOG_TOOL_NAME, BROWSER_HIGHLIGHT_TOOL_NAME,
        BROWSER_NAVIGATE_TOOL_NAME, BROWSER_NETWORK_LOG_TOOL_NAME, BROWSER_OBSERVE_TOOL_NAME,
        BROWSER_PDF_TOOL_NAME, BROWSER_PERMISSIONS_GET_TOOL_NAME,
        BROWSER_PERMISSIONS_SET_TOOL_NAME, BROWSER_PRESS_TOOL_NAME, BROWSER_RESET_STATE_TOOL_NAME,
        BROWSER_SCREENSHOT_TOOL_NAME, BROWSER_SCROLL_TOOL_NAME, BROWSER_SELECT_TOOL_NAME,
        BROWSER_SESSION_CLOSE_TOOL_NAME, BROWSER_SESSION_CREATE_TOOL_NAME,
        BROWSER_TABS_CLOSE_TOOL_NAME, BROWSER_TABS_LIST_TOOL_NAME, BROWSER_TABS_OPEN_TOOL_NAME,
        BROWSER_TABS_SWITCH_TOOL_NAME, BROWSER_TITLE_TOOL_NAME, BROWSER_TYPE_TOOL_NAME,
        BROWSER_WAIT_FOR_TOOL_NAME, MAX_BROWSER_TOOL_INPUT_BYTES,
    },
    tool_protocol::{ToolAttestation, ToolExecutionOutcome},
    transport::grpc::proto::palyra::{browser::v1 as browser_v1, common::v1 as common_v1},
};
pub(crate) async fn execute_browser_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    principal: &str,
    channel: Option<&str>,
    tool_name: &str,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    if input_json.len() > MAX_BROWSER_TOOL_INPUT_BYTES {
        return browser_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.browser.* input exceeds {MAX_BROWSER_TOOL_INPUT_BYTES} bytes"),
        );
    }
    if !runtime_state.config.browser_service.enabled {
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

    let mut client =
        match connect_browser_service(runtime_state.config.browser_service.clone()).await {
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

    let outcome = match tool_name {
        BROWSER_SESSION_CREATE_TOOL_NAME => {
            let idle_ttl_ms = payload.get("idle_ttl_ms").and_then(Value::as_u64).unwrap_or(0);
            let allow_private_targets =
                payload.get("allow_private_targets").and_then(Value::as_bool).unwrap_or(false);
            let profile_id = match payload.get("profile_id") {
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
                                    format!(
                                        "palyra.browser.session.create profile_id is invalid: {error}"
                                    ),
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
                        "palyra.browser.session.create field 'profile_id' must be a string"
                            .to_owned(),
                    );
                }
                None => None,
            };
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
            let mut request = Request::new(browser_v1::CreateSessionRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                principal: principal.to_owned(),
                idle_ttl_ms,
                budget,
                allow_private_targets,
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
                persistence_enabled: payload
                    .get("persistence_enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                persistence_id: payload
                    .get("persistence_id")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .unwrap_or_default()
                    .to_owned(),
                profile_id,
                private_profile: payload
                    .get("private_profile")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                channel: channel.unwrap_or_default().to_owned(),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
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
                    let session_id =
                        if let Some(value) = response.session_id { Some(value.ulid) } else { None };
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
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
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
            let mut request = Request::new(browser_v1::NavigateRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                url: url.to_owned(),
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                allow_redirects: payload
                    .get("allow_redirects")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_redirects: payload.get("max_redirects").and_then(Value::as_u64).unwrap_or(3)
                    as u32,
                allow_private_targets: payload
                    .get("allow_private_targets")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
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
                    let output = json!({
                        "success": response.success,
                        "final_url": response.final_url,
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
                    .unwrap_or(runtime_state.config.browser_service.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
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
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64": STANDARD
                            .encode(response.failure_screenshot_bytes.as_slice()),
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
        BROWSER_TYPE_TOOL_NAME => {
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
                    "palyra.browser.type requires non-empty string field 'selector'".to_owned(),
                );
            };
            if selector.is_empty() {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.type requires non-empty string field 'selector'".to_owned(),
                );
            }
            let text = payload.get("text").and_then(Value::as_str).unwrap_or_default();
            let mut request = Request::new(browser_v1::TypeRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                selector: selector.to_owned(),
                text: text.to_owned(),
                clear_existing: payload
                    .get("clear_existing")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                capture_failure_screenshot: payload
                    .get("capture_failure_screenshot")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_failure_screenshot_bytes: payload
                    .get("max_failure_screenshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(runtime_state.config.browser_service.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
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
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64": STANDARD
                            .encode(response.failure_screenshot_bytes.as_slice()),
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
                    format!("palyra.browser.type failed: {}", sanitize_status_message(&error)),
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
            let Some(key) = payload.get("key").and_then(Value::as_str).map(str::trim) else {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.press requires non-empty string field 'key'".to_owned(),
                );
            };
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
                key: key.to_owned(),
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                capture_failure_screenshot: payload
                    .get("capture_failure_screenshot")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_failure_screenshot_bytes: payload
                    .get("max_failure_screenshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(runtime_state.config.browser_service.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
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
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64": STANDARD
                            .encode(response.failure_screenshot_bytes.as_slice()),
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
                    .unwrap_or(runtime_state.config.browser_service.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
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
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64": STANDARD
                            .encode(response.failure_screenshot_bytes.as_slice()),
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
                    .unwrap_or(runtime_state.config.browser_service.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
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
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64": STANDARD
                            .encode(response.failure_screenshot_bytes.as_slice()),
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
                    .unwrap_or(runtime_state.config.browser_service.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
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
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64": STANDARD
                            .encode(response.failure_screenshot_bytes.as_slice()),
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
                    .unwrap_or(runtime_state.config.browser_service.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
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
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64": STANDARD
                            .encode(response.failure_screenshot_bytes.as_slice()),
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
                    .unwrap_or(runtime_state.config.browser_service.max_title_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
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
                    .unwrap_or(runtime_state.config.browser_service.max_screenshot_bytes as u64),
                format: payload.get("format").and_then(Value::as_str).unwrap_or("png").to_owned(),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
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
                    let output = json!({
                        "success": response.success,
                        "mime_type": response.mime_type,
                        "image_base64": STANDARD
                            .encode(response.image_bytes.as_slice()),
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
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
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
                    let output = json!({
                        "success": response.success,
                        "mime_type": response.mime_type,
                        "size_bytes": response.size_bytes,
                        "sha256": response.sha256,
                        "artifact": response.artifact.map(browser_download_artifact_to_json),
                        "pdf_base64": STANDARD.encode(response.pdf_bytes.as_slice()),
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
                include_visible_text: payload
                    .get("include_visible_text")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
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
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
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
                    let page_url = redact_url(response.page_url.as_str());
                    let observation_scan = merge_scan_results(
                        SafetyPhase::Export,
                        SafetySourceKind::Browser,
                        SafetyContentKind::BrowserObservation,
                        &[
                            dom_export.scan.clone(),
                            accessibility_export.scan.clone(),
                            visible_text_export.scan.clone(),
                        ],
                    );
                    let output = json!({
                        "success": response.success,
                        "dom_snapshot": dom_export.redacted_text,
                        "accessibility_tree": accessibility_export.redacted_text,
                        "visible_text": visible_text_export.redacted_text,
                        "dom_truncated": response.dom_truncated,
                        "accessibility_tree_truncated": response.accessibility_tree_truncated,
                        "visible_text_truncated": response.visible_text_truncated,
                        "page_url": page_url,
                        "safety": browser_safety_json(
                            &observation_scan,
                            dom_export.redacted
                                || accessibility_export.redacted
                                || visible_text_export.redacted
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
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
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
                    let exported_entries = response
                        .entries
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
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
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
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
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
                runtime_state.config.browser_service.auth_token.as_deref(),
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
            let mut request = Request::new(browser_v1::OpenTabRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                url: payload.get("url").and_then(Value::as_str).unwrap_or_default().to_owned(),
                activate: payload.get("activate").and_then(Value::as_bool).unwrap_or(true),
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                allow_redirects: payload
                    .get("allow_redirects")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_redirects: payload.get("max_redirects").and_then(Value::as_u64).unwrap_or(3)
                    as u32,
                allow_private_targets: payload
                    .get("allow_private_targets")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
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
                    let output = json!({
                        "success": response.success,
                        "tab": response.tab.map(browser_tab_to_json),
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
                runtime_state.config.browser_service.auth_token.as_deref(),
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
                runtime_state.config.browser_service.auth_token.as_deref(),
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
                runtime_state.config.browser_service.auth_token.as_deref(),
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
                runtime_state.config.browser_service.auth_token.as_deref(),
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
        _ => (false, b"{}".to_vec(), "palyra.browser.* unsupported tool name".to_owned()),
    };

    browser_tool_execution_outcome(proposal_id, input_json, outcome.0, outcome.1, outcome.2)
}

async fn connect_browser_service(
    config: BrowserServiceRuntimeConfig,
) -> Result<
    browser_v1::browser_service_client::BrowserServiceClient<tonic::transport::Channel>,
    String,
> {
    let endpoint = tonic::transport::Endpoint::from_shared(config.endpoint.clone())
        .map_err(|error| {
            format!("invalid browser service endpoint '{}': {error}", config.endpoint)
        })?
        .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
        .timeout(Duration::from_millis(config.request_timeout_ms));
    let channel = endpoint.connect().await.map_err(|error| {
        format!("failed to connect to browser service '{}': {error}", config.endpoint)
    })?;
    Ok(browser_v1::browser_service_client::BrowserServiceClient::new(channel))
}

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

fn sanitize_status_message(status: &Status) -> String {
    truncate_with_ellipsis(status.message().to_owned(), 512)
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

struct BrowserValueExport {
    value: Value,
    scan: SafetyScanResult,
    redacted: bool,
}

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
    headers.sort_by(|left, right| {
        let left_name = left.get("name").and_then(Value::as_str).unwrap_or_default();
        let right_name = right.get("name").and_then(Value::as_str).unwrap_or_default();
        left_name.cmp(right_name)
    });
    let scan = export_browser_text(raw_scan_input.as_str(), SafetyContentKind::BrowserNetwork);
    let request_url = redact_url(entry.request_url.as_str());
    BrowserValueExport {
        value: json!({
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

fn browser_tool_execution_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    let executed_at_unix_ms = current_unix_ms();
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
            attestation_id: Ulid::new().to_string(),
            execution_sha256,
            executed_at_unix_ms,
            timed_out: false,
            executor: "browser_broker".to_owned(),
            sandbox_enforcement: "browser_service".to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{browser_console_entry_to_json, browser_network_log_entry_to_json};
    use crate::transport::grpc::proto::palyra::browser::v1 as browser_v1;
    use palyra_common::CANONICAL_PROTOCOL_MAJOR;

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
    fn network_log_export_redacts_sensitive_headers() {
        let exported = browser_network_log_entry_to_json(browser_v1::NetworkLogEntry {
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
        });
        assert_eq!(exported.value["headers"][0]["value"], "<redacted>");
        assert_eq!(exported.value["safety"]["action"], "redact");
        assert!(exported.redacted);
    }
}
