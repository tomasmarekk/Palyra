//! gRPC implementation of the `palyra.browser.v1` BrowserService contract.
//!
//! Every RPC authorizes the caller, resolves the session, delegates to the
//! simulated or Chromium engine, then records the action log and persists the
//! session snapshot when persistence is enabled. Most failures are reported
//! in the response payload (`success`/`error`) so clients keep receiving
//! session context; `Status` errors are reserved for auth, argument
//! validation, and infrastructure faults.

use crate::*;

/// Request-extension marker that forces `allow_private_targets = false` for
/// relay-initiated `open_tab` calls regardless of session settings.
///
/// Relay requests originate from browser-extension content, which must never
/// be able to point the daemon at private/internal addresses.
#[derive(Debug, Clone, Copy)]
struct RelayPrivateTargetBlock;

/// Smallest well-formed-enough PDF, returned by the simulated engine's export.
const MINIMAL_SIMULATED_PDF: &[u8] = b"%PDF-1.4\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\n";
const MIN_VIEWPORT_WIDTH: u32 = 50;
const MAX_VIEWPORT_WIDTH: u32 = 10_000;
const MIN_VIEWPORT_HEIGHT: u32 = 50;
const MAX_VIEWPORT_HEIGHT: u32 = 10_000;
const DEFAULT_DEVICE_SCALE_FACTOR: f64 = 1.0;
const MAX_DEVICE_SCALE_FACTOR: f64 = 8.0;
/// Caps width x height before scaling (16 MP).
const MAX_VIEWPORT_CSS_PIXELS: u64 = 16_000_000;
/// 8K UHD (7680 x 4320) - ceiling for width x height x scale^2.
const MAX_VIEWPORT_EFFECTIVE_PIXELS: f64 = 33_177_600.0;
const MAX_OBSERVE_CAPTURE_SELECTORS: usize = 8;
const MAX_OBSERVE_COMPUTED_STYLE_PROPERTIES: usize = 16;
const DEFAULT_OBSERVE_CAPTURE_TEXT_BYTES: u64 = 512;
/// Styles captured when the caller requests none, chosen to explain the most
/// common "element exists but is not visible/clickable" diagnoses.
const DEFAULT_OBSERVE_COMPUTED_STYLE_PROPERTIES: &[&str] = &[
    "display",
    "visibility",
    "opacity",
    "position",
    "z-index",
    "overflow",
    "pointer-events",
    "font-size",
    "line-height",
    "margin-top",
    "margin-bottom",
    "padding-top",
    "padding-bottom",
];

/// gRPC handler for the browser service; all state lives in the shared runtime.
#[derive(Clone)]
pub(crate) struct BrowserServiceImpl {
    pub(crate) runtime: Arc<BrowserRuntimeState>,
}

/// Maps a navigation result to a stable action-log outcome label.
///
/// `NavigateOutcome` carries only free-text errors, so classification sniffs
/// known substrings; keep them in sync with the error strings produced by
/// `navigate_with_guards` and the Chromium engine.
fn navigate_action_outcome(outcome: &NavigateOutcome) -> &'static str {
    if outcome.success {
        return "navigated";
    }
    let error = outcome.error.to_ascii_lowercase();
    if error.contains("blocked url scheme") || error.contains("private/local") {
        "policy_blocked"
    } else if error.contains("socks5") || error.contains("proxy") {
        "browser_proxy_failed"
    } else if error.contains("chromium") || error.contains("tab runtime") {
        "browser_runtime_failed"
    } else if error.contains("request failed") || error.contains("error sending request") {
        "network_request_failed"
    } else {
        "navigation_failed"
    }
}

fn browser_layout_metrics_to_proto(
    metrics: ChromiumLayoutMetrics,
) -> browser_v1::BrowserLayoutMetrics {
    browser_v1::BrowserLayoutMetrics {
        v: CANONICAL_PROTOCOL_MAJOR,
        viewport_width: metrics.viewport_width,
        viewport_height: metrics.viewport_height,
        device_scale_factor: metrics.device_scale_factor,
        document_scroll_width: metrics.document_scroll_width,
        document_scroll_height: metrics.document_scroll_height,
        document_client_width: metrics.document_client_width,
        document_client_height: metrics.document_client_height,
        horizontal_overflow: metrics.horizontal_overflow,
        vertical_overflow: metrics.vertical_overflow,
    }
}

fn browser_dialog_event_to_proto(event: BrowserDialogEvent) -> browser_v1::BrowserDialogEvent {
    browser_v1::BrowserDialogEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        generation: event.generation,
        tab_id: Some(proto::palyra::common::v1::CanonicalId { ulid: event.tab_id }),
        dialog_type: event.dialog_type,
        message: event.message,
        default_prompt: event.default_prompt,
        page_url: event.page_url,
        opened_at_unix_ms: event.opened_at_unix_ms,
        expires_at_unix_ms: event.expires_at_unix_ms,
    }
}

/// Effective observe byte cap: zero means the session limit, anything else is
/// clamped to it, with a floor of one byte.
fn observe_byte_limit(requested: u64, session_limit: u64) -> usize {
    let limit = if requested == 0 { session_limit } else { requested.min(session_limit) }.max(1);
    usize::try_from(limit).unwrap_or(usize::MAX)
}

/// Like `observe_byte_limit`, but an unspecified request gets a small bounded
/// default instead of the full session limit (captures multiply per selector).
fn observe_capture_text_limit(requested: u64, session_limit: u64) -> usize {
    let requested = if requested == 0 { DEFAULT_OBSERVE_CAPTURE_TEXT_BYTES } else { requested };
    observe_byte_limit(requested, session_limit)
}

fn append_reset_state_error(current: &mut String, next: impl AsRef<str>) {
    let next = next.as_ref();
    if next.trim().is_empty() {
        return;
    }
    if current.is_empty() {
        current.push_str(next);
    } else {
        current.push_str("; ");
        current.push_str(next);
    }
}

/// Trims, dedupes, length-caps, and count-caps requested capture selectors.
fn normalize_observe_capture_selectors(selectors: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    for selector in selectors {
        let trimmed = selector.trim();
        if trimmed.is_empty() || normalized.iter().any(|existing: &String| existing == trimmed) {
            continue;
        }
        normalized.push(truncate_utf8_bytes(trimmed, 512));
        if normalized.len() >= MAX_OBSERVE_CAPTURE_SELECTORS {
            break;
        }
    }
    normalized
}

/// Sanitizes requested computed-style property names to kebab-case CSS names,
/// substituting the default capture set when none are requested.
fn normalize_observe_computed_style_properties(properties: &[String]) -> Vec<String> {
    let source = if properties.is_empty() {
        DEFAULT_OBSERVE_COMPUTED_STYLE_PROPERTIES
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>()
    } else {
        properties.to_vec()
    };
    let mut normalized = Vec::new();
    for property in source {
        let raw = property.trim();
        let trimmed = raw.to_ascii_lowercase();
        // The windows(2) scan rejects camelCase spellings (e.g. a JS-style
        // property name): silently lowercasing one would capture a different
        // property than the caller asked for.
        if trimmed.is_empty()
            || trimmed.len() > 64
            || raw.chars().any(char::is_whitespace)
            || raw
                .as_bytes()
                .windows(2)
                .any(|window| window[0].is_ascii_lowercase() && window[1].is_ascii_uppercase())
            || !trimmed.chars().all(|ch| ch.is_ascii_lowercase() || ch == '-')
            || normalized.iter().any(|existing: &String| existing == &trimmed)
        {
            continue;
        }
        normalized.push(trimmed);
        if normalized.len() >= MAX_OBSERVE_COMPUTED_STYLE_PROPERTIES {
            break;
        }
    }
    normalized
}

/// Builds the not-found/error capture shape returned per failing selector.
fn observe_element_capture_error(selector: &str, error: &str) -> browser_v1::BrowserElementCapture {
    browser_v1::BrowserElementCapture {
        v: CANONICAL_PROTOCOL_MAJOR,
        selector: selector.to_owned(),
        found: false,
        bounding_rect: None,
        visible: false,
        tag_name: String::new(),
        id: String::new(),
        class_name: String::new(),
        text: String::new(),
        text_truncated: false,
        computed_styles: Vec::new(),
        error: error.to_owned(),
    }
}

/// Effective timeout: zero means the session limit; anything else is clamped
/// between one millisecond and the session limit.
fn request_timeout_ms(requested: u64, session_limit: u64) -> u64 {
    let limit = session_limit.max(1);
    if requested == 0 {
        limit
    } else {
        requested.max(1).min(limit)
    }
}

/// True when a navigation failure looks like Chromium aborting because the
/// response turned into a download (ERR_ABORTED); callers may then convert the
/// failure into a successful download capture.
fn navigation_error_may_be_download_abort(error: &str) -> bool {
    let normalized = error.to_ascii_lowercase();
    normalized.contains("err_aborted") || normalized.contains("net::err_aborted")
}

/// Drains downloads captured by the Chromium engine for the active tab and
/// stores each through the quarantine pipeline, returning the first record.
///
/// # Errors
/// Returns the drain or storage failure reason.
async fn store_chromium_captured_downloads(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    profile_id: Option<&str>,
) -> Result<Option<DownloadArtifactRecord>, String> {
    let active_tab_id = {
        let sessions = runtime.sessions.lock().await;
        sessions.get(session_id).map(|session| session.active_tab_id.clone())
    };
    let Some(active_tab_id) = active_tab_id else {
        return Ok(None);
    };
    let downloads =
        chromium_drain_client_downloads(runtime, session_id, active_tab_id.as_str()).await?;
    let mut first_record = None;
    for download in downloads {
        let mime_type = sniff_download_mime_type(
            (!download.mime_type.trim().is_empty()).then_some(download.mime_type.as_str()),
            download.file_name.as_str(),
            download.content.as_slice(),
        );
        let record = store_generated_artifact(
            runtime,
            session_id,
            profile_id,
            download.source_url.as_str(),
            download.file_name.as_str(),
            mime_type.as_str(),
            download.content.as_slice(),
        )
        .await?;
        if first_record.is_none() {
            first_record = Some(record);
        }
    }
    Ok(first_record)
}

/// Which sections an observe response should include.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ObserveInclusions {
    include_dom_snapshot: bool,
    include_accessibility_tree: bool,
    include_visible_text: bool,
}

#[derive(Debug, Default)]
struct BrowserHealthRollup {
    healthy_sessions: u32,
    degraded_sessions: u32,
    reconnecting_sessions: u32,
    blocked_sessions: u32,
    process_reconnect_count: u64,
    target_reconnect_count: u64,
    dialog_timeout_count: u64,
}

async fn browser_health_rollup(runtime: &BrowserRuntimeState) -> BrowserHealthRollup {
    if runtime.engine_mode != BrowserEngineMode::Chromium {
        return BrowserHealthRollup {
            healthy_sessions: u32::try_from(runtime.sessions.lock().await.len())
                .unwrap_or(u32::MAX),
            ..BrowserHealthRollup::default()
        };
    }
    let trackers =
        runtime.browser_session_health.lock().await.values().cloned().collect::<Vec<_>>();
    let mut rollup = BrowserHealthRollup::default();
    for tracker in trackers {
        let Ok(tracker) = tracker.lock() else {
            rollup.degraded_sessions = rollup.degraded_sessions.saturating_add(1);
            continue;
        };
        let snapshot = tracker.snapshot();
        match snapshot.state {
            BrowserSessionHealthState::Ready => {
                rollup.healthy_sessions = rollup.healthy_sessions.saturating_add(1);
            }
            BrowserSessionHealthState::Degraded => {
                rollup.degraded_sessions = rollup.degraded_sessions.saturating_add(1);
            }
            BrowserSessionHealthState::Reconnecting => {
                rollup.reconnecting_sessions = rollup.reconnecting_sessions.saturating_add(1);
            }
            BrowserSessionHealthState::Blocked => {
                rollup.blocked_sessions = rollup.blocked_sessions.saturating_add(1);
            }
        }
        rollup.process_reconnect_count =
            rollup.process_reconnect_count.saturating_add(snapshot.process_reconnect_count);
        rollup.target_reconnect_count =
            rollup.target_reconnect_count.saturating_add(snapshot.target_reconnect_count);
        rollup.dialog_timeout_count =
            rollup.dialog_timeout_count.saturating_add(snapshot.dialog_timeout_count);
    }
    rollup
}

async fn browser_session_health_to_proto(
    runtime: &BrowserRuntimeState,
    session_id: &str,
) -> browser_v1::BrowserSessionHealth {
    let snapshot = if runtime.engine_mode == BrowserEngineMode::Chromium {
        let tracker = runtime.browser_session_health.lock().await.get(session_id).cloned();
        tracker
            .and_then(|tracker| tracker.lock().ok().map(|tracker| tracker.snapshot()))
            .unwrap_or_else(|| BrowserSessionHealth::default().snapshot())
    } else {
        let mut health = BrowserSessionHealth::default();
        health.mark_initial_ready();
        health.snapshot()
    };
    let pending_dialog_count = if runtime.engine_mode == BrowserEngineMode::Chromium {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        chromium_sessions
            .get(session_id)
            .map(|session| {
                session
                    .dialog_trackers
                    .values()
                    .filter(|tracker| {
                        tracker.lock().is_ok_and(|tracker| tracker.pending().is_some())
                    })
                    .count()
            })
            .and_then(|count| u32::try_from(count).ok())
            .unwrap_or(0)
    } else {
        0
    };
    browser_v1::BrowserSessionHealth {
        v: CANONICAL_PROTOCOL_MAJOR,
        state: snapshot.state.as_str().to_owned(),
        resilience_profile: runtime.resilience_profile.name().to_owned(),
        automatic_reconnect_enabled: runtime.resilience_profile.automatic_reconnect,
        runtime_generation: snapshot.runtime_generation,
        process_reconnect_count: snapshot.process_reconnect_count,
        target_reconnect_count: snapshot.target_reconnect_count,
        dialog_timeout_count: snapshot.dialog_timeout_count,
        dialog_navigation_cleanup_count: snapshot.dialog_navigation_cleanup_count,
        dialog_close_cleanup_count: snapshot.dialog_close_cleanup_count,
        pending_dialog_count,
        reason_code: snapshot.reason_code,
        updated_at_unix_ms: snapshot.updated_at_unix_ms,
    }
}

/// Preserves every caller-selected observe inclusion bit, including all-false.
fn resolve_observe_inclusions(
    include_dom_snapshot: bool,
    include_accessibility_tree: bool,
    include_visible_text: bool,
) -> ObserveInclusions {
    ObserveInclusions { include_dom_snapshot, include_accessibility_tree, include_visible_text }
}

#[tonic::async_trait]
impl browser_v1::browser_service_server::BrowserService for BrowserServiceImpl {
    async fn health(
        &self,
        request: Request<browser_v1::BrowserHealthRequest>,
    ) -> Result<Response<browser_v1::BrowserHealthResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let active_sessions = self.runtime.sessions.lock().await.len();
        let rollup = browser_health_rollup(self.runtime.as_ref()).await;
        let status = if rollup.degraded_sessions > 0
            || rollup.reconnecting_sessions > 0
            || rollup.blocked_sessions > 0
        {
            "degraded"
        } else {
            "ok"
        };
        Ok(Response::new(browser_v1::BrowserHealthResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            status: status.to_owned(),
            uptime_seconds: self.runtime.started_at.elapsed().as_secs(),
            active_sessions: u32::try_from(active_sessions).unwrap_or(u32::MAX),
            engine_mode: self.runtime.engine_mode.as_str().to_owned(),
            javascript_execution_enabled: self.runtime.engine_mode.executes_javascript(),
            subresource_loading_enabled: self.runtime.engine_mode.loads_subresources(),
            dom_interaction_enabled: self.runtime.engine_mode.supports_live_dom_interaction(),
            resilience_profile: self.runtime.resilience_profile.name().to_owned(),
            automatic_reconnect_enabled: self.runtime.resilience_profile.automatic_reconnect,
            healthy_sessions: rollup.healthy_sessions,
            degraded_sessions: rollup.degraded_sessions,
            reconnecting_sessions: rollup.reconnecting_sessions,
            blocked_sessions: rollup.blocked_sessions,
            process_reconnect_count: rollup.process_reconnect_count,
            target_reconnect_count: rollup.target_reconnect_count,
            dialog_timeout_count: rollup.dialog_timeout_count,
        }))
    }

    async fn create_session(
        &self,
        request: Request<browser_v1::CreateSessionRequest>,
    ) -> Result<Response<browser_v1::CreateSessionResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let authenticated_principal =
            authenticated_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let principal = payload.principal.trim();
        if principal.is_empty() {
            return Err(Status::invalid_argument("principal is required"));
        }
        enforce_authenticated_body_principal(authenticated_principal.as_deref(), principal)?;
        let channel = normalize_optional_string(payload.channel.as_str());
        let requested_profile_id = parse_optional_profile_id_from_proto(payload.profile_id.take())
            .map_err(Status::invalid_argument)?;
        let mut profile = resolve_session_profile(
            self.runtime.as_ref(),
            principal,
            requested_profile_id.as_deref(),
        )
        .await
        .map_err(Status::internal)?;

        let mut private_profile = payload.private_profile;
        let mut persistence_enabled = payload.persistence_enabled;
        let mut persistence_id = if payload.persistence_enabled {
            let fallback_profile_persistence_id =
                profile.as_ref().map(|profile| profile.profile_id.as_str()).unwrap_or_default();
            let Some(value) = sanitize_persistence_id(payload.persistence_id.as_str())
                .or_else(|| sanitize_persistence_id(fallback_profile_persistence_id))
            else {
                return Err(Status::invalid_argument(
                    "persistence_enabled=true requires non-empty persistence_id or profile_id",
                ));
            };
            Some(value)
        } else {
            None
        };
        let mut profile_id = None;
        if let Some(resolved_profile) = profile.as_ref() {
            profile_id = Some(resolved_profile.profile_id.clone());
            private_profile = private_profile || resolved_profile.private_profile;
            if resolved_profile.persistence_enabled && !private_profile {
                persistence_enabled = true;
                persistence_id = Some(resolved_profile.profile_id.clone());
            } else {
                persistence_enabled = false;
                persistence_id = None;
            }
        }

        let restored_snapshot = if persistence_enabled {
            let Some(store) = self.runtime.state_store.as_ref() else {
                return Err(Status::failed_precondition(
                    "state persistence requires PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
                ));
            };
            let Some(state_id) = persistence_id.as_ref() else {
                return Err(Status::invalid_argument(
                    "persistence_enabled=true requires non-empty persistence_id",
                ));
            };
            store.load_snapshot(state_id.as_str(), profile_id.as_deref()).map_err(|error| {
                Status::internal(format!("failed to load persisted state: {error}"))
            })?
        } else {
            None
        };

        let session_id = Ulid::generate().to_string();
        let now = Instant::now();
        let idle_ttl = if payload.idle_ttl_ms == 0 {
            self.runtime.default_idle_ttl
        } else {
            Duration::from_millis(payload.idle_ttl_ms)
        };
        let requested_budget = payload.budget.as_ref();
        // Clamp helpers: zero/absent means "use the daemon default"; explicit
        // requests are capped at the default so sessions cannot raise limits.
        let clamp_u64_budget = |requested: Option<u64>, default: u64| {
            requested.filter(|value| *value > 0).map(|value| value.min(default)).unwrap_or(default)
        };
        let clamp_usize_budget = |requested: Option<usize>, default: usize| {
            requested.filter(|value| *value > 0).map(|value| value.min(default)).unwrap_or(default)
        };
        let budget = SessionBudget {
            max_navigation_timeout_ms: clamp_u64_budget(
                requested_budget.map(|value| value.max_navigation_timeout_ms),
                self.runtime.default_budget.max_navigation_timeout_ms,
            ),
            max_session_lifetime_ms: clamp_u64_budget(
                requested_budget.map(|value| value.max_session_lifetime_ms),
                self.runtime.default_budget.max_session_lifetime_ms,
            ),
            max_screenshot_bytes: clamp_u64_budget(
                requested_budget.map(|value| value.max_screenshot_bytes),
                self.runtime.default_budget.max_screenshot_bytes,
            ),
            max_response_bytes: clamp_u64_budget(
                requested_budget.map(|value| value.max_response_bytes),
                self.runtime.default_budget.max_response_bytes,
            ),
            max_action_timeout_ms: clamp_u64_budget(
                requested_budget.map(|value| value.max_action_timeout_ms),
                self.runtime.default_budget.max_action_timeout_ms,
            ),
            max_type_input_bytes: clamp_u64_budget(
                requested_budget.map(|value| value.max_type_input_bytes),
                self.runtime.default_budget.max_type_input_bytes,
            ),
            max_actions_per_session: clamp_u64_budget(
                requested_budget.map(|value| value.max_actions_per_session),
                self.runtime.default_budget.max_actions_per_session,
            ),
            max_actions_per_window: clamp_u64_budget(
                requested_budget.map(|value| value.max_actions_per_window),
                self.runtime.default_budget.max_actions_per_window,
            ),
            action_rate_window_ms: clamp_u64_budget(
                requested_budget.map(|value| value.action_rate_window_ms),
                self.runtime.default_budget.action_rate_window_ms,
            ),
            max_action_log_entries: clamp_usize_budget(
                requested_budget
                    .map(|value| value.max_action_log_entries)
                    .and_then(|value| usize::try_from(value).ok()),
                self.runtime.default_budget.max_action_log_entries,
            ),
            max_observe_snapshot_bytes: clamp_u64_budget(
                requested_budget.map(|value| value.max_observe_snapshot_bytes),
                self.runtime.default_budget.max_observe_snapshot_bytes,
            ),
            max_visible_text_bytes: clamp_u64_budget(
                requested_budget.map(|value| value.max_visible_text_bytes),
                self.runtime.default_budget.max_visible_text_bytes,
            ),
            max_network_log_entries: clamp_usize_budget(
                requested_budget
                    .map(|value| value.max_network_log_entries)
                    .and_then(|value| usize::try_from(value).ok()),
                self.runtime.default_budget.max_network_log_entries,
            ),
            max_network_log_bytes: clamp_u64_budget(
                requested_budget.map(|value| value.max_network_log_bytes),
                self.runtime.default_budget.max_network_log_bytes,
            ),
            max_tabs_per_session: self.runtime.default_budget.max_tabs_per_session,
            max_title_bytes: self.runtime.default_budget.max_title_bytes,
        };
        let action_allowed_domains =
            normalize_action_allowed_domains(payload.action_allowed_domains.as_slice());
        let mut session = BrowserSessionRecord::with_defaults(BrowserSessionInit {
            principal: principal.to_owned(),
            channel: channel.clone(),
            now,
            idle_ttl,
            budget: budget.clone(),
            allow_private_targets: payload.allow_private_targets,
            allow_downloads: payload.allow_downloads,
            action_allowed_domains: action_allowed_domains.clone(),
            profile_id: profile_id.clone(),
            private_profile,
            persistence: SessionPersistenceState {
                enabled: persistence_enabled,
                persistence_id: persistence_id.clone(),
                state_restored: false,
            },
        });
        if let Some(restored_snapshot) = restored_snapshot {
            if let Some(profile_record) = profile.as_ref() {
                validate_restored_snapshot_against_profile(
                    &restored_snapshot.snapshot,
                    Some(restored_snapshot.raw_hash_sha256.as_str()),
                    profile_record,
                )
                .map_err(|error| {
                    Status::failed_precondition(format!(
                        "persisted state integrity validation failed: {error}"
                    ))
                })?;
            }
            let snapshot = restored_snapshot.snapshot;
            if snapshot.principal != principal {
                return Err(Status::permission_denied(
                    "persisted state principal does not match session principal",
                ));
            }
            if normalize_optional_string(snapshot.channel.as_deref().unwrap_or_default()) != channel
            {
                return Err(Status::permission_denied(
                    "persisted state channel does not match session channel",
                ));
            }
            session.apply_snapshot(snapshot);
            session.persistence.state_restored = true;
        }
        if let Some(record) = profile.as_mut() {
            record.last_used_unix_ms = current_unix_ms();
            record.updated_at_unix_ms = record.last_used_unix_ms;
            refresh_profile_record_hash(record);
            if let Some(store) = self.runtime.state_store.as_ref() {
                upsert_profile_record(
                    store,
                    &self.runtime.profile_registry_lock,
                    record.clone(),
                    false,
                )
                .await
                .map_err(|error| {
                    Status::internal(format!("failed to update browser profile usage: {error}"))
                })?;
            }
        }
        let state_restored = session.persistence.state_restored;
        let persist_on_create = persistence_enabled;
        let mut session_for_persist = None;
        {
            let mut sessions = self.runtime.sessions.lock().await;
            if sessions.len() >= self.runtime.max_sessions {
                return Err(Status::resource_exhausted("browser session capacity reached"));
            }
            sessions.insert(session_id.clone(), session.clone());
            if persist_on_create {
                session_for_persist = Some(session);
            }
        }
        if let Some(record) = session_for_persist {
            persist_session_snapshot(self.runtime.as_ref(), &record)
                .await
                .map_err(|error| Status::internal(format!("failed to persist state: {error}")))?;
        }
        if payload.allow_downloads {
            let sandbox = DownloadSandboxSession::new().map_err(Status::internal)?;
            self.runtime.download_sessions.lock().await.insert(session_id.clone(), sandbox);
        }
        if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            let session_snapshot = {
                let sessions = self.runtime.sessions.lock().await;
                sessions.get(session_id.as_str()).cloned()
            }
            .ok_or_else(|| Status::internal("session registration race during engine init"))?;
            if let Err(error) = initialize_chromium_session_runtime(
                self.runtime.as_ref(),
                session_id.as_str(),
                &session_snapshot,
            )
            .await
            {
                self.runtime.sessions.lock().await.remove(session_id.as_str());
                self.runtime.browser_session_health.lock().await.remove(session_id.as_str());
                self.runtime.download_sessions.lock().await.remove(session_id.as_str());
                return Err(Status::internal(format!(
                    "failed to initialize chromium session runtime: {error}"
                )));
            }
        }

        Ok(Response::new(browser_v1::CreateSessionResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            created_at_unix_ms: current_unix_ms(),
            effective_budget: Some(session_budget_to_proto(&budget)),
            downloads_enabled: payload.allow_downloads,
            action_allowed_domains,
            persistence_enabled,
            persistence_id: persistence_id.unwrap_or_default(),
            state_restored,
            profile_id: profile_id
                .clone()
                .map(|value| proto::palyra::common::v1::CanonicalId { ulid: value }),
            private_profile,
        }))
    }

    async fn close_session(
        &self,
        request: Request<browser_v1::CloseSessionRequest>,
    ) -> Result<Response<browser_v1::CloseSessionResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal =
            optional_cleanup_request_principal(request.metadata())?.map(str::to_owned);
        let session_id = parse_session_id_from_proto(request.into_inner().session_id)
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let removed = self.runtime.sessions.lock().await.remove(session_id.as_str());
        self.runtime.chromium_sessions.lock().await.remove(session_id.as_str());
        self.runtime.browser_session_health.lock().await.remove(session_id.as_str());
        self.runtime.download_sessions.lock().await.remove(session_id.as_str());
        if let Some(record) = removed.as_ref() {
            if record.persistence.enabled {
                persist_session_snapshot(self.runtime.as_ref(), record).await.map_err(|error| {
                    Status::internal(format!(
                        "failed to persist state while closing session: {error}"
                    ))
                })?;
            }
        }
        Ok(Response::new(browser_v1::CloseSessionResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            closed: removed.is_some(),
            reason: if removed.is_some() {
                "closed".to_owned()
            } else {
                "session_not_found".to_owned()
            },
        }))
    }

    async fn list_sessions(
        &self,
        request: Request<browser_v1::ListSessionsRequest>,
    ) -> Result<Response<browser_v1::ListSessionsResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = request_principal(request.metadata())?.to_owned();
        let payload = request.into_inner();
        let principal = normalize_optional_string(payload.principal.as_str());
        if principal.as_deref().is_some_and(|value| value != caller_principal) {
            return Err(Status::permission_denied("principal mismatch"));
        }
        let limit = if payload.limit == 0 {
            self.runtime.max_sessions
        } else {
            usize::try_from(payload.limit).unwrap_or(usize::MAX).min(self.runtime.max_sessions)
        };
        let sessions = self.runtime.sessions.lock().await;
        let mut records = sessions
            .iter()
            .filter(|(_, session)| session.principal == caller_principal)
            .map(|(session_id, session)| (session_id.clone(), session.clone()))
            .collect::<Vec<_>>();
        drop(sessions);
        records.sort_by(|left, right| {
            right
                .1
                .last_active
                .cmp(&left.1.last_active)
                .then_with(|| right.1.created_at.cmp(&left.1.created_at))
                .then_with(|| left.0.cmp(&right.0))
        });
        let truncated = records.len() > limit;
        let sessions = records
            .into_iter()
            .take(limit)
            .map(|(session_id, session)| session_summary_to_proto(session_id.as_str(), &session))
            .collect::<Vec<_>>();
        Ok(Response::new(browser_v1::ListSessionsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            sessions,
            truncated,
            error: String::new(),
        }))
    }

    async fn get_session(
        &self,
        request: Request<browser_v1::GetSessionRequest>,
    ) -> Result<Response<browser_v1::GetSessionResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = request_principal(request.metadata())?.to_owned();
        let session_id = parse_session_id_from_proto(request.into_inner().session_id)
            .map_err(Status::invalid_argument)?;
        let session = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::GetSessionResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    session: None,
                    error: "session_not_found".to_owned(),
                }));
            };
            if session.principal != caller_principal {
                return Err(Status::permission_denied("session access denied"));
            }
            session.last_active = Instant::now();
            session.clone()
        };

        Ok(Response::new(browser_v1::GetSessionResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            session: Some(session_detail_to_proto(session_id.as_str(), &session)),
            error: String::new(),
        }))
    }

    async fn inspect_session(
        &self,
        request: Request<browser_v1::InspectSessionRequest>,
    ) -> Result<Response<browser_v1::InspectSessionResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = request_principal(request.metadata())?.to_owned();
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let default_include = !payload.include_cookies
            && !payload.include_storage
            && !payload.include_action_log
            && !payload.include_network_log
            && !payload.include_page_snapshot
            && !payload.include_console_log
            && !payload.include_page_diagnostics;
        let include_cookies = payload.include_cookies || default_include;
        let include_storage = payload.include_storage || default_include;
        let include_action_log = payload.include_action_log || default_include;
        let include_network_log = payload.include_network_log || default_include;
        let include_page_snapshot = payload.include_page_snapshot || default_include;
        let include_console_log = payload.include_console_log || default_include;
        let include_page_diagnostics = payload.include_page_diagnostics || default_include;

        // Pull a fresh snapshot from the live Chromium tab first so the
        // cookie/storage/page reads below reflect current browser state.
        if (include_cookies
            || include_storage
            || include_page_snapshot
            || include_console_log
            || include_page_diagnostics)
            && self.runtime.engine_mode == BrowserEngineMode::Chromium
        {
            let active_tab_id = {
                let sessions = self.runtime.sessions.lock().await;
                let Some(session) = sessions.get(session_id.as_str()) else {
                    return Ok(Response::new(browser_v1::InspectSessionResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        session: None,
                        cookies: Vec::new(),
                        storage: Vec::new(),
                        action_log: Vec::new(),
                        network_log: Vec::new(),
                        dom_snapshot: String::new(),
                        visible_text: String::new(),
                        page_url: String::new(),
                        cookies_truncated: false,
                        storage_truncated: false,
                        action_log_truncated: false,
                        network_log_truncated: false,
                        dom_truncated: false,
                        visible_text_truncated: false,
                        console_log: Vec::new(),
                        console_log_truncated: false,
                        page_diagnostics: None,
                        session_health: None,
                        error: "session_not_found".to_owned(),
                    }));
                };
                if session.principal != caller_principal {
                    return Err(Status::permission_denied("session access denied"));
                }
                session.active_tab_id.clone()
            };
            let _ = chromium_refresh_tab_snapshot(
                self.runtime.as_ref(),
                session_id.as_str(),
                active_tab_id.as_str(),
            )
            .await;
        }

        let session = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::InspectSessionResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    session: None,
                    cookies: Vec::new(),
                    storage: Vec::new(),
                    action_log: Vec::new(),
                    network_log: Vec::new(),
                    dom_snapshot: String::new(),
                    visible_text: String::new(),
                    page_url: String::new(),
                    cookies_truncated: false,
                    storage_truncated: false,
                    action_log_truncated: false,
                    network_log_truncated: false,
                    dom_truncated: false,
                    visible_text_truncated: false,
                    console_log: Vec::new(),
                    console_log_truncated: false,
                    page_diagnostics: None,
                    session_health: None,
                    error: "session_not_found".to_owned(),
                }));
            };
            if session.principal != caller_principal {
                return Err(Status::permission_denied("session access denied"));
            }
            session.last_active = Instant::now();
            session.clone()
        };
        let Some(active_tab) = session.active_tab() else {
            return Ok(Response::new(browser_v1::InspectSessionResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                session: Some(session_detail_to_proto(session_id.as_str(), &session)),
                cookies: Vec::new(),
                storage: Vec::new(),
                action_log: Vec::new(),
                network_log: Vec::new(),
                dom_snapshot: String::new(),
                visible_text: String::new(),
                page_url: String::new(),
                cookies_truncated: false,
                storage_truncated: false,
                action_log_truncated: false,
                network_log_truncated: false,
                dom_truncated: false,
                visible_text_truncated: false,
                console_log: Vec::new(),
                console_log_truncated: false,
                page_diagnostics: None,
                session_health: Some(
                    browser_session_health_to_proto(self.runtime.as_ref(), session_id.as_str())
                        .await,
                ),
                error: "active_tab_not_found".to_owned(),
            }));
        };

        let cookie_payload_bytes = if payload.max_cookie_bytes == 0 {
            session.budget.max_response_bytes.min(DEFAULT_MAX_INSPECT_COOKIE_BYTES)
        } else {
            payload.max_cookie_bytes.min(session.budget.max_response_bytes)
        }
        .max(1) as usize;
        let storage_payload_bytes = if payload.max_storage_bytes == 0 {
            session.budget.max_response_bytes.min(DEFAULT_MAX_INSPECT_STORAGE_BYTES)
        } else {
            payload.max_storage_bytes.min(session.budget.max_response_bytes)
        }
        .max(1) as usize;
        let max_action_log_entries = if payload.max_action_log_entries == 0 {
            session.budget.max_action_log_entries
        } else {
            usize::try_from(payload.max_action_log_entries)
                .unwrap_or(usize::MAX)
                .min(session.budget.max_action_log_entries)
                .max(1)
        };
        let max_network_log_entries = if payload.max_network_log_entries == 0 {
            session.budget.max_network_log_entries
        } else {
            usize::try_from(payload.max_network_log_entries)
                .unwrap_or(usize::MAX)
                .min(session.budget.max_network_log_entries)
                .max(1)
        };
        let max_network_log_bytes = if payload.max_network_log_bytes == 0 {
            session.budget.max_network_log_bytes
        } else {
            payload.max_network_log_bytes.min(session.budget.max_network_log_bytes)
        }
        .max(1) as usize;
        let max_console_log_entries = if payload.max_console_log_entries == 0 {
            DEFAULT_MAX_CONSOLE_LOG_ENTRIES
        } else {
            usize::try_from(payload.max_console_log_entries)
                .unwrap_or(usize::MAX)
                .clamp(1, DEFAULT_MAX_CONSOLE_LOG_ENTRIES)
        };
        let max_console_log_bytes = if payload.max_console_log_bytes == 0 {
            DEFAULT_MAX_CONSOLE_LOG_BYTES
        } else {
            payload.max_console_log_bytes.min(DEFAULT_MAX_CONSOLE_LOG_BYTES)
        }
        .max(1) as usize;
        let max_dom_snapshot_bytes = if payload.max_dom_snapshot_bytes == 0 {
            session.budget.max_observe_snapshot_bytes
        } else {
            payload.max_dom_snapshot_bytes.min(session.budget.max_observe_snapshot_bytes)
        }
        .max(1) as usize;
        let max_visible_text_bytes = if payload.max_visible_text_bytes == 0 {
            session.budget.max_visible_text_bytes
        } else {
            payload.max_visible_text_bytes.min(session.budget.max_visible_text_bytes)
        }
        .max(1) as usize;

        let mut cookies =
            if include_cookies { cookie_jar_to_proto(&session.cookie_jar) } else { Vec::new() };
        let cookies_truncated = if include_cookies {
            truncate_cookie_payload(&mut cookies, cookie_payload_bytes)
        } else {
            false
        };

        let mut storage = if include_storage {
            storage_entries_to_proto(&session.storage_entries)
        } else {
            Vec::new()
        };
        let storage_truncated = if include_storage {
            truncate_storage_payload(&mut storage, storage_payload_bytes)
        } else {
            false
        };

        let (action_log, action_log_truncated) = if include_action_log {
            let start = session.action_log.len().saturating_sub(max_action_log_entries);
            (
                session
                    .action_log
                    .iter()
                    .skip(start)
                    .map(action_log_entry_to_proto)
                    .collect::<Vec<_>>(),
                start > 0,
            )
        } else {
            (Vec::new(), false)
        };

        let (network_log, network_log_truncated) = if include_network_log {
            let start = active_tab.network_log.len().saturating_sub(max_network_log_entries);
            let mut entries = active_tab
                .network_log
                .iter()
                .skip(start)
                .cloned()
                .map(|entry| network_log_entry_to_proto(entry, true))
                .collect::<Vec<_>>();
            let truncated =
                start > 0 || truncate_network_log_payload(&mut entries, max_network_log_bytes);
            (entries, truncated)
        } else {
            (Vec::new(), false)
        };
        let (console_log, console_log_truncated) = if include_console_log {
            let start = active_tab.console_log.len().saturating_sub(max_console_log_entries);
            let mut entries = active_tab
                .console_log
                .iter()
                .skip(start)
                .map(console_entry_to_proto)
                .collect::<Vec<_>>();
            let truncated =
                start > 0 || truncate_console_log_payload(&mut entries, max_console_log_bytes);
            (entries, truncated)
        } else {
            (Vec::new(), false)
        };

        let page_url =
            normalize_url_with_redaction(active_tab.last_url.as_deref().unwrap_or_default());
        let ((dom_snapshot, dom_truncated), (visible_text, visible_text_truncated)) =
            if include_page_snapshot && !active_tab.last_page_body.trim().is_empty() {
                (
                    build_dom_snapshot(active_tab.last_page_body.as_str(), max_dom_snapshot_bytes),
                    build_visible_text_snapshot(
                        active_tab.last_page_body.as_str(),
                        max_visible_text_bytes,
                    ),
                )
            } else {
                ((String::new(), false), (String::new(), false))
            };
        let page_diagnostics = if include_page_diagnostics {
            Some(page_diagnostics_to_proto(active_tab))
        } else {
            None
        };
        let session_health =
            browser_session_health_to_proto(self.runtime.as_ref(), session_id.as_str()).await;

        Ok(Response::new(browser_v1::InspectSessionResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            session: Some(session_detail_to_proto(session_id.as_str(), &session)),
            cookies,
            storage,
            action_log,
            network_log,
            dom_snapshot,
            visible_text,
            page_url,
            cookies_truncated,
            storage_truncated,
            action_log_truncated,
            network_log_truncated,
            dom_truncated,
            visible_text_truncated,
            console_log,
            console_log_truncated,
            page_diagnostics,
            session_health: Some(session_health),
            error: String::new(),
        }))
    }

    async fn list_profiles(
        &self,
        request: Request<browser_v1::ListProfilesRequest>,
    ) -> Result<Response<browser_v1::ListProfilesResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let authenticated_principal =
            authenticated_request_principal(request.metadata())?.map(str::to_owned);
        let payload = request.into_inner();
        let principal = normalize_profile_principal(payload.principal.as_str())
            .map_err(Status::invalid_argument)?;
        enforce_authenticated_body_principal(
            authenticated_principal.as_deref(),
            principal.as_str(),
        )?;
        let Some(store) = self.runtime.state_store.as_ref() else {
            return Err(Status::failed_precondition(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
            ));
        };
        let _guard = self.runtime.profile_registry_lock.lock().await;
        let mut registry = store.load_profile_registry().map_err(|error| {
            Status::internal(format!("failed to load browser profiles: {error}"))
        })?;
        let active_profile_id =
            registry.active_profile_by_principal.get(principal.as_str()).cloned();
        let mut profiles = registry
            .profiles
            .drain(..)
            .filter(|profile| profile.principal == principal)
            .collect::<Vec<_>>();
        profiles.sort_by_key(|profile| std::cmp::Reverse(profile.last_used_unix_ms));
        Ok(Response::new(browser_v1::ListProfilesResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profiles: profiles
                .iter()
                .map(|profile| {
                    profile_record_to_proto(
                        profile,
                        active_profile_id
                            .as_deref()
                            .map(|value| value == profile.profile_id.as_str())
                            .unwrap_or(false),
                    )
                })
                .collect(),
            active_profile_id: active_profile_id
                .map(|value| proto::palyra::common::v1::CanonicalId { ulid: value }),
        }))
    }

    async fn create_profile(
        &self,
        request: Request<browser_v1::CreateProfileRequest>,
    ) -> Result<Response<browser_v1::CreateProfileResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let authenticated_principal =
            authenticated_request_principal(request.metadata())?.map(str::to_owned);
        let payload = request.into_inner();
        let principal = normalize_profile_principal(payload.principal.as_str())
            .map_err(Status::invalid_argument)?;
        enforce_authenticated_body_principal(
            authenticated_principal.as_deref(),
            principal.as_str(),
        )?;
        let name =
            normalize_profile_name(payload.name.as_str()).map_err(Status::invalid_argument)?;
        let theme = normalize_profile_theme(payload.theme_color.as_str())
            .map_err(Status::invalid_argument)?;
        let Some(store) = self.runtime.state_store.as_ref() else {
            return Err(Status::failed_precondition(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
            ));
        };
        let _guard = self.runtime.profile_registry_lock.lock().await;
        let mut registry = store.load_profile_registry().map_err(|error| {
            Status::internal(format!("failed to load browser profiles: {error}"))
        })?;
        prune_profiles_for_principal(&mut registry, principal.as_str());
        let now = current_unix_ms();
        let mut profile = BrowserProfileRecord {
            profile_id: Ulid::generate().to_string(),
            principal: principal.clone(),
            name,
            theme_color: theme,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            last_used_unix_ms: now,
            persistence_enabled: payload.persistence_enabled && !payload.private_profile,
            private_profile: payload.private_profile,
            state_schema_version: PROFILE_RECORD_SCHEMA_VERSION,
            state_revision: 0,
            state_hash_sha256: None,
            record_hash_sha256: String::new(),
        };
        refresh_profile_record_hash(&mut profile);
        registry.profiles.push(profile.clone());
        registry
            .active_profile_by_principal
            .entry(principal.clone())
            .or_insert_with(|| profile.profile_id.clone());
        prune_profile_registry(&mut registry);
        store.save_profile_registry(&registry).map_err(|error| {
            Status::internal(format!("failed to save browser profiles: {error}"))
        })?;
        let active = registry
            .active_profile_by_principal
            .get(principal.as_str())
            .map(|value| value == &profile.profile_id)
            .unwrap_or(false);
        Ok(Response::new(browser_v1::CreateProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profile: Some(profile_record_to_proto(&profile, active)),
        }))
    }

    async fn rename_profile(
        &self,
        request: Request<browser_v1::RenameProfileRequest>,
    ) -> Result<Response<browser_v1::RenameProfileResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let authenticated_principal =
            authenticated_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let principal = normalize_profile_principal(payload.principal.as_str())
            .map_err(Status::invalid_argument)?;
        enforce_authenticated_body_principal(
            authenticated_principal.as_deref(),
            principal.as_str(),
        )?;
        let profile_id = parse_required_profile_id_from_proto(payload.profile_id.take())
            .map_err(Status::invalid_argument)?;
        let name =
            normalize_profile_name(payload.name.as_str()).map_err(Status::invalid_argument)?;
        let Some(store) = self.runtime.state_store.as_ref() else {
            return Err(Status::failed_precondition(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
            ));
        };
        let _guard = self.runtime.profile_registry_lock.lock().await;
        let mut registry = store.load_profile_registry().map_err(|error| {
            Status::internal(format!("failed to load browser profiles: {error}"))
        })?;
        let Some(profile) = registry
            .profiles
            .iter_mut()
            .find(|profile| profile.profile_id == profile_id && profile.principal == principal)
        else {
            return Err(Status::not_found("browser profile not found"));
        };
        profile.name = name;
        profile.updated_at_unix_ms = current_unix_ms();
        profile.last_used_unix_ms = profile.updated_at_unix_ms;
        refresh_profile_record_hash(profile);
        let active = registry
            .active_profile_by_principal
            .get(principal.as_str())
            .map(|value| value == &profile_id)
            .unwrap_or(false);
        let output = profile_record_to_proto(profile, active);
        store.save_profile_registry(&registry).map_err(|error| {
            Status::internal(format!("failed to save browser profiles: {error}"))
        })?;
        Ok(Response::new(browser_v1::RenameProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profile: Some(output),
        }))
    }

    async fn delete_profile(
        &self,
        request: Request<browser_v1::DeleteProfileRequest>,
    ) -> Result<Response<browser_v1::DeleteProfileResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let authenticated_principal =
            authenticated_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let principal = normalize_profile_principal(payload.principal.as_str())
            .map_err(Status::invalid_argument)?;
        enforce_authenticated_body_principal(
            authenticated_principal.as_deref(),
            principal.as_str(),
        )?;
        let profile_id = parse_required_profile_id_from_proto(payload.profile_id.take())
            .map_err(Status::invalid_argument)?;
        let Some(store) = self.runtime.state_store.as_ref() else {
            return Err(Status::failed_precondition(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
            ));
        };
        let _guard = self.runtime.profile_registry_lock.lock().await;
        let mut registry = store.load_profile_registry().map_err(|error| {
            Status::internal(format!("failed to load browser profiles: {error}"))
        })?;
        let before = registry.profiles.len();
        registry.profiles.retain(|profile| {
            !(profile.profile_id == profile_id && profile.principal == principal)
        });
        let deleted = registry.profiles.len() != before;
        if deleted {
            if registry
                .active_profile_by_principal
                .get(principal.as_str())
                .map(|value| value == &profile_id)
                .unwrap_or(false)
            {
                let replacement = registry
                    .profiles
                    .iter()
                    .filter(|profile| profile.principal == principal)
                    .max_by(|left, right| left.last_used_unix_ms.cmp(&right.last_used_unix_ms))
                    .map(|profile| profile.profile_id.clone());
                if let Some(value) = replacement {
                    registry.active_profile_by_principal.insert(principal.clone(), value);
                } else {
                    registry.active_profile_by_principal.remove(principal.as_str());
                }
            }
            prune_profile_registry(&mut registry);
            store.save_profile_registry(&registry).map_err(|error| {
                Status::internal(format!("failed to save browser profiles after delete: {error}"))
            })?;
            store.delete_snapshot(profile_id.as_str()).map_err(|error| {
                Status::internal(format!("failed to delete browser profile snapshot: {error}"))
            })?;
        }
        Ok(Response::new(browser_v1::DeleteProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            deleted,
            active_profile_id: registry
                .active_profile_by_principal
                .get(principal.as_str())
                .cloned()
                .map(|value| proto::palyra::common::v1::CanonicalId { ulid: value }),
        }))
    }

    async fn set_active_profile(
        &self,
        request: Request<browser_v1::SetActiveProfileRequest>,
    ) -> Result<Response<browser_v1::SetActiveProfileResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let authenticated_principal =
            authenticated_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let principal = normalize_profile_principal(payload.principal.as_str())
            .map_err(Status::invalid_argument)?;
        enforce_authenticated_body_principal(
            authenticated_principal.as_deref(),
            principal.as_str(),
        )?;
        let profile_id = parse_required_profile_id_from_proto(payload.profile_id.take())
            .map_err(Status::invalid_argument)?;
        let Some(store) = self.runtime.state_store.as_ref() else {
            return Err(Status::failed_precondition(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
            ));
        };
        let _guard = self.runtime.profile_registry_lock.lock().await;
        let mut registry = store.load_profile_registry().map_err(|error| {
            Status::internal(format!("failed to load browser profiles: {error}"))
        })?;
        let Some(profile) = registry
            .profiles
            .iter_mut()
            .find(|profile| profile.profile_id == profile_id && profile.principal == principal)
        else {
            return Err(Status::not_found("browser profile not found"));
        };
        profile.last_used_unix_ms = current_unix_ms();
        profile.updated_at_unix_ms = profile.last_used_unix_ms;
        refresh_profile_record_hash(profile);
        let output = profile_record_to_proto(profile, true);
        registry.active_profile_by_principal.insert(principal, profile_id);
        prune_profile_registry(&mut registry);
        store.save_profile_registry(&registry).map_err(|error| {
            Status::internal(format!("failed to save browser profiles: {error}"))
        })?;
        Ok(Response::new(browser_v1::SetActiveProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profile: Some(output),
        }))
    }

    async fn navigate(
        &self,
        request: Request<browser_v1::NavigateRequest>,
    ) -> Result<Response<browser_v1::NavigateResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let url = payload.url.trim().to_owned();
        if url.is_empty() {
            return Err(Status::invalid_argument("navigate requires non-empty url"));
        }
        let started_at_unix_ms = current_unix_ms();
        let (
            timeout_ms,
            max_response_bytes,
            allow_private_targets,
            cookie_header,
            allow_downloads,
            profile_id,
            private_profile,
        ) = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Err(Status::not_found("browser session not found"));
            };
            session.last_active = Instant::now();
            let timeout_ms =
                request_timeout_ms(payload.timeout_ms, session.budget.max_navigation_timeout_ms);
            let cookie_header = if self.runtime.engine_mode == BrowserEngineMode::Simulated {
                cookie_header_for_url(session, url.as_str())
            } else {
                None
            };
            (
                timeout_ms,
                session.budget.max_response_bytes,
                payload.allow_private_targets || session.allow_private_targets,
                cookie_header,
                session.allow_downloads,
                session.profile_id.clone(),
                session.private_profile,
            )
        };

        let mut outcome = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => {
                navigate_with_guards(
                    url.as_str(),
                    timeout_ms,
                    payload.allow_redirects,
                    if payload.max_redirects == 0 { 3 } else { payload.max_redirects },
                    allow_private_targets,
                    max_response_bytes,
                    cookie_header.as_deref(),
                )
                .await
            }
            BrowserEngineMode::Chromium => {
                navigate_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    ChromiumNavigateParams {
                        raw_url: url.clone(),
                        timeout_ms,
                        allow_redirects: payload.allow_redirects,
                        max_redirects: if payload.max_redirects == 0 {
                            3
                        } else {
                            payload.max_redirects
                        },
                        allow_private_targets,
                        max_response_bytes,
                    },
                )
                .await
            }
        };
        // Chromium reports navigations that turn into downloads as aborted
        // failures. Recover only the exact caller-requested URL so unrelated
        // response or page-side download buffers cannot supply the artifact.
        if !outcome.success
            && allow_downloads
            && navigation_error_may_be_download_abort(outcome.error.as_str())
        {
            let fallback_file_name = infer_download_file_name(url.as_str());
            match fetch_http_attachment_download_artifact(
                self.runtime.as_ref(),
                session_id.as_str(),
                if private_profile { None } else { profile_id.as_deref() },
                url.as_str(),
                fallback_file_name.as_str(),
                allow_private_targets,
                timeout_ms,
            )
            .await
            {
                Ok(Some(record)) => {
                    outcome.success = true;
                    outcome.final_url = record.source_url.clone();
                    outcome.status_code = 200;
                    outcome.title.clear();
                    outcome.page_body.clear();
                    outcome.body_bytes = record.size_bytes;
                    outcome.error.clear();
                }
                Ok(None) => {}
                Err(download_error) => {
                    outcome.error =
                        format!("{}; download capture failed: {download_error}", outcome.error);
                }
            }
        }
        let network_log_entries = std::mem::take(&mut outcome.network_log);
        let cookie_updates = std::mem::take(&mut outcome.cookie_updates);
        let mut session_for_persist = None;

        let mut sessions = self.runtime.sessions.lock().await;
        if let Some(session) = sessions.get_mut(session_id.as_str()) {
            let max_network_log_entries = session.budget.max_network_log_entries;
            let max_network_log_bytes = session.budget.max_network_log_bytes;
            if let Some(tab) = session.active_tab_mut() {
                if outcome.success {
                    tab.last_title = outcome.title.clone();
                    tab.last_url = Some(outcome.final_url.clone());
                    tab.last_page_body = outcome.page_body.clone();
                    tab.scroll_x = 0;
                    tab.scroll_y = 0;
                    tab.typed_inputs.clear();
                }
                append_network_log_entries(
                    tab,
                    network_log_entries.as_slice(),
                    max_network_log_entries,
                    max_network_log_bytes,
                );
            }
            apply_cookie_updates(session, cookie_updates.as_slice());
            session.last_active = Instant::now();
            if session.persistence.enabled {
                session_for_persist = Some(session.clone());
            }
        }
        drop(sessions);
        if let Some(record) = session_for_persist {
            persist_session_snapshot(self.runtime.as_ref(), &record).await.map_err(|error| {
                Status::internal(format!("failed to persist state after navigate: {error}"))
            })?;
        }
        let action_selector = normalize_url_with_redaction(url.as_str());
        let _ = finalize_session_action(
            self.runtime.as_ref(),
            session_id.as_str(),
            FinalizeActionRequest {
                action_name: "navigate",
                selector: action_selector.as_str(),
                success: outcome.success,
                outcome: navigate_action_outcome(&outcome),
                error: outcome.error.as_str(),
                started_at_unix_ms,
                attempts: 1,
                capture_failure_screenshot: false,
                max_failure_screenshot_bytes: 0,
            },
        )
        .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "navigate")
            .await
            .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::NavigateResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: outcome.success,
            final_url: outcome.final_url,
            status_code: u32::from(outcome.status_code),
            title: truncate_utf8_bytes(
                outcome.title.as_str(),
                self.runtime.default_budget.max_title_bytes as usize,
            ),
            body_bytes: outcome.body_bytes,
            latency_ms: outcome.latency_ms,
            error: outcome.error,
        }))
    }

    async fn click(
        &self,
        request: Request<browser_v1::ClickRequest>,
    ) -> Result<Response<browser_v1::ClickResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let selector = payload.selector.trim();
        if selector.is_empty() {
            return Err(Status::invalid_argument("click requires non-empty selector"));
        }

        let context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            true,
            ActionSnapshotRefresh::Full,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::ClickResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    error,
                    action_log: None,
                    failure_screenshot_bytes: Vec::new(),
                    failure_screenshot_mime_type: String::new(),
                    artifact: None,
                }));
            }
        };

        let timeout_ms =
            request_timeout_ms(payload.timeout_ms, context.budget.max_action_timeout_ms);
        let max_attempts = payload.max_retries.clamp(0, 16).saturating_add(1);
        let started_at_unix_ms = current_unix_ms();
        let (success, outcome, error, attempts) = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => {
                let started_at = Instant::now();
                let mut attempts = 0_u32;
                let mut success = false;
                let mut outcome = "selector_not_found".to_owned();
                let mut error = format!("selector '{selector}' was not found");
                loop {
                    attempts = attempts.saturating_add(1);
                    if let Some(tag) = find_matching_html_tag(selector, context.page_body.as_str())
                    {
                        if is_download_like_tag(tag.as_str()) && !context.allow_downloads {
                            outcome = "download_blocked".to_owned();
                            error =
                                "download-like click is blocked by session policy (allow_downloads=false)"
                                    .to_owned();
                            break;
                        }
                        success = true;
                        outcome = if is_download_like_tag(tag.as_str()) {
                            "download_allowed".to_owned()
                        } else {
                            "clicked".to_owned()
                        };
                        error.clear();
                        break;
                    }
                    if attempts >= max_attempts
                        || started_at.elapsed() >= Duration::from_millis(timeout_ms)
                    {
                        break;
                    }
                    let remaining_ms =
                        timeout_ms.saturating_sub(started_at.elapsed().as_millis() as u64);
                    let sleep_ms = DEFAULT_ACTION_RETRY_INTERVAL_MS.min(remaining_ms.max(1));
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
                (success, outcome, error, attempts)
            }
            BrowserEngineMode::Chromium => {
                let result = click_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    selector,
                    timeout_ms,
                    max_attempts,
                    context.allow_downloads,
                )
                .await;
                (result.success, result.outcome, result.error, result.attempts)
            }
        };
        let mut success = success;
        let mut outcome = outcome;
        let mut error = error;
        let mut artifact = None;
        // The click script rotates the page-side blob capture generation
        // immediately before activating the selected element, so this drain
        // cannot return entries produced by earlier or unrelated actions.
        if success
            && context.allow_downloads
            && matches!(self.runtime.engine_mode, BrowserEngineMode::Chromium)
        {
            match store_chromium_captured_downloads(
                self.runtime.as_ref(),
                session_id.as_str(),
                if context.private_profile { None } else { context.profile_id.as_deref() },
            )
            .await
            {
                Ok(Some(record)) => {
                    if record.quarantined {
                        outcome = "download_quarantined".to_owned();
                    } else {
                        outcome = "download_allowed".to_owned();
                    }
                    artifact = Some(download_artifact_to_proto(&record));
                }
                Ok(None) => {}
                Err(download_error) => {
                    success = false;
                    outcome = "download_failed".to_owned();
                    error = download_error;
                }
            }
        }
        if success && outcome == "download_allowed" && artifact.is_none() {
            match capture_download_artifact_for_click(
                self.runtime.as_ref(),
                session_id.as_str(),
                selector,
                &context,
                timeout_ms,
            )
            .await
            {
                Ok(record) => {
                    if record.quarantined {
                        outcome = "download_quarantined".to_owned();
                    }
                    artifact = Some(download_artifact_to_proto(&record));
                }
                Err(download_error) => {
                    success = false;
                    outcome = "download_failed".to_owned();
                    error = download_error;
                }
            }
        }

        let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
            finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: "click",
                    selector,
                    success,
                    outcome: outcome.as_str(),
                    error: error.as_str(),
                    started_at_unix_ms,
                    attempts,
                    capture_failure_screenshot: payload.capture_failure_screenshot,
                    max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                },
            )
            .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "click")
            .await
            .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::ClickResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            error,
            action_log,
            failure_screenshot_bytes,
            failure_screenshot_mime_type,
            artifact,
        }))
    }

    async fn r#type(
        &self,
        request: Request<browser_v1::TypeRequest>,
    ) -> Result<Response<browser_v1::TypeResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let selector = payload.selector.trim();
        if selector.is_empty() {
            return Err(Status::invalid_argument("type requires non-empty selector"));
        }

        let context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            true,
            ActionSnapshotRefresh::Full,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::TypeResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    typed_bytes: 0,
                    error,
                    action_log: None,
                    failure_screenshot_bytes: Vec::new(),
                    failure_screenshot_mime_type: String::new(),
                }));
            }
        };

        let text = payload.text;
        if (text.len() as u64) > context.budget.max_type_input_bytes {
            let error = format!(
                "type input exceeds max_type_input_bytes ({} > {})",
                text.len(),
                context.budget.max_type_input_bytes
            );
            let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
                finalize_session_action(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    FinalizeActionRequest {
                        action_name: "type",
                        selector,
                        success: false,
                        outcome: "input_too_large",
                        error: error.as_str(),
                        started_at_unix_ms: current_unix_ms(),
                        attempts: 1,
                        capture_failure_screenshot: payload.capture_failure_screenshot,
                        max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                    },
                )
                .await;
            return Ok(Response::new(browser_v1::TypeResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                typed_bytes: 0,
                error,
                action_log,
                failure_screenshot_bytes,
                failure_screenshot_mime_type,
            }));
        }

        let timeout_ms =
            request_timeout_ms(payload.timeout_ms, context.budget.max_action_timeout_ms);
        let started_at_unix_ms = current_unix_ms();
        let (success, outcome, error, attempts) = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => {
                let started_at = Instant::now();
                let mut attempts = 0_u32;
                let mut success = false;
                let mut outcome = "selector_not_found".to_owned();
                let mut error = format!("selector '{selector}' was not found");
                loop {
                    attempts = attempts.saturating_add(1);
                    if let Some(tag) = find_matching_html_tag(selector, context.page_body.as_str())
                    {
                        if !is_typable_tag(tag.as_str()) {
                            outcome = "selector_not_typable".to_owned();
                            error = format!(
                                "selector '{selector}' does not target an input-like element"
                            );
                            break;
                        }
                        success = true;
                        outcome = "typed".to_owned();
                        error.clear();
                        break;
                    }
                    if started_at.elapsed() >= Duration::from_millis(timeout_ms) {
                        break;
                    }
                    let remaining_ms =
                        timeout_ms.saturating_sub(started_at.elapsed().as_millis() as u64);
                    let sleep_ms = DEFAULT_ACTION_RETRY_INTERVAL_MS.min(remaining_ms.max(1));
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
                (success, outcome, error, attempts)
            }
            BrowserEngineMode::Chromium => {
                let result = type_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    selector,
                    text.as_str(),
                    payload.clear_existing,
                    timeout_ms,
                )
                .await;
                (result.success, result.outcome, result.error, result.attempts)
            }
        };

        if success {
            let mut sessions = self.runtime.sessions.lock().await;
            if let Some(session) = sessions.get_mut(session_id.as_str()) {
                if let Some(tab) = session.active_tab_mut() {
                    let field = tab.typed_inputs.entry(selector.to_owned()).or_default();
                    if payload.clear_existing {
                        *field = text.clone();
                    } else {
                        field.push_str(text.as_str());
                    }
                }
            }
        }

        let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
            finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: "type",
                    selector,
                    success,
                    outcome: outcome.as_str(),
                    error: error.as_str(),
                    started_at_unix_ms,
                    attempts,
                    capture_failure_screenshot: payload.capture_failure_screenshot,
                    max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                },
            )
            .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "type")
            .await
            .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::TypeResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            typed_bytes: if success { text.len() as u64 } else { 0 },
            error,
            action_log,
            failure_screenshot_bytes,
            failure_screenshot_mime_type,
        }))
    }

    async fn set_file_input(
        &self,
        request: Request<browser_v1::SetFileInputRequest>,
    ) -> Result<Response<browser_v1::SetFileInputResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let selector = payload.selector.trim();
        if selector.is_empty() {
            return Err(Status::invalid_argument("set_file_input requires non-empty selector"));
        }
        let file_name = sanitize_download_file_name(payload.file_name.as_str());
        if file_name.is_empty() {
            return Err(Status::invalid_argument("set_file_input requires non-empty file_name"));
        }

        let context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            true,
            ActionSnapshotRefresh::Full,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::SetFileInputResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    error,
                    action_log: None,
                    failure_screenshot_bytes: Vec::new(),
                    failure_screenshot_mime_type: String::new(),
                    uploaded_file_name: String::new(),
                    uploaded_file_bytes: 0,
                }));
            }
        };

        if (payload.file_bytes.len() as u64) > UPLOAD_MAX_FILE_BYTES {
            let error = format!(
                "upload file exceeds max_file_bytes ({} > {})",
                payload.file_bytes.len(),
                UPLOAD_MAX_FILE_BYTES
            );
            let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
                finalize_session_action(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    FinalizeActionRequest {
                        action_name: "set_file_input",
                        selector,
                        success: false,
                        outcome: "input_too_large",
                        error: error.as_str(),
                        started_at_unix_ms: current_unix_ms(),
                        attempts: 1,
                        capture_failure_screenshot: payload.capture_failure_screenshot,
                        max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                    },
                )
                .await;
            return Ok(Response::new(browser_v1::SetFileInputResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                error,
                action_log,
                failure_screenshot_bytes,
                failure_screenshot_mime_type,
                uploaded_file_name: file_name,
                uploaded_file_bytes: 0,
            }));
        }

        let timeout_ms =
            request_timeout_ms(payload.timeout_ms, context.budget.max_action_timeout_ms);
        let started_at_unix_ms = current_unix_ms();
        let (success, outcome, error, attempts) = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => {
                let started_at = Instant::now();
                let mut attempts = 0_u32;
                let mut success = false;
                let mut outcome = "selector_not_found".to_owned();
                let mut error = format!("selector '{selector}' was not found");
                loop {
                    attempts = attempts.saturating_add(1);
                    if let Some(tag) = find_matching_html_tag(selector, context.page_body.as_str())
                    {
                        if !is_file_input_tag(tag.as_str()) {
                            outcome = "selector_not_file_input".to_owned();
                            error = format!(
                                "selector '{selector}' does not target an input[type=file] element"
                            );
                            break;
                        }
                        success = true;
                        outcome = "file_input_set".to_owned();
                        error.clear();
                        break;
                    }
                    if started_at.elapsed() >= Duration::from_millis(timeout_ms) {
                        break;
                    }
                    let remaining_ms =
                        timeout_ms.saturating_sub(started_at.elapsed().as_millis() as u64);
                    let sleep_ms = DEFAULT_ACTION_RETRY_INTERVAL_MS.min(remaining_ms.max(1));
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
                (success, outcome, error, attempts)
            }
            BrowserEngineMode::Chromium => {
                let result = set_file_input_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    selector,
                    file_name.as_str(),
                    payload.file_bytes.as_slice(),
                    timeout_ms,
                )
                .await;
                (result.success, result.outcome, result.error, result.attempts)
            }
        };

        let uploaded_file_bytes = if success { payload.file_bytes.len() as u64 } else { 0 };
        let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
            finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: "set_file_input",
                    selector,
                    success,
                    outcome: outcome.as_str(),
                    error: error.as_str(),
                    started_at_unix_ms,
                    attempts,
                    capture_failure_screenshot: payload.capture_failure_screenshot,
                    max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                },
            )
            .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(
            self.runtime.as_ref(),
            session_for_persist,
            "set_file_input",
        )
        .await
        .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::SetFileInputResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            error,
            action_log,
            failure_screenshot_bytes,
            failure_screenshot_mime_type,
            uploaded_file_name: file_name,
            uploaded_file_bytes,
        }))
    }

    async fn press(
        &self,
        request: Request<browser_v1::PressRequest>,
    ) -> Result<Response<browser_v1::PressResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let key = normalize_press_key_input(payload.key.as_str());
        if key.is_empty() {
            return Err(Status::invalid_argument("press requires non-empty key"));
        }

        let context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            true,
            ActionSnapshotRefresh::Full,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::PressResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    key,
                    error,
                    action_log: None,
                    failure_screenshot_bytes: Vec::new(),
                    failure_screenshot_mime_type: String::new(),
                }));
            }
        };

        let timeout_ms =
            request_timeout_ms(payload.timeout_ms, context.budget.max_action_timeout_ms);
        let started_at_unix_ms = current_unix_ms();
        let (success, outcome, error, attempts) = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => (true, "pressed".to_owned(), String::new(), 1),
            BrowserEngineMode::Chromium => {
                let result = press_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    key.as_str(),
                    timeout_ms,
                )
                .await;
                (result.success, result.outcome, result.error, result.attempts)
            }
        };

        let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
            finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: "press",
                    selector: "",
                    success,
                    outcome: outcome.as_str(),
                    error: error.as_str(),
                    started_at_unix_ms,
                    attempts,
                    capture_failure_screenshot: payload.capture_failure_screenshot,
                    max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                },
            )
            .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "press")
            .await
            .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::PressResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            key,
            error,
            action_log,
            failure_screenshot_bytes,
            failure_screenshot_mime_type,
        }))
    }

    async fn select(
        &self,
        request: Request<browser_v1::SelectRequest>,
    ) -> Result<Response<browser_v1::SelectResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let selector = payload.selector.trim().to_owned();
        let value = payload.value.trim().to_owned();
        if selector.is_empty() {
            return Err(Status::invalid_argument("select requires non-empty selector"));
        }
        if value.is_empty() {
            return Err(Status::invalid_argument("select requires non-empty value"));
        }

        let context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            true,
            ActionSnapshotRefresh::Full,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::SelectResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    selected_value: String::new(),
                    error,
                    action_log: None,
                    failure_screenshot_bytes: Vec::new(),
                    failure_screenshot_mime_type: String::new(),
                }));
            }
        };

        let timeout_ms =
            request_timeout_ms(payload.timeout_ms, context.budget.max_action_timeout_ms);
        let started_at_unix_ms = current_unix_ms();
        let (success, outcome, error, attempts) = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => {
                let tag = find_matching_html_tag(selector.as_str(), context.page_body.as_str());
                match tag {
                    None => (
                        false,
                        "selector_not_found".to_owned(),
                        format!("selector '{selector}' was not found"),
                        1,
                    ),
                    Some(tag) if !tag.to_ascii_lowercase().starts_with("<select") => (
                        false,
                        "selector_not_select".to_owned(),
                        format!("selector '{selector}' does not target a <select> element"),
                        1,
                    ),
                    Some(tag) if tag.to_ascii_lowercase().contains("disabled") => (
                        false,
                        "selector_disabled".to_owned(),
                        format!("selector '{selector}' is disabled"),
                        1,
                    ),
                    Some(_)
                        if !context.page_body.contains(format!("value=\"{value}\"").as_str()) =>
                    {
                        (
                            false,
                            "value_not_found".to_owned(),
                            format!("value '{value}' was not found for selector '{selector}'"),
                            1,
                        )
                    }
                    Some(_) => (true, "selected".to_owned(), String::new(), 1),
                }
            }
            BrowserEngineMode::Chromium => {
                let result = select_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    selector.as_str(),
                    value.as_str(),
                    timeout_ms,
                )
                .await;
                (result.success, result.outcome, result.error, result.attempts)
            }
        };

        if success {
            let mut sessions = self.runtime.sessions.lock().await;
            if let Some(session) = sessions.get_mut(session_id.as_str()) {
                if let Some(tab) = session.active_tab_mut() {
                    tab.typed_inputs.insert(selector.clone(), value.clone());
                }
            }
        }

        let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
            finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: "select",
                    selector: selector.as_str(),
                    success,
                    outcome: outcome.as_str(),
                    error: error.as_str(),
                    started_at_unix_ms,
                    attempts,
                    capture_failure_screenshot: payload.capture_failure_screenshot,
                    max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                },
            )
            .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "select")
            .await
            .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::SelectResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            selected_value: if success { value } else { String::new() },
            error,
            action_log,
            failure_screenshot_bytes,
            failure_screenshot_mime_type,
        }))
    }

    async fn set_viewport(
        &self,
        request: Request<browser_v1::SetViewportRequest>,
    ) -> Result<Response<browser_v1::SetViewportResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        if !(MIN_VIEWPORT_WIDTH..=MAX_VIEWPORT_WIDTH).contains(&payload.width) {
            return Err(Status::invalid_argument(format!(
                "viewport width must be between {MIN_VIEWPORT_WIDTH} and {MAX_VIEWPORT_WIDTH}"
            )));
        }
        if !(MIN_VIEWPORT_HEIGHT..=MAX_VIEWPORT_HEIGHT).contains(&payload.height) {
            return Err(Status::invalid_argument(format!(
                "viewport height must be between {MIN_VIEWPORT_HEIGHT} and {MAX_VIEWPORT_HEIGHT}"
            )));
        }
        let device_scale_factor = if payload.device_scale_factor == 0.0 {
            DEFAULT_DEVICE_SCALE_FACTOR
        } else if payload.device_scale_factor.is_finite()
            && payload.device_scale_factor > 0.0
            && payload.device_scale_factor <= MAX_DEVICE_SCALE_FACTOR
        {
            payload.device_scale_factor
        } else {
            return Err(Status::invalid_argument(format!(
                "device_scale_factor must be greater than 0 and at most {MAX_DEVICE_SCALE_FACTOR}"
            )));
        };
        let css_pixels = viewport_css_pixels(payload.width, payload.height);
        if css_pixels > MAX_VIEWPORT_CSS_PIXELS {
            return Err(Status::invalid_argument(format!(
                "viewport area must be at most {MAX_VIEWPORT_CSS_PIXELS} CSS pixels"
            )));
        }
        let effective_pixels = viewport_effective_pixels(css_pixels, device_scale_factor);
        if effective_pixels > MAX_VIEWPORT_EFFECTIVE_PIXELS {
            return Err(Status::invalid_argument(format!(
                "effective viewport area must be at most {:.0} device pixels after device_scale_factor",
                MAX_VIEWPORT_EFFECTIVE_PIXELS
            )));
        }

        let context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            true,
            ActionSnapshotRefresh::Full,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::SetViewportResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    width: 0,
                    height: 0,
                    device_scale_factor: 0.0,
                    mobile: payload.mobile,
                    error,
                    action_log: None,
                }));
            }
        };

        let _timeout_ms =
            request_timeout_ms(payload.timeout_ms, context.budget.max_action_timeout_ms);
        let started_at_unix_ms = current_unix_ms();
        let (success, width, height, device_scale_factor, mobile, metric_mismatch, error) =
            match self.runtime.engine_mode {
                BrowserEngineMode::Simulated => (
                    true,
                    payload.width,
                    payload.height,
                    device_scale_factor,
                    payload.mobile,
                    false,
                    String::new(),
                ),
                BrowserEngineMode::Chromium => {
                    let result = set_viewport_with_chromium(
                        self.runtime.as_ref(),
                        session_id.as_str(),
                        payload.width,
                        payload.height,
                        device_scale_factor,
                        payload.mobile,
                    )
                    .await;
                    (
                        result.success,
                        result.width,
                        result.height,
                        result.device_scale_factor,
                        result.mobile,
                        result.metric_mismatch,
                        result.error,
                    )
                }
            };
        let outcome = if success && metric_mismatch {
            "viewport_set_metric_mismatch"
        } else if success {
            "viewport_set"
        } else {
            "viewport_failed"
        };

        let (action_log, _, _) = finalize_session_action(
            self.runtime.as_ref(),
            session_id.as_str(),
            FinalizeActionRequest {
                action_name: "viewport",
                selector: "",
                success,
                outcome,
                error: error.as_str(),
                started_at_unix_ms,
                attempts: 1,
                capture_failure_screenshot: false,
                max_failure_screenshot_bytes: 0,
            },
        )
        .await;

        Ok(Response::new(browser_v1::SetViewportResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            width,
            height,
            device_scale_factor,
            mobile,
            error,
            action_log,
        }))
    }

    async fn highlight(
        &self,
        request: Request<browser_v1::HighlightRequest>,
    ) -> Result<Response<browser_v1::HighlightResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let selector = payload.selector.trim().to_owned();
        if selector.is_empty() {
            return Err(Status::invalid_argument("highlight requires non-empty selector"));
        }

        let context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            true,
            ActionSnapshotRefresh::Full,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::HighlightResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    selector,
                    error,
                    action_log: None,
                    failure_screenshot_bytes: Vec::new(),
                    failure_screenshot_mime_type: String::new(),
                }));
            }
        };
        let timeout_ms =
            request_timeout_ms(payload.timeout_ms, context.budget.max_action_timeout_ms);
        let duration_ms = payload.duration_ms.max(500);
        let started_at_unix_ms = current_unix_ms();
        let (success, outcome, error, attempts) = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => {
                if find_matching_html_tag(selector.as_str(), context.page_body.as_str()).is_some() {
                    (true, "highlighted".to_owned(), String::new(), 1)
                } else {
                    (
                        false,
                        "selector_not_found".to_owned(),
                        format!("selector '{selector}' was not found"),
                        1,
                    )
                }
            }
            BrowserEngineMode::Chromium => {
                let result = highlight_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    selector.as_str(),
                    timeout_ms,
                    duration_ms,
                )
                .await;
                (result.success, result.outcome, result.error, result.attempts)
            }
        };

        let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
            finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: "highlight",
                    selector: selector.as_str(),
                    success,
                    outcome: outcome.as_str(),
                    error: error.as_str(),
                    started_at_unix_ms,
                    attempts,
                    capture_failure_screenshot: payload.capture_failure_screenshot,
                    max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                },
            )
            .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "highlight")
            .await
            .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::HighlightResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            selector,
            error,
            action_log,
            failure_screenshot_bytes,
            failure_screenshot_mime_type,
        }))
    }

    async fn scroll(
        &self,
        request: Request<browser_v1::ScrollRequest>,
    ) -> Result<Response<browser_v1::ScrollResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;

        let _context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            false,
            ActionSnapshotRefresh::Full,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::ScrollResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    scroll_x: 0,
                    scroll_y: 0,
                    error,
                    action_log: None,
                    failure_screenshot_bytes: Vec::new(),
                    failure_screenshot_mime_type: String::new(),
                }));
            }
        };

        let (success, scroll_x, scroll_y, error) = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => {
                let mut scroll_x = 0_i64;
                let mut scroll_y = 0_i64;
                {
                    let mut sessions = self.runtime.sessions.lock().await;
                    if let Some(session) = sessions.get_mut(session_id.as_str()) {
                        if let Some(tab) = session.active_tab_mut() {
                            tab.scroll_x = tab.scroll_x.saturating_add(payload.delta_x);
                            tab.scroll_y = tab.scroll_y.saturating_add(payload.delta_y);
                            scroll_x = tab.scroll_x;
                            scroll_y = tab.scroll_y;
                        }
                    }
                }
                (true, scroll_x, scroll_y, String::new())
            }
            BrowserEngineMode::Chromium => {
                let result = scroll_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    payload.delta_x,
                    payload.delta_y,
                )
                .await;
                (result.success, result.scroll_x, result.scroll_y, result.error)
            }
        };

        let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
            finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: "scroll",
                    selector: "",
                    success,
                    outcome: if success { "scrolled" } else { "scroll_failed" },
                    error: error.as_str(),
                    started_at_unix_ms: current_unix_ms(),
                    attempts: 1,
                    capture_failure_screenshot: payload.capture_failure_screenshot,
                    max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                },
            )
            .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "scroll")
            .await
            .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::ScrollResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            scroll_x,
            scroll_y,
            error,
            action_log,
            failure_screenshot_bytes,
            failure_screenshot_mime_type,
        }))
    }

    async fn wait_for(
        &self,
        request: Request<browser_v1::WaitForRequest>,
    ) -> Result<Response<browser_v1::WaitForResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let selector = payload.selector.trim().to_owned();
        let text = payload.text;
        if selector.is_empty() && text.trim().is_empty() {
            return Err(Status::invalid_argument(
                "wait_for requires non-empty selector or non-empty text",
            ));
        }
        let context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            true,
            ActionSnapshotRefresh::Full,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::WaitForResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    waited_ms: 0,
                    error,
                    action_log: None,
                    failure_screenshot_bytes: Vec::new(),
                    failure_screenshot_mime_type: String::new(),
                    matched_selector: String::new(),
                    matched_text: String::new(),
                }));
            }
        };

        let timeout_ms =
            request_timeout_ms(payload.timeout_ms, context.budget.max_action_timeout_ms);
        let poll_interval_ms = payload.poll_interval_ms.clamp(25, 1_000);
        let started_at_unix_ms = current_unix_ms();
        let selector_required = !selector.is_empty();
        let text_required = !text.trim().is_empty();
        let (success, matched_selector, matched_text, attempts, waited_ms, error) = match self
            .runtime
            .engine_mode
        {
            BrowserEngineMode::Simulated => {
                let started = Instant::now();
                let mut attempts = 0_u32;
                let mut matched_selector = String::new();
                let mut matched_text = String::new();
                let mut success = false;
                loop {
                    attempts = attempts.saturating_add(1);
                    let selector_hit = selector_required
                        && find_matching_html_tag(selector.as_str(), context.page_body.as_str())
                            .is_some();
                    let text_hit = text_required && context.page_body.contains(text.as_str());
                    if (!selector_required || selector_hit) && (!text_required || text_hit) {
                        if selector_hit {
                            matched_selector = selector.clone();
                        }
                        if text_hit {
                            matched_text = text.clone();
                        }
                        success = true;
                        break;
                    }
                    if started.elapsed() >= Duration::from_millis(timeout_ms) {
                        break;
                    }
                    let remaining_ms =
                        timeout_ms.saturating_sub(started.elapsed().as_millis() as u64);
                    let sleep_ms = poll_interval_ms.min(remaining_ms.max(1));
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
                let waited_ms = started.elapsed().as_millis() as u64;
                let error = if success {
                    String::new()
                } else {
                    "wait_for condition was not satisfied before timeout".to_owned()
                };
                (success, matched_selector, matched_text, attempts, waited_ms, error)
            }
            BrowserEngineMode::Chromium => {
                let result = wait_for_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    selector.as_str(),
                    text.as_str(),
                    timeout_ms,
                    poll_interval_ms,
                )
                .await;
                (
                    result.success,
                    result.matched_selector,
                    result.matched_text,
                    result.attempts,
                    result.waited_ms,
                    result.error,
                )
            }
        };

        let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
            finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: "wait_for",
                    selector: selector.as_str(),
                    success,
                    outcome: if success { "condition_matched" } else { "condition_timeout" },
                    error: error.as_str(),
                    started_at_unix_ms,
                    attempts,
                    capture_failure_screenshot: payload.capture_failure_screenshot,
                    max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                },
            )
            .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "wait_for")
            .await
            .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::WaitForResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            waited_ms,
            error,
            action_log,
            failure_screenshot_bytes,
            failure_screenshot_mime_type,
            matched_selector,
            matched_text,
        }))
    }

    async fn get_title(
        &self,
        request: Request<browser_v1::GetTitleRequest>,
    ) -> Result<Response<browser_v1::GetTitleResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let max_title_bytes = usize::try_from(payload.max_title_bytes)
            .ok()
            .filter(|value| *value > 0)
            .unwrap_or(self.runtime.default_budget.max_title_bytes as usize);
        let active_tab_id = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::GetTitleResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    title: String::new(),
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            let Some(tab) = session.active_tab() else {
                return Ok(Response::new(browser_v1::GetTitleResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    title: String::new(),
                    error: "active_tab_not_found".to_owned(),
                }));
            };
            tab.tab_id.clone()
        };
        if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            if let Ok(title) = chromium_get_title(
                self.runtime.as_ref(),
                session_id.as_str(),
                active_tab_id.as_str(),
            )
            .await
            {
                let mut sessions = self.runtime.sessions.lock().await;
                if let Some(session) = sessions.get_mut(session_id.as_str()) {
                    if let Some(tab) = session.tabs.get_mut(active_tab_id.as_str()) {
                        tab.last_title = title;
                    }
                }
            }
        }
        let title = {
            let sessions = self.runtime.sessions.lock().await;
            sessions
                .get(session_id.as_str())
                .and_then(|session| session.tabs.get(active_tab_id.as_str()))
                .map(|tab| tab.last_title.clone())
                .unwrap_or_default()
        };
        Ok(Response::new(browser_v1::GetTitleResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            title: truncate_utf8_bytes(title.as_str(), max_title_bytes),
            error: String::new(),
        }))
    }

    async fn handle_dialog(
        &self,
        request: Request<browser_v1::HandleDialogRequest>,
    ) -> Result<Response<browser_v1::HandleDialogResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let action = match browser_v1::BrowserDialogAction::try_from(payload.action)
            .unwrap_or(browser_v1::BrowserDialogAction::Unspecified)
        {
            browser_v1::BrowserDialogAction::Inspect => BrowserDialogAction::Inspect,
            browser_v1::BrowserDialogAction::Accept => BrowserDialogAction::Accept,
            browser_v1::BrowserDialogAction::Dismiss => BrowserDialogAction::Dismiss,
            browser_v1::BrowserDialogAction::Respond => BrowserDialogAction::Respond,
            browser_v1::BrowserDialogAction::Unspecified => {
                return Err(Status::invalid_argument("dialog action must be specified"));
            }
        };
        if payload.prompt_text.len() > MAX_BROWSER_DIALOG_PROMPT_BYTES {
            return Err(Status::invalid_argument(format!(
                "dialog prompt response exceeds {MAX_BROWSER_DIALOG_PROMPT_BYTES} bytes"
            )));
        }
        {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::HandleDialogResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    present: false,
                    event: None,
                    mutated_page: false,
                    timed_out: false,
                    backend_support: self.runtime.engine_mode == BrowserEngineMode::Chromium,
                    error_code: "session_not_found".to_owned(),
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
        }
        if self.runtime.engine_mode != BrowserEngineMode::Chromium {
            return Ok(Response::new(browser_v1::HandleDialogResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                present: false,
                event: None,
                mutated_page: false,
                timed_out: false,
                backend_support: false,
                error_code: "dialog_backend_unavailable".to_owned(),
                error: "native dialogs require the Chromium browser engine".to_owned(),
            }));
        }
        if action.mutates_page() {
            if let Err(error) = consume_action_budget_and_snapshot(
                self.runtime.as_ref(),
                session_id.as_str(),
                false,
                ActionSnapshotRefresh::UrlOnly,
            )
            .await
            {
                return Ok(Response::new(browser_v1::HandleDialogResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    present: false,
                    event: None,
                    mutated_page: false,
                    timed_out: false,
                    backend_support: true,
                    error_code: "dialog_action_blocked".to_owned(),
                    error,
                }));
            }
        }
        let prompt_text = (action == BrowserDialogAction::Respond).then_some(payload.prompt_text);
        let started_at_unix_ms = current_unix_ms();
        let outcome_result = chromium_handle_dialog(
            self.runtime.as_ref(),
            session_id.as_str(),
            action,
            payload.expected_generation,
            prompt_text,
        )
        .await;
        if action.mutates_page() {
            let (success, outcome, error) = match &outcome_result {
                Ok(outcome) => (
                    outcome.success,
                    if outcome.error_code.is_empty() {
                        if outcome.success {
                            "dialog_handled"
                        } else {
                            "dialog_failed"
                        }
                    } else {
                        outcome.error_code.as_str()
                    },
                    outcome.error.as_str(),
                ),
                Err(error) => (false, "dialog_backend_failed", error.as_str()),
            };
            let _ = finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: action.action_log_name(),
                    selector: "",
                    success,
                    outcome,
                    error,
                    started_at_unix_ms,
                    attempts: 1,
                    capture_failure_screenshot: false,
                    max_failure_screenshot_bytes: 0,
                },
            )
            .await;
            let session_for_persist = {
                let sessions = self.runtime.sessions.lock().await;
                sessions
                    .get(session_id.as_str())
                    .filter(|session| session.persistence.enabled)
                    .cloned()
            };
            persist_session_after_mutation(
                self.runtime.as_ref(),
                session_for_persist,
                action.action_log_name(),
            )
            .await
            .map_err(map_persist_error_to_status)?;
        }
        let outcome = outcome_result.map_err(Status::failed_precondition)?;
        Ok(Response::new(browser_v1::HandleDialogResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: outcome.success,
            present: outcome.present,
            event: outcome.event.map(browser_dialog_event_to_proto),
            mutated_page: outcome.mutated_page,
            timed_out: outcome.timed_out,
            backend_support: true,
            error_code: outcome.error_code,
            error: outcome.error,
        }))
    }

    async fn screenshot(
        &self,
        request: Request<browser_v1::ScreenshotRequest>,
    ) -> Result<Response<browser_v1::ScreenshotResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        if !payload.format.trim().is_empty() && !payload.format.trim().eq_ignore_ascii_case("png") {
            return Err(Status::invalid_argument("screenshot format must be empty or 'png'"));
        }
        let max_bytes = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::ScreenshotResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    image_bytes: Vec::new(),
                    mime_type: "image/png".to_owned(),
                    error: "session_not_found".to_owned(),
                    layout_metrics: None,
                }));
            };
            session.last_active = Instant::now();
            payload.max_bytes.max(1).min(session.budget.max_screenshot_bytes)
        };
        let layout_metrics = if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            chromium_layout_metrics(self.runtime.as_ref(), session_id.as_str())
                .await
                .ok()
                .map(browser_layout_metrics_to_proto)
        } else {
            None
        };
        let image_bytes = if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            match chromium_screenshot(self.runtime.as_ref(), session_id.as_str()).await {
                Ok(value) => value,
                Err(error) => {
                    return Ok(Response::new(browser_v1::ScreenshotResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        image_bytes: Vec::new(),
                        mime_type: "image/png".to_owned(),
                        error,
                        layout_metrics,
                    }));
                }
            }
        } else {
            ONE_BY_ONE_PNG.to_vec()
        };
        if (image_bytes.len() as u64) > max_bytes {
            return Ok(Response::new(browser_v1::ScreenshotResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                image_bytes: Vec::new(),
                mime_type: "image/png".to_owned(),
                error: format!(
                    "screenshot output exceeds max_bytes ({} > {max_bytes}); reduce viewport size or device_scale_factor, or increase the browser service max_screenshot_bytes session budget",
                    image_bytes.len()
                ),
                layout_metrics,
            }));
        }
        Ok(Response::new(browser_v1::ScreenshotResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            image_bytes,
            mime_type: "image/png".to_owned(),
            error: String::new(),
            layout_metrics,
        }))
    }

    async fn observe(
        &self,
        request: Request<browser_v1::ObserveRequest>,
    ) -> Result<Response<browser_v1::ObserveResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let inclusions = resolve_observe_inclusions(
            payload.include_dom_snapshot,
            payload.include_accessibility_tree,
            payload.include_visible_text,
        );
        let include_dom_snapshot = inclusions.include_dom_snapshot;
        let include_accessibility_tree = inclusions.include_accessibility_tree;
        let include_visible_text = inclusions.include_visible_text;
        let capture_selectors =
            normalize_observe_capture_selectors(payload.capture_selectors.as_slice());
        let computed_style_properties = normalize_observe_computed_style_properties(
            payload.computed_style_properties.as_slice(),
        );

        let (
            active_tab_id,
            max_dom_snapshot_bytes,
            max_accessibility_tree_bytes,
            max_visible_text_bytes,
            max_capture_text_bytes,
        ) = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::ObserveResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    dom_snapshot: String::new(),
                    accessibility_tree: String::new(),
                    visible_text: String::new(),
                    dom_truncated: false,
                    accessibility_tree_truncated: false,
                    visible_text_truncated: false,
                    page_url: String::new(),
                    error: "session_not_found".to_owned(),
                    element_captures: Vec::new(),
                }));
            };
            session.last_active = Instant::now();
            let Some(tab) = session.active_tab() else {
                return Ok(Response::new(browser_v1::ObserveResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    dom_snapshot: String::new(),
                    accessibility_tree: String::new(),
                    visible_text: String::new(),
                    dom_truncated: false,
                    accessibility_tree_truncated: false,
                    visible_text_truncated: false,
                    page_url: String::new(),
                    error: "active_tab_not_found".to_owned(),
                    element_captures: Vec::new(),
                }));
            };
            (
                tab.tab_id.clone(),
                observe_byte_limit(
                    payload.max_dom_snapshot_bytes,
                    session.budget.max_observe_snapshot_bytes,
                ),
                observe_byte_limit(
                    payload.max_accessibility_tree_bytes,
                    session.budget.max_observe_snapshot_bytes,
                ),
                observe_byte_limit(
                    payload.max_visible_text_bytes,
                    session.budget.max_visible_text_bytes,
                ),
                observe_capture_text_limit(
                    payload.max_capture_text_bytes,
                    session.budget.max_visible_text_bytes,
                ),
            )
        };

        if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            match chromium_observe_snapshot(
                self.runtime.as_ref(),
                session_id.as_str(),
                active_tab_id.as_str(),
            )
            .await
            {
                Ok(snapshot) => {
                    let mut sessions = self.runtime.sessions.lock().await;
                    if let Some(session) = sessions.get_mut(session_id.as_str()) {
                        if let Some(tab) = session.tabs.get_mut(active_tab_id.as_str()) {
                            tab.last_page_body = snapshot.page_body;
                            tab.last_observe_state_summary = snapshot.observe_state_summary;
                            tab.last_title = snapshot.title;
                            tab.last_url = Some(snapshot.page_url);
                        }
                    }
                }
                Err(error) => {
                    return Ok(Response::new(browser_v1::ObserveResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        dom_snapshot: String::new(),
                        accessibility_tree: String::new(),
                        visible_text: String::new(),
                        dom_truncated: false,
                        accessibility_tree_truncated: false,
                        visible_text_truncated: false,
                        page_url: String::new(),
                        error: format!("failed to observe live Chromium tab: {error}"),
                        element_captures: Vec::new(),
                    }));
                }
            }
        }

        let (page_body, page_url) = {
            let sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::ObserveResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    dom_snapshot: String::new(),
                    accessibility_tree: String::new(),
                    visible_text: String::new(),
                    dom_truncated: false,
                    accessibility_tree_truncated: false,
                    visible_text_truncated: false,
                    page_url: String::new(),
                    error: "session_not_found".to_owned(),
                    element_captures: Vec::new(),
                }));
            };
            let Some(tab) = session.tabs.get(active_tab_id.as_str()) else {
                return Ok(Response::new(browser_v1::ObserveResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    dom_snapshot: String::new(),
                    accessibility_tree: String::new(),
                    visible_text: String::new(),
                    dom_truncated: false,
                    accessibility_tree_truncated: false,
                    visible_text_truncated: false,
                    page_url: String::new(),
                    error: "active_tab_not_found".to_owned(),
                    element_captures: Vec::new(),
                }));
            };
            (tab.last_page_body.clone(), tab.last_url.clone().unwrap_or_default())
        };
        if page_body.trim().is_empty() {
            return Ok(Response::new(browser_v1::ObserveResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                dom_snapshot: String::new(),
                accessibility_tree: String::new(),
                visible_text: String::new(),
                dom_truncated: false,
                accessibility_tree_truncated: false,
                visible_text_truncated: false,
                page_url: String::new(),
                error: "navigate must succeed before observe".to_owned(),
                element_captures: Vec::new(),
            }));
        }

        let element_captures = if capture_selectors.is_empty() {
            Vec::new()
        } else if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            match chromium_capture_element_captures(
                self.runtime.as_ref(),
                session_id.as_str(),
                active_tab_id.as_str(),
                capture_selectors.as_slice(),
                computed_style_properties.as_slice(),
                max_capture_text_bytes,
            )
            .await
            {
                Ok(captures) => captures,
                Err(error) => capture_selectors
                    .iter()
                    .map(|selector| observe_element_capture_error(selector, error.as_str()))
                    .collect(),
            }
        } else {
            capture_selectors
                .iter()
                .map(|selector| {
                    observe_element_capture_error(
                        selector,
                        "element_capture_requires_chromium_engine",
                    )
                })
                .collect()
        };

        let (dom_snapshot, dom_truncated) = if include_dom_snapshot {
            build_dom_snapshot(page_body.as_str(), max_dom_snapshot_bytes)
        } else {
            (String::new(), false)
        };
        let (accessibility_tree, accessibility_tree_truncated) = if include_accessibility_tree {
            build_accessibility_tree_snapshot(page_body.as_str(), max_accessibility_tree_bytes)
        } else {
            (String::new(), false)
        };
        let (visible_text, visible_text_truncated) = if include_visible_text {
            build_visible_text_snapshot(page_body.as_str(), max_visible_text_bytes)
        } else {
            (String::new(), false)
        };

        Ok(Response::new(browser_v1::ObserveResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            dom_snapshot,
            accessibility_tree,
            visible_text,
            dom_truncated,
            accessibility_tree_truncated,
            visible_text_truncated,
            page_url: normalize_url_with_redaction(page_url.as_str()),
            error: String::new(),
            element_captures,
        }))
    }

    async fn network_log(
        &self,
        request: Request<browser_v1::NetworkLogRequest>,
    ) -> Result<Response<browser_v1::NetworkLogResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = request_principal(request.metadata())?.to_owned();
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            let active_tab_id = {
                let sessions = self.runtime.sessions.lock().await;
                let Some(session) = sessions.get(session_id.as_str()) else {
                    return Ok(Response::new(browser_v1::NetworkLogResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        entries: Vec::new(),
                        truncated: false,
                        error: "session_not_found".to_owned(),
                    }));
                };
                if session.principal != caller_principal {
                    return Err(Status::permission_denied("session access denied"));
                }
                session.active_tab_id.clone()
            };
            let _ = chromium_refresh_tab_snapshot(
                self.runtime.as_ref(),
                session_id.as_str(),
                active_tab_id.as_str(),
            )
            .await;
        }
        let mut sessions = self.runtime.sessions.lock().await;
        let Some(session) = sessions.get_mut(session_id.as_str()) else {
            return Ok(Response::new(browser_v1::NetworkLogResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                entries: Vec::new(),
                truncated: false,
                error: "session_not_found".to_owned(),
            }));
        };
        if session.principal != caller_principal {
            return Err(Status::permission_denied("session access denied"));
        }
        session.last_active = Instant::now();
        let Some(tab) = session.active_tab() else {
            return Ok(Response::new(browser_v1::NetworkLogResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                entries: Vec::new(),
                truncated: false,
                error: "active_tab_not_found".to_owned(),
            }));
        };
        let limit = if payload.limit == 0 {
            session.budget.max_network_log_entries
        } else {
            usize::try_from(payload.limit).unwrap_or(usize::MAX)
        }
        .min(session.budget.max_network_log_entries)
        .max(1);
        let max_payload_bytes = if payload.max_payload_bytes == 0 {
            session.budget.max_network_log_bytes
        } else {
            payload.max_payload_bytes.min(session.budget.max_network_log_bytes)
        } as usize;

        let start = tab.network_log.len().saturating_sub(limit);
        let mut truncated = start > 0;
        let mut entries = tab
            .network_log
            .iter()
            .skip(start)
            .cloned()
            .map(|entry| network_log_entry_to_proto(entry, payload.include_headers))
            .collect::<Vec<_>>();
        truncated = truncate_network_log_payload(&mut entries, max_payload_bytes) || truncated;

        Ok(Response::new(browser_v1::NetworkLogResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            entries,
            truncated,
            error: String::new(),
        }))
    }

    async fn console_log(
        &self,
        request: Request<browser_v1::ConsoleLogRequest>,
    ) -> Result<Response<browser_v1::ConsoleLogResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = request_principal(request.metadata())?.to_owned();
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;

        if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            let active_tab_id = {
                let sessions = self.runtime.sessions.lock().await;
                let Some(session) = sessions.get(session_id.as_str()) else {
                    return Ok(Response::new(browser_v1::ConsoleLogResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        entries: Vec::new(),
                        truncated: false,
                        page_diagnostics: None,
                        error: "session_not_found".to_owned(),
                    }));
                };
                if session.principal != caller_principal {
                    return Err(Status::permission_denied("session access denied"));
                }
                session.active_tab_id.clone()
            };
            let _ = chromium_refresh_tab_snapshot(
                self.runtime.as_ref(),
                session_id.as_str(),
                active_tab_id.as_str(),
            )
            .await;
        }

        let mut sessions = self.runtime.sessions.lock().await;
        let Some(session) = sessions.get_mut(session_id.as_str()) else {
            return Ok(Response::new(browser_v1::ConsoleLogResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                entries: Vec::new(),
                truncated: false,
                page_diagnostics: None,
                error: "session_not_found".to_owned(),
            }));
        };
        if session.principal != caller_principal {
            return Err(Status::permission_denied("session access denied"));
        }
        session.last_active = Instant::now();
        let Some(active_tab) = session.active_tab() else {
            return Ok(Response::new(browser_v1::ConsoleLogResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                entries: Vec::new(),
                truncated: false,
                page_diagnostics: None,
                error: "active_tab_not_found".to_owned(),
            }));
        };
        let minimum_severity =
            BrowserDiagnosticSeverityInternal::from_proto(payload.minimum_severity);
        let limit = if payload.limit == 0 {
            DEFAULT_MAX_CONSOLE_LOG_ENTRIES
        } else {
            usize::try_from(payload.limit)
                .unwrap_or(usize::MAX)
                .clamp(1, DEFAULT_MAX_CONSOLE_LOG_ENTRIES)
        };
        let max_payload_bytes = if payload.max_payload_bytes == 0 {
            DEFAULT_MAX_CONSOLE_LOG_BYTES
        } else {
            payload.max_payload_bytes.min(DEFAULT_MAX_CONSOLE_LOG_BYTES)
        }
        .max(1) as usize;
        let filtered = active_tab
            .console_log
            .iter()
            .filter(|entry| entry.severity >= minimum_severity)
            .cloned()
            .collect::<Vec<_>>();
        let start = filtered.len().saturating_sub(limit);
        let mut entries = filtered
            .into_iter()
            .skip(start)
            .map(|entry| console_entry_to_proto(&entry))
            .collect::<Vec<_>>();
        let truncated = start > 0 || truncate_console_log_payload(&mut entries, max_payload_bytes);

        Ok(Response::new(browser_v1::ConsoleLogResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            entries,
            truncated,
            page_diagnostics: if payload.include_page_diagnostics {
                Some(page_diagnostics_to_proto(active_tab))
            } else {
                None
            },
            error: String::new(),
        }))
    }

    async fn export_pdf(
        &self,
        request: Request<browser_v1::ExportPdfRequest>,
    ) -> Result<Response<browser_v1::ExportPdfResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = request_principal(request.metadata())?.to_owned();
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let (budget_max_bytes, profile_id, private_profile, current_url) = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::ExportPdfResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    pdf_bytes: Vec::new(),
                    mime_type: "application/pdf".to_owned(),
                    size_bytes: 0,
                    sha256: String::new(),
                    artifact: None,
                    error: "session_not_found".to_owned(),
                }));
            };
            if session.principal != caller_principal {
                return Err(Status::permission_denied("session access denied"));
            }
            session.last_active = Instant::now();
            (
                session.budget.max_response_bytes,
                session.profile_id.clone(),
                session.private_profile,
                session.active_tab().and_then(|tab| tab.last_url.clone()),
            )
        };
        let max_bytes = if payload.max_bytes == 0 {
            budget_max_bytes
        } else {
            payload.max_bytes.min(budget_max_bytes)
        };
        let pdf_bytes = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => MINIMAL_SIMULATED_PDF.to_vec(),
            BrowserEngineMode::Chromium => {
                export_pdf_with_chromium(self.runtime.as_ref(), session_id.as_str())
                    .await
                    .map_err(Status::internal)?
            }
        };
        if (pdf_bytes.len() as u64) > max_bytes {
            return Ok(Response::new(browser_v1::ExportPdfResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                pdf_bytes: Vec::new(),
                mime_type: "application/pdf".to_owned(),
                size_bytes: pdf_bytes.len() as u64,
                sha256: String::new(),
                artifact: None,
                error: format!("pdf output exceeds max_bytes ({} > {max_bytes})", pdf_bytes.len()),
            }));
        }

        let source_url =
            current_url.unwrap_or_else(|| format!("browser://session/{}/export.pdf", session_id));
        // session_id is a validated 26-char ASCII ULID, so this prefix slice
        // cannot panic.
        let artifact = store_generated_artifact(
            self.runtime.as_ref(),
            session_id.as_str(),
            if private_profile { None } else { profile_id.as_deref() },
            source_url.as_str(),
            format!("browser-session-{}.pdf", &session_id[..12]).as_str(),
            "application/pdf",
            pdf_bytes.as_slice(),
        )
        .await
        .map_err(Status::internal)?;

        let size_bytes = pdf_bytes.len() as u64;
        let sha256 = sha256_hex(pdf_bytes.as_slice());
        Ok(Response::new(browser_v1::ExportPdfResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            pdf_bytes,
            mime_type: "application/pdf".to_owned(),
            size_bytes,
            sha256,
            artifact: Some(download_artifact_to_proto(&artifact)),
            error: String::new(),
        }))
    }

    async fn reset_state(
        &self,
        request: Request<browser_v1::ResetStateRequest>,
    ) -> Result<Response<browser_v1::ResetStateResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = request_principal(request.metadata())?.to_owned();
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let default_reset = !payload.clear_cookies
            && !payload.clear_storage
            && !payload.reset_tabs
            && !payload.reset_permissions;
        let clear_cookies = payload.clear_cookies || default_reset;
        let clear_storage = payload.clear_storage || default_reset;
        let mut session_for_persist = None;

        let mut response = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::ResetStateResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    cookies_cleared: 0,
                    storage_entries_cleared: 0,
                    tabs_closed: 0,
                    permissions: Some(SessionPermissionsInternal::default().to_proto()),
                    error: "session_not_found".to_owned(),
                }));
            };
            if session.principal != caller_principal {
                return Err(Status::permission_denied("session access denied"));
            }
            session.last_active = Instant::now();
            let mut cookies_cleared = 0_u32;
            let mut storage_entries_cleared = 0_u32;
            let mut tabs_closed = 0_u32;
            if clear_cookies {
                cookies_cleared =
                    session.cookie_jar.values().map(|cookies| cookies.len() as u32).sum::<u32>();
                session.cookie_store.clear();
                session.cookie_jar.clear();
            }
            if clear_storage {
                storage_entries_cleared = session
                    .storage_entries
                    .values()
                    .map(|entries| entries.len() as u32)
                    .sum::<u32>();
                session.storage_entries.clear();
                if let Some(tab) = session.active_tab_mut() {
                    tab.typed_inputs.clear();
                }
            }
            if payload.reset_tabs && !session.tab_order.is_empty() {
                tabs_closed = session.tab_order.len().saturating_sub(1) as u32;
                let active_tab_id = session.active_tab_id.clone();
                session.tabs.clear();
                session
                    .tabs
                    .insert(active_tab_id.clone(), BrowserTabRecord::new(active_tab_id.clone()));
                session.tab_order = vec![active_tab_id];
            }
            session.clear_network_logs();
            if session.persistence.enabled {
                session_for_persist = Some(session.clone());
            }
            browser_v1::ResetStateResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: true,
                cookies_cleared,
                storage_entries_cleared,
                tabs_closed,
                permissions: Some(session.permissions.to_proto()),
                error: String::new(),
            }
        };
        if clear_storage && matches!(self.runtime.engine_mode, BrowserEngineMode::Chromium) {
            match chromium_clear_active_origin_storage(self.runtime.as_ref(), session_id.as_str())
                .await
            {
                Ok(entries_cleared) => {
                    response.storage_entries_cleared =
                        response.storage_entries_cleared.max(entries_cleared);
                }
                Err(error) => {
                    response.success = false;
                    append_reset_state_error(
                        &mut response.error,
                        format!("failed to clear active Chromium origin storage: {error}"),
                    );
                }
            }
        }
        if clear_cookies && matches!(self.runtime.engine_mode, BrowserEngineMode::Chromium) {
            match chromium_clear_active_tab_cookies(self.runtime.as_ref(), session_id.as_str())
                .await
            {
                Ok(cookies_cleared) => {
                    response.cookies_cleared = response.cookies_cleared.max(cookies_cleared);
                }
                Err(error) => {
                    response.success = false;
                    append_reset_state_error(
                        &mut response.error,
                        format!("failed to clear active Chromium cookies: {error}"),
                    );
                }
            }
        }
        if payload.reset_permissions {
            let permissions = SessionPermissionsInternal::default();
            let apply_result = if matches!(self.runtime.engine_mode, BrowserEngineMode::Chromium) {
                chromium_apply_session_permissions(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    permissions.clone(),
                )
                .await
            } else {
                Ok(())
            };
            match apply_result {
                Ok(()) => {
                    let mut sessions = self.runtime.sessions.lock().await;
                    if let Some(session) = sessions.get_mut(session_id.as_str()) {
                        session.last_active = Instant::now();
                        session.permissions = permissions;
                        response.permissions = Some(session.permissions.to_proto());
                        if session.persistence.enabled {
                            session_for_persist = Some(session.clone());
                        }
                    } else {
                        response.success = false;
                        append_reset_state_error(
                            &mut response.error,
                            "session_not_found while resetting permissions",
                        );
                    }
                }
                Err(error) => {
                    response.success = false;
                    append_reset_state_error(
                        &mut response.error,
                        format!("failed to reset Chromium page permissions: {error}"),
                    );
                }
            }
        }
        if matches!(self.runtime.engine_mode, BrowserEngineMode::Chromium) {
            if let Err(error) =
                chromium_clear_network_diagnostics(self.runtime.as_ref(), session_id.as_str()).await
            {
                warn!(
                    session_id = session_id.as_str(),
                    error = error.as_str(),
                    "failed to clear Chromium network diagnostics during reset_state"
                );
            }
        }
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "reset_state")
            .await
            .map_err(map_persist_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn list_tabs(
        &self,
        request: Request<browser_v1::ListTabsRequest>,
    ) -> Result<Response<browser_v1::ListTabsResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let mut sessions = self.runtime.sessions.lock().await;
        let Some(session) = sessions.get_mut(session_id.as_str()) else {
            return Ok(Response::new(browser_v1::ListTabsResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                tabs: Vec::new(),
                active_tab_id: None,
                error: "session_not_found".to_owned(),
            }));
        };
        session.last_active = Instant::now();
        Ok(Response::new(browser_v1::ListTabsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            tabs: session.list_tabs(),
            active_tab_id: Some(proto::palyra::common::v1::CanonicalId {
                ulid: session.active_tab_id.clone(),
            }),
            error: String::new(),
        }))
    }

    async fn open_tab(
        &self,
        request: Request<browser_v1::OpenTabRequest>,
    ) -> Result<Response<browser_v1::OpenTabResponse>, Status> {
        let relay_private_target_block =
            request.extensions().get::<RelayPrivateTargetBlock>().is_some();
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let url = payload.url.trim().to_owned();
        let (created_tab_id, timeout_ms, max_response_bytes, allow_private_targets, cookie_header) = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::OpenTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    tab: None,
                    navigated: false,
                    status_code: 0,
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            if !session.can_create_tab() {
                return Ok(Response::new(browser_v1::OpenTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    tab: None,
                    navigated: false,
                    status_code: 0,
                    error: "tab_limit_reached".to_owned(),
                }));
            }
            let created_tab_id = session.create_tab();
            if payload.activate {
                session.active_tab_id = created_tab_id.clone();
            }
            let timeout_ms =
                request_timeout_ms(payload.timeout_ms, session.budget.max_navigation_timeout_ms);
            let max_response_bytes = session.budget.max_response_bytes;
            let allow_private_targets = if relay_private_target_block {
                false
            } else {
                payload.allow_private_targets || session.allow_private_targets
            };
            let cookie_header = if self.runtime.engine_mode == BrowserEngineMode::Simulated {
                cookie_header_for_url(session, url.as_str())
            } else {
                None
            };
            (created_tab_id, timeout_ms, max_response_bytes, allow_private_targets, cookie_header)
        };
        let mut session_for_persist = None;
        if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            if let Err(error) = chromium_open_tab_runtime(
                self.runtime.as_ref(),
                session_id.as_str(),
                created_tab_id.as_str(),
            )
            .await
            {
                // Roll back the tab registered above so a failed engine init
                // does not leak a zombie tab; keep a usable active tab.
                let mut sessions = self.runtime.sessions.lock().await;
                if let Some(session) = sessions.get_mut(session_id.as_str()) {
                    if session.tabs.remove(created_tab_id.as_str()).is_some() {
                        session.tab_order.retain(|value| value != created_tab_id.as_str());
                        if session.tab_order.is_empty() {
                            let fallback_id = session.create_tab();
                            session.active_tab_id = fallback_id;
                        } else if session.active_tab_id == created_tab_id {
                            if let Some(first) = session.tab_order.first() {
                                session.active_tab_id = first.clone();
                            }
                        }
                    }
                }
                return Ok(Response::new(browser_v1::OpenTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    tab: None,
                    navigated: false,
                    status_code: 0,
                    error: format!("failed to create chromium tab runtime: {error}"),
                }));
            }
        }

        let mut navigated = false;
        let mut status_code = 0_u32;
        let mut success = true;
        let mut error = String::new();
        if !url.is_empty() {
            navigated = true;
            let mut outcome = match self.runtime.engine_mode {
                BrowserEngineMode::Simulated => {
                    navigate_with_guards(
                        url.as_str(),
                        timeout_ms,
                        payload.allow_redirects,
                        if payload.max_redirects == 0 { 3 } else { payload.max_redirects },
                        allow_private_targets,
                        max_response_bytes,
                        cookie_header.as_deref(),
                    )
                    .await
                }
                BrowserEngineMode::Chromium => {
                    navigate_tab_with_chromium(
                        self.runtime.as_ref(),
                        session_id.as_str(),
                        created_tab_id.as_str(),
                        &ChromiumNavigateParams {
                            raw_url: url.clone(),
                            timeout_ms,
                            allow_redirects: payload.allow_redirects,
                            max_redirects: if payload.max_redirects == 0 {
                                3
                            } else {
                                payload.max_redirects
                            },
                            allow_private_targets,
                            max_response_bytes,
                        },
                    )
                    .await
                }
            };
            status_code = u32::from(outcome.status_code);
            success = outcome.success;
            if !success {
                error = outcome.error.clone();
            }
            let network_log_entries = std::mem::take(&mut outcome.network_log);
            let cookie_updates = std::mem::take(&mut outcome.cookie_updates);
            let mut sessions = self.runtime.sessions.lock().await;
            if let Some(session) = sessions.get_mut(session_id.as_str()) {
                let max_network_log_entries = session.budget.max_network_log_entries;
                let max_network_log_bytes = session.budget.max_network_log_bytes;
                if let Some(tab) = session.tabs.get_mut(created_tab_id.as_str()) {
                    if outcome.success {
                        tab.last_title = outcome.title;
                        tab.last_url = Some(outcome.final_url);
                        tab.last_page_body = outcome.page_body;
                        tab.scroll_x = 0;
                        tab.scroll_y = 0;
                        tab.typed_inputs.clear();
                    }
                    append_network_log_entries(
                        tab,
                        network_log_entries.as_slice(),
                        max_network_log_entries,
                        max_network_log_bytes,
                    );
                }
                apply_cookie_updates(session, cookie_updates.as_slice());
                if session.persistence.enabled {
                    session_for_persist = Some(session.clone());
                }
            }
        } else {
            let mut sessions = self.runtime.sessions.lock().await;
            if let Some(session) = sessions.get_mut(session_id.as_str()) {
                if session.persistence.enabled {
                    session_for_persist = Some(session.clone());
                }
            }
        }
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "open_tab")
            .await
            .map_err(map_persist_error_to_status)?;

        let mut sessions = self.runtime.sessions.lock().await;
        let Some(session) = sessions.get_mut(session_id.as_str()) else {
            return Ok(Response::new(browser_v1::OpenTabResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                tab: None,
                navigated,
                status_code,
                error: "session_not_found".to_owned(),
            }));
        };
        let tab = session.tab_to_proto(created_tab_id.as_str());
        Ok(Response::new(browser_v1::OpenTabResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            tab,
            navigated,
            status_code,
            error,
        }))
    }

    async fn switch_tab(
        &self,
        request: Request<browser_v1::SwitchTabRequest>,
    ) -> Result<Response<browser_v1::SwitchTabResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let tab_id =
            parse_tab_id_from_proto(payload.tab_id.take()).map_err(Status::invalid_argument)?;
        let mut session_for_persist = None;
        let response = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::SwitchTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    active_tab: None,
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            if !session.tabs.contains_key(tab_id.as_str()) {
                browser_v1::SwitchTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    active_tab: None,
                    error: "tab_not_found".to_owned(),
                }
            } else {
                session.active_tab_id = tab_id;
                if session.persistence.enabled {
                    session_for_persist = Some(session.clone());
                }
                browser_v1::SwitchTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: true,
                    active_tab: session.tab_to_proto(session.active_tab_id.as_str()),
                    error: String::new(),
                }
            }
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "switch_tab")
            .await
            .map_err(map_persist_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn close_tab(
        &self,
        request: Request<browser_v1::CloseTabRequest>,
    ) -> Result<Response<browser_v1::CloseTabResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let requested_tab_id = match payload.tab_id.take() {
            Some(value) if !value.ulid.trim().is_empty() => {
                parse_tab_id(Some(value.ulid.trim())).map_err(Status::invalid_argument)?
            }
            _ => String::new(),
        };
        let mut session_for_persist = None;
        let response = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::CloseTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    closed_tab_id: None,
                    active_tab: None,
                    tabs_remaining: 0,
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            let tab_id_to_close = if requested_tab_id.is_empty() {
                session.active_tab_id.clone()
            } else {
                requested_tab_id.clone()
            };
            match session.close_tab(tab_id_to_close.as_str()) {
                Ok((closed_tab_id, _)) => {
                    if session.persistence.enabled {
                        session_for_persist = Some(session.clone());
                    }
                    browser_v1::CloseTabResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: true,
                        closed_tab_id: Some(proto::palyra::common::v1::CanonicalId {
                            ulid: closed_tab_id,
                        }),
                        active_tab: session.tab_to_proto(session.active_tab_id.as_str()),
                        tabs_remaining: session.tabs.len() as u32,
                        error: String::new(),
                    }
                }
                Err(error) => browser_v1::CloseTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    closed_tab_id: None,
                    active_tab: session.tab_to_proto(session.active_tab_id.as_str()),
                    tabs_remaining: session.tabs.len() as u32,
                    error,
                },
            }
        };
        if self.runtime.engine_mode == BrowserEngineMode::Chromium && response.success {
            if let Some(closed_tab_id) = response.closed_tab_id.as_ref() {
                let _ = chromium_close_tab_runtime(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    closed_tab_id.ulid.as_str(),
                )
                .await;
            }
        }
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "close_tab")
            .await
            .map_err(map_persist_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn get_permissions(
        &self,
        request: Request<browser_v1::GetPermissionsRequest>,
    ) -> Result<Response<browser_v1::GetPermissionsResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let mut sessions = self.runtime.sessions.lock().await;
        let Some(session) = sessions.get_mut(session_id.as_str()) else {
            return Ok(Response::new(browser_v1::GetPermissionsResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                permissions: Some(SessionPermissionsInternal::default().to_proto()),
                error: "session_not_found".to_owned(),
            }));
        };
        session.last_active = Instant::now();
        Ok(Response::new(browser_v1::GetPermissionsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            permissions: Some(session.permissions.to_proto()),
            error: String::new(),
        }))
    }

    async fn set_permissions(
        &self,
        request: Request<browser_v1::SetPermissionsRequest>,
    ) -> Result<Response<browser_v1::SetPermissionsResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let mut session_for_persist = None;
        let updated_permissions = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::SetPermissionsResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    permissions: Some(SessionPermissionsInternal::default().to_proto()),
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            let mut updated_permissions = session.permissions.clone();
            updated_permissions.apply_update(
                payload.camera,
                payload.microphone,
                payload.location,
                payload.reset_to_default,
            );
            updated_permissions
        };
        let mut response = browser_v1::SetPermissionsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            permissions: Some(updated_permissions.to_proto()),
            error: String::new(),
        };
        if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            if let Err(error) = chromium_apply_session_permissions(
                self.runtime.as_ref(),
                session_id.as_str(),
                updated_permissions.clone(),
            )
            .await
            {
                response.success = false;
                response.error = format!("failed to apply Chromium page permissions: {error}");
            }
        }
        if response.success {
            let mut sessions = self.runtime.sessions.lock().await;
            if let Some(session) = sessions.get_mut(session_id.as_str()) {
                session.last_active = Instant::now();
                session.permissions = updated_permissions;
                if session.persistence.enabled {
                    session_for_persist = Some(session.clone());
                }
                response.permissions = Some(session.permissions.to_proto());
            } else {
                response.success = false;
                response.error = "session_not_found".to_owned();
            }
        }
        persist_session_after_mutation(
            self.runtime.as_ref(),
            session_for_persist,
            "set_permissions",
        )
        .await
        .map_err(map_persist_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn relay_action(
        &self,
        request: Request<browser_v1::RelayActionRequest>,
    ) -> Result<Response<browser_v1::RelayActionResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let auth_header = request.metadata().get(AUTHORIZATION_HEADER).cloned();
        let caller_principal = optional_request_principal(request.metadata())?.map(str::to_owned);
        let principal_header = request.metadata().get(PRINCIPAL_HEADER).cloned();
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        enforce_session_owner_if_present(
            self.runtime.as_ref(),
            session_id.as_str(),
            caller_principal.as_deref(),
        )
        .await?;
        let extension_id = payload.extension_id.trim();
        if extension_id.is_empty() {
            return Err(Status::invalid_argument("extension_id is required"));
        }
        if extension_id.len() > MAX_RELAY_EXTENSION_ID_BYTES {
            return Err(Status::invalid_argument(format!(
                "extension_id exceeds {MAX_RELAY_EXTENSION_ID_BYTES} bytes"
            )));
        }
        if !extension_id
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'.' | b'-' | b'_'))
        {
            return Err(Status::invalid_argument("extension_id contains unsupported characters"));
        }
        if payload.max_payload_bytes > MAX_RELAY_PAYLOAD_BYTES {
            return Err(Status::invalid_argument(format!(
                "relay max_payload_bytes exceeds {MAX_RELAY_PAYLOAD_BYTES} bytes"
            )));
        }

        let action = browser_v1::RelayActionKind::try_from(payload.action)
            .unwrap_or(browser_v1::RelayActionKind::Unspecified);
        match action {
            browser_v1::RelayActionKind::OpenTab => {
                let Some(browser_v1::relay_action_request::Payload::OpenTab(open_tab)) =
                    payload.payload.take()
                else {
                    return Ok(Response::new(browser_v1::RelayActionResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        action: browser_v1::RelayActionKind::OpenTab as i32,
                        error: "relay open_tab payload is required".to_owned(),
                        result: None,
                    }));
                };
                let mut open_request = Request::new(browser_v1::OpenTabRequest {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    session_id: Some(proto::palyra::common::v1::CanonicalId {
                        ulid: session_id.clone(),
                    }),
                    url: open_tab.url,
                    activate: open_tab.activate,
                    timeout_ms: open_tab.timeout_ms,
                    allow_redirects: true,
                    max_redirects: 3,
                    allow_private_targets: false,
                });
                // The nested open_tab call re-runs authorization, so the
                // caller's authorization metadata must be forwarded verbatim.
                if let Some(value) = auth_header.clone() {
                    open_request.metadata_mut().insert(AUTHORIZATION_HEADER, value);
                }
                if let Some(value) = principal_header.clone() {
                    open_request.metadata_mut().insert(PRINCIPAL_HEADER, value);
                }
                // Marker consumed by open_tab: relay-initiated tabs may never
                // reach private targets, whatever the session allows.
                open_request.extensions_mut().insert(RelayPrivateTargetBlock);
                let open_response = self.open_tab(open_request).await?;
                let output = open_response.into_inner();
                Ok(Response::new(browser_v1::RelayActionResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: output.success,
                    action: browser_v1::RelayActionKind::OpenTab as i32,
                    error: output.error,
                    result: output.tab.map(browser_v1::relay_action_response::Result::OpenedTab),
                }))
            }
            browser_v1::RelayActionKind::CaptureSelection => {
                let Some(browser_v1::relay_action_request::Payload::CaptureSelection(
                    selection_payload,
                )) = payload.payload.take()
                else {
                    return Ok(Response::new(browser_v1::RelayActionResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        action: browser_v1::RelayActionKind::CaptureSelection as i32,
                        error: "relay capture_selection payload is required".to_owned(),
                        result: None,
                    }));
                };
                let selector = selection_payload.selector.trim();
                if selector.is_empty() {
                    return Ok(Response::new(browser_v1::RelayActionResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        action: browser_v1::RelayActionKind::CaptureSelection as i32,
                        error: "relay capture_selection selector is required".to_owned(),
                        result: None,
                    }));
                }
                let max_selection_bytes = if selection_payload.max_selection_bytes == 0 {
                    MAX_RELAY_SELECTION_BYTES
                } else {
                    selection_payload.max_selection_bytes.min(MAX_RELAY_SELECTION_BYTES as u64)
                        as usize
                };
                let (selected_text, truncated) = {
                    let mut sessions = self.runtime.sessions.lock().await;
                    let Some(session) = sessions.get_mut(session_id.as_str()) else {
                        return Ok(Response::new(browser_v1::RelayActionResponse {
                            v: CANONICAL_PROTOCOL_MAJOR,
                            success: false,
                            action: browser_v1::RelayActionKind::CaptureSelection as i32,
                            error: "session_not_found".to_owned(),
                            result: None,
                        }));
                    };
                    session.last_active = Instant::now();
                    let Some(tag) = find_matching_html_tag(
                        selector,
                        session
                            .active_tab()
                            .map(|tab| tab.last_page_body.as_str())
                            .unwrap_or_default(),
                    ) else {
                        return Ok(Response::new(browser_v1::RelayActionResponse {
                            v: CANONICAL_PROTOCOL_MAJOR,
                            success: false,
                            action: browser_v1::RelayActionKind::CaptureSelection as i32,
                            error: format!("selector '{selector}' was not found"),
                            result: None,
                        }));
                    };
                    truncate_utf8_bytes_with_flag(tag.as_str(), max_selection_bytes)
                };
                Ok(Response::new(browser_v1::RelayActionResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: true,
                    action: browser_v1::RelayActionKind::CaptureSelection as i32,
                    error: String::new(),
                    result: Some(browser_v1::relay_action_response::Result::Selection(
                        browser_v1::RelaySelectionResult {
                            selector: selector.to_owned(),
                            selected_text,
                            truncated,
                        },
                    )),
                }))
            }
            browser_v1::RelayActionKind::SendPageSnapshot => {
                let Some(browser_v1::relay_action_request::Payload::PageSnapshot(snapshot_payload)) =
                    payload.payload.take()
                else {
                    return Ok(Response::new(browser_v1::RelayActionResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        action: browser_v1::RelayActionKind::SendPageSnapshot as i32,
                        error: "relay page_snapshot payload is required".to_owned(),
                        result: None,
                    }));
                };
                let mut observe_request = Request::new(browser_v1::ObserveRequest {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    session_id: Some(proto::palyra::common::v1::CanonicalId {
                        ulid: session_id.clone(),
                    }),
                    include_dom_snapshot: snapshot_payload.include_dom_snapshot,
                    include_accessibility_tree: false,
                    include_visible_text: snapshot_payload.include_visible_text,
                    max_dom_snapshot_bytes: snapshot_payload.max_dom_snapshot_bytes,
                    max_accessibility_tree_bytes: 0,
                    max_visible_text_bytes: snapshot_payload.max_visible_text_bytes,
                    capture_selectors: Vec::new(),
                    computed_style_properties: Vec::new(),
                    max_capture_text_bytes: 0,
                });
                // The nested observe call re-runs authorization, so the
                // caller's authorization metadata must be forwarded verbatim.
                if let Some(value) = auth_header {
                    observe_request.metadata_mut().insert(AUTHORIZATION_HEADER, value);
                }
                if let Some(value) = principal_header {
                    observe_request.metadata_mut().insert(PRINCIPAL_HEADER, value);
                }
                let observe = self.observe(observe_request).await?;
                let observe = observe.into_inner();
                Ok(Response::new(browser_v1::RelayActionResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: observe.success,
                    action: browser_v1::RelayActionKind::SendPageSnapshot as i32,
                    error: observe.error,
                    result: if observe.success {
                        Some(browser_v1::relay_action_response::Result::Snapshot(
                            browser_v1::RelayPageSnapshotResult {
                                dom_snapshot: observe.dom_snapshot,
                                visible_text: observe.visible_text,
                                dom_truncated: observe.dom_truncated,
                                visible_text_truncated: observe.visible_text_truncated,
                                page_url: observe.page_url,
                            },
                        ))
                    } else {
                        None
                    },
                }))
            }
            _ => Ok(Response::new(browser_v1::RelayActionResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                action: browser_v1::RelayActionKind::Unspecified as i32,
                error: "unsupported relay action".to_owned(),
                result: None,
            })),
        }
    }

    async fn list_download_artifacts(
        &self,
        request: Request<browser_v1::ListDownloadArtifactsRequest>,
    ) -> Result<Response<browser_v1::ListDownloadArtifactsResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = request_principal(request.metadata())?.to_owned();
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let limit = if payload.limit == 0 {
            MAX_DOWNLOAD_ARTIFACTS_PER_SESSION
        } else {
            usize::try_from(payload.limit).unwrap_or(MAX_DOWNLOAD_ARTIFACTS_PER_SESSION)
        }
        .clamp(1, MAX_DOWNLOAD_ARTIFACTS_PER_SESSION);
        let quarantined_only = payload.quarantined_only;
        {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Err(Status::not_found("browser session not found"));
            };
            // Deliberately not_found rather than permission_denied: do not
            // reveal whether a foreign session ID exists.
            if session.principal != caller_principal {
                return Err(Status::not_found("browser session not found"));
            }
            session.last_active = Instant::now();
        }
        let guard = self.runtime.download_sessions.lock().await;
        if let Some(download_session) = guard.get(session_id.as_str()) {
            let filtered = download_session
                .artifacts
                .iter()
                .filter(|artifact| !quarantined_only || artifact.quarantined)
                .cloned()
                .collect::<Vec<_>>();
            let truncated = filtered.len() > limit;
            let artifacts = filtered
                .into_iter()
                .rev()
                .take(limit)
                .map(|record| download_artifact_to_proto(&record))
                .collect::<Vec<_>>();
            return Ok(Response::new(browser_v1::ListDownloadArtifactsResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                artifacts,
                truncated,
                error: String::new(),
            }));
        }
        Ok(Response::new(browser_v1::ListDownloadArtifactsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            artifacts: Vec::new(),
            truncated: false,
            error: String::new(),
        }))
    }

    async fn get_download_artifact(
        &self,
        request: Request<browser_v1::GetDownloadArtifactRequest>,
    ) -> Result<Response<browser_v1::GetDownloadArtifactResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let caller_principal = request_principal(request.metadata())?.to_owned();
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let artifact_id = parse_session_id_from_proto(payload.artifact_id.take())
            .map_err(Status::invalid_argument)?;
        {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Err(Status::not_found("browser session not found"));
            };
            // Deliberately not_found rather than permission_denied: do not
            // reveal whether a foreign session ID exists.
            if session.principal != caller_principal {
                return Err(Status::not_found("browser session not found"));
            }
            session.last_active = Instant::now();
        }
        match get_download_artifact_content(
            self.runtime.as_ref(),
            session_id.as_str(),
            artifact_id.as_str(),
            payload.max_bytes,
        )
        .await
        {
            Ok(artifact_content) => Ok(Response::new(browser_v1::GetDownloadArtifactResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: true,
                error: String::new(),
                artifact: Some(download_artifact_to_proto(&artifact_content.artifact)),
                content: artifact_content.content,
                content_truncated: artifact_content.truncated,
                content_offset_bytes: artifact_content.offset_bytes,
                content_limit_bytes: artifact_content.limit_bytes,
            })),
            Err(error) => Ok(Response::new(browser_v1::GetDownloadArtifactResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                error,
                artifact: None,
                content: Vec::new(),
                content_truncated: false,
                content_offset_bytes: 0,
                content_limit_bytes: payload.max_bytes,
            })),
        }
    }
}

/// Extracts the trimmed caller principal from request metadata.
///
/// # Errors
/// Returns `Status::unauthenticated` when the principal header is missing,
/// empty, or not valid ASCII.
fn request_principal(metadata: &tonic::metadata::MetadataMap) -> Result<&str, Status> {
    let Some(value) = metadata.get(PRINCIPAL_HEADER) else {
        return Err(Status::unauthenticated("missing caller principal"));
    };
    let principal =
        value.to_str().map_err(|_| Status::unauthenticated("invalid caller principal"))?.trim();
    if principal.is_empty() {
        return Err(Status::unauthenticated("missing caller principal"));
    }
    Ok(principal)
}

fn authenticated_request_principal(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<Option<&str>, Status> {
    if metadata.get(AUTHORIZATION_HEADER).is_none() {
        return Ok(None);
    }
    request_principal(metadata).map(Some)
}

fn enforce_authenticated_body_principal(
    caller_principal: Option<&str>,
    body_principal: &str,
) -> Result<(), Status> {
    let Some(caller_principal) = caller_principal else {
        return Ok(());
    };
    if caller_principal != body_principal {
        return Err(Status::permission_denied("principal mismatch"));
    }
    Ok(())
}

/// Reads an optional caller binding for session operations.
///
/// Authenticated requests must carry a principal-bound credential and cannot
/// use the legacy missing-principal path. Unauthenticated loopback operation
/// remains available only when browserd has no root auth token and no
/// persistent state.
fn optional_request_principal(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<Option<&str>, Status> {
    if metadata.get(PRINCIPAL_HEADER).is_none() {
        if metadata.get(AUTHORIZATION_HEADER).is_some() {
            return Err(Status::unauthenticated("missing caller principal"));
        }
        return Ok(None);
    }
    request_principal(metadata).map(Some)
}

/// Allows the root service credential to perform terminal cleanup without an
/// end-user principal. CloseSession is the only principal-owned RPC with this
/// administrative path; it can release a session but cannot observe its state.
fn optional_cleanup_request_principal(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<Option<&str>, Status> {
    if metadata.get(PRINCIPAL_HEADER).is_none() {
        return Ok(None);
    }
    request_principal(metadata).map(Some)
}

async fn enforce_session_owner_if_present(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    caller_principal: Option<&str>,
) -> Result<(), Status> {
    let Some(caller_principal) = caller_principal else {
        return Ok(());
    };
    let sessions = runtime.sessions.lock().await;
    let Some(session) = sessions.get(session_id) else {
        return Err(Status::not_found("browser session not found"));
    };
    if session.principal != caller_principal {
        return Err(Status::not_found("browser session not found"));
    }
    Ok(())
}

fn viewport_css_pixels(width: u32, height: u32) -> u64 {
    u64::from(width) * u64::from(height)
}

fn viewport_effective_pixels(css_pixels: u64, device_scale_factor: f64) -> f64 {
    css_pixels as f64 * device_scale_factor * device_scale_factor
}

/// Trims key names while preserving a literal single space, a valid key.
fn normalize_press_key_input(raw: &str) -> String {
    if raw == " " {
        " ".to_owned()
    } else {
        raw.trim().to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_observe_capture_selectors, normalize_observe_computed_style_properties,
        normalize_press_key_input, observe_byte_limit, observe_capture_text_limit,
        request_timeout_ms, resolve_observe_inclusions, ObserveInclusions,
        DEFAULT_OBSERVE_COMPUTED_STYLE_PROPERTIES, MAX_OBSERVE_CAPTURE_SELECTORS,
        MAX_OBSERVE_COMPUTED_STYLE_PROPERTIES,
    };

    #[test]
    fn observe_byte_limit_uses_session_budget_for_default_zero() {
        assert_eq!(observe_byte_limit(0, 16 * 1024), 16 * 1024);
    }

    #[test]
    fn observe_byte_limit_clamps_explicit_request_to_session_budget() {
        assert_eq!(observe_byte_limit(32 * 1024, 4096), 4096);
        assert_eq!(observe_byte_limit(256, 4096), 256);
    }

    #[test]
    fn observe_byte_limit_remains_non_zero() {
        assert_eq!(observe_byte_limit(0, 0), 1);
    }

    #[test]
    fn observe_capture_text_limit_uses_bounded_default() {
        assert_eq!(observe_capture_text_limit(0, 2_048), 512);
        assert_eq!(observe_capture_text_limit(0, 128), 128);
    }

    #[test]
    fn observe_capture_text_limit_clamps_explicit_request_to_session_budget() {
        assert_eq!(observe_capture_text_limit(4_096, 1_024), 1_024);
        assert_eq!(observe_capture_text_limit(128, 1_024), 128);
    }

    #[test]
    fn normalize_observe_capture_selectors_trims_dedupes_and_caps() {
        let selectors = [
            "  #hero  ",
            "",
            "#hero",
            ".nav",
            ".footer",
            "main",
            "[data-testid='save']",
            ".modal",
            ".toast",
            ".extra",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();

        let normalized = normalize_observe_capture_selectors(selectors.as_slice());

        assert_eq!(normalized.len(), MAX_OBSERVE_CAPTURE_SELECTORS);
        assert_eq!(normalized[0], "#hero");
        assert_eq!(normalized[1], ".nav");
        assert!(!normalized.iter().any(|selector| selector.is_empty()));
    }

    #[test]
    fn normalize_observe_computed_style_properties_defaults_and_filters_invalid_names() {
        let defaults = normalize_observe_computed_style_properties(&[]);
        assert_eq!(
            defaults,
            DEFAULT_OBSERVE_COMPUTED_STYLE_PROPERTIES
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>()
        );

        let properties =
            [" Display ", "display", "font-size", "backgroundColor", "--custom", "width1", ""]
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>();
        let normalized = normalize_observe_computed_style_properties(properties.as_slice());

        assert_eq!(
            normalized,
            vec!["display".to_owned(), "font-size".to_owned(), "--custom".to_owned()]
        );
        assert!(normalized.len() <= MAX_OBSERVE_COMPUTED_STYLE_PROPERTIES);
    }

    #[test]
    fn request_timeout_uses_session_budget_for_default_zero() {
        assert_eq!(request_timeout_ms(0, 10_000), 10_000);
    }

    #[test]
    fn request_timeout_clamps_explicit_request_to_session_budget() {
        assert_eq!(request_timeout_ms(25_000, 10_000), 10_000);
        assert_eq!(request_timeout_ms(500, 10_000), 500);
    }

    #[test]
    fn request_timeout_remains_non_zero() {
        assert_eq!(request_timeout_ms(0, 0), 1);
    }

    #[test]
    fn normalize_press_key_input_preserves_literal_space() {
        assert_eq!(normalize_press_key_input(" "), " ");
        assert_eq!(normalize_press_key_input(" Space "), "Space");
        assert!(normalize_press_key_input(" \t ").is_empty());
    }

    #[test]
    fn observe_inclusions_preserve_all_false_selection() {
        assert_eq!(
            resolve_observe_inclusions(false, false, false),
            ObserveInclusions {
                include_dom_snapshot: false,
                include_accessibility_tree: false,
                include_visible_text: false,
            }
        );
    }

    #[test]
    fn observe_inclusions_preserve_explicit_component_selection() {
        assert_eq!(
            resolve_observe_inclusions(true, false, false),
            ObserveInclusions {
                include_dom_snapshot: true,
                include_accessibility_tree: false,
                include_visible_text: false,
            }
        );
    }
}
