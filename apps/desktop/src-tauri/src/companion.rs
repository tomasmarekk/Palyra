use anyhow::{anyhow, Context, Result};
use palyra_control_plane::{self as control_plane};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

use super::companion_console::fetch_companion_console_data;
use super::desktop_state::{
    DesktopCompanionNotificationKind, DesktopCompanionOfflineDraft, DesktopCompanionRolloutState,
    DesktopCompanionSection,
};
use super::onboarding::{DesktopRefreshPayload, OnboardingStatusInputs};
use super::snapshot::{
    build_control_plane_client, build_dashboard_open_url, ensure_console_session_with_csrf,
    sanitize_log_line, DashboardOpenInputs,
};
use super::supervisor::{ConsolePayloadCache, ConsoleSessionCache};
use super::{normalize_optional_text, unix_ms_now, ControlCenter, RuntimeConfig};

#[derive(Debug)]
pub(crate) struct DesktopCompanionInputs {
    pub(crate) refresh_inputs: OnboardingStatusInputs,
    pub(crate) runtime: RuntimeConfig,
    pub(crate) admin_token: String,
    pub(crate) http_client: Client,
    pub(crate) console_session_cache: Arc<Mutex<Option<ConsoleSessionCache>>>,
    pub(crate) console_payload_cache: Arc<Mutex<ConsolePayloadCache>>,
    pub(crate) active_profile: DesktopCompanionProfileRecord,
    pub(crate) profiles: Vec<DesktopCompanionProfileRecord>,
    pub(crate) recent_profiles: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct DesktopCompanionPreferencesSnapshot {
    pub(crate) active_section: DesktopCompanionSection,
    pub(crate) active_session_id: Option<String>,
    pub(crate) active_device_id: Option<String>,
    pub(crate) last_run_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DesktopCompanionMetrics {
    pub(crate) unread_notifications: usize,
    pub(crate) pending_approvals: usize,
    pub(crate) queued_offline_drafts: usize,
    pub(crate) active_sessions: usize,
    pub(crate) sessions_with_active_runs: usize,
    pub(crate) trusted_devices: usize,
    pub(crate) stale_devices: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DesktopCompanionProfileRecord {
    pub(crate) context: control_plane::ConsoleProfileContext,
    pub(crate) implicit: bool,
    pub(crate) recent: bool,
    pub(crate) last_used_at_unix_ms: Option<i64>,
    pub(crate) active: bool,
}

impl DesktopCompanionProfileRecord {
    pub(crate) fn from_resolved_profile(
        profile: &super::profile_registry::DesktopResolvedProfile,
        recent: bool,
        implicit_recent_default: bool,
        active: bool,
    ) -> Self {
        Self {
            context: profile.context.clone(),
            implicit: profile.implicit,
            recent: if recent { true } else { implicit_recent_default && profile.implicit },
            last_used_at_unix_ms: profile.last_used_at_unix_ms,
            active,
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct DesktopCompanionSnapshot {
    pub(crate) generated_at_unix_ms: i64,
    pub(crate) control_center: super::snapshot::ControlCenterSnapshot,
    pub(crate) onboarding: super::onboarding::OnboardingStatusSnapshot,
    pub(crate) openai_status: super::openai_auth::OpenAiAuthStatusSnapshot,
    pub(crate) active_profile: DesktopCompanionProfileRecord,
    pub(crate) profiles: Vec<DesktopCompanionProfileRecord>,
    pub(crate) recent_profiles: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) console_session: Option<control_plane::ConsoleSession>,
    pub(crate) connection_state: String,
    pub(crate) rollout: DesktopCompanionRolloutState,
    pub(crate) preferences: DesktopCompanionPreferencesSnapshot,
    pub(crate) notifications: Vec<super::desktop_state::DesktopCompanionNotification>,
    pub(crate) offline_drafts: Vec<DesktopCompanionOfflineDraft>,
    pub(crate) session_catalog: Vec<control_plane::SessionCatalogRecord>,
    pub(crate) session_summary: Option<control_plane::SessionCatalogSummary>,
    pub(crate) approvals: Vec<Value>,
    pub(crate) inventory: Option<control_plane::InventoryListEnvelope>,
    pub(crate) warnings: Vec<String>,
    pub(crate) metrics: DesktopCompanionMetrics,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopCompanionPreferencesRequest {
    #[serde(default)]
    pub(crate) active_section: Option<DesktopCompanionSection>,
    #[serde(default)]
    pub(crate) active_session_id: Option<String>,
    #[serde(default)]
    pub(crate) active_device_id: Option<String>,
    #[serde(default)]
    pub(crate) last_run_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopCompanionRolloutRequest {
    #[serde(default)]
    pub(crate) companion_shell_enabled: Option<bool>,
    #[serde(default)]
    pub(crate) desktop_notifications_enabled: Option<bool>,
    #[serde(default)]
    pub(crate) offline_drafts_enabled: Option<bool>,
    #[serde(default)]
    pub(crate) voice_capture_enabled: Option<bool>,
    #[serde(default)]
    pub(crate) tts_playback_enabled: Option<bool>,
    #[serde(default)]
    pub(crate) release_channel: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopCompanionNotificationsRequest {
    #[serde(default)]
    pub(crate) ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopCompanionResolveSessionRequest {
    #[serde(default)]
    pub(crate) session_id: Option<String>,
    #[serde(default)]
    pub(crate) session_key: Option<String>,
    #[serde(default)]
    pub(crate) session_label: Option<String>,
    #[serde(default)]
    pub(crate) require_existing: bool,
    #[serde(default)]
    pub(crate) reset_session: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopCompanionSendMessageRequest {
    pub(crate) session_id: String,
    pub(crate) text: String,
    #[serde(default)]
    pub(crate) session_label: Option<String>,
    #[serde(default)]
    pub(crate) allow_sensitive_tools: bool,
    #[serde(default = "default_queue_on_failure")]
    pub(crate) queue_on_failure: bool,
    #[serde(default)]
    pub(crate) draft_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DesktopCompanionSendMessageResult {
    pub(crate) queued_offline: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) queued_draft_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) status: Option<String>,
    pub(crate) message: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopCompanionAudioTranscriptionRequest {
    pub(crate) session_id: String,
    pub(crate) filename: String,
    pub(crate) content_type: String,
    pub(crate) bytes_base64: String,
    pub(crate) consent_acknowledged: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DesktopCompanionAudioTranscriptionResult {
    pub(crate) attachment_id: String,
    pub(crate) artifact_id: String,
    pub(crate) transcript_text: String,
    #[serde(default)]
    pub(crate) transcript_summary: Option<String>,
    #[serde(default)]
    pub(crate) transcript_language: Option<String>,
    #[serde(default)]
    pub(crate) transcript_duration_ms: Option<u64>,
    #[serde(default)]
    pub(crate) transcript_processing_ms: Option<u64>,
    #[serde(default)]
    pub(crate) derived_artifact_id: Option<String>,
    pub(crate) privacy_note: String,
    #[serde(default)]
    pub(crate) warnings: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopCompanionApprovalDecisionRequest {
    pub(crate) approval_id: String,
    pub(crate) approved: bool,
    #[serde(default)]
    pub(crate) reason: Option<String>,
    #[serde(default)]
    pub(crate) scope: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopCompanionOpenDashboardRequest {
    #[serde(default)]
    pub(crate) section: Option<String>,
    #[serde(default)]
    pub(crate) session_id: Option<String>,
    #[serde(default)]
    pub(crate) device_id: Option<String>,
    #[serde(default)]
    pub(crate) run_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopCompanionSwitchProfileRequest {
    pub(crate) profile_name: String,
    #[serde(default)]
    pub(crate) allow_strict_switch: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DesktopTranscriptRecord {
    pub(crate) session_id: String,
    pub(crate) run_id: String,
    pub(crate) seq: i64,
    pub(crate) event_type: String,
    pub(crate) payload_json: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) origin_kind: String,
    #[serde(default)]
    pub(crate) origin_run_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DesktopQueuedInputRecord {
    pub(crate) queued_input_id: String,
    pub(crate) run_id: String,
    pub(crate) session_id: String,
    pub(crate) state: String,
    pub(crate) text: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
    #[serde(default)]
    pub(crate) origin_run_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DesktopSessionTranscriptEnvelope {
    pub(crate) session: control_plane::SessionCatalogRecord,
    #[serde(default)]
    pub(crate) records: Vec<DesktopTranscriptRecord>,
    #[serde(default)]
    pub(crate) queued_inputs: Vec<DesktopQueuedInputRecord>,
    #[serde(default)]
    pub(crate) runs: Vec<Value>,
    #[serde(default)]
    pub(crate) background_tasks: Vec<Value>,
}

#[derive(Debug, Deserialize)]
struct ChatSessionResolveEnvelope {
    session: DesktopResolvedChatSessionRecord,
}

#[derive(Debug, Deserialize)]
struct DesktopAttachmentUploadEnvelope {
    attachment: DesktopAttachmentUploadRecord,
    #[serde(default)]
    derived_artifacts: Vec<DesktopAttachmentDerivedArtifactRecord>,
}

#[derive(Debug, Deserialize)]
struct DesktopAttachmentUploadRecord {
    artifact_id: String,
    attachment_id: String,
}

#[derive(Debug, Deserialize)]
struct DesktopAttachmentDerivedArtifactRecord {
    derived_artifact_id: String,
    kind: String,
    state: String,
    #[serde(default)]
    content_text: Option<String>,
    #[serde(default)]
    summary_text: Option<String>,
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    duration_ms: Option<u64>,
    #[serde(default)]
    processing_ms: Option<u64>,
    #[serde(default)]
    warnings: Vec<DesktopAttachmentDerivedArtifactWarning>,
}

#[derive(Debug, Deserialize)]
struct DesktopAttachmentDerivedArtifactWarning {
    code: String,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DesktopResolvedChatSessionRecord {
    pub(crate) session_id: String,
    pub(crate) session_key: String,
    #[serde(default)]
    pub(crate) session_label: Option<String>,
    pub(crate) principal: String,
    pub(crate) device_id: String,
    #[serde(default)]
    pub(crate) channel: Option<String>,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
    #[serde(default)]
    pub(crate) last_run_id: Option<String>,
}

impl ControlCenter {
    pub(crate) fn companion_offline_drafts_enabled(&self) -> bool {
        self.persisted.active_companion().rollout.offline_drafts_enabled
    }

    pub(crate) fn capture_companion_inputs(&mut self) -> DesktopCompanionInputs {
        DesktopCompanionInputs {
            refresh_inputs: self.capture_onboarding_status_inputs(),
            runtime: self.runtime.clone(),
            admin_token: self.admin_token.clone(),
            http_client: self.http_client.clone(),
            console_session_cache: Arc::clone(&self.console_session_cache),
            console_payload_cache: Arc::clone(&self.console_payload_cache),
            active_profile: self.current_profile_record(),
            profiles: self.profile_records(),
            recent_profiles: self.persisted.recent_profile_names().to_vec(),
        }
    }

    pub(crate) fn update_companion_preferences(
        &mut self,
        payload: &DesktopCompanionPreferencesRequest,
    ) -> Result<()> {
        let companion = self.persisted.active_companion_mut();
        if let Some(section) = payload.active_section {
            companion.set_active_section(section);
        }
        if payload.active_session_id.is_some() {
            companion.set_active_session_id(payload.active_session_id.as_deref());
        }
        if payload.active_device_id.is_some() {
            companion.set_active_device_id(payload.active_device_id.as_deref());
        }
        if payload.last_run_id.is_some() {
            companion.set_last_run_id(payload.last_run_id.as_deref());
        }
        self.save_state_file()
    }

    pub(crate) fn update_companion_rollout(
        &mut self,
        payload: &DesktopCompanionRolloutRequest,
    ) -> Result<()> {
        let rollout = &mut self.persisted.active_companion_mut().rollout;
        if let Some(enabled) = payload.companion_shell_enabled {
            rollout.companion_shell_enabled = enabled;
        }
        if let Some(enabled) = payload.desktop_notifications_enabled {
            rollout.desktop_notifications_enabled = enabled;
        }
        if let Some(enabled) = payload.offline_drafts_enabled {
            rollout.offline_drafts_enabled = enabled;
        }
        if let Some(enabled) = payload.voice_capture_enabled {
            rollout.voice_capture_enabled = enabled;
        }
        if let Some(enabled) = payload.tts_playback_enabled {
            rollout.tts_playback_enabled = enabled;
        }
        if let Some(release_channel) =
            payload.release_channel.as_deref().and_then(normalize_optional_text)
        {
            rollout.release_channel = release_channel.to_owned();
        }
        self.save_state_file()
    }

    pub(crate) fn mark_companion_notifications_read(
        &mut self,
        ids: Option<&[String]>,
    ) -> Result<()> {
        self.persisted.active_companion_mut().mark_notifications_read(ids);
        self.save_state_file()
    }

    pub(crate) fn remove_companion_offline_draft(&mut self, draft_id: &str) -> Result<()> {
        self.persisted.active_companion_mut().remove_offline_draft(draft_id);
        self.save_state_file()
    }

    pub(crate) fn record_companion_run_completion(
        &mut self,
        run_id: &str,
        status: &str,
        session_id: &str,
    ) -> Result<()> {
        let detail =
            format!("Run {run_id} for session {session_id} completed with status {status}.");
        self.persisted.active_companion_mut().push_notification(
            DesktopCompanionNotificationKind::Run,
            "Companion run completed",
            detail,
            unix_ms_now(),
        );
        self.persisted.active_companion_mut().set_last_run_id(Some(run_id));
        self.save_state_file()
    }

    pub(crate) fn record_companion_offline_draft(
        &mut self,
        session_id: Option<&str>,
        text: &str,
        reason: &str,
    ) -> Result<String> {
        let draft_id = self.persisted.active_companion_mut().queue_offline_draft(
            session_id,
            text,
            reason,
            unix_ms_now(),
        );
        self.persisted.active_companion_mut().push_notification(
            DesktopCompanionNotificationKind::Draft,
            "Message queued for reconnect",
            format!("Stored a safe offline draft. {reason}"),
            unix_ms_now(),
        );
        self.save_state_file()?;
        Ok(draft_id)
    }

    pub(crate) fn reconcile_companion_snapshot(
        &mut self,
        snapshot: &mut DesktopCompanionSnapshot,
    ) -> Result<()> {
        let companion = self.persisted.active_companion_mut();
        let now_unix_ms = unix_ms_now();
        if companion.last_connection_state != snapshot.connection_state {
            if !companion.last_connection_state.trim().is_empty()
                && companion.last_connection_state != "unknown"
            {
                let title = if snapshot.connection_state == "connected" {
                    "Companion reconnected"
                } else {
                    "Companion connection changed"
                };
                companion.push_notification(
                    DesktopCompanionNotificationKind::Connection,
                    title,
                    format!(
                        "Connection state moved from {} to {}.",
                        companion.last_connection_state, snapshot.connection_state
                    ),
                    now_unix_ms,
                );
            }
            companion.last_connection_state = snapshot.connection_state.clone();
            if snapshot.connection_state == "connected" {
                companion.last_connected_at_unix_ms = Some(now_unix_ms);
            }
        }
        if snapshot.metrics.pending_approvals > companion.last_pending_approval_count {
            companion.push_notification(
                DesktopCompanionNotificationKind::Approval,
                "Approvals waiting",
                format!(
                    "{} companion-visible approvals now need review.",
                    snapshot.metrics.pending_approvals
                ),
                now_unix_ms,
            );
        }
        companion.last_pending_approval_count = snapshot.metrics.pending_approvals;
        snapshot.notifications = companion.notifications.clone();
        snapshot.offline_drafts = companion.offline_drafts.clone();
        snapshot.rollout = companion.rollout.clone();
        snapshot.preferences = DesktopCompanionPreferencesSnapshot {
            active_section: companion.active_section,
            active_session_id: companion.active_session_id.clone(),
            active_device_id: companion.active_device_id.clone(),
            last_run_id: companion.last_run_id.clone(),
        };
        snapshot.metrics.unread_notifications =
            companion.notifications.iter().filter(|entry| !entry.read).count();
        snapshot.metrics.queued_offline_drafts = companion.offline_drafts.len();
        self.save_state_file()
    }
}

pub(crate) async fn build_companion_snapshot(
    inputs: DesktopCompanionInputs,
) -> Result<DesktopCompanionSnapshot> {
    let DesktopCompanionInputs {
        refresh_inputs,
        runtime,
        admin_token,
        http_client,
        console_session_cache,
        console_payload_cache,
        active_profile,
        profiles,
        recent_profiles,
    } = inputs;
    let companion_state = refresh_inputs.persisted.active_companion().clone();
    let refresh_payload = super::build_desktop_refresh_payload(refresh_inputs).await?;
    let DesktopRefreshPayload { snapshot, onboarding_status, openai_status } = refresh_payload;

    let mut warnings = Vec::new();
    let mut console_session = None;
    let mut session_catalog = Vec::new();
    let mut session_summary = None;
    let mut approvals = Vec::new();
    let mut inventory = None;

    if companion_state.rollout.companion_shell_enabled {
        match fetch_companion_console_data(
            &http_client,
            &runtime,
            admin_token.as_str(),
            console_session_cache.as_ref(),
            console_payload_cache.as_ref(),
        )
        .await
        {
            Ok(data) => {
                console_session = Some(data.console_session);
                session_catalog = data.session_catalog.sessions;
                session_summary = Some(data.session_catalog.summary);
                approvals = data.approvals;
                inventory = Some(data.inventory);
                warnings.extend(data.warnings);
            }
            Err(error) => warnings.push(sanitize_log_line(error.to_string().as_str())),
        }
    } else {
        warnings.push("Desktop companion shell is disabled by rollout configuration.".to_owned());
    }

    let companion_connection_ready =
        !companion_state.rollout.companion_shell_enabled || console_session.is_some();
    let connection_state = derive_connection_state(
        companion_connection_ready,
        snapshot.gateway_process.desired_running || snapshot.gateway_process.running,
    );
    let pending_approval_count = approvals
        .iter()
        .filter(|approval| match approval.as_object().and_then(|value| value.get("decision")) {
            None => true,
            Some(value) => value.is_null(),
        })
        .count();

    let selected_session_id = companion_state
        .active_session_id
        .clone()
        .or_else(|| session_catalog.first().map(|record| record.session_id.clone()));
    let selected_device_id = companion_state.active_device_id.clone().or_else(|| {
        inventory
            .as_ref()
            .and_then(|list| list.devices.first().map(|record| record.device_id.clone()))
    });
    let active_sessions = session_summary.as_ref().map(|value| value.active_sessions).unwrap_or(0);
    let sessions_with_active_runs =
        session_summary.as_ref().map(|value| value.sessions_with_active_runs).unwrap_or(0);
    let trusted_devices =
        inventory.as_ref().map(|value| value.summary.trusted_devices).unwrap_or(0);
    let stale_devices = inventory.as_ref().map(|value| value.summary.stale_devices).unwrap_or(0);

    Ok(DesktopCompanionSnapshot {
        generated_at_unix_ms: unix_ms_now(),
        control_center: snapshot,
        onboarding: onboarding_status,
        openai_status,
        active_profile,
        profiles,
        recent_profiles,
        console_session,
        connection_state,
        rollout: companion_state.rollout.clone(),
        preferences: DesktopCompanionPreferencesSnapshot {
            active_section: companion_state.active_section,
            active_session_id: selected_session_id,
            active_device_id: selected_device_id,
            last_run_id: companion_state.last_run_id.clone(),
        },
        notifications: companion_state.notifications.clone(),
        offline_drafts: companion_state.offline_drafts.clone(),
        session_catalog,
        session_summary,
        approvals,
        inventory,
        warnings,
        metrics: DesktopCompanionMetrics {
            unread_notifications: companion_state
                .notifications
                .iter()
                .filter(|entry| !entry.read)
                .count(),
            pending_approvals: pending_approval_count,
            queued_offline_drafts: companion_state.offline_drafts.len(),
            active_sessions,
            sessions_with_active_runs,
            trusted_devices,
            stale_devices,
        },
    })
}

pub(crate) async fn resolve_companion_chat_session(
    http_client: &Client,
    runtime: &RuntimeConfig,
    admin_token: &str,
    payload: &DesktopCompanionResolveSessionRequest,
) -> Result<DesktopResolvedChatSessionRecord> {
    let mut control_plane = build_control_plane_client(http_client.clone(), runtime)?;
    let _csrf_token = ensure_console_session_with_csrf(&mut control_plane, admin_token).await?;
    let raw = control_plane
        .post_json_value(
            "console/v1/chat/sessions",
            &json!({
                "session_id": payload.session_id,
                "session_key": payload.session_key,
                "session_label": payload.session_label,
                "require_existing": payload.require_existing,
                "reset_session": payload.reset_session,
            }),
        )
        .await?;
    let parsed: ChatSessionResolveEnvelope = serde_json::from_value(raw)
        .context("chat session resolve response did not match the expected contract")?;
    Ok(parsed.session)
}

pub(crate) async fn fetch_companion_transcript(
    http_client: &Client,
    runtime: &RuntimeConfig,
    admin_token: &str,
    session_id: &str,
) -> Result<DesktopSessionTranscriptEnvelope> {
    let mut control_plane = build_control_plane_client(http_client.clone(), runtime)?;
    let _csrf_token = ensure_console_session_with_csrf(&mut control_plane, admin_token).await?;
    let raw = control_plane
        .get_json_value(format!("console/v1/chat/sessions/{}/transcript", urlencoding(session_id)))
        .await?;
    serde_json::from_value(raw)
        .context("desktop companion transcript response did not match the expected contract")
}

pub(crate) async fn send_companion_chat_message(
    http_client: &Client,
    runtime: &RuntimeConfig,
    admin_token: &str,
    payload: &DesktopCompanionSendMessageRequest,
) -> Result<DesktopCompanionSendMessageResult> {
    let mut control_plane = build_control_plane_client(http_client.clone(), runtime)?;
    let csrf_token = ensure_console_session_with_csrf(&mut control_plane, admin_token).await?;
    let url = build_console_url(
        runtime,
        format!(
            "console/v1/chat/sessions/{}/messages/stream",
            urlencoding(payload.session_id.as_str())
        )
        .as_str(),
    )?;
    let response = http_client
        .post(url)
        .header("Content-Type", "application/json")
        .header("x-palyra-csrf-token", csrf_token)
        .json(&json!({
            "text": payload.text,
            "allow_sensitive_tools": payload.allow_sensitive_tools,
            "session_label": payload.session_label,
        }))
        .send()
        .await
        .map_err(|error| anyhow!("desktop chat send failed: {error}"))?;
    if !response.status().is_success() {
        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "desktop chat send failed with HTTP {}: {}",
            status,
            sanitize_log_line(body.as_str())
        ));
    }
    let response_text = response
        .text()
        .await
        .map_err(|error| anyhow!("desktop chat stream body could not be read: {error}"))?;
    let stream_lines = response_text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(parse_chat_stream_line)
        .collect::<Result<Vec<_>>>()?;
    let run_id = stream_lines.iter().find_map(|line| {
        line.get("run_id").and_then(Value::as_str).map(str::to_owned).or_else(|| {
            line.get("event")
                .and_then(Value::as_object)
                .and_then(|event| event.get("run_id"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
    });
    let status = stream_lines
        .iter()
        .find_map(|line| line.get("status").and_then(Value::as_str).map(str::to_owned));
    let error_text = stream_lines.iter().find_map(|line| {
        if line.get("type").and_then(Value::as_str) == Some("error") {
            line.get("error").and_then(Value::as_str).map(str::to_owned)
        } else {
            None
        }
    });
    if let Some(error_text) = error_text {
        return Err(anyhow!(sanitize_log_line(error_text.as_str())));
    }
    Ok(DesktopCompanionSendMessageResult {
        queued_offline: false,
        queued_draft_id: None,
        run_id,
        status,
        message: "desktop companion chat turn completed".to_owned(),
    })
}

pub(crate) async fn transcribe_companion_audio(
    http_client: &Client,
    runtime: &RuntimeConfig,
    admin_token: &str,
    payload: &DesktopCompanionAudioTranscriptionRequest,
) -> Result<DesktopCompanionAudioTranscriptionResult> {
    const MAX_AUDIO_UPLOAD_BASE64_BYTES: usize = 16 * 1024 * 1024;
    const TRANSCRIPT_KIND: &str = "transcript";
    const TRANSCRIPT_STATE_SUCCEEDED: &str = "succeeded";

    if !payload.consent_acknowledged {
        return Err(anyhow!(
            "desktop voice capture requires explicit microphone consent acknowledgement"
        ));
    }
    let session_id = normalize_optional_text(payload.session_id.as_str())
        .ok_or_else(|| anyhow!("desktop voice transcription requires a session_id"))?;
    let filename = normalize_optional_text(payload.filename.as_str())
        .ok_or_else(|| anyhow!("desktop voice transcription requires a filename"))?;
    let content_type = normalize_optional_text(payload.content_type.as_str())
        .ok_or_else(|| anyhow!("desktop voice transcription requires a content_type"))?;
    if !content_type.to_ascii_lowercase().starts_with("audio/") {
        return Err(anyhow!("desktop voice transcription only accepts audio/* content types"));
    }
    if payload.bytes_base64.trim().is_empty() {
        return Err(anyhow!("desktop voice transcription requires non-empty audio bytes"));
    }
    if payload.bytes_base64.len() > MAX_AUDIO_UPLOAD_BASE64_BYTES {
        return Err(anyhow!(
            "desktop voice transcription payload exceeds the temporary desktop upload budget"
        ));
    }

    let mut control_plane = build_control_plane_client(http_client.clone(), runtime)?;
    let _csrf_token = ensure_console_session_with_csrf(&mut control_plane, admin_token).await?;
    let raw = control_plane
        .post_json_value(
            format!("console/v1/chat/sessions/{}/attachments", urlencoding(session_id)),
            &json!({
                "filename": filename,
                "content_type": content_type,
                "bytes_base64": payload.bytes_base64,
            }),
        )
        .await?;
    let envelope: DesktopAttachmentUploadEnvelope = serde_json::from_value(raw)
        .context("desktop voice attachment upload response did not match the expected contract")?;
    let transcript = envelope
        .derived_artifacts
        .iter()
        .find(|artifact| {
            artifact.kind == TRANSCRIPT_KIND && artifact.state == TRANSCRIPT_STATE_SUCCEEDED
        })
        .ok_or_else(|| {
            anyhow!("desktop voice transcription did not return a completed transcript")
        })?;
    let transcript_text = transcript
        .content_text
        .as_deref()
        .and_then(normalize_optional_text)
        .ok_or_else(|| anyhow!("desktop voice transcription completed without transcript text"))?
        .to_owned();

    Ok(DesktopCompanionAudioTranscriptionResult {
        attachment_id: envelope.attachment.attachment_id,
        artifact_id: envelope.attachment.artifact_id,
        transcript_text,
        transcript_summary: transcript.summary_text.clone(),
        transcript_language: transcript.language.clone(),
        transcript_duration_ms: transcript.duration_ms,
        transcript_processing_ms: transcript.processing_ms,
        derived_artifact_id: Some(transcript.derived_artifact_id.clone()),
        privacy_note: "Push-to-talk audio is uploaded only after you stop recording, follows the existing media retention/redaction pipeline, and ambient listening remains disabled.".to_owned(),
        warnings: transcript
            .warnings
            .iter()
            .map(|warning| format!("{}: {}", warning.code, warning.message))
            .collect(),
    })
}

pub(crate) async fn decide_companion_approval(
    http_client: &Client,
    runtime: &RuntimeConfig,
    admin_token: &str,
    payload: &DesktopCompanionApprovalDecisionRequest,
) -> Result<Value> {
    let mut control_plane = build_control_plane_client(http_client.clone(), runtime)?;
    let _csrf_token = ensure_console_session_with_csrf(&mut control_plane, admin_token).await?;
    let request = control_plane::ApprovalDecisionRequest {
        approved: payload.approved,
        reason: payload.reason.clone(),
        decision_scope: payload.scope.clone(),
        decision_scope_ttl_ms: None,
    };
    let response = control_plane.decide_approval(payload.approval_id.as_str(), &request).await?;
    serde_json::to_value(response)
        .context("desktop companion approval response could not be serialized")
}

pub(crate) async fn build_companion_handoff_url(
    inputs: DashboardOpenInputs,
    control_center: &super::snapshot::ControlCenterSnapshot,
    payload: &DesktopCompanionOpenDashboardRequest,
) -> Result<String> {
    let redirect_path = build_companion_redirect_path(payload);
    build_dashboard_open_url(
        inputs,
        apply_redirect_path(
            control_center.quick_facts.dashboard_url.as_str(),
            redirect_path.as_str(),
        )?
        .as_str(),
        control_center.quick_facts.dashboard_access_mode.as_str(),
    )
    .await
}

fn build_console_url(runtime: &RuntimeConfig, path: &str) -> Result<Url> {
    Url::parse(format!("http://127.0.0.1:{}/", runtime.gateway_admin_port).as_str())
        .map_err(|error| anyhow!("desktop companion console URL could not be created: {error}"))?
        .join(path.trim_start_matches('/'))
        .map_err(|error| anyhow!("desktop companion console path is invalid: {error}"))
}

fn parse_chat_stream_line(line: &str) -> Result<Value> {
    serde_json::from_str::<Value>(line).map_err(|_| anyhow!("chat stream emitted malformed JSON"))
}

fn derive_connection_state(requests_succeeded: bool, runtime_expected: bool) -> String {
    if requests_succeeded {
        "connected".to_owned()
    } else if runtime_expected {
        "reconnecting".to_owned()
    } else {
        "offline".to_owned()
    }
}

fn build_companion_redirect_path(payload: &DesktopCompanionOpenDashboardRequest) -> String {
    let section =
        payload.section.as_deref().and_then(normalize_optional_text).unwrap_or("overview");
    let mut query = Vec::new();
    if let Some(session_id) = payload.session_id.as_deref().and_then(normalize_optional_text) {
        query.push(format!("sessionId={}", urlencoding(session_id)));
    }
    if let Some(device_id) = payload.device_id.as_deref().and_then(normalize_optional_text) {
        query.push(format!("deviceId={}", urlencoding(device_id)));
    }
    if let Some(run_id) = payload.run_id.as_deref().and_then(normalize_optional_text) {
        query.push(format!("runId={}", urlencoding(run_id)));
    }
    let base_path = match section {
        "chat" => "/#/chat",
        "approvals" => "/#/control/approvals",
        "access" => "/#/settings/access",
        "onboarding" => "/#/settings/profiles",
        "overview" | "home" => "/#/control/overview",
        other if other.starts_with('/') => other,
        other => return format!("/#/control/{other}"),
    };

    if query.is_empty() {
        base_path.to_owned()
    } else {
        format!("{base_path}?{}", query.join("&"))
    }
}

fn apply_redirect_path(dashboard_url: &str, redirect_path: &str) -> Result<String> {
    let mut url = Url::parse(dashboard_url)
        .with_context(|| format!("invalid dashboard URL {dashboard_url}"))?;
    if redirect_path.starts_with("/#/") {
        let mut parts = redirect_path.splitn(2, '#');
        let path = parts.next().unwrap_or("/");
        let fragment = parts.next().unwrap_or_default();
        url.set_path(path);
        url.set_query(None);
        url.set_fragment(Some(fragment));
        return Ok(url.to_string());
    }
    Ok(dashboard_url.to_owned())
}

const fn default_queue_on_failure() -> bool {
    true
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
    use super::{
        apply_redirect_path, build_companion_redirect_path, derive_connection_state,
        DesktopCompanionOpenDashboardRequest,
    };

    #[test]
    fn redirect_path_maps_known_sections_and_preserves_query_context() {
        let payload = DesktopCompanionOpenDashboardRequest {
            section: Some("access".to_owned()),
            session_id: Some("session-1".to_owned()),
            device_id: Some("device-1".to_owned()),
            run_id: Some("run-1".to_owned()),
        };
        let redirect = build_companion_redirect_path(&payload);
        assert_eq!(
            redirect,
            "/#/settings/access?sessionId=session-1&deviceId=device-1&runId=run-1"
        );
    }

    #[test]
    fn apply_redirect_path_converts_hash_route_into_dashboard_url() {
        let redirected =
            apply_redirect_path("http://127.0.0.1:7142/", "/#/chat?sessionId=session-1")
                .expect("redirect should resolve");
        assert_eq!(redirected, "http://127.0.0.1:7142/#/chat?sessionId=session-1");
    }

    #[test]
    fn derive_connection_state_prefers_connected_when_runtime_session_is_available() {
        assert_eq!(derive_connection_state(true, true), "connected");
        assert_eq!(derive_connection_state(false, true), "reconnecting");
        assert_eq!(derive_connection_state(false, false), "offline");
    }
}
