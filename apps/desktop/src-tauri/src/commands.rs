use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use palyra_control_plane as control_plane;
use tauri::{AppHandle, Manager, State};
use tokio::process::Command;
use tokio::sync::Mutex;
use ulid::Ulid;

use super::companion::{
    build_companion_handoff_url, build_companion_snapshot, decide_companion_approval,
    emit_companion_ux_event, fetch_companion_transcript, resolve_companion_chat_session,
    send_companion_chat_message, transcribe_companion_audio, DesktopCompanionAudioTranscriptionRequest,
    DesktopCompanionAudioTranscriptionResult,
    DesktopCompanionApprovalDecisionRequest, DesktopCompanionNotificationsRequest,
    DesktopCompanionOpenDashboardRequest, DesktopCompanionPreferencesRequest,
    DesktopCompanionResolveSessionRequest, DesktopCompanionRolloutRequest,
    DesktopCompanionSendMessageRequest, DesktopCompanionSendMessageResult,
    DesktopCompanionSnapshot, DesktopCompanionSwitchProfileRequest, DesktopCompanionUxEventRequest,
    DesktopSessionTranscriptEnvelope,
};
use super::features::onboarding::connectors::discord::{
    apply_discord_onboarding, run_discord_onboarding_preflight, verify_discord_connector,
    DiscordControlPlaneInputs, DiscordOnboardingApplySnapshot, DiscordOnboardingPreflightSnapshot,
    DiscordVerificationRequest, DiscordVerificationResult,
};
use super::onboarding::{DesktopRefreshPayload, OnboardingStatusSnapshot};
use super::openai_auth::{
    connect_openai_api_key, get_openai_oauth_callback_state, load_openai_auth_status,
    open_external_browser, reconnect_openai_oauth, refresh_openai_profile, revoke_openai_profile,
    set_openai_default_profile, start_openai_oauth_bootstrap, OpenAiApiKeyConnectRequest,
    OpenAiAuthStatusSnapshot, OpenAiControlPlaneInputs, OpenAiOAuthBootstrapRequest,
    OpenAiOAuthCallbackStateRequest, OpenAiOAuthCallbackStateSnapshot, OpenAiOAuthLaunchResult,
    OpenAiProfileActionRequest,
};
use super::snapshot::{
    build_control_plane_client, build_dashboard_open_url, build_snapshot_from_inputs,
    ensure_console_session_with_csrf, run_support_bundle_export, sanitize_log_line, ActionResult,
    ControlCenterSnapshot, DesktopSettingsSnapshot, SupportBundleExportResult,
};
use super::{
    build_onboarding_status, ControlCenter, DesktopOnboardingStep, DiscordOnboardingRequest,
    CONSOLE_PRINCIPAL, SUPERVISOR_TICK_MS,
};

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OnboardingStateRootRequest {
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    confirm_selection: bool,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct DesktopNodeLifecyclePayload {
    installed: bool,
    #[serde(default)]
    device_id: Option<String>,
    detail: String,
}

pub(crate) struct DesktopAppState {
    supervisor: Arc<Mutex<ControlCenter>>,
}

#[tauri::command]
pub(crate) async fn get_snapshot(
    state: State<'_, DesktopAppState>,
) -> Result<ControlCenterSnapshot, String> {
    let snapshot_inputs = {
        let mut supervisor = state.supervisor.lock().await;
        supervisor.capture_snapshot_inputs()
    };
    build_snapshot_from_inputs(snapshot_inputs).await.map_err(command_error)
}

#[tauri::command]
pub(crate) fn show_main_window(app: AppHandle) -> Result<(), String> {
    let window = app
        .get_webview_window("main")
        .ok_or_else(|| "desktop main window is unavailable".to_owned())?;
    window.show().map_err(command_error)?;
    window.set_focus().map_err(command_error)?;
    Ok(())
}

#[tauri::command]
pub(crate) async fn get_settings(
    state: State<'_, DesktopAppState>,
) -> Result<DesktopSettingsSnapshot, String> {
    let supervisor = state.supervisor.lock().await;
    Ok(supervisor.settings_snapshot())
}

#[tauri::command]
pub(crate) async fn get_onboarding_status(
    state: State<'_, DesktopAppState>,
) -> Result<OnboardingStatusSnapshot, String> {
    let inputs = {
        let mut supervisor = state.supervisor.lock().await;
        supervisor.capture_onboarding_status_inputs()
    };
    let status = build_onboarding_status(inputs).await;
    finalize_onboarding_status(state, status.map_err(command_error)?).await
}

#[tauri::command]
pub(crate) async fn get_desktop_refresh_payload(
    state: State<'_, DesktopAppState>,
) -> Result<DesktopRefreshPayload, String> {
    let inputs = {
        let mut supervisor = state.supervisor.lock().await;
        supervisor.capture_onboarding_status_inputs()
    };
    let payload = super::build_desktop_refresh_payload(inputs).await.map_err(command_error)?;
    let onboarding_status = finalize_onboarding_status(state, payload.onboarding_status).await?;
    Ok(DesktopRefreshPayload { onboarding_status, ..payload })
}

#[tauri::command]
pub(crate) async fn get_desktop_companion_snapshot(
    state: State<'_, DesktopAppState>,
) -> Result<DesktopCompanionSnapshot, String> {
    let inputs = {
        let mut supervisor = state.supervisor.lock().await;
        supervisor.capture_companion_inputs()
    };
    let mut snapshot = build_companion_snapshot(inputs).await.map_err(command_error)?;
    {
        let mut supervisor = state.supervisor.lock().await;
        supervisor.reconcile_companion_snapshot(&mut snapshot).map_err(command_error)?;
    }
    Ok(snapshot)
}

#[tauri::command]
pub(crate) async fn acknowledge_onboarding_welcome(
    state: State<'_, DesktopAppState>,
) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.mark_onboarding_welcome_acknowledged().map_err(command_error)?;
    Ok(ActionResult { ok: true, message: "desktop onboarding welcome acknowledged".to_owned() })
}

#[tauri::command]
pub(crate) async fn set_onboarding_state_root_command(
    state: State<'_, DesktopAppState>,
    payload: OnboardingStateRootRequest,
) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    let runtime_root = supervisor
        .set_runtime_state_root_override(payload.path.as_deref(), payload.confirm_selection)
        .map_err(|error| {
            let _ = supervisor
                .record_onboarding_failure(DesktopOnboardingStep::StateRoot, error.to_string());
            command_error(error)
        })?;
    Ok(ActionResult {
        ok: true,
        message: format!("desktop runtime state root set to {}", runtime_root.display()),
    })
}

#[tauri::command]
pub(crate) async fn set_browser_service_enabled(
    state: State<'_, DesktopAppState>,
    enabled: bool,
) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.set_browser_service_enabled(enabled).map_err(command_error)?;
    let message = if enabled { "browser sidecar enabled" } else { "browser sidecar disabled" };
    Ok(ActionResult { ok: true, message: message.to_owned() })
}

#[tauri::command]
pub(crate) async fn update_desktop_companion_preferences(
    state: State<'_, DesktopAppState>,
    payload: DesktopCompanionPreferencesRequest,
) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.update_companion_preferences(&payload).map_err(command_error)?;
    Ok(ActionResult { ok: true, message: "desktop companion preferences updated".to_owned() })
}

#[tauri::command]
pub(crate) async fn update_desktop_companion_rollout(
    state: State<'_, DesktopAppState>,
    payload: DesktopCompanionRolloutRequest,
) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.update_companion_rollout(&payload).map_err(command_error)?;
    Ok(ActionResult { ok: true, message: "desktop companion rollout updated".to_owned() })
}

#[tauri::command]
pub(crate) async fn switch_desktop_companion_profile(
    state: State<'_, DesktopAppState>,
    payload: DesktopCompanionSwitchProfileRequest,
) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    let message = supervisor
        .switch_active_profile(payload.profile_name.as_str(), payload.allow_strict_switch)
        .map_err(command_error)?;
    Ok(ActionResult { ok: true, message })
}

#[tauri::command]
pub(crate) async fn mark_desktop_companion_notifications_read(
    state: State<'_, DesktopAppState>,
    payload: DesktopCompanionNotificationsRequest,
) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.mark_companion_notifications_read(payload.ids.as_deref()).map_err(command_error)?;
    Ok(ActionResult { ok: true, message: "desktop companion notifications updated".to_owned() })
}

#[tauri::command]
pub(crate) async fn remove_desktop_companion_offline_draft(
    state: State<'_, DesktopAppState>,
    draft_id: String,
) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.remove_companion_offline_draft(draft_id.as_str()).map_err(command_error)?;
    Ok(ActionResult { ok: true, message: "desktop companion offline draft removed".to_owned() })
}

#[tauri::command]
pub(crate) async fn start_palyra(
    state: State<'_, DesktopAppState>,
) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.start_all();
    supervisor.refresh_runtime_state();
    let _ = supervisor.record_onboarding_event(
        "runtime_start_requested",
        Some("Desktop requested a local runtime start.".to_owned()),
    );
    Ok(ActionResult { ok: true, message: "start requested".to_owned() })
}

#[tauri::command]
pub(crate) async fn stop_palyra(state: State<'_, DesktopAppState>) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.stop_all();
    let _ = supervisor.record_onboarding_event(
        "runtime_stop_requested",
        Some("Desktop requested a local runtime stop.".to_owned()),
    );
    Ok(ActionResult { ok: true, message: "stop requested".to_owned() })
}

#[tauri::command]
pub(crate) async fn restart_palyra(
    state: State<'_, DesktopAppState>,
) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.restart_all();
    supervisor.refresh_runtime_state();
    let _ = supervisor.record_onboarding_event(
        "runtime_restart_requested",
        Some("Desktop requested a local runtime restart.".to_owned()),
    );
    Ok(ActionResult { ok: true, message: "restart requested".to_owned() })
}

#[tauri::command]
pub(crate) async fn enroll_desktop_node(
    state: State<'_, DesktopAppState>,
) -> Result<ActionResult, String> {
    run_desktop_node_enrollment(state, false).await
}

#[tauri::command]
pub(crate) async fn repair_desktop_node(
    state: State<'_, DesktopAppState>,
) -> Result<ActionResult, String> {
    run_desktop_node_enrollment(state, true).await
}

#[tauri::command]
pub(crate) async fn reset_desktop_node(
    state: State<'_, DesktopAppState>,
) -> Result<ActionResult, String> {
    let runtime_root = {
        let mut supervisor = state.supervisor.lock().await;
        supervisor.stop_node_host();
        supervisor.runtime_root.clone()
    };
    let lifecycle = run_palyra_json_command(runtime_root.as_path(), &["node", "uninstall", "--json"])
        .await
        .map_err(command_error)?;
    let detail = lifecycle
        .get("detail")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("desktop node host reset");
    {
        let mut supervisor = state.supervisor.lock().await;
        supervisor.sync_node_host_desired_state();
        supervisor.refresh_runtime_state();
    }
    Ok(ActionResult { ok: true, message: sanitize_log_line(detail) })
}

#[tauri::command]
pub(crate) async fn open_dashboard(
    state: State<'_, DesktopAppState>,
) -> Result<ActionResult, String> {
    let (snapshot_inputs, dashboard_open_inputs) = {
        let mut supervisor = state.supervisor.lock().await;
        (supervisor.capture_snapshot_inputs(), supervisor.capture_dashboard_open_inputs())
    };
    let snapshot = build_snapshot_from_inputs(snapshot_inputs).await.map_err(command_error)?;
    if snapshot.quick_facts.dashboard_access_mode == "local"
        && snapshot.quick_facts.gateway_version.is_none()
    {
        let message =
            "local runtime is not healthy yet; start or refresh Palyra before opening the dashboard"
                .to_owned();
        let mut supervisor = state.supervisor.lock().await;
        let _ = supervisor
            .record_onboarding_failure(DesktopOnboardingStep::DashboardHandoff, message.clone());
        return Err(message);
    }

    let dashboard_url = build_dashboard_open_url(
        dashboard_open_inputs,
        snapshot.quick_facts.dashboard_url.as_str(),
        snapshot.quick_facts.dashboard_access_mode.as_str(),
    )
    .await
    .map_err(command_error)?;

    let mut supervisor = state.supervisor.lock().await;
    let url = supervisor.open_dashboard(dashboard_url.as_str()).map_err(|error| {
        let _ = supervisor
            .record_onboarding_failure(DesktopOnboardingStep::DashboardHandoff, error.to_string());
        command_error(error)
    })?;
    let _ = supervisor.mark_dashboard_handoff_complete();
    Ok(ActionResult { ok: true, message: format!("opened {url}") })
}

#[tauri::command]
pub(crate) async fn open_desktop_companion_handoff(
    state: State<'_, DesktopAppState>,
    payload: DesktopCompanionOpenDashboardRequest,
) -> Result<ActionResult, String> {
    let (dashboard_inputs, companion_inputs) = {
        let mut supervisor = state.supervisor.lock().await;
        (supervisor.capture_dashboard_open_inputs(), supervisor.capture_companion_inputs())
    };
    let control_center_snapshot =
        build_companion_snapshot(companion_inputs).await.map_err(command_error)?.control_center;
    let handoff_url = build_companion_handoff_url(dashboard_inputs, &control_center_snapshot, &payload)
    .await
    .map_err(command_error)?;
    let mut supervisor = state.supervisor.lock().await;
    let opened = supervisor.open_dashboard(handoff_url.as_str()).map_err(command_error)?;
    let _ = supervisor.mark_dashboard_handoff_complete();
    Ok(ActionResult { ok: true, message: format!("opened {opened}") })
}

#[tauri::command]
pub(crate) async fn emit_desktop_companion_ux_event(
    state: State<'_, DesktopAppState>,
    payload: DesktopCompanionUxEventRequest,
) -> Result<ActionResult, String> {
    let companion_inputs = {
        let mut supervisor = state.supervisor.lock().await;
        supervisor.capture_companion_inputs()
    };
    emit_companion_ux_event(&companion_inputs, &payload)
        .await
        .map_err(command_error)?;
    Ok(ActionResult { ok: true, message: format!("recorded {}", payload.name) })
}

#[tauri::command]
pub(crate) async fn export_support_bundle(
    state: State<'_, DesktopAppState>,
) -> Result<SupportBundleExportResult, String> {
    let export_plan = {
        let mut supervisor = state.supervisor.lock().await;
        let _ = supervisor.record_onboarding_event(
            "support_bundle_export_requested",
            Some("Desktop requested a support bundle export.".to_owned()),
        );
        supervisor.prepare_support_bundle_export()
    };
    match run_support_bundle_export(export_plan).await {
        Ok(result) => {
            let mut supervisor = state.supervisor.lock().await;
            let _ = supervisor
                .record_support_bundle_export_result(true, Some(result.output_path.clone()));
            Ok(result)
        }
        Err(error) => {
            let message = command_error(error);
            let mut supervisor = state.supervisor.lock().await;
            let _ = supervisor.record_support_bundle_export_result(false, Some(message.clone()));
            Err(message)
        }
    }
}

#[tauri::command]
pub(crate) async fn resolve_desktop_companion_chat_session(
    state: State<'_, DesktopAppState>,
    payload: DesktopCompanionResolveSessionRequest,
) -> Result<serde_json::Value, String> {
    let (http_client, runtime, admin_token) = {
        let supervisor = state.supervisor.lock().await;
        (supervisor.http_client.clone(), supervisor.runtime.clone(), supervisor.admin_token.clone())
    };
    let session =
        resolve_companion_chat_session(&http_client, &runtime, admin_token.as_str(), &payload)
            .await
            .map_err(command_error)?;
    {
        let mut supervisor = state.supervisor.lock().await;
        let request = DesktopCompanionPreferencesRequest {
            active_section: None,
            active_session_id: Some(session.session_id.clone()),
            active_device_id: None,
            last_run_id: None,
        };
        let _ = supervisor.update_companion_preferences(&request);
    }
    serde_json::to_value(session).map_err(command_error)
}

#[tauri::command]
pub(crate) async fn get_desktop_companion_session_transcript(
    state: State<'_, DesktopAppState>,
    session_id: String,
) -> Result<DesktopSessionTranscriptEnvelope, String> {
    let (http_client, runtime, admin_token) = {
        let supervisor = state.supervisor.lock().await;
        (supervisor.http_client.clone(), supervisor.runtime.clone(), supervisor.admin_token.clone())
    };
    fetch_companion_transcript(&http_client, &runtime, admin_token.as_str(), session_id.as_str())
        .await
        .map_err(command_error)
}

#[tauri::command]
pub(crate) async fn send_desktop_companion_chat_message(
    state: State<'_, DesktopAppState>,
    payload: DesktopCompanionSendMessageRequest,
) -> Result<DesktopCompanionSendMessageResult, String> {
    let (http_client, runtime, admin_token) = {
        let supervisor = state.supervisor.lock().await;
        (supervisor.http_client.clone(), supervisor.runtime.clone(), supervisor.admin_token.clone())
    };
    match send_companion_chat_message(&http_client, &runtime, admin_token.as_str(), &payload).await
    {
        Ok(mut result) => {
            let mut supervisor = state.supervisor.lock().await;
            let request = DesktopCompanionPreferencesRequest {
                active_section: Some(super::DesktopCompanionSection::Chat),
                active_session_id: Some(payload.session_id.clone()),
                active_device_id: None,
                last_run_id: result.run_id.clone(),
            };
            let _ = supervisor.update_companion_preferences(&request);
            if let Some(draft_id) = payload.draft_id.as_deref() {
                let _ = supervisor.remove_companion_offline_draft(draft_id);
            }
            if let (Some(run_id), Some(status)) =
                (result.run_id.as_deref(), result.status.as_deref())
            {
                let _ = supervisor.record_companion_run_completion(
                    run_id,
                    status,
                    payload.session_id.as_str(),
                );
            }
            result.message =
                format!("desktop companion sent a message to session {}", payload.session_id);
            Ok(result)
        }
        Err(error) => {
            if payload.queue_on_failure {
                let mut supervisor = state.supervisor.lock().await;
                if !supervisor.companion_offline_drafts_enabled() {
                    return Err(command_error(error));
                }
                let draft_id = supervisor
                    .record_companion_offline_draft(
                        Some(payload.session_id.as_str()),
                        payload.text.as_str(),
                        error.to_string().as_str(),
                    )
                    .map_err(command_error)?;
                return Ok(DesktopCompanionSendMessageResult {
                    queued_offline: true,
                    queued_draft_id: Some(draft_id),
                    run_id: None,
                    status: Some("queued_offline".to_owned()),
                    message: "Desktop queued the message locally because the control plane is unavailable.".to_owned(),
                });
            }
            Err(command_error(error))
        }
    }
}

#[tauri::command]
pub(crate) async fn transcribe_desktop_companion_audio(
    state: State<'_, DesktopAppState>,
    payload: DesktopCompanionAudioTranscriptionRequest,
) -> Result<DesktopCompanionAudioTranscriptionResult, String> {
    let (http_client, runtime, admin_token, voice_enabled) = {
        let supervisor = state.supervisor.lock().await;
        (
            supervisor.http_client.clone(),
            supervisor.runtime.clone(),
            supervisor.admin_token.clone(),
            supervisor.persisted.active_companion().rollout.voice_capture_enabled,
        )
    };
    if !voice_enabled {
        return Err(
            "desktop voice capture is disabled by rollout configuration; enable it before recording"
                .to_owned(),
        );
    }
    transcribe_companion_audio(&http_client, &runtime, admin_token.as_str(), &payload)
        .await
        .map_err(command_error)
}

#[tauri::command]
pub(crate) async fn decide_desktop_companion_approval(
    state: State<'_, DesktopAppState>,
    payload: DesktopCompanionApprovalDecisionRequest,
) -> Result<serde_json::Value, String> {
    let (http_client, runtime, admin_token) = {
        let supervisor = state.supervisor.lock().await;
        (supervisor.http_client.clone(), supervisor.runtime.clone(), supervisor.admin_token.clone())
    };
    let response =
        decide_companion_approval(&http_client, &runtime, admin_token.as_str(), &payload)
            .await
            .map_err(command_error)?;
    Ok(response)
}

#[tauri::command]
pub(crate) async fn get_openai_auth_status(
    state: State<'_, DesktopAppState>,
) -> Result<OpenAiAuthStatusSnapshot, String> {
    let inputs = capture_openai_inputs(&state).await;
    match load_openai_auth_status(inputs).await {
        Ok(snapshot) => Ok(snapshot),
        Err(error) => Ok(OpenAiAuthStatusSnapshot::unavailable(command_error(error))),
    }
}

#[tauri::command]
pub(crate) async fn connect_openai_api_key_command(
    state: State<'_, DesktopAppState>,
    payload: OpenAiApiKeyConnectRequest,
) -> Result<ActionResult, String> {
    let inputs = capture_openai_inputs(&state).await;
    let result = match connect_openai_api_key(inputs, payload.clone()).await {
        Ok(result) => result,
        Err(error) => {
            let message = command_error(error);
            let mut supervisor = state.supervisor.lock().await;
            let _ = supervisor
                .record_onboarding_failure(DesktopOnboardingStep::OpenAiConnect, message.clone());
            return Err(message);
        }
    };
    {
        let mut supervisor = state.supervisor.lock().await;
        let _ = supervisor.mark_openai_connected(Some("api_key"), payload.profile_id.as_deref());
    }
    Ok(result)
}

#[tauri::command]
pub(crate) async fn start_openai_oauth_bootstrap_command(
    state: State<'_, DesktopAppState>,
    payload: OpenAiOAuthBootstrapRequest,
) -> Result<OpenAiOAuthLaunchResult, String> {
    let inputs = capture_openai_inputs(&state).await;
    let result = match start_openai_oauth_bootstrap(inputs, payload).await {
        Ok(result) => result,
        Err(error) => {
            let message = command_error(error);
            let mut supervisor = state.supervisor.lock().await;
            let _ = supervisor
                .record_onboarding_failure(DesktopOnboardingStep::OpenAiConnect, message.clone());
            return Err(message);
        }
    };
    {
        let mut supervisor = state.supervisor.lock().await;
        let _ = supervisor
            .record_onboarding_event("openai_oauth_started", Some(result.attempt_id.clone()));
        let _ = supervisor.clear_onboarding_failure();
    }
    open_browser_result(result)
}

#[tauri::command]
pub(crate) async fn get_openai_oauth_callback_state_command(
    state: State<'_, DesktopAppState>,
    payload: OpenAiOAuthCallbackStateRequest,
) -> Result<OpenAiOAuthCallbackStateSnapshot, String> {
    let inputs = capture_openai_inputs(&state).await;
    let response = match get_openai_oauth_callback_state(inputs, payload).await {
        Ok(response) => response,
        Err(error) => {
            let message = command_error(error);
            let mut supervisor = state.supervisor.lock().await;
            let _ = supervisor
                .record_onboarding_failure(DesktopOnboardingStep::OpenAiConnect, message.clone());
            return Err(message);
        }
    };
    {
        let mut supervisor = state.supervisor.lock().await;
        if response.state == "succeeded" {
            let _ = supervisor.mark_openai_connected(Some("oauth"), response.profile_id.as_deref());
        } else if response.state == "failed" {
            let _ = supervisor.record_onboarding_failure(
                DesktopOnboardingStep::OpenAiConnect,
                response.message.clone(),
            );
        }
    }
    Ok(response)
}

#[tauri::command]
pub(crate) async fn reconnect_openai_oauth_command(
    state: State<'_, DesktopAppState>,
    payload: OpenAiProfileActionRequest,
) -> Result<OpenAiOAuthLaunchResult, String> {
    let inputs = capture_openai_inputs(&state).await;
    let result = match reconnect_openai_oauth(inputs, payload).await {
        Ok(result) => result,
        Err(error) => {
            let message = command_error(error);
            let mut supervisor = state.supervisor.lock().await;
            let _ = supervisor
                .record_onboarding_failure(DesktopOnboardingStep::OpenAiConnect, message.clone());
            return Err(message);
        }
    };
    {
        let mut supervisor = state.supervisor.lock().await;
        let _ = supervisor.record_onboarding_event(
            "openai_oauth_reconnect_started",
            Some(result.attempt_id.clone()),
        );
        let _ = supervisor.clear_onboarding_failure();
    }
    open_browser_result(result)
}

#[tauri::command]
pub(crate) async fn refresh_openai_profile_command(
    state: State<'_, DesktopAppState>,
    payload: OpenAiProfileActionRequest,
) -> Result<ActionResult, String> {
    let inputs = capture_openai_inputs(&state).await;
    refresh_openai_profile(inputs, payload).await.map_err(command_error)
}

#[tauri::command]
pub(crate) async fn revoke_openai_profile_command(
    state: State<'_, DesktopAppState>,
    payload: OpenAiProfileActionRequest,
) -> Result<ActionResult, String> {
    let inputs = capture_openai_inputs(&state).await;
    revoke_openai_profile(inputs, payload).await.map_err(command_error)
}

#[tauri::command]
pub(crate) async fn set_openai_default_profile_command(
    state: State<'_, DesktopAppState>,
    payload: OpenAiProfileActionRequest,
) -> Result<ActionResult, String> {
    let inputs = capture_openai_inputs(&state).await;
    set_openai_default_profile(inputs, payload).await.map_err(command_error)
}

#[tauri::command]
pub(crate) async fn run_discord_onboarding_preflight_command(
    state: State<'_, DesktopAppState>,
    payload: DiscordOnboardingRequest,
) -> Result<DiscordOnboardingPreflightSnapshot, String> {
    let inputs = capture_discord_inputs(&state).await;
    let response = match run_discord_onboarding_preflight(inputs, payload.clone()).await {
        Ok(response) => response,
        Err(error) => {
            let message = command_error(error);
            let mut supervisor = state.supervisor.lock().await;
            let _ = supervisor
                .record_onboarding_failure(DesktopOnboardingStep::DiscordConnect, message.clone());
            return Err(message);
        }
    };
    {
        let mut supervisor = state.supervisor.lock().await;
        let _ = supervisor.mark_discord_preflight(&payload);
    }
    Ok(response)
}

#[tauri::command]
pub(crate) async fn apply_discord_onboarding_command(
    state: State<'_, DesktopAppState>,
    payload: DiscordOnboardingRequest,
) -> Result<DiscordOnboardingApplySnapshot, String> {
    let inputs = capture_discord_inputs(&state).await;
    let response = match apply_discord_onboarding(inputs, payload.clone()).await {
        Ok(response) => response,
        Err(error) => {
            let message = command_error(error);
            let mut supervisor = state.supervisor.lock().await;
            let _ = supervisor
                .record_onboarding_failure(DesktopOnboardingStep::DiscordConnect, message.clone());
            return Err(message);
        }
    };
    {
        let mut supervisor = state.supervisor.lock().await;
        let _ = supervisor.mark_discord_applied(&payload);
    }
    Ok(response)
}

#[tauri::command]
pub(crate) async fn verify_discord_connector_command(
    state: State<'_, DesktopAppState>,
    payload: DiscordVerificationRequest,
) -> Result<DiscordVerificationResult, String> {
    let inputs = capture_discord_inputs(&state).await;
    let response = match verify_discord_connector(inputs, payload.clone()).await {
        Ok(response) => response,
        Err(error) => {
            let message = command_error(error);
            let mut supervisor = state.supervisor.lock().await;
            let _ = supervisor
                .record_onboarding_failure(DesktopOnboardingStep::DiscordConnect, message.clone());
            return Err(message);
        }
    };
    {
        let mut supervisor = state.supervisor.lock().await;
        let _ = supervisor
            .mark_discord_verified(response.connector_id.as_str(), response.target.as_str());
    }
    Ok(response)
}

#[tauri::command]
pub(crate) async fn open_external_url_command(url: String) -> Result<ActionResult, String> {
    open_external_browser(url.as_str(), webbrowser::open).map_err(command_error)?;
    Ok(ActionResult { ok: true, message: "opened browser handoff".to_owned() })
}

pub(crate) async fn supervisor_loop(supervisor: Arc<Mutex<ControlCenter>>) {
    loop {
        {
            let mut guard = supervisor.lock().await;
            guard.refresh_runtime_state();
        }
        tokio::time::sleep(Duration::from_millis(SUPERVISOR_TICK_MS)).await;
    }
}

pub(crate) fn format_control_center_init_error(error: &anyhow::Error) -> String {
    format!("desktop initialization failed: {}", sanitize_log_line(error.to_string().as_str()))
}

pub(crate) fn initialize_control_center(
    init: impl FnOnce() -> Result<ControlCenter>,
) -> std::result::Result<ControlCenter, String> {
    init().map_err(|error| format_control_center_init_error(&error))
}

pub(crate) fn prepare_control_center_for_launch(control_center: &mut ControlCenter) {
    control_center.start_all();
    control_center.refresh_runtime_state();
}

pub(crate) fn command_error(error: impl ToString) -> String {
    sanitize_log_line(error.to_string().as_str())
}

async fn finalize_onboarding_status(
    state: State<'_, DesktopAppState>,
    mut status: OnboardingStatusSnapshot,
) -> Result<OnboardingStatusSnapshot, String> {
    if status.current_step == DesktopOnboardingStep::Completion
        && status.completion_unix_ms.is_none()
    {
        let completion_unix_ms = {
            let mut supervisor = state.supervisor.lock().await;
            supervisor.mark_onboarding_complete().map_err(command_error)?;
            supervisor.persisted.active_profile_completion_unix_ms()
        };
        status.phase = "home".to_owned();
        status.completion_unix_ms = completion_unix_ms;
    }

    Ok(status)
}

async fn capture_openai_inputs(state: &State<'_, DesktopAppState>) -> OpenAiControlPlaneInputs {
    let supervisor = state.supervisor.lock().await;
    OpenAiControlPlaneInputs::capture(&supervisor)
}

async fn capture_discord_inputs(state: &State<'_, DesktopAppState>) -> DiscordControlPlaneInputs {
    let supervisor = state.supervisor.lock().await;
    DiscordControlPlaneInputs::capture(&supervisor)
}

fn open_browser_result(result: OpenAiOAuthLaunchResult) -> Result<OpenAiOAuthLaunchResult, String> {
    match open_external_browser(result.authorization_url.as_str(), webbrowser::open) {
        Ok(()) => Ok(result.mark_browser_opened()),
        Err(error) => {
            let warning = format!(
                "Open the pending browser handoff manually if needed. {}",
                command_error(error)
            );
            Ok(result.mark_browser_pending(warning.as_str()))
        }
    }
}

async fn run_desktop_node_enrollment(
    state: State<'_, DesktopAppState>,
    repair: bool,
) -> Result<ActionResult, String> {
    let (http_client, runtime, admin_token, runtime_root, gateway_running) = {
        let supervisor = state.supervisor.lock().await;
        (
            supervisor.http_client.clone(),
            supervisor.runtime.clone(),
            supervisor.admin_token.clone(),
            supervisor.runtime_root.clone(),
            supervisor.gateway.child.is_some(),
        )
    };
    if !gateway_running {
        return Err("desktop node enrollment requires the local gateway to be running".to_owned());
    }

    let status = read_desktop_node_lifecycle(runtime_root.as_path()).await.map_err(command_error)?;
    if status.installed && !repair {
        return Ok(ActionResult {
            ok: true,
            message: sanitize_log_line(status.detail.as_str()),
        });
    }

    let device_id = status.device_id.unwrap_or_else(|| Ulid::new().to_string());
    if repair && status.installed {
        let _ = run_palyra_json_command(runtime_root.as_path(), &["node", "uninstall", "--json"])
            .await
            .map_err(command_error)?;
    }

    let mut control_plane =
        build_control_plane_client(http_client.clone(), &runtime).map_err(command_error)?;
    let _csrf = ensure_console_session_with_csrf(&mut control_plane, admin_token.as_str())
        .await
        .map_err(command_error)?;
    let code = control_plane
        .mint_node_pairing_code(&control_plane::NodePairingCodeMintRequest {
            method: control_plane::NodePairingMethod::Pin,
            issued_by: Some(CONSOLE_PRINCIPAL.to_owned()),
            ttl_ms: Some(10 * 60 * 1_000),
        })
        .await
        .map_err(|error| command_error(anyhow!("failed to mint desktop node pairing code: {error}")))?
        .code
        .code;
    let grpc_url = format!("https://127.0.0.1:{}", runtime.gateway_grpc_port.saturating_add(1));
    let runtime_root_for_install = runtime_root.clone();
    let device_id_for_install = device_id.clone();
    let install_task = tauri::async_runtime::spawn(async move {
        let args = vec![
            "node".to_owned(),
            "install".to_owned(),
            "--json".to_owned(),
            "--device-id".to_owned(),
            device_id_for_install,
            "--grpc-url".to_owned(),
            grpc_url,
            "--method".to_owned(),
            "pin".to_owned(),
            "--pairing-code".to_owned(),
            code,
        ];
        run_palyra_json_command_owned(runtime_root_for_install.as_path(), &args).await
    });
    let request_id = wait_for_pending_node_pairing_request(&control_plane, device_id.as_str())
        .await
        .map_err(command_error)?;
    control_plane
        .approve_node_pairing_request(
            request_id.as_str(),
            &control_plane::NodePairingDecisionRequest {
                reason: Some(if repair {
                    "desktop_control_center_repair_enroll".to_owned()
                } else {
                    "desktop_control_center_enroll".to_owned()
                }),
            },
        )
        .await
        .map_err(|error| command_error(anyhow!("failed to approve desktop node pairing: {error}")))?;
    let install_payload = install_task
        .await
        .map_err(|error| command_error(anyhow!("desktop node install task failed: {error}")))?
        .map_err(command_error)?;
    {
        let mut supervisor = state.supervisor.lock().await;
        supervisor.sync_node_host_desired_state();
        supervisor.refresh_runtime_state();
    }
    let detail = install_payload
        .get("detail")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("desktop node enrolled");
    Ok(ActionResult { ok: true, message: sanitize_log_line(detail) })
}

async fn wait_for_pending_node_pairing_request(
    control_plane: &control_plane::ControlPlaneClient,
    device_id: &str,
) -> Result<String> {
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(20) {
        let envelope = control_plane
            .list_node_pairing_requests(Some(&control_plane::NodePairingListQuery {
                client_kind: Some("node".to_owned()),
                state: Some(control_plane::NodePairingRequestState::PendingApproval),
            }))
            .await
            .map_err(|error| anyhow!("failed to poll desktop node pairing requests: {error}"))?;
        if let Some(request_id) = envelope
            .requests
            .into_iter()
            .find(|record| record.device_id == device_id)
            .map(|record| record.request_id)
        {
            return Ok(request_id);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(anyhow!(
        "timed out waiting for desktop node pairing request for device {device_id}"
    ))
}

async fn read_desktop_node_lifecycle(runtime_root: &Path) -> Result<DesktopNodeLifecyclePayload> {
    let payload = run_palyra_json_command(runtime_root, &["node", "status", "--json"]).await?;
    serde_json::from_value(payload).context("failed to decode desktop node lifecycle payload")
}

async fn run_palyra_json_command(runtime_root: &Path, args: &[&str]) -> Result<serde_json::Value> {
    let owned_args = args.iter().map(|value| (*value).to_owned()).collect::<Vec<_>>();
    run_palyra_json_command_owned(runtime_root, &owned_args).await
}

async fn run_palyra_json_command_owned(
    runtime_root: &Path,
    args: &[String],
) -> Result<serde_json::Value> {
    let cli_path = super::resolve_binary_path("palyra", "PALYRA_DESKTOP_PALYRA_BIN")?;
    let mut command = Command::new(cli_path.as_path());
    super::configure_background_command(&mut command);
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .env("PALYRA_STATE_ROOT", runtime_root.to_string_lossy().into_owned())
        .env(
            "PALYRA_GATEWAY_IDENTITY_STORE_DIR",
            runtime_root.join("identity").to_string_lossy().into_owned(),
        );
    for arg in args {
        command.arg(arg);
    }
    let output = command.output().await.with_context(|| {
        format!("failed to run desktop CLI command `{}`", args.join(" "))
    })?;
    let stdout = String::from_utf8_lossy(output.stdout.as_slice()).to_string();
    let stderr = sanitize_log_line(String::from_utf8_lossy(output.stderr.as_slice()).as_ref());
    if !output.status.success() {
        return Err(anyhow!(
            "desktop CLI command `{}` failed: {}",
            args.join(" "),
            stderr
        ));
    }
    serde_json::from_str(stdout.as_str()).with_context(|| {
        format!("failed to decode desktop CLI JSON output for `{}`", args.join(" "))
    })
}

pub(crate) fn run() {
    let mut control_center = match initialize_control_center(|| {
        super::bootstrap_portable_install_environment()?;
        ControlCenter::new()
    }) {
        Ok(value) => value,
        Err(message) => {
            eprintln!("{message}");
            return;
        }
    };
    prepare_control_center_for_launch(&mut control_center);

    tauri::Builder::default()
        .manage(DesktopAppState { supervisor: Arc::new(Mutex::new(control_center)) })
        .setup(|app| {
            let state = app.state::<DesktopAppState>().supervisor.clone();
            tauri::async_runtime::spawn(supervisor_loop(state));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            show_main_window,
            get_snapshot,
            get_settings,
            get_onboarding_status,
            get_desktop_refresh_payload,
            get_desktop_companion_snapshot,
            acknowledge_onboarding_welcome,
            set_onboarding_state_root_command,
            set_browser_service_enabled,
            update_desktop_companion_preferences,
            update_desktop_companion_rollout,
            switch_desktop_companion_profile,
            mark_desktop_companion_notifications_read,
            remove_desktop_companion_offline_draft,
            start_palyra,
            stop_palyra,
            restart_palyra,
            enroll_desktop_node,
            repair_desktop_node,
            reset_desktop_node,
            open_dashboard,
            open_desktop_companion_handoff,
            emit_desktop_companion_ux_event,
            export_support_bundle,
            resolve_desktop_companion_chat_session,
            get_desktop_companion_session_transcript,
            send_desktop_companion_chat_message,
            transcribe_desktop_companion_audio,
            decide_desktop_companion_approval,
            get_openai_auth_status,
            connect_openai_api_key_command,
            start_openai_oauth_bootstrap_command,
            get_openai_oauth_callback_state_command,
            reconnect_openai_oauth_command,
            refresh_openai_profile_command,
            revoke_openai_profile_command,
            set_openai_default_profile_command,
            run_discord_onboarding_preflight_command,
            apply_discord_onboarding_command,
            verify_discord_connector_command,
            open_external_url_command
        ])
        .run(tauri::generate_context!())
        .expect("tauri desktop runtime failed");
}
