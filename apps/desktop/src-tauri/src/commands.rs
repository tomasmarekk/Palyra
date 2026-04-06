use std::{sync::Arc, time::Duration};

use anyhow::Result;
use tauri::{AppHandle, Manager, State};
use tokio::sync::Mutex;

use super::companion::{
    build_companion_handoff_url, build_companion_snapshot, decide_companion_approval,
    fetch_companion_transcript, resolve_companion_chat_session, send_companion_chat_message,
    DesktopCompanionApprovalDecisionRequest, DesktopCompanionNotificationsRequest,
    DesktopCompanionOpenDashboardRequest, DesktopCompanionPreferencesRequest,
    DesktopCompanionResolveSessionRequest, DesktopCompanionRolloutRequest,
    DesktopCompanionSendMessageRequest, DesktopCompanionSendMessageResult,
    DesktopCompanionSnapshot, DesktopSessionTranscriptEnvelope,
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
    build_dashboard_open_url, build_snapshot_from_inputs, run_support_bundle_export,
    sanitize_log_line, ActionResult, ControlCenterSnapshot, DesktopSettingsSnapshot,
    SupportBundleExportResult,
};
use super::{
    build_onboarding_status, ControlCenter, DesktopOnboardingStep, DiscordOnboardingRequest,
    SUPERVISOR_TICK_MS,
};

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OnboardingStateRootRequest {
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    confirm_selection: bool,
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
        supervisor
            .reconcile_companion_snapshot(&mut snapshot)
            .map_err(command_error)?;
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
    supervisor
        .update_companion_preferences(&payload)
        .map_err(command_error)?;
    Ok(ActionResult { ok: true, message: "desktop companion preferences updated".to_owned() })
}

#[tauri::command]
pub(crate) async fn update_desktop_companion_rollout(
    state: State<'_, DesktopAppState>,
    payload: DesktopCompanionRolloutRequest,
) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor
        .update_companion_rollout(&payload)
        .map_err(command_error)?;
    Ok(ActionResult { ok: true, message: "desktop companion rollout updated".to_owned() })
}

#[tauri::command]
pub(crate) async fn mark_desktop_companion_notifications_read(
    state: State<'_, DesktopAppState>,
    payload: DesktopCompanionNotificationsRequest,
) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor
        .mark_companion_notifications_read(payload.ids.as_deref())
        .map_err(command_error)?;
    Ok(ActionResult { ok: true, message: "desktop companion notifications updated".to_owned() })
}

#[tauri::command]
pub(crate) async fn remove_desktop_companion_offline_draft(
    state: State<'_, DesktopAppState>,
    draft_id: String,
) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor
        .remove_companion_offline_draft(draft_id.as_str())
        .map_err(command_error)?;
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
pub(crate) async fn open_dashboard(
    state: State<'_, DesktopAppState>,
) -> Result<ActionResult, String> {
    let (snapshot_inputs, dashboard_open_inputs) = {
        let mut supervisor = state.supervisor.lock().await;
        (
            supervisor.capture_snapshot_inputs(),
            supervisor.capture_dashboard_open_inputs(),
        )
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
        (
            supervisor.capture_dashboard_open_inputs(),
            supervisor.capture_companion_inputs(),
        )
    };
    let control_center_snapshot =
        build_companion_snapshot(companion_inputs).await.map_err(command_error)?.control_center;
    let handoff_url = build_companion_handoff_url(
        &dashboard_inputs.http_client,
        &dashboard_inputs.runtime,
        dashboard_inputs.admin_token.as_str(),
        &control_center_snapshot,
        &payload,
    )
    .await
    .map_err(command_error)?;
    let mut supervisor = state.supervisor.lock().await;
    let opened = supervisor
        .open_dashboard(handoff_url.as_str())
        .map_err(command_error)?;
    let _ = supervisor.mark_dashboard_handoff_complete();
    Ok(ActionResult { ok: true, message: format!("opened {opened}") })
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
        (
            supervisor.http_client.clone(),
            supervisor.runtime.clone(),
            supervisor.admin_token.clone(),
        )
    };
    let session = resolve_companion_chat_session(&http_client, &runtime, admin_token.as_str(), &payload)
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
        (
            supervisor.http_client.clone(),
            supervisor.runtime.clone(),
            supervisor.admin_token.clone(),
        )
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
        (
            supervisor.http_client.clone(),
            supervisor.runtime.clone(),
            supervisor.admin_token.clone(),
        )
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
            if let (Some(run_id), Some(status)) = (result.run_id.as_deref(), result.status.as_deref())
            {
                let _ = supervisor.record_companion_run_completion(
                    run_id,
                    status,
                    payload.session_id.as_str(),
                );
            }
            result.message = format!("desktop companion sent a message to session {}", payload.session_id);
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
pub(crate) async fn decide_desktop_companion_approval(
    state: State<'_, DesktopAppState>,
    payload: DesktopCompanionApprovalDecisionRequest,
) -> Result<serde_json::Value, String> {
    let (http_client, runtime, admin_token) = {
        let supervisor = state.supervisor.lock().await;
        (
            supervisor.http_client.clone(),
            supervisor.runtime.clone(),
            supervisor.admin_token.clone(),
        )
    };
    let response = decide_companion_approval(&http_client, &runtime, admin_token.as_str(), &payload)
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
            supervisor.persisted.onboarding.completed_at_unix_ms
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

pub(crate) fn run() {
    let control_center = match initialize_control_center(ControlCenter::new) {
        Ok(value) => value,
        Err(message) => {
            eprintln!("{message}");
            return;
        }
    };

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
            mark_desktop_companion_notifications_read,
            remove_desktop_companion_offline_draft,
            start_palyra,
            stop_palyra,
            restart_palyra,
            open_dashboard,
            open_desktop_companion_handoff,
            export_support_bundle,
            resolve_desktop_companion_chat_session,
            get_desktop_companion_session_transcript,
            send_desktop_companion_chat_message,
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
