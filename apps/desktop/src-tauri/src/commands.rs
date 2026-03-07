use std::{sync::Arc, time::Duration};

use anyhow::Result;
use tauri::{Manager, State};
use tokio::sync::Mutex;

use super::openai_auth::{
    connect_openai_api_key, get_openai_oauth_callback_state, load_openai_auth_status,
    open_external_browser, reconnect_openai_oauth, refresh_openai_profile, revoke_openai_profile,
    set_openai_default_profile, start_openai_oauth_bootstrap, OpenAiApiKeyConnectRequest,
    OpenAiAuthStatusSnapshot, OpenAiControlPlaneInputs, OpenAiOAuthBootstrapRequest,
    OpenAiOAuthCallbackStateRequest, OpenAiOAuthCallbackStateSnapshot, OpenAiOAuthLaunchResult,
    OpenAiProfileActionRequest,
};
use super::snapshot::{
    build_snapshot_from_inputs, run_support_bundle_export, sanitize_log_line, ActionResult,
    ControlCenterSnapshot, DesktopSettingsSnapshot, SupportBundleExportResult,
};
use super::{ControlCenter, SUPERVISOR_TICK_MS};

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
pub(crate) async fn get_settings(
    state: State<'_, DesktopAppState>,
) -> Result<DesktopSettingsSnapshot, String> {
    let supervisor = state.supervisor.lock().await;
    Ok(supervisor.settings_snapshot())
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
pub(crate) async fn start_palyra(
    state: State<'_, DesktopAppState>,
) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.start_all();
    Ok(ActionResult { ok: true, message: "start requested".to_owned() })
}

#[tauri::command]
pub(crate) async fn stop_palyra(state: State<'_, DesktopAppState>) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.stop_all();
    Ok(ActionResult { ok: true, message: "stop requested".to_owned() })
}

#[tauri::command]
pub(crate) async fn restart_palyra(
    state: State<'_, DesktopAppState>,
) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.restart_all();
    Ok(ActionResult { ok: true, message: "restart requested".to_owned() })
}

#[tauri::command]
pub(crate) async fn open_dashboard(
    state: State<'_, DesktopAppState>,
) -> Result<ActionResult, String> {
    let supervisor = state.supervisor.lock().await;
    let url = supervisor.open_dashboard().map_err(command_error)?;
    Ok(ActionResult { ok: true, message: format!("opened {url}") })
}

#[tauri::command]
pub(crate) async fn export_support_bundle(
    state: State<'_, DesktopAppState>,
) -> Result<SupportBundleExportResult, String> {
    let export_plan = {
        let supervisor = state.supervisor.lock().await;
        supervisor.prepare_support_bundle_export()
    };
    run_support_bundle_export(export_plan).await.map_err(command_error)
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
    connect_openai_api_key(inputs, payload).await.map_err(command_error)
}

#[tauri::command]
pub(crate) async fn start_openai_oauth_bootstrap_command(
    state: State<'_, DesktopAppState>,
    payload: OpenAiOAuthBootstrapRequest,
) -> Result<OpenAiOAuthLaunchResult, String> {
    let inputs = capture_openai_inputs(&state).await;
    let result = start_openai_oauth_bootstrap(inputs, payload).await.map_err(command_error)?;
    open_browser_result(result)
}

#[tauri::command]
pub(crate) async fn get_openai_oauth_callback_state_command(
    state: State<'_, DesktopAppState>,
    payload: OpenAiOAuthCallbackStateRequest,
) -> Result<OpenAiOAuthCallbackStateSnapshot, String> {
    let inputs = capture_openai_inputs(&state).await;
    get_openai_oauth_callback_state(inputs, payload).await.map_err(command_error)
}

#[tauri::command]
pub(crate) async fn reconnect_openai_oauth_command(
    state: State<'_, DesktopAppState>,
    payload: OpenAiProfileActionRequest,
) -> Result<OpenAiOAuthLaunchResult, String> {
    let inputs = capture_openai_inputs(&state).await;
    let result = reconnect_openai_oauth(inputs, payload).await.map_err(command_error)?;
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

async fn capture_openai_inputs(state: &State<'_, DesktopAppState>) -> OpenAiControlPlaneInputs {
    let supervisor = state.supervisor.lock().await;
    OpenAiControlPlaneInputs::capture(&supervisor)
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
            get_snapshot,
            get_settings,
            set_browser_service_enabled,
            start_palyra,
            stop_palyra,
            restart_palyra,
            open_dashboard,
            export_support_bundle,
            get_openai_auth_status,
            connect_openai_api_key_command,
            start_openai_oauth_bootstrap_command,
            get_openai_oauth_callback_state_command,
            reconnect_openai_oauth_command,
            refresh_openai_profile_command,
            revoke_openai_profile_command,
            set_openai_default_profile_command,
            open_external_url_command
        ])
        .run(tauri::generate_context!())
        .expect("tauri desktop runtime failed");
}
