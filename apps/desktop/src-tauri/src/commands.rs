use std::{sync::Arc, time::Duration};

use anyhow::Result;
use tauri::{Manager, State};
use tokio::sync::Mutex;

use super::{ControlCenter, SUPERVISOR_TICK_MS};
use super::snapshot::{
    ActionResult, ControlCenterSnapshot, DesktopSettingsSnapshot, SupportBundleExportResult,
    build_snapshot_from_inputs, run_support_bundle_export, sanitize_log_line,
};

pub(crate) struct DesktopAppState {
    supervisor: Arc<Mutex<ControlCenter>>,
}

#[tauri::command]
pub(crate) async fn get_snapshot(state: State<'_, DesktopAppState>) -> Result<ControlCenterSnapshot, String> {
    let snapshot_inputs = {
        let mut supervisor = state.supervisor.lock().await;
        supervisor.capture_snapshot_inputs()
    };
    build_snapshot_from_inputs(snapshot_inputs).await.map_err(|error| error.to_string())
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
    supervisor.set_browser_service_enabled(enabled).map_err(|error| error.to_string())?;
    let message = if enabled { "browser sidecar enabled" } else { "browser sidecar disabled" };
    Ok(ActionResult { ok: true, message: message.to_owned() })
}
#[tauri::command]
pub(crate) async fn start_palyra(state: State<'_, DesktopAppState>) -> Result<ActionResult, String> {
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
pub(crate) async fn restart_palyra(state: State<'_, DesktopAppState>) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.restart_all();
    Ok(ActionResult { ok: true, message: "restart requested".to_owned() })
}

#[tauri::command]
pub(crate) async fn open_dashboard(state: State<'_, DesktopAppState>) -> Result<ActionResult, String> {
    let supervisor = state.supervisor.lock().await;
    let url = supervisor.open_dashboard().map_err(|error| error.to_string())?;
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
    run_support_bundle_export(export_plan).await.map_err(|error| error.to_string())
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
    format!(
        "desktop initialization failed: {}",
        sanitize_log_line(error.to_string().as_str())
    )
}

pub(crate) fn initialize_control_center(
    init: impl FnOnce() -> Result<ControlCenter>,
) -> std::result::Result<ControlCenter, String> {
    init().map_err(|error| format_control_center_init_error(&error))
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
            export_support_bundle
        ])
        .run(tauri::generate_context!())
        .expect("tauri desktop runtime failed");
}
