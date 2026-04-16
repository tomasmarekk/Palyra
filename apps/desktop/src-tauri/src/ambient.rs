use anyhow::{anyhow, Context, Result};
use tauri::{
    menu::{CheckMenuItem, Menu, MenuItem},
    tray::{MouseButton, MouseButtonState, TrayIcon, TrayIconBuilder, TrayIconEvent},
    App, AppHandle, Manager, Runtime, WebviewUrl, WebviewWindow, WebviewWindowBuilder,
    WindowEvent,
};
use tauri_plugin_autostart::ManagerExt as AutostartExt;
use tauri_plugin_global_shortcut::{GlobalShortcutExt, ShortcutEvent};

use super::commands::DesktopAppState;
use super::companion::DesktopCompanionSnapshot;
use super::desktop_state::{
    DesktopCompanionRolloutState, DesktopCompanionState, DesktopCompanionSurfaceMode,
};
use super::snapshot::{build_dashboard_open_url, build_snapshot_from_inputs, sanitize_log_line};
use super::ControlCenter;

const TRAY_ID: &str = "desktop-companion";
const WINDOW_MAIN: &str = "main";
const WINDOW_QUICK_PANEL: &str = "quick-panel";
const WINDOW_VOICE_OVERLAY: &str = "voice-overlay";

const MENU_OPEN_MAIN: &str = "companion.open-main";
const MENU_OPEN_QUICK_PANEL: &str = "companion.open-quick-panel";
const MENU_OPEN_VOICE_OVERLAY: &str = "companion.open-voice-overlay";
const MENU_TOGGLE_START_ON_LOGIN: &str = "companion.toggle-start-on-login";
const MENU_TOGGLE_GLOBAL_HOTKEY: &str = "companion.toggle-global-hotkey";
const MENU_OPEN_DASHBOARD: &str = "companion.open-dashboard";
const MENU_QUIT: &str = "companion.quit";

#[derive(Debug, Clone, Default)]
pub(crate) struct AmbientSyncOutcome {
    pub(crate) hotkey_registration_error: Option<String>,
}

pub(crate) struct AmbientRuntimeState<R: Runtime> {
    #[allow(dead_code)]
    tray: TrayIcon<R>,
    open_main: MenuItem<R>,
    open_quick_panel: MenuItem<R>,
    open_voice_overlay: MenuItem<R>,
    toggle_start_on_login: CheckMenuItem<R>,
    toggle_global_hotkey: CheckMenuItem<R>,
}

pub(crate) fn setup_ambient_runtime<R: Runtime>(app: &mut App<R>) -> Result<()> {
    let open_main = MenuItem::with_id(app, MENU_OPEN_MAIN, "Open Control Center", true, None::<&str>)
        .context("failed to create ambient main window menu item")?;
    let open_quick_panel = MenuItem::with_id(
        app,
        MENU_OPEN_QUICK_PANEL,
        "Open Quick Panel",
        true,
        None::<&str>,
    )
    .context("failed to create ambient quick panel menu item")?;
    let open_voice_overlay = MenuItem::with_id(
        app,
        MENU_OPEN_VOICE_OVERLAY,
        "Open Voice Overlay",
        true,
        None::<&str>,
    )
    .context("failed to create ambient voice overlay menu item")?;
    let toggle_start_on_login = CheckMenuItem::with_id(
        app,
        MENU_TOGGLE_START_ON_LOGIN,
        "Start on Login",
        true,
        false,
        None::<&str>,
    )
    .context("failed to create ambient start-on-login menu item")?;
    let toggle_global_hotkey = CheckMenuItem::with_id(
        app,
        MENU_TOGGLE_GLOBAL_HOTKEY,
        "Enable Global Hotkey",
        true,
        true,
        None::<&str>,
    )
    .context("failed to create ambient global hotkey menu item")?;
    let open_dashboard = MenuItem::with_id(
        app,
        MENU_OPEN_DASHBOARD,
        "Open Dashboard",
        true,
        None::<&str>,
    )
    .context("failed to create ambient dashboard menu item")?;
    let quit = MenuItem::with_id(app, MENU_QUIT, "Quit", true, None::<&str>)
        .context("failed to create ambient quit menu item")?;

    let tray_menu = Menu::with_items(
        app,
        &[
            &open_main,
            &open_quick_panel,
            &open_voice_overlay,
            &toggle_start_on_login,
            &toggle_global_hotkey,
            &open_dashboard,
            &quit,
        ],
    )
    .context("failed to build ambient tray menu")?;

    let mut tray_builder = TrayIconBuilder::with_id(TRAY_ID)
        .menu(&tray_menu)
        .tooltip("Palyra desktop companion")
        .show_menu_on_left_click(false)
        .on_menu_event(|app, event| {
            if let Err(error) = handle_tray_menu_event(app, event.id.as_ref()) {
                eprintln!("ambient tray menu event failed: {error}");
            }
        })
        .on_tray_icon_event(|tray, event| {
            if let Err(error) = handle_tray_icon_event(tray, &event) {
                eprintln!("ambient tray icon event failed: {error}");
            }
        });
    if let Some(icon) = app.default_window_icon().cloned() {
        tray_builder = tray_builder.icon(icon);
    }
    let tray = tray_builder.build(app).context("failed to build ambient tray icon")?;

    app.manage(AmbientRuntimeState {
        tray,
        open_main,
        open_quick_panel,
        open_voice_overlay,
        toggle_start_on_login,
        toggle_global_hotkey,
    });

    Ok(())
}

pub(crate) fn sync_ambient_runtime_from_snapshot<R: Runtime>(
    app: &AppHandle<R>,
    snapshot: &DesktopCompanionSnapshot,
) -> Result<AmbientSyncOutcome> {
    sync_ambient_runtime(
        app,
        &snapshot.rollout,
        &snapshot.preferences.active_section,
        &snapshot.ambient.last_surface,
        &snapshot.ambient.global_hotkey,
        snapshot.ambient.global_hotkey_enabled,
        snapshot.ambient.start_on_login_enabled,
        snapshot.connection_state.as_str(),
        snapshot.metrics.unread_notifications,
        snapshot.metrics.pending_approvals,
        snapshot.metrics.active_runs,
        snapshot.offline_drafts.len(),
    )
}

pub(crate) fn sync_ambient_runtime_from_state<R: Runtime>(
    app: &AppHandle<R>,
    companion: &DesktopCompanionState,
) -> Result<AmbientSyncOutcome> {
    sync_ambient_runtime(
        app,
        &companion.rollout,
        &companion.active_section,
        &companion.ambient.last_surface,
        &companion.ambient.global_hotkey,
        companion.ambient.global_hotkey_enabled,
        companion.ambient.start_on_login_enabled,
        companion.last_connection_state.as_str(),
        companion.notifications.iter().filter(|entry| !entry.read).count(),
        companion.last_pending_approval_count,
        0,
        companion.offline_drafts.len(),
    )
}

pub(crate) fn show_surface<R: Runtime>(
    app: &AppHandle<R>,
    requested: DesktopCompanionSurfaceMode,
) -> Result<DesktopCompanionSurfaceMode> {
    let companion = load_companion_state(app)?;
    let surface = resolve_supported_surface(requested, &companion.rollout, companion.ambient.last_surface);
    let window = ensure_surface_window(app, surface)?;
    hide_other_surfaces(app, surface)?;
    window.show().context("failed to show companion surface")?;
    window.set_focus().context("failed to focus companion surface")?;
    persist_last_surface(app, surface)?;
    Ok(surface)
}

pub(crate) fn hide_surface<R: Runtime>(
    app: &AppHandle<R>,
    requested: Option<DesktopCompanionSurfaceMode>,
) -> Result<()> {
    match requested {
        Some(surface) => hide_window_if_present(app, surface_window_label(surface))?,
        None => {
            hide_window_if_present(app, WINDOW_MAIN)?;
            hide_window_if_present(app, WINDOW_QUICK_PANEL)?;
            hide_window_if_present(app, WINDOW_VOICE_OVERLAY)?;
        }
    }
    Ok(())
}

pub(crate) fn toggle_preferred_surface<R: Runtime>(app: &AppHandle<R>) -> Result<()> {
    let companion = load_companion_state(app)?;
    let preferred =
        resolve_supported_surface(companion.ambient.last_surface, &companion.rollout, companion.ambient.last_surface);
    let window = ensure_surface_window(app, preferred)?;
    let visible = window.is_visible().unwrap_or(false);
    if visible {
        window.hide().context("failed to hide preferred companion surface")?;
        return Ok(());
    }
    let _ = show_surface(app, preferred)?;
    Ok(())
}

pub(crate) fn handle_window_event<R: Runtime>(
    window: &tauri::Window<R>,
    event: &WindowEvent,
) -> Result<()> {
    let Some(surface) = surface_from_window_label(window.label()) else {
        return Ok(());
    };
    match event {
        WindowEvent::CloseRequested { api, .. } => {
            api.prevent_close();
            window.hide().context("failed to hide ambient window after close request")?;
        }
        WindowEvent::Focused(false)
            if matches!(surface, DesktopCompanionSurfaceMode::QuickPanel) =>
        {
            window.hide().context("failed to auto-hide transient ambient surface")?;
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn handle_tray_menu_event<R: Runtime>(app: &AppHandle<R>, id: &str) -> Result<()> {
    match id {
        MENU_OPEN_MAIN => {
            let _ = show_surface(app, DesktopCompanionSurfaceMode::Main)?;
        }
        MENU_OPEN_QUICK_PANEL => {
            let _ = show_surface(app, DesktopCompanionSurfaceMode::QuickPanel)?;
        }
        MENU_OPEN_VOICE_OVERLAY => {
            let _ = show_surface(app, DesktopCompanionSurfaceMode::VoiceOverlay)?;
        }
        MENU_TOGGLE_START_ON_LOGIN => {
            let companion = with_supervisor(app, |supervisor| {
                let next = !supervisor.persisted.active_companion().ambient.start_on_login_enabled;
                supervisor.persisted.active_companion_mut().ambient.start_on_login_enabled = next;
                supervisor.save_state_file()?;
                Ok(supervisor.persisted.active_companion().clone())
            })?;
            let outcome = sync_ambient_runtime_from_state(app, &companion)?;
            persist_hotkey_registration_error(app, outcome.hotkey_registration_error.as_deref())?;
        }
        MENU_TOGGLE_GLOBAL_HOTKEY => {
            let companion = with_supervisor(app, |supervisor| {
                let next = !supervisor.persisted.active_companion().ambient.global_hotkey_enabled;
                supervisor.persisted.active_companion_mut().ambient.global_hotkey_enabled = next;
                supervisor.save_state_file()?;
                Ok(supervisor.persisted.active_companion().clone())
            })?;
            let outcome = sync_ambient_runtime_from_state(app, &companion)?;
            persist_hotkey_registration_error(app, outcome.hotkey_registration_error.as_deref())?;
        }
        MENU_OPEN_DASHBOARD => spawn_dashboard_handoff(app.clone()),
        MENU_QUIT => {
            app.exit(0);
        }
        _ => {}
    }
    Ok(())
}

fn handle_tray_icon_event<R: Runtime>(tray: &TrayIcon<R>, event: &TrayIconEvent) -> Result<()> {
    if let TrayIconEvent::Click { button, button_state, .. } = event {
        if *button == MouseButton::Left && *button_state == MouseButtonState::Up {
            toggle_preferred_surface(tray.app_handle())?;
        }
    }
    Ok(())
}

fn sync_ambient_runtime<R: Runtime>(
    app: &AppHandle<R>,
    rollout: &DesktopCompanionRolloutState,
    active_section: &super::DesktopCompanionSection,
    last_surface: &DesktopCompanionSurfaceMode,
    global_hotkey: &str,
    global_hotkey_enabled: bool,
    start_on_login_enabled: bool,
    connection_state: &str,
    unread_notifications: usize,
    pending_approvals: usize,
    active_runs: usize,
    queued_offline_drafts: usize,
) -> Result<AmbientSyncOutcome> {
    let ambient = app.state::<AmbientRuntimeState<R>>();
    let quick_panel_available = rollout.companion_shell_enabled && rollout.ambient_companion_enabled;
    let voice_overlay_available = rollout.companion_shell_enabled
        && rollout.ambient_companion_enabled
        && rollout.voice_capture_enabled
        && rollout.voice_overlay_enabled;

    ambient
        .open_main
        .set_text(format!("Open Control Center ({})", section_label(*active_section)).as_str())
        .context("failed to update tray main label")?;
    ambient
        .open_quick_panel
        .set_text(
            format!(
                "Open Quick Panel ({} drafts, {} approvals)",
                queued_offline_drafts, pending_approvals
            )
            .as_str(),
        )
        .context("failed to update quick panel tray label")?;
    ambient
        .open_quick_panel
        .set_enabled(quick_panel_available)
        .context("failed to update quick panel tray availability")?;
    ambient
        .open_voice_overlay
        .set_text(format!("Open Voice Overlay ({} active runs)", active_runs).as_str())
        .context("failed to update voice overlay tray label")?;
    ambient
        .open_voice_overlay
        .set_enabled(voice_overlay_available)
        .context("failed to update voice overlay tray availability")?;
    ambient
        .toggle_start_on_login
        .set_checked(start_on_login_enabled)
        .context("failed to update start-on-login tray state")?;
    ambient
        .toggle_global_hotkey
        .set_text(
            format!(
                "Enable Global Hotkey{}",
                hotkey_suffix(global_hotkey, global_hotkey_enabled)
            )
            .as_str(),
        )
        .context("failed to update global hotkey tray label")?;
    ambient
        .toggle_global_hotkey
        .set_checked(global_hotkey_enabled)
        .context("failed to update global hotkey tray state")?;
    ambient
        .toggle_global_hotkey
        .set_enabled(quick_panel_available)
        .context("failed to update global hotkey tray availability")?;

    if !voice_overlay_available && *last_surface == DesktopCompanionSurfaceMode::VoiceOverlay {
        hide_window_if_present(app, WINDOW_VOICE_OVERLAY)?;
    }
    if !quick_panel_available && *last_surface == DesktopCompanionSurfaceMode::QuickPanel {
        hide_window_if_present(app, WINDOW_QUICK_PANEL)?;
    }

    refresh_autostart(app, start_on_login_enabled)?;
    let hotkey_registration_error =
        refresh_global_hotkey(app, rollout, global_hotkey_enabled, global_hotkey)?;
    let tray = app
        .tray_by_id(TRAY_ID)
        .ok_or_else(|| anyhow!("ambient tray icon is unavailable"))?;
    tray.set_tooltip(Some(tray_tooltip(
        connection_state,
        unread_notifications,
        pending_approvals,
        active_runs,
        queued_offline_drafts,
    )))
    .context("failed to update ambient tray tooltip")?;

    Ok(AmbientSyncOutcome { hotkey_registration_error })
}

fn ensure_surface_window<R: Runtime>(
    app: &AppHandle<R>,
    surface: DesktopCompanionSurfaceMode,
) -> Result<WebviewWindow<R>> {
    let label = surface_window_label(surface);
    if let Some(window) = app.get_webview_window(label) {
        return Ok(window);
    }

    let mut builder = WebviewWindowBuilder::new(
        app,
        label,
        WebviewUrl::App(surface_window_path(surface).into()),
    )
        .title(surface_window_title(surface))
        .visible(false)
        .focused(true)
        .skip_taskbar(matches!(
            surface,
            DesktopCompanionSurfaceMode::QuickPanel | DesktopCompanionSurfaceMode::VoiceOverlay
        ));
    if let Some(icon) = app.default_window_icon().cloned() {
        builder = builder.icon(icon).context("failed to apply ambient surface icon")?;
    }
    builder = match surface {
        DesktopCompanionSurfaceMode::Main => builder
            .center()
            .inner_size(1120.0, 760.0)
            .min_inner_size(900.0, 620.0),
        DesktopCompanionSurfaceMode::QuickPanel => builder
            .inner_size(440.0, 680.0)
            .min_inner_size(360.0, 520.0)
            .resizable(true)
            .decorations(true)
            .always_on_top(true),
        DesktopCompanionSurfaceMode::VoiceOverlay => builder
            .inner_size(520.0, 620.0)
            .min_inner_size(420.0, 520.0)
            .resizable(false)
            .decorations(false)
            .always_on_top(true),
    };
    builder.build().with_context(|| format!("failed to create ambient surface window '{label}'"))
}

fn hide_other_surfaces<R: Runtime>(
    app: &AppHandle<R>,
    active_surface: DesktopCompanionSurfaceMode,
) -> Result<()> {
    for surface in [
        DesktopCompanionSurfaceMode::Main,
        DesktopCompanionSurfaceMode::QuickPanel,
        DesktopCompanionSurfaceMode::VoiceOverlay,
    ] {
        if surface != active_surface {
            hide_window_if_present(app, surface_window_label(surface))?;
        }
    }
    Ok(())
}

fn hide_window_if_present<R: Runtime>(app: &AppHandle<R>, label: &str) -> Result<()> {
    if let Some(window) = app.get_webview_window(label) {
        window.hide().with_context(|| format!("failed to hide ambient window '{label}'"))?;
    }
    Ok(())
}

fn refresh_autostart<R: Runtime>(app: &AppHandle<R>, enabled: bool) -> Result<()> {
    let autolaunch = app.autolaunch();
    let current = autolaunch.is_enabled().unwrap_or(false);
    if current == enabled {
        return Ok(());
    }
    if enabled {
        autolaunch.enable().context("failed to enable ambient start-on-login")?;
    } else {
        autolaunch.disable().context("failed to disable ambient start-on-login")?;
    }
    Ok(())
}

fn refresh_global_hotkey<R: Runtime>(
    app: &AppHandle<R>,
    rollout: &DesktopCompanionRolloutState,
    enabled: bool,
    shortcut: &str,
) -> Result<Option<String>> {
    let manager = app.global_shortcut();
    manager
        .unregister_all()
        .context("failed to reset desktop companion global hotkeys")?;

    let trimmed = shortcut.trim();
    if !enabled || trimmed.is_empty() || !rollout.companion_shell_enabled || !rollout.ambient_companion_enabled {
        return Ok(None);
    }

    let registration = manager.on_shortcut(trimmed, |app, _shortcut, event: ShortcutEvent| {
        if event.state == tauri_plugin_global_shortcut::ShortcutState::Pressed {
            if let Err(error) = toggle_preferred_surface(app) {
                eprintln!("ambient global hotkey handling failed: {error}");
            }
        }
    });
    match registration {
        Ok(()) => Ok(None),
        Err(error) => Ok(Some(sanitize_log_line(
            format!(
            "desktop global hotkey '{}' could not be registered: {error}",
            trimmed
            )
            .as_str(),
        ))),
    }
}

fn surface_window_label(surface: DesktopCompanionSurfaceMode) -> &'static str {
    match surface {
        DesktopCompanionSurfaceMode::Main => WINDOW_MAIN,
        DesktopCompanionSurfaceMode::QuickPanel => WINDOW_QUICK_PANEL,
        DesktopCompanionSurfaceMode::VoiceOverlay => WINDOW_VOICE_OVERLAY,
    }
}

fn surface_window_title(surface: DesktopCompanionSurfaceMode) -> &'static str {
    match surface {
        DesktopCompanionSurfaceMode::Main => "Palyra Control Center",
        DesktopCompanionSurfaceMode::QuickPanel => "Palyra Quick Panel",
        DesktopCompanionSurfaceMode::VoiceOverlay => "Palyra Voice Overlay",
    }
}

fn surface_window_path(surface: DesktopCompanionSurfaceMode) -> &'static str {
    match surface {
        DesktopCompanionSurfaceMode::Main => "index.html",
        DesktopCompanionSurfaceMode::QuickPanel => "index.html?surface=quick-panel",
        DesktopCompanionSurfaceMode::VoiceOverlay => "index.html?surface=voice-overlay",
    }
}

fn surface_from_window_label(label: &str) -> Option<DesktopCompanionSurfaceMode> {
    match label {
        WINDOW_MAIN => Some(DesktopCompanionSurfaceMode::Main),
        WINDOW_QUICK_PANEL => Some(DesktopCompanionSurfaceMode::QuickPanel),
        WINDOW_VOICE_OVERLAY => Some(DesktopCompanionSurfaceMode::VoiceOverlay),
        _ => None,
    }
}

fn resolve_supported_surface(
    requested: DesktopCompanionSurfaceMode,
    rollout: &DesktopCompanionRolloutState,
    fallback: DesktopCompanionSurfaceMode,
) -> DesktopCompanionSurfaceMode {
    match requested {
        DesktopCompanionSurfaceMode::VoiceOverlay
            if rollout.companion_shell_enabled
                && rollout.ambient_companion_enabled
                && rollout.voice_capture_enabled
                && rollout.voice_overlay_enabled =>
        {
            DesktopCompanionSurfaceMode::VoiceOverlay
        }
        DesktopCompanionSurfaceMode::QuickPanel
            if rollout.companion_shell_enabled && rollout.ambient_companion_enabled =>
        {
            DesktopCompanionSurfaceMode::QuickPanel
        }
        DesktopCompanionSurfaceMode::Main => DesktopCompanionSurfaceMode::Main,
        _ if fallback != requested => resolve_supported_surface(fallback, rollout, DesktopCompanionSurfaceMode::Main),
        _ if rollout.companion_shell_enabled && rollout.ambient_companion_enabled => {
            DesktopCompanionSurfaceMode::QuickPanel
        }
        _ => DesktopCompanionSurfaceMode::Main,
    }
}

fn hotkey_suffix(global_hotkey: &str, enabled: bool) -> String {
    let trimmed = global_hotkey.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if enabled {
        format!(" ({trimmed})")
    } else {
        format!(" ({trimmed}, disabled)")
    }
}

fn section_label(section: super::DesktopCompanionSection) -> &'static str {
    match section {
        super::DesktopCompanionSection::Home => "Home",
        super::DesktopCompanionSection::Chat => "Chat",
        super::DesktopCompanionSection::Approvals => "Approvals",
        super::DesktopCompanionSection::Access => "Access",
        super::DesktopCompanionSection::Onboarding => "Onboarding",
    }
}

fn tray_tooltip(
    connection_state: &str,
    unread_notifications: usize,
    pending_approvals: usize,
    active_runs: usize,
    queued_offline_drafts: usize,
) -> String {
    let status = match connection_state {
        "connected" => "Connected",
        "reconnecting" => "Reconnecting",
        _ => "Offline",
    };
    format!(
        "Palyra desktop companion\nStatus: {status}\nUnread notifications: {unread_notifications}\nPending approvals: {pending_approvals}\nActive runs: {active_runs}\nOffline drafts: {queued_offline_drafts}"
    )
}

fn load_companion_state<R: Runtime>(app: &AppHandle<R>) -> Result<DesktopCompanionState> {
    with_supervisor(app, |supervisor| Ok(supervisor.persisted.active_companion().clone()))
}

fn persist_last_surface<R: Runtime>(
    app: &AppHandle<R>,
    surface: DesktopCompanionSurfaceMode,
) -> Result<()> {
    with_supervisor(app, |supervisor| {
        supervisor.persisted.active_companion_mut().set_last_surface(surface);
        supervisor.save_state_file()
    })
}

fn persist_hotkey_registration_error<R: Runtime>(
    app: &AppHandle<R>,
    message: Option<&str>,
) -> Result<()> {
    with_supervisor(app, |supervisor| {
        supervisor
            .persisted
            .active_companion_mut()
            .set_hotkey_registration_error(message);
        supervisor.save_state_file()
    })
}

fn with_supervisor<R: Runtime, T>(
    app: &AppHandle<R>,
    f: impl FnOnce(&mut ControlCenter) -> Result<T>,
) -> Result<T> {
    let state = app.state::<DesktopAppState>();
    let mut supervisor = state.supervisor.blocking_lock();
    f(&mut supervisor)
}

fn spawn_dashboard_handoff<R: Runtime>(app: AppHandle<R>) {
    tauri::async_runtime::spawn(async move {
        if let Err(error) = open_dashboard_from_tray(&app).await {
            eprintln!("ambient dashboard handoff failed: {error}");
        }
    });
}

async fn open_dashboard_from_tray<R: Runtime>(app: &AppHandle<R>) -> Result<()> {
    let (snapshot_inputs, dashboard_open_inputs) = {
        let state = app.state::<DesktopAppState>();
        let mut supervisor = state.supervisor.lock().await;
        (
            supervisor.capture_snapshot_inputs(),
            supervisor.capture_dashboard_open_inputs(),
        )
    };
    let snapshot = build_snapshot_from_inputs(snapshot_inputs).await?;
    if snapshot.quick_facts.dashboard_access_mode == "local"
        && snapshot.quick_facts.gateway_version.is_none()
    {
        return Err(anyhow!(
            "local runtime is not healthy yet; start or refresh Palyra before opening the dashboard"
        ));
    }
    let dashboard_url = build_dashboard_open_url(
        dashboard_open_inputs,
        snapshot.quick_facts.dashboard_url.as_str(),
        snapshot.quick_facts.dashboard_access_mode.as_str(),
    )
    .await?;
    let state = app.state::<DesktopAppState>();
    let mut supervisor = state.supervisor.lock().await;
    let _ = supervisor.open_dashboard(dashboard_url.as_str())?;
    let _ = supervisor.mark_dashboard_handoff_complete();
    Ok(())
}
