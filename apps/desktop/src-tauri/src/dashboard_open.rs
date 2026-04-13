use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use reqwest::Client;

use super::snapshot::{
    build_control_plane_client, dashboard_redirect_path_from_url,
    ensure_console_session_with_cached_csrf, normalize_local_browser_handoff_url,
};
use super::supervisor::ConsoleSessionCache;
use super::{ControlCenter, RuntimeConfig};

#[derive(Debug, Clone)]
pub(crate) struct DashboardOpenInputs {
    pub(crate) runtime: RuntimeConfig,
    pub(crate) admin_token: String,
    pub(crate) http_client: Client,
    pub(crate) console_session_cache: Arc<Mutex<Option<ConsoleSessionCache>>>,
}

impl ControlCenter {
    pub(crate) fn capture_dashboard_open_inputs(&self) -> DashboardOpenInputs {
        DashboardOpenInputs {
            runtime: self.runtime.clone(),
            admin_token: self.admin_token.clone(),
            http_client: self.http_client.clone(),
            console_session_cache: Arc::clone(&self.console_session_cache),
        }
    }
}

pub(crate) async fn build_dashboard_open_url(
    inputs: DashboardOpenInputs,
    dashboard_url: &str,
    dashboard_access_mode: &str,
) -> Result<String> {
    if !dashboard_access_mode.eq_ignore_ascii_case("local") {
        return Ok(dashboard_url.to_owned());
    }

    let redirect_path = dashboard_redirect_path_from_url(dashboard_url)?;
    let mut control_plane =
        build_control_plane_client(inputs.http_client.clone(), &inputs.runtime)?;
    let _csrf_token = ensure_console_session_with_cached_csrf(
        &mut control_plane,
        inputs.admin_token.as_str(),
        inputs.console_session_cache.as_ref(),
    )
    .await?;
    let handoff = control_plane
        .create_browser_handoff(&palyra_control_plane::ConsoleBrowserHandoffRequest {
            redirect_path: Some(redirect_path),
        })
        .await
        .map_err(|error| anyhow!("browser handoff bootstrap failed: {error}"))?;
    normalize_local_browser_handoff_url(dashboard_url, handoff.handoff_url.as_str())
}
