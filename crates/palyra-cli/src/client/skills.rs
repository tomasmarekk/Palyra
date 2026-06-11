//! Blocking HTTP helper for skill status actions on the daemon admin API.
//!
//! Wraps `POST /admin/v1/skills/{skill_id}/{action}` with the identity headers
//! the daemon expects; connection details fall back to env vars and defaults.

use crate::*;

/// Identity and connection inputs for a skills admin request.
#[derive(Debug, Clone)]
pub(crate) struct SkillsAdminRequestContext {
    pub url: Option<String>,
    pub token: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
}

/// Posts a skill status action (for example `quarantine` or `enable`) and
/// decodes the daemon's `SkillStatusResponse`.
///
/// # Errors
/// Returns an error when the HTTP client cannot be built, the request fails,
/// the daemon responds with a non-success status, or the payload fails to parse.
pub(crate) fn post_skill_status_action(
    skill_id: &str,
    action: &'static str,
    body: &SkillStatusRequestBody,
    context: SkillsAdminRequestContext,
    error_context: &'static str,
) -> Result<SkillStatusResponse> {
    let base_url = context
        .url
        .or_else(|| env::var("PALYRA_DAEMON_URL").ok())
        .unwrap_or_else(|| DEFAULT_DAEMON_URL.to_owned());
    let endpoint =
        format!("{}/admin/v1/skills/{skill_id}/{action}", base_url.trim_end_matches('/'));
    let token = context.token.or_else(|| env::var("PALYRA_ADMIN_TOKEN").ok());
    // Short timeout: skill status actions target the (usually local) daemon
    // admin endpoint, so an unreachable daemon should fail fast, not hang.
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let mut request = client
        .post(endpoint)
        .header("x-palyra-principal", context.principal)
        .header("x-palyra-device-id", context.device_id);
    if let Some(token) = token {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    if let Some(channel) = context.channel {
        request = request.header("x-palyra-channel", channel);
    }
    request
        .json(body)
        .send()
        .with_context(|| error_context.to_owned())?
        .error_for_status()
        .with_context(|| format!("{error_context} (daemon returned non-success status)"))?
        .json()
        .with_context(|| format!("{error_context} (failed to parse response payload)"))
}
