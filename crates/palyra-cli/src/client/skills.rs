//! Blocking HTTP helper for skill status actions on the daemon admin API.
//!
//! Wraps `POST /admin/v1/skills/{skill_id}/{action}` with the identity headers
//! the daemon expects, reusing the root connection resolver when available so
//! stored admin tokens stay bound to their configured daemon origin.

use crate::{
    app, env, Client, Result, SkillStatusRequestBody, SkillStatusResponse, DEFAULT_CHANNEL,
};
use anyhow::Context;

/// Identity and connection inputs for a skills admin request.
#[derive(Debug, Clone)]
pub(crate) struct SkillsAdminRequestContext {
    pub url: Option<String>,
    pub token: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedSkillsAdminRequestContext {
    base_url: String,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
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
    let context = resolve_skills_admin_request_context(context)?;
    let endpoint =
        format!("{}/admin/v1/skills/{skill_id}/{action}", context.base_url.trim_end_matches('/'));
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
    if let Some(token) = context.token {
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

fn resolve_skills_admin_request_context(
    context: SkillsAdminRequestContext,
) -> Result<ResolvedSkillsAdminRequestContext> {
    if let Some(root_context) = app::current_root_context() {
        let connection = root_context.resolve_http_connection(
            app::ConnectionOverrides {
                daemon_url: context.url,
                token: context.token,
                principal: normalize_default_override(
                    context.principal,
                    app::ConnectionDefaults::USER.principal,
                ),
                device_id: normalize_default_override(
                    context.device_id,
                    app::ConnectionDefaults::USER.device_id,
                ),
                channel: context.channel,
                ..Default::default()
            },
            app::ConnectionDefaults::USER,
        )?;
        return Ok(ResolvedSkillsAdminRequestContext {
            base_url: connection.base_url,
            token: connection.token,
            principal: connection.principal,
            device_id: connection.device_id,
            channel: Some(connection.channel),
        });
    }

    let explicit_url = normalized_text(context.url.as_deref()).is_some();
    let base_url = normalize_owned_text(context.url)
        .or_else(|| read_normalized_env_var("PALYRA_DAEMON_URL"))
        .unwrap_or_else(|| crate::DEFAULT_DAEMON_URL.to_owned());
    let explicit_token = normalize_owned_text(context.token);
    let token = match (explicit_token, explicit_url) {
        (Some(token), _) => Some(token),
        (None, true) => None,
        (None, false) => read_normalized_env_var("PALYRA_ADMIN_TOKEN"),
    };
    Ok(ResolvedSkillsAdminRequestContext {
        base_url,
        token,
        principal: context.principal,
        device_id: context.device_id,
        channel: context.channel.or_else(|| Some(DEFAULT_CHANNEL.to_owned())),
    })
}

fn normalize_default_override(value: String, default_value: &str) -> Option<String> {
    if value == default_value {
        None
    } else {
        normalize_owned_text(Some(value))
    }
}

fn read_normalized_env_var(name: &str) -> Option<String> {
    env::var(name).ok().and_then(|value| normalize_owned_text(Some(value)))
}

fn normalize_owned_text(value: Option<String>) -> Option<String> {
    value.map(|value| value.trim().to_owned()).filter(|value| !value.is_empty())
}

fn normalized_text(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::{resolve_skills_admin_request_context, SkillsAdminRequestContext};
    use crate::{
        app::{self, ExplicitConfigPathPolicy},
        args::RootOptions,
    };
    use std::{env, ffi::OsString, fs};
    use tempfile::tempdir;

    struct ScopedEnvVar {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl ScopedEnvVar {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = env::var_os(key);
            env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            match self.previous.as_ref() {
                Some(value) => env::set_var(self.key, value),
                None => env::remove_var(self.key),
            }
        }
    }

    fn request_context(url: Option<&str>, token: Option<&str>) -> SkillsAdminRequestContext {
        SkillsAdminRequestContext {
            url: url.map(ToOwned::to_owned),
            token: token.map(ToOwned::to_owned),
            principal: app::ConnectionDefaults::USER.principal.to_owned(),
            device_id: app::ConnectionDefaults::USER.device_id.to_owned(),
            channel: None,
        }
    }

    #[test]
    fn explicit_skills_url_does_not_reuse_ambient_admin_token_without_root_context() {
        let _guard = app::test_env_lock_for_tests().lock().expect("env lock");
        app::clear_root_context_for_tests();
        let _admin_token = ScopedEnvVar::set("PALYRA_ADMIN_TOKEN", "ambient-token");

        let context = resolve_skills_admin_request_context(request_context(
            Some("http://127.0.0.1:9999"),
            None,
        ))
        .expect("explicit URL context should resolve");

        assert_eq!(context.base_url, "http://127.0.0.1:9999");
        assert_eq!(context.token, None);
        app::clear_root_context_for_tests();
    }

    #[test]
    fn explicit_skills_token_follows_explicit_skills_url() {
        let _guard = app::test_env_lock_for_tests().lock().expect("env lock");
        app::clear_root_context_for_tests();
        let _admin_token = ScopedEnvVar::set("PALYRA_ADMIN_TOKEN", "ambient-token");

        let context = resolve_skills_admin_request_context(request_context(
            Some("http://127.0.0.1:9999"),
            Some("explicit-token"),
        ))
        .expect("explicit token context should resolve");

        assert_eq!(context.base_url, "http://127.0.0.1:9999");
        assert_eq!(context.token.as_deref(), Some("explicit-token"));
        app::clear_root_context_for_tests();
    }

    #[test]
    fn default_skills_endpoint_uses_ambient_admin_token_without_root_context() {
        let _guard = app::test_env_lock_for_tests().lock().expect("env lock");
        app::clear_root_context_for_tests();
        let _admin_token = ScopedEnvVar::set("PALYRA_ADMIN_TOKEN", "ambient-token");

        let context = resolve_skills_admin_request_context(request_context(None, None))
            .expect("default endpoint context should resolve");

        assert_eq!(context.token.as_deref(), Some("ambient-token"));
        app::clear_root_context_for_tests();
    }

    #[test]
    fn explicit_skills_url_uses_root_origin_bound_token_resolution() -> anyhow::Result<()> {
        let _guard = app::test_env_lock_for_tests().lock().expect("env lock");
        app::clear_root_context_for_tests();
        let _admin_token = ScopedEnvVar::set("PALYRA_ADMIN_TOKEN", "ambient-token");
        let temp = tempdir()?;
        let state_root = temp.path().join("state");
        let config_path = temp.path().join("palyra.toml");
        fs::write(
            &config_path,
            r#"
[daemon]
bind_addr = "127.0.0.1"
port = 7142

[admin]
auth_token = "config-token"
"#,
        )?;

        app::install_root_context_with_policy(
            RootOptions {
                config_path: Some(config_path.display().to_string()),
                state_root: Some(state_root.display().to_string()),
                ..RootOptions::default()
            },
            ExplicitConfigPathPolicy::RequireExisting,
        )?;

        let untrusted = resolve_skills_admin_request_context(request_context(
            Some("http://127.0.0.1:9999"),
            None,
        ))?;
        assert_eq!(untrusted.token, None);

        let same_origin = resolve_skills_admin_request_context(request_context(
            Some("http://127.0.0.1:7142/admin"),
            None,
        ))?;
        assert_eq!(same_origin.token.as_deref(), Some("config-token"));

        app::clear_root_context_for_tests();
        Ok(())
    }
}
