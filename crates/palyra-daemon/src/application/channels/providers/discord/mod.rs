mod lifecycle;
mod onboarding;

pub(crate) use lifecycle::{perform_discord_account_logout, perform_discord_account_remove};
pub(crate) use onboarding::{
    apply_discord_onboarding, build_discord_channel_permission_warnings,
    build_discord_inbound_monitor_warnings, build_discord_onboarding_preflight,
    discord_inbound_monitor_is_alive, load_discord_inbound_monitor_summary,
    normalize_optional_discord_channel_id, probe_discord_bot_identity,
};
#[cfg(test)]
pub(crate) use onboarding::{
    build_discord_onboarding_plan, build_discord_onboarding_security_defaults,
    finalize_discord_onboarding_plan, normalize_discord_token, summarize_discord_inbound_monitor,
};
