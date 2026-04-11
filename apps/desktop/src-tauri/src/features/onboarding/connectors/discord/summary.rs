use crate::snapshot::ControlCenterSnapshot;

use super::state::DesktopDiscordOnboardingState;

#[derive(Debug, Clone)]
pub(crate) struct DesktopDiscordOnboardingSummary {
    pub(crate) ready: bool,
    pub(crate) verified: bool,
    pub(crate) last_verified_target: Option<String>,
    pub(crate) last_verified_at_unix_ms: Option<i64>,
    pub(crate) defaults: DesktopDiscordOnboardingState,
}

pub(crate) fn derive_discord_onboarding_summary(
    snapshot: &ControlCenterSnapshot,
    defaults: &DesktopDiscordOnboardingState,
) -> DesktopDiscordOnboardingSummary {
    let ready = is_discord_ready(snapshot);
    let verified = ready
        && defaults.last_verified_at_unix_ms.is_some()
        && defaults
            .last_connector_id
            .as_deref()
            .is_some_and(|value| value == snapshot.quick_facts.discord.connector_id);

    DesktopDiscordOnboardingSummary {
        ready,
        verified,
        last_verified_target: defaults.last_verified_target.clone(),
        last_verified_at_unix_ms: defaults.last_verified_at_unix_ms,
        defaults: defaults.clone(),
    }
}

pub(crate) fn discord_connect_detail(summary: &DesktopDiscordOnboardingSummary) -> String {
    if summary.verified {
        return format!(
            "Discord verification last succeeded for {}.",
            summary
                .last_verified_target
                .as_deref()
                .unwrap_or("the configured target")
        );
    }
    if summary.ready {
        return "Discord connector is ready. Send the verification test to finish onboarding."
            .to_owned();
    }
    "Run Discord preflight, apply the connector config, and verify a test send.".to_owned()
}

fn is_discord_ready(snapshot: &ControlCenterSnapshot) -> bool {
    let discord = &snapshot.quick_facts.discord;
    discord.enabled
        && discord.authenticated
        && discord.readiness.eq_ignore_ascii_case("ready")
        && discord.liveness.eq_ignore_ascii_case("running")
}
