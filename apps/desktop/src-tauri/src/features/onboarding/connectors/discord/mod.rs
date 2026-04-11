pub(crate) mod client;
pub(crate) mod state;
pub(crate) mod summary;

pub(crate) use client::{
    apply_discord_onboarding, run_discord_onboarding_preflight, verify_discord_connector,
    DiscordControlPlaneInputs, DiscordOnboardingApplySnapshot, DiscordOnboardingPreflightSnapshot,
    DiscordOnboardingRequest, DiscordVerificationRequest, DiscordVerificationResult,
};
pub(crate) use state::DesktopDiscordOnboardingState;
pub(crate) use summary::{
    derive_discord_onboarding_summary, discord_connect_detail, DesktopDiscordOnboardingSummary,
};
