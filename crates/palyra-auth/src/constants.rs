//! Crate-wide constants: file names, schema versions, limits, and timing defaults.
//!
//! Timing windows and lock parameters here are policy knobs shared by the registry,
//! refresh scheduling, and health evaluation; change them only with matching test updates.

pub(crate) const REGISTRY_VERSION: u32 = 1;
pub(crate) const REGISTRY_FILE: &str = "auth_profiles.toml";
pub(crate) const RUNTIME_STATE_VERSION: u32 = 1;
pub(crate) const RUNTIME_STATE_FILE: &str = "auth_profile_runtime_state.toml";
pub(crate) const ENV_STATE_ROOT: &str = "PALYRA_STATE_ROOT";
pub(crate) const ENV_REGISTRY_PATH: &str = "PALYRA_AUTH_PROFILES_PATH";
pub(crate) const MAX_PROFILE_COUNT: usize = 2_048;
pub(crate) const MAX_PROFILE_PAGE_LIMIT: usize = 500;
/// Tokens expiring within this window are reported as `Expiring` in health reports.
pub(crate) const DEFAULT_EXPIRING_WINDOW_MS: i64 = 15 * 60 * 1_000;
/// Tokens expiring within this window are considered due for a proactive OAuth refresh.
pub(crate) const DEFAULT_REFRESH_WINDOW_MS: i64 = 5 * 60 * 1_000;
pub(crate) const DEFAULT_REFRESH_TIMEOUT_SECS: u64 = 10;
pub(crate) const REGISTRY_LOCK_MAX_ATTEMPTS: u32 = 40;
pub(crate) const REGISTRY_LOCK_RETRY_DELAY_MS: u64 = 25;
/// A lock file older than this is treated as leaked by a dead process and reclaimed.
pub(crate) const REGISTRY_LOCK_STALE_AFTER_SECS: u64 = 30;
