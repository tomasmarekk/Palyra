//! Auth profile registry, credential descriptors, OAuth refresh, and provider health.
//!
//! Profiles never store secret material inline: every credential field holds a vault
//! reference (`scope/key`) resolved through `palyra-vault` at use time. The registry and
//! its runtime state persist as TOML under the daemon state root and are shared with the
//! daemon's model/provider orchestration via [`AuthProfileRegistry`].

mod constants;
mod error;
mod models;
mod refresh;
mod registry;
mod storage;
mod validation;

pub use error::AuthProfileError;
pub use models::{
    AuthCredential, AuthCredentialType, AuthExpiryDistribution, AuthFailureClass, AuthHealthReport,
    AuthHealthSummary, AuthProfileDoctorHint, AuthProfileDoctorSeverity, AuthProfileEligibility,
    AuthProfileFailureKind, AuthProfileHealthRecord, AuthProfileHealthState, AuthProfileListFilter,
    AuthProfileOrderRecord, AuthProfileRecord, AuthProfileRuntimeRecord, AuthProfileScope,
    AuthProfileSelectionCandidate, AuthProfileSelectionRequest, AuthProfileSelectionResult,
    AuthProfileSetRequest, AuthProfilesPage, AuthProvider, AuthProviderKind,
    AuthRunCredentialCandidateReport, AuthRunCredentialReport, AuthScopeFilter,
    AuthTokenExpiryState, CredentialAttemptBinding, CredentialSelectionReport, OAuthRefreshError,
    OAuthRefreshRequest, OAuthRefreshResponse, OAuthRefreshState,
};
pub use refresh::{
    compute_backoff_ms, provider_backoff_policy, HttpOAuthRefreshAdapter, OAuthRefreshAdapter,
    OAuthRefreshOutcome, OAuthRefreshOutcomeKind, ProviderBackoffPolicy,
};
pub use registry::AuthProfileRegistry;

#[cfg(test)]
pub(crate) use refresh::{load_secret_utf8, persist_secret_utf8};
#[cfg(test)]
pub(crate) use validation::{
    normalize_optional_text, normalize_token_endpoint,
    validate_runtime_token_endpoint_with_resolver,
};

#[cfg(test)]
mod tests;
