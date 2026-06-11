//! Compile-time build metadata (version, git hash, profile) for health and version
//! reporting. The git hash is injected by the crate build script via `PALYRA_GIT_HASH`.

use serde::Serialize;

/// Build identity embedded into every binary at compile time.
#[derive(Debug, Clone, Serialize)]
pub struct BuildMetadata {
    pub version: &'static str,
    pub git_hash: &'static str,
    pub build_profile: &'static str,
}

/// Returns this build's metadata; `git_hash` is `"unknown"` outside a git checkout.
#[must_use]
pub fn build_metadata() -> BuildMetadata {
    BuildMetadata {
        version: env!("CARGO_PKG_VERSION"),
        git_hash: option_env!("PALYRA_GIT_HASH").unwrap_or("unknown"),
        build_profile: if cfg!(debug_assertions) { "debug" } else { "release" },
    }
}
