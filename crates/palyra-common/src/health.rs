//! Shared health-endpoint response shape for daemon and browserd services.
//!
//! Combines build metadata with uptime so every service reports identical health JSON.

use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::build::build_metadata;

/// JSON body returned by service health endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub service: String,
    pub status: String,
    pub version: String,
    pub git_hash: String,
    pub build_profile: String,
    pub uptime_seconds: u64,
}

/// Builds an "ok" health response for `service` using its process start instant.
#[must_use]
pub fn health_response(service: &'static str, started_at: Instant) -> HealthResponse {
    let metadata = build_metadata();
    HealthResponse {
        service: service.to_owned(),
        status: "ok".to_owned(),
        version: metadata.version.to_owned(),
        git_hash: metadata.git_hash.to_owned(),
        build_profile: metadata.build_profile.to_owned(),
        uptime_seconds: started_at.elapsed().as_secs(),
    }
}
