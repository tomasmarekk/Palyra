//! Typed contract and async client for the Palyra `/console/v1` control plane.
//!
//! The serde shapes exported here are the wire protocol shared by the daemon,
//! operator CLI, web console, and desktop app: serialized forms must stay
//! byte-compatible across all surfaces. [`ControlPlaneClient`] is the canonical
//! HTTP client over that contract.

mod client;
mod contract;
mod errors;
mod models;
mod ndjson;
mod transport;

pub use client::{ControlPlaneClient, ControlPlaneClientConfig};
pub use contract::{ContractDescriptor, PageInfo, CONTROL_PLANE_CONTRACT_VERSION};
pub use errors::{ControlPlaneClientError, ErrorCategory, ErrorEnvelope, ValidationIssue};
pub use models::*;
pub use ndjson::{ControlPlaneNdjsonStream, NdjsonStreamLimits};

#[cfg(test)]
mod tests;
