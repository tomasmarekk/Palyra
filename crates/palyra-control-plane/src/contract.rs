//! Contract versioning and pagination primitives shared by every envelope.
//!
//! Each response envelope embeds a [`ContractDescriptor`] so consumers can
//! detect contract drift before interpreting the payload.

use serde::{Deserialize, Serialize};

/// Current control-plane contract version stamped into every response envelope.
pub const CONTROL_PLANE_CONTRACT_VERSION: &str = "control-plane.v1";

/// Pagination metadata attached to list envelopes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageInfo {
    /// Maximum number of items the daemon was asked to return.
    pub limit: usize,
    /// Number of items actually returned in this page.
    pub returned: usize,
    /// Opaque cursor for fetching the next page, when one exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Whether more items exist beyond this page.
    pub has_more: bool,
}

/// Contract version stamp carried by every response envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractDescriptor {
    /// Contract version string, e.g. [`CONTROL_PLANE_CONTRACT_VERSION`].
    pub contract_version: String,
}
