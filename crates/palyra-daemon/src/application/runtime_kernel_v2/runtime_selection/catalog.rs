//! Registry-validated tool catalog input sealed to one explicit catalog epoch.
//!
//! Visible tool names are derived from the exact verified snapshot; callers
//! cannot submit an independent name list alongside a catalog reference.

use std::collections::BTreeSet;

use serde::Serialize;
use serde_json::json;

use crate::application::tool_registry::{
    canonical_json_bytes, stable_hash_value, ModelVisibleToolCatalogSnapshot,
};

use super::{
    bounded::{BoundedVec, SafeLabel},
    digest::SelectionDigest,
    policies::MAX_REQUIRED_TOOLS,
    service::RuntimeSelectionError,
};

const TOOL_CATALOG_SNAPSHOT_DOMAIN: &[u8] = b"palyra.runtime_selection.tool_catalog_snapshot.v1\0";

/// Verified catalog snapshot reference and names derived from that exact snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SealedToolCatalogSelectionV1 {
    snapshot_id: SafeLabel,
    catalog_hash: SelectionDigest,
    snapshot_digest: SelectionDigest,
    catalog_epoch: u64,
    visible_tool_names: BoundedVec<SafeLabel, MAX_REQUIRED_TOOLS>,
}

impl SealedToolCatalogSelectionV1 {
    /// Verifies the live catalog contract and seals it to `catalog_epoch`.
    ///
    /// The hash payload mirrors the tool registry's canonical catalog contract.
    /// Snapshot id, catalog hash, counts, ordering, and full-snapshot digest are
    /// checked before visible names are derived.
    ///
    /// # Errors
    /// Returns [`RuntimeSelectionError::InvalidToolCatalog`] or
    /// [`RuntimeSelectionError::DigestMismatch`] when the snapshot is malformed.
    pub(crate) fn from_registry_snapshot(
        snapshot: &ModelVisibleToolCatalogSnapshot,
        catalog_epoch: u64,
    ) -> Result<Self, RuntimeSelectionError> {
        if catalog_epoch == 0
            || snapshot.direct_tool_count != snapshot.indexed_tools.len()
            || snapshot.exposed_tool_count != snapshot.tools.len()
        {
            return Err(RuntimeSelectionError::InvalidToolCatalog);
        }
        let expected_hash = stable_hash_value(&catalog_hash_payload(snapshot));
        if snapshot.catalog_hash != expected_hash {
            return Err(RuntimeSelectionError::DigestMismatch);
        }
        let expected_snapshot_id = expected_hash
            .get(..16)
            .map(|prefix| format!("toolcat_{prefix}"))
            .ok_or(RuntimeSelectionError::InvalidToolCatalog)?;
        if snapshot.snapshot_id != expected_snapshot_id {
            return Err(RuntimeSelectionError::DigestMismatch);
        }
        let snapshot_value =
            serde_json::to_value(snapshot).map_err(|_| RuntimeSelectionError::Serialization)?;
        let snapshot_digest = SelectionDigest::from_domain_bytes(
            TOOL_CATALOG_SNAPSHOT_DOMAIN,
            canonical_json_bytes(&snapshot_value).as_slice(),
        );
        let mut names = snapshot
            .tools
            .iter()
            .map(|tool| {
                SafeLabel::parse(tool.name.clone())
                    .map_err(|_| RuntimeSelectionError::InvalidToolCatalog)
            })
            .collect::<Result<Vec<_>, _>>()?;
        names.sort();
        if names.windows(2).any(|window| window[0] == window[1]) {
            return Err(RuntimeSelectionError::InvalidToolCatalog);
        }
        Ok(Self {
            snapshot_id: SafeLabel::parse(snapshot.snapshot_id.clone())
                .map_err(|_| RuntimeSelectionError::InvalidToolCatalog)?,
            catalog_hash: SelectionDigest::parse(expected_hash)
                .map_err(|_| RuntimeSelectionError::InvalidToolCatalog)?,
            snapshot_digest,
            catalog_epoch,
            visible_tool_names: BoundedVec::try_new(names)
                .map_err(|_| RuntimeSelectionError::InvalidToolCatalog)?,
        })
    }

    /// Returns the verified catalog hash.
    #[must_use]
    pub(crate) const fn catalog_hash(&self) -> &SelectionDigest {
        &self.catalog_hash
    }

    /// Returns the digest of the complete serialized catalog.
    #[must_use]
    pub(crate) const fn snapshot_digest(&self) -> &SelectionDigest {
        &self.snapshot_digest
    }

    /// Returns the explicit catalog epoch.
    #[must_use]
    pub(crate) const fn catalog_epoch(&self) -> u64 {
        self.catalog_epoch
    }

    pub(super) fn satisfies_tools(&self, required: &[SafeLabel]) -> bool {
        let available = self.visible_tool_names.iter().collect::<BTreeSet<_>>();
        required.iter().all(|tool| available.contains(tool))
    }

    #[cfg(test)]
    pub(crate) fn test_only(
        snapshot_id: SafeLabel,
        catalog_epoch: u64,
        mut visible_tool_names: Vec<SafeLabel>,
    ) -> Self {
        visible_tool_names.sort();
        let catalog_hash =
            SelectionDigest::from_domain_bytes(TOOL_CATALOG_SNAPSHOT_DOMAIN, b"test-catalog");
        let snapshot_digest =
            SelectionDigest::from_domain_bytes(TOOL_CATALOG_SNAPSHOT_DOMAIN, b"test-snapshot");
        Self {
            snapshot_id,
            catalog_hash,
            snapshot_digest,
            catalog_epoch,
            visible_tool_names: BoundedVec::try_new(visible_tool_names)
                .expect("bounded test catalog"),
        }
    }
}

/// Durable hash-only projection of [`SealedToolCatalogSelectionV1`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ToolCatalogSelectionProjectionV1 {
    pub(super) snapshot_id: SafeLabel,
    pub(super) catalog_hash: SelectionDigest,
    pub(super) snapshot_digest: SelectionDigest,
    pub(super) catalog_epoch: u64,
}

impl From<&SealedToolCatalogSelectionV1> for ToolCatalogSelectionProjectionV1 {
    fn from(value: &SealedToolCatalogSelectionV1) -> Self {
        Self {
            snapshot_id: value.snapshot_id.clone(),
            catalog_hash: value.catalog_hash.clone(),
            snapshot_digest: value.snapshot_digest.clone(),
            catalog_epoch: value.catalog_epoch,
        }
    }
}

fn catalog_hash_payload(snapshot: &ModelVisibleToolCatalogSnapshot) -> serde_json::Value {
    json!({
        "schema_version": snapshot.schema_version,
        "provider_dialect": snapshot.provider_dialect.as_str(),
        "provider_kind": snapshot.provider_kind,
        "provider_model_id": snapshot.provider_model_id,
        "surface": snapshot.surface.as_str(),
        "principal_hash": snapshot.principal_hash,
        "channel_hash": snapshot.channel_hash,
        "remaining_tool_budget": snapshot.remaining_tool_budget,
        "created_at_unix_ms": snapshot.created_at_unix_ms,
        "profile_expansion": snapshot.profile_expansion,
        "exposure_mode": snapshot.exposure_mode.as_str(),
        "compact_tool_threshold": snapshot.compact_tool_threshold,
        "direct_tool_count": snapshot.direct_tool_count,
        "exposed_tool_count": snapshot.exposed_tool_count,
        "estimated_direct_tool_bytes": snapshot.estimated_direct_tool_bytes,
        "estimated_exposed_tool_bytes": snapshot.estimated_exposed_tool_bytes,
        "estimated_saved_bytes": snapshot.estimated_saved_bytes,
        "availability_probes": snapshot
            .availability_probes
            .iter()
            .map(|probe| {
                json!({
                    "runtime": probe.runtime,
                    "status": probe.status,
                    "reason_code": probe.reason_code,
                    "cache_key_hash": probe.cache_key_hash,
                    "config_hash": probe.config_hash,
                    "grace_allowed": probe.grace_allowed,
                })
            })
            .collect::<Vec<_>>(),
        "index": snapshot.index,
        "indexed_tools": snapshot.indexed_tools,
        "tools": snapshot.tools,
        "filtered_tools": snapshot.filtered_tools,
    })
}
