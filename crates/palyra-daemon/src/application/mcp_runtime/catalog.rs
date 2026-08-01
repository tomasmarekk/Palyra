//! Host-owned runtime generation and catalog epoch authority.

use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{
    McpRuntimeLifecycleState, McpRuntimeSupervisorError, McpServerCallbackRequest,
    McpServerRecordV2,
};

/// Immutable pin attached to broker preparation and actor requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpCatalogEpochPin {
    /// Durable server identity.
    pub server_id: String,
    /// Transport owner generation.
    pub runtime_generation: u64,
    /// Catalog schema epoch.
    pub catalog_epoch: u64,
    /// SHA-256 digest of the catalog represented by the epoch.
    pub catalog_digest: String,
    /// Durable record revision that established the pin.
    pub record_revision: u64,
}

impl McpCatalogEpochPin {
    /// Builds a pin only from a ready durable record.
    ///
    /// # Errors
    /// Returns [`McpCatalogAuthorityError`] when the record is not ready or its
    /// durable invariants are invalid.
    pub fn from_ready_record(record: &McpServerRecordV2) -> Result<Self, McpCatalogAuthorityError> {
        record.validate()?;
        if record.lifecycle != McpRuntimeLifecycleState::Ready {
            return Err(McpCatalogAuthorityError::NotReady);
        }
        let catalog_digest =
            record.catalog_digest.clone().ok_or(McpCatalogAuthorityError::NotReady)?;
        Ok(Self {
            server_id: record.server_id.clone(),
            runtime_generation: record.runtime_generation,
            catalog_epoch: record.catalog_epoch,
            catalog_digest,
            record_revision: record.revision,
        })
    }

    /// Validates a generation- and epoch-pinned broker request.
    ///
    /// # Errors
    /// Returns a stale authority error when any pin component differs.
    pub fn validate(
        &self,
        server_id: &str,
        runtime_generation: u64,
        catalog_epoch: u64,
    ) -> Result<(), McpCatalogAuthorityError> {
        if self.server_id != server_id {
            return Err(McpCatalogAuthorityError::ServerMismatch);
        }
        if self.runtime_generation != runtime_generation {
            return Err(McpCatalogAuthorityError::StaleGeneration {
                active: self.runtime_generation,
                observed: runtime_generation,
            });
        }
        if self.catalog_epoch != catalog_epoch {
            return Err(McpCatalogAuthorityError::StaleCatalogEpoch {
                active: self.catalog_epoch,
                observed: catalog_epoch,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct McpRuntimeAuthoritySnapshot {
    lifecycle: McpRuntimeLifecycleState,
    runtime_generation: u64,
    record_revision: u64,
    pin: Option<McpCatalogEpochPin>,
}

/// Shared authority updated synchronously after every durable actor commit.
pub struct McpCatalogAuthority {
    server_id: String,
    snapshot: RwLock<Option<McpRuntimeAuthoritySnapshot>>,
}

impl std::fmt::Debug for McpCatalogAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpCatalogAuthority")
            .field("server_id", &self.server_id)
            .field("ready", &self.pin().is_some())
            .finish_non_exhaustive()
    }
}

impl McpCatalogAuthority {
    /// Creates an empty authority for one durable server.
    ///
    /// # Errors
    /// Returns [`McpCatalogAuthorityError::InvalidServerId`] for malformed identity.
    pub fn new(server_id: String) -> Result<Self, McpCatalogAuthorityError> {
        if !valid_identifier(&server_id) {
            return Err(McpCatalogAuthorityError::InvalidServerId);
        }
        Ok(Self { server_id, snapshot: RwLock::new(None) })
    }

    /// Applies the latest already-committed durable record.
    ///
    /// Non-ready records retain generation authority for OAuth fencing while
    /// clearing catalog authority so broker calls and callbacks fail closed.
    ///
    /// # Errors
    /// Returns an identity, stale revision, lock, or record validation error.
    pub fn apply_committed(
        &self,
        record: &McpServerRecordV2,
    ) -> Result<(), McpCatalogAuthorityError> {
        record.validate()?;
        if record.server_id != self.server_id {
            return Err(McpCatalogAuthorityError::ServerMismatch);
        }
        let mut snapshot =
            self.snapshot.write().map_err(|_| McpCatalogAuthorityError::LockPoisoned)?;
        if snapshot.as_ref().is_some_and(|current| current.record_revision > record.revision) {
            return Err(McpCatalogAuthorityError::StaleRecordRevision);
        }
        let pin = (record.lifecycle == McpRuntimeLifecycleState::Ready)
            .then(|| McpCatalogEpochPin::from_ready_record(record))
            .transpose()?;
        *snapshot = Some(McpRuntimeAuthoritySnapshot {
            lifecycle: record.lifecycle,
            runtime_generation: record.runtime_generation,
            record_revision: record.revision,
            pin,
        });
        Ok(())
    }

    /// Returns the current ready catalog pin.
    #[must_use]
    pub fn pin(&self) -> Option<McpCatalogEpochPin> {
        self.snapshot.read().ok()?.as_ref()?.pin.clone()
    }

    /// Fences an OAuth refresh to the actor generation that requested it.
    ///
    /// # Errors
    /// Returns unavailable or stale generation when the durable actor state
    /// cannot authorize the request.
    pub fn validate_runtime_generation(
        &self,
        expected_runtime_generation: u64,
    ) -> Result<(), McpCatalogAuthorityError> {
        let snapshot = self.snapshot.read().map_err(|_| McpCatalogAuthorityError::LockPoisoned)?;
        let snapshot = snapshot.as_ref().ok_or(McpCatalogAuthorityError::Unavailable)?;
        if !matches!(
            snapshot.lifecycle,
            McpRuntimeLifecycleState::Handshaking
                | McpRuntimeLifecycleState::Ready
                | McpRuntimeLifecycleState::Reconnecting
        ) {
            return Err(McpCatalogAuthorityError::Unavailable);
        }
        if snapshot.runtime_generation != expected_runtime_generation {
            return Err(McpCatalogAuthorityError::StaleGeneration {
                active: snapshot.runtime_generation,
                observed: expected_runtime_generation,
            });
        }
        Ok(())
    }

    /// Fences an external callback to the current ready generation and epoch.
    ///
    /// # Errors
    /// Returns unavailable or stale authority for non-current callbacks.
    pub fn validate_callback(
        &self,
        request: &McpServerCallbackRequest,
    ) -> Result<(), McpCatalogAuthorityError> {
        let pin = self.pin().ok_or(McpCatalogAuthorityError::NotReady)?;
        pin.validate(&self.server_id, request.runtime_generation, request.catalog_epoch)
    }
}

/// Catalog or runtime-generation authority failure.
#[derive(Debug, Error)]
pub enum McpCatalogAuthorityError {
    /// Durable record failed its own invariants.
    #[error("invalid mcp durable record")]
    InvalidRecord(#[from] McpRuntimeSupervisorError),
    /// Server identity is malformed.
    #[error("invalid mcp server identity")]
    InvalidServerId,
    /// Record or request belongs to a different server.
    #[error("mcp catalog server mismatch")]
    ServerMismatch,
    /// Durable record is not ready to expose a catalog.
    #[error("mcp catalog is not ready")]
    NotReady,
    /// No active runtime generation can authorize the operation.
    #[error("mcp runtime generation is unavailable")]
    Unavailable,
    /// Observed runtime generation is stale.
    #[error("stale mcp runtime generation: active={active}, observed={observed}")]
    StaleGeneration {
        /// Current actor generation.
        active: u64,
        /// Request generation.
        observed: u64,
    },
    /// Observed catalog epoch is stale.
    #[error("stale mcp catalog epoch: active={active}, observed={observed}")]
    StaleCatalogEpoch {
        /// Current catalog epoch.
        active: u64,
        /// Request catalog epoch.
        observed: u64,
    },
    /// An older durable revision attempted to replace newer authority.
    #[error("stale mcp durable record revision")]
    StaleRecordRevision,
    /// In-memory authority lock was poisoned.
    #[error("mcp catalog authority lock poisoned")]
    LockPoisoned,
}

fn valid_identifier(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= 128
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | ':' | '/')
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::mcp_runtime::McpSessionTransportKind;

    fn ready_record() -> McpServerRecordV2 {
        let configured = McpServerRecordV2::configured(
            "server-a".to_owned(),
            McpSessionTransportKind::Stdio,
            None,
            "trusted-local".to_owned(),
            1_000,
        )
        .expect("configured record should validate");
        configured
            .begin_start(1_001)
            .expect("startup should validate")
            .begin_handshake(1_001)
            .expect("handshake should validate")
            .mark_ready("a".repeat(64), 1_002)
            .expect("ready state should validate")
    }

    #[test]
    fn catalog_pin_rejects_old_epoch_after_committed_change() {
        let authority =
            McpCatalogAuthority::new("server-a".to_owned()).expect("authority should validate");
        let ready = ready_record();
        authority.apply_committed(&ready).expect("ready record should apply");
        let old = authority.pin().expect("ready record should expose pin");
        let advanced =
            ready.advance_catalog(Some("b".repeat(64)), 1_003).expect("catalog should advance");
        authority.apply_committed(&advanced).expect("new epoch should apply");

        assert!(matches!(
            authority.pin().expect("new pin should exist").validate(
                &old.server_id,
                old.runtime_generation,
                old.catalog_epoch
            ),
            Err(McpCatalogAuthorityError::StaleCatalogEpoch { .. })
        ));
    }

    #[test]
    fn reconnect_clears_catalog_but_preserves_generation_fence() {
        let authority =
            McpCatalogAuthority::new("server-a".to_owned()).expect("authority should validate");
        let ready = ready_record();
        authority.apply_committed(&ready).expect("ready record should apply");
        let reconnecting = ready
            .mark_failure(
                &super::super::McpReconnectPolicy::default(),
                "mcp.runtime.test.disconnect",
                1_003,
            )
            .expect("failure should reconnect");
        authority.apply_committed(&reconnecting).expect("reconnecting record should apply");

        assert!(authority.pin().is_none());
        assert!(authority.validate_runtime_generation(reconnecting.runtime_generation).is_ok());
    }
}
