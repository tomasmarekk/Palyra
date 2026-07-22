//! Host-sealed immutable component-health snapshots.

use std::fmt;

#[cfg(test)]
use palyra_common::runtime_contracts::{CircuitBreakerPolicy, RuntimeGeneration};
use palyra_common::runtime_contracts::{
    RuntimeAuthorityClass, RuntimeComponentHealthV1, RuntimeHealthState, RuntimeInstanceId,
};
use serde::{Deserialize, Serialize};

use super::{
    bounded::{BoundedVec, SafeLabel},
    candidates::RuntimeHealthAuthoritySourceV1,
    digest::{digest_serializable, SelectionDigest},
    service::RuntimeSelectionError,
};

const MAX_HEALTH_RECORDS: usize = 96;
const MAX_HOST_RESIDENT_READINESS_RECORDS: usize = 16;
const HEALTH_SNAPSHOT_DOMAIN: &[u8] = b"palyra.runtime_selection.health_snapshot.v1\0";

/// Single-use evidence that records came from the host health registry.
pub(crate) struct HostHealthSnapshotProof {
    registry_epoch: u64,
    _private: (),
}

impl fmt::Debug for HostHealthSnapshotProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HostHealthSnapshotProof")
            .field("registry_epoch", &self.registry_epoch)
            .field("host_capability", &"[redacted]")
            .finish()
    }
}

impl HostHealthSnapshotProof {
    /// Creates host evidence after the health registry has read one atomic epoch.
    pub(in crate::application::runtime_kernel_v2) fn from_verified_registry(
        registry_epoch: u64,
    ) -> Result<Self, RuntimeSelectionError> {
        if registry_epoch == 0 {
            return Err(RuntimeSelectionError::InvalidHealthSnapshot);
        }
        Ok(Self { registry_epoch, _private: () })
    }

    #[cfg(test)]
    pub(crate) fn test_only(registry_epoch: u64) -> Self {
        Self { registry_epoch, _private: () }
    }
}

/// Single-use proof that resident descriptors were read at one real host epoch.
pub(crate) struct HostResidentReadinessProof {
    readiness_epoch: u64,
    _private: (),
}

impl fmt::Debug for HostResidentReadinessProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HostResidentReadinessProof")
            .field("readiness_epoch", &self.readiness_epoch)
            .field("host_capability", &"[redacted]")
            .finish()
    }
}

impl HostResidentReadinessProof {
    pub(in crate::application::runtime_kernel_v2) fn from_gateway_epoch(
        readiness_epoch: u64,
    ) -> Result<Self, RuntimeSelectionError> {
        if readiness_epoch == 0 {
            return Err(RuntimeSelectionError::InvalidHealthSnapshot);
        }
        Ok(Self { readiness_epoch, _private: () })
    }

    #[cfg(test)]
    pub(crate) fn test_only(readiness_epoch: u64) -> Self {
        Self { readiness_epoch, _private: () }
    }
}

/// Readiness evidence for code resident in this daemon process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostResidentReadinessV1 {
    component_id: RuntimeInstanceId,
    readiness_epoch: u64,
    descriptor_digest: SelectionDigest,
    authority_class: RuntimeAuthorityClass,
    ready: bool,
    reason_code: SafeLabel,
    updated_at_unix_ms: i64,
}

impl HostResidentReadinessV1 {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::application::runtime_kernel_v2) fn new(
        component_id: RuntimeInstanceId,
        readiness_epoch: u64,
        descriptor_digest: SelectionDigest,
        authority_class: RuntimeAuthorityClass,
        ready: bool,
        reason_code: SafeLabel,
        updated_at_unix_ms: i64,
    ) -> Result<Self, RuntimeSelectionError> {
        if readiness_epoch == 0 || updated_at_unix_ms < 0 {
            return Err(RuntimeSelectionError::InvalidHealthSnapshot);
        }
        Ok(Self {
            component_id,
            readiness_epoch,
            descriptor_digest,
            authority_class,
            ready,
            reason_code,
            updated_at_unix_ms,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_only(
        component_id: RuntimeInstanceId,
        readiness_epoch: u64,
        descriptor_digest: SelectionDigest,
        authority_class: RuntimeAuthorityClass,
        ready: bool,
        reason_code: SafeLabel,
        updated_at_unix_ms: i64,
    ) -> Self {
        Self {
            component_id,
            readiness_epoch,
            descriptor_digest,
            authority_class,
            ready,
            reason_code,
            updated_at_unix_ms,
        }
    }
}

/// Bounded wire representation used when loading journaled health evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg(test)]
pub(super) struct HealthSnapshotWireV1 {
    observed_at_unix_ms: i64,
    registry_epoch: u64,
    records: BoundedVec<BoundedHealthRecordWireV1, MAX_HEALTH_RECORDS>,
    snapshot_digest: SelectionDigest,
}

/// Wire health record with a streaming-bounded reason code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg(test)]
struct BoundedHealthRecordWireV1 {
    schema_version: u32,
    component_id: RuntimeInstanceId,
    generation: RuntimeGeneration,
    state: RuntimeHealthState,
    authority_class: RuntimeAuthorityClass,
    strike_count: u32,
    reason_code: SafeLabel,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    first_failure_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_failure_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expires_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fallback_component_id: Option<RuntimeInstanceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fallback_authority_class: Option<RuntimeAuthorityClass>,
    security_quarantine: bool,
    policy: CircuitBreakerPolicy,
    updated_at_unix_ms: i64,
}

#[derive(Serialize)]
struct HealthSnapshotPayload<'a> {
    observed_at_unix_ms: i64,
    registry_epoch: u64,
    records: &'a [RuntimeComponentHealthV1],
    host_resident_readiness_epoch: Option<u64>,
    host_resident_readiness: &'a [HostResidentReadinessV1],
}

/// Immutable health evidence sealed by consumed managed and resident capabilities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImmutableHealthSnapshotV1 {
    observed_at_unix_ms: i64,
    registry_epoch: u64,
    records: BoundedVec<RuntimeComponentHealthV1, MAX_HEALTH_RECORDS>,
    host_resident_readiness_epoch: Option<u64>,
    host_resident_readiness:
        BoundedVec<HostResidentReadinessV1, MAX_HOST_RESIDENT_READINESS_RECORDS>,
    snapshot_digest: SelectionDigest,
}

impl ImmutableHealthSnapshotV1 {
    /// Seals one atomic managed-health registry read.
    #[cfg(test)]
    pub(crate) fn capture(
        proof: HostHealthSnapshotProof,
        observed_at_unix_ms: i64,
        records: Vec<RuntimeComponentHealthV1>,
    ) -> Result<Self, RuntimeSelectionError> {
        Self::capture_inner(proof, None, observed_at_unix_ms, records, Vec::new())
    }

    /// Seals managed health and separately typed host-resident readiness.
    pub(crate) fn capture_with_host_resident(
        proof: HostHealthSnapshotProof,
        host_proof: HostResidentReadinessProof,
        observed_at_unix_ms: i64,
        records: Vec<RuntimeComponentHealthV1>,
        host_resident_readiness: Vec<HostResidentReadinessV1>,
    ) -> Result<Self, RuntimeSelectionError> {
        Self::capture_inner(
            proof,
            Some(host_proof),
            observed_at_unix_ms,
            records,
            host_resident_readiness,
        )
    }

    fn capture_inner(
        proof: HostHealthSnapshotProof,
        host_proof: Option<HostResidentReadinessProof>,
        observed_at_unix_ms: i64,
        mut records: Vec<RuntimeComponentHealthV1>,
        mut host_resident_readiness: Vec<HostResidentReadinessV1>,
    ) -> Result<Self, RuntimeSelectionError> {
        if observed_at_unix_ms < 0 || records.is_empty() {
            return Err(RuntimeSelectionError::InvalidHealthSnapshot);
        }
        records.sort_by(|left, right| left.component_id.cmp(&right.component_id));
        if records.windows(2).any(|window| window[0].component_id == window[1].component_id)
            || records.iter().any(|record| {
                record.validate().is_err() || record.updated_at_unix_ms > observed_at_unix_ms
            })
        {
            return Err(RuntimeSelectionError::InvalidHealthSnapshot);
        }
        host_resident_readiness.sort_by(|left, right| left.component_id.cmp(&right.component_id));
        let host_resident_readiness_epoch = host_proof.as_ref().map(|proof| proof.readiness_epoch);
        if host_resident_readiness.is_empty() != host_resident_readiness_epoch.is_none()
            || host_resident_readiness
                .windows(2)
                .any(|window| window[0].component_id >= window[1].component_id)
            || host_resident_readiness.iter().any(|record| {
                Some(record.readiness_epoch) != host_resident_readiness_epoch
                    || record.updated_at_unix_ms > observed_at_unix_ms
                    || records.iter().any(|managed| managed.component_id == record.component_id)
            })
        {
            return Err(RuntimeSelectionError::InvalidHealthSnapshot);
        }
        let records = BoundedVec::try_new(records)
            .map_err(|_| RuntimeSelectionError::InvalidHealthSnapshot)?;
        let host_resident_readiness = BoundedVec::try_new(host_resident_readiness)
            .map_err(|_| RuntimeSelectionError::InvalidHealthSnapshot)?;
        let snapshot_digest = digest_serializable(
            HEALTH_SNAPSHOT_DOMAIN,
            &HealthSnapshotPayload {
                observed_at_unix_ms,
                registry_epoch: proof.registry_epoch,
                records: &records,
                host_resident_readiness_epoch,
                host_resident_readiness: &host_resident_readiness,
            },
        )?;
        Ok(Self {
            observed_at_unix_ms,
            registry_epoch: proof.registry_epoch,
            records,
            host_resident_readiness_epoch,
            host_resident_readiness,
            snapshot_digest,
        })
    }

    #[must_use]
    pub(crate) const fn digest(&self) -> &SelectionDigest {
        &self.snapshot_digest
    }

    #[must_use]
    pub(crate) const fn registry_epoch(&self) -> u64 {
        self.registry_epoch
    }

    #[must_use]
    pub(crate) fn record(
        &self,
        component_id: &RuntimeInstanceId,
    ) -> Option<&RuntimeComponentHealthV1> {
        self.records.iter().find(|record| &record.component_id == component_id)
    }

    pub(super) fn validate_for_selection(&self) -> Result<(), RuntimeSelectionError> {
        self.validate()
    }

    pub(super) fn is_available(
        &self,
        source: RuntimeHealthAuthoritySourceV1,
        component_id: &RuntimeInstanceId,
    ) -> bool {
        match source {
            RuntimeHealthAuthoritySourceV1::Managed => {
                self.record(component_id).is_some_and(|record| {
                    matches!(
                        record.state,
                        RuntimeHealthState::Healthy | RuntimeHealthState::Degraded
                    )
                })
            }
            RuntimeHealthAuthoritySourceV1::HostResident => self
                .host_resident_readiness
                .iter()
                .find(|record| &record.component_id == component_id)
                .is_some_and(|record| record.ready),
        }
    }

    pub(super) fn has_evidence(
        &self,
        source: RuntimeHealthAuthoritySourceV1,
        component_id: &RuntimeInstanceId,
    ) -> bool {
        match source {
            RuntimeHealthAuthoritySourceV1::Managed => self.record(component_id).is_some(),
            RuntimeHealthAuthoritySourceV1::HostResident => self
                .host_resident_readiness
                .iter()
                .any(|record| &record.component_id == component_id),
        }
    }

    fn validate(&self) -> Result<(), RuntimeSelectionError> {
        if self.registry_epoch == 0
            || self.observed_at_unix_ms < 0
            || self.records.is_empty()
            || self
                .records
                .windows(2)
                .any(|window| window[0].component_id >= window[1].component_id)
            || self.host_resident_readiness.is_empty()
                != self.host_resident_readiness_epoch.is_none()
            || self
                .host_resident_readiness
                .windows(2)
                .any(|window| window[0].component_id >= window[1].component_id)
            || self.records.iter().any(|record| {
                record.validate().is_err() || record.updated_at_unix_ms > self.observed_at_unix_ms
            })
            || self.host_resident_readiness.iter().any(|record| {
                Some(record.readiness_epoch) != self.host_resident_readiness_epoch
                    || record.updated_at_unix_ms > self.observed_at_unix_ms
                    || self
                        .records
                        .iter()
                        .any(|managed| managed.component_id == record.component_id)
            })
        {
            return Err(RuntimeSelectionError::InvalidHealthSnapshot);
        }
        let expected = digest_serializable(
            HEALTH_SNAPSHOT_DOMAIN,
            &HealthSnapshotPayload {
                observed_at_unix_ms: self.observed_at_unix_ms,
                registry_epoch: self.registry_epoch,
                records: &self.records,
                host_resident_readiness_epoch: self.host_resident_readiness_epoch,
                host_resident_readiness: &self.host_resident_readiness,
            },
        )?;
        if expected != self.snapshot_digest {
            return Err(RuntimeSelectionError::DigestMismatch);
        }
        Ok(())
    }
}
