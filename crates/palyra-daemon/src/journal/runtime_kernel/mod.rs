//! Durable journal authority for RuntimeKernelV2 snapshots and transitions.
//!
//! This module owns one compare-and-set head per Run and an immutable full-history
//! ledger while reusing the shared canonical runtime-event transaction boundary.

mod rollback;
#[cfg(test)]
mod rollback_tests;
#[cfg(test)]
mod tests;

pub(super) use rollback::{ensure_runtime_rollback_allows_new_side_effect_tx, MIGRATION_76_SQL};
pub(crate) use rollback::{RuntimeRollbackActuationReportV1, RuntimeRollbackBoundaryOutcome};

use palyra_common::runtime_contracts::{
    GenerationLeaseV1, RuntimeEventEnvelopeV2, RuntimeGeneration, RuntimeGenerationLane,
    RuntimeGenerationTransitionKind, RuntimeIdentitySetV1,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde::{de::DeserializeOwned, Serialize};

use crate::application::runtime_kernel_v2::{
    KernelLaneAuthoritySet, KernelStateSnapshot, KernelTransition, PreparedKernelTransition,
    RuntimeKernelV2, RuntimeKernelVersion, TransitionOutcome,
};

use super::{
    current_unix_ms, sha256_hex,
    shared_runtime::{
        activate_or_refresh_generation_tx, active_runtime_generation_for_session_lane_tx,
        active_runtime_generation_tx, append_runtime_event_tx,
        current_provider_configuration_epoch_tx, provider_generation_owner,
        RuntimeEventAppendOutcome, RuntimeEventAppendRequest, RuntimeProviderLaneAuthority,
    },
    JournalError, JournalStore,
};

const RUNTIME_KERNEL_ROW_SCHEMA_VERSION: i64 = 1;
const MAX_RUNTIME_KERNEL_CHILD_OWNER_BYTES: usize = 96;

/// Host request for one phase-lazy child lane under an exact active Run lease.
#[derive(Debug, Clone)]
pub(crate) struct RuntimeKernelChildLaneAcquireRequest {
    identities: RuntimeIdentitySetV1,
    run_lease: GenerationLeaseV1,
    lane: RuntimeGenerationLane,
    owner: String,
}

/// Host request for the Provider lane under one exact V2 Run lease and config epoch.
#[derive(Debug, Clone)]
pub(crate) struct RuntimeKernelProviderLaneAcquireRequest {
    identities: RuntimeIdentitySetV1,
    run_lease: GenerationLeaseV1,
    expected_configuration_epoch: RuntimeGeneration,
}

impl RuntimeKernelProviderLaneAcquireRequest {
    /// Creates a request that the journal will bind to current provider config.
    #[must_use]
    pub(crate) fn new(
        identities: RuntimeIdentitySetV1,
        run_lease: GenerationLeaseV1,
        expected_configuration_epoch: RuntimeGeneration,
    ) -> Self {
        Self { identities, run_lease, expected_configuration_epoch }
    }
}

impl RuntimeKernelChildLaneAcquireRequest {
    /// Creates a request that the journal will revalidate against live authority.
    #[must_use]
    pub(crate) fn new(
        identities: RuntimeIdentitySetV1,
        run_lease: GenerationLeaseV1,
        lane: RuntimeGenerationLane,
        owner: String,
    ) -> Self {
        Self { identities, run_lease, lane, owner }
    }
}

/// Migration 72: authoritative RuntimeKernelV2 head and append-only transition history.
pub(super) const MIGRATION_72_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_kernel_heads (
        run_ulid TEXT PRIMARY KEY,
        session_ulid TEXT NOT NULL,
        runtime_version TEXT NOT NULL,
        run_generation INTEGER NOT NULL CHECK (run_generation > 0),
        revision INTEGER NOT NULL CHECK (revision >= 0),
        snapshot_json TEXT NOT NULL,
        snapshot_sha256 TEXT NOT NULL,
        initialized_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid)
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_kernel_heads_session_run
        ON runtime_kernel_heads(session_ulid, run_ulid);

    CREATE TABLE IF NOT EXISTS runtime_kernel_transition_ledger (
        ledger_index INTEGER PRIMARY KEY AUTOINCREMENT,
        run_ulid TEXT NOT NULL,
        session_ulid TEXT NOT NULL,
        runtime_version TEXT NOT NULL,
        run_generation INTEGER NOT NULL CHECK (run_generation > 0),
        revision INTEGER NOT NULL CHECK (revision > 0),
        idempotency_key TEXT NOT NULL,
        request_sha256 TEXT NOT NULL,
        observation_sha256 TEXT NOT NULL,
        event_ulid TEXT NOT NULL,
        event_sequence INTEGER NOT NULL CHECK (event_sequence >= 0),
        previous_snapshot_json TEXT NOT NULL,
        previous_snapshot_sha256 TEXT NOT NULL,
        next_snapshot_json TEXT NOT NULL,
        next_snapshot_sha256 TEXT NOT NULL,
        prepared_transition_json TEXT NOT NULL,
        prepared_transition_sha256 TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        UNIQUE(run_ulid, revision),
        UNIQUE(run_ulid, idempotency_key),
        UNIQUE(event_ulid),
        FOREIGN KEY(run_ulid) REFERENCES runtime_kernel_heads(run_ulid),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
        FOREIGN KEY(event_ulid) REFERENCES runtime_events_v2(event_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_kernel_ledger_run_index
        ON runtime_kernel_transition_ledger(run_ulid, ledger_index ASC);

    CREATE TRIGGER IF NOT EXISTS trg_runtime_kernel_heads_validate_update
    BEFORE UPDATE ON runtime_kernel_heads
    WHEN NEW.run_ulid != OLD.run_ulid
      OR NEW.session_ulid != OLD.session_ulid
      OR NEW.runtime_version != OLD.runtime_version
      OR NEW.run_generation != OLD.run_generation
      OR NEW.initialized_at_unix_ms != OLD.initialized_at_unix_ms
      OR NEW.schema_version != OLD.schema_version
      OR NEW.revision != OLD.revision + 1
    BEGIN
        SELECT RAISE(ABORT, 'runtime_kernel_heads update violates immutable identity or CAS revision');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_kernel_heads_prevent_delete
    BEFORE DELETE ON runtime_kernel_heads BEGIN
        SELECT RAISE(ABORT, 'runtime_kernel_heads cannot be deleted');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_kernel_ledger_prevent_update
    BEFORE UPDATE ON runtime_kernel_transition_ledger BEGIN
        SELECT RAISE(ABORT, 'runtime_kernel_transition_ledger is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_kernel_ledger_prevent_delete
    BEFORE DELETE ON runtime_kernel_transition_ledger BEGIN
        SELECT RAISE(ABORT, 'runtime_kernel_transition_ledger is append-only');
    END;
"#;

/// The current durable RuntimeKernelV2 head for one immutable Run generation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RuntimeKernelHeadRecord {
    /// Monotonic compare-and-set revision, starting at zero for initialization.
    pub(crate) revision: u64,
    /// Validated authoritative snapshot at this revision.
    pub(crate) snapshot: KernelStateSnapshot,
    /// SHA-256 of the exact canonical snapshot JSON stored in SQLite.
    pub(crate) snapshot_sha256: String,
}

/// One immutable transition-ledger row returned to focused journal tests.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RuntimeKernelTransitionLedgerRecord {
    /// Database-wide monotonic ledger position.
    pub(crate) ledger_index: u64,
    /// Per-run compare-and-set revision produced by this transition.
    pub(crate) revision: u64,
    /// Caller-provided bounded idempotency identity.
    pub(crate) idempotency_key: String,
    /// Kernel-computed canonical request digest.
    pub(crate) request_sha256: String,
    /// Sequence- and stamp-neutral semantic observation digest.
    pub(crate) observation_sha256: String,
    /// Canonical runtime event identity reused by the shared event store.
    pub(crate) event_id: String,
    /// Sequence accepted by the shared runtime-event store.
    pub(crate) event_sequence: u64,
    /// Full validated snapshot before the transition.
    pub(crate) previous_snapshot: KernelStateSnapshot,
    /// Full validated snapshot after the transition.
    pub(crate) next_snapshot: KernelStateSnapshot,
}

/// Durable disposition of one prepared RuntimeKernelV2 transition.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RuntimeKernelTransitionCommitOutcome {
    /// The event, ledger row, and new head were committed in one transaction.
    Applied {
        /// New per-run head revision.
        revision: u64,
        /// Immutable ledger position for this transition.
        ledger_index: u64,
        /// Canonical runtime-event sequence.
        event_sequence: u64,
        /// Newly authoritative snapshot.
        snapshot: KernelStateSnapshot,
    },
    /// The same `(run, idempotency_key, request_sha256)` already committed.
    AlreadyApplied {
        /// Original per-run head revision produced by the request.
        revision: u64,
        /// Original immutable ledger position.
        ledger_index: u64,
        /// Original canonical runtime-event sequence.
        event_sequence: u64,
        /// Snapshot originally produced by the request.
        snapshot: KernelStateSnapshot,
    },
    /// The prepared Run generation no longer owns active write authority.
    StaleSuppressed {
        /// Active Run generation, or `None` if the Run lane has no live authority.
        active_generation: Option<RuntimeGeneration>,
    },
}

/// Sequence-neutral observation committed by the journal under canonical lane ordering.
#[derive(Debug, Clone)]
pub(crate) struct RuntimeKernelObservationCommitRequest {
    /// Exact durable head the caller observed before entering the transaction.
    pub(crate) expected_snapshot: KernelStateSnapshot,
    /// Run generation pinned by the harness attempt.
    pub(crate) expected_run_generation: RuntimeGeneration,
    /// Host-issued lane leases that must still be current at commit.
    pub(crate) lane_authority: KernelLaneAuthoritySet,
    /// Bounded identity for full-history retry resolution.
    pub(crate) idempotency_key: String,
    /// Canonical event fields except for the journal-owned lane sequence.
    pub(crate) event_template: RuntimeEventEnvelopeV2,
    /// Pure-kernel transition paired with the observation.
    pub(crate) transition: KernelTransition,
}

#[derive(Debug)]
struct StoredKernelLedgerEntry {
    ledger_index: u64,
    revision: u64,
    session_id: String,
    runtime_version: String,
    run_generation: RuntimeGeneration,
    idempotency_key: String,
    request_sha256: String,
    observation_sha256: String,
    event_id: String,
    event_sequence: u64,
    previous_snapshot: KernelStateSnapshot,
    next_snapshot: KernelStateSnapshot,
    prepared_transition: PreparedKernelTransition,
}

impl JournalStore {
    /// Acquires the Provider phase lane under exact Run and configuration authority.
    ///
    /// This operation emits no provider-attempt event. Concrete candidate
    /// starts remain owned by `start_provider_attempt`, and every candidate in
    /// the same configuration epoch reuses this exact Provider lease.
    ///
    /// # Errors
    /// Returns [`JournalError::RuntimeKernelChildLaneAuthorityRejected`] when
    /// the parent Run lease is stale or cross-run, the provider configuration
    /// epoch drifted, or another Provider lane owns the session.
    pub(crate) fn acquire_runtime_provider_lane(
        &self,
        request: &RuntimeKernelProviderLaneAcquireRequest,
    ) -> Result<RuntimeProviderLaneAuthority, JournalError> {
        validate_provider_lane_request(request)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run_id = request.identities.run_id.as_str();
        let session_id = request.identities.session_id.as_str();
        let active_run = active_runtime_generation_tx(
            &transaction,
            session_id,
            run_id,
            RuntimeGenerationLane::Run,
            now,
        )?;
        if active_run.as_ref() != Some(&request.run_lease) {
            return Err(provider_lane_rejected(
                request,
                "runtime.kernel.provider_lane.parent_stale",
            ));
        }
        if current_provider_configuration_epoch_tx(&transaction)?
            != Some(request.expected_configuration_epoch)
        {
            return Err(provider_lane_rejected(
                request,
                "runtime.kernel.provider_lane.configuration_epoch_drift",
            ));
        }
        let remaining_ttl_ms = request.run_lease.expires_at_unix_ms.saturating_sub(now);
        if remaining_ttl_ms <= 0 {
            return Err(provider_lane_rejected(
                request,
                "runtime.kernel.provider_lane.parent_expired",
            ));
        }
        let owner = provider_generation_owner(request.expected_configuration_epoch);
        let provider_lease = match active_runtime_generation_for_session_lane_tx(
            &transaction,
            session_id,
            RuntimeGenerationLane::Provider,
            now,
        )? {
            Some(existing)
                if existing.run_id.as_ref() == Some(&request.identities.run_id)
                    && existing.owner == owner
                    && existing.expires_at_unix_ms <= request.run_lease.expires_at_unix_ms =>
            {
                existing
            }
            Some(_) => {
                return Err(provider_lane_rejected(
                    request,
                    "runtime.kernel.provider_lane.active_conflict",
                ));
            }
            None => activate_or_refresh_generation_tx(
                &transaction,
                session_id,
                Some(run_id),
                RuntimeGenerationLane::Provider,
                owner.as_str(),
                remaining_ttl_ms,
                RuntimeGenerationTransitionKind::Activated,
                "runtime.generation.provider_configuration_bound",
                now,
            )?,
        };
        if provider_lease.expires_at_unix_ms > request.run_lease.expires_at_unix_ms {
            return Err(provider_lane_rejected(
                request,
                "runtime.kernel.provider_lane.ttl_exceeded",
            ));
        }
        transaction.commit()?;
        Ok(RuntimeProviderLaneAuthority::from_journal(
            request.run_lease.clone(),
            provider_lease,
            request.expected_configuration_epoch,
        ))
    }

    /// Acquires one phase-lazy child lane under an exact active Run lease.
    ///
    /// Harness, Tool, and Delivery are the only generic child lanes. Provider
    /// authority is issued by `start_provider_attempt`, where provider
    /// configuration and candidate admission are checked atomically.
    ///
    /// An already-active child is returned only when its run, parent-bound
    /// owner, and expiry match this request. Every other overlap fails closed.
    ///
    /// # Errors
    /// Returns [`JournalError::RuntimeKernelChildLaneAuthorityRejected`] for an
    /// unsupported lane, malformed request, stale or cross-run parent, or
    /// conflicting active child authority.
    pub(crate) fn acquire_runtime_kernel_child_lane(
        &self,
        request: &RuntimeKernelChildLaneAcquireRequest,
    ) -> Result<GenerationLeaseV1, JournalError> {
        validate_child_lane_request(request)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run_id = request.identities.run_id.as_str();
        let session_id = request.identities.session_id.as_str();
        let active_run = active_runtime_generation_tx(
            &transaction,
            session_id,
            run_id,
            RuntimeGenerationLane::Run,
            now,
        )?;
        if active_run.as_ref() != Some(&request.run_lease) {
            return Err(child_lane_rejected(request, "runtime.kernel.child_lane.parent_stale"));
        }

        let remaining_ttl_ms = request.run_lease.expires_at_unix_ms.saturating_sub(now);
        if remaining_ttl_ms <= 0 {
            return Err(child_lane_rejected(request, "runtime.kernel.child_lane.parent_expired"));
        }
        let owner = child_lane_owner(request);
        if let Some(existing) = active_runtime_generation_for_session_lane_tx(
            &transaction,
            session_id,
            request.lane,
            now,
        )? {
            if existing.run_id.as_ref() == Some(&request.identities.run_id)
                && existing.owner == owner
                && existing.expires_at_unix_ms <= request.run_lease.expires_at_unix_ms
            {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(child_lane_rejected(request, "runtime.kernel.child_lane.active_conflict"));
        }

        let lease = activate_or_refresh_generation_tx(
            &transaction,
            session_id,
            Some(run_id),
            request.lane,
            owner.as_str(),
            remaining_ttl_ms,
            RuntimeGenerationTransitionKind::Activated,
            child_lane_activation_reason(request.lane),
            now,
        )?;
        if lease.expires_at_unix_ms > request.run_lease.expires_at_unix_ms {
            return Err(child_lane_rejected(request, "runtime.kernel.child_lane.ttl_exceeded"));
        }
        transaction.commit()?;
        Ok(lease)
    }

    /// Initializes one RuntimeKernelV2 head at revision zero.
    ///
    /// An exact repeat returns the existing head. New initialization requires
    /// the snapshot's Run generation to own the active Run lane.
    ///
    /// # Errors
    /// Returns [`JournalError`] when the snapshot is invalid, conflicts with an
    /// existing immutable head, lacks active generation authority, exceeds the
    /// configured JSON bound, or cannot be persisted.
    #[cfg(test)]
    pub(crate) fn initialize_runtime_kernel_state(
        &self,
        snapshot: &KernelStateSnapshot,
    ) -> Result<RuntimeKernelHeadRecord, JournalError> {
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let record = initialize_runtime_kernel_state_tx(
            &transaction,
            snapshot,
            self.config.max_payload_bytes,
            now,
        )?;
        transaction.commit()?;
        Ok(record)
    }

    /// Loads and validates the authoritative RuntimeKernelV2 head for a Run.
    ///
    /// # Errors
    /// Returns [`JournalError`] when stored JSON, digests, immutable identity
    /// columns, schema version, or revision are malformed.
    pub(crate) fn load_runtime_kernel_head(
        &self,
        run_id: &str,
    ) -> Result<Option<RuntimeKernelHeadRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_runtime_kernel_head_tx(&guard, run_id)
    }
}

fn validate_provider_lane_request(
    request: &RuntimeKernelProviderLaneAcquireRequest,
) -> Result<(), JournalError> {
    if request.identities.validate().is_err()
        || request.run_lease.validate().is_err()
        || request.run_lease.lane != RuntimeGenerationLane::Run
        || request.run_lease.session_id != request.identities.session_id
        || request.run_lease.run_id.as_ref() != Some(&request.identities.run_id)
        || request.run_lease.generation != request.identities.generation
    {
        return Err(provider_lane_rejected(
            request,
            "runtime.kernel.provider_lane.invalid_request",
        ));
    }
    Ok(())
}

fn provider_lane_rejected(
    request: &RuntimeKernelProviderLaneAcquireRequest,
    reason_code: &'static str,
) -> JournalError {
    JournalError::RuntimeKernelChildLaneAuthorityRejected {
        run_id: request.identities.run_id.to_string(),
        lane: RuntimeGenerationLane::Provider.as_str().to_owned(),
        reason_code,
    }
}

fn validate_child_lane_request(
    request: &RuntimeKernelChildLaneAcquireRequest,
) -> Result<(), JournalError> {
    if !matches!(
        request.lane,
        RuntimeGenerationLane::Harness
            | RuntimeGenerationLane::Tool
            | RuntimeGenerationLane::Delivery
    ) {
        return Err(child_lane_rejected(request, "runtime.kernel.child_lane.unsupported_lane"));
    }
    if request.identities.validate().is_err()
        || request.run_lease.validate().is_err()
        || request.run_lease.lane != RuntimeGenerationLane::Run
        || request.run_lease.session_id != request.identities.session_id
        || request.run_lease.run_id.as_ref() != Some(&request.identities.run_id)
        || request.run_lease.generation != request.identities.generation
        || request.owner.trim().is_empty()
        || request.owner.len() > MAX_RUNTIME_KERNEL_CHILD_OWNER_BYTES
    {
        return Err(child_lane_rejected(request, "runtime.kernel.child_lane.invalid_request"));
    }
    Ok(())
}

fn child_lane_owner(request: &RuntimeKernelChildLaneAcquireRequest) -> String {
    format!(
        "runtime_kernel_v2:{}:{}:{}",
        request.lane.as_str(),
        request.run_lease.lease_id,
        request.owner.trim()
    )
}

const fn child_lane_activation_reason(lane: RuntimeGenerationLane) -> &'static str {
    match lane {
        RuntimeGenerationLane::Harness => "runtime.generation.kernel_harness_bound",
        RuntimeGenerationLane::Tool => "runtime.generation.kernel_tool_bound",
        RuntimeGenerationLane::Delivery => "runtime.generation.kernel_delivery_bound",
        _ => "runtime.generation.kernel_child_unsupported",
    }
}

fn child_lane_rejected(
    request: &RuntimeKernelChildLaneAcquireRequest,
    reason_code: &'static str,
) -> JournalError {
    JournalError::RuntimeKernelChildLaneAuthorityRejected {
        run_id: request.identities.run_id.to_string(),
        lane: request.lane.as_str().to_owned(),
        reason_code,
    }
}

/// Initializes a revision-zero kernel head inside an existing admission transaction.
pub(crate) fn initialize_runtime_kernel_state_tx(
    transaction: &Transaction<'_>,
    snapshot: &KernelStateSnapshot,
    max_payload_bytes: usize,
    now: i64,
) -> Result<RuntimeKernelHeadRecord, JournalError> {
    snapshot.validate().map_err(|_| JournalError::InvalidRuntimeKernelSnapshot)?;
    if snapshot.revision() != 0 {
        return Err(JournalError::InvalidRuntimeKernelSnapshot);
    }
    let identities = snapshot.base_identities();
    let session_id = identities.session_id.as_str();
    let run_id = identities.run_id.as_str();
    let generation = snapshot.run_generation();
    let snapshot_json =
        canonical_bounded_json(snapshot, "runtime_kernel_snapshot", max_payload_bytes)?;
    let snapshot_sha256 = sha256_hex(snapshot_json.as_bytes());
    if let Some(existing) = load_runtime_kernel_head_tx(transaction, run_id)? {
        if existing.snapshot == *snapshot
            && existing.snapshot_sha256 == snapshot_sha256
            && existing.revision == 0
        {
            return Ok(existing);
        }
        return Err(JournalError::RuntimeKernelInitializationConflict {
            run_id: run_id.to_owned(),
        });
    }

    require_matching_run_identity_tx(transaction, session_id, run_id)?;
    let active = active_runtime_generation_tx(
        transaction,
        session_id,
        run_id,
        RuntimeGenerationLane::Run,
        now,
    )?;
    if active.as_ref().map(|lease| lease.generation) != Some(generation) {
        return Err(JournalError::RuntimeKernelGenerationInactive {
            run_id: run_id.to_owned(),
            generation: generation.get(),
        });
    }

    transaction.execute(
        r#"
                INSERT INTO runtime_kernel_heads (
                    run_ulid, session_ulid, runtime_version, run_generation, revision,
                    snapshot_json, snapshot_sha256, initialized_at_unix_ms,
                    updated_at_unix_ms, schema_version
                ) VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6, ?7, ?7, ?8)
            "#,
        params![
            run_id,
            session_id,
            runtime_kernel_version_str(snapshot.version()),
            generation_sql(generation)?,
            snapshot_json,
            snapshot_sha256,
            now,
            RUNTIME_KERNEL_ROW_SCHEMA_VERSION,
        ],
    )?;
    Ok(RuntimeKernelHeadRecord {
        revision: snapshot.revision(),
        snapshot: snapshot.clone(),
        snapshot_sha256,
    })
}

impl JournalStore {
    /// Commits a sequence-neutral observation under one immediate transaction.
    ///
    /// The journal resolves the canonical lane sequence while holding the write
    /// reservation, then asks the pure kernel to prepare the transition before
    /// appending the event, ledger evidence, and compare-and-set head.
    ///
    /// # Errors
    /// Returns [`JournalError`] when the observation is malformed, the kernel
    /// rejects it, durable authority is stale, or the atomic commit fails.
    pub(crate) fn commit_runtime_kernel_observation(
        &self,
        request: &RuntimeKernelObservationCommitRequest,
    ) -> Result<RuntimeKernelTransitionCommitOutcome, JournalError> {
        if request.event_template.sequence != 0 {
            return Err(JournalError::InvalidPreparedRuntimeKernelTransition);
        }
        let identities = request.expected_snapshot.base_identities();
        let lane = request.event_template.event_name.descriptor().generation_lane;
        let generation = request.event_template.identities.generation;
        let observation_sha256 =
            runtime_kernel_observation_sha256(&request.event_template, request.transition)?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = current_unix_ms()?;
        if let Some(existing) = load_runtime_kernel_ledger_by_idempotency_tx(
            &transaction,
            identities.run_id.as_str(),
            request.idempotency_key.as_str(),
        )? {
            if existing.observation_sha256 != observation_sha256 {
                return Err(JournalError::RuntimeKernelIdempotencyConflict {
                    run_id: identities.run_id.to_string(),
                    idempotency_key: request.idempotency_key.clone(),
                });
            }
            validate_stored_ledger_identity(
                &existing,
                identities.session_id.as_str(),
                identities.run_id.as_str(),
                request.expected_snapshot.version(),
                request.expected_snapshot.run_generation(),
            )?;
            return Ok(RuntimeKernelTransitionCommitOutcome::AlreadyApplied {
                revision: existing.revision,
                ledger_index: existing.ledger_index,
                event_sequence: existing.event_sequence,
                snapshot: existing.next_snapshot,
            });
        }
        let (authority_is_current, active_run_generation) = observation_authority_is_current_tx(
            &transaction,
            &request.expected_snapshot,
            &request.lane_authority,
            now,
        )?;
        if !authority_is_current {
            return Ok(RuntimeKernelTransitionCommitOutcome::StaleSuppressed {
                active_generation: active_run_generation,
            });
        }
        let sequence = next_runtime_kernel_event_sequence_tx(
            &transaction,
            identities.session_id.as_str(),
            lane,
            generation,
        )?;
        let mut event = request.event_template.clone();
        event.sequence = sequence;
        let kernel = RuntimeKernelV2::restore_from_journal(request.expected_snapshot.clone())
            .map_err(|_| JournalError::InvalidPreparedRuntimeKernelTransition)?;
        let prepared = kernel
            .prepare_transition(
                request.expected_run_generation,
                &request.lane_authority,
                request.idempotency_key.as_str(),
                event,
                request.transition,
            )
            .map_err(|_| JournalError::InvalidPreparedRuntimeKernelTransition)?;
        let outcome = self.commit_prepared_runtime_kernel_transition_tx(
            &transaction,
            &prepared,
            now,
            Some(observation_sha256.as_str()),
        )?;
        transaction.commit()?;
        Ok(outcome)
    }

    /// Commits a prepared RuntimeKernelV2 transition under one SQLite transaction.
    ///
    /// The method resolves full-history idempotency before live authority,
    /// verifies the complete previous snapshot against the durable
    /// head, appends or reuses the canonical V2 event, appends immutable ledger
    /// evidence, and advances the head with compare-and-set semantics.
    ///
    /// # Errors
    /// Returns [`JournalError`] for invalid/tampered prepared data, idempotency
    /// conflicts, missing or mismatched heads, event sequence conflicts, payload
    /// bounds, or any transactional storage failure.
    #[cfg(test)]
    pub(crate) fn commit_prepared_runtime_kernel_transition(
        &self,
        prepared: &PreparedKernelTransition,
    ) -> Result<RuntimeKernelTransitionCommitOutcome, JournalError> {
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcome = self.commit_prepared_runtime_kernel_transition_tx(
            &transaction,
            prepared,
            current_unix_ms()?,
            None,
        )?;
        transaction.commit()?;
        Ok(outcome)
    }

    fn commit_prepared_runtime_kernel_transition_tx(
        &self,
        transaction: &rusqlite::Connection,
        prepared: &PreparedKernelTransition,
        now: i64,
        observation_sha256: Option<&str>,
    ) -> Result<RuntimeKernelTransitionCommitOutcome, JournalError> {
        prepared.validate().map_err(|_| JournalError::InvalidPreparedRuntimeKernelTransition)?;
        let previous = prepared.previous_snapshot();
        let next = prepared.next_snapshot();
        let identities = previous.base_identities();
        let session_id = identities.session_id.as_str();
        let run_id = identities.run_id.as_str();
        let generation = previous.run_generation();
        let previous_json = canonical_bounded_json(
            previous,
            "runtime_kernel_previous_snapshot",
            self.config.max_payload_bytes,
        )?;
        let next_json = canonical_bounded_json(
            next,
            "runtime_kernel_next_snapshot",
            self.config.max_payload_bytes,
        )?;
        let prepared_json = canonical_bounded_json(
            prepared,
            "runtime_kernel_prepared_transition",
            self.config.max_payload_bytes,
        )?;
        let event = event_from_prepared_json(prepared_json.as_str())?;
        let previous_sha256 = sha256_hex(previous_json.as_bytes());
        let next_sha256 = sha256_hex(next_json.as_bytes());
        let prepared_sha256 = sha256_hex(prepared_json.as_bytes());
        let observation_sha256 = observation_sha256.map_or_else(
            || runtime_kernel_observation_sha256(&event, prepared.transition()),
            |digest| Ok(digest.to_owned()),
        )?;
        if let Some(existing) = load_runtime_kernel_ledger_by_idempotency_tx(
            transaction,
            run_id,
            prepared.idempotency_key(),
        )? {
            if existing.request_sha256 != prepared.request_sha256() {
                return Err(JournalError::RuntimeKernelIdempotencyConflict {
                    run_id: run_id.to_owned(),
                    idempotency_key: prepared.idempotency_key().to_owned(),
                });
            }
            validate_stored_ledger_identity(
                &existing,
                session_id,
                run_id,
                previous.version(),
                generation,
            )?;
            return Ok(RuntimeKernelTransitionCommitOutcome::AlreadyApplied {
                revision: existing.revision,
                ledger_index: existing.ledger_index,
                event_sequence: existing.event_sequence,
                snapshot: existing.next_snapshot,
            });
        }
        let (authority_is_current, active_run_generation) =
            prepared_authority_is_current_tx(transaction, prepared, now)?;
        if !authority_is_current {
            return Ok(RuntimeKernelTransitionCommitOutcome::StaleSuppressed {
                active_generation: active_run_generation,
            });
        }

        if !matches!(prepared.outcome(), TransitionOutcome::Applied { .. }) {
            return Err(JournalError::RuntimeKernelReplayEvidenceMissing {
                run_id: run_id.to_owned(),
                idempotency_key: prepared.idempotency_key().to_owned(),
            });
        }

        let head = load_runtime_kernel_head_tx(transaction, run_id)?
            .ok_or_else(|| JournalError::RuntimeKernelHeadNotFound { run_id: run_id.to_owned() })?;
        if head.snapshot != *previous
            || head.snapshot_sha256 != previous_sha256
            || head.revision != prepared.expected_revision()
            || previous.revision() != prepared.expected_revision()
            || head.snapshot.version() != next.version()
            || next.base_identities().session_id != identities.session_id
            || next.base_identities().run_id != identities.run_id
            || next.run_generation() != generation
        {
            return Err(JournalError::RuntimeKernelHeadConflict { run_id: run_id.to_owned() });
        }
        let next_revision = next.revision();

        let event_outcome = append_runtime_event_tx(
            transaction,
            self.config.max_payload_bytes,
            &RuntimeEventAppendRequest {
                lane: event.event_name.descriptor().generation_lane,
                envelope: event.clone(),
            },
            now,
        )?;
        let event_sequence = match event_outcome {
            RuntimeEventAppendOutcome::Appended { sequence }
            | RuntimeEventAppendOutcome::AlreadyAppended { sequence } => sequence,
            RuntimeEventAppendOutcome::StaleSuppressed => {
                return Ok(RuntimeKernelTransitionCommitOutcome::StaleSuppressed {
                    active_generation: Some(generation),
                });
            }
        };
        if event_sequence != event.sequence {
            return Err(JournalError::RuntimeKernelEventSequenceConflict {
                run_id: run_id.to_owned(),
                expected_sequence: event.sequence,
                actual_sequence: event_sequence,
            });
        }

        transaction.execute(
            r#"
                INSERT INTO runtime_kernel_transition_ledger (
                    run_ulid, session_ulid, runtime_version, run_generation, revision,
                    idempotency_key, request_sha256, observation_sha256,
                    event_ulid, event_sequence,
                    previous_snapshot_json, previous_snapshot_sha256,
                    next_snapshot_json, next_snapshot_sha256,
                    prepared_transition_json, prepared_transition_sha256,
                    created_at_unix_ms, schema_version
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                    ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18
                )
            "#,
            params![
                run_id,
                session_id,
                runtime_kernel_version_str(previous.version()),
                generation_sql(generation)?,
                revision_sql(next_revision)?,
                prepared.idempotency_key(),
                prepared.request_sha256(),
                observation_sha256,
                event.event_id.as_str(),
                revision_sql(event_sequence)?,
                previous_json,
                previous_sha256,
                next_json,
                next_sha256,
                prepared_json,
                prepared_sha256,
                now,
                RUNTIME_KERNEL_ROW_SCHEMA_VERSION,
            ],
        )?;
        let ledger_index = u64::try_from(transaction.last_insert_rowid())
            .map_err(|_| JournalError::InvalidRuntimeKernelLedger { run_id: run_id.to_owned() })?;

        let updated = transaction.execute(
            r#"
                UPDATE runtime_kernel_heads
                SET revision = ?1,
                    snapshot_json = ?2,
                    snapshot_sha256 = ?3,
                    updated_at_unix_ms = ?4
                WHERE run_ulid = ?5
                  AND revision = ?6
                  AND snapshot_json = ?7
                  AND snapshot_sha256 = ?8
            "#,
            params![
                revision_sql(next_revision)?,
                next_json,
                next_sha256,
                now,
                run_id,
                revision_sql(prepared.expected_revision())?,
                previous_json,
                previous_sha256,
            ],
        )?;
        if updated != 1 {
            return Err(JournalError::RuntimeKernelHeadConflict { run_id: run_id.to_owned() });
        }
        Ok(RuntimeKernelTransitionCommitOutcome::Applied {
            revision: next_revision,
            ledger_index,
            event_sequence,
            snapshot: next.clone(),
        })
    }

    /// Lists validated ledger rows for a focused regression assertion.
    ///
    /// # Errors
    /// Returns [`JournalError`] when any immutable row is malformed.
    #[cfg(test)]
    pub(crate) fn list_runtime_kernel_transition_ledger_for_test(
        &self,
        run_id: &str,
    ) -> Result<Vec<RuntimeKernelTransitionLedgerRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT
                    ledger_index, revision, session_ulid, runtime_version,
                    run_generation, idempotency_key, request_sha256, event_ulid,
                    event_sequence, previous_snapshot_json, previous_snapshot_sha256,
                next_snapshot_json, next_snapshot_sha256,
                    prepared_transition_json, prepared_transition_sha256, schema_version,
                    observation_sha256
                FROM runtime_kernel_transition_ledger
                WHERE run_ulid = ?1
                ORDER BY ledger_index ASC
            "#,
        )?;
        let rows = statement.query_map(params![run_id], hydrate_stored_kernel_ledger_row)?;
        rows.map(|row| {
            let stored = row?;
            validate_stored_kernel_ledger(&stored, run_id)?;
            Ok(RuntimeKernelTransitionLedgerRecord {
                ledger_index: stored.ledger_index,
                revision: stored.revision,
                idempotency_key: stored.idempotency_key,
                request_sha256: stored.request_sha256,
                observation_sha256: stored.observation_sha256,
                event_id: stored.event_id,
                event_sequence: stored.event_sequence,
                previous_snapshot: stored.previous_snapshot,
                next_snapshot: stored.next_snapshot,
            })
        })
        .collect()
    }
}

fn next_runtime_kernel_event_sequence_tx(
    connection: &rusqlite::Connection,
    session_id: &str,
    lane: RuntimeGenerationLane,
    generation: RuntimeGeneration,
) -> Result<u64, JournalError> {
    let generation = generation_sql(generation)?;
    let last_sequence = connection.query_row(
        r#"
            SELECT MAX(sequence)
            FROM runtime_events_v2
            WHERE session_ulid = ?1 AND lane = ?2 AND generation = ?3
        "#,
        params![session_id, lane.as_str(), generation],
        |row| row.get::<_, Option<i64>>(0),
    )?;
    match last_sequence {
        Some(sequence) => u64::try_from(sequence)
            .map_err(|_| {
                JournalError::InvalidArgument(
                    "stored runtime event sequence exceeds supported range".to_owned(),
                )
            })?
            .checked_add(1)
            .ok_or_else(|| {
                JournalError::InvalidArgument("runtime event sequence is exhausted".to_owned())
            }),
        None => Ok(1),
    }
}

fn runtime_kernel_observation_sha256(
    event: &RuntimeEventEnvelopeV2,
    transition: KernelTransition,
) -> Result<String, JournalError> {
    let mut semantic_event = serde_json::to_value(event)?;
    let fields = semantic_event
        .as_object_mut()
        .ok_or(JournalError::InvalidPreparedRuntimeKernelTransition)?;
    fields.remove("event_id");
    fields.remove("sequence");
    fields.remove("occurred_at_unix_ms");
    let canonical = serde_json::to_vec(&(semantic_event, transition))?;
    Ok(sha256_hex(canonical.as_slice()))
}

fn canonical_bounded_json<T: Serialize>(
    value: &T,
    payload_kind: &'static str,
    max_payload_bytes: usize,
) -> Result<String, JournalError> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > max_payload_bytes {
        return Err(JournalError::PayloadTooLarge {
            payload_kind,
            actual_bytes: bytes.len(),
            max_bytes: max_payload_bytes,
        });
    }
    String::from_utf8(bytes).map_err(|_| JournalError::InvalidRuntimeKernelJson { payload_kind })
}

fn decode_canonical_json<T: DeserializeOwned + Serialize>(
    raw: &str,
    payload_kind: &'static str,
) -> Result<T, JournalError> {
    let value = serde_json::from_str::<T>(raw)?;
    let canonical = serde_json::to_string(&value)?;
    if canonical != raw {
        return Err(JournalError::InvalidRuntimeKernelJson { payload_kind });
    }
    Ok(value)
}

fn event_from_prepared_json(prepared_json: &str) -> Result<RuntimeEventEnvelopeV2, JournalError> {
    let prepared_value = serde_json::from_str::<serde_json::Value>(prepared_json)?;
    let event = prepared_value
        .get("event")
        .cloned()
        .ok_or(JournalError::InvalidPreparedRuntimeKernelTransition)?;
    serde_json::from_value(event).map_err(|_| JournalError::InvalidPreparedRuntimeKernelTransition)
}

fn load_runtime_kernel_head_tx(
    connection: &rusqlite::Connection,
    run_id: &str,
) -> Result<Option<RuntimeKernelHeadRecord>, JournalError> {
    let row = connection
        .query_row(
            r#"
                SELECT
                    session_ulid, runtime_version, run_generation, revision,
                    snapshot_json, snapshot_sha256, schema_version
                FROM runtime_kernel_heads
                WHERE run_ulid = ?1
            "#,
            params![run_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )
        .optional()?;
    let Some((
        session_id,
        runtime_version,
        run_generation,
        revision,
        snapshot_json,
        snapshot_sha256,
        schema_version,
    )) = row
    else {
        return Ok(None);
    };
    if schema_version != RUNTIME_KERNEL_ROW_SCHEMA_VERSION
        || sha256_hex(snapshot_json.as_bytes()) != snapshot_sha256
    {
        return Err(JournalError::InvalidRuntimeKernelHead { run_id: run_id.to_owned() });
    }
    let snapshot: KernelStateSnapshot =
        decode_canonical_json(snapshot_json.as_str(), "runtime_kernel_snapshot")?;
    snapshot
        .validate()
        .map_err(|_| JournalError::InvalidRuntimeKernelHead { run_id: run_id.to_owned() })?;
    let stored_generation = generation_from_sql(run_generation, run_id)?;
    let stored_revision = u64::try_from(revision)
        .map_err(|_| JournalError::InvalidRuntimeKernelHead { run_id: run_id.to_owned() })?;
    if snapshot.base_identities().session_id.as_str() != session_id
        || snapshot.base_identities().run_id.as_str() != run_id
        || runtime_kernel_version_str(snapshot.version()) != runtime_version
        || snapshot.run_generation() != stored_generation
        || snapshot.revision() != stored_revision
    {
        return Err(JournalError::InvalidRuntimeKernelHead { run_id: run_id.to_owned() });
    }
    Ok(Some(RuntimeKernelHeadRecord { revision: stored_revision, snapshot, snapshot_sha256 }))
}

fn load_runtime_kernel_ledger_by_idempotency_tx(
    connection: &rusqlite::Connection,
    run_id: &str,
    idempotency_key: &str,
) -> Result<Option<StoredKernelLedgerEntry>, JournalError> {
    let row = connection
        .query_row(
            r#"
                SELECT
                    ledger_index, revision, session_ulid, runtime_version,
                    run_generation, idempotency_key, request_sha256, event_ulid,
                    event_sequence, previous_snapshot_json, previous_snapshot_sha256,
                    next_snapshot_json, next_snapshot_sha256,
                    prepared_transition_json, prepared_transition_sha256, schema_version,
                    observation_sha256
                FROM runtime_kernel_transition_ledger
                WHERE run_ulid = ?1 AND idempotency_key = ?2
            "#,
            params![run_id, idempotency_key],
            hydrate_stored_kernel_ledger_row,
        )
        .optional()?;
    row.map(|stored| {
        validate_stored_kernel_ledger(&stored, run_id)?;
        Ok(stored)
    })
    .transpose()
}

fn hydrate_stored_kernel_ledger_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<StoredKernelLedgerEntry> {
    let ledger_index = row.get::<_, i64>(0)?;
    let revision = row.get::<_, i64>(1)?;
    let run_generation = row.get::<_, i64>(4)?;
    let event_sequence = row.get::<_, i64>(8)?;
    let previous_json = row.get::<_, String>(9)?;
    let previous_sha256 = row.get::<_, String>(10)?;
    let next_json = row.get::<_, String>(11)?;
    let next_sha256 = row.get::<_, String>(12)?;
    let prepared_json = row.get::<_, String>(13)?;
    let prepared_sha256 = row.get::<_, String>(14)?;
    let schema_version = row.get::<_, i64>(15)?;
    let observation_sha256 = row.get::<_, String>(16)?;

    if schema_version != RUNTIME_KERNEL_ROW_SCHEMA_VERSION
        || sha256_hex(previous_json.as_bytes()) != previous_sha256
        || sha256_hex(next_json.as_bytes()) != next_sha256
        || sha256_hex(prepared_json.as_bytes()) != prepared_sha256
    {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            15,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::other("invalid runtime kernel ledger digest or schema")),
        ));
    }
    let previous_snapshot =
        decode_sql_json(previous_json.as_str(), "runtime kernel previous snapshot", 9)?;
    let next_snapshot = decode_sql_json(next_json.as_str(), "runtime kernel next snapshot", 11)?;
    let prepared_transition =
        decode_sql_json(prepared_json.as_str(), "runtime kernel prepared transition", 13)?;
    Ok(StoredKernelLedgerEntry {
        ledger_index: u64::try_from(ledger_index).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::new(error),
            )
        })?,
        revision: u64::try_from(revision).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Integer,
                Box::new(error),
            )
        })?,
        session_id: row.get(2)?,
        runtime_version: row.get(3)?,
        run_generation: RuntimeGeneration::new(u64::try_from(run_generation).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Integer,
                Box::new(error),
            )
        })?)
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Integer,
                Box::new(error),
            )
        })?,
        idempotency_key: row.get(5)?,
        request_sha256: row.get(6)?,
        observation_sha256,
        event_id: row.get(7)?,
        event_sequence: u64::try_from(event_sequence).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Integer,
                Box::new(error),
            )
        })?,
        previous_snapshot,
        next_snapshot,
        prepared_transition,
    })
}

fn decode_sql_json<T: DeserializeOwned + Serialize>(
    raw: &str,
    label: &'static str,
    column: usize,
) -> rusqlite::Result<T> {
    let value = serde_json::from_str::<T>(raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })?;
    let canonical = serde_json::to_string(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })?;
    if canonical != raw {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::other(format!("{label} is not canonical JSON"))),
        ));
    }
    Ok(value)
}

fn validate_stored_kernel_ledger(
    stored: &StoredKernelLedgerEntry,
    run_id: &str,
) -> Result<(), JournalError> {
    stored
        .prepared_transition
        .validate()
        .map_err(|_| JournalError::InvalidRuntimeKernelLedger { run_id: run_id.to_owned() })?;
    if !matches!(stored.prepared_transition.outcome(), TransitionOutcome::Applied { .. })
        || stored.prepared_transition.previous_snapshot() != &stored.previous_snapshot
        || stored.prepared_transition.next_snapshot() != &stored.next_snapshot
        || stored.prepared_transition.idempotency_key() != stored.idempotency_key
        || stored.prepared_transition.request_sha256() != stored.request_sha256
        || runtime_kernel_observation_sha256(
            stored.prepared_transition.event(),
            stored.prepared_transition.transition(),
        )? != stored.observation_sha256
        || stored.prepared_transition.expected_revision() != stored.previous_snapshot.revision()
        || stored.revision != stored.next_snapshot.revision()
        || stored.previous_snapshot.base_identities().run_id.as_str() != run_id
        || stored.next_snapshot.base_identities().run_id.as_str() != run_id
    {
        return Err(JournalError::InvalidRuntimeKernelLedger { run_id: run_id.to_owned() });
    }
    let prepared_json = serde_json::to_value(&stored.prepared_transition)?;
    let event = prepared_json
        .get("event")
        .ok_or_else(|| JournalError::InvalidRuntimeKernelLedger { run_id: run_id.to_owned() })?;
    let event_id = event.get("event_id").and_then(serde_json::Value::as_str);
    let sequence = event.get("sequence").and_then(serde_json::Value::as_u64);
    if event_id != Some(stored.event_id.as_str()) || sequence != Some(stored.event_sequence) {
        return Err(JournalError::InvalidRuntimeKernelLedger { run_id: run_id.to_owned() });
    }
    Ok(())
}

fn validate_stored_ledger_identity(
    stored: &StoredKernelLedgerEntry,
    session_id: &str,
    run_id: &str,
    version: RuntimeKernelVersion,
    generation: RuntimeGeneration,
) -> Result<(), JournalError> {
    if stored.session_id != session_id
        || stored.runtime_version != runtime_kernel_version_str(version)
        || stored.run_generation != generation
        || stored.previous_snapshot.base_identities().run_id.as_str() != run_id
    {
        return Err(JournalError::InvalidRuntimeKernelLedger { run_id: run_id.to_owned() });
    }
    Ok(())
}

fn require_matching_run_identity_tx(
    connection: &rusqlite::Connection,
    session_id: &str,
    run_id: &str,
) -> Result<(), JournalError> {
    let stored_session = connection
        .query_row(
            "SELECT session_ulid FROM orchestrator_runs WHERE run_ulid = ?1",
            params![run_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    match stored_session {
        Some(stored_session) if stored_session == session_id => Ok(()),
        Some(_) => {
            Err(JournalError::RuntimeKernelRunIdentityConflict { run_id: run_id.to_owned() })
        }
        None => Err(JournalError::RunNotFound { run_id: run_id.to_owned() }),
    }
}

fn prepared_authority_is_current_tx(
    connection: &rusqlite::Connection,
    prepared: &PreparedKernelTransition,
    now: i64,
) -> Result<(bool, Option<RuntimeGeneration>), JournalError> {
    observation_authority_is_current_tx(
        connection,
        prepared.previous_snapshot(),
        prepared.lane_authority(),
        now,
    )
}

fn observation_authority_is_current_tx(
    connection: &rusqlite::Connection,
    snapshot: &KernelStateSnapshot,
    lane_authority: &KernelLaneAuthoritySet,
    now: i64,
) -> Result<(bool, Option<RuntimeGeneration>), JournalError> {
    let identities = snapshot.base_identities();
    let session_id = identities.session_id.as_str();
    let run_id = identities.run_id.as_str();
    let mut active_run_generation = None;
    let mut all_match = true;
    for expected in lane_authority.leases() {
        let active =
            active_runtime_generation_tx(connection, session_id, run_id, expected.lane, now)?;
        if expected.lane == RuntimeGenerationLane::Run {
            active_run_generation = active.as_ref().map(|lease| lease.generation);
        }
        if !active.as_ref().is_some_and(|lease| {
            lease.lease_id == expected.lease_id && lease.generation == expected.generation
        }) {
            all_match = false;
        }
    }
    Ok((all_match, active_run_generation))
}

const fn runtime_kernel_version_str(version: RuntimeKernelVersion) -> &'static str {
    match version {
        RuntimeKernelVersion::Legacy => "legacy",
        RuntimeKernelVersion::V2Shadow => "v2_shadow",
        RuntimeKernelVersion::V2Canary => "v2_canary",
        RuntimeKernelVersion::V2 => "v2",
    }
}

fn generation_sql(generation: RuntimeGeneration) -> Result<i64, JournalError> {
    i64::try_from(generation.get())
        .map_err(|_| JournalError::InvalidRuntimeKernelGeneration { generation: generation.get() })
}

fn generation_from_sql(value: i64, run_id: &str) -> Result<RuntimeGeneration, JournalError> {
    let value = u64::try_from(value)
        .map_err(|_| JournalError::InvalidRuntimeKernelHead { run_id: run_id.to_owned() })?;
    RuntimeGeneration::new(value)
        .map_err(|_| JournalError::InvalidRuntimeKernelHead { run_id: run_id.to_owned() })
}

fn revision_sql(value: u64) -> Result<i64, JournalError> {
    i64::try_from(value).map_err(|_| JournalError::RuntimeKernelRevisionOutOfRange { value })
}
