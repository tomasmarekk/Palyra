//! Durable single-finalization and delivery-intent authority for RuntimeKernelV2.
//!
//! Final artifacts and delivery intents are immutable. Delivery observations form
//! an append-only ledger, while connector payloads remain in the existing outbox.

#[cfg(test)]
mod tests;

use palyra_common::runtime_contracts::{
    RuntimeGeneration, RuntimeGenerationLane, RuntimeTerminalOutcome,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};

use super::{
    current_unix_ms, shared_runtime::active_runtime_generation_tx, JournalError, JournalStore,
};

const FINALIZATION_SCHEMA_VERSION: i64 = 1;
const MAX_FINALIZATION_REFERENCES: usize = 64;
const MAX_REFERENCE_BYTES: usize = 192;

/// Migration 74: immutable final artifacts and delivery intents plus append-only links.
pub(super) const MIGRATION_74_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_final_output_artifacts (
        artifact_ulid TEXT PRIMARY KEY,
        session_ulid TEXT NOT NULL,
        run_ulid TEXT NOT NULL,
        run_generation INTEGER NOT NULL CHECK (run_generation > 0),
        run_lease_ulid TEXT NOT NULL,
        terminal_outcome TEXT NOT NULL CHECK (
            terminal_outcome IN ('completed', 'failed', 'cancelled', 'timed_out')
        ),
        content_sha256 TEXT NOT NULL,
        projection_sha256 TEXT NOT NULL,
        user_visible INTEGER NOT NULL CHECK (user_visible IN (0, 1)),
        verification_evidence_json TEXT NOT NULL,
        missing_artifacts_json TEXT NOT NULL,
        active_process_state_json TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        descriptor_json TEXT NOT NULL,
        descriptor_sha256 TEXT NOT NULL,
        committed_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        UNIQUE(run_ulid, run_generation)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_final_artifacts_session_run
        ON runtime_final_output_artifacts(session_ulid, run_ulid, run_generation);

    CREATE TABLE IF NOT EXISTS runtime_delivery_intents_v2 (
        delivery_intent_ulid TEXT PRIMARY KEY,
        artifact_ulid TEXT NOT NULL,
        session_ulid TEXT NOT NULL,
        run_ulid TEXT NOT NULL,
        run_generation INTEGER NOT NULL CHECK (run_generation > 0),
        run_lease_ulid TEXT NOT NULL,
        delivery_generation INTEGER NOT NULL CHECK (delivery_generation > 0),
        delivery_lease_ulid TEXT NOT NULL,
        destination_binding_sha256 TEXT NOT NULL,
        connector_id TEXT NOT NULL,
        outbox_envelope_id TEXT NOT NULL,
        content_sha256 TEXT NOT NULL,
        outbound_request_sha256 TEXT NOT NULL,
        dedupe_key TEXT NOT NULL,
        intent_json TEXT NOT NULL,
        intent_sha256 TEXT NOT NULL,
        committed_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        UNIQUE(run_ulid, run_generation),
        UNIQUE(connector_id, outbox_envelope_id),
        UNIQUE(dedupe_key),
        FOREIGN KEY(artifact_ulid) REFERENCES runtime_final_output_artifacts(artifact_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_delivery_intents_run
        ON runtime_delivery_intents_v2(run_ulid, run_generation);

    CREATE TABLE IF NOT EXISTS runtime_delivery_links_v2 (
        link_index INTEGER PRIMARY KEY AUTOINCREMENT,
        delivery_intent_ulid TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('queued', 'delivered', 'outcome_unknown')),
        connector_id TEXT NOT NULL,
        outbox_envelope_id TEXT NOT NULL,
        evidence_sha256 TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        native_message_id_sha256 TEXT,
        observed_at_unix_ms INTEGER NOT NULL,
        link_json TEXT NOT NULL,
        link_sha256 TEXT NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        UNIQUE(delivery_intent_ulid, state, evidence_sha256),
        FOREIGN KEY(delivery_intent_ulid)
            REFERENCES runtime_delivery_intents_v2(delivery_intent_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_delivery_links_intent
        ON runtime_delivery_links_v2(delivery_intent_ulid, link_index ASC);

    CREATE TRIGGER IF NOT EXISTS trg_runtime_final_artifacts_prevent_update
    BEFORE UPDATE ON runtime_final_output_artifacts BEGIN
        SELECT RAISE(ABORT, 'runtime_final_output_artifacts is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_final_artifacts_prevent_delete
    BEFORE DELETE ON runtime_final_output_artifacts BEGIN
        SELECT RAISE(ABORT, 'runtime_final_output_artifacts is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_delivery_intents_v2_prevent_update
    BEFORE UPDATE ON runtime_delivery_intents_v2 BEGIN
        SELECT RAISE(ABORT, 'runtime_delivery_intents_v2 is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_delivery_intents_v2_prevent_delete
    BEFORE DELETE ON runtime_delivery_intents_v2 BEGIN
        SELECT RAISE(ABORT, 'runtime_delivery_intents_v2 is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_delivery_links_v2_prevent_update
    BEFORE UPDATE ON runtime_delivery_links_v2 BEGIN
        SELECT RAISE(ABORT, 'runtime_delivery_links_v2 is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_delivery_links_v2_prevent_delete
    BEFORE DELETE ON runtime_delivery_links_v2 BEGIN
        SELECT RAISE(ABORT, 'runtime_delivery_links_v2 is append-only');
    END;
"#;

/// One metadata-only reference attached to finalization evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FinalizationEvidenceRef {
    /// Stable evidence class, such as `verification_report` or `process_lease`.
    pub(crate) kind: String,
    /// Opaque host-owned reference.
    pub(crate) reference_id: String,
    /// SHA-256 of the referenced evidence.
    pub(crate) sha256: String,
}

impl FinalizationEvidenceRef {
    fn validate(&self) -> bool {
        valid_label(&self.kind) && valid_reference(&self.reference_id) && valid_sha256(&self.sha256)
    }
}

/// Immutable, generation-bound descriptor of one final output artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FinalOutputArtifactDescriptor {
    /// Stable host artifact identity.
    pub(crate) artifact_id: String,
    /// Session owning the run.
    pub(crate) session_id: String,
    /// Run owning the artifact.
    pub(crate) run_id: String,
    /// Immutable Run generation.
    pub(crate) run_generation: RuntimeGeneration,
    /// Exact active Run lease at commit.
    pub(crate) run_lease_id: String,
    /// Canonical terminal classification.
    pub(crate) terminal_outcome: RuntimeTerminalOutcome,
    /// Hash of final content retained by the host.
    pub(crate) content_sha256: String,
    /// Hash carried by the opaque kernel projection reference.
    pub(crate) projection_sha256: String,
    /// Whether a user-facing delivery may be created.
    pub(crate) user_visible: bool,
    /// Verification evidence attached to the final output.
    pub(crate) verification_evidence: Vec<FinalizationEvidenceRef>,
    /// Expected artifacts that were unavailable at finalization.
    pub(crate) missing_artifacts: Vec<FinalizationEvidenceRef>,
    /// Active process or cleanup state observed at finalization.
    pub(crate) active_process_state: Vec<FinalizationEvidenceRef>,
    /// Stable terminal reason code.
    pub(crate) reason_code: String,
    /// Host commit timestamp.
    pub(crate) committed_at_unix_ms: i64,
}

impl FinalOutputArtifactDescriptor {
    fn validate(&self) -> Result<(), JournalError> {
        if !valid_reference(&self.artifact_id)
            || !valid_reference(&self.session_id)
            || !valid_reference(&self.run_id)
            || !valid_reference(&self.run_lease_id)
            || !valid_sha256(&self.content_sha256)
            || !valid_sha256(&self.projection_sha256)
            || !valid_label(&self.reason_code)
            || self.committed_at_unix_ms < 0
        {
            return Err(JournalError::InvalidRuntimeFinalOutput {
                reason: "identity, digest, reason, or timestamp is invalid".to_owned(),
            });
        }
        validate_evidence_set(&self.verification_evidence)?;
        validate_evidence_set(&self.missing_artifacts)?;
        validate_evidence_set(&self.active_process_state)?;
        Ok(())
    }
}

/// Immutable delivery intent committed before the existing connector outbox is touched.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeDeliveryIntentDescriptor {
    /// Typed delivery identity.
    pub(crate) delivery_intent_id: String,
    /// Final artifact being delivered.
    pub(crate) artifact_id: String,
    /// Session owning the run.
    pub(crate) session_id: String,
    /// Run owning the intent.
    pub(crate) run_id: String,
    /// Immutable Run generation.
    pub(crate) run_generation: RuntimeGeneration,
    /// Exact active Run lease.
    pub(crate) run_lease_id: String,
    /// Active Delivery-lane generation.
    pub(crate) delivery_generation: RuntimeGeneration,
    /// Exact active Delivery-lane lease.
    pub(crate) delivery_lease_id: String,
    /// Domain-separated hash of connector and destination identity.
    pub(crate) destination_binding_sha256: String,
    /// Existing connector selected by the host.
    pub(crate) connector_id: String,
    /// Deterministic envelope id used by the existing outbox.
    pub(crate) outbox_envelope_id: String,
    /// Final content hash copied from the artifact descriptor.
    pub(crate) content_sha256: String,
    /// Canonical hash of the complete effect-bearing outbox request.
    pub(crate) outbound_request_sha256: String,
    /// Exact end-to-end dedupe key.
    pub(crate) dedupe_key: String,
    /// Host commit timestamp.
    pub(crate) committed_at_unix_ms: i64,
}

impl RuntimeDeliveryIntentDescriptor {
    fn validate(&self) -> Result<(), JournalError> {
        if !valid_reference(&self.delivery_intent_id)
            || !valid_reference(&self.artifact_id)
            || !valid_reference(&self.session_id)
            || !valid_reference(&self.run_id)
            || !valid_reference(&self.run_lease_id)
            || !valid_reference(&self.delivery_lease_id)
            || !valid_reference(&self.connector_id)
            || !valid_reference(&self.outbox_envelope_id)
            || !valid_reference(&self.dedupe_key)
            || !valid_sha256(&self.destination_binding_sha256)
            || !valid_sha256(&self.content_sha256)
            || !valid_sha256(&self.outbound_request_sha256)
            || self.dedupe_key != format!("{}:{}", self.connector_id, self.outbox_envelope_id)
            || self.committed_at_unix_ms < 0
        {
            return Err(JournalError::InvalidRuntimeDeliveryIntent {
                reason: "identity, digest, dedupe key, or timestamp is invalid".to_owned(),
            });
        }
        Ok(())
    }
}

/// Current durable state of a delivery intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeDeliveryState {
    /// The intent exists only in the daemon journal.
    IntentRecorded,
    /// The deterministic envelope exists in the connector outbox.
    Queued,
    /// The connector reported an unresolved external outcome.
    OutcomeUnknown,
    /// The connector acknowledged delivery.
    Delivered,
}

/// Current durable delivery state and its original immutable evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeDeliverySnapshot {
    /// Highest-precedence durable state.
    pub(crate) state: RuntimeDeliveryState,
    /// Original link evidence for queued or terminal observations.
    pub(crate) evidence_sha256: Option<String>,
}

impl RuntimeDeliveryState {
    fn parse(value: &str) -> Result<Self, JournalError> {
        match value {
            "queued" => Ok(Self::Queued),
            "outcome_unknown" => Ok(Self::OutcomeUnknown),
            "delivered" => Ok(Self::Delivered),
            _ => Err(JournalError::InvalidRuntimeDeliveryIntent {
                reason: "delivery link has an unsupported state".to_owned(),
            }),
        }
    }

    const fn as_str(self) -> Option<&'static str> {
        match self {
            Self::IntentRecorded => None,
            Self::Queued => Some("queued"),
            Self::OutcomeUnknown => Some("outcome_unknown"),
            Self::Delivered => Some("delivered"),
        }
    }
}

/// One append-only connector-outbox observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeDeliveryLinkObservation {
    /// Delivery intent being advanced.
    pub(crate) delivery_intent_id: String,
    /// New durable state.
    pub(crate) state: RuntimeDeliveryState,
    /// Existing connector identity.
    pub(crate) connector_id: String,
    /// Existing outbox envelope identity.
    pub(crate) outbox_envelope_id: String,
    /// Hash-only connector evidence.
    pub(crate) evidence_sha256: String,
    /// Stable reason code.
    pub(crate) reason_code: String,
    /// Hash of the provider-native message id after acknowledgement.
    pub(crate) native_message_id_sha256: Option<String>,
    /// Observation timestamp.
    pub(crate) observed_at_unix_ms: i64,
}

/// Result of an idempotent immutable commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeFinalizationCommitOutcome {
    /// New evidence was inserted.
    Inserted,
    /// Byte-equivalent evidence already existed.
    Existing,
}

impl JournalStore {
    /// Commits exactly one final artifact for a Run generation.
    ///
    /// # Errors
    /// Returns [`JournalError`] for invalid evidence, stale lease authority,
    /// SQLite failures, or a conflicting second final artifact.
    pub(crate) fn commit_runtime_final_output(
        &self,
        descriptor: &FinalOutputArtifactDescriptor,
    ) -> Result<RuntimeFinalizationCommitOutcome, JournalError> {
        descriptor.validate()?;
        let generation = generation_sql(descriptor.run_generation)?;

        let mut connection = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = load_final_output_tx(
            &transaction,
            descriptor.run_id.as_str(),
            descriptor.run_generation,
        )? {
            if same_finalization_request(&existing, descriptor) {
                transaction.commit()?;
                return Ok(RuntimeFinalizationCommitOutcome::Existing);
            }
            return Err(JournalError::RuntimeFinalOutputConflict {
                run_id: descriptor.run_id.clone(),
            });
        }

        let committed_at_unix_ms = current_unix_ms()?;
        require_exact_lease(
            &transaction,
            descriptor.session_id.as_str(),
            descriptor.run_id.as_str(),
            RuntimeGenerationLane::Run,
            descriptor.run_generation,
            descriptor.run_lease_id.as_str(),
            committed_at_unix_ms,
        )?;
        let mut committed_descriptor = descriptor.clone();
        committed_descriptor.committed_at_unix_ms = committed_at_unix_ms;
        let descriptor_json = serde_json::to_string(&committed_descriptor)?;
        let descriptor_sha256 = super::sha256_hex(descriptor_json.as_bytes());
        let verification_json = serde_json::to_string(&committed_descriptor.verification_evidence)?;
        let missing_json = serde_json::to_string(&committed_descriptor.missing_artifacts)?;
        let process_json = serde_json::to_string(&committed_descriptor.active_process_state)?;

        transaction.execute(
            r#"
                INSERT INTO runtime_final_output_artifacts (
                    artifact_ulid, session_ulid, run_ulid, run_generation,
                    run_lease_ulid, terminal_outcome, content_sha256,
                    projection_sha256, user_visible, verification_evidence_json,
                    missing_artifacts_json, active_process_state_json, reason_code,
                    descriptor_json, descriptor_sha256, committed_at_unix_ms,
                    schema_version
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                    ?13, ?14, ?15, ?16, ?17
                )
            "#,
            params![
                committed_descriptor.artifact_id,
                committed_descriptor.session_id,
                committed_descriptor.run_id,
                generation,
                committed_descriptor.run_lease_id,
                committed_descriptor.terminal_outcome.as_str(),
                committed_descriptor.content_sha256,
                committed_descriptor.projection_sha256,
                if committed_descriptor.user_visible { 1_i64 } else { 0_i64 },
                verification_json,
                missing_json,
                process_json,
                committed_descriptor.reason_code,
                descriptor_json,
                descriptor_sha256,
                committed_at_unix_ms,
                FINALIZATION_SCHEMA_VERSION,
            ],
        )?;
        transaction.commit()?;
        Ok(RuntimeFinalizationCommitOutcome::Inserted)
    }

    /// Loads the immutable final descriptor for one generation.
    ///
    /// # Errors
    /// Returns a storage or validation error if durable evidence is malformed.
    #[cfg(test)]
    pub(crate) fn runtime_final_output(
        &self,
        run_id: &str,
        generation: RuntimeGeneration,
    ) -> Result<Option<FinalOutputArtifactDescriptor>, JournalError> {
        let connection = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_final_output_tx(&connection, run_id, generation)
    }

    /// Commits an immutable delivery intent before any connector outbox enqueue.
    ///
    /// # Errors
    /// Returns [`JournalError`] for stale authority, hidden or missing output,
    /// invalid evidence, or a conflicting intent/dedupe identity.
    pub(crate) fn commit_runtime_delivery_intent(
        &self,
        intent: &RuntimeDeliveryIntentDescriptor,
    ) -> Result<RuntimeFinalizationCommitOutcome, JournalError> {
        intent.validate()?;
        let mut connection = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) =
            load_delivery_intent_tx(&transaction, intent.delivery_intent_id.as_str())?
        {
            if same_delivery_intent_request(&existing, intent) {
                transaction.commit()?;
                return Ok(RuntimeFinalizationCommitOutcome::Existing);
            }
            return Err(JournalError::RuntimeDeliveryIntentConflict {
                intent_id: intent.delivery_intent_id.clone(),
            });
        }

        let committed_at_unix_ms = current_unix_ms()?;
        require_exact_lease(
            &transaction,
            intent.session_id.as_str(),
            intent.run_id.as_str(),
            RuntimeGenerationLane::Run,
            intent.run_generation,
            intent.run_lease_id.as_str(),
            committed_at_unix_ms,
        )?;
        require_exact_lease(
            &transaction,
            intent.session_id.as_str(),
            intent.run_id.as_str(),
            RuntimeGenerationLane::Delivery,
            intent.delivery_generation,
            intent.delivery_lease_id.as_str(),
            committed_at_unix_ms,
        )?;
        let artifact =
            load_final_output_tx(&transaction, intent.run_id.as_str(), intent.run_generation)?
                .ok_or_else(|| JournalError::RuntimeFinalOutputNotFound {
                    run_id: intent.run_id.clone(),
                    generation: intent.run_generation.get(),
                })?;
        if artifact.artifact_id != intent.artifact_id
            || artifact.content_sha256 != intent.content_sha256
        {
            return Err(JournalError::RuntimeDeliveryIntentConflict {
                intent_id: intent.delivery_intent_id.clone(),
            });
        }
        if !artifact.user_visible {
            return Err(JournalError::RuntimeDeliveryNotVisible {
                artifact_id: artifact.artifact_id,
            });
        }
        let mut committed_intent = intent.clone();
        committed_intent.committed_at_unix_ms = committed_at_unix_ms;
        let intent_json = serde_json::to_string(&committed_intent)?;
        let intent_sha256 = super::sha256_hex(intent_json.as_bytes());

        let inserted = transaction.execute(
            r#"
                INSERT OR IGNORE INTO runtime_delivery_intents_v2 (
                    delivery_intent_ulid, artifact_ulid, session_ulid, run_ulid,
                    run_generation, run_lease_ulid, delivery_generation,
                    delivery_lease_ulid, destination_binding_sha256, connector_id,
                    outbox_envelope_id, content_sha256, outbound_request_sha256,
                    dedupe_key, intent_json, intent_sha256, committed_at_unix_ms,
                    schema_version
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                    ?13, ?14, ?15, ?16, ?17, ?18
                )
            "#,
            params![
                committed_intent.delivery_intent_id,
                committed_intent.artifact_id,
                committed_intent.session_id,
                committed_intent.run_id,
                generation_sql(committed_intent.run_generation)?,
                committed_intent.run_lease_id,
                generation_sql(committed_intent.delivery_generation)?,
                committed_intent.delivery_lease_id,
                committed_intent.destination_binding_sha256,
                committed_intent.connector_id,
                committed_intent.outbox_envelope_id,
                committed_intent.content_sha256,
                committed_intent.outbound_request_sha256,
                committed_intent.dedupe_key,
                intent_json,
                intent_sha256,
                committed_at_unix_ms,
                FINALIZATION_SCHEMA_VERSION,
            ],
        )?;
        if inserted == 0 {
            return Err(JournalError::RuntimeDeliveryIntentConflict {
                intent_id: intent.delivery_intent_id.clone(),
            });
        }
        transaction.commit()?;
        Ok(RuntimeFinalizationCommitOutcome::Inserted)
    }

    /// Returns the current delivery state from immutable intent and link evidence.
    ///
    /// # Errors
    /// Returns a storage or validation error for malformed durable evidence.
    #[cfg(test)]
    pub(crate) fn runtime_delivery_state(
        &self,
        intent_id: &str,
    ) -> Result<Option<RuntimeDeliveryState>, JournalError> {
        self.runtime_delivery_snapshot(intent_id)
            .map(|snapshot| snapshot.map(|snapshot| snapshot.state))
    }

    /// Returns current delivery state with the original immutable evidence.
    ///
    /// # Errors
    /// Returns a storage or validation error for malformed durable evidence.
    pub(crate) fn runtime_delivery_snapshot(
        &self,
        intent_id: &str,
    ) -> Result<Option<RuntimeDeliverySnapshot>, JournalError> {
        let connection = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let Some(intent) = load_delivery_intent_tx(&connection, intent_id)? else {
            return Ok(None);
        };
        current_delivery_snapshot_tx(&connection, &intent).map(Some)
    }

    /// Loads and verifies one immutable delivery intent.
    ///
    /// # Errors
    /// Returns a storage or integrity error for malformed durable evidence.
    pub(crate) fn runtime_delivery_intent(
        &self,
        intent_id: &str,
    ) -> Result<Option<RuntimeDeliveryIntentDescriptor>, JournalError> {
        let connection = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_delivery_intent_tx(&connection, intent_id)
    }

    /// Appends one connector outbox link or acknowledgement observation.
    ///
    /// A delivered intent is immutable in history. An outcome-unknown intent
    /// retains its first uncertainty evidence until an explicit acknowledgement.
    ///
    /// # Errors
    /// Returns [`JournalError`] for malformed evidence, identity mismatch, or
    /// storage failures.
    pub(crate) fn record_runtime_delivery_link(
        &self,
        observation: &RuntimeDeliveryLinkObservation,
    ) -> Result<RuntimeDeliveryState, JournalError> {
        validate_link(observation)?;
        let mut connection = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let intent =
            load_delivery_intent_tx(&transaction, observation.delivery_intent_id.as_str())?
                .ok_or_else(|| JournalError::InvalidRuntimeDeliveryIntent {
                    reason: "delivery intent does not exist".to_owned(),
                })?;
        if intent.connector_id != observation.connector_id
            || intent.outbox_envelope_id != observation.outbox_envelope_id
        {
            return Err(JournalError::RuntimeDeliveryIntentConflict {
                intent_id: observation.delivery_intent_id.clone(),
            });
        }
        let current = current_delivery_state_tx(&transaction, &intent)?;
        if current == RuntimeDeliveryState::Delivered {
            transaction.commit()?;
            return Ok(RuntimeDeliveryState::Delivered);
        }
        if current == RuntimeDeliveryState::OutcomeUnknown
            && observation.state != RuntimeDeliveryState::Delivered
        {
            transaction.commit()?;
            return Ok(RuntimeDeliveryState::OutcomeUnknown);
        }
        let state = observation.state.as_str().ok_or_else(|| {
            JournalError::InvalidRuntimeDeliveryIntent {
                reason: "intent_recorded is not a connector observation".to_owned(),
            }
        })?;
        let link_index = transaction.query_row(
            "SELECT COALESCE(MAX(link_index), 0) + 1 FROM runtime_delivery_links_v2",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        let link = CanonicalDeliveryLink::from_observation(link_index, observation)?;
        let link_json = serde_json::to_string(&link)?;
        let link_sha256 = super::sha256_hex(link_json.as_bytes());
        transaction.execute(
            r#"
                INSERT OR IGNORE INTO runtime_delivery_links_v2 (
                    link_index, delivery_intent_ulid, state, connector_id, outbox_envelope_id,
                    evidence_sha256, reason_code, native_message_id_sha256,
                    observed_at_unix_ms, link_json, link_sha256, schema_version
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
            "#,
            params![
                link_index,
                observation.delivery_intent_id,
                state,
                observation.connector_id,
                observation.outbox_envelope_id,
                observation.evidence_sha256,
                observation.reason_code,
                observation.native_message_id_sha256,
                observation.observed_at_unix_ms,
                link_json,
                link_sha256,
                FINALIZATION_SCHEMA_VERSION,
            ],
        )?;
        let current = current_delivery_state_tx(&transaction, &intent)?;
        transaction.commit()?;
        Ok(current)
    }
}

fn validate_evidence_set(values: &[FinalizationEvidenceRef]) -> Result<(), JournalError> {
    if values.len() > MAX_FINALIZATION_REFERENCES || values.iter().any(|value| !value.validate()) {
        return Err(JournalError::InvalidRuntimeFinalOutput {
            reason: "finalization evidence references are invalid or unbounded".to_owned(),
        });
    }
    Ok(())
}

fn same_finalization_request(
    existing: &FinalOutputArtifactDescriptor,
    requested: &FinalOutputArtifactDescriptor,
) -> bool {
    existing.artifact_id == requested.artifact_id
        && existing.session_id == requested.session_id
        && existing.run_id == requested.run_id
        && existing.run_generation == requested.run_generation
        && existing.run_lease_id == requested.run_lease_id
        && existing.terminal_outcome == requested.terminal_outcome
        && existing.content_sha256 == requested.content_sha256
        && existing.projection_sha256 == requested.projection_sha256
        && existing.user_visible == requested.user_visible
        && existing.verification_evidence == requested.verification_evidence
        && existing.missing_artifacts == requested.missing_artifacts
        && existing.active_process_state == requested.active_process_state
        && existing.reason_code == requested.reason_code
}

fn same_delivery_intent_request(
    existing: &RuntimeDeliveryIntentDescriptor,
    requested: &RuntimeDeliveryIntentDescriptor,
) -> bool {
    existing.delivery_intent_id == requested.delivery_intent_id
        && existing.artifact_id == requested.artifact_id
        && existing.session_id == requested.session_id
        && existing.run_id == requested.run_id
        && existing.run_generation == requested.run_generation
        && existing.run_lease_id == requested.run_lease_id
        && existing.delivery_generation == requested.delivery_generation
        && existing.delivery_lease_id == requested.delivery_lease_id
        && existing.destination_binding_sha256 == requested.destination_binding_sha256
        && existing.connector_id == requested.connector_id
        && existing.outbox_envelope_id == requested.outbox_envelope_id
        && existing.content_sha256 == requested.content_sha256
        && existing.outbound_request_sha256 == requested.outbound_request_sha256
        && existing.dedupe_key == requested.dedupe_key
}

fn validate_link(observation: &RuntimeDeliveryLinkObservation) -> Result<(), JournalError> {
    let native_hash_valid =
        observation.native_message_id_sha256.as_deref().is_none_or(valid_sha256);
    if !valid_reference(&observation.delivery_intent_id)
        || !valid_reference(&observation.connector_id)
        || !valid_reference(&observation.outbox_envelope_id)
        || !valid_sha256(&observation.evidence_sha256)
        || !valid_label(&observation.reason_code)
        || !native_hash_valid
        || observation.observed_at_unix_ms < 0
        || observation.state == RuntimeDeliveryState::IntentRecorded
        || ((observation.state == RuntimeDeliveryState::Delivered)
            != observation.native_message_id_sha256.is_some())
    {
        return Err(JournalError::InvalidRuntimeDeliveryIntent {
            reason: "delivery link observation is invalid".to_owned(),
        });
    }
    Ok(())
}

fn require_exact_lease(
    connection: &rusqlite::Connection,
    session_id: &str,
    run_id: &str,
    lane: RuntimeGenerationLane,
    generation: RuntimeGeneration,
    lease_id: &str,
    now: i64,
) -> Result<(), JournalError> {
    let active = active_runtime_generation_tx(connection, session_id, run_id, lane, now)?;
    if active.as_ref().is_some_and(|active| {
        active.generation == generation && active.lease_id.as_str() == lease_id
    }) {
        return Ok(());
    }
    Err(JournalError::RuntimeFinalizationAuthorityStale { run_id: run_id.to_owned() })
}

fn load_final_output_tx(
    connection: &rusqlite::Connection,
    run_id: &str,
    generation: RuntimeGeneration,
) -> Result<Option<FinalOutputArtifactDescriptor>, JournalError> {
    let row = connection
        .query_row(
            r#"
                SELECT artifact_ulid, session_ulid, run_ulid, run_generation,
                       run_lease_ulid, terminal_outcome, content_sha256,
                       projection_sha256, user_visible, verification_evidence_json,
                       missing_artifacts_json, active_process_state_json, reason_code,
                       descriptor_json, descriptor_sha256, committed_at_unix_ms,
                       schema_version
                FROM runtime_final_output_artifacts
                WHERE run_ulid = ?1 AND run_generation = ?2
            "#,
            params![run_id, generation_sql(generation)?],
            |row| {
                Ok(StoredFinalOutputRow {
                    artifact_id: row.get(0)?,
                    session_id: row.get(1)?,
                    run_id: row.get(2)?,
                    run_generation: row.get(3)?,
                    run_lease_id: row.get(4)?,
                    terminal_outcome: row.get(5)?,
                    content_sha256: row.get(6)?,
                    projection_sha256: row.get(7)?,
                    user_visible: row.get(8)?,
                    verification_evidence_json: row.get(9)?,
                    missing_artifacts_json: row.get(10)?,
                    active_process_state_json: row.get(11)?,
                    reason_code: row.get(12)?,
                    descriptor_json: row.get(13)?,
                    descriptor_sha256: row.get(14)?,
                    committed_at_unix_ms: row.get(15)?,
                    schema_version: row.get(16)?,
                })
            },
        )
        .optional()?;
    row.map(|row| row.decode(run_id, generation)).transpose()
}

fn load_delivery_intent_tx(
    connection: &rusqlite::Connection,
    intent_id: &str,
) -> Result<Option<RuntimeDeliveryIntentDescriptor>, JournalError> {
    let row = connection
        .query_row(
            r#"
                SELECT delivery_intent_ulid, artifact_ulid, session_ulid, run_ulid,
                       run_generation, run_lease_ulid, delivery_generation,
                       delivery_lease_ulid, destination_binding_sha256, connector_id,
                       outbox_envelope_id, content_sha256, outbound_request_sha256,
                       dedupe_key, intent_json, intent_sha256, committed_at_unix_ms,
                       schema_version
                FROM runtime_delivery_intents_v2
                WHERE delivery_intent_ulid = ?1
            "#,
            params![intent_id],
            |row| {
                Ok(StoredDeliveryIntentRow {
                    delivery_intent_id: row.get(0)?,
                    artifact_id: row.get(1)?,
                    session_id: row.get(2)?,
                    run_id: row.get(3)?,
                    run_generation: row.get(4)?,
                    run_lease_id: row.get(5)?,
                    delivery_generation: row.get(6)?,
                    delivery_lease_id: row.get(7)?,
                    destination_binding_sha256: row.get(8)?,
                    connector_id: row.get(9)?,
                    outbox_envelope_id: row.get(10)?,
                    content_sha256: row.get(11)?,
                    outbound_request_sha256: row.get(12)?,
                    dedupe_key: row.get(13)?,
                    intent_json: row.get(14)?,
                    intent_sha256: row.get(15)?,
                    committed_at_unix_ms: row.get(16)?,
                    schema_version: row.get(17)?,
                })
            },
        )
        .optional()?;
    row.map(|row| row.decode(intent_id)).transpose()
}

struct StoredFinalOutputRow {
    artifact_id: String,
    session_id: String,
    run_id: String,
    run_generation: i64,
    run_lease_id: String,
    terminal_outcome: String,
    content_sha256: String,
    projection_sha256: String,
    user_visible: i64,
    verification_evidence_json: String,
    missing_artifacts_json: String,
    active_process_state_json: String,
    reason_code: String,
    descriptor_json: String,
    descriptor_sha256: String,
    committed_at_unix_ms: i64,
    schema_version: i64,
}

impl StoredFinalOutputRow {
    fn decode(
        self,
        expected_run_id: &str,
        expected_generation: RuntimeGeneration,
    ) -> Result<FinalOutputArtifactDescriptor, JournalError> {
        if self.schema_version != FINALIZATION_SCHEMA_VERSION {
            return Err(invalid_final_output_row("stored schema version is unsupported"));
        }
        if super::sha256_hex(self.descriptor_json.as_bytes()) != self.descriptor_sha256 {
            return Err(invalid_final_output_row("stored descriptor digest does not match"));
        }
        let descriptor: FinalOutputArtifactDescriptor =
            serde_json::from_str(self.descriptor_json.as_str())?;
        descriptor.validate()?;
        if serde_json::to_string(&descriptor)? != self.descriptor_json {
            return Err(invalid_final_output_row("stored descriptor JSON is not canonical"));
        }
        let verification_evidence_json = serde_json::to_string(&descriptor.verification_evidence)?;
        let missing_artifacts_json = serde_json::to_string(&descriptor.missing_artifacts)?;
        let active_process_state_json = serde_json::to_string(&descriptor.active_process_state)?;
        let user_visible = if descriptor.user_visible { 1 } else { 0 };
        if descriptor.run_id != expected_run_id
            || descriptor.run_generation != expected_generation
            || self.artifact_id != descriptor.artifact_id
            || self.session_id != descriptor.session_id
            || self.run_id != descriptor.run_id
            || self.run_generation != generation_sql(descriptor.run_generation)?
            || self.run_lease_id != descriptor.run_lease_id
            || self.terminal_outcome != descriptor.terminal_outcome.as_str()
            || self.content_sha256 != descriptor.content_sha256
            || self.projection_sha256 != descriptor.projection_sha256
            || self.user_visible != user_visible
            || self.verification_evidence_json != verification_evidence_json
            || self.missing_artifacts_json != missing_artifacts_json
            || self.active_process_state_json != active_process_state_json
            || self.reason_code != descriptor.reason_code
            || self.committed_at_unix_ms != descriptor.committed_at_unix_ms
        {
            return Err(invalid_final_output_row(
                "stored descriptor and denormalized columns disagree",
            ));
        }
        Ok(descriptor)
    }
}

struct StoredDeliveryIntentRow {
    delivery_intent_id: String,
    artifact_id: String,
    session_id: String,
    run_id: String,
    run_generation: i64,
    run_lease_id: String,
    delivery_generation: i64,
    delivery_lease_id: String,
    destination_binding_sha256: String,
    connector_id: String,
    outbox_envelope_id: String,
    content_sha256: String,
    outbound_request_sha256: String,
    dedupe_key: String,
    intent_json: String,
    intent_sha256: String,
    committed_at_unix_ms: i64,
    schema_version: i64,
}

impl StoredDeliveryIntentRow {
    fn decode(
        self,
        expected_intent_id: &str,
    ) -> Result<RuntimeDeliveryIntentDescriptor, JournalError> {
        if self.schema_version != FINALIZATION_SCHEMA_VERSION {
            return Err(invalid_delivery_intent_row("stored schema version is unsupported"));
        }
        if super::sha256_hex(self.intent_json.as_bytes()) != self.intent_sha256 {
            return Err(invalid_delivery_intent_row("stored intent digest does not match"));
        }
        let intent: RuntimeDeliveryIntentDescriptor =
            serde_json::from_str(self.intent_json.as_str())?;
        intent.validate()?;
        if serde_json::to_string(&intent)? != self.intent_json {
            return Err(invalid_delivery_intent_row("stored intent JSON is not canonical"));
        }
        if intent.delivery_intent_id != expected_intent_id
            || self.delivery_intent_id != intent.delivery_intent_id
            || self.artifact_id != intent.artifact_id
            || self.session_id != intent.session_id
            || self.run_id != intent.run_id
            || self.run_generation != generation_sql(intent.run_generation)?
            || self.run_lease_id != intent.run_lease_id
            || self.delivery_generation != generation_sql(intent.delivery_generation)?
            || self.delivery_lease_id != intent.delivery_lease_id
            || self.destination_binding_sha256 != intent.destination_binding_sha256
            || self.connector_id != intent.connector_id
            || self.outbox_envelope_id != intent.outbox_envelope_id
            || self.content_sha256 != intent.content_sha256
            || self.outbound_request_sha256 != intent.outbound_request_sha256
            || self.dedupe_key != intent.dedupe_key
            || self.committed_at_unix_ms != intent.committed_at_unix_ms
        {
            return Err(invalid_delivery_intent_row(
                "stored intent and denormalized columns disagree",
            ));
        }
        Ok(intent)
    }
}

fn invalid_final_output_row(reason: &str) -> JournalError {
    JournalError::InvalidRuntimeFinalOutput { reason: reason.to_owned() }
}

fn invalid_delivery_intent_row(reason: &str) -> JournalError {
    JournalError::InvalidRuntimeDeliveryIntent { reason: reason.to_owned() }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalDeliveryLink {
    link_index: i64,
    delivery_intent_id: String,
    state: String,
    connector_id: String,
    outbox_envelope_id: String,
    evidence_sha256: String,
    reason_code: String,
    native_message_id_sha256: Option<String>,
    observed_at_unix_ms: i64,
    schema_version: i64,
}

impl CanonicalDeliveryLink {
    fn from_observation(
        link_index: i64,
        observation: &RuntimeDeliveryLinkObservation,
    ) -> Result<Self, JournalError> {
        let state = observation.state.as_str().ok_or_else(|| {
            invalid_delivery_intent_row("intent_recorded is not a connector observation")
        })?;
        Ok(Self {
            link_index,
            delivery_intent_id: observation.delivery_intent_id.clone(),
            state: state.to_owned(),
            connector_id: observation.connector_id.clone(),
            outbox_envelope_id: observation.outbox_envelope_id.clone(),
            evidence_sha256: observation.evidence_sha256.clone(),
            reason_code: observation.reason_code.clone(),
            native_message_id_sha256: observation.native_message_id_sha256.clone(),
            observed_at_unix_ms: observation.observed_at_unix_ms,
            schema_version: FINALIZATION_SCHEMA_VERSION,
        })
    }

    fn observation(&self) -> Result<RuntimeDeliveryLinkObservation, JournalError> {
        let observation = RuntimeDeliveryLinkObservation {
            delivery_intent_id: self.delivery_intent_id.clone(),
            state: RuntimeDeliveryState::parse(self.state.as_str())?,
            connector_id: self.connector_id.clone(),
            outbox_envelope_id: self.outbox_envelope_id.clone(),
            evidence_sha256: self.evidence_sha256.clone(),
            reason_code: self.reason_code.clone(),
            native_message_id_sha256: self.native_message_id_sha256.clone(),
            observed_at_unix_ms: self.observed_at_unix_ms,
        };
        validate_link(&observation)?;
        Ok(observation)
    }
}

struct StoredDeliveryLinkRow {
    link_index: i64,
    delivery_intent_id: String,
    state: String,
    connector_id: String,
    outbox_envelope_id: String,
    evidence_sha256: String,
    reason_code: String,
    native_message_id_sha256: Option<String>,
    observed_at_unix_ms: i64,
    link_json: String,
    link_sha256: String,
    schema_version: i64,
}

impl StoredDeliveryLinkRow {
    fn decode(
        self,
        intent: &RuntimeDeliveryIntentDescriptor,
    ) -> Result<(i64, RuntimeDeliveryLinkObservation), JournalError> {
        if self.schema_version != FINALIZATION_SCHEMA_VERSION
            || super::sha256_hex(self.link_json.as_bytes()) != self.link_sha256
        {
            return Err(invalid_delivery_intent_row(
                "stored delivery link schema or digest is invalid",
            ));
        }
        let link: CanonicalDeliveryLink = serde_json::from_str(self.link_json.as_str())?;
        if serde_json::to_string(&link)? != self.link_json {
            return Err(invalid_delivery_intent_row("stored delivery link JSON is not canonical"));
        }
        let observation = link.observation()?;
        if self.link_index != link.link_index
            || self.delivery_intent_id != link.delivery_intent_id
            || self.state != link.state
            || self.connector_id != link.connector_id
            || self.outbox_envelope_id != link.outbox_envelope_id
            || self.evidence_sha256 != link.evidence_sha256
            || self.reason_code != link.reason_code
            || self.native_message_id_sha256 != link.native_message_id_sha256
            || self.observed_at_unix_ms != link.observed_at_unix_ms
            || self.schema_version != link.schema_version
        {
            return Err(invalid_delivery_intent_row(
                "stored delivery link and denormalized columns disagree",
            ));
        }
        if observation.delivery_intent_id != intent.delivery_intent_id
            || observation.connector_id != intent.connector_id
            || observation.outbox_envelope_id != intent.outbox_envelope_id
        {
            return Err(invalid_delivery_intent_row(
                "stored delivery link does not match its immutable intent",
            ));
        }
        Ok((self.link_index, observation))
    }
}

fn current_delivery_state_tx(
    connection: &rusqlite::Connection,
    intent: &RuntimeDeliveryIntentDescriptor,
) -> Result<RuntimeDeliveryState, JournalError> {
    current_delivery_snapshot_tx(connection, intent).map(|snapshot| snapshot.state)
}

/// Loads validated delivery posture for one exact RuntimeKernelV2 generation.
///
/// # Errors
/// Returns [`JournalError`] when immutable intent or link evidence is malformed
/// or contradicts the requested session, Run, or generation.
pub(super) fn runtime_delivery_state_for_run_generation_tx(
    connection: &rusqlite::Connection,
    session_id: &str,
    run_id: &str,
    generation: RuntimeGeneration,
) -> Result<Option<RuntimeDeliveryState>, JournalError> {
    let intent_id = connection
        .query_row(
            r#"
                SELECT delivery_intent_ulid
                FROM runtime_delivery_intents_v2
                WHERE run_ulid = ?1 AND run_generation = ?2
            "#,
            params![run_id, generation_sql(generation)?],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let Some(intent_id) = intent_id else {
        return Ok(None);
    };
    let intent = load_delivery_intent_tx(connection, intent_id.as_str())?.ok_or_else(|| {
        invalid_delivery_intent_row("indexed delivery intent could not be loaded")
    })?;
    if intent.session_id != session_id
        || intent.run_id != run_id
        || intent.run_generation != generation
    {
        return Err(invalid_delivery_intent_row(
            "stored delivery intent does not match requested runtime generation",
        ));
    }
    current_delivery_state_tx(connection, &intent).map(Some)
}

fn current_delivery_snapshot_tx(
    connection: &rusqlite::Connection,
    intent: &RuntimeDeliveryIntentDescriptor,
) -> Result<RuntimeDeliverySnapshot, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT link_index, delivery_intent_ulid, state, connector_id,
                   outbox_envelope_id, evidence_sha256, reason_code,
                   native_message_id_sha256, observed_at_unix_ms, link_json,
                   link_sha256, schema_version
            FROM runtime_delivery_links_v2
            WHERE delivery_intent_ulid = ?1
               OR json_extract(link_json, '$.delivery_intent_id') = ?1
            ORDER BY link_index ASC
        "#,
    )?;
    let rows = statement.query_map(params![intent.delivery_intent_id.as_str()], |row| {
        Ok(StoredDeliveryLinkRow {
            link_index: row.get(0)?,
            delivery_intent_id: row.get(1)?,
            state: row.get(2)?,
            connector_id: row.get(3)?,
            outbox_envelope_id: row.get(4)?,
            evidence_sha256: row.get(5)?,
            reason_code: row.get(6)?,
            native_message_id_sha256: row.get(7)?,
            observed_at_unix_ms: row.get(8)?,
            link_json: row.get(9)?,
            link_sha256: row.get(10)?,
            schema_version: row.get(11)?,
        })
    })?;
    let mut selected: Option<(u8, i64, RuntimeDeliveryLinkObservation)> = None;
    for row in rows {
        let (link_index, observation) = row?.decode(intent)?;
        let rank = match observation.state {
            RuntimeDeliveryState::IntentRecorded => 0,
            RuntimeDeliveryState::Queued => 1,
            RuntimeDeliveryState::OutcomeUnknown => 2,
            RuntimeDeliveryState::Delivered => 3,
        };
        if selected.as_ref().is_none_or(|(current_rank, current_index, _)| {
            (rank, link_index) > (*current_rank, *current_index)
        }) {
            selected = Some((rank, link_index, observation));
        }
    }
    let Some((_, _, observation)) = selected else {
        return Ok(RuntimeDeliverySnapshot {
            state: RuntimeDeliveryState::IntentRecorded,
            evidence_sha256: None,
        });
    };
    Ok(RuntimeDeliverySnapshot {
        state: observation.state,
        evidence_sha256: Some(observation.evidence_sha256),
    })
}

fn generation_sql(generation: RuntimeGeneration) -> Result<i64, JournalError> {
    i64::try_from(generation.get())
        .map_err(|_| JournalError::InvalidRuntimeKernelGeneration { generation: generation.get() })
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_reference(value: &str) -> bool {
    let value = value.as_bytes();
    !value.is_empty()
        && value.len() <= MAX_REFERENCE_BYTES
        && value.iter().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
}

fn valid_label(value: &str) -> bool {
    valid_reference(value)
}

/// Returns the journal clock for finalization adapters that do not own one.
pub(crate) fn runtime_finalization_now() -> Result<i64, JournalError> {
    current_unix_ms()
}
