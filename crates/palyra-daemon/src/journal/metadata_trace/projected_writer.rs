//! Atomic allocation and persistence for metadata projected from tape rows.
//!
//! Sequence, generation, and causal-parent allocation share one SQLite
//! transaction so concurrent producers cannot race on trace identity.

use palyra_common::metadata_trace::{
    MetadataTraceCapacityLimitV1, MetadataTraceEventDataV1, METADATA_TRACE_MAX_EVENTS,
};
use rusqlite::{params, OptionalExtension};

use crate::metadata_trace::{
    metadata_trace_capacity_reached_event, project_orchestrator_tape_record,
    MetadataTraceProjectionContext,
};

use super::super::OrchestratorTapeRecord;
use super::{
    current_unix_ms, insert_event_tx, latest_segment_tx, run_event_count_tx, segment_statuses_tx,
    validate_event_append_tx, JournalError, JournalStore,
};

impl JournalStore {
    /// Projects and appends one tape row with atomically allocated trace identity.
    ///
    /// Returns `false` when the closed projector intentionally ignores the tape
    /// event. Terminal projections use the idempotent terminal writer so event
    /// insertion and segment closure remain one operation.
    ///
    /// # Errors
    /// Returns [`JournalError`] when the active trace is malformed, a hard cap
    /// is reached, projection timestamps cannot be represented, or SQLite fails.
    pub(crate) fn append_projected_metadata_trace_event(
        &self,
        run_id: &str,
        record: &OrchestratorTapeRecord,
    ) -> Result<bool, JournalError> {
        let now = current_unix_ms()?;
        let recorded_at_unix_ms =
            u64::try_from(now).map_err(|_| JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.timestamp_out_of_range",
            })?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;
        let segment = latest_segment_tx(&transaction, run_id)?;
        if !segment_statuses_tx(&transaction, run_id, segment.segment_id.as_str())?.is_empty() {
            return Ok(false);
        }
        let event_count = run_event_count_tx(&transaction, run_id)?;
        let sequence =
            u32::try_from(event_count).map_err(|_| JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.event_sequence_out_of_range",
            })?;
        let generation = u32::try_from(segment.generation).map_err(|_| {
            JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.generation_out_of_range",
            }
        })?;
        let parent_event_id = transaction
            .query_row(
                r#"
                    SELECT event_id_sha256
                    FROM metadata_trace_events
                    WHERE run_ulid = ?1
                    ORDER BY sequence DESC
                    LIMIT 1
                "#,
                params![run_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.root_event_missing",
            })?;
        let context = MetadataTraceProjectionContext {
            run_id,
            sequence,
            generation,
            recorded_at_unix_ms,
            causal_parent_event_id_sha256: Some(parent_event_id.as_str()),
        };
        let Some(event) = project_orchestrator_tape_record(record, context) else {
            return Ok(false);
        };
        if let MetadataTraceEventDataV1::Terminalization(metadata) = &event.event {
            let metadata = metadata.clone();
            drop(transaction);
            drop(guard);
            return self.append_metadata_trace_terminalization(
                run_id,
                metadata.outcome,
                metadata.reason_code.as_str(),
                metadata.output_emitted,
                metadata.side_effect_may_have_occurred,
            );
        }
        if event_count >= METADATA_TRACE_MAX_EVENTS.saturating_sub(2) {
            let capacity_already_recorded = transaction.query_row(
                r#"
                    SELECT EXISTS(
                        SELECT 1
                        FROM metadata_trace_events
                        WHERE segment_ulid = ?1
                          AND event_kind = 'capacity_reached'
                    )
                "#,
                params![segment.segment_id],
                |row| row.get::<_, i64>(0),
            )? != 0;
            if capacity_already_recorded {
                return Ok(false);
            }
            let limit = u32::try_from(METADATA_TRACE_MAX_EVENTS).map_err(|_| {
                JournalError::MetadataTraceInvariant {
                    run_id: run_id.to_owned(),
                    reason_code: "metadata_trace.event_limit_out_of_range",
                }
            })?;
            let capacity_event = metadata_trace_capacity_reached_event(
                context,
                MetadataTraceCapacityLimitV1::EventCount,
                limit,
                limit,
                "metadata_trace.event_count_reached",
            )
            .ok_or_else(|| JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.capacity_event_invalid",
            })?;
            let event_json =
                validate_event_append_tx(&transaction, run_id, &segment, &capacity_event)?;
            insert_event_tx(
                &transaction,
                run_id,
                segment.segment_id.as_str(),
                &capacity_event,
                event_json.as_str(),
                now,
            )?;
            transaction.commit()?;
            return Ok(true);
        }
        let event_json = validate_event_append_tx(&transaction, run_id, &segment, &event)?;
        insert_event_tx(
            &transaction,
            run_id,
            segment.segment_id.as_str(),
            &event,
            event_json.as_str(),
            now,
        )?;
        transaction.commit()?;
        Ok(true)
    }
}
