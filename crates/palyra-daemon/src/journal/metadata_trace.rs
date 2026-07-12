//! Durable storage for bounded, redacted run metadata traces.
//!
//! The tables are immutable ledgers; read-time validation exposes only the
//! longest valid prefix and isolates any corrupt suffix.

use std::collections::BTreeSet;

use palyra_common::metadata_trace::{
    metadata_trace_id_sha256, MetadataTraceEntrypointV1, MetadataTraceEventDataV1,
    MetadataTraceEventV1, MetadataTraceIdDomainV1, MetadataTraceSegmentStatusV1,
    MetadataTraceSegmentV1, MetadataTraceTerminalOutcomeV1, MetadataTraceV1,
    RecoveryContinuationMetadataV1, RunStartedMetadataV1, TerminalizationMetadataV1,
    METADATA_TRACE_MAX_EVENTS, METADATA_TRACE_MAX_EVENT_BYTES, METADATA_TRACE_MAX_SEGMENTS,
    METADATA_TRACE_SCHEMA_VERSION,
};
use rusqlite::{params, Connection, OptionalExtension};
use ulid::Ulid;

use super::{
    current_unix_ms, JournalError, JournalStore, MetadataTraceSegmentPosition,
    OrchestratorRunStartRequest,
};

const RESERVED_TERMINAL_EVENTS: usize = 2;
const MAX_REASON_CODE_BYTES: usize = 96;

mod migration;
mod model;
mod projected_writer;
#[cfg(test)]
mod tests;
mod types;

pub(super) const MIGRATION_44_SQL: &str = migration::SQL;
use model::{event_kind, event_uses_terminal_reserve, parse_status, status_name};
use types::SegmentRow;

fn hash_identifier(
    run_id: &str,
    domain: MetadataTraceIdDomainV1,
    raw: &str,
) -> Result<String, JournalError> {
    metadata_trace_id_sha256(domain, raw).map_err(|_| JournalError::MetadataTraceInvariant {
        run_id: run_id.to_owned(),
        reason_code: "metadata_trace.invalid_identifier",
    })
}

fn validate_reason_code(run_id: &str, reason_code: &str) -> Result<(), JournalError> {
    if reason_code.is_empty()
        || reason_code.len() > MAX_REASON_CODE_BYTES
        || !reason_code.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(JournalError::MetadataTraceInvariant {
            run_id: run_id.to_owned(),
            reason_code: "metadata_trace.invalid_reason_code",
        });
    }
    Ok(())
}

fn serialize_event(run_id: &str, event: &MetadataTraceEventV1) -> Result<String, JournalError> {
    event.validate_shape().map_err(|_| JournalError::MetadataTraceInvariant {
        run_id: run_id.to_owned(),
        reason_code: "metadata_trace.invalid_event_shape",
    })?;
    let event_json = serde_json::to_string(event)?;
    if event_json.len() > METADATA_TRACE_MAX_EVENT_BYTES {
        return Err(JournalError::PayloadTooLarge {
            payload_kind: "metadata_trace_event",
            actual_bytes: event_json.len(),
            max_bytes: METADATA_TRACE_MAX_EVENT_BYTES,
        });
    }
    Ok(event_json)
}

fn insert_segment_tx(
    connection: &Connection,
    request: &OrchestratorRunStartRequest,
    segment_id: &str,
    segment_index: u16,
    generation: u32,
    predecessor_segment_id: Option<&str>,
    opened_at_unix_ms: i64,
) -> Result<(), JournalError> {
    connection.execute(
        r#"
            INSERT INTO metadata_trace_segments (
                segment_ulid,
                run_ulid,
                session_ulid,
                segment_index,
                generation,
                predecessor_segment_ulid,
                schema_version,
                opened_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        "#,
        params![
            segment_id,
            request.run_id,
            request.session_id,
            i64::from(segment_index),
            i64::from(generation),
            predecessor_segment_id,
            i64::from(METADATA_TRACE_SCHEMA_VERSION),
            opened_at_unix_ms,
        ],
    )?;
    Ok(())
}

fn insert_event_tx(
    connection: &Connection,
    run_id: &str,
    segment_id: &str,
    event: &MetadataTraceEventV1,
    event_json: &str,
    created_at_unix_ms: i64,
) -> Result<(), JournalError> {
    let recorded_at_unix_ms = i64::try_from(event.recorded_at_unix_ms).map_err(|_| {
        JournalError::MetadataTraceInvariant {
            run_id: run_id.to_owned(),
            reason_code: "metadata_trace.timestamp_out_of_range",
        }
    })?;
    connection.execute(
        r#"
            INSERT INTO metadata_trace_events (
                event_id_sha256,
                run_ulid,
                segment_ulid,
                sequence,
                generation,
                causal_parent_event_id_sha256,
                event_kind,
                event_json,
                recorded_at_unix_ms,
                created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        "#,
        params![
            event.event_id_sha256,
            run_id,
            segment_id,
            i64::from(event.sequence),
            i64::from(event.generation),
            event.causal_parent_event_id_sha256,
            event_kind(&event.event),
            event_json,
            recorded_at_unix_ms,
            created_at_unix_ms,
        ],
    )?;
    Ok(())
}

/// Creates the root trace segment and initial event inside the run-start transaction.
pub(super) fn create_root_metadata_trace_tx(
    connection: &Connection,
    request: &OrchestratorRunStartRequest,
    now_unix_ms: i64,
) -> Result<MetadataTraceSegmentPosition, JournalError> {
    create_root_metadata_trace_for_entrypoint_tx(
        connection,
        request,
        MetadataTraceEntrypointV1::NewRun,
        now_unix_ms,
    )
}

fn create_root_metadata_trace_for_entrypoint_tx(
    connection: &Connection,
    request: &OrchestratorRunStartRequest,
    entrypoint: MetadataTraceEntrypointV1,
    now_unix_ms: i64,
) -> Result<MetadataTraceSegmentPosition, JournalError> {
    let recorded_at_unix_ms =
        u64::try_from(now_unix_ms).map_err(|_| JournalError::MetadataTraceInvariant {
            run_id: request.run_id.clone(),
            reason_code: "metadata_trace.timestamp_out_of_range",
        })?;
    let segment_id = Ulid::new().to_string();
    let event_identity = Ulid::new().to_string();
    let event = MetadataTraceEventV1 {
        sequence: 0,
        generation: 1,
        recorded_at_unix_ms,
        event_id_sha256: hash_identifier(
            request.run_id.as_str(),
            MetadataTraceIdDomainV1::Event,
            event_identity.as_str(),
        )?,
        causal_parent_event_id_sha256: None,
        stage_duration_ms: None,
        event: MetadataTraceEventDataV1::RunStarted(RunStartedMetadataV1 { entrypoint }),
    };
    let event_json = serialize_event(request.run_id.as_str(), &event)?;
    insert_segment_tx(connection, request, segment_id.as_str(), 0, 1, None, now_unix_ms)?;
    insert_event_tx(
        connection,
        request.run_id.as_str(),
        segment_id.as_str(),
        &event,
        event_json.as_str(),
        now_unix_ms,
    )?;
    Ok(MetadataTraceSegmentPosition { segment_index: 0, generation: 1 })
}

fn latest_segment_tx(connection: &Connection, run_id: &str) -> Result<SegmentRow, JournalError> {
    latest_segment_optional_tx(connection, run_id)?.ok_or_else(|| {
        JournalError::MetadataTraceInvariant {
            run_id: run_id.to_owned(),
            reason_code: "metadata_trace.root_segment_missing",
        }
    })
}

fn latest_segment_optional_tx(
    connection: &Connection,
    run_id: &str,
) -> Result<Option<SegmentRow>, JournalError> {
    Ok(connection
        .query_row(
            r#"
                SELECT
                    segment_ulid,
                    segment_index,
                    generation,
                    predecessor_segment_ulid,
                    schema_version
                FROM metadata_trace_segments
                WHERE run_ulid = ?1
                ORDER BY segment_index DESC
                LIMIT 1
            "#,
            params![run_id],
            |row| {
                Ok(SegmentRow {
                    segment_id: row.get(0)?,
                    segment_index: row.get(1)?,
                    generation: row.get(2)?,
                    predecessor_segment_id: row.get(3)?,
                    schema_version: row.get(4)?,
                })
            },
        )
        .optional()?)
}

fn run_event_count_tx(connection: &Connection, run_id: &str) -> Result<usize, JournalError> {
    let count = connection.query_row(
        "SELECT COUNT(*) FROM metadata_trace_events WHERE run_ulid = ?1",
        params![run_id],
        |row| row.get::<_, i64>(0),
    )?;
    usize::try_from(count).map_err(|_| JournalError::MetadataTraceInvariant {
        run_id: run_id.to_owned(),
        reason_code: "metadata_trace.invalid_event_count",
    })
}

fn segment_statuses_tx(
    connection: &Connection,
    run_id: &str,
    segment_id: &str,
) -> Result<Vec<MetadataTraceSegmentStatusV1>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT status_ordinal, status
            FROM metadata_trace_segment_status_events
            WHERE segment_ulid = ?1
            ORDER BY status_ordinal ASC
        "#,
    )?;
    let rows = statement.query_map(params![segment_id], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut statuses = Vec::new();
    for (expected_ordinal, row) in rows.enumerate() {
        let (ordinal, status) = row?;
        let expected_ordinal =
            i64::try_from(expected_ordinal).map_err(|_| JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.status_ordinal_out_of_range",
            })?;
        if ordinal != expected_ordinal {
            return Err(JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.status_sequence_gap",
            });
        }
        let status =
            parse_status(status.as_str()).ok_or_else(|| JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.unknown_segment_status",
            })?;
        statuses.push(status);
    }
    Ok(statuses)
}

fn insert_status_tx(
    connection: &Connection,
    run_id: &str,
    segment_id: &str,
    status_ordinal: usize,
    status: &MetadataTraceSegmentStatusV1,
    reason_code: &str,
    now_unix_ms: i64,
) -> Result<(), JournalError> {
    connection.execute(
        r#"
            INSERT INTO metadata_trace_segment_status_events (
                status_event_ulid,
                run_ulid,
                segment_ulid,
                status_ordinal,
                status,
                reason_code,
                created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        "#,
        params![
            Ulid::new().to_string(),
            run_id,
            segment_id,
            i64::try_from(status_ordinal).map_err(|_| {
                JournalError::MetadataTraceInvariant {
                    run_id: run_id.to_owned(),
                    reason_code: "metadata_trace.status_ordinal_out_of_range",
                }
            })?,
            status_name(status),
            reason_code,
            now_unix_ms,
        ],
    )?;
    Ok(())
}

fn validate_event_append_tx(
    connection: &Connection,
    run_id: &str,
    segment: &SegmentRow,
    event: &MetadataTraceEventV1,
) -> Result<String, JournalError> {
    if !segment_statuses_tx(connection, run_id, segment.segment_id.as_str())?.is_empty() {
        return Err(JournalError::MetadataTraceInvariant {
            run_id: run_id.to_owned(),
            reason_code: "metadata_trace.segment_already_closed",
        });
    }
    let current_events = run_event_count_tx(connection, run_id)?;
    if current_events >= METADATA_TRACE_MAX_EVENTS {
        return Err(JournalError::MetadataTraceCapacityExceeded {
            run_id: run_id.to_owned(),
            resource: "events",
            current: current_events,
            maximum: METADATA_TRACE_MAX_EVENTS,
        });
    }
    let is_capacity_marker = matches!(&event.event, MetadataTraceEventDataV1::CapacityReached(_));
    let is_terminalization = matches!(&event.event, MetadataTraceEventDataV1::Terminalization(_));
    if current_events >= METADATA_TRACE_MAX_EVENTS - RESERVED_TERMINAL_EVENTS
        && !event_uses_terminal_reserve(&event.event)
    {
        return Err(JournalError::MetadataTraceCapacityExceeded {
            run_id: run_id.to_owned(),
            resource: "non_terminal_events",
            current: current_events,
            maximum: METADATA_TRACE_MAX_EVENTS - RESERVED_TERMINAL_EVENTS,
        });
    }
    if current_events >= METADATA_TRACE_MAX_EVENTS - 1 && !is_terminalization {
        return Err(JournalError::MetadataTraceCapacityExceeded {
            run_id: run_id.to_owned(),
            resource: "terminal_reserve",
            current: current_events,
            maximum: METADATA_TRACE_MAX_EVENTS - 1,
        });
    }
    if is_capacity_marker {
        let already_recorded = connection.query_row(
            r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM metadata_trace_events
                    WHERE run_ulid = ?1
                      AND event_kind = 'capacity_reached'
                )
            "#,
            params![run_id],
            |row| row.get::<_, i64>(0),
        )? != 0;
        if already_recorded {
            return Err(JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.capacity_event_duplicate",
            });
        }
    }
    let expected_sequence =
        u32::try_from(current_events).map_err(|_| JournalError::MetadataTraceInvariant {
            run_id: run_id.to_owned(),
            reason_code: "metadata_trace.event_sequence_out_of_range",
        })?;
    if event.sequence != expected_sequence {
        return Err(JournalError::MetadataTraceSequenceMismatch {
            run_id: run_id.to_owned(),
            expected: expected_sequence,
            actual: event.sequence,
        });
    }
    let generation =
        u32::try_from(segment.generation).map_err(|_| JournalError::MetadataTraceInvariant {
            run_id: run_id.to_owned(),
            reason_code: "metadata_trace.segment_generation_out_of_range",
        })?;
    if event.generation != generation {
        return Err(JournalError::MetadataTraceInvariant {
            run_id: run_id.to_owned(),
            reason_code: "metadata_trace.event_generation_mismatch",
        });
    }
    let parent = event.causal_parent_event_id_sha256.as_deref().ok_or_else(|| {
        JournalError::MetadataTraceInvariant {
            run_id: run_id.to_owned(),
            reason_code: "metadata_trace.causal_parent_missing",
        }
    })?;
    let parent_exists = connection.query_row(
        r#"
            SELECT EXISTS(
                SELECT 1
                FROM metadata_trace_events
                WHERE run_ulid = ?1
                  AND event_id_sha256 = ?2
                  AND sequence < ?3
            )
        "#,
        params![run_id, parent, i64::from(event.sequence)],
        |row| row.get::<_, i64>(0),
    )? != 0;
    if !parent_exists {
        return Err(JournalError::MetadataTraceInvariant {
            run_id: run_id.to_owned(),
            reason_code: "metadata_trace.causal_parent_unknown",
        });
    }
    serialize_event(run_id, event)
}

impl JournalStore {
    #[cfg(test)]
    /// Appends one already-redacted, typed event to the active trace segment.
    ///
    /// # Errors
    /// Returns [`JournalError`] when the event violates sequence, causal,
    /// generation, size, or capacity invariants, or when SQLite persistence fails.
    pub(crate) fn append_metadata_trace_event(
        &self,
        run_id: &str,
        event: &MetadataTraceEventV1,
    ) -> Result<(), JournalError> {
        if matches!(&event.event, MetadataTraceEventDataV1::Terminalization(_)) {
            return Err(JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.terminal_writer_required",
            });
        }
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;
        let segment = latest_segment_tx(&transaction, run_id)?;
        let event_json = validate_event_append_tx(&transaction, run_id, &segment, event)?;
        insert_event_tx(
            &transaction,
            run_id,
            segment.segment_id.as_str(),
            event,
            event_json.as_str(),
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Appends the single terminal event for the active generation and closes its segment.
    ///
    /// A repeated call after a durable terminal event is an idempotent no-op.
    ///
    /// # Errors
    /// Returns [`JournalError`] when the active segment is already closed
    /// inconsistently, the trace is malformed, or SQLite persistence fails.
    pub(crate) fn append_metadata_trace_terminalization(
        &self,
        run_id: &str,
        outcome: MetadataTraceTerminalOutcomeV1,
        reason_code: &str,
        output_emitted: bool,
        side_effect_may_have_occurred: bool,
    ) -> Result<bool, JournalError> {
        validate_reason_code(run_id, reason_code)?;
        let now = current_unix_ms()?;
        let recorded_at_unix_ms =
            u64::try_from(now).map_err(|_| JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.timestamp_out_of_range",
            })?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;
        let segment = latest_segment_tx(&transaction, run_id)?;
        let statuses = segment_statuses_tx(&transaction, run_id, segment.segment_id.as_str())?;
        let existing_terminal = transaction.query_row(
            r#"
                    SELECT EXISTS(
                        SELECT 1
                        FROM metadata_trace_events
                        WHERE segment_ulid = ?1
                          AND event_kind = 'terminalization'
                    )
                "#,
            params![segment.segment_id],
            |row| row.get::<_, i64>(0),
        )? != 0;
        if existing_terminal {
            match statuses.as_slice() {
                [] => {
                    insert_status_tx(
                        &transaction,
                        run_id,
                        segment.segment_id.as_str(),
                        0,
                        &MetadataTraceSegmentStatusV1::Complete,
                        reason_code,
                        now,
                    )?;
                    transaction.commit()?;
                }
                [MetadataTraceSegmentStatusV1::Complete] => {}
                _ => {
                    return Err(JournalError::MetadataTraceInvariant {
                        run_id: run_id.to_owned(),
                        reason_code: "metadata_trace.terminal_status_conflict",
                    });
                }
            }
            return Ok(false);
        }
        if !statuses.is_empty() {
            return Err(JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.closed_segment_missing_terminalization",
            });
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
        let event_identity = Ulid::new().to_string();
        let event = MetadataTraceEventV1 {
            sequence,
            generation,
            recorded_at_unix_ms,
            event_id_sha256: hash_identifier(
                run_id,
                MetadataTraceIdDomainV1::Event,
                event_identity.as_str(),
            )?,
            causal_parent_event_id_sha256: Some(parent_event_id),
            stage_duration_ms: None,
            event: MetadataTraceEventDataV1::Terminalization(TerminalizationMetadataV1 {
                outcome,
                reason_code: reason_code.to_owned(),
                output_emitted,
                side_effect_may_have_occurred,
            }),
        };
        let event_json = validate_event_append_tx(&transaction, run_id, &segment, &event)?;
        insert_event_tx(
            &transaction,
            run_id,
            segment.segment_id.as_str(),
            &event,
            event_json.as_str(),
            now,
        )?;
        insert_status_tx(
            &transaction,
            run_id,
            segment.segment_id.as_str(),
            0,
            &MetadataTraceSegmentStatusV1::Complete,
            reason_code,
            now,
        )?;
        transaction.commit()?;
        Ok(true)
    }

    /// Records terminal metadata without allowing trace degradation to block the run outcome.
    ///
    /// Failures emit only the domain-separated run digest and a stable reason code.
    pub(super) fn append_metadata_trace_terminalization_best_effort(
        &self,
        run_id: &str,
        outcome: MetadataTraceTerminalOutcomeV1,
        reason_code: &'static str,
    ) {
        let output_emitted =
            self.metadata_trace_event_kind_exists(run_id, "delivery_intent").unwrap_or(false);
        if self
            .append_metadata_trace_terminalization(
                run_id,
                outcome,
                reason_code,
                output_emitted,
                true,
            )
            .is_err()
        {
            let run_id_sha256 = metadata_trace_id_sha256(MetadataTraceIdDomainV1::Run, run_id)
                .unwrap_or_else(|_| "invalid".to_owned());
            tracing::warn!(
                run_id_sha256,
                reason_code = "metadata_trace.terminalization_write_failed",
                "metadata trace terminalization could not be persisted"
            );
        }
    }

    fn metadata_trace_event_kind_exists(
        &self,
        run_id: &str,
        event_kind: &str,
    ) -> Result<bool, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let exists = guard.query_row(
            r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM metadata_trace_events
                    WHERE run_ulid = ?1
                      AND event_kind = ?2
                )
            "#,
            params![run_id, event_kind],
            |row| row.get::<_, i64>(0),
        )? != 0;
        Ok(exists)
    }

    /// Closes the active segment as interrupted and starts its recovery continuation.
    ///
    /// The status, continuation segment, and first causal event commit atomically.
    ///
    /// # Errors
    /// Returns [`JournalError`] when the current segment cannot be continued,
    /// a hard cap is reached, or SQLite persistence fails.
    pub(crate) fn start_metadata_trace_recovery_continuation(
        &self,
        run_id: &str,
        reason_code: &str,
    ) -> Result<MetadataTraceSegmentPosition, JournalError> {
        validate_reason_code(run_id, reason_code)?;
        let now = current_unix_ms()?;
        let recorded_at_unix_ms =
            u64::try_from(now).map_err(|_| JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.timestamp_out_of_range",
            })?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;
        let session_id = transaction
            .query_row(
                "SELECT session_ulid FROM orchestrator_runs WHERE run_ulid = ?1",
                params![run_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| JournalError::RunNotFound { run_id: run_id.to_owned() })?;
        let request = OrchestratorRunStartRequest {
            run_id: run_id.to_owned(),
            session_id,
            origin_kind: String::new(),
            origin_run_id: None,
            triggered_by_principal: None,
            parameter_delta_json: None,
        };
        let Some(previous) = latest_segment_optional_tx(&transaction, run_id)? else {
            let position = create_root_metadata_trace_for_entrypoint_tx(
                &transaction,
                &request,
                MetadataTraceEntrypointV1::Recovery,
                now,
            )?;
            transaction.commit()?;
            return Ok(position);
        };
        let statuses = segment_statuses_tx(&transaction, run_id, previous.segment_id.as_str())?;
        match statuses.as_slice() {
            [] => insert_status_tx(
                &transaction,
                run_id,
                previous.segment_id.as_str(),
                0,
                &MetadataTraceSegmentStatusV1::Interrupted,
                reason_code,
                now,
            )?,
            [MetadataTraceSegmentStatusV1::Interrupted] => {}
            _ => {
                return Err(JournalError::MetadataTraceInvariant {
                    run_id: run_id.to_owned(),
                    reason_code: "metadata_trace.closed_segment_cannot_continue",
                });
            }
        }

        let segment_count = transaction.query_row(
            "SELECT COUNT(*) FROM metadata_trace_segments WHERE run_ulid = ?1",
            params![run_id],
            |row| row.get::<_, i64>(0),
        )?;
        let segment_count =
            usize::try_from(segment_count).map_err(|_| JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.invalid_segment_count",
            })?;
        if segment_count >= METADATA_TRACE_MAX_SEGMENTS {
            return Err(JournalError::MetadataTraceCapacityExceeded {
                run_id: run_id.to_owned(),
                resource: "segments",
                current: segment_count,
                maximum: METADATA_TRACE_MAX_SEGMENTS,
            });
        }
        let event_count = run_event_count_tx(&transaction, run_id)?;
        if event_count >= METADATA_TRACE_MAX_EVENTS - RESERVED_TERMINAL_EVENTS {
            return Err(JournalError::MetadataTraceCapacityExceeded {
                run_id: run_id.to_owned(),
                resource: "non_terminal_events",
                current: event_count,
                maximum: METADATA_TRACE_MAX_EVENTS - RESERVED_TERMINAL_EVENTS,
            });
        }
        let previous_event_id = transaction
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
        let segment_index =
            u16::try_from(segment_count).map_err(|_| JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.segment_index_out_of_range",
            })?;
        let generation = u32::try_from(previous.generation)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.generation_out_of_range",
            })?;
        let sequence =
            u32::try_from(event_count).map_err(|_| JournalError::MetadataTraceInvariant {
                run_id: run_id.to_owned(),
                reason_code: "metadata_trace.event_sequence_out_of_range",
            })?;
        let segment_id = Ulid::new().to_string();
        let event_identity = Ulid::new().to_string();
        let event = MetadataTraceEventV1 {
            sequence,
            generation,
            recorded_at_unix_ms,
            event_id_sha256: hash_identifier(
                run_id,
                MetadataTraceIdDomainV1::Event,
                event_identity.as_str(),
            )?,
            causal_parent_event_id_sha256: Some(previous_event_id),
            stage_duration_ms: None,
            event: MetadataTraceEventDataV1::RecoveryContinuation(RecoveryContinuationMetadataV1 {
                previous_segment_id_sha256: hash_identifier(
                    run_id,
                    MetadataTraceIdDomainV1::Segment,
                    previous.segment_id.as_str(),
                )?,
                reason_code: reason_code.to_owned(),
            }),
        };
        let event_json = serialize_event(run_id, &event)?;
        insert_segment_tx(
            &transaction,
            &request,
            segment_id.as_str(),
            segment_index,
            generation,
            Some(previous.segment_id.as_str()),
            now,
        )?;
        insert_event_tx(
            &transaction,
            run_id,
            segment_id.as_str(),
            &event,
            event_json.as_str(),
            now,
        )?;
        transaction.commit()?;
        Ok(MetadataTraceSegmentPosition { segment_index, generation })
    }
}

fn isolate_suffix(
    run_id: &str,
    segments: &mut [MetadataTraceSegmentV1],
) -> Result<(), JournalError> {
    let Some(last) = segments.last_mut() else {
        return Err(JournalError::MetadataTraceInvariant {
            run_id: run_id.to_owned(),
            reason_code: "metadata_trace.no_valid_prefix",
        });
    };
    last.status = MetadataTraceSegmentStatusV1::CorruptSuffixIsolated;
    Ok(())
}

fn load_status_projection(
    connection: &Connection,
    segment_id: &str,
) -> Result<(MetadataTraceSegmentStatusV1, bool), JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT status_ordinal, status
            FROM metadata_trace_segment_status_events
            WHERE segment_ulid = ?1
            ORDER BY status_ordinal ASC
            LIMIT 3
        "#,
    )?;
    let rows = statement.query_map(params![segment_id], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut statuses = Vec::new();
    let mut invalid = false;
    for (expected_ordinal, row) in rows.enumerate() {
        let (ordinal, raw_status) = row?;
        let Ok(expected_ordinal) = i64::try_from(expected_ordinal) else {
            invalid = true;
            break;
        };
        if ordinal != expected_ordinal {
            invalid = true;
            break;
        }
        let Some(status) = parse_status(raw_status.as_str()) else {
            invalid = true;
            break;
        };
        statuses.push(status);
    }
    if invalid {
        return Ok((MetadataTraceSegmentStatusV1::CorruptSuffixIsolated, true));
    }
    match statuses.as_slice() {
        [] => Ok((MetadataTraceSegmentStatusV1::Interrupted, false)),
        [status] => Ok((*status, false)),
        [MetadataTraceSegmentStatusV1::Interrupted, MetadataTraceSegmentStatusV1::CorruptSuffixIsolated] => {
            Ok((MetadataTraceSegmentStatusV1::CorruptSuffixIsolated, false))
        }
        _ => Ok((MetadataTraceSegmentStatusV1::CorruptSuffixIsolated, true)),
    }
}

fn load_segment_rows(
    connection: &Connection,
    run_id: &str,
) -> Result<Vec<SegmentRow>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT
                segment_ulid,
                segment_index,
                generation,
                predecessor_segment_ulid,
                schema_version
            FROM metadata_trace_segments
            WHERE run_ulid = ?1
            ORDER BY segment_index ASC
            LIMIT 17
        "#,
    )?;
    let rows = statement.query_map(params![run_id], |row| {
        Ok(SegmentRow {
            segment_id: row.get(0)?,
            segment_index: row.get(1)?,
            generation: row.get(2)?,
            predecessor_segment_id: row.get(3)?,
            schema_version: row.get(4)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(JournalError::from)
}

impl JournalStore {
    /// Loads a deterministic metadata trace, isolating any corrupt suffix.
    ///
    /// Runs created before metadata trace migration return `None`. Corruption
    /// after a valid prefix is projected as `corrupt_suffix_isolated` and raw
    /// corrupt bytes are never returned.
    ///
    /// # Errors
    /// Returns [`JournalError::RunNotFound`] for an unknown run,
    /// [`JournalError::MetadataTraceInvariant`] when no valid prefix exists,
    /// or [`JournalError`] when SQLite access fails.
    pub(crate) fn load_metadata_trace(
        &self,
        run_id: &str,
    ) -> Result<Option<MetadataTraceV1>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let session_id = guard
            .query_row(
                "SELECT session_ulid FROM orchestrator_runs WHERE run_ulid = ?1",
                params![run_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| JournalError::RunNotFound { run_id: run_id.to_owned() })?;
        let raw_segments = load_segment_rows(&guard, run_id)?;
        if raw_segments.is_empty() {
            return Ok(None);
        }

        let mut segments = Vec::new();
        let mut expected_sequence = 0_u32;
        let mut seen_event_ids = BTreeSet::new();
        let mut previous_segment_id: Option<&str> = None;
        let segment_limit_exceeded = raw_segments.len() > METADATA_TRACE_MAX_SEGMENTS;
        let mut suffix_isolated = false;

        for raw_segment in raw_segments.iter().take(METADATA_TRACE_MAX_SEGMENTS) {
            let expected_segment_index = u16::try_from(segments.len()).map_err(|_| {
                JournalError::MetadataTraceInvariant {
                    run_id: run_id.to_owned(),
                    reason_code: "metadata_trace.segment_index_out_of_range",
                }
            })?;
            let expected_generation = u32::from(expected_segment_index).saturating_add(1);
            let segment_index = u16::try_from(raw_segment.segment_index).ok();
            let generation = u32::try_from(raw_segment.generation).ok();
            let metadata_is_valid = segment_index == Some(expected_segment_index)
                && generation == Some(expected_generation)
                && raw_segment.schema_version == i64::from(METADATA_TRACE_SCHEMA_VERSION)
                && raw_segment.predecessor_segment_id.as_deref() == previous_segment_id;
            if !metadata_is_valid {
                isolate_suffix(run_id, segments.as_mut_slice())?;
                suffix_isolated = true;
                break;
            }

            let segment_id_sha256 = hash_identifier(
                run_id,
                MetadataTraceIdDomainV1::Segment,
                raw_segment.segment_id.as_str(),
            )?;
            let mut statement = guard.prepare(
                r#"
                    SELECT
                        sequence,
                        generation,
                        event_id_sha256,
                        causal_parent_event_id_sha256,
                        event_kind,
                        event_json,
                        recorded_at_unix_ms
                    FROM metadata_trace_events
                    WHERE segment_ulid = ?1
                    ORDER BY sequence ASC
                    LIMIT 513
                "#,
            )?;
            let rows = statement.query_map(params![raw_segment.segment_id], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            })?;
            let mut events = Vec::new();
            let mut segment_corrupt = false;
            for row in rows {
                let (
                    stored_sequence,
                    stored_generation,
                    stored_event_id,
                    stored_parent_id,
                    stored_event_kind,
                    event_json,
                    stored_recorded_at,
                ) = row?;
                if usize::try_from(expected_sequence)
                    .ok()
                    .is_none_or(|sequence| sequence >= METADATA_TRACE_MAX_EVENTS)
                    || event_json.len() > METADATA_TRACE_MAX_EVENT_BYTES
                {
                    segment_corrupt = true;
                    break;
                }
                let Ok(event) = serde_json::from_str::<MetadataTraceEventV1>(event_json.as_str())
                else {
                    segment_corrupt = true;
                    break;
                };
                let stored_recorded_at = u64::try_from(stored_recorded_at).ok();
                let row_matches_event = stored_sequence == i64::from(event.sequence)
                    && stored_generation == i64::from(event.generation)
                    && stored_event_id == event.event_id_sha256
                    && stored_parent_id == event.causal_parent_event_id_sha256
                    && stored_event_kind == event_kind(&event.event)
                    && stored_recorded_at == Some(event.recorded_at_unix_ms);
                let causal_parent_is_valid = if event.sequence == 0 {
                    event.causal_parent_event_id_sha256.is_none()
                } else {
                    event
                        .causal_parent_event_id_sha256
                        .as_ref()
                        .is_some_and(|parent| seen_event_ids.contains(parent))
                };
                if !row_matches_event
                    || event.sequence != expected_sequence
                    || event.generation != expected_generation
                    || !causal_parent_is_valid
                    || event.validate_shape().is_err()
                {
                    segment_corrupt = true;
                    break;
                }
                seen_event_ids.insert(event.event_id_sha256.clone());
                expected_sequence = expected_sequence.checked_add(1).ok_or_else(|| {
                    JournalError::MetadataTraceInvariant {
                        run_id: run_id.to_owned(),
                        reason_code: "metadata_trace.event_sequence_out_of_range",
                    }
                })?;
                events.push(event);
            }
            drop(statement);

            if events.is_empty() {
                isolate_suffix(run_id, segments.as_mut_slice())?;
                suffix_isolated = true;
                break;
            }
            let (mut status, status_corrupt) =
                load_status_projection(&guard, raw_segment.segment_id.as_str())?;
            let complete_without_terminalization = status == MetadataTraceSegmentStatusV1::Complete
                && events.last().map(|event| event_kind(&event.event)) != Some("terminalization");
            if segment_corrupt || status_corrupt || complete_without_terminalization {
                status = MetadataTraceSegmentStatusV1::CorruptSuffixIsolated;
                suffix_isolated = true;
            }
            segments.push(MetadataTraceSegmentV1 {
                segment_id_sha256,
                segment_index: expected_segment_index,
                generation: expected_generation,
                status,
                events,
            });
            previous_segment_id = Some(raw_segment.segment_id.as_str());
            if suffix_isolated {
                break;
            }
        }

        suffix_isolated |= segment_limit_exceeded;
        if suffix_isolated
            && segments.last().is_some_and(|segment| {
                segment.status != MetadataTraceSegmentStatusV1::CorruptSuffixIsolated
            })
        {
            isolate_suffix(run_id, segments.as_mut_slice())?;
        }
        let trace = MetadataTraceV1 {
            schema_version: METADATA_TRACE_SCHEMA_VERSION,
            run_id_sha256: hash_identifier(run_id, MetadataTraceIdDomainV1::Run, run_id)?,
            session_id_sha256: hash_identifier(
                run_id,
                MetadataTraceIdDomainV1::Session,
                session_id.as_str(),
            )?,
            segments,
        };
        trace.validate_shape().map_err(|_| JournalError::MetadataTraceInvariant {
            run_id: run_id.to_owned(),
            reason_code: "metadata_trace.invalid_loaded_prefix",
        })?;
        Ok(Some(trace))
    }
}
