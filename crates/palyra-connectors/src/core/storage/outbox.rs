//! Durable outbox: idempotent enqueue, claim-based draining, and delivery
//! status transitions.
//!
//! Drains claim due entries under a short lease (see `OUTBOX_CLAIM_LEASE_MS`)
//! and every later transition re-checks the claim token. Expired pre-effect
//! claims are reclaimable; expired effect-started claims are parked
//! outcome-unknown until explicit reconciliation proves delivery or absence.
//! Entries are always served oldest-deadline-first.

use rusqlite::{params, OptionalExtension};

use super::super::protocol::OutboundMessageRequest;
use super::records::parse_outbox_row;
use super::{
    next_outbox_claim_token, ConnectorStore, ConnectorStoreError, OutboxDeliverySnapshot,
    OutboxEffectState, OutboxEnqueueOutcome, OutboxEntryRecord, OUTBOX_CLAIM_LEASE_MS,
};

const OUTBOX_EXPIRED_AFTER_EFFECT_REASON: &str = "outbox.claim_expired_after_effect_started";

impl ConnectorStore {
    /// Returns the payload-free state of one deterministic outbox envelope.
    ///
    /// # Errors
    /// Returns a storage error when the lookup or persisted effect-state decode fails.
    pub fn outbox_delivery_snapshot(
        &self,
        connector_id: &str,
        envelope_id: &str,
    ) -> Result<Option<OutboxDeliverySnapshot>, ConnectorStoreError> {
        let connection = self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        connection
            .query_row(
                r#"
                    SELECT status, effect_state, native_message_id
                    FROM outbox
                    WHERE connector_id = ?1 AND envelope_id = ?2
                "#,
                params![connector_id, envelope_id],
                |row| {
                    let status = row.get::<_, String>(0)?;
                    let effect_state = row.get::<_, String>(1)?;
                    Ok((status, effect_state, row.get::<_, Option<String>>(2)?))
                },
            )
            .optional()?
            .map(|(status, effect_state, native_message_id)| {
                Ok(OutboxDeliverySnapshot {
                    connector_id: connector_id.to_owned(),
                    envelope_id: envelope_id.to_owned(),
                    status,
                    effect_state: OutboxEffectState::parse(effect_state.as_str())?,
                    native_message_id,
                })
            })
            .transpose()
    }

    /// Enqueues the payload unless its `(connector_id, envelope_id)` pair is
    /// already present, making enqueue retries idempotent.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::Serde`] when the payload cannot be
    /// encoded and [`ConnectorStoreError::Sqlite`] when the insert fails.
    pub fn enqueue_outbox_if_absent(
        &self,
        payload: &OutboundMessageRequest,
        max_attempts: u32,
        now_unix_ms: i64,
    ) -> Result<OutboxEnqueueOutcome, ConnectorStoreError> {
        let payload_json = serde_json::to_string(payload)?;
        let inserted = self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                INSERT OR IGNORE INTO outbox (
                    connector_id, envelope_id, payload_json, attempts, max_attempts,
                    next_attempt_unix_ms, status, native_message_id, last_error,
                    created_at_unix_ms, updated_at_unix_ms
                )
                VALUES (?1, ?2, ?3, 0, ?4, ?5, 'pending', NULL, NULL, ?5, ?5)
                "#,
                params![
                    payload.connector_id,
                    payload.envelope_id,
                    payload_json,
                    i64::from(max_attempts.max(1)),
                    now_unix_ms,
                ],
            )?;
            Ok(changed)
        })?;
        Ok(OutboxEnqueueOutcome { created: inserted > 0 })
    }

    /// Claims and returns up to `limit` due pending entries under a fresh
    /// claim token, ordered by `next_attempt_unix_ms` then insertion order.
    ///
    /// Entries whose previous pre-effect claim lease expired are reclaimed.
    /// Expired effect-started entries are parked outcome-unknown instead, with
    /// each recovery pass capped by `limit`. Rows beyond the cap remain fenced
    /// and unclaimable until a later drain. Unless `ignore_queue_pause` is set,
    /// paused connectors are skipped.
    ///
    /// # Errors
    /// Returns a storage error when the claim transaction fails or a claimed
    /// payload no longer deserializes.
    pub fn load_due_outbox(
        &self,
        now_unix_ms: i64,
        limit: usize,
        connector_filter: Option<&str>,
        ignore_queue_pause: bool,
    ) -> Result<Vec<OutboxEntryRecord>, ConnectorStoreError> {
        let limit_i64 = i64::try_from(limit)
            .map_err(|_| ConnectorStoreError::ValueOverflow { field: "limit" })?;
        if limit_i64 <= 0 {
            return Ok(Vec::new());
        }
        let claim_token = next_outbox_claim_token(now_unix_ms);
        let claim_expires_unix_ms = now_unix_ms.saturating_add(OUTBOX_CLAIM_LEASE_MS);

        self.with_transaction(|transaction| {
            park_expired_effect_started_entries(transaction, now_unix_ms, limit_i64)?;
            // Four static statements instead of dynamically composed SQL: the
            // filter/pause combinations are few and keeping each statement
            // literal keeps them parameterized and individually reviewable.
            if let Some(connector_id) = connector_filter {
                if ignore_queue_pause {
                    transaction.execute(
                        r#"
                        UPDATE outbox
                        SET claim_token = ?1,
                            claim_expires_unix_ms = ?2,
                            updated_at_unix_ms = ?3
                        WHERE outbox_id IN (
                            SELECT outbox_id
                            FROM outbox
                            WHERE status = 'pending'
                              AND effect_state = 'ready'
                              AND next_attempt_unix_ms <= ?3
                              AND claim_expires_unix_ms <= ?3
                              AND connector_id = ?4
                            ORDER BY next_attempt_unix_ms ASC, outbox_id ASC
                            LIMIT ?5
                        )
                        "#,
                        params![
                            claim_token.as_str(),
                            claim_expires_unix_ms,
                            now_unix_ms,
                            connector_id,
                            limit_i64,
                        ],
                    )?;
                } else {
                    transaction.execute(
                        r#"
                        UPDATE outbox
                        SET claim_token = ?1,
                            claim_expires_unix_ms = ?2,
                            updated_at_unix_ms = ?3
                        WHERE outbox_id IN (
                            SELECT outbox_id
                            FROM outbox
                            LEFT JOIN connector_queue_state
                                ON connector_queue_state.connector_id = outbox.connector_id
                            WHERE outbox.status = 'pending'
                              AND outbox.effect_state = 'ready'
                              AND COALESCE(connector_queue_state.paused, 0) = 0
                              AND outbox.next_attempt_unix_ms <= ?3
                              AND outbox.claim_expires_unix_ms <= ?3
                              AND outbox.connector_id = ?4
                            ORDER BY outbox.next_attempt_unix_ms ASC, outbox.outbox_id ASC
                            LIMIT ?5
                        )
                        "#,
                        params![
                            claim_token.as_str(),
                            claim_expires_unix_ms,
                            now_unix_ms,
                            connector_id,
                            limit_i64,
                        ],
                    )?;
                }
            } else if ignore_queue_pause {
                transaction.execute(
                    r#"
                    UPDATE outbox
                    SET claim_token = ?1,
                        claim_expires_unix_ms = ?2,
                        updated_at_unix_ms = ?3
                    WHERE outbox_id IN (
                        SELECT outbox_id
                        FROM outbox
                        WHERE status = 'pending'
                          AND effect_state = 'ready'
                          AND next_attempt_unix_ms <= ?3
                          AND claim_expires_unix_ms <= ?3
                        ORDER BY next_attempt_unix_ms ASC, outbox_id ASC
                        LIMIT ?4
                    )
                    "#,
                    params![claim_token.as_str(), claim_expires_unix_ms, now_unix_ms, limit_i64],
                )?;
            } else {
                transaction.execute(
                    r#"
                    UPDATE outbox
                    SET claim_token = ?1,
                        claim_expires_unix_ms = ?2,
                        updated_at_unix_ms = ?3
                    WHERE outbox_id IN (
                        SELECT outbox_id
                        FROM outbox
                        LEFT JOIN connector_queue_state
                            ON connector_queue_state.connector_id = outbox.connector_id
                        WHERE outbox.status = 'pending'
                          AND outbox.effect_state = 'ready'
                          AND COALESCE(connector_queue_state.paused, 0) = 0
                          AND outbox.next_attempt_unix_ms <= ?3
                          AND outbox.claim_expires_unix_ms <= ?3
                        ORDER BY outbox.next_attempt_unix_ms ASC, outbox.outbox_id ASC
                        LIMIT ?4
                    )
                    "#,
                    params![claim_token.as_str(), claim_expires_unix_ms, now_unix_ms, limit_i64],
                )?;
            }

            let mut records = Vec::new();
            let mut statement = transaction.prepare(
                r#"
                SELECT outbox_id, connector_id, envelope_id, payload_json, attempts, max_attempts,
                       next_attempt_unix_ms, claim_token, created_at_unix_ms, updated_at_unix_ms,
                       effect_state
                FROM outbox
                WHERE claim_token = ?1
                ORDER BY next_attempt_unix_ms ASC, outbox_id ASC
                "#,
            )?;
            let mut rows = statement.query(params![claim_token.as_str()])?;
            while let Some(row) = rows.next()? {
                records.push(parse_outbox_row(row)?);
            }
            Ok(records)
        })
    }

    /// Recovers claimed rows for a durable QA barrier after its owning process disappeared.
    ///
    /// Pre-effect rows are released for an immediate retry. Rows whose effect fence already
    /// started are parked outcome-unknown, because a restart cannot prove whether the platform
    /// observed the request.
    ///
    /// # Errors
    /// Returns a storage error when any bounded recovery transition fails.
    #[cfg(feature = "qa-fault-injection")]
    pub(crate) fn recover_qa_fault_barrier_claims(
        &self,
        outbox_ids: &[i64],
        reason_code: &str,
        now_unix_ms: i64,
    ) -> Result<usize, ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let mut recovered = 0_usize;
            for outbox_id in outbox_ids {
                let rows = {
                    let mut statement = transaction.prepare(
                        r#"
                        SELECT outbox_id, connector_id, envelope_id, effect_state
                        FROM outbox
                        WHERE outbox_id = ?1
                          AND status = 'pending'
                          AND claim_token IS NOT NULL
                        ORDER BY outbox_id ASC
                        "#,
                    )?;
                    let mapped = statement.query_map(params![outbox_id], |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    })?;
                    mapped.collect::<Result<Vec<_>, _>>()?
                };
                for (outbox_id, connector_id, envelope_id, effect_state) in rows {
                    match effect_state.as_str() {
                        "ready" => {
                            let changed = transaction.execute(
                                r#"
                                UPDATE outbox
                                SET claim_token = NULL,
                                    claim_expires_unix_ms = 0,
                                    next_attempt_unix_ms = ?2,
                                    last_error = ?3,
                                    updated_at_unix_ms = ?2
                                WHERE outbox_id = ?1
                                  AND status = 'pending'
                                  AND effect_state = 'ready'
                                "#,
                                params![outbox_id, now_unix_ms, reason_code],
                            )?;
                            transaction.execute(
                                r#"
                                UPDATE delivery_intents
                                SET status = 'queued',
                                    last_reason_code = ?3,
                                    updated_at_unix_ms = ?4
                                WHERE connector_id = ?1
                                  AND outbox_envelope_id = ?2
                                  AND status IN ('queued', 'adapter_send_started')
                                "#,
                                params![connector_id, envelope_id, reason_code, now_unix_ms],
                            )?;
                            recovered = recovered.saturating_add(changed);
                        }
                        "effect_started" => {
                            park_outbox_identity_unknown(
                                transaction,
                                outbox_id,
                                connector_id.as_str(),
                                envelope_id.as_str(),
                                reason_code,
                                now_unix_ms,
                            )?;
                            recovered = recovered.saturating_add(1);
                        }
                        "outcome_unknown" => {}
                        other => {
                            return Err(ConnectorStoreError::UnknownOutboxEffectState(
                                other.to_owned(),
                            ));
                        }
                    }
                }
            }
            Ok(recovered)
        })
    }

    /// Applies one strict connector crash transition for a claimed QA fault actor.
    ///
    /// `effect_started` selects the only accepted durable fence state. The exact
    /// already-applied state is accepted idempotently when the outbox and every
    /// delivery intent that exists retain the expected QA reason code. Directly
    /// enqueued rows may have no intent, so their exact outbox fence is authoritative.
    /// Every other mismatched, missing, or unprovable row returns `false` unchanged.
    ///
    /// # Errors
    /// Returns a storage error when the exact transition cannot be queried or committed.
    #[cfg(feature = "qa-fault-injection")]
    pub(crate) fn recover_qa_fault_crash_actor(
        &self,
        outbox_id: i64,
        effect_started: bool,
        reason_code: &str,
        now_unix_ms: i64,
    ) -> Result<bool, ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let row = transaction
                .query_row(
                    r#"
                    SELECT connector_id, envelope_id, status, effect_state,
                           claim_token, claim_expires_unix_ms, last_error
                    FROM outbox
                    WHERE outbox_id = ?1
                    "#,
                    params![outbox_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, Option<String>>(4)?,
                            row.get::<_, i64>(5)?,
                            row.get::<_, Option<String>>(6)?,
                        ))
                    },
                )
                .optional()?;
            let Some((
                connector_id,
                envelope_id,
                status,
                effect_state,
                claim_token,
                claim_expires_unix_ms,
                last_error,
            )) = row
            else {
                return Ok(false);
            };
            if status != "pending" {
                return Ok(false);
            }
            let intents = {
                let mut statement = transaction.prepare(
                    r#"
                    SELECT status, last_reason_code
                    FROM delivery_intents
                    WHERE connector_id = ?1
                      AND outbox_envelope_id = ?2
                    ORDER BY intent_id ASC
                    "#,
                )?;
                let mapped = statement
                    .query_map(params![connector_id.as_str(), envelope_id.as_str()], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                    })?;
                mapped.collect::<Result<Vec<_>, _>>()?
            };
            let claims_are_clear = claim_token.is_none() && claim_expires_unix_ms == 0;
            let reason_matches = last_error.as_deref() == Some(reason_code);
            let already_applied = if effect_started {
                effect_state == "outcome_unknown"
                    && claims_are_clear
                    && reason_matches
                    && (intents.is_empty()
                        || intents.iter().all(|(intent_status, intent_reason)| {
                            intent_status == "platform_outcome_unknown"
                                && intent_reason.as_deref() == Some(reason_code)
                        }))
            } else {
                effect_state == "ready"
                    && claims_are_clear
                    && reason_matches
                    && (intents.is_empty()
                        || intents.iter().all(|(intent_status, intent_reason)| {
                            intent_status == "queued"
                                && intent_reason.as_deref() == Some(reason_code)
                        }))
            };
            if already_applied {
                return Ok(true);
            }
            let expected_state = if effect_started { "effect_started" } else { "ready" };
            let fresh_intents_are_provable = if effect_started {
                intents.is_empty()
                    || intents
                        .iter()
                        .all(|(intent_status, _)| intent_status == "adapter_send_started")
            } else {
                intents.is_empty()
                    || intents.iter().all(|(intent_status, _)| {
                        matches!(intent_status.as_str(), "queued" | "adapter_send_started")
                    })
            };
            if effect_state != expected_state
                || claim_token.is_none()
                || !fresh_intents_are_provable
            {
                return Ok(false);
            }
            if effect_started {
                park_outbox_identity_unknown(
                    transaction,
                    outbox_id,
                    connector_id.as_str(),
                    envelope_id.as_str(),
                    reason_code,
                    now_unix_ms,
                )?;
                return Ok(true);
            }
            let changed = transaction.execute(
                r#"
                UPDATE outbox
                SET claim_token = NULL,
                    claim_expires_unix_ms = 0,
                    next_attempt_unix_ms = ?2,
                    last_error = ?3,
                    updated_at_unix_ms = ?2
                WHERE outbox_id = ?1
                  AND status = 'pending'
                  AND effect_state = 'ready'
                  AND claim_token IS NOT NULL
                "#,
                params![outbox_id, now_unix_ms, reason_code],
            )?;
            if changed != 1 {
                return Ok(false);
            }
            transaction.execute(
                r#"
                UPDATE delivery_intents
                SET status = 'queued',
                    last_reason_code = ?3,
                    updated_at_unix_ms = ?4
                WHERE connector_id = ?1
                  AND outbox_envelope_id = ?2
                  AND status IN ('queued', 'adapter_send_started')
                "#,
                params![connector_id, envelope_id, reason_code, now_unix_ms],
            )?;
            Ok(true)
        })
    }

    /// Proves that every named QA barrier outbox row is terminal or outcome-unknown.
    ///
    /// # Errors
    /// Returns a storage error when the bounded proof query fails.
    #[cfg(feature = "qa-fault-injection")]
    pub(crate) fn qa_fault_barrier_actors_are_resolved(
        &self,
        outbox_ids: &[i64],
    ) -> Result<bool, ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let mut statement = transaction.prepare(
                r#"
                SELECT COUNT(*),
                       COALESCE(SUM(
                           CASE
                               WHEN status <> 'pending' OR effect_state = 'outcome_unknown' THEN 1
                               ELSE 0
                           END
                       ), 0)
                FROM outbox
                WHERE outbox_id = ?1
                "#,
            )?;
            for outbox_id in outbox_ids {
                let (total, resolved) = statement.query_row(params![outbox_id], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                })?;
                if total == 0 || resolved != total {
                    return Ok(false);
                }
            }
            Ok(true)
        })
    }

    /// Proves that dispatched barrier claims transitioned and missing actors remain resolved.
    ///
    /// # Errors
    /// Returns a storage error when the bounded proof query fails.
    #[cfg(feature = "qa-fault-injection")]
    pub(crate) fn qa_fault_barrier_completion_is_durable(
        &self,
        transitioned_claims: &[(i64, String)],
        resolved_outbox_ids: &[i64],
    ) -> Result<bool, ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let mut transitioned = transaction.prepare(
                r#"
                SELECT COUNT(*)
                FROM outbox
                WHERE outbox_id = ?1
                  AND (claim_token IS NULL OR claim_token <> ?2)
                "#,
            )?;
            for (outbox_id, original_claim) in transitioned_claims {
                let count = transitioned
                    .query_row(params![outbox_id, original_claim], |row| row.get::<_, i64>(0))?;
                if count != 1 {
                    return Ok(false);
                }
            }
            let mut resolved = transaction.prepare(
                r#"
                SELECT COUNT(*),
                       COALESCE(SUM(
                           CASE
                               WHEN status <> 'pending' OR effect_state = 'outcome_unknown' THEN 1
                               ELSE 0
                           END
                       ), 0)
                FROM outbox
                WHERE outbox_id = ?1
                "#,
            )?;
            for outbox_id in resolved_outbox_ids {
                let (total, resolved_count) = resolved.query_row(params![outbox_id], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                })?;
                if total == 0 || resolved_count != total {
                    return Ok(false);
                }
            }
            Ok(true)
        })
    }

    /// Marks the external-effect fence for a claimed entry immediately before
    /// the adapter call starts.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::OutboxNotFound`] when the row is no
    /// longer owned by `claim_token` or its effect already started.
    pub fn mark_outbox_effect_started(
        &self,
        outbox_id: i64,
        claim_token: &str,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE outbox
                SET effect_state = 'effect_started',
                    updated_at_unix_ms = ?3
                WHERE outbox_id = ?1
                  AND status = 'pending'
                  AND effect_state = 'ready'
                  AND claim_token = ?2
                "#,
                params![outbox_id, claim_token, now_unix_ms],
            )?;
            if changed == 0 {
                return Err(ConnectorStoreError::OutboxNotFound(outbox_id));
            }
            Ok(())
        })
    }

    /// Parks a claimed effect-started entry when the adapter outcome is not
    /// trustworthy enough to retry.
    ///
    /// The outbox fence and any delivery intents move to outcome-unknown in one
    /// transaction, so no later drain can repeat the external effect blindly.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::OutboxNotFound`] when the row is no
    /// longer owned by `claim_token` or the effect fence was not started.
    pub fn mark_outbox_outcome_unknown(
        &self,
        outbox_id: i64,
        claim_token: &str,
        reason_code: &str,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let identity = transaction
                .query_row(
                    r#"
                    SELECT connector_id, envelope_id
                    FROM outbox
                    WHERE outbox_id = ?1
                      AND status = 'pending'
                      AND effect_state = 'effect_started'
                      AND claim_token = ?2
                    "#,
                    params![outbox_id, claim_token],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?;
            let Some((connector_id, envelope_id)) = identity else {
                return Err(ConnectorStoreError::OutboxNotFound(outbox_id));
            };
            park_outbox_identity_unknown(
                transaction,
                outbox_id,
                connector_id.as_str(),
                envelope_id.as_str(),
                reason_code,
                now_unix_ms,
            )?;
            Ok(())
        })
    }

    /// Marks a claimed entry delivered and releases its claim.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::OutboxNotFound`] when the entry is not
    /// pending under `claim_token` (already resolved or reclaimed elsewhere).
    pub fn mark_outbox_delivered(
        &self,
        outbox_id: i64,
        claim_token: &str,
        native_message_id: &str,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE outbox
                SET status = 'delivered',
                    native_message_id = ?3,
                    last_error = NULL,
                    effect_state = 'ready',
                    claim_token = NULL,
                    claim_expires_unix_ms = 0,
                    updated_at_unix_ms = ?4
                WHERE outbox_id = ?1
                  AND status = 'pending'
                  AND claim_token = ?2
                "#,
                params![outbox_id, claim_token, native_message_id, now_unix_ms],
            )?;
            if changed == 0 {
                return Err(ConnectorStoreError::OutboxNotFound(outbox_id));
            }
            Ok(())
        })?;
        Ok(())
    }

    /// Releases a claimed entry back to pending with an updated attempt count
    /// and next-attempt deadline.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::OutboxNotFound`] when the entry is not
    /// pending under `claim_token` (already resolved or reclaimed elsewhere).
    pub fn schedule_outbox_retry(
        &self,
        outbox_id: i64,
        claim_token: &str,
        attempts: u32,
        reason: &str,
        next_attempt_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE outbox
                SET attempts = ?3,
                    next_attempt_unix_ms = ?4,
                    status = 'pending',
                    effect_state = 'ready',
                    last_error = ?5,
                    claim_token = NULL,
                    claim_expires_unix_ms = 0,
                    updated_at_unix_ms = ?4
                WHERE outbox_id = ?1
                  AND status = 'pending'
                  AND claim_token = ?2
                "#,
                params![outbox_id, claim_token, i64::from(attempts), next_attempt_unix_ms, reason],
            )?;
            if changed == 0 {
                return Err(ConnectorStoreError::OutboxNotFound(outbox_id));
            }
            Ok(())
        })?;
        Ok(())
    }

    /// Copies a claimed entry into `dead_letters` and marks it dead, in one
    /// transaction.
    ///
    /// The outbox row is kept (status `dead`) so the envelope id stays
    /// reserved; replay flips it back to pending instead of inserting anew.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::OutboxNotFound`] when the entry is not
    /// pending under `claim_token` (already resolved or reclaimed elsewhere).
    pub fn move_outbox_to_dead_letter(
        &self,
        outbox_id: i64,
        claim_token: &str,
        reason: &str,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let maybe_payload = transaction
                .query_row(
                    r#"
                    SELECT connector_id, envelope_id, payload_json
                    FROM outbox
                    WHERE outbox_id = ?1
                      AND status = 'pending'
                      AND claim_token = ?2
                    "#,
                    params![outbox_id, claim_token],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()?;
            let Some((connector_id, envelope_id, payload_json)) = maybe_payload else {
                return Err(ConnectorStoreError::OutboxNotFound(outbox_id));
            };
            transaction.execute(
                r#"
                INSERT INTO dead_letters (
                    connector_id, envelope_id, reason, payload_json, created_at_unix_ms
                )
                VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
                params![connector_id, envelope_id, reason, payload_json, now_unix_ms],
            )?;
            let changed = transaction.execute(
                r#"
                UPDATE outbox
                SET status = 'dead',
                    effect_state = 'ready',
                    last_error = ?2,
                    claim_token = NULL,
                    claim_expires_unix_ms = 0,
                    updated_at_unix_ms = ?3
                WHERE outbox_id = ?1
                  AND status = 'pending'
                  AND claim_token = ?4
                "#,
                params![outbox_id, reason, now_unix_ms, claim_token],
            )?;
            if changed == 0 {
                return Err(ConnectorStoreError::OutboxNotFound(outbox_id));
            }
            Ok(())
        })
    }
}

fn park_expired_effect_started_entries(
    transaction: &rusqlite::Transaction<'_>,
    now_unix_ms: i64,
    limit: i64,
) -> Result<(), ConnectorStoreError> {
    let expired = {
        let mut statement = transaction.prepare(
            r#"
            SELECT outbox_id, connector_id, envelope_id
            FROM outbox
            WHERE status = 'pending'
              AND effect_state = 'effect_started'
              AND claim_expires_unix_ms <= ?1
            ORDER BY outbox_id ASC
            LIMIT ?2
            "#,
        )?;
        let mut rows = statement.query(params![now_unix_ms, limit])?;
        let mut expired = Vec::new();
        while let Some(row) = rows.next()? {
            expired.push((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ));
        }
        expired
    };
    for (outbox_id, connector_id, envelope_id) in expired {
        park_outbox_identity_unknown(
            transaction,
            outbox_id,
            connector_id.as_str(),
            envelope_id.as_str(),
            OUTBOX_EXPIRED_AFTER_EFFECT_REASON,
            now_unix_ms,
        )?;
    }
    Ok(())
}

fn park_outbox_identity_unknown(
    transaction: &rusqlite::Transaction<'_>,
    outbox_id: i64,
    connector_id: &str,
    envelope_id: &str,
    reason_code: &str,
    now_unix_ms: i64,
) -> Result<(), ConnectorStoreError> {
    let changed = transaction.execute(
        r#"
        UPDATE outbox
        SET effect_state = 'outcome_unknown',
            last_error = ?2,
            claim_token = NULL,
            claim_expires_unix_ms = 0,
            updated_at_unix_ms = ?3
        WHERE outbox_id = ?1
          AND status = 'pending'
          AND effect_state = 'effect_started'
        "#,
        params![outbox_id, reason_code, now_unix_ms],
    )?;
    if changed == 0 {
        return Err(ConnectorStoreError::OutboxNotFound(outbox_id));
    }
    transaction.execute(
        r#"
        UPDATE delivery_intents
        SET status = 'platform_outcome_unknown',
            last_reason_code = ?3,
            updated_at_unix_ms = ?4
        WHERE connector_id = ?1
          AND outbox_envelope_id = ?2
          AND status <> 'delivered'
          AND status <> 'suppressed'
        "#,
        params![connector_id, envelope_id, reason_code, now_unix_ms],
    )?;
    transaction.execute(
        r#"
        INSERT INTO connector_events (
            connector_id, event_type, level, message, details_json, created_at_unix_ms
        )
        VALUES (?1, 'outbox.outcome_unknown', 'warn',
                'outbox effect outcome requires reconciliation', ?2, ?3)
        "#,
        params![
            connector_id,
            serde_json::json!({
                "outbox_id": outbox_id,
                "envelope_id": envelope_id,
                "reason_code": reason_code,
            })
            .to_string(),
            now_unix_ms,
        ],
    )?;
    Ok(())
}
