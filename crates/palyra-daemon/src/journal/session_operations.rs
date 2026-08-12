//! Durable admission ledger for model-visible cross-session operations.
//! Background-task ownership remains the capability authority; this module
//! adds idempotency, rate limiting, and supersede evidence around queue writes.

use super::*;

const SESSION_OPERATION_SCHEMA_VERSION: i64 = 1;
const SESSION_SEND_WINDOW_MS: i64 = 60_000;
const SESSION_SEND_MAX_PER_WINDOW: i64 = 8;

pub(super) const MIGRATION_85_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS session_model_commands_v1 (
        command_ulid TEXT PRIMARY KEY,
        request_key TEXT NOT NULL,
        command_kind TEXT NOT NULL CHECK (
            command_kind IN ('send', 'steer', 'interrupt', 'switch_model')
        ),
        owner_session_ulid TEXT NOT NULL,
        owner_run_ulid TEXT NOT NULL,
        target_session_ulid TEXT NOT NULL,
        target_run_ulid TEXT NOT NULL,
        ownership_task_ulid TEXT NOT NULL,
        owner_principal TEXT NOT NULL,
        device_id TEXT NOT NULL,
        channel TEXT,
        payload_sha256 TEXT NOT NULL,
        requested_model_profile TEXT,
        state TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        queued_input_ulid TEXT,
        superseded_by_command_ulid TEXT,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        UNIQUE(owner_session_ulid, request_key),
        FOREIGN KEY(owner_session_ulid) REFERENCES orchestrator_sessions(session_ulid),
        FOREIGN KEY(owner_run_ulid) REFERENCES orchestrator_runs(run_ulid),
        FOREIGN KEY(target_session_ulid) REFERENCES orchestrator_sessions(session_ulid),
        FOREIGN KEY(target_run_ulid) REFERENCES orchestrator_runs(run_ulid),
        FOREIGN KEY(ownership_task_ulid) REFERENCES orchestrator_background_tasks(task_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_session_model_commands_rate
        ON session_model_commands_v1(
            owner_session_ulid,
            target_session_ulid,
            command_kind,
            created_at_unix_ms DESC
        );
    CREATE INDEX IF NOT EXISTS idx_session_model_commands_pending
        ON session_model_commands_v1(
            target_session_ulid,
            command_kind,
            state,
            created_at_unix_ms DESC
        );
"#;

/// Model-visible session command kinds with distinct mutation semantics.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionModelCommandKind {
    Send,
    Steer,
    Interrupt,
    SwitchModel,
}

impl SessionModelCommandKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Send => "send",
            Self::Steer => "steer",
            Self::Interrupt => "interrupt",
            Self::SwitchModel => "switch_model",
        }
    }

    const fn coalesces_pending(self) -> bool {
        matches!(self, Self::Steer | Self::SwitchModel)
    }
}

/// Atomic admission request for one descendant session command.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionModelCommandReserveRequest {
    pub request_key: String,
    pub command_kind: SessionModelCommandKind,
    pub owner_session_id: String,
    pub owner_run_id: String,
    pub target_session_id: String,
    pub target_run_id: String,
    pub ownership_task_id: String,
    pub owner_principal: String,
    pub device_id: String,
    pub channel: Option<String>,
    pub payload_sha256: String,
    pub requested_model_profile: Option<String>,
}

/// Durable projection of a model-visible cross-session command.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionModelCommandRecord {
    pub command_id: String,
    pub request_key: String,
    pub command_kind: SessionModelCommandKind,
    pub owner_session_id: String,
    pub owner_run_id: String,
    pub target_session_id: String,
    pub target_run_id: String,
    pub ownership_task_id: String,
    pub owner_principal: String,
    pub device_id: String,
    pub channel: Option<String>,
    pub payload_sha256: String,
    pub requested_model_profile: Option<String>,
    pub state: String,
    pub reason_code: String,
    pub queued_input_id: Option<String>,
    pub superseded_by_command_id: Option<String>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// Result of command admission, including idempotent replay and coalescing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionModelCommandReserveOutcome {
    pub command: SessionModelCommandRecord,
    pub duplicate: bool,
    pub superseded_command_id: Option<String>,
}

/// Final queue/control outcome attached to an admitted command.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionModelCommandSettlementRequest {
    pub command_id: String,
    pub state: String,
    pub reason_code: String,
    pub queued_input_id: Option<String>,
}

/// Active run-generation projection for one exactly scoped session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScopedSessionRuntimeGeneration {
    pub session_id: String,
    pub run_id: String,
    pub generation: u64,
}

impl JournalStore {
    /// Reserves one scoped descendant command and applies its durable rate or
    /// coalescing policy before any queue/control side effect can occur.
    ///
    /// # Errors
    /// Returns a typed journal error for invalid ownership, archived targets,
    /// rate exhaustion, malformed requests, or persistence failures.
    pub fn reserve_session_model_command(
        &self,
        request: &SessionModelCommandReserveRequest,
    ) -> Result<SessionModelCommandReserveOutcome, JournalError> {
        validate_session_model_command_request(request)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;

        if let Some(existing) = load_session_model_command_by_request_key_tx(
            &transaction,
            request.owner_session_id.as_str(),
            request.request_key.as_str(),
        )? {
            validate_session_model_command_replay(&existing, request)?;
            transaction.commit()?;
            return Ok(SessionModelCommandReserveOutcome {
                command: existing,
                duplicate: true,
                superseded_command_id: None,
            });
        }

        validate_session_command_authority_tx(&transaction, request)?;
        if request.command_kind == SessionModelCommandKind::Send {
            let window_start = now.saturating_sub(SESSION_SEND_WINDOW_MS);
            let count = transaction.query_row(
                r#"
                    SELECT COUNT(*)
                    FROM session_model_commands_v1
                    WHERE owner_session_ulid = ?1
                      AND target_session_ulid = ?2
                      AND command_kind = 'send'
                      AND created_at_unix_ms >= ?3
                      AND state != 'rejected'
                "#,
                params![request.owner_session_id, request.target_session_id, window_start],
                |row| row.get::<_, i64>(0),
            )?;
            if count >= SESSION_SEND_MAX_PER_WINDOW {
                return Err(JournalError::InvalidArgument(
                    "session send rate limit exceeded".to_owned(),
                ));
            }
        }

        let command_id = Ulid::generate().to_string();
        let superseded_command_id = if request.command_kind.coalesces_pending() {
            supersede_pending_session_command_tx(&transaction, request, command_id.as_str(), now)?
        } else {
            None
        };
        transaction.execute(
            r#"
                INSERT INTO session_model_commands_v1 (
                    command_ulid,
                    request_key,
                    command_kind,
                    owner_session_ulid,
                    owner_run_ulid,
                    target_session_ulid,
                    target_run_ulid,
                    ownership_task_ulid,
                    owner_principal,
                    device_id,
                    channel,
                    payload_sha256,
                    requested_model_profile,
                    state,
                    reason_code,
                    schema_version,
                    created_at_unix_ms,
                    updated_at_unix_ms
                )
                VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9,
                    ?10, ?11, ?12, ?13, 'reserved',
                    'session.command.reserved', ?14, ?15, ?15
                )
            "#,
            params![
                command_id,
                request.request_key,
                request.command_kind.as_str(),
                request.owner_session_id,
                request.owner_run_id,
                request.target_session_id,
                request.target_run_id,
                request.ownership_task_id,
                request.owner_principal,
                request.device_id,
                request.channel,
                request.payload_sha256,
                request.requested_model_profile,
                SESSION_OPERATION_SCHEMA_VERSION,
                now,
            ],
        )?;
        let command = load_session_model_command_tx(&transaction, command_id.as_str())?
            .ok_or_else(|| {
                JournalError::InvalidArgument(
                    "reserved session command could not be reloaded".to_owned(),
                )
            })?;
        transaction.commit()?;
        Ok(SessionModelCommandReserveOutcome { command, duplicate: false, superseded_command_id })
    }

    /// Attaches the observable queue/control result to one reserved command.
    ///
    /// # Errors
    /// Returns a typed journal error when the command is absent or the
    /// requested state is not part of the closed contract. Concurrent or
    /// replayed settlements return the first durable terminal result.
    pub fn settle_session_model_command(
        &self,
        request: &SessionModelCommandSettlementRequest,
    ) -> Result<SessionModelCommandRecord, JournalError> {
        if !matches!(
            request.state.as_str(),
            "queued" | "delivered" | "target_busy" | "rejected" | "interrupted"
        ) {
            return Err(JournalError::InvalidArgument(
                "session command settlement state is invalid".to_owned(),
            ));
        }
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        guard.execute(
            r#"
                UPDATE session_model_commands_v1
                SET
                    state = ?2,
                    reason_code = ?3,
                    queued_input_ulid = ?4,
                    updated_at_unix_ms = ?5
                WHERE command_ulid = ?1
                  AND state = 'reserved'
            "#,
            params![
                request.command_id,
                request.state,
                request.reason_code,
                request.queued_input_id,
                now,
            ],
        )?;
        let record = load_session_model_command_tx(&guard, request.command_id.as_str())?
            .ok_or_else(|| {
                JournalError::InvalidArgument("session command was not found".to_owned())
            })?;
        Ok(record)
    }

    /// Returns only the newest bounded transcript window for one session.
    ///
    /// # Errors
    /// Returns a typed journal error when the query fails.
    pub fn list_bounded_orchestrator_session_transcript(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<OrchestratorSessionTranscriptRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let limit = i64::try_from(limit.clamp(1, 64))
            .map_err(|_| JournalError::InvalidArgument("history limit is invalid".to_owned()))?;
        let mut statement = guard.prepare(
            r#"
                SELECT
                    session_ulid,
                    run_ulid,
                    seq,
                    event_type,
                    payload_json,
                    created_at_unix_ms,
                    origin_kind,
                    origin_run_ulid
                FROM (
                    SELECT
                        runs.session_ulid AS session_ulid,
                        tape.run_ulid AS run_ulid,
                        tape.seq AS seq,
                        tape.event_type AS event_type,
                        tape.payload_json AS payload_json,
                        tape.created_at_unix_ms AS created_at_unix_ms,
                        COALESCE(runs.origin_kind, 'manual') AS origin_kind,
                        runs.origin_run_ulid AS origin_run_ulid
                    FROM orchestrator_tape AS tape
                    INNER JOIN orchestrator_runs AS runs
                        ON runs.run_ulid = tape.run_ulid
                    WHERE runs.session_ulid = ?1
                    ORDER BY runs.started_at_unix_ms DESC, tape.seq DESC
                    LIMIT ?2
                )
                ORDER BY created_at_unix_ms ASC, seq ASC
            "#,
        )?;
        let mut rows = statement.query(params![session_id, limit])?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            records.push(OrchestratorSessionTranscriptRecord {
                session_id: row.get(0)?,
                run_id: row.get(1)?,
                seq: row.get(2)?,
                event_type: row.get(3)?,
                payload_json: row.get(4)?,
                created_at_unix_ms: row.get(5)?,
                origin_kind: row.get(6)?,
                origin_run_id: row.get(7)?,
            });
        }
        Ok(records)
    }

    /// Lists active run generations for one exact principal/device/channel
    /// scope in a single query.
    ///
    /// # Errors
    /// Returns a typed journal error when the query fails or a generation is
    /// outside the public unsigned integer range.
    pub fn list_scoped_session_runtime_generations(
        &self,
        principal: &str,
        device_id: &str,
        channel: Option<&str>,
    ) -> Result<Vec<ScopedSessionRuntimeGeneration>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let now = current_unix_ms()?;
        let mut statement = guard.prepare(
            r#"
                SELECT leases.session_ulid, leases.run_ulid, leases.generation
                FROM runtime_generation_leases AS leases
                INNER JOIN orchestrator_sessions AS sessions
                    ON sessions.session_ulid = leases.session_ulid
                WHERE leases.lane = 'run'
                  AND leases.run_ulid IS NOT NULL
                  AND leases.expires_at_unix_ms > ?1
                  AND sessions.principal = ?2
                  AND sessions.device_id = ?3
                  AND (
                    sessions.channel = ?4
                    OR (sessions.channel IS NULL AND ?4 IS NULL)
                  )
            "#,
        )?;
        let mut rows = statement.query(params![now, principal, device_id, channel])?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            let generation = row.get::<_, i64>(2)?;
            records.push(ScopedSessionRuntimeGeneration {
                session_id: row.get(0)?,
                run_id: row.get::<_, Option<String>>(1)?.ok_or_else(|| {
                    rusqlite::Error::InvalidColumnType(
                        1,
                        "run_ulid".to_owned(),
                        rusqlite::types::Type::Null,
                    )
                })?,
                generation: u64::try_from(generation).map_err(|_| {
                    JournalError::InvalidArgument(
                        "runtime generation is outside the unsigned range".to_owned(),
                    )
                })?,
            });
        }
        Ok(records)
    }
}

fn validate_session_model_command_replay(
    existing: &SessionModelCommandRecord,
    request: &SessionModelCommandReserveRequest,
) -> Result<(), JournalError> {
    if existing.command_kind != request.command_kind
        || existing.owner_run_id != request.owner_run_id
        || existing.target_session_id != request.target_session_id
        || existing.target_run_id != request.target_run_id
        || existing.ownership_task_id != request.ownership_task_id
        || existing.owner_principal != request.owner_principal
        || existing.device_id != request.device_id
        || existing.channel != request.channel
        || existing.payload_sha256 != request.payload_sha256
        || existing.requested_model_profile != request.requested_model_profile
    {
        return Err(JournalError::InvalidArgument(
            "session command idempotency key was reused with different parameters".to_owned(),
        ));
    }
    Ok(())
}

fn validate_session_model_command_request(
    request: &SessionModelCommandReserveRequest,
) -> Result<(), JournalError> {
    for (field, value) in [
        ("request_key", request.request_key.as_str()),
        ("owner_session_id", request.owner_session_id.as_str()),
        ("owner_run_id", request.owner_run_id.as_str()),
        ("target_session_id", request.target_session_id.as_str()),
        ("target_run_id", request.target_run_id.as_str()),
        ("ownership_task_id", request.ownership_task_id.as_str()),
        ("owner_principal", request.owner_principal.as_str()),
        ("device_id", request.device_id.as_str()),
        ("payload_sha256", request.payload_sha256.as_str()),
    ] {
        if value.trim().is_empty() || value.len() > 256 {
            return Err(JournalError::InvalidArgument(format!(
                "session command {field} is invalid"
            )));
        }
    }
    if request.payload_sha256.len() != 64
        || !request.payload_sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(JournalError::InvalidArgument(
            "session command payload_sha256 is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_session_command_authority_tx(
    transaction: &Transaction<'_>,
    request: &SessionModelCommandReserveRequest,
) -> Result<(), JournalError> {
    let owner_session =
        load_orchestrator_session_by_id(transaction, request.owner_session_id.as_str())?
            .ok_or_else(|| JournalError::SessionNotFound {
                selector: request.owner_session_id.clone(),
            })?;
    if owner_session.principal != request.owner_principal
        || owner_session.device_id != request.device_id
        || owner_session.channel != request.channel
    {
        return Err(JournalError::InvalidArgument(
            "session command owner scope is invalid".to_owned(),
        ));
    }
    let target_session =
        load_orchestrator_session_by_id(transaction, request.target_session_id.as_str())?
            .ok_or_else(|| JournalError::SessionNotFound {
                selector: request.target_session_id.clone(),
            })?;
    if target_session.principal != request.owner_principal
        || target_session.device_id != request.device_id
        || target_session.channel != request.channel
    {
        return Err(JournalError::InvalidArgument(
            "session command target scope is invalid".to_owned(),
        ));
    }
    if target_session.archived_at_unix_ms.is_some() {
        return Err(JournalError::InvalidArgument("session command target is archived".to_owned()));
    }
    let owner_run_session = transaction
        .query_row(
            "SELECT session_ulid FROM orchestrator_runs WHERE run_ulid = ?1",
            params![request.owner_run_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if owner_run_session.as_deref() != Some(request.owner_session_id.as_str()) {
        return Err(JournalError::InvalidArgument(
            "session command owner run is invalid".to_owned(),
        ));
    }
    let target_run_session = transaction
        .query_row(
            "SELECT session_ulid FROM orchestrator_runs WHERE run_ulid = ?1",
            params![request.target_run_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if target_run_session.as_deref() != Some(request.target_session_id.as_str()) {
        return Err(JournalError::InvalidArgument(
            "session command target run is invalid".to_owned(),
        ));
    }
    let task_authority = transaction
        .query_row(
            r#"
                SELECT
                    session_ulid,
                    child_session_ulid,
                    target_run_ulid,
                    owner_principal,
                    device_id,
                    channel
                FROM orchestrator_background_tasks
                WHERE task_ulid = ?1
            "#,
            params![request.ownership_task_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((task_session, child_session, target_run, principal, device_id, channel)) =
        task_authority
    else {
        return Err(JournalError::InvalidArgument(
            "session command ownership token is invalid".to_owned(),
        ));
    };
    let capability_parent_is_related = task_session == request.owner_session_id
        || session_descends_from_tx(
            transaction,
            request.owner_session_id.as_str(),
            task_session.as_str(),
        )?;
    if !capability_parent_is_related
        || child_session.as_deref() != Some(request.target_session_id.as_str())
        || target_run.as_deref() != Some(request.target_run_id.as_str())
        || principal != request.owner_principal
        || device_id != request.device_id
        || channel != request.channel
    {
        return Err(JournalError::InvalidArgument(
            "session command ownership token does not authorize the target".to_owned(),
        ));
    }
    Ok(())
}

fn session_descends_from_tx(
    connection: &Connection,
    root_session_id: &str,
    candidate_session_id: &str,
) -> Result<bool, JournalError> {
    let mut cursor = Some(candidate_session_id.to_owned());
    let mut visited = BTreeSet::new();
    for _ in 0..32 {
        let Some(session_id) = cursor else {
            return Ok(false);
        };
        if session_id == root_session_id {
            return Ok(true);
        }
        if !visited.insert(session_id.clone()) {
            return Err(JournalError::InvalidArgument(
                "session lineage contains a cycle".to_owned(),
            ));
        }
        cursor = connection
            .query_row(
                "SELECT parent_session_ulid FROM orchestrator_sessions WHERE session_ulid = ?1",
                params![session_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
    }
    Err(JournalError::InvalidArgument("session lineage exceeds the supported depth".to_owned()))
}

fn supersede_pending_session_command_tx(
    transaction: &Transaction<'_>,
    request: &SessionModelCommandReserveRequest,
    replacement_command_id: &str,
    now: i64,
) -> Result<Option<String>, JournalError> {
    let existing = transaction
        .query_row(
            r#"
                SELECT command_ulid, queued_input_ulid
                FROM session_model_commands_v1
                WHERE target_session_ulid = ?1
                  AND command_kind = ?2
                  AND state IN ('reserved', 'queued', 'target_busy')
                ORDER BY created_at_unix_ms DESC
                LIMIT 1
            "#,
            params![request.target_session_id, request.command_kind.as_str()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?;
    let Some((command_id, queued_input_id)) = existing else {
        return Ok(None);
    };
    transaction.execute(
        r#"
            UPDATE session_model_commands_v1
            SET
                state = 'superseded',
                reason_code = 'session.command.superseded',
                superseded_by_command_ulid = ?2,
                updated_at_unix_ms = ?3
            WHERE command_ulid = ?1
        "#,
        params![command_id, replacement_command_id, now],
    )?;
    if let Some(queued_input_id) = queued_input_id {
        transaction.execute(
            r#"
                UPDATE orchestrator_queued_inputs
                SET
                    state = 'superseded',
                    decision_reason = 'session.command.superseded',
                    terminal_at_unix_ms = ?2,
                    updated_at_unix_ms = ?2
                WHERE queued_input_ulid = ?1
                  AND state IN ('pending', 'deferred', 'claimed')
            "#,
            params![queued_input_id, now],
        )?;
    }
    Ok(Some(command_id))
}

fn load_session_model_command_by_request_key_tx(
    connection: &Connection,
    owner_session_id: &str,
    request_key: &str,
) -> Result<Option<SessionModelCommandRecord>, JournalError> {
    let query = format!(
        "SELECT {SESSION_MODEL_COMMAND_COLUMNS} FROM session_model_commands_v1 \
         WHERE owner_session_ulid = ?1 AND request_key = ?2"
    );
    connection
        .query_row(
            query.as_str(),
            params![owner_session_id, request_key],
            map_session_model_command_row,
        )
        .optional()
        .map_err(JournalError::from)
}

fn load_session_model_command_tx(
    connection: &Connection,
    command_id: &str,
) -> Result<Option<SessionModelCommandRecord>, JournalError> {
    let query = format!(
        "SELECT {SESSION_MODEL_COMMAND_COLUMNS} FROM session_model_commands_v1 \
         WHERE command_ulid = ?1"
    );
    connection
        .query_row(query.as_str(), params![command_id], map_session_model_command_row)
        .optional()
        .map_err(JournalError::from)
}

const SESSION_MODEL_COMMAND_COLUMNS: &str = r#"
    command_ulid,
    request_key,
    command_kind,
    owner_session_ulid,
    owner_run_ulid,
    target_session_ulid,
    target_run_ulid,
    ownership_task_ulid,
    owner_principal,
    device_id,
    channel,
    payload_sha256,
    requested_model_profile,
    state,
    reason_code,
    queued_input_ulid,
    superseded_by_command_ulid,
    created_at_unix_ms,
    updated_at_unix_ms
"#;

fn map_session_model_command_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<SessionModelCommandRecord> {
    let kind = match row.get::<_, String>(2)?.as_str() {
        "send" => SessionModelCommandKind::Send,
        "steer" => SessionModelCommandKind::Steer,
        "interrupt" => SessionModelCommandKind::Interrupt,
        "switch_model" => SessionModelCommandKind::SwitchModel,
        _ => return Err(rusqlite::Error::InvalidQuery),
    };
    Ok(SessionModelCommandRecord {
        command_id: row.get(0)?,
        request_key: row.get(1)?,
        command_kind: kind,
        owner_session_id: row.get(3)?,
        owner_run_id: row.get(4)?,
        target_session_id: row.get(5)?,
        target_run_id: row.get(6)?,
        ownership_task_id: row.get(7)?,
        owner_principal: row.get(8)?,
        device_id: row.get(9)?,
        channel: row.get(10)?,
        payload_sha256: row.get(11)?,
        requested_model_profile: row.get(12)?,
        state: row.get(13)?,
        reason_code: row.get(14)?,
        queued_input_id: row.get(15)?,
        superseded_by_command_id: row.get(16)?,
        created_at_unix_ms: row.get(17)?,
        updated_at_unix_ms: row.get(18)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delegation::{
        DelegationExecutionMode, DelegationMemoryScopeKind, DelegationMergeContract,
        DelegationMergeStrategy, DelegationRole, DelegationRuntimeLimits,
    };

    struct SessionCommandFixture {
        _root: tempfile::TempDir,
        store: JournalStore,
        parent_session_id: String,
        parent_run_id: String,
        child_session_id: String,
        child_run_id: String,
        task: OrchestratorBackgroundTaskRecord,
    }

    fn fixture() -> SessionCommandFixture {
        let root = tempfile::tempdir().expect("temporary journal root should create");
        let store = JournalStore::open(JournalConfig {
            db_path: root.path().join("journal.db"),
            hash_chain_enabled: false,
            max_payload_bytes: 1024 * 1024,
            max_events: 10_000,
        })
        .expect("journal store should open");
        let parent_session_id = Ulid::generate().to_string();
        let parent_run_id = Ulid::generate().to_string();
        let child_session_id = Ulid::generate().to_string();
        let child_run_id = Ulid::generate().to_string();
        store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: parent_session_id.clone(),
                session_key: parent_session_id.clone(),
                session_label: None,
                principal: "user:session-operations".to_owned(),
                device_id: "device-session-operations".to_owned(),
                channel: Some("cli".to_owned()),
            })
            .expect("parent session should create");
        store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: parent_run_id.clone(),
                session_id: parent_session_id.clone(),
                origin_kind: "test".to_owned(),
                origin_run_id: None,
                triggered_by_principal: Some("user:session-operations".to_owned()),
                parameter_delta_json: None,
                delegated_admission: None,
            })
            .expect("parent run should start");
        store
            .update_orchestrator_run_state(
                parent_run_id.as_str(),
                RunLifecycleState::InProgress,
                None,
            )
            .expect("parent run should enter progress");
        let cancellation_context = CancellationContextV1 {
            schema_version: 1,
            scope_id: RuntimeOperationId::parse("child_task:session-operations")
                .expect("child task scope id should validate"),
            scope: CancellationScopeKind::ChildTask,
            generation: RuntimeGeneration::new(1).expect("test generation should validate"),
            parent_scope_id: Some(
                RuntimeOperationId::parse("run:session-operations")
                    .expect("parent run scope id should validate"),
            ),
            reason: None,
            deadline_unix_ms: Some(i64::MAX),
            graceful_settle_ms: 500,
            hard_abort_after_ms: 2_000,
        };
        let task = store
            .create_orchestrator_background_task(&OrchestratorBackgroundTaskCreateRequest {
                task_id: Ulid::generate().to_string(),
                task_kind: AuxiliaryTaskKind::DelegationPrompt.as_str().to_owned(),
                session_id: parent_session_id.clone(),
                child_session_id: Some(child_session_id.clone()),
                parent_run_id: Some(parent_run_id.clone()),
                target_run_id: None,
                planned_child_run_id: Some(child_run_id.clone()),
                queued_input_id: None,
                owner_principal: "user:session-operations".to_owned(),
                device_id: "device-session-operations".to_owned(),
                channel: Some("cli".to_owned()),
                state: AuxiliaryTaskState::Queued.as_str().to_owned(),
                priority: 0,
                max_attempts: 3,
                budget_tokens: 2_048,
                delegation: Some(DelegationSnapshot {
                    profile_id: "research".to_owned(),
                    display_name: "Research".to_owned(),
                    description: None,
                    template_id: None,
                    role: DelegationRole::Research,
                    execution_mode: DelegationExecutionMode::Parallel,
                    group_id: "default".to_owned(),
                    model_profile: "deterministic".to_owned(),
                    tool_allowlist: Vec::new(),
                    skill_allowlist: Vec::new(),
                    memory_scope: DelegationMemoryScopeKind::ParentSession,
                    budget_tokens: 2_048,
                    max_attempts: 3,
                    merge_contract: DelegationMergeContract {
                        strategy: DelegationMergeStrategy::Summarize,
                        approval_required: false,
                    },
                    runtime_limits: DelegationRuntimeLimits::default(),
                    agent_id: Some("main".to_owned()),
                }),
                cancellation_context: Some(cancellation_context.clone()),
                not_before_unix_ms: None,
                expires_at_unix_ms: None,
                notification_target_json: None,
                input_text: Some("child objective".to_owned()),
                payload_json: None,
            })
            .expect("background task should create");
        let task = store
            .claim_orchestrator_background_task(&OrchestratorBackgroundTaskClaimRequest {
                task_id: task.task_id,
                expected_revision: task.revision,
                started_at_unix_ms: current_unix_ms().expect("clock should be available"),
            })
            .expect("background task should enter running");
        store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: child_run_id.clone(),
                session_id: child_session_id.clone(),
                origin_kind: "delegation".to_owned(),
                origin_run_id: Some(parent_run_id.clone()),
                triggered_by_principal: Some("user:session-operations".to_owned()),
                parameter_delta_json: Some(
                    json!({
                        "background_task": {
                            "schema_version": 1,
                            "task_id": task.task_id.clone(),
                            "task_kind": task.task_kind.clone(),
                            "parent_session_id": parent_session_id.clone(),
                            "child_session_id": child_session_id.clone(),
                            "parent_run_id": parent_run_id.clone(),
                            "budget_tokens": task.budget_tokens,
                            "cancellation_context": cancellation_context.clone(),
                        }
                    })
                    .to_string(),
                ),
                delegated_admission: Some(DelegatedRunAdmissionV1 {
                    task_id: task.task_id.clone(),
                    task_kind: task.task_kind.clone(),
                    parent_session_id: parent_session_id.clone(),
                    child_session_id: child_session_id.clone(),
                    parent_run_id: parent_run_id.clone(),
                    cancellation_context,
                }),
            })
            .expect("delegated child run should start");
        store
            .attach_background_task_child(
                task.task_id.as_str(),
                child_run_id.as_str(),
                task.execution_generation,
            )
            .expect("delegated child run should attach");
        store
            .update_orchestrator_run_state(
                child_run_id.as_str(),
                RunLifecycleState::InProgress,
                None,
            )
            .expect("child run should enter progress");
        let task = store
            .get_orchestrator_background_task(task.task_id.as_str())
            .expect("attached task should reload")
            .expect("attached task should exist");
        SessionCommandFixture {
            _root: root,
            store,
            parent_session_id,
            parent_run_id,
            child_session_id,
            child_run_id,
            task,
        }
    }

    fn request(
        fixture: &SessionCommandFixture,
        request_key: &str,
        kind: SessionModelCommandKind,
    ) -> SessionModelCommandReserveRequest {
        SessionModelCommandReserveRequest {
            request_key: request_key.to_owned(),
            command_kind: kind,
            owner_session_id: fixture.parent_session_id.clone(),
            owner_run_id: fixture.parent_run_id.clone(),
            target_session_id: fixture.child_session_id.clone(),
            target_run_id: fixture.child_run_id.clone(),
            ownership_task_id: fixture.task.task_id.clone(),
            owner_principal: "user:session-operations".to_owned(),
            device_id: "device-session-operations".to_owned(),
            channel: Some("cli".to_owned()),
            payload_sha256: sha256_hex(request_key.as_bytes()),
            requested_model_profile: None,
        }
    }

    #[test]
    fn ownership_token_is_exact_and_idempotent() {
        let fixture = fixture();
        let reserved_request = request(&fixture, "send-1", SessionModelCommandKind::Send);
        let first = fixture
            .store
            .reserve_session_model_command(&reserved_request)
            .expect("authorized command should reserve");
        let replay = fixture
            .store
            .reserve_session_model_command(&reserved_request)
            .expect("idempotent replay should load");
        assert!(!first.duplicate);
        assert!(replay.duplicate);
        assert_eq!(first.command.command_id, replay.command.command_id);

        let mut mismatched_replay = reserved_request.clone();
        mismatched_replay.payload_sha256 = sha256_hex(b"different payload");
        let error = fixture
            .store
            .reserve_session_model_command(&mismatched_replay)
            .expect_err("idempotency key reuse with another payload must fail closed");
        assert!(error.to_string().contains("different parameters"));

        let mut invalid = request(&fixture, "send-2", SessionModelCommandKind::Send);
        invalid.ownership_task_id = Ulid::generate().to_string();
        let error = fixture
            .store
            .reserve_session_model_command(&invalid)
            .expect_err("unknown task token must fail closed");
        assert!(error.to_string().contains("ownership token"));
    }

    #[test]
    fn archived_target_is_rejected() {
        let fixture = fixture();
        fixture
            .store
            .connection
            .lock()
            .expect("journal lock should be available")
            .execute(
                "UPDATE orchestrator_sessions SET archived_at_unix_ms = 1 WHERE session_ulid = ?1",
                params![fixture.child_session_id],
            )
            .expect("test target should archive");

        let error = fixture
            .store
            .reserve_session_model_command(&request(
                &fixture,
                "send-archived",
                SessionModelCommandKind::Send,
            ))
            .expect_err("archived target must reject writes");
        assert!(error.to_string().contains("archived"));
    }

    #[test]
    fn send_rate_limit_is_durable() {
        let fixture = fixture();
        for index in 0..SESSION_SEND_MAX_PER_WINDOW {
            fixture
                .store
                .reserve_session_model_command(&request(
                    &fixture,
                    format!("send-{index}").as_str(),
                    SessionModelCommandKind::Send,
                ))
                .expect("send inside rate window should reserve");
        }
        let error = fixture
            .store
            .reserve_session_model_command(&request(
                &fixture,
                "send-over-limit",
                SessionModelCommandKind::Send,
            ))
            .expect_err("burst beyond rate limit must reject");
        assert!(error.to_string().contains("rate limit"));
    }

    #[test]
    fn rapid_model_switch_supersedes_pending_command() {
        let fixture = fixture();
        let mut first_request = request(&fixture, "switch-1", SessionModelCommandKind::SwitchModel);
        first_request.requested_model_profile = Some("model-a".to_owned());
        let first = fixture
            .store
            .reserve_session_model_command(&first_request)
            .expect("first model switch should reserve");
        fixture
            .store
            .settle_session_model_command(&SessionModelCommandSettlementRequest {
                command_id: first.command.command_id.clone(),
                state: "queued".to_owned(),
                reason_code: "session.command.queued".to_owned(),
                queued_input_id: None,
            })
            .expect("first model switch should queue");

        let mut second_request =
            request(&fixture, "switch-2", SessionModelCommandKind::SwitchModel);
        second_request.requested_model_profile = Some("model-b".to_owned());
        let second = fixture
            .store
            .reserve_session_model_command(&second_request)
            .expect("second model switch should reserve");
        assert_eq!(second.superseded_command_id, Some(first.command.command_id.clone()));
        let first_state = fixture
            .store
            .connection
            .lock()
            .expect("journal lock should be available")
            .query_row(
                r#"
                    SELECT state, superseded_by_command_ulid
                    FROM session_model_commands_v1
                    WHERE command_ulid = ?1
                "#,
                params![first.command.command_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .expect("superseded command should load");
        assert_eq!(first_state, ("superseded".to_owned(), Some(second.command.command_id)));
    }

    #[test]
    fn first_command_settlement_wins_concurrent_replay() {
        let fixture = fixture();
        let reserved = fixture
            .store
            .reserve_session_model_command(&request(
                &fixture,
                "settlement-race",
                SessionModelCommandKind::Steer,
            ))
            .expect("command should reserve");
        let first = fixture
            .store
            .settle_session_model_command(&SessionModelCommandSettlementRequest {
                command_id: reserved.command.command_id.clone(),
                state: "queued".to_owned(),
                reason_code: "session.command.queued".to_owned(),
                queued_input_id: Some(reserved.command.command_id.clone()),
            })
            .expect("first settlement should persist");
        let replay = fixture
            .store
            .settle_session_model_command(&SessionModelCommandSettlementRequest {
                command_id: reserved.command.command_id,
                state: "delivered".to_owned(),
                reason_code: "session.command.reconciled_delivered".to_owned(),
                queued_input_id: first.queued_input_id.clone(),
            })
            .expect("concurrent settlement should return the first durable result");

        assert_eq!(first.state, "queued");
        assert_eq!(replay.state, "queued");
        assert_eq!(replay.reason_code, "session.command.queued");
    }
}
