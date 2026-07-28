//! Durable admission ledger for scheduler-owned autonomous routine wakes.
//! Every attempt is appended under one immediate transaction; a separate
//! immutable claim fence makes schedule retries coalesce across restarts.

use super::*;

pub(super) const MIGRATION_91_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS autonomous_wake_admissions_v1 (
        admission_ulid TEXT PRIMARY KEY,
        owner_principal TEXT NOT NULL,
        routine_ulid TEXT NOT NULL,
        job_ulid TEXT NOT NULL,
        coalescing_key TEXT NOT NULL,
        execution_mode TEXT NOT NULL CHECK (
            execution_mode IN ('agent', 'no_agent', 'probe_then_agent')
        ),
        authoritative INTEGER NOT NULL CHECK (authoritative IN (0, 1)),
        decision TEXT NOT NULL CHECK (
            decision IN (
                'admitted',
                'coalesced',
                'cooldown',
                'flood_guard',
                'outside_active_hours',
                'user_preempted'
            )
        ),
        reason_code TEXT NOT NULL,
        related_admission_ulid TEXT,
        cooldown_ms INTEGER NOT NULL CHECK (cooldown_ms >= 0),
        flood_window_ms INTEGER NOT NULL CHECK (flood_window_ms > 0),
        flood_max_wakes INTEGER NOT NULL CHECK (flood_max_wakes > 0),
        evidence_json TEXT NOT NULL,
        occurred_at_unix_ms INTEGER NOT NULL,
        created_at_unix_ms INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_autonomous_wake_admissions_owner_time
        ON autonomous_wake_admissions_v1(
            owner_principal,
            occurred_at_unix_ms DESC,
            admission_ulid DESC
        );
    CREATE INDEX IF NOT EXISTS idx_autonomous_wake_admissions_routine_time
        ON autonomous_wake_admissions_v1(
            owner_principal,
            routine_ulid,
            occurred_at_unix_ms DESC,
            admission_ulid DESC
        );
    CREATE INDEX IF NOT EXISTS idx_autonomous_wake_admissions_decision
        ON autonomous_wake_admissions_v1(
            decision,
            occurred_at_unix_ms DESC,
            admission_ulid DESC
        );

    CREATE TABLE IF NOT EXISTS autonomous_wake_claims_v1 (
        claim_ulid TEXT PRIMARY KEY,
        admission_ulid TEXT NOT NULL UNIQUE,
        owner_principal TEXT NOT NULL,
        routine_ulid TEXT NOT NULL,
        job_ulid TEXT NOT NULL,
        coalescing_key TEXT NOT NULL,
        authoritative INTEGER NOT NULL CHECK (authoritative IN (0, 1)),
        created_at_unix_ms INTEGER NOT NULL,
        UNIQUE(owner_principal, routine_ulid, coalescing_key, authoritative),
        FOREIGN KEY(admission_ulid)
            REFERENCES autonomous_wake_admissions_v1(admission_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_autonomous_wake_claims_job
        ON autonomous_wake_claims_v1(job_ulid, created_at_unix_ms DESC);

    CREATE TRIGGER IF NOT EXISTS autonomous_wake_admissions_v1_no_update
    BEFORE UPDATE ON autonomous_wake_admissions_v1
    BEGIN
        SELECT RAISE(ABORT, 'autonomous wake admissions are append-only');
    END;

    CREATE TRIGGER IF NOT EXISTS autonomous_wake_admissions_v1_no_delete
    BEFORE DELETE ON autonomous_wake_admissions_v1
    BEGIN
        SELECT RAISE(ABORT, 'autonomous wake admissions are append-only');
    END;

    CREATE TRIGGER IF NOT EXISTS autonomous_wake_claims_v1_no_update
    BEFORE UPDATE ON autonomous_wake_claims_v1
    BEGIN
        SELECT RAISE(ABORT, 'autonomous wake claims are append-only');
    END;

    CREATE TRIGGER IF NOT EXISTS autonomous_wake_claims_v1_no_delete
    BEFORE DELETE ON autonomous_wake_claims_v1
    BEGIN
        SELECT RAISE(ABORT, 'autonomous wake claims are append-only');
    END;
"#;

pub(crate) const AUTONOMOUS_WAKE_SCHEMA_VERSION: u64 = 1;
const AUTONOMOUS_WAKE_EVIDENCE_MAX_BYTES: usize = 4_096;
const AUTONOMOUS_WAKE_TEXT_MAX_BYTES: usize = 256;
const AUTONOMOUS_WAKE_FLOOD_WINDOW_MIN_MS: u64 = 60_000;
const AUTONOMOUS_WAKE_FLOOD_WINDOW_MAX_MS: u64 = 24 * 60 * 60 * 1_000;
const AUTONOMOUS_WAKE_FLOOD_MAX_WAKES: u32 = 100;

/// Stable scheduler admission result persisted for every autonomous wake.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AutonomousWakeDecision {
    Admitted,
    Coalesced,
    Cooldown,
    FloodGuard,
    OutsideActiveHours,
    UserPreempted,
}

impl AutonomousWakeDecision {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Coalesced => "coalesced",
            Self::Cooldown => "cooldown",
            Self::FloodGuard => "flood_guard",
            Self::OutsideActiveHours => "outside_active_hours",
            Self::UserPreempted => "user_preempted",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "admitted" => Some(Self::Admitted),
            "coalesced" => Some(Self::Coalesced),
            "cooldown" => Some(Self::Cooldown),
            "flood_guard" => Some(Self::FloodGuard),
            "outside_active_hours" => Some(Self::OutsideActiveHours),
            "user_preempted" => Some(Self::UserPreempted),
            _ => None,
        }
    }

    pub(crate) const fn reason_code(self, authoritative: bool) -> &'static str {
        match (authoritative, self) {
            (true, Self::Admitted) => "wake.admitted",
            (true, Self::Coalesced) => "wake.coalesced",
            (true, Self::Cooldown) => "wake.cooldown",
            (true, Self::FloodGuard) => "wake.flood_guard",
            (true, Self::OutsideActiveHours) => "wake.outside_active_hours",
            (true, Self::UserPreempted) => "wake.user_preempted",
            (false, Self::Admitted) => "wake.shadow.admitted",
            (false, Self::Coalesced) => "wake.shadow.coalesced",
            (false, Self::Cooldown) => "wake.shadow.cooldown",
            (false, Self::FloodGuard) => "wake.shadow.flood_guard",
            (false, Self::OutsideActiveHours) => "wake.shadow.outside_active_hours",
            (false, Self::UserPreempted) => "wake.shadow.user_preempted",
        }
    }

    pub(crate) const fn admitted(self) -> bool {
        matches!(self, Self::Admitted)
    }
}

/// Complete, bounded input to the atomic autonomous wake gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AutonomousWakeAdmissionRequest {
    pub(crate) owner_principal: String,
    pub(crate) routine_id: String,
    pub(crate) job_id: String,
    pub(crate) coalescing_key: String,
    pub(crate) execution_mode: String,
    pub(crate) authoritative: bool,
    pub(crate) active_hours_allowed: bool,
    pub(crate) cooldown_ms: u64,
    pub(crate) flood_window_ms: u64,
    pub(crate) flood_max_wakes: u32,
    pub(crate) evidence_json: String,
    pub(crate) occurred_at_unix_ms: i64,
}

/// Immutable audit record returned to the scheduler and diagnostics surfaces.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AutonomousWakeAdmissionRecord {
    pub(crate) schema_version: u64,
    pub(crate) admission_id: String,
    pub(crate) owner_principal: String,
    pub(crate) routine_id: String,
    pub(crate) job_id: String,
    pub(crate) coalescing_key: String,
    pub(crate) execution_mode: String,
    pub(crate) authoritative: bool,
    pub(crate) decision: AutonomousWakeDecision,
    pub(crate) reason_code: String,
    pub(crate) related_admission_id: Option<String>,
    pub(crate) cooldown_ms: u64,
    pub(crate) flood_window_ms: u64,
    pub(crate) flood_max_wakes: u32,
    pub(crate) evidence_json: String,
    pub(crate) occurred_at_unix_ms: i64,
    pub(crate) created_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct AutonomousWakeDiagnostics {
    pub(crate) schema_version: u64,
    pub(crate) total_attempts: u64,
    pub(crate) authoritative_attempts: u64,
    pub(crate) shadow_attempts: u64,
    pub(crate) admitted_attempts: u64,
    pub(crate) coalesced_attempts: u64,
    pub(crate) authoritative_blocked_attempts: u64,
    pub(crate) shadow_would_block_attempts: u64,
    pub(crate) last_decision: Option<AutonomousWakeDecision>,
    pub(crate) last_reason_code: Option<String>,
}

impl JournalStore {
    /// Atomically applies user priority, coalescing, cooldown, and flood gates.
    pub(crate) fn admit_autonomous_wake(
        &self,
        request: &AutonomousWakeAdmissionRequest,
    ) -> Result<AutonomousWakeAdmissionRecord, JournalError> {
        validate_admission_request(request)?;
        let now = request.occurred_at_unix_ms;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let (decision, related_admission_id) = if !request.active_hours_allowed {
            (AutonomousWakeDecision::OutsideActiveHours, None)
        } else if owner_has_active_user_input_tx(&transaction, request.owner_principal.as_str())? {
            (AutonomousWakeDecision::UserPreempted, None)
        } else if let Some(existing) = existing_claim_admission_tx(
            &transaction,
            request.owner_principal.as_str(),
            request.routine_id.as_str(),
            request.coalescing_key.as_str(),
            request.authoritative,
        )? {
            (AutonomousWakeDecision::Coalesced, Some(existing))
        } else if routine_is_in_cooldown_tx(
            &transaction,
            request.owner_principal.as_str(),
            request.routine_id.as_str(),
            request.authoritative,
            request.cooldown_ms,
            now,
        )? {
            (AutonomousWakeDecision::Cooldown, None)
        } else if owner_flood_budget_exhausted_tx(
            &transaction,
            request.owner_principal.as_str(),
            request.authoritative,
            request.flood_window_ms,
            request.flood_max_wakes,
            now,
        )? {
            (AutonomousWakeDecision::FloodGuard, None)
        } else {
            (AutonomousWakeDecision::Admitted, None)
        };

        let admission_id = Ulid::new().to_string();
        let created_at_unix_ms = current_unix_ms()?;
        transaction.execute(
            r#"
                INSERT INTO autonomous_wake_admissions_v1 (
                    admission_ulid,
                    owner_principal,
                    routine_ulid,
                    job_ulid,
                    coalescing_key,
                    execution_mode,
                    authoritative,
                    decision,
                    reason_code,
                    related_admission_ulid,
                    cooldown_ms,
                    flood_window_ms,
                    flood_max_wakes,
                    evidence_json,
                    occurred_at_unix_ms,
                    created_at_unix_ms
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16
                )
            "#,
            params![
                admission_id,
                request.owner_principal,
                request.routine_id,
                request.job_id,
                request.coalescing_key,
                request.execution_mode,
                i64::from(request.authoritative),
                decision.as_str(),
                decision.reason_code(request.authoritative),
                related_admission_id,
                u64_to_sqlite(request.cooldown_ms, "autonomous wake cooldown_ms")?,
                u64_to_sqlite(request.flood_window_ms, "autonomous wake flood_window_ms")?,
                i64::from(request.flood_max_wakes),
                request.evidence_json,
                now,
                created_at_unix_ms,
            ],
        )?;
        if decision.admitted() {
            transaction.execute(
                r#"
                    INSERT INTO autonomous_wake_claims_v1 (
                        claim_ulid,
                        admission_ulid,
                        owner_principal,
                        routine_ulid,
                        job_ulid,
                        coalescing_key,
                        authoritative,
                        created_at_unix_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                "#,
                params![
                    Ulid::new().to_string(),
                    admission_id,
                    request.owner_principal,
                    request.routine_id,
                    request.job_id,
                    request.coalescing_key,
                    i64::from(request.authoritative),
                    created_at_unix_ms,
                ],
            )?;
        }
        let record = load_admission_tx(&transaction, admission_id.as_str())?.ok_or_else(|| {
            JournalError::InvalidArgument(
                "autonomous wake admission could not be reloaded".to_owned(),
            )
        })?;
        transaction.commit()?;
        Ok(record)
    }

    /// Returns owner-scoped aggregate admission evidence without payload text.
    pub(crate) fn autonomous_wake_diagnostics(
        &self,
        owner_principal: &str,
    ) -> Result<AutonomousWakeDiagnostics, JournalError> {
        validate_text(owner_principal, "owner_principal", AUTONOMOUS_WAKE_TEXT_MAX_BYTES)?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let (
            total,
            authoritative,
            shadow,
            admitted,
            coalesced,
            authoritative_blocked,
            shadow_would_block,
            last_decision,
            last_reason_code,
        ) = guard.query_row(
            r#"
                SELECT
                    COUNT(*),
                    SUM(CASE WHEN authoritative = 1 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN authoritative = 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN decision = 'admitted' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN decision = 'coalesced' THEN 1 ELSE 0 END),
                    SUM(CASE
                        WHEN authoritative = 1
                         AND decision <> 'admitted'
                        THEN 1 ELSE 0
                    END),
                    SUM(CASE
                        WHEN authoritative = 0
                         AND decision <> 'admitted'
                        THEN 1 ELSE 0
                    END),
                    (
                        SELECT decision
                        FROM autonomous_wake_admissions_v1
                        WHERE owner_principal = ?1
                        ORDER BY occurred_at_unix_ms DESC, admission_ulid DESC
                        LIMIT 1
                    ),
                    (
                        SELECT reason_code
                        FROM autonomous_wake_admissions_v1
                        WHERE owner_principal = ?1
                        ORDER BY occurred_at_unix_ms DESC, admission_ulid DESC
                        LIMIT 1
                    )
                FROM autonomous_wake_admissions_v1
                WHERE owner_principal = ?1
            "#,
            params![owner_principal],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<i64>>(1)?.unwrap_or(0),
                    row.get::<_, Option<i64>>(2)?.unwrap_or(0),
                    row.get::<_, Option<i64>>(3)?.unwrap_or(0),
                    row.get::<_, Option<i64>>(4)?.unwrap_or(0),
                    row.get::<_, Option<i64>>(5)?.unwrap_or(0),
                    row.get::<_, Option<i64>>(6)?.unwrap_or(0),
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                ))
            },
        )?;
        let total_attempts = nonnegative_count(total, "autonomous wake total_attempts")?;
        let authoritative_attempts =
            nonnegative_count(authoritative, "autonomous wake authoritative_attempts")?;
        let shadow_attempts = nonnegative_count(shadow, "autonomous wake shadow_attempts")?;
        let admitted_attempts = nonnegative_count(admitted, "autonomous wake admitted_attempts")?;
        let coalesced_attempts =
            nonnegative_count(coalesced, "autonomous wake coalesced_attempts")?;
        let authoritative_blocked_attempts = nonnegative_count(
            authoritative_blocked,
            "autonomous wake authoritative_blocked_attempts",
        )?;
        let shadow_would_block_attempts =
            nonnegative_count(shadow_would_block, "autonomous wake shadow_would_block_attempts")?;
        let last_decision = last_decision.as_deref().map(parse_decision).transpose()?;
        Ok(AutonomousWakeDiagnostics {
            schema_version: AUTONOMOUS_WAKE_SCHEMA_VERSION,
            total_attempts,
            authoritative_attempts,
            shadow_attempts,
            admitted_attempts,
            coalesced_attempts,
            authoritative_blocked_attempts,
            shadow_would_block_attempts,
            last_decision,
            last_reason_code,
        })
    }
}

fn validate_admission_request(
    request: &AutonomousWakeAdmissionRequest,
) -> Result<(), JournalError> {
    for (value, field) in [
        (request.owner_principal.as_str(), "owner_principal"),
        (request.routine_id.as_str(), "routine_id"),
        (request.job_id.as_str(), "job_id"),
        (request.coalescing_key.as_str(), "coalescing_key"),
        (request.execution_mode.as_str(), "execution_mode"),
    ] {
        validate_text(value, field, AUTONOMOUS_WAKE_TEXT_MAX_BYTES)?;
    }
    if !matches!(request.execution_mode.as_str(), "agent" | "no_agent" | "probe_then_agent") {
        return Err(JournalError::InvalidArgument(
            "autonomous wake execution_mode is outside the closed contract".to_owned(),
        ));
    }
    if !(AUTONOMOUS_WAKE_FLOOD_WINDOW_MIN_MS..=AUTONOMOUS_WAKE_FLOOD_WINDOW_MAX_MS)
        .contains(&request.flood_window_ms)
    {
        return Err(JournalError::InvalidArgument(
            "autonomous wake flood_window_ms must be 60000..=86400000".to_owned(),
        ));
    }
    if request.flood_max_wakes == 0 || request.flood_max_wakes > AUTONOMOUS_WAKE_FLOOD_MAX_WAKES {
        return Err(JournalError::InvalidArgument(
            "autonomous wake flood_max_wakes must be 1..=100".to_owned(),
        ));
    }
    if request.evidence_json.len() > AUTONOMOUS_WAKE_EVIDENCE_MAX_BYTES {
        return Err(JournalError::InvalidArgument(
            "autonomous wake evidence_json exceeds the 4096-byte bound".to_owned(),
        ));
    }
    ensure_json_field(request.evidence_json.as_str(), "autonomous wake evidence_json")
}

fn validate_text(value: &str, field: &str, max_bytes: usize) -> Result<(), JournalError> {
    if value.trim().is_empty() || value.len() > max_bytes {
        return Err(JournalError::InvalidArgument(format!(
            "{field} must be 1..={max_bytes} bytes"
        )));
    }
    Ok(())
}

fn owner_has_active_user_input_tx(
    transaction: &Transaction<'_>,
    owner_principal: &str,
) -> Result<bool, JournalError> {
    let active = transaction.query_row(
        r#"
            SELECT EXISTS(
                SELECT 1
                FROM orchestrator_queued_inputs queued
                JOIN orchestrator_sessions sessions
                  ON sessions.session_ulid = queued.session_ulid
                WHERE sessions.principal = ?1
                  AND queued.state IN ('pending', 'claimed', 'deferred')
            )
        "#,
        params![owner_principal],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(active != 0)
}

fn existing_claim_admission_tx(
    transaction: &Transaction<'_>,
    owner_principal: &str,
    routine_id: &str,
    coalescing_key: &str,
    authoritative: bool,
) -> Result<Option<String>, JournalError> {
    transaction
        .query_row(
            r#"
                SELECT admission_ulid
                FROM autonomous_wake_claims_v1
                WHERE owner_principal = ?1
                  AND routine_ulid = ?2
                  AND coalescing_key = ?3
                  AND authoritative = ?4
                LIMIT 1
            "#,
            params![owner_principal, routine_id, coalescing_key, i64::from(authoritative)],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(Into::into)
}

fn routine_is_in_cooldown_tx(
    transaction: &Transaction<'_>,
    owner_principal: &str,
    routine_id: &str,
    authoritative: bool,
    cooldown_ms: u64,
    now_unix_ms: i64,
) -> Result<bool, JournalError> {
    if cooldown_ms == 0 {
        return Ok(false);
    }
    let cooldown_ms = u64_to_sqlite(cooldown_ms, "autonomous wake cooldown_ms")?;
    let last_admitted = transaction
        .query_row(
            r#"
                SELECT occurred_at_unix_ms
                FROM autonomous_wake_admissions_v1
                WHERE owner_principal = ?1
                  AND routine_ulid = ?2
                  AND authoritative = ?3
                  AND decision = 'admitted'
                ORDER BY occurred_at_unix_ms DESC, admission_ulid DESC
                LIMIT 1
            "#,
            params![owner_principal, routine_id, i64::from(authoritative)],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    Ok(last_admitted.is_some_and(|last| last.saturating_add(cooldown_ms) > now_unix_ms))
}

fn owner_flood_budget_exhausted_tx(
    transaction: &Transaction<'_>,
    owner_principal: &str,
    authoritative: bool,
    flood_window_ms: u64,
    flood_max_wakes: u32,
    now_unix_ms: i64,
) -> Result<bool, JournalError> {
    let flood_window_ms = u64_to_sqlite(flood_window_ms, "autonomous wake flood_window_ms")?;
    let window_start = now_unix_ms.saturating_sub(flood_window_ms);
    let admitted = transaction.query_row(
        r#"
            SELECT COUNT(*)
            FROM autonomous_wake_admissions_v1
            WHERE owner_principal = ?1
              AND authoritative = ?2
              AND decision = 'admitted'
              AND occurred_at_unix_ms >= ?3
        "#,
        params![owner_principal, i64::from(authoritative), window_start],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(admitted >= i64::from(flood_max_wakes))
}

fn load_admission_tx(
    connection: &Connection,
    admission_id: &str,
) -> Result<Option<AutonomousWakeAdmissionRecord>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT
                    admission_ulid,
                    owner_principal,
                    routine_ulid,
                    job_ulid,
                    coalescing_key,
                    execution_mode,
                    authoritative,
                    decision,
                    reason_code,
                    related_admission_ulid,
                    cooldown_ms,
                    flood_window_ms,
                    flood_max_wakes,
                    evidence_json,
                    occurred_at_unix_ms,
                    created_at_unix_ms
                FROM autonomous_wake_admissions_v1
                WHERE admission_ulid = ?1
                LIMIT 1
            "#,
            params![admission_id],
            |row| {
                let authoritative = row.get::<_, i64>(6)?;
                let decision = row.get::<_, String>(7)?;
                let decision =
                    AutonomousWakeDecision::from_str(decision.as_str()).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            7,
                            rusqlite::types::Type::Text,
                            Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("invalid autonomous wake decision '{decision}'"),
                            )),
                        )
                    })?;
                let cooldown_ms = row.get::<_, i64>(10)?;
                let flood_window_ms = row.get::<_, i64>(11)?;
                let flood_max_wakes = row.get::<_, i64>(12)?;
                Ok(AutonomousWakeAdmissionRecord {
                    schema_version: AUTONOMOUS_WAKE_SCHEMA_VERSION,
                    admission_id: row.get(0)?,
                    owner_principal: row.get(1)?,
                    routine_id: row.get(2)?,
                    job_id: row.get(3)?,
                    coalescing_key: row.get(4)?,
                    execution_mode: row.get(5)?,
                    authoritative: authoritative != 0,
                    decision,
                    reason_code: row.get(8)?,
                    related_admission_id: row.get(9)?,
                    cooldown_ms: u64::try_from(cooldown_ms)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(10, cooldown_ms))?,
                    flood_window_ms: u64::try_from(flood_window_ms).map_err(|_| {
                        rusqlite::Error::IntegralValueOutOfRange(11, flood_window_ms)
                    })?,
                    flood_max_wakes: u32::try_from(flood_max_wakes).map_err(|_| {
                        rusqlite::Error::IntegralValueOutOfRange(12, flood_max_wakes)
                    })?,
                    evidence_json: row.get(13)?,
                    occurred_at_unix_ms: row.get(14)?,
                    created_at_unix_ms: row.get(15)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

fn parse_decision(value: &str) -> Result<AutonomousWakeDecision, JournalError> {
    AutonomousWakeDecision::from_str(value).ok_or_else(|| {
        JournalError::InvalidArgument(format!("invalid autonomous wake decision '{value}'"))
    })
}

fn nonnegative_count(value: i64, field: &str) -> Result<u64, JournalError> {
    u64::try_from(value)
        .map_err(|_| JournalError::InvalidArgument(format!("{field} must be non-negative")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_store(path: PathBuf) -> JournalStore {
        JournalStore::open(JournalConfig {
            db_path: path,
            hash_chain_enabled: false,
            max_payload_bytes: 1024 * 1024,
            max_events: 10_000,
        })
        .expect("journal store should open")
    }

    fn request(key: &str, occurred_at_unix_ms: i64) -> AutonomousWakeAdmissionRequest {
        AutonomousWakeAdmissionRequest {
            owner_principal: "user:wake".to_owned(),
            routine_id: "routine-health".to_owned(),
            job_id: "routine-health".to_owned(),
            coalescing_key: key.to_owned(),
            execution_mode: "probe_then_agent".to_owned(),
            authoritative: true,
            active_hours_allowed: true,
            cooldown_ms: 0,
            flood_window_ms: 60_000,
            flood_max_wakes: 8,
            evidence_json: r#"{"schema_version":1,"trigger":"schedule_due"}"#.to_owned(),
            occurred_at_unix_ms,
        }
    }

    #[test]
    fn coalescing_claim_survives_restart() {
        let root = tempfile::tempdir().expect("temporary root should create");
        let path = root.path().join("journal.db");
        let first = {
            let store = open_store(path.clone());
            store
                .admit_autonomous_wake(&request("schedule:health:100", 100))
                .expect("first wake should admit")
        };
        assert_eq!(first.decision, AutonomousWakeDecision::Admitted);

        let reopened = open_store(path);
        let replay = reopened
            .admit_autonomous_wake(&request("schedule:health:100", 101))
            .expect("replayed wake should coalesce");
        assert_eq!(replay.decision, AutonomousWakeDecision::Coalesced);
        assert_eq!(replay.related_admission_id.as_deref(), Some(first.admission_id.as_str()));
        let diagnostics =
            reopened.autonomous_wake_diagnostics("user:wake").expect("diagnostics should load");
        assert_eq!(diagnostics.total_attempts, 2);
        assert_eq!(diagnostics.authoritative_attempts, 2);
        assert_eq!(diagnostics.shadow_attempts, 0);
        assert_eq!(diagnostics.admitted_attempts, 1);
        assert_eq!(diagnostics.coalesced_attempts, 1);
        assert_eq!(diagnostics.authoritative_blocked_attempts, 1);
        assert_eq!(diagnostics.shadow_would_block_attempts, 0);
    }

    #[test]
    fn cooldown_and_owner_flood_budget_fail_closed() {
        let root = tempfile::tempdir().expect("temporary root should create");
        let path = root.path().join("journal.db");
        let first = open_store(path.clone())
            .admit_autonomous_wake(&request("schedule:health:100", 100_000))
            .expect("first wake should admit before restart");
        assert_eq!(first.decision, AutonomousWakeDecision::Admitted);

        let store = open_store(path);
        let mut cooldown = request("schedule:health:101", 100_001);
        cooldown.cooldown_ms = 1_000;
        let cooldown =
            store.admit_autonomous_wake(&cooldown).expect("cooldown should survive restart");
        assert_eq!(cooldown.decision, AutonomousWakeDecision::Cooldown);

        let mut flood = request("schedule:health:102", 100_002);
        flood.flood_max_wakes = 1;
        let flood =
            store.admit_autonomous_wake(&flood).expect("owner flood budget should survive restart");
        assert_eq!(flood.decision, AutonomousWakeDecision::FloodGuard);
    }

    #[test]
    fn active_user_input_preempts_autonomous_wake_atomically() {
        let root = tempfile::tempdir().expect("temporary root should create");
        let store = open_store(root.path().join("journal.db"));
        let session_id = Ulid::new().to_string();
        let run_id = Ulid::new().to_string();
        store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: session_id.clone(),
                session_key: format!("wake:{session_id}"),
                session_label: Some("Wake priority".to_owned()),
                principal: "user:wake".to_owned(),
                device_id: "device-wake".to_owned(),
                channel: Some("cli".to_owned()),
            })
            .expect("session should create");
        store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: run_id.clone(),
                session_id: session_id.clone(),
                origin_kind: "manual".to_owned(),
                origin_run_id: None,
                triggered_by_principal: Some("user:wake".to_owned()),
                parameter_delta_json: None,
                delegated_admission: None,
            })
            .expect("run should create");
        store
            .create_orchestrator_queued_input(&OrchestratorQueuedInputCreateRequest {
                queued_input_id: Ulid::new().to_string(),
                run_id: run_id.clone(),
                session_id,
                state: "pending".to_owned(),
                text: "new user instruction".to_owned(),
                origin_run_id: Some(run_id),
                queue_mode: "steer".to_owned(),
                delivery_boundary: "current_run_before_provider".to_owned(),
                expected_active_generation: Some(1),
                priority_lane: "normal".to_owned(),
                coalescing_group: Some("session:wake-priority".to_owned()),
                overflow_summary_ref: None,
                safe_boundary_flags_json: "{}".to_owned(),
                decision_reason: "queue.steer.accepted".to_owned(),
                attachments_json: "[]".to_owned(),
                queue_outcome_json: r#"{"lifecycle_state":"pending"}"#.to_owned(),
                accepted_at_unix_ms: Some(1),
                policy_snapshot_json: "{}".to_owned(),
                explain_json: "{}".to_owned(),
            })
            .expect("queued input should persist");

        let outcome = store
            .admit_autonomous_wake(&request("schedule:health:200", 200))
            .expect("preemption decision should persist");
        assert_eq!(outcome.decision, AutonomousWakeDecision::UserPreempted);
        assert_eq!(outcome.reason_code, "wake.user_preempted");
    }

    #[test]
    fn outside_active_hours_is_audited_without_claiming() {
        let root = tempfile::tempdir().expect("temporary root should create");
        let store = open_store(root.path().join("journal.db"));
        let mut outside = request("schedule:health:300", 300);
        outside.active_hours_allowed = false;

        let outcome =
            store.admit_autonomous_wake(&outside).expect("active-hours decision should persist");

        assert_eq!(outcome.decision, AutonomousWakeDecision::OutsideActiveHours);
        assert_eq!(outcome.related_admission_id, None);
    }

    #[test]
    fn shadow_claims_never_coalesce_authoritative_dispatch() {
        let root = tempfile::tempdir().expect("temporary root should create");
        let store = open_store(root.path().join("journal.db"));
        let mut shadow = request("schedule:health:400", 400);
        shadow.authoritative = false;
        let shadow = store.admit_autonomous_wake(&shadow).expect("shadow wake should be observed");
        let authoritative = store
            .admit_autonomous_wake(&request("schedule:health:400", 401))
            .expect("authoritative wake should have an independent claim");

        assert_eq!(shadow.decision, AutonomousWakeDecision::Admitted);
        assert!(!shadow.authoritative);
        assert_eq!(shadow.reason_code, "wake.shadow.admitted");
        assert_eq!(authoritative.decision, AutonomousWakeDecision::Admitted);
        assert!(authoritative.authoritative);
        assert_eq!(authoritative.reason_code, "wake.admitted");

        let mut shadow_replay_request = request("schedule:health:400", 402);
        shadow_replay_request.authoritative = false;
        let shadow_replay = store
            .admit_autonomous_wake(&shadow_replay_request)
            .expect("shadow replay should coalesce independently");
        assert_eq!(shadow_replay.decision, AutonomousWakeDecision::Coalesced);
        let diagnostics =
            store.autonomous_wake_diagnostics("user:wake").expect("diagnostics should load");
        assert_eq!(diagnostics.authoritative_blocked_attempts, 0);
        assert_eq!(diagnostics.shadow_would_block_attempts, 1);
    }
}
