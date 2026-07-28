//! Durable V2 evidence for review-only commitment candidates.
//! Candidate insertion and owner-scoped deduplication share the commitment
//! creation transaction so retries cannot create recurring automation.

use super::*;

pub(super) const MIGRATION_90_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS commitment_candidates_v2 (
        commitment_ulid TEXT PRIMARY KEY,
        owner_principal TEXT NOT NULL,
        dedupe_key TEXT NOT NULL,
        evidence_start_byte INTEGER NOT NULL CHECK (evidence_start_byte >= 0),
        evidence_end_byte INTEGER NOT NULL CHECK (evidence_end_byte >= evidence_start_byte),
        evidence_sha256 TEXT NOT NULL,
        confidence_bps INTEGER NOT NULL CHECK (confidence_bps BETWEEN 0 AND 10000),
        recurrence_json TEXT NOT NULL,
        sensitivity TEXT NOT NULL CHECK (
            sensitivity IN ('general', 'personal', 'sensitive')
        ),
        extraction_reason_code TEXT NOT NULL,
        selection_reason_code TEXT NOT NULL,
        value_score_bps INTEGER NOT NULL CHECK (value_score_bps BETWEEN 0 AND 10000),
        sample_bucket_bps INTEGER NOT NULL CHECK (sample_bucket_bps BETWEEN 0 AND 9999),
        source_sha256 TEXT NOT NULL,
        schema_version INTEGER NOT NULL CHECK (schema_version = 2),
        created_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(commitment_ulid) REFERENCES commitments(commitment_ulid),
        UNIQUE(owner_principal, dedupe_key)
    );
    CREATE INDEX IF NOT EXISTS idx_commitment_candidates_v2_owner_created
        ON commitment_candidates_v2(owner_principal, created_at_unix_ms, commitment_ulid);
    CREATE INDEX IF NOT EXISTS idx_commitment_candidates_v2_recurrence
        ON commitment_candidates_v2(sensitivity, created_at_unix_ms);

    CREATE TRIGGER IF NOT EXISTS commitment_candidates_v2_no_update
    BEFORE UPDATE ON commitment_candidates_v2
    BEGIN
        SELECT RAISE(ABORT, 'commitment candidate evidence is append-only');
    END;

    CREATE TRIGGER IF NOT EXISTS commitment_candidates_v2_no_delete
    BEFORE DELETE ON commitment_candidates_v2
    BEGIN
        SELECT RAISE(ABORT, 'commitment candidate evidence is append-only');
    END;
"#;

pub(crate) const COMMITMENT_CANDIDATE_V2_SCHEMA_VERSION: u64 = 2;

/// Sensitivity assigned to the redacted evidence span retained for review.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CommitmentCandidateSensitivity {
    General,
    Personal,
    Sensitive,
}

impl CommitmentCandidateSensitivity {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::General => "general",
            Self::Personal => "personal",
            Self::Sensitive => "sensitive",
        }
    }
}

/// Byte-addressed source evidence without retaining unredacted source text.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommitmentEvidenceSpanV2 {
    pub(crate) start_byte: u64,
    pub(crate) end_byte: u64,
    pub(crate) redacted_text_sha256: String,
}

/// Review-only candidate evidence produced by the bounded post-turn extractor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommitmentCandidateV2 {
    pub(crate) schema_version: u64,
    pub(crate) evidence_span: CommitmentEvidenceSpanV2,
    pub(crate) confidence_bps: u64,
    pub(crate) recurrence_json: String,
    pub(crate) sensitivity: CommitmentCandidateSensitivity,
    pub(crate) dedupe_key: String,
    pub(crate) extraction_reason_code: String,
    pub(crate) selection_reason_code: String,
    pub(crate) value_score_bps: u64,
    pub(crate) sample_bucket_bps: u64,
    pub(crate) source_sha256: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct CommitmentCandidateV2Diagnostics {
    pub(crate) schema_version: u64,
    pub(crate) total_candidates: u64,
    pub(crate) recurring_candidates: u64,
    pub(crate) sensitive_candidates: u64,
}

pub(super) fn validate_candidate(candidate: &CommitmentCandidateV2) -> Result<(), JournalError> {
    if candidate.schema_version != COMMITMENT_CANDIDATE_V2_SCHEMA_VERSION {
        return Err(JournalError::InvalidArgument(
            "commitment candidate schema_version must be 2".to_owned(),
        ));
    }
    if candidate.evidence_span.end_byte <= candidate.evidence_span.start_byte {
        return Err(JournalError::InvalidArgument(
            "commitment candidate evidence span must be non-empty".to_owned(),
        ));
    }
    if candidate.confidence_bps > 10_000
        || candidate.value_score_bps > 10_000
        || candidate.sample_bucket_bps >= 10_000
    {
        return Err(JournalError::InvalidArgument(
            "commitment candidate basis-point fields are out of range".to_owned(),
        ));
    }
    for (value, field) in [
        (candidate.evidence_span.redacted_text_sha256.as_str(), "redacted_text_sha256"),
        (candidate.dedupe_key.as_str(), "dedupe_key"),
        (candidate.source_sha256.as_str(), "source_sha256"),
    ] {
        ensure_sha256(value, field)?;
    }
    for (value, field) in [
        (candidate.extraction_reason_code.as_str(), "extraction_reason_code"),
        (candidate.selection_reason_code.as_str(), "selection_reason_code"),
    ] {
        ensure_nonempty_field(value, field)?;
        if value.len() > 128 {
            return Err(JournalError::InvalidArgument(format!(
                "{field} exceeds the 128-byte bound"
            )));
        }
    }
    if candidate.recurrence_json.len() > 4_096 {
        return Err(JournalError::InvalidArgument(
            "candidate recurrence_json exceeds the 4096-byte bound".to_owned(),
        ));
    }
    ensure_json_field(candidate.recurrence_json.as_str(), "candidate recurrence_json")
}

fn ensure_sha256(value: &str, field: &str) -> Result<(), JournalError> {
    if value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(JournalError::InvalidArgument(format!("{field} must be a lowercase SHA-256 digest")))
    }
}

pub(super) fn query_deduped_commitment(
    connection: &Connection,
    owner_principal: &str,
    dedupe_key: &str,
) -> Result<Option<CommitmentRecord>, JournalError> {
    let commitment_id = connection
        .query_row(
            r#"
                SELECT commitment_ulid
                FROM commitment_candidates_v2
                WHERE owner_principal = ?1 AND dedupe_key = ?2
                LIMIT 1
            "#,
            params![owner_principal, dedupe_key],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    commitment_id
        .map(|commitment_id| query_commitment_by_id(connection, commitment_id.as_str()))
        .transpose()
        .map(Option::flatten)
}

pub(super) fn insert_candidate(
    transaction: &Transaction<'_>,
    request: &CommitmentCreateRequest,
    candidate: &CommitmentCandidateV2,
    created_at_unix_ms: i64,
) -> Result<(), JournalError> {
    transaction.execute(
        r#"
            INSERT INTO commitment_candidates_v2 (
                commitment_ulid,
                owner_principal,
                dedupe_key,
                evidence_start_byte,
                evidence_end_byte,
                evidence_sha256,
                confidence_bps,
                recurrence_json,
                sensitivity,
                extraction_reason_code,
                selection_reason_code,
                value_score_bps,
                sample_bucket_bps,
                source_sha256,
                schema_version,
                created_at_unix_ms
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16
            )
        "#,
        params![
            request.commitment_id,
            request.owner_principal,
            candidate.dedupe_key,
            u64_to_sqlite(candidate.evidence_span.start_byte, "evidence_start_byte")?,
            u64_to_sqlite(candidate.evidence_span.end_byte, "evidence_end_byte")?,
            candidate.evidence_span.redacted_text_sha256,
            u64_to_sqlite(candidate.confidence_bps, "candidate confidence_bps")?,
            candidate.recurrence_json,
            candidate.sensitivity.as_str(),
            candidate.extraction_reason_code,
            candidate.selection_reason_code,
            u64_to_sqlite(candidate.value_score_bps, "value_score_bps")?,
            u64_to_sqlite(candidate.sample_bucket_bps, "sample_bucket_bps")?,
            candidate.source_sha256,
            u64_to_sqlite(candidate.schema_version, "candidate schema_version")?,
            created_at_unix_ms,
        ],
    )?;
    Ok(())
}

impl JournalStore {
    /// Returns owner-scoped counts from the redacted V2 candidate ledger.
    pub fn commitment_candidate_v2_diagnostics(
        &self,
        owner_principal: &str,
    ) -> Result<CommitmentCandidateV2Diagnostics, JournalError> {
        ensure_nonempty_field(owner_principal, "owner_principal")?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let (total, recurring, sensitive) = guard.query_row(
            r#"
                SELECT
                    COUNT(*),
                    SUM(CASE
                        WHEN json_extract(recurrence_json, '$.type') <> 'none' THEN 1
                        ELSE 0
                    END),
                    SUM(CASE WHEN sensitivity = 'sensitive' THEN 1 ELSE 0 END)
                FROM commitment_candidates_v2
                WHERE owner_principal = ?1
            "#,
            params![owner_principal],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<i64>>(1)?.unwrap_or(0),
                    row.get::<_, Option<i64>>(2)?.unwrap_or(0),
                ))
            },
        )?;
        Ok(CommitmentCandidateV2Diagnostics {
            schema_version: COMMITMENT_CANDIDATE_V2_SCHEMA_VERSION,
            total_candidates: nonnegative_count(total, "total_candidates")?,
            recurring_candidates: nonnegative_count(recurring, "recurring_candidates")?,
            sensitive_candidates: nonnegative_count(sensitive, "sensitive_candidates")?,
        })
    }
}

fn nonnegative_count(value: i64, field: &str) -> Result<u64, JournalError> {
    u64::try_from(value)
        .map_err(|_| JournalError::InvalidArgument(format!("{field} must be a non-negative count")))
}
