//! SQLite adapter for restart-safe MCP host-policy evidence.

use async_trait::async_trait;
use rusqlite::{params, OptionalExtension, Row, TransactionBehavior};

use crate::application::mcp_runtime::{
    McpPolicyAuditAppendOutcome, McpPolicyAuditEventV1, McpPolicyAuditKind, McpPolicyAuditOutcome,
    McpPolicyAuditStore, McpPolicyAuditStoreError, McpSamplingUsage,
};

use super::super::JournalStore;

const POLICY_EVENT_COLUMNS: &str = "
    event_id, server_id, runtime_generation, catalog_epoch, binding_sha256,
    kind, outcome, reserved_output_tokens, reason_code, request_sha256,
    evidence_sha256, occurred_at_unix_ms
";

#[async_trait]
impl McpPolicyAuditStore for JournalStore {
    async fn append_policy_event(
        &self,
        event: &McpPolicyAuditEventV1,
    ) -> Result<McpPolicyAuditAppendOutcome, McpPolicyAuditStoreError> {
        event.validate()?;
        let mut guard = self.connection.lock().map_err(|_| unavailable("lock_poisoned"))?;
        let transaction = guard
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| unavailable("transaction_begin_failed"))?;
        let inserted = transaction
            .execute(
                r#"
                    INSERT INTO mcp_host_policy_events_v1 (
                        event_id, server_id, runtime_generation, catalog_epoch,
                        binding_sha256, kind, outcome, reserved_output_tokens,
                        reason_code, request_sha256, evidence_sha256,
                        occurred_at_unix_ms
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12
                    )
                    ON CONFLICT(event_id) DO NOTHING
                "#,
                params![
                    event.event_id,
                    event.server_id,
                    to_sqlite(event.runtime_generation, "runtime_generation")?,
                    to_sqlite(event.catalog_epoch, "catalog_epoch")?,
                    event.binding_sha256,
                    event.kind.as_str(),
                    event.outcome.as_str(),
                    to_sqlite(event.reserved_output_tokens, "reserved_output_tokens")?,
                    event.reason_code,
                    event.request_sha256,
                    event.evidence_sha256,
                    event.occurred_at_unix_ms,
                ],
            )
            .map_err(|_| unavailable("event_insert_failed"))?;
        let outcome = if inserted == 1 {
            McpPolicyAuditAppendOutcome::Appended
        } else if inserted == 0 {
            let existing = load_event(&transaction, &event.event_id)?
                .ok_or_else(|| unavailable("event_conflict_missing"))?;
            if existing != *event {
                return Err(McpPolicyAuditStoreError::IdempotencyConflict);
            }
            McpPolicyAuditAppendOutcome::Existing
        } else {
            return Err(unavailable("event_insert_count_invalid"));
        };
        transaction.commit().map_err(|_| unavailable("transaction_commit_failed"))?;
        Ok(outcome)
    }

    async fn sampling_usage(
        &self,
        server_id: &str,
        binding_sha256: &str,
        since_unix_ms: i64,
    ) -> Result<McpSamplingUsage, McpPolicyAuditStoreError> {
        if server_id.trim().is_empty() || !valid_sha256(binding_sha256) {
            return Err(McpPolicyAuditStoreError::InvalidEvent);
        }
        let guard = self.connection.lock().map_err(|_| unavailable("lock_poisoned"))?;
        let (requests, reserved): (i64, i64) = guard
            .query_row(
                r#"
                    SELECT COUNT(*), COALESCE(SUM(reserved_output_tokens), 0)
                    FROM mcp_host_policy_events_v1
                    WHERE server_id = ?1
                      AND binding_sha256 = ?2
                      AND kind = 'sampling'
                      AND outcome = 'allowed'
                      AND occurred_at_unix_ms >= ?3
                "#,
                params![server_id, binding_sha256, since_unix_ms],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| unavailable("sampling_usage_query_failed"))?;
        Ok(McpSamplingUsage {
            requests: from_sqlite(requests, "sampling_requests")?,
            reserved_output_tokens: from_sqlite(reserved, "sampling_reserved_output_tokens")?,
        })
    }
}

fn load_event(
    connection: &rusqlite::Connection,
    event_id: &str,
) -> Result<Option<McpPolicyAuditEventV1>, McpPolicyAuditStoreError> {
    let query =
        format!("SELECT {POLICY_EVENT_COLUMNS} FROM mcp_host_policy_events_v1 WHERE event_id = ?1");
    connection
        .query_row(query.as_str(), params![event_id], RawPolicyEvent::from_row)
        .optional()
        .map_err(|_| unavailable("event_load_failed"))?
        .map(RawPolicyEvent::into_event)
        .transpose()
}

struct RawPolicyEvent {
    event_id: String,
    server_id: String,
    runtime_generation: i64,
    catalog_epoch: i64,
    binding_sha256: String,
    kind: String,
    outcome: String,
    reserved_output_tokens: i64,
    reason_code: String,
    request_sha256: String,
    evidence_sha256: Option<String>,
    occurred_at_unix_ms: i64,
}

impl RawPolicyEvent {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            event_id: row.get(0)?,
            server_id: row.get(1)?,
            runtime_generation: row.get(2)?,
            catalog_epoch: row.get(3)?,
            binding_sha256: row.get(4)?,
            kind: row.get(5)?,
            outcome: row.get(6)?,
            reserved_output_tokens: row.get(7)?,
            reason_code: row.get(8)?,
            request_sha256: row.get(9)?,
            evidence_sha256: row.get(10)?,
            occurred_at_unix_ms: row.get(11)?,
        })
    }

    fn into_event(self) -> Result<McpPolicyAuditEventV1, McpPolicyAuditStoreError> {
        let event = McpPolicyAuditEventV1 {
            event_id: self.event_id,
            server_id: self.server_id,
            runtime_generation: from_sqlite(self.runtime_generation, "runtime_generation")?,
            catalog_epoch: from_sqlite(self.catalog_epoch, "catalog_epoch")?,
            binding_sha256: self.binding_sha256,
            kind: parse_kind(&self.kind)?,
            outcome: parse_outcome(&self.outcome)?,
            reserved_output_tokens: from_sqlite(
                self.reserved_output_tokens,
                "reserved_output_tokens",
            )?,
            reason_code: self.reason_code,
            request_sha256: self.request_sha256,
            evidence_sha256: self.evidence_sha256,
            occurred_at_unix_ms: self.occurred_at_unix_ms,
        };
        event.validate()?;
        Ok(event)
    }
}

fn parse_kind(value: &str) -> Result<McpPolicyAuditKind, McpPolicyAuditStoreError> {
    match value {
        "oauth_refresh" => Ok(McpPolicyAuditKind::OAuthRefresh),
        "elicitation" => Ok(McpPolicyAuditKind::Elicitation),
        "sampling" => Ok(McpPolicyAuditKind::Sampling),
        "roots" => Ok(McpPolicyAuditKind::Roots),
        _ => Err(corrupt("event_kind_unknown")),
    }
}

fn parse_outcome(value: &str) -> Result<McpPolicyAuditOutcome, McpPolicyAuditStoreError> {
    match value {
        "allowed" => Ok(McpPolicyAuditOutcome::Allowed),
        "denied" => Ok(McpPolicyAuditOutcome::Denied),
        "refreshed" => Ok(McpPolicyAuditOutcome::Refreshed),
        "failed" => Ok(McpPolicyAuditOutcome::Failed),
        _ => Err(corrupt("event_outcome_unknown")),
    }
}

fn to_sqlite(value: u64, field: &'static str) -> Result<i64, McpPolicyAuditStoreError> {
    i64::try_from(value).map_err(|_| corrupt(&format!("{field}_out_of_range")))
}

fn from_sqlite(value: i64, field: &'static str) -> Result<u64, McpPolicyAuditStoreError> {
    u64::try_from(value).map_err(|_| corrupt(&format!("{field}_negative")))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn corrupt(reason: &str) -> McpPolicyAuditStoreError {
    McpPolicyAuditStoreError::Corrupt { reason_code: format!("mcp.runtime.policy.store.{reason}") }
}

fn unavailable(reason: &str) -> McpPolicyAuditStoreError {
    McpPolicyAuditStoreError::Unavailable {
        reason_code: format!("mcp.runtime.policy.store.{reason}"),
    }
}
