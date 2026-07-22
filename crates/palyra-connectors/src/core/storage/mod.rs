//! Sqlite-backed durable state for connectors: instances, inbound dedupe,
//! outbox, dead letters, queue pause state, and operational events.
//!
//! [`ConnectorStore`] serializes all access through one mutex-guarded
//! connection. The outbox combines claim-token leases with a durable effect
//! fence: pre-effect claims are reclaimable, while expired in-flight effects
//! are parked for reconciliation instead of being sent twice. Per-table
//! operations live in sibling modules.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use rusqlite::{Connection, Transaction};
use thiserror::Error;

mod dead_letters;
mod delivery_intents;
mod events;
mod ingress;
mod instances;
mod outbox;
mod outbox_reconciliation;
mod queue_state;
mod records;
mod schema;

#[cfg(test)]
mod tests;

pub use records::{
    ChannelIngressEnqueueOutcome, ChannelIngressRecord, ChannelIngressStatus, ConnectorEventRecord,
    ConnectorInstanceRecord, ConnectorQueueSnapshot, DeadLetterRecord, DeliveryIntentDraft,
    DeliveryIntentRecord, DeliveryIntentRetryOutcome, DeliveryIntentStatus,
    IngressBlockedLaneSnapshot, OutboxDeliverySnapshot, OutboxEffectState, OutboxEnqueueOutcome,
    OutboxEntryRecord, OutboxReconciliationEvidence, OutboxReconciliationOutcome,
    OutboxUnknownRecord,
};

/// Handle to the connector sqlite database; cheap to share behind an `Arc`.
#[derive(Debug)]
pub struct ConnectorStore {
    db_path: PathBuf,
    connection: Mutex<Connection>,
}

/// Failure modes of connector storage operations.
#[derive(Debug, Error)]
pub enum ConnectorStoreError {
    #[error("sqlite operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serialization failed: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("connector storage lock is poisoned")]
    PoisonedLock,
    #[error("connector storage schema contains unknown connector kind '{0}'")]
    UnknownConnectorKind(String),
    #[error("connector storage schema contains unknown readiness '{0}'")]
    UnknownReadiness(String),
    #[error("connector storage schema contains unknown liveness '{0}'")]
    UnknownLiveness(String),
    #[error("connector storage schema contains unknown ingress status '{0}'")]
    UnknownIngressStatus(String),
    #[error("connector storage schema contains unknown delivery intent status '{0}'")]
    UnknownDeliveryIntentStatus(String),
    #[error("connector storage schema contains unknown outbox effect state '{0}'")]
    UnknownOutboxEffectState(String),
    #[error("connector storage value overflow while converting '{field}'")]
    ValueOverflow { field: &'static str },
    #[error("connector record not found: {0}")]
    NotFound(String),
    #[error("ingress event not found: {0}")]
    ChannelIngressNotFound(i64),
    #[error("delivery intent not found: {0}")]
    DeliveryIntentNotFound(String),
    #[error("delivery intent '{intent_id}' cannot be retried from status '{status}'")]
    InvalidDeliveryIntentRetry { intent_id: String, status: String },
    #[error("outbox entry not found: {0}")]
    OutboxNotFound(i64),
    #[error("outbox entry is not outcome-unknown: {0}")]
    OutboxNotOutcomeUnknown(i64),
    #[error("outbox reconciliation delivered message id must not be empty")]
    MissingReconciledNativeMessageId,
    #[error("dead-letter entry not found: {0}")]
    DeadLetterNotFound(i64),
    #[error(
        "dead-letter replay conflicts with live outbox envelope: connector_id={connector_id}, envelope_id={envelope_id}"
    )]
    DeadLetterReplayConflict { connector_id: String, envelope_id: String },
}

/// How long a claimed outbox entry stays invisible to other drains before the
/// claim is considered abandoned and the entry becomes reclaimable.
pub(super) const OUTBOX_CLAIM_LEASE_MS: i64 = 60_000;
static OUTBOX_CLAIM_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static INGRESS_CLAIM_SEQUENCE: AtomicU64 = AtomicU64::new(1);

impl ConnectorStore {
    /// Opens (creating if necessary) the database at `path` and applies the schema.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::Sqlite`] when the parent directory cannot
    /// be created, the database cannot be opened, or schema setup fails.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ConnectorStoreError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                // Boxed into rusqlite's catch-all variant so directory-creation
                // failures flow through the single Sqlite error path.
                fs::create_dir_all(parent)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
            }
        }
        let connection = Connection::open(path.as_path())?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        let store = Self { db_path: path, connection: Mutex::new(connection) };
        store.initialize_schema()?;
        Ok(store)
    }

    /// Returns the path of the backing sqlite database file.
    #[must_use]
    pub fn db_path(&self) -> &Path {
        self.db_path.as_path()
    }

    /// Runs `callback` inside one transaction, committing only on `Ok`.
    fn with_transaction<T, F>(&self, callback: F) -> Result<T, ConnectorStoreError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<T, ConnectorStoreError>,
    {
        let mut connection =
            self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        let transaction = connection.transaction()?;
        let output = callback(&transaction)?;
        transaction.commit()?;
        Ok(output)
    }
}

/// Returns a claim token unique within this process; the timestamp prefix
/// keeps tokens from distinct runs distinguishable in stored rows.
pub(super) fn next_outbox_claim_token(now_unix_ms: i64) -> String {
    let sequence = OUTBOX_CLAIM_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("claim-{now_unix_ms}-{sequence}")
}

pub(super) fn next_ingress_claim_token(now_unix_ms: i64) -> String {
    let sequence = INGRESS_CLAIM_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("ingress-claim-{now_unix_ms}-{sequence}")
}
