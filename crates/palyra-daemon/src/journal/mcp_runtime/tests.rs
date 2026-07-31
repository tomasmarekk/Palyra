//! Durable MCP runtime migration and compare-and-swap tests.

use crate::application::mcp_runtime::{
    McpPolicyAuditAppendOutcome, McpPolicyAuditEventV1, McpPolicyAuditKind, McpPolicyAuditOutcome,
    McpPolicyAuditStore, McpRuntimeEventV2, McpRuntimeRecordStore, McpRuntimeStoreError,
    McpServerRecordV2, McpSessionTransportKind,
};

use super::super::{JournalConfig, JournalStore};

fn store(path: std::path::PathBuf) -> JournalStore {
    JournalStore::open(JournalConfig {
        db_path: path,
        hash_chain_enabled: true,
        max_payload_bytes: 256 * 1024,
        max_events: 10_000,
    })
    .expect("journal should open")
}

fn configured_record() -> McpServerRecordV2 {
    McpServerRecordV2::configured(
        "server-a".to_owned(),
        McpSessionTransportKind::Stdio,
        Some("vault-scope-a".to_owned()),
        "trusted-local".to_owned(),
        1_000,
    )
    .expect("configured record should validate")
}

#[tokio::test]
async fn configured_record_and_adjacent_event_survive_restart() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let path = directory.path().join("journal.sqlite3");
    let first = store(path.clone());
    let configured = configured_record();
    first.insert_configured(&configured).await.expect("configured record should persist");
    let handshaking =
        configured.begin_handshake(1_001).expect("handshake transition should validate");
    let event = McpRuntimeEventV2::from_transition(
        &configured,
        &handshaking,
        "mcp.runtime.handshake.started",
    )
    .expect("event should match transition");
    first
        .persist_transition(configured.revision, &handshaking, &event)
        .await
        .expect("transition should commit");
    drop(first);

    let reopened = store(path);
    assert_eq!(reopened.load_all().await.expect("records should restore"), vec![handshaking]);
    let guard = reopened.connection.lock().expect("journal lock should be available");
    let migration_count: i64 = guard
        .query_row("SELECT COUNT(*) FROM schema_migrations WHERE version = 97", [], |row| {
            row.get(0)
        })
        .expect("migration marker should load");
    let event_count: i64 = guard
        .query_row("SELECT COUNT(*) FROM mcp_connection_lifecycle_events_v2", [], |row| row.get(0))
        .expect("event count should load");
    assert_eq!(migration_count, 1);
    assert_eq!(event_count, 1);
}

#[tokio::test]
async fn stale_transition_cannot_mutate_head_or_append_evidence() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = store(directory.path().join("journal.sqlite3"));
    let configured = configured_record();
    store.insert_configured(&configured).await.expect("configured record should persist");
    let handshaking =
        configured.begin_handshake(1_001).expect("handshake transition should validate");
    let event = McpRuntimeEventV2::from_transition(
        &configured,
        &handshaking,
        "mcp.runtime.handshake.started",
    )
    .expect("event should match transition");
    store
        .persist_transition(configured.revision, &handshaking, &event)
        .await
        .expect("first transition should commit");

    let stale = store.persist_transition(configured.revision, &handshaking, &event).await;
    assert_eq!(
        stale,
        Err(McpRuntimeStoreError::RevisionConflict {
            expected: configured.revision,
            actual: Some(handshaking.revision),
        })
    );
    let guard = store.connection.lock().expect("journal lock should be available");
    let event_count: i64 = guard
        .query_row("SELECT COUNT(*) FROM mcp_connection_lifecycle_events_v2", [], |row| row.get(0))
        .expect("event count should load");
    assert_eq!(event_count, 1);
}

#[tokio::test]
async fn corrupt_negative_generation_fails_closed_on_restore() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = store(directory.path().join("journal.sqlite3"));
    let configured = configured_record();
    store.insert_configured(&configured).await.expect("configured record should persist");
    {
        let guard = store.connection.lock().expect("journal lock should be available");
        guard
            .execute_batch(
                "PRAGMA ignore_check_constraints = ON;
                 UPDATE mcp_server_records_v2 SET runtime_generation = -1;
                 PRAGMA ignore_check_constraints = OFF;",
            )
            .expect("test should inject corrupt storage");
    }

    assert!(matches!(store.load_all().await, Err(McpRuntimeStoreError::Corrupt { .. })));
}

#[tokio::test]
async fn catalog_epoch_evidence_commits_atomically_with_ready_head() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let path = directory.path().join("journal.sqlite3");
    let journal = store(path.clone());
    let configured = configured_record();
    journal.insert_configured(&configured).await.expect("configured record should persist");
    let handshaking =
        configured.begin_handshake(1_001).expect("handshake transition should validate");
    let handshake_event = McpRuntimeEventV2::from_transition(
        &configured,
        &handshaking,
        "mcp.runtime.handshake.started",
    )
    .expect("handshake event should validate");
    journal
        .persist_transition(configured.revision, &handshaking, &handshake_event)
        .await
        .expect("handshake should persist");
    let ready =
        handshaking.mark_ready("a".repeat(64), 1_002).expect("ready transition should validate");
    let ready_event =
        McpRuntimeEventV2::from_transition(&handshaking, &ready, "mcp.runtime.session.ready")
            .expect("ready event should validate");
    journal
        .persist_transition(handshaking.revision, &ready, &ready_event)
        .await
        .expect("ready state and catalog evidence should commit");
    {
        let guard = journal.connection.lock().expect("journal lock should be available");
        let catalog: (i64, i64, String) = guard
            .query_row(
                r#"
                    SELECT runtime_generation, catalog_epoch, catalog_digest
                    FROM mcp_catalog_epoch_evidence_v1
                    WHERE server_id = 'server-a'
                "#,
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("catalog evidence should load");
        assert_eq!(catalog, (1, 1, "a".repeat(64)));
    }
    drop(journal);

    let reopened = store(path);
    assert_eq!(reopened.load_all().await.expect("ready head should restore"), vec![ready]);
}

#[tokio::test]
async fn sampling_reservations_and_policy_audit_survive_restart() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let path = directory.path().join("journal.sqlite3");
    let journal = store(path.clone());
    journal
        .insert_configured(&configured_record())
        .await
        .expect("configured record should persist");
    let event = McpPolicyAuditEventV1 {
        event_id: "sampling:server-a:1:1".to_owned(),
        server_id: "server-a".to_owned(),
        runtime_generation: 1,
        catalog_epoch: 1,
        binding_sha256: "b".repeat(64),
        kind: McpPolicyAuditKind::Sampling,
        outcome: McpPolicyAuditOutcome::Allowed,
        reserved_output_tokens: 32,
        reason_code: "mcp.runtime.sampling.authorized".to_owned(),
        request_sha256: "c".repeat(64),
        evidence_sha256: None,
        occurred_at_unix_ms: 2_000,
    };
    assert_eq!(
        journal.append_policy_event(&event).await.expect("policy event should append"),
        McpPolicyAuditAppendOutcome::Appended
    );
    assert_eq!(
        journal.append_policy_event(&event).await.expect("exact replay should be idempotent"),
        McpPolicyAuditAppendOutcome::Existing
    );
    drop(journal);

    let reopened = store(path);
    let usage = reopened
        .sampling_usage("server-a", &"b".repeat(64), 1_000)
        .await
        .expect("sampling usage should restore");
    assert_eq!(usage.requests, 1);
    assert_eq!(usage.reserved_output_tokens, 32);
    let guard = reopened.connection.lock().expect("journal lock should be available");
    let migration_count: i64 = guard
        .query_row("SELECT COUNT(*) FROM schema_migrations WHERE version = 98", [], |row| {
            row.get(0)
        })
        .expect("policy migration marker should load");
    assert_eq!(migration_count, 1);
}
