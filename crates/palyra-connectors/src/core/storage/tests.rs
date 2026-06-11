//! Unit tests for `ConnectorStore`: instance lifecycle, inbound dedupe,
//! outbox claim transitions, dead-letter replay/discard, and queue snapshots.

use tempfile::TempDir;

use super::super::protocol::{ConnectorInstanceSpec, ConnectorKind, OutboundMessageRequest};
use super::{ConnectorStore, ConnectorStoreError};

fn open_store() -> (TempDir, ConnectorStore) {
    let tempdir = TempDir::new().expect("tempdir should initialize");
    let db_path = tempdir.path().join("connectors.sqlite3");
    let store = ConnectorStore::open(db_path).expect("connector store should initialize");
    (tempdir, store)
}

fn sample_spec() -> ConnectorInstanceSpec {
    sample_spec_with_connector("echo:default")
}

fn sample_spec_with_connector(connector_id: &str) -> ConnectorInstanceSpec {
    ConnectorInstanceSpec {
        connector_id: connector_id.to_owned(),
        kind: ConnectorKind::Echo,
        principal: format!("channel:{connector_id}"),
        auth_profile_ref: None,
        token_vault_ref: None,
        egress_allowlist: Vec::new(),
        enabled: true,
    }
}

fn sample_outbound(envelope_id: &str) -> OutboundMessageRequest {
    sample_outbound_for_connector("echo:default", envelope_id)
}

fn sample_outbound_for_connector(connector_id: &str, envelope_id: &str) -> OutboundMessageRequest {
    OutboundMessageRequest {
        envelope_id: envelope_id.to_owned(),
        connector_id: connector_id.to_owned(),
        conversation_id: "conv-1".to_owned(),
        reply_thread_id: None,
        in_reply_to_message_id: None,
        text: "hello".to_owned(),
        broadcast: false,
        auto_ack_text: None,
        auto_reaction: None,
        attachments: Vec::new(),
        structured_json: None,
        a2ui_update: None,
        timeout_ms: 30_000,
        max_payload_bytes: 16_384,
    }
}

#[test]
fn dedupe_accepts_first_event_and_rejects_duplicate_until_expiry() {
    let (_tempdir, store) = open_store();
    store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");

    let first = store
        .record_inbound_dedupe_if_new("echo:default", "env-1", 1_000, 10_000)
        .expect("first dedupe write should succeed");
    let second = store
        .record_inbound_dedupe_if_new("echo:default", "env-1", 1_500, 10_000)
        .expect("duplicate dedupe write should succeed");
    let after_expiry = store
        .record_inbound_dedupe_if_new("echo:default", "env-1", 12_000, 10_000)
        .expect("expired dedupe key should be re-insertable");

    assert!(first, "first inbound should be accepted");
    assert!(!second, "duplicate inbound should be rejected within dedupe window");
    assert!(after_expiry, "dedupe key should expire after configured window");
}

#[test]
fn dedupe_is_scoped_per_connector_instance() {
    let (_tempdir, store) = open_store();
    store
        .upsert_instance(&sample_spec_with_connector("echo:default"), 1_000)
        .expect("default instance should be created");
    store
        .upsert_instance(&sample_spec_with_connector("echo:ops"), 1_000)
        .expect("ops instance should be created");

    let default_first = store
        .record_inbound_dedupe_if_new("echo:default", "env-1", 1_000, 10_000)
        .expect("default first dedupe write should succeed");
    let ops_first = store
        .record_inbound_dedupe_if_new("echo:ops", "env-1", 1_000, 10_000)
        .expect("ops first dedupe write should succeed");
    let default_duplicate = store
        .record_inbound_dedupe_if_new("echo:default", "env-1", 1_100, 10_000)
        .expect("default duplicate dedupe write should succeed");

    assert!(default_first, "default connector should accept first envelope");
    assert!(ops_first, "same envelope id should be accepted for a different connector");
    assert!(
        !default_duplicate,
        "duplicate envelope should still be rejected within dedupe window for the same connector"
    );
}

#[test]
fn outbox_enforces_idempotent_unique_envelope_per_connector() {
    let (_tempdir, store) = open_store();
    store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
    let request = sample_outbound("env-1:0");

    let created = store
        .enqueue_outbox_if_absent(&request, 5, 1_000)
        .expect("first outbox enqueue should succeed");
    let duplicate = store
        .enqueue_outbox_if_absent(&request, 5, 1_000)
        .expect("duplicate outbox enqueue should succeed");

    assert!(created.created, "first enqueue must create a record");
    assert!(!duplicate.created, "duplicate envelope must be ignored");
}

#[test]
fn outbox_allows_same_envelope_for_different_connectors() {
    let (_tempdir, store) = open_store();
    store
        .upsert_instance(&sample_spec_with_connector("echo:default"), 1_000)
        .expect("default instance should be created");
    store
        .upsert_instance(&sample_spec_with_connector("echo:ops"), 1_000)
        .expect("ops instance should be created");

    let default_request = sample_outbound_for_connector("echo:default", "env-1:0");
    let ops_request = sample_outbound_for_connector("echo:ops", "env-1:0");

    let default_outcome = store
        .enqueue_outbox_if_absent(&default_request, 5, 1_000)
        .expect("default outbox enqueue should succeed");
    let ops_outcome = store
        .enqueue_outbox_if_absent(&ops_request, 5, 1_000)
        .expect("ops outbox enqueue should succeed");

    assert!(default_outcome.created, "default connector should enqueue envelope");
    assert!(ops_outcome.created, "same envelope id should still enqueue for a different connector");
}

#[test]
fn delete_instance_removes_runtime_state_but_keeps_audit_records() {
    let (_tempdir, store) = open_store();
    store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
    store
        .record_inbound_dedupe_if_new("echo:default", "env-delete", 1_000, 10_000)
        .expect("dedupe write should succeed");
    store
        .record_event("echo:default", "connector.test", "info", "test event", None, 1_000)
        .expect("event should be recorded");
    let request = sample_outbound("env-delete");
    store.enqueue_outbox_if_absent(&request, 5, 1_000).expect("outbox enqueue should succeed");

    store.delete_instance("echo:default").expect("delete should succeed");

    assert!(
        store.get_instance("echo:default").expect("instance lookup should succeed").is_none(),
        "connector instance should be removed"
    );
    assert!(
        store
            .load_due_outbox(1_000, 10, Some("echo:default"), false)
            .expect("outbox lookup should succeed")
            .is_empty(),
        "outbox records should be removed with the connector"
    );
    assert_eq!(
        store.list_events("echo:default", 10).expect("event lookup should succeed").len(),
        1,
        "audit events should remain after connector removal"
    );
}

#[test]
fn outbox_retry_and_dead_letter_flow_persists_state() {
    let (_tempdir, store) = open_store();
    store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
    let request = sample_outbound("env-2:0");
    store.enqueue_outbox_if_absent(&request, 2, 1_000).expect("outbox enqueue should succeed");

    let due =
        store.load_due_outbox(1_000, 10, Some("echo:default"), false).expect("due outbox query");
    assert_eq!(due.len(), 1);
    let outbox_id = due[0].outbox_id;
    let claim_token = due[0].claim_token.clone();
    store
        .schedule_outbox_retry(outbox_id, claim_token.as_str(), 1, "transient", 2_000)
        .expect("retry should be scheduled");
    let due_after_backoff = store
        .load_due_outbox(1_500, 10, Some("echo:default"), false)
        .expect("outbox due query should succeed");
    assert!(due_after_backoff.is_empty(), "entry should not be due before retry timestamp");
    let due_for_dead_letter = store
        .load_due_outbox(2_100, 10, Some("echo:default"), false)
        .expect("due outbox query should succeed");
    assert_eq!(due_for_dead_letter.len(), 1);
    let dead_letter_claim = due_for_dead_letter[0].claim_token.clone();
    store
        .move_outbox_to_dead_letter(outbox_id, dead_letter_claim.as_str(), "permanent", 2_100)
        .expect("dead letter move should succeed");
    let dead_letters =
        store.list_dead_letters("echo:default", 10).expect("dead letters should be queryable");
    assert_eq!(dead_letters.len(), 1);
    assert_eq!(dead_letters[0].reason, "permanent");
}

#[test]
fn dead_letter_can_be_replayed_back_into_pending_outbox() {
    let (_tempdir, store) = open_store();
    store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
    let request = sample_outbound("env-replay:0");
    store.enqueue_outbox_if_absent(&request, 2, 1_000).expect("outbox enqueue should succeed");

    let due = store
        .load_due_outbox(1_000, 10, Some("echo:default"), false)
        .expect("due outbox query should succeed");
    let claimed = due.first().expect("entry should be claimed");
    store
        .move_outbox_to_dead_letter(
            claimed.outbox_id,
            claimed.claim_token.as_str(),
            "permanent",
            1_100,
        )
        .expect("dead letter move should succeed");
    let dead_letter = store
        .list_dead_letters("echo:default", 10)
        .expect("dead letters should be queryable")
        .into_iter()
        .next()
        .expect("dead letter should exist");

    let replayed = store
        .replay_dead_letter("echo:default", dead_letter.dead_letter_id, 5, 2_000)
        .expect("dead letter replay should succeed");
    assert_eq!(replayed.envelope_id, "env-replay:0");

    let dead_letters_after = store
        .list_dead_letters("echo:default", 10)
        .expect("dead letters after replay should be queryable");
    assert!(dead_letters_after.is_empty(), "replayed dead letter should be removed");

    let replay_due = store
        .load_due_outbox(2_000, 10, Some("echo:default"), false)
        .expect("replayed outbox entry should be pending");
    assert_eq!(replay_due.len(), 1, "replayed entry should be ready for immediate retry");
    assert_eq!(replay_due[0].attempts, 0, "replayed outbox should reset attempts");
    assert_eq!(replay_due[0].envelope_id, "env-replay:0");
}

#[test]
fn dead_letter_can_be_discarded_without_requeueing() {
    let (_tempdir, store) = open_store();
    store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
    let request = sample_outbound("env-discard:0");
    store.enqueue_outbox_if_absent(&request, 2, 1_000).expect("outbox enqueue should succeed");

    let due = store
        .load_due_outbox(1_000, 10, Some("echo:default"), false)
        .expect("due outbox query should succeed");
    let claimed = due.first().expect("entry should be claimed");
    store
        .move_outbox_to_dead_letter(
            claimed.outbox_id,
            claimed.claim_token.as_str(),
            "permanent",
            1_100,
        )
        .expect("dead letter move should succeed");
    let dead_letter = store
        .list_dead_letters("echo:default", 10)
        .expect("dead letters should be queryable")
        .into_iter()
        .next()
        .expect("dead letter should exist");

    let discarded = store
        .discard_dead_letter("echo:default", dead_letter.dead_letter_id)
        .expect("dead letter discard should succeed");
    assert_eq!(discarded.envelope_id, "env-discard:0");
    assert!(
        store
            .list_dead_letters("echo:default", 10)
            .expect("dead letters should remain queryable")
            .is_empty(),
        "discarded dead letter should be removed from listing"
    );
    assert!(
        store
            .load_due_outbox(2_000, 10, Some("echo:default"), false)
            .expect("outbox query should remain valid")
            .is_empty(),
        "discard should not recreate a pending outbox entry"
    );
}

#[test]
fn queue_snapshot_reports_pending_due_claimed_and_dead_letter_counts() {
    let (_tempdir, store) = open_store();
    store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
    store
        .enqueue_outbox_if_absent(&sample_outbound("env-a:0"), 2, 1_000)
        .expect("first outbox enqueue should succeed");
    store
        .enqueue_outbox_if_absent(&sample_outbound("env-b:0"), 2, 2_000)
        .expect("second outbox enqueue should succeed");

    let claimed = store
        .load_due_outbox(2_000, 1, Some("echo:default"), false)
        .expect("due outbox query should succeed");
    assert_eq!(claimed.len(), 1, "one entry should be claimed for in-flight delivery");
    let snapshot =
        store.queue_snapshot("echo:default", 2_000).expect("queue snapshot should succeed");
    assert_eq!(snapshot.pending_outbox, 2);
    assert_eq!(snapshot.claimed_outbox, 1);
    assert_eq!(snapshot.due_outbox, 1);
    assert_eq!(snapshot.dead_letters, 0);
    assert_eq!(snapshot.next_attempt_unix_ms, Some(1_000));
    assert_eq!(snapshot.oldest_pending_created_at_unix_ms, Some(1_000));

    store
        .move_outbox_to_dead_letter(
            claimed[0].outbox_id,
            claimed[0].claim_token.as_str(),
            "permanent",
            2_100,
        )
        .expect("dead letter move should succeed");
    let after_dead_letter = store
        .queue_snapshot("echo:default", 2_100)
        .expect("queue snapshot after dead letter should succeed");
    assert_eq!(after_dead_letter.pending_outbox, 1);
    assert_eq!(after_dead_letter.dead_letters, 1);
    assert_eq!(after_dead_letter.latest_dead_letter_unix_ms, Some(2_100));
}

#[test]
fn outbox_due_claims_are_exclusive_between_loads() {
    let (_tempdir, store) = open_store();
    store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
    let request = sample_outbound("env-claim-exclusive");
    store.enqueue_outbox_if_absent(&request, 2, 1_000).expect("outbox enqueue should succeed");

    let first = store
        .load_due_outbox(1_000, 10, Some("echo:default"), false)
        .expect("first load should claim");
    assert_eq!(first.len(), 1, "first due load should claim the entry");
    let second = store
        .load_due_outbox(1_000, 10, Some("echo:default"), false)
        .expect("second load should succeed");
    assert!(second.is_empty(), "second due load should not re-claim entry while lease is active");
}

#[test]
fn mark_outbox_delivered_reports_missing_outbox() {
    let (_tempdir, store) = open_store();
    let error = store
        .mark_outbox_delivered(9_999, "claim-missing", "native-1", 1_000)
        .expect_err("unknown outbox id should be reported");
    assert!(
        matches!(error, ConnectorStoreError::OutboxNotFound(9_999)),
        "expected OutboxNotFound for missing outbox id"
    );
}

#[test]
fn schedule_outbox_retry_reports_missing_outbox() {
    let (_tempdir, store) = open_store();
    let error = store
        .schedule_outbox_retry(9_998, "claim-missing", 1, "retry", 2_000)
        .expect_err("unknown outbox id should be reported");
    assert!(
        matches!(error, ConnectorStoreError::OutboxNotFound(9_998)),
        "expected OutboxNotFound for missing outbox id"
    );
}

#[test]
fn move_outbox_to_dead_letter_reports_missing_outbox() {
    let (_tempdir, store) = open_store();
    let error = store
        .move_outbox_to_dead_letter(9_997, "claim-missing", "dead", 1_000)
        .expect_err("unknown outbox id should be reported");
    assert!(
        matches!(error, ConnectorStoreError::OutboxNotFound(9_997)),
        "expected OutboxNotFound for missing outbox id"
    );
}

#[test]
fn queue_snapshot_reports_pause_state_and_due_counts() {
    let (_tempdir, store) = open_store();
    store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
    store
        .enqueue_outbox_if_absent(&sample_outbound("env-1"), 3, 1_000)
        .expect("outbox insert should succeed");
    let claimed =
        store.load_due_outbox(1_000, 1, Some("echo:default"), false).expect("claim should succeed");
    let claim_token = claimed[0].claim_token.clone();
    store
        .schedule_outbox_retry(claimed[0].outbox_id, claim_token.as_str(), 1, "retry", 5_000)
        .expect("retry scheduling should succeed");
    store
        .set_queue_paused("echo:default", true, Some("operator_pause"), 2_000)
        .expect("queue pause should persist");

    let snapshot =
        store.queue_snapshot("echo:default", 2_000).expect("queue snapshot should resolve");

    assert_eq!(snapshot.pending_outbox, 1);
    assert_eq!(snapshot.due_outbox, 0);
    assert_eq!(snapshot.claimed_outbox, 0);
    assert!(snapshot.paused, "queue snapshot should reflect paused state");
    assert_eq!(snapshot.pause_reason.as_deref(), Some("operator_pause"));
    assert_eq!(snapshot.pause_updated_at_unix_ms, Some(2_000));
    assert_eq!(snapshot.next_attempt_unix_ms, Some(5_000));
}

#[test]
fn replay_and_discard_dead_letter_update_queue_state() {
    let (_tempdir, store) = open_store();
    store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
    store
        .enqueue_outbox_if_absent(&sample_outbound("env-1"), 3, 1_000)
        .expect("outbox insert should succeed");
    let due = store
        .load_due_outbox(1_000, 1, Some("echo:default"), false)
        .expect("outbox should be claimable");
    let claim_token = due[0].claim_token.clone();
    store
        .move_outbox_to_dead_letter(due[0].outbox_id, claim_token.as_str(), "permanent", 1_500)
        .expect("dead-letter move should succeed");

    let dead_letters =
        store.list_dead_letters("echo:default", 10).expect("dead letters should be listed");
    assert_eq!(dead_letters.len(), 1);

    let replayed = store
        .replay_dead_letter("echo:default", dead_letters[0].dead_letter_id, 5, 2_000)
        .expect("dead letter should replay");
    assert_eq!(replayed.envelope_id, "env-1");
    let snapshot = store
        .queue_snapshot("echo:default", 2_000)
        .expect("queue snapshot should resolve after replay");
    assert_eq!(snapshot.pending_outbox, 1);
    assert_eq!(snapshot.dead_letters, 0);
    assert_eq!(snapshot.due_outbox, 1);

    let due_again = store
        .load_due_outbox(2_000, 1, Some("echo:default"), false)
        .expect("replayed outbox should be claimable");
    let claim_token = due_again[0].claim_token.clone();
    store
        .move_outbox_to_dead_letter(
            due_again[0].outbox_id,
            claim_token.as_str(),
            "retry_exhausted",
            2_100,
        )
        .expect("dead-letter move should succeed");
    let dead_letter_id = store
        .list_dead_letters("echo:default", 10)
        .expect("dead letters should be listed after second failure")[0]
        .dead_letter_id;
    let discarded = store
        .discard_dead_letter("echo:default", dead_letter_id)
        .expect("dead letter should discard");
    assert_eq!(discarded.reason, "retry_exhausted");
    assert!(
        store.list_dead_letters("echo:default", 10).expect("dead letters should reload").is_empty(),
        "discard should remove dead letter from operator queue"
    );
}
