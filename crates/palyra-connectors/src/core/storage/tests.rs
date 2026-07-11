//! Unit tests for `ConnectorStore`: instance lifecycle, inbound dedupe,
//! outbox claim transitions, dead-letter replay/discard, and queue snapshots.

use rusqlite::{params, Connection};
use tempfile::TempDir;

use super::super::protocol::{
    ConnectorInstanceSpec, ConnectorKind, InboundMessageEvent, OutboundMessageRequest,
};
use super::{
    ChannelIngressStatus, ConnectorStore, ConnectorStoreError, DeliveryIntentDraft,
    DeliveryIntentStatus, OutboxEffectState, OutboxReconciliationEvidence,
};

fn open_store() -> (TempDir, ConnectorStore) {
    let tempdir = TempDir::new().expect("tempdir should initialize");
    let db_path = tempdir.path().join("connectors.sqlite3");
    let store = ConnectorStore::open(db_path).expect("connector store should initialize");
    (tempdir, store)
}

#[test]
fn opening_legacy_outbox_parks_claimed_rows_and_keeps_unclaimed_rows_ready() {
    let tempdir = TempDir::new().expect("tempdir should initialize");
    let db_path = tempdir.path().join("legacy-connectors.sqlite3");
    let connection = Connection::open(db_path.as_path()).expect("legacy database should open");
    connection
        .execute_batch(
            r#"
            CREATE TABLE outbox (
                outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_id TEXT NOT NULL,
                envelope_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL,
                next_attempt_unix_ms INTEGER NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('pending', 'delivered', 'dead')),
                native_message_id TEXT,
                last_error TEXT,
                claim_token TEXT,
                claim_expires_unix_ms INTEGER NOT NULL DEFAULT 0,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                UNIQUE(connector_id, envelope_id)
            );

            CREATE TABLE delivery_intents (
                intent_id TEXT PRIMARY KEY,
                connector_id TEXT NOT NULL,
                ingress_event_id INTEGER NOT NULL,
                ingress_envelope_id TEXT NOT NULL,
                session_id TEXT,
                run_id TEXT,
                principal TEXT NOT NULL,
                conversation_id TEXT NOT NULL,
                output_index INTEGER NOT NULL,
                outbox_envelope_id TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                visible_text_preview TEXT NOT NULL,
                status TEXT NOT NULL,
                send_attempts INTEGER NOT NULL DEFAULT 0,
                native_message_id TEXT,
                last_reason_code TEXT,
                redaction_summary_json TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                UNIQUE(connector_id, ingress_event_id, output_index, outbox_envelope_id)
            );
            "#,
        )
        .expect("legacy outbox schema should initialize");
    let claimed_payload = serde_json::to_string(&sample_outbound("env-legacy-claimed"))
        .expect("claimed legacy payload should encode");
    let ready_payload = serde_json::to_string(&sample_outbound("env-legacy-ready"))
        .expect("ready legacy payload should encode");
    connection
        .execute(
            r#"
            INSERT INTO outbox (
                connector_id, envelope_id, payload_json, attempts, max_attempts,
                next_attempt_unix_ms, status, native_message_id, last_error,
                claim_token, claim_expires_unix_ms, created_at_unix_ms, updated_at_unix_ms
            )
            VALUES ('echo:default', 'env-legacy-claimed', ?1, 1, 3, 1000,
                    'pending', NULL, NULL, 'legacy-claim', 60000, 1000, 1001)
            "#,
            params![claimed_payload],
        )
        .expect("claimed legacy outbox row should insert");
    connection
        .execute(
            r#"
            INSERT INTO outbox (
                connector_id, envelope_id, payload_json, attempts, max_attempts,
                next_attempt_unix_ms, status, native_message_id, last_error,
                claim_token, claim_expires_unix_ms, created_at_unix_ms, updated_at_unix_ms
            )
            VALUES ('echo:default', 'env-legacy-ready', ?1, 0, 3, 1000,
                    'pending', NULL, NULL, NULL, 0, 1000, 1000)
            "#,
            params![ready_payload],
        )
        .expect("unclaimed legacy outbox row should insert");
    connection
        .execute(
            r#"
            INSERT INTO delivery_intents (
                intent_id, connector_id, ingress_event_id, ingress_envelope_id,
                session_id, run_id, principal, conversation_id, output_index,
                outbox_envelope_id, payload_hash, visible_text_preview, status,
                send_attempts, native_message_id, last_reason_code,
                redaction_summary_json, created_at_unix_ms, updated_at_unix_ms
            )
            VALUES (
                'intent-legacy-claimed', 'echo:default', 1, 'ingress-legacy',
                NULL, NULL, 'channel:echo:default', 'conv-1', 0,
                'env-legacy-claimed', 'hash-legacy', 'legacy preview',
                'adapter_send_started', 1, NULL, NULL, NULL, 1000, 1001
            )
            "#,
            [],
        )
        .expect("claimed legacy delivery intent should insert");
    drop(connection);

    let store = ConnectorStore::open(db_path).expect("legacy database should migrate");
    let ready = store
        .load_due_outbox(100_000, 10, Some("echo:default"), true)
        .expect("only the unclaimed legacy row should be claimable");
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].envelope_id, "env-legacy-ready");
    assert_eq!(ready[0].effect_state, OutboxEffectState::Ready);

    let unknown = store
        .list_outbox_unknown("echo:default", 10)
        .expect("claimed legacy row should require reconciliation");
    assert_eq!(unknown.len(), 1);
    assert_eq!(unknown[0].envelope_id, "env-legacy-claimed");
    assert_eq!(unknown[0].last_reason_code.as_deref(), Some("outbox.legacy_claim_outcome_unknown"));
    let migrated_intent = store
        .get_delivery_intent("intent-legacy-claimed")
        .expect("legacy delivery intent should migrate with its outbox row");
    assert_eq!(migrated_intent.status, DeliveryIntentStatus::PlatformOutcomeUnknown);
    assert_eq!(
        migrated_intent.last_reason_code.as_deref(),
        Some("outbox.legacy_claim_outcome_unknown")
    );

    store
        .reconcile_outbox_unknown(
            unknown[0].outbox_id,
            &OutboxReconciliationEvidence::ConfirmedAbsent,
            100_001,
        )
        .expect("confirmed absence should make the legacy row retryable");
    let reconciled = store
        .load_due_outbox(100_001, 10, Some("echo:default"), true)
        .expect("reconciled legacy row should become claimable");
    assert_eq!(reconciled.len(), 1);
    assert_eq!(reconciled[0].envelope_id, "env-legacy-claimed");
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

fn sample_inbound(envelope_id: &str, conversation_id: &str) -> InboundMessageEvent {
    InboundMessageEvent {
        envelope_id: envelope_id.to_owned(),
        connector_id: "echo:default".to_owned(),
        conversation_id: conversation_id.to_owned(),
        thread_id: None,
        sender_id: "u1".to_owned(),
        sender_display: None,
        body: "hello".to_owned(),
        adapter_message_id: Some(format!("adapter-{envelope_id}")),
        adapter_thread_id: None,
        received_at_unix_ms: 1_000,
        is_direct_message: true,
        requested_broadcast: false,
        attachments: Vec::new(),
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
fn channel_ingress_persists_payload_hash_and_tombstone_dedupes() {
    let (_tempdir, store) = open_store();
    store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
    let event = sample_inbound("env-ingress", "conv-1");

    let first = store
        .enqueue_channel_ingress_if_absent(&event, "channel:echo:default", 1_000, 3, 10_000)
        .expect("first ingress enqueue should succeed");
    let duplicate = store
        .enqueue_channel_ingress_if_absent(&event, "channel:echo:default", 1_100, 3, 10_000)
        .expect("duplicate ingress enqueue should succeed");

    assert!(first.created);
    assert!(!duplicate.created);
    assert_eq!(first.record.payload_hash, duplicate.record.payload_hash);
    assert_eq!(duplicate.record.status, ChannelIngressStatus::Pending);
    assert_eq!(duplicate.record.payload.body, "hello");
}

#[test]
fn channel_ingress_claims_preserve_lane_order_and_reclaim_stale_claims() {
    let (_tempdir, store) = open_store();
    store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
    store
        .enqueue_channel_ingress_if_absent(
            &sample_inbound("env-a", "conv-1"),
            "channel:echo:default",
            1_000,
            3,
            10_000,
        )
        .expect("first ingress enqueue should succeed");
    store
        .enqueue_channel_ingress_if_absent(
            &sample_inbound("env-b", "conv-1"),
            "channel:echo:default",
            1_001,
            3,
            10_000,
        )
        .expect("second same-lane ingress enqueue should succeed");
    store
        .enqueue_channel_ingress_if_absent(
            &sample_inbound("env-c", "conv-2"),
            "channel:echo:default",
            1_002,
            3,
            10_000,
        )
        .expect("parallel lane ingress enqueue should succeed");

    let first_claim = store
        .load_due_channel_ingress(1_100, 10, Some("echo:default"), 100, false)
        .expect("due ingress claim should succeed");
    let claimed_envelopes =
        first_claim.iter().map(|record| record.envelope_id.as_str()).collect::<Vec<_>>();
    assert_eq!(claimed_envelopes, vec!["env-a", "env-c"]);

    let second_claim = store
        .load_due_channel_ingress(1_150, 10, Some("echo:default"), 100, false)
        .expect("active claims should not be reclaimed");
    assert!(second_claim.is_empty());

    let reclaimed = store
        .load_due_channel_ingress(1_250, 10, Some("echo:default"), 100, false)
        .expect("stale claim should be reclaimed");
    let reclaimed_envelopes =
        reclaimed.iter().map(|record| record.envelope_id.as_str()).collect::<Vec<_>>();
    assert_eq!(reclaimed_envelopes, vec!["env-a", "env-c"]);
    let snapshot = store
        .queue_snapshot("echo:default", 1_250)
        .expect("queue snapshot should include blocked lanes");
    assert_eq!(snapshot.claimed_ingress, 2);
    assert_eq!(snapshot.blocked_ingress_lanes.len(), 2);
}

#[test]
fn delivery_intent_lifecycle_reports_without_raw_payload_and_retries_safe_state() {
    let (_tempdir, store) = open_store();
    store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
    let ingress = store
        .enqueue_channel_ingress_if_absent(
            &sample_inbound("env-delivery", "conv-1"),
            "channel:echo:default",
            1_000,
            3,
            10_000,
        )
        .expect("ingress enqueue should succeed")
        .record;
    let request = sample_outbound("env-delivery:0");
    store.enqueue_outbox_if_absent(&request, 3, 1_000).expect("outbox enqueue should succeed");
    let draft = DeliveryIntentDraft {
        intent_id: "delivery:echo:default:1:env-delivery:0".to_owned(),
        connector_id: "echo:default".to_owned(),
        ingress_event_id: ingress.ingress_event_id,
        ingress_envelope_id: ingress.envelope_id,
        session_id: Some("session-1".to_owned()),
        run_id: Some("run-1".to_owned()),
        principal: "channel:echo:default".to_owned(),
        conversation_id: "conv-1".to_owned(),
        outbox_envelope_id: request.envelope_id.clone(),
        output_index: 0,
        payload_hash: "hash-visible".to_owned(),
        visible_text_preview: "hello".to_owned(),
        status: DeliveryIntentStatus::Queued,
        redaction_summary_json: Some(r#"{"redaction_count":1}"#.to_owned()),
    };

    let intent =
        store.upsert_delivery_intent(&draft, 1_000).expect("delivery intent should upsert");
    assert_eq!(intent.status, DeliveryIntentStatus::Queued);
    assert_eq!(intent.visible_text_preview, "hello");
    assert_eq!(intent.payload_hash, "hash-visible");

    let claimed = store
        .load_due_outbox(1_100, 1, Some("echo:default"), false)
        .expect("outbox row should be claimable")
        .into_iter()
        .next()
        .expect("outbox row should exist");
    store
        .mark_outbox_delivery_intent_started(claimed.outbox_id, claimed.claim_token.as_str(), 1_100)
        .expect("send-start transition should succeed");
    let started = store.get_delivery_intent(draft.intent_id.as_str()).expect("intent should load");
    assert_eq!(started.status, DeliveryIntentStatus::AdapterSendStarted);
    assert_eq!(started.send_attempts, 1);

    store
        .mark_outbox_effect_started(claimed.outbox_id, claimed.claim_token.as_str(), 1_150)
        .expect("effect fence should start");
    store
        .mark_outbox_outcome_unknown(
            claimed.outbox_id,
            claimed.claim_token.as_str(),
            "transient_network",
            1_200,
        )
        .expect("unknown transition should succeed");
    let error = store
        .retry_delivery_intent(draft.intent_id.as_str(), 3, 1_300)
        .expect_err("platform-unknown intent must require reconciliation evidence");
    assert!(matches!(error, ConnectorStoreError::InvalidDeliveryIntentRetry { .. }));
    let reconciliation = store
        .reconcile_outbox_unknown(
            claimed.outbox_id,
            &OutboxReconciliationEvidence::ConfirmedAbsent,
            1_300,
        )
        .expect("confirmed absence should requeue safely");
    assert!(reconciliation.requeued);
    assert_eq!(reconciliation.effect_state, OutboxEffectState::Ready);

    let reclaimed = store
        .load_due_outbox(1_400, 1, Some("echo:default"), false)
        .expect("reconciled row should be claimable")
        .into_iter()
        .next()
        .expect("reconciled row should exist");
    store
        .mark_outbox_delivery_intent_started(
            reclaimed.outbox_id,
            reclaimed.claim_token.as_str(),
            1_400,
        )
        .expect("reconciled intent should start");
    store
        .mark_outbox_effect_started(reclaimed.outbox_id, reclaimed.claim_token.as_str(), 1_400)
        .expect("reconciled effect should start");
    store
        .mark_outbox_and_delivery_intents_delivered(
            reclaimed.outbox_id,
            reclaimed.claim_token.as_str(),
            "native-1",
            1_400,
        )
        .expect("delivery transition should succeed");
    let delivered =
        store.get_delivery_intent(draft.intent_id.as_str()).expect("delivered intent should load");
    assert_eq!(delivered.status, DeliveryIntentStatus::Delivered);
    assert_eq!(delivered.native_message_id.as_deref(), Some("native-1"));
    assert!(
        serde_json::to_string(&delivered)
            .expect("delivery intent report should encode")
            .contains("hello"),
        "report should include only visible preview, not raw outbox payload"
    );
    let error = store
        .retry_delivery_intent(draft.intent_id.as_str(), 3, 1_500)
        .expect_err("delivered intent must not be retryable");
    assert!(matches!(error, ConnectorStoreError::InvalidDeliveryIntentRetry { .. }));
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
fn dead_letter_replay_reports_live_outbox_conflict() {
    let (_tempdir, store) = open_store();
    store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
    let request = sample_outbound("env-replay-conflict:0");
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

    store
        .with_transaction(|transaction| {
            transaction.execute(
                "DELETE FROM outbox WHERE connector_id = ?1 AND envelope_id = ?2 AND status = 'dead'",
                params![request.connector_id.as_str(), request.envelope_id.as_str()],
            )?;
            Ok(())
        })
        .expect("test setup should remove the dead outbox row");
    store
        .enqueue_outbox_if_absent(&request, 2, 1_200)
        .expect("test setup should recreate a live outbox row");

    let error = store
        .replay_dead_letter("echo:default", dead_letter.dead_letter_id, 5, 2_000)
        .expect_err("replay should report a typed live-outbox conflict");
    assert!(
        matches!(
            error,
            ConnectorStoreError::DeadLetterReplayConflict { ref connector_id, ref envelope_id }
                if connector_id == "echo:default" && envelope_id == "env-replay-conflict:0"
        ),
        "expected typed live outbox conflict, got {error:?}"
    );
    assert_eq!(
        store
            .list_dead_letters("echo:default", 10)
            .expect("dead letters should remain queryable")
            .len(),
        1,
        "conflicted replay should leave the operator-visible dead letter in place"
    );
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
fn expired_effect_started_claim_is_parked_until_explicit_reconciliation() {
    let (_tempdir, store) = open_store();
    store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
    store
        .enqueue_outbox_if_absent(&sample_outbound("env-effect-expired"), 2, 1_000)
        .expect("outbox enqueue should succeed");
    let claimed = store
        .load_due_outbox(1_000, 1, Some("echo:default"), false)
        .expect("outbox should be claimable")
        .into_iter()
        .next()
        .expect("claimed outbox should exist");
    store
        .mark_outbox_effect_started(claimed.outbox_id, claimed.claim_token.as_str(), 1_001)
        .expect("effect fence should start");

    let reclaimed = store
        .load_due_outbox(61_000, 1, Some("echo:default"), false)
        .expect("expired claim scan should succeed");
    assert!(reclaimed.is_empty(), "an uncertain effect must never be reclaimed for blind send");
    let unknown =
        store.list_outbox_unknown("echo:default", 10).expect("unknown outbox should be visible");
    assert_eq!(unknown.len(), 1);
    assert_eq!(unknown[0].outbox_id, claimed.outbox_id);
    assert_eq!(
        unknown[0].last_reason_code.as_deref(),
        Some("outbox.claim_expired_after_effect_started")
    );
    assert!(
        store
            .load_due_outbox(120_000, 1, Some("echo:default"), false)
            .expect("later scans should remain safe")
            .is_empty(),
        "outcome-unknown must stay parked across repeated scans"
    );

    let outcome = store
        .reconcile_outbox_unknown(
            claimed.outbox_id,
            &OutboxReconciliationEvidence::ConfirmedAbsent,
            120_001,
        )
        .expect("confirmed absence should remove the effect fence");
    assert!(outcome.requeued);
    let safe_retry = store
        .load_due_outbox(120_001, 1, Some("echo:default"), false)
        .expect("reconciled outbox should be claimable");
    assert_eq!(safe_retry.len(), 1);
    assert_eq!(safe_retry[0].effect_state, OutboxEffectState::Ready);
}

#[test]
fn expired_effect_started_backlog_is_parked_in_bounded_passes() {
    const BACKLOG_SIZE: usize = 23;
    const DRAIN_LIMIT: usize = 4;

    let (_tempdir, store) = open_store();
    store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
    for index in 0..BACKLOG_SIZE {
        store
            .enqueue_outbox_if_absent(
                &sample_outbound(format!("env-effect-expired-{index}").as_str()),
                2,
                1_000,
            )
            .expect("outbox enqueue should succeed");
    }
    let claimed = store
        .load_due_outbox(1_000, BACKLOG_SIZE, Some("echo:default"), false)
        .expect("the complete backlog should be claimable initially");
    assert_eq!(claimed.len(), BACKLOG_SIZE);
    for entry in &claimed {
        store
            .mark_outbox_effect_started(entry.outbox_id, entry.claim_token.as_str(), 1_001)
            .expect("effect fence should start");
    }

    let first_pass = store
        .load_due_outbox(61_000, DRAIN_LIMIT, Some("echo:default"), false)
        .expect("first expired-claim scan should succeed");
    assert!(
        first_pass.is_empty(),
        "expired effects outside the recovery bound must remain fenced from delivery"
    );
    let first_unknown = store
        .list_outbox_unknown("echo:default", BACKLOG_SIZE)
        .expect("first recovery pass should expose parked rows");
    assert_eq!(
        first_unknown.len(),
        DRAIN_LIMIT,
        "one drain transaction must not park more expired effects than its limit"
    );

    let remaining_passes = (BACKLOG_SIZE - DRAIN_LIMIT).div_ceil(DRAIN_LIMIT);
    for _ in 0..remaining_passes {
        let claimed_again = store
            .load_due_outbox(61_000, DRAIN_LIMIT, Some("echo:default"), false)
            .expect("later expired-claim scan should succeed");
        assert!(
            claimed_again.is_empty(),
            "an uncertain external effect must never become a blind retry"
        );
    }
    let all_unknown = store
        .list_outbox_unknown("echo:default", BACKLOG_SIZE)
        .expect("later recovery passes should finish the backlog");
    assert_eq!(all_unknown.len(), BACKLOG_SIZE);
    assert!(all_unknown.iter().all(|entry| {
        entry.last_reason_code.as_deref() == Some("outbox.claim_expired_after_effect_started")
    }));
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
