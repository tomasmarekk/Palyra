use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
};

use crate::{
    application::{
        semantic_memory::{
            SemanticMemoryCandidateDraftV1, SemanticMemoryConsolidationPolicy,
            SemanticMemoryContradictionStatus, SemanticMemoryEpistemicKind,
            SemanticMemoryEvidenceRefV1, SemanticMemoryQualityEvalCaseV1,
            SemanticMemoryRetrievalFeedbackV1, SemanticMemorySensitivity,
        },
        tool_runtime::memory::memory_search_tool_output_payload,
    },
    journal::{
        ApprovalDecision, ApprovalDecisionScope, ApprovalResolveRequest, JournalConfig,
        JournalStore, MemoryEmbeddingProvider, MemoryItemCreateRequest, MemorySearchRequest,
        MemorySource, OrchestratorSessionCleanupRequest, OrchestratorSessionResolveRequest,
    },
};

use super::*;

#[derive(Debug)]
struct BlockingEmbeddingProvider {
    block_on_embed: AtomicBool,
    started_tx: Mutex<Option<mpsc::Sender<()>>>,
    release_rx: Mutex<mpsc::Receiver<()>>,
}

impl MemoryEmbeddingProvider for BlockingEmbeddingProvider {
    fn model_name(&self) -> &'static str {
        "semantic-memory-test-v1"
    }

    fn dimensions(&self) -> usize {
        4
    }

    fn embed_text(&self, _text: &str) -> Vec<f32> {
        if !self.block_on_embed.load(Ordering::SeqCst) {
            return vec![0.4, 0.3, 0.2, 0.1];
        }
        if let Some(sender) = self.started_tx.lock().expect("start lock should work").take() {
            sender.send(()).expect("test should observe embedding");
        }
        self.release_rx
            .lock()
            .expect("release lock should work")
            .recv()
            .expect("test should release embedding");
        vec![0.4, 0.3, 0.2, 0.1]
    }
}

fn config(path: PathBuf) -> JournalConfig {
    JournalConfig {
        db_path: path,
        hash_chain_enabled: false,
        max_payload_bytes: 1024 * 1024,
        max_events: 10_000,
    }
}

fn temp_db_path() -> PathBuf {
    std::env::temp_dir().join(format!("palyra-semantic-memory-{}.sqlite3", Ulid::new()))
}

fn authority(store: &JournalStore) -> SemanticMemoryReviewAuthority {
    let authority = SemanticMemoryReviewAuthority {
        session_id: Ulid::new().to_string(),
        run_id: Ulid::new().to_string(),
        principal: "user-1".to_owned(),
        device_id: "device-1".to_owned(),
        channel: Some("console".to_owned()),
        host_policy_sha256: digest_text("host-policy"),
    };
    store
        .resolve_orchestrator_session(&OrchestratorSessionResolveRequest {
            session_id: Some(authority.session_id.clone()),
            session_key: Some(authority.session_id.clone()),
            session_label: None,
            principal: authority.principal.clone(),
            device_id: authority.device_id.clone(),
            channel: authority.channel.clone(),
            require_existing: false,
            reset_session: false,
        })
        .expect("review session should be durable");
    authority
}

fn target_scope(authority: &SemanticMemoryReviewAuthority) -> SemanticMemoryTargetScope {
    SemanticMemoryTargetScope {
        principal: authority.principal.clone(),
        channel: authority.channel.clone(),
        session_id: None,
    }
}

fn evidence(
    authority: &SemanticMemoryReviewAuthority,
    id: &str,
    source: &str,
    value: &str,
    kind: SemanticMemoryEpistemicKind,
    sensitivity: SemanticMemorySensitivity,
    observed_at_unix_ms: i64,
) -> SemanticMemoryEvidenceRefV1 {
    SemanticMemoryEvidenceRefV1 {
        v: 1,
        evidence_id: id.to_owned(),
        source_ref: source.to_owned(),
        citation_uri: format!("memory://evidence/{id}"),
        content_sha256: digest_text(format!("content-{id}").as_str()),
        provenance_sha256: digest_text(format!("provenance-{id}").as_str()),
        claim_key: "preferred_editor".to_owned(),
        claim_value_sha256: digest_text(value),
        acl_scope: semantic_memory_acl_scope(
            authority.principal.as_str(),
            authority.channel.as_deref(),
            None,
        ),
        epistemic_kind: kind,
        sensitivity,
        confidence_basis_points: 9_200,
        observed_at_unix_ms,
        expires_at_unix_ms: None,
        corrects_evidence_ids: Vec::new(),
    }
}

fn draft(
    authority: &SemanticMemoryReviewAuthority,
    candidate_id: &str,
    now: i64,
) -> SemanticMemoryCandidateDraftV1 {
    SemanticMemoryCandidateDraftV1 {
        candidate_id: candidate_id.to_owned(),
        summary_text: "The preferred editor is Helix.".to_owned(),
        evidence_refs: vec![
            evidence(
                authority,
                "evidence-1",
                "journal.event.one",
                "helix",
                SemanticMemoryEpistemicKind::Preference,
                SemanticMemorySensitivity::Internal,
                now - 20,
            ),
            evidence(
                authority,
                "evidence-2",
                "journal.event.two",
                "helix",
                SemanticMemoryEpistemicKind::Preference,
                SemanticMemorySensitivity::Internal,
                now - 10,
            ),
        ],
        retention_expires_at_unix_ms: None,
        created_at_unix_ms: now,
    }
}

fn eval_cases(candidate_relevant: bool) -> Vec<SemanticMemoryQualityEvalCaseV1> {
    (0..10)
        .map(|index| SemanticMemoryQualityEvalCaseV1 {
            case_id: format!("case-{index}"),
            query: "preferred editor helix".to_owned(),
            expected_baseline_memory_ids: Vec::new(),
            candidate_relevant,
        })
        .collect()
}

fn enabled_policy() -> SemanticMemoryConsolidationPolicy {
    SemanticMemoryConsolidationPolicy {
        enabled: true,
        ..SemanticMemoryConsolidationPolicy::default()
    }
}

fn propose(
    store: &JournalStore,
    memory_id: &str,
    candidate_id: &str,
    authority: &SemanticMemoryReviewAuthority,
    now: i64,
) -> SemanticMemoryProposedRecord {
    store
        .propose_semantic_memory(
            memory_id,
            draft(authority, candidate_id, now),
            eval_cases(true).as_slice(),
            &enabled_policy(),
            &target_scope(authority),
            authority,
        )
        .expect("semantic candidate should pass server eval")
}

fn approve(
    store: &JournalStore,
    proposed: &SemanticMemoryProposedRecord,
    authority: &SemanticMemoryReviewAuthority,
) -> String {
    let approval_id = Ulid::new().to_string();
    store
        .create_approval(&semantic_memory_approval_request(
            approval_id.clone(),
            proposed,
            authority,
        ))
        .expect("approval should persist");
    store
        .resolve_approval(&ApprovalResolveRequest {
            approval_id: approval_id.clone(),
            decision: ApprovalDecision::Allow,
            decision_scope: ApprovalDecisionScope::Once,
            decision_reason: "operator reviewed exact semantic candidate".to_owned(),
            decision_scope_ttl_ms: None,
        })
        .expect("approval should resolve");
    approval_id
}

#[test]
fn reviewed_candidate_is_real_searchable_memory_with_citations_after_restart() {
    let path = temp_db_path();
    let journal_config = config(path.clone());
    let store = JournalStore::open(journal_config.clone()).expect("journal should open");
    let authority = authority(&store);
    let now = crate::gateway::current_unix_ms();
    let proposed = propose(&store, "preferred-editor", "candidate-one", &authority, now);
    assert_eq!(proposed.candidate.contradiction_status, SemanticMemoryContradictionStatus::None);
    assert!(proposed.candidate.quality_eval.qualifies());
    let approval_id = approve(&store, &proposed, &authority);
    let active = store
        .activate_semantic_memory(
            proposed.candidate.candidate_id.as_str(),
            approval_id.as_str(),
            &target_scope(&authority),
            &authority,
            crate::gateway::current_unix_ms(),
        )
        .expect("approved candidate should activate");
    assert_eq!(active.record.citations.len(), 2);

    let hits = store
        .search_memory(&MemorySearchRequest {
            principal: authority.principal.clone(),
            channel: authority.channel.clone(),
            session_id: Some("later-real-session".to_owned()),
            query: "preferred editor helix".to_owned(),
            top_k: 5,
            min_score: 0.0,
            tags: Vec::new(),
            sources: Vec::new(),
        })
        .expect("ordinary memory search should work");
    let hit = hits
        .iter()
        .find(|hit| hit.item.memory_id == active.projected_memory.memory_id)
        .expect("semantic projection must be in ordinary retrieval");
    let semantic = hit.semantic.as_ref().expect("ordinary hit must carry semantic provenance");
    assert_eq!(semantic.citations.len(), 2);
    assert_eq!(semantic.epistemic_label, "preference");
    let tool_payload = memory_search_tool_output_payload(std::slice::from_ref(hit));
    assert_eq!(
        tool_payload.pointer("/hits/0/provenance/semantic/citations/0/evidence_id"),
        Some(&serde_json::Value::String("evidence-1".to_owned()))
    );

    drop(store);
    let reopened = JournalStore::open(journal_config).expect("journal should reopen");
    let reloaded = reopened
        .active_semantic_memory("preferred-editor", &target_scope(&authority))
        .expect("active record should load")
        .expect("active record should survive restart");
    assert_eq!(reloaded.record.record_sha256, active.record.record_sha256);
    assert_eq!(reloaded.projected_memory.memory_id, active.projected_memory.memory_id);
}

#[test]
fn activation_releases_journal_lock_during_projection_embedding() {
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let provider = Arc::new(BlockingEmbeddingProvider {
        block_on_embed: AtomicBool::new(false),
        started_tx: Mutex::new(Some(started_tx)),
        release_rx: Mutex::new(release_rx),
    });
    let store = Arc::new(
        JournalStore::open_with_memory_embedding_provider(config(temp_db_path()), provider.clone())
            .expect("journal should open"),
    );
    let authority = authority(&store);
    let proposed = propose(
        &store,
        "blocking-memory",
        "candidate-blocking",
        &authority,
        crate::gateway::current_unix_ms(),
    );
    let approval_id = approve(&store, &proposed, &authority);
    provider.block_on_embed.store(true, Ordering::SeqCst);

    let activation_store = Arc::clone(&store);
    let activation_authority = authority.clone();
    let activation_scope = target_scope(&authority);
    let activation_candidate_id = proposed.candidate.candidate_id.clone();
    let activation_thread = std::thread::spawn(move || {
        activation_store.activate_semantic_memory(
            activation_candidate_id.as_str(),
            approval_id.as_str(),
            &activation_scope,
            &activation_authority,
            crate::gateway::current_unix_ms(),
        )
    });
    started_rx.recv().expect("activation should start embedding");
    let guard =
        store.connection.try_lock().expect("journal lock must remain available during embedding");
    drop(guard);
    release_tx.send(()).expect("embedding should unblock");
    activation_thread
        .join()
        .expect("activation thread should finish")
        .expect("activation should commit");
}

#[test]
fn lifecycle_and_review_surfaces_deny_cross_principal_and_channel() {
    let store = JournalStore::open(config(temp_db_path())).expect("journal should open");
    let authority = authority(&store);
    let now = crate::gateway::current_unix_ms();
    let proposed = propose(&store, "owned-memory", "candidate-owned", &authority, now);
    let wrong_principal = SemanticMemoryTargetScope {
        principal: "other-user".to_owned(),
        channel: authority.channel.clone(),
        session_id: None,
    };
    let wrong_channel = SemanticMemoryTargetScope {
        principal: authority.principal.clone(),
        channel: Some("other-channel".to_owned()),
        session_id: None,
    };
    assert!(store
        .semantic_memory_proposal(proposed.candidate.candidate_id.as_str(), &wrong_principal,)
        .expect("proposal lookup should complete")
        .is_none());
    assert!(store
        .semantic_memory_proposal(proposed.candidate.candidate_id.as_str(), &wrong_channel,)
        .expect("proposal lookup should complete")
        .is_none());

    let approval_id = approve(&store, &proposed, &authority);
    let active = store
        .activate_semantic_memory(
            proposed.candidate.candidate_id.as_str(),
            approval_id.as_str(),
            &target_scope(&authority),
            &authority,
            crate::gateway::current_unix_ms(),
        )
        .expect("owned candidate should activate");
    for denied_scope in [&wrong_principal, &wrong_channel] {
        let feedback_error = store
            .apply_semantic_memory_feedback(
                "owned-memory",
                denied_scope,
                SemanticMemoryRetrievalFeedbackV1 {
                    useful: true,
                    corrected: false,
                    retrieved_at_unix_ms: now.saturating_add(1),
                    correction_evidence_ref: None,
                },
            )
            .expect_err("cross-scope feedback must fail");
        assert_eq!(
            feedback_error.to_string(),
            "invalid journal argument: semantic_memory.acl_mismatch"
        );
        let archive_error = store
            .archive_semantic_memory_durable("owned-memory", denied_scope, now.saturating_add(2))
            .expect_err("cross-scope archive must fail");
        assert_eq!(
            archive_error.to_string(),
            "invalid journal argument: semantic_memory.acl_mismatch"
        );
        let rollback_error = store
            .semantic_memory_rollback_review(
                "owned-memory",
                active.record.record_sha256.as_str(),
                denied_scope,
                &authority,
            )
            .expect_err("cross-scope rollback review must fail");
        assert_eq!(
            rollback_error.to_string(),
            "invalid journal argument: semantic_memory.acl_mismatch"
        );
        assert!(store
            .search_semantic_memory(&MemorySearchRequest {
                principal: denied_scope.principal.clone(),
                channel: denied_scope.channel.clone(),
                session_id: None,
                query: "preferred editor helix".to_owned(),
                top_k: 5,
                min_score: 0.0,
                tags: Vec::new(),
                sources: Vec::new(),
            })
            .expect("cross-scope search should complete")
            .is_empty());
        assert_eq!(
            store
                .semantic_memory_diagnostics(denied_scope)
                .expect("scoped diagnostics should load")
                .active_memories,
            0
        );
    }
}

#[test]
fn proposal_rejects_disabled_acl_mismatch_and_actual_quality_failure() {
    let store = JournalStore::open(config(temp_db_path())).expect("journal should open");
    let authority = authority(&store);
    let now = crate::gateway::current_unix_ms();
    let disabled = store
        .propose_semantic_memory(
            "disabled-memory",
            draft(&authority, "candidate-disabled", now),
            eval_cases(true).as_slice(),
            &SemanticMemoryConsolidationPolicy::default(),
            &target_scope(&authority),
            &authority,
        )
        .expect_err("default-off policy must fail closed");
    assert_eq!(disabled.to_string(), "invalid journal argument: semantic_memory.rollout_disabled");

    let mut mismatched = draft(&authority, "candidate-acl", now);
    mismatched.evidence_refs[0].acl_scope = semantic_memory_acl_scope("other-user", None, None);
    let acl_error = store
        .propose_semantic_memory(
            "acl-memory",
            mismatched,
            eval_cases(true).as_slice(),
            &enabled_policy(),
            &target_scope(&authority),
            &authority,
        )
        .expect_err("mixed ACL must fail");
    assert_eq!(acl_error.to_string(), "invalid journal argument: semantic_memory.acl_mismatch");

    let mut raw_reference = draft(&authority, "candidate-raw-reference", now);
    raw_reference.evidence_refs[0].source_ref = "session:raw-transcript-id".to_owned();
    let reference_error = store
        .propose_semantic_memory(
            "raw-reference",
            raw_reference,
            eval_cases(true).as_slice(),
            &enabled_policy(),
            &target_scope(&authority),
            &authority,
        )
        .expect_err("raw session references must be replaced by sanitized provenance");
    assert_eq!(
        reference_error.to_string(),
        "invalid journal argument: semantic_memory.reference_redaction_required"
    );

    let quality_error = store
        .propose_semantic_memory(
            "bad-quality",
            draft(&authority, "candidate-bad-quality", now),
            eval_cases(false).as_slice(),
            &enabled_policy(),
            &target_scope(&authority),
            &authority,
        )
        .expect_err("observed candidate false positives must fail activation eval");
    assert_eq!(
        quality_error.to_string(),
        "invalid journal argument: semantic_memory.quality_eval_failed"
    );

    let mut missing_review = authority.clone();
    missing_review.session_id = Ulid::new().to_string();
    let review_error = store
        .propose_semantic_memory(
            "missing-review",
            draft(&missing_review, "candidate-missing-review", now),
            eval_cases(true).as_slice(),
            &enabled_policy(),
            &target_scope(&missing_review),
            &missing_review,
        )
        .expect_err("unknown review session must fail closed");
    assert_eq!(
        review_error.to_string(),
        "invalid journal argument: semantic_memory.review_session_missing_or_stale"
    );

    let stale_review =
        propose(&store, "stale-review-session", "candidate-stale-review", &authority, now);
    let stale_approval = approve(&store, &stale_review, &authority);
    store
        .cleanup_orchestrator_session(&OrchestratorSessionCleanupRequest {
            session_id: Some(authority.session_id.clone()),
            session_key: None,
            principal: authority.principal.clone(),
            device_id: authority.device_id.clone(),
            channel: authority.channel.clone(),
        })
        .expect("review session should archive");
    let stale_review_error = store
        .activate_semantic_memory(
            stale_review.candidate.candidate_id.as_str(),
            stale_approval.as_str(),
            &target_scope(&authority),
            &authority,
            crate::gateway::current_unix_ms(),
        )
        .expect_err("archived review session must invalidate activation");
    assert_eq!(
        stale_review_error.to_string(),
        "invalid journal argument: semantic_memory.review_session_missing_or_stale"
    );
    let guard = store.connection.lock().expect("journal lock should work");
    let consumed = guard
        .query_row(
            "SELECT COUNT(*) FROM approval_consumptions WHERE approval_ulid = ?1",
            params![stale_approval],
            |row| row.get::<_, i64>(0),
        )
        .expect("consumption count should query");
    assert_eq!(consumed, 0);
}

#[test]
fn approval_consumption_and_stale_generation_cannot_publish() {
    let store = JournalStore::open(config(temp_db_path())).expect("journal should open");
    let authority = authority(&store);
    let now = crate::gateway::current_unix_ms();
    let first = propose(&store, "editor", "candidate-first", &authority, now);
    let stale = propose(&store, "editor", "candidate-stale", &authority, now + 1);
    let first_approval = approve(&store, &first, &authority);
    let stale_approval = approve(&store, &stale, &authority);
    let active = store
        .activate_semantic_memory(
            first.candidate.candidate_id.as_str(),
            first_approval.as_str(),
            &target_scope(&authority),
            &authority,
            crate::gateway::current_unix_ms(),
        )
        .expect("first candidate should activate");
    let replay = store
        .activate_semantic_memory(
            first.candidate.candidate_id.as_str(),
            first_approval.as_str(),
            &target_scope(&authority),
            &authority,
            crate::gateway::current_unix_ms(),
        )
        .expect("same committed activation should replay");
    assert_eq!(replay.record.record_sha256, active.record.record_sha256);
    let error = store
        .activate_semantic_memory(
            stale.candidate.candidate_id.as_str(),
            stale_approval.as_str(),
            &target_scope(&authority),
            &authority,
            crate::gateway::current_unix_ms(),
        )
        .expect_err("stale proposal must not consume approval or publish");
    assert_eq!(
        error.to_string(),
        "invalid journal argument: semantic_memory.active_generation_stale"
    );
    let guard = store.connection.lock().expect("journal lock should work");
    let consumed = guard
        .query_row(
            "SELECT COUNT(*) FROM approval_consumptions WHERE approval_ulid = ?1",
            params![stale_approval],
            |row| row.get::<_, i64>(0),
        )
        .expect("consumption count should query");
    assert_eq!(consumed, 0);
}

#[test]
fn degrade_archive_and_rollback_preserve_current_generation_and_retrieval_truth() {
    let store = JournalStore::open(config(temp_db_path())).expect("journal should open");
    let authority = authority(&store);
    let now = crate::gateway::current_unix_ms();
    let proposed = propose(&store, "editor", "candidate-current", &authority, now);
    let approval_id = approve(&store, &proposed, &authority);
    let original = store
        .activate_semantic_memory(
            proposed.candidate.candidate_id.as_str(),
            approval_id.as_str(),
            &target_scope(&authority),
            &authority,
            crate::gateway::current_unix_ms(),
        )
        .expect("candidate should activate");
    let archived = store
        .archive_semantic_memory_durable(
            "editor",
            &target_scope(&authority),
            crate::gateway::current_unix_ms().saturating_add(1),
        )
        .expect("archive should retain lineage");
    assert!(store
        .active_semantic_memory("editor", &target_scope(&authority))
        .expect("active lookup should work")
        .is_none());
    assert_eq!(
        store
            .semantic_memory_activation_context_for_scope("editor", &target_scope(&authority),)
            .expect("current generation should remain")
            .approval_generation,
        2
    );
    let search = store
        .search_semantic_memory(&MemorySearchRequest {
            principal: authority.principal.clone(),
            channel: authority.channel.clone(),
            session_id: None,
            query: "preferred editor helix".to_owned(),
            top_k: 5,
            min_score: 0.0,
            tags: Vec::new(),
            sources: Vec::new(),
        })
        .expect("semantic search should work");
    assert!(search.is_empty(), "archived projection must not remain current");

    let rollback = store
        .semantic_memory_rollback_review(
            "editor",
            original.record.record_sha256.as_str(),
            &target_scope(&authority),
            &authority,
        )
        .expect("direct archived predecessor should be reviewable");
    let rollback_approval = approve(&store, &rollback, &authority);
    let restored = store
        .rollback_semantic_memory_durable(
            "editor",
            original.record.record_sha256.as_str(),
            rollback_approval.as_str(),
            &target_scope(&authority),
            &authority,
            crate::gateway::current_unix_ms().saturating_add(2),
        )
        .expect("approved rollback after archive should restore recall");
    assert_eq!(restored.record.approval_generation, 2);
    assert!(restored.record.rollback_history_sha256.contains(&archived.record_sha256));

    let feedback_record = store
        .apply_semantic_memory_feedback(
            "editor",
            &target_scope(&authority),
            SemanticMemoryRetrievalFeedbackV1 {
                useful: false,
                corrected: true,
                retrieved_at_unix_ms: crate::gateway::current_unix_ms().saturating_add(3),
                correction_evidence_ref: Some({
                    let mut correction = evidence(
                        &authority,
                        "evidence-correction",
                        "journal.event.correction",
                        "zed",
                        SemanticMemoryEpistemicKind::UserFact,
                        SemanticMemorySensitivity::Internal,
                        crate::gateway::current_unix_ms().saturating_add(3),
                    );
                    correction.corrects_evidence_ids =
                        vec![restored.record.evidence_refs[0].evidence_id.clone()];
                    correction
                }),
            },
        )
        .expect("user correction should degrade and preserve evidence");
    assert_eq!(feedback_record.lifecycle, ConsolidatedMemoryLifecycle::Degraded);
    let second_rollback = store
        .semantic_memory_rollback_review(
            "editor",
            restored.record.record_sha256.as_str(),
            &target_scope(&authority),
            &authority,
        )
        .expect("rollback after degradation should keep current lineage");
    let second_approval = approve(&store, &second_rollback, &authority);
    let restored_again = store
        .rollback_semantic_memory_durable(
            "editor",
            restored.record.record_sha256.as_str(),
            second_approval.as_str(),
            &target_scope(&authority),
            &authority,
            crate::gateway::current_unix_ms().saturating_add(4),
        )
        .expect("rollback after degrade should work");
    assert_eq!(restored_again.record.approval_generation, 3);
}

#[test]
fn stale_projection_is_removed_and_next_activation_advances_generation() {
    let store = JournalStore::open(config(temp_db_path())).expect("journal should open");
    let authority = authority(&store);
    let now = crate::gateway::current_unix_ms();
    let first = propose(&store, "stale-memory", "candidate-stale-one", &authority, now);
    let first_approval = approve(&store, &first, &authority);
    let first_active = store
        .activate_semantic_memory(
            first.candidate.candidate_id.as_str(),
            first_approval.as_str(),
            &target_scope(&authority),
            &authority,
            crate::gateway::current_unix_ms(),
        )
        .expect("first candidate should activate");
    let stale_at = crate::gateway::current_unix_ms()
        .max(first_active.record.activated_at_unix_ms.saturating_add(2));
    assert!(store
        .mark_semantic_memory_stale_durable("stale-memory", &target_scope(&authority), stale_at, 1,)
        .expect("staleness evaluation should persist"));
    assert!(store
        .active_semantic_memory("stale-memory", &target_scope(&authority))
        .expect("active lookup should complete")
        .is_none());

    let second = propose(
        &store,
        "stale-memory",
        "candidate-stale-two",
        &authority,
        crate::gateway::current_unix_ms(),
    );
    assert_eq!(second.context.approval_generation, 2);
    let second_approval = approve(&store, &second, &authority);
    let reactivated = store
        .activate_semantic_memory(
            second.candidate.candidate_id.as_str(),
            second_approval.as_str(),
            &target_scope(&authority),
            &authority,
            crate::gateway::current_unix_ms(),
        )
        .expect("new reviewed candidate should reactivate the lineage");
    assert_eq!(reactivated.record.approval_generation, 2);
    assert_eq!(reactivated.record.version, 3);
}

#[test]
fn rollout_filter_uses_durable_projection_id_not_user_tag() {
    let store = JournalStore::open(config(temp_db_path())).expect("journal should open");
    let authority = authority(&store);
    store
        .create_memory_item(&MemoryItemCreateRequest {
            memory_id: "ordinary-tagged-memory".to_owned(),
            principal: authority.principal.clone(),
            channel: authority.channel.clone(),
            session_id: None,
            source: MemorySource::Manual,
            content_text: "preferred editor helix ordinary".to_owned(),
            tags: vec!["semantic_memory".to_owned()],
            confidence: Some(0.9),
            ttl_unix_ms: None,
        })
        .expect("ordinary user memory should persist");
    store
        .create_memory_item(&MemoryItemCreateRequest {
            memory_id: "unreviewed-tagged-summary".to_owned(),
            principal: authority.principal.clone(),
            channel: authority.channel.clone(),
            session_id: None,
            source: MemorySource::Summary,
            content_text: "preferred editor helix unreviewed summary".to_owned(),
            tags: vec!["semantic_memory".to_owned()],
            confidence: Some(0.95),
            ttl_unix_ms: None,
        })
        .expect("unreviewed summary should persist outside semantic lifecycle");
    let now = crate::gateway::current_unix_ms();
    let proposed = propose(&store, "editor", "candidate-filter", &authority, now);
    let approval_id = approve(&store, &proposed, &authority);
    let active = store
        .activate_semantic_memory(
            proposed.candidate.candidate_id.as_str(),
            approval_id.as_str(),
            &target_scope(&authority),
            &authority,
            crate::gateway::current_unix_ms(),
        )
        .expect("semantic candidate should activate");
    let mut candidates = store
        .search_memory_candidates(&MemorySearchRequest {
            principal: authority.principal.clone(),
            channel: authority.channel.clone(),
            session_id: None,
            query: "preferred editor helix".to_owned(),
            top_k: 1,
            min_score: 0.0,
            tags: Vec::new(),
            sources: Vec::new(),
        })
        .expect("pre-score candidates should load");
    store
        .remove_semantic_memory_candidates(&mut candidates)
        .expect("rollout filter should use durable IDs");
    assert!(candidates
        .iter()
        .any(|candidate| candidate.item.memory_id == "ordinary-tagged-memory"));
    assert!(!candidates
        .iter()
        .any(|candidate| candidate.item.memory_id == active.projected_memory.memory_id));

    let mut reviewed_candidates = store
        .search_memory_candidates(&MemorySearchRequest {
            principal: authority.principal.clone(),
            channel: authority.channel.clone(),
            session_id: None,
            query: "preferred editor helix".to_owned(),
            top_k: 4,
            min_score: 0.0,
            tags: Vec::new(),
            sources: vec![MemorySource::Summary],
        })
        .expect("summary candidates should load before exact provenance filtering");
    store
        .retain_active_semantic_memory_candidates(&mut reviewed_candidates)
        .expect("reviewed semantic filter should use durable IDs");
    assert_eq!(reviewed_candidates.len(), 1);
    assert_eq!(reviewed_candidates[0].item.memory_id, active.projected_memory.memory_id);
}

#[test]
fn diagnostics_and_metadata_are_hash_only_and_secret_free() {
    let store = JournalStore::open(config(temp_db_path())).expect("journal should open");
    let authority = authority(&store);
    let now = crate::gateway::current_unix_ms();
    let mut candidate = draft(&authority, "candidate-secret", now);
    let raw_secret = "sk-proj-1234567890abcdefghijklmnop";
    candidate.summary_text = format!("The preferred editor is Helix. token={raw_secret}");
    let rejected = store
        .propose_semantic_memory(
            "secret-redaction",
            candidate,
            eval_cases(true).as_slice(),
            &enabled_policy(),
            &target_scope(&authority),
            &authority,
        )
        .expect_err("secret-bearing summary must not enter eval or persistence");
    assert_eq!(
        rejected.to_string(),
        "invalid journal argument: semantic_memory.summary_redaction_required"
    );
    let proposed = propose(&store, "safe-diagnostics", "candidate-diagnostics", &authority, now);
    assert!(!proposed.candidate.summary_text.contains(raw_secret));
    let diagnostics = store
        .semantic_memory_diagnostics(&target_scope(&authority))
        .expect("diagnostics should load");
    let serialized = serde_json::to_string(&diagnostics).expect("diagnostics should serialize");
    assert!(!serialized.contains(raw_secret));
    assert!(!serialized.contains("preferred editor"));
    assert_eq!(diagnostics.proposed_candidates, 1);
    assert_eq!(
        diagnostics.latest_reason_code, None,
        "inert proposal has no lifecycle transition yet"
    );
}

#[test]
fn sensitive_correction_requires_retention_and_preserves_user_evidence() {
    let store = JournalStore::open(config(temp_db_path())).expect("journal should open");
    let authority = authority(&store);
    let now = crate::gateway::current_unix_ms();
    let mut candidate = draft(&authority, "candidate-sensitive", now);
    for item in &mut candidate.evidence_refs {
        item.sensitivity = SemanticMemorySensitivity::Sensitive;
    }
    let error = store
        .propose_semantic_memory(
            "sensitive",
            candidate.clone(),
            eval_cases(true).as_slice(),
            &enabled_policy(),
            &target_scope(&authority),
            &authority,
        )
        .expect_err("sensitive evidence without retention must fail");
    assert_eq!(
        error.to_string(),
        "invalid journal argument: semantic_memory.sensitive_retention_invalid"
    );
    candidate.retention_expires_at_unix_ms = Some(now + 60_000);
    let proposed = store
        .propose_semantic_memory(
            "sensitive",
            candidate,
            eval_cases(true).as_slice(),
            &enabled_policy(),
            &target_scope(&authority),
            &authority,
        )
        .expect("bounded sensitive candidate should remain reviewable");
    assert!(proposed.candidate.review_required);
    let approval_id = approve(&store, &proposed, &authority);
    store
        .activate_semantic_memory(
            proposed.candidate.candidate_id.as_str(),
            approval_id.as_str(),
            &target_scope(&authority),
            &authority,
            crate::gateway::current_unix_ms(),
        )
        .expect("reviewed sensitive memory should activate");
    assert_eq!(
        store
            .purge_expired_memory_items(now.saturating_add(60_001))
            .expect("retention expiry should reconcile"),
        1
    );
    assert!(store
        .active_semantic_memory("sensitive", &target_scope(&authority))
        .expect("active lookup should complete")
        .is_none());
    let context = store
        .semantic_memory_activation_context_for_scope("sensitive", &target_scope(&authority))
        .expect("expired lineage should remain");
    assert_eq!(context.approval_generation, 2);
}
