//! Behavioral specifications for the pure RuntimeKernelV2 preparation contract.
//!
//! Fixtures use the shared event and generation primitives so lane authority,
//! identity correlation, and durable validation match production boundaries.

use std::collections::{BTreeMap, BTreeSet};

use palyra_common::runtime_contracts::{
    GenerationLeaseV1, RuntimeApprovalSubjectId, RuntimeAttemptId, RuntimeDeliveryIntentId,
    RuntimeErrorPhase, RuntimeEventEnvelopeV2, RuntimeEventId, RuntimeEventName,
    RuntimeEventPayloadRef, RuntimeGeneration, RuntimeGenerationLane, RuntimeIdentitySetV1,
    RuntimeLeaseId, RuntimeOperationId, RuntimeRunId, RuntimeSessionId, RuntimeToolExecutionId,
    RuntimeToolProposalId, RuntimeTraceId, RUNTIME_EVENT_ENVELOPE_SCHEMA_VERSION,
    RUNTIME_GENERATION_SCHEMA_VERSION,
};

use super::*;
use super::{
    profile::{RuntimeKernelCompatibilityOverridesV1, RuntimeKernelProfileConfigV1},
    selection::{
        resolve_runtime_authority, RuntimeAuthority, RuntimeAuthorityDecisionV1,
        RuntimeAuthorityProgressEvidence, SessionCanarySelector, V2RuntimeAvailability,
    },
};

fn generation(value: u64) -> RuntimeGeneration {
    RuntimeGeneration::new(value).expect("test generation is non-zero")
}

fn base_identities() -> RuntimeIdentitySetV1 {
    RuntimeIdentitySetV1::for_run(
        RuntimeTraceId::parse("trace_01").expect("test trace id is valid"),
        RuntimeSessionId::parse("session_01").expect("test session id is valid"),
        RuntimeRunId::parse("run_01").expect("test run id is valid"),
        generation(7),
    )
}

fn lane_generation(lane: RuntimeGenerationLane) -> RuntimeGeneration {
    match lane {
        RuntimeGenerationLane::Run => generation(7),
        RuntimeGenerationLane::Harness => generation(11),
        RuntimeGenerationLane::Provider => generation(12),
        RuntimeGenerationLane::Tool => generation(13),
        RuntimeGenerationLane::Plugin => generation(14),
        RuntimeGenerationLane::Worker => generation(15),
        RuntimeGenerationLane::Process => generation(16),
        RuntimeGenerationLane::Mcp => generation(17),
        RuntimeGenerationLane::Delivery => generation(18),
    }
}

fn lane_authority() -> KernelLaneAuthoritySet {
    let base = base_identities();
    let leases = RuntimeGenerationLane::wire_contract_values()
        .iter()
        .filter_map(|value| RuntimeGenerationLane::parse(value.canonical))
        .enumerate()
        .map(|(index, lane)| GenerationLeaseV1 {
            schema_version: RUNTIME_GENERATION_SCHEMA_VERSION,
            lease_id: RuntimeLeaseId::parse(format!("lease_{index}").as_str())
                .expect("test lease id is valid"),
            session_id: base.session_id.clone(),
            run_id: Some(base.run_id.clone()),
            lane,
            generation: lane_generation(lane),
            owner: "test_host".to_owned(),
            acquired_at_unix_ms: 1,
            expires_at_unix_ms: 2,
        })
        .collect();
    KernelLaneAuthoritySet::new(&base, leases).expect("test authority is valid")
}

fn admitted_run_lease() -> GenerationLeaseV1 {
    lane_authority().run_lease(&base_identities()).expect("test Run lease is valid").clone()
}

fn authority_decision(profile: RuntimeKernelVersion) -> RuntimeAuthorityDecisionV1 {
    let canary_basis_points = if profile == RuntimeKernelVersion::V2Canary { 5_000 } else { 0 };
    let config = RuntimeKernelProfileConfigV1::new(
        profile,
        canary_basis_points,
        RuntimeKernelCompatibilityOverridesV1::none(),
    )
    .expect("test runtime profile is valid");
    if profile == RuntimeKernelVersion::V2Canary {
        for index in 0..128 {
            let key = format!("test-canary-key-{index}");
            let selector = SessionCanarySelector::new(canary_basis_points, key.as_bytes())
                .expect("test selector is valid");
            let decision = resolve_runtime_authority(
                &config,
                &base_identities(),
                V2RuntimeAvailability::Ready,
                RuntimeAuthorityProgressEvidence::pristine(),
                Some(&selector),
            )
            .expect("test canary authority should resolve");
            if decision.selected_runtime() == Some(RuntimeAuthority::V2) {
                return decision;
            }
        }
        panic!("test fixture should find a selected canary bucket");
    }
    resolve_runtime_authority(
        &config,
        &base_identities(),
        V2RuntimeAvailability::Ready,
        RuntimeAuthorityProgressEvidence::pristine(),
        None,
    )
    .expect("test runtime authority should resolve")
}

fn admitted_kernel(
    profile: RuntimeKernelVersion,
) -> Result<RuntimeKernelV2, KernelTransitionError> {
    RuntimeKernelV2::admit_for_test(
        authority_decision(profile),
        base_identities(),
        admitted_run_lease(),
        1,
    )
}

fn lane_authority_with_generation(
    lane: RuntimeGenerationLane,
    active_generation: RuntimeGeneration,
) -> KernelLaneAuthoritySet {
    let mut authority = lane_authority();
    let lease = authority.lease_mut_for_test(lane).expect("test lane lease exists");
    lease.generation = active_generation;
    authority
}

fn event(sequence: u64, name: RuntimeEventName) -> RuntimeEventEnvelopeV2 {
    event_with_generation(sequence, name, lane_generation(name.descriptor().generation_lane))
}

fn event_with_generation(
    sequence: u64,
    name: RuntimeEventName,
    event_generation: RuntimeGeneration,
) -> RuntimeEventEnvelopeV2 {
    let descriptor = name.descriptor();
    let mut identities = RuntimeIdentitySetV1::for_run(
        RuntimeTraceId::parse("trace_01").expect("test trace id is valid"),
        RuntimeSessionId::parse("session_01").expect("test session id is valid"),
        RuntimeRunId::parse("run_01").expect("test run id is valid"),
        event_generation,
    );
    for field in descriptor.required_identity_fields {
        populate_identity(&mut identities, field);
    }
    if matches!(name, RuntimeEventName::ToolProposed) {
        identities.attempt_id =
            Some(RuntimeAttemptId::parse("attempt_01").expect("test attempt id is valid"));
    }
    if matches!(name, RuntimeEventName::ToolIntentRecorded) {
        identities.tool_proposal_id =
            Some(RuntimeToolProposalId::parse("proposal_01").expect("test proposal id is valid"));
    }
    if identities.delivery_intent_id.is_some() {
        identities
            .bind_delivery_intent(
                identities.delivery_intent_id.clone().expect("delivery identity was populated"),
                &RuntimeEventId::parse("output_event_01").expect("test output event id is valid"),
            )
            .expect("test delivery binding is valid");
    }
    RuntimeEventEnvelopeV2 {
        schema_version: RUNTIME_EVENT_ENVELOPE_SCHEMA_VERSION,
        event_id: RuntimeEventId::parse(
            format!("event_{}_{}_{}", name.as_str(), event_generation.get(), sequence).as_str(),
        )
        .expect("test event id is valid"),
        identities,
        sequence,
        causal_parent_event_id: None,
        subsystem: descriptor.subsystem,
        phase: descriptor.phase,
        event_name: name,
        reason_code: format!("test.{}", name.as_str().replace('.', "_")),
        actor_kind: descriptor.actor_kind,
        retryability: descriptor.retryability,
        redaction_class: descriptor.redaction_class,
        terminal: descriptor.terminal,
        payload: RuntimeEventPayloadRef::Inline { metadata: serde_json::json!({}) },
        occurred_at_unix_ms: 1_700_000_000_000,
        extensions: BTreeMap::new(),
    }
}

fn populate_identity(identities: &mut RuntimeIdentitySetV1, field: &str) {
    match field {
        "attempt_id" => {
            identities.attempt_id =
                Some(RuntimeAttemptId::parse("attempt_01").expect("test attempt id is valid"));
        }
        "tool_proposal_id" => {
            identities.tool_proposal_id = Some(
                RuntimeToolProposalId::parse("proposal_01").expect("test proposal id is valid"),
            );
        }
        "tool_execution_id" => {
            identities.tool_execution_id = Some(
                RuntimeToolExecutionId::parse("execution_01").expect("test execution id is valid"),
            );
        }
        "approval_subject_id" => {
            identities.approval_subject_id = Some(
                RuntimeApprovalSubjectId::parse("approval_01").expect("test approval id is valid"),
            );
        }
        "delivery_intent_id" => {
            identities.delivery_intent_id = Some(
                RuntimeDeliveryIntentId::parse("delivery_01").expect("test delivery id is valid"),
            );
        }
        "operation_id" => {
            identities.operation_id = Some(
                RuntimeOperationId::parse("operation_01").expect("test operation id is valid"),
            );
        }
        unexpected => panic!("test fixture does not support required identity {unexpected}"),
    }
}

fn prepare(
    kernel: &RuntimeKernelV2,
    event: RuntimeEventEnvelopeV2,
    transition: KernelTransition,
) -> PreparedKernelTransition {
    let key = format!("request.{}.{}", event.event_name.as_str(), event.sequence);
    kernel
        .prepare_transition(generation(7), &lane_authority(), key.as_str(), event, transition)
        .expect("transition should prepare")
}

fn commit(
    kernel: &mut RuntimeKernelV2,
    event: RuntimeEventEnvelopeV2,
    transition: KernelTransition,
) {
    let prepared = prepare(kernel, event, transition);
    *kernel = RuntimeKernelV2::restore_from_journal(prepared.next_snapshot().clone())
        .expect("journal-restored next snapshot should validate");
}

fn kernel() -> RuntimeKernelV2 {
    admitted_kernel(RuntimeKernelVersion::V2).expect("test admission evidence is valid")
}

#[test]
fn admission_derives_only_permitted_v2_postures_from_persisted_authority() {
    for profile in
        [RuntimeKernelVersion::V2Shadow, RuntimeKernelVersion::V2Canary, RuntimeKernelVersion::V2]
    {
        let admitted = admitted_kernel(profile).expect("V2 posture should admit");
        assert_eq!(admitted.snapshot().version(), profile);
        assert_eq!(admitted.snapshot().runtime_authority_decision().generation(), generation(7));
        assert_eq!(admitted.snapshot().run_lease(), &admitted_run_lease());
    }

    assert_eq!(
        admitted_kernel(RuntimeKernelVersion::Legacy)
            .expect_err("legacy authority cannot construct RuntimeKernelV2"),
        KernelTransitionError::InvalidRuntimeAuthorityDecision
    );
}

#[test]
fn admission_rejects_generation_mismatch_and_inactive_run_lease() {
    let mut mismatched_base = base_identities();
    mismatched_base.generation = generation(8);
    let mut matching_lease = admitted_run_lease();
    matching_lease.generation = generation(8);
    assert_eq!(
        RuntimeKernelV2::admit_for_test(
            authority_decision(RuntimeKernelVersion::V2),
            mismatched_base,
            matching_lease,
            1,
        )
        .expect_err("decision generation must match exact base identities"),
        KernelTransitionError::RuntimeAuthorityGenerationMismatch {
            active: generation(8),
            observed: generation(7),
        }
    );

    assert_eq!(
        RuntimeKernelV2::admit_for_test(
            authority_decision(RuntimeKernelVersion::V2),
            base_identities(),
            admitted_run_lease(),
            2,
        )
        .expect_err("lease is expired at its exclusive expiry timestamp"),
        KernelTransitionError::InactiveRunLease
    );
}

#[test]
fn transition_requires_the_exact_admitted_run_lease() {
    let kernel = kernel();
    let mut authority = lane_authority();
    authority.lease_mut_for_test(RuntimeGenerationLane::Run).expect("Run lease exists").owner =
        "replacement_owner".to_owned();

    assert_eq!(
        kernel
            .prepare_transition(
                generation(7),
                &authority,
                "request.replaced_run_lease",
                event(1, RuntimeEventName::RunStarted),
                KernelTransition::BeginRuntimeSelection,
            )
            .expect_err("same-generation replacement lease cannot inherit admission"),
        KernelTransitionError::RunLeaseMismatch
    );
}

fn advance_to_provider(kernel: &mut RuntimeKernelV2) {
    commit(kernel, event(1, RuntimeEventName::RunStarted), KernelTransition::BeginRuntimeSelection);
    commit(
        kernel,
        event(1, RuntimeEventName::HarnessAttemptStarted),
        KernelTransition::BeginContextAssembly,
    );
    commit(
        kernel,
        event(1, RuntimeEventName::ProviderAttemptStarted),
        KernelTransition::BeginProviderCall,
    );
}

fn advance_to_tool_gate(kernel: &mut RuntimeKernelV2) {
    advance_to_provider(kernel);
    commit(kernel, event(2, RuntimeEventName::ToolProposed), KernelTransition::BeginToolGate);
}

fn advance_to_approval(kernel: &mut RuntimeKernelV2) {
    advance_to_tool_gate(kernel);
    commit(
        kernel,
        event(3, RuntimeEventName::ApprovalRequired),
        KernelTransition::BeginApprovalWait,
    );
}

fn advance_to_tool_execution(kernel: &mut RuntimeKernelV2) {
    advance_to_approval(kernel);
    commit(kernel, event(4, RuntimeEventName::ApprovalResolved), KernelTransition::ResumeToolGate);
    commit(
        kernel,
        event(5, RuntimeEventName::ToolIntentRecorded),
        KernelTransition::BeginToolExecution,
    );
}

#[test]
fn happy_text_path_reaches_one_done_outcome() {
    let mut kernel = kernel();
    advance_to_provider(&mut kernel);
    commit(
        &mut kernel,
        event(2, RuntimeEventName::ProviderAttemptCompleted),
        KernelTransition::BeginFinalization,
    );
    commit(
        &mut kernel,
        event(1, RuntimeEventName::DeliveryIntentRecorded),
        KernelTransition::BeginDeliveryWait,
    );
    commit(&mut kernel, event(2, RuntimeEventName::RunCompleted), KernelTransition::Complete);

    assert_eq!(kernel.snapshot().state(), KernelState::Done);
    assert_eq!(kernel.snapshot().terminal_outcome(), Some(KernelTerminalOutcome::Done));
    kernel.snapshot().validate().expect("completed snapshot should validate");
}

#[test]
fn tool_approval_projection_path_preserves_child_correlation() {
    let mut kernel = kernel();
    advance_to_tool_execution(&mut kernel);
    commit(
        &mut kernel,
        event(6, RuntimeEventName::ToolResultObserved),
        KernelTransition::BeginResultProjection,
    );
    commit(
        &mut kernel,
        event(2, RuntimeEventName::ProviderAttemptStarted),
        KernelTransition::BeginProviderCall,
    );

    assert_eq!(kernel.snapshot().state(), KernelState::CallingProvider);
    assert_eq!(kernel.snapshot().terminal_outcome(), None);
}

#[test]
fn denied_and_synthetic_tool_results_project_without_execution_authority() {
    for idempotency_case in ["denied", "synthetic"] {
        let mut kernel = kernel();
        advance_to_tool_gate(&mut kernel);
        commit(
            &mut kernel,
            event(3, RuntimeEventName::ToolDecisionRecorded),
            KernelTransition::ResolveToolWithoutExecution,
        );

        assert_eq!(
            kernel.snapshot().state(),
            KernelState::ProjectingResult,
            "{idempotency_case} proposal must rejoin the provider path"
        );
    }
}

#[test]
fn approval_denial_or_timeout_can_project_without_execution_authority() {
    let mut kernel = kernel();
    advance_to_approval(&mut kernel);
    commit(
        &mut kernel,
        event(4, RuntimeEventName::ApprovalResolved),
        KernelTransition::ResumeToolGate,
    );
    commit(
        &mut kernel,
        event(5, RuntimeEventName::ToolDecisionRecorded),
        KernelTransition::ResolveToolWithoutExecution,
    );

    assert_eq!(kernel.snapshot().state(), KernelState::ProjectingResult);
}

#[test]
fn durable_pending_approval_does_not_manufacture_a_transition() {
    let mut kernel = kernel();
    advance_to_approval(&mut kernel);
    let before = kernel.snapshot().clone();

    assert_eq!(kernel.snapshot(), &before);
    assert_eq!(kernel.snapshot().state(), KernelState::AwaitingApproval);
}

#[test]
fn compaction_uses_truthful_provider_completion_event() {
    let mut kernel = kernel();
    advance_to_provider(&mut kernel);
    commit(
        &mut kernel,
        event(2, RuntimeEventName::ProviderAttemptCompleted),
        KernelTransition::BeginCompaction,
    );
    assert_eq!(kernel.snapshot().state(), KernelState::Compacting);
    assert_eq!(
        kernel.snapshot().timeout_posture(),
        KernelTimeoutPosture::Bounded { budget_ms: 120_000 }
    );
    commit(
        &mut kernel,
        event(3, RuntimeEventName::ProviderAttemptStarted),
        KernelTransition::BeginProviderCall,
    );
}

#[test]
fn same_generation_recovery_uses_cleanup_evidence_not_generation_activation() {
    let mut kernel = kernel();
    advance_to_provider(&mut kernel);
    commit(&mut kernel, event(2, RuntimeEventName::BackpressureApplied), KernelTransition::Suspend);
    commit(
        &mut kernel,
        event(1, RuntimeEventName::CleanupPartial),
        KernelTransition::BeginRecovery,
    );
    commit(
        &mut kernel,
        event(2, RuntimeEventName::CleanupCompleted),
        KernelTransition::ResumeRuntimeSelection,
    );

    assert_eq!(kernel.snapshot().state(), KernelState::SelectingRuntime);
    assert_eq!(kernel.snapshot().run_generation(), generation(7));
}

#[test]
fn preparation_is_immutable_until_persisted_snapshot_is_restored() {
    let kernel = kernel();
    let before = kernel.snapshot().clone();
    let prepared = prepare(
        &kernel,
        event(1, RuntimeEventName::RunStarted),
        KernelTransition::BeginRuntimeSelection,
    );

    assert_eq!(kernel.snapshot(), &before);
    assert_eq!(prepared.previous_snapshot(), &before);
    assert_eq!(prepared.next_snapshot().state(), KernelState::SelectingRuntime);
    assert!(matches!(prepared.outcome(), TransitionOutcome::Applied { .. }));
    assert_eq!(prepared.expected_revision(), 0);
    assert_eq!(prepared.next_snapshot().revision(), 1);
    prepared.validate_expected_revision(0).expect("unchanged journal head should satisfy CAS");
    assert_eq!(
        prepared.validate_expected_revision(1).expect_err("advanced journal head must conflict"),
        KernelTransitionError::RevisionConflict { expected: 0, actual: 1 }
    );
    prepared.validate().expect("prepared record should validate");
}

#[test]
fn exact_last_request_replay_is_duplicate_without_state_change() {
    let mut kernel = kernel();
    let first_event = event(1, RuntimeEventName::RunStarted);
    let key = "request.start";
    let first = kernel
        .prepare_transition(
            generation(7),
            &lane_authority(),
            key,
            first_event.clone(),
            KernelTransition::BeginRuntimeSelection,
        )
        .expect("first request should prepare");
    kernel = RuntimeKernelV2::restore_from_journal(first.next_snapshot().clone())
        .expect("committed snapshot should restore");

    let duplicate = kernel
        .prepare_transition(
            generation(7),
            &lane_authority(),
            key,
            first_event,
            KernelTransition::BeginRuntimeSelection,
        )
        .expect("exact last replay should be duplicate");

    assert!(matches!(duplicate.outcome(), TransitionOutcome::Duplicate { .. }));
    assert_eq!(duplicate.previous_snapshot(), duplicate.next_snapshot());
    duplicate.validate().expect("duplicate record should validate");
}

#[test]
fn reused_last_event_id_with_changed_sequence_is_rejected() {
    let mut kernel = kernel();
    let first_event = event(1, RuntimeEventName::RunStarted);
    commit(&mut kernel, first_event.clone(), KernelTransition::BeginRuntimeSelection);
    let mut conflict = first_event;
    conflict.sequence = 2;

    let error = kernel
        .prepare_transition(
            generation(7),
            &lane_authority(),
            "request.conflict",
            conflict,
            KernelTransition::BeginRuntimeSelection,
        )
        .expect_err("event id reuse must fail closed");

    assert_eq!(error, KernelTransitionError::EventIdConflict);
}

#[test]
fn independent_lanes_accept_their_own_sequence_one() {
    let mut kernel = kernel();
    advance_to_provider(&mut kernel);

    assert_eq!(kernel.snapshot().event_cursor_count_for_test(), 3);
    assert!(kernel
        .snapshot()
        .cursor_sequence_for_test(RuntimeGenerationLane::Run, generation(7))
        .is_some_and(|sequence| sequence == 1));
    assert!(kernel
        .snapshot()
        .cursor_sequence_for_test(RuntimeGenerationLane::Harness, generation(11))
        .is_some_and(|sequence| sequence == 1));
    assert!(kernel
        .snapshot()
        .cursor_sequence_for_test(RuntimeGenerationLane::Provider, generation(12))
        .is_some_and(|sequence| sequence == 1));
}

#[test]
fn stale_non_run_generation_is_rejected_even_when_equal_to_run_generation() {
    let mut kernel = kernel();
    commit(
        &mut kernel,
        event(1, RuntimeEventName::RunStarted),
        KernelTransition::BeginRuntimeSelection,
    );
    commit(
        &mut kernel,
        event(1, RuntimeEventName::HarnessAttemptStarted),
        KernelTransition::BeginContextAssembly,
    );
    let stale_provider =
        event_with_generation(1, RuntimeEventName::ProviderAttemptStarted, generation(7));

    let error = kernel
        .prepare_transition(
            generation(7),
            &lane_authority(),
            "request.stale_provider",
            stale_provider,
            KernelTransition::BeginProviderCall,
        )
        .expect_err("run generation must not authorize provider lane");

    assert_eq!(
        error,
        KernelTransitionError::LaneGenerationMismatch {
            lane: RuntimeGenerationLane::Provider,
            active: generation(12),
            observed: generation(7),
        }
    );
}

#[test]
fn same_lane_generation_sequence_conflict_and_stale_sequence_are_rejected() {
    let mut kernel = kernel();
    advance_to_provider(&mut kernel);
    let same_sequence = event(1, RuntimeEventName::ProviderAttemptCompleted);
    let conflict = kernel
        .prepare_transition(
            generation(7),
            &lane_authority(),
            "request.same_sequence",
            same_sequence,
            KernelTransition::BeginFinalization,
        )
        .expect_err("same provider cursor sequence must conflict");
    assert!(matches!(conflict, KernelTransitionError::SequenceConflict { .. }));

    let mut lower_sequence = event(0, RuntimeEventName::ProviderAttemptCompleted);
    lower_sequence.event_id =
        RuntimeEventId::parse("event_provider_12_zero").expect("test event id is valid");
    let stale = kernel
        .prepare_transition(
            generation(7),
            &lane_authority(),
            "request.stale_sequence",
            lower_sequence,
            KernelTransition::BeginFinalization,
        )
        .expect_err("lower provider cursor sequence must be stale");
    assert!(matches!(stale, KernelTransitionError::StaleSequence { .. }));
}

#[test]
fn active_snapshot_replaces_old_lane_cursors_across_many_generation_rotations() {
    let mut kernel = kernel();
    advance_to_provider(&mut kernel);
    commit(
        &mut kernel,
        event(2, RuntimeEventName::ProviderAttemptCompleted),
        KernelTransition::BeginCompaction,
    );

    for generation_value in 13..80 {
        let provider_generation = generation(generation_value);
        let authority =
            lane_authority_with_generation(RuntimeGenerationLane::Provider, provider_generation);
        for (sequence, event_name, transition) in [
            (1, RuntimeEventName::ProviderAttemptStarted, KernelTransition::BeginProviderCall),
            (2, RuntimeEventName::ProviderAttemptCompleted, KernelTransition::BeginCompaction),
        ] {
            let prepared = kernel
                .prepare_transition(
                    generation(7),
                    &authority,
                    format!("request.rotation.{generation_value}.{sequence}").as_str(),
                    event_with_generation(sequence, event_name, provider_generation),
                    transition,
                )
                .expect("active provider generation should prepare");
            kernel = RuntimeKernelV2::restore_from_journal(prepared.next_snapshot().clone())
                .expect("committed rotation snapshot should restore");
        }
    }

    assert!(
        kernel.snapshot().event_cursor_count_for_test() <= MAX_KERNEL_EVENT_CURSORS,
        "active snapshots retain at most one cursor per generation lane"
    );
    let current = lane_authority_with_generation(RuntimeGenerationLane::Provider, generation(79));
    let stale = event_with_generation(3, RuntimeEventName::ProviderAttemptStarted, generation(78));
    assert!(matches!(
        kernel.prepare_transition(
            generation(7),
            &current,
            "request.rotation.stale",
            stale,
            KernelTransition::BeginProviderCall,
        ),
        Err(KernelTransitionError::LaneGenerationMismatch { .. })
    ));
}

#[test]
fn run_generation_mismatch_is_rejected_before_event_authority() {
    let kernel = kernel();
    let error = kernel
        .prepare_transition(
            generation(8),
            &lane_authority(),
            "request.wrong_run",
            event(1, RuntimeEventName::RunStarted),
            KernelTransition::BeginRuntimeSelection,
        )
        .expect_err("caller must hold immutable run generation");

    assert_eq!(
        error,
        KernelTransitionError::RunGenerationMismatch {
            active: generation(7),
            observed: generation(8),
        }
    );
}

#[test]
fn invalid_base_identity_with_child_fields_is_rejected() {
    let mut base = base_identities();
    base.attempt_id =
        Some(RuntimeAttemptId::parse("attempt_01").expect("test attempt id is valid"));

    assert_eq!(
        RuntimeKernelV2::admit_for_test(
            authority_decision(RuntimeKernelVersion::V2),
            base,
            admitted_run_lease(),
            1,
        )
        .expect_err("child id must fail"),
        KernelTransitionError::InvalidBaseIdentities
    );
}

#[test]
fn provider_attempt_must_match_the_selected_harness_attempt() {
    let mut kernel = kernel();
    commit(
        &mut kernel,
        event(1, RuntimeEventName::RunStarted),
        KernelTransition::BeginRuntimeSelection,
    );
    commit(
        &mut kernel,
        event(1, RuntimeEventName::HarnessAttemptStarted),
        KernelTransition::BeginContextAssembly,
    );
    let mut provider = event(1, RuntimeEventName::ProviderAttemptStarted);
    provider.identities.attempt_id =
        Some(RuntimeAttemptId::parse("attempt_other").expect("test attempt id is valid"));

    assert_child_mismatch(&kernel, provider, KernelTransition::BeginProviderCall, "attempt_id");
}

#[test]
fn mismatched_attempt_is_rejected() {
    let mut kernel = kernel();
    advance_to_provider(&mut kernel);
    let mut completed = event(2, RuntimeEventName::ProviderAttemptCompleted);
    completed.identities.attempt_id =
        Some(RuntimeAttemptId::parse("attempt_other").expect("test attempt id is valid"));

    assert_child_mismatch(&kernel, completed, KernelTransition::BeginFinalization, "attempt_id");
}

#[test]
fn mismatched_proposal_is_rejected() {
    let mut kernel = kernel();
    advance_to_tool_gate(&mut kernel);
    let mut approval = event(3, RuntimeEventName::ApprovalRequired);
    approval.identities.tool_proposal_id =
        Some(RuntimeToolProposalId::parse("proposal_other").expect("test proposal id is valid"));

    assert_child_mismatch(
        &kernel,
        approval,
        KernelTransition::BeginApprovalWait,
        "tool_proposal_id",
    );
}

#[test]
fn mismatched_approval_subject_is_rejected() {
    let mut kernel = kernel();
    advance_to_approval(&mut kernel);
    let mut resolved = event(4, RuntimeEventName::ApprovalResolved);
    resolved.identities.approval_subject_id =
        Some(RuntimeApprovalSubjectId::parse("approval_other").expect("test approval id is valid"));

    assert_child_mismatch(
        &kernel,
        resolved,
        KernelTransition::ResumeToolGate,
        "approval_subject_id",
    );
}

#[test]
fn mismatched_execution_is_rejected() {
    let mut kernel = kernel();
    advance_to_tool_execution(&mut kernel);
    let mut result = event(6, RuntimeEventName::ToolResultObserved);
    result.identities.tool_execution_id =
        Some(RuntimeToolExecutionId::parse("execution_other").expect("test execution id is valid"));

    assert_child_mismatch(
        &kernel,
        result,
        KernelTransition::BeginResultProjection,
        "tool_execution_id",
    );
}

#[test]
fn mismatched_operation_is_rejected() {
    let mut kernel = kernel();
    advance_to_tool_execution(&mut kernel);
    let mut result = event(6, RuntimeEventName::ToolResultObserved);
    result.identities.operation_id =
        Some(RuntimeOperationId::parse("operation_other").expect("test operation id is valid"));

    assert_child_mismatch(&kernel, result, KernelTransition::BeginResultProjection, "operation_id");
}

fn assert_child_mismatch(
    kernel: &RuntimeKernelV2,
    event: RuntimeEventEnvelopeV2,
    transition: KernelTransition,
    field: &'static str,
) {
    let error = kernel
        .prepare_transition(
            generation(7),
            &lane_authority(),
            "request.identity_mismatch",
            event,
            transition,
        )
        .expect_err("unrelated child identity must fail");
    assert_eq!(error, KernelTransitionError::ChildIdentityMismatch { field });
}

#[test]
fn tampered_snapshot_state_and_cursor_are_rejected_on_restore() {
    let mut kernel = kernel();
    advance_to_provider(&mut kernel);
    let mut value = serde_json::to_value(kernel.snapshot()).expect("snapshot should serialize");
    value["state"] = serde_json::json!("done");
    value["event_cursors"][0]["last_sequence"] = serde_json::json!(99);
    assert!(
        serde_json::from_value::<KernelStateSnapshot>(value).is_err(),
        "tampered snapshots must not enter the validated domain type"
    );
}

#[test]
fn deserialization_rejects_unadmitted_authority_and_lease_binding() {
    let kernel = kernel();
    let mut authority = serde_json::to_value(kernel.snapshot()).expect("snapshot should serialize");
    authority["runtime_authority_decision"]["selected_runtime"] = serde_json::json!("legacy");
    assert!(
        serde_json::from_value::<KernelStateSnapshot>(authority).is_err(),
        "authority decisions validate before entering the snapshot domain"
    );

    let mut lease = serde_json::to_value(kernel.snapshot()).expect("snapshot should serialize");
    lease["run_lease"]["owner"] = serde_json::json!("replacement_owner");
    assert!(
        serde_json::from_value::<KernelStateSnapshot>(lease).is_err(),
        "the persisted snapshot binds the exact admitted lease"
    );
}

#[test]
fn tampered_durable_outcome_is_rejected() {
    let kernel = kernel();
    let prepared = prepare(
        &kernel,
        event(1, RuntimeEventName::RunStarted),
        KernelTransition::BeginRuntimeSelection,
    );
    let mut value = serde_json::to_value(&prepared).expect("prepared record should serialize");
    value["outcome"]["next_state"] = serde_json::json!("failed");
    assert!(
        serde_json::from_value::<PreparedKernelTransition>(value).is_err(),
        "tampered prepared records must not enter the validated domain type"
    );
}

#[test]
fn durable_vector_bounds_are_enforced_during_deserialization() {
    let mut kernel = kernel();
    advance_to_provider(&mut kernel);
    let mut snapshot = serde_json::to_value(kernel.snapshot()).expect("snapshot should serialize");
    let cursor = snapshot["event_cursors"][0].clone();
    snapshot["event_cursors"] =
        serde_json::Value::Array((0..=MAX_KERNEL_EVENT_CURSORS).map(|_| cursor.clone()).collect());
    assert!(serde_json::from_value::<KernelStateSnapshot>(snapshot).is_err());

    let authority = lane_authority();
    let mut authority_json =
        serde_json::to_value(&authority).expect("lane authority should serialize");
    let lease = authority_json["leases"][0].clone();
    authority_json["leases"] =
        serde_json::Value::Array((0..=MAX_KERNEL_EVENT_CURSORS).map(|_| lease.clone()).collect());
    assert!(serde_json::from_value::<KernelLaneAuthoritySet>(authority_json).is_err());
}

#[test]
fn terminal_is_single_shot_but_exact_terminal_replay_is_duplicate() {
    let mut kernel = kernel();
    advance_to_provider(&mut kernel);
    let terminal_event = event(2, RuntimeEventName::RunFailed);
    let key = "request.terminal";
    let prepared = kernel
        .prepare_transition(
            generation(7),
            &lane_authority(),
            key,
            terminal_event.clone(),
            KernelTransition::Fail,
        )
        .expect("terminal should prepare");
    kernel = RuntimeKernelV2::restore_from_journal(prepared.next_snapshot().clone())
        .expect("terminal snapshot should restore");

    let duplicate = kernel
        .prepare_transition(
            generation(7),
            &lane_authority(),
            key,
            terminal_event,
            KernelTransition::Fail,
        )
        .expect("exact terminal replay should be duplicate");
    assert!(matches!(duplicate.outcome(), TransitionOutcome::Duplicate { .. }));

    let second = kernel
        .prepare_transition(
            generation(7),
            &lane_authority(),
            "request.second_terminal",
            event(3, RuntimeEventName::RunCancelled),
            KernelTransition::Cancel,
        )
        .expect_err("different terminal request must fail");
    assert_eq!(second, KernelTransitionError::TerminalState { state: KernelState::Failed });
}

#[test]
fn every_state_has_unique_reason_and_explicit_timeout_posture() {
    let mut reasons = BTreeSet::new();
    for state in KernelState::ALL {
        let descriptor = state.descriptor();
        assert!(reasons.insert(descriptor.reason_code()));
        match descriptor.timeout_posture() {
            KernelTimeoutPosture::None | KernelTimeoutPosture::HostDeadline => {}
            KernelTimeoutPosture::Bounded { budget_ms } => assert!(budget_ms > 0),
        }
    }
    assert_eq!(reasons.len(), KernelState::ALL.len());
}

#[test]
fn invalid_envelope_and_wrong_event_phase_fail_closed() {
    let kernel = kernel();
    let mut invalid = event(1, RuntimeEventName::RunStarted);
    invalid.phase = RuntimeErrorPhase::ProviderCall;
    assert!(matches!(
        kernel.prepare_transition(
            generation(7),
            &lane_authority(),
            "request.invalid_envelope",
            invalid,
            KernelTransition::BeginRuntimeSelection,
        ),
        Err(KernelTransitionError::InvalidEnvelope { .. })
    ));

    assert!(matches!(
        kernel.prepare_transition(
            generation(7),
            &lane_authority(),
            "request.wrong_phase",
            event(1, RuntimeEventName::ModelDelta),
            KernelTransition::BeginRuntimeSelection,
        ),
        Err(KernelTransitionError::EventPhaseMismatch { .. })
    ));
}

#[test]
fn snapshot_and_prepared_record_use_snake_case_and_validate_round_trip() {
    let kernel =
        admitted_kernel(RuntimeKernelVersion::V2Shadow).expect("test kernel should construct");
    let prepared = prepare(
        &kernel,
        event(1, RuntimeEventName::RunStarted),
        KernelTransition::BeginRuntimeSelection,
    );
    let value = serde_json::to_value(&prepared).expect("prepared record should serialize");
    assert_eq!(value["next_snapshot"]["version"], "v2_shadow");
    assert_eq!(value["next_snapshot"]["state"], "selecting_runtime");
    assert_eq!(value["outcome"]["kind"], "applied");
    assert!(prepared.idempotency_key().starts_with("request."));
    assert_eq!(prepared.request_sha256().len(), 64);

    let decoded: PreparedKernelTransition =
        serde_json::from_value(value).expect("prepared record should deserialize");
    decoded.validate().expect("round-tripped prepared record should validate");
    assert_eq!(decoded.next_snapshot().base_identities(), &base_identities());
    assert_eq!(decoded.next_snapshot().reason_code(), "runtime.kernel.selecting_runtime");
    assert_eq!(decoded.next_snapshot().version(), RuntimeKernelVersion::V2Shadow);
}
