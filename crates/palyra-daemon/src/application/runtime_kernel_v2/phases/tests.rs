// Behavioral coverage for typed phase boundaries and capability ownership.
// Tests remain a child of the parent `phases` module so they exercise the same
// private constructors and invariants as the unsplit implementation.

#[cfg(test)]
mod tests {
    use palyra_common::runtime_contracts::{
        BackpressureOverflowAction, RuntimeRunId, RuntimeSessionId, RuntimeTraceId,
        RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
    };

    use super::*;

    // These assertions guard capability shape across the split source files;
    // concatenation preserves the former single-file inspection semantics.
    const PHASE_SOURCE: &str = concat!(
        include_str!("../phases.rs"),
        include_str!("contracts.rs"),
        include_str!("lifecycle.rs"),
        include_str!("tool_authority.rs"),
        include_str!("errors.rs"),
    );

    struct NeverCancelled;

    impl KernelCancellationSignal for NeverCancelled {
        fn current_reason(&self) -> Option<CancellationReason> {
            None
        }

        fn cancelled(&self) -> KernelPhaseFuture<'_, CancellationReason> {
            Box::pin(std::future::pending())
        }
    }

    fn generation() -> RuntimeGeneration {
        RuntimeGeneration::new(7).expect("test generation is non-zero")
    }

    fn identities() -> RuntimeIdentitySetV1 {
        RuntimeIdentitySetV1::for_run(
            RuntimeTraceId::parse("trace_phase").expect("trace id"),
            RuntimeSessionId::parse("session_phase").expect("session id"),
            RuntimeRunId::parse("run_phase").expect("run id"),
            generation(),
        )
    }

    fn lane_authority<P: CanonicalPhase>() -> PhaseLaneAuthority {
        let run_lease_id = RuntimeLeaseId::parse("run_lease_phase").expect("run lease");
        let lane_lease_id = if P::LANE == RuntimeGenerationLane::Run {
            run_lease_id.clone()
        } else {
            RuntimeLeaseId::parse("child_lease_phase").expect("child lease")
        };
        let lane_generation = if P::LANE == RuntimeGenerationLane::Run {
            generation()
        } else {
            RuntimeGeneration::new(11).expect("child generation")
        };
        PhaseLaneAuthority::from_host_leases(
            RuntimeSessionId::parse("session_phase").expect("session id"),
            RuntimeRunId::parse("run_phase").expect("run id"),
            generation(),
            run_lease_id,
            P::LANE,
            lane_generation,
            lane_lease_id,
        )
    }

    fn execution<P: CanonicalPhase>() -> PhaseExecutionContext {
        let cancellation = CancellationContextV1 {
            schema_version: RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
            scope_id: RuntimeOperationId::parse("phase_scope").expect("scope id"),
            scope: P::CANCELLATION_SCOPE,
            generation: generation(),
            parent_scope_id: None,
            reason: None,
            deadline_unix_ms: Some(10_000),
            graceful_settle_ms: 100,
            hard_abort_after_ms: 1_000,
        };
        let cancellation = KernelCancellationScope::new(cancellation, Arc::new(NeverCancelled))
            .expect("cancellation scope");
        let backpressure = BackpressurePolicy {
            schema_version: RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
            capacity: 16,
            overflow_action: BackpressureOverflowAction::BlockProducer,
            preserve_terminal: true,
            preserve_approval: true,
            max_summary_bytes: 512,
        };
        PhaseExecutionContext::new(
            cancellation,
            1_000,
            backpressure,
            lane_authority::<P>(),
            P::AUTHORITY_CLASS,
            P::TRACE_POLICY,
        )
        .expect("phase controls")
    }

    fn context_ref(id: &str) -> ContextInputRef {
        ContextInputRef::from_host(
            RuntimeOperationId::parse(id).expect("context id"),
            [7; SHA256_BYTES],
        )
    }

    fn provider_request_ref(id: &str) -> ProviderRequestRef {
        ProviderRequestRef::from_host(
            RuntimeOperationId::parse(id).expect("provider request id"),
            [7; SHA256_BYTES],
        )
    }

    fn retained_proposal_ref(id: &str) -> RetainedToolProposalRef {
        RetainedToolProposalRef::from_host(
            RuntimeOperationId::parse(id).expect("proposal ref id"),
            [7; SHA256_BYTES],
        )
    }

    fn evidence_ref(id: &str) -> RedactedEvidenceRef {
        RedactedEvidenceRef::from_host(
            RuntimeOperationId::parse(id).expect("evidence id"),
            [7; SHA256_BYTES],
        )
    }

    #[test]
    fn sealed_precontext_contract_matrix_covers_every_test_only_variant() {
        for (index, origin) in [
            AdmissionOrigin::RunStream,
            AdmissionOrigin::Channel,
            AdmissionOrigin::Background,
            AdmissionOrigin::Recovery,
        ]
        .into_iter()
        .enumerate()
        {
            let input: AdmissionPhaseInput = KernelPhaseInput::new(
                identities(),
                generation(),
                execution::<AdmissionPhase>(),
                AdmissionRequest {
                    origin,
                    principal_binding_sha256: [1; SHA256_BYTES],
                    session_binding_sha256: [2; SHA256_BYTES],
                },
            )
            .expect("admission matrix input");
            assert_eq!(input.payload().origin, origin);
            assert_eq!(input.payload().principal_binding_sha256, [1; SHA256_BYTES]);
            assert_eq!(input.payload().session_binding_sha256, [2; SHA256_BYTES]);

            let (decision, reason) = if index == 3 {
                (AdmissionDecision::Rejected, KernelPhaseReason::AdmissionRejected)
            } else {
                (AdmissionDecision::Admitted, KernelPhaseReason::AdmissionGranted)
            };
            let output: AdmissionPhaseOutput =
                KernelPhaseOutput::from_input(&input, reason, decision)
                    .expect("admission matrix output");
            assert_eq!(output.reason(), reason);
            assert_eq!(output.payload(), &decision);
        }

        let selection_input: RuntimeSelectionPhaseInput = KernelPhaseInput::new(
            identities(),
            generation(),
            execution::<RuntimeSelectionPhase>(),
            RuntimeSelectionRequest {
                availability: V2RuntimeAvailability::Ready,
                progress: RuntimeAuthorityProgressEvidence::pristine(),
            },
        )
        .expect("runtime-selection matrix input");
        assert_eq!(selection_input.payload().availability, V2RuntimeAvailability::Ready);
        assert_eq!(
            selection_input.payload().progress,
            RuntimeAuthorityProgressEvidence::pristine()
        );

        for (reason, expected_code) in [
            (KernelPhaseReason::RuntimeSelected, "runtime.phase.runtime_selection.selected"),
            (KernelPhaseReason::RuntimeSelectionBlocked, "runtime.phase.runtime_selection.blocked"),
        ] {
            assert_eq!(reason.phase(), RuntimeErrorPhase::RuntimeSelection);
            assert_eq!(reason.as_str(), expected_code);
        }

        let unresolved_output: Option<RuntimeSelectionPhaseOutput> = None;
        assert!(unresolved_output.is_none());
    }

    #[test]
    fn phase_transition_carries_typed_identity_generation_and_stable_reason() {
        let input = KernelPhaseInput::<ContextAssemblyPhase, _>::new(
            identities(),
            generation(),
            execution::<ContextAssemblyPhase>(),
            ContextAssemblyRequest {
                input_manifest: context_ref("context_input"),
                max_input_tokens: 4_096,
            },
        )
        .expect("phase input");
        let output = KernelPhaseOutput::from_input(
            &input,
            KernelPhaseReason::ContextAssembled,
            ContextAssemblyResult {
                projection_id: RuntimeContextProjectionId::parse("projection_phase")
                    .expect("projection id"),
                provider_request: provider_request_ref("provider_request"),
                segment_manifest_sha256: [3; SHA256_BYTES],
                retained_token_estimate: 1_024,
            },
        )
        .expect("phase output");

        assert_eq!(output.boundary().identities(), input.boundary().identities());
        assert_eq!(output.boundary().generation(), generation());
        assert_eq!(output.reason_code(), "runtime.phase.context_assembly.completed");
    }

    #[test]
    fn phase_rejects_generation_and_reason_code_drift() {
        let newer = generation().next().expect("next generation");
        let generation_mismatch = KernelPhaseInput::<AdmissionPhase, _>::new(
            identities(),
            newer,
            execution::<AdmissionPhase>(),
            AdmissionRequest {
                origin: AdmissionOrigin::RunStream,
                principal_binding_sha256: [1; SHA256_BYTES],
                session_binding_sha256: [2; SHA256_BYTES],
            },
        );
        assert!(matches!(generation_mismatch, Err(KernelPhaseContractError::GenerationMismatch)));

        let input = KernelPhaseInput::<AdmissionPhase, _>::new(
            identities(),
            generation(),
            execution::<AdmissionPhase>(),
            AdmissionRequest {
                origin: AdmissionOrigin::RunStream,
                principal_binding_sha256: [1; SHA256_BYTES],
                session_binding_sha256: [2; SHA256_BYTES],
            },
        )
        .expect("admission input");
        assert!(matches!(
            KernelPhaseOutput::from_input(
                &input,
                KernelPhaseReason::ToolGateGranted,
                AdmissionDecision::Admitted,
            ),
            Err(KernelPhaseContractError::ReasonPhaseMismatch { .. })
        ));
    }

    #[test]
    fn phase_rejects_lane_mismatch_and_stale_run_lease_binding() {
        let lane_mismatch = KernelPhaseInput::<ProviderCallPhase, _>::new(
            identities(),
            generation(),
            execution::<ToolExecutionPhase>(),
            ProviderCallRequest {
                context_projection_id: RuntimeContextProjectionId::parse("projection_phase")
                    .expect("projection id"),
                provider_request: provider_request_ref("provider_request"),
            },
        );
        assert!(matches!(
            lane_mismatch,
            Err(KernelPhaseContractError::LaneMismatch {
                expected: RuntimeGenerationLane::Provider,
                observed: RuntimeGenerationLane::Tool,
            })
        ));

        let mut stale_run_execution = execution::<AdmissionPhase>();
        stale_run_execution.lane_authority.run_lease_id =
            RuntimeLeaseId::parse("stale_run_lease").expect("stale lease");
        let stale_run = KernelPhaseInput::<AdmissionPhase, _>::new(
            identities(),
            generation(),
            stale_run_execution,
            AdmissionRequest {
                origin: AdmissionOrigin::RunStream,
                principal_binding_sha256: [1; SHA256_BYTES],
                session_binding_sha256: [2; SHA256_BYTES],
            },
        );
        assert!(matches!(stale_run, Err(KernelPhaseContractError::InvalidRunLaneBinding)));
    }

    #[test]
    fn tool_execution_request_requires_exact_run_and_tool_lane_grant() {
        let proposal_id = RuntimeToolProposalId::parse("proposal_phase").expect("proposal id");
        let execution_id = RuntimeToolExecutionId::parse("execution_phase").expect("execution id");
        let tool_lane = lane_authority::<ToolExecutionPhase>();
        let grant = GrantedToolAuthority::issue_noninteractive(
            proposal_id.clone(),
            execution_id.clone(),
            tool_lane.clone(),
            retained_proposal_ref("retained_proposal"),
            ToolAuthorityClass::Mutation,
            evidence_ref("grant_evidence"),
        )
        .expect("known Tool lane grant");
        let request = ToolExecutionRequest::new(grant, &tool_lane).expect("matching grant");

        assert_eq!(request.proposal_id(), &proposal_id);
        assert_eq!(request.execution_id(), &execution_id);
        assert_eq!(request.run_generation(), generation());
        assert_eq!(request.lane_authority(), &tool_lane);
    }

    #[test]
    fn tool_execution_rejects_cross_run_or_stale_lane_grant() {
        let grant_lane = lane_authority::<ToolExecutionPhase>();
        let grant = GrantedToolAuthority::issue_noninteractive(
            RuntimeToolProposalId::parse("proposal_cross_run").expect("proposal id"),
            RuntimeToolExecutionId::parse("execution_cross_run").expect("execution id"),
            grant_lane,
            retained_proposal_ref("retained_cross_run"),
            ToolAuthorityClass::ReadOnly,
            evidence_ref("grant_cross_run"),
        )
        .expect("grant");
        let mut other_run_lane = lane_authority::<ToolExecutionPhase>();
        other_run_lane.run_id = RuntimeRunId::parse("run_other").expect("other run");

        assert!(matches!(
            ToolExecutionRequest::new(grant, &other_run_lane),
            Err(KernelPhaseContractError::ToolAuthorityBindingMismatch)
        ));
    }

    #[test]
    fn pending_approval_has_no_execution_id_and_preserves_all_authority_bindings() {
        let proposal_id = RuntimeToolProposalId::parse("proposal_pending").expect("proposal id");
        let retained = retained_proposal_ref("retained_pending");
        let subject = ApprovalSubjectBinding::from_host(
            RuntimeApprovalSubjectId::parse("approval_pending").expect("approval subject"),
            *retained.sha256(),
            ToolAuthorityClass::ExternalEffect,
        );
        let pending = PendingToolAuthority::issue(
            proposal_id.clone(),
            lane_authority::<ApprovalWaitPhase>(),
            retained.clone(),
            ToolAuthorityClass::ExternalEffect,
            subject.clone(),
        )
        .expect("pending authority");
        assert_eq!(pending.run_generation(), generation());
        let execution_id =
            RuntimeToolExecutionId::parse("execution_after_approval").expect("execution id");
        let grant = GrantedToolAuthority::from_approved_pending(
            pending,
            execution_id.clone(),
            evidence_ref("approval_grant"),
        )
        .expect("approved grant");

        assert_eq!(grant.proposal_id(), &proposal_id);
        assert_eq!(grant.execution_id(), &execution_id);
        assert_eq!(grant.run_generation(), generation());
        assert_eq!(grant.retained_proposal(), &retained);
        assert_eq!(grant.authority_class(), ToolAuthorityClass::ExternalEffect);
        assert_eq!(grant.approval_subject(), Some(&subject));

        let pending_fields = PHASE_SOURCE
            .split("pub(crate) struct PendingToolAuthority {")
            .nth(1)
            .and_then(|tail| tail.split("\n}").next())
            .expect("pending declaration");
        assert!(!pending_fields.contains("execution_id"));
    }

    #[test]
    fn unknown_tool_authority_is_never_executable_or_pending() {
        let retained = retained_proposal_ref("retained_unknown");
        let subject = ApprovalSubjectBinding::from_host(
            RuntimeApprovalSubjectId::parse("approval_unknown").expect("approval subject"),
            *retained.sha256(),
            ToolAuthorityClass::Unknown,
        );
        assert!(matches!(
            PendingToolAuthority::issue(
                RuntimeToolProposalId::parse("proposal_unknown_pending").expect("proposal id"),
                lane_authority::<ApprovalWaitPhase>(),
                retained.clone(),
                ToolAuthorityClass::Unknown,
                subject,
            ),
            Err(KernelPhaseContractError::UnknownToolAuthority)
        ));
        assert!(matches!(
            GrantedToolAuthority::issue_noninteractive(
                RuntimeToolProposalId::parse("proposal_unknown_grant").expect("proposal id"),
                RuntimeToolExecutionId::parse("execution_unknown").expect("execution id"),
                lane_authority::<ToolExecutionPhase>(),
                retained,
                ToolAuthorityClass::Unknown,
                evidence_ref("unknown_evidence"),
            ),
            Err(KernelPhaseContractError::UnknownToolAuthority)
        ));
    }

    #[test]
    fn raw_tool_outcome_has_no_infallible_model_projection_conversion() {
        let from_impl =
            ["impl From<RawToolExecutionOutcomeRef>", " for ModelVisibleToolResultRef"].concat();
        let into_impl =
            ["impl Into<ModelVisibleToolResultRef>", " for RawToolExecutionOutcomeRef"].concat();

        assert!(!PHASE_SOURCE.contains(from_impl.as_str()));
        assert!(!PHASE_SOURCE.contains(into_impl.as_str()));
    }

    #[test]
    fn authority_tokens_are_not_cloneable_or_publicly_constructible() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<GrantedToolAuthority>();
        assert_sync::<GrantedToolAuthority>();
        assert_send::<PendingToolAuthority>();
        assert_sync::<PendingToolAuthority>();

        let grant = PHASE_SOURCE
            .split("pub(crate) struct GrantedToolAuthority {")
            .nth(1)
            .and_then(|tail| tail.split("\n}").next())
            .expect("grant declaration");
        assert!(grant.contains("proposal_id: RuntimeToolProposalId"));
        assert!(grant.contains("execution_id: RuntimeToolExecutionId"));
        assert!(grant.contains("lane_authority: PhaseLaneAuthority"));
        let clone_impl = ["impl Clone", " for GrantedToolAuthority"].concat();
        let copy_impl = ["impl Copy", " for GrantedToolAuthority"].concat();
        assert!(!PHASE_SOURCE.contains(clone_impl.as_str()));
        assert!(!PHASE_SOURCE.contains(copy_impl.as_str()));
    }
}
