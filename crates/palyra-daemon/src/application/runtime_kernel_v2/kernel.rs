// Immutable transition preparation and canonical request evidence.

/// Immutable evaluator for one persisted Run-generation snapshot.
#[derive(Debug, Clone)]
pub(crate) struct RuntimeKernelV2 {
    snapshot: KernelStateSnapshot,
}

impl RuntimeKernelV2 {
    /// Admits one persisted runtime-authority decision and exact active Run lease.
    ///
    /// # Errors
    /// Returns [`KernelTransitionError`] when the decision does not select an
    /// allowed V2 posture or decision, identity, lease, generation, and
    /// admission-time evidence do not describe the same active run.
    pub(crate) fn admit(
        runtime_authority_decision: RuntimeAuthorityDecisionV1,
        base_identities: RuntimeIdentitySetV1,
        run_lease: GenerationLeaseV1,
        admitted_at_unix_ms: i64,
    ) -> Result<Self, KernelTransitionError> {
        Ok(Self {
            snapshot: KernelStateSnapshot::initial(
                runtime_authority_decision,
                base_identities,
                run_lease,
                admitted_at_unix_ms,
            )?,
        })
    }

    /// Constructs an admitted kernel for crate integration fixtures.
    ///
    /// Production code cannot access this helper; tests must still provide the
    /// same validated decision, identity, lease, and admission-time evidence.
    #[cfg(test)]
    pub(crate) fn admit_for_test(
        runtime_authority_decision: RuntimeAuthorityDecisionV1,
        base_identities: RuntimeIdentitySetV1,
        run_lease: GenerationLeaseV1,
        admitted_at_unix_ms: i64,
    ) -> Result<Self, KernelTransitionError> {
        Self::admit(runtime_authority_decision, base_identities, run_lease, admitted_at_unix_ms)
    }

    /// Reconstructs an immutable kernel from a journal-restored snapshot.
    ///
    /// A host uses this only after the journal atomically commits a prepared
    /// transition. Calling it directly does not grant generation authority.
    ///
    /// # Errors
    /// Returns [`KernelTransitionError::InvalidSnapshot`] when durable state
    /// does not satisfy all kernel invariants.
    pub(crate) fn restore_from_journal(
        snapshot: KernelStateSnapshot,
    ) -> Result<Self, KernelTransitionError> {
        snapshot.validate()?;
        Ok(Self { snapshot })
    }

    /// Returns the current immutable snapshot.
    #[must_use]
    pub(crate) fn snapshot(&self) -> &KernelStateSnapshot {
        &self.snapshot
    }

    /// Evaluates one request without changing the in-memory kernel.
    ///
    /// The returned record contains both snapshots and all canonical request
    /// evidence needed for one future atomic journal transaction. Exact replay
    /// covers only the last committed transition; the journal owns full-history
    /// idempotency lookup.
    ///
    /// # Errors
    /// Returns [`KernelTransitionError`] for invalid authority, envelope,
    /// identity correlation, cursor ordering, transition, terminal mutation,
    /// idempotency key, or current snapshot.
    pub(crate) fn prepare_transition(
        &self,
        expected_run_generation: RuntimeGeneration,
        lane_authority: &KernelLaneAuthoritySet,
        idempotency_key: &str,
        event: RuntimeEventEnvelopeV2,
        transition: KernelTransition,
    ) -> Result<PreparedKernelTransition, KernelTransitionError> {
        self.snapshot.validate()?;
        if expected_run_generation != self.snapshot.run_generation() {
            return Err(KernelTransitionError::RunGenerationMismatch {
                active: self.snapshot.run_generation(),
                observed: expected_run_generation,
            });
        }
        lane_authority.validate(&self.snapshot.base_identities)?;
        lane_authority.validate_admitted_run_lease(
            &self.snapshot.base_identities,
            &self.snapshot.run_lease,
        )?;
        if !is_idempotency_key(idempotency_key) {
            return Err(KernelTransitionError::InvalidIdempotencyKey);
        }
        event.validate().map_err(|source| KernelTransitionError::InvalidEnvelope { source })?;
        validate_event_base_identity(&self.snapshot.base_identities, &event)?;

        let lane = event.event_name.descriptor().generation_lane;
        let active_lane_generation = lane_authority
            .generation(lane)
            .ok_or(KernelTransitionError::MissingLaneAuthority { lane })?;
        if event.identities.generation != active_lane_generation {
            return Err(KernelTransitionError::LaneGenerationMismatch {
                lane,
                active: active_lane_generation,
                observed: event.identities.generation,
            });
        }
        if lane == RuntimeGenerationLane::Run
            && event.identities.generation != self.snapshot.run_generation()
        {
            return Err(KernelTransitionError::RunGenerationMismatch {
                active: self.snapshot.run_generation(),
                observed: event.identities.generation,
            });
        }

        let request_sha256 = request_sha256(idempotency_key, &event, transition)?;
        if self
            .snapshot
            .last_transition
            .as_ref()
            .is_some_and(|last| last.event.event_id == event.event_id)
        {
            if last_request_matches(
                &self.snapshot,
                idempotency_key,
                request_sha256.as_str(),
                &event,
                transition,
            ) {
                let outcome = TransitionOutcome::Duplicate {
                    state: self.snapshot.state,
                    reason_code: "runtime.kernel.transition.duplicate".to_owned(),
                };
                let prepared = PreparedKernelTransition {
                    schema_version: PREPARED_KERNEL_TRANSITION_SCHEMA_VERSION,
                    previous_snapshot: self.snapshot.clone(),
                    next_snapshot: self.snapshot.clone(),
                    idempotency_key: idempotency_key.to_owned(),
                    request_sha256,
                    lane_authority: lane_authority.clone(),
                    event,
                    transition,
                    outcome,
                };
                prepared.validate()?;
                return Ok(prepared);
            }
            return Err(KernelTransitionError::EventIdConflict);
        }

        validate_cursor_order(&self.snapshot, &event)?;
        if self.snapshot.state.is_terminal() {
            return Err(KernelTransitionError::TerminalState { state: self.snapshot.state });
        }
        let next_snapshot = derive_next_snapshot(
            &self.snapshot,
            &event,
            transition,
            idempotency_key,
            request_sha256.as_str(),
            lane_authority,
        )?;
        let outcome = TransitionOutcome::Applied {
            previous_state: self.snapshot.state,
            next_state: next_snapshot.state,
            reason_code: transition.reason_code().to_owned(),
        };
        let prepared = PreparedKernelTransition {
            schema_version: PREPARED_KERNEL_TRANSITION_SCHEMA_VERSION,
            previous_snapshot: self.snapshot.clone(),
            next_snapshot,
            idempotency_key: idempotency_key.to_owned(),
            request_sha256,
            lane_authority: lane_authority.clone(),
            event,
            transition,
            outcome,
        };
        prepared.validate()?;
        Ok(prepared)
    }
}

fn validate_cursor_order(
    snapshot: &KernelStateSnapshot,
    event: &RuntimeEventEnvelopeV2,
) -> Result<(), KernelTransitionError> {
    let lane = event.event_name.descriptor().generation_lane;
    let generation = event.identities.generation;
    let Some(cursor) = snapshot.cursor(lane, generation) else {
        return Ok(());
    };
    if event.sequence < cursor.last_sequence {
        return Err(KernelTransitionError::StaleSequence {
            lane,
            generation,
            last: cursor.last_sequence,
            observed: event.sequence,
        });
    }
    if event.sequence == cursor.last_sequence {
        return Err(KernelTransitionError::SequenceConflict {
            lane,
            generation,
            sequence: event.sequence,
        });
    }
    Ok(())
}

fn derive_next_snapshot(
    previous: &KernelStateSnapshot,
    event: &RuntimeEventEnvelopeV2,
    transition: KernelTransition,
    idempotency_key: &str,
    request_sha256: &str,
    lane_authority: &KernelLaneAuthoritySet,
) -> Result<KernelStateSnapshot, KernelTransitionError> {
    if !phase_authorizes(transition, event.phase) {
        return Err(KernelTransitionError::EventPhaseMismatch {
            state: previous.state,
            transition,
            observed: event.phase,
        });
    }
    if !edge_is_allowed(previous.state, transition, event.event_name) {
        return Err(KernelTransitionError::InvalidTransition {
            state: previous.state,
            transition,
            event_name: event.event_name,
        });
    }
    let correlations =
        derive_correlations(previous.state, &previous.correlations, event, transition)?;
    let event_sha256 = event_sha256(event)?;
    let mut next = previous.clone();
    update_cursor(&mut next.event_cursors, event, event_sha256.as_str())?;
    next.revision =
        previous.revision.checked_add(1).ok_or(KernelTransitionError::RevisionExhausted)?;
    let target = transition.target_state();
    let descriptor = target.descriptor();
    next.state = target;
    next.reason_code = descriptor.reason_code().to_owned();
    next.timeout_posture = descriptor.timeout_posture();
    next.correlations = correlations;
    next.last_transition = Some(KernelTransitionEvidence {
        previous_revision: previous.revision,
        previous_state: previous.state,
        previous_correlations: previous.correlations.clone(),
        transition,
        transition_reason_code: transition.reason_code().to_owned(),
        idempotency_key: idempotency_key.to_owned(),
        request_sha256: request_sha256.to_owned(),
        event_sha256,
        lane_authority: lane_authority.clone(),
        event: event.clone(),
    });
    next.terminal_outcome = KernelTerminalOutcome::from_state(target);
    Ok(next)
}

fn update_cursor(
    cursors: &mut Vec<KernelEventCursor>,
    event: &RuntimeEventEnvelopeV2,
    event_sha256: &str,
) -> Result<(), KernelTransitionError> {
    let lane = event.event_name.descriptor().generation_lane;
    let generation = event.identities.generation;
    if let Some(cursor) = cursors.iter_mut().find(|cursor| cursor.lane == lane) {
        cursor.generation = generation;
        cursor.last_sequence = event.sequence;
        cursor.last_event_id = event.event_id.clone();
        cursor.last_event_sha256 = event_sha256.to_owned();
        return Ok(());
    }
    if cursors.len() >= MAX_KERNEL_EVENT_CURSORS {
        return Err(KernelTransitionError::EventCursorLimit);
    }
    cursors.push(KernelEventCursor {
        lane,
        generation,
        last_sequence: event.sequence,
        last_event_id: event.event_id.clone(),
        last_event_sha256: event_sha256.to_owned(),
    });
    cursors.sort_by_key(|cursor| (cursor.lane, cursor.generation));
    Ok(())
}

fn derive_correlations(
    state: KernelState,
    previous: &KernelChildCorrelationState,
    event: &RuntimeEventEnvelopeV2,
    transition: KernelTransition,
) -> Result<KernelChildCorrelationState, KernelTransitionError> {
    let mut next = previous.clone();
    match transition {
        KernelTransition::BeginContextAssembly => {
            let attempt_id = required_identity(event.identities.attempt_id.as_ref(), "attempt_id")?;
            next.active_attempt_id = Some(attempt_id.clone());
        }
        KernelTransition::BeginProviderCall => {
            require_equal(
                event.identities.attempt_id.as_ref(),
                next.active_attempt_id.as_ref(),
                "attempt_id",
            )?;
            next.tool_proposal_id = None;
            next.approval_subject_id = None;
            next.tool_execution_id = None;
            next.tool_operation_id = None;
        }
        KernelTransition::BeginToolGate => {
            require_equal(
                event.identities.attempt_id.as_ref(),
                next.active_attempt_id.as_ref(),
                "attempt_id",
            )?;
            let proposal_id =
                required_identity(event.identities.tool_proposal_id.as_ref(), "tool_proposal_id")?;
            next.tool_proposal_id = Some(proposal_id.clone());
            next.approval_subject_id = None;
            next.tool_execution_id = None;
            next.tool_operation_id = None;
        }
        KernelTransition::BeginApprovalWait => {
            require_equal(
                event.identities.tool_proposal_id.as_ref(),
                next.tool_proposal_id.as_ref(),
                "tool_proposal_id",
            )?;
            let approval_id = required_identity(
                event.identities.approval_subject_id.as_ref(),
                "approval_subject_id",
            )?;
            next.approval_subject_id = Some(approval_id.clone());
        }
        KernelTransition::ResumeToolGate => {
            require_equal(
                event.identities.tool_proposal_id.as_ref(),
                next.tool_proposal_id.as_ref(),
                "tool_proposal_id",
            )?;
            require_equal(
                event.identities.approval_subject_id.as_ref(),
                next.approval_subject_id.as_ref(),
                "approval_subject_id",
            )?;
        }
        KernelTransition::ResolveToolWithoutExecution => {
            require_equal(
                event.identities.tool_proposal_id.as_ref(),
                next.tool_proposal_id.as_ref(),
                "tool_proposal_id",
            )?;
            next.tool_execution_id = None;
            next.tool_operation_id = None;
        }
        KernelTransition::BeginToolExecution => {
            require_equal(
                event.identities.tool_proposal_id.as_ref(),
                next.tool_proposal_id.as_ref(),
                "tool_proposal_id",
            )?;
            let execution_id = required_identity(
                event.identities.tool_execution_id.as_ref(),
                "tool_execution_id",
            )?;
            let operation_id =
                required_identity(event.identities.operation_id.as_ref(), "operation_id")?;
            next.tool_execution_id = Some(execution_id.clone());
            next.tool_operation_id = Some(operation_id.clone());
        }
        KernelTransition::BeginResultProjection if state == KernelState::ExecutingTool => {
            require_equal(
                event.identities.tool_proposal_id.as_ref(),
                next.tool_proposal_id.as_ref(),
                "tool_proposal_id",
            )?;
            require_equal(
                event.identities.tool_execution_id.as_ref(),
                next.tool_execution_id.as_ref(),
                "tool_execution_id",
            )?;
            require_equal(
                event.identities.operation_id.as_ref(),
                next.tool_operation_id.as_ref(),
                "operation_id",
            )?;
        }
        KernelTransition::BeginResultProjection => {
            require_equal(
                event.identities.tool_execution_id.as_ref(),
                next.tool_execution_id.as_ref(),
                "tool_execution_id",
            )?;
            require_equal(
                event.identities.operation_id.as_ref(),
                next.tool_operation_id.as_ref(),
                "operation_id",
            )?;
        }
        KernelTransition::BeginCompaction | KernelTransition::BeginFinalization => {
            require_equal(
                event.identities.attempt_id.as_ref(),
                next.active_attempt_id.as_ref(),
                "attempt_id",
            )?;
        }
        KernelTransition::BeginDeliveryWait => {
            let delivery_id = required_identity(
                event.identities.delivery_intent_id.as_ref(),
                "delivery_intent_id",
            )?;
            let operation_id =
                required_identity(event.identities.operation_id.as_ref(), "operation_id")?;
            next.delivery_intent_id = Some(delivery_id.clone());
            next.delivery_operation_id = Some(operation_id.clone());
        }
        KernelTransition::BeginRuntimeSelection
        | KernelTransition::Complete
        | KernelTransition::Fail
        | KernelTransition::Cancel
        | KernelTransition::Suspend
        | KernelTransition::BeginRecovery
        | KernelTransition::ResumeRuntimeSelection => {}
    }
    next.validate().map_err(|_| KernelTransitionError::InvalidSnapshot)?;
    Ok(next)
}

fn required_identity<'a, T>(
    observed: Option<&'a T>,
    field: &'static str,
) -> Result<&'a T, KernelTransitionError> {
    observed.ok_or(KernelTransitionError::ChildIdentityMismatch { field })
}

fn require_equal<T: PartialEq>(
    observed: Option<&T>,
    expected: Option<&T>,
    field: &'static str,
) -> Result<(), KernelTransitionError> {
    if observed.is_some() && observed == expected {
        Ok(())
    } else {
        Err(KernelTransitionError::ChildIdentityMismatch { field })
    }
}

fn last_request_matches(
    snapshot: &KernelStateSnapshot,
    idempotency_key: &str,
    request_sha256: &str,
    event: &RuntimeEventEnvelopeV2,
    transition: KernelTransition,
) -> bool {
    snapshot.last_transition.as_ref().is_some_and(|last| {
        last.idempotency_key == idempotency_key
            && last.request_sha256 == request_sha256
            && last.event == *event
            && last.transition == transition
    })
}

#[derive(Serialize)]
struct KernelRequestDigestMaterial<'a> {
    schema_version: u32,
    idempotency_key: &'a str,
    transition: KernelTransition,
    transition_reason_code: &'static str,
    event: &'a RuntimeEventEnvelopeV2,
}

fn request_sha256(
    idempotency_key: &str,
    event: &RuntimeEventEnvelopeV2,
    transition: KernelTransition,
) -> Result<String, KernelTransitionError> {
    let material = KernelRequestDigestMaterial {
        schema_version: PREPARED_KERNEL_TRANSITION_SCHEMA_VERSION,
        idempotency_key,
        transition,
        transition_reason_code: transition.reason_code(),
        event,
    };
    digest_serializable(b"palyra.runtime.kernel.request.v1\0", &material)
}

fn event_sha256(event: &RuntimeEventEnvelopeV2) -> Result<String, KernelTransitionError> {
    digest_serializable(b"palyra.runtime.kernel.event.v1\0", event)
}

fn digest_serializable(
    domain: &[u8],
    value: &impl Serialize,
) -> Result<String, KernelTransitionError> {
    let bytes = serde_json::to_vec(value).map_err(|_| KernelTransitionError::RequestDigest)?;
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(bytes);
    Ok(hex::encode(digest.finalize()))
}

fn is_idempotency_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDEMPOTENCY_KEY_BYTES
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
