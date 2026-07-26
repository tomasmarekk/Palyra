//! Durable host compaction phase adapter.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use palyra_common::runtime_contracts::{RuntimeContextProjectionId, RuntimeErrorPhase};

use super::ProductionPayloadRetention;
use crate::{
    application::{
        context_compaction::{
            context_compaction_owner_registry, evaluate_compaction_quality,
            ContextCompactionPlanV2, ContextProtectedSegment,
        },
        context_engine::{
            ContextEngineCompactionDisposition, ContextEngineCompactionRequest,
            ContextEngineRegistry,
        },
        runtime_kernel_v2::phases::{
            CompactionPhase, CompactionRequest, CompactionResult, KernelPhaseError,
            KernelPhaseFuture, KernelPhaseOutput, KernelPhaseReason, RuntimePhaseService,
        },
        session_compaction::{
            apply_session_compaction, preview_session_compaction, CompactionSafeguardDecision,
            SessionCompactionApplyRequest,
        },
    },
    gateway::GatewayRuntimeState,
    journal::OrchestratorSessionRecord,
    transport::grpc::auth::RequestContext,
};

pub(crate) enum CompactionHostOutcome {
    Applied { context_projection_id: RuntimeContextProjectionId, evidence_material: Vec<u8> },
}

/// Host-retained material needed to rematerialize the provider request after
/// durable session compaction.
///
/// This payload never crosses the kernel contract. The kernel receives only
/// the successor projection identity and redacted evidence reference.
#[derive(Debug, Clone)]
pub(crate) struct AppliedCompactionProjection {
    pub(crate) artifact_id: String,
    pub(crate) mode: String,
    pub(crate) trigger_reason: String,
    pub(crate) summary_text: String,
}

/// One-shot handoff from the durable compaction service to the host callback.
#[derive(Default)]
pub(crate) struct RunStreamCompactionProjectionStore {
    projection: Mutex<Option<AppliedCompactionProjection>>,
}

impl RunStreamCompactionProjectionStore {
    fn retain(&self, projection: AppliedCompactionProjection) -> Result<(), &'static str> {
        let mut slot = self
            .projection
            .lock()
            .map_err(|_| "runtime.compaction.projection_store_unavailable")?;
        if slot.is_some() {
            return Err("runtime.compaction.projection_already_retained");
        }
        *slot = Some(projection);
        Ok(())
    }

    /// Consumes the exact applied projection once.
    pub(crate) fn take(&self) -> Option<AppliedCompactionProjection> {
        self.projection.lock().ok()?.take()
    }
}

pub(crate) trait RetainedCompactionWork: Send + Sync {
    fn evidence_material(&self) -> &[u8];

    fn compact(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<CompactionHostOutcome, &'static str>> + Send>>;
}

/// Owned durable compaction request over the existing session-compaction service.
pub(crate) struct RunStreamCompactionInput {
    runtime_state: Arc<GatewayRuntimeState>,
    session: OrchestratorSessionRecord,
    actor_principal: String,
    request_context: RequestContext,
    run_id: String,
    manifest_material: Vec<u8>,
    projections: Arc<RunStreamCompactionProjectionStore>,
    plan: ContextCompactionPlanV2,
}

fn observed_protected_segments(
    safeguard_passed: bool,
    pending_approval_open: bool,
    active_objective_present: bool,
    active_goal_preserved: bool,
    tool_pair_intact: bool,
    provenance_present: bool,
) -> Vec<ContextProtectedSegment> {
    let mut observed = vec![
        ContextProtectedSegment::SystemInstructions,
        ContextProtectedSegment::SafetyInstructions,
    ];
    if safeguard_passed && !pending_approval_open {
        observed.push(ContextProtectedSegment::UnresolvedApproval);
    }
    if safeguard_passed && (!active_objective_present || active_goal_preserved) {
        observed.push(ContextProtectedSegment::ActiveObjective);
    }
    if safeguard_passed && tool_pair_intact {
        observed.push(ContextProtectedSegment::SideEffectFence);
        observed.push(ContextProtectedSegment::ToolCallResultPair);
    }
    if safeguard_passed && provenance_present {
        observed.push(ContextProtectedSegment::CitationProvenance);
    }
    observed
}

fn observed_protected_segments_for_plan(
    plan: &crate::application::session_compaction::SessionCompactionPlan,
) -> Vec<ContextProtectedSegment> {
    observed_protected_segments(
        plan.safeguard.decision == CompactionSafeguardDecision::Passed,
        plan.safeguard.pre_checkpoint.pending_approval_open,
        plan.safeguard.pre_checkpoint.active_objective.is_some(),
        !plan.active_task_summary.active_goal.trim().is_empty(),
        plan.successor_transcript.split_guard.tool_pair_intact,
        !plan.evidence_refs.is_empty(),
    )
}

impl RunStreamCompactionInput {
    pub(crate) fn new(
        runtime_state: Arc<GatewayRuntimeState>,
        session: OrchestratorSessionRecord,
        actor_principal: String,
        request_context: RequestContext,
        run_id: String,
        projections: Arc<RunStreamCompactionProjectionStore>,
        plan: ContextCompactionPlanV2,
    ) -> Self {
        let manifest_material = serde_json::to_vec(&serde_json::json!({
            "session_id": session.session_id.as_str(),
            "run_id": run_id.as_str(),
            "mode": "automatic",
            "policy": "runtime_kernel_v2",
            "context_compaction_plan": &plan,
        }))
        .unwrap_or_default();
        Self {
            runtime_state,
            session,
            actor_principal,
            request_context,
            run_id,
            manifest_material,
            projections,
            plan,
        }
    }
}

impl RetainedCompactionWork for RunStreamCompactionInput {
    fn evidence_material(&self) -> &[u8] {
        self.manifest_material.as_slice()
    }

    fn compact(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<CompactionHostOutcome, &'static str>> + Send>> {
        Box::pin(async move {
            let _owner = context_compaction_owner_registry()
                .acquire(self.session.session_id.as_str(), &self.plan)?;
            let engine = ContextEngineRegistry::production_default().selected_engine();
            let engine_outcome = engine
                .compact_if_needed(
                    &self.runtime_state,
                    &self.request_context,
                    ContextEngineCompactionRequest {
                        run_id: self.run_id.as_str(),
                        session_id: self.session.session_id.as_str(),
                    },
                )
                .await
                .unwrap_or_else(|_| {
                    crate::application::context_engine::ContextEngineCompactionOutcome {
                        disposition: ContextEngineCompactionDisposition::HostPlanRequested,
                        reason_code: "context.compaction.engine_failed_safe_fallback".to_owned(),
                    }
                });
            match engine_outcome.disposition {
                ContextEngineCompactionDisposition::HostPlanRequested => {}
                ContextEngineCompactionDisposition::NotNeeded => {
                    return Err("runtime.compaction.engine_declined");
                }
                ContextEngineCompactionDisposition::Applied => {
                    return Err("runtime.compaction.engine_direct_apply_denied");
                }
            }
            let preview = preview_session_compaction(
                &self.runtime_state,
                &self.session,
                Some("runtime_kernel_v2_context_pressure"),
                Some("runtime_kernel_v2"),
                None,
            )
            .await
            .map_err(|_| "runtime.compaction.preflight_failed")?;
            if !preview.eligible {
                return Err("runtime.compaction.preflight_ineligible");
            }
            let preview_observed = observed_protected_segments_for_plan(&preview);
            let preview_quality = evaluate_compaction_quality(
                &self.plan,
                preview.estimated_input_tokens,
                preview.estimated_output_tokens,
                preview_observed.as_slice(),
            );
            if !preview_quality.accepted {
                return Err("runtime.compaction.preflight_quality_gate_failed");
            }
            let execution = apply_session_compaction(SessionCompactionApplyRequest {
                runtime_state: &self.runtime_state,
                session: &self.session,
                actor_principal: self.actor_principal.as_str(),
                run_id: Some(self.run_id.as_str()),
                usage_observation_run_id: Some(self.run_id.as_str()),
                mode: "automatic",
                trigger_reason: Some("runtime_kernel_v2_context_pressure"),
                trigger_policy: Some("runtime_kernel_v2"),
                operator_instruction: None,
                accept_candidate_ids: &[],
                reject_candidate_ids: &[],
            })
            .await
            .map_err(|_| "runtime.compaction.host_failed")?;
            let observed = observed_protected_segments_for_plan(&execution.plan);
            let quality = evaluate_compaction_quality(
                &self.plan,
                execution.artifact.estimated_input_tokens,
                execution.artifact.estimated_output_tokens,
                observed.as_slice(),
            );
            if !quality.accepted {
                return Err("runtime.compaction.quality_gate_failed");
            }
            self.projections.retain(AppliedCompactionProjection {
                artifact_id: execution.artifact.artifact_id.clone(),
                mode: execution.artifact.mode.clone(),
                trigger_reason: execution.artifact.trigger_reason.clone(),
                summary_text: execution.artifact.summary_text.clone(),
            })?;
            let evidence_material = serde_json::to_vec(&serde_json::json!({
                "artifact_id": execution.artifact.artifact_id,
                "pre_checkpoint_id": execution.pre_checkpoint.checkpoint_id,
                "post_checkpoint_id": execution.post_checkpoint.checkpoint_id,
                "estimated_input_tokens": execution.artifact.estimated_input_tokens,
                "estimated_output_tokens": execution.artifact.estimated_output_tokens,
                "write_count": execution.writes.len(),
                "context_compaction_plan": &self.plan,
                "context_engine_reason_code": engine_outcome.reason_code,
                "quality_gate": quality,
            }))
            .map_err(|_| "runtime.compaction.evidence_invalid")?;
            Ok(CompactionHostOutcome::Applied {
                context_projection_id: super::new_projection_id(),
                evidence_material,
            })
        })
    }
}

pub(crate) struct ProductionCompactionService {
    retention: Arc<ProductionPayloadRetention>,
}

impl ProductionCompactionService {
    pub(crate) fn new(retention: Arc<ProductionPayloadRetention>) -> Self {
        Self { retention }
    }
}

impl RuntimePhaseService<CompactionPhase, CompactionRequest, CompactionResult>
    for ProductionCompactionService
{
    fn execute(
        &self,
        input: crate::application::runtime_kernel_v2::phases::CompactionPhaseInput,
    ) -> KernelPhaseFuture<
        '_,
        Result<
            crate::application::runtime_kernel_v2::phases::CompactionPhaseOutput,
            KernelPhaseError,
        >,
    > {
        Box::pin(async move {
            if input.boundary().execution().cancellation().signal().current_reason().is_some() {
                return Err(host_error(
                    &self.retention,
                    "runtime.compaction.cancelled_before_start",
                ));
            }
            let work = self
                .retention
                .compaction_work(&input.payload().pressure_manifest)
                .ok_or_else(|| host_error(&self.retention, "runtime.compaction.missing_input"))?;
            let mut compaction = tokio::spawn(work.compact());
            let deadline = tokio::time::sleep(Duration::from_millis(
                input.boundary().execution().timeout_ms(),
            ));
            tokio::pin!(deadline);
            let outcome = tokio::select! {
                biased;
                _ = input.boundary().execution().cancellation().signal().cancelled() => {
                    await_started_work(&mut compaction).await;
                    return Err(host_error(
                        &self.retention,
                        "runtime.compaction.cancelled_after_start",
                    ));
                }
                _ = &mut deadline => {
                    await_started_work(&mut compaction).await;
                    return Err(host_error(
                        &self.retention,
                        "runtime.compaction.deadline_after_start",
                    ));
                }
                outcome = &mut compaction => outcome
                    .map_err(|_| host_error(
                        &self.retention,
                        "runtime.compaction.host_task_failed",
                    ))?
                    .map_err(|reason| host_error(&self.retention, reason))?,
            };
            let (reason, result) = match outcome {
                CompactionHostOutcome::Applied { context_projection_id, evidence_material } => (
                    KernelPhaseReason::CompactionApplied,
                    CompactionResult::Applied {
                        context_projection_id,
                        evidence: self.retention.retain_evidence_material(
                            "runtime.compaction.applied",
                            evidence_material.as_slice(),
                        ),
                    },
                ),
            };
            Ok(KernelPhaseOutput::from_input(&input, reason, result)?)
        })
    }
}

async fn await_started_work<T>(work: &mut tokio::task::JoinHandle<T>) {
    // Once compaction starts writing, generation terminalization must wait for
    // the host task's own durable terminal point.
    let _ = work.await;
}

fn host_error(retention: &ProductionPayloadRetention, reason: &'static str) -> KernelPhaseError {
    KernelPhaseError::HostService {
        phase: RuntimeErrorPhase::Compaction,
        reason: KernelPhaseReason::CompactionSkipped,
        evidence: Some(retention.retain_evidence(reason)),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use tokio::sync::Notify;

    use super::{
        await_started_work, observed_protected_segments, AppliedCompactionProjection,
        RunStreamCompactionProjectionStore,
    };
    use crate::application::context_compaction::ContextProtectedSegment;

    fn projection(artifact_id: &str) -> AppliedCompactionProjection {
        AppliedCompactionProjection {
            artifact_id: artifact_id.to_owned(),
            mode: "automatic".to_owned(),
            trigger_reason: "runtime_kernel_v2_context_pressure".to_owned(),
            summary_text: "bounded summary".to_owned(),
        }
    }

    #[test]
    fn applied_projection_handoff_is_one_shot() {
        let store = RunStreamCompactionProjectionStore::default();

        store.retain(projection("artifact-1")).expect("first projection should retain");
        assert_eq!(
            store.take().expect("projection should be available once").artifact_id,
            "artifact-1"
        );
        assert!(store.take().is_none());
    }

    #[test]
    fn unresolved_projection_cannot_be_overwritten() {
        let store = RunStreamCompactionProjectionStore::default();
        store.retain(projection("artifact-1")).expect("first projection should retain");

        assert_eq!(
            store
                .retain(projection("artifact-2"))
                .expect_err("an unresolved projection must fail closed"),
            "runtime.compaction.projection_already_retained"
        );
    }

    #[test]
    fn observed_protection_requires_real_safeguard_signals() {
        let observed = observed_protected_segments(true, false, true, true, true, true);
        assert!(observed.contains(&ContextProtectedSegment::UnresolvedApproval));
        assert!(observed.contains(&ContextProtectedSegment::ActiveObjective));
        assert!(observed.contains(&ContextProtectedSegment::ToolCallResultPair));
        assert!(observed.contains(&ContextProtectedSegment::CitationProvenance));

        let unsafe_observed = observed_protected_segments(false, true, true, false, false, false);
        assert_eq!(
            unsafe_observed,
            vec![
                ContextProtectedSegment::SystemInstructions,
                ContextProtectedSegment::SafetyInstructions,
            ]
        );
    }

    #[tokio::test]
    async fn interrupted_phase_waits_for_slow_compaction_terminal_before_returning() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let writes = Arc::new(AtomicUsize::new(0));
        let mut work = tokio::spawn({
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            let writes = Arc::clone(&writes);
            async move {
                started.notify_one();
                release.notified().await;
                writes.fetch_add(1, Ordering::SeqCst);
            }
        });
        started.notified().await;
        let settlement = await_started_work(&mut work);
        tokio::pin!(settlement);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut settlement).await.is_err(),
            "phase must remain non-terminal while durable compaction is unresolved"
        );
        release.notify_one();
        settlement.await;
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        tokio::task::yield_now().await;
        assert_eq!(writes.load(Ordering::SeqCst), 1);
    }
}
