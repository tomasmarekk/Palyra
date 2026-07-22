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
        runtime_kernel_v2::phases::{
            CompactionPhase, CompactionRequest, CompactionResult, KernelPhaseError,
            KernelPhaseFuture, KernelPhaseOutput, KernelPhaseReason, RuntimePhaseService,
        },
        session_compaction::{apply_session_compaction, SessionCompactionApplyRequest},
    },
    gateway::GatewayRuntimeState,
    journal::OrchestratorSessionRecord,
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
    run_id: String,
    manifest_material: Vec<u8>,
    projections: Arc<RunStreamCompactionProjectionStore>,
}

impl RunStreamCompactionInput {
    pub(crate) fn new(
        runtime_state: Arc<GatewayRuntimeState>,
        session: OrchestratorSessionRecord,
        actor_principal: String,
        run_id: String,
        projections: Arc<RunStreamCompactionProjectionStore>,
    ) -> Self {
        let manifest_material = serde_json::to_vec(&serde_json::json!({
            "session_id": session.session_id.as_str(),
            "run_id": run_id.as_str(),
            "mode": "automatic",
            "policy": "runtime_kernel_v2",
        }))
        .unwrap_or_default();
        Self { runtime_state, session, actor_principal, run_id, manifest_material, projections }
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
        await_started_work, AppliedCompactionProjection, RunStreamCompactionProjectionStore,
    };

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
