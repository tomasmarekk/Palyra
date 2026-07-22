//! Canonical, host-owned phase boundaries for RuntimeKernelV2.
//!
//! The kernel moves opaque payload references and typed authority between phases.
//! Raw provider payloads, tool arguments, credentials, and execution receipts stay
//! behind the host services that already own those security boundaries.

use std::{fmt, future::Future, marker::PhantomData, pin::Pin, sync::Arc};

use palyra_common::runtime_contracts::{
    BackpressurePolicy, CancellationContextV1, CancellationReason, CancellationScopeKind,
    RuntimeApprovalSubjectId, RuntimeContextProjectionId, RuntimeDeliveryIntentId,
    RuntimeErrorPhase, RuntimeGeneration, RuntimeGenerationLane, RuntimeIdentityError,
    RuntimeIdentitySetV1, RuntimeLeaseId, RuntimeOperationId, RuntimeRunId, RuntimeSessionId,
    RuntimeTerminalOutcome, RuntimeToolExecutionId, RuntimeToolProposalId, SideEffectFenceState,
};
use thiserror::Error;

#[cfg(test)]
use super::runtime_selection::ResolvedRuntimeSelection;
#[cfg(test)]
use super::selection::{RuntimeAuthorityProgressEvidence, V2RuntimeAvailability};

const SHA256_BYTES: usize = 32;
const MAX_TOOL_NAME_BYTES: usize = 192;
const MAX_EVIDENCE_REFS: usize = 16;

include!("phases/contracts.rs");
include!("phases/lifecycle.rs");
include!("phases/tool_authority.rs");
include!("phases/errors.rs");
include!("phases/tests.rs");

/// Returns the canonical phase plan executed by the embedded V2 adapter.
///
/// Shadow planning consumes this V2-owned projection rather than rebuilding
/// the phase sequence from the legacy run-loop plan.
#[must_use]
pub(crate) fn canonical_v2_expected_phase_plan(tool_capable: bool) -> Vec<RuntimeErrorPhase> {
    let mut phases = vec![
        RuntimeErrorPhase::Admission,
        RuntimeErrorPhase::RuntimeSelection,
        RuntimeErrorPhase::ContextAssembly,
        RuntimeErrorPhase::ProviderCall,
    ];
    if tool_capable {
        phases.extend([
            RuntimeErrorPhase::ToolGate,
            RuntimeErrorPhase::Approval,
            RuntimeErrorPhase::ToolExecution,
            RuntimeErrorPhase::ResultProjection,
            RuntimeErrorPhase::Compaction,
        ]);
    }
    phases.extend([
        RuntimeErrorPhase::Verification,
        RuntimeErrorPhase::Finalization,
        RuntimeErrorPhase::DeliveryIntent,
    ]);
    phases
}
