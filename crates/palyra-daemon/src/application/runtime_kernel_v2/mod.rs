//! Pure, generation-aware transition preparation for the second-generation runtime kernel.
//!
//! The kernel produces journal-ready state deltas but never commits them in memory.
//! Persistence, clocks, transport adapters, selection, and side effects remain host-owned.

mod kernel_contracts {
    use std::{collections::BTreeSet, fmt, marker::PhantomData};

    use palyra_common::runtime_contracts::{
        GenerationLeaseV1, RuntimeApprovalSubjectId, RuntimeDeliveryIntentId, RuntimeErrorPhase,
        RuntimeEventEnvelopeV2, RuntimeEventId, RuntimeEventName, RuntimeEventValidationError,
        RuntimeGeneration, RuntimeGenerationLane, RuntimeIdentitySetV1, RuntimeOperationId,
        RuntimeToolExecutionId, RuntimeToolProposalId,
    };
    use serde::{
        de::{Error as DeError, SeqAccess, Visitor},
        Deserialize, Deserializer, Serialize,
    };
    use sha2::{Digest, Sha256};
    use thiserror::Error;

    use super::selection::{RuntimeAuthority, RuntimeAuthorityDecisionV1, RuntimeAuthorityReason};

    const KERNEL_STATE_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
    const PREPARED_KERNEL_TRANSITION_SCHEMA_VERSION: u32 = 1;
    pub(crate) const MAX_KERNEL_EVENT_CURSORS: usize = 9;
    const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;

    include!("serde_bounds.rs");
    include!("state.rs");
    include!("transition.rs");
    include!("kernel.rs");
}

pub(crate) use kernel_contracts::*;

#[cfg(test)]
mod tests;

pub(crate) mod context;
pub(crate) mod dispatcher;
pub(crate) mod embedded_harness;
pub(crate) mod finalization;
pub(crate) mod harness;
pub(crate) mod host_event_contract;
pub(crate) mod host_event_sink;
mod journal_adapter;
pub(crate) mod phases;
pub(crate) mod production_flow;
pub(crate) mod production_services;
pub(crate) mod profile;
pub(crate) mod profile_resolver;
pub(crate) mod rollback;
pub(crate) mod rollout;
pub(crate) mod runtime_selection;
pub(crate) mod selection;
pub(crate) mod selection_host;
pub(crate) mod shadow;
