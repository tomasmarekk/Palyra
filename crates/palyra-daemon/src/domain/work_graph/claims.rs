//! Generation-fenced claim, heartbeat, reclaim, and settlement contracts.

use std::{collections::BTreeSet, fmt};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{WorkItemRecordV1, WorkItemState, WorkVerificationState};

/// Minimum accepted work-item lease duration.
pub(crate) const MIN_WORK_CLAIM_TTL_MS: u64 = 1_000;

/// Maximum accepted work-item lease duration.
pub(crate) const MAX_WORK_CLAIM_TTL_MS: u64 = 60 * 60 * 1_000;

/// Opaque bearer token returned once to the winning claimer.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct WorkClaimToken([u8; 32]);

impl WorkClaimToken {
    /// Issues a token using operating-system entropy.
    pub(crate) fn issue() -> Result<Self, getrandom::Error> {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes)?;
        Ok(Self(bytes))
    }

    /// Computes the lowercase digest persisted by the journal.
    pub(crate) fn sha256_hex(&self) -> String {
        hex::encode(Sha256::digest(self.0))
    }

    /// Encodes the one-time worker capability for transport to the winning worker.
    pub(crate) fn expose_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parses an exact 32-byte worker capability supplied back to the host.
    pub(crate) fn from_hex(value: &str) -> Option<Self> {
        let decoded = hex::decode(value).ok()?;
        let bytes = <[u8; 32]>::try_from(decoded).ok()?;
        Some(Self(bytes))
    }
}

impl fmt::Debug for WorkClaimToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WorkClaimToken(<redacted>)")
    }
}

/// Current side-effect knowledge used when deciding whether expired work is replay-safe.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkSideEffectFenceState {
    Clear,
    InFlight,
    Committed,
    Unknown,
}

impl WorkSideEffectFenceState {
    /// Stable storage representation.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Clear => "clear",
            Self::InFlight => "in_flight",
            Self::Committed => "committed",
            Self::Unknown => "unknown",
        }
    }

    /// Parses a persisted fence state.
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "clear" => Some(Self::Clear),
            "in_flight" => Some(Self::InFlight),
            "committed" => Some(Self::Committed),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }
}

/// Durable execution authority for one work-item generation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkItemClaimV1 {
    pub(crate) worker_id: String,
    pub(crate) worker_principal: String,
    pub(crate) claim_token_sha256: String,
    pub(crate) generation: u64,
    pub(crate) attempt_id: String,
    pub(crate) runtime_instance_id: String,
    pub(crate) process_start_token: String,
    pub(crate) issued_at_unix_ms: i64,
    pub(crate) expires_at_unix_ms: i64,
    pub(crate) heartbeat_at_unix_ms: i64,
    pub(crate) side_effect_fence: WorkSideEffectFenceState,
    pub(crate) resource_lease_id: Option<String>,
    pub(crate) record_revision: u64,
}

/// Host-policy-scoped claim request for the next eligible item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClaimReadyWorkItemRequest {
    pub(crate) graph_id: String,
    pub(crate) work_item_id: Option<String>,
    pub(crate) expected_item_revision: Option<u64>,
    pub(crate) worker_id: String,
    pub(crate) worker_principal: String,
    pub(crate) authorized_owner_principal: String,
    pub(crate) capability_profiles: BTreeSet<String>,
    pub(crate) provider_backpressure_profiles: BTreeSet<String>,
    pub(crate) memory_pressure: bool,
    pub(crate) resource_lease_id: Option<String>,
    pub(crate) runtime_instance_id: String,
    pub(crate) process_start_token: String,
    pub(crate) lease_ttl_ms: u64,
}

/// Winning claim and the bearer token returned only to its worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkItemClaimGrant {
    pub(crate) item: WorkItemRecordV1,
    pub(crate) claim: WorkItemClaimV1,
    pub(crate) token: WorkClaimToken,
}

/// Outcome of atomic ready-queue admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClaimReadyWorkItemOutcome {
    Granted(Box<WorkItemClaimGrant>),
    NoEligibleItem { reason_code: &'static str },
}

/// Generation and token authority required for worker-owned mutations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkClaimAuthority {
    pub(crate) graph_id: String,
    pub(crate) work_item_id: String,
    pub(crate) worker_id: String,
    pub(crate) generation: u64,
    pub(crate) token: WorkClaimToken,
}

/// Heartbeat request for one current claim generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkItemHeartbeatRequest {
    pub(crate) authority: WorkClaimAuthority,
    pub(crate) extend_by_ms: u64,
}

/// Host decision for a heartbeat attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WorkItemHeartbeatOutcome {
    Renewed(WorkItemClaimV1),
    StaleAuthority { reason_code: &'static str },
    Expired { reason_code: &'static str },
}

/// Claim-scoped update to the host's knowledge of side effects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkItemSideEffectFenceRequest {
    pub(crate) authority: WorkClaimAuthority,
    pub(crate) expected_item_revision: u64,
    pub(crate) state: WorkSideEffectFenceState,
    pub(crate) actor_principal: String,
}

/// Host disposition for a side-effect fence update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WorkItemSideEffectFenceOutcome {
    Updated(WorkItemClaimV1),
    StaleAuthority { reason_code: &'static str },
}

/// Runtime identity observation made by the host before reclaim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkRuntimeLiveness {
    Alive,
    Dead,
    Unknown,
    ProcessIdentityReused,
}

/// Host evidence required to evaluate an expired claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StaleReclaimRequest {
    pub(crate) graph_id: String,
    pub(crate) work_item_id: String,
    pub(crate) expected_item_revision: u64,
    pub(crate) expected_generation: u64,
    pub(crate) runtime_instance_id: String,
    pub(crate) process_start_token: String,
    pub(crate) liveness: WorkRuntimeLiveness,
    pub(crate) observed_side_effect_fence: WorkSideEffectFenceState,
    pub(crate) actor_principal: String,
}

/// Fail-closed reclaim decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StaleReclaimDecision {
    Reclaimed { item: WorkItemRecordV1, reason_code: &'static str },
    DeferredLive { reason_code: &'static str },
    RequiresReview { item: WorkItemRecordV1, reason_code: &'static str },
    NotExpired { reason_code: &'static str },
    LostRace { reason_code: &'static str },
}

/// Worker result proposed under one exact claim generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkClaimSettlementRequest {
    pub(crate) authority: WorkClaimAuthority,
    pub(crate) expected_item_revision: u64,
    pub(crate) target_state: WorkItemState,
    pub(crate) verification_state: WorkVerificationState,
    pub(crate) result_sha256: String,
    pub(crate) reason_code: String,
    pub(crate) actor_principal: String,
}

/// Host disposition for a claim-scoped result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WorkClaimSettlementOutcome {
    Applied { item: Box<WorkItemRecordV1>, graph_revision: u64 },
    Orphaned { reason_code: &'static str },
}

/// Redacted durable claim counters exposed to operator diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkGraphClaimDiagnosticsV1 {
    pub(crate) graph_id: String,
    pub(crate) active_claim_count: u64,
    pub(crate) expired_claim_count: u64,
    pub(crate) total_attempt_count: u64,
    pub(crate) orphan_result_count: u64,
    pub(crate) side_effect_review_count: u64,
    pub(crate) last_reason_code: Option<String>,
}

/// Stable reason codes for claim and reclaim decisions.
pub(crate) mod claim_reason {
    pub(crate) const CLAIMED: &str = "work_graph.claim.granted";
    pub(crate) const NO_READY_ITEM: &str = "work_graph.claim.no_ready_item";
    pub(crate) const CAPABILITY_MISMATCH: &str = "work_graph.claim.capability_mismatch";
    pub(crate) const POLICY_SCOPE_MISMATCH: &str = "work_graph.claim.policy_scope_mismatch";
    pub(crate) const RACE_LOST: &str = "work_graph.claim.race_lost";
    pub(crate) const HEARTBEAT_RENEWED: &str = "work_graph.heartbeat.renewed";
    pub(crate) const HEARTBEAT_STALE: &str = "work_graph.heartbeat.stale_authority";
    pub(crate) const HEARTBEAT_EXPIRED: &str = "work_graph.heartbeat.expired";
    pub(crate) const SIDE_EFFECT_FENCE_UPDATED: &str = "work_graph.side_effect_fence.updated";
    pub(crate) const SIDE_EFFECT_FENCE_STALE: &str = "work_graph.side_effect_fence.stale_authority";
    pub(crate) const RECLAIMED_DEAD: &str = "work_graph.reclaim.dead_worker";
    pub(crate) const RECLAIMED_PID_REUSE: &str = "work_graph.reclaim.process_identity_reused";
    pub(crate) const RECLAIM_DEFERRED_LIVE: &str = "work_graph.reclaim.worker_alive";
    pub(crate) const RECLAIM_LIVENESS_UNKNOWN: &str = "work_graph.reclaim.liveness_unknown";
    pub(crate) const RECLAIM_SIDE_EFFECT_UNKNOWN: &str =
        "work_graph.reclaim.side_effect_confirmation_required";
    pub(crate) const RECLAIM_NOT_EXPIRED: &str = "work_graph.reclaim.not_expired";
    pub(crate) const LATE_RESULT_ORPHANED: &str = "work_graph.result.stale_generation";
}
