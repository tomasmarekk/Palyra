//! Native/external compaction handoff decision contracts.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

pub const NATIVE_COMPACTION_HANDOFF_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeCompactionOwner {
    Palyra,
    Harness,
    ExternalRuntime,
}

impl NativeCompactionOwner {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Palyra => "palyra",
            Self::Harness => "harness",
            Self::ExternalRuntime => "external_runtime",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCompactionHandoffInput {
    pub owner: NativeCompactionOwner,
    pub old_revision: u64,
    pub proposed_new_revision: Option<u64>,
    pub mirror_revision: u64,
    pub summary_sha256: Option<String>,
    pub fence_token: Option<String>,
    pub terminal_state_confirmed: bool,
    pub timeout_elapsed: bool,
    pub local_fallback_attempted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeCompactionHandoffReasonCode {
    LocalCompactionAllowed,
    HandoffRequired,
    FenceRequired,
    TerminalStateRequired,
    MirrorRevisionUpdated,
    ExternalCompactionTimeout,
    NoSilentLocalFallback,
    InvalidSummaryHash,
    RevisionDidNotAdvance,
}

impl NativeCompactionHandoffReasonCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalCompactionAllowed => "native_compaction.local_allowed",
            Self::HandoffRequired => "native_compaction.handoff_required",
            Self::FenceRequired => "native_compaction.fence_required",
            Self::TerminalStateRequired => "native_compaction.terminal_state_required",
            Self::MirrorRevisionUpdated => "native_compaction.mirror_revision_updated",
            Self::ExternalCompactionTimeout => "native_compaction.external_timeout",
            Self::NoSilentLocalFallback => "native_compaction.no_silent_local_fallback",
            Self::InvalidSummaryHash => "native_compaction.invalid_summary_hash",
            Self::RevisionDidNotAdvance => "native_compaction.revision_did_not_advance",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeCompactionHandoffDecisionKind {
    UseLocalCompaction,
    AwaitExternalHandoff,
    HandoffConfirmed,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCompactionHandoffDecision {
    pub schema_version: u32,
    pub owner: NativeCompactionOwner,
    pub decision: NativeCompactionHandoffDecisionKind,
    pub reason_codes: Vec<NativeCompactionHandoffReasonCode>,
    pub old_revision: u64,
    pub new_revision: Option<u64>,
    pub mirror_revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary_hash: Option<String>,
}

#[must_use]
pub fn decide_native_compaction_handoff(
    input: &NativeCompactionHandoffInput,
) -> NativeCompactionHandoffDecision {
    if input.owner == NativeCompactionOwner::Palyra {
        return decision(
            input,
            NativeCompactionHandoffDecisionKind::UseLocalCompaction,
            vec![NativeCompactionHandoffReasonCode::LocalCompactionAllowed],
        );
    }

    if input.local_fallback_attempted {
        return decision(
            input,
            NativeCompactionHandoffDecisionKind::Rejected,
            vec![NativeCompactionHandoffReasonCode::NoSilentLocalFallback],
        );
    }
    if input.timeout_elapsed {
        return decision(
            input,
            NativeCompactionHandoffDecisionKind::Rejected,
            vec![NativeCompactionHandoffReasonCode::ExternalCompactionTimeout],
        );
    }
    if input.fence_token.as_deref().is_none_or(str::is_empty) {
        return decision(
            input,
            NativeCompactionHandoffDecisionKind::AwaitExternalHandoff,
            vec![
                NativeCompactionHandoffReasonCode::HandoffRequired,
                NativeCompactionHandoffReasonCode::FenceRequired,
            ],
        );
    }
    if !input.terminal_state_confirmed {
        return decision(
            input,
            NativeCompactionHandoffDecisionKind::AwaitExternalHandoff,
            vec![
                NativeCompactionHandoffReasonCode::HandoffRequired,
                NativeCompactionHandoffReasonCode::TerminalStateRequired,
            ],
        );
    }
    if !input.summary_sha256.as_deref().is_some_and(is_valid_sha256) {
        return decision(
            input,
            NativeCompactionHandoffDecisionKind::Rejected,
            vec![NativeCompactionHandoffReasonCode::InvalidSummaryHash],
        );
    }
    if input.proposed_new_revision.is_none_or(|revision| revision <= input.old_revision) {
        return decision(
            input,
            NativeCompactionHandoffDecisionKind::Rejected,
            vec![NativeCompactionHandoffReasonCode::RevisionDidNotAdvance],
        );
    }

    decision(
        input,
        NativeCompactionHandoffDecisionKind::HandoffConfirmed,
        vec![
            NativeCompactionHandoffReasonCode::HandoffRequired,
            NativeCompactionHandoffReasonCode::MirrorRevisionUpdated,
        ],
    )
}

fn decision(
    input: &NativeCompactionHandoffInput,
    decision: NativeCompactionHandoffDecisionKind,
    reason_codes: Vec<NativeCompactionHandoffReasonCode>,
) -> NativeCompactionHandoffDecision {
    let mirror_revision = if decision == NativeCompactionHandoffDecisionKind::HandoffConfirmed {
        input.proposed_new_revision.unwrap_or(input.mirror_revision)
    } else {
        input.mirror_revision
    };
    NativeCompactionHandoffDecision {
        schema_version: NATIVE_COMPACTION_HANDOFF_SCHEMA_VERSION,
        owner: input.owner,
        decision,
        reason_codes,
        old_revision: input.old_revision,
        new_revision: input.proposed_new_revision,
        mirror_revision,
        summary_hash: input.summary_sha256.clone(),
    }
}

fn is_valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(owner: NativeCompactionOwner) -> NativeCompactionHandoffInput {
        NativeCompactionHandoffInput {
            owner,
            old_revision: 4,
            proposed_new_revision: Some(5),
            mirror_revision: 4,
            summary_sha256: Some("c".repeat(64)),
            fence_token: Some("fence-1".to_owned()),
            terminal_state_confirmed: true,
            timeout_elapsed: false,
            local_fallback_attempted: false,
        }
    }

    #[test]
    fn palyra_owned_compaction_uses_local_path() {
        let decision = decide_native_compaction_handoff(&input(NativeCompactionOwner::Palyra));

        assert_eq!(decision.decision, NativeCompactionHandoffDecisionKind::UseLocalCompaction);
        assert!(decision
            .reason_codes
            .contains(&NativeCompactionHandoffReasonCode::LocalCompactionAllowed));
    }

    #[test]
    fn external_timeout_has_typed_reason() {
        let decision = decide_native_compaction_handoff(&NativeCompactionHandoffInput {
            timeout_elapsed: true,
            ..input(NativeCompactionOwner::ExternalRuntime)
        });

        assert_eq!(decision.decision, NativeCompactionHandoffDecisionKind::Rejected);
        assert_eq!(
            decision.reason_codes,
            [NativeCompactionHandoffReasonCode::ExternalCompactionTimeout]
        );
    }

    #[test]
    fn external_owner_never_silently_falls_back_to_local_compaction() {
        let decision = decide_native_compaction_handoff(&NativeCompactionHandoffInput {
            local_fallback_attempted: true,
            ..input(NativeCompactionOwner::ExternalRuntime)
        });

        assert_eq!(decision.decision, NativeCompactionHandoffDecisionKind::Rejected);
        assert_eq!(
            decision.reason_codes,
            [NativeCompactionHandoffReasonCode::NoSilentLocalFallback]
        );
    }

    #[test]
    fn mirror_revision_updates_after_confirmed_handoff() {
        let decision = decide_native_compaction_handoff(&input(NativeCompactionOwner::Harness));

        assert_eq!(decision.decision, NativeCompactionHandoffDecisionKind::HandoffConfirmed);
        assert_eq!(decision.mirror_revision, 5);
        assert!(decision
            .reason_codes
            .contains(&NativeCompactionHandoffReasonCode::MirrorRevisionUpdated));
    }
}
