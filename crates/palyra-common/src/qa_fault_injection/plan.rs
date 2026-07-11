//! Versioned fault plans, registry descriptors, and canonical validation.
//!
//! Registry capabilities remain authoritative for actions accepted at each checkpoint.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error as ThisError;

use super::{
    is_bounded_actor, is_bounded_identifier, MAX_IDENTIFIER_BYTES, MAX_LOGICAL_TIME_ADVANCE_MS,
    QA_FAULT_EVIDENCE_ACTIVATION_RECORD_BUDGET_BYTES, QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES,
    QA_FAULT_EVIDENCE_SIDECAR_MAX_RECORDS, QA_FAULT_EVIDENCE_STANDARD_RECORD_BUDGET_BYTES,
    QA_FAULT_INJECTION_MAX_ACTIVATIONS, QA_FAULT_INJECTION_MAX_BARRIER_PARTICIPANTS,
    QA_FAULT_INJECTION_MAX_OCCURRENCE, QA_FAULT_INJECTION_PLAN_FORMAT,
    QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
};

/// Stable lifecycle boundary at which a fault may activate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QaFaultInjectionBoundary {
    /// Before an operation records its intent.
    BeforeIntent,
    /// After intent is recorded but before the effect begins.
    AfterIntent,
    /// Immediately before the external or durable effect.
    BeforeEffect,
    /// After the effect may have happened but before acknowledgement.
    AfterEffectBeforeAck,
    /// After acknowledgement but before the next state transition.
    AfterAckBeforeTransition,
    /// While a delivery operation is in flight.
    DuringDelivery,
    /// While cleanup is in progress.
    DuringCleanup,
}

impl QaFaultInjectionBoundary {
    /// Returns the canonical plan identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BeforeIntent => "before_intent",
            Self::AfterIntent => "after_intent",
            Self::BeforeEffect => "before_effect",
            Self::AfterEffectBeforeAck => "after_effect_before_ack",
            Self::AfterAckBeforeTransition => "after_ack_before_transition",
            Self::DuringDelivery => "during_delivery",
            Self::DuringCleanup => "during_cleanup",
        }
    }
}

/// Host subsystem that owns a registered fault point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QaFaultSubsystem {
    /// Durable connector delivery intent and outbox processing.
    ConnectorOutbox,
    /// Tool execution backend lifecycle.
    ExecutionBackend,
    /// Deterministic fixture provider runtime.
    FixtureProvider,
    /// Durable journal append transaction.
    JournalStore,
    /// Managed child-process lifecycle.
    ManagedProcessRuntime,
    /// MCP discovery and invocation broker.
    McpBroker,
    /// Final run-stream delivery transition.
    RunDelivery,
    /// Run-stream tool execution and acknowledgement.
    ToolRuntime,
    /// Worker lease, heartbeat, and reclaim state.
    WorkerFleet,
}

/// Discriminant for supported fault actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QaFaultActionKind {
    /// Return or simulate a bounded timeout.
    Timeout,
    /// Return or simulate a transport disconnect.
    Disconnect,
    /// Return a deterministic malformed protocol event.
    MalformedEvent,
    /// Terminate the disposable QA process with the reserved exit code.
    TerminateProcess,
    /// Coordinate a seeded deterministic actor release order.
    Barrier,
    /// Advance injected logical time without sleeping.
    AdvanceLogicalTime,
}

impl QaFaultActionKind {
    /// Returns the canonical plan identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Disconnect => "disconnect",
            Self::MalformedEvent => "malformed_event",
            Self::TerminateProcess => "terminate_process",
            Self::Barrier => "barrier",
            Self::AdvanceLogicalTime => "advance_logical_time",
        }
    }
}

/// Closed set of actions an injected checkpoint may request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum QaFaultAction {
    /// Return or simulate a bounded timeout.
    Timeout,
    /// Return or simulate a transport disconnect.
    Disconnect,
    /// Return a deterministic malformed protocol event.
    MalformedEvent,
    /// Terminate the disposable QA process with the reserved exit code.
    TerminateProcess,
    /// Coordinate a fixed number of actors deterministically.
    Barrier {
        /// Number of actors that must join the runtime barrier.
        participants: u16,
    },
    /// Advance injected logical time without sleeping.
    AdvanceLogicalTime {
        /// Deterministic logical-time increment; this never sleeps wall-clock time.
        milliseconds: u64,
    },
}

impl QaFaultAction {
    /// Returns the action discriminant used by registry capability checks.
    #[must_use]
    pub const fn kind(&self) -> QaFaultActionKind {
        match self {
            Self::Timeout => QaFaultActionKind::Timeout,
            Self::Disconnect => QaFaultActionKind::Disconnect,
            Self::MalformedEvent => QaFaultActionKind::MalformedEvent,
            Self::TerminateProcess => QaFaultActionKind::TerminateProcess,
            Self::Barrier { .. } => QaFaultActionKind::Barrier,
            Self::AdvanceLogicalTime { .. } => QaFaultActionKind::AdvanceLogicalTime,
        }
    }
}

/// Stable recovery outcome asserted by QA scenarios and evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QaFaultRecoveryClass {
    /// A bounded retry completed successfully.
    RetrySucceeded,
    /// Durable state allowed execution to resume.
    Resumed,
    /// Idempotency or replay logic suppressed a duplicate effect.
    DuplicateSuppressed,
    /// The system preserved an explicit unknown external outcome.
    OutcomeUnknown,
    /// A durable store proved the effect while acknowledgment reconciliation remains pending.
    EffectConfirmed,
    /// An effect was acknowledged but its following durable transition is unproven.
    TransitionPending,
    /// The operation stopped safely before an unproven effect.
    FailedClosed,
    /// A stale lease or claim was reclaimed safely.
    Reclaimed,
    /// Cleanup completed and was verified.
    CleanupSucceeded,
    /// Cancellation won the terminal-state race.
    Cancelled,
}

impl QaFaultRecoveryClass {
    /// Returns the canonical evidence identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RetrySucceeded => "retry_succeeded",
            Self::Resumed => "resumed",
            Self::DuplicateSuppressed => "duplicate_suppressed",
            Self::OutcomeUnknown => "outcome_unknown",
            Self::EffectConfirmed => "effect_confirmed",
            Self::TransitionPending => "transition_pending",
            Self::FailedClosed => "failed_closed",
            Self::Reclaimed => "reclaimed",
            Self::CleanupSucceeded => "cleanup_succeeded",
            Self::Cancelled => "cancelled",
        }
    }

    /// Parses one canonical evidence identifier.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "retry_succeeded" => Some(Self::RetrySucceeded),
            "resumed" => Some(Self::Resumed),
            "duplicate_suppressed" => Some(Self::DuplicateSuppressed),
            "outcome_unknown" => Some(Self::OutcomeUnknown),
            "effect_confirmed" => Some(Self::EffectConfirmed),
            "transition_pending" => Some(Self::TransitionPending),
            "failed_closed" => Some(Self::FailedClosed),
            "reclaimed" => Some(Self::Reclaimed),
            "cleanup_succeeded" => Some(Self::CleanupSucceeded),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// One stable fault point and its adapter-owned capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct QaFaultPointDescriptor {
    /// Namespaced identifier used by plans and checkpoint calls.
    pub id: &'static str,
    /// Lifecycle boundary represented by this point.
    pub boundary: QaFaultInjectionBoundary,
    /// Host subsystem that owns the adapter.
    pub subsystem: QaFaultSubsystem,
    /// Repository module that must maintain the adapter.
    pub owner: &'static str,
    /// Actions the adapter can implement without changing unrelated semantics.
    pub supported_actions: &'static [QaFaultActionKind],
    /// Recovery outcomes the adapter can prove after one of its actions.
    pub supported_recovery_classes: &'static [QaFaultRecoveryClass],
}

impl QaFaultPointDescriptor {
    /// Returns whether the adapter explicitly supports an action.
    #[must_use]
    pub fn supports(self, action: QaFaultActionKind) -> bool {
        self.supported_actions.contains(&action)
    }

    /// Returns whether the adapter can emit a recovery classification.
    #[must_use]
    pub fn supports_recovery(self, recovery_class: QaFaultRecoveryClass) -> bool {
        self.supported_recovery_classes.contains(&recovery_class)
    }
}

/// Canonically ordered registry for schema version 1.
pub static QA_FAULT_POINT_REGISTRY_V1: &[QaFaultPointDescriptor] = &[
    QaFaultPointDescriptor {
        id: "connector.outbox.after_ack_before_transition",
        boundary: QaFaultInjectionBoundary::AfterAckBeforeTransition,
        subsystem: QaFaultSubsystem::ConnectorOutbox,
        owner: "palyra-connectors::core::supervisor::outbox",
        supported_actions: &[QaFaultActionKind::TerminateProcess],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::Resumed,
            QaFaultRecoveryClass::DuplicateSuppressed,
            QaFaultRecoveryClass::OutcomeUnknown,
            QaFaultRecoveryClass::TransitionPending,
        ],
    },
    QaFaultPointDescriptor {
        id: "connector.outbox.after_effect_before_ack",
        boundary: QaFaultInjectionBoundary::AfterEffectBeforeAck,
        subsystem: QaFaultSubsystem::ConnectorOutbox,
        owner: "palyra-connectors::core::supervisor::outbox",
        supported_actions: &[
            QaFaultActionKind::Timeout,
            QaFaultActionKind::Disconnect,
            QaFaultActionKind::TerminateProcess,
        ],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::OutcomeUnknown,
            QaFaultRecoveryClass::DuplicateSuppressed,
        ],
    },
    QaFaultPointDescriptor {
        id: "connector.outbox.after_intent",
        boundary: QaFaultInjectionBoundary::AfterIntent,
        subsystem: QaFaultSubsystem::ConnectorOutbox,
        owner: "palyra-connectors::core::supervisor::outbox",
        supported_actions: &[
            QaFaultActionKind::Timeout,
            QaFaultActionKind::Disconnect,
            QaFaultActionKind::TerminateProcess,
        ],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::RetrySucceeded,
            QaFaultRecoveryClass::Resumed,
            QaFaultRecoveryClass::FailedClosed,
        ],
    },
    QaFaultPointDescriptor {
        id: "connector.outbox.batch_before_effect",
        boundary: QaFaultInjectionBoundary::BeforeEffect,
        subsystem: QaFaultSubsystem::ConnectorOutbox,
        owner: "palyra-connectors::core::supervisor::outbox",
        supported_actions: &[QaFaultActionKind::Barrier],
        supported_recovery_classes: &[QaFaultRecoveryClass::Resumed],
    },
    QaFaultPointDescriptor {
        id: "connector.outbox.before_effect",
        boundary: QaFaultInjectionBoundary::BeforeEffect,
        subsystem: QaFaultSubsystem::ConnectorOutbox,
        owner: "palyra-connectors::core::supervisor::outbox",
        supported_actions: &[
            QaFaultActionKind::Timeout,
            QaFaultActionKind::Disconnect,
            QaFaultActionKind::TerminateProcess,
        ],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::RetrySucceeded,
            QaFaultRecoveryClass::Resumed,
            QaFaultRecoveryClass::FailedClosed,
        ],
    },
    QaFaultPointDescriptor {
        id: "connector.outbox.before_intent",
        boundary: QaFaultInjectionBoundary::BeforeIntent,
        subsystem: QaFaultSubsystem::ConnectorOutbox,
        owner: "palyra-connectors::core::supervisor::outbox",
        supported_actions: &[
            QaFaultActionKind::Timeout,
            QaFaultActionKind::Disconnect,
            QaFaultActionKind::TerminateProcess,
        ],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::RetrySucceeded,
            QaFaultRecoveryClass::FailedClosed,
        ],
    },
    QaFaultPointDescriptor {
        id: "connector.outbox.during_delivery",
        boundary: QaFaultInjectionBoundary::DuringDelivery,
        subsystem: QaFaultSubsystem::ConnectorOutbox,
        owner: "palyra-connectors::core::supervisor::outbox",
        supported_actions: &[
            QaFaultActionKind::Timeout,
            QaFaultActionKind::Disconnect,
            QaFaultActionKind::TerminateProcess,
        ],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::OutcomeUnknown,
            QaFaultRecoveryClass::DuplicateSuppressed,
            QaFaultRecoveryClass::RetrySucceeded,
        ],
    },
    QaFaultPointDescriptor {
        id: "execution_backend.during_cleanup",
        boundary: QaFaultInjectionBoundary::DuringCleanup,
        subsystem: QaFaultSubsystem::ExecutionBackend,
        owner: "palyra-daemon::execution_backends",
        supported_actions: &[QaFaultActionKind::Timeout, QaFaultActionKind::TerminateProcess],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::CleanupSucceeded,
            QaFaultRecoveryClass::FailedClosed,
        ],
    },
    QaFaultPointDescriptor {
        id: "journal.after_effect_before_ack",
        boundary: QaFaultInjectionBoundary::AfterEffectBeforeAck,
        subsystem: QaFaultSubsystem::JournalStore,
        owner: "palyra-daemon::journal",
        supported_actions: &[QaFaultActionKind::TerminateProcess],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::Resumed,
            QaFaultRecoveryClass::DuplicateSuppressed,
            QaFaultRecoveryClass::EffectConfirmed,
        ],
    },
    QaFaultPointDescriptor {
        id: "journal.before_effect",
        boundary: QaFaultInjectionBoundary::BeforeEffect,
        subsystem: QaFaultSubsystem::JournalStore,
        owner: "palyra-daemon::journal",
        supported_actions: &[QaFaultActionKind::Timeout, QaFaultActionKind::TerminateProcess],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::RetrySucceeded,
            QaFaultRecoveryClass::FailedClosed,
        ],
    },
    QaFaultPointDescriptor {
        id: "managed_process.after_ack_before_transition",
        boundary: QaFaultInjectionBoundary::AfterAckBeforeTransition,
        subsystem: QaFaultSubsystem::ManagedProcessRuntime,
        owner: "palyra-daemon::sandbox_runner",
        supported_actions: &[QaFaultActionKind::TerminateProcess],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::Resumed,
            QaFaultRecoveryClass::CleanupSucceeded,
        ],
    },
    QaFaultPointDescriptor {
        id: "managed_process.after_effect_before_ack",
        boundary: QaFaultInjectionBoundary::AfterEffectBeforeAck,
        subsystem: QaFaultSubsystem::ManagedProcessRuntime,
        owner: "palyra-daemon::sandbox_runner",
        supported_actions: &[QaFaultActionKind::TerminateProcess],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::OutcomeUnknown,
            QaFaultRecoveryClass::CleanupSucceeded,
        ],
    },
    QaFaultPointDescriptor {
        id: "managed_process.before_effect",
        boundary: QaFaultInjectionBoundary::BeforeEffect,
        subsystem: QaFaultSubsystem::ManagedProcessRuntime,
        owner: "palyra-daemon::sandbox_runner",
        supported_actions: &[QaFaultActionKind::Timeout, QaFaultActionKind::TerminateProcess],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::RetrySucceeded,
            QaFaultRecoveryClass::FailedClosed,
        ],
    },
    QaFaultPointDescriptor {
        id: "managed_process.during_cleanup",
        boundary: QaFaultInjectionBoundary::DuringCleanup,
        subsystem: QaFaultSubsystem::ManagedProcessRuntime,
        owner: "palyra-daemon::sandbox_runner",
        supported_actions: &[QaFaultActionKind::Timeout, QaFaultActionKind::TerminateProcess],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::CleanupSucceeded,
            QaFaultRecoveryClass::FailedClosed,
        ],
    },
    QaFaultPointDescriptor {
        id: "provider.fixture.after_effect_before_ack",
        boundary: QaFaultInjectionBoundary::AfterEffectBeforeAck,
        subsystem: QaFaultSubsystem::FixtureProvider,
        owner: "palyra-daemon::model_provider",
        supported_actions: &[
            QaFaultActionKind::Timeout,
            QaFaultActionKind::Disconnect,
            QaFaultActionKind::MalformedEvent,
            QaFaultActionKind::TerminateProcess,
        ],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::OutcomeUnknown,
            QaFaultRecoveryClass::RetrySucceeded,
            QaFaultRecoveryClass::Resumed,
        ],
    },
    QaFaultPointDescriptor {
        id: "provider.fixture.after_intent",
        boundary: QaFaultInjectionBoundary::AfterIntent,
        subsystem: QaFaultSubsystem::FixtureProvider,
        owner: "palyra-daemon::model_provider",
        supported_actions: &[
            QaFaultActionKind::Timeout,
            QaFaultActionKind::Disconnect,
            QaFaultActionKind::MalformedEvent,
        ],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::RetrySucceeded,
            QaFaultRecoveryClass::Resumed,
            QaFaultRecoveryClass::FailedClosed,
        ],
    },
    QaFaultPointDescriptor {
        id: "provider.fixture.before_effect",
        boundary: QaFaultInjectionBoundary::BeforeEffect,
        subsystem: QaFaultSubsystem::FixtureProvider,
        owner: "palyra-daemon::model_provider",
        supported_actions: &[
            QaFaultActionKind::Timeout,
            QaFaultActionKind::Disconnect,
            QaFaultActionKind::MalformedEvent,
        ],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::RetrySucceeded,
            QaFaultRecoveryClass::FailedClosed,
        ],
    },
    QaFaultPointDescriptor {
        id: "provider.fixture.before_intent",
        boundary: QaFaultInjectionBoundary::BeforeIntent,
        subsystem: QaFaultSubsystem::FixtureProvider,
        owner: "palyra-daemon::model_provider",
        supported_actions: &[
            QaFaultActionKind::Timeout,
            QaFaultActionKind::Disconnect,
            QaFaultActionKind::TerminateProcess,
        ],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::RetrySucceeded,
            QaFaultRecoveryClass::FailedClosed,
        ],
    },
    QaFaultPointDescriptor {
        id: "run.final_delivery.after_effect_before_ack",
        boundary: QaFaultInjectionBoundary::AfterEffectBeforeAck,
        subsystem: QaFaultSubsystem::RunDelivery,
        owner: "palyra-daemon::application::run_stream::orchestration",
        supported_actions: &[QaFaultActionKind::TerminateProcess],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::Cancelled,
            QaFaultRecoveryClass::OutcomeUnknown,
            QaFaultRecoveryClass::Resumed,
        ],
    },
    QaFaultPointDescriptor {
        id: "tool.after_ack_before_transition",
        boundary: QaFaultInjectionBoundary::AfterAckBeforeTransition,
        subsystem: QaFaultSubsystem::ToolRuntime,
        owner: "palyra-daemon::application::run_stream::tool_flow",
        supported_actions: &[QaFaultActionKind::TerminateProcess],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::Resumed,
            QaFaultRecoveryClass::DuplicateSuppressed,
            QaFaultRecoveryClass::TransitionPending,
        ],
    },
    QaFaultPointDescriptor {
        id: "tool.after_effect_before_ack",
        boundary: QaFaultInjectionBoundary::AfterEffectBeforeAck,
        subsystem: QaFaultSubsystem::ToolRuntime,
        owner: "palyra-daemon::application::run_stream::tool_flow",
        supported_actions: &[QaFaultActionKind::TerminateProcess],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::DuplicateSuppressed,
            QaFaultRecoveryClass::OutcomeUnknown,
        ],
    },
    QaFaultPointDescriptor {
        id: "tool.before_effect",
        boundary: QaFaultInjectionBoundary::BeforeEffect,
        subsystem: QaFaultSubsystem::ToolRuntime,
        owner: "palyra-daemon::application::run_stream::tool_flow",
        supported_actions: &[QaFaultActionKind::Timeout, QaFaultActionKind::TerminateProcess],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::RetrySucceeded,
            QaFaultRecoveryClass::FailedClosed,
        ],
    },
    QaFaultPointDescriptor {
        id: "worker.claim.before_effect",
        boundary: QaFaultInjectionBoundary::BeforeEffect,
        subsystem: QaFaultSubsystem::WorkerFleet,
        owner: "palyra-workerd::WorkerFleetManager",
        supported_actions: &[QaFaultActionKind::Barrier],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::FailedClosed,
            QaFaultRecoveryClass::RetrySucceeded,
        ],
    },
    QaFaultPointDescriptor {
        id: "worker.heartbeat.before_effect",
        boundary: QaFaultInjectionBoundary::BeforeEffect,
        subsystem: QaFaultSubsystem::WorkerFleet,
        owner: "palyra-workerd::WorkerFleetManager",
        supported_actions: &[QaFaultActionKind::Timeout, QaFaultActionKind::Barrier],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::RetrySucceeded,
            QaFaultRecoveryClass::Reclaimed,
        ],
    },
    QaFaultPointDescriptor {
        id: "worker.stale_reclaim.batch_before_effect",
        boundary: QaFaultInjectionBoundary::BeforeEffect,
        subsystem: QaFaultSubsystem::WorkerFleet,
        owner: "palyra-workerd::WorkerFleetManager",
        supported_actions: &[QaFaultActionKind::Barrier],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::Reclaimed,
            QaFaultRecoveryClass::FailedClosed,
        ],
    },
    QaFaultPointDescriptor {
        id: "worker.stale_reclaim.before_effect",
        boundary: QaFaultInjectionBoundary::BeforeEffect,
        subsystem: QaFaultSubsystem::WorkerFleet,
        owner: "palyra-workerd::WorkerFleetManager",
        supported_actions: &[QaFaultActionKind::AdvanceLogicalTime],
        supported_recovery_classes: &[
            QaFaultRecoveryClass::Reclaimed,
            QaFaultRecoveryClass::FailedClosed,
        ],
    },
];

/// Returns a registered descriptor by exact namespaced point id.
#[must_use]
pub fn qa_fault_point_descriptor(point_id: &str) -> Option<&'static QaFaultPointDescriptor> {
    QA_FAULT_POINT_REGISTRY_V1
        .binary_search_by_key(&point_id, |descriptor| descriptor.id)
        .ok()
        .map(|index| &QA_FAULT_POINT_REGISTRY_V1[index])
}

/// One deterministic activation declared by a fault plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QaFaultActivation {
    /// Stable identifier referenced by scenario expectations and evidence.
    pub id: String,
    /// Exact namespaced point from [`QA_FAULT_POINT_REGISTRY_V1`].
    pub point_id: String,
    /// Optional stable actor selector; omitted entries match the first actor at the occurrence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    /// One-based occurrence for this point and actor.
    pub occurrence: u32,
    /// Closed action requested when the checkpoint matches.
    pub action: QaFaultAction,
}

/// Strict versioned fault-injection plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QaFaultInjectionPlan {
    /// Plan schema version.
    pub schema_version: u32,
    /// Stable plan format label.
    pub format: String,
    /// Reproduction seed used by deterministic scheduling.
    pub seed: u64,
    /// Bounded activation entries evaluated by the injected controller.
    pub activations: Vec<QaFaultActivation>,
}

impl QaFaultInjectionPlan {
    /// Validates all plan fields and registry capabilities.
    ///
    /// # Errors
    /// Returns every semantic validation issue found in the plan.
    pub fn validate(&self) -> Result<(), QaFaultInjectionPlanValidationError> {
        let issues = validate_fault_injection_plan(self);
        if issues.is_empty() {
            Ok(())
        } else {
            Err(QaFaultInjectionPlanValidationError::new(issues))
        }
    }

    /// Encodes the validated plan in its canonical compact JSON form.
    ///
    /// # Errors
    /// Returns validation errors or an unexpected JSON serialization failure.
    pub fn canonical_json(&self) -> Result<Vec<u8>, QaFaultInjectionPlanDigestError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(QaFaultInjectionPlanDigestError::Serialize)
    }

    /// Computes the lowercase SHA-256 digest of canonical plan JSON.
    ///
    /// # Errors
    /// Returns validation errors or an unexpected JSON serialization failure.
    pub fn canonical_sha256(&self) -> Result<String, QaFaultInjectionPlanDigestError> {
        Ok(hex::encode(Sha256::digest(self.canonical_json()?)))
    }
}

/// One path-qualified fault-plan validation issue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaFaultInjectionPlanIssue {
    /// Stable machine-readable reason code.
    pub code: String,
    /// JSON path to the invalid plan field.
    pub path: String,
    /// Bounded operator-facing explanation.
    pub message: String,
}

/// Collection of semantic fault-plan validation issues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QaFaultInjectionPlanValidationError {
    issues: Vec<QaFaultInjectionPlanIssue>,
}

impl QaFaultInjectionPlanValidationError {
    fn new(issues: Vec<QaFaultInjectionPlanIssue>) -> Self {
        debug_assert!(!issues.is_empty());
        Self { issues }
    }

    /// Returns every collected issue.
    #[must_use]
    pub fn issues(&self) -> &[QaFaultInjectionPlanIssue] {
        self.issues.as_slice()
    }
}

impl fmt::Display for QaFaultInjectionPlanValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(issue) = self.issues.first() {
            write!(formatter, "{} at {}: {}", issue.code, issue.path, issue.message)
        } else {
            formatter.write_str("fault-injection plan validation failed")
        }
    }
}

impl Error for QaFaultInjectionPlanValidationError {}

/// Fault-plan parse or semantic validation failure.
#[derive(Debug)]
pub enum QaFaultInjectionPlanParseError {
    /// YAML or JSON could not be decoded into the strict wire shape.
    Parse { source: yaml_serde::Error },
    /// Decoding succeeded but semantic validation failed.
    Invalid(QaFaultInjectionPlanValidationError),
}

impl QaFaultInjectionPlanParseError {
    /// Returns semantic issues when decoding succeeded but validation failed.
    #[must_use]
    pub fn issues(&self) -> Option<&[QaFaultInjectionPlanIssue]> {
        match self {
            Self::Parse { .. } => None,
            Self::Invalid(error) => Some(error.issues()),
        }
    }
}

impl fmt::Display for QaFaultInjectionPlanParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse { source } => write!(formatter, "failed to parse QA fault plan: {source}"),
            Self::Invalid(error) => write!(formatter, "invalid QA fault plan: {error}"),
        }
    }
}

impl Error for QaFaultInjectionPlanParseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Parse { source } => Some(source),
            Self::Invalid(error) => Some(error),
        }
    }
}

/// Canonicalization or digest failure for a fault plan.
#[derive(Debug, ThisError)]
pub enum QaFaultInjectionPlanDigestError {
    /// Semantic plan validation failed before canonicalization.
    #[error(transparent)]
    Invalid(#[from] QaFaultInjectionPlanValidationError),
    /// The validated typed plan unexpectedly failed JSON encoding.
    #[error("failed to serialize canonical QA fault plan: {0}")]
    Serialize(#[source] serde_json::Error),
}

/// Parses and validates a YAML or JSON fault plan.
///
/// # Errors
/// Returns a syntax error or path-qualified semantic validation issues.
pub fn parse_qa_fault_injection_plan_yaml(
    text: &str,
) -> Result<QaFaultInjectionPlan, QaFaultInjectionPlanParseError> {
    let plan = yaml_serde::from_str::<QaFaultInjectionPlan>(text)
        .map_err(|source| QaFaultInjectionPlanParseError::Parse { source })?;
    plan.validate().map_err(QaFaultInjectionPlanParseError::Invalid)?;
    Ok(plan)
}

/// Versioned machine-readable snapshot of the plan contract.
#[must_use]
pub fn qa_fault_injection_plan_schema_snapshot() -> Value {
    json!({
        "schema_version": QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        "format": QA_FAULT_INJECTION_PLAN_FORMAT,
        "encoding": "yaml_or_json",
        "required_plan_fields": ["schema_version", "format", "seed", "activations"],
        "activation_fields": ["id", "point_id", "actor", "occurrence", "action"],
        "boundaries": [
            "before_intent",
            "after_intent",
            "before_effect",
            "after_effect_before_ack",
            "after_ack_before_transition",
            "during_delivery",
            "during_cleanup"
        ],
        "actions": [
            "timeout",
            "disconnect",
            "malformed_event",
            "terminate_process",
            "barrier",
            "advance_logical_time"
        ],
        "recovery_classes": [
            "retry_succeeded",
            "resumed",
            "duplicate_suppressed",
            "outcome_unknown",
            "effect_confirmed",
            "transition_pending",
            "failed_closed",
            "reclaimed",
            "cleanup_succeeded",
            "cancelled"
        ],
        "limits": {
            "max_activations": QA_FAULT_INJECTION_MAX_ACTIVATIONS,
            "max_occurrence": QA_FAULT_INJECTION_MAX_OCCURRENCE,
            "max_barrier_participants": QA_FAULT_INJECTION_MAX_BARRIER_PARTICIPANTS,
            "max_evidence_sidecar_records": QA_FAULT_EVIDENCE_SIDECAR_MAX_RECORDS,
            "max_evidence_sidecar_bytes": QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES,
            "estimated_standard_record_bytes": QA_FAULT_EVIDENCE_STANDARD_RECORD_BUDGET_BYTES,
            "estimated_activation_record_bytes": QA_FAULT_EVIDENCE_ACTIVATION_RECORD_BUDGET_BYTES,
            "max_identifier_bytes": MAX_IDENTIFIER_BYTES,
            "max_logical_time_advance_ms": MAX_LOGICAL_TIME_ADVANCE_MS
        }
    })
}

/// Versioned machine-readable snapshot of all registered point capabilities.
#[must_use]
pub fn qa_fault_point_registry_snapshot() -> Value {
    json!({
        "schema_version": QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        "points": QA_FAULT_POINT_REGISTRY_V1,
    })
}

fn validate_fault_injection_plan(plan: &QaFaultInjectionPlan) -> Vec<QaFaultInjectionPlanIssue> {
    let mut issues = Vec::new();
    if plan.schema_version != QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION {
        push_plan_issue(
            &mut issues,
            "unsupported_schema_version",
            "$.schema_version",
            format!(
                "expected schema_version {QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION}, got {}",
                plan.schema_version
            ),
        );
    }
    if plan.format != QA_FAULT_INJECTION_PLAN_FORMAT {
        push_plan_issue(
            &mut issues,
            "invalid_format",
            "$.format",
            format!("expected format `{QA_FAULT_INJECTION_PLAN_FORMAT}`"),
        );
    }
    if plan.activations.is_empty() {
        push_plan_issue(
            &mut issues,
            "empty_activations",
            "$.activations",
            "fault plan must declare at least one activation",
        );
    } else if plan.activations.len() > QA_FAULT_INJECTION_MAX_ACTIVATIONS {
        push_plan_issue(
            &mut issues,
            "too_many_activations",
            "$.activations",
            format!("fault plan supports at most {QA_FAULT_INJECTION_MAX_ACTIVATIONS} activations"),
        );
    }

    let mut activation_ids = BTreeSet::new();
    let mut triggers = BTreeSet::new();
    let mut wildcard_selectors = BTreeSet::new();
    let mut exact_selectors = BTreeSet::new();
    for (index, activation) in plan.activations.iter().enumerate() {
        let path = format!("$.activations[{index}]");
        if !is_bounded_identifier(activation.id.as_str()) {
            push_plan_issue(
                &mut issues,
                "invalid_activation_id",
                format!("{path}.id"),
                "activation id must be a bounded lowercase identifier",
            );
        } else if !activation_ids.insert(activation.id.as_str()) {
            push_plan_issue(
                &mut issues,
                "duplicate_activation_id",
                format!("{path}.id"),
                format!("activation id `{}` is duplicated", activation.id),
            );
        }
        let descriptor = qa_fault_point_descriptor(activation.point_id.as_str());
        if descriptor.is_none() {
            push_plan_issue(
                &mut issues,
                "unknown_fault_point",
                format!("{path}.point_id"),
                format!("fault point `{}` is not registered", activation.point_id),
            );
        }
        if let Some(actor) = activation.actor.as_deref() {
            if !is_bounded_actor(actor) {
                push_plan_issue(
                    &mut issues,
                    "invalid_actor",
                    format!("{path}.actor"),
                    "actor must be a bounded non-secret ASCII identifier",
                );
            }
            if matches!(activation.action, QaFaultAction::Barrier { .. }) {
                push_plan_issue(
                    &mut issues,
                    "barrier_actor_selector_not_allowed",
                    format!("{path}.actor"),
                    "barrier activations collect multiple actors and cannot select one actor",
                );
            }
        }
        if !(1..=QA_FAULT_INJECTION_MAX_OCCURRENCE).contains(&activation.occurrence) {
            push_plan_issue(
                &mut issues,
                "invalid_occurrence",
                format!("{path}.occurrence"),
                format!("occurrence must be in range 1..={QA_FAULT_INJECTION_MAX_OCCURRENCE}"),
            );
        }
        if activation.actor.is_none() && activation.occurrence > 1 {
            push_plan_issue(
                &mut issues,
                "unbounded_occurrence_actor",
                format!("{path}.occurrence"),
                "occurrence greater than one requires an explicit actor",
            );
        }
        let trigger =
            (activation.point_id.as_str(), activation.actor.as_deref(), activation.occurrence);
        if !triggers.insert(trigger) {
            push_plan_issue(
                &mut issues,
                "duplicate_activation_trigger",
                path.clone(),
                "point, actor, and occurrence must identify at most one activation",
            );
        }
        let selector = (activation.point_id.as_str(), activation.occurrence);
        let overlaps = if activation.actor.is_some() {
            wildcard_selectors.contains(&selector)
        } else {
            exact_selectors.contains(&selector)
        };
        if overlaps {
            push_plan_issue(
                &mut issues,
                "overlapping_activation_selector",
                path.clone(),
                "wildcard and exact actor selectors cannot share a point and occurrence",
            );
        }
        if activation.actor.is_some() {
            exact_selectors.insert(selector);
        } else {
            wildcard_selectors.insert(selector);
        }
        if let Some(descriptor) = descriptor {
            if !descriptor.supports(activation.action.kind()) {
                push_plan_issue(
                    &mut issues,
                    "unsupported_fault_action",
                    format!("{path}.action"),
                    format!(
                        "fault point `{}` does not support `{}`",
                        activation.point_id,
                        activation.action.kind().as_str()
                    ),
                );
            }
        }
        match activation.action {
            QaFaultAction::Barrier { participants }
                if !(2..=QA_FAULT_INJECTION_MAX_BARRIER_PARTICIPANTS).contains(&participants) =>
            {
                push_plan_issue(
                    &mut issues,
                    "invalid_barrier_participants",
                    format!("{path}.action.participants"),
                    format!(
                        "barrier participants must be in range 2..={QA_FAULT_INJECTION_MAX_BARRIER_PARTICIPANTS}"
                    ),
                );
            }
            QaFaultAction::AdvanceLogicalTime { milliseconds }
                if !(1..=MAX_LOGICAL_TIME_ADVANCE_MS).contains(&milliseconds) =>
            {
                push_plan_issue(
                    &mut issues,
                    "invalid_logical_time_advance",
                    format!("{path}.action.milliseconds"),
                    format!(
                        "logical time advance must be in range 1..={MAX_LOGICAL_TIME_ADVANCE_MS}"
                    ),
                );
            }
            _ => {}
        }
    }
    validate_campaign_evidence_budget(plan, &mut issues);
    issues
}

fn validate_campaign_evidence_budget(
    plan: &QaFaultInjectionPlan,
    issues: &mut Vec<QaFaultInjectionPlanIssue>,
) {
    let launch_records = plan.activations.len().saturating_add(1);
    let mut target_occurrences = BTreeMap::<(&str, &str), u32>::new();
    let mut barrier_records = 0usize;
    for activation in &plan.activations {
        if let Some(actor) = activation.actor.as_deref() {
            target_occurrences
                .entry((activation.point_id.as_str(), actor))
                .and_modify(|occurrence| {
                    *occurrence = (*occurrence).max(activation.occurrence);
                })
                .or_insert(activation.occurrence);
        }
        if let QaFaultAction::Barrier { participants } = activation.action {
            barrier_records = barrier_records.saturating_add(usize::from(participants) * 2);
        }
    }
    let observation_records = target_occurrences
        .values()
        .map(|occurrence| usize::try_from(occurrence.saturating_sub(1)).unwrap_or(usize::MAX))
        .fold(0usize, usize::saturating_add);
    let activation_records = plan.activations.len();
    let recovery_records = plan.activations.len();
    let record_budget = launch_records
        .saturating_add(observation_records)
        .saturating_add(activation_records)
        .saturating_add(recovery_records)
        .saturating_add(barrier_records);
    if record_budget > QA_FAULT_EVIDENCE_SIDECAR_MAX_RECORDS {
        push_plan_issue(
            issues,
            "campaign_evidence_record_budget_exceeded",
            "$.activations",
            format!(
                "campaign requires up to {record_budget} evidence records, limit is {QA_FAULT_EVIDENCE_SIDECAR_MAX_RECORDS}"
            ),
        );
    }
    let standard_records = record_budget.saturating_sub(activation_records);
    let estimated_bytes = u64::try_from(standard_records)
        .unwrap_or(u64::MAX)
        .saturating_mul(QA_FAULT_EVIDENCE_STANDARD_RECORD_BUDGET_BYTES)
        .saturating_add(
            u64::try_from(activation_records)
                .unwrap_or(u64::MAX)
                .saturating_mul(QA_FAULT_EVIDENCE_ACTIVATION_RECORD_BUDGET_BYTES),
        );
    if estimated_bytes > u64::try_from(QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES).unwrap_or(u64::MAX) {
        push_plan_issue(
            issues,
            "campaign_evidence_byte_budget_exceeded",
            "$.activations",
            format!(
                "campaign evidence estimate is {estimated_bytes} bytes, limit is {QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES}"
            ),
        );
    }
}

fn push_plan_issue(
    issues: &mut Vec<QaFaultInjectionPlanIssue>,
    code: impl Into<String>,
    path: impl Into<String>,
    message: impl Into<String>,
) {
    issues.push(QaFaultInjectionPlanIssue {
        code: code.into(),
        path: path.into(),
        message: message.into(),
    });
}
