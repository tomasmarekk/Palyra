//! Shared runtime vocabulary for queueing, flow orchestration, delivery policy,
//! auxiliary tasks, and worker lifecycle reporting.
//!
//! Design note:
//! - These enums define the canonical wire names that runtime preview stabilizes before
//!   queue, retrieval, flow, and worker business logic is expanded.
//! - Backward-compatible aliases keep persisted records and existing UI payloads
//!   readable while new surfaces emit only the canonical forms.
//! - Intentionally deferred variants stay out of this module until the
//!   corresponding behavior is implemented and covered by rollout/config
//!   guardrails, diagnostics, and regression harnesses.
//! - Wire names, aliases, and serialized shapes are pinned by the runtime-contract
//!   snapshot gate (`scripts/test/check-runtime-contract-snapshots.sh`): add aliases
//!   instead of renaming canonical strings.
mod error_taxonomy;

pub use error_taxonomy::*;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeMap, fmt};

/// Schema version for the public runtime contract snapshot emitted by this crate.
pub const PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
/// Version identifier for the current public runtime contract snapshot.
pub const PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION: &str = "runtime-contracts.v8";

/// One canonical runtime enum wire value plus deprecated aliases that must keep parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RuntimeContractEnumValue {
    /// Canonical value emitted by new payloads.
    pub canonical: &'static str,
    /// Previously public wire names still accepted on input.
    pub deprecated_aliases: &'static [&'static str],
}

macro_rules! runtime_contract_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $(
                $variant:ident => $canonical:literal $(| $alias:literal )*
            ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub enum $name {
            $(
                #[serde(rename = $canonical $(, alias = $alias)*)]
                $variant,
            )+
        }

        impl $name {
            /// Returns the canonical wire name for this variant.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(
                        Self::$variant => $canonical,
                    )+
                }
            }

            /// Stable enum values covered by the public runtime contract snapshot.
            pub const WIRE_CONTRACT_VALUES: &'static [RuntimeContractEnumValue] = &[
                $(
                    RuntimeContractEnumValue {
                        canonical: $canonical,
                        deprecated_aliases: &[$($alias),*],
                    },
                )+
            ];

            /// Returns canonical wire values and deprecated aliases for snapshot gates.
            #[must_use]
            pub const fn wire_contract_values() -> &'static [RuntimeContractEnumValue] {
                Self::WIRE_CONTRACT_VALUES
            }

            /// Parses a canonical wire name or backward-compatible alias.
            ///
            /// Matching is case-insensitive and ignores surrounding whitespace.
            #[must_use]
            pub fn parse(value: &str) -> Option<Self> {
                let normalized = value.trim().to_ascii_lowercase();
                match normalized.as_str() {
                    $(
                        $canonical $(| $alias )* => Some(Self::$variant),
                    )+
                    _ => None,
                }
            }

            /// Option-returning alias for [`Self::parse`], kept alongside the
            /// `FromStr` impl for call-site ergonomics.
            #[allow(clippy::should_implement_trait)]
            #[must_use]
            pub fn from_str(value: &str) -> Option<Self> {
                Self::parse(value)
            }
        }

        impl std::str::FromStr for $name {
            type Err = ();

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::parse(value).ok_or(())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

/// Builds a public snapshot covering runtime wire enums, hook vocabularies, tool result
/// projection contracts, and stable error envelopes.
#[must_use]
pub fn public_runtime_contract_snapshot() -> Value {
    json!({
        "schema_version": PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_SCHEMA_VERSION,
        "snapshot_version": PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION,
        "changelog_note": "Adds the always-on bounded metadata trace contract while preserving rich trace as a separate approval-gated export.",
        "compatibility_policy": compatibility_policy_snapshot(),
        "runtime_error_contract": runtime_error_contract_snapshot(),
        "metadata_trace": {
            "snapshot_version": "runtime-contracts.metadata_trace.v1",
            "changelog_note": "Introduces a bounded metadata-only run trace with append-only crash-safe segments and a separate approval-gated rich export.",
            "schema_version": crate::metadata_trace::METADATA_TRACE_SCHEMA_VERSION,
            "ordering": "segment_index_then_global_event_sequence",
            "segment_statuses": ["complete", "interrupted", "corrupt_suffix_isolated"],
            "event_kinds": [
                "run_started",
                "runtime_selected",
                "context_assembled",
                "provider_attempt",
                "tool_gate",
                "approval",
                "tool_outcome",
                "recovery",
                "delivery_intent",
                "terminalization",
                "recovery_continuation",
                "capacity_reached"
            ],
            "hard_limits": {
                "segments_per_run": crate::metadata_trace::METADATA_TRACE_MAX_SEGMENTS,
                "events_per_run": crate::metadata_trace::METADATA_TRACE_MAX_EVENTS,
                "bytes_per_event": crate::metadata_trace::METADATA_TRACE_MAX_EVENT_BYTES,
                "schema_hashes_per_selection": crate::metadata_trace::METADATA_TRACE_MAX_SCHEMA_HASHES,
                "stage_duration_ms": crate::metadata_trace::METADATA_TRACE_MAX_STAGE_DURATION_MS,
            },
            "privacy": {
                "raw_prompts": false,
                "raw_secrets": false,
                "raw_tool_arguments": false,
                "raw_provider_payloads": false,
                "raw_stderr": false,
                "identity_projection": "domain_separated_sha256",
            },
            "rich_trace": {
                "always_on": false,
                "approval_required": true,
                "separate_artifact": true,
            },
        },
        "public_runtime_events": public_runtime_event_taxonomy_snapshot(),
        "runtime_enums": [
            enum_contract_snapshot(
                "RunLifecyclePhase",
                "runtime-contracts.run_lifecycle_phase.v1",
                "Run states keep legacy persisted labels as deprecated aliases.",
                RunLifecyclePhase::wire_contract_values(),
            ),
            enum_contract_snapshot(
                "RuntimeActorKind",
                "runtime-contracts.runtime_actor_kind.v1",
                "Audit actor names are stable and user remains an alias for principal.",
                RuntimeActorKind::wire_contract_values(),
            ),
            enum_contract_snapshot(
                "IdempotencyOperationState",
                "runtime-contracts.idempotency_operation_state.v1",
                "Idempotency state labels preserve in_progress and succeeded aliases.",
                IdempotencyOperationState::wire_contract_values(),
            ),
            enum_contract_snapshot(
                "IdempotencyReplayDecision",
                "runtime-contracts.idempotency_replay_decision.v1",
                "Replay decision names are canonical public reason codes.",
                IdempotencyReplayDecision::wire_contract_values(),
            ),
        ],
        "hook_enums": [
            enum_contract_snapshot(
                "RunLifecycleHookPhase",
                "runtime-contracts.run_lifecycle_hook_phase.v1",
                "Hook phases accept short names and legacy run:* event names.",
                RunLifecycleHookPhase::wire_contract_values(),
            ),
            enum_contract_snapshot(
                "RunLifecycleHookDecisionKind",
                "runtime-contracts.run_lifecycle_hook_decision_kind.v1",
                "Hook decision names are arbitration inputs and may not be renamed without a migration.",
                RunLifecycleHookDecisionKind::wire_contract_values(),
            ),
        ],
        "agent_hooks": agent_hook_contract_snapshot(),
        "agent_harness": agent_harness_contract_snapshot(),
        "tool_result_projection": {
            "snapshot_version": "runtime-contracts.tool_result_projection.v1",
            "changelog_note": "Tool result projection wire names pin visibility, policy, decision, sensitivity, and retention output.",
            "enums": [
                enum_contract_snapshot(
                    "ToolResultVisibility",
                    "runtime-contracts.tool_result_visibility.v1",
                    "Visibility values define what reaches the model, audit artifact, or redacted preview.",
                    ToolResultVisibility::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "ToolResultProjectionPolicyKind",
                    "runtime-contracts.tool_result_projection_policy_kind.v1",
                    "Projection policy names are policy-visible and persisted in audit records.",
                    ToolResultProjectionPolicyKind::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "ToolResultProjectionDecisionKind",
                    "runtime-contracts.tool_result_projection_decision_kind.v1",
                    "Projection decisions are audit-visible and must remain stable.",
                    ToolResultProjectionDecisionKind::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "ToolResultSensitivity",
                    "runtime-contracts.tool_result_sensitivity.v1",
                    "Sensitivity labels drive artifact read gates and must not be broadened silently.",
                    ToolResultSensitivity::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "ArtifactRetentionDisposition",
                    "runtime-contracts.artifact_retention_disposition.v1",
                    "Retention dispositions are durable artifact lifecycle vocabulary.",
                    ArtifactRetentionDisposition::wire_contract_values(),
                ),
            ],
            "middleware": tool_result_middleware_contract_snapshot(),
        },
        "realtime_protocol": {
            "snapshot_version": "runtime-contracts.realtime.v1",
            "changelog_note": "Realtime handshake, command, event, and error vocabularies are pinned for console and agent clients.",
            "protocol_versions": {
                "min": REALTIME_PROTOCOL_MIN_VERSION,
                "max": REALTIME_PROTOCOL_MAX_VERSION,
            },
            "enums": [
                enum_contract_snapshot(
                    "RealtimeRole",
                    "runtime-contracts.realtime_role.v1",
                    "Realtime roles are negotiated during handshake.",
                    RealtimeRole::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "RealtimeScope",
                    "runtime-contracts.realtime_scope.v1",
                    "Realtime scopes are authorization inputs and must remain explicit.",
                    RealtimeScope::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "RealtimeCapability",
                    "runtime-contracts.realtime_capability.v1",
                    "Realtime capabilities are feature negotiation outputs.",
                    RealtimeCapability::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "RealtimeCommand",
                    "runtime-contracts.realtime_command.v1",
                    "Realtime command names are public method identifiers.",
                    RealtimeCommand::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "RealtimeEventTopic",
                    "runtime-contracts.realtime_event_topic.v1",
                    "Realtime event topics drive subscription filtering.",
                    RealtimeEventTopic::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "RealtimeEventSensitivity",
                    "runtime-contracts.realtime_event_sensitivity.v1",
                    "Realtime event sensitivity labels are applied before serialization.",
                    RealtimeEventSensitivity::wire_contract_values(),
                ),
            ],
        },
        "acp_protocol": {
            "snapshot_version": "runtime-contracts.acp.v2",
            "changelog_note": "ACP bridge scopes, capabilities, commands, permission outcomes, and binding vocabularies are pinned; run.get is read-only run status.",
            "protocol_versions": {
                "min": ACP_PROTOCOL_MIN_VERSION,
                "max": ACP_PROTOCOL_MAX_VERSION,
            },
            "enums": [
                enum_contract_snapshot(
                    "AcpTransportKind",
                    "runtime-contracts.acp_transport_kind.v1",
                    "ACP transport labels identify the bridge ingress.",
                    AcpTransportKind::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "AcpScope",
                    "runtime-contracts.acp_scope.v1",
                    "ACP scopes are authorization inputs and must remain explicit.",
                    AcpScope::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "AcpCapability",
                    "runtime-contracts.acp_capability.v1",
                    "ACP capabilities are negotiated bridge features.",
                    AcpCapability::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "AcpCommand",
                    "runtime-contracts.acp_command.v2",
                    "ACP command names are public method identifiers; run.get exposes read-only run status and acp.status remains an alias.",
                    AcpCommand::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "AcpSessionMode",
                    "runtime-contracts.acp_session_mode.v1",
                    "ACP session modes are policy-visible execution states.",
                    AcpSessionMode::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "AcpPermissionDecision",
                    "runtime-contracts.acp_permission_decision.v1",
                    "Permission bridge outcomes mirror approval results.",
                    AcpPermissionDecision::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "AcpEventLedgerKind",
                    "runtime-contracts.acp_event_ledger_kind.v1",
                    "ACP ledger event kinds are retained for reconnect replay.",
                    AcpEventLedgerKind::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "ConversationBindingSensitivity",
                    "runtime-contracts.conversation_binding_sensitivity.v1",
                    "Conversation binding sensitivity drives connector-independent filtering.",
                    ConversationBindingSensitivity::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "ConversationBindingConflictState",
                    "runtime-contracts.conversation_binding_conflict_state.v1",
                    "Conversation binding conflict states are explain and repair inputs.",
                    ConversationBindingConflictState::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "AcpBindingConflictKind",
                    "runtime-contracts.acp_binding_conflict_kind.v1",
                    "ACP binding conflict kinds keep expired_reference as a deprecated alias.",
                    AcpBindingConflictKind::wire_contract_values(),
                ),
                enum_contract_snapshot(
                    "AcpBindingRepairActionKind",
                    "runtime-contracts.acp_binding_repair_action_kind.v1",
                    "ACP repair action names are audited apply inputs.",
                    AcpBindingRepairActionKind::wire_contract_values(),
                ),
            ],
        },
        "public_error_envelopes": [
            {
                "name": "PalyraErrorEnvelope",
                "snapshot_version": "runtime-contracts.palyra_error_envelope.v1",
                "changelog_note": "Canonical public API, CLI, and stream error envelope with stable category and reason code.",
                "required_fields": [
                    "schema_version",
                    "code",
                    "category",
                    "message",
                    "recovery_hint",
                    "retryable",
                    "redacted"
                ],
                "optional_fields": ["validation_errors"],
                "categories": PalyraErrorCategory::wire_contract_values(),
                "secret_material_allowed": false,
            },
            {
                "name": "StableErrorEnvelope",
                "snapshot_version": "runtime-contracts.stable_error_envelope.v1",
                "changelog_note": "Stable errors expose code, message, and recovery_hint only.",
                "required_fields": ["code", "message", "recovery_hint"],
                "secret_material_allowed": false,
            },
            {
                "name": "RealtimeErrorEnvelope",
                "snapshot_version": "runtime-contracts.realtime_error_envelope.v1",
                "changelog_note": "Realtime compatibility errors may include supported protocol version range.",
                "required_fields": ["error"],
                "optional_fields": ["supported_protocol_versions"],
                "secret_material_allowed": false,
            },
            {
                "name": "AcpErrorEnvelope",
                "snapshot_version": "runtime-contracts.acp_error_envelope.v1",
                "changelog_note": "ACP compatibility errors may include supported protocol version range.",
                "required_fields": ["error"],
                "optional_fields": ["supported_protocol_versions"],
                "secret_material_allowed": false,
            },
        ],
    })
}

/// Validates that a public contract snapshot carries version notes and no local paths or secrets.
///
/// # Errors
/// Returns a human-readable pointer to the offending field when snapshot metadata,
/// local absolute paths, or obvious secret markers are present.
pub fn validate_public_contract_snapshot(snapshot: &Value) -> Result<(), String> {
    validate_snapshot_metadata("$", snapshot)?;
    validate_snapshot_strings("$", snapshot)
}

fn compatibility_policy_snapshot() -> Value {
    json!({
        "snapshot_version": "runtime-contracts.compatibility_policy.v1",
        "changelog_note": "Breaking public contract changes require a snapshot version bump plus migration note in the same change.",
        "breaking_change_requires_version_bump": true,
        "breaking_change_requires_migration_note": true,
        "deprecated_wire_names_must_remain_aliases": true,
        "unknown_fields_fail_closed_where_declared": true,
    })
}

fn public_runtime_event_taxonomy_snapshot() -> Value {
    json!({
        "snapshot_version": "runtime-contracts.public_runtime_events.v1",
        "changelog_note": "Public stream vocabulary is shared by Responses-compatible streaming, runs events, QA Lab, trajectory export, ACP, and observability.",
        "event_schema_version": 1,
        "unknown_extension_fields": "preserve_and_ignore_unknown_fields",
        "event_names": PublicRuntimeEventName::wire_contract_values(),
        "visibility_levels": PublicRuntimeEventVisibility::wire_contract_values(),
        "redaction_postures": PublicRuntimeEventRedactionPosture::wire_contract_values(),
        "journal_mappings": PublicRuntimeEventJournalMapping::wire_contract_values(),
        "events": PUBLIC_RUNTIME_EVENT_DESCRIPTORS,
    })
}

fn agent_hook_contract_snapshot() -> Value {
    json!({
        "snapshot_version": "runtime-contracts.agent_hooks.v1",
        "changelog_note": "Agent hooks define stable hook names, default redaction, capability grants, timeouts, priorities, and decision shapes.",
        "kinds": enum_contract_snapshot(
            "AgentHookKind",
            "runtime-contracts.agent_hook_kind.v1",
            "Agent hook names are public plugin and harness extension points.",
            AgentHookKind::wire_contract_values(),
        ),
        "decision_authority": enum_contract_snapshot(
            "AgentHookDecisionAuthority",
            "runtime-contracts.agent_hook_decision_authority.v1",
            "Hook authority distinguishes observe-only hooks from hooks that may affect control flow.",
            AgentHookDecisionAuthority::wire_contract_values(),
        ),
        "capability_grants": enum_contract_snapshot(
            "AgentHookCapabilityGrant",
            "runtime-contracts.agent_hook_capability_grant.v1",
            "Hook capability grants are host-issued and plugins cannot self-escalate to raw or mutating data.",
            AgentHookCapabilityGrant::wire_contract_values(),
        ),
        "redaction_postures": enum_contract_snapshot(
            "AgentHookRedactionPosture",
            "runtime-contracts.agent_hook_redaction_posture.v1",
            "Hook redaction postures describe the strongest payload view available without additional trusted grants.",
            AgentHookRedactionPosture::wire_contract_values(),
        ),
        "decision_kinds": enum_contract_snapshot(
            "AgentHookDecisionKind",
            "runtime-contracts.agent_hook_decision_kind.v1",
            "Agent hook decision names cover run gates, delivery rewrites, and tool-result middleware outcomes.",
            AgentHookDecisionKind::wire_contract_values(),
        ),
        "descriptors": AGENT_HOOK_DESCRIPTORS,
    })
}

fn agent_harness_contract_snapshot() -> Value {
    json!({
        "snapshot_version": "runtime-contracts.agent_harness.v4",
        "changelog_note": "Agent harness contracts keep provider resolution, auth, transcript, workspace, sandbox, tool policy, callbacks, journal writes, and structured attempt results host-owned.",
        "selection_modes": enum_contract_snapshot(
            "AgentHarnessSelectionMode",
            "runtime-contracts.agent_harness_selection_mode.v2",
            "Harness selection modes are policy-visible and must not silently fall back for mutating attempts.",
            AgentHarnessSelectionMode::wire_contract_values(),
        ),
        "support_outcomes": enum_contract_snapshot(
            "AgentHarnessSupportOutcome",
            "runtime-contracts.agent_harness_support_outcome.v1",
            "Harness support outcomes are audited before an attempt is routed.",
            AgentHarnessSupportOutcome::wire_contract_values(),
        ),
        "callback_kinds": enum_contract_snapshot(
            "AgentHarnessCallbackKind",
            "runtime-contracts.agent_harness_callback_kind.v2",
            "Harness callbacks are the only supported path for reply, model, tool, approval, verification, lifecycle, and final outcome events.",
            AgentHarnessCallbackKind::wire_contract_values(),
        ),
        "attempt_terminal_statuses": enum_contract_snapshot(
            "AgentHarnessAttemptTerminalStatus",
            "runtime-contracts.agent_harness_attempt_terminal_status.v1",
            "Harness attempt terminal statuses are stable across embedded, plugin, and future native runtimes.",
            AgentHarnessAttemptTerminalStatus::wire_contract_values(),
        ),
        "attempt_classifications": enum_contract_snapshot(
            "AgentHarnessAttemptClassification",
            "runtime-contracts.agent_harness_attempt_classification.v2",
            "Harness attempt classifications avoid generic provider/runtime error strings in diagnostics and replay.",
            AgentHarnessAttemptClassification::wire_contract_values(),
        ),
        "terminal_classifications": enum_contract_snapshot(
            "AgentHarnessTerminalClassification",
            "runtime-contracts.agent_harness_terminal_classification.v1",
            "Terminal classifications provide one shared wait, journal, and diagnostics vocabulary across harness owners.",
            AgentHarnessTerminalClassification::wire_contract_values(),
        ),
        "attempt_replay_safety": enum_contract_snapshot(
            "AgentHarnessAttemptReplaySafety",
            "runtime-contracts.agent_harness_attempt_replay_safety.v1",
            "Harness attempt replay-safety labels distinguish deterministic replay from side-effect-uncertain attempts.",
            AgentHarnessAttemptReplaySafety::wire_contract_values(),
        ),
        "prepared_attempt_schema": PREPARED_AGENT_ATTEMPT_SCHEMA,
    })
}

fn tool_result_middleware_contract_snapshot() -> Value {
    json!({
        "snapshot_version": "runtime-contracts.tool_result_middleware.v1",
        "changelog_note": "Runtime-neutral tool-result middleware may only preserve or downgrade model visibility while keeping audit artifacts host-owned.",
        "phases": [
            AgentHookKind::BeforeToolResultProject,
            AgentHookKind::ToolResultProjected,
            AgentHookKind::ToolResultPersist,
            AgentHookKind::ToolResultModelFeed,
        ],
        "visibility_order_low_to_high": [
            ToolResultVisibility::AuditArtifact,
            ToolResultVisibility::RedactedPreview,
            ToolResultVisibility::ModelSummary,
            ToolResultVisibility::ModelInline,
        ],
        "allowed_decisions": TOOL_RESULT_MIDDLEWARE_DECISIONS,
        "host_policy_authoritative": true,
        "visibility_escalation_allowed": false,
    })
}

fn enum_contract_snapshot(
    name: &'static str,
    snapshot_version: &'static str,
    changelog_note: &'static str,
    values: &'static [RuntimeContractEnumValue],
) -> Value {
    json!({
        "name": name,
        "snapshot_version": snapshot_version,
        "changelog_note": changelog_note,
        "values": values,
    })
}

fn validate_snapshot_metadata(path: &str, value: &Value) -> Result<(), String> {
    match value {
        Value::Object(fields) => {
            if fields.contains_key("snapshot_version") {
                require_non_empty_string(fields.get("snapshot_version"), path, "snapshot_version")?;
                require_non_empty_string(fields.get("changelog_note"), path, "changelog_note")?;
            }
            for (key, child) in fields {
                let child_path = format!("{path}/{key}");
                validate_snapshot_metadata(child_path.as_str(), child)?;
            }
            Ok(())
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                let child_path = format!("{path}/{index}");
                validate_snapshot_metadata(child_path.as_str(), child)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn require_non_empty_string(value: Option<&Value>, path: &str, field: &str) -> Result<(), String> {
    match value.and_then(Value::as_str).map(str::trim) {
        Some(text) if !text.is_empty() => Ok(()),
        _ => Err(format!("{path} has snapshot_version but missing non-empty {field}")),
    }
}

fn validate_snapshot_strings(path: &str, value: &Value) -> Result<(), String> {
    match value {
        Value::String(text) => {
            if contains_local_absolute_path(text) {
                return Err(format!("{path} contains a local absolute path"));
            }
            if contains_obvious_secret_marker(text) {
                return Err(format!("{path} contains an obvious secret marker"));
            }
            Ok(())
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                let child_path = format!("{path}/{index}");
                validate_snapshot_strings(child_path.as_str(), child)?;
            }
            Ok(())
        }
        Value::Object(fields) => {
            for (key, child) in fields {
                let child_path = format!("{path}/{key}");
                validate_snapshot_strings(child_path.as_str(), child)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn contains_local_absolute_path(text: &str) -> bool {
    if text.starts_with("\\\\") {
        return true;
    }
    let normalized = text.replace('\\', "/").to_ascii_lowercase();
    normalized.starts_with("/users/")
        || normalized.starts_with("/home/")
        || normalized.starts_with("/tmp/")
        || normalized.starts_with("/var/folders/")
        || normalized
            .as_bytes()
            .windows(3)
            .any(|window| window[0].is_ascii_alphabetic() && window[1] == b':' && window[2] == b'/')
}

fn contains_obvious_secret_marker(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    text.contains("-----BEGIN ")
        || lower.contains("api_key=")
        || lower.contains("access_token=")
        || lower.contains("secret_token=")
        || lower.contains("bearer ")
}

runtime_contract_enum! {
    /// Public event names shared by runtime streams, QA exports, ACP bridges, and observability.
    pub enum PublicRuntimeEventName {
        RunCreated => "run.created",
        RunQueued => "run.queued",
        RunStarted => "run.started",
        ModelDelta => "model.delta",
        ToolCallStarted => "tool.call.started",
        ToolCallDelta => "tool.call.delta",
        ToolCallCompleted => "tool.call.completed",
        ApprovalRequired => "approval.required",
        ApprovalResolved => "approval.resolved",
        VerificationNudge => "verification.nudge",
        RunCompleted => "run.completed",
        RunFailed => "run.failed",
        RunCancelled => "run.cancelled",
        Heartbeat => "heartbeat"
    }
}

impl PublicRuntimeEventName {
    /// Returns the public descriptor pinned by the runtime contract snapshot.
    #[must_use]
    pub fn descriptor(self) -> &'static PublicRuntimeEventDescriptor {
        PUBLIC_RUNTIME_EVENT_DESCRIPTORS
            .iter()
            .find(|descriptor| descriptor.name == self)
            .expect("every public runtime event name must have a descriptor")
    }

    /// Returns `true` for terminal run lifecycle events.
    #[must_use]
    pub const fn is_terminal_run_event(self) -> bool {
        matches!(self, Self::RunCompleted | Self::RunFailed | Self::RunCancelled)
    }
}

runtime_contract_enum! {
    /// Public visibility level applied before event serialization.
    pub enum PublicRuntimeEventVisibility {
        Public => "public",
        Operator => "operator",
        Internal => "internal",
        Sensitive => "sensitive"
    }
}

runtime_contract_enum! {
    /// Redaction posture required before a public runtime event may be emitted.
    pub enum PublicRuntimeEventRedactionPosture {
        MetadataOnly => "metadata_only",
        RedactedText => "redacted_text",
        RedactedJson => "redacted_json",
        NoPayloadSecrets => "no_payload_secrets"
    }
}

runtime_contract_enum! {
    /// Durable journal surface that should receive the event, when any.
    pub enum PublicRuntimeEventJournalMapping {
        RunLifecycleEvents => "run_lifecycle_events",
        OrchestratorTape => "orchestrator_tape",
        ApprovalPrompts => "approval_prompts",
        VerificationJournal => "verification_journal",
        EphemeralHeartbeat => "ephemeral_heartbeat"
    }
}

/// One field in a public runtime event payload schema descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PublicRuntimeEventPayloadField {
    /// Field name inside `payload`.
    pub name: &'static str,
    /// Compact JSON type label or named schema reference.
    pub type_name: &'static str,
    /// Whether producers must include the field.
    pub required: bool,
    /// Whether the field must be redacted or sanitized before public emission.
    pub redacted: bool,
}

/// Snapshot-pinned public event descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PublicRuntimeEventDescriptor {
    /// Canonical event name.
    pub name: PublicRuntimeEventName,
    /// Per-event schema version.
    pub schema_version: u32,
    /// Required correlation identifiers inside the event envelope.
    pub required_correlation_ids: &'static [&'static str],
    /// Public visibility level.
    pub visibility: PublicRuntimeEventVisibility,
    /// Required redaction posture.
    pub redaction: PublicRuntimeEventRedactionPosture,
    /// Journal mapping for replay, audit, or ephemeral streams.
    pub journal_mapping: PublicRuntimeEventJournalMapping,
    /// Coarse ordering phase for diagnostics and snapshot readers.
    pub ordering_phase: u16,
    /// Payload schema fields for compatibility tests and QA Lab assertions.
    pub payload_schema: &'static [PublicRuntimeEventPayloadField],
    /// Forward-compatibility policy for fields not known by this version.
    pub unknown_extension_fields: &'static str,
}

const RUN_CORRELATION_IDS: &[&str] = &["run_id", "session_id"];
const TOOL_CORRELATION_IDS: &[&str] = &["run_id", "session_id", "tool_call_id"];
const APPROVAL_CORRELATION_IDS: &[&str] = &["run_id", "session_id", "tool_call_id", "approval_id"];

const RUN_EVENT_PAYLOAD_SCHEMA: &[PublicRuntimeEventPayloadField] = &[
    PublicRuntimeEventPayloadField {
        name: "status",
        type_name: "string",
        required: true,
        redacted: false,
    },
    PublicRuntimeEventPayloadField {
        name: "message",
        type_name: "string",
        required: false,
        redacted: true,
    },
    PublicRuntimeEventPayloadField {
        name: "reason_code",
        type_name: "string",
        required: false,
        redacted: false,
    },
];
const MODEL_DELTA_PAYLOAD_SCHEMA: &[PublicRuntimeEventPayloadField] = &[
    PublicRuntimeEventPayloadField {
        name: "delta",
        type_name: "string",
        required: true,
        redacted: true,
    },
    PublicRuntimeEventPayloadField {
        name: "is_final",
        type_name: "boolean",
        required: true,
        redacted: false,
    },
];
const TOOL_STARTED_PAYLOAD_SCHEMA: &[PublicRuntimeEventPayloadField] = &[
    PublicRuntimeEventPayloadField {
        name: "tool_name",
        type_name: "string",
        required: true,
        redacted: false,
    },
    PublicRuntimeEventPayloadField {
        name: "input_json",
        type_name: "object",
        required: true,
        redacted: true,
    },
    PublicRuntimeEventPayloadField {
        name: "approval_required",
        type_name: "boolean",
        required: true,
        redacted: false,
    },
];
const TOOL_DELTA_PAYLOAD_SCHEMA: &[PublicRuntimeEventPayloadField] = &[
    PublicRuntimeEventPayloadField {
        name: "kind",
        type_name: "string",
        required: true,
        redacted: false,
    },
    PublicRuntimeEventPayloadField {
        name: "reason",
        type_name: "string",
        required: false,
        redacted: true,
    },
];
const TOOL_COMPLETED_PAYLOAD_SCHEMA: &[PublicRuntimeEventPayloadField] = &[
    PublicRuntimeEventPayloadField {
        name: "success",
        type_name: "boolean",
        required: true,
        redacted: false,
    },
    PublicRuntimeEventPayloadField {
        name: "output_json",
        type_name: "object",
        required: false,
        redacted: true,
    },
    PublicRuntimeEventPayloadField {
        name: "error",
        type_name: "string",
        required: false,
        redacted: true,
    },
];
const APPROVAL_REQUIRED_PAYLOAD_SCHEMA: &[PublicRuntimeEventPayloadField] = &[
    PublicRuntimeEventPayloadField {
        name: "tool_name",
        type_name: "string",
        required: true,
        redacted: false,
    },
    PublicRuntimeEventPayloadField {
        name: "request_summary",
        type_name: "string",
        required: true,
        redacted: true,
    },
    PublicRuntimeEventPayloadField {
        name: "prompt",
        type_name: "ApprovalPrompt",
        required: true,
        redacted: true,
    },
];
const APPROVAL_RESOLVED_PAYLOAD_SCHEMA: &[PublicRuntimeEventPayloadField] = &[
    PublicRuntimeEventPayloadField {
        name: "approved",
        type_name: "boolean",
        required: true,
        redacted: false,
    },
    PublicRuntimeEventPayloadField {
        name: "reason",
        type_name: "string",
        required: true,
        redacted: true,
    },
    PublicRuntimeEventPayloadField {
        name: "decision_scope",
        type_name: "string",
        required: true,
        redacted: false,
    },
];
const VERIFICATION_NUDGE_PAYLOAD_SCHEMA: &[PublicRuntimeEventPayloadField] = &[
    PublicRuntimeEventPayloadField {
        name: "requirement",
        type_name: "string",
        required: true,
        redacted: false,
    },
    PublicRuntimeEventPayloadField {
        name: "reason",
        type_name: "string",
        required: true,
        redacted: true,
    },
];
const HEARTBEAT_PAYLOAD_SCHEMA: &[PublicRuntimeEventPayloadField] = &[
    PublicRuntimeEventPayloadField {
        name: "status",
        type_name: "string",
        required: true,
        redacted: false,
    },
    PublicRuntimeEventPayloadField {
        name: "interval_ms",
        type_name: "integer",
        required: false,
        redacted: false,
    },
    PublicRuntimeEventPayloadField {
        name: "server_time_unix_ms",
        type_name: "integer",
        required: false,
        redacted: false,
    },
];

/// Public runtime events accepted by stream, QA, ACP, and observability adapters.
pub const PUBLIC_RUNTIME_EVENT_DESCRIPTORS: &[PublicRuntimeEventDescriptor] = &[
    public_event_descriptor(
        PublicRuntimeEventName::RunCreated,
        RUN_CORRELATION_IDS,
        PublicRuntimeEventVisibility::Public,
        PublicRuntimeEventRedactionPosture::MetadataOnly,
        PublicRuntimeEventJournalMapping::RunLifecycleEvents,
        100,
        RUN_EVENT_PAYLOAD_SCHEMA,
    ),
    public_event_descriptor(
        PublicRuntimeEventName::RunQueued,
        RUN_CORRELATION_IDS,
        PublicRuntimeEventVisibility::Public,
        PublicRuntimeEventRedactionPosture::MetadataOnly,
        PublicRuntimeEventJournalMapping::RunLifecycleEvents,
        200,
        RUN_EVENT_PAYLOAD_SCHEMA,
    ),
    public_event_descriptor(
        PublicRuntimeEventName::RunStarted,
        RUN_CORRELATION_IDS,
        PublicRuntimeEventVisibility::Public,
        PublicRuntimeEventRedactionPosture::MetadataOnly,
        PublicRuntimeEventJournalMapping::RunLifecycleEvents,
        300,
        RUN_EVENT_PAYLOAD_SCHEMA,
    ),
    public_event_descriptor(
        PublicRuntimeEventName::ModelDelta,
        RUN_CORRELATION_IDS,
        PublicRuntimeEventVisibility::Public,
        PublicRuntimeEventRedactionPosture::RedactedText,
        PublicRuntimeEventJournalMapping::OrchestratorTape,
        400,
        MODEL_DELTA_PAYLOAD_SCHEMA,
    ),
    public_event_descriptor(
        PublicRuntimeEventName::ToolCallStarted,
        TOOL_CORRELATION_IDS,
        PublicRuntimeEventVisibility::Operator,
        PublicRuntimeEventRedactionPosture::RedactedJson,
        PublicRuntimeEventJournalMapping::OrchestratorTape,
        500,
        TOOL_STARTED_PAYLOAD_SCHEMA,
    ),
    public_event_descriptor(
        PublicRuntimeEventName::ApprovalRequired,
        APPROVAL_CORRELATION_IDS,
        PublicRuntimeEventVisibility::Operator,
        PublicRuntimeEventRedactionPosture::RedactedJson,
        PublicRuntimeEventJournalMapping::ApprovalPrompts,
        600,
        APPROVAL_REQUIRED_PAYLOAD_SCHEMA,
    ),
    public_event_descriptor(
        PublicRuntimeEventName::ApprovalResolved,
        APPROVAL_CORRELATION_IDS,
        PublicRuntimeEventVisibility::Operator,
        PublicRuntimeEventRedactionPosture::RedactedText,
        PublicRuntimeEventJournalMapping::ApprovalPrompts,
        700,
        APPROVAL_RESOLVED_PAYLOAD_SCHEMA,
    ),
    public_event_descriptor(
        PublicRuntimeEventName::ToolCallDelta,
        TOOL_CORRELATION_IDS,
        PublicRuntimeEventVisibility::Operator,
        PublicRuntimeEventRedactionPosture::RedactedText,
        PublicRuntimeEventJournalMapping::OrchestratorTape,
        800,
        TOOL_DELTA_PAYLOAD_SCHEMA,
    ),
    public_event_descriptor(
        PublicRuntimeEventName::ToolCallCompleted,
        TOOL_CORRELATION_IDS,
        PublicRuntimeEventVisibility::Operator,
        PublicRuntimeEventRedactionPosture::RedactedJson,
        PublicRuntimeEventJournalMapping::OrchestratorTape,
        900,
        TOOL_COMPLETED_PAYLOAD_SCHEMA,
    ),
    public_event_descriptor(
        PublicRuntimeEventName::VerificationNudge,
        RUN_CORRELATION_IDS,
        PublicRuntimeEventVisibility::Operator,
        PublicRuntimeEventRedactionPosture::RedactedText,
        PublicRuntimeEventJournalMapping::VerificationJournal,
        950,
        VERIFICATION_NUDGE_PAYLOAD_SCHEMA,
    ),
    public_event_descriptor(
        PublicRuntimeEventName::RunCompleted,
        RUN_CORRELATION_IDS,
        PublicRuntimeEventVisibility::Public,
        PublicRuntimeEventRedactionPosture::MetadataOnly,
        PublicRuntimeEventJournalMapping::RunLifecycleEvents,
        1_000,
        RUN_EVENT_PAYLOAD_SCHEMA,
    ),
    public_event_descriptor(
        PublicRuntimeEventName::RunFailed,
        RUN_CORRELATION_IDS,
        PublicRuntimeEventVisibility::Public,
        PublicRuntimeEventRedactionPosture::RedactedText,
        PublicRuntimeEventJournalMapping::RunLifecycleEvents,
        1_000,
        RUN_EVENT_PAYLOAD_SCHEMA,
    ),
    public_event_descriptor(
        PublicRuntimeEventName::RunCancelled,
        RUN_CORRELATION_IDS,
        PublicRuntimeEventVisibility::Public,
        PublicRuntimeEventRedactionPosture::MetadataOnly,
        PublicRuntimeEventJournalMapping::RunLifecycleEvents,
        1_000,
        RUN_EVENT_PAYLOAD_SCHEMA,
    ),
    public_event_descriptor(
        PublicRuntimeEventName::Heartbeat,
        &[],
        PublicRuntimeEventVisibility::Public,
        PublicRuntimeEventRedactionPosture::MetadataOnly,
        PublicRuntimeEventJournalMapping::EphemeralHeartbeat,
        0,
        HEARTBEAT_PAYLOAD_SCHEMA,
    ),
];

const fn public_event_descriptor(
    name: PublicRuntimeEventName,
    required_correlation_ids: &'static [&'static str],
    visibility: PublicRuntimeEventVisibility,
    redaction: PublicRuntimeEventRedactionPosture,
    journal_mapping: PublicRuntimeEventJournalMapping,
    ordering_phase: u16,
    payload_schema: &'static [PublicRuntimeEventPayloadField],
) -> PublicRuntimeEventDescriptor {
    PublicRuntimeEventDescriptor {
        name,
        schema_version: 1,
        required_correlation_ids,
        visibility,
        redaction,
        journal_mapping,
        ordering_phase,
        payload_schema,
        unknown_extension_fields: "preserve_and_ignore_unknown_fields",
    }
}

/// Correlation identifiers carried by every public runtime event.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicRuntimeEventCorrelation {
    /// Runtime run id for run-scoped events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Session id that owns or produced the event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Parent run id when the event belongs to a delegated or nested run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    /// Tool call id for tool and approval events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    /// Approval prompt id for approval lifecycle events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    /// Transport request id when an adapter can correlate the event to an inbound request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

impl PublicRuntimeEventCorrelation {
    fn has_required_id(&self, field: &str) -> bool {
        match field {
            "run_id" => has_nonempty_value(self.run_id.as_deref()),
            "session_id" => has_nonempty_value(self.session_id.as_deref()),
            "parent_run_id" => has_nonempty_value(self.parent_run_id.as_deref()),
            "tool_call_id" => has_nonempty_value(self.tool_call_id.as_deref()),
            "approval_id" => has_nonempty_value(self.approval_id.as_deref()),
            "request_id" => has_nonempty_value(self.request_id.as_deref()),
            _ => false,
        }
    }
}

/// Public runtime event envelope. Unknown fields are preserved as extension fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicRuntimeEventEnvelope {
    /// Event schema version; currently pinned to the descriptor schema version.
    pub schema_version: u32,
    /// Canonical public event name.
    pub event: PublicRuntimeEventName,
    /// Unique event id within the emitting stream or journal.
    pub event_id: String,
    /// Event timestamp in Unix milliseconds.
    pub occurred_at_unix_ms: i64,
    /// Correlation ids used by clients, QA Lab, and observability.
    pub correlation: PublicRuntimeEventCorrelation,
    /// Visibility level applied before serialization.
    pub visibility: PublicRuntimeEventVisibility,
    /// Required redaction posture for the payload.
    pub redaction: PublicRuntimeEventRedactionPosture,
    /// Durable journal surface associated with this event.
    pub journal_mapping: PublicRuntimeEventJournalMapping,
    /// Event-specific payload described by the taxonomy snapshot.
    #[serde(default)]
    pub payload: Value,
    /// Unknown forward-compatible fields preserved across deserialize/serialize.
    #[serde(default, flatten)]
    pub extensions: BTreeMap<String, Value>,
}

/// Validation failure for public runtime event grammar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicRuntimeEventValidationError {
    /// Stable machine-readable validation code.
    pub code: &'static str,
    /// Safe human-readable validation detail.
    pub message: String,
}

impl PublicRuntimeEventValidationError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }
}

impl fmt::Display for PublicRuntimeEventValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for PublicRuntimeEventValidationError {}

/// Parses and validates a public runtime event name.
///
/// # Errors
/// Returns a validation error when the event name is not in the public taxonomy.
pub fn validate_public_runtime_event_name(
    raw: &str,
) -> Result<PublicRuntimeEventName, PublicRuntimeEventValidationError> {
    let Some(name) = PublicRuntimeEventName::parse(raw) else {
        return Err(PublicRuntimeEventValidationError::new(
            "unknown_event",
            format!("unknown public runtime event '{raw}'"),
        ));
    };
    Ok(name)
}

/// Validates one public runtime event envelope against the pinned grammar.
///
/// # Errors
/// Returns a validation error for schema-version mismatches, missing correlation
/// identifiers, descriptor metadata drift, or heartbeat payloads that contain
/// model tokens.
pub fn validate_public_runtime_event(
    event: &PublicRuntimeEventEnvelope,
) -> Result<(), PublicRuntimeEventValidationError> {
    if event.event_id.trim().is_empty() {
        return Err(PublicRuntimeEventValidationError::new(
            "missing_event_id",
            "public runtime event_id must be non-empty",
        ));
    }
    if event.occurred_at_unix_ms < 0 {
        return Err(PublicRuntimeEventValidationError::new(
            "invalid_timestamp",
            "public runtime event timestamp must be non-negative",
        ));
    }
    let descriptor = event.event.descriptor();
    if event.schema_version != descriptor.schema_version {
        return Err(PublicRuntimeEventValidationError::new(
            "schema_version_mismatch",
            format!(
                "event {} schema_version {} does not match descriptor version {}",
                event.event.as_str(),
                event.schema_version,
                descriptor.schema_version
            ),
        ));
    }
    if event.visibility != descriptor.visibility
        || event.redaction != descriptor.redaction
        || event.journal_mapping != descriptor.journal_mapping
    {
        return Err(PublicRuntimeEventValidationError::new(
            "descriptor_metadata_mismatch",
            format!("event {} metadata does not match its descriptor", event.event.as_str()),
        ));
    }
    for required_id in descriptor.required_correlation_ids {
        if !event.correlation.has_required_id(required_id) {
            return Err(PublicRuntimeEventValidationError::new(
                "missing_correlation_id",
                format!("event {} requires correlation id {}", event.event.as_str(), required_id),
            ));
        }
    }
    if event.event == PublicRuntimeEventName::Heartbeat
        && contains_model_token_payload(&event.payload)
    {
        return Err(PublicRuntimeEventValidationError::new(
            "heartbeat_contains_model_token",
            "heartbeat events must not carry model token or delta payloads",
        ));
    }
    Ok(())
}

/// Validates ordering-sensitive public runtime event grammar for tests and QA Lab.
///
/// # Errors
/// Returns a validation error when an event is invalid, a tool or approval event
/// appears before its start/request event, or non-heartbeat events appear after a
/// terminal run event.
pub fn validate_public_runtime_event_sequence(
    events: &[PublicRuntimeEventEnvelope],
) -> Result<(), PublicRuntimeEventValidationError> {
    let mut seen_tool_calls = std::collections::BTreeSet::new();
    let mut seen_approvals = std::collections::BTreeSet::new();
    let mut terminal_run_event: Option<&'static str> = None;

    for event in events {
        validate_public_runtime_event(event)?;
        if let Some(terminal) = terminal_run_event {
            if event.event != PublicRuntimeEventName::Heartbeat {
                return Err(PublicRuntimeEventValidationError::new(
                    "event_after_terminal_run",
                    format!("event {} appeared after terminal event {terminal}", event.event),
                ));
            }
            continue;
        }

        match event.event {
            PublicRuntimeEventName::ToolCallStarted => {
                if let Some(tool_call_id) = event.correlation.tool_call_id.as_deref() {
                    seen_tool_calls.insert(tool_call_id.to_owned());
                }
            }
            PublicRuntimeEventName::ToolCallDelta | PublicRuntimeEventName::ToolCallCompleted => {
                let Some(tool_call_id) = event.correlation.tool_call_id.as_deref() else {
                    return Err(PublicRuntimeEventValidationError::new(
                        "missing_tool_call_id",
                        "tool event is missing tool_call_id",
                    ));
                };
                if !seen_tool_calls.contains(tool_call_id) {
                    return Err(PublicRuntimeEventValidationError::new(
                        "tool_event_before_start",
                        format!("tool event {} appeared before tool.call.started", event.event),
                    ));
                }
            }
            PublicRuntimeEventName::ApprovalRequired => {
                if let Some(approval_id) = event.correlation.approval_id.as_deref() {
                    seen_approvals.insert(approval_id.to_owned());
                }
            }
            PublicRuntimeEventName::ApprovalResolved => {
                let Some(approval_id) = event.correlation.approval_id.as_deref() else {
                    return Err(PublicRuntimeEventValidationError::new(
                        "missing_approval_id",
                        "approval.resolved is missing approval_id",
                    ));
                };
                if !seen_approvals.contains(approval_id) {
                    return Err(PublicRuntimeEventValidationError::new(
                        "approval_resolved_before_required",
                        "approval.resolved appeared before approval.required",
                    ));
                }
            }
            event_name if event_name.is_terminal_run_event() => {
                terminal_run_event = Some(event_name.as_str());
            }
            _ => {}
        }
    }
    Ok(())
}

#[must_use]
fn has_nonempty_value(value: Option<&str>) -> bool {
    value.is_some_and(|text| !text.trim().is_empty())
}

fn contains_model_token_payload(value: &Value) -> bool {
    match value {
        Value::Object(fields) => fields.iter().any(|(key, child)| {
            matches!(key.as_str(), "token" | "model_token" | "delta")
                || contains_model_token_payload(child)
        }),
        Value::Array(items) => items.iter().any(contains_model_token_payload),
        _ => false,
    }
}

runtime_contract_enum! {
    /// Canonical run lifecycle states shared by daemon, CLI/API, replay, and future realtime/ACP
    /// adapters. These are the public states; individual transports may still keep legacy labels
    /// internally and map them through this type at the boundary.
    pub enum RunLifecyclePhase {
        Queued => "queued" | "pending" | "accepted",
        Running => "running" | "in_progress" | "streaming",
        WaitingForApproval => "waiting_for_approval" | "approval_wait" | "awaiting_approval" | "waiting",
        Paused => "paused",
        Completed => "completed" | "done" | "succeeded",
        Failed => "failed",
        Aborted => "aborted" | "cancelled" | "canceled",
        Expired => "expired" | "timed_out" | "timeout"
    }
}

impl RunLifecyclePhase {
    /// Returns `true` for states a run can never transition out of.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Aborted | Self::Expired)
    }

    /// Returns `true` while the run is parked waiting on an approval or operator action.
    #[must_use]
    pub const fn is_waiting(self) -> bool {
        matches!(self, Self::WaitingForApproval | Self::Paused)
    }
}

runtime_contract_enum! {
    /// Sandboxed hook phases where a plugin can observe a run and return a typed decision.
    pub enum RunLifecycleHookPhase {
        BeforeRun => "before_run" | "run:before_run",
        BeforeTool => "before_tool" | "run:before_tool",
        AfterTool => "after_tool" | "run:after_tool",
        BeforeDelivery => "before_delivery" | "run:before_delivery",
        AfterRun => "after_run" | "run:after_run"
    }
}

impl RunLifecycleHookPhase {
    /// Returns the namespaced `run:*` event name used on event buses.
    #[must_use]
    pub fn event_name(self) -> &'static str {
        match self {
            Self::BeforeRun => "run:before_run",
            Self::BeforeTool => "run:before_tool",
            Self::AfterTool => "run:after_tool",
            Self::BeforeDelivery => "run:before_delivery",
            Self::AfterRun => "run:after_run",
        }
    }

    /// Parses either the short phase name or the namespaced `run:*` event name.
    #[must_use]
    pub fn parse_hook_event(raw: &str) -> Option<Self> {
        Self::parse(raw.trim().to_ascii_lowercase().as_str())
    }
}

runtime_contract_enum! {
    /// Decision returned by a run lifecycle hook after sandboxed execution.
    pub enum RunLifecycleHookDecisionKind {
        Continue => "continue",
        Annotate => "annotate",
        RequestApproval => "request_approval",
        Block => "block",
        TransformPreview => "transform_preview",
        FailRun => "fail_run"
    }
}

impl RunLifecycleHookDecisionKind {
    /// Returns the arbitration weight for this decision kind; higher values win.
    ///
    /// Gaps between values are deliberate so new kinds can be inserted without
    /// renumbering the pinned ordering.
    #[must_use]
    pub const fn priority(self) -> u8 {
        match self {
            Self::Continue => 10,
            Self::Annotate => 20,
            Self::TransformPreview => 30,
            Self::RequestApproval => 40,
            Self::Block => 50,
            Self::FailRun => 60,
        }
    }

    /// Returns `true` for decisions that stop further processing of the phase.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::RequestApproval | Self::Block | Self::FailRun)
    }

    /// Reports whether this decision kind may be returned from the given hook phase.
    #[must_use]
    pub const fn is_allowed_in_phase(self, phase: RunLifecycleHookPhase) -> bool {
        match self {
            Self::Continue | Self::Annotate => true,
            Self::RequestApproval | Self::Block => matches!(
                phase,
                RunLifecycleHookPhase::BeforeRun
                    | RunLifecycleHookPhase::BeforeTool
                    | RunLifecycleHookPhase::BeforeDelivery
            ),
            Self::TransformPreview => matches!(phase, RunLifecycleHookPhase::BeforeDelivery),
            Self::FailRun => matches!(
                phase,
                RunLifecycleHookPhase::BeforeRun
                    | RunLifecycleHookPhase::BeforeTool
                    | RunLifecycleHookPhase::AfterTool
                    | RunLifecycleHookPhase::BeforeDelivery
            ),
        }
    }
}

runtime_contract_enum! {
    /// Stable hook extension points exposed to plugins, harnesses, and diagnostics.
    pub enum AgentHookKind {
        GatewayStartup => "gateway:startup",
        SkillEnabled => "skill:enabled",
        SkillQuarantined => "skill:quarantined",
        SkillDisabled => "skill:disabled",
        RunStarted => "run_started" | "run:started",
        RunFinished => "run_finished" | "run:finished",
        BeforeContextBuild => "before_context_build" | "context:before_build",
        AfterContextBuild => "after_context_build" | "context:after_build",
        BeforeToolPolicy => "before_tool_policy" | "tool:before_policy",
        AfterToolResult => "after_tool_result" | "tool:after_result",
        MemoryCandidateCreated => "memory_candidate_created" | "memory:candidate_created",
        LearningCandidateReviewed => "learning_candidate_reviewed" | "learning:candidate_reviewed",
        ArtifactCreated => "artifact_created" | "artifact:created",
        ApprovalRequested => "approval_requested" | "approval:requested",
        RunBeforeRun => "run:before_run" | "before_run",
        RunBeforeTool => "run:before_tool" | "before_tool",
        RunAfterTool => "run:after_tool" | "after_tool",
        RunBeforeDelivery => "run:before_delivery" | "before_delivery",
        RunAfterRun => "run:after_run" | "after_run",
        BeforeModelResolve => "before_model_resolve",
        BeforePromptBuild => "before_prompt_build",
        BeforeAgentRun => "before_agent_run",
        BeforeAgentReply => "before_agent_reply",
        BeforeAgentFinalize => "before_agent_finalize",
        AgentEnd => "agent_end",
        ModelCallStarted => "model_call_started",
        ModelCallEnded => "model_call_ended",
        BeforeToolCall => "before_tool_call",
        AfterToolCall => "after_tool_call",
        ToolResultPersist => "tool_result_persist",
        InboundClaim => "inbound_claim",
        BeforeMessageWrite => "before_message_write",
        MessageSending => "message_sending",
        ReplyPayloadSending => "reply_payload_sending",
        ReplyDispatch => "reply_dispatch",
        SessionStart => "session_start",
        SessionEnd => "session_end",
        BeforeReset => "before_reset",
        BeforeCompaction => "before_compaction",
        AfterCompaction => "after_compaction",
        SubagentSpawned => "subagent_spawned",
        SubagentEnded => "subagent_ended",
        BeforeToolResultProject => "before_tool_result_project",
        ToolResultProjected => "tool_result_projected",
        ToolResultModelFeed => "tool_result_model_feed"
    }
}

impl AgentHookKind {
    /// Returns `true` for the runtime-neutral tool-result middleware phases.
    #[must_use]
    pub const fn is_tool_result_middleware(self) -> bool {
        matches!(
            self,
            Self::BeforeToolResultProject
                | Self::ToolResultProjected
                | Self::ToolResultPersist
                | Self::ToolResultModelFeed
        )
    }

    /// Returns the public descriptor for this hook kind, when the kind is part of the agent
    /// hook surface rather than the legacy gateway/skill compatibility event set.
    #[must_use]
    pub fn descriptor(self) -> Option<&'static AgentHookDescriptor> {
        agent_hook_descriptor(self)
    }
}

runtime_contract_enum! {
    /// Whether a hook is observe-only or may return host-interpreted decisions.
    pub enum AgentHookDecisionAuthority {
        ObservationOnly => "observation_only",
        DecisionCapable => "decision_capable"
    }
}

runtime_contract_enum! {
    /// Capability grants the host may issue to a hook invocation.
    pub enum AgentHookCapabilityGrant {
        MetadataOnly => "metadata_only",
        RedactedPayload => "redacted_payload",
        RawPromptTrusted => "raw_prompt_trusted",
        DeliveryMutation => "delivery_mutation",
        ToolResultTransform => "tool_result_transform",
        ExecEnvResolve => "exec_env_resolve"
    }
}

runtime_contract_enum! {
    /// Default payload posture used before any trusted capability grant is applied.
    pub enum AgentHookRedactionPosture {
        MetadataOnly => "metadata_only",
        RedactedPayload => "redacted_payload",
        RedactedSummary => "redacted_summary",
        TrustedRawPayload => "trusted_raw_payload"
    }
}

runtime_contract_enum! {
    /// Decisions a hook or middleware phase may ask the host to apply.
    pub enum AgentHookDecisionKind {
        Continue => "continue",
        Annotate => "annotate",
        RequestApproval => "request_approval",
        Block => "block",
        TransformPreview => "transform_preview",
        ReplaceSummary => "replace_summary",
        AttachArtifact => "attach_artifact",
        RedactFields => "redact_fields",
        DowngradeVisibility => "downgrade_visibility",
        RequestFullReadApproval => "request_full_read_approval",
        FailPersistence => "fail_persistence",
        AuditOnly => "audit_only"
    }
}

impl AgentHookDecisionKind {
    /// Returns `true` when the decision can stop or fail the guarded operation.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::RequestApproval
                | Self::Block
                | Self::RequestFullReadApproval
                | Self::FailPersistence
        )
    }

    /// Returns `true` when the decision asks the host to mutate payload visible to another
    /// runtime stage.
    #[must_use]
    pub const fn mutates_payload(self) -> bool {
        matches!(
            self,
            Self::TransformPreview
                | Self::ReplaceSummary
                | Self::AttachArtifact
                | Self::RedactFields
                | Self::DowngradeVisibility
        )
    }
}

/// Public descriptor for one agent hook kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AgentHookDescriptor {
    /// Hook kind being described.
    pub kind: AgentHookKind,
    /// Whether the hook may return host-interpreted decisions.
    pub decision_authority: AgentHookDecisionAuthority,
    /// Default host-side timeout in milliseconds.
    pub default_timeout_ms: u64,
    /// Conflict-resolution priority; higher values are considered first.
    pub priority: u16,
    /// Default payload posture before additional trusted grants.
    pub redaction: AgentHookRedactionPosture,
    /// Capability grants the host may issue for this hook kind.
    pub allowed_capability_grants: &'static [AgentHookCapabilityGrant],
    /// Decisions accepted from this hook kind.
    pub allowed_decisions: &'static [AgentHookDecisionKind],
    /// Whether timeout or panic should fail the guarded runtime stage closed.
    pub fail_closed: bool,
    /// Stable audit event emitted for hook invocation or denial.
    pub audit_event: &'static str,
    /// Stable metrics bucket for hook duration.
    pub metrics_name: &'static str,
}

impl AgentHookDescriptor {
    /// Deterministic decision the host records when this hook times out.
    #[must_use]
    pub const fn timeout_decision(self) -> AgentHookDecisionKind {
        if self.fail_closed {
            if self.kind.is_tool_result_middleware() {
                AgentHookDecisionKind::FailPersistence
            } else {
                AgentHookDecisionKind::Block
            }
        } else {
            AgentHookDecisionKind::AuditOnly
        }
    }

    /// Returns `true` when all requested grants are within the descriptor's allowlist.
    #[must_use]
    pub fn permits_capability_grants(&self, requested: &[AgentHookCapabilityGrant]) -> bool {
        requested.iter().all(|grant| self.allowed_capability_grants.contains(grant))
    }

    /// Returns `true` when the hook accepts this decision kind.
    #[must_use]
    pub fn permits_decision(&self, decision: AgentHookDecisionKind) -> bool {
        self.allowed_decisions.contains(&decision)
    }
}

const NO_HOOK_DECISIONS: &[AgentHookDecisionKind] = &[];
const RUN_GATE_HOOK_DECISIONS: &[AgentHookDecisionKind] = &[
    AgentHookDecisionKind::Continue,
    AgentHookDecisionKind::Annotate,
    AgentHookDecisionKind::RequestApproval,
    AgentHookDecisionKind::Block,
];
const DELIVERY_HOOK_DECISIONS: &[AgentHookDecisionKind] = &[
    AgentHookDecisionKind::Continue,
    AgentHookDecisionKind::Annotate,
    AgentHookDecisionKind::RequestApproval,
    AgentHookDecisionKind::Block,
    AgentHookDecisionKind::TransformPreview,
];
/// Decisions accepted by the tool-result middleware pipeline.
pub const TOOL_RESULT_MIDDLEWARE_DECISIONS: &[AgentHookDecisionKind] = &[
    AgentHookDecisionKind::Continue,
    AgentHookDecisionKind::ReplaceSummary,
    AgentHookDecisionKind::AttachArtifact,
    AgentHookDecisionKind::RedactFields,
    AgentHookDecisionKind::DowngradeVisibility,
    AgentHookDecisionKind::RequestFullReadApproval,
    AgentHookDecisionKind::FailPersistence,
    AgentHookDecisionKind::AuditOnly,
];

const METADATA_HOOK_GRANTS: &[AgentHookCapabilityGrant] = &[AgentHookCapabilityGrant::MetadataOnly];
const REDACTED_HOOK_GRANTS: &[AgentHookCapabilityGrant] =
    &[AgentHookCapabilityGrant::MetadataOnly, AgentHookCapabilityGrant::RedactedPayload];
const PROMPT_HOOK_GRANTS: &[AgentHookCapabilityGrant] = &[
    AgentHookCapabilityGrant::MetadataOnly,
    AgentHookCapabilityGrant::RedactedPayload,
    AgentHookCapabilityGrant::RawPromptTrusted,
];
const DELIVERY_HOOK_GRANTS: &[AgentHookCapabilityGrant] = &[
    AgentHookCapabilityGrant::MetadataOnly,
    AgentHookCapabilityGrant::RedactedPayload,
    AgentHookCapabilityGrant::DeliveryMutation,
];
const TOOL_RESULT_HOOK_GRANTS: &[AgentHookCapabilityGrant] = &[
    AgentHookCapabilityGrant::MetadataOnly,
    AgentHookCapabilityGrant::RedactedPayload,
    AgentHookCapabilityGrant::ToolResultTransform,
];
const EXEC_ENV_HOOK_GRANTS: &[AgentHookCapabilityGrant] = &[
    AgentHookCapabilityGrant::MetadataOnly,
    AgentHookCapabilityGrant::RedactedPayload,
    AgentHookCapabilityGrant::ExecEnvResolve,
];

const fn observe_hook(
    kind: AgentHookKind,
    redaction: AgentHookRedactionPosture,
    priority: u16,
    grants: &'static [AgentHookCapabilityGrant],
) -> AgentHookDescriptor {
    AgentHookDescriptor {
        kind,
        decision_authority: AgentHookDecisionAuthority::ObservationOnly,
        default_timeout_ms: 500,
        priority,
        redaction,
        allowed_capability_grants: grants,
        allowed_decisions: NO_HOOK_DECISIONS,
        fail_closed: false,
        audit_event: "hook.observed",
        metrics_name: "hook.duration",
    }
}

const fn decision_hook(
    kind: AgentHookKind,
    redaction: AgentHookRedactionPosture,
    priority: u16,
    grants: &'static [AgentHookCapabilityGrant],
    decisions: &'static [AgentHookDecisionKind],
    fail_closed: bool,
) -> AgentHookDescriptor {
    AgentHookDescriptor {
        kind,
        decision_authority: AgentHookDecisionAuthority::DecisionCapable,
        default_timeout_ms: 1_000,
        priority,
        redaction,
        allowed_capability_grants: grants,
        allowed_decisions: decisions,
        fail_closed,
        audit_event: "hook.decision",
        metrics_name: "hook.duration",
    }
}

/// Agent hook descriptors pinned by the public runtime contract snapshot.
pub const AGENT_HOOK_DESCRIPTORS: &[AgentHookDescriptor] = &[
    decision_hook(
        AgentHookKind::RunBeforeRun,
        AgentHookRedactionPosture::MetadataOnly,
        800,
        REDACTED_HOOK_GRANTS,
        RUN_GATE_HOOK_DECISIONS,
        true,
    ),
    decision_hook(
        AgentHookKind::RunBeforeTool,
        AgentHookRedactionPosture::RedactedPayload,
        850,
        REDACTED_HOOK_GRANTS,
        RUN_GATE_HOOK_DECISIONS,
        true,
    ),
    observe_hook(
        AgentHookKind::RunAfterTool,
        AgentHookRedactionPosture::RedactedPayload,
        500,
        REDACTED_HOOK_GRANTS,
    ),
    decision_hook(
        AgentHookKind::RunBeforeDelivery,
        AgentHookRedactionPosture::RedactedPayload,
        850,
        DELIVERY_HOOK_GRANTS,
        DELIVERY_HOOK_DECISIONS,
        true,
    ),
    observe_hook(
        AgentHookKind::RunAfterRun,
        AgentHookRedactionPosture::MetadataOnly,
        400,
        METADATA_HOOK_GRANTS,
    ),
    decision_hook(
        AgentHookKind::BeforeModelResolve,
        AgentHookRedactionPosture::MetadataOnly,
        650,
        EXEC_ENV_HOOK_GRANTS,
        RUN_GATE_HOOK_DECISIONS,
        true,
    ),
    decision_hook(
        AgentHookKind::BeforePromptBuild,
        AgentHookRedactionPosture::MetadataOnly,
        700,
        PROMPT_HOOK_GRANTS,
        RUN_GATE_HOOK_DECISIONS,
        true,
    ),
    decision_hook(
        AgentHookKind::BeforeAgentRun,
        AgentHookRedactionPosture::RedactedSummary,
        760,
        REDACTED_HOOK_GRANTS,
        RUN_GATE_HOOK_DECISIONS,
        true,
    ),
    observe_hook(
        AgentHookKind::BeforeAgentReply,
        AgentHookRedactionPosture::RedactedPayload,
        500,
        REDACTED_HOOK_GRANTS,
    ),
    decision_hook(
        AgentHookKind::BeforeAgentFinalize,
        AgentHookRedactionPosture::RedactedSummary,
        700,
        REDACTED_HOOK_GRANTS,
        RUN_GATE_HOOK_DECISIONS,
        true,
    ),
    observe_hook(
        AgentHookKind::AgentEnd,
        AgentHookRedactionPosture::MetadataOnly,
        300,
        METADATA_HOOK_GRANTS,
    ),
    observe_hook(
        AgentHookKind::ModelCallStarted,
        AgentHookRedactionPosture::MetadataOnly,
        300,
        METADATA_HOOK_GRANTS,
    ),
    observe_hook(
        AgentHookKind::ModelCallEnded,
        AgentHookRedactionPosture::RedactedSummary,
        300,
        REDACTED_HOOK_GRANTS,
    ),
    decision_hook(
        AgentHookKind::BeforeToolCall,
        AgentHookRedactionPosture::RedactedPayload,
        820,
        REDACTED_HOOK_GRANTS,
        RUN_GATE_HOOK_DECISIONS,
        true,
    ),
    observe_hook(
        AgentHookKind::AfterToolCall,
        AgentHookRedactionPosture::RedactedPayload,
        400,
        REDACTED_HOOK_GRANTS,
    ),
    decision_hook(
        AgentHookKind::InboundClaim,
        AgentHookRedactionPosture::MetadataOnly,
        900,
        METADATA_HOOK_GRANTS,
        RUN_GATE_HOOK_DECISIONS,
        true,
    ),
    decision_hook(
        AgentHookKind::BeforeMessageWrite,
        AgentHookRedactionPosture::RedactedPayload,
        760,
        DELIVERY_HOOK_GRANTS,
        DELIVERY_HOOK_DECISIONS,
        true,
    ),
    decision_hook(
        AgentHookKind::MessageSending,
        AgentHookRedactionPosture::RedactedPayload,
        760,
        DELIVERY_HOOK_GRANTS,
        DELIVERY_HOOK_DECISIONS,
        true,
    ),
    decision_hook(
        AgentHookKind::ReplyPayloadSending,
        AgentHookRedactionPosture::RedactedPayload,
        760,
        DELIVERY_HOOK_GRANTS,
        DELIVERY_HOOK_DECISIONS,
        true,
    ),
    observe_hook(
        AgentHookKind::ReplyDispatch,
        AgentHookRedactionPosture::MetadataOnly,
        400,
        METADATA_HOOK_GRANTS,
    ),
    observe_hook(
        AgentHookKind::SessionStart,
        AgentHookRedactionPosture::MetadataOnly,
        350,
        METADATA_HOOK_GRANTS,
    ),
    observe_hook(
        AgentHookKind::SessionEnd,
        AgentHookRedactionPosture::MetadataOnly,
        350,
        METADATA_HOOK_GRANTS,
    ),
    decision_hook(
        AgentHookKind::BeforeReset,
        AgentHookRedactionPosture::MetadataOnly,
        800,
        METADATA_HOOK_GRANTS,
        RUN_GATE_HOOK_DECISIONS,
        true,
    ),
    decision_hook(
        AgentHookKind::BeforeCompaction,
        AgentHookRedactionPosture::RedactedSummary,
        650,
        REDACTED_HOOK_GRANTS,
        RUN_GATE_HOOK_DECISIONS,
        true,
    ),
    observe_hook(
        AgentHookKind::AfterCompaction,
        AgentHookRedactionPosture::RedactedSummary,
        350,
        REDACTED_HOOK_GRANTS,
    ),
    observe_hook(
        AgentHookKind::SubagentSpawned,
        AgentHookRedactionPosture::MetadataOnly,
        350,
        METADATA_HOOK_GRANTS,
    ),
    observe_hook(
        AgentHookKind::SubagentEnded,
        AgentHookRedactionPosture::RedactedSummary,
        350,
        REDACTED_HOOK_GRANTS,
    ),
    decision_hook(
        AgentHookKind::BeforeToolResultProject,
        AgentHookRedactionPosture::RedactedPayload,
        780,
        TOOL_RESULT_HOOK_GRANTS,
        TOOL_RESULT_MIDDLEWARE_DECISIONS,
        true,
    ),
    observe_hook(
        AgentHookKind::ToolResultProjected,
        AgentHookRedactionPosture::RedactedSummary,
        420,
        TOOL_RESULT_HOOK_GRANTS,
    ),
    decision_hook(
        AgentHookKind::ToolResultPersist,
        AgentHookRedactionPosture::RedactedPayload,
        780,
        TOOL_RESULT_HOOK_GRANTS,
        TOOL_RESULT_MIDDLEWARE_DECISIONS,
        true,
    ),
    decision_hook(
        AgentHookKind::ToolResultModelFeed,
        AgentHookRedactionPosture::RedactedSummary,
        780,
        TOOL_RESULT_HOOK_GRANTS,
        TOOL_RESULT_MIDDLEWARE_DECISIONS,
        true,
    ),
];

/// Returns the public descriptor for an agent hook kind.
#[must_use]
pub fn agent_hook_descriptor(kind: AgentHookKind) -> Option<&'static AgentHookDescriptor> {
    AGENT_HOOK_DESCRIPTORS.iter().find(|descriptor| descriptor.kind == kind)
}

runtime_contract_enum! {
    /// Host-owned policy for routing a prepared attempt to an agent harness.
    pub enum AgentHarnessSelectionMode {
        Embedded => "embedded" | "embedded_only",
        Auto => "auto",
        Explicit => "explicit",
        ExplicitPlugin => "explicit_plugin",
        PreferredPlugin => "preferred_plugin",
        NativeStub => "native_stub",
        ModelScoped => "model_scoped",
        ProviderScoped => "provider_scoped"
    }
}

runtime_contract_enum! {
    /// Result of asking a harness whether it supports a prepared attempt.
    pub enum AgentHarnessSupportOutcome {
        Declined => "declined",
        Supported => "supported",
        Preferred => "preferred"
    }
}

runtime_contract_enum! {
    /// Callback channels a harness may use to emit host-owned attempt events.
    pub enum AgentHarnessCallbackKind {
        PartialReply => "partial_reply",
        ModelTurnStarted => "model_turn_started",
        ModelToken => "model_token",
        ToolProposed => "tool_proposed",
        ApprovalRequested => "approval_requested",
        ToolStarted => "tool_started",
        ToolEvent => "tool_event",
        Progress => "progress",
        ToolResult => "tool_result",
        VerificationState => "verification_state",
        RecoveryPromptInjected => "recovery_prompt_injected",
        LifecycleEvent => "lifecycle_event",
        FinalOutcome => "final_outcome"
    }
}

runtime_contract_enum! {
    /// Terminal status returned by one harness attempt.
    pub enum AgentHarnessAttemptTerminalStatus {
        Completed => "completed",
        Blocked => "blocked",
        Failed => "failed",
        Cancelled => "cancelled",
        TimedOut => "timed_out",
        Yielded => "yielded"
    }
}

runtime_contract_enum! {
    /// Diagnostic classification for a harness attempt terminal outcome.
    pub enum AgentHarnessAttemptClassification {
        Ok => "ok",
        EmptyResponse => "empty_response",
        ReasoningOnly => "reasoning_only",
        PlanningOnly => "planning_only",
        PolicyBlocked => "policy_blocked",
        HookBlocked => "hook_blocked",
        ProviderError => "provider_error",
        MalformedProviderStream => "malformed_provider_stream",
        ToolError => "tool_error",
        ToolLoopGuardrail => "tool_loop_guardrail",
        SideEffectUncertain => "side_effect_uncertain",
        NativeRuntimeError => "native_runtime_error",
        ApprovalDenied => "approval_denied",
        InternalError => "internal_error",
        DeterministicFailure => "deterministic_failure"
    }
}

runtime_contract_enum! {
    /// Stable terminal classification vocabulary shared by wait, journal, and diagnostics paths.
    pub enum AgentHarnessTerminalClassification {
        Ok => "ok",
        Empty => "empty",
        Cancelled => "cancelled",
        Timeout => "timeout",
        ProviderError => "provider_error",
        ToolError => "tool_error",
        PolicyBlocked => "policy_blocked",
        ApprovalDenied => "approval_denied",
        InternalError => "internal_error",
        DeterministicFailure => "deterministic_failure"
    }
}

runtime_contract_enum! {
    /// Whether a completed or failed attempt can be replayed deterministically.
    pub enum AgentHarnessAttemptReplaySafety {
        ReplaySafe => "replay_safe",
        NotReplaySafe => "not_replay_safe",
        SideEffectUncertain => "side_effect_uncertain",
        Unknown => "unknown"
    }
}

/// Duration spent in one host-observed harness attempt phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHarnessAttemptPhaseTiming {
    /// Stable phase name such as `selection`, `context`, `provider`, or `tools`.
    pub phase: String,
    /// Monotonic elapsed milliseconds recorded by the host.
    pub elapsed_ms: u64,
}

/// Typed source metadata for a harness attempt error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHarnessAttemptErrorSummary {
    /// Error source family, for example `provider`, `policy`, `tool`, or `native_runtime`.
    pub source: String,
    /// Stable safe error code.
    pub code: String,
    /// Redacted operator-safe detail. Raw provider payloads and secrets must not appear here.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub safe_message: Option<String>,
}

impl AgentHarnessAttemptErrorSummary {
    /// Builds a redacted error summary.
    #[must_use]
    pub fn new(
        source: impl Into<String>,
        code: impl Into<String>,
        safe_message: Option<&str>,
    ) -> Self {
        Self {
            source: source.into(),
            code: code.into(),
            safe_message: safe_message.map(crate::redaction::redact_diagnostic_text),
        }
    }
}

/// Aggregate tool-call counters emitted by a harness attempt.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHarnessAttemptToolCallSummary {
    /// Tool calls proposed by the harness or model stream.
    pub proposed: u32,
    /// Tool calls executed through the host-owned bridge.
    pub executed: u32,
    /// Tool calls denied by policy, approval, or execution gate.
    pub denied: u32,
    /// Tool calls that reached the executor and failed.
    pub failed: u32,
}

/// Optional provider usage totals for an attempt.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHarnessAttemptUsageSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
}

/// Context shape observed by a prepared harness attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHarnessAttemptContextSummary {
    /// Context mode selected by the host, such as `palyra_owned` or `harness_provided`.
    pub context_mode: String,
    /// Maximum prompt/context budget made visible to the harness.
    pub token_budget: u64,
    /// Hash of the redacted context surface, if one was produced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub surface_hash: Option<String>,
}

/// Finalizer details safe for diagnostics and replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHarnessAttemptFinalizerSummary {
    /// Whether a final user-visible answer was produced.
    pub final_message_present: bool,
    /// Stable finish reason, when the provider/runtime supplied one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
}

/// Structured result returned by a host-owned harness attempt boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHarnessAttemptResult {
    /// Schema version for this result envelope.
    pub schema_version: u32,
    /// Terminal attempt status.
    pub terminal_status: AgentHarnessAttemptTerminalStatus,
    /// Typed outcome classification.
    pub classification: AgentHarnessAttemptClassification,
    /// Terminal classification shared across embedded, plugin, and native harness owners.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_classification: Option<AgentHarnessTerminalClassification>,
    /// Replay-safety posture for this attempt.
    pub replay_safety: AgentHarnessAttemptReplaySafety,
    /// Optional phase timing summaries.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub phase_timings: Vec<AgentHarnessAttemptPhaseTiming>,
    /// Optional redacted error details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<AgentHarnessAttemptErrorSummary>,
    /// Optional timeout source such as `provider`, `tool`, or `harness`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_source: Option<String>,
    /// Tool-call aggregate counts.
    pub tool_call_summary: AgentHarnessAttemptToolCallSummary,
    /// Optional provider usage counters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage_summary: Option<AgentHarnessAttemptUsageSummary>,
    /// Optional context summary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_summary: Option<AgentHarnessAttemptContextSummary>,
    /// Optional finalizer summary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalizer_summary: Option<AgentHarnessAttemptFinalizerSummary>,
    /// Redacted trace id for diagnostics correlation.
    pub diagnostic_trace_id: String,
}

impl AgentHarnessAttemptResult {
    /// Current schema version emitted by the host.
    pub const SCHEMA_VERSION: u32 = 1;

    /// Builds a minimal result with typed status, classification, replay safety, and trace id.
    #[must_use]
    pub fn minimal(
        terminal_status: AgentHarnessAttemptTerminalStatus,
        classification: AgentHarnessAttemptClassification,
        replay_safety: AgentHarnessAttemptReplaySafety,
        diagnostic_trace_id: impl Into<String>,
    ) -> Self {
        let diagnostic_trace_id = diagnostic_trace_id.into();
        Self {
            schema_version: Self::SCHEMA_VERSION,
            terminal_status,
            classification,
            terminal_classification: Some(classify_agent_harness_terminal(
                terminal_status,
                classification,
            )),
            replay_safety,
            phase_timings: Vec::new(),
            error: None,
            timeout_source: None,
            tool_call_summary: AgentHarnessAttemptToolCallSummary::default(),
            usage_summary: None,
            context_summary: None,
            finalizer_summary: None,
            diagnostic_trace_id: crate::redaction::redact_diagnostic_text(&diagnostic_trace_id),
        }
    }
}

/// Derives the host-wide terminal classification from a status/classification pair.
#[must_use]
pub const fn classify_agent_harness_terminal(
    terminal_status: AgentHarnessAttemptTerminalStatus,
    classification: AgentHarnessAttemptClassification,
) -> AgentHarnessTerminalClassification {
    match terminal_status {
        AgentHarnessAttemptTerminalStatus::Cancelled => {
            AgentHarnessTerminalClassification::Cancelled
        }
        AgentHarnessAttemptTerminalStatus::TimedOut => AgentHarnessTerminalClassification::Timeout,
        AgentHarnessAttemptTerminalStatus::Blocked => match classification {
            AgentHarnessAttemptClassification::ApprovalDenied => {
                AgentHarnessTerminalClassification::ApprovalDenied
            }
            _ => AgentHarnessTerminalClassification::PolicyBlocked,
        },
        AgentHarnessAttemptTerminalStatus::Completed
        | AgentHarnessAttemptTerminalStatus::Yielded => match classification {
            AgentHarnessAttemptClassification::EmptyResponse => {
                AgentHarnessTerminalClassification::Empty
            }
            AgentHarnessAttemptClassification::DeterministicFailure => {
                AgentHarnessTerminalClassification::DeterministicFailure
            }
            AgentHarnessAttemptClassification::InternalError
            | AgentHarnessAttemptClassification::NativeRuntimeError => {
                AgentHarnessTerminalClassification::InternalError
            }
            AgentHarnessAttemptClassification::ToolError
            | AgentHarnessAttemptClassification::ToolLoopGuardrail
            | AgentHarnessAttemptClassification::SideEffectUncertain => {
                AgentHarnessTerminalClassification::ToolError
            }
            AgentHarnessAttemptClassification::ProviderError
            | AgentHarnessAttemptClassification::MalformedProviderStream => {
                AgentHarnessTerminalClassification::ProviderError
            }
            AgentHarnessAttemptClassification::PolicyBlocked
            | AgentHarnessAttemptClassification::HookBlocked => {
                AgentHarnessTerminalClassification::PolicyBlocked
            }
            AgentHarnessAttemptClassification::ApprovalDenied => {
                AgentHarnessTerminalClassification::ApprovalDenied
            }
            AgentHarnessAttemptClassification::Ok
            | AgentHarnessAttemptClassification::ReasoningOnly
            | AgentHarnessAttemptClassification::PlanningOnly => {
                AgentHarnessTerminalClassification::Ok
            }
        },
        AgentHarnessAttemptTerminalStatus::Failed => match classification {
            AgentHarnessAttemptClassification::ProviderError
            | AgentHarnessAttemptClassification::MalformedProviderStream => {
                AgentHarnessTerminalClassification::ProviderError
            }
            AgentHarnessAttemptClassification::ToolError
            | AgentHarnessAttemptClassification::ToolLoopGuardrail
            | AgentHarnessAttemptClassification::SideEffectUncertain => {
                AgentHarnessTerminalClassification::ToolError
            }
            AgentHarnessAttemptClassification::PolicyBlocked
            | AgentHarnessAttemptClassification::HookBlocked => {
                AgentHarnessTerminalClassification::PolicyBlocked
            }
            AgentHarnessAttemptClassification::ApprovalDenied => {
                AgentHarnessTerminalClassification::ApprovalDenied
            }
            AgentHarnessAttemptClassification::DeterministicFailure => {
                AgentHarnessTerminalClassification::DeterministicFailure
            }
            AgentHarnessAttemptClassification::Ok
            | AgentHarnessAttemptClassification::EmptyResponse
            | AgentHarnessAttemptClassification::ReasoningOnly
            | AgentHarnessAttemptClassification::PlanningOnly
            | AgentHarnessAttemptClassification::NativeRuntimeError
            | AgentHarnessAttemptClassification::InternalError => {
                AgentHarnessTerminalClassification::InternalError
            }
        },
    }
}

/// Snapshot-pinned schema descriptor for the host-sanitized attempt object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PreparedAgentAttemptSchemaDescriptor {
    /// Schema version of the descriptor.
    pub schema_version: u32,
    /// Stable schema identifier.
    pub schema_id: &'static str,
    /// Fields visible to a harness after host sanitization.
    pub harness_visible_fields: &'static [&'static str],
    /// Fields and authorities retained by the host.
    pub host_owned_authorities: &'static [&'static str],
    /// Callback kinds exposed instead of direct journal writes.
    pub callback_kinds: &'static [AgentHarnessCallbackKind],
    /// Whether a harness may bypass the host journal API.
    pub direct_journal_write_allowed: bool,
    /// Whether tool execution and approval policy stay with the host.
    pub host_owns_tool_execution: bool,
}

/// Public schema descriptor for prepared agent attempts handed to native harnesses.
pub const PREPARED_AGENT_ATTEMPT_SCHEMA: PreparedAgentAttemptSchemaDescriptor =
    PreparedAgentAttemptSchemaDescriptor {
        schema_version: 1,
        schema_id: "runtime-contracts.prepared_agent_attempt.v1",
        harness_visible_fields: &[
            "run_id",
            "session_id",
            "provider",
            "model",
            "auth_state_metadata",
            "context_token_budget",
            "reasoning_policy",
            "sanitized_transcript_view",
            "tool_surface",
            "tool_policy",
            "workspace_root",
            "sandbox",
            "trace_context",
            "callbacks",
            "cancellation",
        ],
        host_owned_authorities: &[
            "provider_resolution",
            "credential_material",
            "raw_transcript",
            "workspace_policy",
            "sandbox_policy",
            "tool_execution",
            "approval_resolution",
            "journal_writes",
        ],
        callback_kinds: &[
            AgentHarnessCallbackKind::PartialReply,
            AgentHarnessCallbackKind::ModelTurnStarted,
            AgentHarnessCallbackKind::ModelToken,
            AgentHarnessCallbackKind::ToolProposed,
            AgentHarnessCallbackKind::ApprovalRequested,
            AgentHarnessCallbackKind::ToolStarted,
            AgentHarnessCallbackKind::ToolEvent,
            AgentHarnessCallbackKind::Progress,
            AgentHarnessCallbackKind::ToolResult,
            AgentHarnessCallbackKind::VerificationState,
            AgentHarnessCallbackKind::RecoveryPromptInjected,
            AgentHarnessCallbackKind::LifecycleEvent,
            AgentHarnessCallbackKind::FinalOutcome,
        ],
        direct_journal_write_allowed: false,
        host_owns_tool_execution: true,
    };

runtime_contract_enum! {
    /// Stable actor kind names for runtime audit records.
    pub enum RuntimeActorKind {
        System => "system",
        Principal => "principal" | "user",
        Agent => "agent",
        Connector => "connector",
        Scheduler => "scheduler",
        Worker => "worker",
        Policy => "policy",
        Replay => "replay"
    }
}

runtime_contract_enum! {
    /// Stable operation state for global idempotency records.
    pub enum IdempotencyOperationState {
        Started => "started" | "in_progress",
        Completed => "completed" | "succeeded",
        Failed => "failed",
        Expired => "expired"
    }
}

impl IdempotencyOperationState {
    /// Returns `true` for states an idempotency record can never transition out of.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Expired)
    }
}

runtime_contract_enum! {
    /// Result of checking a global idempotency key before a side effect is executed.
    pub enum IdempotencyReplayDecision {
        Reserved => "reserved",
        SamePayloadRetry => "same_payload_retry",
        CompletedReplayResult => "completed_replay_result",
        ConflictingPayload => "conflicting_payload",
        ExpiredRetry => "expired_retry"
    }
}

runtime_contract_enum! {
    /// How a tool result may be exposed after policy, sensitivity, and budget checks.
    pub enum ToolResultVisibility {
        ModelInline => "model_inline",
        ModelSummary => "model_summary",
        AuditArtifact => "audit_artifact",
        RedactedPreview => "redacted_preview"
    }
}

impl ToolResultVisibility {
    /// Ordering used by tool-result middleware checks; larger values expose more to the model.
    #[must_use]
    pub const fn model_visibility_rank(self) -> u8 {
        match self {
            Self::AuditArtifact => 0,
            Self::RedactedPreview => 1,
            Self::ModelSummary => 2,
            Self::ModelInline => 3,
        }
    }
}

/// Failure returned when middleware tries to broaden tool-result visibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolResultMiddlewarePolicyError {
    pub code: String,
    pub message: String,
}

/// Validates that middleware preserves or downgrades host-selected tool-result visibility.
///
/// # Errors
///
/// Returns `visibility_escalation_denied` when `requested` would expose more content to the
/// model than `host_visibility` permits.
pub fn validate_tool_result_visibility_downgrade(
    host_visibility: ToolResultVisibility,
    requested: ToolResultVisibility,
) -> Result<ToolResultVisibility, ToolResultMiddlewarePolicyError> {
    if requested.model_visibility_rank() <= host_visibility.model_visibility_rank() {
        return Ok(requested);
    }
    Err(ToolResultMiddlewarePolicyError {
        code: "visibility_escalation_denied".to_owned(),
        message: format!(
            "tool result middleware cannot raise visibility from {} to {}",
            host_visibility.as_str(),
            requested.as_str()
        ),
    })
}

runtime_contract_enum! {
    /// Registry policy that determines whether a tool result can stay inline.
    pub enum ToolResultProjectionPolicyKind {
        InlineUnlessLarge => "inline_unless_large",
        SummarizeAndArtifact => "summarize_and_artifact",
        RedactedPreviewAndArtifact => "redacted_preview_and_artifact"
    }
}

runtime_contract_enum! {
    /// Decision made for one concrete tool result after applying projection policy and budgets.
    pub enum ToolResultProjectionDecisionKind {
        ModelInline => "model_inline",
        SpilledToArtifact => "spilled_to_artifact"
    }
}

runtime_contract_enum! {
    /// Sensitivity taxonomy for durable tool result artifacts.
    pub enum ToolResultSensitivity {
        Public => "public",
        InternalPath => "internal_path",
        StdoutStderr => "stdout_stderr",
        PersonalData => "personal_data",
        Secret => "secret",
        ProviderRawPayload => "provider_raw_payload",
        ApprovalRiskData => "approval_risk_data"
    }
}

impl ToolResultSensitivity {
    /// Returns `true` when reading the full artifact payload requires the audit read gate.
    #[must_use]
    pub const fn requires_full_read_gate(self) -> bool {
        matches!(
            self,
            Self::InternalPath
                | Self::StdoutStderr
                | Self::PersonalData
                | Self::Secret
                | Self::ProviderRawPayload
                | Self::ApprovalRiskData
        )
    }
}

runtime_contract_enum! {
    /// Retention class for durable tool result artifacts.
    pub enum ArtifactRetentionDisposition {
        Keep => "keep",
        ExpireAfter => "expire_after",
        PurgeOnRequest => "purge_on_request",
        AuditLegalHold => "audit_legal_hold"
    }
}

runtime_contract_enum! {
    /// Coarse public error categories shared by API, CLI, stream events, providers, and tools.
    pub enum PalyraErrorCategory {
        Auth => "auth",
        Validation => "validation",
        Policy => "policy",
        Approval => "approval",
        Sandbox => "sandbox",
        Mcp => "mcp",
        ExecutionBackend => "execution_backend",
        Provider => "provider",
        Tool => "tool",
        Conflict => "conflict",
        NotFound => "not_found",
        RateLimit => "rate_limit",
        Dependency => "dependency",
        Availability => "availability",
        Internal => "internal"
    }
}

/// Stable error envelope used by runtime contracts instead of leaking internal debug strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StableErrorEnvelope {
    pub code: String,
    pub message: String,
    pub recovery_hint: String,
}

impl StableErrorEnvelope {
    /// Creates an envelope from a stable code, operator-facing message, and recovery hint.
    #[must_use]
    pub fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        recovery_hint: impl Into<String>,
    ) -> Self {
        Self { code: code.into(), message: message.into(), recovery_hint: recovery_hint.into() }
    }
}

/// One field-level validation issue inside a [`PalyraErrorEnvelope`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PalyraValidationIssue {
    /// Request field path that failed validation.
    pub field: String,
    /// Stable machine-readable issue code.
    pub code: String,
    /// Safe human-readable validation message.
    pub message: String,
}

/// Canonical public error envelope for API responses, CLI output, and stream events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PalyraErrorEnvelope {
    /// Envelope schema version; starts at 1 and changes only for incompatible shape updates.
    pub schema_version: u32,
    /// Stable reason code, usually `<surface>/<reason>`.
    pub code: String,
    /// Coarse failure category used for retry and UI behavior.
    pub category: PalyraErrorCategory,
    /// Safe human-readable message.
    pub message: String,
    /// Safe operator/client recovery hint.
    pub recovery_hint: String,
    /// Whether retrying the same request may succeed.
    pub retryable: bool,
    /// Whether sensitive detail was stripped from the message.
    #[serde(default)]
    pub redacted: bool,
    /// Field-level validation issues for validation failures.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub validation_errors: Vec<PalyraValidationIssue>,
}

impl PalyraErrorEnvelope {
    /// Creates a schema-version-1 envelope with no validation issues.
    #[must_use]
    pub fn new(
        category: PalyraErrorCategory,
        code: impl Into<String>,
        message: impl Into<String>,
        recovery_hint: impl Into<String>,
        retryable: bool,
        redacted: bool,
    ) -> Self {
        Self {
            schema_version: 1,
            code: code.into(),
            category,
            message: message.into(),
            recovery_hint: recovery_hint.into(),
            retryable,
            redacted,
            validation_errors: Vec::new(),
        }
    }

    /// Builds a public envelope from a compact runtime stable error.
    #[must_use]
    pub fn from_stable_error(
        category: PalyraErrorCategory,
        error: StableErrorEnvelope,
        retryable: bool,
        redacted: bool,
    ) -> Self {
        Self::new(category, error.code, error.message, error.recovery_hint, retryable, redacted)
    }

    /// Returns this public envelope as the compact stable runtime error shape.
    #[must_use]
    pub fn stable_error(&self) -> StableErrorEnvelope {
        StableErrorEnvelope::new(
            self.code.clone(),
            self.message.clone(),
            self.recovery_hint.clone(),
        )
    }

    /// Attaches validation issues and returns the updated envelope.
    #[must_use]
    pub fn with_validation_errors(mut self, validation_errors: Vec<PalyraValidationIssue>) -> Self {
        self.validation_errors = validation_errors;
        self
    }
}

/// Audit-visible identity of the actor that caused a runtime transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeActorRef {
    pub kind: RuntimeActorKind,
    pub id: String,
}

/// Canonical audit record for run lifecycle transitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLifecycleTransitionRecord {
    pub schema_version: u32,
    pub event_id: String,
    pub run_id: String,
    pub session_id: String,
    pub from_state: Option<RunLifecyclePhase>,
    pub to_state: RunLifecyclePhase,
    pub actor: RuntimeActorRef,
    pub correlation_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub reason: String,
    pub occurred_at_unix_ms: i64,
}

/// Typed decision emitted by one sandboxed run lifecycle hook invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunLifecycleHookDecision {
    pub schema_version: u32,
    pub phase: RunLifecycleHookPhase,
    pub kind: RunLifecycleHookDecisionKind,
    pub hook_id: String,
    pub plugin_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default)]
    pub annotations: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transformed_preview: Option<Value>,
}

impl RunLifecycleHookDecision {
    /// Creates a schema-version-1 decision with empty annotations and no preview transform.
    #[must_use]
    pub fn new(
        phase: RunLifecycleHookPhase,
        kind: RunLifecycleHookDecisionKind,
        hook_id: impl Into<String>,
        plugin_id: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: 1,
            phase,
            kind,
            hook_id: hook_id.into(),
            plugin_id: plugin_id.into(),
            reason: None,
            annotations: Value::Object(Default::default()),
            transformed_preview: None,
        }
    }
}

/// Deterministic aggregate of all decisions returned for a lifecycle phase.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunLifecycleHookResolution {
    pub schema_version: u32,
    pub phase: RunLifecycleHookPhase,
    pub selected: RunLifecycleHookDecision,
    pub candidates: Vec<RunLifecycleHookDecision>,
    #[serde(default)]
    pub conflicts: Vec<String>,
    pub terminal: bool,
}

/// Validation failure for lifecycle hook decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLifecycleHookDecisionError {
    pub code: String,
    pub message: String,
}

impl RunLifecycleHookDecisionError {
    /// Creates a validation error from a stable code and human-readable message.
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self { code: code.into(), message: message.into() }
    }
}

/// Resolves sandboxed lifecycle hook decisions with deterministic terminal precedence.
///
/// # Errors
///
/// Returns a [`RunLifecycleHookDecisionError`] with code `phase_mismatch` when a decision
/// was produced for a different phase, or `decision_not_allowed_in_phase` when its kind is
/// not permitted in `phase`.
pub fn resolve_run_lifecycle_hook_decisions(
    phase: RunLifecycleHookPhase,
    mut decisions: Vec<RunLifecycleHookDecision>,
) -> Result<RunLifecycleHookResolution, RunLifecycleHookDecisionError> {
    for decision in &decisions {
        if decision.phase != phase {
            return Err(RunLifecycleHookDecisionError::new(
                "phase_mismatch",
                format!(
                    "hook decision for {} cannot be resolved in {}",
                    decision.phase.as_str(),
                    phase.as_str()
                ),
            ));
        }
        if !decision.kind.is_allowed_in_phase(phase) {
            return Err(RunLifecycleHookDecisionError::new(
                "decision_not_allowed_in_phase",
                format!("{} is not allowed during {}", decision.kind.as_str(), phase.as_str()),
            ));
        }
    }

    // No hook responded: synthesize a Continue so resolution always selects something.
    if decisions.is_empty() {
        decisions.push(RunLifecycleHookDecision::new(
            phase,
            RunLifecycleHookDecisionKind::Continue,
            "runtime",
            "runtime",
        ));
    }

    // Highest priority wins; hook/plugin ids break ties so the outcome is deterministic
    // regardless of hook invocation order.
    decisions.sort_by(|left, right| {
        right
            .kind
            .priority()
            .cmp(&left.kind.priority())
            .then_with(|| left.hook_id.cmp(&right.hook_id))
            .then_with(|| left.plugin_id.cmp(&right.plugin_id))
    });
    let selected = decisions[0].clone();
    // Record losers as conflicts only when either side carried semantic weight (terminal,
    // transform, or annotate); Continue losing to Continue is not a conflict.
    let conflicts = decisions
        .iter()
        .skip(1)
        .filter(|decision| {
            selected.kind.is_terminal()
                || decision.kind.is_terminal()
                || matches!(
                    selected.kind,
                    RunLifecycleHookDecisionKind::TransformPreview
                        | RunLifecycleHookDecisionKind::Annotate
                )
                || matches!(
                    decision.kind,
                    RunLifecycleHookDecisionKind::TransformPreview
                        | RunLifecycleHookDecisionKind::Annotate
                )
        })
        .map(|decision| {
            format!("{}:{}:{}", decision.hook_id, decision.plugin_id, decision.kind.as_str())
        })
        .collect::<Vec<_>>();

    Ok(RunLifecycleHookResolution {
        schema_version: 1,
        phase,
        terminal: selected.kind.is_terminal(),
        selected,
        candidates: decisions,
        conflicts,
    })
}

/// Public snapshot of a global idempotency record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyRecordSnapshot {
    pub key: String,
    pub scope: String,
    pub operation_kind: String,
    pub payload_sha256: String,
    pub state: IdempotencyOperationState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_json: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<StableErrorEnvelope>,
    pub first_seen_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<i64>,
}

/// Result returned by the runtime before a side-effecting operation is executed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyCheckOutcome {
    pub decision: IdempotencyReplayDecision,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<IdempotencyRecordSnapshot>,
}

/// Retention policy attached to a tool result artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRetentionPolicy {
    pub disposition: ArtifactRetentionDisposition,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<i64>,
    pub legal_hold: bool,
}

impl ArtifactRetentionPolicy {
    /// Creates a policy that retains the artifact indefinitely without legal hold.
    #[must_use]
    pub const fn keep() -> Self {
        Self {
            disposition: ArtifactRetentionDisposition::Keep,
            expires_at_unix_ms: None,
            legal_hold: false,
        }
    }

    /// Creates a policy that places the artifact under audit legal hold.
    #[must_use]
    pub const fn audit_legal_hold() -> Self {
        Self {
            disposition: ArtifactRetentionDisposition::AuditLegalHold,
            expires_at_unix_ms: None,
            legal_hold: true,
        }
    }
}

/// Durable reference to a full audit-visible tool result payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolResultArtifactRef {
    pub artifact_id: String,
    pub digest_sha256: String,
    pub mime_type: String,
    pub size_bytes: u64,
    pub sensitivity: ToolResultSensitivity,
    pub retention: ArtifactRetentionPolicy,
    pub origin_tool_call_id: String,
    pub tool_name: String,
    pub run_id: String,
    pub session_id: String,
    pub storage_backend: String,
    pub redacted_preview: String,
    pub created_at_unix_ms: i64,
}

/// Request contract for `palyra.artifact.read`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactReadRequest {
    pub artifact_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_digest_sha256: Option<String>,
    #[serde(default)]
    pub offset_bytes: u64,
    #[serde(default)]
    pub max_bytes: u64,
    #[serde(default = "default_artifact_text_preview")]
    pub text_preview: bool,
}

fn default_artifact_text_preview() -> bool {
    true
}

/// Response contract for `palyra.artifact.read`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactReadResponse {
    pub artifact: ToolResultArtifactRef,
    pub offset_bytes: u64,
    pub returned_bytes: u64,
    pub eof: bool,
    pub visibility: ToolResultVisibility,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

/// Per-turn budget settings used before putting tool output back into the model context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolTurnBudget {
    pub max_model_inline_bytes: usize,
    pub max_model_summary_bytes: usize,
    pub max_artifact_preview_bytes: usize,
    pub max_artifact_read_bytes: usize,
}

impl Default for ToolTurnBudget {
    fn default() -> Self {
        Self {
            max_model_inline_bytes: 8 * 1024,
            max_model_summary_bytes: 2 * 1024,
            max_artifact_preview_bytes: 1_024,
            max_artifact_read_bytes: 16 * 1024,
        }
    }
}

/// Hash- and budget-only audit record for a high-volume tool-result projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolResultProjectionAuditRecord {
    pub schema_version: u32,
    pub proposal_id: String,
    pub tool_name: String,
    pub policy: ToolResultProjectionPolicyKind,
    pub decision: ToolResultProjectionDecisionKind,
    pub visibility: ToolResultVisibility,
    pub sensitivity: ToolResultSensitivity,
    pub reason_code: String,
    pub redaction_level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_digest_sha256: Option<String>,
    pub original_output_bytes: u64,
    pub model_visible_output_bytes: u64,
    pub saved_model_visible_bytes: u64,
    pub budget: ToolTurnBudget,
}

/// Observability counters for model-visible budget projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ToolResultBudgetMetrics {
    pub spilled_artifacts: u64,
    pub rejected_payloads: u64,
    pub saved_model_visible_bytes: u64,
}

/// Lowest realtime protocol version the daemon accepts.
pub const REALTIME_PROTOCOL_MIN_VERSION: u32 = 1;
/// Highest realtime protocol version the daemon accepts.
pub const REALTIME_PROTOCOL_MAX_VERSION: u32 = 1;
/// Default heartbeat interval offered to realtime clients, in milliseconds.
pub const REALTIME_DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 15_000;

runtime_contract_enum! {
    /// Role declared by a realtime client during the initial handshake.
    pub enum RealtimeRole {
        Operator => "operator",
        ReadOnly => "read_only",
        Agent => "agent",
        Connector => "connector",
        Node => "node"
    }
}

runtime_contract_enum! {
    /// Scope names used by realtime method and event authorization.
    pub enum RealtimeScope {
        RunsRead => "runs:read",
        RunsWrite => "runs:write",
        ApprovalsRead => "approvals:read",
        ApprovalsWrite => "approvals:write",
        NodesRead => "nodes:read",
        NodesWrite => "nodes:write",
        ConfigRead => "config:read",
        ConfigWrite => "config:write",
        EventsRead => "events:read",
        EventsSensitive => "events:sensitive"
    }
}

runtime_contract_enum! {
    /// Capability names returned by realtime negotiation and checked by handlers.
    pub enum RealtimeCapability {
        EventStream => "event_stream",
        SnapshotRefresh => "snapshot_refresh",
        RunControl => "run_control",
        ApprovalControl => "approval_control",
        NodePresence => "node_presence",
        CapabilityGrant => "capability_grant",
        ConfigSchemaLookup => "config_schema_lookup",
        ConfigReload => "config_reload",
        SensitiveEvents => "sensitive_events"
    }
}

runtime_contract_enum! {
    /// Stable command names for command-router backed realtime methods.
    pub enum RealtimeCommand {
        RunCreate => "run.create",
        RunWait => "run.wait",
        RunEvents => "run.events",
        RunAbort => "run.abort",
        RunGet => "run.get",
        ApprovalList => "approval.list",
        ApprovalGet => "approval.get",
        ApprovalDecide => "approval.decide",
        NodePresence => "node.presence",
        NodeCapabilityGrant => "node.capability.grant",
        NodeCapabilityRevoke => "node.capability.revoke",
        ConfigSchemaLookup => "config.schema.lookup",
        ConfigReloadPlan => "config.reload.plan",
        ConfigReloadApply => "config.reload.apply"
    }
}

runtime_contract_enum! {
    /// Event topics routed through the realtime event router.
    pub enum RealtimeEventTopic {
        Run => "run",
        Approval => "approval",
        Node => "node",
        Config => "config",
        System => "system"
    }
}

runtime_contract_enum! {
    /// Event sensitivity is evaluated before serialization to each client.
    pub enum RealtimeEventSensitivity {
        Public => "public",
        Internal => "internal",
        Sensitive => "sensitive",
        Secret => "secret"
    }
}

/// Supported realtime protocol range advertised in compatibility errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealtimeProtocolVersionRange {
    pub min: u32,
    pub max: u32,
}

impl Default for RealtimeProtocolVersionRange {
    fn default() -> Self {
        Self { min: REALTIME_PROTOCOL_MIN_VERSION, max: REALTIME_PROTOCOL_MAX_VERSION }
    }
}

impl RealtimeProtocolVersionRange {
    /// Returns `true` when `protocol_version` falls inside the inclusive range.
    #[must_use]
    pub const fn contains(self, protocol_version: u32) -> bool {
        protocol_version >= self.min && protocol_version <= self.max
    }
}

/// Cursor supplied by realtime clients when reconnecting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RealtimeCursor {
    pub sequence: u64,
}

/// Handshake request sent as the first realtime WebSocket frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealtimeHandshakeRequest {
    pub protocol_version: u32,
    pub client_id: String,
    pub role: RealtimeRole,
    #[serde(default)]
    pub requested_scopes: Vec<RealtimeScope>,
    #[serde(default)]
    pub requested_capabilities: Vec<RealtimeCapability>,
    #[serde(default)]
    pub requested_commands: Vec<RealtimeCommand>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_cursor: Option<RealtimeCursor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subscriptions: Vec<RealtimeSubscription>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat_interval_ms: Option<u64>,
}

/// Handshake response after role/scope/capability/command negotiation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealtimeHandshakeAccepted {
    pub protocol_version: u32,
    pub client_id: String,
    pub auth_subject: String,
    pub role: RealtimeRole,
    pub scopes: Vec<RealtimeScope>,
    pub capabilities: Vec<RealtimeCapability>,
    pub commands: Vec<RealtimeCommand>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subscriptions: Vec<RealtimeSubscription>,
    pub cursor: RealtimeCursor,
    pub heartbeat_interval_ms: u64,
    pub server_time_unix_ms: i64,
    pub sdk_abi_version: String,
}

/// Stable realtime error envelope with optional compatibility metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealtimeErrorEnvelope {
    pub error: StableErrorEnvelope,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supported_protocol_versions: Option<RealtimeProtocolVersionRange>,
}

/// Event envelope stored and filtered before serialization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RealtimeEventEnvelope {
    pub schema_version: u32,
    pub sequence: u64,
    pub event_id: String,
    pub topic: RealtimeEventTopic,
    pub sensitivity: RealtimeEventSensitivity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_principal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_session_id: Option<String>,
    pub occurred_at_unix_ms: i64,
    pub payload: Value,
}

/// Subscription filter carried in connection state and restored on reconnect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealtimeSubscription {
    /// Topics to receive; empty means all topics.
    #[serde(default)]
    pub topics: Vec<RealtimeEventTopic>,
    /// Session ids to receive; empty means all sessions.
    #[serde(default)]
    pub session_ids: Vec<String>,
}

impl RealtimeSubscription {
    /// Creates an unfiltered subscription (empty filters match everything).
    #[must_use]
    pub fn all_topics() -> Self {
        Self { topics: Vec::new(), session_ids: Vec::new() }
    }
}

/// Method/command descriptor exported by the runtime registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealtimeMethodDescriptor {
    pub command: RealtimeCommand,
    pub version: u32,
    pub required_scopes: Vec<RealtimeScope>,
    pub required_capabilities: Vec<RealtimeCapability>,
    pub idempotency_required: bool,
    pub side_effecting: bool,
    pub rate_limit_bucket: String,
}

/// Command frame accepted by the shared backend command router.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RealtimeCommandEnvelope {
    pub request_id: String,
    pub command: RealtimeCommand,
    #[serde(default)]
    pub params: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<u64>,
}

/// Stable command-router result envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RealtimeCommandResultEnvelope {
    pub request_id: String,
    pub command: RealtimeCommand,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<StableErrorEnvelope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// `true` when the result was served from an idempotency record instead of re-executing.
    pub replayed: bool,
}

/// Lowest ACP protocol version the daemon bridge accepts.
pub const ACP_PROTOCOL_MIN_VERSION: u32 = 1;
/// Highest ACP protocol version the daemon bridge accepts.
pub const ACP_PROTOCOL_MAX_VERSION: u32 = 1;
/// Default cap on transcript events returned by a single ACP replay.
pub const ACP_DEFAULT_REPLAY_MAX_EVENTS: usize = 200;
/// Grace period in milliseconds before a disconnected ACP client's pending state expires.
pub const ACP_DEFAULT_DISCONNECT_GRACE_MS: i64 = 10 * 60 * 1_000;

runtime_contract_enum! {
    /// Transport that carried an ACP request into the daemon-backed bridge.
    pub enum AcpTransportKind {
        Stdio => "stdio",
        Http => "http",
        Websocket => "websocket"
    }
}

runtime_contract_enum! {
    /// Authorization scopes negotiated by daemon-level ACP clients.
    pub enum AcpScope {
        SessionsRead => "sessions:read",
        SessionsWrite => "sessions:write",
        RunsRead => "runs:read",
        RunsWrite => "runs:write",
        ApprovalsRead => "approvals:read",
        ApprovalsWrite => "approvals:write",
        BindingsRead => "bindings:read",
        BindingsWrite => "bindings:write",
        EventsRead => "events:read",
        EventsSensitive => "events:sensitive"
    }
}

runtime_contract_enum! {
    /// Feature capabilities advertised and enforced by daemon-level ACP.
    pub enum AcpCapability {
        RuntimeStatus => "runtime_status",
        SessionList => "session_list",
        SessionLoad => "session_load",
        SessionNew => "session_new",
        SessionReplay => "session_replay",
        RunControl => "run_control",
        ApprovalBridge => "approval_bridge",
        PendingPrompts => "pending_prompts",
        SessionConfig => "session_config",
        SessionFork => "session_fork",
        SessionCompact => "session_compact",
        SessionExplain => "session_explain",
        ConversationBindings => "conversation_bindings",
        BindingRepair => "binding_repair",
        SensitiveReplay => "sensitive_replay"
    }
}

runtime_contract_enum! {
    /// Stable command names for the ACP control plane.
    pub enum AcpCommand {
        Status => "status" | "acp.status",
        SessionList => "session.list",
        SessionLoad => "session.load",
        SessionNew => "session.new",
        SessionReplay => "session.replay",
        SessionResume => "session.resume",
        SessionFork => "session.fork",
        SessionCompactPreview => "session.compact.preview",
        SessionCompactApply => "session.compact.apply",
        SessionExplain => "session.explain",
        SessionModeSet => "session.mode.set",
        SessionConfigSet => "session.config.set",
        RunCreate => "run.create",
        RunAbort => "run.abort",
        RunGet => "run.get",
        ApprovalList => "approval.list",
        ApprovalRequest => "approval.request",
        ApprovalDecide => "approval.decide",
        BindingList => "binding.list",
        BindingUpsert => "binding.upsert",
        BindingGet => "binding.get",
        BindingDetach => "binding.detach",
        BindingRepairPlan => "binding.repair.plan",
        BindingRepairApply => "binding.repair.apply",
        BindingExplain => "binding.explain",
        Reconnect => "reconnect"
    }
}

runtime_contract_enum! {
    /// ACP-facing session execution mode. Mode changes are policy-visible and auditable.
    pub enum AcpSessionMode {
        Normal => "normal",
        Planning => "planning",
        Review => "review",
        ReadOnly => "read_only"
    }
}

runtime_contract_enum! {
    /// ACP permission bridge outcome after mapping through Palyra approvals.
    pub enum AcpPermissionDecision {
        Allow => "allow",
        Deny => "deny",
        Timeout => "timeout",
        Error => "error"
    }
}

runtime_contract_enum! {
    /// Bounded ACP event ledger category retained for reconnect replay and diagnostics.
    pub enum AcpEventLedgerKind {
        SessionUpdate => "session_update",
        ToolCallUpdate => "tool_call_update",
        ApprovalPrompt => "approval_prompt",
        ApprovalDecision => "approval_decision",
        Terminal => "terminal",
        Cancel => "cancel"
    }
}

runtime_contract_enum! {
    /// Sensitivity label for connector-independent conversation bindings.
    pub enum ConversationBindingSensitivity {
        Public => "public",
        Internal => "internal",
        Sensitive => "sensitive"
    }
}

runtime_contract_enum! {
    /// Conflict state for reverse indexes between external conversations and Palyra sessions.
    pub enum ConversationBindingConflictState {
        None => "none",
        DuplicateActiveBinding => "duplicate_active_binding",
        DuplicateExternalIdentity => "duplicate_external_identity",
        DuplicateSession => "duplicate_session",
        StaleThread => "stale_thread",
        PrincipalMismatch => "principal_mismatch",
        WorkspaceMismatch => "workspace_mismatch",
        ExpiredReference => "expired_reference",
        ParentMissing => "parent_missing",
        Detached => "detached"
    }
}

runtime_contract_enum! {
    /// Diagnostic conflict kind emitted by ACP binding explain and repair previews.
    pub enum AcpBindingConflictKind {
        DuplicateActiveBinding => "duplicate_active_binding",
        StaleThread => "stale_thread",
        PrincipalMismatch => "principal_mismatch",
        WorkspaceMismatch => "workspace_mismatch",
        ExpiredReferenced => "expired_referenced" | "expired_reference",
        ParentMissing => "parent_missing"
    }
}

runtime_contract_enum! {
    /// Safe repair action vocabulary for ACP binding previews and audited apply flows.
    pub enum AcpBindingRepairActionKind {
        Detach => "detach",
        Rebind => "rebind",
        Expire => "expire",
        Split => "split",
        MarkStale => "mark_stale"
    }
}

/// Supported ACP protocol range advertised in compatibility responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcpProtocolVersionRange {
    pub min: u32,
    pub max: u32,
}

impl Default for AcpProtocolVersionRange {
    fn default() -> Self {
        Self { min: ACP_PROTOCOL_MIN_VERSION, max: ACP_PROTOCOL_MAX_VERSION }
    }
}

impl AcpProtocolVersionRange {
    /// Returns `true` when `protocol_version` falls inside the inclusive range.
    #[must_use]
    pub const fn contains(self, protocol_version: u32) -> bool {
        protocol_version >= self.min && protocol_version <= self.max
    }
}

/// Cursor supplied by ACP clients when reconnecting or replaying.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AcpCursor {
    pub sequence: u64,
}

/// Authenticated daemon-side context for an ACP client request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpClientContext {
    pub protocol_version: u32,
    pub client_id: String,
    pub transport: AcpTransportKind,
    pub owner_principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default)]
    pub scopes: Vec<AcpScope>,
    #[serde(default)]
    pub capabilities: Vec<AcpCapability>,
}

/// Durable mapping between an ACP client session and a Palyra orchestrator session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpSessionBindingRecord {
    pub schema_version: u32,
    pub binding_id: String,
    pub acp_client_id: String,
    pub acp_session_id: String,
    pub palyra_session_id: String,
    pub session_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_label: Option<String>,
    pub owner_principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default)]
    pub scopes: Vec<AcpScope>,
    #[serde(default)]
    pub capabilities: Vec<AcpCapability>,
    pub mode: AcpSessionMode,
    #[serde(default)]
    pub config: Value,
    pub cursor: AcpCursor,
    pub last_seen_at_unix_ms: i64,
    pub protocol_version: u32,
    pub stale_permissions: bool,
}

/// Pending prompt or approval retained during ACP disconnect grace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpPendingPromptRecord {
    pub prompt_id: String,
    pub acp_client_id: String,
    pub acp_session_id: String,
    pub palyra_session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub prompt_kind: String,
    pub redacted_summary: String,
    pub created_at_unix_ms: i64,
    pub expires_at_unix_ms: i64,
}

/// Redacted ACP event retained across short disconnects for client replay.
///
/// The original event payload is never persisted in this record. Callers store a
/// human-readable redacted summary and a SHA-256 digest of the canonical
/// redacted payload so clients can correlate replayed frames without exposing
/// secrets in the durable ACP state file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpEventLedgerRecord {
    pub schema_version: u32,
    pub event_id: String,
    pub kind: AcpEventLedgerKind,
    pub sequence: u64,
    pub acp_client_id: String,
    pub acp_session_id: String,
    pub palyra_session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    pub redacted_summary: String,
    pub payload_sha256: String,
    pub created_at_unix_ms: i64,
    pub protocol_version: u32,
}

/// Canonical connector-independent binding from an external conversation to a Palyra session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationBindingRecord {
    pub schema_version: u32,
    pub binding_id: String,
    pub connector_kind: String,
    pub external_identity: String,
    pub external_conversation_id: String,
    pub palyra_session_id: String,
    pub owner_principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    pub sensitivity: ConversationBindingSensitivity,
    pub delivery_cursor: AcpCursor,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_id: Option<String>,
    pub conflict_state: ConversationBindingConflictState,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// ACP command frame accepted by the daemon bridge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpCommandEnvelope {
    pub request_id: String,
    pub command: AcpCommand,
    #[serde(default)]
    pub params: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<u64>,
}

/// Stable ACP result envelope that mirrors command-router error semantics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpCommandResultEnvelope {
    pub request_id: String,
    pub command: AcpCommand,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<StableErrorEnvelope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// `true` when the result was served from an idempotency record instead of re-executing.
    pub replayed: bool,
}

/// Replay budget applied before ACP transcript events leave the daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpReplayCap {
    pub max_events: usize,
    pub max_payload_bytes: usize,
    pub include_sensitive: bool,
}

impl Default for AcpReplayCap {
    fn default() -> Self {
        Self {
            max_events: ACP_DEFAULT_REPLAY_MAX_EVENTS,
            max_payload_bytes: 64 * 1024,
            include_sensitive: false,
        }
    }
}

/// ACP compatibility error envelope with protocol-range metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpErrorEnvelope {
    pub error: StableErrorEnvelope,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supported_protocol_versions: Option<AcpProtocolVersionRange>,
}

/// Schema lookup record for runtime config control-plane clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeConfigSchemaField {
    pub path: String,
    pub value_type: String,
    pub default_value: String,
    pub validator: String,
    pub sensitivity: ToolResultSensitivity,
    pub reloadable: bool,
    pub reload_impact: String,
}

/// Node/device presence surfaced over realtime without exposing secrets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealtimeNodePresence {
    pub device_id: String,
    pub state: String,
    pub ttl_ms: u64,
    pub last_seen_at_unix_ms: i64,
    pub heartbeat_interval_ms: u64,
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub attestation: Vec<String>,
}

runtime_contract_enum! {
    /// Canonical queue runtime modes used by queue orchestration surfaces.
    pub enum QueueMode {
        Followup => "followup" | "follow_up",
        Collect => "collect",
        Steer => "steer",
        SteerBacklog => "steer_backlog" | "steer-backlog",
        Interrupt => "interrupt"
    }
}

runtime_contract_enum! {
    /// Canonical queue decisions used by queue explainability and event payloads.
    pub enum QueueDecision {
        Enqueue => "enqueue",
        Merge => "merge" | "coalesce",
        Steer => "steer",
        SteerBacklog => "steer_backlog" | "steer-backlog",
        Interrupt => "interrupt",
        Overflow => "overflow",
        Defer => "defer" | "deferred"
    }
}

runtime_contract_enum! {
    /// High-level pruning policy classes that keep future rollout knobs stable.
    pub enum PruningPolicyClass {
        Disabled => "disabled" | "off",
        Conservative => "conservative" | "safe",
        Balanced => "balanced" | "default",
        Aggressive => "aggressive" | "high_reduction"
    }
}

runtime_contract_enum! {
    /// Background and auxiliary task kinds shared across daemon, CLI, and web console.
    pub enum AuxiliaryTaskKind {
        BackgroundPrompt => "background_prompt",
        DelegationPrompt => "delegation_prompt",
        Summary => "summary" | "auxiliary_summary",
        RecallSearch => "recall_search" | "auxiliary_recall",
        Classification => "classification" | "auxiliary_classification",
        Extraction => "extraction" | "auxiliary_extraction",
        ObjectiveJudge => "objective_judge",
        Vision => "vision" | "auxiliary_vision",
        AttachmentDerivation => "attachment_derivation",
        AttachmentRecompute => "attachment_recompute",
        PostRunReflection => "post_run_reflection" | "reflection"
    }
}

runtime_contract_enum! {
    /// Canonical auxiliary task lifecycle states.
    pub enum AuxiliaryTaskState {
        Queued => "queued" | "pending",
        Running => "running" | "in_progress",
        Paused => "paused",
        Succeeded => "succeeded" | "complete" | "completed",
        Failed => "failed",
        CancelRequested => "cancel_requested",
        Cancelled => "cancelled" | "canceled",
        Expired => "expired"
    }
}

impl AuxiliaryTaskState {
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Queued | Self::Running | Self::Paused | Self::CancelRequested)
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled | Self::Expired)
    }
}

runtime_contract_enum! {
    /// Queue lifecycle states currently persisted for queued inputs.
    pub enum QueuedInputState {
        Pending => "pending" | "queued",
        Forwarded => "forwarded" | "delivered",
        DeliveryFailed => "delivery_failed" | "failed_delivery",
        Merged => "merged",
        Steered => "steered",
        Interrupted => "interrupted",
        Overflowed => "overflowed" | "overflow",
        Rejected => "rejected" | "reject",
        Cancelled => "cancelled" | "canceled"
    }
}

impl QueuedInputState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Pending)
    }
}

runtime_contract_enum! {
    /// Canonical flow states for future durable orchestration surfaces.
    pub enum FlowState {
        Pending => "pending",
        Ready => "ready",
        Running => "running" | "in_progress",
        WaitingForApproval => "waiting_for_approval" | "approval_wait" | "waiting",
        Paused => "paused",
        Blocked => "blocked",
        Retrying => "retrying",
        Compensating => "compensating",
        TimedOut => "timed_out" | "timeout",
        Succeeded => "succeeded" | "completed",
        Failed => "failed",
        CancelRequested => "cancel_requested",
        Cancelled => "cancelled" | "canceled"
    }
}

impl FlowState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::TimedOut | Self::Cancelled)
    }
}

runtime_contract_enum! {
    /// Canonical flow step states for step adapter and retry surfaces.
    pub enum FlowStepState {
        Pending => "pending",
        Ready => "ready",
        Running => "running" | "in_progress",
        WaitingForApproval => "waiting_for_approval" | "approval_wait" | "waiting",
        Paused => "paused",
        Blocked => "blocked",
        Retrying => "retrying",
        Skipped => "skipped",
        Compensating => "compensating",
        Compensated => "compensated",
        TimedOut => "timed_out" | "timeout",
        Succeeded => "succeeded" | "completed",
        Failed => "failed",
        CancelRequested => "cancel_requested",
        Cancelled => "cancelled" | "canceled"
    }
}

impl FlowStepState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Skipped
                | Self::Compensated
                | Self::TimedOut
                | Self::Succeeded
                | Self::Failed
                | Self::Cancelled
        )
    }
}

runtime_contract_enum! {
    /// Delivery arbitration policies reserved for descendant-aware completion.
    pub enum DeliveryPolicy {
        PreferTerminalDescendant => "prefer_terminal_descendant" | "prefer_child_terminal",
        SuppressStaleParent => "suppress_stale_parent",
        MergeProgressUpdates => "merge_progress_updates" | "coalesce_progress",
        DeliverInterimParent => "deliver_interim_parent",
        RequireFinalReview => "require_final_review"
    }
}

runtime_contract_enum! {
    /// Shared worker lifecycle states surfaced by preview diagnostics and audit events.
    pub enum WorkerLifecycleState {
        Registered => "registered",
        Available => "available" | "ready",
        Assigned => "assigned" | "leased",
        Busy => "busy" | "running",
        Degraded => "degraded",
        Draining => "draining",
        Offline => "offline",
        Completed => "completed" | "succeeded",
        Failed => "failed",
        Orphaned => "orphaned"
    }
}

#[cfg(test)]
mod tests {
    use super::{
        public_runtime_contract_snapshot, validate_public_contract_snapshot,
        validate_public_runtime_event, validate_public_runtime_event_name,
        validate_public_runtime_event_sequence, AcpBindingConflictKind, AcpBindingRepairActionKind,
        AcpCapability, AcpClientContext, AcpCommand, AcpCommandEnvelope, AcpProtocolVersionRange,
        AcpScope, AcpSessionBindingRecord, AcpSessionMode, AcpTransportKind,
        AgentHarnessAttemptClassification, AgentHarnessAttemptErrorSummary,
        AgentHarnessAttemptReplaySafety, AgentHarnessAttemptResult,
        AgentHarnessAttemptTerminalStatus, AgentHarnessCallbackKind, AgentHarnessSelectionMode,
        AgentHookCapabilityGrant, AgentHookDecisionAuthority, AgentHookDecisionKind, AgentHookKind,
        AgentHookRedactionPosture, ArtifactReadRequest, ArtifactRetentionDisposition,
        ArtifactRetentionPolicy, AuxiliaryTaskKind, AuxiliaryTaskState, DeliveryPolicy, FlowState,
        FlowStepState, IdempotencyReplayDecision, PalyraErrorCategory, PalyraErrorEnvelope,
        PalyraValidationIssue, PruningPolicyClass, PublicRuntimeEventCorrelation,
        PublicRuntimeEventEnvelope, PublicRuntimeEventName, QueueDecision, QueueMode,
        QueuedInputState, RealtimeCapability, RealtimeCommand, RealtimeCommandEnvelope,
        RealtimeEventSensitivity, RealtimeEventTopic, RealtimeHandshakeRequest,
        RealtimeProtocolVersionRange, RealtimeRole, RealtimeScope, RealtimeSubscription,
        RunLifecycleHookDecision, RunLifecycleHookDecisionKind, RunLifecycleHookPhase,
        RunLifecyclePhase, StableErrorEnvelope, ToolResultProjectionAuditRecord,
        ToolResultProjectionDecisionKind, ToolResultProjectionPolicyKind, ToolResultSensitivity,
        ToolResultVisibility, ToolTurnBudget, WorkerLifecycleState, ACP_PROTOCOL_MAX_VERSION,
        ACP_PROTOCOL_MIN_VERSION, AGENT_HOOK_DESCRIPTORS, PREPARED_AGENT_ATTEMPT_SCHEMA,
        PUBLIC_RUNTIME_EVENT_DESCRIPTORS, REALTIME_PROTOCOL_MAX_VERSION,
        REALTIME_PROTOCOL_MIN_VERSION,
    };
    use serde_json::{json, Value};
    use std::collections::BTreeMap;

    const EXPECTED_PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_JSON: &str =
        include_str!("../tests/golden/public_runtime_contract_snapshot.json");
    const PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_PATH: &str =
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/public_runtime_contract_snapshot.json");

    fn pretty_json(value: &Value) -> String {
        let canonical = canonical_json_value(value);
        let mut encoded =
            serde_json::to_string_pretty(&canonical).expect("snapshot should serialize to json");
        encoded.push('\n');
        encoded
    }

    fn canonical_json_value(value: &Value) -> Value {
        match value {
            Value::Object(object) => {
                let sorted = object
                    .iter()
                    .map(|(key, value)| (key.clone(), canonical_json_value(value)))
                    .collect::<BTreeMap<_, _>>();
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(items) => Value::Array(items.iter().map(canonical_json_value).collect()),
            scalar => scalar.clone(),
        }
    }

    fn assert_snapshot_matches_golden(
        label: &str,
        actual: &Value,
        expected: &str,
        update_path: Option<&str>,
    ) -> Result<(), String> {
        let actual = pretty_json(actual);
        let expected = expected.replace("\r\n", "\n");
        if std::env::var_os("PALYRA_UPDATE_CONTRACT_SNAPSHOTS").is_some() {
            if let Some(update_path) = update_path {
                std::fs::write(update_path, actual.as_bytes())
                    .map_err(|error| format!("failed to update {update_path}: {error}"))?;
                return Ok(());
            }
        }
        if actual == expected {
            return Ok(());
        }
        let expected_lines = expected.lines().collect::<Vec<_>>();
        let actual_lines = actual.lines().collect::<Vec<_>>();
        let mismatch_index = expected_lines
            .iter()
            .zip(actual_lines.iter())
            .position(|(left, right)| left != right)
            .unwrap_or_else(|| expected_lines.len().min(actual_lines.len()));
        let expected_line = expected_lines.get(mismatch_index).copied().unwrap_or("<missing>");
        let actual_line = actual_lines.get(mismatch_index).copied().unwrap_or("<missing>");

        Err(format!(
            "{label} changed at line {}.\nexpected: {expected_line}\nactual:   {actual_line}\nNext step: if this public contract change is intentional, update the matching golden snapshot, bump the changed snapshot_version, and include a changelog_note/migration note in the same change.\nFull actual snapshot:\n{actual}",
            mismatch_index + 1
        ))
    }

    #[test]
    fn public_runtime_contract_snapshot_matches_golden() {
        let snapshot = public_runtime_contract_snapshot();
        validate_public_contract_snapshot(&snapshot)
            .expect("runtime contract snapshot should be public-safe");
        assert_snapshot_matches_golden(
            "public runtime contract snapshot",
            &snapshot,
            EXPECTED_PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_JSON,
            Some(PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_PATH),
        )
        .unwrap_or_else(|error| panic!("{error}"));
    }

    #[test]
    fn public_runtime_contract_snapshot_rejects_breaking_enum_rename() {
        let mut snapshot = public_runtime_contract_snapshot();
        *snapshot
            .pointer_mut("/runtime_enums/0/values/1/canonical")
            .expect("run lifecycle running value should exist") =
            Value::String("in_progress".to_owned());

        let error = assert_snapshot_matches_golden(
            "public runtime contract snapshot",
            &snapshot,
            EXPECTED_PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_JSON,
            None,
        )
        .expect_err("breaking enum rename must fail the golden gate");
        assert!(error.contains("public runtime contract snapshot changed"));
        assert!(error.contains("bump the changed snapshot_version"));
    }

    #[test]
    fn public_runtime_contract_snapshot_rejects_absolute_paths_and_secret_markers() {
        let mut local_path_snapshot = public_runtime_contract_snapshot();
        local_path_snapshot["changelog_note"] =
            Value::String("local path C:\\Users\\Palo\\Desktop\\palyra".to_owned());
        let path_error = validate_public_contract_snapshot(&local_path_snapshot)
            .expect_err("local absolute path should be rejected");
        assert!(path_error.contains("local absolute path"));

        let mut secret_snapshot = public_runtime_contract_snapshot();
        secret_snapshot["changelog_note"] = Value::String("-----BEGIN PRIVATE KEY-----".to_owned());
        let secret_error = validate_public_contract_snapshot(&secret_snapshot)
            .expect_err("obvious secret marker should be rejected");
        assert!(secret_error.contains("secret marker"));
    }

    fn public_event(
        event: PublicRuntimeEventName,
        correlation: PublicRuntimeEventCorrelation,
        payload: Value,
    ) -> PublicRuntimeEventEnvelope {
        let descriptor = event.descriptor();
        PublicRuntimeEventEnvelope {
            schema_version: descriptor.schema_version,
            event,
            event_id: format!("evt_{}", event.as_str().replace('.', "_")),
            occurred_at_unix_ms: 42,
            correlation,
            visibility: descriptor.visibility,
            redaction: descriptor.redaction,
            journal_mapping: descriptor.journal_mapping,
            payload,
            extensions: std::collections::BTreeMap::new(),
        }
    }

    fn run_correlation() -> PublicRuntimeEventCorrelation {
        PublicRuntimeEventCorrelation {
            run_id: Some("run_01".to_owned()),
            session_id: Some("session_01".to_owned()),
            ..PublicRuntimeEventCorrelation::default()
        }
    }

    fn tool_correlation() -> PublicRuntimeEventCorrelation {
        PublicRuntimeEventCorrelation {
            tool_call_id: Some("tool_01".to_owned()),
            ..run_correlation()
        }
    }

    fn approval_correlation() -> PublicRuntimeEventCorrelation {
        PublicRuntimeEventCorrelation {
            approval_id: Some("approval_01".to_owned()),
            ..tool_correlation()
        }
    }

    #[test]
    fn public_runtime_event_taxonomy_lists_required_events_in_order() {
        let event_names = PUBLIC_RUNTIME_EVENT_DESCRIPTORS
            .iter()
            .map(|descriptor| descriptor.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            event_names,
            vec![
                "run.created",
                "run.queued",
                "run.started",
                "model.delta",
                "tool.call.started",
                "approval.required",
                "approval.resolved",
                "tool.call.delta",
                "tool.call.completed",
                "verification.nudge",
                "run.completed",
                "run.failed",
                "run.cancelled",
                "heartbeat",
            ]
        );
        assert_eq!(
            validate_public_runtime_event_name("tool.call.completed")
                .expect("tool event name should parse"),
            PublicRuntimeEventName::ToolCallCompleted
        );
        assert!(validate_public_runtime_event_name("tool.completed").is_err());
    }

    #[test]
    fn public_runtime_event_validator_rejects_heartbeat_model_tokens() {
        let heartbeat = public_event(
            PublicRuntimeEventName::Heartbeat,
            PublicRuntimeEventCorrelation::default(),
            json!({ "status": "alive", "interval_ms": 5_000 }),
        );
        validate_public_runtime_event(&heartbeat).expect("heartbeat metadata should validate");

        let heartbeat_with_token = public_event(
            PublicRuntimeEventName::Heartbeat,
            PublicRuntimeEventCorrelation::default(),
            json!({ "status": "alive", "delta": "must not be here" }),
        );
        let error = validate_public_runtime_event(&heartbeat_with_token)
            .expect_err("heartbeat must not carry model deltas");
        assert_eq!(error.code, "heartbeat_contains_model_token");
    }

    #[test]
    fn public_runtime_event_unknown_extension_fields_round_trip() {
        let raw = json!({
            "schema_version": 1,
            "event": "model.delta",
            "event_id": "evt_model_delta",
            "occurred_at_unix_ms": 42,
            "correlation": {
                "run_id": "run_01",
                "session_id": "session_01"
            },
            "visibility": "public",
            "redaction": "redacted_text",
            "journal_mapping": "orchestrator_tape",
            "payload": {
                "delta": "hello",
                "is_final": false
            },
            "future_event_field": {
                "preserved": true
            }
        });

        let event: PublicRuntimeEventEnvelope =
            serde_json::from_value(raw).expect("event should deserialize with extension field");
        validate_public_runtime_event(&event).expect("extension fields should be tolerated");
        assert_eq!(event.extensions["future_event_field"]["preserved"], true);

        let encoded = serde_json::to_value(&event).expect("event should serialize");
        assert_eq!(encoded["future_event_field"]["preserved"], true);
    }

    #[test]
    fn public_runtime_event_ordering_accepts_tool_call_with_approval() {
        let events = vec![
            public_event(
                PublicRuntimeEventName::RunStarted,
                run_correlation(),
                json!({ "status": "running" }),
            ),
            public_event(
                PublicRuntimeEventName::ToolCallStarted,
                tool_correlation(),
                json!({
                    "tool_name": "shell_command",
                    "input_json": {},
                    "approval_required": true
                }),
            ),
            public_event(
                PublicRuntimeEventName::ApprovalRequired,
                approval_correlation(),
                json!({
                    "tool_name": "shell_command",
                    "request_summary": "Run command",
                    "prompt": {}
                }),
            ),
            public_event(
                PublicRuntimeEventName::ApprovalResolved,
                approval_correlation(),
                json!({
                    "approved": true,
                    "reason": "operator approved",
                    "decision_scope": "once"
                }),
            ),
            public_event(
                PublicRuntimeEventName::ToolCallDelta,
                tool_correlation(),
                json!({ "kind": "allow", "reason": "approved" }),
            ),
            public_event(
                PublicRuntimeEventName::ToolCallCompleted,
                tool_correlation(),
                json!({ "success": true, "output_json": {} }),
            ),
            public_event(
                PublicRuntimeEventName::RunCompleted,
                run_correlation(),
                json!({ "status": "completed" }),
            ),
        ];

        validate_public_runtime_event_sequence(events.as_slice())
            .expect("tool call with approval should be valid");

        let invalid = vec![public_event(
            PublicRuntimeEventName::ApprovalResolved,
            approval_correlation(),
            json!({
                "approved": true,
                "reason": "operator approved",
                "decision_scope": "once"
            }),
        )];
        let error = validate_public_runtime_event_sequence(invalid.as_slice())
            .expect_err("approval.resolved must follow approval.required");
        assert_eq!(error.code, "approval_resolved_before_required");
    }

    #[test]
    fn queue_modes_round_trip_with_canonical_serialization() {
        let serialized =
            serde_json::to_string(&QueueMode::SteerBacklog).expect("queue mode should serialize");
        assert_eq!(serialized, "\"steer_backlog\"");
        let parsed: QueueMode =
            serde_json::from_str("\"steer_backlog\"").expect("queue mode should deserialize");
        assert_eq!(parsed, QueueMode::SteerBacklog);
        assert_eq!(parsed.as_str(), "steer_backlog");
    }

    #[test]
    fn runtime_contract_aliases_stay_backward_compatible() {
        assert_eq!(QueueMode::parse("follow_up"), Some(QueueMode::Followup));
        assert_eq!(QueueDecision::parse("coalesce"), Some(QueueDecision::Merge));
        assert_eq!(QueuedInputState::parse("delivered"), Some(QueuedInputState::Forwarded));
        assert_eq!(AuxiliaryTaskState::parse("canceled"), Some(AuxiliaryTaskState::Cancelled));
        assert_eq!(WorkerLifecycleState::parse("leased"), Some(WorkerLifecycleState::Assigned));
    }

    #[test]
    fn task_and_flow_state_helpers_identify_terminal_states() {
        assert!(AuxiliaryTaskState::Succeeded.is_terminal());
        assert!(AuxiliaryTaskState::Queued.is_active());
        assert!(QueuedInputState::DeliveryFailed.is_terminal());
        assert!(FlowState::TimedOut.is_terminal());
        assert!(FlowStepState::Compensated.is_terminal());
    }

    #[test]
    fn extended_runtime_contracts_expose_expected_canonical_names() {
        assert_eq!(PruningPolicyClass::Balanced.as_str(), "balanced");
        assert_eq!(AuxiliaryTaskKind::Summary.as_str(), "summary");
        assert_eq!(AuxiliaryTaskKind::RecallSearch.as_str(), "recall_search");
        assert_eq!(AuxiliaryTaskKind::Classification.as_str(), "classification");
        assert_eq!(AuxiliaryTaskKind::Extraction.as_str(), "extraction");
        assert_eq!(AuxiliaryTaskKind::ObjectiveJudge.as_str(), "objective_judge");
        assert_eq!(AuxiliaryTaskKind::Vision.as_str(), "vision");
        assert_eq!(AuxiliaryTaskKind::PostRunReflection.as_str(), "post_run_reflection");
        assert_eq!(DeliveryPolicy::PreferTerminalDescendant.as_str(), "prefer_terminal_descendant");
    }

    #[test]
    fn phase_one_runtime_contracts_parse_legacy_aliases_to_canonical_names() {
        assert_eq!(RunLifecyclePhase::parse("accepted"), Some(RunLifecyclePhase::Queued));
        assert_eq!(RunLifecyclePhase::parse("in_progress"), Some(RunLifecyclePhase::Running));
        assert_eq!(RunLifecyclePhase::parse("done"), Some(RunLifecyclePhase::Completed));
        assert_eq!(RunLifecyclePhase::parse("cancelled"), Some(RunLifecyclePhase::Aborted));
        assert!(RunLifecyclePhase::Completed.is_terminal());
        assert!(RunLifecyclePhase::WaitingForApproval.is_waiting());
        assert_eq!(ToolResultVisibility::AuditArtifact.as_str(), "audit_artifact");
        assert_eq!(IdempotencyReplayDecision::ConflictingPayload.as_str(), "conflicting_payload");
    }

    #[test]
    fn artifact_read_defaults_to_text_preview() {
        let request: ArtifactReadRequest = serde_json::from_value(json!({
            "artifact_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        }))
        .expect("artifact read request should deserialize");

        assert!(request.text_preview);
    }

    #[test]
    fn lifecycle_hook_events_and_decisions_use_stable_rules() {
        assert_eq!(
            RunLifecycleHookPhase::parse_hook_event("run:before_tool"),
            Some(RunLifecycleHookPhase::BeforeTool)
        );
        assert_eq!(RunLifecycleHookPhase::BeforeDelivery.event_name(), "run:before_delivery");
        assert!(RunLifecycleHookDecisionKind::TransformPreview
            .is_allowed_in_phase(RunLifecycleHookPhase::BeforeDelivery));
        assert!(!RunLifecycleHookDecisionKind::TransformPreview
            .is_allowed_in_phase(RunLifecycleHookPhase::AfterRun));
        assert!(!RunLifecycleHookDecisionKind::FailRun
            .is_allowed_in_phase(RunLifecycleHookPhase::AfterRun));
    }

    #[test]
    fn lifecycle_hook_resolution_prefers_terminal_decisions() {
        let phase = RunLifecycleHookPhase::BeforeTool;
        let decisions = vec![
            RunLifecycleHookDecision::new(
                phase,
                RunLifecycleHookDecisionKind::Annotate,
                "audit-note",
                "annotator",
            ),
            RunLifecycleHookDecision::new(
                phase,
                RunLifecycleHookDecisionKind::RequestApproval,
                "approval-gate",
                "approver",
            ),
            RunLifecycleHookDecision::new(
                phase,
                RunLifecycleHookDecisionKind::Block,
                "policy-gate",
                "policy",
            ),
        ];
        let resolution = super::resolve_run_lifecycle_hook_decisions(phase, decisions)
            .expect("valid decisions should resolve");
        assert_eq!(resolution.selected.kind, RunLifecycleHookDecisionKind::Block);
        assert!(resolution.terminal);
        assert_eq!(resolution.candidates.len(), 3);
        assert_eq!(resolution.conflicts.len(), 2);
    }

    #[test]
    fn lifecycle_hook_resolution_rejects_invalid_terminal_phase() {
        let phase = RunLifecycleHookPhase::AfterRun;
        let decisions = vec![RunLifecycleHookDecision::new(
            phase,
            RunLifecycleHookDecisionKind::FailRun,
            "late-fail",
            "plugin",
        )];
        let error = super::resolve_run_lifecycle_hook_decisions(phase, decisions)
            .expect_err("after_run must not accept terminal decisions");
        assert_eq!(error.code, "decision_not_allowed_in_phase");
    }

    #[test]
    fn agent_hook_descriptors_pin_redaction_and_decision_authority() {
        let before_prompt = AgentHookKind::BeforePromptBuild
            .descriptor()
            .expect("before_prompt_build descriptor should exist");
        assert_eq!(before_prompt.decision_authority, AgentHookDecisionAuthority::DecisionCapable);
        assert_eq!(before_prompt.redaction, AgentHookRedactionPosture::MetadataOnly);
        assert!(before_prompt
            .allowed_capability_grants
            .contains(&AgentHookCapabilityGrant::RawPromptTrusted));
        assert!(before_prompt.permits_capability_grants(&[
            AgentHookCapabilityGrant::MetadataOnly,
            AgentHookCapabilityGrant::RedactedPayload,
        ]));

        let model_started = AgentHookKind::ModelCallStarted
            .descriptor()
            .expect("model_call_started descriptor should exist");
        assert_eq!(model_started.decision_authority, AgentHookDecisionAuthority::ObservationOnly);
        assert!(model_started.allowed_decisions.is_empty());
        assert!(
            !model_started.permits_capability_grants(&[AgentHookCapabilityGrant::RawPromptTrusted])
        );
    }

    #[test]
    fn agent_hook_timeout_outcomes_are_deterministic() {
        let gate = AgentHookKind::BeforeToolCall
            .descriptor()
            .expect("before_tool_call descriptor should exist");
        assert_eq!(gate.timeout_decision(), AgentHookDecisionKind::Block);

        let observe =
            AgentHookKind::AgentEnd.descriptor().expect("agent_end descriptor should exist");
        assert_eq!(observe.timeout_decision(), AgentHookDecisionKind::AuditOnly);

        let persist = AgentHookKind::ToolResultPersist
            .descriptor()
            .expect("tool_result_persist descriptor should exist");
        assert_eq!(persist.timeout_decision(), AgentHookDecisionKind::FailPersistence);
    }

    #[test]
    fn prepared_attempt_schema_keeps_journal_and_tools_host_owned() {
        let schema =
            serde_json::to_value(PREPARED_AGENT_ATTEMPT_SCHEMA).expect("schema should serialize");
        assert_eq!(schema["host_owns_tool_execution"], true);
        assert_eq!(schema["direct_journal_write_allowed"], false);
        assert_eq!(
            AgentHarnessSelectionMode::parse("explicit_plugin"),
            Some(AgentHarnessSelectionMode::ExplicitPlugin)
        );
        assert_eq!(
            AgentHarnessSelectionMode::parse("native_stub"),
            Some(AgentHarnessSelectionMode::NativeStub)
        );
        assert!(PREPARED_AGENT_ATTEMPT_SCHEMA
            .callback_kinds
            .contains(&AgentHarnessCallbackKind::PartialReply));
        assert!(PREPARED_AGENT_ATTEMPT_SCHEMA
            .host_owned_authorities
            .contains(&"approval_resolution"));
    }

    #[test]
    fn agent_harness_attempt_result_redacts_error_and_trace_details() {
        let mut result = AgentHarnessAttemptResult::minimal(
            AgentHarnessAttemptTerminalStatus::Failed,
            AgentHarnessAttemptClassification::ProviderError,
            AgentHarnessAttemptReplaySafety::NotReplaySafe,
            "trace?access_token=secret-token",
        );
        result.error = Some(AgentHarnessAttemptErrorSummary::new(
            "provider",
            "provider_failed",
            Some("Authorization: Bearer secret-token"),
        ));

        let serialized = serde_json::to_string(&result).expect("result should serialize");

        assert!(!serialized.contains("secret-token"));
        assert!(serialized.contains("provider_failed"));
        assert_eq!(result.terminal_status, AgentHarnessAttemptTerminalStatus::Failed);
        assert_eq!(result.classification, AgentHarnessAttemptClassification::ProviderError);
    }

    #[test]
    fn tool_result_middleware_may_only_downgrade_visibility() {
        assert_eq!(
            super::validate_tool_result_visibility_downgrade(
                ToolResultVisibility::ModelSummary,
                ToolResultVisibility::RedactedPreview,
            )
            .expect("downgrade should be accepted"),
            ToolResultVisibility::RedactedPreview
        );

        let error = super::validate_tool_result_visibility_downgrade(
            ToolResultVisibility::RedactedPreview,
            ToolResultVisibility::ModelInline,
        )
        .expect_err("visibility escalation must be rejected");
        assert_eq!(error.code, "visibility_escalation_denied");
    }

    #[test]
    fn agent_hook_contract_snapshot_covers_all_descriptors() {
        assert!(AGENT_HOOK_DESCRIPTORS.len() >= 30);
        for descriptor in AGENT_HOOK_DESCRIPTORS {
            assert!(AgentHookKind::parse(descriptor.kind.as_str()).is_some());
            if descriptor.decision_authority == AgentHookDecisionAuthority::ObservationOnly {
                assert!(descriptor.allowed_decisions.is_empty());
            }
            if descriptor.kind.is_tool_result_middleware() {
                assert!(descriptor
                    .allowed_capability_grants
                    .contains(&AgentHookCapabilityGrant::ToolResultTransform));
            }
        }
    }

    #[test]
    fn artifact_contracts_capture_sensitivity_retention_and_budget_defaults() {
        assert!(ToolResultSensitivity::Secret.requires_full_read_gate());
        assert!(ToolResultSensitivity::StdoutStderr.requires_full_read_gate());
        assert!(ToolResultSensitivity::InternalPath.requires_full_read_gate());

        let keep = ArtifactRetentionPolicy::keep();
        assert_eq!(keep.disposition, ArtifactRetentionDisposition::Keep);
        assert!(!keep.legal_hold);

        let hold = ArtifactRetentionPolicy::audit_legal_hold();
        assert_eq!(hold.disposition, ArtifactRetentionDisposition::AuditLegalHold);
        assert!(hold.legal_hold);

        let budget = ToolTurnBudget::default();
        assert!(budget.max_model_inline_bytes > budget.max_model_summary_bytes);
        assert!(budget.max_artifact_read_bytes >= budget.max_model_inline_bytes);
    }

    #[test]
    fn palyra_error_envelope_serializes_stable_public_shape() {
        let envelope = PalyraErrorEnvelope::from_stable_error(
            PalyraErrorCategory::Provider,
            StableErrorEnvelope::new(
                "provider/auth_failed",
                "provider credentials were rejected",
                "refresh the configured provider credential",
            ),
            false,
            true,
        )
        .with_validation_errors(vec![PalyraValidationIssue {
            field: "model".to_owned(),
            code: "unsupported_model".to_owned(),
            message: "model is not available for this provider".to_owned(),
        }]);
        let value = serde_json::to_value(&envelope).expect("error envelope should serialize");

        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["category"], "provider");
        assert_eq!(value["code"], "provider/auth_failed");
        assert_eq!(value["retryable"], false);
        assert_eq!(value["redacted"], true);
        assert_eq!(value["validation_errors"][0]["field"], "model");
        assert_eq!(envelope.stable_error().code, "provider/auth_failed");
    }

    #[test]
    fn palyra_error_categories_cover_public_failure_sources() {
        for category in [
            PalyraErrorCategory::Provider,
            PalyraErrorCategory::Policy,
            PalyraErrorCategory::Approval,
            PalyraErrorCategory::Sandbox,
            PalyraErrorCategory::Mcp,
            PalyraErrorCategory::ExecutionBackend,
            PalyraErrorCategory::Validation,
        ] {
            assert!(
                PalyraErrorCategory::parse(category.as_str()).is_some(),
                "category {} should remain parseable",
                category.as_str()
            );
        }
    }

    #[test]
    fn tool_result_projection_audit_record_uses_stable_wire_names() {
        let record = ToolResultProjectionAuditRecord {
            schema_version: 1,
            proposal_id: "call-01".to_owned(),
            tool_name: "palyra.process.run".to_owned(),
            policy: ToolResultProjectionPolicyKind::RedactedPreviewAndArtifact,
            decision: ToolResultProjectionDecisionKind::SpilledToArtifact,
            visibility: ToolResultVisibility::RedactedPreview,
            sensitivity: ToolResultSensitivity::StdoutStderr,
            reason_code: "tool_result_projection.high_volume_artifact".to_owned(),
            redaction_level: "redacted_preview_only".to_owned(),
            artifact_id: Some("artifact-01".to_owned()),
            artifact_digest_sha256: Some("digest".to_owned()),
            original_output_bytes: 65_536,
            model_visible_output_bytes: 1_024,
            saved_model_visible_bytes: 64_512,
            budget: ToolTurnBudget::default(),
        };
        let value = serde_json::to_value(record).expect("record should serialize");

        assert_eq!(value["policy"], "redacted_preview_and_artifact");
        assert_eq!(value["decision"], "spilled_to_artifact");
        assert_eq!(value["visibility"], "redacted_preview");
        assert_eq!(value["sensitivity"], "stdout_stderr");
    }

    #[test]
    fn realtime_contracts_use_stable_wire_names() {
        assert_eq!(RealtimeRole::Operator.as_str(), "operator");
        assert_eq!(RealtimeScope::RunsRead.as_str(), "runs:read");
        assert_eq!(RealtimeCapability::SnapshotRefresh.as_str(), "snapshot_refresh");
        assert_eq!(RealtimeCommand::ConfigReloadApply.as_str(), "config.reload.apply");
        assert_eq!(RealtimeEventTopic::Approval.as_str(), "approval");
        assert_eq!(RealtimeEventSensitivity::Sensitive.as_str(), "sensitive");
        assert!(RealtimeProtocolVersionRange::default().contains(REALTIME_PROTOCOL_MIN_VERSION));
        assert!(RealtimeProtocolVersionRange::default().contains(REALTIME_PROTOCOL_MAX_VERSION));
    }

    #[test]
    fn realtime_handshake_and_command_frames_are_json_safe() {
        let handshake = RealtimeHandshakeRequest {
            protocol_version: 1,
            client_id: "console-a".to_owned(),
            role: RealtimeRole::Operator,
            requested_scopes: vec![RealtimeScope::RunsRead, RealtimeScope::ApprovalsWrite],
            requested_capabilities: vec![RealtimeCapability::RunControl],
            requested_commands: vec![RealtimeCommand::RunGet, RealtimeCommand::ApprovalDecide],
            event_cursor: None,
            subscriptions: vec![RealtimeSubscription {
                topics: vec![RealtimeEventTopic::Run],
                session_ids: vec!["session-a".to_owned()],
            }],
            heartbeat_interval_ms: Some(5_000),
        };
        let serialized = serde_json::to_value(&handshake).expect("handshake should serialize");
        assert_eq!(serialized["requested_scopes"], json!(["runs:read", "approvals:write"]));
        assert_eq!(serialized["subscriptions"][0]["topics"], json!(["run"]));

        let command = RealtimeCommandEnvelope {
            request_id: "req-1".to_owned(),
            command: RealtimeCommand::RunWait,
            params: json!({ "run_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV" }),
            idempotency_key: None,
            expected_version: None,
        };
        let decoded: RealtimeCommandEnvelope =
            serde_json::from_value(serde_json::to_value(command).unwrap()).unwrap();
        assert_eq!(decoded.command, RealtimeCommand::RunWait);
    }

    #[test]
    fn realtime_handshake_rejects_unknown_scope_and_capability_names() {
        let unknown_scope = json!({
            "protocol_version": 1,
            "client_id": "console-a",
            "role": "operator",
            "requested_scopes": ["runs:read", "unknown:scope"],
            "requested_capabilities": [],
            "requested_commands": []
        });
        assert!(serde_json::from_value::<RealtimeHandshakeRequest>(unknown_scope).is_err());

        let unknown_capability = json!({
            "protocol_version": 1,
            "client_id": "console-a",
            "role": "operator",
            "requested_scopes": [],
            "requested_capabilities": ["unknown_capability"],
            "requested_commands": []
        });
        assert!(serde_json::from_value::<RealtimeHandshakeRequest>(unknown_capability).is_err());
    }

    #[test]
    fn acp_contracts_use_stable_wire_names() {
        assert_eq!(AcpTransportKind::Stdio.as_str(), "stdio");
        assert_eq!(AcpScope::SessionsRead.as_str(), "sessions:read");
        assert_eq!(AcpCapability::ApprovalBridge.as_str(), "approval_bridge");
        assert_eq!(AcpCapability::RuntimeStatus.as_str(), "runtime_status");
        assert_eq!(AcpCommand::Status.as_str(), "status");
        assert_eq!(AcpCommand::BindingRepairApply.as_str(), "binding.repair.apply");
        assert_eq!(AcpSessionMode::ReadOnly.as_str(), "read_only");
        assert_eq!(
            AcpBindingConflictKind::DuplicateActiveBinding.as_str(),
            "duplicate_active_binding"
        );
        assert_eq!(AcpBindingRepairActionKind::MarkStale.as_str(), "mark_stale");
        assert!(AcpProtocolVersionRange::default().contains(ACP_PROTOCOL_MIN_VERSION));
        assert!(AcpProtocolVersionRange::default().contains(ACP_PROTOCOL_MAX_VERSION));
    }

    #[test]
    fn acp_context_and_command_frames_reject_unknown_contract_values() {
        let context = AcpClientContext {
            protocol_version: 1,
            client_id: "zed-extension".to_owned(),
            transport: AcpTransportKind::Stdio,
            owner_principal: "operator".to_owned(),
            device_id: "desktop".to_owned(),
            channel: None,
            scopes: vec![AcpScope::SessionsRead, AcpScope::ApprovalsWrite],
            capabilities: vec![AcpCapability::SessionReplay],
        };
        let serialized = serde_json::to_value(&context).expect("context should serialize");
        assert_eq!(serialized["scopes"], json!(["sessions:read", "approvals:write"]));
        assert_eq!(serialized["capabilities"], json!(["session_replay"]));

        let command = AcpCommandEnvelope {
            request_id: "req-1".to_owned(),
            command: AcpCommand::SessionReplay,
            params: json!({ "session_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV" }),
            idempotency_key: None,
            expected_version: None,
        };
        let decoded: AcpCommandEnvelope =
            serde_json::from_value(serde_json::to_value(command).unwrap()).unwrap();
        assert_eq!(decoded.command, AcpCommand::SessionReplay);

        let unknown_scope = json!({
            "protocol_version": 1,
            "client_id": "zed-extension",
            "transport": "stdio",
            "owner_principal": "operator",
            "device_id": "desktop",
            "scopes": ["sessions:read", "unknown:scope"],
            "capabilities": []
        });
        assert!(serde_json::from_value::<AcpClientContext>(unknown_scope).is_err());
    }

    #[test]
    fn acp_binding_record_persists_cursor_mode_and_stale_permissions() {
        let binding = AcpSessionBindingRecord {
            schema_version: 1,
            binding_id: "acpbind_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            acp_client_id: "zed-extension".to_owned(),
            acp_session_id: "acp-session-a".to_owned(),
            palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            session_key: "cwd:C:/repo".to_owned(),
            session_label: Some("Repo".to_owned()),
            owner_principal: "operator".to_owned(),
            device_id: "desktop".to_owned(),
            channel: None,
            scopes: vec![AcpScope::SessionsRead],
            capabilities: vec![AcpCapability::SessionLoad],
            mode: AcpSessionMode::Review,
            config: json!({ "model_profile": "default" }),
            cursor: Default::default(),
            last_seen_at_unix_ms: 1,
            protocol_version: 1,
            stale_permissions: true,
        };
        let serialized = serde_json::to_value(&binding).expect("binding should serialize");
        assert_eq!(serialized["mode"], "review");
        assert_eq!(serialized["stale_permissions"], true);
        assert_eq!(serialized["cursor"]["sequence"], 0);
    }
}
