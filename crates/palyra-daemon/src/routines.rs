//! Routine definitions, run metadata persistence, and trigger/delivery contracts.
//!
//! Owns the JSON-backed [`RoutineRegistry`] under `<state_root>/routines/`, normalization of
//! routine and run metadata, natural-language schedule parsing, file-watch trigger observation,
//! and the delivery/approval/wake-gate contracts the cron scheduler and gateway services apply
//! when dispatching routine runs. Many of the `as_str` values and reason strings here are wire
//! contract pinned by fixtures and CLI parity tests; do not reword them casually.

use std::{
    collections::BTreeSet,
    fs,
    io::{Read, Seek, SeekFrom},
    path::{Component, Path, PathBuf},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    cron::{self, CronTimezoneMode},
    gateway::proto::palyra::cron::v1 as cron_v1,
    journal::{CronJobRecord, CronRunRecord, CronRunStatus, CronScheduleType},
};
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use palyra_common::{
    config_system::write_content_with_backups,
    default_state_root,
    redaction::{is_sensitive_key, REDACTED},
    IdentityStorePathError,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

const ROUTINE_REGISTRY_VERSION: u32 = 1;
const ROUTINES_DIR: &str = "routines";
const ROUTINES_REGISTRY_FILE: &str = "definitions.json";
const ROUTINE_RUNS_FILE: &str = "run_metadata.json";
const MAX_ROUTINE_COUNT: usize = 2_048;
const MAX_ROUTINE_RUN_METADATA_COUNT: usize = 8_192;
// Hard parse floor for "every ..." phrases. Same value as the auto-enable guard below, but kept
// as a separate constant on purpose: loosening the parser floor must not silently loosen the
// approval guard, and vice versa.
const MIN_EVERY_INTERVAL_MS: u64 = 30 * 1_000;
/// Recurring routine schedules below this interval require review before they
/// can be enabled, even when the caller omits an approval policy.
pub const MIN_AUTO_ENABLE_EVERY_INTERVAL_MS: u64 = 30_000;
/// Far-future timestamp used as a never-firing `at` schedule placeholder for routines that are
/// triggered manually or by hooks instead of by the cron clock.
pub const SHADOW_AT_TIMESTAMP_RFC3339: &str = "2100-01-01T00:00:00Z";
/// Schema identifier embedded in [`RoutineExportBundle`] payloads.
pub const ROUTINE_EXPORT_SCHEMA_ID: &str = "palyra.routine.export.v1";
/// Schema version embedded in [`RoutineExportBundle`] payloads.
pub const ROUTINE_EXPORT_SCHEMA_VERSION: u32 = 1;
/// Version of the built-in [`routine_templates`] pack.
pub const ROUTINE_TEMPLATE_PACK_VERSION: u32 = 1;
/// An active run whose record has not been updated for this long is treated as lease-expired
/// and becomes eligible for repair (see [`routine_run_lifecycle_snapshot`]).
pub const ROUTINE_RUN_LEASE_TTL_MS: i64 = 15 * 60 * 1_000;
/// Poll interval applied when a file-watch trigger payload omits `poll_interval_ms`.
pub const DEFAULT_FILE_WATCH_POLL_INTERVAL_MS: u64 = 30 * 1_000;
/// Lower bound for file-watch polling; faster polling is rejected at validation time.
pub const MIN_FILE_WATCH_POLL_INTERVAL_MS: u64 = 30 * 1_000;

/// What fires a routine: the cron clock, an internal hook, an inbound webhook, a system event,
/// a polled file watch, or an explicit manual dispatch.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutineTriggerKind {
    Schedule,
    Hook,
    Webhook,
    SystemEvent,
    FileWatch,
    Manual,
}

impl RoutineTriggerKind {
    /// Returns the canonical snake_case wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Schedule => "schedule",
            Self::Hook => "hook",
            Self::Webhook => "webhook",
            Self::SystemEvent => "system_event",
            Self::FileWatch => "file_watch",
            Self::Manual => "manual",
        }
    }

    /// Parses a wire name (trimmed, case-insensitive); returns `None` for unknown values.
    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "schedule" => Some(Self::Schedule),
            "hook" => Some(Self::Hook),
            "webhook" => Some(Self::Webhook),
            "system_event" => Some(Self::SystemEvent),
            "file_watch" | "file-watch" => Some(Self::FileWatch),
            "manual" => Some(Self::Manual),
            _ => None,
        }
    }
}

/// Whether a routine run reuses the originating session or starts a fresh one.
///
/// Fresh-session prompts must be self-contained; see [`validate_routine_prompt_self_contained`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RoutineRunMode {
    #[default]
    SameSession,
    FreshSession,
}

impl RoutineRunMode {
    /// Returns the canonical snake_case wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SameSession => "same_session",
            Self::FreshSession => "fresh_session",
        }
    }

    /// Parses a wire name (trimmed, case-insensitive); returns `None` for unknown values.
    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "same_session" => Some(Self::SameSession),
            "fresh_session" => Some(Self::FreshSession),
            _ => None,
        }
    }
}

/// Tool-access posture for routine runs; `SensitiveTools` opts the run into stricter gating.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RoutineExecutionPosture {
    #[default]
    Standard,
    SensitiveTools,
}

impl RoutineExecutionPosture {
    /// Returns the canonical snake_case wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::SensitiveTools => "sensitive_tools",
        }
    }

    /// Parses a wire name (trimmed, case-insensitive); returns `None` for unknown values.
    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "standard" => Some(Self::Standard),
            "sensitive_tools" => Some(Self::SensitiveTools),
            _ => None,
        }
    }
}

/// Where routine output is announced: the origin channel, an explicit channel, the local
/// automation session only, or logs/diagnostics only.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutineDeliveryMode {
    SameChannel,
    SpecificChannel,
    LocalOnly,
    LogsOnly,
}

impl RoutineDeliveryMode {
    /// Returns the canonical snake_case wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SameChannel => "same_channel",
            Self::SpecificChannel => "specific_channel",
            Self::LocalOnly => "local_only",
            Self::LogsOnly => "logs_only",
        }
    }

    /// Parses a wire name (trimmed, case-insensitive); returns `None` for unknown values.
    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "same_channel" => Some(Self::SameChannel),
            "specific_channel" => Some(Self::SpecificChannel),
            "local_only" => Some(Self::LocalOnly),
            "logs_only" => Some(Self::LogsOnly),
            _ => None,
        }
    }
}

/// How chatty a routine is: announce everything, announce failures only, or stay silent and
/// rely on the audit trail.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RoutineSilentPolicy {
    #[default]
    Noisy,
    FailureOnly,
    AuditOnly,
}

impl RoutineSilentPolicy {
    /// Returns the canonical snake_case wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Noisy => "noisy",
            Self::FailureOnly => "failure_only",
            Self::AuditOnly => "audit_only",
        }
    }

    /// Parses a wire name (trimmed, case-insensitive); returns `None` for unknown values.
    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "noisy" => Some(Self::Noisy),
            "failure_only" => Some(Self::FailureOnly),
            "audit_only" => Some(Self::AuditOnly),
            _ => None,
        }
    }
}

/// Effective outcome of a routine run; richer than the raw cron status because metadata can
/// override it (for example no-op successes or throttled runs).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutineRunOutcomeKind {
    Pending,
    SuccessWithOutput,
    SuccessNoOp,
    Skipped,
    Throttled,
    Failed,
    Denied,
}

impl RoutineRunOutcomeKind {
    /// Returns the canonical snake_case wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::SuccessWithOutput => "success_with_output",
            Self::SuccessNoOp => "success_no_op",
            Self::Skipped => "skipped",
            Self::Throttled => "throttled",
            Self::Failed => "failed",
            Self::Denied => "denied",
        }
    }
}

/// Lease view of a run: `Active` while heartbeating, `Expired` after
/// [`ROUTINE_RUN_LEASE_TTL_MS`] without progress, `Released` once terminal.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutineRunLeaseState {
    Active,
    Expired,
    Released,
}

/// Approval gate of a run as derived from the routine policy and the run's approval note.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutineApprovalGateState {
    NotRequired,
    Pending,
    Approved,
    Denied,
}

/// Shape of a run's delivery obligation: a channel announcement, an artifact-only result, a
/// deliberate silence, or a dead-letter held for operator review.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutineDeliveryContractKind {
    Channel,
    ArtifactOnly,
    Silent,
    OperatorReview,
}

/// Resolved delivery obligation for one run outcome, built by [`routine_delivery_contract`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineDeliveryContract {
    pub kind: RoutineDeliveryContractKind,
    pub mode: RoutineDeliveryMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub announced: bool,
    pub requires_operator_review: bool,
    pub retryable: bool,
    pub dead_letter: bool,
    pub reason: String,
}

/// Combined lease/approval/terminality view of a run with a machine-readable recovery hint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineRunLifecycleSnapshot {
    pub run_id: String,
    pub routine_id: String,
    pub status: CronRunStatus,
    pub lease_state: RoutineRunLeaseState,
    pub approval_gate: RoutineApprovalGateState,
    pub terminal: bool,
    pub delivery_ready: bool,
    pub recovery_hint: String,
}

/// Contracts for routine preflight steps and the wake gate that decides whether a triggered
/// routine may actually dispatch. Preflight context is fenced (allow-listed, redacted, bounded)
/// before it can reach a model or tool.
#[allow(dead_code)]
pub(crate) mod routine_preflight_contracts {
    use super::*;

    /// Verdict of one preflight step; anything but `Proceed` blocks dispatch.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum RoutinePreflightOutcome {
        Proceed,
        Skip,
        Defer,
        AskApproval,
        Fail,
    }

    impl RoutinePreflightOutcome {
        /// Returns the canonical snake_case wire name.
        #[must_use]
        pub(crate) const fn as_str(self) -> &'static str {
            match self {
                Self::Proceed => "proceed",
                Self::Skip => "skip",
                Self::Defer => "defer",
                Self::AskApproval => "ask_approval",
                Self::Fail => "fail",
            }
        }
    }

    /// Declaration of one preflight tool invocation, validated by [`validate_preflight_step`].
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub(crate) struct RoutinePreflightStep {
        pub(crate) step_id: String,
        pub(crate) tool_name: String,
        #[serde(default)]
        pub(crate) input_schema: Value,
        #[serde(default)]
        pub(crate) output_schema: Value,
        #[serde(default)]
        pub(crate) required_scopes: Vec<String>,
        #[serde(default)]
        pub(crate) capability_names: Vec<String>,
        pub(crate) timeout_ms: u64,
    }

    /// Result a preflight step reports back: outcome, reason, and a context delta to merge.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub(crate) struct RoutinePreflightOutput {
        pub(crate) outcome: RoutinePreflightOutcome,
        pub(crate) reason: String,
        #[serde(default)]
        pub(crate) context_delta: Value,
    }

    /// Accept/reject verdict for a preflight step declaration with a stable reason code.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub(crate) struct RoutinePreflightValidation {
        pub(crate) accepted: bool,
        pub(crate) reason: String,
    }

    /// Validates a preflight step declaration fail-closed: empty identifiers, out-of-range
    /// timeouts, wildcard or empty scopes, and empty capability names are all rejected.
    #[must_use]
    pub(crate) fn validate_preflight_step(
        step: &RoutinePreflightStep,
    ) -> RoutinePreflightValidation {
        if step.step_id.trim().is_empty() {
            return validation_error("preflight_step_id_empty");
        }
        if step.tool_name.trim().is_empty() {
            return validation_error("preflight_tool_name_empty");
        }
        if step.timeout_ms == 0 || step.timeout_ms > 120_000 {
            return validation_error("preflight_timeout_out_of_range");
        }
        if step.required_scopes.iter().any(|scope| scope.trim().is_empty() || scope == "*") {
            return validation_error("preflight_required_scope_invalid");
        }
        if step.capability_names.iter().any(|capability| capability.trim().is_empty()) {
            return validation_error("preflight_capability_invalid");
        }
        RoutinePreflightValidation { accepted: true, reason: "preflight_step_valid".to_owned() }
    }

    /// Allow-list plus size bound applied to preflight context objects.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct RoutinePreflightContextFence {
        pub(crate) allowed_keys: BTreeSet<String>,
        pub(crate) max_value_bytes: usize,
    }

    impl RoutinePreflightContextFence {
        /// Builds a fence allowing exactly `keys`, each value capped at `max_value_bytes`.
        #[must_use]
        pub(crate) fn allow_keys(keys: &[&str], max_value_bytes: usize) -> Self {
            Self {
                allowed_keys: keys.iter().map(|key| (*key).to_owned()).collect(),
                max_value_bytes,
            }
        }
    }

    /// Fenced context plus the audit lists of which keys were dropped or redacted.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub(crate) struct RoutinePreflightFenceResult {
        pub(crate) context: Value,
        pub(crate) dropped_keys: Vec<String>,
        pub(crate) redacted_keys: Vec<String>,
    }

    /// Applies a fence to untrusted preflight context.
    ///
    /// Non-object inputs fence to an empty object. Keys outside the allow-list are dropped,
    /// allow-listed keys with sensitive names are redacted (allow-listing alone must not leak a
    /// secret), and surviving values are truncated to the fence's byte bound.
    #[must_use]
    pub(crate) fn fence_preflight_context(
        input: &Value,
        fence: &RoutinePreflightContextFence,
    ) -> RoutinePreflightFenceResult {
        let mut context = serde_json::Map::new();
        let mut dropped_keys = Vec::new();
        let mut redacted_keys = Vec::new();
        let Some(object) = input.as_object() else {
            return RoutinePreflightFenceResult {
                context: Value::Object(context),
                dropped_keys,
                redacted_keys,
            };
        };
        for (key, value) in object {
            if !fence.allowed_keys.contains(key) {
                dropped_keys.push(key.clone());
                continue;
            }
            if is_sensitive_key(key) {
                context.insert(key.clone(), Value::String(REDACTED.to_owned()));
                redacted_keys.push(key.clone());
                continue;
            }
            context.insert(key.clone(), bounded_json_value(value, fence.max_value_bytes));
        }
        dropped_keys.sort();
        redacted_keys.sort();
        RoutinePreflightFenceResult { context: Value::Object(context), dropped_keys, redacted_keys }
    }

    /// Stable reason code for a wake-gate decision.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum RoutineWakeGateReason {
        Allowed,
        RoutineDisabled,
        ScheduleTickMissing,
        LastRunStillActive,
        PreflightSkipped,
        PreflightDeferred,
        PreflightApprovalRequired,
        PreflightFailed,
        ProviderCooldown,
        ChannelUnhealthy,
        PolicyDenied,
    }

    impl RoutineWakeGateReason {
        /// Returns the canonical snake_case wire name.
        #[must_use]
        pub(crate) const fn as_str(self) -> &'static str {
            match self {
                Self::Allowed => "allowed",
                Self::RoutineDisabled => "routine_disabled",
                Self::ScheduleTickMissing => "schedule_tick_missing",
                Self::LastRunStillActive => "last_run_still_active",
                Self::PreflightSkipped => "preflight_skipped",
                Self::PreflightDeferred => "preflight_deferred",
                Self::PreflightApprovalRequired => "preflight_approval_required",
                Self::PreflightFailed => "preflight_failed",
                Self::ProviderCooldown => "provider_cooldown",
                Self::ChannelUnhealthy => "channel_unhealthy",
                Self::PolicyDenied => "policy_denied",
            }
        }
    }

    /// Everything the wake gate looks at for one candidate dispatch.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct RoutineWakeGateInput {
        pub(crate) enabled: bool,
        pub(crate) schedule_tick_at_unix_ms: Option<i64>,
        pub(crate) last_run_status: Option<CronRunStatus>,
        pub(crate) preflight_outcome: Option<RoutinePreflightOutcome>,
        pub(crate) provider_cooldown_until_unix_ms: Option<i64>,
        pub(crate) channel_healthy: bool,
        pub(crate) policy_allowed: bool,
        pub(crate) now_unix_ms: i64,
    }

    /// Wake-gate verdict with a stable reason and a machine-readable recovery hint.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub(crate) struct RoutineWakeGateDecision {
        pub(crate) allowed: bool,
        pub(crate) reason: RoutineWakeGateReason,
        pub(crate) recovery_hint: String,
    }

    impl RoutineWakeGateDecision {
        /// Renders the decision as the JSON shape journaled with the dispatch attempt.
        #[must_use]
        pub(crate) fn snapshot_json(&self) -> Value {
            json!({
                "allowed": self.allowed,
                "reason": self.reason.as_str(),
                "recovery_hint": self.recovery_hint,
            })
        }
    }

    /// Decides whether a triggered routine may dispatch.
    ///
    /// Checks run in a fixed precedence order that is part of the contract -- disabled routine,
    /// missing schedule tick, still-active previous run, preflight verdict, provider cooldown,
    /// channel health, then policy -- so the reported block reason is always the first gate that
    /// failed, not an arbitrary one.
    #[must_use]
    pub(crate) fn evaluate_routine_wake_gate(
        input: RoutineWakeGateInput,
    ) -> RoutineWakeGateDecision {
        if !input.enabled {
            return wake_gate_denied(
                RoutineWakeGateReason::RoutineDisabled,
                "enable_routine_before_dispatch",
            );
        }
        if input.schedule_tick_at_unix_ms.is_none() {
            return wake_gate_denied(
                RoutineWakeGateReason::ScheduleTickMissing,
                "wait_for_next_schedule_tick",
            );
        }
        if input.last_run_status.is_some_and(CronRunStatus::is_active) {
            return wake_gate_denied(
                RoutineWakeGateReason::LastRunStillActive,
                "repair_or_wait_for_active_run",
            );
        }
        match input.preflight_outcome {
            Some(RoutinePreflightOutcome::Skip) => {
                return wake_gate_denied(RoutineWakeGateReason::PreflightSkipped, "record_skip");
            }
            Some(RoutinePreflightOutcome::Defer) => {
                return wake_gate_denied(
                    RoutineWakeGateReason::PreflightDeferred,
                    "retry_after_preflight_deferral",
                );
            }
            Some(RoutinePreflightOutcome::AskApproval) => {
                return wake_gate_denied(
                    RoutineWakeGateReason::PreflightApprovalRequired,
                    "request_operator_approval",
                );
            }
            Some(RoutinePreflightOutcome::Fail) => {
                return wake_gate_denied(
                    RoutineWakeGateReason::PreflightFailed,
                    "record_failed_preflight",
                );
            }
            Some(RoutinePreflightOutcome::Proceed) | None => {}
        }
        if input
            .provider_cooldown_until_unix_ms
            .is_some_and(|cooldown| cooldown > input.now_unix_ms)
        {
            return wake_gate_denied(
                RoutineWakeGateReason::ProviderCooldown,
                "wait_for_provider_cooldown",
            );
        }
        if !input.channel_healthy {
            return wake_gate_denied(
                RoutineWakeGateReason::ChannelUnhealthy,
                "route_to_failure_destination_or_operator_inbox",
            );
        }
        if !input.policy_allowed {
            return wake_gate_denied(RoutineWakeGateReason::PolicyDenied, "policy_denied");
        }
        RoutineWakeGateDecision {
            allowed: true,
            reason: RoutineWakeGateReason::Allowed,
            recovery_hint: "dispatch_routine".to_owned(),
        }
    }

    fn validation_error(reason: &str) -> RoutinePreflightValidation {
        RoutinePreflightValidation { accepted: false, reason: reason.to_owned() }
    }

    fn wake_gate_denied(
        reason: RoutineWakeGateReason,
        recovery_hint: &str,
    ) -> RoutineWakeGateDecision {
        RoutineWakeGateDecision { allowed: false, reason, recovery_hint: recovery_hint.to_owned() }
    }

    // Truncates by char count rather than byte count so a multi-byte character is never split;
    // the bound is therefore approximate (a truncated value can exceed max_value_bytes in bytes).
    fn bounded_json_value(value: &Value, max_value_bytes: usize) -> Value {
        let rendered = value.to_string();
        if rendered.len() <= max_value_bytes {
            return value.clone();
        }
        let mut truncated = String::with_capacity(max_value_bytes.min(rendered.len()) + 3);
        for character in rendered.chars().take(max_value_bytes) {
            truncated.push(character);
        }
        truncated.push_str("...");
        Value::String(truncated)
    }
}

/// Why a run was dispatched: a normal trigger, an operator test run, or a replay of a prior run.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RoutineDispatchMode {
    #[default]
    Normal,
    TestRun,
    Replay,
}

impl RoutineDispatchMode {
    /// Returns the canonical snake_case wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::TestRun => "test_run",
            Self::Replay => "replay",
        }
    }
}

/// Execution settings of a routine: session reuse, optional pinned procedure/skill/provider
/// profiles, and the tool-access posture.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineExecutionConfig {
    #[serde(default)]
    pub run_mode: RoutineRunMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub procedure_profile_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_profile_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_profile_id: Option<String>,
    #[serde(default)]
    pub execution_posture: RoutineExecutionPosture,
}

impl Default for RoutineExecutionConfig {
    fn default() -> Self {
        Self {
            run_mode: RoutineRunMode::SameSession,
            procedure_profile_id: None,
            skill_profile_id: None,
            provider_profile_id: None,
            execution_posture: RoutineExecutionPosture::Standard,
        }
    }
}

/// When operator approval is required: never, before the routine can be enabled, or before its
/// first run may deliver.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutineApprovalMode {
    None,
    BeforeEnable,
    BeforeFirstRun,
}

impl RoutineApprovalMode {
    /// Returns the canonical snake_case wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::BeforeEnable => "before_enable",
            Self::BeforeFirstRun => "before_first_run",
        }
    }

    /// Parses a wire name (trimmed, case-insensitive); returns `None` for unknown values.
    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "none" => Some(Self::None),
            "before_enable" => Some(Self::BeforeEnable),
            "before_first_run" => Some(Self::BeforeFirstRun),
            _ => None,
        }
    }
}

/// Delivery settings of a routine, including an optional separate failure target.
///
/// `failure_mode`/`failure_channel` fall back to the success target when unset; validation
/// requires a channel whenever a `SpecificChannel` mode cannot resolve one.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineDeliveryConfig {
    pub mode: RoutineDeliveryMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_mode: Option<RoutineDeliveryMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_channel: Option<String>,
    #[serde(default)]
    pub silent_policy: RoutineSilentPolicy,
}

impl Default for RoutineDeliveryConfig {
    fn default() -> Self {
        Self {
            mode: RoutineDeliveryMode::SameChannel,
            channel: None,
            failure_mode: None,
            failure_channel: None,
            silent_policy: RoutineSilentPolicy::Noisy,
        }
    }
}

/// Daily window (minutes of day, 0..=1439) during which a routine stays quiet; the window may
/// wrap past midnight when `start` is greater than `end`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineQuietHours {
    pub start_minute_of_day: u16,
    pub end_minute_of_day: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
}

/// Approval policy wrapper; high-frequency schedules may force the mode up to `BeforeEnable`
/// via [`routine_approval_policy_with_auto_enable_guard`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineApprovalPolicy {
    pub mode: RoutineApprovalMode,
}

impl Default for RoutineApprovalPolicy {
    fn default() -> Self {
        Self { mode: RoutineApprovalMode::None }
    }
}

/// Applies the routine schedule auto-enable guard to an approval policy.
///
/// Inputs are the normalized schedule type and payload produced by the cron
/// normalization layer. The returned policy preserves normal schedules and
/// fail-closes high-frequency recurring schedules to `before_enable`; malformed
/// `every` payloads are treated as high-frequency for this guard.
#[must_use]
pub fn routine_approval_policy_with_auto_enable_guard(
    schedule_type: CronScheduleType,
    schedule_payload_json: &str,
    approval_policy: RoutineApprovalPolicy,
) -> RoutineApprovalPolicy {
    if schedule_requires_auto_enable_guard(schedule_type, schedule_payload_json)
        && approval_policy.mode != RoutineApprovalMode::BeforeEnable
    {
        return RoutineApprovalPolicy { mode: RoutineApprovalMode::BeforeEnable };
    }
    approval_policy
}

/// Returns true when a normalized schedule is too frequent to auto-enable.
#[must_use]
pub fn schedule_requires_auto_enable_guard(
    schedule_type: CronScheduleType,
    schedule_payload_json: &str,
) -> bool {
    if schedule_type != CronScheduleType::Every {
        return false;
    }
    every_interval_ms_from_schedule_payload(schedule_payload_json)
        .is_none_or(|interval_ms| interval_ms < MIN_AUTO_ENABLE_EVERY_INTERVAL_MS)
}

fn every_interval_ms_from_schedule_payload(schedule_payload_json: &str) -> Option<u64> {
    let payload = serde_json::from_str::<Value>(schedule_payload_json).ok()?;
    payload.get("interval_ms").and_then(|value| {
        value.as_u64().or_else(|| value.as_i64().and_then(|signed| u64::try_from(signed).ok()))
    })
}

/// Persisted routine definition; the routine id matches the backing cron job id for
/// schedule-triggered routines.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineMetadataRecord {
    pub routine_id: String,
    pub trigger_kind: RoutineTriggerKind,
    pub trigger_payload_json: String,
    #[serde(default)]
    pub execution: RoutineExecutionConfig,
    pub delivery: RoutineDeliveryConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quiet_hours: Option<RoutineQuietHours>,
    #[serde(default)]
    pub cooldown_ms: u64,
    #[serde(default)]
    pub approval_policy: RoutineApprovalPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template_id: Option<String>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// Caller-provided fields for [`RoutineRegistry::upsert_routine`]; normalized before storage.
#[derive(Debug, Clone)]
pub struct RoutineMetadataUpsert {
    pub routine_id: String,
    pub trigger_kind: RoutineTriggerKind,
    pub trigger_payload_json: String,
    pub execution: RoutineExecutionConfig,
    pub delivery: RoutineDeliveryConfig,
    pub quiet_hours: Option<RoutineQuietHours>,
    pub cooldown_ms: u64,
    pub approval_policy: RoutineApprovalPolicy,
    pub template_id: Option<String>,
}

/// Persisted per-run metadata that augments the cron run record (trigger context, dispatch
/// mode, outcome overrides, delivery/approval/safety notes).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineRunMetadataRecord {
    pub run_id: String,
    pub routine_id: String,
    pub trigger_kind: RoutineTriggerKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_reason: Option<String>,
    pub trigger_payload_json: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_dedupe_key: Option<String>,
    #[serde(default)]
    pub execution: RoutineExecutionConfig,
    pub delivery: RoutineDeliveryConfig,
    #[serde(default)]
    pub dispatch_mode: RoutineDispatchMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_override: Option<RoutineRunOutcomeKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_delivered: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_note: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub safety_note: Option<String>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// Caller-provided fields for [`RoutineRegistry::upsert_run_metadata`]; normalized before
/// storage.
#[derive(Debug, Clone)]
pub struct RoutineRunMetadataUpsert {
    pub run_id: String,
    pub routine_id: String,
    pub trigger_kind: RoutineTriggerKind,
    pub trigger_reason: Option<String>,
    pub trigger_payload_json: String,
    pub trigger_dedupe_key: Option<String>,
    pub execution: RoutineExecutionConfig,
    pub delivery: RoutineDeliveryConfig,
    pub dispatch_mode: RoutineDispatchMode,
    pub source_run_id: Option<String>,
    pub outcome_override: Option<RoutineRunOutcomeKind>,
    pub outcome_message: Option<String>,
    pub output_delivered: Option<bool>,
    pub skip_reason: Option<String>,
    pub delivery_reason: Option<String>,
    pub approval_note: Option<String>,
    pub safety_note: Option<String>,
}

/// Preview of how a natural-language phrase was parsed into a normalized schedule.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RoutineSchedulePreview {
    pub phrase: String,
    pub normalized_text: String,
    pub explanation: String,
    pub schedule_type: String,
    pub schedule_payload_json: String,
    pub schedule_payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_run_at_unix_ms: Option<i64>,
    pub timezone: String,
}

/// One observation of a watched path; `signature` hashes the fields that define "changed".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineFileWatchObservation {
    pub exists: bool,
    pub path: String,
    pub resolved_path: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_unix_ms: Option<u64>,
    pub signature: String,
}

/// Validated file-watch trigger payload, including the baseline observation polls diff against.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineFileWatchConfig {
    pub path: String,
    pub resolved_path: String,
    pub poll_interval_ms: u64,
    #[serde(default)]
    pub fire_on_start: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_observed: Option<RoutineFileWatchObservation>,
}

/// Detected change on a watched path; `config` carries the advanced baseline to persist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineFileWatchChange {
    pub event: String,
    pub config: RoutineFileWatchConfig,
    pub previous: Option<RoutineFileWatchObservation>,
    pub current: RoutineFileWatchObservation,
}

/// Built-in routine template offered by [`routine_templates`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineTemplateDefinition {
    pub template_id: String,
    pub title: String,
    pub description: String,
    pub trigger_kind: RoutineTriggerKind,
    pub default_name: String,
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub natural_language_schedule: Option<String>,
    pub delivery_mode: RoutineDeliveryMode,
    pub approval_mode: RoutineApprovalMode,
    #[serde(default)]
    pub tags: Vec<String>,
}

/// Portable export of one routine plus its backing cron job, versioned by
/// [`ROUTINE_EXPORT_SCHEMA_ID`]/[`ROUTINE_EXPORT_SCHEMA_VERSION`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineExportBundle {
    pub schema_id: String,
    pub schema_version: u32,
    pub exported_at_unix_ms: i64,
    pub routine: RoutineMetadataRecord,
    pub job: CronJobRecord,
}

/// Retention bounds for run metadata: a time-to-live and a record-count cap.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutineRetentionPolicy {
    pub ttl_ms: i64,
    pub max_records: usize,
}

/// One run metadata record a retention sweep would delete, with its protection status.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RoutineRetentionCandidate {
    pub run_id: String,
    pub routine_id: String,
    pub reason: String,
    pub protected_by_active_ref: bool,
}

/// Result of [`routine_retention_dry_run`]; nothing is deleted, candidates are only reported.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RoutineRetentionDryRun {
    pub dry_run: bool,
    pub would_delete_count: usize,
    pub retained_active_refs: usize,
    pub candidates: Vec<RoutineRetentionCandidate>,
}

/// Cross-reference report of cron jobs, routine metadata, and run metadata produced by
/// [`routine_runtime_backfill_plan`].
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RoutineRuntimeBackfillReport {
    pub dry_run: bool,
    pub cron_jobs_missing_metadata: Vec<String>,
    pub routines_missing_cron_job: Vec<String>,
    pub run_metadata_without_routine: Vec<String>,
    pub changed_records: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RoutineRegistryDocument {
    schema_version: u32,
    #[serde(default)]
    routines: Vec<RoutineMetadataRecord>,
}

impl Default for RoutineRegistryDocument {
    fn default() -> Self {
        Self { schema_version: ROUTINE_REGISTRY_VERSION, routines: Vec::new() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RoutineRunMetadataDocument {
    schema_version: u32,
    #[serde(default)]
    runs: Vec<RoutineRunMetadataRecord>,
}

impl Default for RoutineRunMetadataDocument {
    fn default() -> Self {
        Self { schema_version: ROUTINE_REGISTRY_VERSION, runs: Vec::new() }
    }
}

#[derive(Debug, Clone)]
struct RegistryPath {
    path: PathBuf,
}

impl RegistryPath {
    fn as_path(&self) -> &Path {
        self.path.as_path()
    }

    fn to_path_buf(&self) -> PathBuf {
        self.path.clone()
    }
}

/// File-backed store for routine definitions and run metadata.
///
/// Both documents are held in memory behind mutexes and rewritten wholesale on every mutation,
/// so the on-disk JSON always mirrors the in-memory state. Lock poisoning is reported as
/// [`RoutineRegistryError::LockPoisoned`] instead of panicking.
#[derive(Debug)]
pub struct RoutineRegistry {
    definitions_path: RegistryPath,
    definitions_file: Mutex<fs::File>,
    definitions: Mutex<RoutineRegistryDocument>,
    run_metadata_path: RegistryPath,
    run_metadata_file: Mutex<fs::File>,
    run_metadata: Mutex<RoutineRunMetadataDocument>,
}

/// Errors returned by [`RoutineRegistry`] and the routine helper functions in this module.
#[derive(Debug, Error)]
pub enum RoutineRegistryError {
    #[error("routine registry lock poisoned")]
    LockPoisoned,
    #[error("failed to read routine registry {path}: {source}")]
    ReadRegistry {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse routine registry {path}: {source}")]
    ParseRegistry {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to write routine registry {path}: {source}")]
    WriteRegistry {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize routine registry: {0}")]
    SerializeRegistry(#[from] serde_json::Error),
    #[error("unsupported routine registry version {0}")]
    UnsupportedVersion(u32),
    #[error("routine registry limit exceeded")]
    RegistryLimitExceeded,
    #[error("invalid {field}: {message}")]
    InvalidField { field: &'static str, message: String },
    #[error("system time before unix epoch: {0}")]
    InvalidSystemTime(#[from] std::time::SystemTimeError),
    #[error("failed to resolve default state root: {0}")]
    DefaultStateRoot(#[from] IdentityStorePathError),
}

impl RoutineRegistry {
    /// Opens (or initializes) both registry documents under `<state_root>/routines/`.
    ///
    /// # Errors
    ///
    /// Returns a read/write/parse variant when a document cannot be prepared, or
    /// [`RoutineRegistryError::UnsupportedVersion`] for an unknown schema version.
    pub fn open(state_root: &Path) -> Result<Self, RoutineRegistryError> {
        let routines_root = resolve_routines_root(Some(state_root))?;
        let definitions_path = RegistryPath { path: routines_root.join(ROUTINES_REGISTRY_FILE) };
        let run_metadata_path = RegistryPath { path: routines_root.join(ROUTINE_RUNS_FILE) };
        let mut definitions_file = open_registry_file(&definitions_path)?;
        let definitions = load_registry_document(&definitions_path, &mut definitions_file)?;
        let mut run_metadata_file = open_registry_file(&run_metadata_path)?;
        let run_metadata = load_run_metadata_document(&run_metadata_path, &mut run_metadata_file)?;
        Ok(Self {
            definitions_path,
            definitions_file: Mutex::new(definitions_file),
            definitions: Mutex::new(definitions),
            run_metadata_path,
            run_metadata_file: Mutex::new(run_metadata_file),
            run_metadata: Mutex::new(run_metadata),
        })
    }

    /// Returns a snapshot of all routine definitions in `routine_id` order.
    ///
    /// # Errors
    ///
    /// Returns [`RoutineRegistryError::LockPoisoned`] if a previous holder panicked.
    pub fn list_routines(&self) -> Result<Vec<RoutineMetadataRecord>, RoutineRegistryError> {
        let definitions =
            self.definitions.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        Ok(definitions.routines.clone())
    }

    /// Looks up one routine; returns `Ok(None)` when it does not exist.
    ///
    /// # Errors
    ///
    /// Returns [`RoutineRegistryError::InvalidField`] for a malformed id or
    /// [`RoutineRegistryError::LockPoisoned`] if a previous holder panicked.
    pub fn get_routine(
        &self,
        routine_id: &str,
    ) -> Result<Option<RoutineMetadataRecord>, RoutineRegistryError> {
        let normalized = normalize_identifier(routine_id, "routine_id")?;
        let definitions =
            self.definitions.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        Ok(definitions.routines.iter().find(|entry| entry.routine_id == normalized).cloned())
    }

    /// Normalizes and persists a routine definition; existing routines keep their original
    /// `created_at_unix_ms`. Returns the record as stored.
    ///
    /// # Errors
    ///
    /// Returns [`RoutineRegistryError::InvalidField`] for validation failures,
    /// [`RoutineRegistryError::RegistryLimitExceeded`] when inserting beyond the cap,
    /// [`RoutineRegistryError::LockPoisoned`] if a previous holder panicked, or a write error
    /// when persisting fails.
    pub fn upsert_routine(
        &self,
        request: RoutineMetadataUpsert,
    ) -> Result<RoutineMetadataRecord, RoutineRegistryError> {
        let now = unix_ms_now()?;
        let normalized = normalize_routine_metadata_upsert(request, now)?;
        let mut definitions =
            self.definitions.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        if let Some(existing) =
            definitions.routines.iter_mut().find(|entry| entry.routine_id == normalized.routine_id)
        {
            existing.trigger_kind = normalized.trigger_kind;
            existing.trigger_payload_json = normalized.trigger_payload_json;
            existing.execution = normalized.execution;
            existing.delivery = normalized.delivery;
            existing.quiet_hours = normalized.quiet_hours;
            existing.cooldown_ms = normalized.cooldown_ms;
            existing.approval_policy = normalized.approval_policy;
            existing.template_id = normalized.template_id;
            existing.updated_at_unix_ms = now;
            let updated = existing.clone();
            let document = RoutineRegistryDocument {
                schema_version: ROUTINE_REGISTRY_VERSION,
                routines: definitions.routines.clone(),
            };
            drop(definitions);
            write_registry_document(&self.definitions_path, &self.definitions_file, &document)?;
            return Ok(updated);
        }
        if definitions.routines.len() >= MAX_ROUTINE_COUNT {
            return Err(RoutineRegistryError::RegistryLimitExceeded);
        }
        definitions.routines.push(normalized.clone());
        definitions.routines.sort_by(|left, right| left.routine_id.cmp(&right.routine_id));
        let document = RoutineRegistryDocument {
            schema_version: ROUTINE_REGISTRY_VERSION,
            routines: definitions.routines.clone(),
        };
        drop(definitions);
        write_registry_document(&self.definitions_path, &self.definitions_file, &document)?;
        Ok(normalized)
    }

    /// Deletes a routine definition; returns `false` when no matching routine existed.
    ///
    /// # Errors
    ///
    /// Returns [`RoutineRegistryError::InvalidField`] for a malformed id,
    /// [`RoutineRegistryError::LockPoisoned`] if a previous holder panicked, or a write error
    /// when persisting fails.
    pub fn delete_routine(&self, routine_id: &str) -> Result<bool, RoutineRegistryError> {
        let normalized = normalize_identifier(routine_id, "routine_id")?;
        let mut definitions =
            self.definitions.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        let before = definitions.routines.len();
        definitions.routines.retain(|entry| entry.routine_id != normalized);
        let deleted = definitions.routines.len() != before;
        if deleted {
            let document = RoutineRegistryDocument {
                schema_version: ROUTINE_REGISTRY_VERSION,
                routines: definitions.routines.clone(),
            };
            drop(definitions);
            write_registry_document(&self.definitions_path, &self.definitions_file, &document)?;
        }
        Ok(deleted)
    }

    /// Reconciles schedule-triggered routine definitions against the current cron job set:
    /// refreshes payloads for existing jobs, creates default definitions for new jobs, and
    /// removes schedule routines whose cron job disappeared. Non-schedule routines are never
    /// touched.
    ///
    /// # Errors
    ///
    /// Returns [`RoutineRegistryError::LockPoisoned`] if a previous holder panicked, or a
    /// serialize/write error when persisting fails.
    pub fn sync_schedule_routines(
        &self,
        cron_jobs: &[CronJobRecord],
    ) -> Result<(), RoutineRegistryError> {
        let now = unix_ms_now()?;
        let mut definitions =
            self.definitions.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        let schedule_ids =
            cron_jobs.iter().map(|job| job.job_id.clone()).collect::<BTreeSet<String>>();
        for job in cron_jobs {
            if let Some(existing) =
                definitions.routines.iter_mut().find(|entry| entry.routine_id == job.job_id)
            {
                // A non-schedule routine that shares an id with a cron job stays authoritative;
                // only schedule-triggered definitions mirror the cron payload.
                if existing.trigger_kind != RoutineTriggerKind::Schedule {
                    continue;
                }
                existing.trigger_payload_json = build_schedule_trigger_payload(job)?;
                existing.updated_at_unix_ms = now;
                continue;
            }
            definitions.routines.push(RoutineMetadataRecord {
                routine_id: job.job_id.clone(),
                trigger_kind: RoutineTriggerKind::Schedule,
                trigger_payload_json: build_schedule_trigger_payload(job)?,
                execution: RoutineExecutionConfig::default(),
                delivery: RoutineDeliveryConfig::default(),
                quiet_hours: None,
                cooldown_ms: 0,
                approval_policy: RoutineApprovalPolicy::default(),
                template_id: None,
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            });
        }
        definitions.routines.retain(|entry| {
            entry.trigger_kind != RoutineTriggerKind::Schedule
                || schedule_ids.contains(&entry.routine_id)
        });
        definitions.routines.sort_by(|left, right| left.routine_id.cmp(&right.routine_id));
        let document = RoutineRegistryDocument {
            schema_version: ROUTINE_REGISTRY_VERSION,
            routines: definitions.routines.clone(),
        };
        drop(definitions);
        write_registry_document(&self.definitions_path, &self.definitions_file, &document)
    }

    /// Returns the newest run metadata entries (optionally for one routine) in chronological
    /// order; `limit` is clamped to the registry capacity and to at least one entry.
    ///
    /// # Errors
    ///
    /// Returns [`RoutineRegistryError::InvalidField`] for a malformed routine id or
    /// [`RoutineRegistryError::LockPoisoned`] if a previous holder panicked.
    pub fn list_run_metadata(
        &self,
        routine_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RoutineRunMetadataRecord>, RoutineRegistryError> {
        let normalized_routine_id = match routine_id {
            Some(value) => Some(normalize_identifier(value, "routine_id")?),
            None => None,
        };
        let run_metadata =
            self.run_metadata.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        let mut entries = run_metadata
            .runs
            .iter()
            .rev()
            .filter(|entry| {
                normalized_routine_id
                    .as_ref()
                    .is_none_or(|routine_id| entry.routine_id == *routine_id)
            })
            .take(limit.clamp(1, MAX_ROUTINE_RUN_METADATA_COUNT))
            .cloned()
            .collect::<Vec<_>>();
        entries.reverse();
        Ok(entries)
    }

    /// Looks up run metadata by run id; returns `Ok(None)` when it does not exist.
    ///
    /// # Errors
    ///
    /// Returns [`RoutineRegistryError::InvalidField`] for a malformed id or
    /// [`RoutineRegistryError::LockPoisoned`] if a previous holder panicked.
    pub fn find_run_metadata(
        &self,
        run_id: &str,
    ) -> Result<Option<RoutineRunMetadataRecord>, RoutineRegistryError> {
        let normalized = normalize_identifier(run_id, "run_id")?;
        let run_metadata =
            self.run_metadata.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        Ok(run_metadata.runs.iter().find(|entry| entry.run_id == normalized).cloned())
    }

    /// Returns whether any retained run of this routine already used `dedupe_key`.
    ///
    /// Deduplication only spans the retained metadata window: once old records are evicted by
    /// the capacity cap, their dedupe keys are forgotten with them.
    ///
    /// # Errors
    ///
    /// Returns [`RoutineRegistryError::InvalidField`] for malformed inputs or
    /// [`RoutineRegistryError::LockPoisoned`] if a previous holder panicked.
    pub fn seen_dedupe_key(
        &self,
        routine_id: &str,
        dedupe_key: &str,
    ) -> Result<bool, RoutineRegistryError> {
        let normalized_routine_id = normalize_identifier(routine_id, "routine_id")?;
        let normalized_dedupe_key =
            normalize_freeform_identifier(dedupe_key, "trigger_dedupe_key")?;
        let run_metadata =
            self.run_metadata.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        Ok(run_metadata.runs.iter().any(|entry| {
            entry.routine_id == normalized_routine_id
                && entry.trigger_dedupe_key.as_deref() == Some(normalized_dedupe_key.as_str())
        }))
    }

    /// Normalizes and persists run metadata keyed by run id; when the capacity cap is exceeded
    /// the oldest records are evicted first (FIFO). Returns the record as stored.
    ///
    /// # Errors
    ///
    /// Returns [`RoutineRegistryError::InvalidField`] for validation failures,
    /// [`RoutineRegistryError::LockPoisoned`] if a previous holder panicked, or a write error
    /// when persisting fails.
    pub fn upsert_run_metadata(
        &self,
        request: RoutineRunMetadataUpsert,
    ) -> Result<RoutineRunMetadataRecord, RoutineRegistryError> {
        let now = unix_ms_now()?;
        let normalized = normalize_routine_run_metadata_upsert(request, now)?;
        let mut run_metadata =
            self.run_metadata.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        if let Some(existing) =
            run_metadata.runs.iter_mut().find(|entry| entry.run_id == normalized.run_id)
        {
            existing.trigger_kind = normalized.trigger_kind;
            existing.trigger_reason = normalized.trigger_reason;
            existing.trigger_payload_json = normalized.trigger_payload_json;
            existing.trigger_dedupe_key = normalized.trigger_dedupe_key;
            existing.execution = normalized.execution;
            existing.delivery = normalized.delivery;
            existing.dispatch_mode = normalized.dispatch_mode;
            existing.source_run_id = normalized.source_run_id;
            existing.outcome_override = normalized.outcome_override;
            existing.outcome_message = normalized.outcome_message;
            existing.output_delivered = normalized.output_delivered;
            existing.skip_reason = normalized.skip_reason;
            existing.delivery_reason = normalized.delivery_reason;
            existing.approval_note = normalized.approval_note;
            existing.safety_note = normalized.safety_note;
            existing.updated_at_unix_ms = now;
            let updated = existing.clone();
            let document = RoutineRunMetadataDocument {
                schema_version: ROUTINE_REGISTRY_VERSION,
                runs: run_metadata.runs.clone(),
            };
            drop(run_metadata);
            write_registry_document(&self.run_metadata_path, &self.run_metadata_file, &document)?;
            return Ok(updated);
        }
        run_metadata.runs.push(normalized.clone());
        if run_metadata.runs.len() > MAX_ROUTINE_RUN_METADATA_COUNT {
            let overflow = run_metadata.runs.len() - MAX_ROUTINE_RUN_METADATA_COUNT;
            run_metadata.runs.drain(0..overflow);
        }
        let document = RoutineRunMetadataDocument {
            schema_version: ROUTINE_REGISTRY_VERSION,
            runs: run_metadata.runs.clone(),
        };
        drop(run_metadata);
        write_registry_document(&self.run_metadata_path, &self.run_metadata_file, &document)?;
        Ok(normalized)
    }
}

/// Resolves (and creates if needed) the routines storage directory under the given state root,
/// falling back to the default state root when `None`.
///
/// # Errors
///
/// Returns [`RoutineRegistryError::DefaultStateRoot`] when the default root cannot be resolved
/// or [`RoutineRegistryError::WriteRegistry`] when the directory cannot be created.
pub fn resolve_routines_root(state_root: Option<&Path>) -> Result<PathBuf, RoutineRegistryError> {
    let root = match state_root {
        Some(path) => path.to_path_buf(),
        None => default_state_root()?,
    };
    let routines_root = root.join(ROUTINES_DIR);
    fs::create_dir_all(routines_root.as_path()).map_err(|source| {
        RoutineRegistryError::WriteRegistry { path: routines_root.clone(), source }
    })?;
    Ok(routines_root)
}

/// Returns the `at` schedule payload pointing at [`SHADOW_AT_TIMESTAMP_RFC3339`], used as a
/// never-firing shadow schedule for manually triggered routines.
#[must_use]
pub fn shadow_manual_schedule_payload_json() -> String {
    json!({ "timestamp_rfc3339": SHADOW_AT_TIMESTAMP_RFC3339 }).to_string()
}

/// Validates a raw file-watch trigger payload and takes the initial observation.
///
/// With `fire_on_start` the baseline is left empty so the first poll reports an event even for
/// a pre-existing file; otherwise the current observation becomes the baseline and pre-existing
/// state does not fire.
///
/// # Errors
///
/// Returns [`RoutineRegistryError::InvalidField`] when the payload, path, or poll interval is
/// missing or invalid, or when the path fails the watch-root policy.
pub fn normalize_file_watch_trigger_payload(
    payload: Option<&Value>,
) -> Result<RoutineFileWatchConfig, RoutineRegistryError> {
    let payload = payload.ok_or_else(|| RoutineRegistryError::InvalidField {
        field: "trigger_payload",
        message: "file_watch routines require trigger_payload.path".to_owned(),
    })?;
    let path = payload
        .get("path")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RoutineRegistryError::InvalidField {
            field: "trigger_payload.path",
            message: "file_watch path must be a non-empty absolute OS path".to_owned(),
        })?;
    let poll_interval_ms = payload
        .get("poll_interval_ms")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_FILE_WATCH_POLL_INTERVAL_MS);
    if poll_interval_ms < MIN_FILE_WATCH_POLL_INTERVAL_MS {
        return Err(RoutineRegistryError::InvalidField {
            field: "trigger_payload.poll_interval_ms",
            message: format!(
                "file_watch poll interval must be at least {MIN_FILE_WATCH_POLL_INTERVAL_MS} ms"
            ),
        });
    }
    let fire_on_start = payload.get("fire_on_start").and_then(Value::as_bool).unwrap_or(false);
    let observation = observe_file_watch_path(path)?;
    Ok(RoutineFileWatchConfig {
        path: path.to_owned(),
        resolved_path: observation.resolved_path.clone(),
        poll_interval_ms,
        fire_on_start,
        last_observed: if fire_on_start { None } else { Some(observation) },
    })
}

/// Parses a stored file-watch trigger payload back into its config.
///
/// # Errors
///
/// Returns [`RoutineRegistryError::InvalidField`] when the JSON does not match
/// [`RoutineFileWatchConfig`].
pub fn parse_file_watch_config(
    payload_json: &str,
) -> Result<RoutineFileWatchConfig, RoutineRegistryError> {
    serde_json::from_str::<RoutineFileWatchConfig>(payload_json).map_err(|error| {
        RoutineRegistryError::InvalidField {
            field: "trigger_payload_json",
            message: format!("file_watch trigger payload is invalid: {error}"),
        }
    })
}

/// Re-observes the watched path and reports a change when its signature moved from the
/// baseline; the returned config carries the advanced baseline either way.
///
/// # Errors
///
/// Returns [`RoutineRegistryError::InvalidField`] when the path can no longer be observed or
/// fails the watch-root policy.
pub fn evaluate_file_watch_change(
    mut config: RoutineFileWatchConfig,
) -> Result<Option<RoutineFileWatchChange>, RoutineRegistryError> {
    let current = observe_file_watch_path(config.path.as_str())?;
    // take() instead of clone(): last_observed is unconditionally replaced below, so the old
    // baseline can be moved out as `previous` without copying it.
    let previous = config.last_observed.take();
    if previous.as_ref().is_some_and(|previous| previous.signature == current.signature) {
        config.last_observed = Some(current);
        return Ok(None);
    }
    let event = match previous.as_ref() {
        // No baseline means fire_on_start: the very first poll classifies whatever it finds.
        None if current.exists => "created",
        None => "missing",
        Some(previous) if !previous.exists && current.exists => "created",
        Some(previous) if previous.exists && !current.exists => "deleted",
        Some(_) => "modified",
    }
    .to_owned();
    config.resolved_path = current.resolved_path.clone();
    config.last_observed = Some(current.clone());
    Ok(Some(RoutineFileWatchChange { event, config, previous, current }))
}

/// Observes a watched path: validates and resolves it, enforces the watch-root policy, and
/// captures existence, kind, size, mtime, and a change signature. A missing path is a valid
/// observation, not an error.
///
/// # Errors
///
/// Returns [`RoutineRegistryError::InvalidField`] for malformed or policy-rejected paths and
/// for metadata failures other than not-found.
pub fn observe_file_watch_path(
    path: &str,
) -> Result<RoutineFileWatchObservation, RoutineRegistryError> {
    let requested_path = parse_absolute_watch_path(path)?;
    let resolved_path = resolve_watch_target_path(requested_path.as_path())?;
    ensure_watch_path_allowed(resolved_path.as_path())?;
    let requested = display_path(requested_path.as_path());
    let resolved = display_path(resolved_path.as_path());
    let metadata = match fs::metadata(resolved_path.as_path()) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let signature = file_watch_signature(false, resolved.as_str(), "missing", None, None);
            return Ok(RoutineFileWatchObservation {
                exists: false,
                path: requested,
                resolved_path: resolved,
                kind: "missing".to_owned(),
                size_bytes: None,
                modified_unix_ms: None,
                signature,
            });
        }
        Err(error) => {
            return Err(RoutineRegistryError::InvalidField {
                field: "trigger_payload.path",
                message: format!("failed to inspect watched path: {error}"),
            })
        }
    };
    let kind = if metadata.is_dir() {
        "directory"
    } else if metadata.is_file() {
        "file"
    } else {
        "other"
    };
    let size_bytes = Some(metadata.len());
    let modified_unix_ms = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX));
    let signature =
        file_watch_signature(true, resolved.as_str(), kind, size_bytes, modified_unix_ms);
    Ok(RoutineFileWatchObservation {
        exists: true,
        path: requested,
        resolved_path: resolved,
        kind: kind.to_owned(),
        size_bytes,
        modified_unix_ms,
        signature,
    })
}

fn file_watch_signature(
    exists: bool,
    resolved_path: &str,
    kind: &str,
    size_bytes: Option<u64>,
    modified_unix_ms: Option<u64>,
) -> String {
    let payload = json!({
        "exists": exists,
        "resolved_path": resolved_path,
        "kind": kind,
        "size_bytes": size_bytes,
        "modified_unix_ms": modified_unix_ms,
    });
    hex::encode(Sha256::digest(payload.to_string().as_bytes()))
}

fn parse_absolute_watch_path(path: &str) -> Result<PathBuf, RoutineRegistryError> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(RoutineRegistryError::InvalidField {
            field: "trigger_payload.path",
            message: "path must be non-empty".to_owned(),
        });
    }
    if trimmed.chars().any(char::is_control) {
        return Err(RoutineRegistryError::InvalidField {
            field: "trigger_payload.path",
            message: "path contains unsupported control characters".to_owned(),
        });
    }
    let parsed = PathBuf::from(trimmed);
    if !parsed.is_absolute() {
        return Err(RoutineRegistryError::InvalidField {
            field: "trigger_payload.path",
            message: "file_watch path must be absolute".to_owned(),
        });
    }
    if parsed
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(RoutineRegistryError::InvalidField {
            field: "trigger_payload.path",
            message: "file_watch path must not contain '.' or '..' components".to_owned(),
        });
    }
    Ok(parsed)
}

// Canonicalizes through the nearest existing ancestor so a watch may target a file that does
// not exist yet (its later creation is exactly the event being watched for).
fn resolve_watch_target_path(path: &Path) -> Result<PathBuf, RoutineRegistryError> {
    if path.exists() {
        return fs::canonicalize(path).map_err(|error| RoutineRegistryError::InvalidField {
            field: "trigger_payload.path",
            message: format!("failed to resolve watched path: {error}"),
        });
    }
    let (existing_ancestor, missing_suffix) = nearest_existing_watch_ancestor(path)?;
    let canonical_ancestor = fs::canonicalize(existing_ancestor.as_path()).map_err(|error| {
        RoutineRegistryError::InvalidField {
            field: "trigger_payload.path",
            message: format!("failed to resolve watched path ancestor: {error}"),
        }
    })?;
    Ok(canonical_ancestor.join(missing_suffix))
}

fn nearest_existing_watch_ancestor(
    path: &Path,
) -> Result<(PathBuf, PathBuf), RoutineRegistryError> {
    let mut cursor = path.to_path_buf();
    while !cursor.exists() {
        if !cursor.pop() {
            return Err(RoutineRegistryError::InvalidField {
                field: "trigger_payload.path",
                message: "watched path has no existing ancestor".to_owned(),
            });
        }
    }
    if !cursor.is_dir() {
        let Some(parent) = cursor.parent() else {
            return Err(RoutineRegistryError::InvalidField {
                field: "trigger_payload.path",
                message: "watched path ancestor has no parent directory".to_owned(),
            });
        };
        cursor = parent.to_path_buf();
    }
    let suffix = path
        .strip_prefix(cursor.as_path())
        .map_err(|_| RoutineRegistryError::InvalidField {
            field: "trigger_payload.path",
            message: "failed to resolve watched path relative to existing ancestor".to_owned(),
        })?
        .to_path_buf();
    Ok((cursor, suffix))
}

// Fail-closed watch-path policy: protected OS locations are always rejected, and everything
// else must live under a user-owned root (home/profile/temp). Watch targets come from operator
// input, so this is the boundary that keeps routines from observing system paths.
fn ensure_watch_path_allowed(path: &Path) -> Result<(), RoutineRegistryError> {
    if protected_os_path(path) {
        return Err(RoutineRegistryError::InvalidField {
            field: "trigger_payload.path",
            message: format!("watched path is protected: {}", display_path(path)),
        });
    }
    let roots = user_owned_os_roots();
    if roots.iter().any(|root| path_starts_with(path, root.as_path())) {
        return Ok(());
    }
    Err(RoutineRegistryError::InvalidField {
        field: "trigger_payload.path",
        message: format!(
            "watched path {} is outside approved user-owned OS roots",
            display_path(path)
        ),
    })
}

fn user_owned_os_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    for key in ["USERPROFILE", "HOME"] {
        if let Some(value) = std::env::var_os(key) {
            push_canonical_root(&mut roots, PathBuf::from(value));
        }
    }
    push_canonical_root(&mut roots, std::env::temp_dir());
    #[cfg(unix)]
    {
        push_canonical_root(&mut roots, PathBuf::from("/var/tmp"));
    }
    roots
}

fn push_canonical_root(roots: &mut Vec<PathBuf>, root: PathBuf) {
    if let Ok(canonical) = fs::canonicalize(root.as_path()) {
        if canonical.is_dir()
            && !roots.iter().any(|existing| same_path(existing.as_path(), canonical.as_path()))
        {
            roots.push(canonical);
        }
    }
}

fn protected_os_path(path: &Path) -> bool {
    #[cfg(windows)]
    {
        let normalized = path.to_string_lossy().replace('\\', "/").to_ascii_lowercase();
        normalized.ends_with(":/")
            || normalized.contains(":/windows")
            || normalized.contains(":/program files")
            || normalized.contains(":/program files (x86)")
            || normalized.contains(":/system volume information")
    }
    #[cfg(not(windows))]
    {
        let normalized = path.to_string_lossy().replace('\\', "/");
        if normalized == "/" {
            return true;
        }
        for prefix in ["/etc", "/bin", "/sbin", "/usr", "/lib", "/lib64", "/System", "/Library"] {
            if normalized == prefix || normalized.starts_with(format!("{prefix}/").as_str()) {
                return true;
            }
        }
        false
    }
}

fn path_starts_with(path: &Path, root: &Path) -> bool {
    if path.starts_with(root) {
        return true;
    }
    // Windows paths compare case-insensitively with normalized separators, because the same
    // location can be spelled with different casing and slash styles.
    #[cfg(windows)]
    {
        let path = path.to_string_lossy().replace('\\', "/").to_ascii_lowercase();
        let root = root.to_string_lossy().replace('\\', "/").to_ascii_lowercase();
        path == root || path.starts_with(format!("{root}/").as_str())
    }
    #[cfg(not(windows))]
    {
        false
    }
}

fn same_path(left: &Path, right: &Path) -> bool {
    #[cfg(windows)]
    {
        left.to_string_lossy()
            .replace('\\', "/")
            .eq_ignore_ascii_case(&right.to_string_lossy().replace('\\', "/"))
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Maps a raw cron run status to the routine outcome used when metadata supplies no override.
#[must_use]
pub fn default_outcome_from_cron_status(status: CronRunStatus) -> RoutineRunOutcomeKind {
    match status {
        CronRunStatus::Succeeded => RoutineRunOutcomeKind::SuccessWithOutput,
        CronRunStatus::Skipped => RoutineRunOutcomeKind::Skipped,
        CronRunStatus::Denied => RoutineRunOutcomeKind::Denied,
        CronRunStatus::Failed => RoutineRunOutcomeKind::Failed,
        CronRunStatus::Accepted | CronRunStatus::Running => RoutineRunOutcomeKind::Pending,
    }
}

/// Derives the lease/approval/delivery lifecycle view of one run at `now_unix_ms`.
///
/// The run's `updated_at_unix_ms` doubles as its heartbeat: an active run that has not been
/// updated within [`ROUTINE_RUN_LEASE_TTL_MS`] is reported as lease-expired. Delivery is ready
/// only for terminal runs whose approval gate is satisfied.
#[must_use]
pub fn routine_run_lifecycle_snapshot(
    routine_id: &str,
    run: &CronRunRecord,
    metadata: Option<&RoutineRunMetadataRecord>,
    approval_policy: &RoutineApprovalPolicy,
    now_unix_ms: i64,
) -> RoutineRunLifecycleSnapshot {
    let terminal = !run.status.is_active();
    let lease_state = if terminal {
        RoutineRunLeaseState::Released
    } else if now_unix_ms.saturating_sub(run.updated_at_unix_ms) > ROUTINE_RUN_LEASE_TTL_MS {
        RoutineRunLeaseState::Expired
    } else {
        RoutineRunLeaseState::Active
    };
    let approval_gate = routine_approval_gate_state(approval_policy, metadata);
    let delivery_ready = terminal
        && matches!(
            approval_gate,
            RoutineApprovalGateState::NotRequired | RoutineApprovalGateState::Approved
        );
    let recovery_hint = if lease_state == RoutineRunLeaseState::Expired {
        "repair_or_cancel_expired_routine_run_lease"
    } else if approval_gate == RoutineApprovalGateState::Pending {
        "request_or_resolve_operator_approval"
    } else if approval_gate == RoutineApprovalGateState::Denied {
        "keep_delivery_suppressed_after_denied_approval"
    } else if !terminal {
        "wait_for_terminal_routine_run"
    } else {
        "delivery_contract_ready"
    };

    RoutineRunLifecycleSnapshot {
        run_id: run.run_id.clone(),
        routine_id: routine_id.to_owned(),
        status: run.status,
        lease_state,
        approval_gate,
        terminal,
        delivery_ready,
        recovery_hint: recovery_hint.to_owned(),
    }
}

/// Joins a cron run record with its optional routine metadata into the JSON shape served to
/// console and CLI surfaces; missing metadata falls back to sensible defaults.
pub fn join_run_metadata(
    routine_id: &str,
    run: &CronRunRecord,
    metadata: Option<&RoutineRunMetadataRecord>,
) -> Value {
    let terminal = !run.status.is_active();
    let outcome_kind = effective_run_outcome_kind(run.status, metadata);
    let execution = metadata.map(|entry| entry.execution.clone()).unwrap_or_default();
    let delivery = metadata.map(|entry| entry.delivery.clone()).unwrap_or_default();
    let output_delivered = terminal
        && metadata
            .and_then(|entry| entry.output_delivered)
            .unwrap_or_else(|| delivery_announced_for_outcome(&delivery, outcome_kind));
    let delivery_reason = if terminal {
        metadata
            .and_then(|entry| entry.delivery_reason.clone())
            .unwrap_or_else(|| delivery_reason_for_outcome(&delivery, outcome_kind))
    } else {
        delivery_reason_for_outcome(&delivery, outcome_kind)
    };
    let effective_delivery = effective_delivery_target(
        &delivery,
        matches!(outcome_kind, RoutineRunOutcomeKind::Failed | RoutineRunOutcomeKind::Denied),
    );
    let delivery_contract = routine_delivery_contract(
        &delivery,
        outcome_kind,
        metadata
            .and_then(|entry| entry.approval_note.as_deref())
            .is_some_and(approval_note_requires_operator_review),
    );
    // AIDEV-NOTE: this join has neither the routine's approval policy nor a real clock, so the
    // embedded lifecycle uses the default (approval-free) policy and the run's own updated_at as
    // "now" -- it can therefore never report Pending/Denied approval or an Expired lease. Callers
    // that need those signals must call routine_run_lifecycle_snapshot with real inputs.
    let lifecycle = routine_run_lifecycle_snapshot(
        routine_id,
        run,
        metadata,
        &RoutineApprovalPolicy::default(),
        run.updated_at_unix_ms,
    );
    json!({
        "routine_id": routine_id,
        "run_id": run.run_id,
        "status": run.status.as_str(),
        "outcome_kind": outcome_kind.as_str(),
        "outcome_provisional": !terminal,
        "outcome_message": metadata.and_then(|entry| entry.outcome_message.clone()).or_else(|| run.error_message_redacted.clone()),
        "error_kind": run.error_kind,
        "trigger_kind": metadata.map(|entry| entry.trigger_kind.as_str()).unwrap_or(RoutineTriggerKind::Schedule.as_str()),
        "trigger_reason": metadata.and_then(|entry| entry.trigger_reason.clone()),
        "trigger_payload": metadata.and_then(|entry| serde_json::from_str::<Value>(&entry.trigger_payload_json).ok()).unwrap_or_else(|| json!({})),
        "run_mode": execution.run_mode.as_str(),
        "execution_posture": execution.execution_posture.as_str(),
        "procedure_profile_id": execution.procedure_profile_id,
        "skill_profile_id": execution.skill_profile_id,
        "provider_profile_id": execution.provider_profile_id,
        "provider_routing": provider_routing_preview(&execution),
        "delivery_mode": delivery.mode.as_str(),
        "delivery_channel": delivery.channel,
        "delivery_failure_mode": delivery.failure_mode.map(RoutineDeliveryMode::as_str),
        "delivery_failure_channel": delivery.failure_channel,
        "silent_policy": delivery.silent_policy.as_str(),
        "delivery_preview": routine_delivery_preview(&delivery),
        "delivery_contract": delivery_contract,
        "lifecycle": lifecycle,
        "effective_delivery_mode": effective_delivery.mode.as_str(),
        "effective_delivery_channel": effective_delivery.channel,
        "delivery_reason": delivery_reason,
        "dispatch_mode": metadata.map(|entry| entry.dispatch_mode.as_str()).unwrap_or(RoutineDispatchMode::Normal.as_str()),
        "source_run_id": metadata.and_then(|entry| entry.source_run_id.clone()),
        "skip_reason": metadata.and_then(|entry| entry.skip_reason.clone()).or_else(|| run.error_kind.clone()),
        "approval_note": metadata.and_then(|entry| entry.approval_note.clone()),
        "safety_note": metadata.and_then(|entry| entry.safety_note.clone()),
        "output_delivered": output_delivered,
        "attempt": run.attempt,
        "session_id": run.session_id,
        "orchestrator_run_id": run.orchestrator_run_id,
        "started_at_unix_ms": run.started_at_unix_ms,
        "finished_at_unix_ms": run.finished_at_unix_ms,
        "model_tokens_in": run.model_tokens_in,
        "model_tokens_out": run.model_tokens_out,
        "tool_calls": run.tool_calls,
        "tool_denies": run.tool_denies,
    })
}

fn effective_run_outcome_kind(
    status: CronRunStatus,
    metadata: Option<&RoutineRunMetadataRecord>,
) -> RoutineRunOutcomeKind {
    if status.is_active() {
        return RoutineRunOutcomeKind::Pending;
    }
    // A stale Pending override on a terminal run is reconciled back to the status-derived
    // outcome; a terminal run must never present itself as still pending.
    match metadata.and_then(|entry| entry.outcome_override) {
        Some(RoutineRunOutcomeKind::Pending) | None => default_outcome_from_cron_status(status),
        Some(outcome_kind) => outcome_kind,
    }
}

/// Packages a routine and its cron job into a versioned export bundle stamped with the current
/// time.
///
/// # Errors
///
/// Returns [`RoutineRegistryError::InvalidSystemTime`] when the system clock reports a
/// pre-epoch time.
pub fn build_routine_export_bundle(
    job: &CronJobRecord,
    routine: &RoutineMetadataRecord,
) -> Result<RoutineExportBundle, RoutineRegistryError> {
    Ok(RoutineExportBundle {
        schema_id: ROUTINE_EXPORT_SCHEMA_ID.to_owned(),
        schema_version: ROUTINE_EXPORT_SCHEMA_VERSION,
        exported_at_unix_ms: unix_ms_now()?,
        routine: routine.clone(),
        job: job.clone(),
    })
}

/// Checks an imported bundle against the supported schema id and version.
///
/// # Errors
///
/// Returns [`RoutineRegistryError::InvalidField`] naming the mismatched field.
pub fn validate_routine_export_bundle(
    bundle: &RoutineExportBundle,
) -> Result<(), RoutineRegistryError> {
    if bundle.schema_id.trim() != ROUTINE_EXPORT_SCHEMA_ID {
        return Err(RoutineRegistryError::InvalidField {
            field: "schema_id",
            message: format!(
                "unsupported routine export schema '{}'; expected {}",
                bundle.schema_id, ROUTINE_EXPORT_SCHEMA_ID
            ),
        });
    }
    if bundle.schema_version != ROUTINE_EXPORT_SCHEMA_VERSION {
        return Err(RoutineRegistryError::InvalidField {
            field: "schema_version",
            message: format!(
                "unsupported routine export schema version {}; expected {}",
                bundle.schema_version, ROUTINE_EXPORT_SCHEMA_VERSION
            ),
        });
    }
    Ok(())
}

/// Plans (without applying) which run metadata records a retention sweep would delete under
/// `policy`; records referenced by `active_run_ids` are reported but protected from deletion.
#[allow(dead_code)]
#[must_use]
pub fn routine_retention_dry_run(
    runs: &[RoutineRunMetadataRecord],
    active_run_ids: &BTreeSet<String>,
    policy: RoutineRetentionPolicy,
    now_unix_ms: i64,
) -> RoutineRetentionDryRun {
    // `runs` is stored oldest-first, so the first `overflow_count` entries are the ones the
    // max_records cap would evict.
    let overflow_count = runs.len().saturating_sub(policy.max_records);
    let mut candidates = Vec::new();
    let mut retained_active_refs = 0usize;
    for (position, run) in runs.iter().enumerate() {
        let expired = now_unix_ms.saturating_sub(run.updated_at_unix_ms) > policy.ttl_ms;
        let overflow = position < overflow_count;
        if !expired && !overflow {
            continue;
        }
        let protected_by_active_ref = active_run_ids.contains(run.run_id.as_str());
        if protected_by_active_ref {
            retained_active_refs = retained_active_refs.saturating_add(1);
        }
        candidates.push(RoutineRetentionCandidate {
            run_id: run.run_id.clone(),
            routine_id: run.routine_id.clone(),
            reason: if expired { "ttl_expired" } else { "max_records_overflow" }.to_owned(),
            protected_by_active_ref,
        });
    }
    let would_delete_count =
        candidates.iter().filter(|candidate| !candidate.protected_by_active_ref).count();
    RoutineRetentionDryRun { dry_run: true, would_delete_count, retained_active_refs, candidates }
}

/// Cross-references cron jobs, routine definitions, and run metadata and reports every orphan
/// in deterministic (sorted) order; this function only plans, it never mutates.
#[allow(dead_code)]
#[must_use]
pub fn routine_runtime_backfill_plan(
    routines: &[RoutineMetadataRecord],
    cron_jobs: &[CronJobRecord],
    run_metadata: &[RoutineRunMetadataRecord],
    dry_run: bool,
) -> RoutineRuntimeBackfillReport {
    let routine_ids =
        routines.iter().map(|routine| routine.routine_id.as_str()).collect::<BTreeSet<_>>();
    let cron_job_ids = cron_jobs.iter().map(|job| job.job_id.as_str()).collect::<BTreeSet<_>>();

    let mut cron_jobs_missing_metadata = cron_jobs
        .iter()
        .filter(|job| !routine_ids.contains(job.job_id.as_str()))
        .map(|job| job.job_id.clone())
        .collect::<Vec<_>>();
    let mut routines_missing_cron_job = routines
        .iter()
        .filter(|routine| !cron_job_ids.contains(routine.routine_id.as_str()))
        .map(|routine| routine.routine_id.clone())
        .collect::<Vec<_>>();
    let mut run_metadata_without_routine = run_metadata
        .iter()
        .filter(|run| !routine_ids.contains(run.routine_id.as_str()))
        .map(|run| run.run_id.clone())
        .collect::<Vec<_>>();

    cron_jobs_missing_metadata.sort();
    routines_missing_cron_job.sort();
    run_metadata_without_routine.sort();
    let changed_records = cron_jobs_missing_metadata
        .len()
        .saturating_add(routines_missing_cron_job.len())
        .saturating_add(run_metadata_without_routine.len());
    RoutineRuntimeBackfillReport {
        dry_run,
        cron_jobs_missing_metadata,
        routines_missing_cron_job,
        run_metadata_without_routine,
        changed_records,
    }
}

/// Returns the built-in routine template pack (see [`ROUTINE_TEMPLATE_PACK_VERSION`]).
#[must_use]
pub fn routine_templates() -> Vec<RoutineTemplateDefinition> {
    vec![
        RoutineTemplateDefinition {
            template_id: "heartbeat".to_owned(),
            title: "Heartbeat".to_owned(),
            description: "Lightweight liveness check that posts a brief status heartbeat on a fixed cadence."
                .to_owned(),
            trigger_kind: RoutineTriggerKind::Schedule,
            default_name: "Heartbeat".to_owned(),
            prompt: "Check system heartbeat, summarize current status in one short paragraph, and include only actionable anomalies."
                .to_owned(),
            natural_language_schedule: Some("every weekday at 9".to_owned()),
            delivery_mode: RoutineDeliveryMode::SameChannel,
            approval_mode: RoutineApprovalMode::None,
            tags: vec!["status".to_owned(), "ops".to_owned()],
        },
        RoutineTemplateDefinition {
            template_id: "daily-report".to_owned(),
            title: "Daily report".to_owned(),
            description: "Collect a compact daily operational report for a chosen channel or team inbox."
                .to_owned(),
            trigger_kind: RoutineTriggerKind::Schedule,
            default_name: "Daily report".to_owned(),
            prompt: "Prepare a daily report covering incidents, pending approvals, and notable usage changes. Keep the output concise and operator-focused."
                .to_owned(),
            natural_language_schedule: Some("every weekday at 17".to_owned()),
            delivery_mode: RoutineDeliveryMode::SpecificChannel,
            approval_mode: RoutineApprovalMode::None,
            tags: vec!["report".to_owned(), "ops".to_owned()],
        },
        RoutineTemplateDefinition {
            template_id: "follow-up".to_owned(),
            title: "Follow-up".to_owned(),
            description: "Reusable follow-up template for manual or event-driven reminders."
                .to_owned(),
            trigger_kind: RoutineTriggerKind::Manual,
            default_name: "Follow-up".to_owned(),
            prompt: "Review the latest context for the routine target and draft a short follow-up with next steps only."
                .to_owned(),
            natural_language_schedule: None,
            delivery_mode: RoutineDeliveryMode::SameChannel,
            approval_mode: RoutineApprovalMode::None,
            tags: vec!["workflow".to_owned(), "reminder".to_owned()],
        },
        RoutineTemplateDefinition {
            template_id: "change-check".to_owned(),
            title: "Change check".to_owned(),
            description: "Periodic check for recent repository or environment changes that deserve operator attention."
                .to_owned(),
            trigger_kind: RoutineTriggerKind::Schedule,
            default_name: "Change check".to_owned(),
            prompt: "Inspect recent changes, call out risky diffs or regressions, and highlight only material updates that need action."
                .to_owned(),
            natural_language_schedule: Some("every 2h".to_owned()),
            delivery_mode: RoutineDeliveryMode::LogsOnly,
            approval_mode: RoutineApprovalMode::BeforeFirstRun,
            tags: vec!["changes".to_owned(), "review".to_owned()],
        },
        RoutineTemplateDefinition {
            template_id: "document-ingest".to_owned(),
            title: "Document ingest".to_owned(),
            description: "Process new document payloads coming from a webhook or manual fire and turn them into structured summaries."
                .to_owned(),
            trigger_kind: RoutineTriggerKind::Webhook,
            default_name: "Document ingest".to_owned(),
            prompt: "Inspect the incoming document payload, produce a summary, and extract durable facts that are safe to index."
                .to_owned(),
            natural_language_schedule: None,
            delivery_mode: RoutineDeliveryMode::LocalOnly,
            approval_mode: RoutineApprovalMode::BeforeFirstRun,
            tags: vec!["documents".to_owned(), "ingest".to_owned()],
        },
    ]
}

/// Parses an English schedule phrase (for example `in 30 minutes`, `every 2h`, `every Monday
/// at 09:00`, `daily at 9`, or an RFC3339 timestamp) into a normalized schedule preview.
///
/// # Errors
///
/// Returns [`RoutineRegistryError::InvalidField`] for unsupported phrases, intervals below the
/// minimum, bounded `for ...` suffixes, and out-of-range times; the message lists supported
/// shapes.
pub fn natural_language_schedule_preview(
    phrase: &str,
    timezone_mode: CronTimezoneMode,
    now_unix_ms: i64,
) -> Result<RoutineSchedulePreview, RoutineRegistryError> {
    let normalized_phrase = phrase.trim();
    if normalized_phrase.is_empty() {
        return Err(RoutineRegistryError::InvalidField {
            field: "phrase",
            message: "phrase cannot be empty".to_owned(),
        });
    }

    // Parser precedence matters: relative ("in ...") before interval ("every ..."), and the
    // weekly-day form before the generic weekday form, so the most specific match wins.
    if let Some(parsed) = parse_relative_phrase(normalized_phrase, now_unix_ms)? {
        return preview_from_schedule(normalized_phrase, timezone_mode, now_unix_ms, parsed);
    }
    if let Some(parsed) = parse_interval_phrase(normalized_phrase)? {
        return preview_from_schedule(normalized_phrase, timezone_mode, now_unix_ms, parsed);
    }
    if let Some(parsed) = parse_weekly_day_phrase(normalized_phrase)? {
        return preview_from_schedule(normalized_phrase, timezone_mode, now_unix_ms, parsed);
    }
    if let Some(parsed) = parse_weekday_phrase(normalized_phrase)? {
        return preview_from_schedule(normalized_phrase, timezone_mode, now_unix_ms, parsed);
    }
    if let Some(parsed) = parse_daily_phrase(normalized_phrase)? {
        return preview_from_schedule(normalized_phrase, timezone_mode, now_unix_ms, parsed);
    }
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(normalized_phrase) {
        let timestamp = timestamp.with_timezone(&Utc);
        return preview_from_schedule(
            normalized_phrase,
            timezone_mode,
            now_unix_ms,
            ParsedNaturalLanguageSchedule {
                normalized_text: timestamp.to_rfc3339(),
                explanation: format!(
                    "Interpreted as one explicit timestamp in {} mode.",
                    timezone_mode.as_str()
                ),
                schedule: cron_v1::Schedule {
                    r#type: cron_v1::ScheduleType::At as i32,
                    spec: Some(cron_v1::schedule::Spec::At(cron_v1::AtSchedule {
                        timestamp_rfc3339: timestamp.to_rfc3339(),
                    })),
                },
            },
        );
    }

    Err(RoutineRegistryError::InvalidField {
        field: "phrase",
        message: "supported phrases include 'in 30 minutes', 'every 40 seconds', 'every 2h', 'every Monday at 09:00', 'every weekday at 9', 'daily at 9', or an RFC3339 timestamp".to_owned(),
    })
}

fn build_schedule_trigger_payload(job: &CronJobRecord) -> Result<String, RoutineRegistryError> {
    serde_json::to_string(&json!({
        "schedule_type": job.schedule_type.as_str(),
        "workdir": job.workdir.as_deref(),
        "schedule_payload": serde_json::from_str::<Value>(job.schedule_payload_json.as_str()).unwrap_or_else(|_| json!({ "raw": job.schedule_payload_json })),
    }))
    .map_err(Into::into)
}

fn normalize_routine_metadata_upsert(
    request: RoutineMetadataUpsert,
    now: i64,
) -> Result<RoutineMetadataRecord, RoutineRegistryError> {
    Ok(RoutineMetadataRecord {
        routine_id: normalize_identifier(request.routine_id.as_str(), "routine_id")?,
        trigger_kind: request.trigger_kind,
        trigger_payload_json: normalize_payload_json(
            request.trigger_payload_json,
            "trigger_payload_json",
        )?,
        execution: normalize_execution(request.execution)?,
        delivery: normalize_delivery(request.delivery)?,
        quiet_hours: normalize_quiet_hours(request.quiet_hours)?,
        cooldown_ms: request.cooldown_ms,
        approval_policy: request.approval_policy,
        template_id: request.template_id.and_then(trim_to_option),
        created_at_unix_ms: now,
        updated_at_unix_ms: now,
    })
}

fn normalize_routine_run_metadata_upsert(
    request: RoutineRunMetadataUpsert,
    now: i64,
) -> Result<RoutineRunMetadataRecord, RoutineRegistryError> {
    Ok(RoutineRunMetadataRecord {
        run_id: normalize_identifier(request.run_id.as_str(), "run_id")?,
        routine_id: normalize_identifier(request.routine_id.as_str(), "routine_id")?,
        trigger_kind: request.trigger_kind,
        trigger_reason: request.trigger_reason.and_then(trim_to_option),
        trigger_payload_json: normalize_payload_json(
            request.trigger_payload_json,
            "trigger_payload_json",
        )?,
        trigger_dedupe_key: request
            .trigger_dedupe_key
            .map(|value| normalize_freeform_identifier(value.as_str(), "trigger_dedupe_key"))
            .transpose()?,
        execution: normalize_execution(request.execution)?,
        delivery: normalize_delivery(request.delivery)?,
        dispatch_mode: request.dispatch_mode,
        source_run_id: request
            .source_run_id
            .map(|value| normalize_identifier(value.as_str(), "source_run_id"))
            .transpose()?,
        outcome_override: request.outcome_override,
        outcome_message: request.outcome_message.and_then(trim_to_option),
        output_delivered: request.output_delivered,
        skip_reason: request.skip_reason.and_then(trim_to_option),
        delivery_reason: request.delivery_reason.and_then(trim_to_option),
        approval_note: request.approval_note.and_then(trim_to_option),
        safety_note: request.safety_note.and_then(trim_to_option),
        created_at_unix_ms: now,
        updated_at_unix_ms: now,
    })
}

fn normalize_execution(
    execution: RoutineExecutionConfig,
) -> Result<RoutineExecutionConfig, RoutineRegistryError> {
    Ok(RoutineExecutionConfig {
        run_mode: execution.run_mode,
        procedure_profile_id: execution
            .procedure_profile_id
            .map(|value| normalize_identifier(value.as_str(), "execution.procedure_profile_id"))
            .transpose()?,
        skill_profile_id: execution
            .skill_profile_id
            .map(|value| normalize_identifier(value.as_str(), "execution.skill_profile_id"))
            .transpose()?,
        provider_profile_id: execution
            .provider_profile_id
            .map(|value| normalize_identifier(value.as_str(), "execution.provider_profile_id"))
            .transpose()?,
        execution_posture: execution.execution_posture,
    })
}

fn normalize_delivery(
    delivery: RoutineDeliveryConfig,
) -> Result<RoutineDeliveryConfig, RoutineRegistryError> {
    let channel = delivery.channel.and_then(trim_to_option);
    let failure_channel = delivery.failure_channel.and_then(trim_to_option);
    if matches!(delivery.mode, RoutineDeliveryMode::SpecificChannel) && channel.is_none() {
        return Err(RoutineRegistryError::InvalidField {
            field: "delivery.channel",
            message: "delivery.channel is required for delivery.mode=specific_channel".to_owned(),
        });
    }
    if matches!(delivery.failure_mode, Some(RoutineDeliveryMode::SpecificChannel))
        && failure_channel.as_ref().or(channel.as_ref()).is_none()
    {
        return Err(RoutineRegistryError::InvalidField {
            field: "delivery.failure_channel",
            message: "delivery.failure_channel or delivery.channel is required for failure_mode=specific_channel".to_owned(),
        });
    }
    Ok(RoutineDeliveryConfig {
        mode: delivery.mode,
        channel,
        failure_mode: delivery.failure_mode,
        failure_channel,
        silent_policy: delivery.silent_policy,
    })
}

fn normalize_quiet_hours(
    quiet_hours: Option<RoutineQuietHours>,
) -> Result<Option<RoutineQuietHours>, RoutineRegistryError> {
    let Some(quiet_hours) = quiet_hours else {
        return Ok(None);
    };
    if quiet_hours.start_minute_of_day >= 1_440 || quiet_hours.end_minute_of_day >= 1_440 {
        return Err(RoutineRegistryError::InvalidField {
            field: "quiet_hours",
            message: "quiet hours minute-of-day values must be between 0 and 1439".to_owned(),
        });
    }
    Ok(Some(RoutineQuietHours {
        start_minute_of_day: quiet_hours.start_minute_of_day,
        end_minute_of_day: quiet_hours.end_minute_of_day,
        timezone: quiet_hours.timezone.and_then(trim_to_option),
    }))
}

// The approval gate is inferred from the free-form operator note. Denial keywords are checked
// before approval keywords so an ambiguous note ("approved then denied") fails closed, and a
// note matching neither set stays Pending.
fn routine_approval_gate_state(
    approval_policy: &RoutineApprovalPolicy,
    metadata: Option<&RoutineRunMetadataRecord>,
) -> RoutineApprovalGateState {
    if approval_policy.mode == RoutineApprovalMode::None {
        return RoutineApprovalGateState::NotRequired;
    }
    let Some(note) = metadata.and_then(|entry| entry.approval_note.as_deref()) else {
        return RoutineApprovalGateState::Pending;
    };
    let normalized = note.trim().to_ascii_lowercase();
    if normalized.contains("denied") || normalized.contains("rejected") {
        RoutineApprovalGateState::Denied
    } else if normalized.contains("approved") || normalized.contains("allowed") {
        RoutineApprovalGateState::Approved
    } else {
        RoutineApprovalGateState::Pending
    }
}

// A note that already records a decision (approved/denied) needs no review; otherwise pending
// or review-style wording flags the run for an operator.
fn approval_note_requires_operator_review(note: &str) -> bool {
    let normalized = note.trim().to_ascii_lowercase();
    if normalized.contains("approved")
        || normalized.contains("allowed")
        || normalized.contains("denied")
        || normalized.contains("rejected")
    {
        return false;
    }
    normalized.contains("pending")
        || normalized.contains("required")
        || normalized.contains("review")
        || normalized.contains("approval")
}

/// Resolves the delivery obligation for one run outcome.
///
/// Failure-path outcomes use the configured failure target. Pending approvals and unannounced
/// failures escalate to operator review; only failures that need no review are retryable, and
/// reviewed failures are dead-lettered instead of retried.
#[must_use]
pub fn routine_delivery_contract(
    delivery: &RoutineDeliveryConfig,
    outcome_kind: RoutineRunOutcomeKind,
    approval_required: bool,
) -> RoutineDeliveryContract {
    let failure_path =
        matches!(outcome_kind, RoutineRunOutcomeKind::Failed | RoutineRunOutcomeKind::Denied);
    let target = effective_delivery_target(delivery, failure_path);
    let announced = delivery_announced_for_outcome(delivery, outcome_kind);
    let requires_operator_review = approval_required || (failure_path && !announced);
    let kind = if requires_operator_review {
        RoutineDeliveryContractKind::OperatorReview
    } else if !announced {
        RoutineDeliveryContractKind::Silent
    } else if matches!(target.mode, RoutineDeliveryMode::LocalOnly | RoutineDeliveryMode::LogsOnly)
    {
        RoutineDeliveryContractKind::ArtifactOnly
    } else {
        RoutineDeliveryContractKind::Channel
    };
    let retryable = failure_path && !requires_operator_review;
    let dead_letter = failure_path && requires_operator_review;
    let reason = if approval_required {
        "operator approval must resolve before routine output delivery".to_owned()
    } else if dead_letter {
        "terminal failure is retained for operator review because no channel announcement is allowed"
            .to_owned()
    } else {
        delivery_reason_for_outcome(delivery, outcome_kind)
    };

    RoutineDeliveryContract {
        kind,
        mode: target.mode,
        channel: target.channel,
        announced,
        requires_operator_review,
        retryable,
        dead_letter,
        reason,
    }
}

/// Rejects empty prompts and, for fresh-session routines, prompts that lean on conversation
/// context that will not exist when the routine runs ("as above", "resume where you left off").
///
/// # Errors
///
/// Returns [`RoutineRegistryError::InvalidField`] naming the fragile marker that was found.
pub fn validate_routine_prompt_self_contained(
    prompt: &str,
    execution: &RoutineExecutionConfig,
) -> Result<(), RoutineRegistryError> {
    let trimmed = prompt.trim();
    if trimmed.is_empty() {
        return Err(RoutineRegistryError::InvalidField {
            field: "prompt",
            message: "prompt cannot be empty".to_owned(),
        });
    }
    if execution.run_mode != RoutineRunMode::FreshSession {
        return Ok(());
    }

    const FRAGILE_PROMPT_MARKERS: &[&str] = &[
        "as above",
        "same as before",
        "previous context",
        "prior context",
        "resume where you left off",
        "pick up where you left off",
        "continue from earlier",
        "the earlier thread",
        "the conversation above",
    ];
    let normalized = trimmed.to_ascii_lowercase();
    if let Some(marker) =
        FRAGILE_PROMPT_MARKERS.iter().copied().find(|marker| normalized.contains(marker))
    {
        return Err(RoutineRegistryError::InvalidField {
            field: "prompt",
            message: format!(
                "fresh-session routines must stay self-contained; remove fragile context reference '{marker}'"
            ),
        });
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct EffectiveDeliveryTarget {
    mode: RoutineDeliveryMode,
    channel: Option<String>,
}

fn effective_delivery_target(
    delivery: &RoutineDeliveryConfig,
    failure_path: bool,
) -> EffectiveDeliveryTarget {
    if failure_path {
        EffectiveDeliveryTarget {
            mode: delivery.failure_mode.unwrap_or(delivery.mode),
            channel: delivery.failure_channel.clone().or_else(|| delivery.channel.clone()),
        }
    } else {
        EffectiveDeliveryTarget { mode: delivery.mode, channel: delivery.channel.clone() }
    }
}

fn delivery_announced_for_outcome(
    delivery: &RoutineDeliveryConfig,
    outcome_kind: RoutineRunOutcomeKind,
) -> bool {
    if outcome_kind == RoutineRunOutcomeKind::Pending {
        return false;
    }
    if delivery.silent_policy == RoutineSilentPolicy::AuditOnly {
        return false;
    }
    let failure_path =
        matches!(outcome_kind, RoutineRunOutcomeKind::Failed | RoutineRunOutcomeKind::Denied);
    if !failure_path && delivery.silent_policy == RoutineSilentPolicy::FailureOnly {
        return false;
    }
    let effective = effective_delivery_target(delivery, failure_path);
    !matches!(effective.mode, RoutineDeliveryMode::LocalOnly | RoutineDeliveryMode::LogsOnly)
}

fn delivery_reason_for_outcome(
    delivery: &RoutineDeliveryConfig,
    outcome_kind: RoutineRunOutcomeKind,
) -> String {
    if outcome_kind == RoutineRunOutcomeKind::Pending {
        return "run is still active; output delivery waits until the routine reaches a terminal status"
            .to_owned();
    }
    if delivery.silent_policy == RoutineSilentPolicy::AuditOnly {
        return "delivery suppressed by silent_policy=audit_only; audit trail remains available"
            .to_owned();
    }
    let failure_path =
        matches!(outcome_kind, RoutineRunOutcomeKind::Failed | RoutineRunOutcomeKind::Denied);
    if !failure_path && delivery.silent_policy == RoutineSilentPolicy::FailureOnly {
        return "successful runs stay silent; failures still use the configured failure target"
            .to_owned();
    }
    let effective = effective_delivery_target(delivery, failure_path);
    match effective.mode {
        RoutineDeliveryMode::LocalOnly => {
            "delivery stays local to the automation session and is not announced externally"
                .to_owned()
        }
        RoutineDeliveryMode::LogsOnly => {
            "delivery is restricted to logs and diagnostics surfaces".to_owned()
        }
        RoutineDeliveryMode::SameChannel => {
            "delivery is eligible for the routine origin channel".to_owned()
        }
        RoutineDeliveryMode::SpecificChannel => format!(
            "delivery is eligible for explicit channel {}",
            effective.channel.unwrap_or_else(|| "unknown".to_owned())
        ),
    }
}

fn provider_routing_preview(execution: &RoutineExecutionConfig) -> Value {
    if let Some(profile_id) = execution.provider_profile_id.as_ref() {
        json!({
            "mode": "pinned",
            "profile_id": profile_id,
        })
    } else {
        json!({
            "mode": "auto",
        })
    }
}

/// Renders the success- and failure-path delivery targets of a config as preview JSON.
pub fn routine_delivery_preview(delivery: &RoutineDeliveryConfig) -> Value {
    let success_target = effective_delivery_target(delivery, false);
    let failure_target = effective_delivery_target(delivery, true);
    json!({
        "silent_policy": delivery.silent_policy.as_str(),
        "success": {
            "mode": success_target.mode.as_str(),
            "channel": success_target.channel,
            "announced": delivery_announced_for_outcome(delivery, RoutineRunOutcomeKind::SuccessWithOutput),
            "reason": delivery_reason_for_outcome(delivery, RoutineRunOutcomeKind::SuccessWithOutput),
        },
        "failure": {
            "mode": failure_target.mode.as_str(),
            "channel": failure_target.channel,
            "announced": delivery_announced_for_outcome(delivery, RoutineRunOutcomeKind::Failed),
            "reason": delivery_reason_for_outcome(delivery, RoutineRunOutcomeKind::Failed),
        },
    })
}

fn normalize_payload_json(
    payload_json: String,
    field: &'static str,
) -> Result<String, RoutineRegistryError> {
    let parsed = serde_json::from_str::<Value>(payload_json.as_str()).map_err(|error| {
        RoutineRegistryError::InvalidField {
            field,
            message: format!("payload must be valid JSON: {error}"),
        }
    })?;
    serde_json::to_string(&parsed).map_err(Into::into)
}

fn normalize_identifier(raw: &str, field: &'static str) -> Result<String, RoutineRegistryError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(RoutineRegistryError::InvalidField {
            field,
            message: "value cannot be empty".to_owned(),
        });
    }
    if trimmed.len() > 128 {
        return Err(RoutineRegistryError::InvalidField {
            field,
            message: "value must be 128 bytes or fewer".to_owned(),
        });
    }
    Ok(trimmed.to_owned())
}

fn normalize_freeform_identifier(
    raw: &str,
    field: &'static str,
) -> Result<String, RoutineRegistryError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(RoutineRegistryError::InvalidField {
            field,
            message: "value cannot be empty".to_owned(),
        });
    }
    if trimmed.len() > 256 {
        return Err(RoutineRegistryError::InvalidField {
            field,
            message: "value must be 256 bytes or fewer".to_owned(),
        });
    }
    Ok(trimmed.to_owned())
}

fn trim_to_option(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn unix_ms_now() -> Result<i64, RoutineRegistryError> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    // Intentional `as` narrowing: epoch milliseconds stay far below i64::MAX for any real clock.
    Ok(now.as_millis() as i64)
}

fn open_registry_file(path: &RegistryPath) -> Result<fs::File, RoutineRegistryError> {
    let parent = path.as_path().parent().ok_or_else(|| RoutineRegistryError::WriteRegistry {
        path: path.to_path_buf(),
        source: std::io::Error::other("routine registry path has no parent"),
    })?;
    fs::create_dir_all(parent).map_err(|source| RoutineRegistryError::WriteRegistry {
        path: parent.to_path_buf(),
        source,
    })?;
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path.as_path())
        .map_err(|source| RoutineRegistryError::WriteRegistry { path: path.to_path_buf(), source })
}

fn load_registry_document(
    path: &RegistryPath,
    file: &mut fs::File,
) -> Result<RoutineRegistryDocument, RoutineRegistryError> {
    load_json_document(path, file)
}

fn load_run_metadata_document(
    path: &RegistryPath,
    file: &mut fs::File,
) -> Result<RoutineRunMetadataDocument, RoutineRegistryError> {
    load_json_document(path, file)
}

fn load_json_document<T>(
    path: &RegistryPath,
    file: &mut fs::File,
) -> Result<T, RoutineRegistryError>
where
    T: for<'de> Deserialize<'de> + Default + HasSchemaVersion,
{
    file.seek(SeekFrom::Start(0)).map_err(|source| RoutineRegistryError::ReadRegistry {
        path: path.to_path_buf(),
        source,
    })?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).map_err(|source| RoutineRegistryError::ReadRegistry {
        path: path.to_path_buf(),
        source,
    })?;
    if buffer.is_empty() {
        return Ok(T::default());
    }
    let parsed = serde_json::from_slice::<T>(&buffer).map_err(|source| {
        RoutineRegistryError::ParseRegistry { path: path.to_path_buf(), source }
    })?;
    if parsed.schema_version() != ROUTINE_REGISTRY_VERSION {
        return Err(RoutineRegistryError::UnsupportedVersion(parsed.schema_version()));
    }
    Ok(parsed)
}

fn write_registry_document<T>(
    path: &RegistryPath,
    file: &Mutex<fs::File>,
    document: &T,
) -> Result<(), RoutineRegistryError>
where
    T: Serialize,
{
    let payload = serde_json::to_string_pretty(document)?;
    let _file = file.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
    write_content_with_backups(path.as_path(), payload.as_str(), 0).map_err(|source| {
        RoutineRegistryError::WriteRegistry {
            path: path.to_path_buf(),
            source: std::io::Error::other(source.to_string()),
        }
    })
}

trait HasSchemaVersion {
    fn schema_version(&self) -> u32;
}

impl HasSchemaVersion for RoutineRegistryDocument {
    fn schema_version(&self) -> u32 {
        self.schema_version
    }
}

impl HasSchemaVersion for RoutineRunMetadataDocument {
    fn schema_version(&self) -> u32 {
        self.schema_version
    }
}

#[derive(Debug, Clone)]
struct ParsedNaturalLanguageSchedule {
    normalized_text: String,
    explanation: String,
    schedule: cron_v1::Schedule,
}

fn preview_from_schedule(
    phrase: &str,
    timezone_mode: CronTimezoneMode,
    now_unix_ms: i64,
    parsed: ParsedNaturalLanguageSchedule,
) -> Result<RoutineSchedulePreview, RoutineRegistryError> {
    let normalized = cron::normalize_schedule(Some(parsed.schedule), now_unix_ms, timezone_mode)
        .map_err(|error| RoutineRegistryError::InvalidField {
            field: "phrase",
            message: error.message().to_owned(),
        })?;
    let schedule_payload = serde_json::from_str::<Value>(normalized.schedule_payload_json.as_str())
        .map_err(RoutineRegistryError::SerializeRegistry)?;
    Ok(RoutineSchedulePreview {
        phrase: phrase.trim().to_owned(),
        normalized_text: parsed.normalized_text,
        explanation: parsed.explanation,
        schedule_type: normalized.schedule_type.as_str().to_owned(),
        schedule_payload_json: normalized.schedule_payload_json,
        schedule_payload,
        next_run_at_unix_ms: normalized.next_run_at_unix_ms,
        timezone: timezone_mode.as_str().to_owned(),
    })
}

fn parse_relative_phrase(
    phrase: &str,
    now_unix_ms: i64,
) -> Result<Option<ParsedNaturalLanguageSchedule>, RoutineRegistryError> {
    let normalized = normalize_phrase(phrase);
    let tokens = normalized.split_whitespace().collect::<Vec<_>>();
    let (quantity, unit) = match tokens.as_slice() {
        ["in", quantity, unit] => (*quantity, *unit),
        _ => return Ok(None),
    };
    let duration_ms = parse_duration_to_ms(quantity, unit, "phrase")?;
    let now = Utc.timestamp_millis_opt(now_unix_ms).single().ok_or_else(|| {
        RoutineRegistryError::InvalidField {
            field: "phrase",
            message: "current timestamp could not be resolved".to_owned(),
        }
    })?;
    let target = now
        .checked_add_signed(ChronoDuration::milliseconds(duration_ms as i64))
        .ok_or_else(|| RoutineRegistryError::InvalidField {
            field: "phrase",
            message: "relative schedule overflows supported timestamp range".to_owned(),
        })?;
    Ok(Some(ParsedNaturalLanguageSchedule {
        normalized_text: target.to_rfc3339(),
        explanation: format!(
            "Interpreted as a one-time run {} from now.",
            humanize_duration(duration_ms)
        ),
        schedule: cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::At as i32,
            spec: Some(cron_v1::schedule::Spec::At(cron_v1::AtSchedule {
                timestamp_rfc3339: target.to_rfc3339(),
            })),
        },
    }))
}

fn parse_interval_phrase(
    phrase: &str,
) -> Result<Option<ParsedNaturalLanguageSchedule>, RoutineRegistryError> {
    let normalized = normalize_phrase(phrase);
    let (interval_phrase, bounded_suffix) = split_bounded_duration_suffix(normalized.as_str());
    let tokens = interval_phrase.split_whitespace().collect::<Vec<_>>();
    let (quantity, unit) = match tokens.as_slice() {
        ["every", unit @ ("minute" | "minutes")] => ("1", *unit),
        ["every", compact] => split_compact_duration(compact).unwrap_or(("", "")),
        ["every", quantity, unit] => (*quantity, *unit),
        _ => return Ok(None),
    };
    if quantity.is_empty() || unit.is_empty() {
        return Ok(None);
    }
    let interval_ms = parse_duration_to_ms(quantity, unit, "phrase")?;
    if let Some(suffix) = bounded_suffix {
        return Err(RoutineRegistryError::InvalidField {
            field: "phrase",
            message: format!(
                "bounded recurring duration '{suffix}' is not supported; parsed the interval part as every {}. Use 'every 30 seconds' for an unbounded repeating schedule, then disable or delete the job after the desired duration.",
                humanize_duration(interval_ms)
            ),
        });
    }
    if interval_ms < MIN_EVERY_INTERVAL_MS {
        return Err(RoutineRegistryError::InvalidField {
            field: "phrase",
            message: format!(
                "repeating schedules must be at least {} second(s) apart",
                MIN_EVERY_INTERVAL_MS / 1_000
            ),
        });
    }
    Ok(Some(ParsedNaturalLanguageSchedule {
        normalized_text: format!("every {}", humanize_duration(interval_ms)),
        explanation: format!(
            "Interpreted as a repeating interval of {}.",
            humanize_duration(interval_ms)
        ),
        schedule: cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Every as i32,
            spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule { interval_ms })),
        },
    }))
}

// Splits "every 30 seconds for 2 minutes" into the interval part and the bounded suffix. The
// suffix keeps its leading "for" on purpose so the rejection message can echo the operator's
// own phrasing (pinned by tests).
fn split_bounded_duration_suffix(normalized: &str) -> (&str, Option<&str>) {
    for marker in [" for "] {
        if let Some(index) = normalized.find(marker) {
            let suffix_start = index.saturating_add(1);
            return (normalized[..index].trim_end(), Some(normalized[suffix_start..].trim()));
        }
    }
    (normalized, None)
}

fn parse_weekday_phrase(
    phrase: &str,
) -> Result<Option<ParsedNaturalLanguageSchedule>, RoutineRegistryError> {
    let normalized = normalize_phrase(phrase);
    let prefix = if normalized.starts_with("every weekday at ") {
        "every weekday at "
    } else {
        return Ok(None);
    };
    let time = parse_time_components(normalized.trim_start_matches(prefix), "phrase")?;
    let expression = format!("{} {} * * 1-5", time.minute, time.hour);
    Ok(Some(ParsedNaturalLanguageSchedule {
        normalized_text: format!("weekdays at {:02}:{:02}", time.hour, time.minute),
        explanation: "Interpreted as every weekday at a fixed local/UTC wall clock time."
            .to_owned(),
        schedule: cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Cron as i32,
            spec: Some(cron_v1::schedule::Spec::Cron(cron_v1::CronSchedule { expression })),
        },
    }))
}

fn parse_weekly_day_phrase(
    phrase: &str,
) -> Result<Option<ParsedNaturalLanguageSchedule>, RoutineRegistryError> {
    let normalized = normalize_phrase(phrase);
    let tokens = normalized.split_whitespace().collect::<Vec<_>>();
    let (weekday_token, time_raw) = match tokens.as_slice() {
        ["every", weekday, "at", time @ ..] if !time.is_empty() => (*weekday, time.join(" ")),
        _ => return Ok(None),
    };
    let Some(weekday) = cron_weekday_number(weekday_token) else {
        return Ok(None);
    };
    let time = parse_time_components(time_raw.as_str(), "phrase")?;
    let expression = format!("{} {} * * {}", time.minute, time.hour, weekday);
    Ok(Some(ParsedNaturalLanguageSchedule {
        normalized_text: format!(
            "weekly on {} at {:02}:{:02}",
            cron_weekday_name(weekday),
            time.hour,
            time.minute
        ),
        explanation: format!(
            "Interpreted as every {} at a fixed local/UTC wall clock time.",
            cron_weekday_name(weekday)
        ),
        schedule: cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Cron as i32,
            spec: Some(cron_v1::schedule::Spec::Cron(cron_v1::CronSchedule { expression })),
        },
    }))
}

fn parse_daily_phrase(
    phrase: &str,
) -> Result<Option<ParsedNaturalLanguageSchedule>, RoutineRegistryError> {
    let normalized = normalize_phrase(phrase);
    let prefix = if normalized.starts_with("daily at ") {
        "daily at "
    } else {
        return Ok(None);
    };
    let time = parse_time_components(normalized.trim_start_matches(prefix), "phrase")?;
    let expression = format!("{} {} * * *", time.minute, time.hour);
    Ok(Some(ParsedNaturalLanguageSchedule {
        normalized_text: format!("daily at {:02}:{:02}", time.hour, time.minute),
        explanation: "Interpreted as a daily wall clock schedule.".to_owned(),
        schedule: cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Cron as i32,
            spec: Some(cron_v1::schedule::Spec::Cron(cron_v1::CronSchedule { expression })),
        },
    }))
}

fn normalize_phrase(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase()
}

fn split_compact_duration(value: &str) -> Option<(&str, &str)> {
    let digits = value
        .char_indices()
        .take_while(|(_, character)| character.is_ascii_digit())
        .last()
        .map(|(index, character)| index + character.len_utf8())
        .unwrap_or(0);
    if digits == 0 || digits >= value.len() {
        return None;
    }
    Some(value.split_at(digits))
}

fn parse_duration_to_ms(
    quantity: &str,
    unit: &str,
    field: &'static str,
) -> Result<u64, RoutineRegistryError> {
    let quantity = quantity.parse::<u64>().map_err(|_| RoutineRegistryError::InvalidField {
        field,
        message: format!("duration quantity '{quantity}' must be numeric"),
    })?;
    if quantity == 0 {
        return Err(RoutineRegistryError::InvalidField {
            field,
            message: "duration quantity must be greater than zero".to_owned(),
        });
    }
    let normalized = unit.trim().to_lowercase();
    let multiplier = match normalized.as_str() {
        "s" | "sec" | "secs" | "second" | "seconds" => 1_000,
        "m" | "min" | "mins" | "minute" | "minutes" => 60_000,
        "h" | "hr" | "hrs" | "hour" | "hours" => 60 * 60_000,
        "d" | "day" | "days" => 24 * 60 * 60_000,
        _ => {
            return Err(RoutineRegistryError::InvalidField {
                field,
                message: format!("unsupported duration unit '{unit}'"),
            })
        }
    };
    quantity.checked_mul(multiplier).ok_or_else(|| RoutineRegistryError::InvalidField {
        field,
        message: "duration is too large".to_owned(),
    })
}

#[derive(Debug, Clone, Copy)]
struct ParsedTimeOfDay {
    hour: u8,
    minute: u8,
}

fn parse_time_components(
    raw: &str,
    field: &'static str,
) -> Result<ParsedTimeOfDay, RoutineRegistryError> {
    let trimmed = raw.trim();
    let (hour, minute) = if let Some((hour, minute)) = trimmed.split_once(':') {
        (hour, minute)
    } else {
        (trimmed, "0")
    };
    let hour = hour.parse::<u8>().map_err(|_| RoutineRegistryError::InvalidField {
        field,
        message: format!("time '{trimmed}' must use hour or hour:minute format"),
    })?;
    let minute = minute.parse::<u8>().map_err(|_| RoutineRegistryError::InvalidField {
        field,
        message: format!("time '{trimmed}' must use hour or hour:minute format"),
    })?;
    if hour > 23 || minute > 59 {
        return Err(RoutineRegistryError::InvalidField {
            field,
            message: format!("time '{trimmed}' must stay within 00:00-23:59"),
        });
    }
    Ok(ParsedTimeOfDay { hour, minute })
}

fn cron_weekday_number(value: &str) -> Option<u8> {
    match value {
        "sunday" | "sun" => Some(0),
        "monday" | "mon" => Some(1),
        "tuesday" | "tue" => Some(2),
        "wednesday" | "wed" => Some(3),
        "thursday" | "thu" => Some(4),
        "friday" | "fri" => Some(5),
        "saturday" | "sat" => Some(6),
        _ => None,
    }
}

fn cron_weekday_name(value: u8) -> &'static str {
    match value {
        0 => "Sunday",
        1 => "Monday",
        2 => "Tuesday",
        3 => "Wednesday",
        4 => "Thursday",
        5 => "Friday",
        6 => "Saturday",
        _ => "weekday",
    }
}

fn humanize_duration(duration_ms: u64) -> String {
    if duration_ms.is_multiple_of(60 * 60 * 1_000) {
        format!("{} hour(s)", duration_ms / (60 * 60 * 1_000))
    } else if duration_ms.is_multiple_of(60 * 1_000) {
        format!("{} minute(s)", duration_ms / (60 * 1_000))
    } else if duration_ms.is_multiple_of(1_000) {
        format!("{} second(s)", duration_ms / 1_000)
    } else {
        format!("{} ms", duration_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_routine_export_bundle, default_outcome_from_cron_status, join_run_metadata,
        natural_language_schedule_preview, normalize_file_watch_trigger_payload,
        resolve_routines_root, routine_approval_policy_with_auto_enable_guard,
        routine_delivery_contract, routine_delivery_preview, routine_retention_dry_run,
        routine_run_lifecycle_snapshot, routine_runtime_backfill_plan,
        schedule_requires_auto_enable_guard, shadow_manual_schedule_payload_json,
        validate_routine_export_bundle, validate_routine_prompt_self_contained,
        RoutineApprovalGateState, RoutineApprovalMode, RoutineApprovalPolicy,
        RoutineDeliveryConfig, RoutineDeliveryContractKind, RoutineDeliveryMode,
        RoutineExecutionConfig, RoutineMetadataRecord, RoutineRegistry, RoutineRetentionPolicy,
        RoutineRunLeaseState, RoutineRunMetadataRecord, RoutineRunMetadataUpsert, RoutineRunMode,
        RoutineRunOutcomeKind, RoutineSilentPolicy, RoutineTriggerKind,
        MIN_AUTO_ENABLE_EVERY_INTERVAL_MS, ROUTINE_EXPORT_SCHEMA_ID, ROUTINE_RUN_LEASE_TTL_MS,
    };
    use crate::{
        cron::CronTimezoneMode,
        journal::{
            CronConcurrencyPolicy, CronJobRecord, CronMisfirePolicy, CronRetryPolicy,
            CronRunRecord, CronRunStatus, CronScheduleType,
        },
    };
    use chrono::DateTime;
    use serde_json::{json, Value};
    use std::{
        collections::BTreeSet,
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_state_root() -> std::path::PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("palyra-routines-test-{stamp}"))
    }

    fn sample_cron_run(status: CronRunStatus, updated_at_unix_ms: i64) -> CronRunRecord {
        CronRunRecord {
            run_id: "run-1".to_owned(),
            job_id: "routine-1".to_owned(),
            attempt: 1,
            session_id: Some("session-1".to_owned()),
            orchestrator_run_id: Some("orchestrator-1".to_owned()),
            started_at_unix_ms: 1_000,
            finished_at_unix_ms: if status.is_active() { None } else { Some(2_000) },
            status,
            error_kind: None,
            error_message_redacted: None,
            model_tokens_in: 0,
            model_tokens_out: 0,
            tool_calls: 0,
            tool_denies: 0,
            created_at_unix_ms: 1_000,
            updated_at_unix_ms,
        }
    }

    fn sample_run_metadata(
        run_id: &str,
        routine_id: &str,
        updated_at_unix_ms: i64,
    ) -> RoutineRunMetadataRecord {
        RoutineRunMetadataRecord {
            run_id: run_id.to_owned(),
            routine_id: routine_id.to_owned(),
            trigger_kind: RoutineTriggerKind::Manual,
            trigger_reason: None,
            trigger_payload_json: json!({ "source": "test" }).to_string(),
            trigger_dedupe_key: None,
            execution: RoutineExecutionConfig::default(),
            delivery: RoutineDeliveryConfig::default(),
            dispatch_mode: super::RoutineDispatchMode::Normal,
            source_run_id: None,
            outcome_override: None,
            outcome_message: None,
            output_delivered: None,
            skip_reason: None,
            delivery_reason: None,
            approval_note: None,
            safety_note: None,
            created_at_unix_ms: updated_at_unix_ms,
            updated_at_unix_ms,
        }
    }

    fn sample_routine_metadata(routine_id: &str) -> RoutineMetadataRecord {
        RoutineMetadataRecord {
            routine_id: routine_id.to_owned(),
            trigger_kind: RoutineTriggerKind::Manual,
            trigger_payload_json: json!({ "kind": "manual" }).to_string(),
            execution: RoutineExecutionConfig::default(),
            delivery: RoutineDeliveryConfig::default(),
            quiet_hours: None,
            cooldown_ms: 0,
            approval_policy: RoutineApprovalPolicy::default(),
            template_id: None,
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        }
    }

    fn sample_cron_job(job_id: &str) -> CronJobRecord {
        CronJobRecord {
            job_id: job_id.to_owned(),
            name: "routine".to_owned(),
            prompt: "test".to_owned(),
            owner_principal: "user:ops".to_owned(),
            channel: "system:routines".to_owned(),
            session_key: None,
            session_label: None,
            workdir: None,
            schedule_type: CronScheduleType::At,
            schedule_payload_json: shadow_manual_schedule_payload_json(),
            enabled: true,
            concurrency_policy: CronConcurrencyPolicy::Forbid,
            retry_policy: CronRetryPolicy { max_attempts: 1, backoff_ms: 1 },
            misfire_policy: CronMisfirePolicy::Skip,
            jitter_ms: 0,
            next_run_at_unix_ms: None,
            last_run_at_unix_ms: None,
            queued_run: false,
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        }
    }

    #[test]
    fn resolve_routines_root_creates_directory() {
        let root = temp_state_root();
        let routines_root =
            resolve_routines_root(Some(root.as_path())).expect("root should resolve");
        assert!(routines_root.exists(), "routines directory should exist");
    }

    #[test]
    fn file_watch_payload_baselines_and_detects_modifications() {
        let root = temp_state_root();
        fs::create_dir_all(root.as_path()).expect("watch root should be created");
        let watched_path = root.join("watch.txt");
        fs::write(watched_path.as_path(), "baseline").expect("watch file should write");
        let config = normalize_file_watch_trigger_payload(Some(&json!({
            "path": watched_path.to_string_lossy(),
            "poll_interval_ms": 30_000,
        })))
        .expect("file_watch config should normalize");

        assert_eq!(config.poll_interval_ms, 30_000);
        assert!(config.last_observed.as_ref().is_some_and(|entry| entry.exists));
        assert!(
            super::evaluate_file_watch_change(config.clone())
                .expect("unchanged watch should evaluate")
                .is_none(),
            "unchanged watched files must not dispatch a routine run"
        );

        fs::write(watched_path.as_path(), "baseline plus update")
            .expect("watch file should update");
        let change = super::evaluate_file_watch_change(config)
            .expect("changed watch should evaluate")
            .expect("changed watch should dispatch");

        assert_eq!(change.event, "modified");
        assert!(change.current.exists);
        assert_ne!(
            change.previous.expect("baseline should exist").signature,
            change.current.signature
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn registry_round_trips_metadata_and_run_metadata() {
        let root = temp_state_root();
        let registry = RoutineRegistry::open(root.as_path()).expect("registry should open");
        let created = registry
            .upsert_routine(super::RoutineMetadataUpsert {
                routine_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                trigger_kind: RoutineTriggerKind::Manual,
                trigger_payload_json: json!({ "kind": "manual" }).to_string(),
                execution: RoutineExecutionConfig::default(),
                delivery: RoutineDeliveryConfig {
                    mode: RoutineDeliveryMode::SpecificChannel,
                    channel: Some("system:routines".to_owned()),
                    failure_mode: None,
                    failure_channel: None,
                    silent_policy: RoutineSilentPolicy::Noisy,
                },
                quiet_hours: None,
                cooldown_ms: 60_000,
                approval_policy: RoutineApprovalPolicy {
                    mode: RoutineApprovalMode::BeforeFirstRun,
                },
                template_id: Some("heartbeat".to_owned()),
            })
            .expect("metadata upsert should succeed");
        assert_eq!(created.trigger_kind, RoutineTriggerKind::Manual);

        let run = registry
            .upsert_run_metadata(RoutineRunMetadataUpsert {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned(),
                routine_id: created.routine_id.clone(),
                trigger_kind: RoutineTriggerKind::Manual,
                trigger_reason: Some("manual fire".to_owned()),
                trigger_payload_json: json!({ "source": "operator" }).to_string(),
                trigger_dedupe_key: Some("manual:1".to_owned()),
                execution: RoutineExecutionConfig::default(),
                delivery: created.delivery.clone(),
                dispatch_mode: super::RoutineDispatchMode::Normal,
                source_run_id: None,
                outcome_override: None,
                outcome_message: None,
                output_delivered: Some(true),
                skip_reason: None,
                delivery_reason: None,
                approval_note: None,
                safety_note: None,
            })
            .expect("run metadata upsert should succeed");
        assert_eq!(run.trigger_kind, RoutineTriggerKind::Manual);
        assert!(
            registry
                .seen_dedupe_key(created.routine_id.as_str(), "manual:1")
                .expect("dedupe lookup should succeed"),
            "dedupe key should be discoverable"
        );
    }

    #[test]
    fn shadow_manual_schedule_payload_uses_future_timestamp() {
        let payload =
            serde_json::from_str::<serde_json::Value>(&shadow_manual_schedule_payload_json())
                .expect("payload should parse");
        assert_eq!(payload, json!({ "timestamp_rfc3339": "2100-01-01T00:00:00Z" }));
        let _ = CronScheduleType::At;
    }

    #[test]
    fn default_outcome_mapping_tracks_cron_status() {
        assert_eq!(
            default_outcome_from_cron_status(CronRunStatus::Succeeded).as_str(),
            "success_with_output"
        );
        assert_eq!(default_outcome_from_cron_status(CronRunStatus::Running).as_str(), "pending");
        assert_eq!(default_outcome_from_cron_status(CronRunStatus::Accepted).as_str(), "pending");
        assert_eq!(default_outcome_from_cron_status(CronRunStatus::Skipped).as_str(), "skipped");
    }

    #[test]
    fn join_run_metadata_marks_active_run_outcome_as_pending() {
        let now = 1_700_000_000_000_i64;
        let run = CronRunRecord {
            run_id: "run-active".to_owned(),
            job_id: "routine-active".to_owned(),
            status: CronRunStatus::Running,
            started_at_unix_ms: now,
            finished_at_unix_ms: None,
            attempt: 1,
            session_id: Some("session-active".to_owned()),
            orchestrator_run_id: None,
            error_kind: None,
            error_message_redacted: None,
            model_tokens_in: 0,
            model_tokens_out: 0,
            tool_calls: 0,
            tool_denies: 0,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };

        let value = join_run_metadata("routine-active", &run, None);

        assert_eq!(value.get("outcome_kind").and_then(Value::as_str), Some("pending"));
        assert_eq!(value.get("output_delivered").and_then(Value::as_bool), Some(false));
        assert_eq!(value.get("outcome_provisional").and_then(Value::as_bool), Some(true));
        assert!(
            value
                .get("delivery_reason")
                .and_then(Value::as_str)
                .is_some_and(|reason| reason.contains("terminal status")),
            "{value}"
        );
    }

    #[test]
    fn join_run_metadata_reconciles_stale_pending_override_for_terminal_run() {
        let run = sample_cron_run(CronRunStatus::Succeeded, 3_000);
        let mut metadata = sample_run_metadata("run-1", "routine-1", 2_000);
        metadata.outcome_override = Some(RoutineRunOutcomeKind::Pending);

        let value = join_run_metadata("routine-1", &run, Some(&metadata));

        assert_eq!(value.get("outcome_kind").and_then(Value::as_str), Some("success_with_output"));
        assert_eq!(value.get("outcome_provisional").and_then(Value::as_bool), Some(false));
        assert!(
            !value
                .get("delivery_reason")
                .and_then(Value::as_str)
                .is_some_and(|reason| reason.contains("still active")),
            "{value}"
        );
    }

    #[test]
    fn natural_language_preview_supports_english_inputs() {
        let now = 1_700_000_000_000_i64;
        let english = natural_language_schedule_preview("every 2h", CronTimezoneMode::Utc, now)
            .expect("english schedule preview should parse");
        assert_eq!(english.schedule_type, "every");
        assert_eq!(english.schedule_payload["interval_ms"], json!(7_200_000_u64));

        let every_minute =
            natural_language_schedule_preview("every minute", CronTimezoneMode::Utc, now)
                .expect("minute interval schedule preview should parse");
        assert_eq!(every_minute.schedule_type, "every");
        assert_eq!(every_minute.schedule_payload["interval_ms"], json!(60_000_u64));

        let every_one_minute =
            natural_language_schedule_preview("every 1 minute", CronTimezoneMode::Utc, now)
                .expect("explicit minute interval schedule preview should parse");
        assert_eq!(every_one_minute.schedule_payload["interval_ms"], json!(60_000_u64));

        let every_seconds =
            natural_language_schedule_preview("every 40 seconds", CronTimezoneMode::Utc, now)
                .expect("second interval schedule preview should parse");
        assert_eq!(every_seconds.schedule_payload["interval_ms"], json!(40_000_u64));

        let saturday =
            DateTime::parse_from_rfc3339("2026-05-23T12:00:00Z").unwrap().timestamp_millis();
        let english_weekly = natural_language_schedule_preview(
            "every Monday at 09:00",
            CronTimezoneMode::Utc,
            saturday,
        )
        .expect("english weekly schedule preview should parse");
        assert_eq!(english_weekly.schedule_type, "cron");
        assert_eq!(english_weekly.schedule_payload["expression"], json!("0 9 * * 1"));
        assert_eq!(english_weekly.schedule_payload["timezone"], json!("utc"));
        assert_eq!(
            english_weekly.next_run_at_unix_ms,
            Some(DateTime::parse_from_rfc3339("2026-05-25T09:00:00Z").unwrap().timestamp_millis())
        );
    }

    #[test]
    fn natural_language_preview_rejects_zero_repeat() {
        let error = natural_language_schedule_preview("every 0m", CronTimezoneMode::Utc, 0)
            .expect_err("zero repeat should be rejected");
        assert!(
            error.to_string().contains("greater than zero"),
            "error should explain minimum repeat interval"
        );
    }

    #[test]
    fn schedule_auto_enable_guard_requires_before_enable_for_fast_every_schedules() {
        let payload = json!({ "interval_ms": MIN_AUTO_ENABLE_EVERY_INTERVAL_MS - 1 }).to_string();

        assert!(schedule_requires_auto_enable_guard(CronScheduleType::Every, payload.as_str()));

        let guarded = routine_approval_policy_with_auto_enable_guard(
            CronScheduleType::Every,
            payload.as_str(),
            RoutineApprovalPolicy::default(),
        );
        assert_eq!(guarded.mode, RoutineApprovalMode::BeforeEnable);
    }

    #[test]
    fn schedule_auto_enable_guard_preserves_one_minute_every_schedules() {
        let payload = json!({ "interval_ms": 60_000_u64 }).to_string();

        assert!(!schedule_requires_auto_enable_guard(CronScheduleType::Every, payload.as_str()));

        let guarded = routine_approval_policy_with_auto_enable_guard(
            CronScheduleType::Every,
            payload.as_str(),
            RoutineApprovalPolicy::default(),
        );
        assert_eq!(guarded.mode, RoutineApprovalMode::None);
    }

    #[test]
    fn schedule_auto_enable_guard_preserves_normal_every_schedules() {
        let payload = json!({ "interval_ms": MIN_AUTO_ENABLE_EVERY_INTERVAL_MS }).to_string();

        assert!(!schedule_requires_auto_enable_guard(CronScheduleType::Every, payload.as_str()));

        let guarded = routine_approval_policy_with_auto_enable_guard(
            CronScheduleType::Every,
            payload.as_str(),
            RoutineApprovalPolicy::default(),
        );
        assert_eq!(guarded.mode, RoutineApprovalMode::None);
    }

    #[test]
    fn schedule_auto_enable_guard_fail_closes_malformed_every_payloads() {
        let guarded = routine_approval_policy_with_auto_enable_guard(
            CronScheduleType::Every,
            "{}",
            RoutineApprovalPolicy { mode: RoutineApprovalMode::BeforeFirstRun },
        );

        assert_eq!(guarded.mode, RoutineApprovalMode::BeforeEnable);
    }

    #[test]
    fn natural_language_preview_rejects_bounded_recurring_suffix_with_specific_hint() {
        let error = natural_language_schedule_preview(
            "every 30 seconds for 2 minutes",
            CronTimezoneMode::Utc,
            0,
        )
        .expect_err("bounded recurring schedules should require an explicit lifecycle");
        let rendered = error.to_string();

        assert!(rendered.contains("bounded recurring duration"), "{rendered}");
        assert!(rendered.contains("for 2 minutes"), "{rendered}");
        assert!(rendered.contains("every 30 second(s)"), "{rendered}");
        assert!(rendered.contains("every 30 seconds"), "{rendered}");
    }

    #[test]
    fn natural_language_preview_preserves_dst_boundary_timestamps() {
        let preview = natural_language_schedule_preview(
            "2026-03-29T03:30:00+02:00",
            CronTimezoneMode::Utc,
            0,
        )
        .expect("dst boundary timestamp should parse");
        assert_eq!(preview.schedule_type, "at");
        let expected = DateTime::parse_from_rfc3339("2026-03-29T03:30:00+02:00")
            .expect("timestamp should parse")
            .timestamp_millis();
        assert_eq!(preview.next_run_at_unix_ms, Some(expected));
    }

    #[test]
    fn routine_export_bundle_round_trips_metadata() {
        let bundle = build_routine_export_bundle(
            &crate::journal::CronJobRecord {
                job_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                name: "Heartbeat".to_owned(),
                prompt: "Ping".to_owned(),
                owner_principal: "user:test".to_owned(),
                channel: "system:routines".to_owned(),
                session_key: None,
                session_label: None,
                workdir: None,
                schedule_type: CronScheduleType::Every,
                schedule_payload_json: json!({ "interval_ms": 3_600_000_u64 }).to_string(),
                enabled: true,
                concurrency_policy: crate::journal::CronConcurrencyPolicy::Forbid,
                retry_policy: crate::journal::CronRetryPolicy {
                    max_attempts: 1,
                    backoff_ms: 1_000,
                },
                misfire_policy: crate::journal::CronMisfirePolicy::Skip,
                jitter_ms: 0,
                next_run_at_unix_ms: Some(1_700_000_000_000),
                last_run_at_unix_ms: None,
                queued_run: false,
                created_at_unix_ms: 1_700_000_000_000,
                updated_at_unix_ms: 1_700_000_000_000,
            },
            &super::RoutineMetadataRecord {
                routine_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                trigger_kind: RoutineTriggerKind::Schedule,
                trigger_payload_json: json!({ "schedule_type": "every" }).to_string(),
                execution: RoutineExecutionConfig::default(),
                delivery: RoutineDeliveryConfig::default(),
                quiet_hours: None,
                cooldown_ms: 0,
                approval_policy: RoutineApprovalPolicy::default(),
                template_id: Some("heartbeat".to_owned()),
                created_at_unix_ms: 1_700_000_000_000,
                updated_at_unix_ms: 1_700_000_000_000,
            },
        )
        .expect("export bundle should build");
        assert_eq!(bundle.schema_id, ROUTINE_EXPORT_SCHEMA_ID);
        validate_routine_export_bundle(&bundle).expect("bundle should validate");
    }

    #[test]
    fn routine_retention_dry_run_protects_active_refs() {
        let runs = vec![
            sample_run_metadata("old-active", "routine-1", 1_000),
            sample_run_metadata("old-free", "routine-1", 2_000),
            sample_run_metadata("fresh", "routine-1", 10_000),
        ];
        let active_run_ids = BTreeSet::from(["old-active".to_owned()]);
        let plan = routine_retention_dry_run(
            runs.as_slice(),
            &active_run_ids,
            RoutineRetentionPolicy { ttl_ms: 5_000, max_records: 2 },
            10_000,
        );

        assert!(plan.dry_run);
        assert_eq!(plan.retained_active_refs, 1);
        assert_eq!(plan.would_delete_count, 1);
        assert!(
            plan.candidates
                .iter()
                .any(|candidate| candidate.run_id == "old-active"
                    && candidate.protected_by_active_ref)
        );
        assert!(plan
            .candidates
            .iter()
            .any(|candidate| candidate.run_id == "old-free" && !candidate.protected_by_active_ref));
    }

    #[test]
    fn routine_backfill_plan_reports_missing_runtime_records_idempotently() {
        let routines = vec![
            sample_routine_metadata("routine-present"),
            sample_routine_metadata("routine-orphan"),
        ];
        let cron_jobs = vec![sample_cron_job("routine-present"), sample_cron_job("cron-orphan")];
        let run_metadata = vec![
            sample_run_metadata("run-present", "routine-present", 1_000),
            sample_run_metadata("run-orphan", "routine-missing", 1_000),
        ];

        let plan = routine_runtime_backfill_plan(
            routines.as_slice(),
            cron_jobs.as_slice(),
            run_metadata.as_slice(),
            true,
        );

        assert!(plan.dry_run);
        assert_eq!(plan.cron_jobs_missing_metadata, vec!["cron-orphan".to_owned()]);
        assert_eq!(plan.routines_missing_cron_job, vec!["routine-orphan".to_owned()]);
        assert_eq!(plan.run_metadata_without_routine, vec!["run-orphan".to_owned()]);
        assert_eq!(plan.changed_records, 3);

        let clean = routine_runtime_backfill_plan(
            &[sample_routine_metadata("routine-present")],
            &[sample_cron_job("routine-present")],
            &[sample_run_metadata("run-present", "routine-present", 1_000)],
            true,
        );
        assert_eq!(clean.changed_records, 0);
    }

    #[test]
    fn fresh_session_prompt_validation_rejects_brittle_references() {
        let error = validate_routine_prompt_self_contained(
            "Resume where you left off and keep the same output style.",
            &RoutineExecutionConfig {
                run_mode: RoutineRunMode::FreshSession,
                ..RoutineExecutionConfig::default()
            },
        )
        .expect_err("fresh-session prompt should reject implicit context");
        assert!(
            error.to_string().contains("self-contained"),
            "error should explain the self-contained requirement"
        );
    }

    #[test]
    fn delivery_preview_reflects_failure_only_policy() {
        let preview = routine_delivery_preview(&RoutineDeliveryConfig {
            mode: RoutineDeliveryMode::SameChannel,
            channel: None,
            failure_mode: Some(RoutineDeliveryMode::SpecificChannel),
            failure_channel: Some("ops:alerts".to_owned()),
            silent_policy: RoutineSilentPolicy::FailureOnly,
        });
        assert_eq!(preview["silent_policy"], json!("failure_only"));
        assert_eq!(preview["success"]["announced"], json!(false));
        assert_eq!(preview["failure"]["mode"], json!("specific_channel"));
        assert_eq!(preview["failure"]["channel"], json!("ops:alerts"));
        assert_eq!(preview["failure"]["announced"], json!(true));
    }

    #[test]
    fn delivery_contract_classifies_silent_and_operator_review_paths() {
        let silent = routine_delivery_contract(
            &RoutineDeliveryConfig {
                mode: RoutineDeliveryMode::SameChannel,
                channel: None,
                failure_mode: None,
                failure_channel: None,
                silent_policy: RoutineSilentPolicy::AuditOnly,
            },
            RoutineRunOutcomeKind::SuccessWithOutput,
            false,
        );
        assert_eq!(silent.kind, RoutineDeliveryContractKind::Silent);
        assert!(!silent.announced);
        assert!(!silent.dead_letter);

        let review = routine_delivery_contract(
            &RoutineDeliveryConfig {
                mode: RoutineDeliveryMode::LogsOnly,
                channel: None,
                failure_mode: None,
                failure_channel: None,
                silent_policy: RoutineSilentPolicy::Noisy,
            },
            RoutineRunOutcomeKind::Failed,
            false,
        );
        assert_eq!(review.kind, RoutineDeliveryContractKind::OperatorReview);
        assert!(review.requires_operator_review);
        assert!(review.dead_letter);
        assert!(!review.retryable);
    }

    #[test]
    fn lifecycle_snapshot_blocks_delivery_until_approval_and_detects_expired_lease() {
        let terminal = sample_cron_run(CronRunStatus::Succeeded, 2_000);
        let approval_pending = routine_run_lifecycle_snapshot(
            "routine-1",
            &terminal,
            None,
            &RoutineApprovalPolicy { mode: RoutineApprovalMode::BeforeFirstRun },
            2_100,
        );
        assert_eq!(approval_pending.approval_gate, RoutineApprovalGateState::Pending);
        assert_eq!(approval_pending.lease_state, RoutineRunLeaseState::Released);
        assert!(!approval_pending.delivery_ready);

        let active = sample_cron_run(CronRunStatus::Running, 1_000);
        let expired = routine_run_lifecycle_snapshot(
            "routine-1",
            &active,
            None,
            &RoutineApprovalPolicy::default(),
            1_000 + ROUTINE_RUN_LEASE_TTL_MS + 1,
        );
        assert_eq!(expired.lease_state, RoutineRunLeaseState::Expired);
        assert_eq!(expired.approval_gate, RoutineApprovalGateState::NotRequired);
        assert!(!expired.delivery_ready);
        assert_eq!(expired.recovery_hint, "repair_or_cancel_expired_routine_run_lease");
    }

    #[test]
    fn preflight_step_validation_fails_closed_for_scope_and_timeout() {
        use super::routine_preflight_contracts::*;

        let valid = validate_preflight_step(&RoutinePreflightStep {
            step_id: "provider-health".to_owned(),
            tool_name: "palyra.routines.preflight.provider_health".to_owned(),
            input_schema: json!({"type": "object"}),
            output_schema: json!({"type": "object"}),
            required_scopes: vec!["routines:read".to_owned()],
            capability_names: vec!["routine_preflight".to_owned()],
            timeout_ms: 5_000,
        });
        let invalid = validate_preflight_step(&RoutinePreflightStep {
            step_id: "bad".to_owned(),
            tool_name: "palyra.routines.preflight.provider_health".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            required_scopes: vec!["*".to_owned()],
            capability_names: vec!["routine_preflight".to_owned()],
            timeout_ms: 0,
        });

        assert!(valid.accepted);
        assert!(!invalid.accepted);
        assert_eq!(invalid.reason, "preflight_timeout_out_of_range");
    }

    #[test]
    fn preflight_context_fence_drops_and_redacts_context() {
        use super::routine_preflight_contracts::*;

        let fence = RoutinePreflightContextFence::allow_keys(
            &["routine_id", "channel", "api_token", "large"],
            16,
        );
        let fenced = fence_preflight_context(
            &json!({
                "routine_id": "routine-1",
                "channel": "ops",
                "api_token": "secret",
                "large": "abcdefghijklmnopqrstuvwxyz",
                "untrusted": "drop",
            }),
            &fence,
        );

        assert_eq!(fenced.context["routine_id"], json!("routine-1"));
        assert_eq!(fenced.context["api_token"], json!(palyra_common::redaction::REDACTED));
        assert!(fenced.context["large"].as_str().unwrap().ends_with("..."));
        assert_eq!(fenced.dropped_keys, vec!["untrusted".to_owned()]);
        assert_eq!(fenced.redacted_keys, vec!["api_token".to_owned()]);
    }

    #[test]
    fn wake_gate_records_typed_block_reasons() {
        use super::routine_preflight_contracts::*;

        let approval = evaluate_routine_wake_gate(RoutineWakeGateInput {
            enabled: true,
            schedule_tick_at_unix_ms: Some(1_000),
            last_run_status: None,
            preflight_outcome: Some(RoutinePreflightOutcome::AskApproval),
            provider_cooldown_until_unix_ms: None,
            channel_healthy: true,
            policy_allowed: true,
            now_unix_ms: 1_000,
        });
        let active = evaluate_routine_wake_gate(RoutineWakeGateInput {
            enabled: true,
            schedule_tick_at_unix_ms: Some(1_000),
            last_run_status: Some(CronRunStatus::Running),
            preflight_outcome: Some(RoutinePreflightOutcome::Proceed),
            provider_cooldown_until_unix_ms: None,
            channel_healthy: true,
            policy_allowed: true,
            now_unix_ms: 1_000,
        });
        let allowed = evaluate_routine_wake_gate(RoutineWakeGateInput {
            enabled: true,
            schedule_tick_at_unix_ms: Some(1_000),
            last_run_status: Some(CronRunStatus::Succeeded),
            preflight_outcome: Some(RoutinePreflightOutcome::Proceed),
            provider_cooldown_until_unix_ms: None,
            channel_healthy: true,
            policy_allowed: true,
            now_unix_ms: 1_000,
        });

        assert!(!approval.allowed);
        assert_eq!(approval.reason, RoutineWakeGateReason::PreflightApprovalRequired);
        assert!(!active.allowed);
        assert_eq!(active.reason, RoutineWakeGateReason::LastRunStillActive);
        assert!(allowed.allowed);
        assert_eq!(allowed.snapshot_json()["reason"], json!("allowed"));
    }
}
