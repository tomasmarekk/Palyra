//! Typed middleware patches, one-shot execution wrappers, and durable hook traces.
//!
//! The host applies these contracts after deterministic arbitration and before
//! schema and policy revalidation; plugins never receive authority to commit them.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::AgentHookKind;

/// Schema version for typed middleware patches and invocation traces.
pub const HOOK_MIDDLEWARE_SCHEMA_VERSION: u32 = 1;
/// Maximum field-level diff entries retained in one durable invocation trace.
pub const MAX_HOOK_APPLIED_DIFFS: usize = 16;
/// Maximum stable reason-code bytes retained in one durable invocation trace.
pub const MAX_HOOK_REASON_CODE_BYTES: usize = 128;

runtime_contract_enum! {
    /// Host-approved role for one middleware invocation.
    pub enum HookMiddlewareRole {
        Observer => "observer",
        Reducer => "reducer",
        Blocker => "blocker",
        Transformer => "transformer",
        ExecutionWrapper => "execution_wrapper"
    }
}

runtime_contract_enum! {
    /// Failure posture jointly enforced by the manifest binding and host policy.
    pub enum HookFailureMode {
        FailOpen => "fail_open",
        FailClosed => "fail_closed"
    }
}

runtime_contract_enum! {
    /// Typed patch family an invocation point accepts.
    pub enum HookPatchKind {
        None => "none",
        ProviderRequest => "provider_request",
        ToolArguments => "tool_arguments"
    }
}

runtime_contract_enum! {
    /// Stable terminal outcome retained for a middleware invocation.
    pub enum HookInvocationOutcome {
        Applied => "applied",
        Observed => "observed",
        NoChange => "no_change",
        Blocked => "blocked",
        FailedOpen => "failed_open",
        FailedClosed => "failed_closed",
        TimedOut => "timed_out",
        Panicked => "panicked",
        Conflict => "conflict"
    }
}

/// One production lifecycle point in the central hook invocation map.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct HookInvocationMapEntry {
    /// Public hook kind dispatched at this lifecycle point.
    pub hook: AgentHookKind,
    /// Durable runtime stage identifier.
    pub runtime_stage: &'static str,
    /// Strongest role the host may grant at this point.
    pub role: HookMiddlewareRole,
    /// Host-enforced failure posture.
    pub failure_mode: HookFailureMode,
    /// Typed patch family accepted from this lifecycle point.
    pub patch_kind: HookPatchKind,
}

const fn hook_map_entry(
    hook: AgentHookKind,
    runtime_stage: &'static str,
    role: HookMiddlewareRole,
    failure_mode: HookFailureMode,
    patch_kind: HookPatchKind,
) -> HookInvocationMapEntry {
    HookInvocationMapEntry { hook, runtime_stage, role, failure_mode, patch_kind }
}

/// Central production invocation map for the complete approved hook taxonomy.
///
/// Observer points are fail-open. Points capable of blocking, transforming, or
/// wrapping execution are fail-closed so a plugin failure cannot bypass policy.
pub const HOOK_INVOCATION_MAP: &[HookInvocationMapEntry] = &[
    hook_map_entry(
        AgentHookKind::RunBeforeRun,
        "run.admission",
        HookMiddlewareRole::Blocker,
        HookFailureMode::FailClosed,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::RunBeforeTool,
        "tool.request",
        HookMiddlewareRole::Transformer,
        HookFailureMode::FailClosed,
        HookPatchKind::ToolArguments,
    ),
    hook_map_entry(
        AgentHookKind::RunAfterTool,
        "tool.result",
        HookMiddlewareRole::Observer,
        HookFailureMode::FailOpen,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::RunBeforeDelivery,
        "delivery.request",
        HookMiddlewareRole::Transformer,
        HookFailureMode::FailClosed,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::RunAfterRun,
        "run.terminal",
        HookMiddlewareRole::Observer,
        HookFailureMode::FailOpen,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::BeforeModelResolve,
        "provider.resolve",
        HookMiddlewareRole::Reducer,
        HookFailureMode::FailClosed,
        HookPatchKind::ProviderRequest,
    ),
    hook_map_entry(
        AgentHookKind::BeforePromptBuild,
        "provider.prompt_build",
        HookMiddlewareRole::Reducer,
        HookFailureMode::FailClosed,
        HookPatchKind::ProviderRequest,
    ),
    hook_map_entry(
        AgentHookKind::BeforeAgentRun,
        "agent.run",
        HookMiddlewareRole::Blocker,
        HookFailureMode::FailClosed,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::BeforeAgentReply,
        "agent.reply",
        HookMiddlewareRole::Observer,
        HookFailureMode::FailOpen,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::BeforeAgentFinalize,
        "agent.finalize",
        HookMiddlewareRole::Blocker,
        HookFailureMode::FailClosed,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::AgentEnd,
        "agent.end",
        HookMiddlewareRole::Observer,
        HookFailureMode::FailOpen,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::ModelCallStarted,
        "provider.execution.started",
        HookMiddlewareRole::Observer,
        HookFailureMode::FailOpen,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::ModelCallEnded,
        "provider.execution.ended",
        HookMiddlewareRole::Observer,
        HookFailureMode::FailOpen,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::BeforeToolCall,
        "tool.execution.started",
        HookMiddlewareRole::ExecutionWrapper,
        HookFailureMode::FailClosed,
        HookPatchKind::ToolArguments,
    ),
    hook_map_entry(
        AgentHookKind::AfterToolCall,
        "tool.execution.ended",
        HookMiddlewareRole::Observer,
        HookFailureMode::FailOpen,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::InboundClaim,
        "message.inbound_claim",
        HookMiddlewareRole::Blocker,
        HookFailureMode::FailClosed,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::BeforeMessageWrite,
        "message.persist",
        HookMiddlewareRole::Transformer,
        HookFailureMode::FailClosed,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::MessageSending,
        "message.send",
        HookMiddlewareRole::Transformer,
        HookFailureMode::FailClosed,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::ReplyPayloadSending,
        "reply.send",
        HookMiddlewareRole::Transformer,
        HookFailureMode::FailClosed,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::ReplyDispatch,
        "reply.dispatched",
        HookMiddlewareRole::Observer,
        HookFailureMode::FailOpen,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::SessionStart,
        "session.started",
        HookMiddlewareRole::Observer,
        HookFailureMode::FailOpen,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::SessionEnd,
        "session.ended",
        HookMiddlewareRole::Observer,
        HookFailureMode::FailOpen,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::BeforeReset,
        "session.reset",
        HookMiddlewareRole::Blocker,
        HookFailureMode::FailClosed,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::BeforeCompaction,
        "session.compaction.started",
        HookMiddlewareRole::Blocker,
        HookFailureMode::FailClosed,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::AfterCompaction,
        "session.compaction.ended",
        HookMiddlewareRole::Observer,
        HookFailureMode::FailOpen,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::SubagentSpawned,
        "subagent.started",
        HookMiddlewareRole::Observer,
        HookFailureMode::FailOpen,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::SubagentEnded,
        "subagent.ended",
        HookMiddlewareRole::Observer,
        HookFailureMode::FailOpen,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::BeforeToolResultProject,
        "tool_result.project",
        HookMiddlewareRole::Transformer,
        HookFailureMode::FailClosed,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::ToolResultProjected,
        "tool_result.projected",
        HookMiddlewareRole::Observer,
        HookFailureMode::FailOpen,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::ToolResultPersist,
        "tool_result.persist",
        HookMiddlewareRole::Transformer,
        HookFailureMode::FailClosed,
        HookPatchKind::None,
    ),
    hook_map_entry(
        AgentHookKind::ToolResultModelFeed,
        "tool_result.model_feed",
        HookMiddlewareRole::Transformer,
        HookFailureMode::FailClosed,
        HookPatchKind::None,
    ),
];

/// Returns the central invocation-map entry for a hook kind.
#[must_use]
pub fn hook_invocation_map_entry(hook: AgentHookKind) -> Option<&'static HookInvocationMapEntry> {
    HOOK_INVOCATION_MAP.iter().find(|entry| entry.hook == hook)
}

/// Typed, authority-reducing patch for a provider request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRequestPatch {
    /// Patch schema version.
    pub schema_version: u32,
    /// SHA-256 of the exact host request the patch was computed against.
    pub base_request_sha256: String,
    /// Optional lower output-token ceiling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u64>,
    /// Optional request for JSON output; a patch cannot disable an existing requirement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json_mode: Option<bool>,
}

/// Host-approved provider request projection after applying a patch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderRequestPatchProjection {
    /// Effective output-token ceiling.
    pub max_output_tokens: Option<u64>,
    /// Effective JSON response posture.
    pub json_mode: bool,
}

/// Field-level typed patch for tool arguments.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolArgumentPatch {
    /// Patch schema version.
    pub schema_version: u32,
    /// SHA-256 of the exact normalized argument object being patched.
    pub base_arguments_sha256: String,
    /// Existing top-level fields to replace.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub set_fields: BTreeMap<String, Value>,
    /// Existing top-level fields to remove.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remove_fields: Vec<String>,
}

/// Validation or deterministic arbitration failure for a middleware patch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MiddlewarePatchError {
    /// Stable reason code.
    pub code: String,
    /// Redacted operator-safe message.
    pub message: String,
}

impl MiddlewarePatchError {
    fn new(code: &str, message: impl Into<String>) -> Self {
        Self { code: code.to_owned(), message: message.into() }
    }
}

/// Resolves provider patches deterministically and rejects conflicting values.
///
/// # Errors
///
/// Returns a stable conflict or schema error when patches do not target the
/// same request or propose different values for one field.
pub fn resolve_provider_request_patches(
    patches: &[ProviderRequestPatch],
) -> Result<Option<ProviderRequestPatch>, MiddlewarePatchError> {
    let Some(first) = patches.first() else {
        return Ok(None);
    };
    validate_patch_schema(first.schema_version)?;
    let mut resolved = first.clone();
    for patch in patches.iter().skip(1) {
        validate_patch_schema(patch.schema_version)?;
        if patch.base_request_sha256 != resolved.base_request_sha256 {
            return Err(MiddlewarePatchError::new(
                "middleware.patch.base_conflict",
                "provider patches target different request revisions",
            ));
        }
        merge_optional_field(
            &mut resolved.max_output_tokens,
            patch.max_output_tokens,
            "max_output_tokens",
        )?;
        merge_optional_field(&mut resolved.json_mode, patch.json_mode, "json_mode")?;
    }
    Ok(Some(resolved))
}

/// Applies an authority-reducing provider request patch.
///
/// # Errors
///
/// Returns a stable error for revision mismatch, zero/increased token budget,
/// or an attempt to disable host-required JSON output.
pub fn apply_provider_request_patch(
    host_request_sha256: &str,
    host: ProviderRequestPatchProjection,
    patch: &ProviderRequestPatch,
) -> Result<ProviderRequestPatchProjection, MiddlewarePatchError> {
    validate_patch_schema(patch.schema_version)?;
    if patch.base_request_sha256 != host_request_sha256 {
        return Err(MiddlewarePatchError::new(
            "middleware.patch.stale_base",
            "provider patch does not target the current request revision",
        ));
    }
    let max_output_tokens = match (host.max_output_tokens, patch.max_output_tokens) {
        (_, Some(0)) => {
            return Err(MiddlewarePatchError::new(
                "middleware.patch.invalid_budget",
                "provider output token ceiling must be nonzero",
            ));
        }
        (Some(host_limit), Some(requested)) if requested > host_limit => {
            return Err(MiddlewarePatchError::new(
                "middleware.patch.authority_increase",
                "provider patch cannot increase the output token ceiling",
            ));
        }
        (_, Some(requested)) => Some(requested),
        (host_limit, None) => host_limit,
    };
    let json_mode = match patch.json_mode {
        Some(false) if host.json_mode => {
            return Err(MiddlewarePatchError::new(
                "middleware.patch.authority_increase",
                "provider patch cannot disable host-required JSON output",
            ));
        }
        Some(requested) => requested,
        None => host.json_mode,
    };
    Ok(ProviderRequestPatchProjection { max_output_tokens, json_mode })
}

/// Resolves tool-argument patches deterministically and rejects field conflicts.
///
/// # Errors
///
/// Returns a stable conflict or schema error when patches target different
/// argument revisions, set one field differently, or both set and remove it.
pub fn resolve_tool_argument_patches(
    patches: &[ToolArgumentPatch],
) -> Result<Option<ToolArgumentPatch>, MiddlewarePatchError> {
    let Some(first) = patches.first() else {
        return Ok(None);
    };
    validate_patch_schema(first.schema_version)?;
    let mut resolved = first.clone();
    validate_tool_patch_internal_conflicts(&resolved)?;
    resolved.remove_fields.sort();
    resolved.remove_fields.dedup();
    for patch in patches.iter().skip(1) {
        validate_patch_schema(patch.schema_version)?;
        validate_tool_patch_internal_conflicts(patch)?;
        if patch.base_arguments_sha256 != resolved.base_arguments_sha256 {
            return Err(MiddlewarePatchError::new(
                "middleware.patch.base_conflict",
                "tool patches target different argument revisions",
            ));
        }
        for (field, value) in &patch.set_fields {
            if resolved.remove_fields.iter().any(|removed| removed == field)
                || resolved.set_fields.get(field).is_some_and(|existing| existing != value)
            {
                return Err(field_conflict(field));
            }
            resolved.set_fields.insert(field.clone(), value.clone());
        }
        for field in &patch.remove_fields {
            if resolved.set_fields.contains_key(field) {
                return Err(field_conflict(field));
            }
            if !resolved.remove_fields.contains(field) {
                resolved.remove_fields.push(field.clone());
            }
        }
    }
    resolved.remove_fields.sort();
    Ok(Some(resolved))
}

/// Applies a tool-argument patch to host-allowlisted existing fields.
///
/// This structural gate is intentionally followed by the owning tool schema
/// validator and policy engine; it is not a substitute for either boundary.
///
/// # Errors
///
/// Returns a stable error for revision mismatch, unknown/non-allowlisted
/// fields, or authority-bearing field names.
pub fn apply_tool_argument_patch(
    host_arguments_sha256: &str,
    host_arguments: &Value,
    allowlisted_fields: &BTreeSet<String>,
    patch: &ToolArgumentPatch,
) -> Result<Value, MiddlewarePatchError> {
    validate_patch_schema(patch.schema_version)?;
    if patch.base_arguments_sha256 != host_arguments_sha256 {
        return Err(MiddlewarePatchError::new(
            "middleware.patch.stale_base",
            "tool patch does not target the current argument revision",
        ));
    }
    let Value::Object(host_fields) = host_arguments else {
        return Err(MiddlewarePatchError::new(
            "middleware.patch.invalid_arguments",
            "tool arguments must be a JSON object",
        ));
    };
    let mut patched = host_fields.clone();
    for (field, value) in &patch.set_fields {
        validate_tool_patch_field(field, allowlisted_fields)?;
        if !host_fields.contains_key(field) {
            return Err(MiddlewarePatchError::new(
                "middleware.patch.field_addition_denied",
                format!("tool patch cannot add field {field}"),
            ));
        }
        patched.insert(field.clone(), value.clone());
    }
    for field in &patch.remove_fields {
        validate_tool_patch_field(field, allowlisted_fields)?;
        if !host_fields.contains_key(field) {
            return Err(MiddlewarePatchError::new(
                "middleware.patch.unknown_field",
                format!("tool patch cannot remove absent field {field}"),
            ));
        }
        patched.remove(field);
    }
    Ok(Value::Object(patched))
}

/// One-shot host capability for execution-wrapper middleware.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionWrapperCapability {
    invocation_id_hash: String,
    consumed: bool,
}

impl ExecutionWrapperCapability {
    /// Creates an unconsumed capability bound to a redacted invocation id.
    #[must_use]
    pub fn new(invocation_id_hash: impl Into<String>) -> Self {
        Self { invocation_id_hash: invocation_id_hash.into(), consumed: false }
    }

    /// Consumes the sole permission to call the wrapped execution.
    ///
    /// # Errors
    ///
    /// Returns `middleware.execution_wrapper.double_next` after the first call.
    pub fn next_call(&mut self) -> Result<(), MiddlewarePatchError> {
        if self.consumed {
            return Err(MiddlewarePatchError::new(
                "middleware.execution_wrapper.double_next",
                "execution wrapper next capability was already consumed",
            ));
        }
        self.consumed = true;
        Ok(())
    }

    /// Returns the redacted invocation identity the capability is bound to.
    #[must_use]
    pub fn invocation_id_hash(&self) -> &str {
        self.invocation_id_hash.as_str()
    }

    /// Returns whether the wrapped execution permission was consumed.
    #[must_use]
    pub const fn is_consumed(&self) -> bool {
        self.consumed
    }
}

/// One bounded field-level diff retained in a hook invocation trace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookAppliedDiff {
    /// Allowlisted field name; never the field value.
    pub field: String,
    /// SHA-256 of the pre-patch value or `none`.
    pub before_sha256: String,
    /// SHA-256 of the post-patch value or `none`.
    pub after_sha256: String,
}

/// Bounded, redacted durable trace for one middleware invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookInvocationTrace {
    /// Trace schema version.
    pub schema_version: u32,
    /// Domain-separated hash of the invocation identity.
    pub invocation_id_hash: String,
    /// Hook kind that ran.
    pub hook: AgentHookKind,
    /// Deterministic invocation order within the lifecycle point.
    pub order: u16,
    /// Host-approved role.
    pub role: HookMiddlewareRole,
    /// Effective failure posture.
    pub failure_mode: HookFailureMode,
    /// Monotonic host-observed duration.
    pub duration_ms: u64,
    /// Terminal outcome.
    pub outcome: HookInvocationOutcome,
    /// Typed patch family considered.
    pub patch_kind: HookPatchKind,
    /// Hash-only applied field diffs.
    pub applied_diff: Vec<HookAppliedDiff>,
    /// Stable bounded reason code.
    pub reason_code: String,
    /// Always true; raw payloads and values are excluded by contract.
    pub redacted: bool,
}

impl HookInvocationTrace {
    /// Builds a bounded trace, truncating excess diff metadata deterministically.
    #[must_use]
    pub fn new(
        invocation_id_hash: impl Into<String>,
        hook: AgentHookKind,
        order: u16,
        duration_ms: u64,
        outcome: HookInvocationOutcome,
        applied_diff: Vec<HookAppliedDiff>,
        reason_code: &str,
    ) -> Self {
        let map_entry = hook_invocation_map_entry(hook);
        let role = map_entry.map_or(HookMiddlewareRole::Observer, |entry| entry.role);
        let failure_mode = map_entry.map_or(HookFailureMode::FailOpen, |entry| entry.failure_mode);
        let patch_kind = map_entry.map_or(HookPatchKind::None, |entry| entry.patch_kind);
        Self {
            schema_version: HOOK_MIDDLEWARE_SCHEMA_VERSION,
            invocation_id_hash: invocation_id_hash.into(),
            hook,
            order,
            role,
            failure_mode,
            duration_ms,
            outcome,
            patch_kind,
            applied_diff: applied_diff.into_iter().take(MAX_HOOK_APPLIED_DIFFS).collect(),
            reason_code: bounded_reason_code(reason_code),
            redacted: true,
        }
    }
}

fn validate_patch_schema(schema_version: u32) -> Result<(), MiddlewarePatchError> {
    if schema_version == HOOK_MIDDLEWARE_SCHEMA_VERSION {
        return Ok(());
    }
    Err(MiddlewarePatchError::new(
        "middleware.patch.schema_version_unsupported",
        format!("unsupported middleware patch schema version {schema_version}"),
    ))
}

fn merge_optional_field<T: Copy + PartialEq>(
    current: &mut Option<T>,
    incoming: Option<T>,
    field: &str,
) -> Result<(), MiddlewarePatchError> {
    let Some(incoming) = incoming else {
        return Ok(());
    };
    if current.is_some_and(|existing| existing != incoming) {
        return Err(field_conflict(field));
    }
    *current = Some(incoming);
    Ok(())
}

fn field_conflict(field: &str) -> MiddlewarePatchError {
    MiddlewarePatchError::new(
        "middleware.patch.conflict",
        format!("middleware patches conflict on field {field}"),
    )
}

fn validate_tool_patch_internal_conflicts(
    patch: &ToolArgumentPatch,
) -> Result<(), MiddlewarePatchError> {
    for field in &patch.remove_fields {
        if patch.set_fields.contains_key(field) {
            return Err(field_conflict(field));
        }
    }
    Ok(())
}

fn validate_tool_patch_field(
    field: &str,
    allowlisted_fields: &BTreeSet<String>,
) -> Result<(), MiddlewarePatchError> {
    if field.is_empty()
        || field.len() > 128
        || !field.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(MiddlewarePatchError::new(
            "middleware.patch.field_invalid",
            "tool patch field is outside the stable field-name grammar",
        ));
    }
    let normalized = field.to_ascii_lowercase().replace('-', "_");
    if [
        "approval",
        "budget",
        "capabilities",
        "credential",
        "egress",
        "policy",
        "scope",
        "secret",
        "timeout",
    ]
    .iter()
    .any(|authority| normalized.contains(authority))
    {
        return Err(MiddlewarePatchError::new(
            "middleware.patch.authority_field_denied",
            format!("tool patch cannot modify authority-bearing field {field}"),
        ));
    }
    if !allowlisted_fields.contains(field) {
        return Err(MiddlewarePatchError::new(
            "middleware.patch.field_not_allowlisted",
            format!("tool patch field {field} is not allowlisted by the host"),
        ));
    }
    Ok(())
}

fn bounded_reason_code(reason_code: &str) -> String {
    reason_code
        .chars()
        .take_while(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_'))
        .take(MAX_HOOK_REASON_CODE_BYTES)
        .collect()
}

/// Returns a field-name allowlist from an already schema-normalized tool argument object.
#[must_use]
pub fn existing_tool_argument_fields(arguments: &Value) -> BTreeSet<String> {
    match arguments {
        Value::Object(fields) => fields.keys().cloned().collect(),
        _ => BTreeSet::new(),
    }
}

/// Builds hash-only diffs for provider request fields changed by a patch.
#[must_use]
pub fn provider_patch_applied_diff(
    before: ProviderRequestPatchProjection,
    after: ProviderRequestPatchProjection,
    hash_value: impl Fn(&Value) -> String,
) -> Vec<HookAppliedDiff> {
    let mut diffs = Vec::new();
    if before.max_output_tokens != after.max_output_tokens {
        diffs.push(diff_entry(
            "max_output_tokens",
            &serde_json::to_value(before.max_output_tokens).unwrap_or(Value::Null),
            &serde_json::to_value(after.max_output_tokens).unwrap_or(Value::Null),
            &hash_value,
        ));
    }
    if before.json_mode != after.json_mode {
        diffs.push(diff_entry(
            "json_mode",
            &Value::Bool(before.json_mode),
            &Value::Bool(after.json_mode),
            &hash_value,
        ));
    }
    diffs
}

/// Builds hash-only diffs for fields changed by a tool-argument patch.
#[must_use]
pub fn tool_patch_applied_diff(
    before: &Value,
    after: &Value,
    patch: &ToolArgumentPatch,
    hash_value: impl Fn(&Value) -> String,
) -> Vec<HookAppliedDiff> {
    let before_fields = before.as_object();
    let after_fields = after.as_object();
    patch
        .set_fields
        .keys()
        .chain(patch.remove_fields.iter())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|field| {
            diff_entry(
                field,
                before_fields.and_then(|fields| fields.get(field)).unwrap_or(&Value::Null),
                after_fields.and_then(|fields| fields.get(field)).unwrap_or(&Value::Null),
                &hash_value,
            )
        })
        .take(MAX_HOOK_APPLIED_DIFFS)
        .collect()
}

fn diff_entry(
    field: &str,
    before: &Value,
    after: &Value,
    hash_value: &impl Fn(&Value) -> String,
) -> HookAppliedDiff {
    HookAppliedDiff {
        field: field.to_owned(),
        before_sha256: hash_value(before),
        after_sha256: hash_value(after),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use proptest::prelude::*;
    use serde_json::json;

    use super::{
        apply_provider_request_patch, apply_tool_argument_patch, resolve_provider_request_patches,
        resolve_tool_argument_patches, ExecutionWrapperCapability, HookFailureMode,
        HookInvocationOutcome, HookInvocationTrace, HookMiddlewareRole, ProviderRequestPatch,
        ProviderRequestPatchProjection, ToolArgumentPatch, HOOK_INVOCATION_MAP,
    };
    use crate::runtime_contracts::{
        AgentHookDecisionAuthority, AgentHookKind, AGENT_HOOK_DESCRIPTORS,
    };

    proptest! {
        #[test]
        fn provider_patch_never_increases_a_host_budget(
            host_limit in 1_u64..1_000_000,
            requested_limit in 1_u64..2_000_000,
        ) {
            let host = ProviderRequestPatchProjection {
                max_output_tokens: Some(host_limit),
                json_mode: false,
            };
            let patch = ProviderRequestPatch {
                schema_version: 1,
                base_request_sha256: "base".to_owned(),
                max_output_tokens: Some(requested_limit),
                json_mode: None,
            };
            if let Ok(applied) = apply_provider_request_patch("base", host, &patch) {
                prop_assert!(applied.max_output_tokens.is_some_and(|limit| limit <= host_limit));
            }
        }
    }

    #[test]
    fn invocation_map_covers_every_public_descriptor_once() {
        let mapped = HOOK_INVOCATION_MAP.iter().map(|entry| entry.hook).collect::<BTreeSet<_>>();
        assert_eq!(mapped.len(), HOOK_INVOCATION_MAP.len());
        for descriptor in AGENT_HOOK_DESCRIPTORS {
            assert!(mapped.contains(&descriptor.kind), "missing {:?}", descriptor.kind);
            let entry = HOOK_INVOCATION_MAP
                .iter()
                .find(|entry| entry.hook == descriptor.kind)
                .expect("descriptor should have an invocation point");
            if descriptor.decision_authority == AgentHookDecisionAuthority::ObservationOnly {
                assert_eq!(entry.role, HookMiddlewareRole::Observer);
                assert_eq!(entry.failure_mode, HookFailureMode::FailOpen);
            } else {
                assert_eq!(entry.failure_mode, HookFailureMode::FailClosed);
            }
        }
    }

    #[test]
    fn provider_patch_cannot_increase_budget_or_disable_json_mode() {
        let host = ProviderRequestPatchProjection { max_output_tokens: Some(500), json_mode: true };
        let patch = ProviderRequestPatch {
            schema_version: 1,
            base_request_sha256: "base".to_owned(),
            max_output_tokens: Some(501),
            json_mode: None,
        };
        let error = apply_provider_request_patch("base", host, &patch)
            .expect_err("budget increase must fail");
        assert_eq!(error.code, "middleware.patch.authority_increase");

        let patch =
            ProviderRequestPatch { max_output_tokens: None, json_mode: Some(false), ..patch };
        let error = apply_provider_request_patch("base", host, &patch)
            .expect_err("JSON downgrade must fail");
        assert_eq!(error.code, "middleware.patch.authority_increase");
    }

    #[test]
    fn conflicting_provider_patches_are_rejected() {
        let first = ProviderRequestPatch {
            schema_version: 1,
            base_request_sha256: "base".to_owned(),
            max_output_tokens: Some(400),
            json_mode: None,
        };
        let second = ProviderRequestPatch { max_output_tokens: Some(300), ..first.clone() };
        let error = resolve_provider_request_patches(&[first, second])
            .expect_err("conflicting patches must fail");
        assert_eq!(error.code, "middleware.patch.conflict");
    }

    #[test]
    fn tool_patch_rejects_authority_fields_and_field_additions() {
        let arguments = json!({"query": "safe", "timeout_ms": 50});
        let mut set_fields = BTreeMap::new();
        set_fields.insert("timeout_ms".to_owned(), json!(500));
        let patch = ToolArgumentPatch {
            schema_version: 1,
            base_arguments_sha256: "base".to_owned(),
            set_fields,
            remove_fields: Vec::new(),
        };
        let allowlisted = ["query".to_owned(), "timeout_ms".to_owned()].into_iter().collect();
        let error = apply_tool_argument_patch("base", &arguments, &allowlisted, &patch)
            .expect_err("authority field mutation must fail");
        assert_eq!(error.code, "middleware.patch.authority_field_denied");

        let patch = ToolArgumentPatch {
            set_fields: BTreeMap::from([("new_field".to_owned(), json!(true))]),
            ..patch
        };
        let error = apply_tool_argument_patch(
            "base",
            &arguments,
            &["new_field".to_owned()].into_iter().collect(),
            &patch,
        )
        .expect_err("field addition must fail");
        assert_eq!(error.code, "middleware.patch.field_addition_denied");
    }

    #[test]
    fn conflicting_tool_patches_are_rejected() {
        let first = ToolArgumentPatch {
            schema_version: 1,
            base_arguments_sha256: "base".to_owned(),
            set_fields: BTreeMap::from([("query".to_owned(), json!("one"))]),
            remove_fields: Vec::new(),
        };
        let second = ToolArgumentPatch {
            set_fields: BTreeMap::from([("query".to_owned(), json!("two"))]),
            ..first.clone()
        };
        let error =
            resolve_tool_argument_patches(&[first, second]).expect_err("conflict must fail");
        assert_eq!(error.code, "middleware.patch.conflict");

        let self_conflicting = ToolArgumentPatch {
            schema_version: 1,
            base_arguments_sha256: "base".to_owned(),
            set_fields: BTreeMap::from([("query".to_owned(), json!("one"))]),
            remove_fields: vec!["query".to_owned()],
        };
        let error = resolve_tool_argument_patches(&[self_conflicting])
            .expect_err("one patch cannot set and remove the same field");
        assert_eq!(error.code, "middleware.patch.conflict");
    }

    #[test]
    fn execution_wrapper_rejects_second_next_call() {
        let mut capability = ExecutionWrapperCapability::new("invocation-hash");
        capability.next_call().expect("first next call should be admitted");
        let error = capability.next_call().expect_err("second next call must be rejected");
        assert_eq!(error.code, "middleware.execution_wrapper.double_next");
        assert!(capability.is_consumed());
    }

    #[test]
    fn invocation_trace_is_redacted_bounded_and_replay_stable() {
        let trace = HookInvocationTrace::new(
            "trace-hash",
            AgentHookKind::BeforeToolCall,
            3,
            25,
            HookInvocationOutcome::Applied,
            Vec::new(),
            "hook.tool_arguments.revalidated: secret-token-value",
        );
        let first = serde_json::to_vec(&trace).expect("trace should serialize");
        let second = serde_json::to_vec(&trace).expect("trace replay should serialize");

        assert_eq!(first, second);
        assert!(trace.redacted);
        assert_eq!(trace.reason_code, "hook.tool_arguments.revalidated");
        assert!(!String::from_utf8(first).expect("trace is UTF-8").contains("secret-token-value"));
    }
}
