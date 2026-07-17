//! Input-aware restart and reconciliation semantics for builtin tools.
//!
//! Catalog replay classes are intentionally coarse. This module resolves the
//! normalized call input into the narrower durable side-effect contract used
//! by execution fences, without treating an unknown external mutation as if a
//! receipt-based reconciler existed.

use palyra_common::runtime_contracts::{
    ReconciliationStrategy, RuntimeIdempotencyClass, SideEffectRestartPolicy,
    ToolExecutionSemantics,
};
use serde_json::{Map, Value};

use super::types::ToolReplaySafetyClass;

/// Closed mutation classes supported by the daemon's reconciler registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ToolMutationClass {
    WorkspaceFilePatch,
    ProcessMutation,
    HttpMutationWithExternalIdempotencyKey,
    HttpMutationWithoutExternalIdempotencyKey,
    Delivery,
    WorkerTask,
    OperatorConfirmation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuiltinToolEffect {
    ReadOnly,
    DeterministicIdempotent,
    Mutation(ToolMutationClass),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BuiltinToolEffectResolution {
    effect: BuiltinToolEffect,
    external_idempotency_key_sha256: Option<String>,
}

impl BuiltinToolEffectResolution {
    const fn read_only() -> Self {
        Self { effect: BuiltinToolEffect::ReadOnly, external_idempotency_key_sha256: None }
    }

    const fn deterministic_idempotent() -> Self {
        Self {
            effect: BuiltinToolEffect::DeterministicIdempotent,
            external_idempotency_key_sha256: None,
        }
    }

    const fn mutation(mutation_class: ToolMutationClass) -> Self {
        Self {
            effect: BuiltinToolEffect::Mutation(mutation_class),
            external_idempotency_key_sha256: None,
        }
    }

    fn keyed_http_mutation(key_sha256: String) -> Self {
        Self {
            effect: BuiltinToolEffect::Mutation(
                ToolMutationClass::HttpMutationWithExternalIdempotencyKey,
            ),
            external_idempotency_key_sha256: Some(key_sha256),
        }
    }
}

/// Semantics plus the optional key digest that must be persisted atomically
/// with a side-effect fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedToolExecutionSemantics {
    pub(crate) semantics: ToolExecutionSemantics,
    pub(crate) external_idempotency_key_sha256: Option<String>,
}

/// Resolves one normalized tool call into its durable restart contract.
///
/// Unknown mutating tools and malformed input fail closed to operator
/// confirmation. The resolver never assigns a receipt strategy merely
/// because a catalog entry has the coarse `external_side_effect` label.
#[must_use]
pub(crate) fn resolve_tool_execution_semantics(
    tool_name: &str,
    replay_safety_class: ToolReplaySafetyClass,
    input_json: &[u8],
) -> ResolvedToolExecutionSemantics {
    let resolution = builtin_tool_effect(tool_name, input_json)
        .unwrap_or_else(|| fallback_effect(replay_safety_class));
    let semantics = semantics_for_effect(tool_name, resolution.effect);
    ResolvedToolExecutionSemantics {
        semantics,
        external_idempotency_key_sha256: resolution.external_idempotency_key_sha256,
    }
}

/// Projects a tool's default input posture for callers that do not have a
/// normalized payload. Runtime execution must use
/// [`resolve_tool_execution_semantics`] so mixed-operation tools are precise.
#[cfg(test)]
#[must_use]
pub(crate) fn tool_execution_semantics(
    tool_name: &str,
    replay_safety_class: ToolReplaySafetyClass,
) -> ToolExecutionSemantics {
    resolve_tool_execution_semantics(tool_name, replay_safety_class, b"{}").semantics
}

#[cfg(test)]
pub(super) fn builtin_has_explicit_execution_semantics(tool_name: &str) -> bool {
    builtin_tool_effect(tool_name, b"{}").is_some()
}

fn fallback_effect(replay_safety_class: ToolReplaySafetyClass) -> BuiltinToolEffectResolution {
    match replay_safety_class {
        ToolReplaySafetyClass::ReadOnly => BuiltinToolEffectResolution::read_only(),
        ToolReplaySafetyClass::IdempotentWrite => {
            BuiltinToolEffectResolution::deterministic_idempotent()
        }
        ToolReplaySafetyClass::NonIdempotentWrite
        | ToolReplaySafetyClass::ExternalSideEffect
        | ToolReplaySafetyClass::RequiresHumanConfirmation => {
            BuiltinToolEffectResolution::mutation(ToolMutationClass::OperatorConfirmation)
        }
    }
}

fn semantics_for_effect(tool_name: &str, effect: BuiltinToolEffect) -> ToolExecutionSemantics {
    let (idempotency_class, restart_policy, reconciliation_strategy, key_required) = match effect {
        BuiltinToolEffect::ReadOnly => (
            RuntimeIdempotencyClass::ReadOnly,
            SideEffectRestartPolicy::SafeRetry,
            ReconciliationStrategy::None,
            false,
        ),
        BuiltinToolEffect::DeterministicIdempotent => (
            RuntimeIdempotencyClass::DeterministicIdempotent,
            SideEffectRestartPolicy::SafeRetry,
            ReconciliationStrategy::None,
            false,
        ),
        BuiltinToolEffect::Mutation(mutation_class) => mutation_semantics(mutation_class),
    };
    ToolExecutionSemantics {
        schema_version: 1,
        tool_name: tool_name.to_owned(),
        idempotency_class,
        restart_policy,
        reconciliation_strategy,
        external_idempotency_key_required: key_required,
    }
}

const fn mutation_semantics(
    mutation_class: ToolMutationClass,
) -> (RuntimeIdempotencyClass, SideEffectRestartPolicy, ReconciliationStrategy, bool) {
    match mutation_class {
        ToolMutationClass::WorkspaceFilePatch => (
            RuntimeIdempotencyClass::ReconciliableMutation,
            SideEffectRestartPolicy::ReconcileBeforeRetry,
            ReconciliationStrategy::WorkspaceDigest,
            false,
        ),
        ToolMutationClass::ProcessMutation => (
            RuntimeIdempotencyClass::ReconciliableMutation,
            SideEffectRestartPolicy::ReconcileBeforeRetry,
            ReconciliationStrategy::ProcessProvenance,
            false,
        ),
        ToolMutationClass::HttpMutationWithExternalIdempotencyKey => (
            RuntimeIdempotencyClass::ExternalIdempotencyKey,
            SideEffectRestartPolicy::ReconcileBeforeRetry,
            ReconciliationStrategy::ExternalIdempotencyReceipt,
            true,
        ),
        ToolMutationClass::HttpMutationWithoutExternalIdempotencyKey
        | ToolMutationClass::OperatorConfirmation => (
            RuntimeIdempotencyClass::NonIdempotent,
            SideEffectRestartPolicy::RequireConfirmation,
            ReconciliationStrategy::None,
            false,
        ),
        ToolMutationClass::Delivery => (
            RuntimeIdempotencyClass::ReconciliableMutation,
            SideEffectRestartPolicy::ReconcileBeforeRetry,
            ReconciliationStrategy::DeliveryAcknowledgement,
            false,
        ),
        ToolMutationClass::WorkerTask => (
            RuntimeIdempotencyClass::ReconciliableMutation,
            SideEffectRestartPolicy::ReconcileBeforeRetry,
            ReconciliationStrategy::WorkerLeaseReceipt,
            false,
        ),
    }
}

fn builtin_tool_effect(tool_name: &str, input_json: &[u8]) -> Option<BuiltinToolEffectResolution> {
    let resolution = match tool_name {
        "palyra.echo"
        | "palyra.tools.search"
        | "palyra.tools.describe"
        | "palyra.mcp.resources.list"
        | "palyra.mcp.resources.read"
        | "palyra.mcp.prompts.list"
        | "palyra.mcp.prompts.get"
        | "palyra.memory.status"
        | "palyra.memory.search"
        | "palyra.memory.recall"
        | "palyra.memory.session_search"
        | "palyra.session_search"
        | "palyra.memory.reflect"
        | "palyra.routines.query"
        | "palyra.artifact.read"
        | "palyra.image.observe"
        | "palyra.fs.read_file"
        | "palyra.fs.list_dir"
        | "palyra.fs.search"
        | "palyra.code.health"
        | "palyra.code.diagnostics"
        | "palyra.code.symbols"
        | "palyra.code.definition"
        | "palyra.code.references"
        | "palyra.code.hover"
        | "palyra.code.workspace_symbols"
        | "palyra.code.outline"
        | "palyra.delegation.query"
        | "sessions_yield"
        | "palyra.process.status"
        | "palyra.process.list"
        | "palyra.browser.wait_for"
        | "palyra.browser.title"
        | "palyra.browser.observe"
        | "palyra.browser.vision"
        | "palyra.browser.images.list"
        | "palyra.browser.cdp.invoke"
        | "palyra.browser.storage"
        | "palyra.browser.network_log"
        | "palyra.browser.console_log"
        | "palyra.browser.tabs.list"
        | "palyra.browser.permissions.get"
        | "palyra.browser.downloads.list" => BuiltinToolEffectResolution::read_only(),
        "palyra.sleep" => BuiltinToolEffectResolution::deterministic_idempotent(),
        "palyra.fs.apply_patch" => workspace_patch_effect(input_json),
        "palyra.process.run"
        | "palyra.exec.run"
        | "palyra.process.input"
        | "palyra.process.send_keys"
        | "palyra.process.stop" => {
            BuiltinToolEffectResolution::mutation(ToolMutationClass::ProcessMutation)
        }
        "palyra.http.fetch" => http_fetch_effect(input_json),
        "palyra.clarify.ask" => BuiltinToolEffectResolution::mutation(ToolMutationClass::Delivery),
        "palyra.delegation.control" | "sessions_spawn" => {
            BuiltinToolEffectResolution::mutation(ToolMutationClass::WorkerTask)
        }
        "palyra.plan.manage" => plan_manage_effect(input_json),
        "palyra.fs.os_file" => os_file_effect(input_json),
        "palyra.browser.screenshot" | "palyra.browser.pdf" | "palyra.browser.downloads.get" => {
            browser_output_effect(input_json)
        }
        "palyra.browser.dialog" => browser_dialog_effect(input_json),
        "palyra.tools.invoke"
        | "palyra.memory.retain"
        | "palyra.retain"
        | "palyra.memory.delete"
        | "palyra.memory.replace"
        | "palyra.routines.control"
        | "palyra.tool_program.run"
        | "palyra.plugin.run"
        | "palyra.browser.session.create"
        | "palyra.browser.session.close"
        | "palyra.browser.navigate"
        | "palyra.browser.reload"
        | "palyra.browser.click"
        | "palyra.browser.type"
        | "palyra.browser.fill"
        | "palyra.browser.upload"
        | "palyra.browser.press"
        | "palyra.browser.select"
        | "palyra.browser.viewport"
        | "palyra.browser.highlight"
        | "palyra.browser.scroll"
        | "palyra.browser.reset_state"
        | "palyra.browser.tabs.open"
        | "palyra.browser.tabs.switch"
        | "palyra.browser.tabs.close"
        | "palyra.browser.permissions.set" => {
            BuiltinToolEffectResolution::mutation(ToolMutationClass::OperatorConfirmation)
        }
        _ => return None,
    };
    Some(resolution)
}

fn workspace_patch_effect(input_json: &[u8]) -> BuiltinToolEffectResolution {
    if input_object(input_json)
        .and_then(|payload| payload.get("dry_run").and_then(Value::as_bool))
        .unwrap_or(false)
    {
        BuiltinToolEffectResolution::read_only()
    } else {
        BuiltinToolEffectResolution::mutation(ToolMutationClass::WorkspaceFilePatch)
    }
}

fn http_fetch_effect(input_json: &[u8]) -> BuiltinToolEffectResolution {
    let Some(payload) = input_object(input_json) else {
        return BuiltinToolEffectResolution::mutation(
            ToolMutationClass::HttpMutationWithoutExternalIdempotencyKey,
        );
    };
    let method =
        payload.get("method").and_then(Value::as_str).unwrap_or("GET").trim().to_ascii_uppercase();
    match method.as_str() {
        "GET" | "HEAD" => BuiltinToolEffectResolution::read_only(),
        "POST" => match external_idempotency_key_sha256(&payload) {
            Some(key_sha256) => BuiltinToolEffectResolution::keyed_http_mutation(key_sha256),
            None => BuiltinToolEffectResolution::mutation(
                ToolMutationClass::HttpMutationWithoutExternalIdempotencyKey,
            ),
        },
        _ => BuiltinToolEffectResolution::mutation(
            ToolMutationClass::HttpMutationWithoutExternalIdempotencyKey,
        ),
    }
}

fn external_idempotency_key_sha256(payload: &Map<String, Value>) -> Option<String> {
    let headers = payload.get("headers")?.as_object()?;
    let mut matching_headers =
        headers.iter().filter(|(name, _)| name.eq_ignore_ascii_case("idempotency-key"));
    let (_, value) = matching_headers.next()?;
    if matching_headers.next().is_some() {
        return None;
    }
    let raw_key = value.as_str()?;
    if raw_key.trim().is_empty() {
        return None;
    }
    Some(crate::sha256_hex(raw_key.as_bytes()))
}

fn plan_manage_effect(input_json: &[u8]) -> BuiltinToolEffectResolution {
    if input_operation(input_json).as_deref() == Some("read") {
        BuiltinToolEffectResolution::read_only()
    } else {
        BuiltinToolEffectResolution::mutation(ToolMutationClass::OperatorConfirmation)
    }
}

fn os_file_effect(input_json: &[u8]) -> BuiltinToolEffectResolution {
    let Some(payload) = input_object(input_json) else {
        return BuiltinToolEffectResolution::mutation(ToolMutationClass::OperatorConfirmation);
    };
    if payload.get("dry_run").and_then(Value::as_bool).unwrap_or(false) {
        return BuiltinToolEffectResolution::read_only();
    }
    match payload.get("operation").and_then(Value::as_str) {
        Some("stat" | "read" | "list_dir" | "search") => BuiltinToolEffectResolution::read_only(),
        _ => BuiltinToolEffectResolution::mutation(ToolMutationClass::OperatorConfirmation),
    }
}

fn browser_output_effect(input_json: &[u8]) -> BuiltinToolEffectResolution {
    match input_object(input_json) {
        Some(payload) if !payload.contains_key("output_path") => {
            BuiltinToolEffectResolution::read_only()
        }
        _ => BuiltinToolEffectResolution::mutation(ToolMutationClass::OperatorConfirmation),
    }
}

fn browser_dialog_effect(input_json: &[u8]) -> BuiltinToolEffectResolution {
    let action = input_object(input_json)
        .and_then(|payload| payload.get("action").cloned())
        .and_then(|value| value.as_str().map(str::to_owned));
    if action.as_deref().unwrap_or("inspect") == "inspect" {
        BuiltinToolEffectResolution::read_only()
    } else {
        BuiltinToolEffectResolution::mutation(ToolMutationClass::OperatorConfirmation)
    }
}

fn input_operation(input_json: &[u8]) -> Option<String> {
    input_object(input_json)?.get("operation").and_then(Value::as_str).map(str::to_owned)
}

fn input_object(input_json: &[u8]) -> Option<Map<String, Value>> {
    serde_json::from_slice::<Value>(input_json).ok()?.as_object().cloned()
}
