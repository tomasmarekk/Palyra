//! Vault access over RPC: error mapping, scope enforcement, and the layered
//! approval gate every secret read passes before bytes leave the daemon.
//! Also hosts the approval/memory cache-key builders shared with tool flow.

use super::*;
use crate::application::service_authorization::authorize_vault_action;

/// Maps a vault backend error to a gRPC status. `NotFound` deliberately
/// carries no scope/key detail so the error cannot confirm which secrets
/// exist; IO failures are tagged with `operation` for diagnostics.
pub(crate) fn map_vault_error(operation: &str, error: VaultError) -> Status {
    match error {
        VaultError::NotFound => Status::not_found("secret not found"),
        VaultError::InvalidScope(message)
        | VaultError::InvalidKey(message)
        | VaultError::InvalidObjectId(message)
        | VaultError::Crypto(message) => Status::invalid_argument(message),
        VaultError::ValueTooLarge { actual, max } => {
            Status::invalid_argument(format!("secret value exceeds limit ({actual} > {max})"))
        }
        VaultError::BackendUnavailable(message) => Status::failed_precondition(message),
        VaultError::Io(message) => Status::internal(format!("{operation} failed: {message}")),
    }
}

/// Parses a vault scope literal (for example `global` or
/// `principal:<id>`).
///
/// # Errors
/// Returns `Status::invalid_argument` when the literal is not a valid scope.
#[allow(clippy::result_large_err)]
pub(crate) fn parse_vault_scope(raw: &str) -> Result<VaultScope, Status> {
    raw.parse::<VaultScope>()
        .map_err(|error| Status::invalid_argument(format!("invalid vault scope: {error}")))
}

/// Enforces that the authenticated request context may touch the given vault
/// scope. This is the identity gate for every external vault RPC:
///
/// - `Global` is open to any authenticated caller (further restricted by the
///   approval policy and service authorization downstream).
/// - `Principal` must match the caller's own principal exactly.
/// - `Channel` requires the caller's channel context to equal
///   `<channel_name>:<account_id>`.
/// - `Skill` is never reachable over external RPC; skill-scoped secrets are
///   resolved only by the internal skill runtime.
///
/// # Errors
/// Returns `Status::permission_denied` for any scope/context mismatch.
#[allow(clippy::result_large_err)]
pub(crate) fn enforce_vault_scope_access(
    scope: &VaultScope,
    context: &RequestContext,
) -> Result<(), Status> {
    match scope {
        VaultScope::Global => Ok(()),
        VaultScope::Principal { principal_id } => {
            if principal_id == &context.principal {
                Ok(())
            } else {
                Err(Status::permission_denied(
                    "vault principal scope must match authenticated principal context",
                ))
            }
        }
        VaultScope::Channel { channel_name, account_id } => {
            let context_channel = context.channel.as_deref().ok_or_else(|| {
                Status::permission_denied(
                    "vault channel scope requires authenticated channel context",
                )
            })?;
            let expected_with_account = format!("{channel_name}:{account_id}");
            if context_channel == expected_with_account {
                Ok(())
            } else {
                Err(Status::permission_denied(
                    "vault channel scope must match authenticated channel context",
                ))
            }
        }
        VaultScope::Skill { .. } => Err(Status::permission_denied(
            "vault skill scope is not allowed over external RPC context",
        )),
    }
}

/// Converts vault secret metadata (never the value) to its proto form.
pub(crate) fn vault_secret_metadata_message(
    metadata: &VaultSecretMetadata,
) -> gateway_v1::VaultSecretMetadata {
    gateway_v1::VaultSecretMetadata {
        scope: metadata.scope.to_string(),
        key: metadata.key.clone(),
        created_at_unix_ms: metadata.created_at_unix_ms,
        updated_at_unix_ms: metadata.updated_at_unix_ms,
        // Intentional narrowing cast: secret values are capped at
        // MAX_VAULT_SECRET_BYTES (64 KiB), far below u32::MAX.
        value_bytes: metadata.value_bytes as u32,
    }
}

/// Builds the memory-search cache key from every request field that affects
/// results, including caller identity, so cached hits never leak across
/// principals, channels, or sessions. `serde_json` orders object keys
/// deterministically, making the rendered string a stable key.
pub(crate) fn memory_search_cache_key(request: &MemorySearchRequest) -> String {
    json!({
        "principal": request.principal,
        "channel": request.channel,
        "session_id": request.session_id,
        "query": request.query,
        "top_k": request.top_k,
        "min_score": request.min_score,
        "tags": request.tags,
        "sources": request.sources.iter().map(|source| source.as_str()).collect::<Vec<_>>(),
    })
    .to_string()
}

/// Builds the identity-scoped prefix for tool-approval cache keys. Every
/// caller dimension (principal, device, channel, session) is baked in so a
/// session-scoped approval decision can never be replayed by a different
/// identity; the trailing delimiter also lets callers invalidate a whole
/// session's entries by prefix match.
pub(crate) fn tool_approval_cache_key_prefix(context: &RequestContext, session_id: &str) -> String {
    format!(
        "principal={}|device_id={}|channel={}|session={}|",
        context.principal,
        context.device_id,
        context.channel.as_deref().unwrap_or_default(),
        session_id
    )
}

/// Full cache key for one approval subject under the caller's identity
/// prefix (see [`tool_approval_cache_key_prefix`]).
pub(crate) fn tool_approval_cache_key(
    context: &RequestContext,
    session_id: &str,
    subject_id: &str,
) -> String {
    format!("{}subject={subject_id}", tool_approval_cache_key_prefix(context, session_id))
}

/// Derives a tool approval outcome from a stored approval record;
/// `fallback_decision` stands in when the record has not been resolved yet.
/// Only an explicit `Allow` yields `approved == true` - timeouts and errors
/// stay denials.
pub(crate) fn tool_approval_outcome_from_record(
    record: &ApprovalRecord,
    fallback_decision: ApprovalDecision,
) -> ToolApprovalOutcome {
    let decision = record.decision.unwrap_or(fallback_decision);
    ToolApprovalOutcome {
        approval_id: record.approval_id.clone(),
        approved: matches!(decision, ApprovalDecision::Allow),
        reason: record.decision_reason.clone().unwrap_or_else(|| "approval resolved".to_owned()),
        decision,
        decision_scope: record.decision_scope.unwrap_or(ApprovalDecisionScope::Once),
        decision_scope_ttl_ms: record.decision_scope_ttl_ms,
    }
}

/// Rejects requests whose protocol major version differs from the daemon's
/// canonical major.
///
/// # Errors
/// Returns `Status::failed_precondition` on version mismatch.
#[allow(clippy::result_large_err)]
pub(crate) fn require_supported_version(v: u32) -> Result<(), Status> {
    if v != CANONICAL_PROTOCOL_MAJOR {
        return Err(Status::failed_precondition("unsupported protocol major version"));
    }
    Ok(())
}

// Canonical "scope/key" form used to match configured approval-required refs.
fn normalize_vault_ref_literal(scope: &VaultScope, key: &str) -> String {
    format!("{scope}/{key}").to_ascii_lowercase()
}

fn is_mcp_oauth_credential(scope: &VaultScope, key: &str) -> bool {
    if !matches!(scope, VaultScope::Global) {
        return false;
    }

    let normalized = key.to_ascii_lowercase();
    let Some((credential_prefix, suffix)) = normalized.rsplit_once('.') else {
        return false;
    };
    if !matches!(suffix, "access" | "refresh") {
        return false;
    }

    let Some((slug, digest)) =
        credential_prefix.strip_prefix("mcp.").and_then(|prefix| prefix.rsplit_once('.'))
    else {
        return false;
    };
    !slug.is_empty() && digest.len() == 12 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Whether reading this secret is configured to require an explicit
/// approval. MCP OAuth credentials are intrinsically gated so operators
/// cannot accidentally remove their protection by replacing the configured
/// ref list. Other configured refs are `scope/key` literals matched
/// case-insensitively against the request.
pub(crate) fn vault_get_requires_approval(
    scope: &VaultScope,
    key: &str,
    approval_required_refs: &[String],
) -> bool {
    if is_mcp_oauth_credential(scope, key) {
        return true;
    }
    if approval_required_refs.is_empty() {
        return false;
    }
    let candidate = normalize_vault_ref_literal(scope, key);
    approval_required_refs
        .iter()
        .any(|configured| configured.eq_ignore_ascii_case(candidate.as_str()))
}

/// Enforces the approval requirement for reading an approval-gated secret.
///
/// Secrets not listed in `approval_required_refs` pass through. For gated
/// ones the decision is delegated to the deny-by-default policy engine with
/// `vault.get` registered as a sensitive action and `approval_granted`
/// mapped onto `allow_sensitive_tools` - reusing the engine instead of an ad
/// hoc boolean check keeps the decision and its journal explanation
/// consistent with tool approvals.
///
/// # Errors
/// Returns `Status::permission_denied` (with the policy reason) when
/// approval is required but not granted, and `Status::internal` when policy
/// evaluation itself fails.
#[allow(clippy::result_large_err)]
pub(crate) fn enforce_vault_get_approval_policy(
    principal: &str,
    scope: &VaultScope,
    key: &str,
    approval_required_refs: &[String],
    approval_granted: bool,
) -> Result<(), Status> {
    if !vault_get_requires_approval(scope, key, approval_required_refs) {
        return Ok(());
    }
    let evaluation = evaluate_with_config(
        &PolicyRequest {
            principal: principal.to_owned(),
            action: "vault.get".to_owned(),
            resource: format!("secrets:{scope}:{key}"),
        },
        &PolicyEvaluationConfig {
            allow_sensitive_tools: approval_granted,
            sensitive_actions: vec!["vault.get".to_owned()],
            ..PolicyEvaluationConfig::default()
        },
    )
    .map_err(|error| {
        Status::internal(format!("failed to evaluate vault approval policy: {error}"))
    })?;
    match evaluation.decision {
        PolicyDecision::Allow => Ok(()),
        PolicyDecision::DenyByDefault { reason } => Err(Status::permission_denied(format!(
            "vault read requires explicit approval for {scope}/{key}: {reason}"
        ))),
    }
}

/// Reads a secret value for an authenticated request context, applying the
/// full gate stack in order: scope/identity match, approval policy for gated
/// refs, service authorization, and finally the vault fetch.
///
/// The `secret.accessed` journal event is mandatory: if recording it fails,
/// the error propagates and the caller never receives the value, so no
/// secret read can go unaudited.
///
/// # Errors
/// Returns `Status::permission_denied` from any gate, `Status::not_found`
/// for missing secrets, and storage/journal errors otherwise.
#[allow(clippy::result_large_err)]
pub(crate) async fn read_vault_secret_for_context(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    scope: VaultScope,
    key: String,
    approval_granted: bool,
) -> Result<Vec<u8>, Status> {
    enforce_vault_scope_access(&scope, context)?;
    enforce_vault_get_approval_policy(
        context.principal.as_str(),
        &scope,
        key.as_str(),
        runtime_state.config.vault_get_approval_required_refs.as_slice(),
        approval_granted,
    )?;
    authorize_vault_action(
        context.principal.as_str(),
        "vault.get",
        format!("secrets:{scope}:{key}").as_str(),
    )?;
    let value = runtime_state.vault_get_secret(scope.clone(), key.clone()).await?;
    record_vault_journal_event(
        runtime_state,
        context,
        "secret.accessed",
        "vault.get",
        &scope,
        Some(key.as_str()),
        Some(value.len()),
    )
    .await?;
    Ok(value)
}

/// Console-facing secret reveal: parses the scope/key literals and reads via
/// [`read_vault_secret_for_context`] with `approval_granted = true`, because
/// the console handler has already authenticated an operator session and
/// required an explicit reveal acknowledgment - that interaction *is* the
/// approval for approval-gated refs.
///
/// # Errors
/// Returns `Status::invalid_argument` for a bad scope literal, plus
/// everything [`read_vault_secret_for_context`] can return.
#[allow(clippy::result_large_err)]
pub(crate) async fn reveal_vault_secret_for_console(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    scope_literal: &str,
    key_literal: &str,
) -> Result<Vec<u8>, Status> {
    let scope = parse_vault_scope(scope_literal)?;
    let key = key_literal.trim().to_owned();
    read_vault_secret_for_context(runtime_state, context, scope, key, true).await
}
