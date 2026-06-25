//! Deny-by-default authorization gates for sensitive gRPC service surfaces.
//!
//! Wraps `palyra-policy` evaluation and maps decisions to `tonic::Status`
//! errors. Sensitive surfaces (agent management, auth profiles, approvals)
//! additionally require an admin/system principal prefix regardless of the
//! policy outcome. Called from the gateway gRPC handlers before any work.

use palyra_policy::{
    evaluate_with_config, evaluate_with_context, PolicyDecision, PolicyEvaluationConfig,
    PolicyRequest, PolicyRequestContext,
};
use tonic::Status;

/// Principal-prefix requirement applied on top of policy evaluation for
/// sensitive service surfaces.
#[derive(Clone, Copy)]
pub(crate) enum SensitiveServiceRole {
    AdminOnly,
    AdminOrSystem,
}

/// Returns whether `principal` carries the prefix required by `role`.
///
/// Matching is case-insensitive on the prefix only; the remainder of the
/// principal is not validated here.
#[must_use]
pub(crate) fn principal_has_sensitive_service_role(
    principal: &str,
    role: SensitiveServiceRole,
) -> bool {
    let normalized_principal = principal.to_ascii_lowercase();
    match role {
        SensitiveServiceRole::AdminOnly => normalized_principal.starts_with("admin:"),
        SensitiveServiceRole::AdminOrSystem => {
            normalized_principal.starts_with("admin:")
                || normalized_principal.starts_with("system:")
        }
    }
}

/// Authorizes a cron service action through the default policy.
///
/// # Errors
/// Returns `Status::permission_denied` when policy denies the action and
/// `Status::internal` when policy evaluation itself fails.
#[allow(clippy::result_large_err)]
pub(crate) fn authorize_cron_action(
    principal: &str,
    action: &str,
    resource: &str,
) -> Result<(), Status> {
    authorize_policy_action(principal, action, resource, "cron")
}

/// Authorizes message routing with the channel forwarded as policy context.
///
/// The session and run identifiers are accepted for call-site symmetry but
/// are not part of the policy request today.
///
/// # Errors
/// Returns `Status::permission_denied` when policy denies the action and
/// `Status::internal` when policy evaluation itself fails.
#[allow(clippy::result_large_err)]
pub(crate) fn authorize_message_action(
    principal: &str,
    action: &str,
    resource: &str,
    channel: Option<&str>,
    _session_id: Option<&str>,
    _run_id: Option<&str>,
) -> Result<(), Status> {
    let evaluation = evaluate_with_context(
        &PolicyRequest {
            principal: principal.to_owned(),
            action: action.to_owned(),
            resource: resource.to_owned(),
        },
        &PolicyRequestContext {
            channel: channel.map(str::to_owned),
            ..PolicyRequestContext::default()
        },
        &PolicyEvaluationConfig::default(),
    )
    .map_err(|error| {
        Status::internal(format!("failed to evaluate message routing policy: {error}"))
    })?;
    map_policy_decision(action, resource, evaluation.decision)
}

/// Authorizes a memory service action through the default policy.
///
/// # Errors
/// Returns `Status::permission_denied` when policy denies the action and
/// `Status::internal` when policy evaluation itself fails.
#[allow(clippy::result_large_err)]
pub(crate) fn authorize_memory_action(
    principal: &str,
    action: &str,
    resource: &str,
) -> Result<(), Status> {
    authorize_policy_action(principal, action, resource, "memory")
}

/// Authorizes destructive memory purge operations.
///
/// # Errors
/// Returns `Status::permission_denied` when the principal lacks an
/// admin/system prefix or sensitive-action approval, and `Status::internal`
/// when policy evaluation fails.
#[allow(clippy::result_large_err)]
pub(crate) fn authorize_memory_purge_action(
    principal: &str,
    action: &str,
    resource: &str,
    user_confirmed: bool,
) -> Result<(), Status> {
    if !principal_has_sensitive_service_role(principal, SensitiveServiceRole::AdminOrSystem) {
        return Err(Status::permission_denied(format!(
            "policy denied action '{action}' on '{resource}': memory purge requires admin/system principal prefix"
        )));
    }
    let evaluation = evaluate_with_config(
        &PolicyRequest {
            principal: principal.to_owned(),
            action: action.to_owned(),
            resource: resource.to_owned(),
        },
        &PolicyEvaluationConfig {
            allow_sensitive_tools: user_confirmed,
            ..PolicyEvaluationConfig::default()
        },
    )
    .map_err(|error| {
        Status::internal(format!("failed to evaluate memory purge policy: {error}"))
    })?;
    map_policy_decision(action, resource, evaluation.decision)
}

/// Authorizes a vault service action through the default policy.
///
/// # Errors
/// Returns `Status::permission_denied` when policy denies the action and
/// `Status::internal` when policy evaluation itself fails.
#[allow(clippy::result_large_err)]
pub(crate) fn authorize_vault_action(
    principal: &str,
    action: &str,
    resource: &str,
) -> Result<(), Status> {
    authorize_policy_action(principal, action, resource, "vault")
}

/// Authorizes agent management; requires an `admin:` principal prefix.
///
/// # Errors
/// Returns `Status::permission_denied` when the principal lacks the required
/// role prefix and `Status::internal` when policy evaluation fails.
#[allow(clippy::result_large_err)]
pub(crate) fn authorize_agent_management_action(
    principal: &str,
    action: &str,
    resource: &str,
) -> Result<(), Status> {
    authorize_sensitive_service_action(
        principal,
        action,
        resource,
        "agent",
        SensitiveServiceRole::AdminOnly,
        "agent management requires admin principal prefix 'admin:'",
    )
}

/// Authorizes auth profile management; requires an admin/system principal.
///
/// # Errors
/// Returns `Status::permission_denied` when the principal lacks the required
/// role prefix and `Status::internal` when policy evaluation fails.
#[allow(clippy::result_large_err)]
pub(crate) fn authorize_auth_profile_action(
    principal: &str,
    action: &str,
    resource: &str,
) -> Result<(), Status> {
    authorize_sensitive_service_action(
        principal,
        action,
        resource,
        "auth profile",
        SensitiveServiceRole::AdminOrSystem,
        "auth profile management requires admin/system principal prefix",
    )
}

/// Authorizes approvals APIs; requires an admin/system principal.
///
/// # Errors
/// Returns `Status::permission_denied` when the principal lacks the required
/// role prefix and `Status::internal` when policy evaluation fails.
#[allow(clippy::result_large_err)]
pub(crate) fn authorize_approvals_action(
    principal: &str,
    action: &str,
    resource: &str,
) -> Result<(), Status> {
    authorize_sensitive_service_action(
        principal,
        action,
        resource,
        "approvals",
        SensitiveServiceRole::AdminOrSystem,
        "approvals APIs require admin/system principal prefix",
    )
}

#[allow(clippy::result_large_err)]
fn authorize_policy_action(
    principal: &str,
    action: &str,
    resource: &str,
    surface: &str,
) -> Result<(), Status> {
    let evaluation = evaluate_with_config(
        &PolicyRequest {
            principal: principal.to_owned(),
            action: action.to_owned(),
            resource: resource.to_owned(),
        },
        &PolicyEvaluationConfig::default(),
    )
    .map_err(|error| Status::internal(format!("failed to evaluate {surface} policy: {error}")))?;
    map_policy_decision(action, resource, evaluation.decision)
}

#[allow(clippy::result_large_err)]
fn authorize_sensitive_service_action(
    principal: &str,
    action: &str,
    resource: &str,
    surface: &str,
    role: SensitiveServiceRole,
    allow_reason: &str,
) -> Result<(), Status> {
    let evaluation = evaluate_with_config(
        &PolicyRequest {
            principal: principal.to_owned(),
            action: action.to_owned(),
            resource: resource.to_owned(),
        },
        &PolicyEvaluationConfig::default(),
    )
    .map_err(|error| Status::internal(format!("failed to evaluate {surface} policy: {error}")))?;
    if principal_has_sensitive_service_role(principal, role) {
        return Ok(());
    }
    // The role prefix is a hard requirement: even a policy Allow is rejected
    // for principals without it, and the surface-specific allow_reason
    // explains the missing prefix instead of the policy's allow rationale.
    let reason = match evaluation.decision {
        PolicyDecision::Allow => allow_reason.to_owned(),
        PolicyDecision::DenyByDefault { reason } => reason,
    };
    Err(Status::permission_denied(format!(
        "policy denied action '{action}' on '{resource}': {reason}"
    )))
}

#[allow(clippy::result_large_err)]
fn map_policy_decision(
    action: &str,
    resource: &str,
    decision: PolicyDecision,
) -> Result<(), Status> {
    match decision {
        PolicyDecision::Allow => Ok(()),
        PolicyDecision::DenyByDefault { reason } => Err(Status::permission_denied(format!(
            "policy denied action '{action}' on '{resource}': {reason}"
        ))),
    }
}
