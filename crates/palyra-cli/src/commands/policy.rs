//! Offline policy explanation against the deny-by-default Cedar engine.
//!
//! Evaluates locally with the same tool allowlist the daemon would load
//! (env override first, then config), and overlays the runtime tool
//! catalog's approval requirements so output mirrors live enforcement.

use crate::*;
use palyra_common::tool_catalog::{
    sensitive_allowlisted_tool_names, tool_policy_capability_names, tool_requires_approval,
    SENSITIVE_CAPABILITY_POLICY_NAMES,
};

/// Runs `palyra policy explain`, emitting the decision in JSON, NDJSON, or
/// the pinned text form.
///
/// # Errors
/// Fails when policy config loading, Cedar evaluation, or output encoding
/// fails.
pub(crate) fn run_policy(command: PolicyCommand) -> Result<()> {
    match command {
        PolicyCommand::Explain { principal, action, resource, json } => {
            let request = PolicyRequest { principal, action, resource };
            let policy_context = load_policy_explain_context(&request)?;
            let evaluation = palyra_policy::evaluate_with_context(
                &request,
                &policy_context.request_context,
                &policy_context.config,
            )
            .context("failed to evaluate policy with Cedar engine")?;
            let matched_policies = if evaluation.explanation.matched_policy_ids.is_empty() {
                "none".to_owned()
            } else {
                evaluation.explanation.matched_policy_ids.join(",")
            };
            let (decision, approval_required, reason) = match &evaluation.decision {
                PolicyDecision::Allow => ("allow", false, evaluation.explanation.reason.as_str()),
                PolicyDecision::DenyByDefault { reason } => {
                    ("deny_by_default", true, reason.as_str())
                }
            };
            let diagnostics =
                palyra_policy::policy_explain_diagnostics_value(&request, &evaluation);
            let runtime_tool_approval_required =
                policy_context.requested_tool.as_deref().is_some_and(tool_requires_approval);
            if output::preferred_json(json) {
                return output::print_json_pretty(
                    &json!({
                        "decision": decision,
                        "principal": request.principal,
                        "action": request.action,
                        "resource": request.resource,
                        "approval_required": approval_required,
                        "reason": reason,
                        "matched_policies": evaluation.explanation.matched_policy_ids,
                        "diagnostics": diagnostics,
                        "runtime_approval_overlay": {
                            "applied": policy_context.requested_tool.is_some(),
                            "requested_tool": policy_context.requested_tool,
                            "approval_required": runtime_tool_approval_required,
                            "reason": if runtime_tool_approval_required {
                                Some("runtime tool catalog marks this tool as sensitive or approval-gated")
                            } else {
                                None
                            },
                        },
                        "policy_config": {
                            "source": policy_context.config_source,
                            "allowlisted_tools": policy_context.config.allowlisted_tools,
                            "sensitive_tool_names": policy_context.config.sensitive_tool_names,
                            "sensitive_capability_names": policy_context.config.sensitive_capability_names,
                        },
                        "explanation": {
                            "evaluated_with_cedar": evaluation.explanation.evaluated_with_cedar,
                            "diagnostics_errors": evaluation.explanation.diagnostics_errors,
                            "is_sensitive_action": evaluation.explanation.is_sensitive_action,
                            "is_allowlisted_tool": evaluation.explanation.is_allowlisted_tool,
                            "is_allowlisted_skill": evaluation.explanation.is_allowlisted_skill,
                            "is_tool_execute_principal_allowed": evaluation
                                .explanation
                                .is_tool_execute_principal_allowed,
                            "is_tool_execute_channel_allowed": evaluation
                                .explanation
                                .is_tool_execute_channel_allowed,
                            "requested_tool": evaluation.explanation.requested_tool,
                            "requested_skill": evaluation.explanation.requested_skill,
                            "request_capabilities": evaluation.explanation.request_capabilities,
                            "constructed_entities": evaluation.explanation.constructed_entities,
                        },
                    }),
                    "failed to encode policy explain output as JSON",
                );
            }
            if output::preferred_ndjson(json, false) {
                output::print_json_line(
                    &json!({
                        "decision": decision,
                        "principal": request.principal,
                        "action": request.action,
                        "resource": request.resource,
                        "approval_required": approval_required,
                        "reason": reason,
                        "matched_policies": evaluation.explanation.matched_policy_ids,
                        "policy_config_source": policy_context.config_source,
                        "runtime_approval_required": runtime_tool_approval_required,
                    }),
                    "failed to encode policy explain output as NDJSON",
                )?;
                return std::io::stdout().flush().context("stdout flush failed");
            }
            match evaluation.decision {
                PolicyDecision::Allow => {
                    println!(
                        "decision=allow principal={} action={} resource={} approval_required={} runtime_approval_required={} reason={} matched_policies={} policy_config_source={}",
                        request.principal,
                        request.action,
                        request.resource,
                        approval_required,
                        runtime_tool_approval_required,
                        evaluation.explanation.reason,
                        matched_policies,
                        policy_context.config_source,
                    );
                }
                PolicyDecision::DenyByDefault { reason } => {
                    println!(
                        "decision=deny_by_default principal={} action={} resource={} approval_required=true reason={} matched_policies={} policy_config_source={}",
                        request.principal,
                        request.action,
                        request.resource,
                        reason,
                        matched_policies,
                        policy_context.config_source,
                    );
                }
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
    }
}

/// Evaluation inputs assembled for one explain request, with the provenance
/// of the tool allowlist for the output's `policy_config_source` field.
struct PolicyExplainContext {
    config: PolicyEvaluationConfig,
    request_context: palyra_policy::PolicyRequestContext,
    config_source: String,
    requested_tool: Option<String>,
}

fn load_policy_explain_context(request: &PolicyRequest) -> Result<PolicyExplainContext> {
    let (allowlisted_tools, config_source) = load_policy_allowlisted_tools()?;
    let requested_tool = requested_tool_for_policy_explain(request);
    Ok(PolicyExplainContext {
        config: PolicyEvaluationConfig {
            sensitive_tool_names: sensitive_allowlisted_tool_names(allowlisted_tools.as_slice()),
            sensitive_capability_names: SENSITIVE_CAPABILITY_POLICY_NAMES
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            allowlisted_tools,
            ..PolicyEvaluationConfig::default()
        },
        request_context: palyra_policy::PolicyRequestContext {
            tool_name: requested_tool.clone(),
            capabilities: requested_tool
                .as_deref()
                .map(tool_policy_capability_names)
                .unwrap_or_default(),
            ..palyra_policy::PolicyRequestContext::default()
        },
        config_source,
        requested_tool,
    })
}

// Extracts the lowercased tool name from a tool.execute resource, accepting
// both bare names and the "tool:" prefixed form.
fn requested_tool_for_policy_explain(request: &PolicyRequest) -> Option<String> {
    if !request.action.eq_ignore_ascii_case("tool.execute") {
        return None;
    }
    let trimmed = request.resource.trim();
    if trimmed.is_empty() {
        return None;
    }
    let tool_name = trimmed.strip_prefix("tool:").unwrap_or(trimmed).trim();
    if tool_name.is_empty() {
        None
    } else {
        Some(tool_name.to_ascii_lowercase())
    }
}

fn load_policy_allowlisted_tools() -> Result<(Vec<String>, String)> {
    if let Ok(raw) = env::var("PALYRA_TOOL_CALL_ALLOWED_TOOLS") {
        return Ok((
            parse_policy_tool_allowlist(raw.as_str()),
            "env:PALYRA_TOOL_CALL_ALLOWED_TOOLS".to_owned(),
        ));
    }

    let Some(config_path) = resolve_policy_config_path()? else {
        return Ok((Vec::new(), "default:empty".to_owned()));
    };
    let raw = fs::read_to_string(config_path.as_path())
        .with_context(|| format!("failed to read policy config {}", config_path.display()))?;
    let document = toml::from_str::<toml::Value>(raw.as_str())
        .with_context(|| format!("failed to parse policy config {}", config_path.display()))?;
    Ok((
        read_policy_tool_allowlist_from_document(&document),
        format!("config:{}", config_path.display()),
    ))
}

fn resolve_policy_config_path() -> Result<Option<PathBuf>> {
    if let Some(path) = app::current_root_context()
        .as_ref()
        .and_then(|context| context.config_path().map(Path::to_path_buf))
        .filter(|path| path.exists())
    {
        return Ok(Some(path));
    }
    if let Ok(raw) = env::var("PALYRA_CONFIG") {
        if let Some(raw) = normalize_optional_text(raw.as_str()) {
            let path =
                parse_config_path(raw).with_context(|| "PALYRA_CONFIG contains an invalid path")?;
            return Ok(path.exists().then_some(path));
        }
    }
    Ok(default_config_search_paths().into_iter().find(|candidate| candidate.exists()))
}

fn read_policy_tool_allowlist_from_document(document: &toml::Value) -> Vec<String> {
    let Some(value) =
        document.get("tool_call").and_then(|tool_call| tool_call.get("allowed_tools"))
    else {
        return Vec::new();
    };
    value
        .as_array()
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str())
                .flat_map(parse_policy_tool_allowlist)
                .collect()
        })
        .unwrap_or_default()
}

fn parse_policy_tool_allowlist(raw: &str) -> Vec<String> {
    raw.split(',').map(str::trim).filter(|value| !value.is_empty()).map(ToOwned::to_owned).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_explain_marks_process_runner_as_approval_required() -> Result<()> {
        let request = PolicyRequest {
            principal: "admin:local".to_owned(),
            action: "tool.execute".to_owned(),
            resource: "tool:palyra.process.run".to_owned(),
        };
        let config = PolicyEvaluationConfig {
            allowlisted_tools: vec!["palyra.process.run".to_owned()],
            sensitive_tool_names: sensitive_allowlisted_tool_names(&[
                "palyra.process.run".to_owned()
            ]),
            sensitive_capability_names: SENSITIVE_CAPABILITY_POLICY_NAMES
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            ..PolicyEvaluationConfig::default()
        };
        let context = palyra_policy::PolicyRequestContext {
            tool_name: requested_tool_for_policy_explain(&request),
            capabilities: tool_policy_capability_names("palyra.process.run"),
            ..palyra_policy::PolicyRequestContext::default()
        };

        let evaluation = palyra_policy::evaluate_with_context(&request, &context, &config)?;

        assert!(matches!(evaluation.decision, PolicyDecision::DenyByDefault { .. }));
        assert!(evaluation.explanation.is_sensitive_action);
        assert_eq!(evaluation.explanation.requested_tool.as_deref(), Some("palyra.process.run"));
        Ok(())
    }

    #[test]
    fn policy_explain_keeps_echo_allow_without_approval() -> Result<()> {
        let request = PolicyRequest {
            principal: "admin:local".to_owned(),
            action: "tool.execute".to_owned(),
            resource: "tool:palyra.echo".to_owned(),
        };
        let config = PolicyEvaluationConfig {
            allowlisted_tools: vec!["palyra.echo".to_owned()],
            sensitive_tool_names: sensitive_allowlisted_tool_names(&["palyra.echo".to_owned()]),
            sensitive_capability_names: SENSITIVE_CAPABILITY_POLICY_NAMES
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            ..PolicyEvaluationConfig::default()
        };
        let context = palyra_policy::PolicyRequestContext {
            tool_name: requested_tool_for_policy_explain(&request),
            capabilities: tool_policy_capability_names("palyra.echo"),
            ..palyra_policy::PolicyRequestContext::default()
        };

        let evaluation = palyra_policy::evaluate_with_context(&request, &context, &config)?;

        assert_eq!(evaluation.decision, PolicyDecision::Allow);
        assert!(!evaluation.explanation.is_sensitive_action);
        Ok(())
    }
}
