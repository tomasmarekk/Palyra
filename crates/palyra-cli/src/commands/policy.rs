use crate::*;

pub(crate) fn run_policy(command: PolicyCommand) -> Result<()> {
    match command {
        PolicyCommand::Explain { principal, action, resource } => {
            let request = PolicyRequest { principal, action, resource };
            let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
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
            if output::preferred_json(false) {
                return output::print_json_pretty(
                    &json!({
                        "decision": decision,
                        "principal": request.principal,
                        "action": request.action,
                        "resource": request.resource,
                        "approval_required": approval_required,
                        "reason": reason,
                        "matched_policies": evaluation.explanation.matched_policy_ids,
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
            if output::preferred_ndjson(false, false) {
                output::print_json_line(
                    &json!({
                        "decision": decision,
                        "principal": request.principal,
                        "action": request.action,
                        "resource": request.resource,
                        "approval_required": approval_required,
                        "reason": reason,
                        "matched_policies": evaluation.explanation.matched_policy_ids,
                    }),
                    "failed to encode policy explain output as NDJSON",
                )?;
                return std::io::stdout().flush().context("stdout flush failed");
            }
            match evaluation.decision {
                PolicyDecision::Allow => {
                    println!(
                        "decision=allow principal={} action={} resource={} reason={} matched_policies={}",
                        request.principal,
                        request.action,
                        request.resource,
                        evaluation.explanation.reason,
                        matched_policies,
                    );
                }
                PolicyDecision::DenyByDefault { reason } => {
                    println!(
                        "decision=deny_by_default principal={} action={} resource={} approval_required=true reason={} matched_policies={}",
                        request.principal,
                        request.action,
                        request.resource,
                        reason,
                        matched_policies,
                    );
                }
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
    }
}
