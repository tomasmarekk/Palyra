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
use serde::Serialize;

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
        PolicyCommand::Conformance { baseline, json } => {
            let report = build_policy_posture_report(None, None)?;
            if let Some(path) = baseline {
                let diff = build_policy_diff_report(path.as_str(), &report)?;
                emit_policy_report(&diff, json)
            } else {
                emit_policy_report(&report, json)
            }
        }
        PolicyCommand::Diff { baseline, candidate, json } => {
            let baseline_value = read_policy_report_value(baseline.as_str())?;
            let candidate_value = read_policy_report_value(candidate.as_str())?;
            let report = diff_policy_report_values(&baseline_value, &candidate_value);
            emit_policy_report(&report, json)
        }
        PolicyCommand::Posture { session, json } => {
            let report = build_policy_posture_report(session, None)?;
            emit_policy_report(&report, json)
        }
        PolicyCommand::ToolPosture { catalog, json } => {
            let report = build_policy_posture_report(None, catalog)?;
            emit_policy_report(&report, json)
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct PolicyPostureReport {
    schema_version: u32,
    report_kind: &'static str,
    status: &'static str,
    source: String,
    session: Option<String>,
    weaker_rules: Vec<PolicyRuleFinding>,
    missing_rules: Vec<PolicyRuleFinding>,
    invalid_rules: Vec<PolicyRuleFinding>,
    fix_hints: Vec<String>,
    findings: Vec<PolicyRuleFinding>,
}

#[derive(Debug, Clone, Serialize)]
struct PolicyRuleFinding {
    severity: String,
    code: String,
    message: String,
    fix_hint: String,
}

fn build_policy_posture_report(
    session: Option<String>,
    catalog: Option<String>,
) -> Result<PolicyPostureReport> {
    let (allowlisted_tools, source) = load_policy_allowlisted_tools()?;
    let mut weaker_rules = Vec::new();
    let mut missing_rules = Vec::new();
    let mut invalid_rules = Vec::new();

    if allowlisted_tools.iter().any(|entry| entry == "*") {
        weaker_rules.push(policy_finding(
            "critical",
            "wildcard_allow_risk",
            "tool allowlist contains a wildcard entry",
            "replace '*' with explicit tool names and require approval for sensitive tools",
        ));
    }
    if !allowlisted_tools.is_empty() && env::var("PALYRA_TOOL_CALL_DENIED_TOOLS").is_err() {
        missing_rules.push(policy_finding(
            "warning",
            "missing_explicit_deny_rules",
            "tool allowlist is configured without an explicit deny overlay",
            "add deny rules for high-risk tools that must never run in this profile",
        ));
    }
    if env::var("PALYRA_HTTP_FETCH_ALLOWED_HOSTS").is_err()
        && env::var("PALYRA_HTTP_FETCH_ALLOWLIST").is_err()
    {
        missing_rules.push(policy_finding(
            "warning",
            "egress_without_allowlist",
            "HTTP fetch policy has no visible host allowlist override",
            "set an explicit HTTP fetch allowlist before enabling network egress in CI or production",
        ));
    }
    if env::var("PALYRA_CHANNEL_DELIVERY_PIPELINE_MODE")
        .map(|value| value.eq_ignore_ascii_case("group"))
        .unwrap_or(false)
        && env::var("PALYRA_CHANNEL_ROUTER_GROUP_GUARDRAILS").is_err()
    {
        missing_rules.push(policy_finding(
            "warning",
            "group_delivery_without_guardrails",
            "group delivery mode is enabled without a visible guardrail override",
            "configure channel-router guardrails before enabling group delivery",
        ));
    }

    for sensitive in sensitive_allowlisted_tool_names(allowlisted_tools.as_slice()) {
        if !tool_requires_approval(sensitive.as_str()) {
            invalid_rules.push(policy_finding(
                "critical",
                "secret_access_without_approval_gate",
                format!("sensitive tool '{sensitive}' is allowlisted without an approval gate"),
                "mark the tool approval-required or remove it from the allowlist",
            ));
        }
    }
    if let Some(catalog_path) = catalog {
        append_tool_catalog_posture_findings(catalog_path.as_str(), &mut invalid_rules)?;
    }

    let mut findings = Vec::new();
    findings.extend(weaker_rules.clone());
    findings.extend(missing_rules.clone());
    findings.extend(invalid_rules.clone());
    let status = if findings.iter().any(|entry| entry.severity == "critical") {
        "fail"
    } else if findings.is_empty() {
        "pass"
    } else {
        "warn"
    };
    let fix_hints = findings.iter().map(|entry| entry.fix_hint.clone()).collect::<Vec<_>>();
    Ok(PolicyPostureReport {
        schema_version: 1,
        report_kind: "policy_posture",
        status,
        source,
        session,
        weaker_rules,
        missing_rules,
        invalid_rules,
        fix_hints,
        findings,
    })
}

fn append_tool_catalog_posture_findings(
    catalog_path: &str,
    invalid_rules: &mut Vec<PolicyRuleFinding>,
) -> Result<()> {
    let bytes = fs::read(catalog_path)
        .with_context(|| format!("failed to read tool posture catalog {catalog_path}"))?;
    let value: Value = serde_json::from_slice(bytes.as_slice())
        .with_context(|| format!("failed to parse tool posture catalog {catalog_path}"))?;
    let tools = value
        .get("tools")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_else(|| value.as_array().cloned().unwrap_or_default());
    for tool in tools {
        let name = tool.get("name").and_then(Value::as_str).unwrap_or_default();
        let sensitivity = tool.get("sensitivity").and_then(Value::as_str).unwrap_or_default();
        let approval_required =
            tool.get("approval_required").and_then(Value::as_bool).unwrap_or_else(|| {
                (!name.is_empty() && tool_requires_approval(name)) || sensitivity == "sensitive"
            });
        if tool_requires_approval(name) && matches!(sensitivity, "public" | "low" | "normal") {
            invalid_rules.push(policy_finding(
                "critical",
                "tool_sensitivity_downgrade",
                format!(
                    "tool '{name}' is approval-gated by runtime catalog but marked '{sensitivity}'"
                ),
                "raise the tool sensitivity label or keep the approval-required flag",
            ));
        }
        if (name.contains("secret") || name.contains("token") || sensitivity == "sensitive")
            && !approval_required
        {
            invalid_rules.push(policy_finding(
                "critical",
                "secret_access_without_approval_gate",
                format!("tool '{name}' can access secrets without approval_required=true"),
                "require approval for secret-capable tools",
            ));
        }
    }
    Ok(())
}

fn build_policy_diff_report(
    baseline_path: &str,
    candidate: &PolicyPostureReport,
) -> Result<PolicyPostureReport> {
    let baseline = read_policy_report_value(baseline_path)?;
    let candidate = serde_json::to_value(candidate).context("failed to encode policy report")?;
    Ok(diff_policy_report_values(&baseline, &candidate))
}

fn diff_policy_report_values(baseline: &Value, candidate: &Value) -> PolicyPostureReport {
    let baseline_codes = finding_codes(baseline);
    let candidate_findings =
        candidate.get("findings").and_then(Value::as_array).cloned().unwrap_or_default();
    let mut weaker_rules = Vec::new();
    let mut missing_rules = Vec::new();
    let mut invalid_rules = Vec::new();
    for finding in candidate_findings {
        let code = finding.get("code").and_then(Value::as_str).unwrap_or("unknown_policy_drift");
        if baseline_codes.contains(code) {
            continue;
        }
        let severity = finding.get("severity").and_then(Value::as_str).unwrap_or("warning");
        let message = finding
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("candidate policy posture introduced a new finding")
            .to_owned();
        let fix_hint = finding
            .get("fix_hint")
            .and_then(Value::as_str)
            .unwrap_or("inspect the candidate policy posture report")
            .to_owned();
        let entry = PolicyRuleFinding {
            severity: if severity == "critical" { "critical" } else { "warning" }.to_owned(),
            code: code.to_owned(),
            message,
            fix_hint,
        };
        match code {
            "missing_explicit_deny_rules"
            | "egress_without_allowlist"
            | "group_delivery_without_guardrails" => missing_rules.push(entry),
            "tool_sensitivity_downgrade" | "secret_access_without_approval_gate" => {
                invalid_rules.push(entry)
            }
            _ => weaker_rules.push(entry),
        }
    }
    let mut findings = Vec::new();
    findings.extend(weaker_rules.clone());
    findings.extend(missing_rules.clone());
    findings.extend(invalid_rules.clone());
    let status = if findings.iter().any(|entry| entry.severity == "critical") {
        "fail"
    } else if findings.is_empty() {
        "pass"
    } else {
        "warn"
    };
    let fix_hints = findings.iter().map(|entry| entry.fix_hint.clone()).collect::<Vec<_>>();
    PolicyPostureReport {
        schema_version: 1,
        report_kind: "policy_diff",
        status,
        source: "policy_report_diff".to_owned(),
        session: None,
        weaker_rules,
        missing_rules,
        invalid_rules,
        fix_hints,
        findings,
    }
}

fn read_policy_report_value(path: &str) -> Result<Value> {
    let bytes = fs::read(path).with_context(|| format!("failed to read policy report {path}"))?;
    serde_json::from_slice(bytes.as_slice())
        .with_context(|| format!("failed to parse policy report {path}"))
}

fn finding_codes(value: &Value) -> std::collections::BTreeSet<&str> {
    value
        .get("findings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("code").and_then(Value::as_str))
        .collect()
}

fn policy_finding(
    severity: &'static str,
    code: &'static str,
    message: impl Into<String>,
    fix_hint: impl Into<String>,
) -> PolicyRuleFinding {
    PolicyRuleFinding {
        severity: severity.to_owned(),
        code: code.to_owned(),
        message: message.into(),
        fix_hint: fix_hint.into(),
    }
}

fn emit_policy_report(report: &PolicyPostureReport, json: bool) -> Result<()> {
    if output::preferred_json(json) {
        return output::print_json_pretty(report, "failed to encode policy posture report as JSON");
    }
    println!(
        "policy.{} status={} findings={} weaker_rules={} missing_rules={} invalid_rules={}",
        report.report_kind,
        report.status,
        report.findings.len(),
        report.weaker_rules.len(),
        report.missing_rules.len(),
        report.invalid_rules.len(),
    );
    for finding in &report.findings {
        println!(
            "finding severity={} code={} message={} fix_hint={}",
            finding.severity, finding.code, finding.message, finding.fix_hint
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
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

    #[test]
    fn policy_diff_reports_new_wildcard_allow_as_weaker_rule() {
        let baseline = json!({ "findings": [] });
        let candidate = json!({
            "findings": [
                {
                    "severity": "critical",
                    "code": "wildcard_allow_risk",
                    "message": "wildcard introduced",
                    "fix_hint": "replace wildcard"
                }
            ]
        });

        let report = diff_policy_report_values(&baseline, &candidate);

        assert_eq!(report.status, "fail");
        assert_eq!(report.weaker_rules.len(), 1);
        assert_eq!(report.weaker_rules[0].code, "wildcard_allow_risk");
        assert_eq!(report.fix_hints, vec!["replace wildcard".to_owned()]);
    }

    #[test]
    fn tool_catalog_posture_detects_sensitivity_and_secret_approval_regressions() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let catalog_path = temp.path().join("catalog.json");
        fs::write(
            catalog_path.as_path(),
            serde_json::to_vec(&json!({
                "tools": [
                    {
                        "name": "palyra.process.run",
                        "sensitivity": "public",
                        "approval_required": true
                    },
                    {
                        "name": "secret.reader",
                        "sensitivity": "sensitive",
                        "approval_required": false
                    }
                ]
            }))?,
        )?;
        let mut invalid = Vec::new();

        append_tool_catalog_posture_findings(
            catalog_path.to_string_lossy().as_ref(),
            &mut invalid,
        )?;

        let codes = invalid.iter().map(|entry| entry.code.as_str()).collect::<Vec<_>>();
        assert!(codes.contains(&"tool_sensitivity_downgrade"));
        assert!(codes.contains(&"secret_access_without_approval_gate"));
        Ok(())
    }
}
