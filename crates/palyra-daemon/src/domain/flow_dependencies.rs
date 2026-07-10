//! Fail-closed dependency-graph validation for durable flow steps.
//!
//! The journal validates this graph before writes, while the coordinator uses the same
//! typed result before dispatch. Diagnostic projections intentionally omit raw dependency JSON.

use std::collections::{BTreeMap, BTreeSet};

use palyra_common::runtime_contracts::FlowStepState;
use palyra_safety::{redact_text_for_export, SafetyContentKind, SafetySourceKind, TrustLabel};
use serde_json::{json, Value};

pub(crate) const FLOW_DEPENDENCY_REPORT_SCHEMA: &str = "palyra.flow.dependency_validation.v1";
const MAX_DIAGNOSTIC_ISSUES: usize = 32;
const MAX_DIAGNOSTIC_CYCLE_MEMBERS: usize = 16;
const MAX_DIAGNOSTIC_ID_CHARS: usize = 128;

/// Stable classification for a dependency-graph validation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum FlowDependencyReasonCode {
    MalformedDependencyJson,
    UnknownDependency,
    DependencyCycle,
    DuplicateStepId,
}

impl FlowDependencyReasonCode {
    /// Returns the stable snake_case reason code persisted in audit metadata.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::MalformedDependencyJson => "malformed_dependency_json",
            Self::UnknownDependency => "unknown_dependency",
            Self::DependencyCycle => "dependency_cycle",
            Self::DuplicateStepId => "duplicate_step_id",
        }
    }
}

/// One deterministic, payload-free dependency validation finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlowDependencyIssue {
    step_id: String,
    reason_code: FlowDependencyReasonCode,
    dependency_id: Option<String>,
    cycle_step_ids: Vec<String>,
    cycle_step_count: usize,
}

impl FlowDependencyIssue {
    /// Returns the affected durable step id.
    pub(crate) fn step_id(&self) -> &str {
        self.step_id.as_str()
    }

    /// Returns the stable failure classification.
    pub(crate) const fn reason_code(&self) -> FlowDependencyReasonCode {
        self.reason_code
    }

    /// Builds bounded metadata suitable for journal events and support diagnostics.
    pub(crate) fn diagnostic_value(&self) -> Value {
        json!({
            "step_id": bounded_id(self.step_id.as_str()),
            "reason_code": self.reason_code.as_str(),
            "dependency_id": self.dependency_id.as_deref().map(bounded_id),
            "cycle_step_ids": self
                .cycle_step_ids
                .iter()
                .take(MAX_DIAGNOSTIC_CYCLE_MEMBERS)
                .map(|step_id| bounded_id(step_id.as_str()))
                .collect::<Vec<_>>(),
            "cycle_step_count": self.cycle_step_count,
            "cycle_step_ids_truncated": self.cycle_step_count > self.cycle_step_ids.len(),
        })
    }
}

/// Deterministic report returned when a flow dependency graph is unsafe to dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlowDependencyValidationReport {
    issues: Vec<FlowDependencyIssue>,
    issue_count: usize,
    affected_steps: BTreeMap<String, FlowDependencyReasonCode>,
}

impl FlowDependencyValidationReport {
    /// Returns the bounded stored sample in step/dependency declaration order.
    #[cfg(test)]
    pub(crate) fn issues(&self) -> &[FlowDependencyIssue] {
        self.issues.as_slice()
    }

    /// Returns the total number of findings, including findings omitted from bounded storage.
    #[cfg(test)]
    pub(crate) const fn issue_count(&self) -> usize {
        self.issue_count
    }

    /// Returns whether validation identified the given step as requiring dependency repair.
    pub(crate) fn affects_step(&self, step_id: &str) -> bool {
        self.affected_steps.contains_key(step_id)
    }

    /// Returns the first stable validation reason associated with a step.
    pub(crate) fn reason_code_for_step(&self, step_id: &str) -> Option<FlowDependencyReasonCode> {
        self.affected_steps.get(step_id).copied()
    }

    /// Returns the first stable finding for client-facing error mapping.
    pub(crate) fn primary_issue(&self) -> &FlowDependencyIssue {
        self.issues.first().expect("invalid dependency reports always contain at least one issue")
    }

    /// Builds a bounded report without including untrusted dependency payloads.
    pub(crate) fn diagnostic_value(&self) -> Value {
        json!({
            "schema": FLOW_DEPENDENCY_REPORT_SCHEMA,
            "valid": false,
            "issue_count": self.issue_count,
            "issues": self
                .issues
                .iter()
                .take(MAX_DIAGNOSTIC_ISSUES)
                .map(FlowDependencyIssue::diagnostic_value)
                .collect::<Vec<_>>(),
            "issues_truncated": self.issue_count > self.issues.len(),
        })
    }
}

#[derive(Default)]
struct ValidationFindings {
    issues: Vec<FlowDependencyIssue>,
    issue_count: usize,
    affected_steps: BTreeMap<String, FlowDependencyReasonCode>,
}

impl ValidationFindings {
    fn push(&mut self, issue: FlowDependencyIssue) {
        self.issue_count = self.issue_count.saturating_add(1);
        self.affected_steps.entry(issue.step_id.clone()).or_insert(issue.reason_code);
        if self.issues.len() < MAX_DIAGNOSTIC_ISSUES {
            self.issues.push(issue);
        }
    }

    const fn is_empty(&self) -> bool {
        self.issue_count == 0
    }

    fn into_report(self) -> FlowDependencyValidationReport {
        FlowDependencyValidationReport {
            issues: self.issues,
            issue_count: self.issue_count,
            affected_steps: self.affected_steps,
        }
    }
}

/// Borrowed dependency columns for one flow step.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FlowDependencyNode<'a> {
    pub(crate) step_id: &'a str,
    pub(crate) dependencies_json: &'a str,
}

/// A dependency graph whose JSON, references, and acyclicity have been proven.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedFlowDependencyGraph {
    dependencies_by_step: BTreeMap<String, Vec<String>>,
}

impl ValidatedFlowDependencyGraph {
    /// Evaluates whether all parents of `step_id` are in success-like terminal states.
    pub(crate) fn gate_for<F>(&self, step_id: &str, mut state_for: F) -> Option<FlowDependencyGate>
    where
        F: FnMut(&str) -> Option<FlowStepState>,
    {
        let dependencies = self.dependencies_by_step.get(step_id)?;
        let blocking_dependency_ids = dependencies
            .iter()
            .filter(|dependency_id| {
                !state_for(dependency_id.as_str()).is_some_and(is_success_like_state)
            })
            .cloned()
            .collect::<Vec<_>>();
        if blocking_dependency_ids.is_empty() {
            Some(FlowDependencyGate::Satisfied)
        } else {
            Some(FlowDependencyGate::Blocked { blocking_dependency_ids })
        }
    }

    /// Builds the valid form of the diagnostics contract.
    pub(crate) fn diagnostic_value(&self) -> Value {
        json!({
            "schema": FLOW_DEPENDENCY_REPORT_SCHEMA,
            "valid": true,
            "issue_count": 0,
            "issues": [],
            "issues_truncated": false,
        })
    }
}

/// Dispatch gate for a step in a validated graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FlowDependencyGate {
    Satisfied,
    Blocked { blocking_dependency_ids: Vec<String> },
}

/// Parses and validates a complete flow dependency graph.
///
/// # Errors
/// Returns a deterministic report for malformed JSON, duplicate step ids, unknown parents, or
/// cycles. The report never includes the raw JSON payload.
pub(crate) fn validate_flow_dependency_graph<'a>(
    input_nodes: impl IntoIterator<Item = FlowDependencyNode<'a>>,
) -> Result<ValidatedFlowDependencyGraph, FlowDependencyValidationReport> {
    let nodes = input_nodes.into_iter().collect::<Vec<FlowDependencyNode<'a>>>();
    let mut findings = ValidationFindings::default();
    let mut step_ids = BTreeSet::new();
    for node in &nodes {
        if node.step_id.trim().is_empty() {
            findings.push(malformed_issue(node.step_id));
        }
        if !step_ids.insert(node.step_id) {
            findings.push(FlowDependencyIssue {
                step_id: node.step_id.to_owned(),
                reason_code: FlowDependencyReasonCode::DuplicateStepId,
                dependency_id: None,
                cycle_step_ids: Vec::new(),
                cycle_step_count: 0,
            });
        }
    }

    let mut dependencies_by_step = BTreeMap::<String, Vec<String>>::new();
    for node in &nodes {
        match parse_flow_dependency_ids(node.dependencies_json) {
            Ok(dependencies) => {
                dependencies_by_step.insert(node.step_id.to_owned(), dependencies);
            }
            Err(_) => findings.push(malformed_issue(node.step_id)),
        }
    }
    for node in &nodes {
        dependencies_by_step.entry(node.step_id.to_owned()).or_default();
    }

    for node in &nodes {
        let Some(dependencies) = dependencies_by_step.get(node.step_id) else {
            continue;
        };
        for dependency_id in dependencies {
            if !step_ids.contains(dependency_id.as_str()) {
                findings.push(FlowDependencyIssue {
                    step_id: node.step_id.to_owned(),
                    reason_code: FlowDependencyReasonCode::UnknownDependency,
                    dependency_id: Some(dependency_id.clone()),
                    cycle_step_ids: Vec::new(),
                    cycle_step_count: 0,
                });
            }
        }
    }

    let known_dependencies_by_step = dependencies_by_step
        .iter()
        .map(|(step_id, dependencies)| {
            (
                step_id.clone(),
                dependencies
                    .iter()
                    .filter(|dependency_id| step_ids.contains(dependency_id.as_str()))
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for component in cyclic_components(nodes.as_slice(), &known_dependencies_by_step) {
        let cycle_step_count = component.len();
        let cycle_step_ids =
            component.iter().take(MAX_DIAGNOSTIC_CYCLE_MEMBERS).cloned().collect::<Vec<_>>();
        for step_id in component {
            findings.push(FlowDependencyIssue {
                step_id,
                reason_code: FlowDependencyReasonCode::DependencyCycle,
                dependency_id: None,
                cycle_step_ids: cycle_step_ids.clone(),
                cycle_step_count,
            });
        }
    }
    if !findings.is_empty() {
        return Err(findings.into_report());
    }

    Ok(ValidatedFlowDependencyGraph { dependencies_by_step })
}

fn malformed_issue(step_id: &str) -> FlowDependencyIssue {
    FlowDependencyIssue {
        step_id: step_id.to_owned(),
        reason_code: FlowDependencyReasonCode::MalformedDependencyJson,
        dependency_id: None,
        cycle_step_ids: Vec::new(),
        cycle_step_count: 0,
    }
}

/// Parses one dependency list using the same semantic contract as graph validation.
///
/// # Errors
/// Returns MalformedDependencyJson for invalid JSON or duplicate or empty ids. The raw payload is
/// never included in the error.
pub(crate) fn parse_flow_dependency_ids(
    raw: &str,
) -> Result<Vec<String>, FlowDependencyReasonCode> {
    let dependencies = serde_json::from_str::<Vec<String>>(raw)
        .map_err(|_| FlowDependencyReasonCode::MalformedDependencyJson)?;
    let mut unique_dependencies = BTreeSet::new();
    if dependencies.iter().any(|dependency_id| {
        dependency_id.trim().is_empty() || !unique_dependencies.insert(dependency_id.as_str())
    }) {
        return Err(FlowDependencyReasonCode::MalformedDependencyJson);
    }
    Ok(dependencies)
}

/// Returns whether a persisted waiting reason belongs to dependency validation.
pub(crate) fn is_dependency_validation_reason(reason: &str) -> bool {
    matches!(
        reason,
        "malformed_dependency_json"
            | "unknown_dependency"
            | "dependency_cycle"
            | "duplicate_step_id"
    )
}

// Two iterative Kosaraju passes keep validation O(V + E) without allowing an untrusted graph to
// drive recursive stack growth. Only actual strongly connected components are reported.
fn cyclic_components(
    nodes: &[FlowDependencyNode<'_>],
    dependencies_by_step: &BTreeMap<String, Vec<String>>,
) -> Vec<Vec<String>> {
    let mut reverse_edges = BTreeMap::<String, Vec<String>>::new();
    for node in nodes {
        for dependency_id in &dependencies_by_step[node.step_id] {
            reverse_edges.entry(dependency_id.clone()).or_default().push(node.step_id.to_owned());
        }
    }

    let mut visited = BTreeSet::new();
    let mut finish_order = Vec::with_capacity(nodes.len());
    for node in nodes {
        if visited.contains(node.step_id) {
            continue;
        }
        let mut stack = vec![(node.step_id.to_owned(), false)];
        while let Some((step_id, expanded)) = stack.pop() {
            if expanded {
                finish_order.push(step_id);
                continue;
            }
            if !visited.insert(step_id.clone()) {
                continue;
            }
            stack.push((step_id.clone(), true));
            for dependency_id in dependencies_by_step[step_id.as_str()].iter().rev() {
                if !visited.contains(dependency_id) {
                    stack.push((dependency_id.clone(), false));
                }
            }
        }
    }

    let order_by_step = nodes
        .iter()
        .enumerate()
        .map(|(index, node)| (node.step_id, index))
        .collect::<BTreeMap<_, _>>();
    visited.clear();
    let mut components = Vec::new();
    for start_step_id in finish_order.into_iter().rev() {
        if !visited.insert(start_step_id.clone()) {
            continue;
        }
        let mut component = Vec::new();
        let mut stack = vec![start_step_id];
        while let Some(step_id) = stack.pop() {
            component.push(step_id.clone());
            for child_id in reverse_edges.get(step_id.as_str()).into_iter().flatten().rev() {
                if visited.insert(child_id.clone()) {
                    stack.push(child_id.clone());
                }
            }
        }
        component.sort_by_key(|step_id| order_by_step[step_id.as_str()]);
        let self_cycle = component.len() == 1
            && dependencies_by_step[component[0].as_str()].contains(&component[0]);
        if component.len() > 1 || self_cycle {
            components.push(component);
        }
    }
    components.sort_by_key(|component| order_by_step[component[0].as_str()]);
    components
}

fn is_success_like_state(state: FlowStepState) -> bool {
    matches!(state, FlowStepState::Succeeded | FlowStepState::Skipped | FlowStepState::Compensated)
}

fn bounded_id(value: &str) -> String {
    let redacted = redact_text_for_export(
        value,
        SafetySourceKind::Unknown,
        SafetyContentKind::PlainText,
        TrustLabel::ExternalUntrusted,
    )
    .redacted_text;
    let mut bounded = redacted.chars().take(MAX_DIAGNOSTIC_ID_CHARS + 1).collect::<String>();
    if bounded.chars().count() > MAX_DIAGNOSTIC_ID_CHARS {
        bounded = bounded.chars().take(MAX_DIAGNOSTIC_ID_CHARS).collect();
        bounded.push_str("...");
    }
    bounded
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;

    fn node<'a>(step_id: &'a str, dependencies_json: &'a str) -> FlowDependencyNode<'a> {
        FlowDependencyNode { step_id, dependencies_json }
    }

    #[test]
    fn malformed_dependency_json_is_rejected_without_payload_echo() {
        let raw = "{secret-bearing-untrusted-payload";
        let report = validate_flow_dependency_graph([node("child", raw)])
            .expect_err("malformed dependency JSON must fail closed");

        assert_eq!(
            report.primary_issue().reason_code(),
            FlowDependencyReasonCode::MalformedDependencyJson
        );
        assert!(!report.diagnostic_value().to_string().contains(raw));
    }

    #[test]
    fn unknown_parent_is_rejected() {
        let report = validate_flow_dependency_graph([node("child", r#"["missing"]"#)])
            .expect_err("unknown dependency must fail closed");

        assert_eq!(
            report.primary_issue().reason_code(),
            FlowDependencyReasonCode::UnknownDependency
        );
    }

    #[test]
    fn empty_and_duplicate_dependency_ids_are_rejected() {
        for dependencies in [r#"[""]"#, r#"["parent","parent"]"#] {
            let report =
                validate_flow_dependency_graph([node("parent", "[]"), node("child", dependencies)])
                    .expect_err("ambiguous dependency ids must fail closed");
            assert_eq!(
                report.primary_issue().reason_code(),
                FlowDependencyReasonCode::MalformedDependencyJson
            );
        }
    }

    #[test]
    fn diagnostic_dependency_ids_are_bounded_and_redacted() {
        let secret_id = "api_key=secret_should_not_appear";
        let report = validate_flow_dependency_graph([node(
            "child",
            serde_json::to_string(&vec![secret_id]).expect("fixture should serialize").as_str(),
        )]);
        let report = report.expect_err("unknown secret-bearing dependency must fail closed");
        let diagnostic = report.diagnostic_value().to_string();

        assert!(!diagnostic.contains("secret_should_not_appear"));
        assert!(diagnostic.contains("REDACTED"));
    }

    #[test]
    fn self_and_multi_node_cycles_are_rejected() {
        let self_cycle = validate_flow_dependency_graph([node("self", r#"["self"]"#)])
            .expect_err("self-cycle must fail closed");
        assert_eq!(
            self_cycle.primary_issue().reason_code(),
            FlowDependencyReasonCode::DependencyCycle
        );

        let multi_cycle = validate_flow_dependency_graph([
            node("one", r#"["three"]"#),
            node("two", r#"["one"]"#),
            node("three", r#"["two"]"#),
        ])
        .expect_err("multi-node cycle must fail closed");
        assert_eq!(multi_cycle.issues().len(), 3);
        assert!(multi_cycle
            .issues()
            .iter()
            .all(|issue| issue.reason_code() == FlowDependencyReasonCode::DependencyCycle));
    }

    #[test]
    fn mixed_parse_and_cycle_failures_report_every_affected_step() {
        let report = validate_flow_dependency_graph([
            node("malformed", "{"),
            node("cycle-a", r#"["cycle-b"]"#),
            node("cycle-b", r#"["cycle-a"]"#),
        ])
        .expect_err("mixed dependency corruption must fail closed");

        assert!(report.affects_step("malformed"));
        assert!(report.affects_step("cycle-a"));
        assert!(report.affects_step("cycle-b"));
        assert_eq!(
            report.reason_code_for_step("malformed"),
            Some(FlowDependencyReasonCode::MalformedDependencyJson)
        );
        assert_eq!(
            report.reason_code_for_step("cycle-a"),
            Some(FlowDependencyReasonCode::DependencyCycle)
        );
    }

    #[test]
    fn large_cycle_report_has_bounded_storage() {
        let fixtures = (0..100)
            .map(|index| {
                let step_id = format!("step-{index}");
                let dependency_id = format!("step-{}", (index + 99) % 100);
                let dependencies =
                    serde_json::to_string(&vec![dependency_id]).expect("fixture should serialize");
                (step_id, dependencies)
            })
            .collect::<Vec<_>>();
        let report =
            validate_flow_dependency_graph(fixtures.iter().map(|(step_id, dependencies_json)| {
                node(step_id.as_str(), dependencies_json.as_str())
            }))
            .expect_err("large cycle must fail closed");

        assert_eq!(report.issue_count(), 100);
        assert!(report.affects_step("step-99"));
        assert_eq!(report.issues().len(), MAX_DIAGNOSTIC_ISSUES);
        assert!(report
            .issues()
            .iter()
            .all(|issue| issue.cycle_step_ids.len() <= MAX_DIAGNOSTIC_CYCLE_MEMBERS));
        assert_eq!(report.diagnostic_value()["issues_truncated"], true);
    }

    #[test]
    fn large_valid_graphs_are_not_reclassified_by_diagnostic_bounds() {
        let wide =
            (0..513).map(|index| (format!("step-{index}"), "[]".to_owned())).collect::<Vec<_>>();
        validate_flow_dependency_graph(wide.iter().map(|(step_id, dependencies_json)| {
            node(step_id.as_str(), dependencies_json.as_str())
        }))
        .expect("diagnostic bounds must not reject a large valid graph");

        let dependency_ids = (0..129).map(|index| format!("parent-{index}")).collect::<Vec<_>>();
        let mut fan_in = dependency_ids
            .iter()
            .map(|step_id| (step_id.clone(), "[]".to_owned()))
            .collect::<Vec<_>>();
        fan_in.push((
            "child".to_owned(),
            serde_json::to_string(&dependency_ids).expect("fixture should serialize"),
        ));
        validate_flow_dependency_graph(fan_in.iter().map(|(step_id, dependencies_json)| {
            node(step_id.as_str(), dependencies_json.as_str())
        }))
        .expect("diagnostic bounds must not reject a wide valid fan-in");
    }

    #[test]
    fn valid_dag_distinguishes_satisfied_and_blocked_parents() {
        let graph =
            validate_flow_dependency_graph([node("parent", "[]"), node("child", r#"["parent"]"#)])
                .expect("valid DAG should pass validation");
        let mut states = BTreeMap::from([("parent", FlowStepState::Succeeded)]);
        assert_eq!(
            graph.gate_for("child", |step_id| states.get(step_id).copied()),
            Some(FlowDependencyGate::Satisfied)
        );

        for state in [FlowStepState::Failed, FlowStepState::Cancelled, FlowStepState::Running] {
            states.insert("parent", state);
            assert!(matches!(
                graph.gate_for("child", |step_id| states.get(step_id).copied()),
                Some(FlowDependencyGate::Blocked { .. })
            ));
        }
        for state in [FlowStepState::Skipped, FlowStepState::Compensated] {
            states.insert("parent", state);
            assert_eq!(
                graph.gate_for("child", |step_id| states.get(step_id).copied()),
                Some(FlowDependencyGate::Satisfied)
            );
        }
    }

    proptest! {
        #[test]
        fn arbitrary_invalid_dependency_payload_never_validates(raw in ".*") {
            prop_assume!(serde_json::from_str::<Vec<String>>(raw.as_str()).is_err());
            let result = validate_flow_dependency_graph([node("step", raw.as_str())]);
            prop_assert!(result.is_err());
        }
    }
}
