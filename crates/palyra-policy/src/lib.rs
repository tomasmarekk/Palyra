#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyRequest {
    pub principal: String,
    pub action: String,
    pub resource: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyDecision {
    Allow,
    DenyByDefault { reason: String },
}

const READ_ONLY_ACTION_ALLOWLIST: &[&str] = &[
    "tool.read",
    "tool.read.status",
    "tool.status",
    "tool.list",
    "tool.health",
    "tool.get",
    "daemon.status",
    "protocol.version",
];

#[must_use]
pub fn evaluate(request: &PolicyRequest) -> PolicyDecision {
    if is_sensitive_action(request) {
        return PolicyDecision::DenyByDefault {
            reason: "sensitive action blocked by default; explicit user approval required"
                .to_owned(),
        };
    }

    if is_read_only_action(request) {
        return PolicyDecision::Allow;
    }

    PolicyDecision::DenyByDefault { reason: "deny-by-default baseline policy".to_owned() }
}

fn is_sensitive_action(request: &PolicyRequest) -> bool {
    let action = request.action.to_ascii_lowercase();
    let resource = request.resource.to_ascii_lowercase();
    action.contains("shell")
        || action.contains("delete")
        || action.contains("payment")
        || resource.contains("secrets")
        || resource.contains("credential")
}

fn is_read_only_action(request: &PolicyRequest) -> bool {
    let action = request.action.to_ascii_lowercase();
    READ_ONLY_ACTION_ALLOWLIST.contains(&action.as_str())
}

#[cfg(test)]
mod tests {
    use super::{evaluate, PolicyDecision, PolicyRequest};

    #[test]
    fn default_policy_denies_all_requests() {
        let request = PolicyRequest {
            principal: "user:bootstrap".to_owned(),
            action: "tool.execute".to_owned(),
            resource: "tool:filesystem".to_owned(),
        };

        let decision = evaluate(&request);

        assert!(matches!(decision, PolicyDecision::DenyByDefault { .. }));
    }

    #[test]
    fn sensitive_actions_require_explicit_approval() {
        let request = PolicyRequest {
            principal: "user:bootstrap".to_owned(),
            action: "tool.execute.shell".to_owned(),
            resource: "tool:shell".to_owned(),
        };

        let decision = evaluate(&request);

        assert_eq!(
            decision,
            PolicyDecision::DenyByDefault {
                reason: "sensitive action blocked by default; explicit user approval required"
                    .to_owned(),
            }
        );
    }

    #[test]
    fn read_only_actions_are_allowed() {
        let request = PolicyRequest {
            principal: "user:bootstrap".to_owned(),
            action: "tool.read.status".to_owned(),
            resource: "tool:daemon".to_owned(),
        };

        let decision = evaluate(&request);

        assert_eq!(decision, PolicyDecision::Allow);
    }

    #[test]
    fn substring_collision_does_not_grant_read_only_access() {
        let request = PolicyRequest {
            principal: "user:bootstrap".to_owned(),
            action: "tool.target.reset".to_owned(),
            resource: "tool:filesystem".to_owned(),
        };

        let decision = evaluate(&request);

        assert!(matches!(decision, PolicyDecision::DenyByDefault { .. }));
    }

    #[test]
    fn mixed_scope_mutating_action_is_denied() {
        let request = PolicyRequest {
            principal: "user:bootstrap".to_owned(),
            action: "tool.read.write".to_owned(),
            resource: "tool:filesystem".to_owned(),
        };

        let decision = evaluate(&request);

        assert!(matches!(decision, PolicyDecision::DenyByDefault { .. }));
    }

    #[test]
    fn stacked_read_only_tokens_are_denied_when_not_allowlisted() {
        let request = PolicyRequest {
            principal: "user:bootstrap".to_owned(),
            action: "tool.status.health".to_owned(),
            resource: "tool:daemon".to_owned(),
        };

        let decision = evaluate(&request);

        assert!(matches!(decision, PolicyDecision::DenyByDefault { .. }));
    }
}
