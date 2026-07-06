//! ACP permission relay decision model.

#![allow(dead_code)]

use std::collections::BTreeMap;

use palyra_common::runtime_contracts::AcpPermissionDecision;
use serde::{Deserialize, Serialize};

pub(crate) const ACP_PERMISSION_RELAY_SCHEMA_VERSION: u32 = 1;
const DEFAULT_RATE_LIMIT_WINDOW_MS: i64 = 60_000;
const DEFAULT_RATE_LIMIT_MAX_REQUESTS: u32 = 8;
const MAX_ALLOW_ALWAYS_TTL_MS: i64 = 10 * 60 * 1_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpPermissionRelayRequest {
    pub runtime_id: String,
    pub acp_session_id: String,
    pub palyra_session_id: String,
    pub tool_name: String,
    pub args_sha256: String,
    pub requested_scope: String,
    pub mutating: bool,
    pub stale_permission_state: bool,
    pub allow_always_ttl_ms: Option<i64>,
    pub policy_allows_allow_always: bool,
    pub now_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpPermissionRequestEnvelope {
    pub schema_version: u32,
    pub approval_subject_id: String,
    pub runtime_id: String,
    pub acp_session_id: String,
    pub palyra_session_id: String,
    pub tool_name: String,
    pub args_sha256: String,
    pub requested_scope: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_always_until_unix_ms: Option<i64>,
    pub allow_always_binding: String,
    pub reason_codes: Vec<AcpPermissionRelayReasonCode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcpPermissionRelayReasonCode {
    ApprovalRequired,
    RequestValidated,
    StalePermissionBlocked,
    AllowAlwaysScoped,
    AllowAlwaysPolicyDenied,
    InvalidDigest,
    MissingScope,
    RateLimited,
}

impl AcpPermissionRelayReasonCode {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ApprovalRequired => "acp_permission.approval_required",
            Self::RequestValidated => "acp_permission.request_validated",
            Self::StalePermissionBlocked => "acp_permission.stale_permission_blocked",
            Self::AllowAlwaysScoped => "acp_permission.allow_always_scoped",
            Self::AllowAlwaysPolicyDenied => "acp_permission.allow_always_policy_denied",
            Self::InvalidDigest => "acp_permission.invalid_digest",
            Self::MissingScope => "acp_permission.missing_scope",
            Self::RateLimited => "acp_permission.rate_limited",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcpPermissionRelayDecision {
    ApprovalRequired(Box<AcpPermissionRequestEnvelope>),
    Denied {
        decision: AcpPermissionDecision,
        reason_code: AcpPermissionRelayReasonCode,
        safe_message: String,
    },
}

#[derive(Debug, Clone, Copy)]
struct RateLimitBucket {
    window_start_unix_ms: i64,
    count: u32,
}

#[derive(Debug, Default)]
pub(crate) struct AcpPermissionRelay {
    buckets: BTreeMap<String, RateLimitBucket>,
}

impl AcpPermissionRelay {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn evaluate(
        &mut self,
        request: AcpPermissionRelayRequest,
    ) -> AcpPermissionRelayDecision {
        if !consume_rate_limit(&mut self.buckets, &request) {
            return denied(
                AcpPermissionRelayReasonCode::RateLimited,
                "ACP permission request rate limit exceeded",
            );
        }
        if !is_valid_sha256(request.args_sha256.as_str()) {
            return denied(AcpPermissionRelayReasonCode::InvalidDigest, "invalid args digest");
        }
        if request.requested_scope.trim().is_empty() {
            return denied(AcpPermissionRelayReasonCode::MissingScope, "missing requested scope");
        }
        if request.stale_permission_state && request.mutating {
            return denied(
                AcpPermissionRelayReasonCode::StalePermissionBlocked,
                "stale ACP permissions block mutating tool requests",
            );
        }

        let mut reason_codes = vec![
            AcpPermissionRelayReasonCode::RequestValidated,
            AcpPermissionRelayReasonCode::ApprovalRequired,
        ];
        let allow_always_until_unix_ms = match request.allow_always_ttl_ms {
            Some(ttl_ms) if request.policy_allows_allow_always => {
                reason_codes.push(AcpPermissionRelayReasonCode::AllowAlwaysScoped);
                Some(
                    request
                        .now_unix_ms
                        .saturating_add(ttl_ms.clamp(1_000, MAX_ALLOW_ALWAYS_TTL_MS)),
                )
            }
            Some(_) => {
                return denied(
                    AcpPermissionRelayReasonCode::AllowAlwaysPolicyDenied,
                    "allow-always requires policy approval and a bounded scope",
                );
            }
            None => None,
        };

        let approval_subject_id = approval_subject_id(&request);
        let allow_always_binding =
            format!("scope:{}:args:{}", request.requested_scope, request.args_sha256);
        AcpPermissionRelayDecision::ApprovalRequired(Box::new(AcpPermissionRequestEnvelope {
            schema_version: ACP_PERMISSION_RELAY_SCHEMA_VERSION,
            approval_subject_id,
            runtime_id: request.runtime_id,
            acp_session_id: request.acp_session_id,
            palyra_session_id: request.palyra_session_id,
            tool_name: request.tool_name,
            args_sha256: request.args_sha256,
            requested_scope: request.requested_scope,
            allow_always_until_unix_ms,
            allow_always_binding,
            reason_codes,
        }))
    }
}

#[must_use]
pub(crate) fn map_approval_result_to_acp(
    approved: bool,
    envelope: &AcpPermissionRequestEnvelope,
    returned_scope: &str,
) -> AcpPermissionDecision {
    if !approved {
        return AcpPermissionDecision::Deny;
    }
    if returned_scope == envelope.requested_scope {
        AcpPermissionDecision::Allow
    } else {
        AcpPermissionDecision::Error
    }
}

fn consume_rate_limit(
    buckets: &mut BTreeMap<String, RateLimitBucket>,
    request: &AcpPermissionRelayRequest,
) -> bool {
    let bucket_key =
        format!("{}:{}:{}", request.runtime_id, request.acp_session_id, request.tool_name);
    let bucket = buckets
        .entry(bucket_key)
        .or_insert(RateLimitBucket { window_start_unix_ms: request.now_unix_ms, count: 0 });
    if request.now_unix_ms.saturating_sub(bucket.window_start_unix_ms)
        >= DEFAULT_RATE_LIMIT_WINDOW_MS
    {
        bucket.window_start_unix_ms = request.now_unix_ms;
        bucket.count = 0;
    }
    if bucket.count >= DEFAULT_RATE_LIMIT_MAX_REQUESTS {
        return false;
    }
    bucket.count = bucket.count.saturating_add(1);
    true
}

fn approval_subject_id(request: &AcpPermissionRelayRequest) -> String {
    crate::sha256_hex(
        format!(
            "{}|{}|{}|{}|{}",
            request.runtime_id,
            request.acp_session_id,
            request.tool_name,
            request.requested_scope,
            request.args_sha256
        )
        .as_bytes(),
    )
}

fn denied(
    reason_code: AcpPermissionRelayReasonCode,
    safe_message: &str,
) -> AcpPermissionRelayDecision {
    AcpPermissionRelayDecision::Denied {
        decision: AcpPermissionDecision::Deny,
        reason_code,
        safe_message: safe_message.to_owned(),
    }
}

fn is_valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> AcpPermissionRelayRequest {
        AcpPermissionRelayRequest {
            runtime_id: "native-acp".to_owned(),
            acp_session_id: "acp-session-a".to_owned(),
            palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            tool_name: "palyra.fs.apply_patch".to_owned(),
            args_sha256: "a".repeat(64),
            requested_scope: "workspace:write".to_owned(),
            mutating: true,
            stale_permission_state: false,
            allow_always_ttl_ms: None,
            policy_allows_allow_always: false,
            now_unix_ms: 1_000,
        }
    }

    #[test]
    fn acp_permission_request_creates_palyra_approval_envelope() {
        let mut relay = AcpPermissionRelay::new();
        let decision = relay.evaluate(request());

        let AcpPermissionRelayDecision::ApprovalRequired(envelope) = decision else {
            panic!("request should map to approval");
        };
        assert_eq!(envelope.tool_name, "palyra.fs.apply_patch");
        assert_eq!(envelope.requested_scope, "workspace:write");
        assert!(envelope.reason_codes.contains(&AcpPermissionRelayReasonCode::ApprovalRequired));
    }

    #[test]
    fn approval_result_preserves_scope() {
        let mut relay = AcpPermissionRelay::new();
        let AcpPermissionRelayDecision::ApprovalRequired(envelope) = relay.evaluate(request())
        else {
            panic!("request should map to approval");
        };

        assert_eq!(
            map_approval_result_to_acp(true, &envelope, "workspace:write"),
            AcpPermissionDecision::Allow
        );
        assert_eq!(
            map_approval_result_to_acp(true, &envelope, "global"),
            AcpPermissionDecision::Error
        );
    }

    #[test]
    fn stale_permission_state_blocks_mutating_request() {
        let mut relay = AcpPermissionRelay::new();
        let decision =
            relay.evaluate(AcpPermissionRelayRequest { stale_permission_state: true, ..request() });

        let AcpPermissionRelayDecision::Denied { reason_code, .. } = decision else {
            panic!("stale mutating request should be denied");
        };
        assert_eq!(reason_code, AcpPermissionRelayReasonCode::StalePermissionBlocked);
    }

    #[test]
    fn allow_always_is_scope_and_digest_bound() {
        let mut relay = AcpPermissionRelay::new();
        let decision = relay.evaluate(AcpPermissionRelayRequest {
            allow_always_ttl_ms: Some(600_000),
            policy_allows_allow_always: true,
            ..request()
        });

        let AcpPermissionRelayDecision::ApprovalRequired(envelope) = decision else {
            panic!("request should map to approval");
        };
        assert_eq!(envelope.allow_always_until_unix_ms, Some(601_000));
        assert_eq!(
            envelope.allow_always_binding,
            format!("scope:workspace:write:args:{}", "a".repeat(64))
        );
    }

    #[test]
    fn repeated_permission_requests_are_rate_limited() {
        let mut relay = AcpPermissionRelay::new();
        for _ in 0..DEFAULT_RATE_LIMIT_MAX_REQUESTS {
            assert!(matches!(
                relay.evaluate(request()),
                AcpPermissionRelayDecision::ApprovalRequired(_)
            ));
        }

        let decision = relay.evaluate(request());
        let AcpPermissionRelayDecision::Denied { reason_code, .. } = decision else {
            panic!("rate limit should deny");
        };
        assert_eq!(reason_code, AcpPermissionRelayReasonCode::RateLimited);
    }
}
