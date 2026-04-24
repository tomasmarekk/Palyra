use std::collections::{BTreeMap, BTreeSet, VecDeque};

use palyra_common::runtime_contracts::{
    RealtimeCapability, RealtimeCommand, RealtimeCursor, RealtimeErrorEnvelope,
    RealtimeEventEnvelope, RealtimeEventSensitivity, RealtimeEventTopic, RealtimeHandshakeAccepted,
    RealtimeHandshakeRequest, RealtimeMethodDescriptor, RealtimeProtocolVersionRange, RealtimeRole,
    RealtimeScope, RealtimeSubscription, StableErrorEnvelope,
    REALTIME_DEFAULT_HEARTBEAT_INTERVAL_MS,
};
use serde_json::json;

pub(crate) const REALTIME_SDK_ABI_VERSION: &str = "palyra.realtime.sdk.v1";
pub(crate) const REALTIME_EVENT_BUFFER_CAPACITY: usize = 1_024;
pub(crate) const REALTIME_RATE_LIMIT_WINDOW_MS: i64 = 1_000;
pub(crate) const REALTIME_RATE_LIMIT_MAX_REQUESTS: u32 = 60;

#[derive(Debug, Clone)]
pub(crate) struct RealtimeConnectionContext {
    pub(crate) client_id: String,
    pub(crate) auth_subject: String,
    pub(crate) role: RealtimeRole,
    pub(crate) scopes: BTreeSet<RealtimeScope>,
    pub(crate) capabilities: BTreeSet<RealtimeCapability>,
    pub(crate) commands: BTreeSet<RealtimeCommand>,
    pub(crate) cursor: RealtimeCursor,
    pub(crate) heartbeat_interval_ms: u64,
    pub(crate) subscriptions: Vec<RealtimeSubscription>,
}

impl RealtimeConnectionContext {
    pub(crate) fn has_scope(&self, scope: RealtimeScope) -> bool {
        self.scopes.contains(&scope)
    }

    pub(crate) fn has_capability(&self, capability: RealtimeCapability) -> bool {
        self.capabilities.contains(&capability)
    }

    pub(crate) fn has_command(&self, command: RealtimeCommand) -> bool {
        self.commands.contains(&command)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum RealtimeReplayOutcome {
    Events(Vec<RealtimeEventEnvelope>),
    SnapshotRequired { cursor: RealtimeCursor, first_available_sequence: u64 },
}

#[derive(Debug, Clone)]
pub(crate) struct RealtimeEventRouter {
    capacity: usize,
    next_sequence: u64,
    events: VecDeque<RealtimeEventEnvelope>,
}

impl Default for RealtimeEventRouter {
    fn default() -> Self {
        Self {
            capacity: REALTIME_EVENT_BUFFER_CAPACITY,
            next_sequence: 1,
            events: VecDeque::with_capacity(REALTIME_EVENT_BUFFER_CAPACITY),
        }
    }
}

impl RealtimeEventRouter {
    #[must_use]
    pub(crate) fn publish(&mut self, mut event: RealtimeEventEnvelope) -> RealtimeEventEnvelope {
        event.sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        if self.events.len() >= self.capacity {
            self.events.pop_front();
        }
        self.events.push_back(event.clone());
        event
    }

    #[must_use]
    pub(crate) fn replay_from(
        &self,
        context: &RealtimeConnectionContext,
        cursor: RealtimeCursor,
    ) -> RealtimeReplayOutcome {
        let first_available_sequence = self.events.front().map(|event| event.sequence).unwrap_or(0);
        if first_available_sequence > 0
            && cursor.sequence.saturating_add(1) < first_available_sequence
        {
            return RealtimeReplayOutcome::SnapshotRequired { cursor, first_available_sequence };
        }
        let events = self
            .events
            .iter()
            .filter(|event| event.sequence > cursor.sequence)
            .filter(|event| event_visible_to_context(context, event))
            .cloned()
            .collect();
        RealtimeReplayOutcome::Events(events)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RealtimeRateLimiter {
    window_ms: i64,
    max_requests: u32,
    buckets: BTreeMap<String, RealtimeRateLimitBucket>,
}

impl Default for RealtimeRateLimiter {
    fn default() -> Self {
        Self {
            window_ms: REALTIME_RATE_LIMIT_WINDOW_MS,
            max_requests: REALTIME_RATE_LIMIT_MAX_REQUESTS,
            buckets: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct RealtimeRateLimitBucket {
    window_started_at_unix_ms: i64,
    requests: u32,
}

impl RealtimeRateLimiter {
    pub(crate) fn check(
        &mut self,
        context: &RealtimeConnectionContext,
        command: RealtimeCommand,
        now_unix_ms: i64,
    ) -> Result<(), StableErrorEnvelope> {
        let descriptor = descriptor_for_command(command).ok_or_else(|| {
            stable_error(
                "realtime/unknown_command",
                "realtime command is not registered",
                "refresh the method registry and retry with a supported command",
            )
        })?;
        let key = format!(
            "{}:{}:{}",
            descriptor.rate_limit_bucket, context.auth_subject, context.client_id
        );
        let bucket = self.buckets.entry(key).or_insert(RealtimeRateLimitBucket {
            window_started_at_unix_ms: now_unix_ms,
            requests: 0,
        });
        if now_unix_ms.saturating_sub(bucket.window_started_at_unix_ms) >= self.window_ms {
            bucket.window_started_at_unix_ms = now_unix_ms;
            bucket.requests = 0;
        }
        if bucket.requests >= self.max_requests {
            return Err(stable_error(
                "realtime/rate_limited",
                "realtime command rate limit exceeded",
                "back off before retrying this command bucket",
            ));
        }
        bucket.requests = bucket.requests.saturating_add(1);
        Ok(())
    }
}

pub(crate) fn negotiate_realtime_handshake(
    request: RealtimeHandshakeRequest,
    auth_subject: String,
    now_unix_ms: i64,
) -> Result<(RealtimeHandshakeAccepted, RealtimeConnectionContext), RealtimeErrorEnvelope> {
    let supported = RealtimeProtocolVersionRange::default();
    if !supported.contains(request.protocol_version) {
        return Err(RealtimeErrorEnvelope {
            error: stable_error(
                "realtime/incompatible_protocol",
                "realtime protocol version is not supported",
                "retry with a protocol version in the advertised supported range",
            ),
            supported_protocol_versions: Some(supported),
        });
    }
    let client_id = request.client_id.trim();
    if client_id.is_empty() || client_id.len() > 128 {
        return Err(realtime_error(
            "realtime/invalid_client_id",
            "realtime client_id must be present and bounded",
            "send a stable client_id of 1 to 128 bytes",
        ));
    }

    let allowed_scopes = allowed_scopes_for_role(request.role, auth_subject.as_str());
    let requested_scopes = if request.requested_scopes.is_empty() {
        allowed_scopes.clone()
    } else {
        request.requested_scopes.iter().copied().collect()
    };
    let scopes = intersection(&requested_scopes, &allowed_scopes);
    if scopes.is_empty() {
        return Err(realtime_error(
            "realtime/no_scopes_granted",
            "no requested realtime scopes are permitted for this subject",
            "request a scope allowed by the authenticated role",
        ));
    }

    let allowed_capabilities = capabilities_for_scopes(&scopes);
    let requested_capabilities = if request.requested_capabilities.is_empty() {
        allowed_capabilities.clone()
    } else {
        request.requested_capabilities.iter().copied().collect()
    };
    let capabilities = intersection(&requested_capabilities, &allowed_capabilities);
    let allowed_commands = commands_for_grants(&scopes, &capabilities);
    let requested_commands = if request.requested_commands.is_empty() {
        allowed_commands.clone()
    } else {
        request.requested_commands.iter().copied().collect()
    };
    let commands = intersection(&requested_commands, &allowed_commands);
    if commands.is_empty() {
        return Err(realtime_error(
            "realtime/no_commands_granted",
            "no requested realtime commands are permitted for this subject",
            "request commands compatible with the granted scopes and capabilities",
        ));
    }

    let heartbeat_interval_ms = request
        .heartbeat_interval_ms
        .unwrap_or(REALTIME_DEFAULT_HEARTBEAT_INTERVAL_MS)
        .clamp(5_000, 60_000);
    let cursor = request.event_cursor.unwrap_or_default();
    let subscriptions = if request.subscriptions.is_empty() {
        vec![RealtimeSubscription::all_topics()]
    } else {
        request.subscriptions.clone()
    };
    let context = RealtimeConnectionContext {
        client_id: client_id.to_owned(),
        auth_subject: auth_subject.clone(),
        role: request.role,
        scopes: scopes.clone(),
        capabilities: capabilities.clone(),
        commands: commands.clone(),
        cursor,
        heartbeat_interval_ms,
        subscriptions: subscriptions.clone(),
    };
    let accepted = RealtimeHandshakeAccepted {
        protocol_version: request.protocol_version,
        client_id: client_id.to_owned(),
        auth_subject,
        role: request.role,
        scopes: sorted_set(scopes),
        capabilities: sorted_set(capabilities),
        commands: sorted_set(commands),
        subscriptions,
        cursor,
        heartbeat_interval_ms,
        server_time_unix_ms: now_unix_ms,
        sdk_abi_version: REALTIME_SDK_ABI_VERSION.to_owned(),
    };
    Ok((accepted, context))
}

pub(crate) fn realtime_method_descriptors() -> Vec<RealtimeMethodDescriptor> {
    vec![
        descriptor(
            RealtimeCommand::RunCreate,
            &[RealtimeScope::RunsWrite],
            &[RealtimeCapability::RunControl],
            true,
            true,
            "runs.write",
        ),
        descriptor(
            RealtimeCommand::RunWait,
            &[RealtimeScope::RunsRead],
            &[RealtimeCapability::EventStream],
            false,
            false,
            "runs.read",
        ),
        descriptor(
            RealtimeCommand::RunEvents,
            &[RealtimeScope::RunsRead, RealtimeScope::EventsRead],
            &[RealtimeCapability::EventStream],
            false,
            false,
            "runs.read",
        ),
        descriptor(
            RealtimeCommand::RunAbort,
            &[RealtimeScope::RunsWrite],
            &[RealtimeCapability::RunControl],
            true,
            true,
            "runs.write",
        ),
        descriptor(
            RealtimeCommand::RunGet,
            &[RealtimeScope::RunsRead],
            &[],
            false,
            false,
            "runs.read",
        ),
        descriptor(
            RealtimeCommand::ApprovalList,
            &[RealtimeScope::ApprovalsRead],
            &[],
            false,
            false,
            "approvals.read",
        ),
        descriptor(
            RealtimeCommand::ApprovalGet,
            &[RealtimeScope::ApprovalsRead],
            &[],
            false,
            false,
            "approvals.read",
        ),
        descriptor(
            RealtimeCommand::ApprovalDecide,
            &[RealtimeScope::ApprovalsWrite],
            &[RealtimeCapability::ApprovalControl],
            true,
            true,
            "approvals.write",
        ),
        descriptor(
            RealtimeCommand::NodePresence,
            &[RealtimeScope::NodesRead],
            &[RealtimeCapability::NodePresence],
            false,
            false,
            "nodes.read",
        ),
        descriptor(
            RealtimeCommand::NodeCapabilityGrant,
            &[RealtimeScope::NodesWrite],
            &[RealtimeCapability::CapabilityGrant],
            true,
            true,
            "nodes.write",
        ),
        descriptor(
            RealtimeCommand::NodeCapabilityRevoke,
            &[RealtimeScope::NodesWrite],
            &[RealtimeCapability::CapabilityGrant],
            true,
            true,
            "nodes.write",
        ),
        descriptor(
            RealtimeCommand::ConfigSchemaLookup,
            &[RealtimeScope::ConfigRead],
            &[RealtimeCapability::ConfigSchemaLookup],
            false,
            false,
            "config.read",
        ),
        descriptor(
            RealtimeCommand::ConfigReloadPlan,
            &[RealtimeScope::ConfigRead],
            &[RealtimeCapability::ConfigReload],
            false,
            false,
            "config.read",
        ),
        descriptor(
            RealtimeCommand::ConfigReloadApply,
            &[RealtimeScope::ConfigWrite],
            &[RealtimeCapability::ConfigReload],
            true,
            true,
            "config.write",
        ),
    ]
}

pub(crate) fn descriptor_for_command(command: RealtimeCommand) -> Option<RealtimeMethodDescriptor> {
    realtime_method_descriptors().into_iter().find(|descriptor| descriptor.command == command)
}

pub(crate) fn authorize_realtime_command(
    context: &RealtimeConnectionContext,
    command: RealtimeCommand,
    idempotency_key: Option<&str>,
) -> Result<RealtimeMethodDescriptor, StableErrorEnvelope> {
    if !context.has_command(command) {
        return Err(stable_error(
            "realtime/command_not_granted",
            "realtime command was not granted during handshake",
            "renegotiate with the required command and scopes",
        ));
    }
    let descriptor = descriptor_for_command(command).ok_or_else(|| {
        stable_error(
            "realtime/unknown_command",
            "realtime command is not registered",
            "refresh the method registry and retry with a supported command",
        )
    })?;
    if descriptor.required_scopes.iter().any(|scope| !context.has_scope(*scope)) {
        return Err(stable_error(
            "realtime/missing_scope",
            "realtime command requires a scope that was not granted",
            "request the required scope during handshake",
        ));
    }
    if descriptor
        .required_capabilities
        .iter()
        .any(|capability| !context.has_capability(*capability))
    {
        return Err(stable_error(
            "realtime/missing_capability",
            "realtime command requires a capability that was not granted",
            "request the required capability during handshake",
        ));
    }
    if descriptor.idempotency_required
        && idempotency_key.map(str::trim).filter(|value| !value.is_empty()).is_none()
    {
        return Err(stable_error(
            "realtime/idempotency_required",
            "side-effecting realtime command requires an idempotency key",
            "retry with a stable idempotency_key for this command payload",
        ));
    }
    Ok(descriptor)
}

pub(crate) fn event_visible_to_context(
    context: &RealtimeConnectionContext,
    event: &RealtimeEventEnvelope,
) -> bool {
    if !topic_scope_allowed(context, event.topic) {
        return false;
    }
    match event.sensitivity {
        RealtimeEventSensitivity::Public | RealtimeEventSensitivity::Internal => {}
        RealtimeEventSensitivity::Sensitive => {
            if !context.has_scope(RealtimeScope::EventsSensitive)
                || !context.has_capability(RealtimeCapability::SensitiveEvents)
            {
                return false;
            }
        }
        RealtimeEventSensitivity::Secret => return false,
    }
    if let Some(owner) = event.owner_principal.as_deref() {
        if owner != context.auth_subject && !context.auth_subject.starts_with("admin:") {
            return false;
        }
    }
    context.subscriptions.iter().any(|subscription| subscription_allows(subscription, event))
}

pub(crate) fn snapshot_refresh_event(
    cursor: RealtimeCursor,
    first_available_sequence: u64,
    now_unix_ms: i64,
) -> RealtimeEventEnvelope {
    RealtimeEventEnvelope {
        schema_version: 1,
        sequence: 0,
        event_id: "snapshot-refresh-required".to_owned(),
        topic: RealtimeEventTopic::System,
        sensitivity: RealtimeEventSensitivity::Internal,
        owner_principal: None,
        owner_session_id: None,
        occurred_at_unix_ms: now_unix_ms,
        payload: json!({
            "reason": "event_gap",
            "requested_after_sequence": cursor.sequence,
            "first_available_sequence": first_available_sequence,
        }),
    }
}

fn allowed_scopes_for_role(role: RealtimeRole, auth_subject: &str) -> BTreeSet<RealtimeScope> {
    let mut scopes = match role {
        RealtimeRole::Operator => BTreeSet::from([
            RealtimeScope::RunsRead,
            RealtimeScope::RunsWrite,
            RealtimeScope::ApprovalsRead,
            RealtimeScope::ApprovalsWrite,
            RealtimeScope::NodesRead,
            RealtimeScope::NodesWrite,
            RealtimeScope::ConfigRead,
            RealtimeScope::ConfigWrite,
            RealtimeScope::EventsRead,
            RealtimeScope::EventsSensitive,
        ]),
        RealtimeRole::ReadOnly => BTreeSet::from([
            RealtimeScope::RunsRead,
            RealtimeScope::ApprovalsRead,
            RealtimeScope::NodesRead,
            RealtimeScope::ConfigRead,
            RealtimeScope::EventsRead,
        ]),
        RealtimeRole::Agent => BTreeSet::from([
            RealtimeScope::RunsRead,
            RealtimeScope::RunsWrite,
            RealtimeScope::EventsRead,
        ]),
        RealtimeRole::Connector => BTreeSet::from([
            RealtimeScope::RunsRead,
            RealtimeScope::RunsWrite,
            RealtimeScope::ApprovalsRead,
            RealtimeScope::EventsRead,
        ]),
        RealtimeRole::Node => BTreeSet::from([RealtimeScope::NodesRead, RealtimeScope::NodesWrite]),
    };
    if !auth_subject.starts_with("admin:") {
        scopes.remove(&RealtimeScope::RunsWrite);
        scopes.remove(&RealtimeScope::ApprovalsWrite);
        scopes.remove(&RealtimeScope::NodesWrite);
        scopes.remove(&RealtimeScope::ConfigWrite);
        scopes.remove(&RealtimeScope::EventsSensitive);
    }
    scopes
}

fn capabilities_for_scopes(scopes: &BTreeSet<RealtimeScope>) -> BTreeSet<RealtimeCapability> {
    let mut capabilities = BTreeSet::new();
    if scopes.contains(&RealtimeScope::EventsRead) {
        capabilities.insert(RealtimeCapability::EventStream);
        capabilities.insert(RealtimeCapability::SnapshotRefresh);
    }
    if scopes.contains(&RealtimeScope::RunsWrite) {
        capabilities.insert(RealtimeCapability::RunControl);
    }
    if scopes.contains(&RealtimeScope::ApprovalsWrite) {
        capabilities.insert(RealtimeCapability::ApprovalControl);
    }
    if scopes.contains(&RealtimeScope::NodesRead) {
        capabilities.insert(RealtimeCapability::NodePresence);
    }
    if scopes.contains(&RealtimeScope::NodesWrite) {
        capabilities.insert(RealtimeCapability::CapabilityGrant);
    }
    if scopes.contains(&RealtimeScope::ConfigRead) {
        capabilities.insert(RealtimeCapability::ConfigSchemaLookup);
    }
    if scopes.contains(&RealtimeScope::ConfigRead) || scopes.contains(&RealtimeScope::ConfigWrite) {
        capabilities.insert(RealtimeCapability::ConfigReload);
    }
    if scopes.contains(&RealtimeScope::EventsSensitive) {
        capabilities.insert(RealtimeCapability::SensitiveEvents);
    }
    capabilities
}

fn commands_for_grants(
    scopes: &BTreeSet<RealtimeScope>,
    capabilities: &BTreeSet<RealtimeCapability>,
) -> BTreeSet<RealtimeCommand> {
    realtime_method_descriptors()
        .into_iter()
        .filter(|descriptor| {
            descriptor.required_scopes.iter().all(|scope| scopes.contains(scope))
                && descriptor
                    .required_capabilities
                    .iter()
                    .all(|capability| capabilities.contains(capability))
        })
        .map(|descriptor| descriptor.command)
        .collect()
}

fn descriptor(
    command: RealtimeCommand,
    required_scopes: &[RealtimeScope],
    required_capabilities: &[RealtimeCapability],
    idempotency_required: bool,
    side_effecting: bool,
    rate_limit_bucket: &str,
) -> RealtimeMethodDescriptor {
    RealtimeMethodDescriptor {
        command,
        version: 1,
        required_scopes: required_scopes.to_vec(),
        required_capabilities: required_capabilities.to_vec(),
        idempotency_required,
        side_effecting,
        rate_limit_bucket: rate_limit_bucket.to_owned(),
    }
}

fn topic_scope_allowed(context: &RealtimeConnectionContext, topic: RealtimeEventTopic) -> bool {
    match topic {
        RealtimeEventTopic::Run => {
            context.has_scope(RealtimeScope::RunsRead)
                && context.has_scope(RealtimeScope::EventsRead)
        }
        RealtimeEventTopic::Approval => {
            context.has_scope(RealtimeScope::ApprovalsRead)
                && context.has_scope(RealtimeScope::EventsRead)
        }
        RealtimeEventTopic::Node => {
            context.has_scope(RealtimeScope::NodesRead)
                && context.has_scope(RealtimeScope::EventsRead)
        }
        RealtimeEventTopic::Config => {
            context.has_scope(RealtimeScope::ConfigRead)
                && context.has_scope(RealtimeScope::EventsRead)
        }
        RealtimeEventTopic::System => context.has_scope(RealtimeScope::EventsRead),
    }
}

fn subscription_allows(subscription: &RealtimeSubscription, event: &RealtimeEventEnvelope) -> bool {
    let topic_matches =
        subscription.topics.is_empty() || subscription.topics.contains(&event.topic);
    let session_matches = subscription.session_ids.is_empty()
        || event
            .owner_session_id
            .as_ref()
            .is_some_and(|session_id| subscription.session_ids.contains(session_id));
    topic_matches && session_matches
}

fn intersection<T>(requested: &BTreeSet<T>, allowed: &BTreeSet<T>) -> BTreeSet<T>
where
    T: Copy + Ord,
{
    requested.intersection(allowed).copied().collect()
}

fn sorted_set<T>(set: BTreeSet<T>) -> Vec<T> {
    set.into_iter().collect()
}

fn realtime_error(
    code: impl Into<String>,
    message: impl Into<String>,
    recovery_hint: impl Into<String>,
) -> RealtimeErrorEnvelope {
    RealtimeErrorEnvelope {
        error: stable_error(code, message, recovery_hint),
        supported_protocol_versions: None,
    }
}

fn stable_error(
    code: impl Into<String>,
    message: impl Into<String>,
    recovery_hint: impl Into<String>,
) -> StableErrorEnvelope {
    StableErrorEnvelope::new(code, message, recovery_hint)
}

#[cfg(test)]
mod tests {
    use super::*;
    use palyra_common::runtime_contracts::{
        REALTIME_PROTOCOL_MAX_VERSION, REALTIME_PROTOCOL_MIN_VERSION,
    };

    #[test]
    fn handshake_rejects_incompatible_protocol() {
        let request = RealtimeHandshakeRequest {
            protocol_version: REALTIME_PROTOCOL_MAX_VERSION + 1,
            client_id: "client-a".to_owned(),
            role: RealtimeRole::Operator,
            requested_scopes: Vec::new(),
            requested_capabilities: Vec::new(),
            requested_commands: Vec::new(),
            event_cursor: None,
            subscriptions: Vec::new(),
            heartbeat_interval_ms: None,
        };
        let error = negotiate_realtime_handshake(request, "admin:test".to_owned(), 1)
            .expect_err("unsupported protocol should fail");
        assert_eq!(error.error.code, "realtime/incompatible_protocol");
        assert_eq!(error.supported_protocol_versions.unwrap().min, REALTIME_PROTOCOL_MIN_VERSION);
    }

    #[test]
    fn negotiation_grants_only_permitted_intersection() {
        let request = RealtimeHandshakeRequest {
            protocol_version: 1,
            client_id: "client-a".to_owned(),
            role: RealtimeRole::ReadOnly,
            requested_scopes: vec![RealtimeScope::RunsRead, RealtimeScope::ConfigWrite],
            requested_capabilities: Vec::new(),
            requested_commands: vec![RealtimeCommand::RunGet, RealtimeCommand::ConfigReloadApply],
            event_cursor: Some(RealtimeCursor { sequence: 3 }),
            subscriptions: vec![RealtimeSubscription {
                topics: vec![RealtimeEventTopic::Run],
                session_ids: vec!["session-a".to_owned()],
            }],
            heartbeat_interval_ms: Some(100),
        };
        let (accepted, context) =
            negotiate_realtime_handshake(request, "admin:test".to_owned(), 42).unwrap();
        assert_eq!(accepted.cursor.sequence, 3);
        assert_eq!(accepted.heartbeat_interval_ms, 5_000);
        assert!(accepted.scopes.contains(&RealtimeScope::RunsRead));
        assert_eq!(accepted.subscriptions[0].session_ids, vec!["session-a"]);
        assert!(!accepted.scopes.contains(&RealtimeScope::ConfigWrite));
        assert!(context.has_command(RealtimeCommand::RunGet));
        assert!(!context.has_command(RealtimeCommand::ConfigReloadApply));
    }

    #[test]
    fn event_router_filters_foreign_and_sensitive_events() {
        let request = RealtimeHandshakeRequest {
            protocol_version: 1,
            client_id: "client-a".to_owned(),
            role: RealtimeRole::ReadOnly,
            requested_scopes: vec![RealtimeScope::RunsRead, RealtimeScope::EventsRead],
            requested_capabilities: Vec::new(),
            requested_commands: vec![RealtimeCommand::RunEvents],
            event_cursor: None,
            subscriptions: Vec::new(),
            heartbeat_interval_ms: None,
        };
        let (_, context) = negotiate_realtime_handshake(request, "user:a".to_owned(), 1).unwrap();
        let mut router = RealtimeEventRouter::default();
        let _ = router.publish(event(
            "e1",
            RealtimeEventSensitivity::Internal,
            Some("user:a"),
            Some("session-a"),
        ));
        let _ = router.publish(event(
            "e2",
            RealtimeEventSensitivity::Internal,
            Some("user:b"),
            Some("session-b"),
        ));
        let _ = router.publish(event(
            "e3",
            RealtimeEventSensitivity::Sensitive,
            Some("user:a"),
            Some("session-a"),
        ));
        let RealtimeReplayOutcome::Events(events) =
            router.replay_from(&context, RealtimeCursor { sequence: 0 })
        else {
            panic!("expected replay events");
        };
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_id, "e1");
    }

    #[test]
    fn event_router_reports_gap_for_stale_cursor() {
        let request = RealtimeHandshakeRequest {
            protocol_version: 1,
            client_id: "client-a".to_owned(),
            role: RealtimeRole::Operator,
            requested_scopes: Vec::new(),
            requested_capabilities: Vec::new(),
            requested_commands: Vec::new(),
            event_cursor: None,
            subscriptions: Vec::new(),
            heartbeat_interval_ms: None,
        };
        let (_, context) =
            negotiate_realtime_handshake(request, "admin:test".to_owned(), 1).unwrap();
        let mut router = RealtimeEventRouter {
            capacity: 1,
            next_sequence: 1,
            events: VecDeque::with_capacity(1),
        };
        let _ = router.publish(event("e1", RealtimeEventSensitivity::Internal, None, None));
        let _ = router.publish(event("e2", RealtimeEventSensitivity::Internal, None, None));
        let RealtimeReplayOutcome::SnapshotRequired { first_available_sequence, .. } =
            router.replay_from(&context, RealtimeCursor { sequence: 0 })
        else {
            panic!("expected snapshot refresh");
        };
        assert_eq!(first_available_sequence, 2);
    }

    #[test]
    fn command_authorization_requires_idempotency_for_side_effects() {
        let request = RealtimeHandshakeRequest {
            protocol_version: 1,
            client_id: "client-a".to_owned(),
            role: RealtimeRole::Operator,
            requested_scopes: Vec::new(),
            requested_capabilities: Vec::new(),
            requested_commands: vec![RealtimeCommand::RunAbort],
            event_cursor: None,
            subscriptions: Vec::new(),
            heartbeat_interval_ms: None,
        };
        let (_, context) =
            negotiate_realtime_handshake(request, "admin:test".to_owned(), 1).unwrap();
        let error = authorize_realtime_command(&context, RealtimeCommand::RunAbort, None)
            .expect_err("side effect without idempotency should fail");
        assert_eq!(error.code, "realtime/idempotency_required");
        assert!(
            authorize_realtime_command(&context, RealtimeCommand::RunAbort, Some("idem-1")).is_ok()
        );
    }

    #[test]
    fn method_registry_golden_abi_stays_versioned() {
        let methods = realtime_method_descriptors();
        let wire_names = methods
            .iter()
            .map(|method| {
                format!(
                    "{}:v{}:{}:{}",
                    method.command.as_str(),
                    method.version,
                    method.required_scopes.len(),
                    method.idempotency_required
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            wire_names,
            vec![
                "run.create:v1:1:true",
                "run.wait:v1:1:false",
                "run.events:v1:2:false",
                "run.abort:v1:1:true",
                "run.get:v1:1:false",
                "approval.list:v1:1:false",
                "approval.get:v1:1:false",
                "approval.decide:v1:1:true",
                "node.presence:v1:1:false",
                "node.capability.grant:v1:1:true",
                "node.capability.revoke:v1:1:true",
                "config.schema.lookup:v1:1:false",
                "config.reload.plan:v1:1:false",
                "config.reload.apply:v1:1:true",
            ]
        );
        assert_eq!(REALTIME_SDK_ABI_VERSION, "palyra.realtime.sdk.v1");
    }

    #[test]
    fn rate_limiter_is_scoped_to_method_subject_and_client() {
        let request = RealtimeHandshakeRequest {
            protocol_version: 1,
            client_id: "client-a".to_owned(),
            role: RealtimeRole::Operator,
            requested_scopes: Vec::new(),
            requested_capabilities: Vec::new(),
            requested_commands: vec![RealtimeCommand::RunGet],
            event_cursor: None,
            subscriptions: Vec::new(),
            heartbeat_interval_ms: None,
        };
        let (_, context) =
            negotiate_realtime_handshake(request, "admin:test".to_owned(), 1).unwrap();
        let mut limiter =
            RealtimeRateLimiter { window_ms: 1_000, max_requests: 1, buckets: BTreeMap::new() };
        assert!(limiter.check(&context, RealtimeCommand::RunGet, 10).is_ok());
        let error = limiter
            .check(&context, RealtimeCommand::RunGet, 11)
            .expect_err("second request in same window should fail");
        assert_eq!(error.code, "realtime/rate_limited");

        let mut other_client = context.clone();
        other_client.client_id = "client-b".to_owned();
        assert!(limiter.check(&other_client, RealtimeCommand::RunGet, 12).is_ok());
    }

    fn event(
        event_id: &str,
        sensitivity: RealtimeEventSensitivity,
        owner_principal: Option<&str>,
        owner_session_id: Option<&str>,
    ) -> RealtimeEventEnvelope {
        RealtimeEventEnvelope {
            schema_version: 1,
            sequence: 0,
            event_id: event_id.to_owned(),
            topic: RealtimeEventTopic::Run,
            sensitivity,
            owner_principal: owner_principal.map(str::to_owned),
            owner_session_id: owner_session_id.map(str::to_owned),
            occurred_at_unix_ms: 1,
            payload: json!({ "event": event_id }),
        }
    }
}
