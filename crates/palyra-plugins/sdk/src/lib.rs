//! Stable contract identifiers and typed plugin contracts for the Palyra plugin SDK.
//!
//! Defines the WIT/ABI identifier constants shared by the host and plugins, the
//! host-published typed plugin contract descriptors, and a fixture-driven
//! negotiation simulator used by contract tests. The WIT source of truth lives
//! in `wit/palyra-sdk.wit`; identifier strings in this module are wire contract
//! and are pinned by a golden ABI fingerprint test.

use serde::{Deserialize, Serialize};

/// WIT package identifier for the bootstrap plugin SDK contract.
pub const WIT_PACKAGE_ID: &str = "palyra:plugins/sdk@0.1.0";
/// Host/plugin ABI marker independent from the package crate version.
pub const SDK_ABI_VERSION: &str = "palyra.plugins.sdk.abi.v1";
/// Current SDK ABI major version.
pub const SDK_ABI_MAJOR: u32 = 1;
/// Oldest SDK ABI major version accepted by this host.
pub const SDK_ABI_MIN_MAJOR: u32 = 1;
/// Newest SDK ABI major version accepted by this host.
pub const SDK_ABI_MAX_MAJOR: u32 = 1;
/// WIT world exported by plugin modules.
pub const WIT_WORLD_NAME: &str = "palyra-plugin";
/// Core Wasm import module that exposes Tier A capability handles.
pub const HOST_CAPABILITIES_IMPORT_MODULE: &str = "palyra:plugins/host-capabilities@0.1.0";

// Function names below are the Tier A capability contract exposed through
// `HOST_CAPABILITIES_IMPORT_MODULE`.
/// Host import returning the number of granted HTTP host handles.
pub const HOST_CAPABILITY_HTTP_COUNT_FN: &str = "http-count";
/// Host import resolving an HTTP host handle by index.
pub const HOST_CAPABILITY_HTTP_HANDLE_FN: &str = "http-handle";
/// Host import returning the number of granted secret handles.
pub const HOST_CAPABILITY_SECRET_COUNT_FN: &str = "secret-count";
/// Host import resolving a secret handle by index.
pub const HOST_CAPABILITY_SECRET_HANDLE_FN: &str = "secret-handle";
/// Host import returning the number of granted storage-prefix handles.
pub const HOST_CAPABILITY_STORAGE_COUNT_FN: &str = "storage-count";
/// Host import resolving a storage-prefix handle by index.
pub const HOST_CAPABILITY_STORAGE_HANDLE_FN: &str = "storage-handle";
/// Host import returning the number of granted channel handles.
pub const HOST_CAPABILITY_CHANNEL_COUNT_FN: &str = "channel-count";
/// Host import resolving a channel handle by index.
pub const HOST_CAPABILITY_CHANNEL_HANDLE_FN: &str = "channel-handle";

/// Default plugin entrypoint exported by the runtime interface.
pub const DEFAULT_RUNTIME_ENTRYPOINT: &str = "run";
/// Source of truth WIT document embedded for tooling/tests.
pub const WIT_SOURCE: &str = include_str!("../wit/palyra-sdk.wit");
/// Initial contract version for typed plugin extension points.
pub const DEFAULT_TYPED_PLUGIN_CONTRACT_VERSION: u32 = 1;
/// Default per-invocation timeout for typed plugin contracts.
pub const DEFAULT_TYPED_PLUGIN_CONTRACT_TIMEOUT_MS: u64 = 2_000;

/// Typed plugin extension points the host can negotiate.
///
/// Wire identifiers come from [`TypedPluginContractKind::as_str`] and are
/// stable contract surface.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum TypedPluginContractKind {
    MemoryProvider,
    ContextEngine,
    RoutingStrategy,
    RunLifecycleHook,
    CompactionProvider,
    DiagnosticsProvider,
    PolicySignalProvider,
    ChannelBindingProvider,
    DeliveryAdapter,
    SchedulerTaskProvider,
    ModelAuthProvider,
    ConnectorAdapter,
}

impl TypedPluginContractKind {
    /// Returns the stable snake_case wire identifier for this contract kind.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MemoryProvider => "memory_provider",
            Self::ContextEngine => "context_engine",
            Self::RoutingStrategy => "routing_strategy",
            Self::RunLifecycleHook => "run_lifecycle_hook",
            Self::CompactionProvider => "compaction_provider",
            Self::DiagnosticsProvider => "diagnostics_provider",
            Self::PolicySignalProvider => "policy_signal_provider",
            Self::ChannelBindingProvider => "channel_binding_provider",
            Self::DeliveryAdapter => "delivery_adapter",
            Self::SchedulerTaskProvider => "scheduler_task_provider",
            Self::ModelAuthProvider => "model_auth_provider",
            Self::ConnectorAdapter => "connector_adapter",
        }
    }
}

/// Capability classes a typed contract may be allowed to request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum TypedPluginCapabilityClass {
    HttpHosts,
    Secrets,
    StoragePrefixes,
    Channels,
}

impl TypedPluginCapabilityClass {
    /// Returns the stable snake_case wire identifier for this capability class.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HttpHosts => "http_hosts",
            Self::Secrets => "secrets",
            Self::StoragePrefixes => "storage_prefixes",
            Self::Channels => "channels",
        }
    }
}

/// Capability-scoped host services that a Wasm plugin may request through the
/// daemon host boundary.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum HostCapabilityServiceKind {
    Logging,
    Events,
    ConfigLookup,
    TasksCreate,
    TasksUpdate,
    FlowSignal,
    ChannelSendIntent,
    MemoryProposeCandidate,
    BoundedLlmComplete,
    VaultSecretLeaseRequest,
}

impl HostCapabilityServiceKind {
    /// Returns the stable snake_case wire identifier for this host service.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Logging => "logging",
            Self::Events => "events",
            Self::ConfigLookup => "config_lookup",
            Self::TasksCreate => "tasks_create",
            Self::TasksUpdate => "tasks_update",
            Self::FlowSignal => "flow_signal",
            Self::ChannelSendIntent => "channel_send_intent",
            Self::MemoryProposeCandidate => "memory_propose_candidate",
            Self::BoundedLlmComplete => "bounded_llm_complete",
            Self::VaultSecretLeaseRequest => "vault_secret_lease_request",
        }
    }
}

/// Host-published descriptor for one capability-scoped service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HostCapabilityServiceDescriptor {
    /// Host service kind.
    pub service: HostCapabilityServiceKind,
    /// Optional Tier A capability class that must also be granted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_capability_class: Option<TypedPluginCapabilityClass>,
    /// Default host-side timeout in milliseconds.
    pub default_timeout_ms: u64,
    /// Maximum accepted request payload size.
    pub max_payload_bytes: u64,
    /// Stable audit event emitted around invocation or denial.
    pub audit_event: String,
    /// Payload field paths redacted from logs and audit output.
    #[serde(default)]
    pub redacted_fields: Vec<String>,
    /// Whether the service can return raw secret material.
    pub returns_secret_material: bool,
}

/// Sensitivity classification applied to a contract's payload data.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum TypedPluginDataSensitivity {
    Public,
    #[default]
    Internal,
    Sensitive,
    Secret,
}

impl TypedPluginDataSensitivity {
    /// Returns the stable snake_case wire identifier for this sensitivity level.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Sensitive => "sensitive",
            Self::Secret => "secret",
        }
    }
}

/// Lifecycle phases a typed plugin contract moves through on the host.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TypedPluginContractLifecyclePhase {
    Discover,
    Negotiate,
    Bind,
    Invoke,
    Audit,
}

impl TypedPluginContractLifecyclePhase {
    /// Returns the stable snake_case wire identifier for this lifecycle phase.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Discover => "discover",
            Self::Negotiate => "negotiate",
            Self::Bind => "bind",
            Self::Invoke => "invoke",
            Self::Audit => "audit",
        }
    }
}

/// Operations a typed plugin contract can be invoked with.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TypedPluginContractOperation {
    ProvideMemory,
    PlanContext,
    RouteModel,
    DecideRunLifecycle,
    CompactContext,
    EmitDiagnostics,
    EmitPolicySignal,
    DiscoverBinding,
    ValidateBinding,
    RepairBindingHint,
    ExplainBinding,
    DeliverMessage,
    ScheduleTask,
    ResolveModelAuth,
    ConnectorInbound,
    ConnectorOutbound,
    ConnectorRateLimit,
}

impl TypedPluginContractOperation {
    /// Returns the stable snake_case wire identifier for this operation.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProvideMemory => "provide_memory",
            Self::PlanContext => "plan_context",
            Self::RouteModel => "route_model",
            Self::DecideRunLifecycle => "decide_run_lifecycle",
            Self::CompactContext => "compact_context",
            Self::EmitDiagnostics => "emit_diagnostics",
            Self::EmitPolicySignal => "emit_policy_signal",
            Self::DiscoverBinding => "discover_binding",
            Self::ValidateBinding => "validate_binding",
            Self::RepairBindingHint => "repair_binding_hint",
            Self::ExplainBinding => "explain_binding",
            Self::DeliverMessage => "deliver_message",
            Self::ScheduleTask => "schedule_task",
            Self::ResolveModelAuth => "resolve_model_auth",
            Self::ConnectorInbound => "connector_inbound",
            Self::ConnectorOutbound => "connector_outbound",
            Self::ConnectorRateLimit => "connector_rate_limit",
        }
    }
}

/// A plugin's declaration that it implements one typed contract kind and version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TypedPluginContractDeclaration {
    /// Contract kind being declared.
    pub kind: TypedPluginContractKind,
    /// Contract version; defaults to [`DEFAULT_TYPED_PLUGIN_CONTRACT_VERSION`]
    /// when omitted.
    #[serde(default = "default_typed_plugin_contract_version")]
    pub version: u32,
}

/// Test fixture pairing typed contract declarations with the negotiation
/// outcome they are expected to produce.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SdkContractSimulationFixture {
    /// Unique fixture name; must not be blank.
    pub name: String,
    /// Whether the simulated negotiation is expected to accept the fixture.
    pub expected_accepted: bool,
    /// Typed contracts the simulated plugin declares.
    pub declarations: Vec<TypedPluginContractDeclaration>,
    /// Capability classes the simulated plugin requests.
    #[serde(default)]
    pub requested_capability_classes: Vec<TypedPluginCapabilityClass>,
}

/// Result of simulating one fixture against the SDK's host negotiation rules.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SdkContractSimulationReport {
    /// Name of the fixture that was simulated.
    pub fixture_name: String,
    /// Whether the simulated negotiation accepted the fixture.
    pub accepted: bool,
    /// Number of declared contracts the host publishes a descriptor for.
    pub supported_contract_count: usize,
    /// Human-readable rejection reasons; empty when accepted.
    pub rejected_reasons: Vec<String>,
}

/// Host-published descriptor for one supported typed contract version.
///
/// Every field is ABI surface: the golden fingerprint test in this crate pins
/// descriptor contents, so any change here is a contract change.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TypedPluginContractDescriptor {
    /// Contract kind the descriptor applies to.
    pub kind: TypedPluginContractKind,
    /// Contract version the descriptor applies to.
    pub version: u32,
    /// SDK ABI major version the descriptor was authored against.
    #[serde(default = "default_sdk_abi_major")]
    pub sdk_abi_major: u32,
    /// Versioned identifier of the input payload schema.
    #[serde(default)]
    pub input_schema: String,
    /// Versioned identifier of the output payload schema.
    #[serde(default)]
    pub output_schema: String,
    /// Default per-invocation timeout in milliseconds.
    #[serde(default = "default_typed_plugin_contract_timeout_ms")]
    pub default_timeout_ms: u64,
    /// Default sensitivity classification of contract payloads.
    #[serde(default)]
    pub sensitivity: TypedPluginDataSensitivity,
    /// Lifecycle phases the host drives for this contract.
    #[serde(default)]
    pub lifecycle: Vec<TypedPluginContractLifecyclePhase>,
    /// Operations the contract can be invoked with.
    #[serde(default)]
    pub operations: Vec<TypedPluginContractOperation>,
    /// Capability classes plugins of this contract may request; empty means none.
    #[serde(default)]
    pub allowed_capability_classes: Vec<TypedPluginCapabilityClass>,
    /// Stable error codes the contract may surface.
    #[serde(default)]
    pub error_codes: Vec<String>,
    /// Payload field paths that must be redacted from logs and audit output.
    #[serde(default)]
    pub redacted_fields: Vec<String>,
    /// Audit event hooks emitted around contract invocations.
    #[serde(default)]
    pub audit_hooks: Vec<String>,
    /// Observability/metric hooks emitted for the contract.
    #[serde(default)]
    pub observability_hooks: Vec<String>,
}

/// Inclusive range of SDK ABI major versions accepted by a host.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SdkAbiCompatibility {
    /// ABI major version the host itself targets.
    pub abi_major: u32,
    /// Oldest ABI major version the host accepts.
    pub min_abi_major: u32,
    /// Newest ABI major version the host accepts.
    pub max_abi_major: u32,
}

impl SdkAbiCompatibility {
    /// Returns `true` when `abi_major` falls inside the accepted inclusive range.
    #[must_use]
    pub const fn accepts(self, abi_major: u32) -> bool {
        abi_major >= self.min_abi_major && abi_major <= self.max_abi_major
    }
}

/// Returns the WIT package identifier.
#[must_use]
pub fn wit_package_id() -> &'static str {
    WIT_PACKAGE_ID
}

/// Returns embedded WIT source text.
#[must_use]
pub fn wit_source() -> &'static str {
    WIT_SOURCE
}

/// Returns the SDK ABI marker independent from the package crate version.
#[must_use]
pub fn sdk_abi_version() -> &'static str {
    SDK_ABI_VERSION
}

/// Returns the SDK ABI compatibility range supported by this host.
#[must_use]
pub const fn sdk_abi_compatibility() -> SdkAbiCompatibility {
    SdkAbiCompatibility {
        abi_major: SDK_ABI_MAJOR,
        min_abi_major: SDK_ABI_MIN_MAJOR,
        max_abi_major: SDK_ABI_MAX_MAJOR,
    }
}

/// Returns the default version for typed plugin contracts.
#[must_use]
pub fn default_typed_plugin_contract_version() -> u32 {
    DEFAULT_TYPED_PLUGIN_CONTRACT_VERSION
}

/// Returns the default SDK ABI major version for persisted descriptor compatibility.
#[must_use]
pub fn default_sdk_abi_major() -> u32 {
    SDK_ABI_MAJOR
}

/// Returns the default timeout for typed plugin contract invocations.
#[must_use]
pub fn default_typed_plugin_contract_timeout_ms() -> u64 {
    DEFAULT_TYPED_PLUGIN_CONTRACT_TIMEOUT_MS
}

/// Returns all typed plugin contract kinds known by this SDK.
#[must_use]
pub fn all_typed_plugin_contract_kinds() -> Vec<TypedPluginContractKind> {
    vec![
        TypedPluginContractKind::MemoryProvider,
        TypedPluginContractKind::ContextEngine,
        TypedPluginContractKind::RoutingStrategy,
        TypedPluginContractKind::RunLifecycleHook,
        TypedPluginContractKind::CompactionProvider,
        TypedPluginContractKind::DiagnosticsProvider,
        TypedPluginContractKind::PolicySignalProvider,
        TypedPluginContractKind::ChannelBindingProvider,
        TypedPluginContractKind::DeliveryAdapter,
        TypedPluginContractKind::SchedulerTaskProvider,
        TypedPluginContractKind::ModelAuthProvider,
        TypedPluginContractKind::ConnectorAdapter,
    ]
}

/// Returns the built-in descriptor for a typed plugin contract version supported by the host.
///
/// Returns `None` for any version other than
/// [`DEFAULT_TYPED_PLUGIN_CONTRACT_VERSION`], which is the only version this
/// SDK ABI publishes.
#[must_use]
pub fn typed_plugin_contract_descriptor(
    kind: TypedPluginContractKind,
    version: u32,
) -> Option<TypedPluginContractDescriptor> {
    if version != DEFAULT_TYPED_PLUGIN_CONTRACT_VERSION {
        return None;
    }

    let descriptor = match kind {
        TypedPluginContractKind::MemoryProvider => build_descriptor(
            kind,
            version,
            "palyra.plugin.memory_provider.input.v1",
            "palyra.plugin.memory_provider.output.v1",
            TypedPluginDataSensitivity::Sensitive,
            vec![TypedPluginContractOperation::ProvideMemory],
            default_data_capabilities(),
            vec![
                "contract_negotiation_failed".to_owned(),
                "embed_request_failed".to_owned(),
                "invalid_embedding_dimensions".to_owned(),
                "degraded_fallback_active".to_owned(),
            ],
            vec![
                "config.credentials".to_owned(),
                "request.input_text".to_owned(),
                "response.embedding_preview".to_owned(),
            ],
            vec![
                "binding.negotiated".to_owned(),
                "memory.embedding_runtime".to_owned(),
                "memory.embedding_failure".to_owned(),
            ],
            vec![
                "memory.embedding_runtime".to_owned(),
                "memory.embedding_backfill".to_owned(),
                "memory.embedding_degradation".to_owned(),
            ],
        ),
        TypedPluginContractKind::ContextEngine => build_descriptor(
            kind,
            version,
            "palyra.plugin.context_engine.input.v1",
            "palyra.plugin.context_engine.output.v1",
            TypedPluginDataSensitivity::Sensitive,
            vec![TypedPluginContractOperation::PlanContext],
            default_data_capabilities(),
            vec![
                "contract_negotiation_failed".to_owned(),
                "context_plan_failed".to_owned(),
                "segment_budget_exceeded".to_owned(),
                "segment_redaction_required".to_owned(),
            ],
            vec![
                "context.segment.preview".to_owned(),
                "context.compaction.summary".to_owned(),
                "context.references.inline".to_owned(),
            ],
            vec![
                "binding.negotiated".to_owned(),
                "context.plan".to_owned(),
                "context.segment_drop".to_owned(),
            ],
            vec![
                "context.plan".to_owned(),
                "context.compaction".to_owned(),
                "context.recall_mix".to_owned(),
            ],
        ),
        TypedPluginContractKind::RoutingStrategy => build_descriptor(
            kind,
            version,
            "palyra.plugin.routing_strategy.input.v1",
            "palyra.plugin.routing_strategy.output.v1",
            TypedPluginDataSensitivity::Sensitive,
            vec![TypedPluginContractOperation::RouteModel],
            default_data_capabilities(),
            vec![
                "contract_negotiation_failed".to_owned(),
                "routing_decision_failed".to_owned(),
                "budget_gate_blocked".to_owned(),
                "provider_selection_rejected".to_owned(),
            ],
            vec![
                "routing.prompt_preview".to_owned(),
                "routing.provider_credentials".to_owned(),
                "routing.override_payload".to_owned(),
            ],
            vec![
                "binding.negotiated".to_owned(),
                "routing.decision".to_owned(),
                "routing.budget_gate".to_owned(),
            ],
            vec![
                "routing.decision".to_owned(),
                "routing.model_mix".to_owned(),
                "routing.provider_health".to_owned(),
            ],
        ),
        TypedPluginContractKind::RunLifecycleHook => build_descriptor(
            kind,
            version,
            "palyra.plugin.run_lifecycle_hook.input.v1",
            "palyra.plugin.run_lifecycle_hook.decision.v1",
            TypedPluginDataSensitivity::Sensitive,
            vec![TypedPluginContractOperation::DecideRunLifecycle],
            Vec::new(),
            vec![
                "contract_negotiation_failed".to_owned(),
                "hook_timeout".to_owned(),
                "hook_panic".to_owned(),
                "terminal_decision_conflict".to_owned(),
            ],
            vec!["hook.payload.preview".to_owned(), "hook.decision.reason".to_owned()],
            vec![
                "hook.dispatched".to_owned(),
                "hook.failed".to_owned(),
                "hook.decision".to_owned(),
            ],
            vec!["hook.latency".to_owned(), "hook.terminal_decision".to_owned()],
        ),
        TypedPluginContractKind::CompactionProvider => build_descriptor(
            kind,
            version,
            "palyra.plugin.compaction_provider.input.v1",
            "palyra.plugin.compaction_provider.output.v1",
            TypedPluginDataSensitivity::Sensitive,
            vec![TypedPluginContractOperation::CompactContext],
            default_data_capabilities(),
            vec![
                "contract_negotiation_failed".to_owned(),
                "compaction_budget_exceeded".to_owned(),
                "compaction_output_invalid".to_owned(),
            ],
            vec!["compaction.input.preview".to_owned(), "compaction.output.summary".to_owned()],
            vec!["compaction.provider_invoked".to_owned(), "compaction.provider_failed".to_owned()],
            vec!["compaction.provider_latency".to_owned()],
        ),
        TypedPluginContractKind::DiagnosticsProvider => build_descriptor(
            kind,
            version,
            "palyra.plugin.diagnostics_provider.input.v1",
            "palyra.plugin.diagnostics_provider.output.v1",
            TypedPluginDataSensitivity::Sensitive,
            vec![TypedPluginContractOperation::EmitDiagnostics],
            vec![TypedPluginCapabilityClass::StoragePrefixes],
            vec![
                "contract_negotiation_failed".to_owned(),
                "diagnostics_redaction_required".to_owned(),
                "diagnostics_output_invalid".to_owned(),
            ],
            vec!["diagnostics.raw_payload".to_owned(), "diagnostics.secret_like_values".to_owned()],
            vec!["diagnostics.provider_invoked".to_owned()],
            vec!["diagnostics.provider_latency".to_owned()],
        ),
        TypedPluginContractKind::PolicySignalProvider => build_descriptor(
            kind,
            version,
            "palyra.plugin.policy_signal_provider.input.v1",
            "palyra.plugin.policy_signal_provider.output.v1",
            TypedPluginDataSensitivity::Sensitive,
            vec![TypedPluginContractOperation::EmitPolicySignal],
            Vec::new(),
            vec!["contract_negotiation_failed".to_owned(), "policy_signal_invalid".to_owned()],
            vec!["policy.signal.context".to_owned()],
            vec!["policy.signal.provider_invoked".to_owned()],
            vec!["policy.signal.count".to_owned()],
        ),
        TypedPluginContractKind::ChannelBindingProvider => build_descriptor(
            kind,
            version,
            "palyra.plugin.channel_binding_provider.input.v1",
            "palyra.plugin.channel_binding_provider.output.v1",
            TypedPluginDataSensitivity::Internal,
            vec![
                TypedPluginContractOperation::DiscoverBinding,
                TypedPluginContractOperation::ValidateBinding,
                TypedPluginContractOperation::RepairBindingHint,
                TypedPluginContractOperation::ExplainBinding,
            ],
            vec![TypedPluginCapabilityClass::Channels],
            vec![
                "contract_negotiation_failed".to_owned(),
                "binding_collision".to_owned(),
                "binding_validation_failed".to_owned(),
            ],
            vec!["binding.external_identity".to_owned()],
            vec!["binding.provider_invoked".to_owned(), "binding.repair_hint".to_owned()],
            vec!["binding.provider_latency".to_owned()],
        ),
        TypedPluginContractKind::DeliveryAdapter => build_descriptor(
            kind,
            version,
            "palyra.plugin.delivery_adapter.input.v1",
            "palyra.plugin.delivery_adapter.receipt.v1",
            TypedPluginDataSensitivity::Sensitive,
            vec![TypedPluginContractOperation::DeliverMessage],
            vec![TypedPluginCapabilityClass::Channels],
            vec![
                "contract_negotiation_failed".to_owned(),
                "delivery_ack_unknown".to_owned(),
                "delivery_duplicate_ack".to_owned(),
                "delivery_nack".to_owned(),
            ],
            vec!["delivery.message_preview".to_owned()],
            vec!["delivery.adapter_invoked".to_owned(), "delivery.receipt".to_owned()],
            vec!["delivery.latency".to_owned(), "delivery.retry_class".to_owned()],
        ),
        TypedPluginContractKind::SchedulerTaskProvider => build_descriptor(
            kind,
            version,
            "palyra.plugin.scheduler_task_provider.input.v1",
            "palyra.plugin.scheduler_task_provider.output.v1",
            TypedPluginDataSensitivity::Internal,
            vec![TypedPluginContractOperation::ScheduleTask],
            vec![TypedPluginCapabilityClass::StoragePrefixes],
            vec![
                "contract_negotiation_failed".to_owned(),
                "scheduler_task_invalid".to_owned(),
                "wake_gate_denied".to_owned(),
            ],
            vec!["scheduler.task_payload".to_owned()],
            vec!["scheduler.provider_invoked".to_owned()],
            vec!["scheduler.task_count".to_owned()],
        ),
        TypedPluginContractKind::ModelAuthProvider => build_descriptor(
            kind,
            version,
            "palyra.plugin.model_auth_provider.input.v1",
            "palyra.plugin.model_auth_provider.output.v1",
            TypedPluginDataSensitivity::Secret,
            vec![TypedPluginContractOperation::ResolveModelAuth],
            vec![TypedPluginCapabilityClass::Secrets],
            vec![
                "contract_negotiation_failed".to_owned(),
                "credential_handle_missing".to_owned(),
                "rate_limit_metadata_invalid".to_owned(),
            ],
            vec!["model_auth.credential_handle".to_owned()],
            vec!["model_auth.provider_invoked".to_owned()],
            vec!["model_auth.rate_limit".to_owned()],
        ),
        TypedPluginContractKind::ConnectorAdapter => build_descriptor(
            kind,
            version,
            "palyra.plugin.connector_adapter.input.v1",
            "palyra.plugin.connector_adapter.output.v1",
            TypedPluginDataSensitivity::Sensitive,
            vec![
                TypedPluginContractOperation::ConnectorInbound,
                TypedPluginContractOperation::ConnectorOutbound,
                TypedPluginContractOperation::ConnectorRateLimit,
                TypedPluginContractOperation::DiscoverBinding,
            ],
            vec![
                TypedPluginCapabilityClass::HttpHosts,
                TypedPluginCapabilityClass::Secrets,
                TypedPluginCapabilityClass::StoragePrefixes,
                TypedPluginCapabilityClass::Channels,
            ],
            vec![
                "contract_negotiation_failed".to_owned(),
                "connector_reconnect_failed".to_owned(),
                "connector_duplicate_inbound".to_owned(),
                "connector_delivery_nack".to_owned(),
            ],
            vec![
                "connector.inbound.body".to_owned(),
                "connector.outbound.body".to_owned(),
                "connector.auth.handle".to_owned(),
            ],
            vec!["connector.adapter_invoked".to_owned(), "connector.binding_discovered".to_owned()],
            vec!["connector.rate_limit".to_owned(), "connector.delivery_latency".to_owned()],
        ),
    };
    Some(descriptor)
}

/// Runs a typed-contract fixture against the SDK's simulated host negotiation rules.
///
/// A fixture is rejected when its name is blank, it declares no contracts, a
/// declared kind/version has no host descriptor, or it requests a capability
/// class that none of its declared contracts allow.
#[must_use]
pub fn simulate_sdk_contract_fixture(
    fixture: &SdkContractSimulationFixture,
) -> SdkContractSimulationReport {
    let mut rejected_reasons = Vec::new();
    if fixture.name.trim().is_empty() {
        rejected_reasons.push("fixture name cannot be empty".to_owned());
    }
    if fixture.declarations.is_empty() {
        rejected_reasons.push("fixture must declare at least one typed contract".to_owned());
    }

    let mut supported_contract_count = 0_usize;
    let mut allowed_capability_classes = Vec::<TypedPluginCapabilityClass>::new();
    for declaration in &fixture.declarations {
        let Some(descriptor) =
            typed_plugin_contract_descriptor(declaration.kind, declaration.version)
        else {
            rejected_reasons.push(format!(
                "{}@{} is not supported by SDK ABI {}",
                declaration.kind.as_str(),
                declaration.version,
                SDK_ABI_MAJOR
            ));
            continue;
        };
        supported_contract_count += 1;
        for capability in descriptor.allowed_capability_classes {
            if !allowed_capability_classes.contains(&capability) {
                allowed_capability_classes.push(capability);
            }
        }
    }

    for requested in &fixture.requested_capability_classes {
        if !allowed_capability_classes.contains(requested) {
            rejected_reasons.push(format!(
                "capability class '{}' is not allowed by declared typed contracts",
                requested.as_str()
            ));
        }
    }

    SdkContractSimulationReport {
        fixture_name: fixture.name.clone(),
        accepted: rejected_reasons.is_empty(),
        supported_contract_count,
        rejected_reasons,
    }
}

/// Returns built-in good and bad fixtures used by SDK contract tests.
#[must_use]
pub fn built_in_sdk_contract_fixtures() -> Vec<SdkContractSimulationFixture> {
    vec![
        SdkContractSimulationFixture {
            name: "good.delivery_adapter.channel_only".to_owned(),
            expected_accepted: true,
            declarations: vec![TypedPluginContractDeclaration {
                kind: TypedPluginContractKind::DeliveryAdapter,
                version: DEFAULT_TYPED_PLUGIN_CONTRACT_VERSION,
            }],
            requested_capability_classes: vec![TypedPluginCapabilityClass::Channels],
        },
        SdkContractSimulationFixture {
            name: "bad.run_lifecycle_hook.secret_capability".to_owned(),
            expected_accepted: false,
            declarations: vec![TypedPluginContractDeclaration {
                kind: TypedPluginContractKind::RunLifecycleHook,
                version: DEFAULT_TYPED_PLUGIN_CONTRACT_VERSION,
            }],
            requested_capability_classes: vec![TypedPluginCapabilityClass::Secrets],
        },
        SdkContractSimulationFixture {
            name: "bad.connector_adapter.incompatible_abi".to_owned(),
            expected_accepted: false,
            declarations: vec![TypedPluginContractDeclaration {
                kind: TypedPluginContractKind::ConnectorAdapter,
                version: DEFAULT_TYPED_PLUGIN_CONTRACT_VERSION + 1,
            }],
            requested_capability_classes: vec![TypedPluginCapabilityClass::Channels],
        },
    ]
}

/// Returns the full set of built-in typed plugin contracts supported by the host.
#[must_use]
pub fn supported_typed_plugin_contracts() -> Vec<TypedPluginContractDescriptor> {
    all_typed_plugin_contract_kinds()
        .into_iter()
        .filter_map(|kind| {
            typed_plugin_contract_descriptor(kind, DEFAULT_TYPED_PLUGIN_CONTRACT_VERSION)
        })
        .collect()
}

/// Returns every capability-scoped host service descriptor published by the SDK.
#[must_use]
pub fn supported_host_capability_services() -> Vec<HostCapabilityServiceDescriptor> {
    [
        HostCapabilityServiceKind::Logging,
        HostCapabilityServiceKind::Events,
        HostCapabilityServiceKind::ConfigLookup,
        HostCapabilityServiceKind::TasksCreate,
        HostCapabilityServiceKind::TasksUpdate,
        HostCapabilityServiceKind::FlowSignal,
        HostCapabilityServiceKind::ChannelSendIntent,
        HostCapabilityServiceKind::MemoryProposeCandidate,
        HostCapabilityServiceKind::BoundedLlmComplete,
        HostCapabilityServiceKind::VaultSecretLeaseRequest,
    ]
    .into_iter()
    .map(host_capability_service_descriptor)
    .collect()
}

/// Returns the descriptor for one capability-scoped host service.
#[must_use]
pub fn host_capability_service_descriptor(
    service: HostCapabilityServiceKind,
) -> HostCapabilityServiceDescriptor {
    let (required_capability_class, default_timeout_ms, max_payload_bytes, redacted_fields) =
        match service {
            HostCapabilityServiceKind::Logging | HostCapabilityServiceKind::Events => {
                (None, 250, 8 * 1024, Vec::new())
            }
            HostCapabilityServiceKind::ConfigLookup => {
                (None, 500, 16 * 1024, vec!["config.default_value".to_owned()])
            }
            HostCapabilityServiceKind::TasksCreate | HostCapabilityServiceKind::TasksUpdate => (
                Some(TypedPluginCapabilityClass::StoragePrefixes),
                1_000,
                32 * 1024,
                vec!["task.payload".to_owned()],
            ),
            HostCapabilityServiceKind::FlowSignal => {
                (None, 500, 16 * 1024, vec!["flow.context".to_owned()])
            }
            HostCapabilityServiceKind::ChannelSendIntent => (
                Some(TypedPluginCapabilityClass::Channels),
                1_000,
                32 * 1024,
                vec!["message.body".to_owned()],
            ),
            HostCapabilityServiceKind::MemoryProposeCandidate => (
                Some(TypedPluginCapabilityClass::StoragePrefixes),
                1_000,
                32 * 1024,
                vec!["candidate.content".to_owned()],
            ),
            HostCapabilityServiceKind::BoundedLlmComplete => (
                Some(TypedPluginCapabilityClass::HttpHosts),
                2_000,
                64 * 1024,
                vec!["prompt".to_owned(), "completion.preview".to_owned()],
            ),
            HostCapabilityServiceKind::VaultSecretLeaseRequest => (
                Some(TypedPluginCapabilityClass::Secrets),
                1_000,
                8 * 1024,
                vec!["secret.ref".to_owned(), "lease.metadata".to_owned()],
            ),
        };
    HostCapabilityServiceDescriptor {
        service,
        required_capability_class,
        default_timeout_ms,
        max_payload_bytes,
        audit_event: "plugin.host_call.invoked".to_owned(),
        redacted_fields,
        returns_secret_material: false,
    }
}

fn default_lifecycle() -> Vec<TypedPluginContractLifecyclePhase> {
    vec![
        TypedPluginContractLifecyclePhase::Discover,
        TypedPluginContractLifecyclePhase::Negotiate,
        TypedPluginContractLifecyclePhase::Bind,
        TypedPluginContractLifecyclePhase::Invoke,
        TypedPluginContractLifecyclePhase::Audit,
    ]
}

// Data-plane classes shared by contracts that fetch, authenticate, and persist
// data. Channels are deliberately excluded: channel access is granted only to
// contracts that address channels explicitly.
fn default_data_capabilities() -> Vec<TypedPluginCapabilityClass> {
    vec![
        TypedPluginCapabilityClass::HttpHosts,
        TypedPluginCapabilityClass::Secrets,
        TypedPluginCapabilityClass::StoragePrefixes,
    ]
}

#[expect(
    clippy::too_many_arguments,
    reason = "descriptor fields are passed positionally once per contract kind"
)]
fn build_descriptor(
    kind: TypedPluginContractKind,
    version: u32,
    input_schema: &'static str,
    output_schema: &'static str,
    sensitivity: TypedPluginDataSensitivity,
    operations: Vec<TypedPluginContractOperation>,
    allowed_capability_classes: Vec<TypedPluginCapabilityClass>,
    error_codes: Vec<String>,
    redacted_fields: Vec<String>,
    audit_hooks: Vec<String>,
    observability_hooks: Vec<String>,
) -> TypedPluginContractDescriptor {
    TypedPluginContractDescriptor {
        kind,
        version,
        sdk_abi_major: SDK_ABI_MAJOR,
        input_schema: input_schema.to_owned(),
        output_schema: output_schema.to_owned(),
        default_timeout_ms: DEFAULT_TYPED_PLUGIN_CONTRACT_TIMEOUT_MS,
        sensitivity,
        lifecycle: default_lifecycle(),
        operations,
        allowed_capability_classes,
        error_codes,
        redacted_fields,
        audit_hooks,
        observability_hooks,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        all_typed_plugin_contract_kinds, built_in_sdk_contract_fixtures,
        default_typed_plugin_contract_version, host_capability_service_descriptor,
        sdk_abi_compatibility, sdk_abi_version, simulate_sdk_contract_fixture,
        supported_host_capability_services, supported_typed_plugin_contracts,
        typed_plugin_contract_descriptor, wit_package_id, wit_source, HostCapabilityServiceKind,
        TypedPluginCapabilityClass, TypedPluginContractDescriptor, TypedPluginContractKind,
        TypedPluginContractOperation, HOST_CAPABILITIES_IMPORT_MODULE,
        HOST_CAPABILITY_CHANNEL_COUNT_FN, HOST_CAPABILITY_CHANNEL_HANDLE_FN,
        HOST_CAPABILITY_HTTP_COUNT_FN, HOST_CAPABILITY_HTTP_HANDLE_FN,
        HOST_CAPABILITY_SECRET_COUNT_FN, HOST_CAPABILITY_SECRET_HANDLE_FN,
        HOST_CAPABILITY_STORAGE_COUNT_FN, HOST_CAPABILITY_STORAGE_HANDLE_FN, SDK_ABI_MAJOR,
        WIT_WORLD_NAME,
    };

    #[test]
    fn wit_package_id_is_stable() {
        assert_eq!(wit_package_id(), "palyra:plugins/sdk@0.1.0");
        assert_eq!(sdk_abi_version(), "palyra.plugins.sdk.abi.v1");
        assert!(sdk_abi_compatibility().accepts(SDK_ABI_MAJOR));
    }

    #[test]
    fn wit_source_contains_expected_world_and_imports() {
        let source = wit_source();
        assert!(source.contains("world palyra-plugin"));
        assert!(source.contains("import host-capabilities"));
        assert!(source.contains("run: func() -> s32"));
        assert!(source.contains("sdk-abi-version: func() -> string"));
        assert!(source.contains("plugin-hello: func() -> string"));
        assert!(source.contains(HOST_CAPABILITY_HTTP_COUNT_FN));
        assert!(source.contains(HOST_CAPABILITY_HTTP_HANDLE_FN));
        assert!(source.contains(HOST_CAPABILITY_SECRET_COUNT_FN));
        assert!(source.contains(HOST_CAPABILITY_SECRET_HANDLE_FN));
        assert!(source.contains(HOST_CAPABILITY_STORAGE_COUNT_FN));
        assert!(source.contains(HOST_CAPABILITY_STORAGE_HANDLE_FN));
        assert!(source.contains(HOST_CAPABILITY_CHANNEL_COUNT_FN));
        assert!(source.contains(HOST_CAPABILITY_CHANNEL_HANDLE_FN));
    }

    #[test]
    fn exported_wit_symbol_names_are_stable() {
        assert_eq!(WIT_WORLD_NAME, "palyra-plugin");
        assert_eq!(HOST_CAPABILITIES_IMPORT_MODULE, "palyra:plugins/host-capabilities@0.1.0");
    }

    #[test]
    fn typed_contract_descriptors_cover_supported_extension_points() {
        let supported = supported_typed_plugin_contracts();
        assert_eq!(supported.len(), all_typed_plugin_contract_kinds().len());
        assert!(supported.iter().all(|descriptor| descriptor.version == 1));
        assert!(supported.iter().all(|descriptor| descriptor.sdk_abi_major == 1));
        assert!(supported.iter().all(|descriptor| !descriptor.input_schema.is_empty()));
        assert!(supported.iter().all(|descriptor| !descriptor.output_schema.is_empty()));
        assert!(supported.iter().all(|descriptor| descriptor.default_timeout_ms > 0));
        assert!(supported.iter().all(|descriptor| !descriptor.lifecycle.is_empty()));
        assert!(supported.iter().any(|descriptor| {
            descriptor.kind == TypedPluginContractKind::RunLifecycleHook
                && descriptor.operations.contains(&TypedPluginContractOperation::DecideRunLifecycle)
                && descriptor.allowed_capability_classes.is_empty()
        }));
        assert!(supported.iter().any(|descriptor| {
            descriptor.kind == TypedPluginContractKind::ConnectorAdapter
                && descriptor
                    .allowed_capability_classes
                    .contains(&TypedPluginCapabilityClass::Channels)
        }));
    }

    #[test]
    fn typed_contract_descriptor_rejects_unknown_versions() {
        assert!(
            typed_plugin_contract_descriptor(TypedPluginContractKind::MemoryProvider, 99).is_none()
        );
        assert_eq!(default_typed_plugin_contract_version(), 1);
    }

    #[test]
    fn sdk_contract_simulator_accepts_good_fixture_and_rejects_bad_fixtures() {
        for fixture in built_in_sdk_contract_fixtures() {
            let report = simulate_sdk_contract_fixture(&fixture);
            assert_eq!(
                report.accepted, fixture.expected_accepted,
                "fixture '{}' produced unexpected report: {:?}",
                fixture.name, report
            );
            if fixture.expected_accepted {
                assert!(report.rejected_reasons.is_empty());
            } else {
                assert!(
                    !report.rejected_reasons.is_empty(),
                    "bad fixture should explain why negotiation failed"
                );
            }
        }
    }

    #[test]
    fn host_capability_services_are_capability_scoped_and_redacted() {
        let services = supported_host_capability_services();
        assert!(services
            .iter()
            .any(|service| service.service == HostCapabilityServiceKind::TasksCreate));
        let tasks_create =
            host_capability_service_descriptor(HostCapabilityServiceKind::TasksCreate);
        assert_eq!(
            tasks_create.required_capability_class,
            Some(TypedPluginCapabilityClass::StoragePrefixes)
        );
        assert!(tasks_create.redacted_fields.contains(&"task.payload".to_owned()));

        let vault =
            host_capability_service_descriptor(HostCapabilityServiceKind::VaultSecretLeaseRequest);
        assert_eq!(vault.required_capability_class, Some(TypedPluginCapabilityClass::Secrets));
        assert!(
            !vault.returns_secret_material,
            "vault host service may return lease metadata, never raw secret material"
        );
        assert_eq!(vault.audit_event, "plugin.host_call.invoked");
    }

    #[test]
    fn typed_contract_abi_fingerprint_matches_golden() {
        const EXPECTED_TYPED_CONTRACT_ABI_FINGERPRINT: u64 = 0xf789_73f0_e89b_94ec;
        let snapshot = typed_contract_abi_snapshot();
        assert_eq!(
            stable_fingerprint(snapshot.as_str()),
            EXPECTED_TYPED_CONTRACT_ABI_FINGERPRINT,
            "typed contract ABI snapshot changed:\n{snapshot}"
        );
    }

    fn typed_contract_abi_snapshot() -> String {
        let mut lines = vec![format!(
            "package={}|abi={}|range={}..{}",
            wit_package_id(),
            sdk_abi_version(),
            sdk_abi_compatibility().min_abi_major,
            sdk_abi_compatibility().max_abi_major
        )];
        for descriptor in supported_typed_plugin_contracts() {
            lines.push(descriptor_abi_line(&descriptor));
        }
        lines.join("\n")
    }

    fn descriptor_abi_line(descriptor: &TypedPluginContractDescriptor) -> String {
        format!(
            "{}|v{}|abi{}|timeout{}|sensitivity={}|input={}|output={}|lifecycle={}|ops={}|caps={}|errors={}|redacted={}|audit={}|obs={}",
            descriptor.kind.as_str(),
            descriptor.version,
            descriptor.sdk_abi_major,
            descriptor.default_timeout_ms,
            descriptor.sensitivity.as_str(),
            descriptor.input_schema,
            descriptor.output_schema,
            descriptor
                .lifecycle
                .iter()
                .map(|phase| phase.as_str())
                .collect::<Vec<_>>()
                .join(","),
            descriptor
                .operations
                .iter()
                .map(|operation| operation.as_str())
                .collect::<Vec<_>>()
                .join(","),
            descriptor
                .allowed_capability_classes
                .iter()
                .map(|capability| capability.as_str())
                .collect::<Vec<_>>()
                .join(","),
            descriptor.error_codes.join(","),
            descriptor.redacted_fields.join(","),
            descriptor.audit_hooks.join(","),
            descriptor.observability_hooks.join(",")
        )
    }

    fn stable_fingerprint(input: &str) -> u64 {
        const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
        const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
        input
            .as_bytes()
            .iter()
            .fold(FNV_OFFSET_BASIS, |hash, byte| (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME))
    }
}
