//! Version-two plugin invocation envelopes and lifecycle invariants.
//!
//! This module is the transport-neutral source of truth shared by the Wasm
//! runtime and future component-model adapters. Payload bytes remain opaque
//! here and are interpreted by the contract-specific types in `contracts_v2`.

use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

/// Stable ABI marker for executable plugin invocation.
pub const PLUGIN_ABI_V2_VERSION: &str = "palyra.plugins.sdk.abi.v2";
/// WIT package identifier for the version-two component-model contract.
pub const PLUGIN_ABI_V2_WIT_PACKAGE_ID: &str = "palyra:plugins/sdk@0.2.0";
/// Prefix shared by the six capability-specific WIT worlds.
pub const PLUGIN_ABI_V2_WIT_WORLD_PREFIX: &str = "palyra-plugin-v2";
/// Version-two WIT source embedded for SDK tooling and compatibility tests.
pub const WIT_SOURCE_V2: &str = include_str!("../wit/palyra-sdk-v2.wit");
/// Core-Wasm import module used by the CI-qualified memory ABI.
pub const PLUGIN_ABI_V2_HOST_IMPORT_MODULE: &str = "palyra:plugins/abi-v2@2";
/// Host import used by guests to publish bounded invocation events.
pub const PLUGIN_ABI_V2_EMIT_EVENT_IMPORT: &str = "emit-event";
/// Host import used by guests to observe cooperative cancellation.
pub const PLUGIN_ABI_V2_IS_CANCELLED_IMPORT: &str = "is-cancelled";
/// Required guest linear-memory export.
pub const PLUGIN_ABI_V2_CORE_MEMORY_EXPORT: &str = "memory";
/// Required guest allocator export.
pub const PLUGIN_ABI_V2_CORE_ALLOC_EXPORT: &str = "palyra-abi-v2-alloc";
/// Required guest invocation export.
pub const PLUGIN_ABI_V2_CORE_INVOKE_EXPORT: &str = "palyra-abi-v2-invoke";
/// Required guest deallocator export.
pub const PLUGIN_ABI_V2_CORE_DEALLOC_EXPORT: &str = "palyra-abi-v2-dealloc";
/// Fixed magic at the beginning of every core-Wasm request.
pub const PLUGIN_CORE_WIRE_MAGIC_V2: [u8; 8] = *b"PLYRABI2";
/// Schema version encoded in every core-Wasm request header.
pub const PLUGIN_CORE_WIRE_SCHEMA_VERSION_V2: u16 = 2;

const MAX_IDENTIFIER_BYTES: usize = 128;
const SHA256_HEX_BYTES: usize = 64;

/// Validation failure for an ABI value constructed at a trust boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PluginAbiValueError {
    /// An identifier is empty or exceeds the ABI bound.
    InvalidIdentifierLength { field: &'static str, length: usize },
    /// An identifier contains a character outside the stable wire alphabet.
    InvalidIdentifierCharacter { field: &'static str },
    /// A schema or scope hash is not canonical lowercase SHA-256 hex.
    InvalidSha256 { field: &'static str },
    /// Runtime generation zero is reserved for unbound values.
    InvalidRuntimeGeneration,
    /// A budget is zero or internally inconsistent.
    InvalidBudget { field: &'static str },
    /// A capability handle expires before it is issued.
    InvalidCapabilityLifetime,
    /// A binding does not describe its selected executable contract.
    InvalidBindingContract,
}

impl fmt::Display for PluginAbiValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentifierLength { field, length } => {
                write!(formatter, "invalid {field} length: {length}")
            }
            Self::InvalidIdentifierCharacter { field } => {
                write!(formatter, "invalid character in {field}")
            }
            Self::InvalidSha256 { field } => write!(formatter, "invalid sha256 in {field}"),
            Self::InvalidRuntimeGeneration => formatter.write_str("invalid runtime generation"),
            Self::InvalidBudget { field } => {
                write!(formatter, "invalid invocation budget: {field}")
            }
            Self::InvalidCapabilityLifetime => {
                formatter.write_str("invalid capability handle lifetime")
            }
            Self::InvalidBindingContract => formatter.write_str("invalid binding contract"),
        }
    }
}

impl Error for PluginAbiValueError {}

macro_rules! identifier_type {
    ($name:ident, $field:literal, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Constructs a validated ", $field, ".")]
            ///
            /// # Errors
            /// Returns [`PluginAbiValueError`] when the identifier is empty,
            /// over 128 bytes, or contains characters outside the stable ABI
            /// identifier alphabet.
            pub fn new(value: impl Into<String>) -> Result<Self, PluginAbiValueError> {
                let value = value.into();
                validate_identifier($field, &value)?;
                Ok(Self(value))
            }

            #[doc = concat!("Returns the validated ", $field, " wire value.")]
            #[must_use]
            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.debug_tuple(stringify!($name)).field(&self.0).finish()
            }
        }
    };
}

identifier_type!(PluginCallIdV2, "call_id", "A host-issued identifier for one plugin invocation.");
identifier_type!(
    PluginBindingIdV2,
    "binding_id",
    "A stable identifier for one host-approved plugin binding."
);

/// An opaque host-issued capability handle identifier.
///
/// Its `Debug` representation is deliberately redacted so diagnostics cannot
/// accidentally expose the bearer-like identifier.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(transparent)]
pub struct PluginCapabilityHandleIdV2(String);

impl PluginCapabilityHandleIdV2 {
    /// Constructs a validated opaque handle identifier.
    ///
    /// # Errors
    /// Returns [`PluginAbiValueError`] for an invalid ABI identifier.
    pub fn new(value: impl Into<String>) -> Result<Self, PluginAbiValueError> {
        let value = value.into();
        validate_identifier("capability_handle_id", &value)?;
        Ok(Self(value))
    }

    /// Returns the opaque wire value for transport to the bound guest.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for PluginCapabilityHandleIdV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PluginCapabilityHandleIdV2(***)")
    }
}

/// A nonzero generation pin for a plugin runtime instance.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(transparent)]
pub struct PluginRuntimeGenerationV2(u64);

impl PluginRuntimeGenerationV2 {
    /// Constructs a nonzero runtime generation.
    ///
    /// # Errors
    /// Returns [`PluginAbiValueError::InvalidRuntimeGeneration`] for zero.
    pub fn new(value: u64) -> Result<Self, PluginAbiValueError> {
        if value == 0 {
            return Err(PluginAbiValueError::InvalidRuntimeGeneration);
        }
        Ok(Self(value))
    }

    /// Returns the generation number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A canonical lowercase SHA-256 hash used to bind schemas and scopes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(transparent)]
pub struct PluginSchemaHashV2(String);

impl PluginSchemaHashV2 {
    /// Parses a canonical lowercase SHA-256 hash.
    ///
    /// # Errors
    /// Returns [`PluginAbiValueError::InvalidSha256`] unless `value` contains
    /// exactly 64 lowercase hexadecimal characters.
    pub fn parse(
        field: &'static str,
        value: impl Into<String>,
    ) -> Result<Self, PluginAbiValueError> {
        let value = value.into();
        if value.len() != SHA256_HEX_BYTES
            || !value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(PluginAbiValueError::InvalidSha256 { field });
        }
        Ok(Self(value))
    }

    /// Returns the canonical hash.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// The six contract kinds qualified for executable ABI v2 conformance.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ExecutablePluginContractKindV2 {
    /// External agent harness contract.
    AgentHarness,
    /// Session context planning contract.
    ContextEngine,
    /// Tool-result authority-preserving middleware contract.
    ToolResultMiddleware,
    /// Run lifecycle policy hook contract.
    RunLifecycleHook,
    /// Candidate-only memory provider contract.
    MemoryProvider,
    /// Opaque-handle-only model authentication contract.
    ModelAuthProvider,
}

/// Host fallback posture when an executable plugin invocation times out.
///
/// Fail-open contracts can be skipped without granting new authority or
/// suppressing a required policy decision. Fail-closed contracts participate
/// in execution, authorization, or credential selection and therefore cannot
/// be bypassed safely.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PluginTimeoutDispositionV2 {
    /// Continue through the host-owned path without the plugin projection.
    FailOpen,
    /// Stop the affected operation with an explicit deadline failure.
    FailClosed,
}

impl ExecutablePluginContractKindV2 {
    /// Returns the stable wire identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AgentHarness => "agent_harness",
            Self::ContextEngine => "context_engine",
            Self::ToolResultMiddleware => "tool_result_middleware",
            Self::RunLifecycleHook => "run_lifecycle_hook",
            Self::MemoryProvider => "memory_provider",
            Self::ModelAuthProvider => "model_auth_provider",
        }
    }

    /// Returns the compact core-Wasm wire tag.
    #[must_use]
    pub const fn core_wire_tag(self) -> u8 {
        match self {
            Self::AgentHarness => 1,
            Self::ContextEngine => 2,
            Self::ToolResultMiddleware => 3,
            Self::RunLifecycleHook => 4,
            Self::MemoryProvider => 5,
            Self::ModelAuthProvider => 6,
        }
    }

    /// Returns the pinned timeout posture for this contract kind.
    #[must_use]
    pub const fn timeout_disposition(self) -> PluginTimeoutDispositionV2 {
        match self {
            Self::ContextEngine | Self::ToolResultMiddleware | Self::MemoryProvider => {
                PluginTimeoutDispositionV2::FailOpen
            }
            Self::AgentHarness | Self::RunLifecycleHook | Self::ModelAuthProvider => {
                PluginTimeoutDispositionV2::FailClosed
            }
        }
    }
}

/// Every contract kind currently qualified for executable ABI v2.
pub const EXECUTABLE_PLUGIN_CONTRACTS_V2: [ExecutablePluginContractKindV2; 6] = [
    ExecutablePluginContractKindV2::AgentHarness,
    ExecutablePluginContractKindV2::ContextEngine,
    ExecutablePluginContractKindV2::ToolResultMiddleware,
    ExecutablePluginContractKindV2::RunLifecycleHook,
    ExecutablePluginContractKindV2::MemoryProvider,
    ExecutablePluginContractKindV2::ModelAuthProvider,
];

/// Operation selected within an executable ABI v2 contract.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ExecutablePluginOperationV2 {
    /// Execute an agent attempt.
    RunAgentAttempt,
    /// Plan the next context projection.
    PlanContext,
    /// Transform a tool result without expanding authority.
    TransformToolResult,
    /// Evaluate a run lifecycle transition.
    DecideRunLifecycle,
    /// Produce memory candidates for host review.
    ProvideMemoryCandidates,
    /// Resolve an opaque model credential handle.
    ResolveModelAuthHandle,
}

impl ExecutablePluginOperationV2 {
    /// Returns the contract kind that owns this operation.
    #[must_use]
    pub const fn contract(self) -> ExecutablePluginContractKindV2 {
        match self {
            Self::RunAgentAttempt => ExecutablePluginContractKindV2::AgentHarness,
            Self::PlanContext => ExecutablePluginContractKindV2::ContextEngine,
            Self::TransformToolResult => ExecutablePluginContractKindV2::ToolResultMiddleware,
            Self::DecideRunLifecycle => ExecutablePluginContractKindV2::RunLifecycleHook,
            Self::ProvideMemoryCandidates => ExecutablePluginContractKindV2::MemoryProvider,
            Self::ResolveModelAuthHandle => ExecutablePluginContractKindV2::ModelAuthProvider,
        }
    }

    /// Returns the compact core-Wasm wire tag.
    #[must_use]
    pub const fn core_wire_tag(self) -> u8 {
        match self {
            Self::RunAgentAttempt => 1,
            Self::PlanContext => 2,
            Self::TransformToolResult => 3,
            Self::DecideRunLifecycle => 4,
            Self::ProvideMemoryCandidates => 5,
            Self::ResolveModelAuthHandle => 6,
        }
    }
}

/// Stable schema identifiers and hashes for one executable contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginContractSchemaV2 {
    /// Contract kind described by these schemas.
    pub contract: ExecutablePluginContractKindV2,
    /// Versioned input schema identifier.
    pub input_schema_id: String,
    /// Canonical input schema hash.
    pub input_schema_hash: PluginSchemaHashV2,
    /// Versioned output schema identifier.
    pub output_schema_id: String,
    /// Canonical output schema hash.
    pub output_schema_hash: PluginSchemaHashV2,
    /// Host behavior when the contract exceeds its absolute deadline.
    pub timeout_disposition: PluginTimeoutDispositionV2,
}

/// Explicit migration posture for the retained v1 compatibility world.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginAbiMigrationPostureV2 {
    /// Legacy ABI marker accepted only through the compatibility path.
    pub legacy_abi_version: String,
    /// Legacy WIT package retained for migration.
    pub legacy_wit_package_id: String,
    /// Whether new production bindings may select v1.
    pub legacy_production_bindings_allowed: bool,
    /// Stable migration target.
    pub migration_target: String,
}

/// Golden-snapshot surface for executable plugin ABI v2.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginExecutableAbiSnapshotV2 {
    /// Snapshot schema version.
    pub schema_version: u16,
    /// Executable ABI marker.
    pub abi_version: String,
    /// WIT package containing the capability-specific worlds.
    pub wit_package_id: String,
    /// Core-Wasm wire schema version.
    pub core_wire_schema_version: u16,
    /// Required host import module.
    pub host_import_module: String,
    /// Required guest memory ABI exports.
    pub core_exports: Vec<String>,
    /// Qualified executable contracts and their schema hashes.
    pub executable_contracts: Vec<PluginContractSchemaV2>,
    /// Explicit legacy migration posture.
    pub migration: PluginAbiMigrationPostureV2,
}

/// Returns the versioned executable ABI snapshot pinned by CI.
#[must_use]
pub fn executable_plugin_abi_snapshot_v2() -> PluginExecutableAbiSnapshotV2 {
    PluginExecutableAbiSnapshotV2 {
        schema_version: 2,
        abi_version: PLUGIN_ABI_V2_VERSION.to_owned(),
        wit_package_id: PLUGIN_ABI_V2_WIT_PACKAGE_ID.to_owned(),
        core_wire_schema_version: PLUGIN_CORE_WIRE_SCHEMA_VERSION_V2,
        host_import_module: PLUGIN_ABI_V2_HOST_IMPORT_MODULE.to_owned(),
        core_exports: vec![
            PLUGIN_ABI_V2_CORE_MEMORY_EXPORT.to_owned(),
            PLUGIN_ABI_V2_CORE_ALLOC_EXPORT.to_owned(),
            PLUGIN_ABI_V2_CORE_INVOKE_EXPORT.to_owned(),
            PLUGIN_ABI_V2_CORE_DEALLOC_EXPORT.to_owned(),
        ],
        executable_contracts: EXECUTABLE_PLUGIN_CONTRACTS_V2
            .iter()
            .copied()
            .map(executable_plugin_contract_schema_v2)
            .collect(),
        migration: PluginAbiMigrationPostureV2 {
            legacy_abi_version: "palyra.plugins.sdk.abi.v1".to_owned(),
            legacy_wit_package_id: "palyra:plugins/sdk@0.1.0".to_owned(),
            legacy_production_bindings_allowed: false,
            migration_target: PLUGIN_ABI_V2_VERSION.to_owned(),
        },
    }
}

/// Returns the pinned schema descriptor for an executable contract.
#[must_use]
pub fn executable_plugin_contract_schema_v2(
    contract: ExecutablePluginContractKindV2,
) -> PluginContractSchemaV2 {
    let (input_schema_id, input_hash, output_schema_id, output_hash) = match contract {
        ExecutablePluginContractKindV2::AgentHarness => (
            "palyra.plugin.agent_harness.invocation.v2",
            "8fa6dc9e8938d94c3562386431c7707348fcd753dfd36bc57040dac1d890ad80",
            "palyra.plugin.agent_harness.result.v2",
            "2847c8e83f4bfe706552886fcac266456a8054884188914d5bf70f5fe1281d33",
        ),
        ExecutablePluginContractKindV2::ContextEngine => (
            "palyra.plugin.context_engine.invocation.v2",
            "1db41fe674b882bb4eb0f662fc6b4ee6949f55be17534ae4f88058bb500f6c92",
            "palyra.plugin.context_engine.result.v2",
            "51e0af9cc435f411fb41f89ab32f9f406bc2d05df1404c3c2cb1be40c06714e3",
        ),
        ExecutablePluginContractKindV2::ToolResultMiddleware => (
            "palyra.plugin.tool_result_middleware.invocation.v2",
            "f0161348bcad632bd6c38c5920ce115b8f41ca2598b7a06d6917dc973291432f",
            "palyra.plugin.tool_result_middleware.result.v2",
            "b97e31bb6cae139a1bb65b5f98b13121a6fb26a154043cd8b17bd93574412b0e",
        ),
        ExecutablePluginContractKindV2::RunLifecycleHook => (
            "palyra.plugin.run_lifecycle_hook.invocation.v2",
            "e8baac369785d0c7723245398c00cad3235aee1821063ac903d0b8d4b83481ad",
            "palyra.plugin.run_lifecycle_hook.result.v2",
            "682d457aefda8c8187fd154dcdf1fb2dba0eb90509ac680425efd444b397b601",
        ),
        ExecutablePluginContractKindV2::MemoryProvider => (
            "palyra.plugin.memory_provider.invocation.v2",
            "4eb8a8f667fe817e600c21af1ca04dcbb5f8e0fd92b4bc5b4c230c7cfb768fe8",
            "palyra.plugin.memory_provider.candidates.v2",
            "1bb897494014e22666449638af8b0444d87d00063e25a72873fdb42d82ce96dd",
        ),
        ExecutablePluginContractKindV2::ModelAuthProvider => (
            "palyra.plugin.model_auth_provider.invocation.v2",
            "3ab1b8db9ef88596a7ec475cbfb5d80e4df3cc2e6c37ebd104720cc1b5c74468",
            "palyra.plugin.model_auth_provider.handle.v2",
            "c1e7fe922a9b259419f9d8d9a8fcb7f91ef763f40a30115313005b417b2286c9",
        ),
    };
    PluginContractSchemaV2 {
        contract,
        input_schema_id: input_schema_id.to_owned(),
        input_schema_hash: known_schema_hash("input_schema_hash", input_hash),
        output_schema_id: output_schema_id.to_owned(),
        output_schema_hash: known_schema_hash("output_schema_hash", output_hash),
        timeout_disposition: contract.timeout_disposition(),
    }
}

/// An absolute deadline and byte/event budgets for one invocation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginInvocationBudgetV2 {
    /// Absolute Unix deadline in milliseconds.
    pub absolute_deadline_unix_ms: u64,
    /// Maximum request payload bytes accepted before Wasm execution.
    pub max_input_bytes: u32,
    /// Maximum terminal output bytes copied from guest memory.
    pub max_output_bytes: u32,
    /// Maximum bytes in one streamed event.
    pub max_event_bytes: u32,
    /// Maximum number of events accepted before backpressure fails closed.
    pub max_events: u32,
}

impl PluginInvocationBudgetV2 {
    /// Validates nonzero byte/event budgets.
    ///
    /// # Errors
    /// Returns [`PluginAbiValueError::InvalidBudget`] for a zero bound.
    pub fn validate(&self) -> Result<(), PluginAbiValueError> {
        for (field, value) in [
            ("max_input_bytes", self.max_input_bytes),
            ("max_output_bytes", self.max_output_bytes),
            ("max_event_bytes", self.max_event_bytes),
            ("max_events", self.max_events),
        ] {
            if value == 0 {
                return Err(PluginAbiValueError::InvalidBudget { field });
            }
        }
        if self.absolute_deadline_unix_ms == 0 {
            return Err(PluginAbiValueError::InvalidBudget { field: "absolute_deadline_unix_ms" });
        }
        Ok(())
    }
}

/// Capability scope visible to a plugin through opaque metadata.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PluginCapabilityScopeV2 {
    /// Bounded HTTP host mediation.
    HttpHost,
    /// Opaque vault lease mediation.
    SecretLease,
    /// Bounded storage-prefix mediation.
    StoragePrefix,
    /// Bounded channel mediation.
    Channel,
    /// Host callback mediation for agent harnesses.
    HarnessCallback,
}

/// A scoped, expiring, generation-pinned capability reference.
///
/// The record contains only an opaque handle and a hash of its scope; it has
/// no field capable of carrying raw secret material.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginCapabilityHandleV2 {
    handle_id: PluginCapabilityHandleIdV2,
    /// Capability class mediated by the host.
    pub scope: PluginCapabilityScopeV2,
    /// Hash of the host-owned scope definition.
    pub scope_hash: PluginSchemaHashV2,
    /// Runtime generation in which the handle is valid.
    pub runtime_generation: PluginRuntimeGenerationV2,
    /// Unix timestamp after which the host rejects the handle.
    pub expires_at_unix_ms: u64,
}

impl PluginCapabilityHandleV2 {
    /// Constructs validated opaque capability metadata.
    ///
    /// # Errors
    /// Returns [`PluginAbiValueError::InvalidCapabilityLifetime`] when the
    /// expiry is zero.
    pub fn new(
        handle_id: PluginCapabilityHandleIdV2,
        scope: PluginCapabilityScopeV2,
        scope_hash: PluginSchemaHashV2,
        runtime_generation: PluginRuntimeGenerationV2,
        expires_at_unix_ms: u64,
    ) -> Result<Self, PluginAbiValueError> {
        if expires_at_unix_ms == 0 {
            return Err(PluginAbiValueError::InvalidCapabilityLifetime);
        }
        Ok(Self { handle_id, scope, scope_hash, runtime_generation, expires_at_unix_ms })
    }

    /// Returns the opaque handle for transport to the bound guest.
    #[must_use]
    pub fn handle_id(&self) -> &PluginCapabilityHandleIdV2 {
        &self.handle_id
    }
}

impl fmt::Debug for PluginCapabilityHandleV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginCapabilityHandleV2")
            .field("handle_id", &"***")
            .field("scope", &self.scope)
            .field("scope_hash", &self.scope_hash)
            .field("runtime_generation", &self.runtime_generation)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .finish()
    }
}

/// Host-approved binding between a plugin generation and one contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginBindingRecordV2 {
    /// Binding identity pinned into every request.
    pub binding_id: PluginBindingIdV2,
    /// Executable contract selected for the binding.
    pub contract: ExecutablePluginContractKindV2,
    /// Contract operation selected for the binding.
    pub operation: ExecutablePluginOperationV2,
    /// Runtime generation that owns the binding.
    pub runtime_generation: PluginRuntimeGenerationV2,
    /// Expected request schema hash.
    pub input_schema_hash: PluginSchemaHashV2,
    /// Expected terminal output schema hash.
    pub output_schema_hash: PluginSchemaHashV2,
    /// Time at which the host issued the binding.
    pub issued_at_unix_ms: u64,
    /// Time at which the binding expires.
    pub expires_at_unix_ms: u64,
    /// Opaque capability metadata granted to this binding.
    #[serde(default)]
    pub granted_capability_handles: Vec<PluginCapabilityHandleV2>,
}

impl PluginBindingRecordV2 {
    /// Validates contract, lifetime, schema, and handle generation invariants.
    ///
    /// # Errors
    /// Returns [`PluginAbiValueError`] when the binding is internally
    /// inconsistent.
    pub fn validate(&self) -> Result<(), PluginAbiValueError> {
        if self.operation.contract() != self.contract {
            return Err(PluginAbiValueError::InvalidBindingContract);
        }
        if self.issued_at_unix_ms == 0 || self.expires_at_unix_ms <= self.issued_at_unix_ms {
            return Err(PluginAbiValueError::InvalidCapabilityLifetime);
        }
        let schema = executable_plugin_contract_schema_v2(self.contract);
        if self.input_schema_hash != schema.input_schema_hash
            || self.output_schema_hash != schema.output_schema_hash
        {
            return Err(PluginAbiValueError::InvalidBindingContract);
        }
        if self.granted_capability_handles.iter().any(|handle| {
            handle.runtime_generation != self.runtime_generation
                || handle.expires_at_unix_ms <= self.issued_at_unix_ms
                || handle.expires_at_unix_ms > self.expires_at_unix_ms
        }) {
            return Err(PluginAbiValueError::InvalidCapabilityLifetime);
        }
        Ok(())
    }
}

/// Typed host request delivered across the ABI boundary.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginInvocationRequestV2 {
    /// Envelope schema version, always `2`.
    pub schema_version: u16,
    /// Invocation identity.
    pub call_id: PluginCallIdV2,
    /// Binding selected by the host.
    pub binding_id: PluginBindingIdV2,
    /// Runtime generation selected by the host.
    pub runtime_generation: PluginRuntimeGenerationV2,
    /// Executable contract selected by the binding.
    pub contract: ExecutablePluginContractKindV2,
    /// Operation selected within the contract.
    pub operation: ExecutablePluginOperationV2,
    /// Absolute deadline and backpressure budgets.
    pub budget: PluginInvocationBudgetV2,
    /// Schema hash for `input_bytes`.
    pub input_schema_hash: PluginSchemaHashV2,
    /// Schema hash required for terminal output.
    pub output_schema_hash: PluginSchemaHashV2,
    /// Contract-specific serialized request.
    pub input_bytes: Vec<u8>,
    /// Opaque capability metadata available to the guest.
    #[serde(default)]
    pub granted_capability_handles: Vec<PluginCapabilityHandleV2>,
}

impl fmt::Debug for PluginInvocationRequestV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginInvocationRequestV2")
            .field("schema_version", &self.schema_version)
            .field("call_id", &self.call_id)
            .field("binding_id", &self.binding_id)
            .field("runtime_generation", &self.runtime_generation)
            .field("contract", &self.contract)
            .field("operation", &self.operation)
            .field("budget", &self.budget)
            .field("input_schema_hash", &self.input_schema_hash)
            .field("output_schema_hash", &self.output_schema_hash)
            .field("input_bytes", &format_args!("<redacted:{} bytes>", self.input_bytes.len()))
            .field("granted_capability_handles", &self.granted_capability_handles)
            .finish()
    }
}

/// Stable fail-closed reason code for an invocation failure.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PluginInvocationErrorCodeV2 {
    /// The requested binding does not exist.
    BindingNotFound,
    /// The binding is no longer active.
    BindingInactive,
    /// Request and binding identities differ.
    BindingMismatch,
    /// A stale runtime generation attempted to invoke.
    StaleGeneration,
    /// The absolute deadline already elapsed.
    DeadlineExceeded,
    /// Request or response schema hash does not match the binding.
    SchemaMismatch,
    /// Input bytes exceed the host-approved budget.
    InputTooLarge,
    /// Output bytes exceed the host-approved budget.
    OutputTooLarge,
    /// Event bytes or event count exceeded backpressure limits.
    EventBackpressureExceeded,
    /// A capability handle is missing, expired, out of scope, or stale.
    CapabilityHandleInvalid,
    /// Guest module compilation or linking failed.
    GuestRejected,
    /// Guest execution trapped.
    GuestTrapped,
    /// Guest exhausted fuel, memory, table, or instance limits.
    ResourceLimitExceeded,
    /// Guest output is not a valid typed contract result.
    InvalidContractOutput,
    /// Guest attempted to expand or misrepresent host authority.
    AuthorityExpansionDenied,
    /// Invocation was cooperatively cancelled.
    Cancelled,
    /// Binding was disposed during lifecycle cleanup.
    Disposed,
    /// Binding was quarantined after conformance or runtime strikes.
    Quarantined,
}

impl PluginInvocationErrorCodeV2 {
    /// Returns the stable metadata-trace reason code.
    #[must_use]
    pub const fn reason_code(self) -> &'static str {
        match self {
            Self::BindingNotFound => "plugin.binding.not_found",
            Self::BindingInactive => "plugin.binding.inactive",
            Self::BindingMismatch => "plugin.binding.mismatch",
            Self::StaleGeneration => "plugin.generation.stale",
            Self::DeadlineExceeded => "plugin.deadline.exceeded",
            Self::SchemaMismatch => "plugin.schema.mismatch",
            Self::InputTooLarge => "plugin.input.too_large",
            Self::OutputTooLarge => "plugin.output.too_large",
            Self::EventBackpressureExceeded => "plugin.event.backpressure_exceeded",
            Self::CapabilityHandleInvalid => "plugin.capability_handle.invalid",
            Self::GuestRejected => "plugin.guest.rejected",
            Self::GuestTrapped => "plugin.guest.trapped",
            Self::ResourceLimitExceeded => "plugin.resource_limit.exceeded",
            Self::InvalidContractOutput => "plugin.contract_output.invalid",
            Self::AuthorityExpansionDenied => "plugin.authority_expansion.denied",
            Self::Cancelled => "plugin.invocation.cancelled",
            Self::Disposed => "plugin.binding.disposed",
            Self::Quarantined => "plugin.binding.quarantined",
        }
    }
}

/// Redacted invocation failure safe for diagnostics and metadata trace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginInvocationErrorV2 {
    /// Stable error classification.
    pub code: PluginInvocationErrorCodeV2,
    /// Invocation identity when admission reached a call.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_id: Option<PluginCallIdV2>,
    /// Binding identity when one was supplied.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binding_id: Option<PluginBindingIdV2>,
    /// Runtime generation associated with the failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_generation: Option<PluginRuntimeGenerationV2>,
}

impl PluginInvocationErrorV2 {
    /// Constructs a payload-free, stable-code diagnostic.
    #[must_use]
    pub fn new(
        code: PluginInvocationErrorCodeV2,
        call_id: Option<PluginCallIdV2>,
        binding_id: Option<PluginBindingIdV2>,
        runtime_generation: Option<PluginRuntimeGenerationV2>,
    ) -> Self {
        Self { code, call_id, binding_id, runtime_generation }
    }

    /// Returns the stable reason code suitable for metadata trace.
    #[must_use]
    pub const fn reason_code(&self) -> &'static str {
        self.code.reason_code()
    }
}

impl fmt::Display for PluginInvocationErrorV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.reason_code())
    }
}

impl Error for PluginInvocationErrorV2 {}

/// Host acknowledgement emitted before guest execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginInvocationAcceptedV2 {
    /// Invocation identity.
    pub call_id: PluginCallIdV2,
    /// Binding identity.
    pub binding_id: PluginBindingIdV2,
    /// Runtime generation that accepted the call.
    pub runtime_generation: PluginRuntimeGenerationV2,
}

/// One bounded guest event emitted after acceptance.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginInvocationEventV2 {
    /// Invocation identity.
    pub call_id: PluginCallIdV2,
    /// One-based monotonic sequence number.
    pub sequence: u32,
    /// Output schema hash under which the event is interpreted.
    pub schema_hash: PluginSchemaHashV2,
    /// Contract-specific serialized event.
    pub event_bytes: Vec<u8>,
}

impl fmt::Debug for PluginInvocationEventV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginInvocationEventV2")
            .field("call_id", &self.call_id)
            .field("sequence", &self.sequence)
            .field("schema_hash", &self.schema_hash)
            .field("event_bytes", &format_args!("<redacted:{} bytes>", self.event_bytes.len()))
            .finish()
    }
}

/// Cancellation classifications visible across the ABI boundary.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PluginCancellationReasonV2 {
    /// Operator or caller requested cancellation.
    Requested,
    /// Absolute invocation deadline elapsed.
    Deadline,
    /// Binding disposal cancelled the active work.
    BindingDisposed,
    /// Quarantine cancelled active work.
    BindingQuarantined,
}

/// Exactly-one terminal outcome for a plugin invocation.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PluginInvocationTerminalOutcomeV2 {
    /// Guest completed with typed output bytes.
    Completed {
        /// Schema hash applied to `output_bytes`.
        schema_hash: PluginSchemaHashV2,
        /// Contract-specific serialized result.
        output_bytes: Vec<u8>,
    },
    /// Invocation failed closed with a redacted error.
    Failed {
        /// Stable diagnostic.
        error: PluginInvocationErrorV2,
    },
    /// Invocation observed cooperative cancellation.
    Cancelled {
        /// Stable cancellation classification.
        reason: PluginCancellationReasonV2,
    },
}

impl fmt::Debug for PluginInvocationTerminalOutcomeV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed { schema_hash, output_bytes } => formatter
                .debug_struct("Completed")
                .field("schema_hash", schema_hash)
                .field("output_bytes", &format_args!("<redacted:{} bytes>", output_bytes.len()))
                .finish(),
            Self::Failed { error } => {
                formatter.debug_struct("Failed").field("error", error).finish()
            }
            Self::Cancelled { reason } => {
                formatter.debug_struct("Cancelled").field("reason", reason).finish()
            }
        }
    }
}

/// Terminal frame emitted once and only once for an accepted call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginInvocationTerminalV2 {
    /// Invocation identity.
    pub call_id: PluginCallIdV2,
    /// Terminal outcome.
    pub outcome: PluginInvocationTerminalOutcomeV2,
}

/// Ordered frame in an invocation transcript.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "frame", rename_all = "snake_case")]
pub enum PluginInvocationFrameV2 {
    /// First frame emitted for an admitted invocation.
    Accepted(PluginInvocationAcceptedV2),
    /// Zero or more bounded guest events.
    Event(PluginInvocationEventV2),
    /// Final frame, present exactly once.
    Terminal(PluginInvocationTerminalV2),
}

/// Lifecycle validation failure for an invocation transcript.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PluginLifecycleErrorV2 {
    /// Transcript does not begin with acceptance.
    AcceptedFrameMissing,
    /// More than one accepted frame was present.
    DuplicateAcceptedFrame,
    /// Event sequence is not contiguous and one-based.
    EventSequenceInvalid,
    /// A frame uses a different call identity.
    CallIdMismatch,
    /// Transcript does not end in exactly one terminal frame.
    TerminalFrameInvalid,
}

impl fmt::Display for PluginLifecycleErrorV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AcceptedFrameMissing => "accepted frame missing",
            Self::DuplicateAcceptedFrame => "duplicate accepted frame",
            Self::EventSequenceInvalid => "invalid event sequence",
            Self::CallIdMismatch => "invocation call id mismatch",
            Self::TerminalFrameInvalid => "invalid terminal frame count or position",
        })
    }
}

impl Error for PluginLifecycleErrorV2 {}

/// Validated accepted/event/terminal transcript for one call.
#[derive(Clone, Serialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct PluginInvocationTranscriptV2(Vec<PluginInvocationFrameV2>);

impl PluginInvocationTranscriptV2 {
    /// Validates ordered frames and constructs a transcript.
    ///
    /// # Errors
    /// Returns [`PluginLifecycleErrorV2`] unless frames contain one accepted
    /// frame first, contiguous events, and exactly one terminal frame last.
    pub fn from_frames(
        frames: Vec<PluginInvocationFrameV2>,
    ) -> Result<Self, PluginLifecycleErrorV2> {
        let Some(PluginInvocationFrameV2::Accepted(accepted)) = frames.first() else {
            return Err(PluginLifecycleErrorV2::AcceptedFrameMissing);
        };
        let call_id = &accepted.call_id;
        let mut accepted_count = 0_u32;
        let mut terminal_count = 0_u32;
        let mut expected_sequence = 1_u32;
        for (index, frame) in frames.iter().enumerate() {
            match frame {
                PluginInvocationFrameV2::Accepted(current) => {
                    accepted_count = accepted_count.saturating_add(1);
                    if &current.call_id != call_id {
                        return Err(PluginLifecycleErrorV2::CallIdMismatch);
                    }
                    if index != 0 {
                        return Err(PluginLifecycleErrorV2::DuplicateAcceptedFrame);
                    }
                }
                PluginInvocationFrameV2::Event(event) => {
                    if &event.call_id != call_id {
                        return Err(PluginLifecycleErrorV2::CallIdMismatch);
                    }
                    if event.sequence != expected_sequence {
                        return Err(PluginLifecycleErrorV2::EventSequenceInvalid);
                    }
                    expected_sequence = expected_sequence.saturating_add(1);
                }
                PluginInvocationFrameV2::Terminal(terminal) => {
                    if &terminal.call_id != call_id {
                        return Err(PluginLifecycleErrorV2::CallIdMismatch);
                    }
                    terminal_count = terminal_count.saturating_add(1);
                    if index != frames.len().saturating_sub(1) {
                        return Err(PluginLifecycleErrorV2::TerminalFrameInvalid);
                    }
                }
            }
        }
        if accepted_count != 1 || terminal_count != 1 {
            return Err(PluginLifecycleErrorV2::TerminalFrameInvalid);
        }
        Ok(Self(frames))
    }

    /// Returns the validated ordered frames.
    #[must_use]
    pub fn frames(&self) -> &[PluginInvocationFrameV2] {
        self.0.as_slice()
    }

    /// Returns the terminal outcome guaranteed by construction.
    #[must_use]
    pub fn terminal(&self) -> &PluginInvocationTerminalV2 {
        match self.0.last() {
            Some(PluginInvocationFrameV2::Terminal(terminal)) => terminal,
            _ => unreachable!("validated invocation transcript always ends in a terminal frame"),
        }
    }
}

impl fmt::Debug for PluginInvocationTranscriptV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let event_count = self
            .0
            .iter()
            .filter(|frame| matches!(frame, PluginInvocationFrameV2::Event(_)))
            .count();
        formatter
            .debug_struct("PluginInvocationTranscriptV2")
            .field("frame_count", &self.0.len())
            .field("event_count", &event_count)
            .field("terminal", &self.terminal())
            .finish()
    }
}

/// Current lifecycle state of a registered binding.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PluginBindingStateV2 {
    /// Binding can accept calls.
    Active,
    /// Binding has released all capability handles.
    Disposed,
    /// Binding is isolated until a higher generation is rebound.
    Quarantined,
}

/// Cleanup evidence emitted by dispose or quarantine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginBindingCleanupV2 {
    /// Binding whose resources were released.
    pub binding_id: PluginBindingIdV2,
    /// Generation cleaned up.
    pub runtime_generation: PluginRuntimeGenerationV2,
    /// Final binding state.
    pub final_state: PluginBindingStateV2,
    /// Number of opaque handles released without exposing their values.
    pub released_capability_handle_count: u32,
    /// Stable cleanup reason code.
    pub reason_code: String,
}

/// Redacted runtime diagnostic for one binding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginRuntimeDiagnosticEntryV2 {
    /// Binding identity visible to operators.
    pub binding_id: PluginBindingIdV2,
    /// Executable contract.
    pub contract: ExecutablePluginContractKindV2,
    /// Active runtime generation.
    pub runtime_generation: PluginRuntimeGenerationV2,
    /// Current lifecycle state.
    pub state: PluginBindingStateV2,
    /// Current quarantine strike count.
    pub quarantine_strikes: u32,
    /// Number of opaque handles, without their values or scope contents.
    pub granted_capability_handle_count: u32,
    /// Binding expiry.
    pub expires_at_unix_ms: u64,
}

/// Redacted snapshot of executable ABI v2 runtime state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginRuntimeDiagnosticsV2 {
    /// Diagnostics schema version.
    pub schema_version: u16,
    /// ABI marker.
    pub abi_version: String,
    /// Current binding entries in deterministic identifier order.
    pub bindings: Vec<PluginRuntimeDiagnosticEntryV2>,
}

/// Pass/fail result for a conformance case.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PluginConformanceVerdictV2 {
    /// Guest and host satisfied the lifecycle and security checks.
    Passed,
    /// At least one required check failed.
    Failed,
}

/// One executable contract conformance case.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginConformanceCaseV2 {
    /// Stable fixture identifier.
    pub fixture_id: String,
    /// Contract exercised by the fixture.
    pub contract: ExecutablePluginContractKindV2,
    /// Overall case verdict.
    pub verdict: PluginConformanceVerdictV2,
    /// Whether accepted/event/terminal semantics validated.
    pub lifecycle_valid: bool,
    /// Whether authority and secret boundaries validated.
    pub security_valid: bool,
    /// Stable reason codes for failures or notable denials.
    #[serde(default)]
    pub reason_codes: Vec<String>,
}

/// CI-suitable report for executable ABI v2 conformance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginConformanceReportV2 {
    /// Report schema version.
    pub schema_version: u16,
    /// ABI marker exercised by the report.
    pub abi_version: String,
    /// Deterministically ordered fixture results.
    pub cases: Vec<PluginConformanceCaseV2>,
}

impl PluginConformanceReportV2 {
    /// Returns true only when all six executable contracts have a passing case.
    #[must_use]
    pub fn is_execution_complete(&self) -> bool {
        EXECUTABLE_PLUGIN_CONTRACTS_V2.iter().all(|contract| {
            self.cases.iter().any(|case| {
                case.contract == *contract
                    && case.verdict == PluginConformanceVerdictV2::Passed
                    && case.lifecycle_valid
                    && case.security_valid
            })
        })
    }
}

fn validate_identifier(field: &'static str, value: &str) -> Result<(), PluginAbiValueError> {
    if value.is_empty() || value.len() > MAX_IDENTIFIER_BYTES {
        return Err(PluginAbiValueError::InvalidIdentifierLength { field, length: value.len() });
    }
    if !value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
    }) {
        return Err(PluginAbiValueError::InvalidIdentifierCharacter { field });
    }
    Ok(())
}

fn known_schema_hash(field: &'static str, value: &str) -> PluginSchemaHashV2 {
    PluginSchemaHashV2::parse(field, value)
        .expect("built-in executable plugin schema hashes are canonical sha256")
}

#[cfg(test)]
mod tests {
    use super::{
        executable_plugin_abi_snapshot_v2, executable_plugin_contract_schema_v2,
        ExecutablePluginContractKindV2, PluginBindingIdV2, PluginCallIdV2,
        PluginInvocationAcceptedV2, PluginInvocationFrameV2, PluginInvocationTerminalOutcomeV2,
        PluginInvocationTerminalV2, PluginInvocationTranscriptV2, PluginLifecycleErrorV2,
        PluginRuntimeGenerationV2, EXECUTABLE_PLUGIN_CONTRACTS_V2, PLUGIN_ABI_V2_VERSION,
        WIT_SOURCE_V2,
    };
    use serde_json::Value;
    use std::collections::BTreeMap;

    const EXPECTED_ABI_V2_SNAPSHOT: &str =
        include_str!("../tests/golden/plugin_executable_abi_v2_snapshot.json");

    #[test]
    fn v2_wit_has_capability_specific_worlds_and_migration_posture() {
        assert!(WIT_SOURCE_V2.contains("world agent-harness-plugin-v2"));
        assert!(WIT_SOURCE_V2.contains("world context-engine-plugin-v2"));
        assert!(WIT_SOURCE_V2.contains("world tool-result-middleware-plugin-v2"));
        assert!(WIT_SOURCE_V2.contains("world run-lifecycle-hook-plugin-v2"));
        assert!(WIT_SOURCE_V2.contains("world memory-provider-plugin-v2"));
        assert!(WIT_SOURCE_V2.contains("world model-auth-provider-plugin-v2"));
        assert!(WIT_SOURCE_V2.contains("candidate"));
        assert!(WIT_SOURCE_V2.contains("credential-handle"));
        assert!(WIT_SOURCE_V2.contains("migrate"));
        assert_eq!(PLUGIN_ABI_V2_VERSION, "palyra.plugins.sdk.abi.v2");
    }

    #[test]
    fn executable_contract_schema_hashes_are_distinct_and_canonical() {
        let mut hashes = EXECUTABLE_PLUGIN_CONTRACTS_V2
            .iter()
            .flat_map(|contract| {
                let descriptor = executable_plugin_contract_schema_v2(*contract);
                [
                    descriptor.input_schema_hash.as_str().to_owned(),
                    descriptor.output_schema_hash.as_str().to_owned(),
                ]
            })
            .collect::<Vec<_>>();
        hashes.sort();
        hashes.dedup();
        assert_eq!(hashes.len(), EXECUTABLE_PLUGIN_CONTRACTS_V2.len() * 2);
    }

    #[test]
    fn transcript_rejects_missing_or_duplicate_terminal_frames() {
        let call_id = PluginCallIdV2::new("call-1").expect("fixture call id is valid");
        let binding_id = PluginBindingIdV2::new("binding-1").expect("fixture binding id is valid");
        let generation = PluginRuntimeGenerationV2::new(1).expect("fixture generation is nonzero");
        let accepted = PluginInvocationFrameV2::Accepted(PluginInvocationAcceptedV2 {
            call_id: call_id.clone(),
            binding_id,
            runtime_generation: generation,
        });

        assert_eq!(
            PluginInvocationTranscriptV2::from_frames(vec![accepted.clone()]),
            Err(PluginLifecycleErrorV2::TerminalFrameInvalid)
        );
        let terminal = PluginInvocationFrameV2::Terminal(PluginInvocationTerminalV2 {
            call_id,
            outcome: PluginInvocationTerminalOutcomeV2::Cancelled {
                reason: super::PluginCancellationReasonV2::Requested,
            },
        });
        assert_eq!(
            PluginInvocationTranscriptV2::from_frames(vec![accepted, terminal.clone(), terminal]),
            Err(PluginLifecycleErrorV2::TerminalFrameInvalid)
        );
    }

    #[test]
    fn operation_contract_mapping_covers_supported_kinds() {
        assert_eq!(
            super::ExecutablePluginOperationV2::PlanContext.contract(),
            ExecutablePluginContractKindV2::ContextEngine
        );
    }

    #[test]
    fn executable_abi_v2_snapshot_matches_golden() {
        let value =
            serde_json::to_value(executable_plugin_abi_snapshot_v2()).expect("snapshot serializes");
        let canonical = canonical_json_value(&value);
        let mut actual = serde_json::to_string_pretty(&canonical).expect("snapshot formats");
        actual.push('\n');
        assert_eq!(
            actual,
            EXPECTED_ABI_V2_SNAPSHOT.replace("\r\n", "\n"),
            "executable ABI v2 compatibility snapshot changed; update the golden and migration posture together"
        );
    }

    fn canonical_json_value(value: &Value) -> Value {
        match value {
            Value::Object(object) => {
                let sorted = object
                    .iter()
                    .map(|(key, value)| (key.clone(), canonical_json_value(value)))
                    .collect::<BTreeMap<_, _>>();
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(items) => Value::Array(items.iter().map(canonical_json_value).collect()),
            scalar => scalar.clone(),
        }
    }
}
