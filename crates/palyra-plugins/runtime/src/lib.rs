//! Sandboxed Wasm execution for Palyra plugins.
//!
//! Wraps a wasmtime [`Engine`] with hard execution limits (fuel, memory, tables,
//! instances, and an optional wall-clock timeout) and links the Tier A
//! host-capability imports defined by `palyra-plugins-sdk`. Also hosts the typed
//! plugin contract negotiation performed against daemon adapters.

mod abi_v2;

pub use abi_v2::{PluginConformanceFixtureV2, PluginCoreWasmCancellationTokenV2, PluginRuntimeV2};

use std::{collections::BTreeSet, sync::mpsc, time::Duration};

use palyra_plugins_sdk::{
    host_capability_service_descriptor, typed_plugin_contract_descriptor,
    HostCapabilityServiceDescriptor, HostCapabilityServiceKind, TypedPluginCapabilityClass,
    TypedPluginContractDeclaration, TypedPluginContractDescriptor, TypedPluginContractKind,
    HOST_CAPABILITIES_IMPORT_MODULE, HOST_CAPABILITY_CHANNEL_COUNT_FN,
    HOST_CAPABILITY_CHANNEL_HANDLE_FN, HOST_CAPABILITY_HTTP_COUNT_FN,
    HOST_CAPABILITY_HTTP_HANDLE_FN, HOST_CAPABILITY_SECRET_COUNT_FN,
    HOST_CAPABILITY_SECRET_HANDLE_FN, HOST_CAPABILITY_STORAGE_COUNT_FN,
    HOST_CAPABILITY_STORAGE_HANDLE_FN,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use wasmtime::{
    Caller, Config, Engine, Instance, Linker, Module, Store, StoreLimits, StoreLimitsBuilder,
    TypedFunc,
};

// Handle bases keep each capability class in a disjoint numeric range, so a raw
// handle value alone identifies its class. Plugins and tests rely on these values.
const HTTP_HANDLE_BASE: i32 = 10_000;
const SECRET_HANDLE_BASE: i32 = 20_000;
const STORAGE_HANDLE_BASE: i32 = 30_000;
const CHANNEL_HANDLE_BASE: i32 = 40_000;
// With a timeout, a single epoch increment from the watchdog thread must interrupt
// execution. Without one, nothing increments the epoch, so the deadline is placed
// far enough away that epoch interruption can never fire.
const EPOCH_DEADLINE_TICKS_WITH_TIMEOUT: u64 = 1;
const EPOCH_DEADLINE_TICKS_WITHOUT_TIMEOUT: u64 = 1_000_000_000;

/// Hard per-execution resource limits enforced on plugin modules.
///
/// Limits fail closed: exceeding any of them aborts the execution with
/// [`RuntimeError::ExecutionLimitExceeded`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeLimits {
    /// Wasmtime fuel budget for the whole invocation, roughly proportional to
    /// the number of executed instructions.
    pub fuel_budget: u64,
    /// Maximum linear memory the store may grow to, in bytes.
    pub max_memory_bytes: usize,
    /// Maximum number of table elements across all tables in the store.
    pub max_table_elements: usize,
    /// Maximum number of module instances the store may host.
    pub max_instances: usize,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            fuel_budget: 10_000_000,
            max_memory_bytes: 64 * 1024 * 1024,
            max_table_elements: 100_000,
            max_instances: 256,
        }
    }
}

/// Capability grants requested for one plugin invocation, grouped by Tier A
/// capability class.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CapabilityGrantSet {
    /// HTTP hosts the plugin may reach.
    pub http_hosts: Vec<String>,
    /// Secret keys the plugin may resolve.
    pub secret_keys: Vec<String>,
    /// Storage key prefixes the plugin may read and write under.
    pub storage_prefixes: Vec<String>,
    /// Channels the plugin may address.
    pub channels: Vec<String>,
}

impl CapabilityGrantSet {
    /// Returns a copy with every list trimmed, stripped of empty entries,
    /// sorted, and deduplicated.
    ///
    /// Handle derivation in [`CapabilityHandles::from_grants`] depends on this
    /// canonical order being deterministic.
    #[must_use]
    pub fn canonicalized(&self) -> Self {
        Self {
            http_hosts: dedupe_sorted(self.http_hosts.as_slice()),
            secret_keys: dedupe_sorted(self.secret_keys.as_slice()),
            storage_prefixes: dedupe_sorted(self.storage_prefixes.as_slice()),
            channels: dedupe_sorted(self.channels.as_slice()),
        }
    }
}

/// Opaque integer handles issued to a plugin for its granted capabilities.
///
/// Handle values are deterministic for a given canonical grant order and sit in
/// a disjoint numeric range per capability class.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CapabilityHandles {
    /// Handles for granted HTTP hosts.
    pub http_handles: Vec<i32>,
    /// Handles for granted secret keys.
    pub secret_handles: Vec<i32>,
    /// Handles for granted storage prefixes.
    pub storage_handles: Vec<i32>,
    /// Handles for granted channels.
    pub channel_handles: Vec<i32>,
}

impl CapabilityHandles {
    /// Derives handles from `grants` after canonicalization; each handle's
    /// index matches the index of its grant in the canonical list.
    #[must_use]
    pub fn from_grants(grants: &CapabilityGrantSet) -> Self {
        let grants = grants.canonicalized();
        Self {
            http_handles: build_handles(grants.http_hosts.as_slice(), HTTP_HANDLE_BASE),
            secret_handles: build_handles(grants.secret_keys.as_slice(), SECRET_HANDLE_BASE),
            storage_handles: build_handles(grants.storage_prefixes.as_slice(), STORAGE_HANDLE_BASE),
            channel_handles: build_handles(grants.channels.as_slice(), CHANNEL_HANDLE_BASE),
        }
    }
}

/// A daemon adapter's declared support for one typed plugin contract kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedPluginContractAdapterSupport {
    /// Contract kind the adapter serves.
    pub kind: TypedPluginContractKind,
    /// Stable identifier of the daemon-side adapter.
    pub adapter: String,
    /// Contract versions the adapter accepts.
    #[serde(default)]
    pub supported_versions: Vec<u32>,
    /// Capability classes plugins bound to this adapter may request.
    #[serde(default)]
    pub allowed_capability_classes: Vec<TypedPluginCapabilityClass>,
}

/// Negotiation mode a plugin runs under.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypedPluginContractMode {
    /// Plugin declared no typed contracts and runs through the legacy untyped path.
    #[default]
    UntypedLegacy,
    /// Plugin declared typed contracts and went through negotiation.
    Typed,
}

/// Outcome of negotiating a single typed contract declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypedPluginContractStatus {
    /// The declaration is supported by the host and a daemon adapter.
    Accepted,
    /// The declaration cannot be served; see the entry's rejection reasons.
    Rejected,
}

/// Negotiation result for one declared typed contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedPluginContractNegotiationEntry {
    /// Contract kind that was declared.
    pub kind: TypedPluginContractKind,
    /// Contract version the plugin asked for.
    pub requested_version: u32,
    /// Whether the declaration was accepted or rejected.
    pub status: TypedPluginContractStatus,
    /// Daemon adapter matched for the kind, if one exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adapter: Option<String>,
    /// Host-published descriptor for the requested kind/version, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub descriptor: Option<TypedPluginContractDescriptor>,
    /// Human-readable rejection reasons; empty when accepted.
    #[serde(default)]
    pub reasons: Vec<String>,
}

/// Aggregate outcome of typed plugin contract negotiation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedPluginContractNegotiationReport {
    /// Mode the plugin will run under.
    pub mode: TypedPluginContractMode,
    /// True when every declared contract was accepted; legacy mode is always ready.
    pub ready: bool,
    /// Per-declaration results; empty in legacy mode.
    #[serde(default)]
    pub entries: Vec<TypedPluginContractNegotiationEntry>,
}

impl Default for TypedPluginContractNegotiationReport {
    fn default() -> Self {
        Self { mode: TypedPluginContractMode::UntypedLegacy, ready: true, entries: Vec::new() }
    }
}

/// Borrowed inputs for [`negotiate_typed_plugin_contracts`].
pub struct TypedPluginContractNegotiationInput<'a> {
    /// Typed contracts the plugin declares it implements.
    pub declarations: &'a [TypedPluginContractDeclaration],
    /// Capability classes the plugin requests across all contracts.
    pub capability_classes: &'a [TypedPluginCapabilityClass],
    /// Typed adapters the daemon currently exposes.
    pub adapters: &'a [TypedPluginContractAdapterSupport],
}

/// Service grants for capability-scoped Wasm host calls.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HostCapabilityServiceGrantSet {
    /// Host services the plugin may call.
    pub allowed_services: Vec<HostCapabilityServiceKind>,
    /// Tier A data-plane grants that service descriptors may require.
    pub capability_grants: CapabilityGrantSet,
}

impl HostCapabilityServiceGrantSet {
    /// Returns a canonical copy with stable service order and canonical data grants.
    #[must_use]
    pub fn canonicalized(&self) -> Self {
        Self {
            allowed_services: self
                .allowed_services
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            capability_grants: self.capability_grants.canonicalized(),
        }
    }
}

/// Request metadata for one Wasm host service call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmHostCallRequest {
    /// Service being requested.
    pub service: HostCapabilityServiceKind,
    /// Serialized payload size before host parsing.
    pub payload_bytes: u64,
    /// Requested timeout in milliseconds.
    pub timeout_ms: u64,
}

/// Authorization status for one Wasm host service call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WasmHostCallStatus {
    /// Host call may be dispatched after policy enforcement.
    Allowed,
    /// Host call is denied before dispatch.
    Denied,
}

/// Authorization decision for one Wasm host service call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmHostCallDecision {
    /// Requested service.
    pub service: HostCapabilityServiceKind,
    /// Decision status.
    pub status: WasmHostCallStatus,
    /// Stable reason code.
    pub reason_code: String,
    /// Host service descriptor used for the decision.
    pub descriptor: HostCapabilityServiceDescriptor,
    /// Audit event the daemon should record.
    pub audit_event: String,
    /// Redacted fields that must not be logged raw.
    #[serde(default)]
    pub redacted_fields: Vec<String>,
}

/// Authorizes a Wasm host service call against service grants, data grants,
/// timeout, and payload budgets without dispatching the call.
#[must_use]
pub fn authorize_wasm_host_call(
    grants: &HostCapabilityServiceGrantSet,
    request: &WasmHostCallRequest,
) -> WasmHostCallDecision {
    let grants = grants.canonicalized();
    let descriptor = host_capability_service_descriptor(request.service);
    if !grants.allowed_services.contains(&request.service) {
        return wasm_host_call_decision(
            request.service,
            WasmHostCallStatus::Denied,
            "plugin.host_call.denied.service_grant_missing",
            descriptor,
        );
    }
    if request.payload_bytes > descriptor.max_payload_bytes {
        return wasm_host_call_decision(
            request.service,
            WasmHostCallStatus::Denied,
            "plugin.host_call.denied.payload_budget_exceeded",
            descriptor,
        );
    }
    if request.timeout_ms > descriptor.default_timeout_ms {
        return wasm_host_call_decision(
            request.service,
            WasmHostCallStatus::Denied,
            "plugin.host_call.denied.timeout_budget_exceeded",
            descriptor,
        );
    }
    if let Some(required) = descriptor.required_capability_class {
        if !capability_class_granted(&grants.capability_grants, required) {
            return wasm_host_call_decision(
                request.service,
                WasmHostCallStatus::Denied,
                "plugin.host_call.denied.capability_class_missing",
                descriptor,
            );
        }
    }
    wasm_host_call_decision(
        request.service,
        WasmHostCallStatus::Allowed,
        "plugin.host_call.allowed",
        descriptor,
    )
}

fn capability_class_granted(
    grants: &CapabilityGrantSet,
    required: TypedPluginCapabilityClass,
) -> bool {
    match required {
        TypedPluginCapabilityClass::HttpHosts => !grants.http_hosts.is_empty(),
        TypedPluginCapabilityClass::Secrets => !grants.secret_keys.is_empty(),
        TypedPluginCapabilityClass::StoragePrefixes => !grants.storage_prefixes.is_empty(),
        TypedPluginCapabilityClass::Channels => !grants.channels.is_empty(),
    }
}

fn wasm_host_call_decision(
    service: HostCapabilityServiceKind,
    status: WasmHostCallStatus,
    reason_code: &str,
    descriptor: HostCapabilityServiceDescriptor,
) -> WasmHostCallDecision {
    let audit_event = if status == WasmHostCallStatus::Allowed {
        descriptor.audit_event.clone()
    } else {
        "plugin.host_call.denied".to_owned()
    };
    WasmHostCallDecision {
        service,
        status,
        reason_code: reason_code.to_owned(),
        redacted_fields: descriptor.redacted_fields.clone(),
        descriptor,
        audit_event,
    }
}

/// Negotiates a plugin's declared typed contracts against host descriptors and
/// daemon adapters.
///
/// With no declarations the plugin is treated as a legacy untyped plugin and
/// the report is immediately ready. Otherwise each declaration is checked
/// against the host-published descriptor, the matching daemon adapter, the
/// adapter's supported versions, and its allowed capability classes; the
/// report is ready only if every declaration is accepted.
#[must_use]
pub fn negotiate_typed_plugin_contracts(
    input: TypedPluginContractNegotiationInput<'_>,
) -> TypedPluginContractNegotiationReport {
    if input.declarations.is_empty() {
        return TypedPluginContractNegotiationReport::default();
    }

    let capability_classes = canonicalized_capability_classes(input.capability_classes);
    let mut entries = Vec::with_capacity(input.declarations.len());
    for declaration in input.declarations {
        let mut reasons = Vec::new();
        let descriptor = typed_plugin_contract_descriptor(declaration.kind, declaration.version);
        if descriptor.is_none() {
            reasons.push(format!(
                "host does not publish contract {} version {}",
                declaration.kind.as_str(),
                declaration.version
            ));
        }

        let Some(adapter) =
            input.adapters.iter().find(|candidate| candidate.kind == declaration.kind)
        else {
            reasons.push(format!(
                "daemon does not expose a typed adapter for {}",
                declaration.kind.as_str()
            ));
            entries.push(TypedPluginContractNegotiationEntry {
                kind: declaration.kind,
                requested_version: declaration.version,
                status: TypedPluginContractStatus::Rejected,
                adapter: None,
                descriptor,
                reasons,
            });
            continue;
        };

        let supported_versions = canonicalized_versions(adapter.supported_versions.as_slice());
        if !supported_versions.contains(&declaration.version) {
            reasons.push(format!(
                "adapter '{}' supports versions {}",
                adapter.adapter,
                join_u32_values(supported_versions.as_slice())
            ));
        }

        let allowed_capability_classes =
            canonicalized_capability_classes(adapter.allowed_capability_classes.as_slice());
        let unsupported_capability_classes = capability_classes
            .iter()
            .copied()
            .filter(|class| !allowed_capability_classes.contains(class))
            .map(TypedPluginCapabilityClass::as_str)
            .collect::<Vec<_>>();
        if !unsupported_capability_classes.is_empty() {
            reasons.push(format!(
                "adapter '{}' does not allow capability classes {}",
                adapter.adapter,
                unsupported_capability_classes.join(", ")
            ));
        }

        let status = if reasons.is_empty() {
            TypedPluginContractStatus::Accepted
        } else {
            TypedPluginContractStatus::Rejected
        };
        entries.push(TypedPluginContractNegotiationEntry {
            kind: declaration.kind,
            requested_version: declaration.version,
            status,
            adapter: Some(adapter.adapter.clone()),
            descriptor,
            reasons,
        });
    }

    TypedPluginContractNegotiationReport {
        mode: TypedPluginContractMode::Typed,
        ready: entries.iter().all(|entry| entry.status == TypedPluginContractStatus::Accepted),
        entries,
    }
}

/// Result of a successful plugin entrypoint invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmExecutionResult {
    /// Value returned by the plugin's `() -> i32` entrypoint.
    pub exit_code: i32,
    /// Capability handles that were issued to the plugin for this invocation.
    pub capability_handles: CapabilityHandles,
}

/// Errors produced while compiling, linking, or executing a plugin module.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// Module bytes failed to compile or validate (also covers engine setup).
    #[error("failed to compile wasm module: {0}")]
    Compile(#[from] wasmtime::Error),
    /// Host capability imports could not be registered, or module imports
    /// failed to link during instantiation.
    #[error("failed to link wasm host capability interface: {0}")]
    Linker(wasmtime::Error),
    /// The module trapped or faulted during execution.
    #[error("wasm execution failed: {0}")]
    Execution(wasmtime::Error),
    /// The wall-clock timeout elapsed before the entrypoint returned.
    #[error("wasm execution timed out")]
    ExecutionTimedOut,
    /// The requested entrypoint is not exported or does not have the expected
    /// `() -> i32` signature.
    #[error("failed to resolve exported function '{0}'")]
    MissingExport(String),
    /// A fuel, memory, table, or instance limit from [`RuntimeLimits`] was hit.
    #[error("wasm execution exceeded runtime limits")]
    ExecutionLimitExceeded,
}

/// Reusable wasmtime engine that executes plugin modules under [`RuntimeLimits`].
///
/// Fuel metering and epoch interruption are always enabled, so every execution
/// stays bounded even for hostile modules.
pub struct WasmRuntime {
    engine: Engine,
    limits: RuntimeLimits,
}

pub(crate) fn build_runtime_engine() -> Result<Engine, RuntimeError> {
    let mut config = Config::new();
    config.consume_fuel(true);
    config.epoch_interruption(true);
    Ok(Engine::new(&config)?)
}

impl WasmRuntime {
    /// Creates a runtime with [`RuntimeLimits::default`].
    ///
    /// # Errors
    /// Returns [`RuntimeError::Compile`] if the wasmtime engine cannot be built.
    pub fn new() -> Result<Self, RuntimeError> {
        Self::new_with_limits(RuntimeLimits::default())
    }

    /// Creates a runtime that enforces the given `limits` on every execution.
    ///
    /// # Errors
    /// Returns [`RuntimeError::Compile`] if the wasmtime engine cannot be built.
    pub fn new_with_limits(limits: RuntimeLimits) -> Result<Self, RuntimeError> {
        let engine = build_runtime_engine()?;
        Ok(Self { engine, limits })
    }

    /// Compiles `module_bytes` and calls the `() -> i32` export `export_name`
    /// with no capability grants.
    ///
    /// # Errors
    /// Same failure modes as [`WasmRuntime::execute_i32_entrypoint`].
    pub fn call_noarg_i32_export(
        &self,
        module_bytes: &[u8],
        export_name: &str,
    ) -> Result<i32, RuntimeError> {
        let result =
            self.execute_i32_entrypoint(module_bytes, export_name, &CapabilityGrantSet::default())?;
        Ok(result.exit_code)
    }

    /// Compiles `module_bytes` and invokes the `() -> i32` export `entrypoint`
    /// with capability handles derived from `capabilities`.
    ///
    /// Execution is bounded by the runtime's fuel budget and store limits, but
    /// not by wall-clock time; use
    /// [`WasmRuntime::execute_i32_entrypoint_with_timeout`] for that.
    ///
    /// # Errors
    /// Returns [`RuntimeError::Compile`] for invalid module bytes,
    /// [`RuntimeError::Linker`] when imports cannot be satisfied,
    /// [`RuntimeError::MissingExport`] when `entrypoint` is absent or has the
    /// wrong signature, [`RuntimeError::ExecutionLimitExceeded`] when a
    /// [`RuntimeLimits`] bound is hit, and [`RuntimeError::Execution`] for any
    /// other trap.
    pub fn execute_i32_entrypoint(
        &self,
        module_bytes: &[u8],
        entrypoint: &str,
        capabilities: &CapabilityGrantSet,
    ) -> Result<WasmExecutionResult, RuntimeError> {
        self.execute_i32_entrypoint_internal(module_bytes, entrypoint, capabilities, None)
    }

    /// Like [`WasmRuntime::execute_i32_entrypoint`], but additionally aborts
    /// the execution once `timeout` of wall-clock time has elapsed.
    ///
    /// On targets without 64-bit atomics the timeout cannot be armed and only
    /// fuel and store limits bound the execution.
    ///
    /// Timed executions use a per-invocation engine so one timeout watchdog cannot
    /// interrupt another concurrent timed invocation.
    ///
    /// # Errors
    /// Same as [`WasmRuntime::execute_i32_entrypoint`], plus
    /// [`RuntimeError::ExecutionTimedOut`] when the timeout elapses first.
    pub fn execute_i32_entrypoint_with_timeout(
        &self,
        module_bytes: &[u8],
        entrypoint: &str,
        capabilities: &CapabilityGrantSet,
        timeout: Duration,
    ) -> Result<WasmExecutionResult, RuntimeError> {
        self.execute_i32_entrypoint_internal(module_bytes, entrypoint, capabilities, Some(timeout))
    }

    fn execute_i32_entrypoint_internal(
        &self,
        module_bytes: &[u8],
        entrypoint: &str,
        capabilities: &CapabilityGrantSet,
        timeout: Option<Duration>,
    ) -> Result<WasmExecutionResult, RuntimeError> {
        let engine = if timeout.is_some() { build_runtime_engine()? } else { self.engine.clone() };
        let module = Module::new(&engine, module_bytes)?;
        let capability_handles = CapabilityHandles::from_grants(capabilities);
        let store_limits = StoreLimitsBuilder::new()
            .memory_size(self.limits.max_memory_bytes)
            .table_elements(self.limits.max_table_elements)
            .instances(self.limits.max_instances)
            .build();
        let mut store = Store::new(
            &engine,
            RuntimeStoreState {
                limits: store_limits,
                capability_handles: capability_handles.clone(),
            },
        );
        store.limiter(|state| &mut state.limits);
        store.set_fuel(self.limits.fuel_budget)?;
        configure_epoch_deadline(&mut store, timeout.is_some());
        // The named binding keeps the watchdog armed until this function returns;
        // `let _ = ...` would drop the guard (cancelling the timeout) immediately.
        let _timeout_guard =
            timeout.map(|duration| arm_epoch_timeout_guard(engine.clone(), duration));
        let instance = Self::instantiate_with_linker(&engine, &module, &mut store)?;
        let function: TypedFunc<(), i32> = instance
            .get_typed_func(&mut store, entrypoint)
            .map_err(|_| RuntimeError::MissingExport(entrypoint.to_owned()))?;
        let output = function
            .call(&mut store, ())
            .map_err(|error| map_execution_error_with_store(error, &store))?;
        Ok(WasmExecutionResult { exit_code: output, capability_handles })
    }

    fn instantiate_with_linker(
        engine: &Engine,
        module: &Module,
        store: &mut Store<RuntimeStoreState>,
    ) -> Result<Instance, RuntimeError> {
        let mut linker = Linker::new(engine);
        register_capability_bindings(&mut linker)?;
        linker
            .instantiate(&mut *store, module)
            .map_err(|error| map_instantiate_error_with_store(error, store))
    }
}

struct RuntimeStoreState {
    limits: StoreLimits,
    capability_handles: CapabilityHandles,
}

fn register_capability_bindings(
    linker: &mut Linker<RuntimeStoreState>,
) -> Result<(), RuntimeError> {
    linker
        .func_wrap(HOST_CAPABILITIES_IMPORT_MODULE, HOST_CAPABILITY_HTTP_COUNT_FN, host_http_count)
        .map_err(RuntimeError::Linker)?;
    linker
        .func_wrap(
            HOST_CAPABILITIES_IMPORT_MODULE,
            HOST_CAPABILITY_HTTP_HANDLE_FN,
            host_http_handle,
        )
        .map_err(RuntimeError::Linker)?;
    linker
        .func_wrap(
            HOST_CAPABILITIES_IMPORT_MODULE,
            HOST_CAPABILITY_SECRET_COUNT_FN,
            host_secret_count,
        )
        .map_err(RuntimeError::Linker)?;
    linker
        .func_wrap(
            HOST_CAPABILITIES_IMPORT_MODULE,
            HOST_CAPABILITY_SECRET_HANDLE_FN,
            host_secret_handle,
        )
        .map_err(RuntimeError::Linker)?;
    linker
        .func_wrap(
            HOST_CAPABILITIES_IMPORT_MODULE,
            HOST_CAPABILITY_STORAGE_COUNT_FN,
            host_storage_count,
        )
        .map_err(RuntimeError::Linker)?;
    linker
        .func_wrap(
            HOST_CAPABILITIES_IMPORT_MODULE,
            HOST_CAPABILITY_STORAGE_HANDLE_FN,
            host_storage_handle,
        )
        .map_err(RuntimeError::Linker)?;
    linker
        .func_wrap(
            HOST_CAPABILITIES_IMPORT_MODULE,
            HOST_CAPABILITY_CHANNEL_COUNT_FN,
            host_channel_count,
        )
        .map_err(RuntimeError::Linker)?;
    linker
        .func_wrap(
            HOST_CAPABILITIES_IMPORT_MODULE,
            HOST_CAPABILITY_CHANNEL_HANDLE_FN,
            host_channel_handle,
        )
        .map_err(RuntimeError::Linker)?;
    Ok(())
}

// Tier A host imports. The `len() as i32` casts cannot truncate in practice:
// handle tables are bounded by per-invocation grant lists, which stay far below
// `i32::MAX` entries.
fn host_http_count(caller: Caller<'_, RuntimeStoreState>) -> i32 {
    caller.data().capability_handles.http_handles.len() as i32
}

fn host_http_handle(caller: Caller<'_, RuntimeStoreState>, index: i32) -> i32 {
    resolve_capability_handle(caller.data().capability_handles.http_handles.as_slice(), index)
}

fn host_secret_count(caller: Caller<'_, RuntimeStoreState>) -> i32 {
    caller.data().capability_handles.secret_handles.len() as i32
}

fn host_secret_handle(caller: Caller<'_, RuntimeStoreState>, index: i32) -> i32 {
    resolve_capability_handle(caller.data().capability_handles.secret_handles.as_slice(), index)
}

fn host_storage_count(caller: Caller<'_, RuntimeStoreState>) -> i32 {
    caller.data().capability_handles.storage_handles.len() as i32
}

fn host_storage_handle(caller: Caller<'_, RuntimeStoreState>, index: i32) -> i32 {
    resolve_capability_handle(caller.data().capability_handles.storage_handles.as_slice(), index)
}

fn host_channel_count(caller: Caller<'_, RuntimeStoreState>) -> i32 {
    caller.data().capability_handles.channel_handles.len() as i32
}

fn host_channel_handle(caller: Caller<'_, RuntimeStoreState>, index: i32) -> i32 {
    resolve_capability_handle(caller.data().capability_handles.channel_handles.as_slice(), index)
}

/// Maps a plugin-supplied index to a granted handle; `-1` signals a negative or
/// out-of-range index, as specified by the WIT host-capabilities contract.
fn resolve_capability_handle(handles: &[i32], index: i32) -> i32 {
    if index < 0 {
        return -1;
    }
    handles.get(index as usize).copied().unwrap_or(-1)
}

// Epoch interruption requires 64-bit atomics; on targets without them the
// deadline cannot be armed and bounded execution relies on fuel metering alone.
pub(crate) fn configure_epoch_deadline<T>(store: &mut Store<T>, timeout_enabled: bool) {
    #[cfg(target_has_atomic = "64")]
    {
        let delta = if timeout_enabled {
            EPOCH_DEADLINE_TICKS_WITH_TIMEOUT
        } else {
            EPOCH_DEADLINE_TICKS_WITHOUT_TIMEOUT
        };
        store.set_epoch_deadline(delta);
    }
    #[cfg(not(target_has_atomic = "64"))]
    let _ = (store, timeout_enabled);
}

/// Cancels the timeout watchdog when dropped, before it can bump the epoch.
pub(crate) struct EpochTimeoutGuard {
    cancel_tx: Option<mpsc::Sender<()>>,
}

impl Drop for EpochTimeoutGuard {
    fn drop(&mut self) {
        if let Some(cancel_tx) = self.cancel_tx.take() {
            let _ = cancel_tx.send(());
        }
    }
}

/// Spawns a watchdog thread that bumps the engine epoch after `timeout`,
/// interrupting the store that was armed with a one-tick deadline.
pub(crate) fn arm_epoch_timeout_guard(engine: Engine, timeout: Duration) -> EpochTimeoutGuard {
    #[cfg(target_has_atomic = "64")]
    {
        let (cancel_tx, cancel_rx) = mpsc::channel::<()>();
        std::thread::spawn(move || match cancel_rx.recv_timeout(timeout) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => engine.increment_epoch(),
        });
        EpochTimeoutGuard { cancel_tx: Some(cancel_tx) }
    }
    #[cfg(not(target_has_atomic = "64"))]
    {
        let _ = (engine, timeout);
        EpochTimeoutGuard { cancel_tx: None }
    }
}

fn map_instantiate_error_with_store(
    error: wasmtime::Error,
    store: &Store<RuntimeStoreState>,
) -> RuntimeError {
    if is_timeout_error(&error) {
        return RuntimeError::ExecutionTimedOut;
    }
    if is_execution_limit_error(&error, store) {
        return RuntimeError::ExecutionLimitExceeded;
    }
    RuntimeError::Linker(error)
}

fn map_execution_error_with_store(
    error: wasmtime::Error,
    store: &Store<RuntimeStoreState>,
) -> RuntimeError {
    if is_timeout_error(&error) {
        return RuntimeError::ExecutionTimedOut;
    }
    if is_execution_limit_error(&error, store) {
        return RuntimeError::ExecutionLimitExceeded;
    }
    RuntimeError::Execution(error)
}

pub(crate) fn is_timeout_error(error: &wasmtime::Error) -> bool {
    matches!(error.downcast_ref::<wasmtime::Trap>(), Some(wasmtime::Trap::Interrupt))
}

pub(crate) fn is_execution_limit_error<T>(error: &wasmtime::Error, store: &Store<T>) -> bool {
    // An exhausted fuel budget counts as a limit violation even when the failure
    // surfaces as a different trap first, and store-limit violations raised during
    // instantiation are only identifiable by their message text.
    store.get_fuel().ok() == Some(0)
        || matches!(
            error.downcast_ref::<wasmtime::Trap>(),
            Some(wasmtime::Trap::OutOfFuel | wasmtime::Trap::AllocationTooLarge)
        )
        || error_chain_contains_any(error, &["resource limit exceeded", "exceeds memory limits"])
}

fn error_chain_contains_any(error: &wasmtime::Error, needles: &[&str]) -> bool {
    let message = error.to_string();
    if needles.iter().any(|needle| message.contains(needle)) {
        return true;
    }
    let mut source = error.source();
    while let Some(current) = source {
        let message = current.to_string();
        if needles.iter().any(|needle| message.contains(needle)) {
            return true;
        }
        source = current.source();
    }
    false
}

fn dedupe_sorted(values: &[String]) -> Vec<String> {
    let mut normalized = values
        .iter()
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

fn canonicalized_capability_classes(
    values: &[TypedPluginCapabilityClass],
) -> Vec<TypedPluginCapabilityClass> {
    values.iter().copied().collect::<BTreeSet<_>>().into_iter().collect()
}

fn canonicalized_versions(values: &[u32]) -> Vec<u32> {
    values.iter().copied().collect::<BTreeSet<_>>().into_iter().collect()
}

fn join_u32_values(values: &[u32]) -> String {
    values.iter().map(u32::to_string).collect::<Vec<_>>().join(", ")
}

fn build_handles(values: &[String], base: i32) -> Vec<i32> {
    values.iter().enumerate().map(|(index, _)| base + index as i32).collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        authorize_wasm_host_call, negotiate_typed_plugin_contracts, CapabilityGrantSet,
        HostCapabilityServiceGrantSet, RuntimeError, RuntimeLimits,
        TypedPluginContractAdapterSupport, TypedPluginContractMode,
        TypedPluginContractNegotiationInput, TypedPluginContractStatus, WasmHostCallRequest,
        WasmHostCallStatus, WasmRuntime,
    };
    use palyra_plugins_sdk::{
        HostCapabilityServiceKind, TypedPluginCapabilityClass, TypedPluginContractDeclaration,
        TypedPluginContractKind, DEFAULT_RUNTIME_ENTRYPOINT, HOST_CAPABILITIES_IMPORT_MODULE,
        HOST_CAPABILITY_CHANNEL_COUNT_FN, HOST_CAPABILITY_HTTP_COUNT_FN,
        HOST_CAPABILITY_HTTP_HANDLE_FN, HOST_CAPABILITY_SECRET_COUNT_FN,
        HOST_CAPABILITY_STORAGE_COUNT_FN,
    };

    #[test]
    fn typed_contract_negotiation_accepts_supported_declarations() {
        let report = negotiate_typed_plugin_contracts(TypedPluginContractNegotiationInput {
            declarations: &[
                TypedPluginContractDeclaration {
                    kind: TypedPluginContractKind::MemoryProvider,
                    version: 1,
                },
                TypedPluginContractDeclaration {
                    kind: TypedPluginContractKind::ContextEngine,
                    version: 1,
                },
                TypedPluginContractDeclaration {
                    kind: TypedPluginContractKind::RoutingStrategy,
                    version: 1,
                },
            ],
            capability_classes: &[
                TypedPluginCapabilityClass::HttpHosts,
                TypedPluginCapabilityClass::Secrets,
                TypedPluginCapabilityClass::StoragePrefixes,
            ],
            adapters: &[
                TypedPluginContractAdapterSupport {
                    kind: TypedPluginContractKind::MemoryProvider,
                    adapter: "journal.memory_embedding_provider".to_owned(),
                    supported_versions: vec![1],
                    allowed_capability_classes: vec![
                        TypedPluginCapabilityClass::HttpHosts,
                        TypedPluginCapabilityClass::Secrets,
                        TypedPluginCapabilityClass::StoragePrefixes,
                    ],
                },
                TypedPluginContractAdapterSupport {
                    kind: TypedPluginContractKind::ContextEngine,
                    adapter: "application.context_engine".to_owned(),
                    supported_versions: vec![1],
                    allowed_capability_classes: vec![
                        TypedPluginCapabilityClass::HttpHosts,
                        TypedPluginCapabilityClass::Secrets,
                        TypedPluginCapabilityClass::StoragePrefixes,
                    ],
                },
                TypedPluginContractAdapterSupport {
                    kind: TypedPluginContractKind::RoutingStrategy,
                    adapter: "usage_governance.routing".to_owned(),
                    supported_versions: vec![1],
                    allowed_capability_classes: vec![
                        TypedPluginCapabilityClass::HttpHosts,
                        TypedPluginCapabilityClass::Secrets,
                        TypedPluginCapabilityClass::StoragePrefixes,
                    ],
                },
            ],
        });

        assert_eq!(report.mode, TypedPluginContractMode::Typed);
        assert!(report.ready);
        assert_eq!(report.entries.len(), 3);
        assert!(report
            .entries
            .iter()
            .all(|entry| entry.status == TypedPluginContractStatus::Accepted));
    }

    #[test]
    fn typed_contract_negotiation_rejects_incompatible_versions_and_capabilities() {
        let report = negotiate_typed_plugin_contracts(TypedPluginContractNegotiationInput {
            declarations: &[TypedPluginContractDeclaration {
                kind: TypedPluginContractKind::MemoryProvider,
                version: 2,
            }],
            capability_classes: &[TypedPluginCapabilityClass::Channels],
            adapters: &[TypedPluginContractAdapterSupport {
                kind: TypedPluginContractKind::MemoryProvider,
                adapter: "journal.memory_embedding_provider".to_owned(),
                supported_versions: vec![1],
                allowed_capability_classes: vec![
                    TypedPluginCapabilityClass::HttpHosts,
                    TypedPluginCapabilityClass::Secrets,
                    TypedPluginCapabilityClass::StoragePrefixes,
                ],
            }],
        });

        assert!(!report.ready);
        assert_eq!(report.entries.len(), 1);
        assert_eq!(report.entries[0].status, TypedPluginContractStatus::Rejected);
        assert!(report.entries[0]
            .reasons
            .iter()
            .any(|reason| reason.contains("host does not publish contract")));
        assert!(report.entries[0]
            .reasons
            .iter()
            .any(|reason| reason.contains("does not allow capability classes channels")));
    }

    #[test]
    fn typed_contract_negotiation_keeps_legacy_plugins_compatible() {
        let report = negotiate_typed_plugin_contracts(TypedPluginContractNegotiationInput {
            declarations: &[],
            capability_classes: &[],
            adapters: &[],
        });

        assert_eq!(report.mode, TypedPluginContractMode::UntypedLegacy);
        assert!(report.ready);
        assert!(report.entries.is_empty());
    }

    #[test]
    fn host_call_authorization_denies_service_without_explicit_grant() {
        let decision = authorize_wasm_host_call(
            &HostCapabilityServiceGrantSet::default(),
            &WasmHostCallRequest {
                service: HostCapabilityServiceKind::TasksCreate,
                payload_bytes: 128,
                timeout_ms: 100,
            },
        );

        assert_eq!(decision.status, WasmHostCallStatus::Denied);
        assert_eq!(decision.reason_code, "plugin.host_call.denied.service_grant_missing");
        assert_eq!(decision.audit_event, "plugin.host_call.denied");
    }

    #[test]
    fn host_call_authorization_denies_missing_capability_class() {
        let grants = HostCapabilityServiceGrantSet {
            allowed_services: vec![HostCapabilityServiceKind::TasksCreate],
            capability_grants: CapabilityGrantSet::default(),
        };

        let decision = authorize_wasm_host_call(
            &grants,
            &WasmHostCallRequest {
                service: HostCapabilityServiceKind::TasksCreate,
                payload_bytes: 128,
                timeout_ms: 100,
            },
        );

        assert_eq!(decision.status, WasmHostCallStatus::Denied);
        assert_eq!(decision.reason_code, "plugin.host_call.denied.capability_class_missing");
    }

    #[test]
    fn host_call_authorization_allows_vault_lease_metadata_with_secret_grant() {
        let grants = HostCapabilityServiceGrantSet {
            allowed_services: vec![HostCapabilityServiceKind::VaultSecretLeaseRequest],
            capability_grants: CapabilityGrantSet {
                secret_keys: vec!["global/openai_api_key".to_owned()],
                ..CapabilityGrantSet::default()
            },
        };

        let decision = authorize_wasm_host_call(
            &grants,
            &WasmHostCallRequest {
                service: HostCapabilityServiceKind::VaultSecretLeaseRequest,
                payload_bytes: 256,
                timeout_ms: 250,
            },
        );

        assert_eq!(decision.status, WasmHostCallStatus::Allowed);
        assert_eq!(decision.reason_code, "plugin.host_call.allowed");
        assert_eq!(decision.audit_event, "plugin.host_call.invoked");
        assert!(
            !decision.descriptor.returns_secret_material,
            "vault host service must only expose lease metadata"
        );
        assert!(decision.redacted_fields.contains(&"secret.ref".to_owned()));
    }

    #[test]
    fn runtime_can_load_module_and_call_exported_function() {
        let module = br#"
            (module
                (func (export "answer") (result i32)
                    i32.const 42
                )
            )
        "#;
        let runtime = WasmRuntime::new().expect("runtime should initialize");

        let answer = runtime
            .call_noarg_i32_export(module, "answer")
            .expect("module should execute exported function");

        assert_eq!(answer, 42);
    }

    #[test]
    fn runtime_surfaces_capability_counts_and_handles() {
        let module = format!(
            r#"
            (module
                (import "{host_module}" "{http_count_fn}" (func $http_count (result i32)))
                (import "{host_module}" "{secret_count_fn}" (func $secret_count (result i32)))
                (import "{host_module}" "{storage_count_fn}" (func $storage_count (result i32)))
                (import "{host_module}" "{channel_count_fn}" (func $channel_count (result i32)))
                (import "{host_module}" "{http_handle_fn}" (func $http_handle (param i32) (result i32)))
                (func (export "{entrypoint}") (result i32)
                    (local $sum i32)
                    call $http_count
                    local.set $sum
                    local.get $sum
                    call $secret_count
                    i32.add
                    local.set $sum
                    local.get $sum
                    call $storage_count
                    i32.add
                    local.set $sum
                    local.get $sum
                    call $channel_count
                    i32.add
                    drop
                    i32.const 0
                    call $http_handle
                )
            )
            "#,
            host_module = HOST_CAPABILITIES_IMPORT_MODULE,
            http_count_fn = HOST_CAPABILITY_HTTP_COUNT_FN,
            secret_count_fn = HOST_CAPABILITY_SECRET_COUNT_FN,
            storage_count_fn = HOST_CAPABILITY_STORAGE_COUNT_FN,
            channel_count_fn = HOST_CAPABILITY_CHANNEL_COUNT_FN,
            http_handle_fn = HOST_CAPABILITY_HTTP_HANDLE_FN,
            entrypoint = DEFAULT_RUNTIME_ENTRYPOINT,
        );
        let runtime = WasmRuntime::new().expect("runtime should initialize");
        let capabilities = CapabilityGrantSet {
            http_hosts: vec!["api.example.com".to_owned()],
            secret_keys: vec!["db_password".to_owned()],
            storage_prefixes: vec!["plugins/cache".to_owned()],
            channels: vec!["cli".to_owned()],
        };

        let result = runtime
            .execute_i32_entrypoint(module.as_bytes(), DEFAULT_RUNTIME_ENTRYPOINT, &capabilities)
            .expect("module should execute and access capability host imports");

        assert_eq!(result.exit_code, 10_000);
        assert_eq!(result.capability_handles.http_handles, vec![10_000]);
        assert_eq!(result.capability_handles.secret_handles, vec![20_000]);
        assert_eq!(result.capability_handles.storage_handles, vec![30_000]);
        assert_eq!(result.capability_handles.channel_handles, vec![40_000]);
    }

    #[test]
    fn runtime_returns_minus_one_for_out_of_bounds_handle_requests() {
        let module = format!(
            r#"
            (module
                (import "{host_module}" "{http_handle_fn}" (func $http_handle (param i32) (result i32)))
                (func (export "{entrypoint}") (result i32)
                    i32.const 42
                    call $http_handle
                )
            )
            "#,
            host_module = HOST_CAPABILITIES_IMPORT_MODULE,
            http_handle_fn = HOST_CAPABILITY_HTTP_HANDLE_FN,
            entrypoint = DEFAULT_RUNTIME_ENTRYPOINT,
        );
        let runtime = WasmRuntime::new().expect("runtime should initialize");
        let capabilities = CapabilityGrantSet {
            http_hosts: vec!["api.example.com".to_owned()],
            ..Default::default()
        };

        let result = runtime
            .execute_i32_entrypoint(module.as_bytes(), DEFAULT_RUNTIME_ENTRYPOINT, &capabilities)
            .expect("module should execute");

        assert_eq!(result.exit_code, -1);
    }

    #[test]
    fn runtime_interrupts_infinite_loop_with_fuel_limit() {
        let module = format!(
            r#"
            (module
                (func (export "{entrypoint}") (result i32)
                    (loop
                        br 0
                    )
                    i32.const 0
                )
            )
            "#,
            entrypoint = DEFAULT_RUNTIME_ENTRYPOINT,
        );
        let runtime = WasmRuntime::new_with_limits(RuntimeLimits {
            fuel_budget: 5_000,
            ..RuntimeLimits::default()
        })
        .expect("runtime should initialize");

        let result = runtime.execute_i32_entrypoint(
            module.as_bytes(),
            DEFAULT_RUNTIME_ENTRYPOINT,
            &CapabilityGrantSet::default(),
        );

        assert!(
            matches!(result, Err(RuntimeError::ExecutionLimitExceeded)),
            "expected fuel exhaustion error, got: {result:?}"
        );
    }

    #[test]
    fn runtime_rejects_module_exceeding_memory_limit() {
        let module = format!(
            r#"
            (module
                (memory 2000)
                (func (export "{entrypoint}") (result i32)
                    i32.const 42
                )
            )
            "#,
            entrypoint = DEFAULT_RUNTIME_ENTRYPOINT,
        );
        let runtime = WasmRuntime::new().expect("runtime should initialize");

        let result = runtime.execute_i32_entrypoint(
            module.as_bytes(),
            DEFAULT_RUNTIME_ENTRYPOINT,
            &CapabilityGrantSet::default(),
        );

        assert!(
            matches!(result, Err(RuntimeError::ExecutionLimitExceeded)),
            "expected memory limit error, got: {result:?}"
        );
    }

    #[test]
    fn runtime_reports_trap_as_execution_error() {
        let module = format!(
            r#"
            (module
                (func (export "{entrypoint}") (result i32)
                    unreachable
                    i32.const 0
                )
            )
            "#,
            entrypoint = DEFAULT_RUNTIME_ENTRYPOINT,
        );
        let runtime = WasmRuntime::new().expect("runtime should initialize");

        let result = runtime.execute_i32_entrypoint(
            module.as_bytes(),
            DEFAULT_RUNTIME_ENTRYPOINT,
            &CapabilityGrantSet::default(),
        );

        assert!(
            matches!(result, Err(RuntimeError::Execution(_))),
            "expected execution trap error, got: {result:?}"
        );
    }

    #[test]
    fn runtime_interrupts_infinite_loop_with_wall_clock_timeout() {
        let module = format!(
            r#"
            (module
                (func (export "{entrypoint}") (result i32)
                    (loop
                        br 0
                    )
                    i32.const 0
                )
            )
            "#,
            entrypoint = DEFAULT_RUNTIME_ENTRYPOINT,
        );
        let runtime = WasmRuntime::new_with_limits(RuntimeLimits {
            fuel_budget: 1_000_000_000,
            ..RuntimeLimits::default()
        })
        .expect("runtime should initialize");

        let result = runtime.execute_i32_entrypoint_with_timeout(
            module.as_bytes(),
            DEFAULT_RUNTIME_ENTRYPOINT,
            &CapabilityGrantSet::default(),
            Duration::from_millis(10),
        );

        assert!(
            matches!(result, Err(RuntimeError::ExecutionTimedOut)),
            "expected wall-clock timeout error, got: {result:?}"
        );
    }

    #[test]
    fn runtime_reports_import_contract_mismatch_as_linker_error() {
        let module = format!(
            r#"
            (module
                (import "{host_module}" "{http_count_fn}" (func $http_count (param i32) (result i32)))
                (func (export "{entrypoint}") (result i32)
                    i32.const 7
                )
            )
            "#,
            host_module = HOST_CAPABILITIES_IMPORT_MODULE,
            http_count_fn = HOST_CAPABILITY_HTTP_COUNT_FN,
            entrypoint = DEFAULT_RUNTIME_ENTRYPOINT,
        );
        let runtime = WasmRuntime::new().expect("runtime should initialize");

        let result = runtime.execute_i32_entrypoint(
            module.as_bytes(),
            DEFAULT_RUNTIME_ENTRYPOINT,
            &CapabilityGrantSet::default(),
        );

        assert!(
            matches!(result, Err(RuntimeError::Linker(_))),
            "expected linker/import-contract error, got: {result:?}"
        );
    }

    #[test]
    fn runtime_returns_missing_export_error_for_unknown_entrypoint() {
        let module = br#"
            (module
                (func (export "something_else") (result i32)
                    i32.const 7
                )
            )
        "#;
        let runtime = WasmRuntime::new().expect("runtime should initialize");

        let result = runtime.execute_i32_entrypoint(
            module,
            DEFAULT_RUNTIME_ENTRYPOINT,
            &CapabilityGrantSet::default(),
        );

        assert!(
            matches!(result, Err(RuntimeError::MissingExport(_))),
            "expected missing export error, got: {result:?}"
        );
    }
}
