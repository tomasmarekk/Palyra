//! Executable core-Wasm implementation of the version-two plugin ABI.
//!
//! The host serializes the full typed request behind a fixed binary header,
//! copies it through guest linear memory, and validates guest-produced bytes
//! against the selected contract before constructing a terminal frame.

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use palyra_plugins_sdk::{
    AgentHarnessInvocationV2, AgentHarnessResultV2, ContextEngineInvocationV2,
    ContextEngineResultV2, ExecutablePluginContractKindV2, MemoryProviderInvocationV2,
    MemoryProviderResultV2, ModelAuthProviderInvocationV2, ModelAuthProviderResultV2,
    PluginBindingCleanupV2, PluginBindingIdV2, PluginBindingRecordV2, PluginBindingStateV2,
    PluginCancellationReasonV2, PluginCapabilityScopeV2, PluginConformanceCaseV2,
    PluginConformanceReportV2, PluginConformanceVerdictV2, PluginInvocationAcceptedV2,
    PluginInvocationErrorCodeV2, PluginInvocationErrorV2, PluginInvocationEventV2,
    PluginInvocationFrameV2, PluginInvocationRequestV2, PluginInvocationTerminalOutcomeV2,
    PluginInvocationTerminalV2, PluginInvocationTranscriptV2, PluginRuntimeDiagnosticEntryV2,
    PluginRuntimeDiagnosticsV2, RunLifecycleActionV2, RunLifecycleHookInvocationV2,
    RunLifecycleHookResultV2, RunLifecycleHookRoleV2, ToolResultMiddlewareInvocationV2,
    ToolResultMiddlewareResultV2, PLUGIN_ABI_V2_CORE_ALLOC_EXPORT,
    PLUGIN_ABI_V2_CORE_DEALLOC_EXPORT, PLUGIN_ABI_V2_CORE_INVOKE_EXPORT,
    PLUGIN_ABI_V2_CORE_MEMORY_EXPORT, PLUGIN_ABI_V2_EMIT_EVENT_IMPORT,
    PLUGIN_ABI_V2_HOST_IMPORT_MODULE, PLUGIN_ABI_V2_IS_CANCELLED_IMPORT, PLUGIN_ABI_V2_VERSION,
    PLUGIN_CORE_WIRE_MAGIC_V2, PLUGIN_CORE_WIRE_SCHEMA_VERSION_V2,
};
use wasmtime::{
    Caller, Extern, Instance, Linker, Memory, Module, Store, StoreLimits, StoreLimitsBuilder,
    TypedFunc,
};

use crate::{
    arm_epoch_timeout_guard, build_runtime_engine, configure_epoch_deadline,
    is_execution_limit_error, is_timeout_error, RuntimeError, RuntimeLimits,
};

const CORE_WIRE_HEADER_BYTES: usize = 16;
const MAX_CORE_WIRE_BYTES: usize = 16 * 1024 * 1024;
const MAX_CAPABILITY_HANDLES: usize = 64;
const QUARANTINE_STRIKE_THRESHOLD: u32 = 3;

const GUEST_STATUS_CANCELLED: i32 = -2;
const GUEST_STATUS_OUTPUT_TOO_LARGE: i32 = -3;
const GUEST_STATUS_EVENT_BACKPRESSURE: i32 = -4;

const HOST_EVENT_ACCEPTED: i32 = 0;
const HOST_EVENT_CANCELLED: i32 = -2;
const HOST_EVENT_TOO_LARGE: i32 = -3;
const HOST_EVENT_COUNT_EXCEEDED: i32 = -4;
const HOST_EVENT_MEMORY_INVALID: i32 = -5;

/// Cooperative cancellation flag shared with an invoking thread.
#[derive(Debug, Clone, Default)]
pub struct PluginCoreWasmCancellationTokenV2 {
    cancelled: Arc<AtomicBool>,
    events_emitted: Arc<AtomicU32>,
}

impl PluginCoreWasmCancellationTokenV2 {
    /// Creates an uncancelled token.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests fail-closed cooperative cancellation.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Returns whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Returns the number of events accepted before cancellation.
    ///
    /// This is a synchronization point for deterministic supervisors and
    /// tests; event payloads remain in the invocation transcript.
    #[must_use]
    pub fn observed_event_count(&self) -> u32 {
        self.events_emitted.load(Ordering::Acquire)
    }
}

/// Borrowed fixture used by the executable conformance runner.
#[derive(Debug)]
pub struct PluginConformanceFixtureV2<'a> {
    /// Stable fixture identifier reported to CI.
    pub fixture_id: &'a str,
    /// Real core-Wasm module bytes or WAT source compiled by Wasmtime.
    pub module_bytes: &'a [u8],
    /// Binding registered before invocation.
    pub binding: PluginBindingRecordV2,
    /// Typed request copied into guest memory.
    pub request: PluginInvocationRequestV2,
}

#[derive(Debug, Clone)]
struct RegisteredBindingV2 {
    record: PluginBindingRecordV2,
    state: PluginBindingStateV2,
    quarantine_strikes: u32,
}

/// Stateful executable ABI v2 runtime with generation-safe binding lifecycle.
pub struct PluginRuntimeV2 {
    limits: RuntimeLimits,
    bindings: BTreeMap<PluginBindingIdV2, RegisteredBindingV2>,
}

impl PluginRuntimeV2 {
    /// Creates a runtime with default Wasmtime resource limits.
    ///
    /// # Errors
    /// Returns [`RuntimeError::Compile`] when the Wasmtime engine cannot be
    /// configured.
    pub fn new() -> Result<Self, RuntimeError> {
        Self::new_with_limits(RuntimeLimits::default())
    }

    /// Creates a runtime with explicit Wasmtime resource limits.
    ///
    /// # Errors
    /// Returns [`RuntimeError::Compile`] when the Wasmtime engine cannot be
    /// configured.
    pub fn new_with_limits(limits: RuntimeLimits) -> Result<Self, RuntimeError> {
        build_runtime_engine()?;
        Ok(Self { limits, bindings: BTreeMap::new() })
    }

    /// Registers a validated binding or replaces an older generation.
    ///
    /// A quarantined or disposed binding can only recover through a strictly
    /// newer generation, preventing stale handle reuse after runtime restart.
    ///
    /// # Errors
    /// Returns a redacted [`PluginInvocationErrorV2`] when the record is
    /// invalid or does not advance an existing generation.
    pub fn register_binding(
        &mut self,
        binding: PluginBindingRecordV2,
    ) -> Result<(), PluginInvocationErrorV2> {
        binding.validate().map_err(|_| {
            invocation_error(
                PluginInvocationErrorCodeV2::BindingMismatch,
                None,
                Some(binding.binding_id.clone()),
                Some(binding.runtime_generation),
            )
        })?;
        if binding.granted_capability_handles.len() > MAX_CAPABILITY_HANDLES {
            return Err(invocation_error(
                PluginInvocationErrorCodeV2::CapabilityHandleInvalid,
                None,
                Some(binding.binding_id.clone()),
                Some(binding.runtime_generation),
            ));
        }
        if let Some(existing) = self.bindings.get(&binding.binding_id) {
            if binding.runtime_generation <= existing.record.runtime_generation {
                return Err(invocation_error(
                    PluginInvocationErrorCodeV2::StaleGeneration,
                    None,
                    Some(binding.binding_id.clone()),
                    Some(binding.runtime_generation),
                ));
            }
        }
        self.bindings.insert(
            binding.binding_id.clone(),
            RegisteredBindingV2 {
                record: binding,
                state: PluginBindingStateV2::Active,
                quarantine_strikes: 0,
            },
        );
        Ok(())
    }

    /// Executes a typed request across real guest linear memory.
    ///
    /// Admission rejects stale bindings, expired deadlines, schema mismatch,
    /// oversized input, and invalid capability handles before Wasm execution.
    /// Once admitted, the return value always validates the sequence
    /// `accepted -> event* -> exactly one terminal`.
    ///
    /// # Errors
    /// Returns a redacted admission error when no guest call may start.
    /// Execution failures after acceptance are represented by the transcript's
    /// terminal frame.
    pub fn invoke(
        &mut self,
        module_bytes: &[u8],
        request: &PluginInvocationRequestV2,
        now_unix_ms: u64,
        cancellation: PluginCoreWasmCancellationTokenV2,
    ) -> Result<PluginInvocationTranscriptV2, PluginInvocationErrorV2> {
        let binding =
            self.bindings.get(&request.binding_id).cloned().ok_or_else(|| {
                request_error(PluginInvocationErrorCodeV2::BindingNotFound, request)
            })?;
        validate_request_admission(&binding, request, now_unix_ms)?;
        validate_contract_input(request)?;

        let accepted = PluginInvocationFrameV2::Accepted(PluginInvocationAcceptedV2 {
            call_id: request.call_id.clone(),
            binding_id: request.binding_id.clone(),
            runtime_generation: request.runtime_generation,
        });
        let execution = self.execute_core_wasm(module_bytes, request, now_unix_ms, cancellation);
        let (events, outcome) = match execution {
            Ok(execution) => (
                execution.events,
                PluginInvocationTerminalOutcomeV2::Completed {
                    schema_hash: request.output_schema_hash.clone(),
                    output_bytes: execution.output_bytes,
                },
            ),
            Err(CoreGuestFailureV2::Cancelled(events)) => (
                events,
                PluginInvocationTerminalOutcomeV2::Cancelled {
                    reason: PluginCancellationReasonV2::Requested,
                },
            ),
            Err(CoreGuestFailureV2::Failed { code, events }) => {
                self.record_quarantine_strike(&request.binding_id, code);
                (
                    events,
                    PluginInvocationTerminalOutcomeV2::Failed {
                        error: request_error(code, request),
                    },
                )
            }
        };
        let mut frames = Vec::with_capacity(events.len().saturating_add(2));
        frames.push(accepted);
        frames.extend(events.into_iter().map(PluginInvocationFrameV2::Event));
        frames.push(PluginInvocationFrameV2::Terminal(PluginInvocationTerminalV2 {
            call_id: request.call_id.clone(),
            outcome,
        }));
        PluginInvocationTranscriptV2::from_frames(frames)
            .map_err(|_| request_error(PluginInvocationErrorCodeV2::InvalidContractOutput, request))
    }

    /// Disposes a binding and releases every opaque capability handle.
    ///
    /// # Errors
    /// Returns [`PluginInvocationErrorCodeV2::BindingNotFound`] for an unknown
    /// binding or [`PluginInvocationErrorCodeV2::StaleGeneration`] when the
    /// requested generation is stale.
    pub fn dispose_binding(
        &mut self,
        binding_id: &PluginBindingIdV2,
        generation: palyra_plugins_sdk::PluginRuntimeGenerationV2,
    ) -> Result<PluginBindingCleanupV2, PluginInvocationErrorV2> {
        self.cleanup_binding(
            binding_id,
            generation,
            PluginBindingStateV2::Disposed,
            "plugin.binding.dispose.completed",
        )
    }

    /// Quarantines a binding and releases every opaque capability handle.
    ///
    /// # Errors
    /// Returns [`PluginInvocationErrorCodeV2::BindingNotFound`] for an unknown
    /// binding or [`PluginInvocationErrorCodeV2::StaleGeneration`] when the
    /// requested generation is stale.
    pub fn quarantine_binding(
        &mut self,
        binding_id: &PluginBindingIdV2,
        generation: palyra_plugins_sdk::PluginRuntimeGenerationV2,
    ) -> Result<PluginBindingCleanupV2, PluginInvocationErrorV2> {
        self.cleanup_binding(
            binding_id,
            generation,
            PluginBindingStateV2::Quarantined,
            "plugin.binding.quarantine.completed",
        )
    }

    /// Returns a deterministic, payload-free binding diagnostic snapshot.
    #[must_use]
    pub fn diagnostics(&self) -> PluginRuntimeDiagnosticsV2 {
        let bindings = self
            .bindings
            .values()
            .map(|binding| PluginRuntimeDiagnosticEntryV2 {
                binding_id: binding.record.binding_id.clone(),
                contract: binding.record.contract,
                runtime_generation: binding.record.runtime_generation,
                state: binding.state,
                quarantine_strikes: binding.quarantine_strikes,
                granted_capability_handle_count: u32::try_from(
                    binding.record.granted_capability_handles.len(),
                )
                .unwrap_or(u32::MAX),
                expires_at_unix_ms: binding.record.expires_at_unix_ms,
            })
            .collect();
        PluginRuntimeDiagnosticsV2 {
            schema_version: 2,
            abi_version: PLUGIN_ABI_V2_VERSION.to_owned(),
            bindings,
        }
    }

    /// Runs real Wasmtime fixtures and returns a CI-suitable conformance report.
    ///
    /// # Errors
    /// Returns a redacted binding error when a fixture cannot be registered.
    /// Guest execution failures remain case verdicts so the report preserves
    /// evidence for every runnable fixture.
    pub fn run_conformance_suite(
        &mut self,
        fixtures: Vec<PluginConformanceFixtureV2<'_>>,
        now_unix_ms: u64,
    ) -> Result<PluginConformanceReportV2, PluginInvocationErrorV2> {
        let mut cases = Vec::with_capacity(fixtures.len());
        for fixture in fixtures {
            let contract = fixture.request.contract;
            self.register_binding(fixture.binding)?;
            let transcript = self.invoke(
                fixture.module_bytes,
                &fixture.request,
                now_unix_ms,
                PluginCoreWasmCancellationTokenV2::new(),
            )?;
            let completed = matches!(
                transcript.terminal().outcome,
                PluginInvocationTerminalOutcomeV2::Completed { .. }
            );
            let reason_codes = match &transcript.terminal().outcome {
                PluginInvocationTerminalOutcomeV2::Failed { error } => {
                    vec![error.reason_code().to_owned()]
                }
                PluginInvocationTerminalOutcomeV2::Cancelled { .. } => {
                    vec![PluginInvocationErrorCodeV2::Cancelled.reason_code().to_owned()]
                }
                PluginInvocationTerminalOutcomeV2::Completed { .. } => Vec::new(),
            };
            cases.push(PluginConformanceCaseV2 {
                fixture_id: fixture.fixture_id.to_owned(),
                contract,
                verdict: if completed {
                    PluginConformanceVerdictV2::Passed
                } else {
                    PluginConformanceVerdictV2::Failed
                },
                lifecycle_valid: true,
                security_valid: completed,
                reason_codes,
            });
        }
        Ok(PluginConformanceReportV2 {
            schema_version: 2,
            abi_version: PLUGIN_ABI_V2_VERSION.to_owned(),
            cases,
        })
    }

    fn execute_core_wasm(
        &self,
        module_bytes: &[u8],
        request: &PluginInvocationRequestV2,
        now_unix_ms: u64,
        cancellation: PluginCoreWasmCancellationTokenV2,
    ) -> Result<CoreGuestExecutionV2, CoreGuestFailureV2> {
        let wire_request = encode_core_wire_request(request)
            .map_err(|code| CoreGuestFailureV2::Failed { code, events: Vec::new() })?;
        let timeout_ms =
            request.budget.absolute_deadline_unix_ms.checked_sub(now_unix_ms).ok_or_else(|| {
                CoreGuestFailureV2::Failed {
                    code: PluginInvocationErrorCodeV2::DeadlineExceeded,
                    events: Vec::new(),
                }
            })?;
        let engine = build_runtime_engine().map_err(|_| CoreGuestFailureV2::Failed {
            code: PluginInvocationErrorCodeV2::GuestRejected,
            events: Vec::new(),
        })?;
        let module =
            Module::new(&engine, module_bytes).map_err(|_| CoreGuestFailureV2::Failed {
                code: PluginInvocationErrorCodeV2::GuestRejected,
                events: Vec::new(),
            })?;
        let store_limits = StoreLimitsBuilder::new()
            .memory_size(self.limits.max_memory_bytes)
            .table_elements(self.limits.max_table_elements)
            .instances(self.limits.max_instances)
            .build();
        let mut store = Store::new(
            &engine,
            AbiV2StoreState {
                limits: store_limits,
                cancellation,
                call_id: request.call_id.clone(),
                output_schema_hash: request.output_schema_hash.clone(),
                max_event_bytes: request.budget.max_event_bytes,
                max_events: request.budget.max_events,
                events: Vec::new(),
                host_failure: None,
                request: request.clone(),
            },
        );
        store.limiter(|state| &mut state.limits);
        store.set_fuel(self.limits.fuel_budget).map_err(|_| CoreGuestFailureV2::Failed {
            code: PluginInvocationErrorCodeV2::ResourceLimitExceeded,
            events: Vec::new(),
        })?;
        configure_epoch_deadline(&mut store, true);
        let _timeout_guard =
            arm_epoch_timeout_guard(engine.clone(), Duration::from_millis(timeout_ms));
        let mut linker = Linker::new(&engine);
        register_abi_v2_host_imports(&mut linker).map_err(|_| CoreGuestFailureV2::Failed {
            code: PluginInvocationErrorCodeV2::GuestRejected,
            events: Vec::new(),
        })?;
        let instance = linker.instantiate(&mut store, &module).map_err(|error| {
            map_guest_error(error, &store, PluginInvocationErrorCodeV2::GuestRejected)
        })?;
        execute_guest_instance(
            &instance,
            &mut store,
            &wire_request,
            request.budget.max_output_bytes,
        )
    }

    fn cleanup_binding(
        &mut self,
        binding_id: &PluginBindingIdV2,
        generation: palyra_plugins_sdk::PluginRuntimeGenerationV2,
        final_state: PluginBindingStateV2,
        reason_code: &str,
    ) -> Result<PluginBindingCleanupV2, PluginInvocationErrorV2> {
        let binding = self.bindings.get_mut(binding_id).ok_or_else(|| {
            invocation_error(
                PluginInvocationErrorCodeV2::BindingNotFound,
                None,
                Some(binding_id.clone()),
                Some(generation),
            )
        })?;
        if binding.record.runtime_generation != generation {
            return Err(invocation_error(
                PluginInvocationErrorCodeV2::StaleGeneration,
                None,
                Some(binding_id.clone()),
                Some(generation),
            ));
        }
        let released_capability_handle_count =
            u32::try_from(binding.record.granted_capability_handles.len()).unwrap_or(u32::MAX);
        binding.record.granted_capability_handles.clear();
        binding.state = final_state;
        Ok(PluginBindingCleanupV2 {
            binding_id: binding_id.clone(),
            runtime_generation: generation,
            final_state,
            released_capability_handle_count,
            reason_code: reason_code.to_owned(),
        })
    }

    fn record_quarantine_strike(
        &mut self,
        binding_id: &PluginBindingIdV2,
        code: PluginInvocationErrorCodeV2,
    ) {
        if !matches!(
            code,
            PluginInvocationErrorCodeV2::GuestTrapped
                | PluginInvocationErrorCodeV2::ResourceLimitExceeded
                | PluginInvocationErrorCodeV2::InvalidContractOutput
                | PluginInvocationErrorCodeV2::AuthorityExpansionDenied
                | PluginInvocationErrorCodeV2::EventBackpressureExceeded
        ) {
            return;
        }
        let Some(binding) = self.bindings.get_mut(binding_id) else {
            return;
        };
        binding.quarantine_strikes = binding.quarantine_strikes.saturating_add(1);
        if binding.quarantine_strikes >= QUARANTINE_STRIKE_THRESHOLD {
            binding.state = PluginBindingStateV2::Quarantined;
            binding.record.granted_capability_handles.clear();
        }
    }
}

struct AbiV2StoreState {
    limits: StoreLimits,
    cancellation: PluginCoreWasmCancellationTokenV2,
    call_id: palyra_plugins_sdk::PluginCallIdV2,
    output_schema_hash: palyra_plugins_sdk::PluginSchemaHashV2,
    max_event_bytes: u32,
    max_events: u32,
    events: Vec<PluginInvocationEventV2>,
    host_failure: Option<PluginInvocationErrorCodeV2>,
    request: PluginInvocationRequestV2,
}

struct CoreGuestExecutionV2 {
    output_bytes: Vec<u8>,
    events: Vec<PluginInvocationEventV2>,
}

enum CoreGuestFailureV2 {
    Cancelled(Vec<PluginInvocationEventV2>),
    Failed { code: PluginInvocationErrorCodeV2, events: Vec<PluginInvocationEventV2> },
}

fn validate_request_admission(
    binding: &RegisteredBindingV2,
    request: &PluginInvocationRequestV2,
    now_unix_ms: u64,
) -> Result<(), PluginInvocationErrorV2> {
    if binding.state != PluginBindingStateV2::Active {
        let code = match binding.state {
            PluginBindingStateV2::Active => PluginInvocationErrorCodeV2::BindingInactive,
            PluginBindingStateV2::Disposed => PluginInvocationErrorCodeV2::Disposed,
            PluginBindingStateV2::Quarantined => PluginInvocationErrorCodeV2::Quarantined,
        };
        return Err(request_error(code, request));
    }
    if request.schema_version != 2
        || request.binding_id != binding.record.binding_id
        || request.contract != binding.record.contract
        || request.operation != binding.record.operation
    {
        return Err(request_error(PluginInvocationErrorCodeV2::BindingMismatch, request));
    }
    if request.runtime_generation != binding.record.runtime_generation {
        return Err(request_error(PluginInvocationErrorCodeV2::StaleGeneration, request));
    }
    if binding.record.issued_at_unix_ms > now_unix_ms {
        return Err(request_error(PluginInvocationErrorCodeV2::BindingInactive, request));
    }
    request
        .budget
        .validate()
        .map_err(|_| request_error(PluginInvocationErrorCodeV2::BindingMismatch, request))?;
    if request.budget.absolute_deadline_unix_ms <= now_unix_ms
        || binding.record.expires_at_unix_ms <= now_unix_ms
    {
        return Err(request_error(PluginInvocationErrorCodeV2::DeadlineExceeded, request));
    }
    if request.input_schema_hash != binding.record.input_schema_hash
        || request.output_schema_hash != binding.record.output_schema_hash
    {
        return Err(request_error(PluginInvocationErrorCodeV2::SchemaMismatch, request));
    }
    let max_input = usize::try_from(request.budget.max_input_bytes)
        .map_err(|_| request_error(PluginInvocationErrorCodeV2::InputTooLarge, request))?;
    if request.input_bytes.len() > max_input {
        return Err(request_error(PluginInvocationErrorCodeV2::InputTooLarge, request));
    }
    if request.granted_capability_handles != binding.record.granted_capability_handles
        || request.granted_capability_handles.iter().any(|handle| {
            handle.runtime_generation != request.runtime_generation
                || handle.expires_at_unix_ms <= now_unix_ms
        })
    {
        return Err(request_error(PluginInvocationErrorCodeV2::CapabilityHandleInvalid, request));
    }
    Ok(())
}

fn validate_contract_input(
    request: &PluginInvocationRequestV2,
) -> Result<(), PluginInvocationErrorV2> {
    let valid = match request.contract {
        ExecutablePluginContractKindV2::AgentHarness => {
            AgentHarnessInvocationV2::decode_core_bytes(&request.input_bytes)
                .map(|input| input.max_steps > 0)
        }
        ExecutablePluginContractKindV2::ContextEngine => {
            ContextEngineInvocationV2::decode_core_bytes(&request.input_bytes)
                .map(|input| input.max_segments > 0 && input.token_budget > 0)
        }
        ExecutablePluginContractKindV2::ToolResultMiddleware => {
            ToolResultMiddlewareInvocationV2::decode_core_bytes(&request.input_bytes)
                .map(|input| input.max_projection_bytes > 0)
        }
        ExecutablePluginContractKindV2::RunLifecycleHook => {
            RunLifecycleHookInvocationV2::decode_core_bytes(&request.input_bytes)
                .map(|input| !input.phase.trim().is_empty())
        }
        ExecutablePluginContractKindV2::MemoryProvider => {
            MemoryProviderInvocationV2::decode_core_bytes(&request.input_bytes)
                .map(|input| input.max_candidates > 0 && !input.namespace_ref.trim().is_empty())
        }
        ExecutablePluginContractKindV2::ModelAuthProvider => {
            ModelAuthProviderInvocationV2::decode_core_bytes(&request.input_bytes)
                .map(|input| !input.provider_id.trim().is_empty())
        }
    }
    .map_err(|_| request_error(PluginInvocationErrorCodeV2::SchemaMismatch, request))?;
    if !valid {
        return Err(request_error(PluginInvocationErrorCodeV2::SchemaMismatch, request));
    }
    Ok(())
}

fn encode_core_wire_request(
    request: &PluginInvocationRequestV2,
) -> Result<Vec<u8>, PluginInvocationErrorCodeV2> {
    let encoded = encode_invocation_request_body(request)?;
    let encoded_len =
        u32::try_from(encoded.len()).map_err(|_| PluginInvocationErrorCodeV2::InputTooLarge)?;
    let total_len = CORE_WIRE_HEADER_BYTES
        .checked_add(encoded.len())
        .ok_or(PluginInvocationErrorCodeV2::InputTooLarge)?;
    if total_len > MAX_CORE_WIRE_BYTES {
        return Err(PluginInvocationErrorCodeV2::InputTooLarge);
    }
    let mut wire = Vec::with_capacity(total_len);
    wire.extend_from_slice(&PLUGIN_CORE_WIRE_MAGIC_V2);
    wire.extend_from_slice(&PLUGIN_CORE_WIRE_SCHEMA_VERSION_V2.to_le_bytes());
    wire.push(request.contract.core_wire_tag());
    wire.push(request.operation.core_wire_tag());
    wire.extend_from_slice(&encoded_len.to_le_bytes());
    wire.extend_from_slice(&encoded);
    Ok(wire)
}

fn encode_invocation_request_body(
    request: &PluginInvocationRequestV2,
) -> Result<Vec<u8>, PluginInvocationErrorCodeV2> {
    let mut encoded = Vec::new();
    write_bounded_string(&mut encoded, request.call_id.as_str())?;
    write_bounded_string(&mut encoded, request.binding_id.as_str())?;
    encoded.extend_from_slice(&request.runtime_generation.get().to_le_bytes());
    encoded.extend_from_slice(&request.budget.absolute_deadline_unix_ms.to_le_bytes());
    encoded.extend_from_slice(&request.budget.max_input_bytes.to_le_bytes());
    encoded.extend_from_slice(&request.budget.max_output_bytes.to_le_bytes());
    encoded.extend_from_slice(&request.budget.max_event_bytes.to_le_bytes());
    encoded.extend_from_slice(&request.budget.max_events.to_le_bytes());
    encoded.extend_from_slice(request.input_schema_hash.as_str().as_bytes());
    encoded.extend_from_slice(request.output_schema_hash.as_str().as_bytes());
    let handle_count = u16::try_from(request.granted_capability_handles.len())
        .map_err(|_| PluginInvocationErrorCodeV2::CapabilityHandleInvalid)?;
    encoded.extend_from_slice(&handle_count.to_le_bytes());
    for handle in &request.granted_capability_handles {
        write_bounded_string(&mut encoded, handle.handle_id().as_str())?;
        encoded.push(capability_scope_core_tag(handle.scope));
        encoded.extend_from_slice(handle.scope_hash.as_str().as_bytes());
        encoded.extend_from_slice(&handle.runtime_generation.get().to_le_bytes());
        encoded.extend_from_slice(&handle.expires_at_unix_ms.to_le_bytes());
    }
    let input_len = u32::try_from(request.input_bytes.len())
        .map_err(|_| PluginInvocationErrorCodeV2::InputTooLarge)?;
    encoded.extend_from_slice(&input_len.to_le_bytes());
    encoded.extend_from_slice(&request.input_bytes);
    Ok(encoded)
}

fn write_bounded_string(
    encoded: &mut Vec<u8>,
    value: &str,
) -> Result<(), PluginInvocationErrorCodeV2> {
    let length =
        u16::try_from(value.len()).map_err(|_| PluginInvocationErrorCodeV2::InputTooLarge)?;
    encoded.extend_from_slice(&length.to_le_bytes());
    encoded.extend_from_slice(value.as_bytes());
    Ok(())
}

fn capability_scope_core_tag(scope: PluginCapabilityScopeV2) -> u8 {
    match scope {
        PluginCapabilityScopeV2::HttpHost => 1,
        PluginCapabilityScopeV2::SecretLease => 2,
        PluginCapabilityScopeV2::StoragePrefix => 3,
        PluginCapabilityScopeV2::Channel => 4,
        PluginCapabilityScopeV2::HarnessCallback => 5,
    }
}

fn register_abi_v2_host_imports(
    linker: &mut Linker<AbiV2StoreState>,
) -> Result<(), wasmtime::Error> {
    linker.func_wrap(
        PLUGIN_ABI_V2_HOST_IMPORT_MODULE,
        PLUGIN_ABI_V2_EMIT_EVENT_IMPORT,
        host_emit_event,
    )?;
    linker.func_wrap(
        PLUGIN_ABI_V2_HOST_IMPORT_MODULE,
        PLUGIN_ABI_V2_IS_CANCELLED_IMPORT,
        host_is_cancelled,
    )?;
    Ok(())
}

fn host_emit_event(mut caller: Caller<'_, AbiV2StoreState>, pointer: i32, length: i32) -> i32 {
    if caller.data().cancellation.is_cancelled() {
        return HOST_EVENT_CANCELLED;
    }
    let Ok(length) = usize::try_from(length) else {
        caller.data_mut().host_failure =
            Some(PluginInvocationErrorCodeV2::EventBackpressureExceeded);
        return HOST_EVENT_MEMORY_INVALID;
    };
    let Ok(max_event_bytes) = usize::try_from(caller.data().max_event_bytes) else {
        caller.data_mut().host_failure =
            Some(PluginInvocationErrorCodeV2::EventBackpressureExceeded);
        return HOST_EVENT_TOO_LARGE;
    };
    if length > max_event_bytes {
        caller.data_mut().host_failure =
            Some(PluginInvocationErrorCodeV2::EventBackpressureExceeded);
        return HOST_EVENT_TOO_LARGE;
    }
    let Ok(max_events) = usize::try_from(caller.data().max_events) else {
        caller.data_mut().host_failure =
            Some(PluginInvocationErrorCodeV2::EventBackpressureExceeded);
        return HOST_EVENT_COUNT_EXCEEDED;
    };
    if caller.data().events.len() >= max_events {
        caller.data_mut().host_failure =
            Some(PluginInvocationErrorCodeV2::EventBackpressureExceeded);
        return HOST_EVENT_COUNT_EXCEEDED;
    }
    let Some(Extern::Memory(memory)) = caller.get_export(PLUGIN_ABI_V2_CORE_MEMORY_EXPORT) else {
        caller.data_mut().host_failure = Some(PluginInvocationErrorCodeV2::GuestRejected);
        return HOST_EVENT_MEMORY_INVALID;
    };
    let Ok(pointer) = usize::try_from(pointer) else {
        caller.data_mut().host_failure = Some(PluginInvocationErrorCodeV2::GuestRejected);
        return HOST_EVENT_MEMORY_INVALID;
    };
    let mut event_bytes = vec![0_u8; length];
    if memory.read(&caller, pointer, &mut event_bytes).is_err() {
        caller.data_mut().host_failure = Some(PluginInvocationErrorCodeV2::GuestRejected);
        return HOST_EVENT_MEMORY_INVALID;
    }
    let sequence = match u32::try_from(caller.data().events.len().saturating_add(1)) {
        Ok(sequence) => sequence,
        Err(_) => {
            caller.data_mut().host_failure =
                Some(PluginInvocationErrorCodeV2::EventBackpressureExceeded);
            return HOST_EVENT_COUNT_EXCEEDED;
        }
    };
    let event = PluginInvocationEventV2 {
        call_id: caller.data().call_id.clone(),
        sequence,
        schema_hash: caller.data().output_schema_hash.clone(),
        event_bytes,
    };
    caller.data_mut().events.push(event);
    caller.data().cancellation.events_emitted.fetch_add(1, Ordering::Release);
    HOST_EVENT_ACCEPTED
}

fn host_is_cancelled(caller: Caller<'_, AbiV2StoreState>) -> i32 {
    i32::from(caller.data().cancellation.is_cancelled())
}

fn execute_guest_instance(
    instance: &Instance,
    store: &mut Store<AbiV2StoreState>,
    wire_request: &[u8],
    max_output_bytes: u32,
) -> Result<CoreGuestExecutionV2, CoreGuestFailureV2> {
    let memory = instance
        .get_memory(&mut *store, PLUGIN_ABI_V2_CORE_MEMORY_EXPORT)
        .ok_or_else(|| failure_without_events(PluginInvocationErrorCodeV2::GuestRejected))?;
    let allocate: TypedFunc<i32, i32> = instance
        .get_typed_func(&mut *store, PLUGIN_ABI_V2_CORE_ALLOC_EXPORT)
        .map_err(|_| failure_without_events(PluginInvocationErrorCodeV2::GuestRejected))?;
    let invoke: TypedFunc<(i32, i32, i32, i32), i32> = instance
        .get_typed_func(&mut *store, PLUGIN_ABI_V2_CORE_INVOKE_EXPORT)
        .map_err(|_| failure_without_events(PluginInvocationErrorCodeV2::GuestRejected))?;
    let deallocate: TypedFunc<(i32, i32), ()> = instance
        .get_typed_func(&mut *store, PLUGIN_ABI_V2_CORE_DEALLOC_EXPORT)
        .map_err(|_| failure_without_events(PluginInvocationErrorCodeV2::GuestRejected))?;
    let request_len = i32::try_from(wire_request.len())
        .map_err(|_| failure_without_events(PluginInvocationErrorCodeV2::InputTooLarge))?;
    let output_capacity = i32::try_from(max_output_bytes)
        .map_err(|_| failure_without_events(PluginInvocationErrorCodeV2::OutputTooLarge))?;
    let request_pointer = allocate.call(&mut *store, request_len).map_err(|error| {
        map_guest_error(error, store, PluginInvocationErrorCodeV2::GuestTrapped)
    })?;
    write_guest_memory(&memory, store, request_pointer, wire_request)?;
    let output_pointer = allocate.call(&mut *store, output_capacity).map_err(|error| {
        map_guest_error(error, store, PluginInvocationErrorCodeV2::GuestTrapped)
    })?;
    validate_guest_range(&memory, store, output_pointer, output_capacity)?;
    let returned_length = invoke
        .call(&mut *store, (request_pointer, request_len, output_pointer, output_capacity))
        .map_err(|error| {
            map_guest_error(error, store, PluginInvocationErrorCodeV2::GuestTrapped)
        })?;
    let host_failure = store.data().host_failure;
    let cancelled =
        store.data().cancellation.is_cancelled() || returned_length == GUEST_STATUS_CANCELLED;
    let mut events = std::mem::take(&mut store.data_mut().events);
    let output_bytes =
        read_returned_output(&memory, store, output_pointer, returned_length, max_output_bytes);
    let cleanup_result = deallocate
        .call(&mut *store, (request_pointer, request_len))
        .and_then(|()| deallocate.call(&mut *store, (output_pointer, output_capacity)));
    if let Err(error) = cleanup_result {
        return Err(map_guest_error_with_events(
            error,
            store,
            PluginInvocationErrorCodeV2::GuestTrapped,
            events,
        ));
    }
    if let Some(code) = host_failure {
        return Err(CoreGuestFailureV2::Failed { code, events });
    }
    if cancelled {
        return Err(CoreGuestFailureV2::Cancelled(events));
    }
    if returned_length == GUEST_STATUS_OUTPUT_TOO_LARGE {
        return Err(CoreGuestFailureV2::Failed {
            code: PluginInvocationErrorCodeV2::OutputTooLarge,
            events,
        });
    }
    if returned_length == GUEST_STATUS_EVENT_BACKPRESSURE {
        return Err(CoreGuestFailureV2::Failed {
            code: PluginInvocationErrorCodeV2::EventBackpressureExceeded,
            events,
        });
    }
    let output_bytes = output_bytes
        .map_err(|code| CoreGuestFailureV2::Failed { code, events: std::mem::take(&mut events) })?;
    validate_contract_output(store, &output_bytes)
        .map_err(|code| CoreGuestFailureV2::Failed { code, events: std::mem::take(&mut events) })?;
    Ok(CoreGuestExecutionV2 { output_bytes, events })
}

fn read_returned_output(
    memory: &Memory,
    store: &Store<AbiV2StoreState>,
    output_pointer: i32,
    returned_length: i32,
    max_output_bytes: u32,
) -> Result<Vec<u8>, PluginInvocationErrorCodeV2> {
    let returned_length =
        usize::try_from(returned_length).map_err(|_| PluginInvocationErrorCodeV2::GuestTrapped)?;
    let output_capacity = usize::try_from(max_output_bytes)
        .map_err(|_| PluginInvocationErrorCodeV2::OutputTooLarge)?;
    if returned_length > output_capacity {
        return Err(PluginInvocationErrorCodeV2::OutputTooLarge);
    }
    let output_pointer =
        usize::try_from(output_pointer).map_err(|_| PluginInvocationErrorCodeV2::GuestTrapped)?;
    let mut output_bytes = vec![0_u8; returned_length];
    memory
        .read(store, output_pointer, &mut output_bytes)
        .map_err(|_| PluginInvocationErrorCodeV2::GuestTrapped)?;
    Ok(output_bytes)
}

fn write_guest_memory(
    memory: &Memory,
    store: &mut Store<AbiV2StoreState>,
    pointer: i32,
    bytes: &[u8],
) -> Result<(), CoreGuestFailureV2> {
    let pointer = usize::try_from(pointer)
        .map_err(|_| failure_without_events(PluginInvocationErrorCodeV2::GuestRejected))?;
    memory
        .write(&mut *store, pointer, bytes)
        .map_err(|_| failure_without_events(PluginInvocationErrorCodeV2::GuestRejected))
}

fn validate_guest_range(
    memory: &Memory,
    store: &Store<AbiV2StoreState>,
    pointer: i32,
    length: i32,
) -> Result<(), CoreGuestFailureV2> {
    let pointer = usize::try_from(pointer)
        .map_err(|_| failure_without_events(PluginInvocationErrorCodeV2::GuestRejected))?;
    let length = usize::try_from(length)
        .map_err(|_| failure_without_events(PluginInvocationErrorCodeV2::GuestRejected))?;
    let end = pointer
        .checked_add(length)
        .ok_or_else(|| failure_without_events(PluginInvocationErrorCodeV2::GuestRejected))?;
    if end > memory.data_size(store) {
        return Err(failure_without_events(PluginInvocationErrorCodeV2::GuestRejected));
    }
    Ok(())
}

fn validate_contract_output(
    store: &Store<AbiV2StoreState>,
    output_bytes: &[u8],
) -> Result<(), PluginInvocationErrorCodeV2> {
    let request = decode_request_from_store_context(store)?;
    match request.contract {
        ExecutablePluginContractKindV2::AgentHarness => {
            let input = AgentHarnessInvocationV2::decode_core_bytes(&request.input_bytes)
                .map_err(|_| PluginInvocationErrorCodeV2::InvalidContractOutput)?;
            let output = AgentHarnessResultV2::decode_core_bytes(output_bytes)
                .map_err(|_| PluginInvocationErrorCodeV2::InvalidContractOutput)?;
            if output.steps_used > input.max_steps {
                return Err(PluginInvocationErrorCodeV2::AuthorityExpansionDenied);
            }
        }
        ExecutablePluginContractKindV2::ContextEngine => {
            let input = ContextEngineInvocationV2::decode_core_bytes(&request.input_bytes)
                .map_err(|_| PluginInvocationErrorCodeV2::InvalidContractOutput)?;
            let output = ContextEngineResultV2::decode_core_bytes(output_bytes)
                .map_err(|_| PluginInvocationErrorCodeV2::InvalidContractOutput)?;
            let estimated_tokens = output
                .candidates
                .iter()
                .try_fold(0_u32, |total, candidate| total.checked_add(candidate.estimated_tokens))
                .ok_or(PluginInvocationErrorCodeV2::InvalidContractOutput)?;
            let max_segments = usize::try_from(input.max_segments)
                .map_err(|_| PluginInvocationErrorCodeV2::InvalidContractOutput)?;
            if output.candidates.len() > max_segments
                || estimated_tokens > input.token_budget
                || output.candidates.iter().any(|candidate| candidate.relevance_millis > 1_000)
            {
                return Err(PluginInvocationErrorCodeV2::InvalidContractOutput);
            }
        }
        ExecutablePluginContractKindV2::ToolResultMiddleware => {
            let input = ToolResultMiddlewareInvocationV2::decode_core_bytes(&request.input_bytes)
                .map_err(|_| PluginInvocationErrorCodeV2::InvalidContractOutput)?;
            let output = ToolResultMiddlewareResultV2::decode_core_bytes(output_bytes)
                .map_err(|_| PluginInvocationErrorCodeV2::InvalidContractOutput)?;
            let projection_limit = usize::try_from(input.max_projection_bytes)
                .map_err(|_| PluginInvocationErrorCodeV2::InvalidContractOutput)?;
            if output.mutation_class != input.mutation_class
                || (input.approval_required && !output.approval_required)
            {
                return Err(PluginInvocationErrorCodeV2::AuthorityExpansionDenied);
            }
            if output.projected_bytes.len() > projection_limit {
                return Err(PluginInvocationErrorCodeV2::InvalidContractOutput);
            }
        }
        ExecutablePluginContractKindV2::RunLifecycleHook => {
            let input = RunLifecycleHookInvocationV2::decode_core_bytes(&request.input_bytes)
                .map_err(|_| PluginInvocationErrorCodeV2::InvalidContractOutput)?;
            let output = RunLifecycleHookResultV2::decode_core_bytes(output_bytes)
                .map_err(|_| PluginInvocationErrorCodeV2::InvalidContractOutput)?;
            if output.role != input.role || !hook_action_allowed(output.role, output.action) {
                return Err(PluginInvocationErrorCodeV2::AuthorityExpansionDenied);
            }
        }
        ExecutablePluginContractKindV2::MemoryProvider => {
            let input = MemoryProviderInvocationV2::decode_core_bytes(&request.input_bytes)
                .map_err(|_| PluginInvocationErrorCodeV2::InvalidContractOutput)?;
            let output = MemoryProviderResultV2::decode_core_bytes(output_bytes)
                .map_err(|_| PluginInvocationErrorCodeV2::InvalidContractOutput)?;
            let max_candidates = usize::try_from(input.max_candidates)
                .map_err(|_| PluginInvocationErrorCodeV2::InvalidContractOutput)?;
            if output.candidates.len() > max_candidates
                || output.candidates.iter().any(|candidate| candidate.relevance_millis > 1_000)
            {
                return Err(PluginInvocationErrorCodeV2::InvalidContractOutput);
            }
        }
        ExecutablePluginContractKindV2::ModelAuthProvider => {
            let output = ModelAuthProviderResultV2::decode_core_bytes(output_bytes)
                .map_err(|_| PluginInvocationErrorCodeV2::InvalidContractOutput)?;
            let handle = &output.credential_handle;
            if handle.scope != PluginCapabilityScopeV2::SecretLease
                || handle.runtime_generation != request.runtime_generation
                || !request.granted_capability_handles.iter().any(|granted| granted == handle)
            {
                return Err(PluginInvocationErrorCodeV2::AuthorityExpansionDenied);
            }
        }
    }
    Ok(())
}

// The typed request is retained in guest memory rather than host state. For
// output validation, the store carries its JSON projection to avoid rereading
// guest-controlled memory after deallocation.
fn decode_request_from_store_context(
    store: &Store<AbiV2StoreState>,
) -> Result<PluginInvocationRequestV2, PluginInvocationErrorCodeV2> {
    Ok(store.data().request.clone())
}

fn hook_action_allowed(role: RunLifecycleHookRoleV2, action: RunLifecycleActionV2) -> bool {
    match role {
        RunLifecycleHookRoleV2::Observer => action == RunLifecycleActionV2::Continue,
        RunLifecycleHookRoleV2::Annotator => {
            matches!(action, RunLifecycleActionV2::Continue | RunLifecycleActionV2::Annotate)
        }
        RunLifecycleHookRoleV2::Filter => {
            matches!(action, RunLifecycleActionV2::Continue | RunLifecycleActionV2::Filter)
        }
        RunLifecycleHookRoleV2::ApprovalRequester => {
            matches!(action, RunLifecycleActionV2::Continue | RunLifecycleActionV2::RequestApproval)
        }
        RunLifecycleHookRoleV2::Blocker => {
            matches!(action, RunLifecycleActionV2::Continue | RunLifecycleActionV2::Block)
        }
        RunLifecycleHookRoleV2::LimitedTransformer => {
            matches!(action, RunLifecycleActionV2::Continue | RunLifecycleActionV2::Transform)
        }
    }
}

fn map_guest_error(
    error: wasmtime::Error,
    store: &Store<AbiV2StoreState>,
    fallback: PluginInvocationErrorCodeV2,
) -> CoreGuestFailureV2 {
    map_guest_error_with_events(error, store, fallback, Vec::new())
}

fn map_guest_error_with_events(
    error: wasmtime::Error,
    store: &Store<AbiV2StoreState>,
    fallback: PluginInvocationErrorCodeV2,
    events: Vec<PluginInvocationEventV2>,
) -> CoreGuestFailureV2 {
    let code = if is_timeout_error(&error) {
        PluginInvocationErrorCodeV2::DeadlineExceeded
    } else if is_execution_limit_error(&error, store) {
        PluginInvocationErrorCodeV2::ResourceLimitExceeded
    } else {
        fallback
    };
    CoreGuestFailureV2::Failed { code, events }
}

fn failure_without_events(code: PluginInvocationErrorCodeV2) -> CoreGuestFailureV2 {
    CoreGuestFailureV2::Failed { code, events: Vec::new() }
}

fn request_error(
    code: PluginInvocationErrorCodeV2,
    request: &PluginInvocationRequestV2,
) -> PluginInvocationErrorV2 {
    invocation_error(
        code,
        Some(request.call_id.clone()),
        Some(request.binding_id.clone()),
        Some(request.runtime_generation),
    )
}

fn invocation_error(
    code: PluginInvocationErrorCodeV2,
    call_id: Option<palyra_plugins_sdk::PluginCallIdV2>,
    binding_id: Option<PluginBindingIdV2>,
    runtime_generation: Option<palyra_plugins_sdk::PluginRuntimeGenerationV2>,
) -> PluginInvocationErrorV2 {
    PluginInvocationErrorV2::new(code, call_id, binding_id, runtime_generation)
}
