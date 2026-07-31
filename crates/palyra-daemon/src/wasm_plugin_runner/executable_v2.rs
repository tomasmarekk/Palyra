//! Host lifecycle for executable plugin ABI v2 bindings.
//!
//! This layer owns module registration, invocation cancellation, binding
//! disposal/quarantine, and payload-free diagnostics around `PluginRuntimeV2`.

use std::{
    collections::{btree_map::Entry, BTreeMap},
    fmt,
    sync::{Arc, Mutex},
};

use palyra_plugins_runtime::{
    PluginCoreWasmCancellationTokenV2, PluginRuntimeV2, RuntimeError, RuntimeLimits,
};
use palyra_plugins_sdk::{
    PluginBindingCleanupV2, PluginBindingIdV2, PluginBindingRecordV2, PluginCallIdV2,
    PluginInvocationErrorV2, PluginInvocationRequestV2, PluginInvocationTranscriptV2,
    PluginRuntimeDiagnosticsV2, PluginRuntimeGenerationV2,
};

use super::WasmPluginRunnerPolicy;

#[derive(Debug)]
struct ExecutablePluginBinding {
    module_bytes: Arc<[u8]>,
}

struct ExecutablePluginRuntimeState {
    runtime: PluginRuntimeV2,
    bindings: BTreeMap<PluginBindingIdV2, ExecutablePluginBinding>,
}

#[derive(Debug, Clone)]
struct ActivePluginInvocation {
    binding_id: PluginBindingIdV2,
    runtime_generation: PluginRuntimeGenerationV2,
    cancellation: PluginCoreWasmCancellationTokenV2,
}

/// Fail-closed daemon error for executable plugin binding lifecycle.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ExecutablePluginHostError {
    /// Runtime policy disabled Wasm plugin execution.
    #[error("executable plugin runtime is disabled by runtime policy")]
    Disabled,
    /// The resolved artifact did not contain executable module bytes.
    #[error("executable plugin module is empty")]
    EmptyModule,
    /// The resolved artifact exceeded the configured module-size limit.
    #[error("executable plugin module exceeds max_module_size_bytes={max_bytes}")]
    ModuleTooLarge {
        /// Configured maximum module size.
        max_bytes: u64,
    },
    /// Runtime initialization failed before a binding could be registered.
    #[error("failed to initialize executable plugin runtime")]
    RuntimeInitialization {
        /// Wasmtime initialization failure.
        #[source]
        source: RuntimeError,
    },
    /// A binding or invocation violated the typed ABI contract.
    #[error("{source}")]
    Invocation {
        /// Payload-free ABI error with a stable reason code.
        #[from]
        source: PluginInvocationErrorV2,
    },
    /// A call identifier was already active.
    #[error("plugin invocation call id is already active")]
    DuplicateCall,
    /// Internal runtime state is unavailable after a panic.
    #[error("executable plugin runtime state is unavailable")]
    StateUnavailable,
}

/// Stateful, policy-bound host for executable ABI v2 plugin calls.
///
/// Invocations are synchronous because Wasmtime execution is CPU-bound. Async
/// callers must execute [`Self::invoke`] on their existing blocking executor.
pub(crate) struct ExecutablePluginRuntimeHost {
    max_module_size_bytes: u64,
    state: Mutex<ExecutablePluginRuntimeState>,
    active: Mutex<BTreeMap<PluginCallIdV2, ActivePluginInvocation>>,
}

impl fmt::Debug for ExecutablePluginRuntimeHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let diagnostics = self.diagnostics().ok();
        let active_count = self.active.lock().map(|active| active.len()).unwrap_or_default();
        formatter
            .debug_struct("ExecutablePluginRuntimeHost")
            .field("max_module_size_bytes", &self.max_module_size_bytes)
            .field("active_count", &active_count)
            .field("diagnostics", &diagnostics)
            .finish()
    }
}

impl ExecutablePluginRuntimeHost {
    /// Creates a host pinned to the daemon's Wasm policy and resource limits.
    ///
    /// # Errors
    /// Returns [`ExecutablePluginHostError::Disabled`] when policy disables
    /// Wasm execution, or `RuntimeInitialization` when Wasmtime setup fails.
    pub(crate) fn new(
        policy: &WasmPluginRunnerPolicy,
        limits: RuntimeLimits,
    ) -> Result<Self, ExecutablePluginHostError> {
        if !policy.enabled {
            return Err(ExecutablePluginHostError::Disabled);
        }
        let runtime = PluginRuntimeV2::new_with_limits(limits)
            .map_err(|source| ExecutablePluginHostError::RuntimeInitialization { source })?;
        Ok(Self {
            max_module_size_bytes: policy.max_module_size_bytes,
            state: Mutex::new(ExecutablePluginRuntimeState { runtime, bindings: BTreeMap::new() }),
            active: Mutex::new(BTreeMap::new()),
        })
    }

    /// Registers a host-approved binding and its signed artifact module.
    ///
    /// # Errors
    /// Returns a size/admission error before retaining the module, or
    /// `StateUnavailable` if an earlier panic poisoned runtime state.
    pub(crate) fn bind(
        &self,
        binding: PluginBindingRecordV2,
        module_bytes: Vec<u8>,
    ) -> Result<(), ExecutablePluginHostError> {
        if module_bytes.is_empty() {
            return Err(ExecutablePluginHostError::EmptyModule);
        }
        let module_size = u64::try_from(module_bytes.len()).unwrap_or(u64::MAX);
        if module_size > self.max_module_size_bytes {
            return Err(ExecutablePluginHostError::ModuleTooLarge {
                max_bytes: self.max_module_size_bytes,
            });
        }
        let binding_id = binding.binding_id.clone();
        let mut state =
            self.state.lock().map_err(|_| ExecutablePluginHostError::StateUnavailable)?;
        state.runtime.register_binding(binding)?;
        state
            .bindings
            .insert(binding_id, ExecutablePluginBinding { module_bytes: Arc::from(module_bytes) });
        Ok(())
    }

    /// Invokes the module registered for a request's exact binding.
    ///
    /// The caller supplies a cancellation token so a concurrent supervisor can
    /// interrupt guest execution without acquiring the Wasmtime state lock.
    ///
    /// # Errors
    /// Returns a stable ABI admission error, duplicate-call error, or
    /// `StateUnavailable` when host state cannot be accessed.
    pub(crate) fn invoke(
        &self,
        request: &PluginInvocationRequestV2,
        now_unix_ms: u64,
        cancellation: PluginCoreWasmCancellationTokenV2,
    ) -> Result<PluginInvocationTranscriptV2, ExecutablePluginHostError> {
        {
            let mut active =
                self.active.lock().map_err(|_| ExecutablePluginHostError::StateUnavailable)?;
            match active.entry(request.call_id.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(ActivePluginInvocation {
                        binding_id: request.binding_id.clone(),
                        runtime_generation: request.runtime_generation,
                        cancellation: cancellation.clone(),
                    });
                }
                Entry::Occupied(_) => return Err(ExecutablePluginHostError::DuplicateCall),
            }
        }

        let result = (|| {
            let mut state =
                self.state.lock().map_err(|_| ExecutablePluginHostError::StateUnavailable)?;
            let module_bytes = state
                .bindings
                .get(&request.binding_id)
                .map(|binding| Arc::clone(&binding.module_bytes))
                .ok_or_else(|| {
                    PluginInvocationErrorV2::new(
                        palyra_plugins_sdk::PluginInvocationErrorCodeV2::BindingNotFound,
                        Some(request.call_id.clone()),
                        Some(request.binding_id.clone()),
                        Some(request.runtime_generation),
                    )
                })?;
            state
                .runtime
                .invoke(module_bytes.as_ref(), request, now_unix_ms, cancellation)
                .map_err(ExecutablePluginHostError::from)
        })();
        self.remove_active_call(&request.call_id);
        result
    }

    /// Requests cooperative cancellation for an active call.
    ///
    /// Returns `true` only when the call was active at the time of the request.
    pub(crate) fn cancel(
        &self,
        call_id: &PluginCallIdV2,
    ) -> Result<bool, ExecutablePluginHostError> {
        let active = self.active.lock().map_err(|_| ExecutablePluginHostError::StateUnavailable)?;
        let Some(invocation) = active.get(call_id) else {
            return Ok(false);
        };
        invocation.cancellation.cancel();
        Ok(true)
    }

    /// Disposes a generation after cancelling its active calls.
    ///
    /// # Errors
    /// Returns a binding lifecycle error or `StateUnavailable`.
    pub(crate) fn dispose(
        &self,
        binding_id: &PluginBindingIdV2,
        generation: PluginRuntimeGenerationV2,
    ) -> Result<PluginBindingCleanupV2, ExecutablePluginHostError> {
        self.cancel_binding_calls(binding_id, generation)?;
        let mut state =
            self.state.lock().map_err(|_| ExecutablePluginHostError::StateUnavailable)?;
        let cleanup = state.runtime.dispose_binding(binding_id, generation)?;
        state.bindings.remove(binding_id);
        Ok(cleanup)
    }

    /// Quarantines a generation after cancelling its active calls.
    ///
    /// # Errors
    /// Returns a binding lifecycle error or `StateUnavailable`.
    pub(crate) fn quarantine(
        &self,
        binding_id: &PluginBindingIdV2,
        generation: PluginRuntimeGenerationV2,
    ) -> Result<PluginBindingCleanupV2, ExecutablePluginHostError> {
        self.cancel_binding_calls(binding_id, generation)?;
        let mut state =
            self.state.lock().map_err(|_| ExecutablePluginHostError::StateUnavailable)?;
        let cleanup = state.runtime.quarantine_binding(binding_id, generation)?;
        state.bindings.remove(binding_id);
        Ok(cleanup)
    }

    /// Returns deterministic binding diagnostics without module or payload bytes.
    ///
    /// # Errors
    /// Returns `StateUnavailable` if runtime state was poisoned by a panic.
    pub(crate) fn diagnostics(
        &self,
    ) -> Result<PluginRuntimeDiagnosticsV2, ExecutablePluginHostError> {
        let state = self.state.lock().map_err(|_| ExecutablePluginHostError::StateUnavailable)?;
        Ok(state.runtime.diagnostics())
    }

    fn cancel_binding_calls(
        &self,
        binding_id: &PluginBindingIdV2,
        generation: PluginRuntimeGenerationV2,
    ) -> Result<(), ExecutablePluginHostError> {
        let active = self.active.lock().map_err(|_| ExecutablePluginHostError::StateUnavailable)?;
        for invocation in active.values().filter(|invocation| {
            &invocation.binding_id == binding_id && invocation.runtime_generation == generation
        }) {
            invocation.cancellation.cancel();
        }
        Ok(())
    }

    fn remove_active_call(&self, call_id: &PluginCallIdV2) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(call_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        thread,
        time::{Duration, Instant},
    };

    use palyra_plugins_runtime::{PluginCoreWasmCancellationTokenV2, RuntimeLimits};
    use palyra_plugins_sdk::{
        executable_plugin_contract_schema_v2, AgentHarnessInvocationV2, AgentHarnessOutcomeV2,
        AgentHarnessResultV2, ExecutablePluginContractKindV2, ExecutablePluginOperationV2,
        PluginBindingIdV2, PluginBindingRecordV2, PluginCallIdV2, PluginInvocationBudgetV2,
        PluginInvocationRequestV2, PluginInvocationTerminalOutcomeV2, PluginRuntimeGenerationV2,
    };

    use super::ExecutablePluginRuntimeHost;
    use crate::wasm_plugin_runner::WasmPluginRunnerPolicy;

    const AGENT_HARNESS_TEMPLATE: &str =
        include_str!("../../../palyra-plugins/runtime/tests/fixtures/agent_harness_v2.wat");
    const CANCELLATION_MODULE: &[u8] =
        include_bytes!("../../../palyra-plugins/runtime/tests/fixtures/cancellation_stream_v2.wat");
    const NOW_UNIX_MS: u64 = 1_000;

    #[test]
    fn policy_bound_host_invokes_and_disposes_real_module_without_payload_diagnostics() {
        let output = AgentHarnessResultV2 {
            outcome: AgentHarnessOutcomeV2::Completed,
            output_ref: Some("artifact-1".to_owned()),
            steps_used: 2,
        }
        .encode_core_bytes()
        .expect("fixture output should encode");
        let module = materialize_fixture(AGENT_HARNESS_TEMPLATE, &output);
        let (binding, request) = binding_and_request();
        let host = ExecutablePluginRuntimeHost::new(&test_policy(), RuntimeLimits::default())
            .expect("host should initialize");
        host.bind(binding.clone(), module).expect("binding should register");

        let transcript = host
            .invoke(&request, NOW_UNIX_MS, PluginCoreWasmCancellationTokenV2::new())
            .expect("real module should execute");
        assert!(matches!(
            transcript.terminal().outcome,
            PluginInvocationTerminalOutcomeV2::Completed { .. }
        ));

        let diagnostics = host.diagnostics().expect("diagnostics should be available");
        let serialized = serde_json::to_string(&diagnostics).expect("diagnostics should serialize");
        assert_eq!(diagnostics.bindings.len(), 1);
        assert_eq!(diagnostics.bindings[0].granted_capability_handle_count, 0);
        assert!(!serialized.contains("artifact-1"));
        assert!(!serialized.contains("prepared-attempt"));

        let cleanup = host
            .dispose(&binding.binding_id, binding.runtime_generation)
            .expect("binding should dispose");
        assert_eq!(cleanup.released_capability_handle_count, 0);
        assert!(
            host.diagnostics().expect("diagnostics should remain available").bindings[0].state
                == palyra_plugins_sdk::PluginBindingStateV2::Disposed
        );
    }

    #[test]
    fn policy_bound_host_rejects_disabled_and_oversize_modules() {
        let mut policy = test_policy();
        policy.enabled = false;
        assert!(ExecutablePluginRuntimeHost::new(&policy, RuntimeLimits::default()).is_err());

        let policy = WasmPluginRunnerPolicy { max_module_size_bytes: 1, ..test_policy() };
        let host = ExecutablePluginRuntimeHost::new(&policy, RuntimeLimits::default())
            .expect("host should initialize");
        let (binding, _) = binding_and_request();
        assert!(host.bind(binding, vec![0, 1]).is_err());
    }

    #[test]
    fn policy_bound_host_cancels_and_quarantines_active_generation() {
        let (binding, request) = binding_and_request();
        let host = Arc::new(
            ExecutablePluginRuntimeHost::new(
                &test_policy(),
                RuntimeLimits { fuel_budget: 1_000_000_000, ..RuntimeLimits::default() },
            )
            .expect("host should initialize"),
        );
        host.bind(binding.clone(), CANCELLATION_MODULE.to_vec()).expect("binding should register");
        let cancellation = PluginCoreWasmCancellationTokenV2::new();
        let cancellation_for_guest = cancellation.clone();
        let host_for_guest = Arc::clone(&host);
        let request_for_guest = request.clone();
        let worker = thread::spawn(move || {
            host_for_guest.invoke(&request_for_guest, NOW_UNIX_MS, cancellation_for_guest)
        });

        // A fixed yield count can expire before the worker is scheduled under full-suite load.
        let event_deadline = Instant::now() + Duration::from_secs(30);
        while cancellation.observed_event_count() == 0 {
            assert!(
                Instant::now() < event_deadline,
                "guest did not emit its pre-cancellation event"
            );
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(cancellation.observed_event_count(), 1);
        assert!(
            host.cancel(&request.call_id).expect("active call should be cancellable"),
            "host should find the active call"
        );
        let transcript =
            worker.join().expect("worker should not panic").expect("call should be admitted");
        assert!(matches!(
            transcript.terminal().outcome,
            PluginInvocationTerminalOutcomeV2::Cancelled { .. }
        ));

        let cleanup = host
            .quarantine(&binding.binding_id, binding.runtime_generation)
            .expect("binding should quarantine");
        assert_eq!(cleanup.final_state, palyra_plugins_sdk::PluginBindingStateV2::Quarantined);
        assert_eq!(cleanup.released_capability_handle_count, 0);
        assert_eq!(
            host.diagnostics().expect("diagnostics should be available").bindings[0].state,
            palyra_plugins_sdk::PluginBindingStateV2::Quarantined
        );
    }

    fn binding_and_request() -> (PluginBindingRecordV2, PluginInvocationRequestV2) {
        let contract = ExecutablePluginContractKindV2::AgentHarness;
        let operation = ExecutablePluginOperationV2::RunAgentAttempt;
        let schema = executable_plugin_contract_schema_v2(contract);
        let runtime_generation =
            PluginRuntimeGenerationV2::new(1).expect("generation should be nonzero");
        let binding_id =
            PluginBindingIdV2::new("binding-agent").expect("binding id should be valid");
        let binding = PluginBindingRecordV2 {
            binding_id: binding_id.clone(),
            contract,
            operation,
            runtime_generation,
            input_schema_hash: schema.input_schema_hash.clone(),
            output_schema_hash: schema.output_schema_hash.clone(),
            issued_at_unix_ms: 500,
            expires_at_unix_ms: 10_000,
            granted_capability_handles: Vec::new(),
        };
        let input_bytes = AgentHarnessInvocationV2 {
            prepared_attempt_ref: "prepared-attempt".to_owned(),
            objective_hash: schema.input_schema_hash.clone(),
            max_steps: 4,
        }
        .encode_core_bytes()
        .expect("fixture input should encode");
        let request = PluginInvocationRequestV2 {
            schema_version: 2,
            call_id: PluginCallIdV2::new("call-agent").expect("call id should be valid"),
            binding_id,
            runtime_generation,
            contract,
            operation,
            budget: PluginInvocationBudgetV2 {
                absolute_deadline_unix_ms: 5_000,
                max_input_bytes: 4_096,
                max_output_bytes: 4_096,
                max_event_bytes: 256,
                max_events: 4,
            },
            input_schema_hash: schema.input_schema_hash,
            output_schema_hash: schema.output_schema_hash,
            input_bytes,
            granted_capability_handles: Vec::new(),
        };
        (binding, request)
    }

    fn materialize_fixture(template: &str, output: &[u8]) -> Vec<u8> {
        let escaped = output.iter().map(|byte| format!("\\{byte:02x}")).collect::<String>();
        template
            .replace("{{OUTPUT_LEN}}", output.len().to_string().as_str())
            .replace("{{OUTPUT}}", &escaped)
            .into_bytes()
    }

    fn test_policy() -> WasmPluginRunnerPolicy {
        WasmPluginRunnerPolicy {
            enabled: true,
            allow_inline_modules: false,
            max_module_size_bytes: 256 * 1024,
            fuel_budget: 10_000_000,
            max_memory_bytes: 64 * 1024 * 1024,
            max_table_elements: 100_000,
            max_instances: 256,
            allowed_http_hosts: Vec::new(),
            allowed_secrets: Vec::new(),
            allowed_storage_prefixes: Vec::new(),
            allowed_channels: Vec::new(),
        }
    }
}
