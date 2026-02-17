use std::{sync::mpsc, time::Duration};

use palyra_plugins_sdk::{
    HOST_CAPABILITIES_IMPORT_MODULE, HOST_CAPABILITY_CHANNEL_COUNT_FN,
    HOST_CAPABILITY_CHANNEL_HANDLE_FN, HOST_CAPABILITY_HTTP_COUNT_FN,
    HOST_CAPABILITY_HTTP_HANDLE_FN, HOST_CAPABILITY_SECRET_COUNT_FN,
    HOST_CAPABILITY_SECRET_HANDLE_FN, HOST_CAPABILITY_STORAGE_COUNT_FN,
    HOST_CAPABILITY_STORAGE_HANDLE_FN,
};
use thiserror::Error;
use wasmtime::{
    Caller, Config, Engine, Instance, Linker, Module, Store, StoreLimits, StoreLimitsBuilder,
    TypedFunc,
};

const HTTP_HANDLE_BASE: i32 = 10_000;
const SECRET_HANDLE_BASE: i32 = 20_000;
const STORAGE_HANDLE_BASE: i32 = 30_000;
const CHANNEL_HANDLE_BASE: i32 = 40_000;
const EPOCH_DEADLINE_TICKS_WITH_TIMEOUT: u64 = 1;
const EPOCH_DEADLINE_TICKS_WITHOUT_TIMEOUT: u64 = 1_000_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeLimits {
    pub fuel_budget: u64,
    pub max_memory_bytes: usize,
    pub max_table_elements: usize,
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CapabilityGrantSet {
    pub http_hosts: Vec<String>,
    pub secret_keys: Vec<String>,
    pub storage_prefixes: Vec<String>,
    pub channels: Vec<String>,
}

impl CapabilityGrantSet {
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CapabilityHandles {
    pub http_handles: Vec<i32>,
    pub secret_handles: Vec<i32>,
    pub storage_handles: Vec<i32>,
    pub channel_handles: Vec<i32>,
}

impl CapabilityHandles {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmExecutionResult {
    pub exit_code: i32,
    pub capability_handles: CapabilityHandles,
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("failed to compile wasm module: {0}")]
    Compile(#[from] wasmtime::Error),
    #[error("failed to link wasm host capability interface: {0}")]
    Linker(wasmtime::Error),
    #[error("wasm execution failed: {0}")]
    Execution(wasmtime::Error),
    #[error("wasm execution timed out")]
    ExecutionTimedOut,
    #[error("failed to resolve exported function '{0}'")]
    MissingExport(String),
    #[error("wasm execution exceeded runtime limits")]
    ExecutionLimitExceeded,
}

pub struct WasmRuntime {
    engine: Engine,
    limits: RuntimeLimits,
}

impl WasmRuntime {
    pub fn new() -> Result<Self, RuntimeError> {
        Self::new_with_limits(RuntimeLimits::default())
    }

    pub fn new_with_limits(limits: RuntimeLimits) -> Result<Self, RuntimeError> {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.epoch_interruption(true);
        let engine = Engine::new(&config)?;
        Ok(Self { engine, limits })
    }

    pub fn call_noarg_i32_export(
        &self,
        module_bytes: &[u8],
        export_name: &str,
    ) -> Result<i32, RuntimeError> {
        let result =
            self.execute_i32_entrypoint(module_bytes, export_name, &CapabilityGrantSet::default())?;
        Ok(result.exit_code)
    }

    pub fn execute_i32_entrypoint(
        &self,
        module_bytes: &[u8],
        entrypoint: &str,
        capabilities: &CapabilityGrantSet,
    ) -> Result<WasmExecutionResult, RuntimeError> {
        self.execute_i32_entrypoint_internal(module_bytes, entrypoint, capabilities, None)
    }

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
        let module = Module::new(&self.engine, module_bytes)?;
        let capability_handles = CapabilityHandles::from_grants(capabilities);
        let store_limits = StoreLimitsBuilder::new()
            .memory_size(self.limits.max_memory_bytes)
            .table_elements(self.limits.max_table_elements)
            .instances(self.limits.max_instances)
            .build();
        let mut store = Store::new(
            &self.engine,
            RuntimeStoreState {
                limits: store_limits,
                capability_handles: capability_handles.clone(),
            },
        );
        store.limiter(|state| &mut state.limits);
        store.set_fuel(self.limits.fuel_budget)?;
        configure_epoch_deadline(&mut store, timeout.is_some());
        let _timeout_guard =
            timeout.map(|duration| arm_epoch_timeout_guard(self.engine.clone(), duration));
        let instance = self.instantiate_with_linker(&module, &mut store)?;
        let function: TypedFunc<(), i32> = instance
            .get_typed_func(&mut store, entrypoint)
            .map_err(|_| RuntimeError::MissingExport(entrypoint.to_owned()))?;
        let output = function
            .call(&mut store, ())
            .map_err(|error| map_execution_error_with_store(error, &store))?;
        Ok(WasmExecutionResult { exit_code: output, capability_handles })
    }

    fn instantiate_with_linker(
        &self,
        module: &Module,
        store: &mut Store<RuntimeStoreState>,
    ) -> Result<Instance, RuntimeError> {
        let mut linker = Linker::new(&self.engine);
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

fn resolve_capability_handle(handles: &[i32], index: i32) -> i32 {
    if index < 0 {
        return -1;
    }
    handles.get(index as usize).copied().unwrap_or(-1)
}

fn configure_epoch_deadline(store: &mut Store<RuntimeStoreState>, timeout_enabled: bool) {
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

struct EpochTimeoutGuard {
    cancel_tx: Option<mpsc::Sender<()>>,
}

impl Drop for EpochTimeoutGuard {
    fn drop(&mut self) {
        if let Some(cancel_tx) = self.cancel_tx.take() {
            let _ = cancel_tx.send(());
        }
    }
}

fn arm_epoch_timeout_guard(engine: Engine, timeout: Duration) -> EpochTimeoutGuard {
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

fn is_timeout_error(error: &wasmtime::Error) -> bool {
    matches!(error.downcast_ref::<wasmtime::Trap>(), Some(wasmtime::Trap::Interrupt))
}

fn is_execution_limit_error(error: &wasmtime::Error, store: &Store<RuntimeStoreState>) -> bool {
    store.get_fuel().ok() == Some(0)
        || matches!(
            error.downcast_ref::<wasmtime::Trap>(),
            Some(wasmtime::Trap::OutOfFuel | wasmtime::Trap::AllocationTooLarge)
        )
        || error_chain_contains_any(error, &["resource limit exceeded", "exceeds memory limits"])
}

fn error_chain_contains_any(error: &wasmtime::Error, needles: &[&str]) -> bool {
    if needles.iter().any(|needle| error.to_string().contains(needle)) {
        return true;
    }
    let mut source = error.source();
    while let Some(current) = source {
        if needles.iter().any(|needle| current.to_string().contains(needle)) {
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

fn build_handles(values: &[String], base: i32) -> Vec<i32> {
    values.iter().enumerate().map(|(index, _)| base + index as i32).collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{CapabilityGrantSet, RuntimeError, RuntimeLimits, WasmRuntime};
    use palyra_plugins_sdk::{
        DEFAULT_RUNTIME_ENTRYPOINT, HOST_CAPABILITIES_IMPORT_MODULE,
        HOST_CAPABILITY_CHANNEL_COUNT_FN, HOST_CAPABILITY_HTTP_COUNT_FN,
        HOST_CAPABILITY_HTTP_HANDLE_FN, HOST_CAPABILITY_SECRET_COUNT_FN,
        HOST_CAPABILITY_STORAGE_COUNT_FN,
    };

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
