use thiserror::Error;
use wasmtime::{
    Config, Engine, Instance, Module, Store, StoreLimits, StoreLimitsBuilder, TypedFunc,
};

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("failed to compile wasm module: {0}")]
    Compile(#[from] wasmtime::Error),
    #[error("wasm execution failed: {0}")]
    Execution(wasmtime::Error),
    #[error("failed to resolve exported function '{0}'")]
    MissingExport(String),
    #[error("wasm execution exceeded runtime limits")]
    ExecutionLimitExceeded,
}

pub struct WasmRuntime {
    engine: Engine,
    fuel_budget: u64,
    max_memory_bytes: usize,
    max_table_elements: usize,
    max_instances: usize,
}

impl WasmRuntime {
    pub fn new() -> Result<Self, RuntimeError> {
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config)?;
        Ok(Self {
            engine,
            fuel_budget: 10_000_000,
            max_memory_bytes: 64 * 1024 * 1024,
            max_table_elements: 100_000,
            max_instances: 256,
        })
    }

    pub fn call_noarg_i32_export(
        &self,
        module_bytes: &[u8],
        export_name: &str,
    ) -> Result<i32, RuntimeError> {
        let module = Module::new(&self.engine, module_bytes)?;
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.max_memory_bytes)
            .table_elements(self.max_table_elements)
            .instances(self.max_instances)
            .build();
        let mut store = Store::new(&self.engine, RuntimeStoreState { limits });
        store.limiter(|state| &mut state.limits);
        store.set_fuel(self.fuel_budget)?;
        let instance = Instance::new(&mut store, &module, &[]).map_err(map_execution_error)?;
        let function: TypedFunc<(), i32> = instance
            .get_typed_func(&mut store, export_name)
            .map_err(|_| RuntimeError::MissingExport(export_name.to_owned()))?;
        let output = function.call(&mut store, ()).map_err(map_execution_error)?;
        Ok(output)
    }
}

struct RuntimeStoreState {
    limits: StoreLimits,
}

fn map_execution_error(error: wasmtime::Error) -> RuntimeError {
    if error_chain_contains_any(
        &error,
        &[
            "all fuel consumed",
            "exceeds memory limits",
            "memory minimum size",
            "resource limit exceeded",
        ],
    ) {
        return RuntimeError::ExecutionLimitExceeded;
    }
    RuntimeError::Execution(error)
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

#[cfg(test)]
mod tests {
    use super::{RuntimeError, WasmRuntime};

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
    fn runtime_interrupts_infinite_loop_with_fuel_limit() {
        let module = br#"
            (module
                (func (export "spin") (result i32)
                    (loop
                        br 0
                    )
                    i32.const 0
                )
            )
        "#;
        let runtime = WasmRuntime::new().expect("runtime should initialize");

        let result = runtime.call_noarg_i32_export(module, "spin");

        assert!(
            matches!(result, Err(RuntimeError::ExecutionLimitExceeded)),
            "expected fuel exhaustion error, got: {result:?}"
        );
    }

    #[test]
    fn runtime_rejects_module_exceeding_memory_limit() {
        let module = br#"
            (module
                (memory 2000)
                (func (export "answer") (result i32)
                    i32.const 42
                )
            )
        "#;
        let runtime = WasmRuntime::new().expect("runtime should initialize");

        let result = runtime.call_noarg_i32_export(module, "answer");

        assert!(
            matches!(result, Err(RuntimeError::ExecutionLimitExceeded)),
            "expected memory limit error, got: {result:?}"
        );
    }

    #[test]
    fn runtime_reports_trap_as_execution_error() {
        let module = br#"
            (module
                (func (export "trap") (result i32)
                    unreachable
                    i32.const 0
                )
            )
        "#;
        let runtime = WasmRuntime::new().expect("runtime should initialize");

        let result = runtime.call_noarg_i32_export(module, "trap");

        assert!(
            matches!(result, Err(RuntimeError::Execution(_))),
            "expected execution trap error, got: {result:?}"
        );
    }
}
