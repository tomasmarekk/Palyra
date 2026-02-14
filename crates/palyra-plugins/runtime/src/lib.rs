use thiserror::Error;
use wasmtime::{Engine, Instance, Module, Store, TypedFunc};

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("failed to compile wasm module: {0}")]
    Compile(#[from] wasmtime::Error),
    #[error("failed to resolve exported function '{0}'")]
    MissingExport(String),
}

pub struct WasmRuntime {
    engine: Engine,
}

impl WasmRuntime {
    pub fn new() -> Result<Self, RuntimeError> {
        let engine = Engine::default();
        Ok(Self { engine })
    }

    pub fn call_noarg_i32_export(
        &self,
        module_bytes: &[u8],
        export_name: &str,
    ) -> Result<i32, RuntimeError> {
        let module = Module::new(&self.engine, module_bytes)?;
        let mut store = Store::new(&self.engine, ());
        let instance = Instance::new(&mut store, &module, &[])?;
        let function: TypedFunc<(), i32> = instance
            .get_typed_func(&mut store, export_name)
            .map_err(|_| RuntimeError::MissingExport(export_name.to_owned()))?;
        let output = function.call(&mut store, ())?;
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::WasmRuntime;

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
}
