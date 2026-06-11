//! Arguments for `palyra sandbox`: listing and explaining the effective
//! process-runner and WASM runtime isolation policy. Help text is pinned by
//! snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::{Subcommand, ValueEnum};

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum SandboxCommand {
    List {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Explain {
        #[arg(long, value_enum, default_value_t = SandboxRuntimeArg::All)]
        runtime: SandboxRuntimeArg,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SandboxRuntimeArg {
    All,
    ProcessRunner,
    WasmRuntime,
}
