//! Startup bootstrap: CLI parsing, config load, and fail-closed policy preflight.

use anyhow::Result;
use clap::Parser;

use crate::{
    config::{load_config, LoadedConfig},
    validate_process_runner_backend_policy,
};

// Clap derive: keep comments as `//` so they never leak into --help output.
#[derive(Debug, Clone, Parser)]
#[command(name = "palyrad", about = "Palyra gateway skeleton daemon")]
struct Args {
    #[arg(long)]
    bind: Option<String>,
    #[arg(long)]
    port: Option<u16>,
    #[arg(long)]
    grpc_bind: Option<String>,
    #[arg(long)]
    grpc_port: Option<u16>,
    #[arg(long, default_value_t = false)]
    journal_migrate_only: bool,
}

/// Resolved startup inputs: merged config plus startup-mode flags derived from it.
pub(crate) struct BootstrapContext {
    pub(crate) loaded: LoadedConfig,
    /// When set, the daemon applies journal migrations and exits without serving.
    pub(crate) journal_migrate_only: bool,
    pub(crate) node_rpc_mtls_required: bool,
}

/// Parses CLI arguments, loads layered config, applies CLI overrides, and runs
/// the process-runner policy preflight.
///
/// Calls `Args::parse`, so invalid CLI input terminates the process with a
/// usage error before this function returns.
///
/// # Errors
///
/// Returns an error if config loading fails or if the configured process-runner
/// tier/egress combination is rejected by the fail-closed backend policy.
pub(crate) fn load_runtime_bootstrap() -> Result<BootstrapContext> {
    let args = Args::parse();
    let mut loaded = load_config()?;
    apply_cli_overrides(&mut loaded, &args);
    validate_process_runner_backend_policy(
        loaded.tool_call.process_runner.enabled,
        loaded.tool_call.process_runner.tier,
        loaded.tool_call.process_runner.egress_enforcement_mode,
        !loaded.tool_call.process_runner.allowed_egress_hosts.is_empty()
            || !loaded.tool_call.process_runner.allowed_dns_suffixes.is_empty(),
    )?;
    let node_rpc_mtls_required = !loaded.identity.allow_insecure_node_rpc_without_mtls;
    Ok(BootstrapContext {
        loaded,
        journal_migrate_only: args.journal_migrate_only,
        node_rpc_mtls_required,
    })
}

/// Applies CLI flag overrides onto the loaded config, appending each override
/// to `loaded.source` so diagnostics can show where every value came from.
fn apply_cli_overrides(loaded: &mut LoadedConfig, args: &Args) {
    if let Some(bind) = args.bind.as_ref() {
        loaded.daemon.bind_addr = bind.clone();
        loaded.source.push_str(" +cli(--bind)");
    }
    if let Some(port) = args.port {
        loaded.daemon.port = port;
        loaded.source.push_str(" +cli(--port)");
    }
    if let Some(grpc_bind) = args.grpc_bind.as_ref() {
        loaded.gateway.grpc_bind_addr = grpc_bind.clone();
        loaded.source.push_str(" +cli(--grpc-bind)");
    }
    if let Some(grpc_port) = args.grpc_port {
        loaded.gateway.grpc_port = grpc_port;
        loaded.source.push_str(" +cli(--grpc-port)");
    }
}
