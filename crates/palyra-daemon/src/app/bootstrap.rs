//! Startup bootstrap: CLI parsing, config load, and fail-closed policy preflight.

use anyhow::{Context, Result};
use clap::Parser;
use palyra_common::config_system::backup_path;

use crate::{
    config::{config_recovery_paths, load_config, load_config_from_path, LoadedConfig},
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
    let mut loaded = load_config_with_backup_fallback()?;
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

fn load_config_with_backup_fallback() -> Result<LoadedConfig> {
    let primary_error = match load_config() {
        Ok(loaded) => return Ok(loaded),
        Err(error) => error,
    };
    let recovery_paths = config_recovery_paths().with_context(|| {
        format!("config load failed ({primary_error}); recovery path resolution also failed")
    })?;
    for original_path in recovery_paths {
        for backup_index in 1..=3 {
            let candidate = backup_path(original_path.as_path(), backup_index);
            if !candidate.exists() {
                continue;
            }
            match load_config_from_path(candidate.as_path()) {
                Ok(mut recovered) => {
                    let environment_suffix = recovered
                        .source
                        .find(" +env(")
                        .map(|index| recovered.source[index..].to_owned())
                        .unwrap_or_default();
                    recovered.source =
                        format!("{}{environment_suffix}", original_path.to_string_lossy());
                    tracing::warn!(
                        backup_index,
                        reason_code = "daemon.config.startup_recovered_last_known_good",
                        "primary config failed validation; using validated last-known-good backup"
                    );
                    return Ok(recovered);
                }
                Err(error) => {
                    tracing::warn!(
                        backup_index,
                        message = %error,
                        "config backup candidate failed validation"
                    );
                }
            }
        }
    }
    Err(primary_error).context("config load failed and no valid last-known-good backup was found")
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

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, fs, path::Path};

    use super::*;

    struct EnvRestore {
        name: &'static str,
        previous: Option<OsString>,
    }

    impl EnvRestore {
        fn set_path(name: &'static str, value: &Path) -> Self {
            let previous = std::env::var_os(name);
            std::env::set_var(name, value);
            Self { name, previous }
        }
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous {
                std::env::set_var(self.name, previous);
            } else {
                std::env::remove_var(self.name);
            }
        }
    }

    #[test]
    fn startup_uses_valid_backup_when_explicit_config_is_missing() {
        let _environment = crate::test_env::lock();
        let directory = tempfile::tempdir().expect("temp directory should exist");
        let config_path = directory.path().join("config.toml");
        let backup = backup_path(config_path.as_path(), 1);
        fs::write(&backup, "version = 1\n").expect("backup fixture should write");
        let _config = EnvRestore::set_path("PALYRA_CONFIG", config_path.as_path());

        let loaded = load_config_with_backup_fallback().expect("backup should recover startup");

        assert_eq!(loaded.config_version, 1);
        assert!(loaded.source.starts_with(config_path.to_string_lossy().as_ref()));
        assert!(!config_path.exists(), "runtime fallback must not overwrite the invalid source");
    }

    #[test]
    fn invalid_backup_does_not_replace_primary_error() {
        let _environment = crate::test_env::lock();
        let directory = tempfile::tempdir().expect("temp directory should exist");
        let config_path = directory.path().join("config.toml");
        let backup = backup_path(config_path.as_path(), 1);
        fs::write(&config_path, "not valid = [").expect("invalid primary should write");
        fs::write(&backup, "also invalid = [").expect("invalid backup should write");
        let _config = EnvRestore::set_path("PALYRA_CONFIG", config_path.as_path());

        let error =
            load_config_with_backup_fallback().expect_err("invalid backup must fail closed");

        assert!(error.to_string().contains("no valid last-known-good backup"));
    }
}
