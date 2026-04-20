use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{Mutex, OnceLock},
};

use anyhow::{Context, Result};

static WORKFLOW_BINARY_BUILD_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

pub fn workspace_root() -> Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .context("failed to resolve workspace root from CARGO_MANIFEST_DIR")
}

pub fn resolve_workspace_binary_path(base_name: &str) -> Result<PathBuf> {
    let workspace_root = workspace_root()?;
    let executable = if cfg!(windows) { format!("{base_name}.exe") } else { base_name.to_owned() };
    let path = workspace_root.join("target").join("debug").join(executable);
    ensure_workspace_binary(base_name, workspace_root.as_path(), path.as_path())?;
    Ok(path)
}

fn ensure_workspace_binary(
    base_name: &str,
    workspace_root: &Path,
    binary_path: &Path,
) -> Result<()> {
    if binary_path.is_file() {
        return Ok(());
    }

    let _guard = WORKFLOW_BINARY_BUILD_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| anyhow::anyhow!("workflow binary build lock was poisoned"))?;
    if binary_path.is_file() {
        return Ok(());
    }

    let package_name = match base_name {
        "palyrad" => "palyra-daemon",
        "palyra-browserd" => "palyra-browserd",
        _ => anyhow::bail!("unsupported workflow regression test binary: {base_name}"),
    };
    let cargo_bin = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let status = Command::new(cargo_bin)
        .current_dir(workspace_root)
        .args(["build", "-p", package_name, "--bin", base_name, "--locked"])
        .status()
        .with_context(|| format!("failed to build required test binary {base_name}"))?;
    if !status.success() {
        anyhow::bail!("building required test binary {base_name} failed with status {status}");
    }
    if !binary_path.is_file() {
        anyhow::bail!(
            "required test binary is still missing after build: {}",
            binary_path.display()
        );
    }
    Ok(())
}
