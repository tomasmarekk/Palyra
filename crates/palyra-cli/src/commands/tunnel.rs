//! SSH local-forward helper for reaching a remote daemon dashboard.
//!
//! Shells out to the system `ssh` binary and blocks until the tunnel
//! process exits; the experimental backend flag only changes the reported
//! preview status, not behavior.

use crate::*;

const SSH_TUNNEL_BACKEND_FLAG: &str = "PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_SSH_TUNNEL";

/// Runs `palyra tunnel`, forwarding a local port to the remote daemon over
/// ssh and optionally opening the local dashboard URL first.
///
/// Blocks for the lifetime of the ssh process.
///
/// # Errors
/// Fails when arguments are blank, the browser open fails, ssh cannot be
/// launched, or ssh exits unsuccessfully.
pub(crate) fn run_tunnel(
    ssh: String,
    remote_port: u16,
    local_port: u16,
    open: bool,
    identity_file: Option<String>,
) -> Result<()> {
    let ssh = normalize_required_text_arg(ssh, "--ssh")?;
    let identity_file = identity_file.and_then(normalize_optional_text_arg);
    let local_dashboard_url = format!("http://127.0.0.1:{local_port}/");
    let preview_enabled = std::env::var(SSH_TUNNEL_BACKEND_FLAG).is_ok_and(|value| {
        matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
    });
    println!(
        "tunnel.backend_profile=ssh_tunnel preview_enabled={} rollout_flag={} manual_forward_required=true",
        preview_enabled, SSH_TUNNEL_BACKEND_FLAG
    );
    println!(
        "tunnel.status=starting ssh_target={} local_dashboard_url={} forward={}=>127.0.0.1:{}",
        ssh, local_dashboard_url, local_port, remote_port
    );
    std::io::stdout().flush().context("stdout flush failed")?;

    if open {
        open_url_in_default_browser(local_dashboard_url.as_str()).with_context(|| {
            format!("failed to open local dashboard URL {}", local_dashboard_url)
        })?;
    }

    let mut command = Command::new("ssh");
    command.arg("-N");
    command.arg("-L");
    command.arg(format!("{local_port}:127.0.0.1:{remote_port}"));
    if let Some(identity_file) = identity_file {
        command.arg("-i");
        command.arg(identity_file);
    }
    command.arg(ssh);

    let status = command.status().context(
        "failed to launch ssh for tunnel helper; ensure `ssh` is installed and available on PATH",
    )?;
    if !status.success() {
        anyhow::bail!(
            "ssh tunnel exited with status {}",
            status.code().map(|value| value.to_string()).unwrap_or_else(|| "unknown".to_owned())
        );
    }
    Ok(())
}
