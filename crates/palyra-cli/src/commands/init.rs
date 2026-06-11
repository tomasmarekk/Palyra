//! Non-interactive initialization of a fresh Palyra installation.
//!
//! Writes a daemon-compatible config with a generated admin token, creates
//! the state root (identity, vault, and local workspace directories), and
//! activates the new paths in the CLI profile.

use crate::*;

/// Runs `palyra init`, scaffolding config, state directories, and optional
/// TLS material for the selected deployment profile.
///
/// The effective mode follows the deployment profile rather than the raw
/// mode argument so profile defaults stay authoritative.
///
/// # Errors
/// Fails when the target config exists without `--force`, a directory or
/// file cannot be created, the generated config fails daemon schema
/// validation, or output encoding fails.
pub(crate) fn run_init(
    mode: InitModeArg,
    deployment_profile: Option<DeploymentProfileArg>,
    path: Option<String>,
    force: bool,
    tls_scaffold: InitTlsScaffoldArg,
    json: bool,
) -> Result<()> {
    let requested_mode = InitMode::from_arg(mode);
    let deployment_profile = deployment_profile
        .map(deployment_profile_id_from_arg)
        .unwrap_or_else(|| default_deployment_profile_for_init(requested_mode));
    let mode =
        if deployment_profile == palyra_common::deployment_profiles::DeploymentProfileId::Local {
            InitMode::LocalDesktop
        } else {
            InitMode::RemoteVps
        };
    let config_path = resolve_init_path(path)?;
    if config_path.exists() && !force {
        anyhow::bail!(
            "init target already exists: {} (use --force to overwrite)",
            config_path.display()
        );
    }
    if let Some(parent) = config_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create config directory {}", parent.display())
            })?;
        }
    }

    let state_root = resolve_init_state_root()?;
    fs::create_dir_all(state_root.as_path())
        .with_context(|| format!("failed to create state root {}", state_root.display()))?;
    let identity_store_dir = state_root.join("identity");
    let vault_dir = state_root.join("vault");
    fs::create_dir_all(identity_store_dir.as_path()).with_context(|| {
        format!("failed to create identity store directory {}", identity_store_dir.display())
    })?;
    fs::create_dir_all(vault_dir.as_path())
        .with_context(|| format!("failed to create vault directory {}", vault_dir.display()))?;
    if mode == InitMode::LocalDesktop {
        let workspace_dir = state_root.join("workspace");
        fs::create_dir_all(workspace_dir.as_path()).with_context(|| {
            format!("failed to create local process workspace {}", workspace_dir.display())
        })?;
    }

    let tls_paths =
        if mode == InitMode::RemoteVps && !matches!(tls_scaffold, InitTlsScaffoldArg::None) {
            let tls_root = config_path
                .parent()
                .filter(|value| !value.as_os_str().is_empty())
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from("."))
                .join("tls");
            fs::create_dir_all(tls_root.as_path()).with_context(|| {
                format!("failed to create TLS scaffold directory {}", tls_root.display())
            })?;
            Some((tls_root.join("gateway.crt"), tls_root.join("gateway.key")))
        } else {
            None
        };

    let admin_token = generate_admin_token();
    let document = build_init_config_document(
        mode,
        deployment_profile,
        identity_store_dir.as_path(),
        vault_dir.as_path(),
        admin_token.as_str(),
        tls_paths.as_ref(),
    )?;
    validate_daemon_compatible_document(&document)
        .context("generated init config does not match daemon schema")?;
    let rendered =
        serialize_document_pretty(&document).context("failed to serialize init config document")?;
    fs::write(config_path.as_path(), rendered)
        .with_context(|| format!("failed to write init config {}", config_path.display()))?;
    app::update_active_profile_paths(Some(config_path.as_path()), Some(state_root.as_path()))?;
    if mode == InitMode::LocalDesktop {
        super::browser::configure_local_browser_prerequisites(Some(
            config_path.display().to_string(),
        ))
        .with_context(|| {
            format!("failed to configure local browser prerequisites for {}", config_path.display())
        })?;
    }

    if output::preferred_json(json) {
        return output::print_json_pretty(
            &json!({
                "status": "complete",
                "mode": mode.deployment_mode(),
                "config_path": config_path,
                "force": force,
                "deployment_profile": deployment_profile.as_str(),
                "state_root": state_root,
                "identity_store": identity_store_dir,
                "vault_dir": vault_dir,
                "admin_token_generated": true,
                "admin_token_location": "config(admin.auth_token)",
                "tls_scaffold": init_tls_scaffold_label(tls_scaffold),
                "tls_cert_path": tls_paths.as_ref().map(|(cert_path, _)| cert_path),
                "tls_key_path": tls_paths.as_ref().map(|(_, key_path)| key_path),
                "next": [
                    "run `palyra doctor` and `palyra status`",
                    "start daemon with `palyra gateway run`"
                ],
            }),
            "failed to encode init summary as JSON",
        );
    } else {
        println!(
            "init.status=complete mode={} config_path={} force={}",
            mode.deployment_mode(),
            config_path.display(),
            force
        );
        println!("init.deployment_profile={}", deployment_profile.as_str());
        println!(
            "init.state_root={} identity_store={} vault_dir={}",
            state_root.display(),
            identity_store_dir.display(),
            vault_dir.display()
        );
        println!("init.admin_token_generated=true location=config(admin.auth_token)");

        if mode == InitMode::RemoteVps {
            emit_remote_init_guidance(tls_scaffold, tls_paths.as_ref())?;
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}

fn init_tls_scaffold_label(value: InitTlsScaffoldArg) -> &'static str {
    match value {
        InitTlsScaffoldArg::None => "none",
        InitTlsScaffoldArg::BringYourOwn => "bring-your-own",
        InitTlsScaffoldArg::SelfSigned => "self-signed",
    }
}
