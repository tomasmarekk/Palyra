use crate::*;

pub(crate) fn run_init(
    mode: InitModeArg,
    path: Option<String>,
    force: bool,
    tls_scaffold: InitTlsScaffoldArg,
) -> Result<()> {
    let mode = InitMode::from_arg(mode);
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

    println!(
        "init.status=complete mode={} config_path={} force={}",
        mode.deployment_mode(),
        config_path.display(),
        force
    );
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

    std::io::stdout().flush().context("stdout flush failed")
}
