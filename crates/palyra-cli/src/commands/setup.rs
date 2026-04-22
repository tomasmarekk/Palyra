use crate::*;

pub(crate) fn run_setup(
    mode: InitModeArg,
    path: Option<String>,
    force: bool,
    tls_scaffold: InitTlsScaffoldArg,
    wizard: bool,
    wizard_options: SetupWizardOverridesArg,
) -> Result<()> {
    let requested_path = path.clone();
    let deployment_profile = wizard_options.deployment_profile;
    if !wizard {
        commands::init::run_init(mode, deployment_profile, path, force, tls_scaffold)?;
    } else {
        let wizard_options = WizardOverridesArg {
            flow: wizard_options.flow,
            non_interactive: wizard_options.non_interactive,
            accept_risk: wizard_options.accept_risk,
            json: wizard_options.json,
            workspace_root: wizard_options.workspace_root,
            auth_method: wizard_options.auth_method,
            api_key_env: wizard_options.api_key_env,
            api_key_stdin: wizard_options.api_key_stdin,
            api_key_prompt: wizard_options.api_key_prompt,
            deployment_profile,
            bind_profile: wizard_options.bind_profile,
            daemon_port: wizard_options.daemon_port,
            grpc_port: wizard_options.grpc_port,
            quic_port: wizard_options.quic_port,
            tls_scaffold: Some(tls_scaffold),
            tls_cert_path: wizard_options.tls_cert_path,
            tls_key_path: wizard_options.tls_key_path,
            remote_base_url: wizard_options.remote_base_url,
            admin_token_env: wizard_options.admin_token_env,
            admin_token_stdin: wizard_options.admin_token_stdin,
            admin_token_prompt: wizard_options.admin_token_prompt,
            remote_verification: wizard_options.remote_verification,
            pinned_server_cert_sha256: wizard_options.pinned_server_cert_sha256,
            pinned_gateway_ca_sha256: wizard_options.pinned_gateway_ca_sha256,
            ssh_target: wizard_options.ssh_target,
            skip_health: wizard_options.skip_health,
            skip_channels: wizard_options.skip_channels,
            skip_skills: wizard_options.skip_skills,
        };
        commands::operator_wizard::run_setup_wizard(
            mode,
            path,
            force,
            tls_scaffold,
            wizard_options,
        )?;
    }
    let config_path = resolve_init_path(requested_path)?;
    let state_root = app::current_root_context().map(|context| context.state_root().to_path_buf());
    app::update_active_profile_paths(Some(config_path.as_path()), state_root.as_deref())?;
    Ok(())
}
