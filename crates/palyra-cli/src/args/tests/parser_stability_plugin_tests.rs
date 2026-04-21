use clap::Parser;

use super::*;

fn run_cli_parse_test_with_large_stack(test: impl FnOnce() + Send + 'static) {
    let handle = std::thread::Builder::new()
        .name("cli-parse-test".to_owned())
        .stack_size(8 * 1024 * 1024)
        .spawn(test)
        .expect("spawn CLI parse test thread with expanded stack");
    if let Err(payload) = handle.join() {
        std::panic::resume_unwind(payload);
    }
}

#[test]
fn parse_skills_audit_command() {
    let audit = Cli::parse_from([
        "palyra",
        "skills",
        "audit",
        "acme.echo_http",
        "--version",
        "1.2.3",
        "--skills-dir",
        "state/skills",
        "--trust-store",
        "state/skills-trust.json",
        "--trusted-publisher",
        "acme=001122",
        "--allow-untrusted",
        "--json",
    ]);
    assert_eq!(
        audit.command,
        Command::Skills {
            command: SkillsCommand::Audit {
                skill_id: Some("acme.echo_http".to_owned()),
                version: Some("1.2.3".to_owned()),
                artifact: None,
                skills_dir: Some("state/skills".to_owned()),
                trust_store: Some("state/skills-trust.json".to_owned()),
                trusted_publishers: vec!["acme=001122".to_owned()],
                allow_untrusted: true,
                json: true,
            },
        }
    );
}

#[test]
fn parse_skills_quarantine_command() {
    let quarantine = Cli::parse_from([
        "palyra",
        "skill",
        "quarantine",
        "acme.echo_http",
        "--version",
        "1.2.3",
        "--reason",
        "manual security hold",
        "--url",
        "http://127.0.0.1:7142",
        "--token",
        "admin-token",
        "--principal",
        "user:ops",
        "--device-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--channel",
        "cli",
        "--json",
    ]);
    assert_eq!(
        quarantine.command,
        Command::Skills {
            command: SkillsCommand::Quarantine {
                skill_id: "acme.echo_http".to_owned(),
                version: Some("1.2.3".to_owned()),
                skills_dir: None,
                reason: Some("manual security hold".to_owned()),
                url: Some("http://127.0.0.1:7142".to_owned()),
                token: Some("admin-token".to_owned()),
                principal: "user:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("cli".to_owned()),
                json: true,
            },
        }
    );
}

#[test]
fn parse_skills_enable_command() {
    let enable = Cli::parse_from([
        "palyra",
        "skills",
        "enable",
        "acme.echo_http",
        "--version",
        "1.2.3",
        "--override",
        "--reason",
        "operator re-enabled after review",
        "--json",
    ]);
    assert_eq!(
        enable.command,
        Command::Skills {
            command: SkillsCommand::Enable {
                skill_id: "acme.echo_http".to_owned(),
                version: Some("1.2.3".to_owned()),
                skills_dir: None,
                override_enabled: true,
                reason: Some("operator re-enabled after review".to_owned()),
                url: None,
                token: None,
                principal: "user:local".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: None,
                json: true,
            },
        }
    );
}

#[test]
fn parse_plugins_list_command() {
    let plugins_list = Cli::parse_from([
        "palyra",
        "plugins",
        "list",
        "--plugin-id",
        "acme-agent",
        "--skill-id",
        "acme.echo_http",
        "--enabled-only",
        "--ready-only",
        "--json",
    ]);
    assert_eq!(
        plugins_list.command,
        Command::Plugins {
            command: PluginsCommand::List {
                plugin_id: Some("acme-agent".to_owned()),
                skill_id: Some("acme.echo_http".to_owned()),
                enabled_only: true,
                ready_only: true,
                json: true,
            },
        }
    );
}

#[test]
fn parse_plugins_install_legacy_flags_command() {
    run_cli_parse_test_with_large_stack(|| {
        let plugin_install = Cli::parse_from([
            "palyra",
            "plugins",
            "install",
            "acme-agent",
            "--artifact-path",
            "dist/acme.echo_http.palyra-skill",
            "--tool-id",
            "acme.echo",
            "--module-path",
            "modules/echo.wasm",
            "--entrypoint",
            "run",
            "--cap-http-host",
            "api.example.com",
            "--cap-secret",
            "skill:acme.echo_http/api_token",
            "--cap-storage-prefix",
            "skills/cache",
            "--cap-channel",
            "discord",
            "--display-name",
            "Acme agent",
            "--notes",
            "prod plugin",
            "--owner-principal",
            "user:ops",
            "--tag",
            "prod",
            "--allow-tofu",
            "--allow-untrusted",
            "--json",
        ]);
        assert_eq!(
            plugin_install.command,
            Command::Plugins {
                command: PluginsCommand::Install {
                    plugin_id: "acme-agent".to_owned(),
                    skill_id: None,
                    skill_version: None,
                    artifact_path: Some("dist/acme.echo_http.palyra-skill".to_owned()),
                    tool_id: Some("acme.echo".to_owned()),
                    module_path: Some("modules/echo.wasm".to_owned()),
                    entrypoint: Some("run".to_owned()),
                    capability_http_hosts: vec!["api.example.com".to_owned()],
                    capability_secrets: vec!["skill:acme.echo_http/api_token".to_owned()],
                    capability_storage_prefixes: vec!["skills/cache".to_owned()],
                    capability_channels: vec!["discord".to_owned()],
                    display_name: Some("Acme agent".to_owned()),
                    notes: Some("prod plugin".to_owned()),
                    owner_principal: Some("user:ops".to_owned()),
                    tags: vec!["prod".to_owned()],
                    config_json: None,
                    clear_config: false,
                    disabled: false,
                    allow_tofu: true,
                    allow_untrusted: true,
                    json: true,
                },
            }
        );
    });
}

#[test]
fn parse_hooks_bind_command() {
    let hook_bind = Cli::parse_from([
        "palyra",
        "hooks",
        "bind",
        "acme-startup",
        "--event",
        "gateway:startup",
        "--plugin-id",
        "acme-agent",
        "--display-name",
        "Startup hook",
        "--notes",
        "boot automation",
        "--owner-principal",
        "user:ops",
        "--json",
    ]);
    assert_eq!(
        hook_bind.command,
        Command::Hooks {
            command: HooksCommand::Bind {
                hook_id: "acme-startup".to_owned(),
                event: "gateway:startup".to_owned(),
                plugin_id: "acme-agent".to_owned(),
                display_name: Some("Startup hook".to_owned()),
                notes: Some("boot automation".to_owned()),
                owner_principal: Some("user:ops".to_owned()),
                disabled: false,
                json: true,
            },
        }
    );
}

#[test]
fn parse_plugins_install_command() {
    run_cli_parse_test_with_large_stack(|| {
        let install = Cli::parse_from([
            "palyra",
            "plugins",
            "install",
            "acme.echo_http_plugin",
            "--artifact",
            "dist/acme.echo_http.palyra-skill",
            "--skill-id",
            "acme.echo_http",
            "--skill-version",
            "1.2.3",
            "--tool-id",
            "acme.echo_http",
            "--module-path",
            "modules/plugin.wasm",
            "--entrypoint",
            "run",
            "--cap-http-host",
            "api.example.com",
            "--cap-secret",
            "global/openai_api_key",
            "--cap-storage-prefix",
            "plugins/cache",
            "--cap-channel",
            "cli",
            "--display-name",
            "Echo HTTP",
            "--notes",
            "ops managed",
            "--owner-principal",
            "user:ops",
            "--tag",
            "prod",
            "--config-json",
            "{\"api_token\":\"vault:global/openai\"}",
            "--disabled",
            "--allow-untrusted",
            "--json",
        ]);
        match install.command {
            Command::Plugins {
                command:
                    PluginsCommand::Install {
                        plugin_id,
                        skill_id,
                        skill_version,
                        artifact_path,
                        tool_id,
                        module_path,
                        entrypoint,
                        capability_http_hosts,
                        capability_secrets,
                        capability_storage_prefixes,
                        capability_channels,
                        display_name,
                        notes,
                        owner_principal,
                        tags,
                        config_json,
                        clear_config,
                        disabled,
                        allow_tofu,
                        allow_untrusted,
                        json,
                    },
            } => {
                assert_eq!(plugin_id, "acme.echo_http_plugin");
                assert_eq!(skill_id.as_deref(), Some("acme.echo_http"));
                assert_eq!(skill_version.as_deref(), Some("1.2.3"));
                assert_eq!(artifact_path.as_deref(), Some("dist/acme.echo_http.palyra-skill"));
                assert_eq!(tool_id.as_deref(), Some("acme.echo_http"));
                assert_eq!(module_path.as_deref(), Some("modules/plugin.wasm"));
                assert_eq!(entrypoint.as_deref(), Some("run"));
                assert_eq!(capability_http_hosts, vec!["api.example.com".to_owned()]);
                assert_eq!(capability_secrets, vec!["global/openai_api_key".to_owned()]);
                assert_eq!(capability_storage_prefixes, vec!["plugins/cache".to_owned()]);
                assert_eq!(capability_channels, vec!["cli".to_owned()]);
                assert_eq!(display_name.as_deref(), Some("Echo HTTP"));
                assert_eq!(notes.as_deref(), Some("ops managed"));
                assert_eq!(owner_principal.as_deref(), Some("user:ops"));
                assert_eq!(tags, vec!["prod".to_owned()]);
                assert_eq!(config_json.as_deref(), Some("{\"api_token\":\"vault:global/openai\"}"));
                assert!(!clear_config);
                assert!(disabled);
                assert!(!allow_tofu);
                assert!(allow_untrusted);
                assert!(json);
            }
            other => panic!("unexpected install parse result: {other:?}"),
        }
    });
}

#[test]
fn parse_plugins_discover_command() {
    let discover = Cli::parse_from([
        "palyra",
        "plugins",
        "discover",
        "--skill-id",
        "acme.echo_http",
        "--ready-only",
        "--json",
    ]);
    match discover.command {
        Command::Plugins {
            command:
                PluginsCommand::Discover { plugin_id, skill_id, enabled_only, ready_only, json },
        } => {
            assert_eq!(plugin_id, None);
            assert_eq!(skill_id.as_deref(), Some("acme.echo_http"));
            assert!(!enabled_only);
            assert!(ready_only);
            assert!(json);
        }
        other => panic!("unexpected discover parse result: {other:?}"),
    }
}

#[test]
fn parse_plugins_explain_command() {
    let explain = Cli::parse_from(["palyra", "plugins", "explain", "acme.echo_http_plugin"]);
    match explain.command {
        Command::Plugins { command: PluginsCommand::Explain { plugin_id, json } } => {
            assert_eq!(plugin_id, "acme.echo_http_plugin");
            assert!(!json);
        }
        other => panic!("unexpected explain parse result: {other:?}"),
    }
}

#[test]
fn parse_plugins_doctor_command() {
    let doctor = Cli::parse_from([
        "palyra",
        "plugins",
        "doctor",
        "--plugin-id",
        "acme.echo_http_plugin",
        "--json",
    ]);
    match doctor.command {
        Command::Plugins { command: PluginsCommand::Doctor { plugin_id, json } } => {
            assert_eq!(plugin_id.as_deref(), Some("acme.echo_http_plugin"));
            assert!(json);
        }
        other => panic!("unexpected doctor parse result: {other:?}"),
    }
}

#[test]
fn parse_plugins_update_command() {
    run_cli_parse_test_with_large_stack(|| {
        let update = Cli::parse_from([
            "palyra",
            "plugins",
            "update",
            "acme.echo_http_plugin",
            "--skill-id",
            "acme.echo_http",
            "--clear-config",
        ]);
        match update.command {
            Command::Plugins {
                command:
                    PluginsCommand::Update {
                        plugin_id,
                        skill_id,
                        skill_version,
                        artifact_path,
                        tool_id,
                        module_path,
                        entrypoint,
                        capability_http_hosts,
                        capability_secrets,
                        capability_storage_prefixes,
                        capability_channels,
                        display_name,
                        notes,
                        owner_principal,
                        tags,
                        config_json,
                        clear_config,
                        disabled,
                        allow_tofu,
                        allow_untrusted,
                        json,
                    },
            } => {
                assert_eq!(plugin_id, "acme.echo_http_plugin");
                assert_eq!(skill_id.as_deref(), Some("acme.echo_http"));
                assert_eq!(skill_version, None);
                assert_eq!(artifact_path, None);
                assert_eq!(tool_id, None);
                assert_eq!(module_path, None);
                assert_eq!(entrypoint, None);
                assert!(capability_http_hosts.is_empty());
                assert!(capability_secrets.is_empty());
                assert!(capability_storage_prefixes.is_empty());
                assert!(capability_channels.is_empty());
                assert_eq!(display_name, None);
                assert_eq!(notes, None);
                assert_eq!(owner_principal, None);
                assert!(tags.is_empty());
                assert_eq!(config_json, None);
                assert!(clear_config);
                assert!(!disabled);
                assert!(!allow_tofu);
                assert!(!allow_untrusted);
                assert!(!json);
            }
            other => panic!("unexpected update parse result: {other:?}"),
        }
    });
}

#[test]
fn parse_hooks_bind_with_disabled_flag() {
    let bind = Cli::parse_from([
        "palyra",
        "hooks",
        "bind",
        "ops.skill_enabled",
        "--event",
        "skill:enabled",
        "--plugin-id",
        "acme.echo_http_plugin",
        "--display-name",
        "Skill Enabled Hook",
        "--notes",
        "dispatch after enabling",
        "--owner-principal",
        "user:ops",
        "--disabled",
        "--json",
    ]);
    assert_eq!(
        bind.command,
        Command::Hooks {
            command: HooksCommand::Bind {
                hook_id: "ops.skill_enabled".to_owned(),
                event: "skill:enabled".to_owned(),
                plugin_id: "acme.echo_http_plugin".to_owned(),
                display_name: Some("Skill Enabled Hook".to_owned()),
                notes: Some("dispatch after enabling".to_owned()),
                owner_principal: Some("user:ops".to_owned()),
                disabled: true,
                json: true,
            },
        }
    );
}

#[test]
fn parse_hooks_info_command() {
    let info = Cli::parse_from(["palyra", "hooks", "info", "ops.skill_enabled"]);
    assert_eq!(
        info.command,
        Command::Hooks {
            command: HooksCommand::Info { hook_id: "ops.skill_enabled".to_owned(), json: false },
        }
    );
}

#[test]
fn parse_hooks_check_command() {
    let check = Cli::parse_from(["palyra", "hooks", "check", "ops.skill_enabled"]);
    assert_eq!(
        check.command,
        Command::Hooks {
            command: HooksCommand::Check { hook_id: "ops.skill_enabled".to_owned(), json: false },
        }
    );
}
