use clap::Parser;

use super::{
    AcpBridgeArgs, AcpCommand, AcpConnectionArgs, AcpSessionDefaultsArgs, AcpShimArgs,
    AcpSubcommand, AgentCommand, AgentsCommand, ApprovalDecisionArg, ApprovalDecisionScopeArg,
    ApprovalExportFormatArg, ApprovalResolveDecisionArg, ApprovalSubjectTypeArg, ApprovalsCommand,
    AuthAccessCommand, AuthCommand, AuthCredentialArg, AuthOpenAiCommand, AuthProfilesCommand,
    AuthProviderArg, AuthScopeArg, BackupCommand, BackupComponentArg, BrowserCommand,
    BrowserPermissionsCommand, BrowserSessionCommand, ChannelProviderArg, ChannelResolveEntityArg,
    ChannelsCommand, ChannelsDiscordCommand, ChannelsRouterCommand, Cli, Command, CompletionShell,
    ConfigCommand, ConfigureSectionArg, CronCommand, CronConcurrencyPolicyArg,
    CronMisfirePolicyArg, CronScheduleTypeArg, DaemonCommand, DevicesCommand, DocsCommand,
    FlowStateArg, FlowsCommand, GatewayBindProfileArg, HooksCommand, InitModeArg,
    InitTlsScaffoldArg, JournalCheckpointModeArg, MemoryCommand, MemoryLearningCommand,
    MemoryScopeArg, MemorySourceArg, MessageCommand, ModelsCommand, NodeCommand, NodesCommand,
    ObjectiveKindArg, ObjectivePriorityArg, ObjectiveScheduleTypeArg, ObjectiveUpsertCommandArgs,
    ObjectivesCommand, OnboardingAuthMethodArg, OnboardingCommand, OnboardingFlowArg,
    PairingClientKindArg, PairingCommand, PairingMethodArg, PairingStateArg, PatchCommand,
    PluginsCommand, PolicyCommand, ProfileCommand, ProfileExportModeArg, ProfileModeArg,
    ProfileRiskLevelArg, ProtocolCommand, RemoteVerificationModeArg, ResetCommand, ResetScopeArg,
    RoutineApprovalModeArg, RoutineDeliveryModeArg, RoutineExecutionPostureArg,
    RoutinePreviewTimezoneArg, RoutineRunModeArg, RoutineSilentPolicyArg, RoutineTriggerKindArg,
    RoutineUpsertCommand, RoutinesCommand, SandboxCommand, SandboxRuntimeArg, SecretsCommand,
    SecretsConfigureCommand, SecurityCommand, SessionsCommand, SetupWizardOverridesArg,
    SkillsCommand, SkillsPackageCommand, SupportBundleCommand, SystemCommand, SystemEventCommand,
    SystemEventSeverityArg, TuiCommand, UninstallCommand, UpdateCommand, WebhooksCommand,
    WizardOverridesArg, WorkspaceRoleArg,
};

mod parser_stability_plugin_tests;
mod parser_stability_tests;

#[test]
fn parse_version_subcommand() {
    let parsed = Cli::parse_from(["palyra", "version"]);
    assert_eq!(parsed.command, Command::Version);
}

#[test]
fn parse_gateway_status_with_json_flag() {
    let parsed = Cli::parse_from(["palyra", "gateway", "status", "--json"]);
    assert_eq!(
        parsed.command,
        Command::Gateway { command: DaemonCommand::Status { url: None, json: true } }
    );
}

#[test]
fn parse_profile_create_with_guided_defaults() {
    let parsed = Cli::parse_from([
        "palyra",
        "profile",
        "create",
        "staging",
        "--mode",
        "remote",
        "--label",
        "Staging cluster",
        "--environment",
        "staging",
        "--color",
        "amber",
        "--risk-level",
        "high",
        "--strict-mode",
        "--daemon-url",
        "https://gateway.example.com",
        "--grpc-url",
        "https://grpc.example.com",
        "--admin-token-env",
        "PALYRA_STAGING_ADMIN_TOKEN",
        "--principal",
        "admin:staging",
        "--device-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        "--channel",
        "ops",
        "--set-default",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Profile {
            command: ProfileCommand::Create {
                name: "staging".to_owned(),
                mode: ProfileModeArg::Remote,
                label: Some("Staging cluster".to_owned()),
                environment: Some("staging".to_owned()),
                color: Some("amber".to_owned()),
                risk_level: Some(ProfileRiskLevelArg::High),
                strict_mode: true,
                config_path: None,
                state_root: None,
                daemon_url: Some("https://gateway.example.com".to_owned()),
                grpc_url: Some("https://grpc.example.com".to_owned()),
                admin_token_env: Some("PALYRA_STAGING_ADMIN_TOKEN".to_owned()),
                principal: Some("admin:staging".to_owned()),
                device_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned()),
                channel: Some("ops".to_owned()),
                set_default: true,
                force: false,
                json: true,
            }
        }
    );
}

#[test]
fn parse_init_remote_with_overrides() {
    let parsed = Cli::parse_from([
        "palyra",
        "init",
        "--mode",
        "remote",
        "--path",
        "config/palyra.toml",
        "--force",
        "--tls-scaffold",
        "self-signed",
    ]);
    assert_eq!(
        parsed.command,
        Command::Setup {
            mode: InitModeArg::Remote,
            path: Some("config/palyra.toml".to_owned()),
            force: true,
            tls_scaffold: InitTlsScaffoldArg::SelfSigned,
            wizard: false,
            wizard_options: SetupWizardOverridesArg {
                flow: None,
                non_interactive: false,
                accept_risk: false,
                json: false,
                workspace_root: None,
                auth_method: None,
                api_key_env: None,
                api_key_stdin: false,
                api_key_prompt: false,
                deployment_profile: None,
                bind_profile: None,
                daemon_port: None,
                grpc_port: None,
                quic_port: None,
                tls_cert_path: None,
                tls_key_path: None,
                remote_base_url: None,
                admin_token_env: None,
                admin_token_stdin: false,
                admin_token_prompt: false,
                remote_verification: None,
                pinned_server_cert_sha256: None,
                pinned_gateway_ca_sha256: None,
                ssh_target: None,
                skip_health: false,
                skip_channels: false,
                skip_skills: false,
            },
        }
    );
}

#[test]
fn parse_doctor_strict() {
    let parsed = Cli::parse_from(["palyra", "doctor", "--strict"]);
    assert_eq!(
        parsed.command,
        Command::Doctor {
            strict: true,
            json: false,
            repair: false,
            dry_run: false,
            force: false,
            only: Vec::new(),
            skip: Vec::new(),
            rollback_run: None,
        }
    );
}

#[test]
fn parse_doctor_json() {
    let parsed = Cli::parse_from(["palyra", "doctor", "--json"]);
    assert_eq!(
        parsed.command,
        Command::Doctor {
            strict: false,
            json: true,
            repair: false,
            dry_run: false,
            force: false,
            only: Vec::new(),
            skip: Vec::new(),
            rollback_run: None,
        }
    );
}

#[test]
fn parse_doctor_repair_preview_filters() {
    let parsed = Cli::parse_from([
        "palyra",
        "doctor",
        "--repair",
        "--dry-run",
        "--force",
        "--only",
        "config.schema_version",
        "--skip",
        "stale_runtime.cleanup",
        "--rollback-run",
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
    ]);
    assert_eq!(
        parsed.command,
        Command::Doctor {
            strict: false,
            json: false,
            repair: true,
            dry_run: true,
            force: true,
            only: vec!["config.schema_version".to_owned()],
            skip: vec!["stale_runtime.cleanup".to_owned()],
            rollback_run: Some("01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned()),
        }
    );
}

#[test]
fn parse_health_with_explicit_endpoints() {
    let parsed = Cli::parse_from([
        "palyra",
        "health",
        "--url",
        "http://127.0.0.1:7142",
        "--grpc-url",
        "http://127.0.0.1:7443",
    ]);
    assert_eq!(
        parsed.command,
        Command::Health {
            url: Some("http://127.0.0.1:7142".to_owned()),
            grpc_url: Some("http://127.0.0.1:7443".to_owned()),
        }
    );
}

#[test]
fn parse_logs_with_follow() {
    let parsed = Cli::parse_from([
        "palyra",
        "logs",
        "--db-path",
        "data/journal.sqlite3",
        "--lines",
        "100",
        "--follow",
        "--poll-interval-ms",
        "2500",
    ]);
    assert_eq!(
        parsed.command,
        Command::Logs {
            db_path: Some("data/journal.sqlite3".to_owned()),
            lines: 100,
            follow: true,
            poll_interval_ms: 2500,
        }
    );
}

#[test]
fn parse_backup_create_with_explicit_components() {
    let parsed = Cli::parse_from([
        "palyra",
        "backup",
        "create",
        "--output",
        "artifacts/palyra-backup.zip",
        "--config-path",
        "config/palyra.toml",
        "--state-root",
        "state",
        "--workspace-root",
        "workspace",
        "--include",
        "workspace",
        "--include",
        "support-bundle",
        "--include-workspace",
        "--include-support-bundle",
        "--force",
    ]);
    assert_eq!(
        parsed.command,
        Command::Backup {
            command: BackupCommand::Create {
                output: Some("artifacts/palyra-backup.zip".to_owned()),
                config_path: Some("config/palyra.toml".to_owned()),
                state_root: Some("state".to_owned()),
                workspace_root: Some("workspace".to_owned()),
                include: vec![BackupComponentArg::Workspace, BackupComponentArg::SupportBundle],
                include_workspace: true,
                include_support_bundle: true,
                force: true,
            }
        }
    );
}

#[test]
fn parse_backup_verify() {
    let parsed =
        Cli::parse_from(["palyra", "backup", "verify", "--archive", "artifacts/palyra-backup.zip"]);
    assert_eq!(
        parsed.command,
        Command::Backup {
            command: BackupCommand::Verify { archive: "artifacts/palyra-backup.zip".to_owned() }
        }
    );
}

#[test]
fn parse_reset_with_repeated_scope() {
    let parsed = Cli::parse_from([
        "palyra",
        "reset",
        "--scope",
        "state",
        "--scope",
        "service",
        "--workspace-root",
        "workspace",
        "--dry-run",
    ]);
    assert_eq!(
        parsed.command,
        Command::Reset {
            command: ResetCommand {
                scopes: vec![ResetScopeArg::State, ResetScopeArg::Service],
                config_path: None,
                workspace_root: Some("workspace".to_owned()),
                dry_run: true,
                yes: false,
            }
        }
    );
}

#[test]
fn parse_uninstall_with_remove_state() {
    let parsed = Cli::parse_from([
        "palyra",
        "uninstall",
        "--install-root",
        "install",
        "--remove-state",
        "--yes",
    ]);
    assert_eq!(
        parsed.command,
        Command::Uninstall {
            command: UninstallCommand {
                install_root: Some("install".to_owned()),
                remove_state: true,
                yes: true,
                dry_run: false,
            }
        }
    );
}

#[test]
fn parse_update_check_with_archive_hint() {
    let parsed = Cli::parse_from([
        "palyra",
        "update",
        "--install-root",
        "install",
        "--archive",
        "artifacts/palyra-headless.zip",
        "--check",
        "--skip-service-restart",
    ]);
    assert_eq!(
        parsed.command,
        Command::Update {
            command: UpdateCommand {
                install_root: Some("install".to_owned()),
                archive: Some("artifacts/palyra-headless.zip".to_owned()),
                check: true,
                dry_run: false,
                yes: false,
                skip_service_restart: true,
            }
        }
    );
}

#[test]
fn parse_status_with_admin_context() {
    let parsed = Cli::parse_from([
        "palyra",
        "status",
        "--url",
        "http://127.0.0.1:7142",
        "--grpc-url",
        "http://127.0.0.1:7443",
        "--admin",
        "--token",
        "test-token",
        "--principal",
        "user:ops",
        "--device-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--channel",
        "cli",
    ]);
    assert_eq!(
        parsed.command,
        Command::Status {
            url: Some("http://127.0.0.1:7142".to_owned()),
            grpc_url: Some("http://127.0.0.1:7443".to_owned()),
            admin: true,
            token: Some("test-token".to_owned()),
            principal: Some("user:ops".to_owned()),
            device_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
            channel: Some("cli".to_owned()),
        }
    );
}

#[test]
fn parse_agent_run_with_prompt() {
    let parsed = Cli::parse_from([
        "palyra",
        "agent",
        "run",
        "--grpc-url",
        "http://127.0.0.1:7443",
        "--token",
        "test-token",
        "--session-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAW",
        "--run-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAX",
        "--prompt",
        "hello",
        "--allow-sensitive-tools",
        "--ndjson",
    ]);
    assert_eq!(
        parsed.command,
        Command::Agent {
            command: AgentCommand::Run {
                grpc_url: Some("http://127.0.0.1:7443".to_owned()),
                token: Some("test-token".to_owned()),
                principal: None,
                device_id: None,
                channel: None,
                session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned()),
                session_key: None,
                session_label: None,
                require_existing: false,
                reset_session: false,
                run_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned()),
                prompt: Some("hello".to_owned()),
                prompt_stdin: false,
                allow_sensitive_tools: true,
                ndjson: true,
            }
        }
    );
}

#[test]
fn parse_agent_interactive_with_defaults() {
    let parsed = Cli::parse_from(["palyra", "agent", "interactive"]);
    assert_eq!(
        parsed.command,
        Command::Agent {
            command: AgentCommand::Interactive {
                grpc_url: None,
                token: None,
                principal: None,
                device_id: None,
                channel: None,
                session_id: None,
                session_key: None,
                session_label: None,
                require_existing: false,
                allow_sensitive_tools: false,
                ndjson: false,
            }
        }
    );
}

#[test]
fn parse_agent_acp_shim_from_ndjson_stdin() {
    let parsed = Cli::parse_from([
        "palyra",
        "agent",
        "acp-shim",
        "--grpc-url",
        "http://127.0.0.1:7443",
        "--ndjson-stdin",
    ]);
    assert_eq!(
        parsed.command,
        Command::Agent {
            command: AgentCommand::AcpShim {
                command: AcpShimArgs {
                    connection: AcpConnectionArgs {
                        grpc_url: Some("http://127.0.0.1:7443".to_owned()),
                        token: None,
                        principal: None,
                        device_id: None,
                        channel: None,
                    },
                    session_id: None,
                    session_defaults: AcpSessionDefaultsArgs::default(),
                    run_id: None,
                    prompt: None,
                    prompt_stdin: false,
                    allow_sensitive_tools: false,
                    ndjson_stdin: true,
                }
            }
        }
    );
}

#[test]
fn parse_agent_run_with_session_key_controls() {
    let parsed = Cli::parse_from([
        "palyra",
        "agent",
        "run",
        "--session-key",
        "ops:triage",
        "--session-label",
        "Ops Triage",
        "--require-existing",
        "--reset-session",
        "--prompt",
        "continue",
    ]);
    assert_eq!(
        parsed.command,
        Command::Agent {
            command: AgentCommand::Run {
                grpc_url: None,
                token: None,
                principal: None,
                device_id: None,
                channel: None,
                session_id: None,
                session_key: Some("ops:triage".to_owned()),
                session_label: Some("Ops Triage".to_owned()),
                require_existing: true,
                reset_session: true,
                run_id: None,
                prompt: Some("continue".to_owned()),
                prompt_stdin: false,
                allow_sensitive_tools: false,
                ndjson: false,
            }
        }
    );
}

#[test]
fn parse_agent_interactive_with_session_key() {
    let parsed = Cli::parse_from([
        "palyra",
        "agent",
        "interactive",
        "--session-key",
        "ops:triage",
        "--session-label",
        "Ops Triage",
        "--require-existing",
    ]);
    assert_eq!(
        parsed.command,
        Command::Agent {
            command: AgentCommand::Interactive {
                grpc_url: None,
                token: None,
                principal: None,
                device_id: None,
                channel: None,
                session_id: None,
                session_key: Some("ops:triage".to_owned()),
                session_label: Some("Ops Triage".to_owned()),
                require_existing: true,
                allow_sensitive_tools: false,
                ndjson: false,
            }
        }
    );
}

#[test]
fn parse_agent_acp_with_defaults() {
    let parsed = Cli::parse_from(["palyra", "agent", "acp"]);
    assert_eq!(
        parsed.command,
        Command::Agent {
            command: AgentCommand::Acp {
                command: AcpBridgeArgs {
                    connection: AcpConnectionArgs::default(),
                    session_defaults: AcpSessionDefaultsArgs::default(),
                    allow_sensitive_tools: false,
                }
            }
        }
    );
}

#[test]
fn parse_top_level_acp_with_session_defaults() {
    let parsed = Cli::parse_from([
        "palyra",
        "acp",
        "--session-key",
        "ops:triage",
        "--session-label",
        "Ops Triage",
        "--require-existing",
        "--reset-session",
        "--allow-sensitive-tools",
    ]);
    assert_eq!(
        parsed.command,
        Command::Acp {
            command: AcpCommand {
                bridge: AcpBridgeArgs {
                    connection: AcpConnectionArgs::default(),
                    session_defaults: AcpSessionDefaultsArgs {
                        session_key: Some("ops:triage".to_owned()),
                        session_label: Some("Ops Triage".to_owned()),
                        require_existing: true,
                        reset_session: true,
                    },
                    allow_sensitive_tools: true,
                },
                subcommand: None,
            }
        }
    );
}

#[test]
fn parse_top_level_acp_shim_from_ndjson_stdin() {
    let parsed = Cli::parse_from([
        "palyra",
        "acp",
        "shim",
        "--grpc-url",
        "http://127.0.0.1:7443",
        "--ndjson-stdin",
    ]);
    assert_eq!(
        parsed.command,
        Command::Acp {
            command: AcpCommand {
                bridge: AcpBridgeArgs::default(),
                subcommand: Some(AcpSubcommand::Shim {
                    command: AcpShimArgs {
                        connection: AcpConnectionArgs {
                            grpc_url: Some("http://127.0.0.1:7443".to_owned()),
                            token: None,
                            principal: None,
                            device_id: None,
                            channel: None,
                        },
                        session_id: None,
                        session_defaults: AcpSessionDefaultsArgs::default(),
                        run_id: None,
                        prompt: None,
                        prompt_stdin: false,
                        allow_sensitive_tools: false,
                        ndjson_stdin: true,
                    }
                }),
            }
        }
    );
}

#[test]
fn parse_docs_search_and_show() {
    let parsed_search = Cli::parse_from(["palyra", "docs", "search", "gateway", "--limit", "5"]);
    assert_eq!(
        parsed_search.command,
        Command::Docs {
            command: DocsCommand::Search { query: "gateway".to_owned(), limit: 5, json: false }
        }
    );

    let parsed_show = Cli::parse_from(["palyra", "docs", "show", "help/docs-help", "--json"]);
    assert_eq!(
        parsed_show.command,
        Command::Docs {
            command: DocsCommand::Show { slug_or_path: "help/docs-help".to_owned(), json: true }
        }
    );
}

#[test]
fn parse_agent_acp_shim_ndjson_stdin_conflicts_with_prompt() {
    let result =
        Cli::try_parse_from(["palyra", "agent", "acp-shim", "--ndjson-stdin", "--prompt", "hello"]);
    assert!(result.is_err(), "--ndjson-stdin must conflict with --prompt");
}

#[test]
fn parse_agent_acp_shim_ndjson_stdin_conflicts_with_prompt_stdin() {
    let result =
        Cli::try_parse_from(["palyra", "agent", "acp-shim", "--ndjson-stdin", "--prompt-stdin"]);
    assert!(result.is_err(), "--ndjson-stdin must conflict with --prompt-stdin");
}

#[test]
fn parse_agent_acp_shim_ndjson_stdin_conflicts_with_session_and_run_ids() {
    let with_session = Cli::try_parse_from([
        "palyra",
        "agent",
        "acp-shim",
        "--ndjson-stdin",
        "--session-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAW",
    ]);
    assert!(with_session.is_err(), "--ndjson-stdin must conflict with --session-id");

    let with_run = Cli::try_parse_from([
        "palyra",
        "agent",
        "acp-shim",
        "--ndjson-stdin",
        "--run-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAX",
    ]);
    assert!(with_run.is_err(), "--ndjson-stdin must conflict with --run-id");
}

#[test]
fn parse_agents_list_json() {
    let parsed =
        Cli::parse_from(["palyra", "agents", "list", "--after", "main", "--limit", "25", "--json"]);
    assert_eq!(
        parsed.command,
        Command::Agents {
            command: AgentsCommand::List {
                after: Some("main".to_owned()),
                limit: Some(25),
                json: true,
                ndjson: false,
            }
        }
    );
}

#[test]
fn parse_agents_create_with_workspace_roots() {
    let parsed = Cli::parse_from([
        "palyra",
        "agents",
        "create",
        "reviewer",
        "--display-name",
        "Code Reviewer",
        "--workspace-root",
        "workspace",
        "--workspace-root",
        "scratch",
        "--model-profile",
        "gpt-4o-mini",
        "--tool-allow",
        "palyra.echo",
        "--skill-allow",
        "acme.review",
        "--set-default",
        "--allow-absolute-paths",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Agents {
            command: AgentsCommand::Create {
                agent_id: "reviewer".to_owned(),
                display_name: "Code Reviewer".to_owned(),
                agent_dir: None,
                workspace_root: vec!["workspace".to_owned(), "scratch".to_owned()],
                model_profile: Some("gpt-4o-mini".to_owned()),
                execution_backend: None,
                tool_allow: vec!["palyra.echo".to_owned()],
                skill_allow: vec!["acme.review".to_owned()],
                set_default: true,
                allow_absolute_paths: true,
                json: true,
            }
        }
    );
}

#[test]
fn parse_agents_list_ndjson_conflicts_with_json() {
    let result = Cli::try_parse_from(["palyra", "agents", "list", "--json", "--ndjson"]);
    assert!(result.is_err(), "--json and --ndjson must be mutually exclusive for agents list");
}

#[test]
fn parse_sessions_list_ndjson() {
    let parsed = Cli::parse_from([
        "palyra",
        "sessions",
        "list",
        "--after",
        "principal=user:ops|device=01ARZ3NDEKTSV4RRFFQ69G5FAV|channel=cli|session=main",
        "--limit",
        "20",
        "--ndjson",
    ]);
    assert_eq!(
        parsed.command,
        Command::Sessions {
            command: SessionsCommand::List {
                after: Some(
                    "principal=user:ops|device=01ARZ3NDEKTSV4RRFFQ69G5FAV|channel=cli|session=main"
                        .to_owned(),
                ),
                limit: Some(20),
                include_archived: false,
                json: false,
                ndjson: true,
            }
        }
    );
}

#[test]
fn parse_sessions_resolve_with_label_and_reset() {
    let parsed = Cli::parse_from([
        "palyra",
        "sessions",
        "resolve",
        "--session-key",
        "ops:triage",
        "--session-label",
        "Ops Triage",
        "--reset",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Sessions {
            command: SessionsCommand::Resolve {
                session_id: None,
                session_key: Some("ops:triage".to_owned()),
                session_label: Some("Ops Triage".to_owned()),
                require_existing: false,
                reset: true,
                json: true,
            }
        }
    );
}

#[test]
fn parse_sessions_rename_and_abort() {
    let rename = Cli::parse_from([
        "palyra",
        "sessions",
        "rename",
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        "--session-label",
        "Primary session",
    ]);
    assert_eq!(
        rename.command,
        Command::Sessions {
            command: SessionsCommand::Rename {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned(),
                session_label: "Primary session".to_owned(),
                json: false,
            }
        }
    );

    let abort = Cli::parse_from([
        "palyra",
        "sessions",
        "abort",
        "01ARZ3NDEKTSV4RRFFQ69G5FB1",
        "--reason",
        "operator requested stop",
        "--json",
    ]);
    assert_eq!(
        abort.command,
        Command::Sessions {
            command: SessionsCommand::Abort {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FB1".to_owned(),
                reason: Some("operator requested stop".to_owned()),
                json: true,
            }
        }
    );
}

#[test]
fn parse_sessions_queue_controls() {
    let policy = Cli::parse_from([
        "palyra",
        "sessions",
        "queue-policy",
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        "--json",
    ]);
    assert_eq!(
        policy.command,
        Command::Sessions {
            command: SessionsCommand::QueuePolicy {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned(),
                json: true,
            }
        }
    );

    let pause = Cli::parse_from([
        "palyra",
        "sessions",
        "queue-pause",
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        "--reason",
        "operator handoff",
    ]);
    assert_eq!(
        pause.command,
        Command::Sessions {
            command: SessionsCommand::QueuePause {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned(),
                reason: Some("operator handoff".to_owned()),
                json: false,
            }
        }
    );

    let cancel = Cli::parse_from([
        "palyra",
        "sessions",
        "queue-cancel",
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        "01ARZ3NDEKTSV4RRFFQ69G5FB1",
        "--json",
    ]);
    assert_eq!(
        cancel.command,
        Command::Sessions {
            command: SessionsCommand::QueueCancel {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned(),
                queued_input_id: "01ARZ3NDEKTSV4RRFFQ69G5FB1".to_owned(),
                reason: None,
                json: true,
            }
        }
    );
}

#[test]
fn parse_sessions_history_with_resume_first() {
    let parsed = Cli::parse_from([
        "palyra",
        "sessions",
        "history",
        "--query",
        "ops status",
        "--include-archived",
        "--resume-first",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Sessions {
            command: SessionsCommand::History {
                query: Some("ops status".to_owned()),
                limit: None,
                include_archived: true,
                resume_first: true,
                json: true,
                ndjson: false,
            }
        }
    );
}

#[test]
fn parse_sessions_cleanup_with_dry_run() {
    let parsed = Cli::parse_from([
        "palyra",
        "sessions",
        "cleanup",
        "--session-key",
        "ops:triage",
        "--dry-run",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Sessions {
            command: SessionsCommand::Cleanup {
                session_id: None,
                session_key: Some("ops:triage".to_owned()),
                yes: false,
                dry_run: true,
                json: true,
            }
        }
    );
}

#[test]
fn parse_sessions_retry_branch_search_and_export() {
    let retry =
        Cli::parse_from(["palyra", "sessions", "retry", "01ARZ3NDEKTSV4RRFFQ69G5FB2", "--json"]);
    assert_eq!(
        retry.command,
        Command::Sessions {
            command: SessionsCommand::Retry {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FB2".to_owned(),
                json: true,
            }
        }
    );

    let branch = Cli::parse_from([
        "palyra",
        "sessions",
        "branch",
        "01ARZ3NDEKTSV4RRFFQ69G5FB3",
        "--session-label",
        "Investigate alternate plan",
    ]);
    assert_eq!(
        branch.command,
        Command::Sessions {
            command: SessionsCommand::Branch {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FB3".to_owned(),
                session_label: Some("Investigate alternate plan".to_owned()),
                json: false,
            }
        }
    );

    let search = Cli::parse_from([
        "palyra",
        "sessions",
        "transcript-search",
        "01ARZ3NDEKTSV4RRFFQ69G5FB4",
        "--query",
        "follow-up",
    ]);
    assert_eq!(
        search.command,
        Command::Sessions {
            command: SessionsCommand::TranscriptSearch {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FB4".to_owned(),
                query: "follow-up".to_owned(),
                json: false,
            }
        }
    );

    let export = Cli::parse_from([
        "palyra",
        "sessions",
        "export",
        "01ARZ3NDEKTSV4RRFFQ69G5FB5",
        "--format",
        "markdown",
        "--json",
    ]);
    assert_eq!(
        export.command,
        Command::Sessions {
            command: SessionsCommand::Export {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FB5".to_owned(),
                format: "markdown".to_owned(),
                json: true,
            }
        }
    );
}

#[test]
fn parse_message_send_with_thread_id() {
    let parsed = Cli::parse_from([
        "palyra",
        "message",
        "send",
        "discord:default",
        "--to",
        "123456789",
        "--text",
        "hello",
        "--confirm",
        "--thread-id",
        "thread-123",
        "--reply-to-message-id",
        "message-456",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Message {
            command: MessageCommand::Send {
                connector_id: "discord:default".to_owned(),
                to: "123456789".to_owned(),
                text: "hello".to_owned(),
                confirm: true,
                auto_reaction: None,
                thread_id: Some("thread-123".to_owned()),
                reply_to_message_id: Some("message-456".to_owned()),
                url: None,
                token: None,
                principal: "user:local".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: None,
                json: true,
            }
        }
    );
}

#[test]
fn parse_message_read_with_connection_overrides() {
    let parsed = Cli::parse_from([
        "palyra",
        "message",
        "read",
        "discord:default",
        "--conversation-id",
        "dm-ops",
        "--message-id",
        "msg-123",
        "--url",
        "http://127.0.0.1:7142",
        "--token",
        "secret-token",
        "--principal",
        "user:test",
        "--device-id",
        "01TESTDEVICE0000000000000000",
        "--channel",
        "discord",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Message {
            command: MessageCommand::Read {
                connector_id: "discord:default".to_owned(),
                conversation_id: "dm-ops".to_owned(),
                thread_id: None,
                message_id: Some("msg-123".to_owned()),
                before_message_id: None,
                after_message_id: None,
                around_message_id: None,
                limit: 25,
                url: Some("http://127.0.0.1:7142".to_owned()),
                token: Some("secret-token".to_owned()),
                principal: "user:test".to_owned(),
                device_id: "01TESTDEVICE0000000000000000".to_owned(),
                channel: Some("discord".to_owned()),
                json: true,
            }
        }
    );
}

#[test]
fn parse_tui_with_session_controls() {
    let parsed = Cli::parse_from([
        "palyra",
        "tui",
        "--grpc-url",
        "http://127.0.0.1:7443",
        "--token",
        "test-token",
        "--session-key",
        "ops:triage",
        "--session-label",
        "Ops Triage",
        "--require-existing",
        "--allow-sensitive-tools",
    ]);
    assert_eq!(
        parsed.command,
        Command::Tui {
            command: TuiCommand {
                grpc_url: Some("http://127.0.0.1:7443".to_owned()),
                token: Some("test-token".to_owned()),
                principal: None,
                device_id: None,
                channel: None,
                session_id: None,
                session_key: Some("ops:triage".to_owned()),
                session_label: Some("Ops Triage".to_owned()),
                require_existing: true,
                allow_sensitive_tools: true,
                include_archived_sessions: false,
            }
        }
    );
}

#[test]
fn parse_tui_with_archived_sessions_flag() {
    let parsed = Cli::parse_from(["palyra", "tui", "--include-archived-sessions"]);
    assert_eq!(
        parsed.command,
        Command::Tui {
            command: TuiCommand {
                grpc_url: None,
                token: None,
                principal: None,
                device_id: None,
                channel: None,
                session_id: None,
                session_key: None,
                session_label: None,
                require_existing: false,
                allow_sensitive_tools: false,
                include_archived_sessions: true,
            }
        }
    );
}

#[test]
fn parse_cron_add() {
    let parsed = Cli::parse_from([
        "palyra",
        "cron",
        "add",
        "--name",
        "Health summary",
        "--prompt",
        "Summarize status",
        "--schedule-type",
        "cron",
        "--schedule",
        "*/5 * * * *",
        "--enabled",
        "--concurrency",
        "forbid",
        "--retry-max-attempts",
        "3",
        "--retry-backoff-ms",
        "2000",
        "--misfire",
        "skip",
        "--jitter-ms",
        "150",
        "--owner",
        "user:ops",
        "--channel",
        "system:cron",
        "--session-key",
        "cron:health",
        "--session-label",
        "Health",
    ]);
    assert_eq!(
        parsed.command,
        Command::Cron {
            command: CronCommand::Add {
                name: "Health summary".to_owned(),
                prompt: "Summarize status".to_owned(),
                schedule_type: CronScheduleTypeArg::Cron,
                schedule: "*/5 * * * *".to_owned(),
                enabled: true,
                concurrency: CronConcurrencyPolicyArg::Forbid,
                retry_max_attempts: 3,
                retry_backoff_ms: 2000,
                misfire: CronMisfirePolicyArg::Skip,
                jitter_ms: 150,
                owner: Some("user:ops".to_owned()),
                channel: Some("system:cron".to_owned()),
                session_key: Some("cron:health".to_owned()),
                session_label: Some("Health".to_owned()),
                json: false,
            }
        }
    );
}

#[test]
fn parse_cron_update() {
    let parsed = Cli::parse_from([
        "palyra",
        "cron",
        "update",
        "--id",
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        "--name",
        "Health summary v2",
        "--schedule-type",
        "every",
        "--schedule",
        "60000",
        "--enabled",
        "true",
        "--concurrency",
        "replace",
        "--retry-max-attempts",
        "4",
        "--retry-backoff-ms",
        "500",
        "--misfire",
        "catch-up",
        "--jitter-ms",
        "50",
        "--owner",
        "user:ops",
        "--channel",
        "system:cron",
        "--session-key",
        "cron:health-v2",
        "--session-label",
        "Health summary v2",
    ]);
    assert_eq!(
        parsed.command,
        Command::Cron {
            command: CronCommand::Update {
                id: "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned(),
                name: Some("Health summary v2".to_owned()),
                prompt: None,
                schedule_type: Some(CronScheduleTypeArg::Every),
                schedule: Some("60000".to_owned()),
                enabled: Some(true),
                concurrency: Some(CronConcurrencyPolicyArg::Replace),
                retry_max_attempts: Some(4),
                retry_backoff_ms: Some(500),
                misfire: Some(CronMisfirePolicyArg::CatchUp),
                jitter_ms: Some(50),
                owner: Some("user:ops".to_owned()),
                channel: Some("system:cron".to_owned()),
                session_key: Some("cron:health-v2".to_owned()),
                session_label: Some("Health summary v2".to_owned()),
                json: false,
            }
        }
    );
}

#[test]
fn parse_cron_update_requires_schedule_pair() {
    let missing_schedule = Cli::try_parse_from([
        "palyra",
        "cron",
        "update",
        "--id",
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        "--schedule-type",
        "cron",
    ]);
    assert!(missing_schedule.is_err(), "--schedule-type requires --schedule");

    let missing_type = Cli::try_parse_from([
        "palyra",
        "cron",
        "update",
        "--id",
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        "--schedule",
        "*/5 * * * *",
    ]);
    assert!(missing_type.is_err(), "--schedule requires --schedule-type");
}

#[test]
fn parse_cron_delete() {
    let parsed = Cli::parse_from([
        "palyra",
        "cron",
        "delete",
        "--id",
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Cron {
            command: CronCommand::Delete {
                id: "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned(),
                json: true,
            }
        }
    );
}

#[test]
fn parse_cron_status() {
    let parsed = Cli::parse_from([
        "palyra",
        "cron",
        "status",
        "--limit",
        "10",
        "--enabled",
        "true",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Cron {
            command: CronCommand::Status {
                after: None,
                limit: Some(10),
                enabled: Some(true),
                owner: None,
                channel: None,
                json: true,
            }
        }
    );
}

#[test]
fn parse_cron_aliases() {
    let edit = Cli::parse_from([
        "palyra",
        "cron",
        "edit",
        "--id",
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        "--enabled",
        "false",
    ]);
    assert!(matches!(edit.command, Command::Cron { command: CronCommand::Update { .. } }));

    let runs = Cli::parse_from(["palyra", "cron", "runs", "--id", "01ARZ3NDEKTSV4RRFFQ69G5FB0"]);
    assert!(matches!(runs.command, Command::Cron { command: CronCommand::Logs { .. } }));

    let rm = Cli::parse_from(["palyra", "cron", "rm", "--id", "01ARZ3NDEKTSV4RRFFQ69G5FB0"]);
    assert!(matches!(rm.command, Command::Cron { command: CronCommand::Delete { .. } }));
}

#[test]
fn parse_routines_upsert() {
    let parsed = Cli::parse_from([
        "palyra",
        "routines",
        "upsert",
        "--id",
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        "--name",
        "Daily report",
        "--prompt",
        "Summarize incidents",
        "--trigger-kind",
        "schedule",
        "--natural-language-schedule",
        "every weekday at 9",
        "--enabled",
        "true",
        "--concurrency",
        "queue-one",
        "--retry-max-attempts",
        "2",
        "--retry-backoff-ms",
        "5000",
        "--misfire",
        "catch-up",
        "--jitter-ms",
        "250",
        "--delivery-mode",
        "specific-channel",
        "--delivery-channel",
        "ops:summary",
        "--delivery-failure-mode",
        "logs-only",
        "--silent-policy",
        "failure-only",
        "--run-mode",
        "fresh-session",
        "--procedure-profile-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FC0",
        "--skill-profile-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FC1",
        "--provider-profile-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FC2",
        "--execution-posture",
        "sensitive-tools",
        "--quiet-hours-start",
        "22:00",
        "--quiet-hours-end",
        "06:00",
        "--quiet-hours-timezone",
        "utc",
        "--cooldown-ms",
        "120000",
        "--approval-mode",
        "before-first-run",
        "--template-id",
        "daily-report",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Routines {
            command: RoutinesCommand::Upsert(Box::new(RoutineUpsertCommand {
                id: Some("01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned()),
                name: "Daily report".to_owned(),
                prompt: "Summarize incidents".to_owned(),
                trigger_kind: RoutineTriggerKindArg::Schedule,
                owner: None,
                channel: None,
                session_key: None,
                session_label: None,
                enabled: Some(true),
                natural_language_schedule: Some("every weekday at 9".to_owned()),
                schedule_type: None,
                schedule: None,
                trigger_payload: None,
                trigger_payload_stdin: false,
                concurrency: CronConcurrencyPolicyArg::QueueOne,
                retry_max_attempts: 2,
                retry_backoff_ms: 5000,
                misfire: CronMisfirePolicyArg::CatchUp,
                jitter_ms: 250,
                delivery_mode: RoutineDeliveryModeArg::SpecificChannel,
                delivery_channel: Some("ops:summary".to_owned()),
                delivery_failure_mode: Some(RoutineDeliveryModeArg::LogsOnly),
                delivery_failure_channel: None,
                silent_policy: RoutineSilentPolicyArg::FailureOnly,
                run_mode: RoutineRunModeArg::FreshSession,
                procedure_profile_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FC0".to_owned()),
                skill_profile_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FC1".to_owned()),
                provider_profile_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FC2".to_owned()),
                execution_posture: RoutineExecutionPostureArg::SensitiveTools,
                quiet_hours_start: Some("22:00".to_owned()),
                quiet_hours_end: Some("06:00".to_owned()),
                quiet_hours_timezone: Some(RoutinePreviewTimezoneArg::Utc),
                cooldown_ms: 120000,
                approval_mode: RoutineApprovalModeArg::BeforeFirstRun,
                template_id: Some("daily-report".to_owned()),
                json: true,
            }))
        }
    );
}

#[test]
fn parse_routines_test_run() {
    let parsed = Cli::parse_from([
        "palyra",
        "routines",
        "test-run",
        "--id",
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        "--source-run-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FC0",
        "--trigger-reason",
        "replay after failure",
        "--trigger-payload",
        "{\"event\":\"push\"}",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Routines {
            command: RoutinesCommand::TestRun {
                id: "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned(),
                source_run_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FC0".to_owned()),
                trigger_reason: Some("replay after failure".to_owned()),
                trigger_payload: Some("{\"event\":\"push\"}".to_owned()),
                trigger_payload_stdin: false,
                json: true,
            }
        }
    );
}

#[test]
fn parse_routines_template_and_import() {
    let template = Cli::parse_from([
        "palyra",
        "routines",
        "create-from-template",
        "--template-id",
        "heartbeat",
        "--name",
        "Morning heartbeat",
        "--delivery-channel",
        "ops:summary",
    ]);
    assert_eq!(
        template.command,
        Command::Routines {
            command: RoutinesCommand::CreateFromTemplate {
                template_id: "heartbeat".to_owned(),
                id: None,
                name: Some("Morning heartbeat".to_owned()),
                prompt: None,
                owner: None,
                channel: None,
                session_key: None,
                session_label: None,
                enabled: None,
                natural_language_schedule: None,
                delivery_channel: Some("ops:summary".to_owned()),
                trigger_payload: None,
                trigger_payload_stdin: false,
                json: false,
            }
        }
    );

    let import = Cli::parse_from([
        "palyra",
        "routines",
        "import",
        "--file",
        "routine.json",
        "--id",
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        "--enabled",
        "false",
        "--json",
    ]);
    assert_eq!(
        import.command,
        Command::Routines {
            command: RoutinesCommand::Import {
                file: Some("routine.json".to_owned()),
                stdin: false,
                id: Some("01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned()),
                enabled: Some(false),
                json: true,
            }
        }
    );
}

#[test]
fn parse_objectives_list_with_filters() {
    let parsed = Cli::parse_from([
        "palyra",
        "objectives",
        "list",
        "--after",
        "obj-01",
        "--limit",
        "25",
        "--kind",
        "heartbeat",
        "--state",
        "active",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Objectives {
            command: ObjectivesCommand::List {
                after: Some("obj-01".to_owned()),
                limit: Some(25),
                kind: Some(ObjectiveKindArg::Heartbeat),
                state: Some(super::ObjectiveStateArg::Active),
                json: true,
            }
        }
    );
}

#[test]
fn parse_flows_controls() {
    let list = Cli::parse_from([
        "palyra",
        "flows",
        "list",
        "--limit",
        "25",
        "--state",
        "waiting-for-approval",
        "--active-only",
        "--json",
    ]);
    assert_eq!(
        list.command,
        Command::Flows {
            command: FlowsCommand::List {
                limit: Some(25),
                state: Some(FlowStateArg::WaitingForApproval),
                active_only: true,
                json: true,
            }
        }
    );

    let retry = Cli::parse_from([
        "palyra",
        "flows",
        "retry-step",
        "--id",
        "flow-01",
        "--step-id",
        "step-01",
        "--reason",
        "adapter recovered",
        "--json",
    ]);
    assert_eq!(
        retry.command,
        Command::Flows {
            command: FlowsCommand::RetryStep {
                id: "flow-01".to_owned(),
                step_id: "step-01".to_owned(),
                reason: Some("adapter recovered".to_owned()),
                json: true,
            }
        }
    );
}

#[test]
fn parse_objectives_upsert_with_schedule_and_budget() {
    let parsed = Cli::parse_from([
        "palyra",
        "objectives",
        "upsert",
        "--id",
        "obj-01",
        "--kind",
        "standing-order",
        "--name",
        "Ops daily objective",
        "--prompt",
        "Summarize incidents and propose next action",
        "--owner",
        "operator:primary",
        "--channel",
        "ops",
        "--session-key",
        "ops:daily",
        "--session-label",
        "Ops daily",
        "--priority",
        "critical",
        "--max-runs",
        "5",
        "--max-tokens",
        "12000",
        "--budget-notes",
        "bounded trial",
        "--current-focus",
        "Incident triage",
        "--success-criteria",
        "Inbox empty",
        "--exit-condition",
        "No open sev1",
        "--next-recommended-step",
        "Ping on-call",
        "--standing-order",
        "Run at shift start",
        "--enabled",
        "true",
        "--natural-language-schedule",
        "every weekday at 09:00",
        "--schedule-type",
        "every",
        "--schedule",
        "3600000",
        "--delivery-mode",
        "specific-channel",
        "--delivery-channel",
        "ops:summary",
        "--quiet-hours-start",
        "22:00",
        "--quiet-hours-end",
        "06:00",
        "--quiet-hours-timezone",
        "utc",
        "--cooldown-ms",
        "300000",
        "--approval-mode",
        "before-first-run",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Objectives {
            command: ObjectivesCommand::Upsert(Box::new(ObjectiveUpsertCommandArgs {
                id: Some("obj-01".to_owned()),
                kind: ObjectiveKindArg::StandingOrder,
                name: "Ops daily objective".to_owned(),
                prompt: "Summarize incidents and propose next action".to_owned(),
                owner: Some("operator:primary".to_owned()),
                channel: Some("ops".to_owned()),
                session_key: Some("ops:daily".to_owned()),
                session_label: Some("Ops daily".to_owned()),
                priority: ObjectivePriorityArg::Critical,
                max_runs: Some(5),
                max_tokens: Some(12000),
                budget_notes: Some("bounded trial".to_owned()),
                current_focus: Some("Incident triage".to_owned()),
                success_criteria: Some("Inbox empty".to_owned()),
                exit_condition: Some("No open sev1".to_owned()),
                next_recommended_step: Some("Ping on-call".to_owned()),
                standing_order: Some("Run at shift start".to_owned()),
                enabled: Some(true),
                natural_language_schedule: Some("every weekday at 09:00".to_owned()),
                schedule_type: Some(ObjectiveScheduleTypeArg::Every),
                schedule: Some("3600000".to_owned()),
                delivery_mode: RoutineDeliveryModeArg::SpecificChannel,
                delivery_channel: Some("ops:summary".to_owned()),
                quiet_hours_start: Some("22:00".to_owned()),
                quiet_hours_end: Some("06:00".to_owned()),
                quiet_hours_timezone: Some(RoutinePreviewTimezoneArg::Utc),
                cooldown_ms: 300000,
                approval_mode: RoutineApprovalModeArg::BeforeFirstRun,
                json: true,
            }))
        }
    );
}

#[test]
fn parse_memory_search() {
    let parsed = Cli::parse_from([
        "palyra",
        "memory",
        "search",
        "release notes",
        "--scope",
        "channel",
        "--top-k",
        "8",
        "--min-score",
        "0.25",
        "--tag",
        "release",
        "--source",
        "summary",
        "--include-score-breakdown",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Memory {
            command: MemoryCommand::Search {
                query: "release notes".to_owned(),
                scope: MemoryScopeArg::Channel,
                session: None,
                channel: None,
                top_k: Some(8),
                min_score: Some("0.25".to_owned()),
                tag: vec!["release".to_owned()],
                source: vec![MemorySourceArg::Summary],
                include_score_breakdown: true,
                show_metadata: false,
                json: true,
            }
        }
    );
}

#[test]
fn parse_memory_session_search() {
    let parsed = Cli::parse_from([
        "palyra",
        "memory",
        "session-search",
        "cross session recall",
        "--channel",
        "cli",
        "--top-k",
        "6",
        "--min-score",
        "0.2",
        "--window-before",
        "2",
        "--window-after",
        "3",
        "--max-windows-per-session",
        "4",
        "--include-archived",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Memory {
            command: MemoryCommand::SessionSearch {
                query: "cross session recall".to_owned(),
                channel: Some("cli".to_owned()),
                top_k: Some(6),
                min_score: Some("0.2".to_owned()),
                window_before: Some(2),
                window_after: Some(3),
                max_windows_per_session: Some(4),
                include_archived: true,
                json: true,
            }
        }
    );
}

#[test]
fn parse_memory_recall_artifacts() {
    let parsed = Cli::parse_from([
        "palyra",
        "memory",
        "recall-artifacts",
        "--kind",
        "session_search",
        "--session",
        "01ARZ3NDEKTSV4RRFFQ69G5SA1",
        "--channel",
        "cli",
        "--limit",
        "12",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Memory {
            command: MemoryCommand::RecallArtifacts {
                kind: Some("session_search".to_owned()),
                session: Some("01ARZ3NDEKTSV4RRFFQ69G5SA1".to_owned()),
                channel: Some("cli".to_owned()),
                limit: Some(12),
                json: true,
            }
        }
    );
}

#[test]
fn parse_memory_status_and_index() {
    let status = Cli::parse_from(["palyra", "memory", "status", "--json"]);
    assert_eq!(status.command, Command::Memory { command: MemoryCommand::Status { json: true } });

    let index = Cli::parse_from([
        "palyra",
        "memory",
        "reindex",
        "--batch-size",
        "32",
        "--until-complete",
        "--run-maintenance",
    ]);
    assert_eq!(
        index.command,
        Command::Memory {
            command: MemoryCommand::Index {
                batch_size: Some(32),
                until_complete: true,
                run_maintenance: true,
                json: false,
            }
        }
    );

    let drift = Cli::parse_from(["palyra", "memory", "index-drift", "--json"]);
    assert_eq!(
        drift.command,
        Command::Memory { command: MemoryCommand::IndexDrift { json: true } }
    );

    let reconcile = Cli::parse_from(["palyra", "memory", "index-reconcile", "--batch-size", "512"]);
    assert_eq!(
        reconcile.command,
        Command::Memory {
            command: MemoryCommand::IndexReconcile { batch_size: Some(512), json: false }
        }
    );
}

#[test]
fn parse_system_commands() {
    let heartbeat = Cli::parse_from(["palyra", "system", "heartbeat", "--json"]);
    assert_eq!(
        heartbeat.command,
        Command::System { command: SystemCommand::Heartbeat { json: true } }
    );

    let events = Cli::parse_from(["palyra", "system", "events", "list", "--limit", "25"]);
    assert_eq!(
        events.command,
        Command::System {
            command: SystemCommand::Event {
                command: SystemEventCommand::List { limit: Some(25), json: false }
            }
        }
    );

    let emit = Cli::parse_from([
        "palyra",
        "system",
        "event",
        "emit",
        "operator.heartbeat",
        "--message",
        "manual probe",
        "--severity",
        "warn",
        "--tag",
        "ops",
    ]);
    assert_eq!(
        emit.command,
        Command::System {
            command: SystemCommand::Event {
                command: SystemEventCommand::Emit {
                    event: "operator.heartbeat".to_owned(),
                    message: Some("manual probe".to_owned()),
                    severity: SystemEventSeverityArg::Warn,
                    tag: vec!["ops".to_owned()],
                    json: false,
                }
            }
        }
    );
}

#[test]
fn parse_sandbox_commands() {
    let list = Cli::parse_from(["palyra", "sandbox", "list", "--json"]);
    assert_eq!(list.command, Command::Sandbox { command: SandboxCommand::List { json: true } });

    let explain = Cli::parse_from(["palyra", "sandbox", "explain", "--runtime", "process-runner"]);
    assert_eq!(
        explain.command,
        Command::Sandbox {
            command: SandboxCommand::Explain {
                runtime: SandboxRuntimeArg::ProcessRunner,
                json: false,
            }
        }
    );
}

#[test]
fn parse_memory_purge() {
    let parsed = Cli::parse_from([
        "palyra",
        "memory",
        "purge",
        "--session",
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Memory {
            command: MemoryCommand::Purge {
                session: Some("01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned()),
                channel: None,
                principal: false,
                json: true,
            }
        }
    );
}

#[test]
fn parse_memory_ingest() {
    let parsed = Cli::parse_from([
        "palyra",
        "memory",
        "ingest",
        "important finding",
        "--source",
        "manual",
        "--tag",
        "ops",
        "--confidence",
        "0.9",
        "--ttl-unix-ms",
        "1730000000000",
    ]);
    assert_eq!(
        parsed.command,
        Command::Memory {
            command: MemoryCommand::Ingest {
                content: "important finding".to_owned(),
                source: MemorySourceArg::Manual,
                session: None,
                channel: None,
                tag: vec!["ops".to_owned()],
                confidence: Some("0.9".to_owned()),
                ttl_unix_ms: Some(1_730_000_000_000),
                json: false,
            }
        }
    );
}

#[test]
fn parse_approvals_list() {
    let parsed = Cli::parse_from([
        "palyra",
        "approvals",
        "list",
        "--after",
        "01ARZ3NDEKTSV4RRFFQ69G5FB1",
        "--limit",
        "50",
        "--since",
        "1730000000000",
        "--until",
        "1730001000000",
        "--subject",
        "tool:palyra.process.run",
        "--principal",
        "user:ops",
        "--decision",
        "deny",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Approvals {
            command: ApprovalsCommand::List {
                after: Some("01ARZ3NDEKTSV4RRFFQ69G5FB1".to_owned()),
                limit: Some(50),
                since: Some(1_730_000_000_000),
                until: Some(1_730_001_000_000),
                subject: Some("tool:palyra.process.run".to_owned()),
                principal: Some("user:ops".to_owned()),
                decision: Some(ApprovalDecisionArg::Deny),
                subject_type: None,
                json: true,
            }
        }
    );
}

#[test]
fn parse_approvals_show() {
    let parsed =
        Cli::parse_from(["palyra", "approvals", "show", "01ARZ3NDEKTSV4RRFFQ69G5FB2", "--json"]);
    assert_eq!(
        parsed.command,
        Command::Approvals {
            command: ApprovalsCommand::Show {
                approval_id: "01ARZ3NDEKTSV4RRFFQ69G5FB2".to_owned(),
                json: true,
            }
        }
    );
}

#[test]
fn parse_approvals_export() {
    let parsed = Cli::parse_from([
        "palyra",
        "approvals",
        "export",
        "--format",
        "json",
        "--limit",
        "200",
        "--decision",
        "allow",
    ]);
    assert_eq!(
        parsed.command,
        Command::Approvals {
            command: ApprovalsCommand::Export {
                format: ApprovalExportFormatArg::Json,
                limit: Some(200),
                since: None,
                until: None,
                subject: None,
                principal: None,
                decision: Some(ApprovalDecisionArg::Allow),
                subject_type: None,
            }
        }
    );
}

#[test]
fn parse_approvals_list_with_subject_type() {
    let parsed =
        Cli::parse_from(["palyra", "approvals", "list", "--subject-type", "browser-action"]);
    assert_eq!(
        parsed.command,
        Command::Approvals {
            command: ApprovalsCommand::List {
                after: None,
                limit: None,
                since: None,
                until: None,
                subject: None,
                principal: None,
                decision: None,
                subject_type: Some(ApprovalSubjectTypeArg::BrowserAction),
                json: false,
            }
        }
    );
}

#[test]
fn parse_approvals_export_with_subject_type() {
    let parsed =
        Cli::parse_from(["palyra", "approvals", "export", "--subject-type", "node-capability"]);
    assert_eq!(
        parsed.command,
        Command::Approvals {
            command: ApprovalsCommand::Export {
                format: ApprovalExportFormatArg::Ndjson,
                limit: None,
                since: None,
                until: None,
                subject: None,
                principal: None,
                decision: None,
                subject_type: Some(ApprovalSubjectTypeArg::NodeCapability),
            }
        }
    );
}

#[test]
fn parse_approvals_decide_allow_timeboxed() {
    let parsed = Cli::parse_from([
        "palyra",
        "approvals",
        "decide",
        "01ARZ3NDEKTSV4RRFFQ69G5FB3",
        "--decision",
        "allow",
        "--scope",
        "timeboxed",
        "--ttl-ms",
        "600000",
        "--reason",
        "operator-approved",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Approvals {
            command: ApprovalsCommand::Decide {
                approval_id: "01ARZ3NDEKTSV4RRFFQ69G5FB3".to_owned(),
                decision: ApprovalResolveDecisionArg::Allow,
                scope: ApprovalDecisionScopeArg::Timeboxed,
                ttl_ms: Some(600_000),
                reason: Some("operator-approved".to_owned()),
                json: true,
            }
        }
    );
}

#[test]
fn parse_approvals_decide_deny_defaults_to_once() {
    let parsed = Cli::parse_from([
        "palyra",
        "approvals",
        "decide",
        "01ARZ3NDEKTSV4RRFFQ69G5FB4",
        "--decision",
        "deny",
    ]);
    assert_eq!(
        parsed.command,
        Command::Approvals {
            command: ApprovalsCommand::Decide {
                approval_id: "01ARZ3NDEKTSV4RRFFQ69G5FB4".to_owned(),
                decision: ApprovalResolveDecisionArg::Deny,
                scope: ApprovalDecisionScopeArg::Once,
                ttl_ms: None,
                reason: None,
                json: false,
            }
        }
    );
}

#[test]
fn parse_auth_profiles_list() {
    let parsed = Cli::parse_from([
        "palyra",
        "auth",
        "profiles",
        "list",
        "--after",
        "openai-default",
        "--limit",
        "25",
        "--provider",
        "openai",
        "--scope",
        "agent",
        "--agent-id",
        "assistant",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Auth {
            command: AuthCommand::Profiles {
                command: AuthProfilesCommand::List {
                    after: Some("openai-default".to_owned()),
                    limit: Some(25),
                    provider: Some(AuthProviderArg::Openai),
                    provider_name: None,
                    scope: Some(AuthScopeArg::Agent),
                    agent_id: Some("assistant".to_owned()),
                    json: true,
                },
            },
        }
    );
}

#[test]
fn parse_auth_profiles_set_oauth() {
    let parsed = Cli::parse_from([
        "palyra",
        "auth",
        "profiles",
        "set",
        "openai-default",
        "--provider",
        "openai",
        "--profile-name",
        "Default OpenAI",
        "--scope",
        "agent",
        "--agent-id",
        "assistant",
        "--credential",
        "oauth",
        "--access-token-ref",
        "global/openai_access",
        "--refresh-token-ref",
        "global/openai_refresh",
        "--token-endpoint",
        "https://example.com/oauth/token",
        "--client-id",
        "client-123",
        "--client-secret-ref",
        "global/openai_client_secret",
        "--scope-value",
        "chat:read",
        "--scope-value",
        "chat:write",
        "--expires-at-unix-ms",
        "1730000000000",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Auth {
            command: AuthCommand::Profiles {
                command: AuthProfilesCommand::Set {
                    profile_id: "openai-default".to_owned(),
                    provider: AuthProviderArg::Openai,
                    provider_name: None,
                    profile_name: "Default OpenAI".to_owned(),
                    scope: AuthScopeArg::Agent,
                    agent_id: Some("assistant".to_owned()),
                    credential: AuthCredentialArg::Oauth,
                    api_key_ref: None,
                    access_token_ref: Some("global/openai_access".to_owned()),
                    refresh_token_ref: Some("global/openai_refresh".to_owned()),
                    token_endpoint: Some("https://example.com/oauth/token".to_owned()),
                    client_id: Some("client-123".to_owned()),
                    client_secret_ref: Some("global/openai_client_secret".to_owned()),
                    scope_value: vec!["chat:read".to_owned(), "chat:write".to_owned()],
                    expires_at_unix_ms: Some(1_730_000_000_000),
                    json: true,
                },
            },
        }
    );
}

#[test]
fn parse_auth_openai_api_key_from_stdin() {
    let parsed = Cli::parse_from([
        "palyra",
        "auth",
        "openai",
        "api-key",
        "--profile-name",
        "Default OpenAI",
        "--scope",
        "agent",
        "--agent-id",
        "assistant",
        "--api-key-stdin",
        "--set-default",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Auth {
            command: AuthCommand::Openai {
                command: AuthOpenAiCommand::ApiKey {
                    profile_id: None,
                    profile_name: "Default OpenAI".to_owned(),
                    scope: AuthScopeArg::Agent,
                    agent_id: Some("assistant".to_owned()),
                    api_key_env: None,
                    api_key_stdin: true,
                    api_key_prompt: false,
                    set_default: true,
                    json: true,
                },
            },
        }
    );
}

#[test]
fn parse_auth_openai_oauth_start() {
    let parsed = Cli::parse_from([
        "palyra",
        "auth",
        "openai",
        "oauth-start",
        "--profile-id",
        "openai-default",
        "--client-id",
        "client-123",
        "--client-secret-env",
        "OPENAI_CLIENT_SECRET",
        "--scope-value",
        "openid",
        "--scope-value",
        "offline_access",
        "--set-default",
        "--open",
    ]);
    assert_eq!(
        parsed.command,
        Command::Auth {
            command: AuthCommand::Openai {
                command: AuthOpenAiCommand::OauthStart {
                    profile_id: Some("openai-default".to_owned()),
                    profile_name: None,
                    scope: AuthScopeArg::Global,
                    agent_id: None,
                    client_id: "client-123".to_owned(),
                    client_secret_env: Some("OPENAI_CLIENT_SECRET".to_owned()),
                    client_secret_stdin: false,
                    client_secret_prompt: false,
                    scope_value: vec!["openid".to_owned(), "offline_access".to_owned()],
                    set_default: true,
                    open: true,
                    json: false,
                },
            },
        }
    );
}

#[test]
fn parse_auth_openai_reconnect() {
    let parsed =
        Cli::parse_from(["palyra", "auth", "openai", "reconnect", "openai-default", "--json"]);
    assert_eq!(
        parsed.command,
        Command::Auth {
            command: AuthCommand::Openai {
                command: AuthOpenAiCommand::Reconnect {
                    profile_id: "openai-default".to_owned(),
                    json: true,
                },
            },
        }
    );
}

#[test]
fn parse_auth_profiles_health() {
    let parsed = Cli::parse_from([
        "palyra",
        "auth",
        "profiles",
        "health",
        "--agent-id",
        "assistant",
        "--include-profiles",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Auth {
            command: AuthCommand::Profiles {
                command: AuthProfilesCommand::Health {
                    agent_id: Some("assistant".to_owned()),
                    include_profiles: true,
                    json: true,
                },
            },
        }
    );
}

#[test]
fn parse_auth_access_backfill_dry_run() {
    let parsed = Cli::parse_from(["palyra", "auth", "access", "backfill", "--dry-run", "--json"]);
    assert_eq!(
        parsed.command,
        Command::Auth {
            command: AuthCommand::Access {
                command: AuthAccessCommand::Backfill { dry_run: true, json: true },
            },
        }
    );
}

#[test]
fn parse_auth_access_feature_toggle() {
    let parsed = Cli::parse_from([
        "palyra",
        "auth",
        "access",
        "feature",
        "compat_api",
        "true",
        "--stage",
        "admin_only",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Auth {
            command: AuthCommand::Access {
                command: AuthAccessCommand::Feature {
                    feature_key: "compat_api".to_owned(),
                    enabled: true,
                    stage: Some("admin_only".to_owned()),
                    json: true,
                },
            },
        }
    );
}

#[test]
fn parse_auth_access_token_create_for_workspace() {
    let parsed = Cli::parse_from([
        "palyra",
        "auth",
        "access",
        "token-create",
        "--label",
        "Compat API",
        "--principal",
        "user:alice",
        "--workspace-id",
        "01WORKSPACE",
        "--role",
        "admin",
        "--scope",
        "compat.chat.create",
        "--scope",
        "compat.responses.create",
        "--rate-limit-per-minute",
        "240",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Auth {
            command: AuthCommand::Access {
                command: AuthAccessCommand::TokenCreate {
                    label: "Compat API".to_owned(),
                    principal: "user:alice".to_owned(),
                    workspace_id: Some("01WORKSPACE".to_owned()),
                    role: WorkspaceRoleArg::Admin,
                    scope: vec![
                        "compat.chat.create".to_owned(),
                        "compat.responses.create".to_owned()
                    ],
                    expires_at_unix_ms: None,
                    rate_limit_per_minute: Some(240),
                    json: true,
                },
            },
        }
    );
}

#[test]
fn parse_channels_enable() {
    let parsed = Cli::parse_from([
        "palyra",
        "channels",
        "enable",
        "echo:default",
        "--url",
        "http://127.0.0.1:7142",
        "--token",
        "admin-token",
        "--principal",
        "admin:ops",
        "--device-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--channel",
        "cli",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Channels {
            command: ChannelsCommand::Enable {
                connector_id: "echo:default".to_owned(),
                url: Some("http://127.0.0.1:7142".to_owned()),
                token: Some("admin-token".to_owned()),
                principal: "admin:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("cli".to_owned()),
                json: true,
            }
        }
    );
}

#[test]
fn parse_channels_add_non_interactive_discord() {
    let parsed = Cli::parse_from([
        "palyra",
        "channels",
        "add",
        "--provider",
        "discord",
        "--account-id",
        "ops",
        "--credential-stdin",
        "--mode",
        "remote_vps",
        "--inbound-scope",
        "allowlisted_guild_channels",
        "--allow-from",
        "ops-team",
        "--deny-from",
        "spam-bot",
        "--require-mention",
        "true",
        "--mention-pattern",
        "<@123>",
        "--direct-message-policy",
        "pairing",
        "--broadcast-strategy",
        "mention_only",
        "--concurrency-limit",
        "4",
        "--verify-channel-id",
        "123456789012345678",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Channels {
            command: ChannelsCommand::Add {
                provider: ChannelProviderArg::Discord,
                account_id: "ops".to_owned(),
                interactive: false,
                credential: None,
                credential_stdin: true,
                credential_prompt: false,
                mode: "remote_vps".to_owned(),
                inbound_scope: "allowlisted_guild_channels".to_owned(),
                allow_from: vec!["ops-team".to_owned()],
                deny_from: vec!["spam-bot".to_owned()],
                require_mention: Some(true),
                mention_patterns: vec!["<@123>".to_owned()],
                concurrency_limit: Some(4),
                direct_message_policy: Some("pairing".to_owned()),
                broadcast_strategy: Some("mention_only".to_owned()),
                confirm_open_guild_channels: false,
                verify_channel_id: Some("123456789012345678".to_owned()),
                url: None,
                token: None,
                principal: "user:local".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: None,
                json: true,
            }
        }
    );
}

#[test]
fn parse_channels_logout() {
    let parsed = Cli::parse_from([
        "palyra",
        "channels",
        "logout",
        "--provider",
        "discord",
        "--account-id",
        "ops",
        "--keep-credential",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Channels {
            command: ChannelsCommand::Logout {
                provider: ChannelProviderArg::Discord,
                account_id: "ops".to_owned(),
                keep_credential: true,
                url: None,
                token: None,
                principal: "user:local".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: None,
                json: true,
            }
        }
    );
}

#[test]
fn parse_channels_discord_setup() {
    let parsed = Cli::parse_from([
        "palyra",
        "channels",
        "discord",
        "setup",
        "--account-id",
        "ops",
        "--url",
        "http://127.0.0.1:7142",
        "--token",
        "admin-token",
        "--principal",
        "admin:ops",
        "--device-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--channel",
        "cli",
        "--verify-channel-id",
        "123456789012345678",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Channels {
            command: ChannelsCommand::Discord {
                command: ChannelsDiscordCommand::Setup {
                    account_id: "ops".to_owned(),
                    url: Some("http://127.0.0.1:7142".to_owned()),
                    token: Some("admin-token".to_owned()),
                    principal: "admin:ops".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                    channel: Some("cli".to_owned()),
                    verify_channel_id: Some("123456789012345678".to_owned()),
                    json: true,
                },
            }
        }
    );
}

#[test]
fn parse_channels_capabilities_with_provider_selector() {
    let parsed = Cli::parse_from([
        "palyra",
        "channels",
        "capabilities",
        "--provider",
        "discord",
        "--account-id",
        "ops",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Channels {
            command: ChannelsCommand::Capabilities {
                connector_id: None,
                provider: Some(ChannelProviderArg::Discord),
                account_id: Some("ops".to_owned()),
                url: None,
                token: None,
                principal: "user:local".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: None,
                json: true,
            }
        }
    );
}

#[test]
fn parse_channels_resolve_discord_user() {
    let parsed = Cli::parse_from([
        "palyra",
        "channels",
        "resolve",
        "--provider",
        "discord",
        "--account-id",
        "ops",
        "--entity",
        "user",
        "--value",
        "<@12345>",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Channels {
            command: ChannelsCommand::Resolve {
                provider: ChannelProviderArg::Discord,
                account_id: "ops".to_owned(),
                entity: ChannelResolveEntityArg::User,
                value: "<@12345>".to_owned(),
                json: true,
            }
        }
    );
}

#[test]
fn parse_channels_discord_status() {
    let parsed = Cli::parse_from([
        "palyra",
        "channels",
        "discord",
        "status",
        "--account-id",
        "ops",
        "--url",
        "http://127.0.0.1:7142",
        "--token",
        "admin-token",
        "--principal",
        "admin:ops",
        "--device-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--channel",
        "cli",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Channels {
            command: ChannelsCommand::Discord {
                command: ChannelsDiscordCommand::Status {
                    account_id: "ops".to_owned(),
                    url: Some("http://127.0.0.1:7142".to_owned()),
                    token: Some("admin-token".to_owned()),
                    principal: "admin:ops".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                    channel: Some("cli".to_owned()),
                    json: true,
                },
            }
        }
    );
}

#[test]
fn parse_channels_discord_health_refresh() {
    let parsed = Cli::parse_from([
        "palyra",
        "channels",
        "discord",
        "health-refresh",
        "--account-id",
        "ops",
        "--verify-channel-id",
        "123456789012345678",
        "--url",
        "http://127.0.0.1:7142",
        "--token",
        "admin-token",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Channels {
            command: ChannelsCommand::Discord {
                command: ChannelsDiscordCommand::HealthRefresh {
                    account_id: "ops".to_owned(),
                    verify_channel_id: Some("123456789012345678".to_owned()),
                    url: Some("http://127.0.0.1:7142".to_owned()),
                    token: Some("admin-token".to_owned()),
                    principal: "user:local".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                    channel: None,
                    json: true,
                },
            }
        }
    );
}

#[test]
fn parse_channels_pairings_with_provider_selector() {
    let parsed = Cli::parse_from([
        "palyra",
        "channels",
        "pairings",
        "--provider",
        "discord",
        "--account-id",
        "ops",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Channels {
            command: ChannelsCommand::Pairings {
                connector_id: None,
                provider: Some(ChannelProviderArg::Discord),
                account_id: Some("ops".to_owned()),
                url: None,
                token: None,
                principal: "user:local".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: None,
                json: true,
            }
        }
    );
}

#[test]
fn parse_channels_qr_with_artifact() {
    let parsed = Cli::parse_from([
        "palyra",
        "channels",
        "qr",
        "--provider",
        "discord",
        "--account-id",
        "ops",
        "--issued-by",
        "admin:ops@01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--ttl-ms",
        "600000",
        "--artifact",
        "artifacts/pairing.txt",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Channels {
            command: ChannelsCommand::Qr {
                connector_id: None,
                provider: Some(ChannelProviderArg::Discord),
                account_id: Some("ops".to_owned()),
                issued_by: Some("admin:ops@01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
                ttl_ms: Some(600000),
                artifact: Some("artifacts/pairing.txt".to_owned()),
                url: None,
                token: None,
                principal: "user:local".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: None,
                json: true,
            }
        }
    );
}

#[test]
fn parse_channels_discord_verify_via_test_send_alias() {
    let parsed = Cli::parse_from([
        "palyra",
        "channels",
        "discord",
        "test-send",
        "--account-id",
        "default",
        "--to",
        "channel:123456",
        "--text",
        "hello",
        "--confirm",
        "--auto-reaction",
        ":white_check_mark:",
        "--thread-id",
        "thread-1",
        "--url",
        "http://127.0.0.1:7142",
        "--token",
        "admin-token",
    ]);
    assert_eq!(
        parsed.command,
        Command::Channels {
            command: ChannelsCommand::Discord {
                command: ChannelsDiscordCommand::Verify {
                    account_id: "default".to_owned(),
                    to: "channel:123456".to_owned(),
                    text: "hello".to_owned(),
                    confirm: true,
                    auto_reaction: Some(":white_check_mark:".to_owned()),
                    thread_id: Some("thread-1".to_owned()),
                    url: Some("http://127.0.0.1:7142".to_owned()),
                    token: Some("admin-token".to_owned()),
                    principal: "user:local".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                    channel: None,
                    json: false,
                },
            }
        }
    );
}

#[test]
fn parse_channels_discord_verify_command_name() {
    let parsed = Cli::parse_from([
        "palyra",
        "channels",
        "discord",
        "verify",
        "--to",
        "channel:123456",
        "--confirm",
    ]);
    assert_eq!(
        parsed.command,
        Command::Channels {
            command: ChannelsCommand::Discord {
                command: ChannelsDiscordCommand::Verify {
                    account_id: "default".to_owned(),
                    to: "channel:123456".to_owned(),
                    text: "palyra discord test message".to_owned(),
                    confirm: true,
                    auto_reaction: None,
                    thread_id: None,
                    url: None,
                    token: None,
                    principal: "user:local".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                    channel: None,
                    json: false,
                },
            }
        }
    );
}

#[test]
fn parse_channels_router_preview() {
    let parsed = Cli::parse_from([
        "palyra",
        "channels",
        "router",
        "preview",
        "--route-channel",
        "discord:default",
        "--text",
        "pair ABCDEF",
        "--sender-identity",
        "discord:user:12345",
        "--max-payload-bytes",
        "2048",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Channels {
            command: ChannelsCommand::Router {
                command: ChannelsRouterCommand::Preview {
                    route_channel: "discord:default".to_owned(),
                    text: "pair ABCDEF".to_owned(),
                    conversation_id: None,
                    sender_identity: Some("discord:user:12345".to_owned()),
                    sender_display: None,
                    sender_verified: true,
                    is_direct_message: true,
                    requested_broadcast: false,
                    adapter_message_id: None,
                    adapter_thread_id: None,
                    max_payload_bytes: Some(2048),
                    url: None,
                    token: None,
                    principal: "user:local".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                    channel: None,
                    json: true,
                },
            },
        }
    );
}

#[test]
fn parse_channels_queue_pause() {
    let parsed = Cli::parse_from([
        "palyra",
        "channels",
        "queue-pause",
        "echo:default",
        "--url",
        "http://127.0.0.1:7142",
        "--token",
        "admin-token",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Channels {
            command: ChannelsCommand::QueuePause {
                connector_id: "echo:default".to_owned(),
                url: Some("http://127.0.0.1:7142".to_owned()),
                token: Some("admin-token".to_owned()),
                principal: "user:local".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: None,
                json: true,
            }
        }
    );
}

#[test]
fn parse_channels_dead_letter_replay() {
    let parsed = Cli::parse_from([
        "palyra",
        "channels",
        "dead-letter-replay",
        "discord:default",
        "42",
        "--token",
        "admin-token",
    ]);
    assert_eq!(
        parsed.command,
        Command::Channels {
            command: ChannelsCommand::DeadLetterReplay {
                connector_id: "discord:default".to_owned(),
                dead_letter_id: 42,
                url: None,
                token: Some("admin-token".to_owned()),
                principal: "user:local".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: None,
                json: false,
            }
        }
    );
}

#[test]
fn parse_channels_router_mint_pairing_code() {
    let parsed = Cli::parse_from([
        "palyra",
        "channels",
        "router",
        "mint-pairing-code",
        "--route-channel",
        "discord:default",
        "--issued-by",
        "admin:ops@01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--ttl-ms",
        "600000",
        "--url",
        "http://127.0.0.1:7142",
        "--token",
        "admin-token",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Channels {
            command: ChannelsCommand::Router {
                command: ChannelsRouterCommand::MintPairingCode {
                    route_channel: "discord:default".to_owned(),
                    issued_by: Some("admin:ops@01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
                    ttl_ms: Some(600000),
                    url: Some("http://127.0.0.1:7142".to_owned()),
                    token: Some("admin-token".to_owned()),
                    principal: "user:local".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                    channel: None,
                    json: true,
                },
            },
        }
    );
}

#[test]
fn parse_browser_status() {
    let parsed = Cli::parse_from([
        "palyra",
        "browser",
        "status",
        "--endpoint",
        "http://127.0.0.1:7543",
        "--health-url",
        "http://127.0.0.1:7143",
        "--token",
        "browser-token",
    ]);
    assert_eq!(
        parsed.command,
        Command::Browser {
            command: BrowserCommand::Status {
                endpoint: Some("http://127.0.0.1:7543".to_owned()),
                health_url: Some("http://127.0.0.1:7143".to_owned()),
                token: Some("browser-token".to_owned()),
            }
        }
    );
}

#[test]
fn parse_browser_session_create() {
    let parsed = Cli::parse_from([
        "palyra",
        "browser",
        "session",
        "create",
        "--principal",
        "user:browser",
        "--channel",
        "cli:test",
        "--idle-ttl-ms",
        "60000",
        "--allow-private-targets",
        "--allow-downloads",
        "--allow-domain",
        "example.com",
        "--allow-domain",
        "docs.palyra.dev",
        "--persistence-enabled",
        "--persistence-id",
        "profile-cache",
        "--profile-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--private-profile",
    ]);
    assert_eq!(
        parsed.command,
        Command::Browser {
            command: BrowserCommand::Session {
                command: BrowserSessionCommand::Create {
                    principal: Some("user:browser".to_owned()),
                    channel: Some("cli:test".to_owned()),
                    idle_ttl_ms: Some(60000),
                    allow_private_targets: true,
                    allow_downloads: true,
                    action_allowed_domains: vec![
                        "example.com".to_owned(),
                        "docs.palyra.dev".to_owned(),
                    ],
                    persistence_enabled: true,
                    persistence_id: Some("profile-cache".to_owned()),
                    profile_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
                    private_profile: true,
                },
            }
        }
    );
}

#[test]
fn parse_browser_snapshot_with_output() {
    let parsed = Cli::parse_from([
        "palyra",
        "browser",
        "snapshot",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--include-dom-snapshot",
        "--include-visible-text",
        "--max-dom-snapshot-bytes",
        "8192",
        "--max-visible-text-bytes",
        "4096",
        "--output",
        "artifacts/browser-snapshot.json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Browser {
            command: BrowserCommand::Snapshot {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                include_dom_snapshot: true,
                include_accessibility_tree: false,
                include_visible_text: true,
                max_dom_snapshot_bytes: Some(8192),
                max_accessibility_tree_bytes: None,
                max_visible_text_bytes: Some(4096),
                output: Some("artifacts/browser-snapshot.json".to_owned()),
            }
        }
    );
}

#[test]
fn parse_browser_permissions_set() {
    let parsed = Cli::parse_from([
        "palyra",
        "browser",
        "permissions",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "set",
        "--camera",
        "allow",
        "--microphone",
        "deny",
        "--location",
        "default",
        "--reset-to-default",
    ]);
    assert_eq!(
        parsed.command,
        Command::Browser {
            command: BrowserCommand::Permissions {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                command: BrowserPermissionsCommand::Set {
                    camera: Some("allow".to_owned()),
                    microphone: Some("deny".to_owned()),
                    location: Some("default".to_owned()),
                    reset_to_default: true,
                },
            }
        }
    );
}

#[test]
fn parse_tunnel_command_with_overrides() {
    let parsed = Cli::parse_from([
        "palyra",
        "tunnel",
        "--ssh",
        "ops@example.com",
        "--remote-port",
        "7442",
        "--local-port",
        "17442",
        "--open",
        "--identity-file",
        "C:/Users/test/.ssh/id_ed25519",
    ]);
    assert_eq!(
        parsed.command,
        Command::Tunnel {
            ssh: "ops@example.com".to_owned(),
            remote_port: 7442,
            local_port: 17442,
            open: true,
            identity_file: Some("C:/Users/test/.ssh/id_ed25519".to_owned()),
        }
    );
}

#[test]
fn parse_tunnel_command_defaults_ports_to_gateway_admin() {
    let parsed = Cli::parse_from(["palyra", "tunnel", "--ssh", "ops@example.com"]);
    assert_eq!(
        parsed.command,
        Command::Tunnel {
            ssh: "ops@example.com".to_owned(),
            remote_port: 7142,
            local_port: 7142,
            open: false,
            identity_file: None,
        }
    );
}

#[test]
fn parse_completion_powershell() {
    let parsed = Cli::parse_from(["palyra", "completion", "--shell", "powershell"]);
    assert_eq!(parsed.command, Command::Completion { shell: CompletionShell::Powershell });
}

#[test]
fn parse_onboarding_wizard_with_custom_path() {
    let parsed = Cli::parse_from([
        "palyra",
        "onboarding",
        "wizard",
        "--path",
        "config/palyra.toml",
        "--force",
        "--flow",
        "manual",
        "--non-interactive",
        "--accept-risk",
        "--auth-method",
        "api-key",
        "--api-key-env",
        "OPENAI_API_KEY",
        "--bind-profile",
        "public-tls",
        "--daemon-port",
        "7145",
        "--grpc-port",
        "7445",
        "--quic-port",
        "7446",
        "--tls-scaffold",
        "self-signed",
        "--remote-base-url",
        "https://dashboard.example.com/",
        "--remote-verification",
        "server-cert",
        "--pinned-server-cert-sha256",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "--skip-health",
        "--skip-channels",
    ]);
    assert_eq!(
        parsed.command,
        Command::Onboarding {
            command: OnboardingCommand::Wizard {
                path: Some("config/palyra.toml".to_owned()),
                force: true,
                options: Box::new(WizardOverridesArg {
                    flow: Some(OnboardingFlowArg::Manual),
                    non_interactive: true,
                    accept_risk: true,
                    json: false,
                    workspace_root: None,
                    auth_method: Some(OnboardingAuthMethodArg::ApiKey),
                    api_key_env: Some("OPENAI_API_KEY".to_owned()),
                    api_key_stdin: false,
                    api_key_prompt: false,
                    deployment_profile: None,
                    bind_profile: Some(GatewayBindProfileArg::PublicTls),
                    daemon_port: Some(7145),
                    grpc_port: Some(7445),
                    quic_port: Some(7446),
                    tls_scaffold: Some(InitTlsScaffoldArg::SelfSigned),
                    tls_cert_path: None,
                    tls_key_path: None,
                    remote_base_url: Some("https://dashboard.example.com/".to_owned()),
                    admin_token_env: None,
                    admin_token_stdin: false,
                    admin_token_prompt: false,
                    remote_verification: Some(RemoteVerificationModeArg::ServerCert),
                    pinned_server_cert_sha256: Some(
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_owned(),
                    ),
                    pinned_gateway_ca_sha256: None,
                    ssh_target: None,
                    skip_health: true,
                    skip_channels: true,
                    skip_skills: false,
                }),
            }
        }
    );
}

#[test]
fn parse_setup_wizard_with_overrides() {
    let parsed = Cli::parse_from([
        "palyra",
        "setup",
        "--wizard",
        "--mode",
        "remote",
        "--tls-scaffold",
        "bring-your-own",
        "--flow",
        "remote",
        "--non-interactive",
        "--accept-risk",
        "--remote-base-url",
        "https://dashboard.example.com/",
        "--remote-verification",
        "gateway-ca",
        "--pinned-gateway-ca-sha256",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    ]);
    assert_eq!(
        parsed.command,
        Command::Setup {
            mode: InitModeArg::Remote,
            path: None,
            force: false,
            tls_scaffold: InitTlsScaffoldArg::BringYourOwn,
            wizard: true,
            wizard_options: SetupWizardOverridesArg {
                flow: Some(OnboardingFlowArg::Remote),
                non_interactive: true,
                accept_risk: true,
                json: false,
                workspace_root: None,
                auth_method: None,
                api_key_env: None,
                api_key_stdin: false,
                api_key_prompt: false,
                deployment_profile: None,
                bind_profile: None,
                daemon_port: None,
                grpc_port: None,
                quic_port: None,
                tls_cert_path: None,
                tls_key_path: None,
                remote_base_url: Some("https://dashboard.example.com/".to_owned()),
                admin_token_env: None,
                admin_token_stdin: false,
                admin_token_prompt: false,
                remote_verification: Some(RemoteVerificationModeArg::GatewayCa),
                pinned_server_cert_sha256: None,
                pinned_gateway_ca_sha256: Some(
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                ),
                ssh_target: None,
                skip_health: false,
                skip_channels: false,
                skip_skills: false,
            }
        }
    );
}

#[test]
fn parse_configure_with_sections_and_remote_settings() {
    let parsed = Cli::parse_from([
        "palyra",
        "configure",
        "--section",
        "workspace",
        "--section",
        "gateway",
        "--non-interactive",
        "--accept-risk",
        "--workspace-root",
        "C:/workspace",
        "--bind-profile",
        "public-tls",
        "--remote-base-url",
        "https://dashboard.example.com/",
        "--remote-verification",
        "server-cert",
        "--pinned-server-cert-sha256",
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "--skip-skills",
    ]);
    assert_eq!(
        parsed.command,
        Command::Configure {
            path: None,
            sections: vec![ConfigureSectionArg::Workspace, ConfigureSectionArg::Gateway],
            deployment_profile: None,
            non_interactive: true,
            accept_risk: true,
            json: false,
            workspace_root: Some("C:/workspace".to_owned()),
            auth_method: None,
            api_key_env: None,
            api_key_stdin: false,
            api_key_prompt: false,
            bind_profile: Some(GatewayBindProfileArg::PublicTls),
            daemon_port: None,
            grpc_port: None,
            quic_port: None,
            tls_scaffold: None,
            tls_cert_path: None,
            tls_key_path: None,
            remote_base_url: Some("https://dashboard.example.com/".to_owned()),
            admin_token_env: None,
            admin_token_stdin: false,
            admin_token_prompt: false,
            remote_verification: Some(RemoteVerificationModeArg::ServerCert),
            pinned_server_cert_sha256: Some(
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
            ),
            pinned_gateway_ca_sha256: None,
            ssh_target: None,
            skip_health: false,
            skip_channels: false,
            skip_skills: true,
        }
    );
}

#[test]
fn parse_daemon_status_with_url() {
    let parsed = Cli::parse_from(["palyra", "daemon", "status", "--url", "http://127.0.0.1:7142"]);
    assert_eq!(
        parsed.command,
        Command::Gateway {
            command: DaemonCommand::Status {
                url: Some("http://127.0.0.1:7142".to_owned()),
                json: false,
            }
        }
    );
}

#[test]
fn parse_daemon_install_with_service_options() {
    let parsed = Cli::parse_from([
        "palyra",
        "daemon",
        "install",
        "--service-name",
        "PalyraGateway",
        "--bin-path",
        "./target/debug/palyrad",
        "--log-dir",
        "./logs",
        "--start",
    ]);
    assert_eq!(
        parsed.command,
        Command::Gateway {
            command: DaemonCommand::Install {
                service_name: Some("PalyraGateway".to_owned()),
                bin_path: Some("./target/debug/palyrad".to_owned()),
                log_dir: Some("./logs".to_owned()),
                start: true,
            }
        }
    );
}

#[test]
fn parse_daemon_service_actions() {
    let start = Cli::parse_from(["palyra", "daemon", "start"]);
    assert_eq!(start.command, Command::Gateway { command: DaemonCommand::Start });

    let stop = Cli::parse_from(["palyra", "daemon", "stop"]);
    assert_eq!(stop.command, Command::Gateway { command: DaemonCommand::Stop });

    let restart = Cli::parse_from(["palyra", "daemon", "restart"]);
    assert_eq!(restart.command, Command::Gateway { command: DaemonCommand::Restart });

    let uninstall = Cli::parse_from(["palyra", "daemon", "uninstall"]);
    assert_eq!(uninstall.command, Command::Gateway { command: DaemonCommand::Uninstall });
}

#[test]
fn parse_daemon_logs_with_follow() {
    let parsed = Cli::parse_from([
        "palyra",
        "daemon",
        "logs",
        "--db-path",
        "state/journal.sqlite3",
        "--lines",
        "75",
        "--follow",
        "--poll-interval-ms",
        "2000",
    ]);
    assert_eq!(
        parsed.command,
        Command::Gateway {
            command: DaemonCommand::Logs {
                db_path: Some("state/journal.sqlite3".to_owned()),
                lines: 75,
                follow: true,
                poll_interval_ms: 2000,
            }
        }
    );
}

#[test]
fn parse_daemon_probe_with_context_and_dashboard_options() {
    let parsed = Cli::parse_from([
        "palyra",
        "daemon",
        "probe",
        "--url",
        "http://127.0.0.1:7142",
        "--grpc-url",
        "http://127.0.0.1:7443",
        "--token",
        "test-token",
        "--principal",
        "user:ops",
        "--device-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--channel",
        "cli",
        "--path",
        "./palyra.toml",
        "--verify-remote",
        "--identity-store-dir",
        "./identity",
    ]);
    assert_eq!(
        parsed.command,
        Command::Gateway {
            command: DaemonCommand::Probe {
                url: Some("http://127.0.0.1:7142".to_owned()),
                grpc_url: Some("http://127.0.0.1:7443".to_owned()),
                token: Some("test-token".to_owned()),
                principal: Some("user:ops".to_owned()),
                device_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
                channel: Some("cli".to_owned()),
                path: Some("./palyra.toml".to_owned()),
                verify_remote: true,
                identity_store_dir: Some("./identity".to_owned()),
            }
        }
    );
}

#[test]
fn parse_daemon_call_with_params() {
    let parsed = Cli::parse_from([
        "palyra",
        "daemon",
        "call",
        "run.status",
        "--params",
        "{\"run_id\":\"01ARZ3NDEKTSV4RRFFQ69G5FAX\"}",
        "--token",
        "test-token",
    ]);
    assert_eq!(
        parsed.command,
        Command::Gateway {
            command: DaemonCommand::Call {
                method: "run.status".to_owned(),
                params: Some("{\"run_id\":\"01ARZ3NDEKTSV4RRFFQ69G5FAX\"}".to_owned()),
                url: None,
                grpc_url: None,
                token: Some("test-token".to_owned()),
                principal: None,
                device_id: None,
                channel: None,
            }
        }
    );
}

#[test]
fn parse_daemon_usage_cost_with_days() {
    let parsed = Cli::parse_from([
        "palyra",
        "daemon",
        "usage-cost",
        "--db-path",
        "data/journal.sqlite3",
        "--days",
        "7",
    ]);
    assert_eq!(
        parsed.command,
        Command::Gateway {
            command: DaemonCommand::UsageCost {
                db_path: Some("data/journal.sqlite3".to_owned()),
                days: 7,
            }
        }
    );
}

#[test]
fn parse_support_bundle_export_with_overrides() {
    let parsed = Cli::parse_from([
        "palyra",
        "support-bundle",
        "export",
        "--output",
        "artifacts/support-bundle.json",
        "--max-bytes",
        "131072",
        "--journal-hash-limit",
        "48",
        "--error-limit",
        "20",
    ]);
    assert_eq!(
        parsed.command,
        Command::SupportBundle {
            command: SupportBundleCommand::Export {
                output: Some("artifacts/support-bundle.json".to_owned()),
                max_bytes: 131_072,
                journal_hash_limit: 48,
                error_limit: 20,
            },
        }
    );
}

#[test]
fn parse_support_bundle_replay_workflows() {
    let export = Cli::parse_from([
        "palyra",
        "support-bundle",
        "replay-export",
        "--run-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FA2",
        "--output",
        "artifacts/replay.json",
        "--journal-db",
        "data/journal.sqlite3",
        "--max-events",
        "256",
    ]);
    assert_eq!(
        export.command,
        Command::SupportBundle {
            command: SupportBundleCommand::ReplayExport {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FA2".to_owned(),
                output: "artifacts/replay.json".to_owned(),
                journal_db: Some("data/journal.sqlite3".to_owned()),
                max_events: 256,
            },
        }
    );

    let replay = Cli::parse_from([
        "palyra",
        "support-bundle",
        "replay-run",
        "--input",
        "artifacts/replay.json",
        "--diff-output",
        "artifacts/replay-diff.json",
    ]);
    assert_eq!(
        replay.command,
        Command::SupportBundle {
            command: SupportBundleCommand::ReplayRun {
                input: "artifacts/replay.json".to_owned(),
                diff_output: Some("artifacts/replay-diff.json".to_owned()),
            },
        }
    );
}

#[test]
fn parse_daemon_admin_status_with_explicit_context() {
    let parsed = Cli::parse_from([
        "palyra",
        "daemon",
        "admin-status",
        "--url",
        "http://127.0.0.1:7142",
        "--token",
        "test-token",
        "--principal",
        "user:ops",
        "--device-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--channel",
        "cli",
    ]);
    assert_eq!(
        parsed.command,
        Command::Gateway {
            command: DaemonCommand::AdminStatus {
                url: Some("http://127.0.0.1:7142".to_owned()),
                token: Some("test-token".to_owned()),
                principal: Some("user:ops".to_owned()),
                device_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
                channel: Some("cli".to_owned()),
            }
        }
    );
}

#[test]
fn parse_daemon_journal_recent_with_limit() {
    let parsed = Cli::parse_from([
        "palyra",
        "daemon",
        "journal-recent",
        "--url",
        "http://127.0.0.1:7142",
        "--token",
        "test-token",
        "--principal",
        "user:ops",
        "--device-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--channel",
        "cli",
        "--limit",
        "25",
    ]);
    assert_eq!(
        parsed.command,
        Command::Gateway {
            command: DaemonCommand::JournalRecent {
                url: Some("http://127.0.0.1:7142".to_owned()),
                token: Some("test-token".to_owned()),
                principal: Some("user:ops".to_owned()),
                device_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
                channel: Some("cli".to_owned()),
                limit: Some(25),
            }
        }
    );
}

#[test]
fn parse_daemon_journal_vacuum_with_db_path() {
    let parsed = Cli::parse_from([
        "palyra",
        "daemon",
        "journal-vacuum",
        "--db-path",
        "data/journal.sqlite3",
    ]);
    assert_eq!(
        parsed.command,
        Command::Gateway {
            command: DaemonCommand::JournalVacuum {
                db_path: Some("data/journal.sqlite3".to_owned())
            }
        }
    );
}

#[test]
fn parse_daemon_journal_checkpoint_with_mode() {
    let parsed = Cli::parse_from([
        "palyra",
        "daemon",
        "journal-checkpoint",
        "--db-path",
        "data/journal.sqlite3",
        "--mode",
        "restart",
    ]);
    assert_eq!(
        parsed.command,
        Command::Gateway {
            command: DaemonCommand::JournalCheckpoint {
                db_path: Some("data/journal.sqlite3".to_owned()),
                mode: JournalCheckpointModeArg::Restart,
                sign: false,
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                identity_store_dir: None,
                attestation_out: None,
                json: false,
            }
        }
    );
}

#[test]
fn parse_daemon_journal_checkpoint_with_signature_options() {
    let parsed = Cli::parse_from([
        "palyra",
        "daemon",
        "journal-checkpoint",
        "--db-path",
        "data/journal.sqlite3",
        "--mode",
        "truncate",
        "--sign",
        "--device-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAX",
        "--identity-store-dir",
        "state/identity",
        "--attestation-out",
        "artifacts/journal-checkpoint.json",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Gateway {
            command: DaemonCommand::JournalCheckpoint {
                db_path: Some("data/journal.sqlite3".to_owned()),
                mode: JournalCheckpointModeArg::Truncate,
                sign: true,
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                identity_store_dir: Some("state/identity".to_owned()),
                attestation_out: Some("artifacts/journal-checkpoint.json".to_owned()),
                json: true,
            }
        }
    );
}

#[test]
fn parse_daemon_run_status_with_run_id() {
    let parsed = Cli::parse_from([
        "palyra",
        "daemon",
        "run-status",
        "--url",
        "http://127.0.0.1:7142",
        "--token",
        "test-token",
        "--principal",
        "user:ops",
        "--device-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--channel",
        "cli",
        "--run-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAX",
    ]);
    assert_eq!(
        parsed.command,
        Command::Gateway {
            command: DaemonCommand::RunStatus {
                url: Some("http://127.0.0.1:7142".to_owned()),
                token: Some("test-token".to_owned()),
                principal: Some("user:ops".to_owned()),
                device_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
                channel: Some("cli".to_owned()),
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
            }
        }
    );
}

#[test]
fn parse_daemon_run_cancel_with_reason() {
    let parsed = Cli::parse_from([
        "palyra",
        "daemon",
        "run-cancel",
        "--url",
        "http://127.0.0.1:7142",
        "--token",
        "test-token",
        "--run-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAX",
        "--reason",
        "operator requested",
    ]);
    assert_eq!(
        parsed.command,
        Command::Gateway {
            command: DaemonCommand::RunCancel {
                url: Some("http://127.0.0.1:7142".to_owned()),
                token: Some("test-token".to_owned()),
                principal: None,
                device_id: None,
                channel: None,
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                reason: Some("operator requested".to_owned()),
            }
        }
    );
}

#[test]
fn parse_daemon_dashboard_url_with_verification_options() {
    let parsed = Cli::parse_from([
        "palyra",
        "daemon",
        "dashboard-url",
        "--path",
        "config/palyra.toml",
        "--verify-remote",
        "--identity-store-dir",
        "state/identity",
        "--open",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Gateway {
            command: DaemonCommand::DashboardUrl {
                path: Some("config/palyra.toml".to_owned()),
                verify_remote: true,
                identity_store_dir: Some("state/identity".to_owned()),
                open: true,
                json: true,
            }
        }
    );
}

#[test]
fn parse_policy_explain() {
    let parsed = Cli::parse_from([
        "palyra",
        "policy",
        "explain",
        "--principal",
        "user:test",
        "--action",
        "tool.execute",
        "--resource",
        "tool:filesystem",
    ]);
    assert_eq!(
        parsed.command,
        Command::Policy {
            command: PolicyCommand::Explain {
                principal: "user:test".to_owned(),
                action: "tool.execute".to_owned(),
                resource: "tool:filesystem".to_owned(),
            }
        }
    );
}

#[test]
fn parse_protocol_version() {
    let parsed = Cli::parse_from(["palyra", "protocol", "version"]);
    assert_eq!(parsed.command, Command::Protocol { command: ProtocolCommand::Version });
}

#[test]
fn parse_protocol_validate_id() {
    let parsed = Cli::parse_from([
        "palyra",
        "protocol",
        "validate-id",
        "--id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    ]);
    assert_eq!(
        parsed.command,
        Command::Protocol {
            command: ProtocolCommand::ValidateId { id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned() }
        }
    );
}

#[test]
fn parse_patch_apply_from_stdin() {
    let parsed = Cli::parse_from(["palyra", "patch", "apply", "--stdin", "--dry-run", "--json"]);
    assert_eq!(
        parsed.command,
        Command::Patch { command: PatchCommand::Apply { stdin: true, dry_run: true, json: true } }
    );
}

#[test]
fn parse_config_validate_with_path() {
    let parsed = Cli::parse_from(["palyra", "config", "validate", "--path", "custom.toml"]);
    assert_eq!(
        parsed.command,
        Command::Config {
            command: Some(ConfigCommand::Validate { path: Some("custom.toml".to_owned()) })
        }
    );
}

#[test]
fn parse_config_without_subcommand_defaults_to_none() {
    let parsed = Cli::parse_from(["palyra", "config"]);
    assert_eq!(parsed.command, Command::Config { command: None });
}

#[test]
fn parse_config_status_with_json() {
    let parsed = Cli::parse_from(["palyra", "config", "status", "--json"]);
    assert_eq!(
        parsed.command,
        Command::Config { command: Some(ConfigCommand::Status { path: None, json: true }) }
    );
}

#[test]
fn parse_config_path_with_explicit_path() {
    let parsed = Cli::parse_from(["palyra", "config", "path", "--path", "custom.toml", "--json"]);
    assert_eq!(
        parsed.command,
        Command::Config {
            command: Some(ConfigCommand::Path { path: Some("custom.toml".to_owned()), json: true })
        }
    );
}

#[test]
fn parse_config_get_with_key() {
    let parsed = Cli::parse_from([
        "palyra",
        "config",
        "get",
        "--path",
        "custom.toml",
        "--key",
        "daemon.port",
    ]);
    assert_eq!(
        parsed.command,
        Command::Config {
            command: Some(ConfigCommand::Get {
                path: Some("custom.toml".to_owned()),
                key: "daemon.port".to_owned(),
                show_secrets: false,
            })
        }
    );
}

#[test]
fn parse_config_get_with_show_secrets() {
    let parsed = Cli::parse_from([
        "palyra",
        "config",
        "get",
        "--path",
        "custom.toml",
        "--key",
        "admin.auth_token",
        "--show-secrets",
    ]);
    assert_eq!(
        parsed.command,
        Command::Config {
            command: Some(ConfigCommand::Get {
                path: Some("custom.toml".to_owned()),
                key: "admin.auth_token".to_owned(),
                show_secrets: true,
            })
        }
    );
}

#[test]
fn parse_config_list_with_show_secrets() {
    let parsed =
        Cli::parse_from(["palyra", "config", "list", "--path", "custom.toml", "--show-secrets"]);
    assert_eq!(
        parsed.command,
        Command::Config {
            command: Some(ConfigCommand::List {
                path: Some("custom.toml".to_owned()),
                show_secrets: true,
            })
        }
    );
}

#[test]
fn parse_config_set_with_backups() {
    let parsed = Cli::parse_from([
        "palyra",
        "config",
        "set",
        "--path",
        "custom.toml",
        "--key",
        "daemon.port",
        "--value",
        "7443",
        "--backups",
        "7",
    ]);
    assert_eq!(
        parsed.command,
        Command::Config {
            command: Some(ConfigCommand::Set {
                path: Some("custom.toml".to_owned()),
                key: "daemon.port".to_owned(),
                value: "7443".to_owned(),
                backups: 7,
            })
        }
    );
}

#[test]
fn parse_config_unset_with_defaults() {
    let parsed = Cli::parse_from([
        "palyra",
        "config",
        "unset",
        "--path",
        "custom.toml",
        "--key",
        "daemon.port",
    ]);
    assert_eq!(
        parsed.command,
        Command::Config {
            command: Some(ConfigCommand::Unset {
                path: Some("custom.toml".to_owned()),
                key: "daemon.port".to_owned(),
                backups: 5,
            })
        }
    );
}

#[test]
fn parse_config_migrate_with_backups() {
    let parsed =
        Cli::parse_from(["palyra", "config", "migrate", "--path", "custom.toml", "--backups", "3"]);
    assert_eq!(
        parsed.command,
        Command::Config {
            command: Some(ConfigCommand::Migrate {
                path: Some("custom.toml".to_owned()),
                backups: 3,
            })
        }
    );
}

#[test]
fn parse_config_recover_with_backup_index() {
    let parsed = Cli::parse_from([
        "palyra",
        "config",
        "recover",
        "--path",
        "custom.toml",
        "--backup",
        "2",
        "--backups",
        "4",
    ]);
    assert_eq!(
        parsed.command,
        Command::Config {
            command: Some(ConfigCommand::Recover {
                path: Some("custom.toml".to_owned()),
                backup: 2,
                backups: 4,
            })
        }
    );
}

#[test]
fn parse_models_set_embeddings() {
    let parsed = Cli::parse_from([
        "palyra",
        "models",
        "set-embeddings",
        "text-embedding-3-small",
        "--dims",
        "1536",
        "--path",
        "custom.toml",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Models {
            command: ModelsCommand::SetEmbeddings {
                model: "text-embedding-3-small".to_owned(),
                dims: Some(1536),
                path: Some("custom.toml".to_owned()),
                backups: 5,
                json: true,
            }
        }
    );
}

#[test]
fn parse_models_test_connection() {
    let parsed = Cli::parse_from([
        "palyra",
        "models",
        "test-connection",
        "--provider",
        "anthropic-primary",
        "--timeout-ms",
        "7000",
        "--refresh",
        "--path",
        "custom.toml",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Models {
            command: ModelsCommand::TestConnection {
                path: Some("custom.toml".to_owned()),
                provider: Some("anthropic-primary".to_owned()),
                timeout_ms: 7000,
                refresh: true,
                json: true,
            }
        }
    );
}

#[test]
fn parse_models_explain() {
    let parsed = Cli::parse_from([
        "palyra",
        "models",
        "explain",
        "--model",
        "claude-3-5-sonnet-latest",
        "--json-mode",
        "--vision",
        "--path",
        "custom.toml",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Models {
            command: ModelsCommand::Explain {
                path: Some("custom.toml".to_owned()),
                model: Some("claude-3-5-sonnet-latest".to_owned()),
                json_mode: true,
                vision: true,
                json: true,
            }
        }
    );
}

#[test]
fn parse_skills_package_build() {
    let parsed = Cli::parse_from([
        "palyra",
        "skills",
        "package",
        "build",
        "--manifest",
        "examples/skill.toml",
        "--module",
        "build/module.wasm",
        "--asset",
        "assets/prompt.txt",
        "--sbom",
        "build/sbom.cdx.json",
        "--provenance",
        "build/provenance.json",
        "--output",
        "dist/acme.palyra-skill",
        "--signing-key-stdin",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Skills {
            command: SkillsCommand::Package {
                command: SkillsPackageCommand::Build {
                    manifest: "examples/skill.toml".to_owned(),
                    module: vec!["build/module.wasm".to_owned()],
                    asset: vec!["assets/prompt.txt".to_owned()],
                    sbom: "build/sbom.cdx.json".to_owned(),
                    provenance: "build/provenance.json".to_owned(),
                    output: "dist/acme.palyra-skill".to_owned(),
                    signing_key_vault_ref: None,
                    signing_key_stdin: true,
                    json: true,
                },
            },
        }
    );
}

#[test]
fn parse_skills_package_verify() {
    let parsed = Cli::parse_from([
        "palyra",
        "skills",
        "package",
        "verify",
        "--artifact",
        "dist/acme.palyra-skill",
        "--trust-store",
        "state/skills-trust.json",
        "--trusted-publisher",
        "acme=001122",
        "--allow-tofu",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Skills {
            command: SkillsCommand::Package {
                command: SkillsPackageCommand::Verify {
                    artifact: "dist/acme.palyra-skill".to_owned(),
                    trust_store: Some("state/skills-trust.json".to_owned()),
                    trusted_publishers: vec!["acme=001122".to_owned()],
                    allow_tofu: true,
                    json: true,
                },
            },
        }
    );
}

#[test]
fn parse_skill_alias_install_from_artifact() {
    let parsed = Cli::parse_from([
        "palyra",
        "skill",
        "install",
        "--artifact",
        "dist/acme.palyra-skill",
        "--skills-dir",
        "state/skills",
        "--trust-store",
        "state/skills-trust.json",
        "--trusted-publisher",
        "acme=001122",
        "--allow-untrusted",
        "--non-interactive",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Skills {
            command: SkillsCommand::Install {
                artifact: Some("dist/acme.palyra-skill".to_owned()),
                registry_dir: None,
                registry_url: None,
                skill_id: None,
                version: None,
                registry_ca_cert: None,
                skills_dir: Some("state/skills".to_owned()),
                trust_store: Some("state/skills-trust.json".to_owned()),
                trusted_publishers: vec!["acme=001122".to_owned()],
                allow_untrusted: true,
                non_interactive: true,
                json: true,
            },
        }
    );
}

#[test]
fn parse_skills_install_from_remote_registry() {
    let parsed = Cli::parse_from([
        "palyra",
        "skills",
        "install",
        "--registry-url",
        "https://registry.example.com/index.json",
        "--skill-id",
        "acme.echo_http",
        "--version",
        "1.2.3",
        "--registry-ca-cert",
        "certs/registry-ca.pem",
    ]);
    assert_eq!(
        parsed.command,
        Command::Skills {
            command: SkillsCommand::Install {
                artifact: None,
                registry_dir: None,
                registry_url: Some("https://registry.example.com/index.json".to_owned()),
                skill_id: Some("acme.echo_http".to_owned()),
                version: Some("1.2.3".to_owned()),
                registry_ca_cert: Some("certs/registry-ca.pem".to_owned()),
                skills_dir: None,
                trust_store: None,
                trusted_publishers: Vec::new(),
                allow_untrusted: false,
                non_interactive: false,
                json: false,
            },
        }
    );
}

#[test]
fn parse_skills_update_from_local_registry() {
    let parsed = Cli::parse_from([
        "palyra",
        "skills",
        "update",
        "--registry-dir",
        "registry",
        "--skill-id",
        "acme.echo_http",
        "--skills-dir",
        "state/skills",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Skills {
            command: SkillsCommand::Update {
                registry_dir: Some("registry".to_owned()),
                registry_url: None,
                skill_id: "acme.echo_http".to_owned(),
                version: None,
                registry_ca_cert: None,
                skills_dir: Some("state/skills".to_owned()),
                trust_store: None,
                trusted_publishers: Vec::new(),
                allow_untrusted: false,
                non_interactive: false,
                json: true,
            },
        }
    );
}

#[test]
fn parse_skills_remove_list_and_verify() {
    let remove = Cli::parse_from([
        "palyra",
        "skills",
        "remove",
        "acme.echo_http",
        "--version",
        "1.2.3",
        "--skills-dir",
        "state/skills",
        "--json",
    ]);
    assert_eq!(
        remove.command,
        Command::Skills {
            command: SkillsCommand::Remove {
                skill_id: "acme.echo_http".to_owned(),
                version: Some("1.2.3".to_owned()),
                skills_dir: Some("state/skills".to_owned()),
                json: true,
            },
        }
    );

    let list = Cli::parse_from(["palyra", "skills", "list", "--skills-dir", "state/skills"]);
    assert_eq!(
        list.command,
        Command::Skills {
            command: SkillsCommand::List {
                skills_dir: Some("state/skills".to_owned()),
                publisher: None,
                current_only: false,
                quarantined_only: false,
                eligible_only: false,
                json: false,
            },
        }
    );

    let filtered_list = Cli::parse_from([
        "palyra",
        "skills",
        "list",
        "--skills-dir",
        "state/skills",
        "--publisher",
        "acme",
        "--current-only",
        "--quarantined-only",
        "--eligible-only",
        "--json",
    ]);
    assert_eq!(
        filtered_list.command,
        Command::Skills {
            command: SkillsCommand::List {
                skills_dir: Some("state/skills".to_owned()),
                publisher: Some("acme".to_owned()),
                current_only: true,
                quarantined_only: true,
                eligible_only: true,
                json: true,
            },
        }
    );

    let info = Cli::parse_from([
        "palyra",
        "skills",
        "info",
        "acme.echo_http",
        "--version",
        "1.2.3",
        "--skills-dir",
        "state/skills",
        "--json",
    ]);
    assert_eq!(
        info.command,
        Command::Skills {
            command: SkillsCommand::Info {
                skill_id: "acme.echo_http".to_owned(),
                version: Some("1.2.3".to_owned()),
                skills_dir: Some("state/skills".to_owned()),
                json: true,
            },
        }
    );

    let check = Cli::parse_from([
        "palyra",
        "skills",
        "check",
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
        check.command,
        Command::Skills {
            command: SkillsCommand::Check {
                skill_id: Some("acme.echo_http".to_owned()),
                version: Some("1.2.3".to_owned()),
                skills_dir: Some("state/skills".to_owned()),
                trust_store: Some("state/skills-trust.json".to_owned()),
                trusted_publishers: vec!["acme=001122".to_owned()],
                allow_untrusted: true,
                json: true,
            },
        }
    );

    let verify = Cli::parse_from([
        "palyra",
        "skills",
        "verify",
        "acme.echo_http",
        "--version",
        "1.2.3",
        "--allow-untrusted",
        "--json",
    ]);
    assert_eq!(
        verify.command,
        Command::Skills {
            command: SkillsCommand::Verify {
                skill_id: "acme.echo_http".to_owned(),
                version: Some("1.2.3".to_owned()),
                skills_dir: None,
                trust_store: None,
                trusted_publishers: Vec::new(),
                allow_untrusted: true,
                json: true,
            },
        }
    );
}

#[test]
fn parse_secrets_set_with_stdin() {
    let parsed =
        Cli::parse_from(["palyra", "secrets", "set", "global", "openai_api_key", "--value-stdin"]);
    assert_eq!(
        parsed.command,
        Command::Secrets {
            command: SecretsCommand::Set {
                scope: "global".to_owned(),
                key: "openai_api_key".to_owned(),
                value_stdin: true,
            }
        }
    );
}

#[test]
fn parse_secrets_get_with_reveal() {
    let parsed =
        Cli::parse_from(["palyra", "secrets", "get", "global", "openai_api_key", "--reveal"]);
    assert_eq!(
        parsed.command,
        Command::Secrets {
            command: SecretsCommand::Get {
                scope: "global".to_owned(),
                key: "openai_api_key".to_owned(),
                reveal: true,
            }
        }
    );
}

#[test]
fn parse_secrets_list_scope() {
    let parsed = Cli::parse_from(["palyra", "secrets", "list", "principal:user:ops"]);
    assert_eq!(
        parsed.command,
        Command::Secrets {
            command: SecretsCommand::List { scope: "principal:user:ops".to_owned() }
        }
    );
}

#[test]
fn parse_secrets_delete_scope_key() {
    let parsed = Cli::parse_from(["palyra", "secrets", "delete", "skill:slack", "bot_token"]);
    assert_eq!(
        parsed.command,
        Command::Secrets {
            command: SecretsCommand::Delete {
                scope: "skill:slack".to_owned(),
                key: "bot_token".to_owned(),
            }
        }
    );
}

#[test]
fn parse_secrets_audit_offline_strict_json() {
    let parsed = Cli::parse_from([
        "palyra",
        "secrets",
        "audit",
        "--path",
        "config/palyra.toml",
        "--offline",
        "--strict",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Secrets {
            command: SecretsCommand::Audit {
                path: Some("config/palyra.toml".to_owned()),
                offline: true,
                strict: true,
                json: true,
            }
        }
    );
}

#[test]
fn parse_secrets_apply_offline_strict_json() {
    let parsed = Cli::parse_from([
        "palyra",
        "secrets",
        "apply",
        "--path",
        "config/palyra.toml",
        "--offline",
        "--strict",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Secrets {
            command: SecretsCommand::Apply {
                path: Some("config/palyra.toml".to_owned()),
                offline: true,
                strict: true,
                runtime: false,
                dry_run: false,
                json: true,
            }
        }
    );
}

#[test]
fn parse_secrets_configure_openai_api_key() {
    let parsed = Cli::parse_from([
        "palyra",
        "secrets",
        "configure",
        "openai-api-key",
        "global",
        "openai_api_key",
        "--value-stdin",
        "--path",
        "config/palyra.toml",
        "--backups",
        "3",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Secrets {
            command: SecretsCommand::Configure {
                command: SecretsConfigureCommand::OpenaiApiKey {
                    scope: "global".to_owned(),
                    key: "openai_api_key".to_owned(),
                    value_stdin: true,
                    path: Some("config/palyra.toml".to_owned()),
                    backups: 3,
                    json: true,
                }
            }
        }
    );
}

#[test]
fn parse_secrets_configure_browser_state_key() {
    let parsed = Cli::parse_from([
        "palyra",
        "secrets",
        "configure",
        "browser-state-key",
        "global",
        "browser_state_key",
        "--value-stdin",
        "--path",
        "config/palyra.toml",
        "--backups",
        "2",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Secrets {
            command: SecretsCommand::Configure {
                command: SecretsConfigureCommand::BrowserStateKey {
                    scope: "global".to_owned(),
                    key: "browser_state_key".to_owned(),
                    value_stdin: true,
                    path: Some("config/palyra.toml".to_owned()),
                    backups: 2,
                    json: true,
                }
            }
        }
    );
}

#[test]
fn parse_webhooks_add_and_test() {
    let add = Cli::parse_from([
        "palyra",
        "webhooks",
        "add",
        "github_repo_a",
        "github",
        "--secret-ref",
        "global/github_repo_a",
        "--allow-event",
        "push",
        "--allow-source",
        "github.repo_a",
        "--require-signature",
        "--json",
    ]);
    assert_eq!(
        add.command,
        Command::Webhooks {
            command: WebhooksCommand::Add {
                integration_id: "github_repo_a".to_owned(),
                provider: "github".to_owned(),
                display_name: None,
                secret_vault_ref: "global/github_repo_a".to_owned(),
                allowed_events: vec!["push".to_owned()],
                allowed_sources: vec!["github.repo_a".to_owned()],
                disabled: false,
                require_signature: true,
                max_payload_bytes: None,
                json: true,
            }
        }
    );

    let test = Cli::parse_from([
        "palyra",
        "webhooks",
        "test",
        "github_repo_a",
        "--payload-stdin",
        "--json",
    ]);
    assert_eq!(
        test.command,
        Command::Webhooks {
            command: WebhooksCommand::Test {
                integration_id: "github_repo_a".to_owned(),
                payload_stdin: true,
                payload_file: None,
                json: true,
            }
        }
    );
}

#[test]
fn parse_webhooks_verify_alias_and_payload_source_conflict() {
    let verify =
        Cli::parse_from(["palyra", "webhooks", "verify", "github_repo_a", "--payload-stdin"]);
    assert_eq!(
        verify.command,
        Command::Webhooks {
            command: WebhooksCommand::Test {
                integration_id: "github_repo_a".to_owned(),
                payload_stdin: true,
                payload_file: None,
                json: false,
            }
        }
    );

    let conflict = Cli::try_parse_from([
        "palyra",
        "webhooks",
        "test",
        "github_repo_a",
        "--payload-stdin",
        "--payload-file",
        "fixtures/webhook.json",
    ]);
    assert!(conflict.is_err(), "webhook test payload sources must remain mutually exclusive");
}

#[test]
fn parse_plugins_inspect_and_check_commands() {
    let info = Cli::parse_from(["palyra", "plugins", "info", "acme.echo_http_plugin"]);
    match info.command {
        Command::Plugins { command: PluginsCommand::Inspect { plugin_id, json } } => {
            assert_eq!(plugin_id, "acme.echo_http_plugin");
            assert!(!json);
        }
        other => panic!("unexpected inspect parse result: {other:?}"),
    }

    let check = Cli::parse_from(["palyra", "plugins", "check", "acme.echo_http_plugin"]);
    match check.command {
        Command::Plugins { command: PluginsCommand::Check { plugin_id, json } } => {
            assert_eq!(plugin_id, "acme.echo_http_plugin");
            assert!(!json);
        }
        other => panic!("unexpected check parse result: {other:?}"),
    }
}

#[test]
fn parse_security_audit_offline_strict_json() {
    let parsed = Cli::parse_from([
        "palyra",
        "security",
        "audit",
        "--path",
        "config/palyra.toml",
        "--offline",
        "--strict",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Security {
            command: SecurityCommand::Audit {
                path: Some("config/palyra.toml".to_owned()),
                offline: true,
                strict: true,
                json: true,
            }
        }
    );
}

#[test]
fn parse_pairing_pair_with_defaults() {
    let parsed = Cli::parse_from([
        "palyra",
        "pairing",
        "pair",
        "--device-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--proof",
        "123456",
        "--allow-insecure-proof-arg",
        "--approve",
    ]);
    assert_eq!(
        parsed.command,
        Command::Pairing {
            command: PairingCommand::Pair {
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                client_kind: PairingClientKindArg::Node,
                method: PairingMethodArg::Pin,
                proof: Some("123456".to_owned()),
                proof_stdin: false,
                allow_insecure_proof_arg: true,
                store_dir: None,
                approve: true,
                simulate_rotation: false,
            }
        }
    );
}

#[test]
fn parse_pairing_pair_desktop_qr() {
    let parsed = Cli::parse_from([
        "palyra",
        "pairing",
        "pair",
        "--device-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--client-kind",
        "desktop",
        "--method",
        "qr",
        "--proof",
        "0123456789ABCDEF0123456789ABCDEF",
        "--allow-insecure-proof-arg",
        "--store-dir",
        "tmp-identity",
        "--approve",
        "--simulate-rotation",
    ]);
    assert_eq!(
        parsed.command,
        Command::Pairing {
            command: PairingCommand::Pair {
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                client_kind: PairingClientKindArg::Desktop,
                method: PairingMethodArg::Qr,
                proof: Some("0123456789ABCDEF0123456789ABCDEF".to_owned()),
                proof_stdin: false,
                allow_insecure_proof_arg: true,
                store_dir: Some("tmp-identity".to_owned()),
                approve: true,
                simulate_rotation: true,
            }
        }
    );
}

#[test]
fn parse_pairing_pair_with_proof_stdin() {
    let parsed = Cli::parse_from([
        "palyra",
        "pairing",
        "pair",
        "--device-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--proof-stdin",
        "--approve",
    ]);
    assert_eq!(
        parsed.command,
        Command::Pairing {
            command: PairingCommand::Pair {
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                client_kind: PairingClientKindArg::Node,
                method: PairingMethodArg::Pin,
                proof: None,
                proof_stdin: true,
                allow_insecure_proof_arg: false,
                store_dir: None,
                approve: true,
                simulate_rotation: false,
            }
        }
    );
}

#[test]
fn parse_pairing_pair_rejects_proof_without_insecure_ack() {
    let result = Cli::try_parse_from([
        "palyra",
        "pairing",
        "pair",
        "--device-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--proof",
        "123456",
        "--approve",
    ]);
    assert!(result.is_err(), "proof should require explicit insecure acknowledgement flag");
}

#[test]
fn parse_tui_with_session_key() {
    let parsed = Cli::parse_from([
        "palyra",
        "tui",
        "--session-key",
        "ops:triage",
        "--session-label",
        "Ops Triage",
        "--allow-sensitive-tools",
    ]);
    assert_eq!(
        parsed.command,
        Command::Tui {
            command: TuiCommand {
                grpc_url: None,
                token: None,
                principal: None,
                device_id: None,
                channel: None,
                session_id: None,
                session_key: Some("ops:triage".to_owned()),
                session_label: Some("Ops Triage".to_owned()),
                require_existing: false,
                allow_sensitive_tools: true,
                include_archived_sessions: false,
            }
        }
    );
}

#[test]
fn parse_pairing_list_with_filters() {
    let parsed = Cli::parse_from([
        "palyra",
        "pairing",
        "list",
        "--client-kind",
        "node",
        "--state",
        "pending-approval",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Pairing {
            command: PairingCommand::List {
                client_kind: Some("node".to_owned()),
                state: Some(PairingStateArg::PendingApproval),
                json: true,
                ndjson: false,
            }
        }
    );
}

#[test]
fn parse_pairing_code_qr_with_ttl() {
    let parsed = Cli::parse_from([
        "palyra",
        "pairing",
        "code",
        "--method",
        "qr",
        "--issued-by",
        "ops:local",
        "--ttl-ms",
        "60000",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Pairing {
            command: PairingCommand::Code {
                method: PairingMethodArg::Qr,
                issued_by: Some("ops:local".to_owned()),
                ttl_ms: Some(60_000),
                json: true,
            }
        }
    );
}

#[test]
fn parse_devices_revoke_with_reason() {
    let parsed = Cli::parse_from([
        "palyra",
        "devices",
        "revoke",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--reason",
        "compromised",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Devices {
            command: DevicesCommand::Revoke {
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                reason: Some("compromised".to_owned()),
                json: true,
            }
        }
    );
}

#[test]
fn parse_node_install_with_bootstrap_material() {
    let parsed = Cli::parse_from([
        "palyra",
        "node",
        "install",
        "--grpc-url",
        "https://127.0.0.1:7444",
        "--gateway-ca-file",
        "./gateway-ca.pem",
        "--device-id",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "--method",
        "pin",
        "--pairing-code",
        "123456",
        "--start",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Node {
            command: NodeCommand::Install {
                grpc_url: Some("https://127.0.0.1:7444".to_owned()),
                gateway_ca_file: Some("./gateway-ca.pem".to_owned()),
                device_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
                method: Some(PairingMethodArg::Pin),
                pairing_code: Some("123456".to_owned()),
                start: true,
                json: true,
            }
        }
    );
}

#[test]
fn parse_nodes_invoke_with_json_payload() {
    let parsed = Cli::parse_from([
        "palyra",
        "nodes",
        "invoke",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "system.health",
        "--input-json",
        "{\"verbose\":true}",
        "--max-payload-bytes",
        "4096",
        "--json",
    ]);
    assert_eq!(
        parsed.command,
        Command::Nodes {
            command: NodesCommand::Invoke {
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                capability: "system.health".to_owned(),
                input_json: Some("{\"verbose\":true}".to_owned()),
                input_stdin: false,
                max_payload_bytes: Some(4096),
                json: true,
            }
        }
    );
}
