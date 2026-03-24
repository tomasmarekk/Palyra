use clap::Parser;

use super::{
    AgentCommand, AgentsCommand, ApprovalDecisionArg, ApprovalDecisionScopeArg,
    ApprovalExportFormatArg, ApprovalResolveDecisionArg, ApprovalSubjectTypeArg, ApprovalsCommand,
    AuthCommand, AuthCredentialArg, AuthOpenAiCommand, AuthProfilesCommand, AuthProviderArg,
    AuthScopeArg, BrowserCommand, ChannelsCommand, ChannelsDiscordCommand, ChannelsRouterCommand,
    Cli, Command, CompletionShell, ConfigCommand, CronCommand, CronConcurrencyPolicyArg,
    CronMisfirePolicyArg, CronScheduleTypeArg, DaemonCommand, InitModeArg, InitTlsScaffoldArg,
    JournalCheckpointModeArg, MemoryCommand, MemoryScopeArg, MemorySourceArg, ModelsCommand,
    OnboardingCommand, PatchCommand, PolicyCommand, ProtocolCommand, SecretsCommand,
    SecretsConfigureCommand, SecurityCommand, SkillsCommand, SkillsPackageCommand,
    SupportBundleCommand,
};
#[cfg(not(windows))]
use super::{PairingClientKindArg, PairingCommand, PairingMethodArg};

#[test]
fn parse_version_subcommand() {
    let parsed = Cli::parse_from(["palyra", "version"]);
    assert_eq!(parsed.command, Command::Version);
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
        }
    );
}

#[test]
fn parse_doctor_strict() {
    let parsed = Cli::parse_from(["palyra", "doctor", "--strict"]);
    assert_eq!(parsed.command, Command::Doctor { strict: true, json: false });
}

#[test]
fn parse_doctor_json() {
    let parsed = Cli::parse_from(["palyra", "doctor", "--json"]);
    assert_eq!(parsed.command, Command::Doctor { strict: false, json: true });
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
                grpc_url: Some("http://127.0.0.1:7443".to_owned()),
                token: None,
                principal: None,
                device_id: None,
                channel: None,
                session_id: None,
                run_id: None,
                prompt: None,
                prompt_stdin: false,
                allow_sensitive_tools: false,
                ndjson_stdin: true,
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
                grpc_url: None,
                token: None,
                principal: None,
                device_id: None,
                channel: None,
                allow_sensitive_tools: false,
            }
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
                json: true,
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
    let parsed = Cli::parse_from(["palyra", "browser", "status", "--url", "http://127.0.0.1:7143"]);
    assert_eq!(
        parsed.command,
        Command::Browser {
            command: BrowserCommand::Status { url: Some("http://127.0.0.1:7143".to_owned()) }
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
        "--daemon-url",
        "http://127.0.0.1:7142",
        "--admin-token-env",
        "PALYRA_ADMIN_TOKEN",
    ]);
    assert_eq!(
        parsed.command,
        Command::Onboarding {
            command: OnboardingCommand::Wizard {
                path: Some("config/palyra.toml".to_owned()),
                force: true,
                daemon_url: "http://127.0.0.1:7142".to_owned(),
                admin_token_env: "PALYRA_ADMIN_TOKEN".to_owned(),
            }
        }
    );
}

#[test]
fn parse_daemon_status_with_url() {
    let parsed = Cli::parse_from(["palyra", "daemon", "status", "--url", "http://127.0.0.1:7142"]);
    assert_eq!(
        parsed.command,
        Command::Gateway {
            command: DaemonCommand::Status { url: Some("http://127.0.0.1:7142".to_owned()) }
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
                json: false,
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
fn parse_skills_audit_quarantine_and_enable() {
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
#[cfg(not(windows))]
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
#[cfg(not(windows))]
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
#[cfg(not(windows))]
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
#[cfg(not(windows))]
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
#[cfg(windows)]
fn parse_pairing_command_is_unavailable_on_windows() {
    let result = Cli::try_parse_from([
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
    assert!(result.is_err(), "pairing command should not be exposed on windows");
}
