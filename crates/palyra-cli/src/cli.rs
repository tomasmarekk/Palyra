use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "palyra", about = "Palyra CLI bootstrap stub")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum Command {
    Version,
    Doctor {
        #[arg(long, default_value_t = false)]
        strict: bool,
    },
    Status {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        grpc_url: Option<String>,
        #[arg(long, default_value_t = false)]
        admin: bool,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
    },
    Agent {
        #[command(subcommand)]
        command: AgentCommand,
    },
    Cron {
        #[command(subcommand)]
        command: CronCommand,
    },
    Approvals {
        #[command(subcommand)]
        command: ApprovalsCommand,
    },
    Channels {
        #[command(subcommand)]
        command: ChannelsCommand,
    },
    Browser {
        #[command(subcommand)]
        command: BrowserCommand,
    },
    Completion {
        #[arg(long, value_enum)]
        shell: CompletionShell,
    },
    Onboarding {
        #[command(subcommand)]
        command: OnboardingCommand,
    },
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    Policy {
        #[command(subcommand)]
        command: PolicyCommand,
    },
    Protocol {
        #[command(subcommand)]
        command: ProtocolCommand,
    },
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    #[cfg(not(windows))]
    Pairing {
        #[command(subcommand)]
        command: PairingCommand,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum AgentCommand {
    Run {
        #[arg(long)]
        grpc_url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long, default_value = "cli")]
        channel: String,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long)]
        prompt: Option<String>,
        #[arg(long, default_value_t = false)]
        prompt_stdin: bool,
        #[arg(long, default_value_t = false)]
        allow_sensitive_tools: bool,
        #[arg(long, default_value_t = false)]
        ndjson: bool,
    },
    Interactive {
        #[arg(long)]
        grpc_url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long, default_value = "cli")]
        channel: String,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long, default_value_t = false)]
        allow_sensitive_tools: bool,
        #[arg(long, default_value_t = false)]
        ndjson: bool,
    },
    AcpShim {
        #[arg(long)]
        grpc_url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long, default_value = "cli")]
        channel: String,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long)]
        prompt: Option<String>,
        #[arg(long, default_value_t = false)]
        prompt_stdin: bool,
        #[arg(long, default_value_t = false)]
        allow_sensitive_tools: bool,
        #[arg(
            long,
            default_value_t = false,
            conflicts_with_all = ["session_id", "run_id", "prompt", "prompt_stdin"]
        )]
        ndjson_stdin: bool,
    },
    Acp {
        #[arg(long)]
        grpc_url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long, default_value = "cli")]
        channel: String,
        #[arg(long, default_value_t = false)]
        allow_sensitive_tools: bool,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum CronCommand {
    List {
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        enabled: Option<bool>,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Show {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Add {
        #[arg(long)]
        name: String,
        #[arg(long)]
        prompt: String,
        #[arg(long, value_enum)]
        schedule_type: CronScheduleTypeArg,
        #[arg(long)]
        schedule: String,
        #[arg(long, default_value_t = true)]
        enabled: bool,
        #[arg(long, value_enum, default_value_t = CronConcurrencyPolicyArg::Forbid)]
        concurrency: CronConcurrencyPolicyArg,
        #[arg(long, default_value_t = 1)]
        retry_max_attempts: u32,
        #[arg(long, default_value_t = 1000)]
        retry_backoff_ms: u64,
        #[arg(long, value_enum, default_value_t = CronMisfirePolicyArg::Skip)]
        misfire: CronMisfirePolicyArg,
        #[arg(long, default_value_t = 0)]
        jitter_ms: u64,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        session_key: Option<String>,
        #[arg(long)]
        session_label: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Update {
        #[arg(long)]
        id: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        prompt: Option<String>,
        #[arg(long, value_enum, requires = "schedule")]
        schedule_type: Option<CronScheduleTypeArg>,
        #[arg(long, requires = "schedule_type")]
        schedule: Option<String>,
        #[arg(long)]
        enabled: Option<bool>,
        #[arg(long, value_enum)]
        concurrency: Option<CronConcurrencyPolicyArg>,
        #[arg(long, requires = "retry_backoff_ms")]
        retry_max_attempts: Option<u32>,
        #[arg(long, requires = "retry_max_attempts")]
        retry_backoff_ms: Option<u64>,
        #[arg(long, value_enum)]
        misfire: Option<CronMisfirePolicyArg>,
        #[arg(long)]
        jitter_ms: Option<u64>,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        session_key: Option<String>,
        #[arg(long)]
        session_label: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Enable {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Disable {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    RunNow {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Delete {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Logs {
        #[arg(long)]
        id: String,
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ApprovalsCommand {
    List {
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        since: Option<i64>,
        #[arg(long)]
        until: Option<i64>,
        #[arg(long)]
        subject: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long, value_enum)]
        decision: Option<ApprovalDecisionArg>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Show {
        approval_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Export {
        #[arg(long, value_enum, default_value_t = ApprovalExportFormatArg::Ndjson)]
        format: ApprovalExportFormatArg,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        since: Option<i64>,
        #[arg(long)]
        until: Option<i64>,
        #[arg(long)]
        subject: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long, value_enum)]
        decision: Option<ApprovalDecisionArg>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ApprovalDecisionArg {
    Allow,
    Deny,
    Timeout,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ApprovalExportFormatArg {
    Ndjson,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CronScheduleTypeArg {
    Cron,
    Every,
    At,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CronConcurrencyPolicyArg {
    Forbid,
    Replace,
    QueueOne,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CronMisfirePolicyArg {
    Skip,
    CatchUp,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ChannelsCommand {
    List,
    Connect {
        #[arg(long)]
        kind: String,
        #[arg(long)]
        name: String,
    },
    Disconnect {
        #[arg(long)]
        name: String,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum BrowserCommand {
    Status {
        #[arg(long)]
        url: Option<String>,
    },
    Open {
        #[arg(long)]
        url: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CompletionShell {
    Bash,
    Zsh,
    Fish,
    Powershell,
    Elvish,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum OnboardingCommand {
    Wizard {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[arg(long, default_value = "http://127.0.0.1:7142")]
        daemon_url: String,
        #[arg(long, default_value = "PALYRA_ADMIN_TOKEN")]
        admin_token_env: String,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum DaemonCommand {
    Status {
        #[arg(long)]
        url: Option<String>,
    },
    AdminStatus {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
    },
    JournalRecent {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
    },
    RunStatus {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        run_id: String,
    },
    RunTape {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        run_id: String,
        #[arg(long)]
        after_seq: Option<i64>,
        #[arg(long)]
        limit: Option<usize>,
    },
    RunCancel {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        run_id: String,
        #[arg(long)]
        reason: Option<String>,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum PolicyCommand {
    Explain {
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "tool.execute.shell")]
        action: String,
        #[arg(long, default_value = "tool:shell")]
        resource: String,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ProtocolCommand {
    Version,
    ValidateId {
        #[arg(long)]
        id: String,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ConfigCommand {
    Validate {
        #[arg(long)]
        path: Option<String>,
    },
    List {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        show_secrets: bool,
    },
    Get {
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        key: String,
        #[arg(long, default_value_t = false)]
        show_secrets: bool,
    },
    Set {
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        key: String,
        #[arg(long)]
        value: String,
        #[arg(long, default_value_t = 5)]
        backups: usize,
    },
    Unset {
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        key: String,
        #[arg(long, default_value_t = 5)]
        backups: usize,
    },
    Migrate {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = 5)]
        backups: usize,
    },
    Recover {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = 1)]
        backup: usize,
        #[arg(long, default_value_t = 5)]
        backups: usize,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum PairingCommand {
    Pair {
        #[arg(long)]
        device_id: String,
        #[arg(long, value_enum, default_value_t = PairingClientKindArg::Node)]
        client_kind: PairingClientKindArg,
        #[arg(long, value_enum, default_value_t = PairingMethodArg::Pin)]
        method: PairingMethodArg,
        #[arg(
            long,
            hide = true,
            conflicts_with = "proof_stdin",
            requires = "allow_insecure_proof_arg"
        )]
        proof: Option<String>,
        #[arg(long, default_value_t = false)]
        proof_stdin: bool,
        #[arg(long, default_value_t = false)]
        allow_insecure_proof_arg: bool,
        #[arg(long)]
        store_dir: Option<String>,
        #[arg(long, default_value_t = false)]
        approve: bool,
        #[arg(long, default_value_t = false)]
        simulate_rotation: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PairingClientKindArg {
    Cli,
    Desktop,
    Node,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PairingMethodArg {
    Pin,
    Qr,
}

impl PairingMethodArg {
    #[must_use]
    #[cfg_attr(windows, allow(dead_code))]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pin => "pin",
            Self::Qr => "qr",
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{
        AgentCommand, ApprovalDecisionArg, ApprovalExportFormatArg, ApprovalsCommand,
        BrowserCommand, ChannelsCommand, Cli, Command, CompletionShell, ConfigCommand, CronCommand,
        CronConcurrencyPolicyArg, CronMisfirePolicyArg, CronScheduleTypeArg, DaemonCommand,
        OnboardingCommand, PolicyCommand, ProtocolCommand,
    };
    #[cfg(not(windows))]
    use super::{PairingClientKindArg, PairingCommand, PairingMethodArg};

    #[test]
    fn parse_version_subcommand() {
        let parsed = Cli::parse_from(["palyra", "version"]);
        assert_eq!(parsed.command, Command::Version);
    }

    #[test]
    fn parse_doctor_strict() {
        let parsed = Cli::parse_from(["palyra", "doctor", "--strict"]);
        assert_eq!(parsed.command, Command::Doctor { strict: true });
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
                principal: "user:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
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
                    principal: "user:local".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                    channel: "cli".to_owned(),
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
                    principal: "user:local".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                    channel: "cli".to_owned(),
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
                    principal: "user:local".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                    channel: "cli".to_owned(),
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
                    principal: "user:local".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                    channel: "cli".to_owned(),
                    allow_sensitive_tools: false,
                }
            }
        );
    }

    #[test]
    fn parse_agent_acp_shim_ndjson_stdin_conflicts_with_prompt() {
        let result = Cli::try_parse_from([
            "palyra",
            "agent",
            "acp-shim",
            "--ndjson-stdin",
            "--prompt",
            "hello",
        ]);
        assert!(result.is_err(), "--ndjson-stdin must conflict with --prompt");
    }

    #[test]
    fn parse_agent_acp_shim_ndjson_stdin_conflicts_with_prompt_stdin() {
        let result = Cli::try_parse_from([
            "palyra",
            "agent",
            "acp-shim",
            "--ndjson-stdin",
            "--prompt-stdin",
        ]);
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
                    json: true,
                }
            }
        );
    }

    #[test]
    fn parse_approvals_show() {
        let parsed = Cli::parse_from([
            "palyra",
            "approvals",
            "show",
            "01ARZ3NDEKTSV4RRFFQ69G5FB2",
            "--json",
        ]);
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
                }
            }
        );
    }

    #[test]
    fn parse_channels_connect() {
        let parsed =
            Cli::parse_from(["palyra", "channels", "connect", "--kind", "slack", "--name", "ops"]);
        assert_eq!(
            parsed.command,
            Command::Channels {
                command: ChannelsCommand::Connect {
                    kind: "slack".to_owned(),
                    name: "ops".to_owned(),
                }
            }
        );
    }

    #[test]
    fn parse_browser_status() {
        let parsed =
            Cli::parse_from(["palyra", "browser", "status", "--url", "http://127.0.0.1:7143"]);
        assert_eq!(
            parsed.command,
            Command::Browser {
                command: BrowserCommand::Status { url: Some("http://127.0.0.1:7143".to_owned()) }
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
        let parsed =
            Cli::parse_from(["palyra", "daemon", "status", "--url", "http://127.0.0.1:7142"]);
        assert_eq!(
            parsed.command,
            Command::Daemon {
                command: DaemonCommand::Status { url: Some("http://127.0.0.1:7142".to_owned()) }
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
            Command::Daemon {
                command: DaemonCommand::AdminStatus {
                    url: Some("http://127.0.0.1:7142".to_owned()),
                    token: Some("test-token".to_owned()),
                    principal: "user:ops".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
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
            Command::Daemon {
                command: DaemonCommand::JournalRecent {
                    url: Some("http://127.0.0.1:7142".to_owned()),
                    token: Some("test-token".to_owned()),
                    principal: "user:ops".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                    channel: Some("cli".to_owned()),
                    limit: Some(25),
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
            Command::Daemon {
                command: DaemonCommand::RunStatus {
                    url: Some("http://127.0.0.1:7142".to_owned()),
                    token: Some("test-token".to_owned()),
                    principal: "user:ops".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
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
            Command::Daemon {
                command: DaemonCommand::RunCancel {
                    url: Some("http://127.0.0.1:7142".to_owned()),
                    token: Some("test-token".to_owned()),
                    principal: "user:local".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                    channel: None,
                    run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                    reason: Some("operator requested".to_owned()),
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
                command: ProtocolCommand::ValidateId {
                    id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()
                }
            }
        );
    }

    #[test]
    fn parse_config_validate_with_path() {
        let parsed = Cli::parse_from(["palyra", "config", "validate", "--path", "custom.toml"]);
        assert_eq!(
            parsed.command,
            Command::Config {
                command: ConfigCommand::Validate { path: Some("custom.toml".to_owned()) }
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
                command: ConfigCommand::Get {
                    path: Some("custom.toml".to_owned()),
                    key: "daemon.port".to_owned(),
                    show_secrets: false,
                }
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
                command: ConfigCommand::Get {
                    path: Some("custom.toml".to_owned()),
                    key: "admin.auth_token".to_owned(),
                    show_secrets: true,
                }
            }
        );
    }

    #[test]
    fn parse_config_list_with_show_secrets() {
        let parsed = Cli::parse_from([
            "palyra",
            "config",
            "list",
            "--path",
            "custom.toml",
            "--show-secrets",
        ]);
        assert_eq!(
            parsed.command,
            Command::Config {
                command: ConfigCommand::List {
                    path: Some("custom.toml".to_owned()),
                    show_secrets: true,
                }
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
                command: ConfigCommand::Set {
                    path: Some("custom.toml".to_owned()),
                    key: "daemon.port".to_owned(),
                    value: "7443".to_owned(),
                    backups: 7,
                }
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
                command: ConfigCommand::Unset {
                    path: Some("custom.toml".to_owned()),
                    key: "daemon.port".to_owned(),
                    backups: 5,
                }
            }
        );
    }

    #[test]
    fn parse_config_migrate_with_backups() {
        let parsed = Cli::parse_from([
            "palyra",
            "config",
            "migrate",
            "--path",
            "custom.toml",
            "--backups",
            "3",
        ]);
        assert_eq!(
            parsed.command,
            Command::Config {
                command: ConfigCommand::Migrate {
                    path: Some("custom.toml".to_owned()),
                    backups: 3
                }
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
                command: ConfigCommand::Recover {
                    path: Some("custom.toml".to_owned()),
                    backup: 2,
                    backups: 4,
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
}
