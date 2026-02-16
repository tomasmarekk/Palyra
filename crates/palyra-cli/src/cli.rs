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

    use super::{Cli, Command, ConfigCommand, DaemonCommand, PolicyCommand, ProtocolCommand};
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
