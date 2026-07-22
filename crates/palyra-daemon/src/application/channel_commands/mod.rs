//! Operator command surface for chat channels (`/palyra ...`).
//!
//! Defines the built-in command catalog with typed argument specs, parses
//! both free-text and connector-native invocations into a common
//! [`ChannelCommandInvocation`], and renders policy-aware, redacted
//! responses with audit payloads. Side-effecting commands fail closed when
//! the scope has neither an active conversation binding nor an explicit
//! target argument. Routing and policy checks live in `route_message`.

use std::collections::BTreeMap;

use palyra_common::redaction::{redact_auth_error, redact_url_segments_in_text};
use palyra_connectors::{
    ChannelCommandArgumentKind, ChannelNativeCommandArgument,
    ChannelNativeCommandInvocationPayload, ChannelNativeCommandSpec,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

const CHANNEL_COMMAND_SCHEMA_VERSION: u32 = 1;
const COMMAND_PREFIXES: &[&str] = &["/palyra", "!palyra"];
const MAX_FREEFORM_ARG_BYTES: usize = 8 * 1024;

/// Canonical names of the built-in channel commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChannelCommandName {
    Status,
    Stop,
    Reset,
    Compact,
    Approve,
    Queue,
    RoutineStatus,
    RoutinePause,
    RoutineResume,
    RoutineRunNow,
    RoutineCancel,
    RoutineHistory,
    Delegate,
    Delegations,
    DelegationStatus,
    DelegationInterrupt,
    MergePreview,
    Whoami,
}

impl ChannelCommandName {
    /// Canonical kebab-case command token shown to users and used in the
    /// native command catalog.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Stop => "stop",
            Self::Reset => "reset",
            Self::Compact => "compact",
            Self::Approve => "approve",
            Self::Queue => "queue",
            Self::RoutineStatus => "routine-status",
            Self::RoutinePause => "routine-pause",
            Self::RoutineResume => "routine-resume",
            Self::RoutineRunNow => "routine-run-now",
            Self::RoutineCancel => "routine-cancel",
            Self::RoutineHistory => "routine-history",
            Self::Delegate => "delegate",
            Self::Delegations => "delegations",
            Self::DelegationStatus => "delegation-status",
            Self::DelegationInterrupt => "delegation-interrupt",
            Self::MergePreview => "merge-preview",
            Self::Whoami => "whoami",
        }
    }

    /// Policy action string evaluated before the command executes.
    #[must_use]
    pub(crate) const fn policy_action(self) -> &'static str {
        match self {
            Self::Status => "channel.command.status",
            Self::Stop => "channel.command.stop",
            Self::Reset => "channel.command.reset",
            Self::Compact => "channel.command.compact",
            Self::Approve => "channel.command.approve",
            Self::Queue => "channel.command.queue",
            Self::RoutineStatus => "channel.command.routine.status",
            Self::RoutinePause => "channel.command.routine.pause",
            Self::RoutineResume => "channel.command.routine.resume",
            Self::RoutineRunNow => "channel.command.routine.run_now",
            Self::RoutineCancel => "channel.command.routine.cancel",
            Self::RoutineHistory => "channel.command.routine.history",
            Self::Delegate => "channel.command.delegate",
            Self::Delegations => "channel.command.delegations",
            Self::DelegationStatus => "channel.command.delegation_status",
            Self::DelegationInterrupt => "channel.command.delegation_interrupt",
            Self::MergePreview => "channel.command.merge_preview",
            Self::Whoami => "channel.command.whoami",
        }
    }

    /// Returns whether the command mutates state.
    ///
    /// INTENTIONAL: written as the negation of the read-only set so a new
    /// variant is treated as side-effecting (and binding-gated) until it is
    /// explicitly listed as read-only.
    #[must_use]
    pub(crate) const fn side_effecting(self) -> bool {
        !matches!(
            self,
            Self::Status
                | Self::Queue
                | Self::RoutineStatus
                | Self::RoutineHistory
                | Self::Delegations
                | Self::DelegationStatus
                | Self::MergePreview
                | Self::Whoami
        )
    }

    /// Resolves a command token, accepting the canonical name plus
    /// case-insensitive aliases and separator variants
    /// (`routine.status`/`routine:status`).
    #[must_use]
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "status" => Some(Self::Status),
            "stop" | "abort" | "cancel" => Some(Self::Stop),
            "reset" => Some(Self::Reset),
            "compact" | "summarize" => Some(Self::Compact),
            "approve" | "approval" => Some(Self::Approve),
            "queue" => Some(Self::Queue),
            "routine-status" | "routine.status" | "routine:status" => Some(Self::RoutineStatus),
            "routine-pause" | "routine.pause" | "routine:pause" => Some(Self::RoutinePause),
            "routine-resume" | "routine.resume" | "routine:resume" => Some(Self::RoutineResume),
            "routine-run-now" | "routine-run" | "routine.run-now" | "routine:run-now" => {
                Some(Self::RoutineRunNow)
            }
            "routine-cancel" | "routine.cancel" | "routine:cancel" => Some(Self::RoutineCancel),
            "routine-history" | "routine.history" | "routine:history" => Some(Self::RoutineHistory),
            "delegate" | "spawn" => Some(Self::Delegate),
            "delegations" | "delegation-list" | "delegate-list" => Some(Self::Delegations),
            "delegation-status" | "delegate-status" | "child-status" => {
                Some(Self::DelegationStatus)
            }
            "delegation-interrupt" | "delegate-interrupt" | "interrupt-delegation" => {
                Some(Self::DelegationInterrupt)
            }
            "merge-preview" | "delegation-merge-preview" | "delegate-merge-preview" => {
                Some(Self::MergePreview)
            }
            "whoami" | "who" => Some(Self::Whoami),
            _ => None,
        }
    }
}

/// How an invocation arrived: parsed from message text or from a
/// connector-native command interaction (e.g. a slash command).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChannelCommandSourceKind {
    Text,
    Native,
}

/// Typed argument declaration within a [`ChannelCommandSpec`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelCommandArgumentSpec {
    pub(crate) name: String,
    pub(crate) kind: ChannelCommandArgumentKind,
    pub(crate) required: bool,
    pub(crate) enum_values: Vec<String>,
    pub(crate) description: String,
}

impl ChannelCommandArgumentSpec {
    #[must_use]
    fn native(&self) -> ChannelNativeCommandArgument {
        ChannelNativeCommandArgument {
            name: self.name.clone(),
            kind: self.kind,
            required: self.required,
            enum_values: self.enum_values.clone(),
            description: Some(self.description.clone()),
        }
    }
}

/// One registered command: description, policy action, side-effect flag,
/// and ordered argument specs (order drives positional text parsing).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelCommandSpec {
    pub(crate) name: ChannelCommandName,
    pub(crate) description: String,
    pub(crate) policy_action: String,
    pub(crate) side_effecting: bool,
    pub(crate) arguments: Vec<ChannelCommandArgumentSpec>,
}

impl ChannelCommandSpec {
    /// Converts the spec to the connector-facing native command shape.
    #[must_use]
    pub(crate) fn native(&self) -> ChannelNativeCommandSpec {
        ChannelNativeCommandSpec {
            name: self.name.as_str().to_owned(),
            description: self.description.clone(),
            policy_action: self.policy_action.clone(),
            side_effecting: self.side_effecting,
            arguments: self.arguments.iter().map(ChannelCommandArgumentSpec::native).collect(),
        }
    }
}

/// A successfully parsed command with validated, typed arguments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelCommandInvocation {
    pub(crate) command: ChannelCommandName,
    pub(crate) source: ChannelCommandSourceKind,
    pub(crate) arguments: BTreeMap<String, ChannelCommandValue>,
    pub(crate) raw_text: Option<String>,
    pub(crate) native_interaction_id: Option<String>,
}

impl ChannelCommandInvocation {
    /// Deterministic dedup key over command, arguments, and scope, so a
    /// retried delivery of the same command in the same conversation can be
    /// recognized without suppressing identical commands elsewhere.
    #[must_use]
    pub(crate) fn idempotency_key(&self, scope: &ChannelCommandScope) -> String {
        let payload = serde_json::to_vec(&json!({
            "schema_version": CHANNEL_COMMAND_SCHEMA_VERSION,
            "command": self.command.as_str(),
            "arguments": self.arguments,
            "scope": scope,
        }))
        .unwrap_or_default();
        format!("channel_command:{}:{}", self.command.as_str(), sha256_hex(payload.as_slice()))
    }
}

/// Typed value of one parsed argument, tagged by argument kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub(crate) enum ChannelCommandValue {
    String(String),
    Enum(String),
    Bool(bool),
    Int(i64),
    DurationMs(u64),
    IdRef(String),
    FreeformTail(String),
}

impl ChannelCommandValue {
    // Free-text values are redacted before rendering because operators can
    // paste URLs or tokens into command arguments.
    #[must_use]
    fn user_visible(&self) -> String {
        match self {
            Self::String(value)
            | Self::Enum(value)
            | Self::IdRef(value)
            | Self::FreeformTail(value) => redact_url_segments_in_text(&redact_auth_error(value)),
            Self::Bool(value) => value.to_string(),
            Self::Int(value) => value.to_string(),
            Self::DurationMs(value) => format!("{value}ms"),
        }
    }
}

/// Conversation scope a command executes in; part of the idempotency key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelCommandScope {
    pub(crate) channel: String,
    pub(crate) conversation_id: Option<String>,
    pub(crate) thread_id: Option<String>,
    pub(crate) sender_identity: Option<String>,
    pub(crate) principal: String,
}

/// Snapshot of the scoped runtime (queue depth, binding, session/run ids)
/// that response rendering reads; assembled by the caller per command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelCommandRuntimeView {
    pub(crate) queue_depth: usize,
    pub(crate) route_config_hash: String,
    pub(crate) command_catalog_hash: String,
    pub(crate) binding_id: Option<String>,
    pub(crate) binding_kind: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) run_id: Option<String>,
    pub(crate) pending_approval_count: usize,
    pub(crate) provider_wait_ms: Option<u64>,
    pub(crate) last_error: Option<String>,
    pub(crate) observed_at_unix_ms: i64,
}

/// Rendered command outcome: user-facing text plus the audit JSON that is
/// journaled verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelCommandResponse {
    pub(crate) ok: bool,
    pub(crate) code: String,
    pub(crate) text: String,
    pub(crate) audit_json: Value,
}

/// Parse/validation failure with a stable code and a recovery hint that is
/// safe to echo back to the channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelCommandErrorEnvelope {
    pub(crate) code: String,
    pub(crate) message: String,
    pub(crate) recovery_hint: String,
}

/// Three-way parse result: ordinary message (`NotCommand`), a valid
/// invocation, or a command-shaped input that failed validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChannelCommandParseOutcome {
    NotCommand,
    Parsed(ChannelCommandInvocation),
    Malformed(ChannelCommandErrorEnvelope),
}

/// Catalog of registered commands; `BTreeMap` keeps native spec export and
/// the catalog hash deterministic.
#[derive(Debug, Clone)]
pub(crate) struct ChannelCommandRegistry {
    specs: BTreeMap<ChannelCommandName, ChannelCommandSpec>,
}

impl Default for ChannelCommandRegistry {
    fn default() -> Self {
        Self::builtin()
    }
}

impl ChannelCommandRegistry {
    /// Builds the registry of all built-in commands.
    #[must_use]
    pub(crate) fn builtin() -> Self {
        let specs = [
            command_spec(
                ChannelCommandName::Status,
                "Show scoped channel runtime status",
                &[
                    arg(
                        "session_id",
                        ChannelCommandArgumentKind::IdRef,
                        false,
                        &[],
                        "Session id to inspect",
                    ),
                    arg(
                        "run_id",
                        ChannelCommandArgumentKind::IdRef,
                        false,
                        &[],
                        "Run id to inspect",
                    ),
                ],
            ),
            command_spec(
                ChannelCommandName::Stop,
                "Stop the bound or specified run",
                &[
                    arg("run_id", ChannelCommandArgumentKind::IdRef, false, &[], "Run id to stop"),
                    arg(
                        "reason",
                        ChannelCommandArgumentKind::FreeformTail,
                        false,
                        &[],
                        "Stop reason",
                    ),
                ],
            ),
            command_spec(
                ChannelCommandName::Reset,
                "Detach/reset the scoped conversation binding",
                &[
                    arg(
                        "session_id",
                        ChannelCommandArgumentKind::IdRef,
                        false,
                        &[],
                        "Session id to reset",
                    ),
                    arg(
                        "confirm",
                        ChannelCommandArgumentKind::Bool,
                        false,
                        &[],
                        "Require explicit confirmation",
                    ),
                ],
            ),
            command_spec(
                ChannelCommandName::Compact,
                "Compact the scoped session context",
                &[
                    arg(
                        "session_id",
                        ChannelCommandArgumentKind::IdRef,
                        false,
                        &[],
                        "Session id to compact",
                    ),
                    arg(
                        "mode",
                        ChannelCommandArgumentKind::Enum,
                        false,
                        &["hybrid", "deterministic"],
                        "Compaction mode",
                    ),
                    arg(
                        "reason",
                        ChannelCommandArgumentKind::FreeformTail,
                        false,
                        &[],
                        "Compaction reason",
                    ),
                ],
            ),
            command_spec(
                ChannelCommandName::Approve,
                "Resolve a pending approval in scope",
                &[
                    arg("approval_id", ChannelCommandArgumentKind::IdRef, true, &[], "Approval id"),
                    arg(
                        "decision",
                        ChannelCommandArgumentKind::Enum,
                        true,
                        &["allow", "deny"],
                        "Approval decision",
                    ),
                    arg(
                        "reason",
                        ChannelCommandArgumentKind::FreeformTail,
                        false,
                        &[],
                        "Decision reason",
                    ),
                ],
            ),
            command_spec(
                ChannelCommandName::Queue,
                "Inspect or explain the scoped queue",
                &[
                    arg("session_id", ChannelCommandArgumentKind::IdRef, false, &[], "Session id"),
                    arg(
                        "action",
                        ChannelCommandArgumentKind::Enum,
                        false,
                        &["list", "explain"],
                        "Queue action",
                    ),
                ],
            ),
            command_spec(
                ChannelCommandName::RoutineStatus,
                "Show routine status in the scoped channel",
                &[arg(
                    "routine_id",
                    ChannelCommandArgumentKind::IdRef,
                    false,
                    &[],
                    "Routine id to inspect",
                )],
            ),
            command_spec(
                ChannelCommandName::RoutinePause,
                "Pause a routine in the scoped channel",
                &[
                    arg(
                        "routine_id",
                        ChannelCommandArgumentKind::IdRef,
                        false,
                        &[],
                        "Routine id to pause",
                    ),
                    arg(
                        "reason",
                        ChannelCommandArgumentKind::FreeformTail,
                        false,
                        &[],
                        "Pause reason",
                    ),
                ],
            ),
            command_spec(
                ChannelCommandName::RoutineResume,
                "Resume a paused routine in the scoped channel",
                &[
                    arg(
                        "routine_id",
                        ChannelCommandArgumentKind::IdRef,
                        false,
                        &[],
                        "Routine id to resume",
                    ),
                    arg(
                        "reason",
                        ChannelCommandArgumentKind::FreeformTail,
                        false,
                        &[],
                        "Resume reason",
                    ),
                ],
            ),
            command_spec(
                ChannelCommandName::RoutineRunNow,
                "Dispatch a manual routine run now",
                &[
                    arg(
                        "routine_id",
                        ChannelCommandArgumentKind::IdRef,
                        false,
                        &[],
                        "Routine id to run",
                    ),
                    arg(
                        "reason",
                        ChannelCommandArgumentKind::FreeformTail,
                        false,
                        &[],
                        "Run reason",
                    ),
                ],
            ),
            command_spec(
                ChannelCommandName::RoutineCancel,
                "Cancel an active routine run",
                &[
                    arg(
                        "run_id",
                        ChannelCommandArgumentKind::IdRef,
                        false,
                        &[],
                        "Routine run id to cancel",
                    ),
                    arg(
                        "routine_id",
                        ChannelCommandArgumentKind::IdRef,
                        false,
                        &[],
                        "Routine id to cancel",
                    ),
                    arg(
                        "reason",
                        ChannelCommandArgumentKind::FreeformTail,
                        false,
                        &[],
                        "Cancel reason",
                    ),
                ],
            ),
            command_spec(
                ChannelCommandName::RoutineHistory,
                "Show routine run history in the scoped channel",
                &[
                    arg(
                        "routine_id",
                        ChannelCommandArgumentKind::IdRef,
                        false,
                        &[],
                        "Routine id to inspect",
                    ),
                    arg(
                        "limit",
                        ChannelCommandArgumentKind::Int,
                        false,
                        &[],
                        "Maximum run records to return",
                    ),
                ],
            ),
            command_spec(
                ChannelCommandName::Delegate,
                "Create a bounded delegated child run from this scope",
                &[
                    arg(
                        "objective",
                        ChannelCommandArgumentKind::FreeformTail,
                        true,
                        &[],
                        "Delegated child objective",
                    ),
                    arg(
                        "profile_id",
                        ChannelCommandArgumentKind::String,
                        false,
                        &[],
                        "Delegation profile id",
                    ),
                    arg(
                        "template_id",
                        ChannelCommandArgumentKind::String,
                        false,
                        &[],
                        "Delegation template id",
                    ),
                    arg(
                        "parent_run_id",
                        ChannelCommandArgumentKind::IdRef,
                        false,
                        &[],
                        "Parent run id",
                    ),
                ],
            ),
            command_spec(
                ChannelCommandName::Delegations,
                "List or explain delegated child runs in scope",
                &[
                    arg("session_id", ChannelCommandArgumentKind::IdRef, false, &[], "Session id"),
                    arg("run_id", ChannelCommandArgumentKind::IdRef, false, &[], "Parent run id"),
                    arg(
                        "action",
                        ChannelCommandArgumentKind::Enum,
                        false,
                        &["list", "explain"],
                        "Delegation query action",
                    ),
                ],
            ),
            command_spec(
                ChannelCommandName::DelegationStatus,
                "Show delegated child run status",
                &[
                    arg("task_id", ChannelCommandArgumentKind::IdRef, false, &[], "Task id"),
                    arg("run_id", ChannelCommandArgumentKind::IdRef, false, &[], "Child run id"),
                ],
            ),
            command_spec(
                ChannelCommandName::DelegationInterrupt,
                "Interrupt a delegated child run",
                &[
                    arg("task_id", ChannelCommandArgumentKind::IdRef, false, &[], "Task id"),
                    arg("run_id", ChannelCommandArgumentKind::IdRef, false, &[], "Child run id"),
                    arg(
                        "reason",
                        ChannelCommandArgumentKind::FreeformTail,
                        false,
                        &[],
                        "Interrupt reason",
                    ),
                ],
            ),
            command_spec(
                ChannelCommandName::MergePreview,
                "Show delegated child merge preview",
                &[
                    arg("task_id", ChannelCommandArgumentKind::IdRef, false, &[], "Task id"),
                    arg("run_id", ChannelCommandArgumentKind::IdRef, false, &[], "Child run id"),
                ],
            ),
            command_spec(
                ChannelCommandName::Whoami,
                "Show the channel principal and binding identity",
                &[],
            ),
        ]
        .into_iter()
        .map(|spec| (spec.name, spec))
        .collect::<BTreeMap<_, _>>();
        Self { specs }
    }

    /// Exports the catalog in connector-native form for registration.
    #[must_use]
    pub(crate) fn native_specs(&self) -> Vec<ChannelNativeCommandSpec> {
        self.specs.values().map(ChannelCommandSpec::native).collect()
    }

    /// Deterministic sha256 over the native catalog, used to detect when
    /// connectors must re-register commands.
    #[must_use]
    pub(crate) fn catalog_hash(&self) -> String {
        let payload = serde_json::to_vec(&self.native_specs()).unwrap_or_default();
        sha256_hex(payload.as_slice())
    }

    /// Parses free-form message text into a command invocation.
    ///
    /// Returns `NotCommand` for anything that does not start with a known
    /// prefix followed by whitespace (so `/palyrafoo` stays an ordinary
    /// message); after the prefix matches, every failure is `Malformed`
    /// rather than silently treated as chat.
    #[must_use]
    pub(crate) fn parse_text(&self, text: &str) -> ChannelCommandParseOutcome {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return ChannelCommandParseOutcome::NotCommand;
        }
        let Some(body) = COMMAND_PREFIXES.iter().find_map(|prefix| {
            trimmed
                .strip_prefix(prefix)
                .and_then(|rest| rest.strip_prefix(char::is_whitespace))
                .map(str::trim)
        }) else {
            return ChannelCommandParseOutcome::NotCommand;
        };
        let tokens = match split_command_tokens(body) {
            Ok(tokens) => tokens,
            Err(message) => return malformed("channel_command/malformed_text", message),
        };
        self.parse_tokens(tokens, Some(trimmed.to_owned()))
    }

    /// Parses a connector-native command interaction (arguments arrive as a
    /// JSON object, already named).
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn parse_native(
        &self,
        payload: &ChannelNativeCommandInvocationPayload,
    ) -> ChannelCommandParseOutcome {
        if let Err(error) = payload.validate() {
            return malformed(
                "channel_command/malformed_native",
                format!("native command payload failed validation: {error}"),
            );
        }
        let Some(command) = ChannelCommandName::parse(payload.command.as_str()) else {
            return malformed(
                "channel_command/unknown",
                format!("unknown channel command `{}`", payload.command),
            );
        };
        let Some(spec) = self.specs.get(&command) else {
            return malformed(
                "channel_command/unregistered",
                format!("channel command `{}` is not registered", command.as_str()),
            );
        };
        let args = if payload.args_json.is_empty() {
            Value::Object(Map::new())
        } else {
            match serde_json::from_slice::<Value>(payload.args_json.as_slice()) {
                Ok(value) => value,
                Err(error) => {
                    return malformed(
                        "channel_command/malformed_native",
                        format!("native command args_json is not valid JSON: {error}"),
                    );
                }
            }
        };
        let Some(object) = args.as_object() else {
            return malformed(
                "channel_command/malformed_native",
                "native command args_json must be a JSON object",
            );
        };
        match coerce_named_arguments(spec, object) {
            Ok(arguments) => ChannelCommandParseOutcome::Parsed(ChannelCommandInvocation {
                command,
                source: ChannelCommandSourceKind::Native,
                arguments,
                raw_text: None,
                native_interaction_id: payload.native_interaction_id.clone(),
            }),
            Err(error) => malformed("channel_command/invalid_arguments", error),
        }
    }

    fn parse_tokens(
        &self,
        tokens: Vec<String>,
        raw_text: Option<String>,
    ) -> ChannelCommandParseOutcome {
        let Some(command_token) = tokens.first() else {
            return malformed("channel_command/missing_command", "missing command name");
        };
        let Some(command) = ChannelCommandName::parse(command_token) else {
            return malformed(
                "channel_command/unknown",
                format!("unknown channel command `{command_token}`"),
            );
        };
        let Some(spec) = self.specs.get(&command) else {
            return malformed(
                "channel_command/unregistered",
                format!("channel command `{}` is not registered", command.as_str()),
            );
        };
        match coerce_text_arguments(spec, &tokens[1..]) {
            Ok(arguments) => ChannelCommandParseOutcome::Parsed(ChannelCommandInvocation {
                command,
                source: ChannelCommandSourceKind::Text,
                arguments,
                raw_text,
                native_interaction_id: None,
            }),
            Err(error) => malformed("channel_command/invalid_arguments", error),
        }
    }
}

/// Renders the response and audit payload for a parsed command.
///
/// Read-only commands always succeed and report the runtime view.
/// Side-effecting commands without an active binding fail closed with
/// `channel_command/requires_binding` unless the invocation names an
/// explicit target (run/session/task/approval/routine id).
#[must_use]
pub(crate) fn build_channel_command_response(
    invocation: &ChannelCommandInvocation,
    scope: &ChannelCommandScope,
    runtime: &ChannelCommandRuntimeView,
) -> ChannelCommandResponse {
    let idempotency_key = invocation.idempotency_key(scope);
    let rendered_args = invocation
        .arguments
        .iter()
        .map(|(key, value)| format!("{key}={}", value.user_visible()))
        .collect::<Vec<_>>();
    let command = invocation.command.as_str();
    let code = match invocation.command {
        ChannelCommandName::Status => "channel_command/status",
        ChannelCommandName::Queue => "channel_command/queue",
        ChannelCommandName::RoutineStatus => "channel_command/routine_status",
        ChannelCommandName::RoutineHistory => "channel_command/routine_history",
        ChannelCommandName::Delegations => "channel_command/delegations",
        ChannelCommandName::DelegationStatus => "channel_command/delegation_status",
        ChannelCommandName::MergePreview => "channel_command/merge_preview",
        ChannelCommandName::Whoami => "channel_command/whoami",
        ChannelCommandName::Stop
        | ChannelCommandName::Reset
        | ChannelCommandName::Compact
        | ChannelCommandName::Approve
        | ChannelCommandName::RoutinePause
        | ChannelCommandName::RoutineResume
        | ChannelCommandName::RoutineRunNow
        | ChannelCommandName::RoutineCancel
        | ChannelCommandName::Delegate
        | ChannelCommandName::DelegationInterrupt => {
            if runtime.binding_id.is_none() && !has_explicit_target(invocation) {
                "channel_command/requires_binding"
            } else {
                "channel_command/accepted"
            }
        }
    };
    let ok = !code.ends_with("requires_binding");
    let text = match invocation.command {
        ChannelCommandName::Status => format!(
            "status: channel={} queue_depth={} pending_approvals={} binding={} session={} run={} last_error={}",
            scope.channel,
            runtime.queue_depth,
            runtime.pending_approval_count,
            runtime.binding_id.as_deref().unwrap_or("none"),
            runtime.session_id.as_deref().unwrap_or("none"),
            runtime.run_id.as_deref().unwrap_or("none"),
            runtime
                .last_error
                .as_deref()
                .map(redact_auth_error)
                .unwrap_or_else(|| "none".to_owned())
        ),
        ChannelCommandName::Queue => format!(
            "queue: channel={} route_queue_depth={} action={}",
            scope.channel,
            runtime.queue_depth,
            invocation
                .arguments
                .get("action")
                .map(ChannelCommandValue::user_visible)
                .unwrap_or_else(|| "list".to_owned())
        ),
        ChannelCommandName::RoutineStatus => format!(
            "routine-status: channel={} routine={} binding={} session={}",
            scope.channel,
            invocation
                .arguments
                .get("routine_id")
                .map(ChannelCommandValue::user_visible)
                .unwrap_or_else(|| "scoped".to_owned()),
            runtime.binding_id.as_deref().unwrap_or("none"),
            runtime.session_id.as_deref().unwrap_or("none")
        ),
        ChannelCommandName::RoutineHistory => format!(
            "routine-history: channel={} routine={} limit={} binding={} session={}",
            scope.channel,
            invocation
                .arguments
                .get("routine_id")
                .map(ChannelCommandValue::user_visible)
                .unwrap_or_else(|| "scoped".to_owned()),
            invocation
                .arguments
                .get("limit")
                .map(ChannelCommandValue::user_visible)
                .unwrap_or_else(|| "20".to_owned()),
            runtime.binding_id.as_deref().unwrap_or("none"),
            runtime.session_id.as_deref().unwrap_or("none")
        ),
        ChannelCommandName::Delegations => format!(
            "delegations: channel={} action={} session={} parent_run={} binding={}",
            scope.channel,
            invocation
                .arguments
                .get("action")
                .map(ChannelCommandValue::user_visible)
                .unwrap_or_else(|| "list".to_owned()),
            invocation
                .arguments
                .get("session_id")
                .map(ChannelCommandValue::user_visible)
                .or_else(|| runtime.session_id.clone())
                .unwrap_or_else(|| "none".to_owned()),
            invocation
                .arguments
                .get("run_id")
                .map(ChannelCommandValue::user_visible)
                .or_else(|| runtime.run_id.clone())
                .unwrap_or_else(|| "none".to_owned()),
            runtime.binding_id.as_deref().unwrap_or("none")
        ),
        ChannelCommandName::DelegationStatus => format!(
            "delegation-status: task={} child_run={} session={} binding={}",
            invocation
                .arguments
                .get("task_id")
                .map(ChannelCommandValue::user_visible)
                .unwrap_or_else(|| "none".to_owned()),
            invocation
                .arguments
                .get("run_id")
                .map(ChannelCommandValue::user_visible)
                .unwrap_or_else(|| "none".to_owned()),
            runtime.session_id.as_deref().unwrap_or("none"),
            runtime.binding_id.as_deref().unwrap_or("none")
        ),
        ChannelCommandName::MergePreview => format!(
            "merge-preview: task={} child_run={} session={} binding={}",
            invocation
                .arguments
                .get("task_id")
                .map(ChannelCommandValue::user_visible)
                .unwrap_or_else(|| "none".to_owned()),
            invocation
                .arguments
                .get("run_id")
                .map(ChannelCommandValue::user_visible)
                .unwrap_or_else(|| "none".to_owned()),
            runtime.session_id.as_deref().unwrap_or("none"),
            runtime.binding_id.as_deref().unwrap_or("none")
        ),
        ChannelCommandName::Whoami => format!(
            "whoami: principal={} device_scope=channel sender={} conversation={} thread={} binding={}",
            redact_auth_error(scope.principal.as_str()),
            scope.sender_identity.as_deref().unwrap_or("unknown"),
            scope.conversation_id.as_deref().unwrap_or("default"),
            scope.thread_id.as_deref().unwrap_or("none"),
            runtime.binding_id.as_deref().unwrap_or("none")
        ),
        ChannelCommandName::Stop
        | ChannelCommandName::Reset
        | ChannelCommandName::Compact
        | ChannelCommandName::Approve
        | ChannelCommandName::RoutinePause
        | ChannelCommandName::RoutineResume
        | ChannelCommandName::RoutineRunNow
        | ChannelCommandName::RoutineCancel
        | ChannelCommandName::Delegate
        | ChannelCommandName::DelegationInterrupt
            if !ok =>
        {
            format!(
                "{command}: no active conversation binding or explicit target was found; send a scoped target argument or wait for this channel thread to bind to a session"
            )
        }
        _ => format!(
            "{command}: accepted in scoped command runtime; args={}",
            if rendered_args.is_empty() { "none".to_owned() } else { rendered_args.join(" ") }
        ),
    };
    ChannelCommandResponse {
        ok,
        code: code.to_owned(),
        text,
        audit_json: json!({
            "schema_version": CHANNEL_COMMAND_SCHEMA_VERSION,
            "event": "channel.command.evaluated",
            "command": command,
            "source": invocation.source,
            "policy_action": invocation.command.policy_action(),
            "side_effecting": invocation.command.side_effecting(),
            "idempotency_key": idempotency_key,
            "scope": scope,
            "runtime": runtime,
            "arguments": invocation.arguments,
            "outcome": {
                "ok": ok,
                "code": code,
            },
        }),
    }
}

/// Renders the response for a command that failed parsing or validation,
/// echoing the (redacted) error and recovery hint back to the channel.
#[must_use]
pub(crate) fn build_malformed_command_response(
    error: &ChannelCommandErrorEnvelope,
    scope: &ChannelCommandScope,
    runtime: &ChannelCommandRuntimeView,
) -> ChannelCommandResponse {
    let message = redact_auth_error(error.message.as_str());
    ChannelCommandResponse {
        ok: false,
        code: error.code.clone(),
        text: format!("command error: {message}; hint: {}", error.recovery_hint),
        audit_json: json!({
            "schema_version": CHANNEL_COMMAND_SCHEMA_VERSION,
            "event": "channel.command.rejected",
            "code": error.code,
            "message": message,
            "recovery_hint": error.recovery_hint,
            "scope": scope,
            "runtime": runtime,
        }),
    }
}

/// Renders the response for a command the policy engine denied.
#[must_use]
pub(crate) fn build_policy_denied_command_response(
    invocation: &ChannelCommandInvocation,
    scope: &ChannelCommandScope,
    runtime: &ChannelCommandRuntimeView,
    reason: &str,
) -> ChannelCommandResponse {
    let message = redact_auth_error(reason);
    ChannelCommandResponse {
        ok: false,
        code: "channel_command/policy_denied".to_owned(),
        text: format!("{}: denied by policy; {}", invocation.command.as_str(), message),
        audit_json: json!({
            "schema_version": CHANNEL_COMMAND_SCHEMA_VERSION,
            "event": "channel.command.denied",
            "command": invocation.command.as_str(),
            "source": invocation.source,
            "policy_action": invocation.command.policy_action(),
            "reason": message,
            "scope": scope,
            "runtime": runtime,
        }),
    }
}

fn command_spec(
    name: ChannelCommandName,
    description: &str,
    arguments: &[ChannelCommandArgumentSpec],
) -> ChannelCommandSpec {
    ChannelCommandSpec {
        name,
        description: description.to_owned(),
        policy_action: name.policy_action().to_owned(),
        side_effecting: name.side_effecting(),
        arguments: arguments.to_vec(),
    }
}

fn arg(
    name: &str,
    kind: ChannelCommandArgumentKind,
    required: bool,
    enum_values: &[&str],
    description: &str,
) -> ChannelCommandArgumentSpec {
    ChannelCommandArgumentSpec {
        name: name.to_owned(),
        kind,
        required,
        enum_values: enum_values.iter().map(|value| (*value).to_owned()).collect(),
        description: description.to_owned(),
    }
}

// Binds tokens to the spec's arguments. Precedence per argument, in spec
// order: a `name=value` token wins; otherwise the next unconsumed
// positional token is taken; FreeformTail joins and consumes all remaining
// positional tokens. Missing required arguments fail with a stable message.
fn coerce_text_arguments(
    spec: &ChannelCommandSpec,
    tokens: &[String],
) -> Result<BTreeMap<String, ChannelCommandValue>, String> {
    let mut named = Map::new();
    let mut positional = Vec::new();
    for token in tokens {
        if let Some((name, value)) = token.split_once('=') {
            named.insert(name.trim().to_owned(), Value::String(value.trim().to_owned()));
        } else {
            positional.push(token.clone());
        }
    }

    let mut output = BTreeMap::new();
    let mut position = 0usize;
    for argument in &spec.arguments {
        if argument.kind == ChannelCommandArgumentKind::FreeformTail {
            // position never exceeds positional.len(): it only advances
            // after a successful positional.get(position) above.
            let tail = positional[position..].join(" ");
            if !tail.is_empty() {
                output.insert(argument.name.clone(), coerce_text_value(argument, tail.as_str())?);
                position = positional.len();
            } else if argument.required {
                return Err(format!("missing required argument `{}`", argument.name));
            }
            continue;
        }
        if let Some(value) = named.get(argument.name.as_str()) {
            output.insert(argument.name.clone(), coerce_json_value(argument, value)?);
            continue;
        }
        if let Some(value) = positional.get(position) {
            output.insert(argument.name.clone(), coerce_text_value(argument, value)?);
            position = position.saturating_add(1);
        } else if argument.required {
            return Err(format!("missing required argument `{}`", argument.name));
        }
    }
    Ok(output)
}

fn coerce_named_arguments(
    spec: &ChannelCommandSpec,
    object: &Map<String, Value>,
) -> Result<BTreeMap<String, ChannelCommandValue>, String> {
    let mut output = BTreeMap::new();
    for argument in &spec.arguments {
        match object.get(argument.name.as_str()) {
            Some(value) => {
                output.insert(argument.name.clone(), coerce_json_value(argument, value)?);
            }
            None if argument.required => {
                return Err(format!("missing required argument `{}`", argument.name));
            }
            None => {}
        }
    }
    Ok(output)
}

fn coerce_json_value(
    spec: &ChannelCommandArgumentSpec,
    value: &Value,
) -> Result<ChannelCommandValue, String> {
    match spec.kind {
        ChannelCommandArgumentKind::Bool => value
            .as_bool()
            .map(ChannelCommandValue::Bool)
            .ok_or_else(|| format!("argument `{}` must be boolean", spec.name)),
        ChannelCommandArgumentKind::Int => value
            .as_i64()
            .map(ChannelCommandValue::Int)
            .ok_or_else(|| format!("argument `{}` must be integer", spec.name)),
        _ => {
            let Some(text) = value.as_str() else {
                return Err(format!("argument `{}` must be string-compatible", spec.name));
            };
            coerce_text_value(spec, text)
        }
    }
}

fn coerce_text_value(
    spec: &ChannelCommandArgumentSpec,
    value: &str,
) -> Result<ChannelCommandValue, String> {
    let trimmed = value.trim();
    match spec.kind {
        ChannelCommandArgumentKind::String => Ok(ChannelCommandValue::String(trimmed.to_owned())),
        ChannelCommandArgumentKind::Enum => {
            let normalized = trimmed.to_ascii_lowercase();
            if spec.enum_values.iter().any(|candidate| candidate == &normalized) {
                Ok(ChannelCommandValue::Enum(normalized))
            } else {
                Err(format!(
                    "argument `{}` must be one of {}",
                    spec.name,
                    spec.enum_values.join("|")
                ))
            }
        }
        ChannelCommandArgumentKind::Bool => parse_bool(trimmed)
            .map(ChannelCommandValue::Bool)
            .ok_or_else(|| format!("argument `{}` must be true or false", spec.name)),
        ChannelCommandArgumentKind::Int => trimmed
            .parse::<i64>()
            .map(ChannelCommandValue::Int)
            .map_err(|_| format!("argument `{}` must be integer", spec.name)),
        ChannelCommandArgumentKind::Duration => parse_duration_ms(trimmed)
            .map(ChannelCommandValue::DurationMs)
            .ok_or_else(|| format!("argument `{}` must be a duration like 30s or 5m", spec.name)),
        ChannelCommandArgumentKind::IdRef => {
            if trimmed.is_empty() {
                Err(format!("argument `{}` must not be empty", spec.name))
            } else {
                Ok(ChannelCommandValue::IdRef(trimmed.to_owned()))
            }
        }
        ChannelCommandArgumentKind::FreeformTail => {
            if trimmed.len() > MAX_FREEFORM_ARG_BYTES {
                Err(format!("argument `{}` exceeds freeform size limit", spec.name))
            } else {
                Ok(ChannelCommandValue::FreeformTail(trimmed.to_owned()))
            }
        }
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "yes" | "y" | "1" | "on" => Some(true),
        "false" | "no" | "n" | "0" | "off" => Some(false),
        _ => None,
    }
}

// Accepts bare milliseconds or a digits+suffix form (ms/s/m/h); checked
// multiplication rejects values that would overflow instead of wrapping.
fn parse_duration_ms(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    let suffix_start = trimmed.find(|ch: char| !ch.is_ascii_digit()).unwrap_or(trimmed.len());
    let (digits, suffix) = trimmed.split_at(suffix_start);
    let amount = digits.parse::<u64>().ok()?;
    match suffix {
        "" | "ms" => Some(amount),
        "s" => amount.checked_mul(1_000),
        "m" => amount.checked_mul(60_000),
        "h" => amount.checked_mul(60 * 60_000),
        _ => None,
    }
}

// Whitespace tokenizer with single/double quoting (no escape sequences);
// an unterminated quote is a parse error rather than an implicit token.
fn split_command_tokens(input: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    for ch in input.chars() {
        if let Some(active_quote) = quote {
            if ch == active_quote {
                quote = None;
            } else {
                current.push(ch);
            }
            continue;
        }
        match ch {
            '"' | '\'' => quote = Some(ch),
            ch if ch.is_whitespace() => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }
    if quote.is_some() {
        return Err("unterminated quoted argument".to_owned());
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    Ok(tokens)
}

// An explicit target lets a side-effecting command run without an active
// binding, because the operator named exactly what to act on.
fn has_explicit_target(invocation: &ChannelCommandInvocation) -> bool {
    invocation.arguments.contains_key("run_id")
        || invocation.arguments.contains_key("session_id")
        || invocation.arguments.contains_key("parent_run_id")
        || invocation.arguments.contains_key("task_id")
        || invocation.arguments.contains_key("approval_id")
        || invocation.arguments.contains_key("routine_id")
}

fn malformed(code: impl Into<String>, message: impl Into<String>) -> ChannelCommandParseOutcome {
    ChannelCommandParseOutcome::Malformed(ChannelCommandErrorEnvelope {
        code: code.into(),
        message: message.into(),
        recovery_hint: "refresh the command schema and retry with supported arguments".to_owned(),
    })
}

fn sha256_hex(payload: &[u8]) -> String {
    let digest = Sha256::digest(payload);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::{
        build_channel_command_response, ChannelCommandName, ChannelCommandParseOutcome,
        ChannelCommandRegistry, ChannelCommandRuntimeView, ChannelCommandScope,
    };

    #[test]
    fn text_parser_validates_typed_arguments() {
        let registry = ChannelCommandRegistry::builtin();
        let parsed =
            registry.parse_text("/palyra approve approval_id=01ARZ3 decision=allow reason ok");
        let ChannelCommandParseOutcome::Parsed(invocation) = parsed else {
            panic!("approve command should parse");
        };

        assert_eq!(invocation.command, ChannelCommandName::Approve);
        assert!(invocation.arguments.contains_key("approval_id"));
        assert!(invocation.arguments.contains_key("decision"));
        assert!(invocation.arguments.contains_key("reason"));
    }

    #[test]
    fn text_parser_accepts_delegation_commands() {
        let registry = ChannelCommandRegistry::builtin();
        let ChannelCommandParseOutcome::Parsed(delegate) = registry.parse_text(
            "/palyra delegate profile_id=research Summarize outage evidence from parent context",
        ) else {
            panic!("delegate command should parse");
        };
        assert_eq!(delegate.command, ChannelCommandName::Delegate);
        assert!(delegate.arguments.contains_key("objective"));
        assert!(delegate.arguments.contains_key("profile_id"));

        let ChannelCommandParseOutcome::Parsed(freeform_delegate) =
            registry.parse_text("/palyra delegate fix the build")
        else {
            panic!("delegate freeform command should parse");
        };
        assert!(matches!(
            freeform_delegate.arguments.get("objective"),
            Some(super::ChannelCommandValue::FreeformTail(value)) if value == "fix the build"
        ));
        assert!(
            !freeform_delegate.arguments.contains_key("profile_id")
                && !freeform_delegate.arguments.contains_key("template_id")
                && !freeform_delegate.arguments.contains_key("parent_run_id"),
            "objective tail tokens must not be reused as later positional arguments"
        );

        let ChannelCommandParseOutcome::Parsed(status) =
            registry.parse_text("/palyra delegation-status task_id=01ARZ3")
        else {
            panic!("delegation-status command should parse");
        };
        assert_eq!(status.command, ChannelCommandName::DelegationStatus);
        assert!(status.arguments.contains_key("task_id"));

        let ChannelCommandParseOutcome::Parsed(preview) =
            registry.parse_text("/palyra merge-preview run_id=01ARZ3")
        else {
            panic!("merge-preview command should parse");
        };
        assert_eq!(preview.command, ChannelCommandName::MergePreview);
        assert!(preview.arguments.contains_key("run_id"));
    }

    #[test]
    fn text_parser_accepts_routine_control_commands() {
        let registry = ChannelCommandRegistry::builtin();
        let ChannelCommandParseOutcome::Parsed(status) =
            registry.parse_text("/palyra routine-status routine_id=daily-report")
        else {
            panic!("routine status command should parse");
        };
        assert_eq!(status.command, ChannelCommandName::RoutineStatus);
        assert!(status.arguments.contains_key("routine_id"));
        assert!(!status.command.side_effecting());

        let ChannelCommandParseOutcome::Parsed(run_now) =
            registry.parse_text("/palyra routine-run-now routine_id=daily-report operator request")
        else {
            panic!("routine run-now command should parse");
        };
        assert_eq!(run_now.command, ChannelCommandName::RoutineRunNow);
        assert!(run_now.arguments.contains_key("routine_id"));
        assert!(run_now.arguments.contains_key("reason"));
        assert!(run_now.command.side_effecting());

        let ChannelCommandParseOutcome::Parsed(history) =
            registry.parse_text("/palyra routine-history daily-report 5")
        else {
            panic!("routine history command should parse");
        };
        assert_eq!(history.command, ChannelCommandName::RoutineHistory);
        assert!(!history.command.side_effecting());
    }

    #[test]
    fn delegate_command_requires_objective() {
        let registry = ChannelCommandRegistry::builtin();
        let parsed = registry.parse_text("/palyra delegate profile_id=research");

        let ChannelCommandParseOutcome::Malformed(error) = parsed else {
            panic!("delegate without objective should be malformed");
        };
        assert_eq!(error.code, "channel_command/invalid_arguments");
        assert!(error.message.contains("objective"));
    }

    #[test]
    fn command_catalog_hash_is_deterministic() {
        let first = ChannelCommandRegistry::builtin().catalog_hash();
        let second = ChannelCommandRegistry::builtin().catalog_hash();

        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
    }

    #[test]
    fn malformed_command_reports_stable_error() {
        let registry = ChannelCommandRegistry::builtin();
        let parsed = registry.parse_text("/palyra missing");

        let ChannelCommandParseOutcome::Malformed(error) = parsed else {
            panic!("unknown command should be malformed");
        };
        assert_eq!(error.code, "channel_command/unknown");
    }

    #[test]
    fn side_effecting_command_without_binding_fails_closed() {
        let registry = ChannelCommandRegistry::builtin();
        let ChannelCommandParseOutcome::Parsed(invocation) = registry.parse_text("/palyra stop")
        else {
            panic!("stop command should parse");
        };
        let response = build_channel_command_response(
            &invocation,
            &ChannelCommandScope {
                channel: "discord:default".to_owned(),
                conversation_id: Some("c1".to_owned()),
                thread_id: None,
                sender_identity: Some("u1".to_owned()),
                principal: "channel:discord:default".to_owned(),
            },
            &ChannelCommandRuntimeView {
                queue_depth: 0,
                route_config_hash: "0".repeat(64),
                command_catalog_hash: "0".repeat(64),
                binding_id: None,
                binding_kind: None,
                session_id: None,
                run_id: None,
                pending_approval_count: 0,
                provider_wait_ms: None,
                last_error: None,
                observed_at_unix_ms: 1,
            },
        );

        assert!(!response.ok);
        assert_eq!(response.code, "channel_command/requires_binding");
    }

    #[test]
    fn delegation_interrupt_without_binding_fails_closed() {
        let registry = ChannelCommandRegistry::builtin();
        let ChannelCommandParseOutcome::Parsed(invocation) =
            registry.parse_text("/palyra delegation-interrupt")
        else {
            panic!("delegation interrupt command should parse");
        };
        let response = build_channel_command_response(
            &invocation,
            &ChannelCommandScope {
                channel: "discord:default".to_owned(),
                conversation_id: Some("c1".to_owned()),
                thread_id: None,
                sender_identity: Some("u1".to_owned()),
                principal: "channel:discord:default".to_owned(),
            },
            &ChannelCommandRuntimeView {
                queue_depth: 0,
                route_config_hash: "0".repeat(64),
                command_catalog_hash: "0".repeat(64),
                binding_id: None,
                binding_kind: None,
                session_id: None,
                run_id: None,
                pending_approval_count: 0,
                provider_wait_ms: None,
                last_error: None,
                observed_at_unix_ms: 1,
            },
        );

        assert!(!response.ok);
        assert_eq!(response.code, "channel_command/requires_binding");
    }

    #[test]
    fn routine_side_effecting_command_allows_explicit_routine_target_without_binding() {
        let registry = ChannelCommandRegistry::builtin();
        let ChannelCommandParseOutcome::Parsed(invocation) =
            registry.parse_text("/palyra routine-pause routine_id=daily-report planned hold")
        else {
            panic!("routine pause command should parse");
        };
        let response = build_channel_command_response(
            &invocation,
            &ChannelCommandScope {
                channel: "discord:default".to_owned(),
                conversation_id: Some("c1".to_owned()),
                thread_id: None,
                sender_identity: Some("u1".to_owned()),
                principal: "channel:discord:default".to_owned(),
            },
            &ChannelCommandRuntimeView {
                queue_depth: 0,
                route_config_hash: "0".repeat(64),
                command_catalog_hash: "0".repeat(64),
                binding_id: None,
                binding_kind: None,
                session_id: None,
                run_id: None,
                pending_approval_count: 0,
                provider_wait_ms: None,
                last_error: None,
                observed_at_unix_ms: 1,
            },
        );

        assert!(response.ok);
        assert_eq!(response.code, "channel_command/accepted");
        assert_eq!(response.audit_json["policy_action"], "channel.command.routine.pause");
    }
}
