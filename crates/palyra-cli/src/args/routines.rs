//! Arguments for `palyra routines`: the unified automation surface over
//! schedule, hook, webhook, system-event, file-watch, and manual triggers.
//! Schedule/concurrency/misfire enums come from the `cron` compatibility family
//! (`cron.rs`), and several value enums here are reused by `objectives.rs`.
//! Help text is pinned by snapshot tests; see the doc-comment rules in `mod.rs`.

use std::{fmt, str::FromStr};

use clap::{ArgGroup, Args, Subcommand, ValueEnum};

use super::cron::{CronConcurrencyPolicyArg, CronMisfirePolicyArg, CronScheduleTypeArg};
use super::RequiredCommandIdArg;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum RoutinesCommand {
    Status {
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, value_enum)]
        trigger_kind: Option<RoutineTriggerKindArg>,
        #[arg(long)]
        enabled: Option<bool>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        template_id: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    List {
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, value_enum)]
        trigger_kind: Option<RoutineTriggerKindArg>,
        #[arg(long)]
        enabled: Option<bool>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        template_id: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Show {
        #[command(flatten)]
        id: RequiredCommandIdArg,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(visible_alias = "apply")]
    Upsert(Box<RoutineUpsertCommand>),
    CreateFromTemplate {
        #[arg(long)]
        template_id: String,
        #[arg(
            long,
            value_name = "ULID",
            help = "Canonical routine ULID. Omit to generate one; use --name for a human-readable label"
        )]
        id: Option<String>,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        prompt: Option<String>,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        session_key: Option<String>,
        #[arg(long)]
        session_label: Option<String>,
        #[arg(long)]
        enabled: Option<bool>,
        #[arg(long)]
        natural_language_schedule: Option<String>,
        #[arg(long)]
        delivery_channel: Option<String>,
        #[arg(long)]
        trigger_payload: Option<String>,
        #[arg(long, default_value_t = false, conflicts_with = "trigger_payload")]
        trigger_payload_stdin: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Enable {
        #[command(flatten)]
        id: RequiredCommandIdArg,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Disable {
        #[command(flatten)]
        id: RequiredCommandIdArg,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    RunNow {
        #[command(flatten)]
        id: RequiredCommandIdArg,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(visible_alias = "replay")]
    TestRun {
        #[command(flatten)]
        id: RequiredCommandIdArg,
        #[arg(long)]
        source_run_id: Option<String>,
        #[arg(long)]
        trigger_reason: Option<String>,
        #[arg(long)]
        trigger_payload: Option<String>,
        #[arg(long, default_value_t = false, conflicts_with = "trigger_payload")]
        trigger_payload_stdin: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(visible_alias = "runs")]
    Logs {
        #[command(flatten)]
        id: RequiredCommandIdArg,
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Dispatch {
        #[command(flatten)]
        id: RequiredCommandIdArg,
        #[arg(long, value_enum)]
        trigger_kind: Option<RoutineTriggerKindArg>,
        #[arg(long)]
        trigger_reason: Option<String>,
        #[arg(long)]
        trigger_payload: Option<String>,
        #[arg(long, default_value_t = false, conflicts_with = "trigger_payload")]
        trigger_payload_stdin: bool,
        #[arg(long)]
        trigger_dedupe_key: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(visible_alias = "rm")]
    Delete {
        #[command(flatten)]
        id: RequiredCommandIdArg,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Templates {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    SchedulePreview {
        phrase: String,
        #[arg(long, default_value = "local")]
        timezone: RoutinePreviewTimezoneArg,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Export {
        #[command(flatten)]
        id: RequiredCommandIdArg,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(group(
        ArgGroup::new("routine_import_source")
            .required(true)
            .args(["file", "stdin"])
    ))]
    Import {
        #[arg(long)]
        file: Option<String>,
        #[arg(long, default_value_t = false)]
        stdin: bool,
        #[arg(
            long,
            value_name = "ULID",
            help = "Routine ULID override. Omit to keep the imported id or generate one"
        )]
        id: Option<String>,
        #[arg(long)]
        enabled: Option<bool>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Debug, Args, PartialEq, Eq)]
pub struct RoutineUpsertCommand {
    #[arg(
        long,
        value_name = "ULID",
        help = "Canonical routine ULID. Omit to generate one; use --name for a human-readable label"
    )]
    pub id: Option<String>,
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub prompt: String,
    #[arg(long, value_enum)]
    pub trigger_kind: RoutineTriggerKindArg,
    #[arg(long)]
    pub owner: Option<String>,
    #[arg(long)]
    pub channel: Option<String>,
    #[arg(long)]
    pub session_key: Option<String>,
    #[arg(long)]
    pub session_label: Option<String>,
    #[arg(
        long,
        value_name = "PATH",
        help = "Project working directory for scheduled runs; relative paths resolve from the current directory and are stored as absolute paths"
    )]
    pub workdir: Option<String>,
    #[arg(long)]
    pub enabled: Option<bool>,
    #[arg(long)]
    pub natural_language_schedule: Option<String>,
    #[arg(
        long,
        value_name = "TIMEZONE",
        help = "Schedule timezone: local, utc, or an IANA timezone such as Europe/Prague"
    )]
    pub schedule_timezone: Option<RoutinePreviewTimezoneArg>,
    #[arg(long, value_enum, requires = "schedule")]
    pub schedule_type: Option<CronScheduleTypeArg>,
    #[arg(
        long,
        requires = "schedule_type",
        help = "Schedule payload: cron expression for cron, RFC3339 timestamp for at, or milliseconds/duration such as 300000 or 5m for every"
    )]
    pub schedule: Option<String>,
    #[arg(long)]
    pub trigger_payload: Option<String>,
    #[arg(long, default_value_t = false, conflicts_with = "trigger_payload")]
    pub trigger_payload_stdin: bool,
    #[arg(
        long,
        value_name = "PATH",
        help = "Absolute user-owned OS path watched by trigger-kind=file-watch"
    )]
    pub watch_path: Option<String>,
    #[arg(
        long,
        value_name = "MS",
        help = "Polling interval for file-watch routines; minimum 30000"
    )]
    pub watch_poll_interval_ms: Option<u64>,
    #[arg(long, value_enum, default_value_t = CronConcurrencyPolicyArg::Forbid)]
    pub concurrency: CronConcurrencyPolicyArg,
    #[arg(long, default_value_t = 1)]
    pub retry_max_attempts: u32,
    #[arg(long, default_value_t = 1000)]
    pub retry_backoff_ms: u64,
    #[arg(long, value_enum, default_value_t = CronMisfirePolicyArg::Skip)]
    pub misfire: CronMisfirePolicyArg,
    #[arg(long, default_value_t = 0)]
    pub jitter_ms: u64,
    #[arg(
        long,
        value_name = "COUNT",
        help = "Automatically disable scheduled routines after this many executed runs"
    )]
    pub max_runs: Option<u32>,
    #[arg(long, value_enum, default_value_t = RoutineDeliveryModeArg::SameChannel)]
    pub delivery_mode: RoutineDeliveryModeArg,
    #[arg(long)]
    pub delivery_channel: Option<String>,
    #[arg(long, value_enum)]
    pub delivery_failure_mode: Option<RoutineDeliveryModeArg>,
    #[arg(long)]
    pub delivery_failure_channel: Option<String>,
    #[arg(long, value_enum, default_value_t = RoutineSilentPolicyArg::Noisy)]
    pub silent_policy: RoutineSilentPolicyArg,
    #[arg(long, value_enum)]
    pub run_mode: Option<RoutineRunModeArg>,
    #[arg(long)]
    pub procedure_profile_id: Option<String>,
    #[arg(long)]
    pub skill_profile_id: Option<String>,
    #[arg(long)]
    pub provider_profile_id: Option<String>,
    #[arg(long, value_enum)]
    pub execution_posture: Option<RoutineExecutionPostureArg>,
    #[arg(long)]
    pub quiet_hours_start: Option<String>,
    #[arg(long)]
    pub quiet_hours_end: Option<String>,
    #[arg(long)]
    pub quiet_hours_timezone: Option<RoutinePreviewTimezoneArg>,
    #[arg(long, default_value_t = 0)]
    pub cooldown_ms: u64,
    #[arg(long, value_enum)]
    pub approval_mode: Option<RoutineApprovalModeArg>,
    #[arg(long)]
    pub template_id: Option<String>,
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RoutineTriggerKindArg {
    Schedule,
    Hook,
    Webhook,
    SystemEvent,
    FileWatch,
    Manual,
}

impl RoutineTriggerKindArg {
    /// Returns the daemon contract identifier (snake_case, unlike the
    /// kebab-case CLI value).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Schedule => "schedule",
            Self::Hook => "hook",
            Self::Webhook => "webhook",
            Self::SystemEvent => "system_event",
            Self::FileWatch => "file_watch",
            Self::Manual => "manual",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RoutineDeliveryModeArg {
    SameChannel,
    SpecificChannel,
    LocalOnly,
    LogsOnly,
}

impl RoutineDeliveryModeArg {
    /// Returns the daemon contract identifier for this delivery mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SameChannel => "same_channel",
            Self::SpecificChannel => "specific_channel",
            Self::LocalOnly => "local_only",
            Self::LogsOnly => "logs_only",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RoutineSilentPolicyArg {
    Noisy,
    FailureOnly,
    AuditOnly,
}

impl RoutineSilentPolicyArg {
    /// Returns the daemon contract identifier for this silent policy.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Noisy => "noisy",
            Self::FailureOnly => "failure_only",
            Self::AuditOnly => "audit_only",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RoutineRunModeArg {
    SameSession,
    FreshSession,
}

impl RoutineRunModeArg {
    /// Returns the daemon contract identifier for this run mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SameSession => "same_session",
            Self::FreshSession => "fresh_session",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RoutineExecutionPostureArg {
    Standard,
    SensitiveTools,
}

impl RoutineExecutionPostureArg {
    /// Returns the daemon contract identifier for this execution posture.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::SensitiveTools => "sensitive_tools",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RoutineApprovalModeArg {
    None,
    BeforeEnable,
    BeforeFirstRun,
}

impl RoutineApprovalModeArg {
    /// Returns the daemon contract identifier for this approval mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::BeforeEnable => "before_enable",
            Self::BeforeFirstRun => "before_first_run",
        }
    }
}

/// Schedule/preview timezone accepted as `local`, `utc`, or an IANA name such
/// as `Europe/Prague`.
///
/// A validated newtype rather than a `ValueEnum` because arbitrary IANA
/// identifiers must be accepted; only shape validation happens at parse time
/// and full timezone resolution is performed downstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutinePreviewTimezoneArg(String);

impl RoutinePreviewTimezoneArg {
    /// Builds the `local` sentinel value.
    #[must_use]
    pub fn local() -> Self {
        Self("local".to_owned())
    }

    /// Builds the `utc` sentinel value.
    #[must_use]
    pub fn utc() -> Self {
        Self("utc".to_owned())
    }

    /// Returns the raw timezone string as entered (trimmed).
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for RoutinePreviewTimezoneArg {
    type Err = String;

    /// Parses a timezone argument, trimming surrounding whitespace.
    ///
    /// # Errors
    ///
    /// Returns a message when the trimmed value is empty or contains control
    /// characters.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err("timezone cannot be empty".to_owned());
        }
        if trimmed.chars().any(char::is_control) {
            return Err("timezone contains unsupported control characters".to_owned());
        }
        Ok(Self(trimmed.to_owned()))
    }
}

impl fmt::Display for RoutinePreviewTimezoneArg {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
