use clap::{ArgGroup, Args, Subcommand, ValueEnum};

use super::cron::{CronConcurrencyPolicyArg, CronMisfirePolicyArg, CronScheduleTypeArg};

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
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(visible_alias = "apply")]
    Upsert(Box<RoutineUpsertCommand>),
    CreateFromTemplate {
        #[arg(long)]
        template_id: String,
        #[arg(long)]
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
    #[command(visible_alias = "replay")]
    TestRun {
        #[arg(long)]
        id: String,
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
        #[arg(long)]
        id: String,
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Dispatch {
        #[arg(long)]
        id: String,
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
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Templates {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    SchedulePreview {
        phrase: String,
        #[arg(long, value_enum, default_value_t = RoutinePreviewTimezoneArg::Local)]
        timezone: RoutinePreviewTimezoneArg,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Export {
        #[arg(long)]
        id: String,
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
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        enabled: Option<bool>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Debug, Args, PartialEq, Eq)]
pub struct RoutineUpsertCommand {
    #[arg(long)]
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
    #[arg(long)]
    pub enabled: Option<bool>,
    #[arg(long)]
    pub natural_language_schedule: Option<String>,
    #[arg(long, value_enum, requires = "schedule")]
    pub schedule_type: Option<CronScheduleTypeArg>,
    #[arg(long, requires = "schedule_type")]
    pub schedule: Option<String>,
    #[arg(long)]
    pub trigger_payload: Option<String>,
    #[arg(long, default_value_t = false, conflicts_with = "trigger_payload")]
    pub trigger_payload_stdin: bool,
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
    #[arg(long, value_enum, default_value_t = RoutineRunModeArg::SameSession)]
    pub run_mode: RoutineRunModeArg,
    #[arg(long)]
    pub procedure_profile_id: Option<String>,
    #[arg(long)]
    pub skill_profile_id: Option<String>,
    #[arg(long)]
    pub provider_profile_id: Option<String>,
    #[arg(long, value_enum, default_value_t = RoutineExecutionPostureArg::Standard)]
    pub execution_posture: RoutineExecutionPostureArg,
    #[arg(long)]
    pub quiet_hours_start: Option<String>,
    #[arg(long)]
    pub quiet_hours_end: Option<String>,
    #[arg(long, value_enum)]
    pub quiet_hours_timezone: Option<RoutinePreviewTimezoneArg>,
    #[arg(long, default_value_t = 0)]
    pub cooldown_ms: u64,
    #[arg(long, value_enum, default_value_t = RoutineApprovalModeArg::None)]
    pub approval_mode: RoutineApprovalModeArg,
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
    Manual,
}

impl RoutineTriggerKindArg {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Schedule => "schedule",
            Self::Hook => "hook",
            Self::Webhook => "webhook",
            Self::SystemEvent => "system_event",
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
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::BeforeEnable => "before_enable",
            Self::BeforeFirstRun => "before_first_run",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RoutinePreviewTimezoneArg {
    Local,
    Utc,
}

impl RoutinePreviewTimezoneArg {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Utc => "utc",
        }
    }
}
