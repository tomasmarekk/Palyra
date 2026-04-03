use clap::{ArgGroup, Subcommand, ValueEnum};

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
    Upsert {
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        name: String,
        #[arg(long)]
        prompt: String,
        #[arg(long, value_enum)]
        trigger_kind: RoutineTriggerKindArg,
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
        #[arg(long, value_enum, requires = "schedule")]
        schedule_type: Option<CronScheduleTypeArg>,
        #[arg(long, requires = "schedule_type")]
        schedule: Option<String>,
        #[arg(long)]
        trigger_payload: Option<String>,
        #[arg(long, default_value_t = false, conflicts_with = "trigger_payload")]
        trigger_payload_stdin: bool,
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
        #[arg(long, value_enum, default_value_t = RoutineDeliveryModeArg::SameChannel)]
        delivery_mode: RoutineDeliveryModeArg,
        #[arg(long)]
        delivery_channel: Option<String>,
        #[arg(long)]
        quiet_hours_start: Option<String>,
        #[arg(long)]
        quiet_hours_end: Option<String>,
        #[arg(long, value_enum)]
        quiet_hours_timezone: Option<RoutinePreviewTimezoneArg>,
        #[arg(long, default_value_t = 0)]
        cooldown_ms: u64,
        #[arg(long, value_enum, default_value_t = RoutineApprovalModeArg::None)]
        approval_mode: RoutineApprovalModeArg,
        #[arg(long)]
        template_id: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
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
