use clap::{Args, Subcommand, ValueEnum};

use super::routines::{RoutineApprovalModeArg, RoutineDeliveryModeArg, RoutinePreviewTimezoneArg};

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ObjectivesCommand {
    Status {
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, value_enum)]
        kind: Option<ObjectiveKindArg>,
        #[arg(long, value_enum)]
        state: Option<ObjectiveStateArg>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    List {
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, value_enum)]
        kind: Option<ObjectiveKindArg>,
        #[arg(long, value_enum)]
        state: Option<ObjectiveStateArg>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Show {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Summary {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(visible_alias = "apply")]
    Upsert(Box<ObjectiveUpsertCommandArgs>),
    Fire {
        #[arg(long)]
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Pause {
        #[arg(long)]
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Resume {
        #[arg(long)]
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Cancel {
        #[arg(long)]
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Archive {
        #[arg(long)]
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Args, Debug, PartialEq, Eq)]
pub struct ObjectiveUpsertCommandArgs {
    #[arg(long)]
    pub id: Option<String>,
    #[arg(long, value_enum)]
    pub kind: ObjectiveKindArg,
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub prompt: String,
    #[arg(long)]
    pub owner: Option<String>,
    #[arg(long)]
    pub channel: Option<String>,
    #[arg(long)]
    pub session_key: Option<String>,
    #[arg(long)]
    pub session_label: Option<String>,
    #[arg(long, value_enum, default_value_t = ObjectivePriorityArg::Normal)]
    pub priority: ObjectivePriorityArg,
    #[arg(long)]
    pub max_runs: Option<u32>,
    #[arg(long)]
    pub max_tokens: Option<u64>,
    #[arg(long)]
    pub budget_notes: Option<String>,
    #[arg(long)]
    pub current_focus: Option<String>,
    #[arg(long)]
    pub success_criteria: Option<String>,
    #[arg(long)]
    pub exit_condition: Option<String>,
    #[arg(long)]
    pub next_recommended_step: Option<String>,
    #[arg(long)]
    pub standing_order: Option<String>,
    #[arg(long)]
    pub enabled: Option<bool>,
    #[arg(long)]
    pub natural_language_schedule: Option<String>,
    #[arg(long, value_enum)]
    pub schedule_type: Option<ObjectiveScheduleTypeArg>,
    #[arg(long)]
    pub schedule: Option<String>,
    #[arg(long, value_enum, default_value_t = RoutineDeliveryModeArg::SameChannel)]
    pub delivery_mode: RoutineDeliveryModeArg,
    #[arg(long)]
    pub delivery_channel: Option<String>,
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
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ObjectiveKindArg {
    Objective,
    Heartbeat,
    StandingOrder,
    Program,
}

impl ObjectiveKindArg {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Objective => "objective",
            Self::Heartbeat => "heartbeat",
            Self::StandingOrder => "standing_order",
            Self::Program => "program",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ObjectiveStateArg {
    Draft,
    Active,
    Paused,
    Cancelled,
    Archived,
}

impl ObjectiveStateArg {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Cancelled => "cancelled",
            Self::Archived => "archived",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ObjectivePriorityArg {
    Low,
    Normal,
    High,
    Critical,
}

impl ObjectivePriorityArg {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Normal => "normal",
            Self::High => "high",
            Self::Critical => "critical",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ObjectiveScheduleTypeArg {
    Cron,
    Every,
    At,
}

impl ObjectiveScheduleTypeArg {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cron => "cron",
            Self::Every => "every",
            Self::At => "at",
        }
    }
}
