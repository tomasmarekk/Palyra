use clap::{Subcommand, ValueEnum};

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
    Upsert {
        #[arg(long)]
        id: Option<String>,
        #[arg(long, value_enum)]
        kind: ObjectiveKindArg,
        #[arg(long)]
        name: String,
        #[arg(long)]
        prompt: String,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        session_key: Option<String>,
        #[arg(long)]
        session_label: Option<String>,
        #[arg(long, value_enum, default_value_t = ObjectivePriorityArg::Normal)]
        priority: ObjectivePriorityArg,
        #[arg(long)]
        max_runs: Option<u32>,
        #[arg(long)]
        max_tokens: Option<u64>,
        #[arg(long)]
        budget_notes: Option<String>,
        #[arg(long)]
        current_focus: Option<String>,
        #[arg(long)]
        success_criteria: Option<String>,
        #[arg(long)]
        exit_condition: Option<String>,
        #[arg(long)]
        next_recommended_step: Option<String>,
        #[arg(long)]
        standing_order: Option<String>,
        #[arg(long)]
        enabled: Option<bool>,
        #[arg(long)]
        natural_language_schedule: Option<String>,
        #[arg(long, value_enum)]
        schedule_type: Option<ObjectiveScheduleTypeArg>,
        #[arg(long)]
        schedule: Option<String>,
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
        #[arg(long, default_value_t = false)]
        json: bool,
    },
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
