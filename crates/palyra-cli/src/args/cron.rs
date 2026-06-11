//! Arguments for `palyra cron`: the schedule-only compatibility surface backed
//! by unified routines. The schedule/concurrency/misfire enums defined here are
//! shared with `routines.rs`, which is the first-class automation family.
//! Help text is pinned by snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::{ArgGroup, Subcommand, ValueEnum};

use super::routines::{
    RoutineApprovalModeArg, RoutineExecutionPostureArg, RoutinePreviewTimezoneArg,
};
use super::RequiredCommandIdArg;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum CronCommand {
    Status {
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
        #[command(flatten)]
        id: RequiredCommandIdArg,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(group(
        ArgGroup::new("cron_add_prompt_source")
            .required(true)
            .args(["prompt", "prompt_stdin"])
    ))]
    Add {
        #[arg(long)]
        name: String,
        #[arg(
            long,
            help = "Single-line prompt text. Use --prompt-stdin for multi-line or blank-line separated prompts."
        )]
        prompt: Option<String>,
        #[arg(long, default_value_t = false)]
        prompt_stdin: bool,
        #[arg(long, value_enum)]
        schedule_type: CronScheduleTypeArg,
        #[arg(
            long,
            help = "Schedule payload: cron expression for cron, RFC3339 timestamp for at, or milliseconds/duration such as 300000 or 5m for every"
        )]
        schedule: String,
        #[arg(
            long,
            default_value = "local",
            help = "Timezone used to evaluate cron expression schedules"
        )]
        timezone: RoutinePreviewTimezoneArg,
        #[arg(
            long,
            default_value_t = false,
            help = "Create the scheduled job enabled immediately; omitted jobs are created disabled"
        )]
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
        #[arg(
            long,
            value_name = "COUNT",
            help = "Automatically disable the job after this many executed runs"
        )]
        max_runs: Option<u32>,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        session_key: Option<String>,
        #[arg(long)]
        session_label: Option<String>,
        #[arg(
            long,
            value_name = "PATH",
            help = "Project working directory for scheduled runs; relative paths resolve from the current directory and are stored as absolute paths"
        )]
        workdir: Option<String>,
        #[arg(
            long,
            value_enum,
            help = "Tool posture for scheduled runs; when omitted, jobs with --workdir default to sensitive-tools with enable approval"
        )]
        execution_posture: Option<RoutineExecutionPostureArg>,
        #[arg(
            long,
            value_enum,
            help = "Approval gate for enabling or first-running this job; when omitted, sensitive-tool jobs default to before-enable"
        )]
        approval_mode: Option<RoutineApprovalModeArg>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(visible_alias = "edit")]
    Update {
        #[command(flatten)]
        id: RequiredCommandIdArg,
        #[arg(long)]
        name: Option<String>,
        #[arg(
            long,
            help = "Single-line prompt text. Use --prompt-stdin for multi-line or blank-line separated prompts."
        )]
        prompt: Option<String>,
        #[arg(long, default_value_t = false, conflicts_with = "prompt")]
        prompt_stdin: bool,
        #[arg(long, value_enum, requires = "schedule")]
        schedule_type: Option<CronScheduleTypeArg>,
        #[arg(
            long,
            requires = "schedule_type",
            help = "Schedule payload: cron expression for cron, RFC3339 timestamp for at, or milliseconds/duration such as 300000 or 5m for every"
        )]
        schedule: Option<String>,
        #[arg(long, help = "Timezone used to evaluate cron expression schedules")]
        timezone: Option<RoutinePreviewTimezoneArg>,
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
        #[arg(
            long,
            value_name = "COUNT",
            help = "Automatically disable the job after this many executed runs"
        )]
        max_runs: Option<u32>,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        session_key: Option<String>,
        #[arg(long)]
        session_label: Option<String>,
        #[arg(
            long,
            value_name = "PATH",
            help = "Update the project working directory for scheduled runs; relative paths resolve from the current directory"
        )]
        workdir: Option<String>,
        #[arg(long, value_enum)]
        execution_posture: Option<RoutineExecutionPostureArg>,
        #[arg(long, value_enum)]
        approval_mode: Option<RoutineApprovalModeArg>,
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
    #[command(visible_alias = "rm")]
    Delete {
        #[command(flatten)]
        id: RequiredCommandIdArg,
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
