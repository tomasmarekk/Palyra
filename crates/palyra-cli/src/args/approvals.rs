use clap::{Subcommand, ValueEnum};

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
        #[arg(long, value_enum)]
        subject_type: Option<ApprovalSubjectTypeArg>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Show {
        approval_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Decide {
        approval_id: String,
        #[arg(long, value_enum)]
        decision: ApprovalResolveDecisionArg,
        #[arg(long, value_enum, default_value_t = ApprovalDecisionScopeArg::Once)]
        scope: ApprovalDecisionScopeArg,
        #[arg(long)]
        ttl_ms: Option<i64>,
        #[arg(long)]
        reason: Option<String>,
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
        #[arg(long, value_enum)]
        subject_type: Option<ApprovalSubjectTypeArg>,
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
pub enum ApprovalResolveDecisionArg {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum ApprovalDecisionScopeArg {
    #[default]
    Once,
    Session,
    Timeboxed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ApprovalSubjectTypeArg {
    Tool,
    ChannelSend,
    SecretAccess,
    BrowserAction,
    NodeCapability,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ApprovalExportFormatArg {
    Ndjson,
    Json,
}
