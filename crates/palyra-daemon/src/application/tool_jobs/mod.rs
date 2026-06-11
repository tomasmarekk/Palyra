//! Durable registry contract for long-running tool jobs.
//!
//! Re-exports the journal-backed tool-job types so application code can
//! depend on this module instead of reaching into `crate::journal` directly.

pub use crate::journal::{
    ToolJobAttachRequest, ToolJobCreateRequest, ToolJobRecord, ToolJobRetentionPolicy,
    ToolJobRetryPolicy, ToolJobRetryRequest, ToolJobState, ToolJobTailAppendRequest,
    ToolJobTailEntry, ToolJobTailPage, ToolJobTailReadRequest, ToolJobTailStream,
    ToolJobTransitionRequest, ToolJobsListFilter,
};
