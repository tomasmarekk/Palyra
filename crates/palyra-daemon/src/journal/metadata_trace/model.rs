//! Closed wire-value helpers for metadata trace persistence.
//!
//! Keeping these mappings exhaustive prevents SQLite rows from drifting from
//! the shared typed contract when new variants are introduced.

use palyra_common::metadata_trace::{MetadataTraceEventDataV1, MetadataTraceSegmentStatusV1};

pub(super) fn event_kind(event: &MetadataTraceEventDataV1) -> &'static str {
    event.kind()
}

pub(super) fn status_name(status: &MetadataTraceSegmentStatusV1) -> &'static str {
    status.as_str()
}

pub(super) fn parse_status(status: &str) -> Option<MetadataTraceSegmentStatusV1> {
    match status {
        "complete" => Some(MetadataTraceSegmentStatusV1::Complete),
        "interrupted" => Some(MetadataTraceSegmentStatusV1::Interrupted),
        "corrupt_suffix_isolated" => Some(MetadataTraceSegmentStatusV1::CorruptSuffixIsolated),
        _ => None,
    }
}

pub(super) fn event_uses_terminal_reserve(event: &MetadataTraceEventDataV1) -> bool {
    matches!(
        event,
        MetadataTraceEventDataV1::Terminalization(_) | MetadataTraceEventDataV1::CapacityReached(_)
    )
}
