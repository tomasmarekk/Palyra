//! Internal SQLite row types for metadata trace storage.
//!
//! Raw SQLite identifiers stay private to the journal and never enter the
//! operator-facing trace projection.

#[derive(Debug, Clone)]
pub(super) struct SegmentRow {
    pub(super) segment_id: String,
    pub(super) segment_index: i64,
    pub(super) generation: i64,
    pub(super) predecessor_segment_id: Option<String>,
    pub(super) schema_version: i64,
}
