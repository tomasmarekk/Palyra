//! Host-authoritative work graph contracts and validation.
//! The journal persists these records; this module owns their state and DAG invariants.

mod claims;
mod concurrency;
mod contracts;
mod validation;

pub(crate) use claims::*;
pub(crate) use concurrency::*;
pub(crate) use contracts::*;
pub(crate) use validation::{
    validate_graph_create_request, validate_loaded_graph, validate_transition,
};

#[cfg(test)]
mod tests;
