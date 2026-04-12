//! Compatibility shim for the legacy `palyra-connector-core` crate.
//!
//! The generic connector core implementation now lives in
//! `palyra-connectors/src/core`. This crate remains temporarily to keep
//! downstream compatibility until provider code is absorbed in the next
//! milestone.

pub mod core;

pub use core::*;
