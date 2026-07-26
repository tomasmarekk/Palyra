//! Daemon configuration: validated runtime types ([`schema`]) and the loader
//! ([`load`]) that merges defaults, the TOML config file, and `PALYRA_*`
//! environment overrides (in that precedence order, env winning) into a
//! [`LoadedConfig`].
//!
//! The raw on-disk TOML shape lives in `palyra_common::daemon_config_schema`
//! (the `File*` serde types); this module owns the parsed, validated runtime
//! counterparts and their secure defaults.

pub(crate) mod load;
pub(crate) mod runtime_kernel;
pub(crate) mod schema;

pub use load::load_config;
pub(crate) use load::{config_recovery_paths, load_config_from_path};
pub use schema::*;
