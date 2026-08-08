//! Per-tool execution backends for the daemon tool runtime.
//!
//! Each submodule implements one tool family (browser, HTTP fetch, workspace
//! files, memory, routines, ...) behind the gateway's approval and policy
//! checks; dispatch happens in `crate::gateway`, which selects the module by
//! tool name and converts results into journaled tool outcomes.

pub(crate) mod artifacts;
pub(crate) mod browser;
pub(crate) mod code_intel;
pub(crate) mod context;
pub(crate) mod delegation;
pub(crate) mod document;
pub(crate) mod dynamic_tools;
pub(crate) mod http_fetch;
pub(crate) mod image_observe;
pub(crate) mod memory;
pub(crate) mod networked_worker;
pub(crate) mod os_file;
pub(crate) mod plan;
pub(crate) mod process_registry;
pub(crate) mod routines;
pub(crate) mod session_operations;
pub(crate) mod tool_program;
pub(crate) mod tool_rpc;
pub(crate) mod web_search;
pub(crate) mod work_graph;
pub(crate) mod workspace_file;
pub(crate) mod workspace_patch;
pub(crate) mod workspace_scope;
