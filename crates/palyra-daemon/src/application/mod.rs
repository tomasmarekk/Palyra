//! Application layer of the daemon: channel routing, session orchestration,
//! tool execution, memory, and approval flows built on top of the gateway
//! runtime and journal. Submodules are grouped by capability; `route_message`
//! and `run_stream` are the inbound and provider-turn entry points.

pub mod approvals;
pub mod auth;
pub mod channel_commands;
pub mod channel_turn;
pub mod channels;
pub mod code_intel_runtime;
pub mod context_compression;
pub mod context_engine;
pub mod context_references;
pub mod conversation_bindings;
pub mod delivery_arbitration;
pub mod execution_gate;
pub mod inbound_coalescer;
pub mod instruction_compiler;
pub mod learning;
pub mod mcp_broker;
pub mod memory;
pub mod memory_provider;
pub mod outbound_lifecycle;
pub mod plan_state;
pub mod progress_draft;
pub mod project_context;
pub mod project_context_summary;
pub mod project_facts;
pub mod provider_events;
pub mod provider_input;
pub mod recall;
pub mod service_authorization;
pub mod session_compaction;
pub mod session_pruning;
pub mod session_queue;
pub mod tool_jobs;
pub mod tool_registry;
pub mod tool_runtime;
pub mod tool_security;
pub mod verification;
pub mod workspace_observability;

pub mod route_message;
pub mod run_stream;
