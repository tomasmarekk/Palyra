//! Bidirectional run-stream pipeline for the gateway gRPC `RunStream` surface.
//!
//! A run streams `RunStreamEvent`s to the client while appending a replayable
//! orchestrator tape: [`orchestration`] drives the agent loop and provider
//! turns, [`agent_loop`] tracks turn/tool/wall-clock budgets, [`tool_flow`]
//! gates and executes tool proposals (approvals, policy, parallelism),
//! [`tape`] emits wire events paired with tape appends, and [`cancellation`]
//! handles the cancel transition shared by all of them.

pub(crate) mod agent_loop;
pub(crate) mod cancellation;
pub(crate) mod flow_control;
pub(crate) mod orchestration;
pub(crate) mod public_events;
pub(crate) mod tape;
pub(crate) mod tool_flow;
