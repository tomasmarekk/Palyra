//! Single-shot message routing pipeline for the gateway `RouteMessage` surface.
//!
//! Unlike `run_stream`'s interactive bidirectional loop, a routed message is
//! one channel-originated request that must produce a complete outbound reply
//! in a single response: [`orchestration`] drives session resolution, policy
//! checks, and the single provider exchange; [`tool_flow`] executes tool
//! proposals inline (results summarized back to the model rather than
//! streamed); [`approval`] records approval requests that have no interactive
//! client to answer them; and [`response`] turns provider output into
//! size-bounded outbound messages. Every step still lands on the orchestrator
//! tape so routed runs replay like streamed ones.

pub(crate) mod approval;
pub(crate) mod orchestration;
pub(crate) mod response;
pub(crate) mod tool_flow;
