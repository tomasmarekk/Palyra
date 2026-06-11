//! Daemon client adapters: HTTP and gRPC transports the CLI uses to talk to
//! `palyrad` (gateway runtime, control plane, channels, skills, messages).
//! Command modules build requests here; output formatting stays in `output`.

pub(crate) mod channels;
pub(crate) mod control_plane;
pub(crate) mod grpc;
pub(crate) mod message;
pub(crate) mod operator;
pub(crate) mod runtime;
pub(crate) mod skills;
