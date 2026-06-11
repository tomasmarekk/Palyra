//! Tonic-generated protobuf modules included from the build output.
//!
//! The source of truth lives under `schemas/proto`; do not edit generated
//! definitions here.

/// Generated Palyra protobuf package tree.
pub mod palyra {
    /// Common protocol DTOs shared across services.
    pub mod common {
        /// Version 1 common protocol DTOs.
        pub mod v1 {
            tonic::include_proto!("palyra.common.v1");
        }
    }

    /// Gateway service DTOs and service traits.
    pub mod gateway {
        /// Version 1 gateway protocol.
        pub mod v1 {
            tonic::include_proto!("palyra.gateway.v1");
        }
    }

    /// Cron service DTOs and service traits.
    pub mod cron {
        /// Version 1 cron protocol.
        pub mod v1 {
            tonic::include_proto!("palyra.cron.v1");
        }
    }

    /// Memory service DTOs and service traits.
    pub mod memory {
        /// Version 1 memory protocol.
        pub mod v1 {
            tonic::include_proto!("palyra.memory.v1");
        }
    }

    /// Auth service DTOs and service traits.
    pub mod auth {
        /// Version 1 auth protocol.
        pub mod v1 {
            tonic::include_proto!("palyra.auth.v1");
        }
    }

    /// Node RPC service DTOs and service traits.
    pub mod node {
        /// Version 1 node protocol.
        pub mod v1 {
            tonic::include_proto!("palyra.node.v1");
        }
    }

    /// Browser service DTOs and service traits.
    pub mod browser {
        /// Version 1 browser protocol.
        pub mod v1 {
            tonic::include_proto!("palyra.browser.v1");
        }
    }
}
