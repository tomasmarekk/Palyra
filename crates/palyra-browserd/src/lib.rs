pub mod app;
pub mod domain;
pub mod engine;
pub mod infra;
pub mod persistence;
pub mod security;
pub mod support;
pub mod transport;

#[cfg(test)]
pub(crate) use app::bootstrap::enforce_non_loopback_bind_auth;
pub use app::bootstrap::run;
pub(crate) use app::*;
pub(crate) use domain::*;
pub(crate) use engine::*;
pub(crate) use persistence::*;
pub(crate) use security::*;
pub(crate) use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    ffi::{OsStr, OsString},
    fs,
    io::Write,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, LazyLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
pub(crate) use support::*;
#[cfg(test)]
pub(crate) use transport::grpc::BrowserServiceImpl;

pub(crate) use anyhow::{Context, Result};
pub(crate) use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
pub(crate) use base64::Engine as _;
pub(crate) use clap::Parser;
pub(crate) use headless_chrome::{
    browser::tab::RequestPausedDecision,
    protocol::cdp::{Fetch, Network, Page},
    Browser as HeadlessBrowser, LaunchOptionsBuilder, Tab as HeadlessTab,
};
pub(crate) use palyra_common::{
    build_metadata, health_response, netguard, parse_daemon_bind_socket, validate_canonical_id,
    HealthResponse, CANONICAL_PROTOCOL_MAJOR,
};
pub(crate) use reqwest::{redirect::Policy, Url};
pub(crate) use ring::{
    aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305},
    digest::{Context as DigestContext, SHA256},
    rand::{SecureRandom, SystemRandom},
};
pub(crate) use serde::{Deserialize, Serialize};
pub(crate) use tempfile::TempDir;
pub(crate) use tokio::time::{interval, MissedTickBehavior};
pub(crate) use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{oneshot, Mutex},
};
pub(crate) use tokio_stream::wrappers::TcpListenerStream;
pub(crate) use tonic::{transport::Server, Request, Response, Status};
pub(crate) use tracing::{info, warn};
pub(crate) use tracing_subscriber::EnvFilter;
pub(crate) use ulid::Ulid;

pub mod proto {
    pub mod palyra {
        pub mod common {
            pub mod v1 {
                tonic::include_proto!("palyra.common.v1");
            }
        }

        pub mod browser {
            pub mod v1 {
                tonic::include_proto!("palyra.browser.v1");
            }
        }
    }
}

pub(crate) use proto::palyra::browser::v1 as browser_v1;

#[cfg(test)]
#[path = "support/tests.rs"]
mod tests;
