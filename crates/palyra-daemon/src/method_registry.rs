//! Runtime method and scope registry for public daemon HTTP surfaces.
//!
//! The registry is generated from the router source so route additions and
//! method introspection drift together in tests instead of relying on stale
//! hand-maintained lists.

use serde::Serialize;

use crate::access_control::{
    PERMISSION_COMPAT_CHAT_CREATE, PERMISSION_COMPAT_EMBEDDINGS_CREATE,
    PERMISSION_COMPAT_MODELS_READ, PERMISSION_COMPAT_RESPONSES_CREATE,
    PERMISSION_COMPAT_TOOLS_INVOKE,
};

const METHOD_REGISTRY_SCHEMA_VERSION: u32 = 1;
const METHOD_REGISTRY_VERSION: &str = "method-registry.v1";
const ROUTER_SOURCE: &str = include_str!("transport/http/router.rs");

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct MethodRegistrySnapshot {
    pub(crate) schema_version: u32,
    pub(crate) registry_version: &'static str,
    pub(crate) source: RegistrySource,
    pub(crate) methods: Vec<MethodDescriptor>,
    pub(crate) scopes: Vec<ScopeDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct RegistrySource {
    pub(crate) route_table: &'static str,
    pub(crate) schema_hash_basis: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct MethodDescriptor {
    pub(crate) surface: String,
    pub(crate) route: String,
    pub(crate) http_method: String,
    pub(crate) method_name: String,
    pub(crate) stability: &'static str,
    pub(crate) required_scope: String,
    pub(crate) request_schema_id: String,
    pub(crate) request_schema_hash: String,
    pub(crate) response_schema_id: String,
    pub(crate) response_schema_hash: String,
    pub(crate) streaming_supported: bool,
    pub(crate) idempotency_supported: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) deprecated: Option<DeprecatedMethod>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct DeprecatedMethod {
    pub(crate) replacement_method: Option<String>,
    pub(crate) reason_code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct ScopeDescriptor {
    pub(crate) scope: &'static str,
    pub(crate) category: &'static str,
    pub(crate) description: &'static str,
    pub(crate) grants: &'static [&'static str],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublicRouteContract {
    pub(crate) http_method: String,
    pub(crate) path: String,
}

pub(crate) fn build_method_registry_snapshot() -> MethodRegistrySnapshot {
    let mut methods =
        public_route_contracts().into_iter().map(method_descriptor_for_route).collect::<Vec<_>>();
    methods.sort_by(|left, right| {
        (left.surface.as_str(), left.route.as_str(), left.http_method.as_str()).cmp(&(
            right.surface.as_str(),
            right.route.as_str(),
            right.http_method.as_str(),
        ))
    });

    MethodRegistrySnapshot {
        schema_version: METHOD_REGISTRY_SCHEMA_VERSION,
        registry_version: METHOD_REGISTRY_VERSION,
        source: RegistrySource {
            route_table: "crates/palyra-daemon/src/transport/http/router.rs",
            schema_hash_basis: "palyra.method_registry.v1:{request|response}:{method}:{route}",
        },
        methods,
        scopes: scope_registry(),
    }
}

pub(crate) fn public_route_contracts() -> Vec<PublicRouteContract> {
    parse_router_routes(ROUTER_SOURCE)
}

fn method_descriptor_for_route(route: PublicRouteContract) -> MethodDescriptor {
    let surface = surface_for_path(route.path.as_str()).to_owned();
    let method_name = method_name_for_route(route.http_method.as_str(), route.path.as_str());
    let request_schema_id = schema_id("request", route.http_method.as_str(), route.path.as_str());
    let response_schema_id = schema_id("response", route.http_method.as_str(), route.path.as_str());
    MethodDescriptor {
        stability: stability_for_path(route.path.as_str()),
        required_scope: required_scope_for_route(route.http_method.as_str(), route.path.as_str())
            .to_owned(),
        request_schema_hash: schema_hash(
            "request",
            route.http_method.as_str(),
            route.path.as_str(),
        ),
        response_schema_hash: schema_hash(
            "response",
            route.http_method.as_str(),
            route.path.as_str(),
        ),
        request_schema_id,
        response_schema_id,
        streaming_supported: route_supports_streaming(route.path.as_str()),
        idempotency_supported: route_supports_idempotency(
            route.http_method.as_str(),
            route.path.as_str(),
        ),
        deprecated: deprecated_route(route.http_method.as_str(), route.path.as_str()),
        surface,
        route: route.path,
        http_method: route.http_method,
        method_name,
    }
}

fn parse_router_routes(source: &str) -> Vec<PublicRouteContract> {
    let mut routes = Vec::new();
    let mut pending_path: Option<String> = None;

    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.contains(".route(") || pending_path.is_some() {
            if pending_path.is_none() {
                pending_path = extract_quoted_path(trimmed);
            }
            if pending_path.is_none() {
                continue;
            }
            let methods = extract_route_methods(trimmed);
            if !methods.is_empty() {
                if let Some(path) = pending_path.take() {
                    routes.extend(methods.into_iter().map(|method| PublicRouteContract {
                        http_method: method.to_owned(),
                        path: path.clone(),
                    }));
                }
            }
            continue;
        }

        if pending_path.is_none() {
            pending_path = extract_quoted_path(trimmed);
        }
    }

    routes.sort_by(|left, right| {
        (left.path.as_str(), left.http_method.as_str())
            .cmp(&(right.path.as_str(), right.http_method.as_str()))
    });
    routes.dedup_by(|left, right| left.path == right.path && left.http_method == right.http_method);
    routes
}

fn extract_quoted_path(line: &str) -> Option<String> {
    let start = line.find('"')?;
    let rest = &line[start + 1..];
    let end = rest.find('"')?;
    let candidate = &rest[..end];
    candidate.starts_with('/').then(|| candidate.to_owned())
}

fn extract_route_methods(line: &str) -> Vec<&'static str> {
    [
        ("get(", "GET"),
        ("post(", "POST"),
        ("delete(", "DELETE"),
        ("put(", "PUT"),
        ("patch(", "PATCH"),
    ]
    .into_iter()
    .filter_map(|(needle, method)| line.contains(needle).then_some(method))
    .collect()
}

fn surface_for_path(path: &str) -> &'static str {
    if path.starts_with("/admin/") {
        "admin"
    } else if path.starts_with("/console/") {
        "console"
    } else if path.starts_with("/v1/") {
        "compat"
    } else if path.starts_with("/canvas/") {
        "canvas"
    } else if path.starts_with("/realtime/") {
        "realtime"
    } else {
        "public"
    }
}

fn required_scope_for_route(method: &str, path: &str) -> &'static str {
    match (method, path) {
        ("GET", "/v1/models") | ("GET", "/v1/models/{model_id}") | ("GET", "/v1/capabilities") => {
            PERMISSION_COMPAT_MODELS_READ
        }
        ("POST", "/v1/chat/completions") => PERMISSION_COMPAT_CHAT_CREATE,
        ("POST", "/v1/embeddings") => PERMISSION_COMPAT_EMBEDDINGS_CREATE,
        ("POST", "/v1/responses")
        | ("GET", "/v1/responses/{response_id}")
        | ("DELETE", "/v1/responses/{response_id}")
        | ("POST", "/v1/runs")
        | ("GET", "/v1/runs/{run_id}")
        | ("GET", "/v1/runs/{run_id}/events")
        | ("POST", "/v1/runs/{run_id}/stop")
        | ("POST", "/v1/runs/{run_id}/detach")
        | ("POST", "/v1/runs/{run_id}/approval") => PERMISSION_COMPAT_RESPONSES_CREATE,
        ("POST", "/v1/tools/invoke") => PERMISSION_COMPAT_TOOLS_INVOKE,
        _ if path.starts_with("/healthz") || path == "/runtime" => "public.read",
        _ if path.contains("/realtime/ws") => "realtime.stream",
        _ if path.starts_with("/canvas/") => "canvas.read",
        _ if path.starts_with("/admin/") && method == "GET" => "admin.read",
        _ if path.starts_with("/admin/") => "admin.write",
        _ if path.contains("/approvals") && method == "GET" => "approval.read",
        _ if path.contains("/approvals") => "approval.write",
        _ if path.contains("/secrets") && method == "GET" => "secrets.read",
        _ if path.contains("/secrets") => "secrets.write",
        _ if path.contains("/config") && method == "GET" => "config.read",
        _ if path.contains("/config") => "config.write",
        _ if path.starts_with("/console/") && method == "GET" => "console.read",
        _ if path.starts_with("/console/") => "console.write",
        _ => "public.read",
    }
}

fn stability_for_path(path: &str) -> &'static str {
    if path.starts_with("/v1/") || path.starts_with("/admin/") {
        "stable"
    } else if path.starts_with("/canvas/") || path.starts_with("/realtime/") {
        "preview"
    } else {
        "gated_production"
    }
}

fn route_supports_streaming(path: &str) -> bool {
    path.ends_with("/ws")
        || path.ends_with("/tail")
        || path.ends_with("/events")
        || path.contains("/stream")
}

fn route_supports_idempotency(method: &str, path: &str) -> bool {
    method == "GET"
        || matches!(
            (method, path),
            ("POST", "/v1/responses")
                | ("POST", "/v1/runs")
                | ("POST", "/v1/runs/{run_id}/stop")
                | ("POST", "/v1/runs/{run_id}/detach")
                | ("POST", "/v1/runs/{run_id}/approval")
        )
        || path.ends_with("/checkpoint")
        || path.ends_with("/retry")
}

fn deprecated_route(_method: &str, _path: &str) -> Option<DeprecatedMethod> {
    None
}

fn method_name_for_route(method: &str, path: &str) -> String {
    let mut parts = vec![surface_for_path(path).to_owned(), method.to_ascii_lowercase()];
    parts.extend(
        path.trim_matches('/')
            .split('/')
            .filter(|segment| !segment.is_empty())
            .map(normalize_method_segment),
    );
    parts.join(".")
}

fn schema_id(direction: &str, method: &str, path: &str) -> String {
    format!("{}.{}.{}", method_name_for_route(method, path), direction, METHOD_REGISTRY_VERSION)
}

fn schema_hash(direction: &str, method: &str, path: &str) -> String {
    crate::sha256_hex(
        format!("palyra.method_registry.v1:{direction}:{}:{path}", method.to_ascii_uppercase())
            .as_bytes(),
    )
}

fn normalize_method_segment(segment: &str) -> String {
    let trimmed = segment.trim_start_matches("{*").trim_start_matches('{').trim_end_matches('}');
    let prefix = if segment.starts_with('{') { "by_" } else { "" };
    let mut normalized = String::new();
    normalized.push_str(prefix);
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
        } else {
            normalized.push('_');
        }
    }
    if segment.starts_with("{*") {
        normalized.push_str("_wildcard");
    }
    normalized
}

fn scope_registry() -> Vec<ScopeDescriptor> {
    vec![
        scope("public.read", "read", "Unauthenticated read-only health and handoff metadata.", &[]),
        scope("admin.read", "admin", "Authenticated read-only daemon administration.", &[]),
        scope("admin.write", "admin", "Authenticated daemon administration mutations.", &[]),
        scope("console.read", "read", "Authenticated console read operations.", &[]),
        scope("console.write", "write", "Authenticated console mutation operations.", &[]),
        scope("approval.read", "approval", "Read pending approval and decision state.", &[]),
        scope("approval.write", "approval", "Submit or mutate approval decisions.", &[]),
        scope("config.read", "read", "Inspect daemon configuration without mutation.", &[]),
        scope("config.write", "write", "Mutate, migrate, or repair daemon configuration.", &[]),
        scope("secrets.read", "admin", "List or reveal scoped secret metadata.", &[]),
        scope("secrets.write", "admin", "Create, update, reveal, or delete scoped secrets.", &[]),
        scope("canvas.read", "read", "Read canvas runtime assets and state.", &[]),
        scope("realtime.stream", "read", "Open realtime websocket streams.", &[]),
        scope(
            PERMISSION_COMPAT_MODELS_READ,
            "read",
            "List OpenAI-compatible models and model details.",
            &[PERMISSION_COMPAT_MODELS_READ],
        ),
        scope(
            PERMISSION_COMPAT_CHAT_CREATE,
            "write",
            "Create OpenAI-compatible chat completion requests.",
            &[PERMISSION_COMPAT_CHAT_CREATE],
        ),
        scope(
            PERMISSION_COMPAT_EMBEDDINGS_CREATE,
            "write",
            "Create OpenAI-compatible embedding requests.",
            &[PERMISSION_COMPAT_EMBEDDINGS_CREATE],
        ),
        scope(
            PERMISSION_COMPAT_RESPONSES_CREATE,
            "write",
            "Create OpenAI-compatible responses requests.",
            &[PERMISSION_COMPAT_RESPONSES_CREATE],
        ),
        scope(
            PERMISSION_COMPAT_TOOLS_INVOKE,
            "write",
            "Invoke the conservative OpenAI-compatible tool-call boundary.",
            &[PERMISSION_COMPAT_TOOLS_INVOKE],
        ),
    ]
}

fn scope(
    scope: &'static str,
    category: &'static str,
    description: &'static str,
    grants: &'static [&'static str],
) -> ScopeDescriptor {
    ScopeDescriptor { scope, category, description, grants }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    #[test]
    fn router_source_parser_finds_public_route_contracts() {
        let contracts = public_route_contracts();

        assert!(
            contracts
                .iter()
                .any(|route| route.http_method == "GET" && route.path == "/admin/v1/status"),
            "admin status route should be parsed from router.rs"
        );
        assert!(
            contracts.iter().any(|route| {
                route.http_method == "POST" && route.path == "/v1/chat/completions"
            }),
            "compat chat route should be parsed from router.rs"
        );
        assert!(
            contracts.iter().any(|route| {
                route.http_method == "DELETE" && route.path == "/v1/responses/{response_id}"
            }),
            "chained compat delete route should be parsed from router.rs"
        );
        assert!(
            contracts.len() > 100,
            "method registry should cover the broad public HTTP route table"
        );
    }

    #[test]
    fn method_registry_covers_every_public_route_with_scope_and_schema_hashes() {
        let contracts = public_route_contracts();
        let snapshot = build_method_registry_snapshot();
        let by_route = snapshot
            .methods
            .iter()
            .map(|descriptor| {
                ((descriptor.http_method.as_str(), descriptor.route.as_str()), descriptor)
            })
            .collect::<BTreeMap<_, _>>();
        let scopes = snapshot.scopes.iter().map(|entry| entry.scope).collect::<BTreeSet<_>>();

        for contract in &contracts {
            let descriptor = by_route
                .get(&(contract.http_method.as_str(), contract.path.as_str()))
                .unwrap_or_else(|| {
                    panic!(
                        "missing method descriptor for {} {}",
                        contract.http_method, contract.path
                    )
                });
            assert!(
                scopes.contains(descriptor.required_scope.as_str()),
                "descriptor {} {} should use a registered scope",
                descriptor.http_method,
                descriptor.route
            );
            assert_eq!(descriptor.request_schema_hash.len(), 64);
            assert_eq!(descriptor.response_schema_hash.len(), 64);
        }

        assert_eq!(snapshot.methods.len(), contracts.len());
        for descriptor in &snapshot.methods {
            if descriptor.stability == "deprecated" {
                let deprecated = descriptor
                    .deprecated
                    .as_ref()
                    .expect("deprecated descriptors should carry metadata");
                assert!(
                    deprecated.replacement_method.is_some()
                        || !deprecated.reason_code.trim().is_empty(),
                    "deprecated descriptors should name a replacement or reason code"
                );
            }
        }
    }
}
