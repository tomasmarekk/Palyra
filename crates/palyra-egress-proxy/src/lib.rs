//! Outbound egress policy evaluation for proxied HTTP requests.
//!
//! [`EgressProxyPolicyService`] validates scheme, host allowlists, resolved addresses
//! (blocking private/local targets, including mixed DNS-rebinding answers), and vault-only
//! credential bindings before `palyra-daemon`'s HTTP fetch tool performs any network I/O.
//! Every check fails closed: a request is sendable only when evaluation returns a verdict.

use std::net::SocketAddr;

use palyra_common::{
    netguard,
    secret_refs::{SecretRef, SecretSource},
};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Plan for injecting one vault-backed credential header into a proxied request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialBindingPlan {
    /// Header that will carry the secret; must be credential-shaped, otherwise evaluation
    /// fails with [`EgressPolicyError::InvalidCredentialHeader`].
    pub header_name: String,
    /// Secret source to inject into the header. Egress policy only permits vault-backed refs.
    pub secret_ref: SecretRef,
    /// Whether the proxied request must fail when the secret cannot be resolved.
    pub required: bool,
}

/// Borrowed view of an outbound request to evaluate against egress policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EgressProxyRequest<'a> {
    /// HTTP method; policy-neutral but part of the request fingerprint.
    pub method: &'a str,
    /// Absolute target URL; only `http` and `https` schemes pass evaluation.
    pub url: &'a str,
    /// Operator opt-in permitting resolution to private/loopback/link-local addresses.
    pub allow_private_targets: bool,
    /// Exact-match host allowlist; empty together with `allowed_dns_suffixes` means any host.
    pub allowed_hosts: &'a [String],
    /// DNS-suffix allowlist (`example.com` also matches `api.example.com`).
    pub allowed_dns_suffixes: &'a [String],
    /// Response body budget in bytes; must be greater than zero.
    pub max_response_bytes: usize,
    /// Credential headers to inject; every binding must be vault-backed.
    pub credential_bindings: &'a [CredentialBindingPlan],
}

/// Outcome of a successful egress policy evaluation.
///
/// Only produced for allowed requests; every denial surfaces as an [`EgressPolicyError`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EgressPolicyVerdict {
    /// Always `true` today, because denials are returned as errors instead of verdicts.
    pub allowed: bool,
    /// Stable machine-readable reason (`egress.allowed`).
    pub reason_code: String,
    /// Human-readable summary for audit logs.
    pub message: String,
    /// Hex SHA-256 over the policy-relevant request fields, for audit correlation.
    pub request_fingerprint_sha256: String,
    /// Lowercased target host.
    pub host: String,
    /// Resolved socket addresses for connection pinning; runtime-only, never serialized.
    #[serde(skip_serializing, skip_deserializing, default)]
    pub resolved_addresses: Vec<SocketAddr>,
    /// String form of [`Self::resolved_addresses`] that survives serialization.
    pub resolved_socket_addrs: Vec<String>,
    /// Lowercased names of headers approved for credential injection.
    pub injected_credential_headers: Vec<String>,
}

/// Policy denials and validation failures for an egress request.
///
/// Every variant is fail-closed: any error means the request must not be sent.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum EgressPolicyError {
    /// URL parsing failed before scheme, host, or credential checks could run.
    #[error("URL parse failed: {message}")]
    InvalidUrl { message: String },
    /// URL scheme is neither `http` nor `https`.
    #[error("unsupported URL scheme '{0}'")]
    UnsupportedScheme(String),
    /// URL embeds userinfo credentials (`user:pass@host`), which policy never allows.
    #[error("URL credentials are not allowed")]
    CredentialsForbidden,
    /// URL has no host component.
    #[error("URL host is required")]
    MissingHost,
    /// URL has no explicit port and the scheme has no known default.
    #[error("URL port could not be resolved")]
    MissingPort,
    /// DNS resolution succeeded but returned an empty address set.
    #[error("DNS resolution returned no addresses for host '{host}'")]
    EmptyResolution { host: String },
    /// DNS resolution or IP-literal parsing failed.
    #[error("DNS resolution failed for host '{host}': {message}")]
    DnsResolution { host: String, message: String },
    /// Host matched neither the exact-host nor the DNS-suffix allowlist.
    #[error("host '{host}' is not present in the egress allowlist")]
    HostNotAllowlisted { host: String },
    /// At least one resolved address is private/local and the request did not opt in.
    #[error("target resolves to private/local address and is blocked by policy")]
    PrivateTargetBlocked,
    /// `max_response_bytes` was zero.
    #[error("response budget must be greater than zero")]
    InvalidResponseBudget,
    /// Credential header name is not credential-shaped.
    #[error("credential binding '{header_name}' uses a disallowed header name")]
    InvalidCredentialHeader { header_name: String },
    /// Credential binding references an exec-backed secret source.
    #[error("credential binding '{header_name}' cannot use exec-backed secret sources")]
    ExecCredentialSourceForbidden { header_name: String },
    /// Credential binding references a non-vault, non-exec secret source (e.g. env).
    #[error("credential binding '{header_name}' cannot use {source_kind}-backed secret sources")]
    CredentialSourceForbidden { header_name: String, source_kind: String },
    /// Credential binding's vault reference failed its own validation.
    #[error("credential binding '{header_name}' has invalid secret reference: {message}")]
    InvalidCredentialSecretRef { header_name: String, message: String },
}

/// Stateless evaluator applying the deny-by-default egress policy to outbound requests.
#[derive(Debug, Default)]
pub struct EgressProxyPolicyService;

impl EgressProxyPolicyService {
    /// Evaluates an outbound request against the egress policy and returns an allow verdict.
    ///
    /// Performs blocking DNS resolution for non-IP-literal hosts; on an async runtime,
    /// call this from a blocking-capable context.
    ///
    /// # Errors
    ///
    /// Returns an [`EgressPolicyError`] (fail closed) when the response budget is zero, the
    /// URL is malformed, non-HTTP(S), credentialed, or hostless, the host is not
    /// allowlisted, resolution fails or yields a private/local address without
    /// `allow_private_targets`, or any credential binding is not a valid vault-backed
    /// reference.
    pub fn evaluate_request(
        &self,
        request: &EgressProxyRequest<'_>,
    ) -> Result<EgressPolicyVerdict, EgressPolicyError> {
        if request.max_response_bytes == 0 {
            return Err(EgressPolicyError::InvalidResponseBudget);
        }

        let url = Url::parse(request.url)
            .map_err(|error| EgressPolicyError::InvalidUrl { message: error.to_string() })?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(EgressPolicyError::UnsupportedScheme(url.scheme().to_owned()));
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(EgressPolicyError::CredentialsForbidden);
        }

        let host = url.host_str().ok_or(EgressPolicyError::MissingHost)?.to_ascii_lowercase();
        let port = url.port_or_known_default().ok_or(EgressPolicyError::MissingPort)?;
        validate_host_allowlist(
            host.as_str(),
            request.allowed_hosts,
            request.allowed_dns_suffixes,
        )?;
        let resolved = resolve_socket_addrs(host.as_str(), port)?;
        validate_resolved_addrs(resolved.as_slice(), request.allow_private_targets)?;
        validate_credential_bindings(request.credential_bindings)?;

        let resolved_socket_addrs = resolved.iter().map(ToString::to_string).collect();
        Ok(EgressPolicyVerdict {
            allowed: true,
            reason_code: "egress.allowed".to_owned(),
            message: format!(
                "egress allowed for host '{host}' with {} resolved address(es)",
                resolved.len()
            ),
            request_fingerprint_sha256: request_fingerprint(request),
            host,
            resolved_addresses: resolved,
            resolved_socket_addrs,
            injected_credential_headers: request
                .credential_bindings
                .iter()
                .map(|binding| binding.header_name.to_ascii_lowercase())
                .collect(),
        })
    }
}

/// Validates already-resolved socket addresses against the private-target policy.
///
/// A single private/local address rejects the whole set: mixed public/private answers are
/// treated as DNS rebinding. Exposed so callers with their own resolution step (and the
/// security attack-scenario suite) can re-check addresses directly.
///
/// # Errors
///
/// Returns [`EgressPolicyError::PrivateTargetBlocked`] when `addrs` is empty, or when it
/// contains a private, loopback, or otherwise non-public address while
/// `allow_private_targets` is `false`.
pub fn validate_resolved_addrs(
    addrs: &[SocketAddr],
    allow_private_targets: bool,
) -> Result<(), EgressPolicyError> {
    let ips = addrs.iter().map(|address| address.ip()).collect::<Vec<_>>();
    netguard::validate_resolved_ip_addrs(ips.as_slice(), allow_private_targets)
        .map_err(|_| EgressPolicyError::PrivateTargetBlocked)
}

fn validate_host_allowlist(
    host: &str,
    allowed_hosts: &[String],
    allowed_dns_suffixes: &[String],
) -> Result<(), EgressPolicyError> {
    if allowed_hosts.is_empty() && allowed_dns_suffixes.is_empty() {
        return Ok(());
    }
    let host_allowed = allowed_hosts.iter().any(|candidate| candidate.eq_ignore_ascii_case(host));
    let suffix_allowed = allowed_dns_suffixes.iter().any(|suffix| {
        let normalized = suffix.trim().trim_start_matches('.').to_ascii_lowercase();
        // Suffixes match only on label boundaries: "example.com" must not allow
        // "notexample.com", hence the required '.' before the suffix.
        !normalized.is_empty()
            && (host == normalized
                || host
                    .strip_suffix(normalized.as_str())
                    .is_some_and(|prefix| prefix.ends_with('.')))
    });
    if host_allowed || suffix_allowed {
        return Ok(());
    }
    Err(EgressPolicyError::HostNotAllowlisted { host: host.to_owned() })
}

fn resolve_socket_addrs(host: &str, port: u16) -> Result<Vec<SocketAddr>, EgressPolicyError> {
    let addrs = if let Some(ip) = netguard::parse_host_ip_literal(host).map_err(|error| {
        EgressPolicyError::DnsResolution { host: host.to_owned(), message: error }
    })? {
        vec![SocketAddr::new(ip, port)]
    } else {
        std::net::ToSocketAddrs::to_socket_addrs(&(host, port))
            .map_err(|error| EgressPolicyError::DnsResolution {
                host: host.to_owned(),
                message: error.to_string(),
            })?
            .collect::<Vec<_>>()
    };
    if addrs.is_empty() {
        return Err(EgressPolicyError::EmptyResolution { host: host.to_owned() });
    }
    Ok(addrs)
}

fn validate_credential_bindings(
    bindings: &[CredentialBindingPlan],
) -> Result<(), EgressPolicyError> {
    for binding in bindings {
        let normalized = binding.header_name.trim().to_ascii_lowercase();
        // Only credential-shaped header names may carry injected secrets; this blocks
        // smuggling secrets into arbitrary headers such as Host or User-Agent.
        if normalized.is_empty()
            || !(normalized.starts_with("authorization")
                || normalized.starts_with("x-")
                || normalized.ends_with("-token")
                || normalized.ends_with("-api-key")
                || normalized == "cookie")
        {
            return Err(EgressPolicyError::InvalidCredentialHeader {
                header_name: binding.header_name.clone(),
            });
        }
        match &binding.secret_ref.source {
            SecretSource::Vault { .. } => {
                binding.secret_ref.validate().map_err(|error| {
                    EgressPolicyError::InvalidCredentialSecretRef {
                        header_name: binding.header_name.clone(),
                        message: error.to_string(),
                    }
                })?;
            }
            SecretSource::Exec { .. } => {
                return Err(EgressPolicyError::ExecCredentialSourceForbidden {
                    header_name: binding.header_name.clone(),
                });
            }
            source => {
                return Err(EgressPolicyError::CredentialSourceForbidden {
                    header_name: binding.header_name.clone(),
                    source_kind: source.kind().to_owned(),
                });
            }
        }
    }
    Ok(())
}

fn request_fingerprint(request: &EgressProxyRequest<'_>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.egress.proxy.v2");
    hash_bytes(&mut hasher, b"method", request.method.as_bytes());
    hash_bytes(&mut hasher, b"url", request.url.as_bytes());
    hash_bytes(&mut hasher, b"allow_private_targets", &[u8::from(request.allow_private_targets)]);
    hash_u64(
        &mut hasher,
        b"max_response_bytes",
        u64::try_from(request.max_response_bytes).unwrap_or(u64::MAX),
    );
    hash_u64(&mut hasher, b"allowed_hosts.len", request.allowed_hosts.len() as u64);
    for host in request.allowed_hosts {
        hash_bytes(&mut hasher, b"allowed_hosts.item", host.as_bytes());
    }
    hash_u64(&mut hasher, b"allowed_dns_suffixes.len", request.allowed_dns_suffixes.len() as u64);
    for suffix in request.allowed_dns_suffixes {
        hash_bytes(&mut hasher, b"allowed_dns_suffixes.item", suffix.as_bytes());
    }
    hash_u64(&mut hasher, b"credential_bindings.len", request.credential_bindings.len() as u64);
    for binding in request.credential_bindings {
        hash_bytes(&mut hasher, b"credential_header", binding.header_name.as_bytes());
        hash_bytes(
            &mut hasher,
            b"credential_secret_ref",
            binding.secret_ref.fingerprint().as_bytes(),
        );
        hash_bytes(&mut hasher, b"credential_required", &[u8::from(binding.required)]);
    }
    hex::encode(hasher.finalize())
}

fn hash_bytes(hasher: &mut Sha256, field: &[u8], value: &[u8]) {
    hash_u64(hasher, b"field_name_len", field.len() as u64);
    hasher.update(field);
    hash_u64(hasher, b"value_len", value.len() as u64);
    hasher.update(value);
}

fn hash_u64(hasher: &mut Sha256, field: &[u8], value: u64) {
    hasher.update((field.len() as u64).to_be_bytes());
    hasher.update(field);
    hasher.update(value.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use palyra_common::secret_refs::{
        SecretRef, SecretRefreshPolicy, SecretSnapshotPolicy, SecretSource,
    };

    use super::{
        request_fingerprint, validate_resolved_addrs, CredentialBindingPlan, EgressPolicyError,
        EgressProxyPolicyService, EgressProxyRequest,
    };

    fn binding(header_name: &str) -> CredentialBindingPlan {
        CredentialBindingPlan {
            header_name: header_name.to_owned(),
            secret_ref: SecretRef::from_legacy_vault_ref("global/example"),
            required: true,
        }
    }

    fn exec_binding(header_name: &str) -> CredentialBindingPlan {
        CredentialBindingPlan {
            header_name: header_name.to_owned(),
            secret_ref: SecretRef {
                source: SecretSource::Exec {
                    command: vec!["echo".to_owned(), "secret".to_owned()],
                    inherited_env: Vec::new(),
                    cwd: None,
                },
                required: true,
                refresh_policy: SecretRefreshPolicy::OnStartup,
                snapshot_policy: SecretSnapshotPolicy::FreezeUntilReload,
                max_bytes: None,
                exec_timeout_ms: None,
                redaction_label: None,
                display_name: None,
            },
            required: true,
        }
    }

    fn env_binding(header_name: &str) -> CredentialBindingPlan {
        CredentialBindingPlan {
            header_name: header_name.to_owned(),
            secret_ref: SecretRef {
                source: SecretSource::Env { variable: "PALYRA_SECRET".to_owned() },
                required: true,
                refresh_policy: SecretRefreshPolicy::OnStartup,
                snapshot_policy: SecretSnapshotPolicy::FreezeUntilReload,
                max_bytes: None,
                exec_timeout_ms: None,
                redaction_label: None,
                display_name: None,
            },
            required: true,
        }
    }

    #[test]
    fn egress_proxy_allows_explicit_allowlisted_host() {
        let service = EgressProxyPolicyService;
        let request = EgressProxyRequest {
            method: "GET",
            url: "https://93.184.216.34/path",
            allow_private_targets: false,
            allowed_hosts: &["93.184.216.34".to_owned()],
            allowed_dns_suffixes: &[],
            max_response_bytes: 1024,
            credential_bindings: &[binding("authorization")],
        };
        let verdict = service.evaluate_request(&request).expect("request should pass");
        assert!(verdict.allowed);
        assert_eq!(verdict.reason_code, "egress.allowed");
        assert_eq!(verdict.host, "93.184.216.34");
    }

    #[test]
    fn egress_proxy_rejects_exec_secret_credential_bindings() {
        let service = EgressProxyPolicyService;
        let request = EgressProxyRequest {
            method: "GET",
            url: "https://93.184.216.34/path",
            allow_private_targets: false,
            allowed_hosts: &["93.184.216.34".to_owned()],
            allowed_dns_suffixes: &[],
            max_response_bytes: 1024,
            credential_bindings: &[exec_binding("x-palyra-secret")],
        };
        let error = service
            .evaluate_request(&request)
            .expect_err("exec-backed credential binding must be rejected before resolution");
        assert_eq!(
            error,
            EgressPolicyError::ExecCredentialSourceForbidden {
                header_name: "x-palyra-secret".to_owned()
            }
        );
    }

    #[test]
    fn malformed_url_error_does_not_reuse_dns_resolution_or_leak_url() {
        let service = EgressProxyPolicyService;
        let request = EgressProxyRequest {
            method: "GET",
            url: "https://exa mple.test/path?token=secret",
            allow_private_targets: false,
            allowed_hosts: &[],
            allowed_dns_suffixes: &[],
            max_response_bytes: 1024,
            credential_bindings: &[],
        };
        let error =
            service.evaluate_request(&request).expect_err("malformed URL should be rejected");

        assert!(matches!(error, EgressPolicyError::InvalidUrl { .. }));
        assert!(!error.to_string().contains("token=secret"));
    }

    #[test]
    fn egress_proxy_rejects_non_vault_credential_bindings() {
        let service = EgressProxyPolicyService;
        let request = EgressProxyRequest {
            method: "GET",
            url: "https://93.184.216.34/path",
            allow_private_targets: false,
            allowed_hosts: &["93.184.216.34".to_owned()],
            allowed_dns_suffixes: &[],
            max_response_bytes: 1024,
            credential_bindings: &[env_binding("x-palyra-secret")],
        };
        let error = service
            .evaluate_request(&request)
            .expect_err("env-backed credential binding must be rejected before resolution");
        assert_eq!(
            error,
            EgressPolicyError::CredentialSourceForbidden {
                header_name: "x-palyra-secret".to_owned(),
                source_kind: "env".to_owned(),
            }
        );
    }

    #[test]
    fn resolved_private_targets_are_blocked_fail_closed() {
        let addrs = vec![std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            443,
        )];
        let error = validate_resolved_addrs(addrs.as_slice(), false)
            .expect_err("loopback target should be rejected");
        assert_eq!(error, EgressPolicyError::PrivateTargetBlocked);
    }

    #[test]
    fn request_fingerprint_length_prefixes_method_and_url() {
        let first = EgressProxyRequest {
            method: "AB",
            url: "C",
            allow_private_targets: false,
            allowed_hosts: &[],
            allowed_dns_suffixes: &[],
            max_response_bytes: 1024,
            credential_bindings: &[],
        };
        let second = EgressProxyRequest {
            method: "A",
            url: "BC",
            allow_private_targets: false,
            allowed_hosts: &[],
            allowed_dns_suffixes: &[],
            max_response_bytes: 1024,
            credential_bindings: &[],
        };

        assert_ne!(request_fingerprint(&first), request_fingerprint(&second));
    }

    #[test]
    fn mixed_public_and_private_resolution_is_treated_like_dns_rebinding_and_rejected() {
        let addrs = vec![
            std::net::SocketAddr::new(
                std::net::IpAddr::V4(std::net::Ipv4Addr::new(93, 184, 216, 34)),
                443,
            ),
            std::net::SocketAddr::new(
                std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 7)),
                443,
            ),
        ];
        let error = validate_resolved_addrs(addrs.as_slice(), false)
            .expect_err("mixed private/public answers should fail closed");
        assert_eq!(error, EgressPolicyError::PrivateTargetBlocked);
    }
}
