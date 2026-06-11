//! Egress guard for connector network targets.
//!
//! Combines a per-connector host allowlist with the shared
//! `palyra_common::netguard` checks so adapters can only dial allowlisted
//! public hosts; localhost, private, and unresolvable targets are rejected
//! fail-closed before any connection is attempted.

use std::net::IpAddr;

use palyra_common::netguard;
use thiserror::Error;

/// Host allowlist guard applied to connector egress targets before any dial.
///
/// Allowlist entries are exact hostnames or `*.suffix` wildcard patterns;
/// a wildcard matches subdomains only, never the apex domain itself.
#[derive(Debug, Clone)]
pub struct ConnectorNetGuard {
    allowlist: Vec<String>,
}

/// Reasons an allowlist pattern or runtime target host is rejected.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConnectorNetGuardError {
    #[error("target host must be non-empty")]
    EmptyHost,
    #[error("target host is malformed")]
    MalformedHost,
    #[error("target host '{0}' is not allowlisted")]
    HostNotAllowlisted(String),
    #[error("target host '{0}' is blocked as localhost")]
    LocalhostBlocked(String),
    #[error("target host '{0}' resolves to private/local addresses and is blocked")]
    PrivateAddressBlocked(String),
    #[error("target host '{0}' is rejected: {1}")]
    InvalidHostLiteral(String, String),
}

impl ConnectorNetGuard {
    /// Builds a guard from raw allowlist patterns, normalizing and deduplicating them.
    ///
    /// # Errors
    /// Returns [`ConnectorNetGuardError::EmptyHost`] or
    /// [`ConnectorNetGuardError::MalformedHost`] when a pattern is empty or
    /// contains characters outside the supported hostname alphabet.
    pub fn new(allowlist: &[String]) -> Result<Self, ConnectorNetGuardError> {
        let mut normalized = Vec::new();
        for host in allowlist {
            let value = normalize_host_pattern(host.as_str())?;
            if !normalized.contains(&value) {
                normalized.push(value);
            }
        }
        Ok(Self { allowlist: normalized })
    }

    /// Returns the normalized allowlist patterns in first-seen order.
    #[must_use]
    pub fn allowlist(&self) -> &[String] {
        self.allowlist.as_slice()
    }

    /// Validates a target host together with the addresses DNS resolved for it.
    ///
    /// IP-literal hosts are checked directly; hostname targets are checked
    /// against `resolved_addrs`, so callers must pass the exact addresses they
    /// will dial to keep DNS-rebinding out of the trust path.
    ///
    /// # Errors
    /// Returns an error when the host is malformed, not allowlisted, points at
    /// localhost, or resolves only to private/local addresses.
    pub fn validate_target(
        &self,
        host: &str,
        resolved_addrs: &[IpAddr],
    ) -> Result<(), ConnectorNetGuardError> {
        let normalized_host = normalize_runtime_host(host)?;
        if netguard::is_localhost_hostname(normalized_host.as_str()) {
            return Err(ConnectorNetGuardError::LocalhostBlocked(normalized_host));
        }
        if !self.is_allowlisted(normalized_host.as_str()) {
            return Err(ConnectorNetGuardError::HostNotAllowlisted(normalized_host));
        }
        if let Some(ip_literal) = netguard::parse_host_ip_literal(normalized_host.as_str())
            .map_err(|error| {
                ConnectorNetGuardError::InvalidHostLiteral(normalized_host.clone(), error)
            })?
        {
            if netguard::is_private_or_local_ip(ip_literal) {
                return Err(ConnectorNetGuardError::PrivateAddressBlocked(normalized_host));
            }
            return Ok(());
        }
        // Fail closed: an empty resolution set is treated like a private
        // address so a connector can never dial a host it could not resolve.
        if netguard::validate_resolved_ip_addrs(resolved_addrs, false).is_err() {
            return Err(ConnectorNetGuardError::PrivateAddressBlocked(normalized_host));
        }
        Ok(())
    }

    /// Returns whether the host matches any allowlist entry after normalization.
    #[must_use]
    pub fn is_allowlisted(&self, host: &str) -> bool {
        let normalized = host.trim().trim_end_matches('.').to_ascii_lowercase();
        self.allowlist
            .iter()
            .any(|candidate| host_matches_pattern(normalized.as_str(), candidate.as_str()))
    }
}

fn normalize_host_pattern(raw: &str) -> Result<String, ConnectorNetGuardError> {
    let trimmed = raw.trim().trim_end_matches('.').to_ascii_lowercase();
    if trimmed.is_empty() {
        return Err(ConnectorNetGuardError::EmptyHost);
    }
    let stripped = trimmed.strip_prefix("*.").unwrap_or(trimmed.as_str());
    if stripped.is_empty() || stripped.contains("..") {
        return Err(ConnectorNetGuardError::MalformedHost);
    }
    if !stripped.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '.') {
        return Err(ConnectorNetGuardError::MalformedHost);
    }
    Ok(trimmed)
}

fn normalize_runtime_host(raw: &str) -> Result<String, ConnectorNetGuardError> {
    let trimmed = raw.trim().trim_end_matches('.').to_ascii_lowercase();
    if trimmed.is_empty() {
        return Err(ConnectorNetGuardError::EmptyHost);
    }
    if trimmed.contains("..")
        || !trimmed
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '.' || ch == ':')
    {
        return Err(ConnectorNetGuardError::MalformedHost);
    }
    Ok(trimmed)
}

fn host_matches_pattern(host: &str, pattern: &str) -> bool {
    if let Some(suffix) = pattern.strip_prefix("*.") {
        // `*.suffix` requires at least one subdomain label; the apex domain
        // must be allowlisted explicitly.
        return host.strip_suffix(suffix).is_some_and(|head| head.ends_with('.'));
    }
    host == pattern
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::{ConnectorNetGuard, ConnectorNetGuardError};

    #[test]
    fn rejects_non_allowlisted_hosts() {
        let guard = ConnectorNetGuard::new(&["discord.com".to_owned(), "*.discord.com".to_owned()])
            .expect("guard should build");
        let result = guard.validate_target("example.com", &[]);
        assert_eq!(
            result,
            Err(ConnectorNetGuardError::HostNotAllowlisted("example.com".to_owned()))
        );
    }

    #[test]
    fn accepts_allowlisted_discord_hosts() {
        let guard = ConnectorNetGuard::new(&["discord.com".to_owned(), "*.discord.com".to_owned()])
            .expect("guard should build");
        let public = [IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))];
        assert!(guard.validate_target("discord.com", &public).is_ok());
        assert!(guard.validate_target("gateway.discord.com", &public).is_ok());
    }

    #[test]
    fn wildcard_allowlist_does_not_match_apex_host_without_explicit_entry() {
        let guard =
            ConnectorNetGuard::new(&["*.discord.com".to_owned()]).expect("guard should build");
        let public = [IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))];
        let result = guard.validate_target("discord.com", &public);
        assert_eq!(
            result,
            Err(ConnectorNetGuardError::HostNotAllowlisted("discord.com".to_owned()))
        );
    }

    #[test]
    fn blocks_private_ip_even_when_allowlisted() {
        let guard =
            ConnectorNetGuard::new(&["*.discord.com".to_owned()]).expect("guard should build");
        let private = [IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))];
        let result = guard.validate_target("gateway.discord.com", &private);
        assert_eq!(
            result,
            Err(ConnectorNetGuardError::PrivateAddressBlocked("gateway.discord.com".to_owned()))
        );
    }

    #[test]
    fn rejects_allowlisted_host_when_dns_resolution_is_empty() {
        let guard =
            ConnectorNetGuard::new(&["*.discord.com".to_owned()]).expect("guard should build");
        let result = guard.validate_target("gateway.discord.com", &[]);
        assert_eq!(
            result,
            Err(ConnectorNetGuardError::PrivateAddressBlocked("gateway.discord.com".to_owned()))
        );
    }
}
