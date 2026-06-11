//! SSRF guards shared by the egress proxy and HTTP fetch tooling.
//!
//! Classifies IP addresses as private/local/special-use and rejects non-canonical IPv4
//! literal forms that resolvers interpret differently than naive string checks. All
//! checks fail closed: anything ambiguous is treated as blocked.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Parses canonical IP literals and rejects legacy/non-canonical IPv4 literal forms
/// (decimal integer, dotted octal/hex, short dotted forms) to keep SSRF handling fail-closed.
///
/// Returns `Ok(None)` when `host` is a hostname rather than an IP literal.
///
/// # Errors
/// Returns an error message for an empty host or a non-canonical IPv4 literal.
pub fn parse_host_ip_literal(host: &str) -> Result<Option<IpAddr>, String> {
    let normalized = host.trim();
    if normalized.is_empty() {
        return Err("URL host is required".to_owned());
    }
    if is_non_canonical_ipv4_literal(normalized) {
        return Err(format!(
            "host '{normalized}' uses unsupported non-canonical IPv4 literal format and is blocked by policy"
        ));
    }
    Ok(normalized.parse::<IpAddr>().ok())
}

/// Validates a DNS answer set against the private-target policy.
///
/// A single private/local address fails the whole set: DNS can interleave public and
/// private answers (rebinding-style attacks), and the connector may pick any of them.
///
/// # Errors
/// Returns an error message for an empty answer set, or for any private/local/special-use
/// address unless `allow_private_targets` is set.
pub fn validate_resolved_ip_addrs(
    addrs: &[IpAddr],
    allow_private_targets: bool,
) -> Result<(), String> {
    if addrs.is_empty() {
        return Err("DNS resolution returned no addresses".to_owned());
    }
    if !allow_private_targets && addrs.iter().any(|address| is_private_or_local_ip(*address)) {
        return Err("target resolves to private/local address and is blocked by policy".to_owned());
    }
    Ok(())
}

/// Returns whether a hostname is `localhost` or a `*.localhost` name (RFC 6761).
#[must_use]
pub fn is_localhost_hostname(host: &str) -> bool {
    let normalized = host.trim_end_matches('.').to_ascii_lowercase();
    normalized == "localhost" || normalized.ends_with(".localhost")
}

/// Returns whether an address is private, loopback, link-local, or in a special-use range
/// that must never be reachable through policy-guarded egress.
#[must_use]
pub fn is_private_or_local_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(ipv4) => is_private_or_local_ipv4(ipv4),
        IpAddr::V6(ipv6) => is_private_or_local_ipv6(ipv6),
    }
}

fn is_private_or_local_ipv4(address: Ipv4Addr) -> bool {
    address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_unspecified()
        || address.is_multicast()
        || address == Ipv4Addr::BROADCAST
        || is_special_ipv4_ssrf_range(address)
}

fn is_private_or_local_ipv6(address: Ipv6Addr) -> bool {
    // IPv4-mapped addresses (::ffff:a.b.c.d) must be judged by the embedded IPv4 rules,
    // otherwise wrapping a private IPv4 target in IPv6 would bypass the guard.
    if let Some(mapped_ipv4) = address.to_ipv4_mapped() {
        return is_private_or_local_ipv4(mapped_ipv4);
    }
    address.is_loopback()
        || address.is_unicast_link_local()
        || address.is_unique_local()
        || address.is_unspecified()
        || address.is_multicast()
        || is_documentation_ipv6(address)
        || is_site_local_ipv6(address)
        || is_ipv6_6to4(address)
        || is_teredo_ipv6(address)
}

// Special-use IPv4 ranges beyond what std classifies: 0.0.0.0/8 ("this network"),
// 100.64/10 (carrier-grade NAT), 192.0.0/24 (protocol assignments), 192.0.2/24,
// 198.51.100/24, 203.0.113/24 (documentation TEST-NETs), 192.88.99/24 (6to4 relay),
// 198.18/15 (benchmarking), and 240/4 (reserved, includes 255.255.255.255).
fn is_special_ipv4_ssrf_range(address: Ipv4Addr) -> bool {
    let octets = address.octets();
    let first = octets[0];
    let second = octets[1];
    let third = octets[2];

    first == 0
        || (first == 100 && (64..=127).contains(&second))
        || (first == 192 && second == 0 && third == 0)
        || (first == 192 && second == 0 && third == 2)
        || (first == 192 && second == 88 && third == 99)
        || (first == 198 && second == 18)
        || (first == 198 && second == 19)
        || (first == 198 && second == 51 && third == 100)
        || (first == 203 && second == 0 && third == 113)
        || first >= 240
}

fn is_documentation_ipv6(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    segments[0] == 0x2001 && segments[1] == 0x0db8
}

fn is_site_local_ipv6(address: Ipv6Addr) -> bool {
    (address.segments()[0] & 0xffc0) == 0xfec0
}

fn is_ipv6_6to4(address: Ipv6Addr) -> bool {
    address.segments()[0] == 0x2002
}

fn is_teredo_ipv6(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    segments[0] == 0x2001 && segments[1] == 0
}

// Detects IPv4 spellings that `IpAddr::from_str` rejects but OS resolvers and libcurl
// accept (decimal "2130706433", hex "0x7f000001", octal "0177.0.0.1", short "127.1").
// Allowing them through as "hostnames" would let DNS-stage code resolve them to
// addresses the literal-stage guard never inspected.
fn is_non_canonical_ipv4_literal(host: &str) -> bool {
    let normalized = host.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return false;
    }
    if normalized.starts_with("0x")
        && normalized.len() > 2
        && normalized[2..].bytes().all(|value| value.is_ascii_hexdigit())
    {
        return true;
    }
    if normalized.bytes().all(|value| value.is_ascii_digit()) {
        return true;
    }
    if !normalized.contains('.') {
        return false;
    }

    let segments = normalized.split('.').collect::<Vec<_>>();
    if !segments.iter().all(|segment| is_numeric_like_ipv4_segment(segment)) {
        return false;
    }
    if segments.len() != 4 {
        return true;
    }
    segments.iter().any(|segment| {
        if segment.starts_with("0x") {
            return true;
        }
        if segment.len() > 1 && segment.starts_with('0') {
            return true;
        }
        segment.parse::<u8>().is_err()
    })
}

fn is_numeric_like_ipv4_segment(segment: &str) -> bool {
    if segment.is_empty() {
        return false;
    }
    if let Some(hex) = segment.strip_prefix("0x") {
        return !hex.is_empty() && hex.bytes().all(|value| value.is_ascii_hexdigit());
    }
    segment.bytes().all(|value| value.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::{
        is_localhost_hostname, is_private_or_local_ip, parse_host_ip_literal,
        validate_resolved_ip_addrs,
    };
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn parse_host_ip_literal_accepts_canonical_literals() {
        assert_eq!(
            parse_host_ip_literal("127.0.0.1").expect("canonical IPv4 literal should parse"),
            Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)))
        );
        assert_eq!(
            parse_host_ip_literal("::1").expect("canonical IPv6 literal should parse"),
            Some(IpAddr::V6(Ipv6Addr::LOCALHOST))
        );
        assert_eq!(
            parse_host_ip_literal("example.com").expect("hostname should pass parsing"),
            None
        );
    }

    #[test]
    fn parse_host_ip_literal_rejects_non_canonical_ipv4_forms() {
        for host in ["2130706433", "0x7f000001", "0177.0.0.1", "127.1", "127.000.0.1"] {
            let error = parse_host_ip_literal(host)
                .expect_err("non-canonical IPv4 literals must fail closed");
            assert!(
                error.contains("non-canonical IPv4 literal"),
                "error should explain guard behavior for {host}: {error}"
            );
        }
    }

    #[test]
    fn validate_resolved_ip_addrs_blocks_non_public_by_default() {
        let blocked = validate_resolved_ip_addrs(
            &[IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))],
            false,
        );
        assert!(blocked.is_err(), "mixed private/public answers must be denied by default");

        let allowed = validate_resolved_ip_addrs(
            &[IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))],
            true,
        );
        assert!(allowed.is_ok(), "explicit private-target opt-in should permit mixed answers");
    }

    #[test]
    fn private_or_local_classifier_covers_special_use_ranges() {
        for address in [
            IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(198, 18, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
            IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)),
            IpAddr::V6(Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 1)),
            IpAddr::V6(Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0, 1)),
            IpAddr::V6(Ipv6Addr::new(0x2002, 0, 0, 0, 0, 0, 0, 1)),
            IpAddr::V6(Ipv6Addr::new(0x2001, 0, 0, 0, 0, 0, 0, 1)),
        ] {
            assert!(
                is_private_or_local_ip(address),
                "{address} should be classified as private/local/special-use"
            );
        }
        assert!(
            !is_private_or_local_ip(IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))),
            "public IPv4 address should stay allowlisted"
        );
    }

    #[test]
    fn localhost_hostname_detection_is_stable() {
        assert!(is_localhost_hostname("localhost"));
        assert!(is_localhost_hostname("LOCALHOST."));
        assert!(is_localhost_hostname("dev.localhost"));
        assert!(!is_localhost_hostname("example.local"));
    }
}
