//! Daemon bind-address parsing shared by daemon startup and CLI validation.

/// Parses a daemon bind address plus port into a socket address.
///
/// Tries `bind_addr` as a bare IP first so IPv6 literals work without brackets
/// (e.g. `::1`), then falls back to parsing the joined `host:port` form.
///
/// # Errors
/// Returns the underlying `AddrParseError` when `bind_addr` is neither a bare IP nor
/// valid as part of a `host:port` socket address.
pub fn parse_daemon_bind_socket(
    bind_addr: &str,
    port: u16,
) -> Result<std::net::SocketAddr, std::net::AddrParseError> {
    if let Ok(ip) = bind_addr.parse::<std::net::IpAddr>() {
        return Ok(std::net::SocketAddr::new(ip, port));
    }
    format!("{bind_addr}:{port}").parse()
}
