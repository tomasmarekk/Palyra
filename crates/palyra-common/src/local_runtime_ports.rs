//! Loopback port selection for local runtime services (gateway and browserd).
//!
//! Tries the well-known default ports first so URLs stay stable across restarts, then
//! falls back to the first free contiguous block in the reserved 7142-7241 range. Only
//! loopback hosts are supported — auto-selection must never bind a routable interface.

use std::net::{IpAddr, TcpListener};

/// Loopback host used for all local runtime port probing and defaults.
pub const LOCAL_RUNTIME_LOOPBACK_HOST: &str = "127.0.0.1";
/// First port of the reserved fallback range for auto-selected local runtime ports.
pub const LOCAL_RUNTIME_PORT_RANGE_START: u16 = 7142;
/// Last port (inclusive) of the reserved fallback range.
pub const LOCAL_RUNTIME_PORT_RANGE_END: u16 = 7241;

/// Default gateway admin/console HTTP port.
pub const DEFAULT_GATEWAY_ADMIN_PORT: u16 = 7142;
/// Default browserd health HTTP port.
pub const DEFAULT_BROWSER_HEALTH_PORT: u16 = 7143;
/// Default gateway gRPC port.
pub const DEFAULT_GATEWAY_GRPC_PORT: u16 = 7443;
/// Default gateway QUIC port.
pub const DEFAULT_GATEWAY_QUIC_PORT: u16 = 7444;
/// Default browserd gRPC port.
pub const DEFAULT_BROWSER_GRPC_PORT: u16 = 7543;

/// Full set of loopback ports for one local runtime (gateway plus browserd).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalRuntimePorts {
    pub gateway_admin: u16,
    pub gateway_grpc: u16,
    pub gateway_quic: u16,
    pub browser_health: u16,
    pub browser_grpc: u16,
}

/// Loopback ports for a standalone gateway runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewayRuntimePorts {
    pub admin: u16,
    pub grpc: u16,
    pub quic: u16,
}

/// Loopback ports for a standalone browserd runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrowserRuntimePorts {
    pub health: u16,
    pub grpc: u16,
}

/// Bind-probe outcome for a single port, with the OS error when unavailable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortAvailability {
    pub port: u16,
    pub available: bool,
    pub error: Option<String>,
}

/// Returns the well-known default port set for a full local runtime.
#[must_use]
pub const fn default_local_runtime_ports() -> LocalRuntimePorts {
    LocalRuntimePorts {
        gateway_admin: DEFAULT_GATEWAY_ADMIN_PORT,
        gateway_grpc: DEFAULT_GATEWAY_GRPC_PORT,
        gateway_quic: DEFAULT_GATEWAY_QUIC_PORT,
        browser_health: DEFAULT_BROWSER_HEALTH_PORT,
        browser_grpc: DEFAULT_BROWSER_GRPC_PORT,
    }
}

/// Returns the well-known default gateway port set.
#[must_use]
pub const fn default_gateway_runtime_ports() -> GatewayRuntimePorts {
    GatewayRuntimePorts {
        admin: DEFAULT_GATEWAY_ADMIN_PORT,
        grpc: DEFAULT_GATEWAY_GRPC_PORT,
        quic: DEFAULT_GATEWAY_QUIC_PORT,
    }
}

/// Returns the well-known default browserd port set.
#[must_use]
pub const fn default_browser_runtime_ports() -> BrowserRuntimePorts {
    BrowserRuntimePorts { health: DEFAULT_BROWSER_HEALTH_PORT, grpc: DEFAULT_BROWSER_GRPC_PORT }
}

/// Selects ports for a full local runtime: defaults if all are free, otherwise the first
/// free 5-port block in the reserved range.
///
/// Probing is best-effort: listeners are dropped before returning, so a small race window
/// remains until the runtime actually binds.
///
/// # Errors
/// Returns an error message if `host` is not loopback or no free block exists.
pub fn select_available_local_runtime_ports(host: &str) -> Result<LocalRuntimePorts, String> {
    ensure_loopback_host(host)?;
    let defaults = default_local_runtime_ports();
    let default_ports = [
        defaults.gateway_admin,
        defaults.gateway_grpc,
        defaults.gateway_quic,
        defaults.browser_health,
        defaults.browser_grpc,
    ];
    if reserve_ports(host, &default_ports).is_ok() {
        return Ok(defaults);
    }

    let block = select_available_port_block(
        host,
        LOCAL_RUNTIME_PORT_RANGE_START,
        LOCAL_RUNTIME_PORT_RANGE_END,
        5,
        &[],
    )
    .ok_or_else(|| local_runtime_ports_exhausted_message(host, 5))?;
    Ok(LocalRuntimePorts {
        gateway_admin: block[0],
        gateway_grpc: block[1],
        gateway_quic: block[2],
        browser_health: block[3],
        browser_grpc: block[4],
    })
}

/// Gateway-only variant of [`select_available_local_runtime_ports`] (3-port block).
///
/// # Errors
/// Returns an error message if `host` is not loopback or no free block exists.
pub fn select_available_gateway_runtime_ports(host: &str) -> Result<GatewayRuntimePorts, String> {
    select_available_gateway_runtime_ports_excluding(host, &[])
}

/// Gateway-only port selection that never returns a port reserved by another
/// service in the same local runtime profile.
///
/// # Errors
/// Returns an error message if `host` is not loopback or no free block exists.
pub fn select_available_gateway_runtime_ports_excluding(
    host: &str,
    excluded_ports: &[u16],
) -> Result<GatewayRuntimePorts, String> {
    ensure_loopback_host(host)?;
    let defaults = default_gateway_runtime_ports();
    let default_ports = [defaults.admin, defaults.grpc, defaults.quic];
    if !ports_overlap(&default_ports, excluded_ports) && reserve_ports(host, &default_ports).is_ok()
    {
        return Ok(defaults);
    }

    let block = select_available_port_block(
        host,
        LOCAL_RUNTIME_PORT_RANGE_START,
        LOCAL_RUNTIME_PORT_RANGE_END,
        3,
        excluded_ports,
    )
    .ok_or_else(|| local_runtime_ports_exhausted_message(host, 3))?;
    Ok(GatewayRuntimePorts { admin: block[0], grpc: block[1], quic: block[2] })
}

/// Browserd-only variant of [`select_available_local_runtime_ports`] (2-port block).
///
/// # Errors
/// Returns an error message if `host` is not loopback or no free block exists.
pub fn select_available_browser_runtime_ports(host: &str) -> Result<BrowserRuntimePorts, String> {
    select_available_browser_runtime_ports_excluding(host, &[])
}

/// Browserd-only port selection that never returns a port reserved by another
/// service in the same local runtime profile.
///
/// # Errors
/// Returns an error message if `host` is not loopback or no free block exists.
pub fn select_available_browser_runtime_ports_excluding(
    host: &str,
    excluded_ports: &[u16],
) -> Result<BrowserRuntimePorts, String> {
    ensure_loopback_host(host)?;
    let defaults = default_browser_runtime_ports();
    let default_ports = [defaults.health, defaults.grpc];
    if !ports_overlap(&default_ports, excluded_ports) && reserve_ports(host, &default_ports).is_ok()
    {
        return Ok(defaults);
    }

    let block = select_available_port_block(
        host,
        LOCAL_RUNTIME_PORT_RANGE_START,
        LOCAL_RUNTIME_PORT_RANGE_END,
        2,
        excluded_ports,
    )
    .ok_or_else(|| local_runtime_ports_exhausted_message(host, 2))?;
    Ok(BrowserRuntimePorts { health: block[0], grpc: block[1] })
}

/// Probes a single port by binding a throwaway TCP listener.
#[must_use]
pub fn port_availability(host: &str, port: u16) -> PortAvailability {
    // Port 0 is the "OS-assigned/disabled" sentinel in runtime config; nothing to probe.
    if port == 0 {
        return PortAvailability { port, available: true, error: None };
    }
    match TcpListener::bind((host, port)) {
        Ok(_listener) => PortAvailability { port, available: true, error: None },
        Err(error) => PortAvailability { port, available: false, error: Some(error.to_string()) },
    }
}

/// Probes the given ports and returns entries only for those that failed to bind.
#[must_use]
pub fn unavailable_ports(host: &str, ports: &[u16]) -> Vec<PortAvailability> {
    ports
        .iter()
        .copied()
        .map(|port| port_availability(host, port))
        .filter(|availability| !availability.available)
        .collect()
}

/// Returns whether `host` is `localhost` or a loopback IP literal.
#[must_use]
pub fn is_loopback_host(host: &str) -> bool {
    let trimmed = host.trim();
    trimmed.eq_ignore_ascii_case("localhost")
        || trimmed.parse::<IpAddr>().is_ok_and(|address| address.is_loopback())
}

fn ensure_loopback_host(host: &str) -> Result<(), String> {
    if is_loopback_host(host) {
        Ok(())
    } else {
        Err(format!(
            "local runtime port auto-selection only supports loopback hosts, got `{}`",
            host.trim()
        ))
    }
}

// Scans for the first run of `width` consecutive free ports. Contiguous blocks keep one
// runtime's ports adjacent, so concurrent runtimes claim disjoint ranges instead of
// interleaving.
fn select_available_port_block(
    host: &str,
    range_start: u16,
    range_end: u16,
    width: u16,
    excluded_ports: &[u16],
) -> Option<Vec<u16>> {
    if width == 0 || range_end < range_start {
        return None;
    }
    let last_start = range_end.checked_sub(width.saturating_sub(1))?;
    if last_start < range_start {
        return None;
    }
    for block_start in range_start..=last_start {
        let ports = (0..width).map(|offset| block_start + offset).collect::<Vec<_>>();
        if ports_overlap(ports.as_slice(), excluded_ports) {
            continue;
        }
        if reserve_ports(host, ports.as_slice()).is_ok() {
            return Some(ports);
        }
    }
    None
}

fn ports_overlap(candidate_ports: &[u16], excluded_ports: &[u16]) -> bool {
    candidate_ports.iter().any(|candidate| *candidate != 0 && excluded_ports.contains(candidate))
}

// Binds all ports simultaneously so a block is only reported free when every port in it
// could be held at once; the returned listeners release the ports when dropped.
fn reserve_ports(host: &str, ports: &[u16]) -> std::io::Result<Vec<TcpListener>> {
    let mut listeners = Vec::with_capacity(ports.len());
    for port in ports {
        if *port == 0 {
            continue;
        }
        listeners.push(TcpListener::bind((host, *port))?);
    }
    Ok(listeners)
}

fn local_runtime_ports_exhausted_message(host: &str, width: u16) -> String {
    format!(
        "no free loopback port block of width {width} was found for {host} in range {LOCAL_RUNTIME_PORT_RANGE_START}-{LOCAL_RUNTIME_PORT_RANGE_END}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_detection_accepts_local_hosts() {
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("::1"));
        assert!(is_loopback_host("localhost"));
        assert!(!is_loopback_host("192.168.1.10"));
    }

    #[test]
    fn unavailable_ports_reports_reserved_listener() {
        let listener = TcpListener::bind((LOCAL_RUNTIME_LOOPBACK_HOST, 0))
            .expect("test should reserve a loopback port");
        let port = listener.local_addr().expect("listener address").port();

        let unavailable = unavailable_ports(LOCAL_RUNTIME_LOOPBACK_HOST, &[port]);

        assert_eq!(unavailable.len(), 1);
        assert_eq!(unavailable[0].port, port);
        assert!(!unavailable[0].available);
        assert!(unavailable[0].error.is_some());
    }

    #[test]
    fn port_block_selection_skips_reserved_listener() {
        let listener = TcpListener::bind((LOCAL_RUNTIME_LOOPBACK_HOST, 0))
            .expect("test should reserve a loopback port");
        let port = listener.local_addr().expect("listener address").port();

        let selected = select_available_port_block(LOCAL_RUNTIME_LOOPBACK_HOST, port, port, 1, &[]);

        assert!(selected.is_none(), "reserved port must not be selected");
    }

    #[test]
    fn profile_reserved_port_overlap_is_detected_without_binding() {
        assert!(ports_overlap(&[7144, 7145], &[7142, 7144, 7145]));
        assert!(!ports_overlap(&[7147, 7148], &[7142, 7144, 7145]));
    }

    #[test]
    fn browser_selection_never_reuses_gateway_profile_ports() {
        let defaults = default_browser_runtime_ports();
        let gateway_reserved = [
            defaults.health,
            defaults.grpc,
            LOCAL_RUNTIME_PORT_RANGE_START,
            LOCAL_RUNTIME_PORT_RANGE_START + 1,
        ];

        let selected = select_available_browser_runtime_ports_excluding(
            LOCAL_RUNTIME_LOOPBACK_HOST,
            &gateway_reserved,
        )
        .expect("a browser pair outside the reserved gateway set should exist");

        assert!(!gateway_reserved.contains(&selected.health));
        assert!(!gateway_reserved.contains(&selected.grpc));
    }
}
