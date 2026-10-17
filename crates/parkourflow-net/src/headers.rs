//! Just enough header parsing to summarize a packet: its protocol and its
//! source and destination addresses.
//!
//! Covers the link types interfaces commonly report (Ethernet with VLAN
//! tags, BSD loopback, raw IP, Linux cooked capture), IPv4 and IPv6 (with
//! extension headers), ARP, and TCP/UDP/SCTP ports. Anything else is
//! summarized as far as it was understood. Truncated packets never fail;
//! they just yield less.

use std::net::{IpAddr, Ipv4Addr};

/// What [`summarize`] found in a packet.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Summary {
    /// The innermost protocol understood, e.g. `TCP`, `ICMPv6`, `ARP`.
    pub protocol: String,
    /// Source address: `ip:port` for TCP/UDP/SCTP (`[ip]:port` for IPv6),
    /// the IP for other IP traffic, the MAC for other Ethernet traffic.
    /// Empty when unknown.
    pub source: String,
    pub destination: String,
}

impl Summary {
    fn new(protocol: impl Into<String>, source: String, destination: String) -> Self {
        Self {
            protocol: protocol.into(),
            source,
            destination,
        }
    }
}

const ETHERTYPE_IPV4: u16 = 0x0800;
const ETHERTYPE_ARP: u16 = 0x0806;
const ETHERTYPE_IPV6: u16 = 0x86dd;
const ETHERTYPE_VLAN: u16 = 0x8100;
const ETHERTYPE_QINQ: u16 = 0x88a8;

/// Summarizes a packet whose bytes start at a header of `link_type` (a pcap
/// `LINKTYPE_*` value).
pub fn summarize(link_type: i32, data: &[u8]) -> Summary {
    match link_type {
        // LINKTYPE_ETHERNET
        1 => ethernet(data),
        // LINKTYPE_NULL and LINKTYPE_LOOP (BSD loopback, macOS utun): a
        // 4-byte address family whose byte order varies, so go by the IP
        // version nibble instead.
        0 | 108 => ip(data.get(4..).unwrap_or_default()),
        // LINKTYPE_RAW (and its OpenBSD number), LINKTYPE_IPV4, LINKTYPE_IPV6
        101 | 12 | 228 | 229 => ip(data),
        // LINKTYPE_LINUX_SLL: protocol at bytes 14..16, payload at 16.
        113 => match be16(data, 14) {
            Some(ethertype) => by_ethertype(ethertype, data.get(16..).unwrap_or_default(), None),
            None => Summary::new("Linux cooked", String::new(), String::new()),
        },
        // LINKTYPE_LINUX_SLL2: protocol at bytes 0..2, payload at 20.
        276 => match be16(data, 0) {
            Some(ethertype) => by_ethertype(ethertype, data.get(20..).unwrap_or_default(), None),
            None => Summary::new("Linux cooked", String::new(), String::new()),
        },
        other => Summary::new(format!("LINKTYPE {other}"), String::new(), String::new()),
    }
}

fn be16(data: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_be_bytes(data.get(at..at + 2)?.try_into().ok()?))
}

fn mac(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(":")
}

fn ethernet(data: &[u8]) -> Summary {
    if data.len() < 14 {
        return Summary::new("Ethernet", String::new(), String::new());
    }
    let macs = (mac(&data[6..12]), mac(&data[0..6]));
    let mut ethertype = be16(data, 12).unwrap_or_default();
    let mut offset = 14;
    // Up to two VLAN tags (802.1Q, and 802.1ad outer tags).
    for _ in 0..2 {
        if ethertype != ETHERTYPE_VLAN && ethertype != ETHERTYPE_QINQ {
            break;
        }
        match be16(data, offset + 2) {
            Some(inner) => {
                ethertype = inner;
                offset += 4;
            }
            None => return Summary::new("VLAN", macs.0, macs.1),
        }
    }
    by_ethertype(
        ethertype,
        data.get(offset..).unwrap_or_default(),
        Some(macs),
    )
}

/// `macs` are the Ethernet source and destination, used when the payload
/// isn't IP.
fn by_ethertype(ethertype: u16, payload: &[u8], macs: Option<(String, String)>) -> Summary {
    let (src, dst) = macs.unwrap_or_default();
    match ethertype {
        ETHERTYPE_IPV4 | ETHERTYPE_IPV6 => {
            let summary = ip(payload);
            // A cut-short IP header still tells us the hosts' MACs.
            if summary.source.is_empty() && !src.is_empty() {
                Summary::new(summary.protocol, src, dst)
            } else {
                summary
            }
        }
        ETHERTYPE_ARP => arp(payload, src, dst),
        other => Summary::new(format!("Ethernet 0x{other:04x}"), src, dst),
    }
}

fn arp(data: &[u8], src: String, dst: String) -> Summary {
    // Ethernet/IPv4 ARP: sender IP at 14..18, target IP at 24..28.
    let is_ipv4 = be16(data, 2) == Some(ETHERTYPE_IPV4) && data.get(5) == Some(&4);
    match (is_ipv4, data.get(14..18), data.get(24..28)) {
        (true, Some(spa), Some(tpa)) => Summary::new(
            "ARP",
            Ipv4Addr::from(<[u8; 4]>::try_from(spa).unwrap_or_default()).to_string(),
            Ipv4Addr::from(<[u8; 4]>::try_from(tpa).unwrap_or_default()).to_string(),
        ),
        _ => Summary::new("ARP", src, dst),
    }
}

/// An IP packet, by its version nibble.
fn ip(data: &[u8]) -> Summary {
    match data.first().map(|b| b >> 4) {
        Some(4) => ipv4(data),
        Some(6) => ipv6(data),
        _ => Summary::new("Unknown", String::new(), String::new()),
    }
}

fn ipv4(data: &[u8]) -> Summary {
    let header_len = usize::from(data[0] & 0x0f) * 4;
    if data.len() < 20 || header_len < 20 {
        return Summary::new("IPv4", String::new(), String::new());
    }
    let src = IpAddr::from(<[u8; 4]>::try_from(&data[12..16]).unwrap_or_default());
    let dst = IpAddr::from(<[u8; 4]>::try_from(&data[16..20]).unwrap_or_default());
    // Only the first fragment carries the transport header.
    let fragment_offset = be16(data, 6).unwrap_or_default() & 0x1fff;
    let payload = (fragment_offset == 0)
        .then(|| data.get(header_len..))
        .flatten();
    transport(data[9], payload, src, dst)
}

fn ipv6(data: &[u8]) -> Summary {
    if data.len() < 40 {
        return Summary::new("IPv6", String::new(), String::new());
    }
    let src = IpAddr::from(<[u8; 16]>::try_from(&data[8..24]).unwrap_or_default());
    let dst = IpAddr::from(<[u8; 16]>::try_from(&data[24..40]).unwrap_or_default());
    let mut next = data[6];
    let mut offset = 40;
    // Skip extension headers to reach the transport protocol.
    for _ in 0..8 {
        let header_len = match next {
            // Hop-by-hop, routing, destination options.
            0 | 43 | 60 => data.get(offset + 1).map(|&n| (usize::from(n) + 1) * 8),
            // Fragment: later fragments have no transport header.
            44 => {
                let later = be16(data, offset + 2).is_some_and(|f| f & 0xfff8 != 0);
                if later {
                    return transport(data.get(offset).copied().unwrap_or(59), None, src, dst);
                }
                Some(8)
            }
            // Authentication header.
            51 => data.get(offset + 1).map(|&n| (usize::from(n) + 2) * 4),
            _ => break,
        };
        match (header_len, data.get(offset)) {
            (Some(len), Some(&following)) => {
                next = following;
                offset += len;
            }
            _ => return Summary::new("IPv6", src.to_string(), dst.to_string()),
        }
    }
    transport(next, data.get(offset..), src, dst)
}

/// The transport layer of an IP packet. `payload` is `None` when it isn't
/// available (a later fragment).
fn transport(protocol: u8, payload: Option<&[u8]>, src: IpAddr, dst: IpAddr) -> Summary {
    let name = match protocol {
        1 => "ICMP",
        2 => "IGMP",
        6 => "TCP",
        17 => "UDP",
        47 => "GRE",
        50 => "ESP",
        58 => "ICMPv6",
        132 => "SCTP",
        _ => {
            let version = if src.is_ipv4() { "IPv4" } else { "IPv6" };
            return Summary::new(
                format!("{version} proto {protocol}"),
                src.to_string(),
                dst.to_string(),
            );
        }
    };
    let ports = match (protocol, payload) {
        (6 | 17 | 132, Some(p)) => be16(p, 0).zip(be16(p, 2)),
        _ => None,
    };
    match ports {
        Some((sport, dport)) => Summary::new(name, endpoint(src, sport), endpoint(dst, dport)),
        None => Summary::new(name, src.to_string(), dst.to_string()),
    }
}

fn endpoint(ip: IpAddr, port: u16) -> String {
    match ip {
        IpAddr::V4(ip) => format!("{ip}:{port}"),
        IpAddr::V6(ip) => format!("[{ip}]:{port}"),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::net::Ipv6Addr;

    use super::*;

    pub const MAC_A: [u8; 6] = [0x02, 0, 0, 0, 0, 0x0a];
    pub const MAC_B: [u8; 6] = [0x02, 0, 0, 0, 0, 0x0b];

    pub fn ethernet_frame(ethertype: u16, payload: &[u8]) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&MAC_B); // destination
        frame.extend_from_slice(&MAC_A); // source
        frame.extend_from_slice(&ethertype.to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    pub fn ipv4_packet(protocol: u8, src: [u8; 4], dst: [u8; 4], payload: &[u8]) -> Vec<u8> {
        let mut p = vec![0x45, 0, 0, 0, 0, 0, 0, 0, 64, protocol, 0, 0];
        p.extend_from_slice(&src);
        p.extend_from_slice(&dst);
        p.extend_from_slice(payload);
        p
    }

    pub fn ipv6_packet(next: u8, src: Ipv6Addr, dst: Ipv6Addr, payload: &[u8]) -> Vec<u8> {
        let mut p = vec![0x60, 0, 0, 0, 0, 0, next, 64];
        p.extend_from_slice(&src.octets());
        p.extend_from_slice(&dst.octets());
        p.extend_from_slice(payload);
        p
    }

    pub fn ports(src: u16, dst: u16) -> Vec<u8> {
        let mut p = src.to_be_bytes().to_vec();
        p.extend_from_slice(&dst.to_be_bytes());
        p.extend_from_slice(&[0; 4]);
        p
    }

    fn summary(protocol: &str, source: &str, destination: &str) -> Summary {
        Summary::new(protocol, source.into(), destination.into())
    }

    #[test]
    fn ethernet_ipv4_udp() {
        let frame = ethernet_frame(
            ETHERTYPE_IPV4,
            &ipv4_packet(17, [192, 168, 1, 5], [8, 8, 8, 8], &ports(53000, 53)),
        );
        assert_eq!(
            summarize(1, &frame),
            summary("UDP", "192.168.1.5:53000", "8.8.8.8:53")
        );
    }

    #[test]
    fn ethernet_ipv6_tcp_through_an_extension_header() {
        let src: Ipv6Addr = "2001:db8::1".parse().unwrap();
        let dst: Ipv6Addr = "2001:db8::2".parse().unwrap();
        // Hop-by-hop options (8 bytes) whose next header is TCP.
        let mut payload = vec![6, 0, 0, 0, 0, 0, 0, 0];
        payload.extend_from_slice(&ports(443, 51000));
        let frame = ethernet_frame(ETHERTYPE_IPV6, &ipv6_packet(0, src, dst, &payload));
        assert_eq!(
            summarize(1, &frame),
            summary("TCP", "[2001:db8::1]:443", "[2001:db8::2]:51000")
        );
    }

    #[test]
    fn vlan_tagged_icmp() {
        let ip = ipv4_packet(1, [10, 0, 0, 1], [10, 0, 0, 2], &[8, 0, 0, 0]);
        let mut tagged = vec![0x00, 0x64];
        tagged.extend_from_slice(&ETHERTYPE_IPV4.to_be_bytes());
        tagged.extend_from_slice(&ip);
        let frame = ethernet_frame(ETHERTYPE_VLAN, &tagged);
        assert_eq!(
            summarize(1, &frame),
            summary("ICMP", "10.0.0.1", "10.0.0.2")
        );
    }

    #[test]
    fn arp_uses_protocol_addresses() {
        let mut arp = vec![0, 1, 0x08, 0x00, 6, 4, 0, 1];
        arp.extend_from_slice(&MAC_A);
        arp.extend_from_slice(&[192, 168, 1, 5]);
        arp.extend_from_slice(&[0; 6]);
        arp.extend_from_slice(&[192, 168, 1, 1]);
        let frame = ethernet_frame(ETHERTYPE_ARP, &arp);
        assert_eq!(
            summarize(1, &frame),
            summary("ARP", "192.168.1.5", "192.168.1.1")
        );
    }

    #[test]
    fn other_ethertypes_use_macs() {
        let frame = ethernet_frame(0x88cc, &[0; 10]);
        assert_eq!(
            summarize(1, &frame),
            summary("Ethernet 0x88cc", "02:00:00:00:00:0a", "02:00:00:00:00:0b")
        );
    }

    #[test]
    fn loopback_and_raw_ip() {
        let ip = ipv4_packet(6, [127, 0, 0, 1], [127, 0, 0, 1], &ports(5000, 80));
        let expected = summary("TCP", "127.0.0.1:5000", "127.0.0.1:80");
        // macOS lo0: address family 2 in host (little-endian) order.
        let mut null = vec![2, 0, 0, 0];
        null.extend_from_slice(&ip);
        assert_eq!(summarize(0, &null), expected);
        assert_eq!(summarize(101, &ip), expected);
    }

    #[test]
    fn later_fragments_have_no_ports() {
        let mut ip = ipv4_packet(17, [10, 0, 0, 1], [10, 0, 0, 2], &[0xff; 8]);
        ip[6] = 0x00;
        ip[7] = 0x10; // fragment offset 16
        assert_eq!(summarize(101, &ip), summary("UDP", "10.0.0.1", "10.0.0.2"));
    }

    #[test]
    fn truncated_packets_yield_less() {
        let full = ethernet_frame(
            ETHERTYPE_IPV4,
            &ipv4_packet(6, [10, 0, 0, 1], [10, 0, 0, 2], &ports(1, 2)),
        );
        // IP header cut short: fall back to the Ethernet addresses.
        assert_eq!(
            summarize(1, &full[..20]),
            summary("IPv4", "02:00:00:00:00:0a", "02:00:00:00:00:0b")
        );
        // Ports cut off.
        assert_eq!(
            summarize(1, &full[..35]),
            summary("TCP", "10.0.0.1", "10.0.0.2")
        );
        assert_eq!(summarize(1, &full[..4]), summary("Ethernet", "", ""));
        assert_eq!(summarize(1, &[]), summary("Ethernet", "", ""));
        assert_eq!(summarize(147, &full), summary("LINKTYPE 147", "", ""));
    }
}
