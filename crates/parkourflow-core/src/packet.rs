//! The payload of `Packet` streams: one captured network packet.

use std::sync::Arc;
use std::time::SystemTime;

/// One packet as captured from a network interface or capture file.
///
/// The bytes are copied once out of the capture buffer; after that, stream
/// items share the packet by reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    /// When the packet was captured (wall clock).
    pub timestamp: SystemTime,
    /// The interface or file it was captured from, shared by every packet of
    /// the stream.
    pub source: Arc<str>,
    /// Link-layer header type, as a pcap `LINKTYPE_*` value (1 is Ethernet).
    pub link_type: i32,
    /// Length of the packet on the wire. Larger than `data.len()` when the
    /// capture's snapshot length cut it short.
    pub original_len: u32,
    /// The captured bytes, starting at the link-layer header.
    pub data: Box<[u8]>,
}

impl Packet {
    /// `LINKTYPE_ETHERNET`.
    pub const ETHERNET: i32 = 1;

    /// Whether the snapshot length cut the packet short.
    pub fn is_truncated(&self) -> bool {
        (self.data.len() as u64) < u64::from(self.original_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncation() {
        let packet = |captured: usize, original| Packet {
            timestamp: SystemTime::UNIX_EPOCH,
            source: "en0".into(),
            link_type: Packet::ETHERNET,
            original_len: original,
            data: vec![0; captured].into(),
        };
        assert!(!packet(60, 60).is_truncated());
        assert!(packet(64, 1500).is_truncated());
    }
}
