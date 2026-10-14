//! ParkourFlow core runtime.
//!
//! For now this only holds the node metadata types the UI uses to render
//! nodes. Graph scheduling, edge transport, and lifecycle management come
//! later.

use serde::Serialize;

/// The broad role a node plays in a graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeKind {
    Source,
    Processor,
    Sink,
}

/// A typed input or output port.
#[derive(Debug, Clone, Serialize)]
pub struct PortDescriptor {
    pub name: String,
    /// The stream type carried by this port, e.g. `VideoFrame` or `Packet`.
    pub stream_type: String,
}

/// Metadata describing a node type, sent to the UI so it can render the node
/// without node-specific frontend code.
#[derive(Debug, Clone, Serialize)]
pub struct NodeDescriptor {
    pub id: String,
    pub label: String,
    pub kind: NodeKind,
    pub category: String,
    pub inputs: Vec<PortDescriptor>,
    pub outputs: Vec<PortDescriptor>,
}

fn port(name: &str, stream_type: &str) -> PortDescriptor {
    PortDescriptor {
        name: name.into(),
        stream_type: stream_type.into(),
    }
}

/// The node types built into this build of the runtime.
pub fn builtin_nodes() -> Vec<NodeDescriptor> {
    vec![
        NodeDescriptor {
            id: "net.pcap_source".into(),
            label: "libpcap Source".into(),
            kind: NodeKind::Source,
            category: "Network".into(),
            inputs: vec![],
            outputs: vec![port("packets", "Packet")],
        },
        NodeDescriptor {
            id: "net.dns_parser".into(),
            label: "DNS Parser".into(),
            kind: NodeKind::Processor,
            category: "Network".into(),
            inputs: vec![port("packets", "Packet")],
            outputs: vec![port("messages", "DnsMessage")],
        },
        NodeDescriptor {
            id: "file.csv_writer".into(),
            label: "CSV Writer".into(),
            kind: NodeKind::Sink,
            category: "Files".into(),
            inputs: vec![port("records", "DnsMessage")],
            outputs: vec![],
        },
    ]
}

/// Runtime version, for display in the UI.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_nodes_serialize() {
        let json = serde_json::to_value(builtin_nodes()).unwrap();
        assert_eq!(json[0]["kind"], "source");
        assert_eq!(json[1]["inputs"][0]["stream_type"], "Packet");
    }
}
