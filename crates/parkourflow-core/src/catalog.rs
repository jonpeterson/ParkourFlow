//! Node types available in this build.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::error::WorkflowError;
use crate::node::{DataType, NodeKind, Port};

/// Metadata describing a node type. The UI renders nodes from this without
/// node-specific frontend code, and [`Workflow::add_node`] creates node
/// instances from it.
///
/// [`Workflow::add_node`]: crate::Workflow::add_node
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDescriptor {
    /// Stable identifier for the node type, e.g. `net.pcap_source`.
    pub type_id: String,
    pub label: String,
    pub kind: NodeKind,
    pub category: String,
    pub inputs: Vec<Port>,
    pub outputs: Vec<Port>,
}

impl NodeDescriptor {
    /// Checks that the ports fit the node kind and that port names are unique
    /// on each side.
    pub fn validate(&self) -> Result<(), WorkflowError> {
        let invalid = |reason| WorkflowError::InvalidPorts {
            kind: self.kind,
            reason,
        };
        match self.kind {
            NodeKind::Source if !self.inputs.is_empty() => {
                return Err(invalid("cannot have input ports"))
            }
            NodeKind::Sink if !self.outputs.is_empty() => {
                return Err(invalid("cannot have output ports"))
            }
            _ => {}
        }
        if self.kind != NodeKind::Sink && self.outputs.is_empty() {
            return Err(invalid("need at least one output port"));
        }
        if self.kind != NodeKind::Source && self.inputs.is_empty() {
            return Err(invalid("need at least one input port"));
        }
        for ports in [&self.inputs, &self.outputs] {
            let mut seen = HashSet::new();
            for port in ports {
                if !seen.insert(port.name.as_str()) {
                    return Err(WorkflowError::DuplicatePort(port.name.clone()));
                }
            }
        }
        Ok(())
    }
}

/// The node types built into this build of the runtime.
pub fn builtin_nodes() -> Vec<NodeDescriptor> {
    let dns_message = DataType::new("DnsMessage");
    vec![
        NodeDescriptor {
            type_id: "net.pcap_source".into(),
            label: "libpcap Source".into(),
            kind: NodeKind::Source,
            category: "Network".into(),
            inputs: vec![],
            outputs: vec![Port::new("packets", DataType::PACKET)],
        },
        NodeDescriptor {
            type_id: "net.dns_parser".into(),
            label: "DNS Parser".into(),
            kind: NodeKind::Processor,
            category: "Network".into(),
            inputs: vec![Port::new("packets", DataType::PACKET)],
            outputs: vec![Port::new("messages", dns_message.clone())],
        },
        NodeDescriptor {
            type_id: "file.csv_writer".into(),
            label: "CSV Writer".into(),
            kind: NodeKind::Sink,
            category: "Files".into(),
            inputs: vec![Port::new("records", dns_message)],
            outputs: vec![],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(kind: NodeKind, inputs: Vec<Port>, outputs: Vec<Port>) -> NodeDescriptor {
        NodeDescriptor {
            type_id: "test".into(),
            label: "Test".into(),
            kind,
            category: "Test".into(),
            inputs,
            outputs,
        }
    }

    #[test]
    fn builtin_nodes_are_valid() {
        for node in builtin_nodes() {
            node.validate().unwrap();
        }
    }

    #[test]
    fn builtin_nodes_serialize() {
        let json = serde_json::to_value(builtin_nodes()).unwrap();
        assert_eq!(json[0]["kind"], "source");
        assert_eq!(json[1]["inputs"][0]["data_type"], "Packet");
    }

    #[test]
    fn ports_must_match_kind() {
        let packet = || vec![Port::new("p", DataType::PACKET)];
        assert!(descriptor(NodeKind::Source, packet(), packet())
            .validate()
            .is_err());
        assert!(descriptor(NodeKind::Source, vec![], vec![])
            .validate()
            .is_err());
        assert!(descriptor(NodeKind::Sink, packet(), packet())
            .validate()
            .is_err());
        assert!(descriptor(NodeKind::Sink, vec![], vec![])
            .validate()
            .is_err());
        assert!(descriptor(NodeKind::Processor, vec![], packet())
            .validate()
            .is_err());
        assert!(descriptor(NodeKind::Processor, packet(), vec![])
            .validate()
            .is_err());
        assert!(descriptor(NodeKind::Processor, packet(), packet())
            .validate()
            .is_ok());
    }

    #[test]
    fn port_names_must_be_unique() {
        let ports = vec![
            Port::new("p", DataType::PACKET),
            Port::new("p", DataType::FRAME),
        ];
        assert_eq!(
            descriptor(NodeKind::Source, vec![], ports).validate(),
            Err(WorkflowError::DuplicatePort("p".into()))
        );
    }
}
