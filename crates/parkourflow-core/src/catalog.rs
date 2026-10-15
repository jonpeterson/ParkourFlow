//! Node types available in this build.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::error::WorkflowError;
use crate::node::{DataType, NodeKind, Port};
use crate::property::{FileMode, Property, PropertyType};

/// Metadata describing a node type. The UI renders nodes from this without
/// node-specific frontend code, and [`Workflow::add_node`] creates node
/// instances from it.
///
/// [`Workflow::add_node`]: crate::Workflow::add_node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeDescriptor {
    /// Stable identifier for the node type, e.g. `net.pcap_source`.
    pub type_id: String,
    pub label: String,
    pub kind: NodeKind,
    pub category: String,
    pub inputs: Vec<Port>,
    pub outputs: Vec<Port>,
    /// Configuration fields, shown in the UI's inspector.
    #[serde(default)]
    pub properties: Vec<Property>,
}

impl NodeDescriptor {
    /// Checks that the ports fit the node kind, that port and property names
    /// are unique, and that property defaults are valid.
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
        let mut seen = HashSet::new();
        for property in &self.properties {
            if !seen.insert(property.name.as_str()) {
                return Err(WorkflowError::DuplicateProperty(property.name.clone()));
            }
            if let PropertyType::Enum { options } = &property.ty {
                if options.is_empty() {
                    return Err(WorkflowError::InvalidPropertyValue {
                        property: property.name.clone(),
                        reason: "has no options".into(),
                    });
                }
            }
            if let Some(default) = &property.default {
                property.validate(default.clone())?;
            }
        }
        Ok(())
    }
}

fn delimiter() -> Property {
    Property::new(
        "delimiter",
        "Delimiter",
        PropertyType::Enum {
            options: vec![",".into(), ";".into(), "tab".into(), "|".into()],
        },
    )
    .default(",")
}

/// The node types built into this build of the runtime.
///
/// The first three (pcap source, DNS parser, CSV writer) form the demo DNS
/// pipeline used in tests.
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
            properties: vec![
                Property::new(
                    "interface",
                    "Interface",
                    PropertyType::Device {
                        class: "network_interface".into(),
                    },
                )
                .required(),
                Property::new(
                    "filter",
                    "Capture filter",
                    PropertyType::String { multiline: false },
                )
                .description("BPF expression, such as \"udp port 53\". Empty captures everything.")
                .default(""),
                Property::new("promiscuous", "Promiscuous mode", PropertyType::Bool).default(true),
                Property::new(
                    "snaplen",
                    "Snapshot length",
                    PropertyType::Integer {
                        min: Some(64),
                        max: Some(262_144),
                    },
                )
                .description("Maximum bytes captured per packet.")
                .default(65_535),
            ],
        },
        NodeDescriptor {
            type_id: "net.dns_parser".into(),
            label: "DNS Parser".into(),
            kind: NodeKind::Processor,
            category: "Network".into(),
            inputs: vec![Port::new("packets", DataType::PACKET)],
            outputs: vec![Port::new("messages", dns_message.clone())],
            properties: vec![Property::new(
                "include_responses",
                "Include responses",
                PropertyType::Bool,
            )
            .default(true)
            .live()],
        },
        NodeDescriptor {
            type_id: "file.csv_writer".into(),
            label: "CSV Writer".into(),
            kind: NodeKind::Sink,
            category: "Files".into(),
            inputs: vec![Port::new("records", dns_message)],
            outputs: vec![],
            properties: vec![
                Property::new(
                    "path",
                    "Output file",
                    PropertyType::File {
                        mode: FileMode::Save,
                        extensions: vec!["csv".into()],
                    },
                )
                .required(),
                delimiter(),
                Property::new("write_header", "Write header row", PropertyType::Bool).default(true),
            ],
        },
        NodeDescriptor {
            type_id: "file.csv_source".into(),
            label: "CSV Source".into(),
            kind: NodeKind::Source,
            category: "Files".into(),
            inputs: vec![],
            outputs: vec![Port::new("records", DataType::RECORD)],
            properties: vec![
                Property::new(
                    "path",
                    "Input file",
                    PropertyType::File {
                        mode: FileMode::Open,
                        extensions: vec!["csv".into(), "tsv".into()],
                    },
                )
                .required(),
                delimiter(),
                Property::new("has_header", "Header row", PropertyType::Bool)
                    .description("Treat the first row as column names.")
                    .default(true),
            ],
        },
        NodeDescriptor {
            type_id: "net.pcap_file_source".into(),
            label: "PCAP File Source".into(),
            kind: NodeKind::Source,
            category: "Network".into(),
            inputs: vec![],
            outputs: vec![Port::new("packets", DataType::PACKET)],
            properties: vec![
                Property::new(
                    "path",
                    "Capture file",
                    PropertyType::File {
                        mode: FileMode::Open,
                        extensions: vec!["pcap".into(), "pcapng".into()],
                    },
                )
                .required(),
                Property::new("realtime", "Real-time playback", PropertyType::Bool)
                    .description(
                        "Replay at the original capture rate instead of as fast as possible.",
                    )
                    .default(false),
                Property::new(
                    "speed",
                    "Playback speed",
                    PropertyType::Float {
                        min: Some(0.1),
                        max: Some(10.0),
                        step: Some(0.1),
                    },
                )
                .description("Multiplier for real-time playback.")
                .default(1.0)
                .live(),
            ],
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
            properties: vec![],
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

    #[test]
    fn property_names_must_be_unique() {
        let mut d = descriptor(
            NodeKind::Source,
            vec![],
            vec![Port::new("p", DataType::PACKET)],
        );
        d.properties = vec![
            Property::new("x", "X", PropertyType::Bool),
            Property::new("x", "X", PropertyType::Bool),
        ];
        assert_eq!(
            d.validate(),
            Err(WorkflowError::DuplicateProperty("x".into()))
        );
    }

    #[test]
    fn property_defaults_must_be_valid() {
        let mut d = descriptor(
            NodeKind::Source,
            vec![],
            vec![Port::new("p", DataType::PACKET)],
        );
        d.properties = vec![Property::new(
            "n",
            "N",
            PropertyType::Integer {
                min: Some(1),
                max: None,
            },
        )
        .default(0)];
        assert!(matches!(
            d.validate(),
            Err(WorkflowError::InvalidPropertyValue { .. })
        ));
    }
}
