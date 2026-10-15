//! Building blocks shared by node types and node instances.

use std::borrow::Cow;
use std::fmt;

use serde::{Deserialize, Serialize};

/// The role a node plays in a graph. The kind decides which ports a node may
/// have:
///
/// | Kind        | Inputs | Outputs |
/// | ----------- | ------ | ------- |
/// | `Source`    | none   | 1+      |
/// | `Processor` | 1+     | 1+      |
/// | `Sink`      | 1+     | none    |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeKind {
    Source,
    Processor,
    Sink,
}

impl fmt::Display for NodeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            NodeKind::Source => "source",
            NodeKind::Processor => "processor",
            NodeKind::Sink => "sink",
        })
    }
}

/// The type of data a port emits or consumes, e.g. `Packet` or `Frame`.
///
/// This is an open set: node modules can define their own types with
/// [`DataType::new`]. Two ports are compatible when their data types are
/// equal.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DataType(Cow<'static, str>);

impl DataType {
    pub const PACKET: DataType = DataType(Cow::Borrowed("Packet"));
    pub const FRAME: DataType = DataType(Cow::Borrowed("Frame"));
    pub const TEXTURE: DataType = DataType(Cow::Borrowed("Texture"));
    pub const AUDIO: DataType = DataType(Cow::Borrowed("Audio"));
    pub const RECORD: DataType = DataType(Cow::Borrowed("Record"));

    pub fn new(name: impl Into<Cow<'static, str>>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A named input or output port and the data type it carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Port {
    pub name: String,
    pub data_type: DataType,
}

impl Port {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }
}
