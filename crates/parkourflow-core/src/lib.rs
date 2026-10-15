//! ParkourFlow core runtime.
//!
//! A workflow has two halves that are kept apart on purpose:
//!
//! - [`workflow`] — the *configuration*: groups, nodes, ports, and
//!   connections. This is what the editor changes and what will eventually be
//!   saved and loaded.
//! - [`state`] — the *runtime state*: node status and connection edges.
//!   It lives only in memory and is rebuilt from the configuration.
//!
//! [`transport`] is the data plane between nodes: a bounded, credit-based
//! channel per connection that applies the connection's overflow policy.
//!
//! [`catalog`] lists the node types this build provides.

pub mod buffer;
pub mod catalog;
pub mod error;
pub mod node;
pub mod state;
pub mod transport;
pub mod workflow;

pub use buffer::{ConnectionBuffer, Offer, Payload, StreamItem};
pub use catalog::{builtin_nodes, NodeDescriptor};
pub use error::WorkflowError;
pub use node::{DataType, NodeKind, Port};
pub use state::{NodeState, NodeStatus, WorkflowState};
pub use transport::{
    Edge, EdgeStats, FanOut, Receiver, SendError, Sender, Sent, TryRecvError, TrySendError,
};
pub use workflow::{
    Connection, ConnectionId, Group, GroupId, Node, NodeId, OverflowPolicy, PortRef, Workflow,
    DEFAULT_BUFFER_CAPACITY,
};

/// Runtime version, for display in the UI.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
