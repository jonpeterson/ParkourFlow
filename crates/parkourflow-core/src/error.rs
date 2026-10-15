use crate::node::{DataType, NodeKind};
use crate::workflow::{ConnectionId, GroupId, NodeId, PortRef};

/// Errors from editing a [`Workflow`](crate::Workflow).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WorkflowError {
    #[error("{0} not found")]
    GroupNotFound(GroupId),
    #[error("{0} not found")]
    NodeNotFound(NodeId),
    #[error("{0} not found")]
    ConnectionNotFound(ConnectionId),
    #[error("the root group cannot be removed")]
    CannotRemoveRoot,
    #[error("{kind} nodes {reason}")]
    InvalidPorts {
        kind: NodeKind,
        reason: &'static str,
    },
    #[error("duplicate port name `{0}`")]
    DuplicatePort(String),
    #[error("{0} is not an output port")]
    OutputPortNotFound(PortRef),
    #[error("{0} is not an input port")]
    InputPortNotFound(PortRef),
    #[error("cannot connect {output} output to {input} input")]
    DataTypeMismatch { output: DataType, input: DataType },
    #[error("{0} is already connected")]
    InputAlreadyConnected(PortRef),
    #[error("{0} cannot connect to itself")]
    SelfConnection(NodeId),
    #[error("buffer capacity must be at least 1")]
    ZeroBufferCapacity,
}
