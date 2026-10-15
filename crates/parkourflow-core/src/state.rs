//! Runtime state for a workflow: node status and connection buffers.
//!
//! State is keyed by the ids in a [`Workflow`] but stored separately from it.
//! After editing the workflow, call [`WorkflowState::reconcile`] to bring the
//! state in line with the configuration.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::buffer::ConnectionBuffer;
use crate::workflow::{ConnectionId, NodeId, Workflow};

/// Lifecycle status of a node.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NodeStatus {
    #[default]
    Stopped,
    Starting,
    Running,
    Error,
}

/// Runtime state of one node.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct NodeState {
    pub status: NodeStatus,
    /// Why the node is in [`NodeStatus::Error`]. `None` in any other status.
    pub error: Option<String>,
}

impl NodeState {
    /// Sets a non-error status and clears any previous error.
    pub fn set_status(&mut self, status: NodeStatus) {
        self.status = status;
        self.error = None;
    }

    /// Puts the node in [`NodeStatus::Error`] with a message.
    pub fn fail(&mut self, message: impl Into<String>) {
        self.status = NodeStatus::Error;
        self.error = Some(message.into());
    }
}

/// Runtime state for every node and connection in a workflow.
#[derive(Debug, Default)]
pub struct WorkflowState {
    nodes: HashMap<NodeId, NodeState>,
    buffers: HashMap<ConnectionId, ConnectionBuffer>,
}

impl WorkflowState {
    /// Creates state for `workflow`: every node stopped, every buffer empty.
    pub fn new(workflow: &Workflow) -> Self {
        let mut state = Self::default();
        state.reconcile(workflow);
        state
    }

    /// Brings the state in line with `workflow`:
    ///
    /// - removes state for nodes and connections that no longer exist,
    /// - adds stopped nodes and empty buffers for new ones, and
    /// - applies changed buffer capacities, dropping the oldest items if a
    ///   buffer shrank.
    pub fn reconcile(&mut self, workflow: &Workflow) {
        self.nodes.retain(|id, _| workflow.node(*id).is_some());
        self.buffers
            .retain(|id, _| workflow.connection(*id).is_some());

        for node in workflow.nodes() {
            self.nodes.entry(node.id).or_default();
        }
        for connection in workflow.connections() {
            let buffer = self
                .buffers
                .entry(connection.id)
                .or_insert_with(|| ConnectionBuffer::with_capacity(connection.buffer_capacity));
            if buffer.capacity() != connection.buffer_capacity {
                buffer.set_capacity(connection.buffer_capacity);
            }
        }
    }

    pub fn node(&self, id: NodeId) -> Option<&NodeState> {
        self.nodes.get(&id)
    }

    pub fn node_mut(&mut self, id: NodeId) -> Option<&mut NodeState> {
        self.nodes.get_mut(&id)
    }

    pub fn buffer(&self, id: ConnectionId) -> Option<&ConnectionBuffer> {
        self.buffers.get(&id)
    }

    pub fn buffer_mut(&mut self, id: ConnectionId) -> Option<&mut ConnectionBuffer> {
        self.buffers.get_mut(&id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::StreamItem;
    use crate::catalog::builtin_nodes;
    use crate::workflow::PortRef;

    fn two_nodes() -> (Workflow, NodeId, NodeId, ConnectionId) {
        let nodes = builtin_nodes();
        let mut wf = Workflow::new("test");
        let root = wf.root();
        let source = wf.add_node(root, "Capture", &nodes[0]).unwrap();
        let parser = wf.add_node(root, "Parse", &nodes[1]).unwrap();
        let conn = wf
            .connect(
                PortRef::new(source, "packets"),
                PortRef::new(parser, "packets"),
                4,
            )
            .unwrap();
        (wf, source, parser, conn)
    }

    #[test]
    fn new_state_is_stopped_and_empty() {
        let (wf, source, parser, conn) = two_nodes();
        let state = WorkflowState::new(&wf);
        for node in [source, parser] {
            assert_eq!(state.node(node).unwrap().status, NodeStatus::Stopped);
        }
        let buffer = state.buffer(conn).unwrap();
        assert!(buffer.is_empty());
        assert_eq!(buffer.capacity(), 4);
    }

    #[test]
    fn status_and_errors() {
        let (wf, source, ..) = two_nodes();
        let mut state = WorkflowState::new(&wf);
        let node = state.node_mut(source).unwrap();

        node.fail("device not found");
        assert_eq!(node.status, NodeStatus::Error);
        assert_eq!(node.error.as_deref(), Some("device not found"));

        node.set_status(NodeStatus::Starting);
        assert_eq!(node.status, NodeStatus::Starting);
        assert_eq!(node.error, None);
    }

    #[test]
    fn reconcile_follows_config_changes() {
        let (mut wf, source, parser, conn) = two_nodes();
        let mut state = WorkflowState::new(&wf);
        state
            .node_mut(source)
            .unwrap()
            .set_status(NodeStatus::Running);
        let buffer = state.buffer_mut(conn).unwrap();
        for i in 0..4 {
            buffer.push(StreamItem::new(i, ())).unwrap();
        }

        // Shrinking the buffer keeps the newest items; other state survives.
        wf.set_buffer_capacity(conn, 2).unwrap();
        state.reconcile(&wf);
        let buffer = state.buffer(conn).unwrap();
        assert_eq!(buffer.capacity(), 2);
        assert_eq!(buffer.len(), 2);
        assert_eq!(state.node(source).unwrap().status, NodeStatus::Running);

        // Removed nodes and connections lose their state.
        wf.remove_node(parser).unwrap();
        state.reconcile(&wf);
        assert!(state.node(parser).is_none());
        assert!(state.buffer(conn).is_none());
    }
}
