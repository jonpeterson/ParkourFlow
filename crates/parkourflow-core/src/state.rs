//! Runtime state for a workflow: node status and connection edges.
//!
//! State is keyed by the ids in a [`Workflow`] but stored separately from it.
//! After editing the workflow, call [`WorkflowState::reconcile`] to bring the
//! state in line with the configuration.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::transport::{Edge, EdgeStats};
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
///
/// Each connection has an [`Edge`]: its buffer, overflow policy, and drop
/// counter.
#[derive(Debug, Default)]
pub struct WorkflowState {
    nodes: HashMap<NodeId, NodeState>,
    edges: HashMap<ConnectionId, Edge>,
}

impl WorkflowState {
    /// Creates state for `workflow`: every node stopped, every edge empty.
    pub fn new(workflow: &Workflow) -> Self {
        let mut state = Self::default();
        state.reconcile(workflow);
        state
    }

    /// Brings the state in line with `workflow`:
    ///
    /// - removes state for nodes and connections that no longer exist,
    ///   closing their edges,
    /// - adds stopped nodes and empty edges for new ones, and
    /// - applies changed buffer capacities and overflow policies, dropping
    ///   (and counting) the oldest items if a buffer shrank.
    pub fn reconcile(&mut self, workflow: &Workflow) {
        self.nodes.retain(|id, _| workflow.node(*id).is_some());
        self.edges.retain(|id, edge| {
            let keep = workflow.connection(*id).is_some();
            if !keep {
                edge.close();
            }
            keep
        });

        for node in workflow.nodes() {
            self.nodes.entry(node.id).or_default();
        }
        for connection in workflow.connections() {
            let edge = self
                .edges
                .entry(connection.id)
                .or_insert_with(|| Edge::for_connection(connection));
            if edge.policy() != connection.overflow_policy
                || edge.stats().capacity != connection.effective_capacity()
            {
                edge.configure(connection.buffer_capacity, connection.overflow_policy);
            }
        }
    }

    pub fn node(&self, id: NodeId) -> Option<&NodeState> {
        self.nodes.get(&id)
    }

    pub fn node_mut(&mut self, id: NodeId) -> Option<&mut NodeState> {
        self.nodes.get_mut(&id)
    }

    pub fn edge(&self, id: ConnectionId) -> Option<&Edge> {
        self.edges.get(&id)
    }

    /// Queue depth, capacity, and drop count of every connection.
    pub fn edge_stats(&self) -> impl Iterator<Item = (ConnectionId, EdgeStats)> + '_ {
        self.edges.iter().map(|(id, edge)| (*id, edge.stats()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::StreamItem;
    use crate::catalog::builtin_nodes;
    use crate::workflow::{OverflowPolicy, PortRef};

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
        let stats = state.edge(conn).unwrap().stats();
        assert_eq!(stats.queue_depth, 0);
        assert_eq!(stats.capacity, 4);
        assert_eq!(stats.dropped, 0);
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
        let edge = state.edge(conn).unwrap().clone();
        let tx = edge.sender();
        let rx = edge.receiver();
        for i in 0..4 {
            tx.try_send(StreamItem::new(i, ())).unwrap();
        }

        // Shrinking the buffer keeps the newest items and counts the drops;
        // other state survives.
        wf.set_buffer_capacity(conn, 2).unwrap();
        state.reconcile(&wf);
        let stats = edge.stats();
        assert_eq!(
            (stats.capacity, stats.queue_depth, stats.dropped),
            (2, 2, 2)
        );
        assert_eq!(rx.try_recv().unwrap().sequence, 2);
        assert_eq!(state.node(source).unwrap().status, NodeStatus::Running);

        // A policy change reaches the edge; KeepLatest holds one item.
        wf.set_overflow_policy(conn, OverflowPolicy::KeepLatest)
            .unwrap();
        state.reconcile(&wf);
        assert_eq!(edge.policy(), OverflowPolicy::KeepLatest);
        assert_eq!(edge.stats().capacity, 1);
        assert_eq!(state.edge_stats().count(), 1);

        // Removed nodes and connections lose their state, and the edge
        // closes so its producer and consumer stop.
        wf.remove_node(parser).unwrap();
        state.reconcile(&wf);
        assert!(state.node(parser).is_none());
        assert!(state.edge(conn).is_none());
        assert!(tx.is_closed());
        assert!(tx.try_send(StreamItem::new(9, ())).is_err());
    }
}
