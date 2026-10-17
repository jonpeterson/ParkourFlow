//! Runtime state for a workflow: node status and connection edges.
//!
//! State is keyed by the ids in a [`Workflow`] but stored separately from it.
//! After editing the workflow, call [`WorkflowState::reconcile`] to bring the
//! state in line with the configuration.

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::WorkflowError;
use crate::preview::{PreviewSlot, PreviewSnapshot};
use crate::runner::{CancelToken, Wiring};
use crate::transport::{Edge, EdgeStats};
use crate::workflow::{ConnectionId, NodeId, Workflow};

/// Lifecycle status of a node.
///
/// - start: `STOPPED` or `ERROR` → `STARTING`
/// - startup succeeds: `STARTING` → `RUNNING`; startup fails: `STARTING` → `ERROR`
/// - stop: `RUNNING` → `STOPPING`
/// - the runner returns: `RUNNING` or `STOPPING` → `STOPPED`, or `ERROR` if
///   it failed
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NodeStatus {
    #[default]
    Stopped,
    Starting,
    Running,
    Stopping,
    Error,
}

impl fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            NodeStatus::Stopped => "STOPPED",
            NodeStatus::Starting => "STARTING",
            NodeStatus::Running => "RUNNING",
            NodeStatus::Stopping => "STOPPING",
            NodeStatus::Error => "ERROR",
        })
    }
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
/// counter. Each running node has a [`CancelToken`] that stops it.
#[derive(Debug, Default)]
pub struct WorkflowState {
    nodes: HashMap<NodeId, NodeState>,
    edges: HashMap<ConnectionId, Edge>,
    runs: HashMap<NodeId, CancelToken>,
    previews: HashMap<NodeId, PreviewSlot>,
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
    ///   closing their edges and cancelling removed nodes that are running,
    /// - adds stopped nodes and empty edges for new ones, and
    /// - applies changed buffer capacities and overflow policies, dropping
    ///   (and counting) the oldest items if a buffer shrank.
    pub fn reconcile(&mut self, workflow: &Workflow) {
        self.nodes.retain(|id, _| workflow.node(*id).is_some());
        self.previews.retain(|id, _| workflow.node(*id).is_some());
        self.runs.retain(|id, cancel| {
            let keep = workflow.node(*id).is_some();
            if !keep {
                cancel.cancel();
            }
            keep
        });
        self.edges.retain(|id, edge| {
            let keep = workflow.connection(*id).is_some();
            if !keep {
                edge.close();
            }
            keep
        });

        for node in workflow.nodes() {
            self.nodes.entry(node.id).or_default();
            self.previews.entry(node.id).or_default();
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

    fn node_for(&mut self, id: NodeId) -> Result<&mut NodeState, WorkflowError> {
        self.nodes
            .get_mut(&id)
            .ok_or(WorkflowError::NodeNotFound(id))
    }

    /// Begins starting a node: `STOPPED` or `ERROR` → `STARTING`.
    ///
    /// The caller then runs the node's startup work (see
    /// [`preflight`](crate::lifecycle::preflight)) and reports the outcome
    /// with [`finish_start`](Self::finish_start).
    pub fn begin_start(&mut self, id: NodeId) -> Result<&NodeState, WorkflowError> {
        let node = self.node_for(id)?;
        match node.status {
            NodeStatus::Stopped | NodeStatus::Error => {
                node.set_status(NodeStatus::Starting);
                Ok(node)
            }
            status => Err(WorkflowError::InvalidTransition {
                node: id,
                status,
                action: "start",
            }),
        }
    }

    /// Completes a start begun with [`begin_start`](Self::begin_start):
    /// `STARTING` → `RUNNING`, or `ERROR` with the failure message.
    ///
    /// On success, `cancel` is kept to stop the node later; the caller then
    /// runs the node and reports its end with
    /// [`finish_run`](Self::finish_run).
    ///
    /// Returns `None` if the node was removed or is no longer `STARTING`, in
    /// which case nothing changes and the caller must not run the node.
    pub fn finish_start(
        &mut self,
        id: NodeId,
        result: Result<CancelToken, String>,
    ) -> Option<&NodeState> {
        let node = self.nodes.get_mut(&id)?;
        if node.status != NodeStatus::Starting {
            return None;
        }
        match result {
            Ok(cancel) => {
                node.set_status(NodeStatus::Running);
                self.runs.insert(id, cancel);
            }
            Err(message) => node.fail(message),
        }
        Some(node)
    }

    /// Stops a running node: `RUNNING` → `STOPPING`, and cancels its runner.
    /// The node becomes `STOPPED` once the runner has returned (see
    /// [`finish_run`](Self::finish_run)).
    pub fn begin_stop(&mut self, id: NodeId) -> Result<&NodeState, WorkflowError> {
        let node = self
            .nodes
            .get_mut(&id)
            .ok_or(WorkflowError::NodeNotFound(id))?;
        match node.status {
            NodeStatus::Running => {
                node.set_status(NodeStatus::Stopping);
                if let Some(cancel) = self.runs.get(&id) {
                    cancel.cancel();
                }
                Ok(node)
            }
            status => Err(WorkflowError::InvalidTransition {
                node: id,
                status,
                action: "stop",
            }),
        }
    }

    /// Records that a node's runner has returned, on its own or after
    /// [`begin_stop`](Self::begin_stop): `RUNNING` or `STOPPING` →
    /// `STOPPED`, or `ERROR` with the runner's message.
    ///
    /// Returns `None` if the node was removed or is not running.
    pub fn finish_run(&mut self, id: NodeId, result: Result<(), String>) -> Option<&NodeState> {
        let node = self.nodes.get_mut(&id)?;
        if !matches!(node.status, NodeStatus::Running | NodeStatus::Stopping) {
            return None;
        }
        self.runs.remove(&id);
        match result {
            Ok(()) => node.set_status(NodeStatus::Stopped),
            Err(message) => node.fail(message),
        }
        Some(node)
    }

    /// The edges each of a node's ports is connected to, for starting it.
    pub fn wiring(&self, workflow: &Workflow, id: NodeId) -> Result<Wiring, WorkflowError> {
        let node = workflow.node(id).ok_or(WorkflowError::NodeNotFound(id))?;
        let edge = |conn: ConnectionId| self.edges.get(&conn).cloned();
        let connections: Vec<_> = workflow.connections_of(id).collect();
        let inputs = node
            .inputs
            .iter()
            .map(|port| {
                let edge = connections
                    .iter()
                    .find(|c| c.to.node == id && c.to.port == port.name)
                    .and_then(|c| edge(c.id));
                (port.name.clone(), edge)
            })
            .collect();
        let outputs = node
            .outputs
            .iter()
            .map(|port| {
                let edges = connections
                    .iter()
                    .filter(|c| c.from.node == id && c.from.port == port.name)
                    .filter_map(|c| edge(c.id))
                    .collect();
                (port.name.clone(), edges)
            })
            .collect();
        let preview = self.previews.get(&id).cloned().unwrap_or_default();
        Ok(Wiring {
            inputs,
            outputs,
            preview,
        })
    }

    pub fn edge(&self, id: ConnectionId) -> Option<&Edge> {
        self.edges.get(&id)
    }

    /// The latest preview of every node that has one.
    pub fn previews(&self) -> impl Iterator<Item = (NodeId, PreviewSnapshot)> + '_ {
        self.previews
            .iter()
            .filter_map(|(id, slot)| Some((*id, slot.snapshot()?)))
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

    fn ok() -> Result<CancelToken, String> {
        Ok(CancelToken::new())
    }

    #[test]
    fn start_and_stop_transitions() {
        let (wf, source, ..) = two_nodes();
        let mut state = WorkflowState::new(&wf);
        let invalid = |status, action| WorkflowError::InvalidTransition {
            node: source,
            status,
            action,
        };

        assert_eq!(
            state.begin_stop(source),
            Err(invalid(NodeStatus::Stopped, "stop"))
        );
        assert_eq!(
            state.begin_start(source).unwrap().status,
            NodeStatus::Starting
        );
        assert_eq!(
            state.begin_start(source),
            Err(invalid(NodeStatus::Starting, "start"))
        );
        assert_eq!(
            state.begin_stop(source),
            Err(invalid(NodeStatus::Starting, "stop"))
        );
        assert!(state.finish_run(source, Ok(())).is_none());

        let cancel = CancelToken::new();
        let node = state.finish_start(source, Ok(cancel.clone())).unwrap();
        assert_eq!(node.status, NodeStatus::Running);
        assert_eq!(
            state.begin_start(source),
            Err(invalid(NodeStatus::Running, "start"))
        );

        // Stopping cancels the runner; the node is STOPPED once it returns.
        let node = state.begin_stop(source).unwrap();
        assert_eq!(node.status, NodeStatus::Stopping);
        assert!(cancel.is_cancelled());
        assert_eq!(
            state.begin_stop(source),
            Err(invalid(NodeStatus::Stopping, "stop"))
        );
        let node = state.finish_run(source, Ok(())).unwrap();
        assert_eq!(node.status, NodeStatus::Stopped);
        assert!(state.finish_run(source, Ok(())).is_none());
    }

    #[test]
    fn runner_end_and_failure() {
        let (wf, source, ..) = two_nodes();
        let mut state = WorkflowState::new(&wf);

        // A runner that returns on its own stops the node.
        state.begin_start(source).unwrap();
        state.finish_start(source, ok()).unwrap();
        let node = state.finish_run(source, Ok(())).unwrap();
        assert_eq!(node.status, NodeStatus::Stopped);

        // A failing runner puts it in ERROR, which can be retried.
        state.begin_start(source).unwrap();
        state.finish_start(source, ok()).unwrap();
        let node = state.finish_run(source, Err("disk full".into())).unwrap();
        assert_eq!(node.status, NodeStatus::Error);
        assert_eq!(node.error.as_deref(), Some("disk full"));
        assert_eq!(
            state.begin_start(source).unwrap().status,
            NodeStatus::Starting
        );
    }

    #[test]
    fn removing_a_running_node_cancels_it() {
        let (mut wf, source, ..) = two_nodes();
        let mut state = WorkflowState::new(&wf);
        let cancel = CancelToken::new();
        state.begin_start(source).unwrap();
        state.finish_start(source, Ok(cancel.clone())).unwrap();
        wf.remove_node(source).unwrap();
        state.reconcile(&wf);
        assert!(cancel.is_cancelled());
        assert!(state.finish_run(source, Ok(())).is_none());
    }

    #[test]
    fn wiring_lists_each_ports_edges() {
        let (wf, source, parser, conn) = two_nodes();
        let state = WorkflowState::new(&wf);
        let wiring = state.wiring(&wf, source).unwrap();
        assert!(wiring.inputs.is_empty());
        assert_eq!(wiring.outputs.len(), 1);
        assert_eq!(wiring.outputs[0].0, "packets");
        assert_eq!(wiring.outputs[0].1.len(), 1);

        let wiring = state.wiring(&wf, parser).unwrap();
        let input = wiring.inputs[0].1.as_ref().unwrap();
        assert_eq!(input.stats(), state.edge(conn).unwrap().stats());
        assert!(wiring.outputs[0].1.is_empty());
    }

    #[test]
    fn failed_start_can_be_retried() {
        let (wf, source, ..) = two_nodes();
        let mut state = WorkflowState::new(&wf);
        state.begin_start(source).unwrap();
        let node = state
            .finish_start(source, Err("file not found".into()))
            .unwrap();
        assert_eq!(node.status, NodeStatus::Error);
        assert_eq!(node.error.as_deref(), Some("file not found"));

        let node = state.begin_start(source).unwrap();
        assert_eq!(node.status, NodeStatus::Starting);
        assert_eq!(node.error, None);
    }

    #[test]
    fn finish_start_ignores_nodes_no_longer_starting() {
        let (mut wf, source, ..) = two_nodes();
        let mut state = WorkflowState::new(&wf);
        assert!(state.finish_start(source, ok()).is_none());
        assert_eq!(state.node(source).unwrap().status, NodeStatus::Stopped);

        state.begin_start(source).unwrap();
        wf.remove_node(source).unwrap();
        state.reconcile(&wf);
        assert!(state.finish_start(source, ok()).is_none());
        assert_eq!(
            state.begin_start(source),
            Err(WorkflowError::NodeNotFound(source))
        );
    }
}
