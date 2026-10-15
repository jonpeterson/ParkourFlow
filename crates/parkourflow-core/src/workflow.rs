//! Workflow configuration: the group tree, nodes, and connections.
//!
//! Nothing here holds runtime state. Node status and buffered data live in
//! [`WorkflowState`](crate::WorkflowState).

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::catalog::NodeDescriptor;
use crate::error::WorkflowError;
use crate::node::{NodeKind, Port};

/// Buffer capacity used when a connection doesn't specify one.
pub const DEFAULT_BUFFER_CAPACITY: usize = 64;

macro_rules! id_type {
    ($(#[$meta:meta])* $name:ident, $label:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(u64);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($label, " {}"), self.0)
            }
        }
    };
}

id_type!(
    /// Identifies a group within a workflow.
    GroupId,
    "group"
);
id_type!(
    /// Identifies a node within a workflow.
    NodeId,
    "node"
);
id_type!(
    /// Identifies a connection within a workflow.
    ConnectionId,
    "connection"
);

/// A named port on a specific node.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PortRef {
    pub node: NodeId,
    pub port: String,
}

impl PortRef {
    pub fn new(node: NodeId, port: impl Into<String>) -> Self {
        Self {
            node,
            port: port.into(),
        }
    }
}

impl fmt::Display for PortRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} port `{}`", self.node, self.port)
    }
}

/// A container for nodes and child groups. Every workflow has a root group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Group {
    pub id: GroupId,
    pub name: String,
    /// `None` only for the root group.
    pub parent: Option<GroupId>,
    pub groups: Vec<GroupId>,
    pub nodes: Vec<NodeId>,
}

/// A node instance in a workflow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Node {
    pub id: NodeId,
    /// The group this node belongs to.
    pub group: GroupId,
    pub name: String,
    /// The [`NodeDescriptor::type_id`] this node was created from.
    pub type_id: String,
    pub kind: NodeKind,
    pub inputs: Vec<Port>,
    pub outputs: Vec<Port>,
}

impl Node {
    pub fn input(&self, name: &str) -> Option<&Port> {
        self.inputs.iter().find(|p| p.name == name)
    }

    pub fn output(&self, name: &str) -> Option<&Port> {
        self.outputs.iter().find(|p| p.name == name)
    }
}

/// What a connection does when an item arrives and its buffer is full.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverflowPolicy {
    /// The producer waits for space. Nothing is lost. Live sources, which
    /// cannot wait, drop the incoming item instead.
    #[default]
    Block,
    /// Evict the oldest queued item to make room.
    DropOldest,
    /// Discard the incoming item.
    DropNewest,
    /// Hold a single item; each new item replaces it.
    KeepLatest,
}

impl OverflowPolicy {
    /// The number of items a buffer holds under this policy:
    /// 1 for [`KeepLatest`](Self::KeepLatest), otherwise `configured`.
    pub fn effective_capacity(self, configured: usize) -> usize {
        match self {
            Self::KeepLatest => 1,
            _ => configured,
        }
    }
}

/// A stream from one node's output port to another node's input port.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Connection {
    pub id: ConnectionId,
    /// The output port that emits data.
    pub from: PortRef,
    /// The input port that receives data.
    pub to: PortRef,
    /// Maximum number of items buffered on this connection.
    pub buffer_capacity: usize,
    pub overflow_policy: OverflowPolicy,
}

impl Connection {
    /// The number of items the buffer actually holds: 1 for
    /// [`OverflowPolicy::KeepLatest`], otherwise `buffer_capacity`.
    pub fn effective_capacity(&self) -> usize {
        self.overflow_policy
            .effective_capacity(self.buffer_capacity)
    }
}

/// A workflow's configuration.
///
/// Groups, nodes, and connections are stored flat and keyed by id; the tree
/// is formed by [`Group::groups`] and [`Group::nodes`]. Connections are
/// stored at the workflow level because they can cross group boundaries.
///
/// All edits go through methods that keep the workflow valid: every
/// connection joins an existing output port to an existing input port of the
/// same data type, and each input port has at most one connection.
#[derive(Debug, Clone, Serialize)]
pub struct Workflow {
    name: String,
    root: GroupId,
    groups: BTreeMap<GroupId, Group>,
    nodes: BTreeMap<NodeId, Node>,
    connections: BTreeMap<ConnectionId, Connection>,
    next_id: u64,
}

impl Workflow {
    /// Creates an empty workflow. Its root group shares the workflow's name.
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        let root = GroupId(0);
        let groups = BTreeMap::from([(
            root,
            Group {
                id: root,
                name: name.clone(),
                parent: None,
                groups: Vec::new(),
                nodes: Vec::new(),
            },
        )]);
        Self {
            name,
            root,
            groups,
            nodes: BTreeMap::new(),
            connections: BTreeMap::new(),
            next_id: 1,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn root(&self) -> GroupId {
        self.root
    }

    pub fn group(&self, id: GroupId) -> Option<&Group> {
        self.groups.get(&id)
    }

    pub fn node(&self, id: NodeId) -> Option<&Node> {
        self.nodes.get(&id)
    }

    pub fn connection(&self, id: ConnectionId) -> Option<&Connection> {
        self.connections.get(&id)
    }

    pub fn groups(&self) -> impl Iterator<Item = &Group> {
        self.groups.values()
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.values()
    }

    pub fn connections(&self) -> impl Iterator<Item = &Connection> {
        self.connections.values()
    }

    /// Connections that start or end at `node`.
    pub fn connections_of(&self, node: NodeId) -> impl Iterator<Item = &Connection> {
        self.connections
            .values()
            .filter(move |c| c.from.node == node || c.to.node == node)
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn group_mut(&mut self, id: GroupId) -> Result<&mut Group, WorkflowError> {
        self.groups
            .get_mut(&id)
            .ok_or(WorkflowError::GroupNotFound(id))
    }

    /// Adds an empty group inside `parent`.
    pub fn add_group(
        &mut self,
        parent: GroupId,
        name: impl Into<String>,
    ) -> Result<GroupId, WorkflowError> {
        self.group_mut(parent)?;
        let id = GroupId(self.next_id());
        self.groups.insert(
            id,
            Group {
                id,
                name: name.into(),
                parent: Some(parent),
                groups: Vec::new(),
                nodes: Vec::new(),
            },
        );
        self.group_mut(parent)?.groups.push(id);
        Ok(id)
    }

    /// Removes a group along with all of its nodes, child groups, and any
    /// connections to those nodes.
    pub fn remove_group(&mut self, id: GroupId) -> Result<(), WorkflowError> {
        if id == self.root {
            return Err(WorkflowError::CannotRemoveRoot);
        }
        let group = self
            .groups
            .get(&id)
            .ok_or(WorkflowError::GroupNotFound(id))?;
        if let Some(parent) = group.parent {
            self.group_mut(parent)?.groups.retain(|&g| g != id);
        }

        let mut pending = vec![id];
        while let Some(gid) = pending.pop() {
            let Some(group) = self.groups.remove(&gid) else {
                continue;
            };
            pending.extend(group.groups);
            for node in group.nodes {
                self.nodes.remove(&node);
                self.connections
                    .retain(|_, c| c.from.node != node && c.to.node != node);
            }
        }
        Ok(())
    }

    /// Adds a node of the given type to `group`.
    pub fn add_node(
        &mut self,
        group: GroupId,
        name: impl Into<String>,
        descriptor: &NodeDescriptor,
    ) -> Result<NodeId, WorkflowError> {
        descriptor.validate()?;
        self.group_mut(group)?;
        let id = NodeId(self.next_id());
        self.nodes.insert(
            id,
            Node {
                id,
                group,
                name: name.into(),
                type_id: descriptor.type_id.clone(),
                kind: descriptor.kind,
                inputs: descriptor.inputs.clone(),
                outputs: descriptor.outputs.clone(),
            },
        );
        self.group_mut(group)?.nodes.push(id);
        Ok(id)
    }

    /// Removes a node and every connection to or from it.
    pub fn remove_node(&mut self, id: NodeId) -> Result<Node, WorkflowError> {
        let node = self
            .nodes
            .remove(&id)
            .ok_or(WorkflowError::NodeNotFound(id))?;
        self.group_mut(node.group)?.nodes.retain(|&n| n != id);
        self.connections
            .retain(|_, c| c.from.node != id && c.to.node != id);
        Ok(node)
    }

    /// Moves a node into another group. Its connections are unaffected.
    pub fn move_node(&mut self, id: NodeId, group: GroupId) -> Result<(), WorkflowError> {
        self.group_mut(group)?;
        let node = self
            .nodes
            .get_mut(&id)
            .ok_or(WorkflowError::NodeNotFound(id))?;
        let old = std::mem::replace(&mut node.group, group);
        self.group_mut(old)?.nodes.retain(|&n| n != id);
        self.group_mut(group)?.nodes.push(id);
        Ok(())
    }

    /// Connects an output port to an input port with the default
    /// [`OverflowPolicy::Block`].
    ///
    /// Fails unless both ports exist, their data types match, and the input
    /// isn't already connected. One output may feed many inputs.
    pub fn connect(
        &mut self,
        from: PortRef,
        to: PortRef,
        buffer_capacity: usize,
    ) -> Result<ConnectionId, WorkflowError> {
        if buffer_capacity == 0 {
            return Err(WorkflowError::ZeroBufferCapacity);
        }
        if from.node == to.node {
            return Err(WorkflowError::SelfConnection(from.node));
        }
        let output = self
            .nodes
            .get(&from.node)
            .ok_or(WorkflowError::NodeNotFound(from.node))?
            .output(&from.port)
            .ok_or_else(|| WorkflowError::OutputPortNotFound(from.clone()))?;
        let input = self
            .nodes
            .get(&to.node)
            .ok_or(WorkflowError::NodeNotFound(to.node))?
            .input(&to.port)
            .ok_or_else(|| WorkflowError::InputPortNotFound(to.clone()))?;
        if output.data_type != input.data_type {
            return Err(WorkflowError::DataTypeMismatch {
                output: output.data_type.clone(),
                input: input.data_type.clone(),
            });
        }
        if self.connections.values().any(|c| c.to == to) {
            return Err(WorkflowError::InputAlreadyConnected(to));
        }

        let id = ConnectionId(self.next_id());
        self.connections.insert(
            id,
            Connection {
                id,
                from,
                to,
                buffer_capacity,
                overflow_policy: OverflowPolicy::default(),
            },
        );
        Ok(id)
    }

    fn connection_mut(&mut self, id: ConnectionId) -> Result<&mut Connection, WorkflowError> {
        self.connections
            .get_mut(&id)
            .ok_or(WorkflowError::ConnectionNotFound(id))
    }

    pub fn disconnect(&mut self, id: ConnectionId) -> Result<Connection, WorkflowError> {
        self.connections
            .remove(&id)
            .ok_or(WorkflowError::ConnectionNotFound(id))
    }

    pub fn set_buffer_capacity(
        &mut self,
        id: ConnectionId,
        capacity: usize,
    ) -> Result<(), WorkflowError> {
        if capacity == 0 {
            return Err(WorkflowError::ZeroBufferCapacity);
        }
        self.connection_mut(id)?.buffer_capacity = capacity;
        Ok(())
    }

    pub fn set_overflow_policy(
        &mut self,
        id: ConnectionId,
        policy: OverflowPolicy,
    ) -> Result<(), WorkflowError> {
        self.connection_mut(id)?.overflow_policy = policy;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::builtin_nodes;

    /// pcap source → DNS parser → CSV writer, with the parser and writer in a
    /// child group.
    fn dns_pipeline() -> (Workflow, GroupId, [NodeId; 3]) {
        let [pcap, dns, csv]: [NodeDescriptor; 3] = builtin_nodes().try_into().unwrap();
        let mut wf = Workflow::new("DNS capture");
        let root = wf.root();
        let group = wf.add_group(root, "Parsing").unwrap();
        let source = wf.add_node(root, "Capture", &pcap).unwrap();
        let parser = wf.add_node(group, "Parse", &dns).unwrap();
        let sink = wf.add_node(group, "Write", &csv).unwrap();
        (wf, group, [source, parser, sink])
    }

    #[test]
    fn builds_a_pipeline() {
        let (mut wf, group, [source, parser, sink]) = dns_pipeline();
        let a = wf
            .connect(
                PortRef::new(source, "packets"),
                PortRef::new(parser, "packets"),
                DEFAULT_BUFFER_CAPACITY,
            )
            .unwrap();
        wf.connect(
            PortRef::new(parser, "messages"),
            PortRef::new(sink, "records"),
            8,
        )
        .unwrap();

        assert_eq!(wf.connections().count(), 2);
        assert_eq!(
            wf.connection(a).unwrap().buffer_capacity,
            DEFAULT_BUFFER_CAPACITY
        );
        assert_eq!(wf.group(wf.root()).unwrap().nodes, vec![source]);
        assert_eq!(wf.group(wf.root()).unwrap().groups, vec![group]);
        assert_eq!(wf.group(group).unwrap().nodes, vec![parser, sink]);
        assert_eq!(wf.connections_of(parser).count(), 2);
    }

    #[test]
    fn rejects_invalid_connections() {
        let (mut wf, _, [source, parser, sink]) = dns_pipeline();
        let port = PortRef::new;

        // Data types must match.
        assert!(matches!(
            wf.connect(port(source, "packets"), port(sink, "records"), 1),
            Err(WorkflowError::DataTypeMismatch { .. })
        ));
        // `from` must be an output and `to` an input.
        assert_eq!(
            wf.connect(port(parser, "packets"), port(sink, "records"), 1),
            Err(WorkflowError::OutputPortNotFound(port(parser, "packets")))
        );
        assert_eq!(
            wf.connect(port(source, "packets"), port(parser, "messages"), 1),
            Err(WorkflowError::InputPortNotFound(port(parser, "messages")))
        );
        assert_eq!(
            wf.connect(port(parser, "messages"), port(parser, "packets"), 1),
            Err(WorkflowError::SelfConnection(parser))
        );
        assert_eq!(
            wf.connect(port(source, "packets"), port(parser, "packets"), 0),
            Err(WorkflowError::ZeroBufferCapacity)
        );

        // An input accepts only one connection.
        wf.connect(port(source, "packets"), port(parser, "packets"), 1)
            .unwrap();
        assert_eq!(
            wf.connect(port(source, "packets"), port(parser, "packets"), 1),
            Err(WorkflowError::InputAlreadyConnected(port(
                parser, "packets"
            )))
        );
    }

    #[test]
    fn output_can_fan_out() {
        let (mut wf, group, [source, parser, _]) = dns_pipeline();
        let dns = &builtin_nodes()[1];
        let parser2 = wf.add_node(group, "Parse 2", dns).unwrap();
        for target in [parser, parser2] {
            wf.connect(
                PortRef::new(source, "packets"),
                PortRef::new(target, "packets"),
                1,
            )
            .unwrap();
        }
        assert_eq!(wf.connections_of(source).count(), 2);
    }

    #[test]
    fn removing_a_node_removes_its_connections() {
        let (mut wf, group, [source, parser, sink]) = dns_pipeline();
        wf.connect(
            PortRef::new(source, "packets"),
            PortRef::new(parser, "packets"),
            1,
        )
        .unwrap();
        wf.connect(
            PortRef::new(parser, "messages"),
            PortRef::new(sink, "records"),
            1,
        )
        .unwrap();

        wf.remove_node(parser).unwrap();
        assert!(wf.node(parser).is_none());
        assert_eq!(wf.connections().count(), 0);
        assert_eq!(wf.group(group).unwrap().nodes, vec![sink]);
    }

    #[test]
    fn removing_a_group_removes_its_contents() {
        let (mut wf, group, [source, parser, sink]) = dns_pipeline();
        let nested = wf.add_group(group, "Nested").unwrap();
        let nested_node = wf.add_node(nested, "Parse 2", &builtin_nodes()[1]).unwrap();
        wf.connect(
            PortRef::new(source, "packets"),
            PortRef::new(parser, "packets"),
            1,
        )
        .unwrap();

        wf.remove_group(group).unwrap();
        for node in [parser, sink, nested_node] {
            assert!(wf.node(node).is_none());
        }
        assert!(wf.group(nested).is_none());
        assert!(wf.node(source).is_some());
        assert_eq!(wf.connections().count(), 0);
        assert!(wf.group(wf.root()).unwrap().groups.is_empty());

        assert_eq!(
            wf.remove_group(wf.root()),
            Err(WorkflowError::CannotRemoveRoot)
        );
    }

    #[test]
    fn move_node_between_groups() {
        let (mut wf, group, [source, ..]) = dns_pipeline();
        wf.move_node(source, group).unwrap();
        assert_eq!(wf.node(source).unwrap().group, group);
        assert!(wf.group(wf.root()).unwrap().nodes.is_empty());
        assert!(wf.group(group).unwrap().nodes.contains(&source));
    }

    #[test]
    fn serializes_to_json() {
        let (mut wf, _, [source, parser, _]) = dns_pipeline();
        wf.connect(
            PortRef::new(source, "packets"),
            PortRef::new(parser, "packets"),
            4,
        )
        .unwrap();
        let json = serde_json::to_value(&wf).unwrap();
        assert_eq!(json["name"], "DNS capture");
        assert_eq!(json["groups"]["0"]["nodes"][0], 2);
        assert_eq!(json["nodes"]["3"]["kind"], "processor");
        assert_eq!(json["connections"]["5"]["buffer_capacity"], 4);
        assert_eq!(json["connections"]["5"]["overflow_policy"], "block");
    }

    #[test]
    fn overflow_policy_and_effective_capacity() {
        let (mut wf, _, [source, parser, _]) = dns_pipeline();
        let id = wf
            .connect(
                PortRef::new(source, "packets"),
                PortRef::new(parser, "packets"),
                8,
            )
            .unwrap();
        let conn = wf.connection(id).unwrap();
        assert_eq!(conn.overflow_policy, OverflowPolicy::Block);
        assert_eq!(conn.effective_capacity(), 8);

        wf.set_overflow_policy(id, OverflowPolicy::KeepLatest)
            .unwrap();
        let conn = wf.connection(id).unwrap();
        assert_eq!(conn.effective_capacity(), 1);
        // The configured capacity is kept for when the policy changes back.
        assert_eq!(conn.buffer_capacity, 8);

        let missing = ConnectionId(999);
        assert_eq!(
            wf.set_overflow_policy(missing, OverflowPolicy::DropOldest),
            Err(WorkflowError::ConnectionNotFound(missing))
        );
    }
}
