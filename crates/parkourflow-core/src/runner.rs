//! Node execution: the code a `RUNNING` node runs.
//!
//! A node type that can run registers a [`BuildRunner`] in a
//! [`NodeRegistry`]. Starting a node goes through [`prepare`]: it runs
//! [`preflight`], builds the node's [`NodeRunner`] from its typed config
//! (`Node::config::<T>()`), and attaches the node to its connections' edges.
//! [`PreparedRun::run`] then runs it on the calling thread, which the caller
//! dedicates to the node.
//!
//! A runner gets a [`NodeContext`] with an [`Input`] per input port and an
//! [`Output`] per output port. Both wait for data or credit like the
//! underlying [`Receiver`] and [`Sender`](crate::transport::Sender) do, and
//! both give up when the node is stopped ([`CancelToken`]), so a runner
//! blocked on an edge still stops promptly.
//!
//! When a runner returns, its handles drop and its edges close: downstream
//! nodes drain what is queued and then see the end of the stream, and
//! upstream nodes see their sends fail. So a pipeline that reads a file
//! finishes on its own once the file is read.

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::Serialize;

use crate::buffer::StreamItem;
use crate::lifecycle::preflight;
use crate::node::NodeKind;
use crate::preview::PreviewSlot;
use crate::property::{DeviceOption, ListDevices};
use crate::transport::{Edge, FanOut, Receiver};
use crate::workflow::{Node, NodeId};

/// Whether a node's outputs can wait for downstream credit. See README →
/// "Flow control".
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Pacing {
    /// Waits for credit on full `Block` edges: lossless, bounded memory.
    /// Files, replays, decoders, and most processors.
    #[default]
    Pausable,
    /// Never waits. On a full `Block` edge the item is dropped and counted.
    /// Capture devices, cameras, microphones, sockets.
    Live,
}

/// The work a running node does.
///
/// A runner is built from a node's configuration when the node starts and
/// consumed when it runs. It runs on a thread of its own, so it may block,
/// but it must return soon after its [`NodeContext`] is cancelled. Blocking
/// on an [`Input`] or [`Output`] already does; other loops should check
/// [`NodeContext::is_cancelled`].
pub trait NodeRunner: Send + 'static {
    /// How this node's outputs behave under backpressure. Asked once, after
    /// the runner is built and before it runs, so it may depend on the
    /// node's properties (a PCAP replay in real time is live).
    fn pacing(&self) -> Pacing {
        Pacing::Pausable
    }

    /// Processes data until the inputs end, the outputs close, or the node
    /// is stopped. Returning `Ok` moves the node to `STOPPED`; an error (or a
    /// panic) moves it to `ERROR` with the message.
    fn run(self: Box<Self>, ctx: NodeContext) -> Result<(), String>;
}

/// Builds a node's runner from its configuration. Fails with a message for
/// the user, e.g. when a file can't be opened.
pub type BuildRunner = fn(&Node) -> Result<Box<dyn NodeRunner>, String>;

/// The node types that can run, by `type_id`, and the device lists for
/// device properties, by device class.
#[derive(Clone, Default)]
pub struct NodeRegistry {
    builders: HashMap<String, BuildRunner>,
    devices: HashMap<String, ListDevices>,
}

impl NodeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, type_id: impl Into<String>, build: BuildRunner) -> &mut Self {
        self.builders.insert(type_id.into(), build);
        self
    }

    /// Registers how to list the devices of `class` (e.g.
    /// `network_interface`) for [`PropertyType::Device`] properties.
    ///
    /// [`PropertyType::Device`]: crate::PropertyType::Device
    pub fn register_devices(&mut self, class: impl Into<String>, list: ListDevices) -> &mut Self {
        self.devices.insert(class.into(), list);
        self
    }

    /// The devices of `class` available right now. Empty for a class nothing
    /// registered.
    pub fn device_options(&self, class: &str) -> Result<Vec<DeviceOption>, String> {
        match self.devices.get(class) {
            Some(list) => list(),
            None => Ok(Vec::new()),
        }
    }

    pub fn contains(&self, type_id: &str) -> bool {
        self.builders.contains_key(type_id)
    }

    pub fn build(&self, node: &Node) -> Result<Box<dyn NodeRunner>, String> {
        let build = self
            .builders
            .get(&node.type_id)
            .ok_or_else(|| format!("{} nodes can't run yet.", node.type_id))?;
        build(node)
    }
}

impl fmt::Debug for NodeRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut types: Vec<_> = self.builders.keys().collect();
        types.sort();
        let mut classes: Vec<_> = self.devices.keys().collect();
        classes.sort();
        f.debug_struct("NodeRegistry")
            .field("types", &types)
            .field("device_classes", &classes)
            .finish()
    }
}

/// Stops a running node. Cloning shares the token.
///
/// Cancelling wakes every edge the node is attached to, so an [`Input`] or
/// [`Output`] that is waiting returns at once.
#[derive(Clone, Default)]
pub struct CancelToken(Arc<CancelInner>);

#[derive(Default)]
struct CancelInner {
    cancelled: AtomicBool,
    edges: Mutex<Vec<Edge>>,
}

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::SeqCst);
        let edges = self.0.edges.lock().unwrap_or_else(|e| e.into_inner());
        for edge in edges.iter() {
            edge.wake();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::SeqCst)
    }

    /// Wakes `edge` on cancellation.
    fn watch(&self, edge: Edge) {
        self.0
            .edges
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(edge);
    }
}

impl fmt::Debug for CancelToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CancelToken")
            .field(&self.is_cancelled())
            .finish()
    }
}

/// The edges a node's ports are connected to, gathered from the workflow
/// when the node starts (see [`WorkflowState::wiring`]).
///
/// [`WorkflowState::wiring`]: crate::WorkflowState::wiring
#[derive(Debug, Clone, Default)]
pub struct Wiring {
    /// Each input port and its edge, if connected.
    pub inputs: Vec<(String, Option<Edge>)>,
    /// Each output port and its edges (several if it fans out).
    pub outputs: Vec<(String, Vec<Edge>)>,
    /// Where the node can publish its latest item for the UI.
    pub preview: PreviewSlot,
}

/// An input port of a running node.
pub struct Input {
    receiver: Option<Receiver>,
    cancel: CancelToken,
}

impl Input {
    /// Waits for the next item. Returns `None` once the stream has ended
    /// (the edge closed and drained) or the node is stopped.
    pub fn recv(&self) -> Option<StreamItem> {
        let receiver = self.receiver.as_ref()?;
        receiver.recv_blocking_until(|| self.cancel.is_cancelled())
    }

    pub fn is_connected(&self) -> bool {
        self.receiver.is_some()
    }
}

/// Why [`Output::send`] did not deliver an item: every edge is closed, or
/// the node is stopping. Either way the runner should finish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputClosed;

impl fmt::Display for OutputClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("output closed")
    }
}

impl std::error::Error for OutputClosed {}

/// An output port of a running node, feeding every connection from it.
pub struct Output {
    edges: FanOut,
    pacing: Pacing,
    cancel: CancelToken,
}

impl Output {
    /// Sends an item to every connection, following the node's [`Pacing`]:
    /// a pausable node waits for credit on full `Block` edges, a live node
    /// drops (and counts) instead. With no connections the item is
    /// discarded; there is no edge to count it on.
    pub fn send(&self, item: StreamItem) -> Result<(), OutputClosed> {
        if self.cancel.is_cancelled() {
            return Err(OutputClosed);
        }
        if self.edges.is_empty() {
            return Ok(());
        }
        let sent = match self.pacing {
            Pacing::Pausable => self
                .edges
                .send_blocking_until(item, || self.cancel.is_cancelled()),
            Pacing::Live => self.edges.send_live(item),
        };
        sent.map_err(|_| OutputClosed)
    }

    pub fn is_connected(&self) -> bool {
        !self.edges.is_empty()
    }
}

/// What a running node works with: its ports and its stop signal.
pub struct NodeContext {
    node: NodeId,
    inputs: BTreeMap<String, Input>,
    outputs: BTreeMap<String, Output>,
    cancel: CancelToken,
    preview: PreviewSlot,
}

impl NodeContext {
    /// Attaches to the wired edges. Each edge is reopened if a previous run
    /// closed it (see [`Edge::attach_sender`]).
    /// The node's preview starts empty for the new run.
    pub fn new(node: NodeId, wiring: Wiring, pacing: Pacing, cancel: CancelToken) -> Self {
        wiring.preview.clear();
        let inputs = wiring
            .inputs
            .into_iter()
            .map(|(port, edge)| {
                let receiver = edge.map(|edge| {
                    cancel.watch(edge.clone());
                    edge.attach_receiver()
                });
                let cancel = cancel.clone();
                (port, Input { receiver, cancel })
            })
            .collect();
        let outputs = wiring
            .outputs
            .into_iter()
            .map(|(port, edges)| {
                let senders = edges
                    .into_iter()
                    .map(|edge| {
                        cancel.watch(edge.clone());
                        edge.attach_sender()
                    })
                    .collect();
                let output = Output {
                    edges: FanOut::new(senders),
                    pacing,
                    cancel: cancel.clone(),
                };
                (port, output)
            })
            .collect();
        Self {
            node,
            inputs,
            outputs,
            cancel,
            preview: wiring.preview,
        }
    }

    /// Where this node publishes its latest item for the UI (see
    /// [`PreviewSlot::record`]).
    pub fn preview(&self) -> &PreviewSlot {
        &self.preview
    }

    pub fn node(&self) -> NodeId {
        self.node
    }

    /// Takes the input port named `port`.
    pub fn take_input(&mut self, port: &str) -> Result<Input, String> {
        self.inputs
            .remove(port)
            .ok_or_else(|| format!("no input port `{port}`"))
    }

    /// Takes the output port named `port`.
    pub fn take_output(&mut self, port: &str) -> Result<Output, String> {
        self.outputs
            .remove(port)
            .ok_or_else(|| format!("no output port `{port}`"))
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    pub fn cancel_token(&self) -> &CancelToken {
        &self.cancel
    }
}

/// A node ready to run: checked, built, and attached to its edges.
pub struct PreparedRun {
    runner: Box<dyn NodeRunner>,
    ctx: NodeContext,
    pacing: Pacing,
}

/// Startup work for a node: [`preflight`], wiring checks, and building its
/// runner. Returns a message for the user on failure.
///
/// Every input must be connected, and a source needs at least one connected
/// output. Edges are attached only once everything else has succeeded.
pub fn prepare(
    registry: &NodeRegistry,
    node: &Node,
    wiring: Wiring,
) -> Result<PreparedRun, String> {
    preflight(node)?;
    let unconnected: Vec<_> = wiring
        .inputs
        .iter()
        .filter(|(_, edge)| edge.is_none())
        .map(|(port, _)| port.as_str())
        .collect();
    if !unconnected.is_empty() {
        return Err(format!(
            "Connect {} before starting.",
            unconnected.join(", ")
        ));
    }
    if node.kind == NodeKind::Source && wiring.outputs.iter().all(|(_, edges)| edges.is_empty()) {
        return Err("Connect an output before starting.".into());
    }

    let runner = registry.build(node)?;
    let pacing = runner.pacing();
    let ctx = NodeContext::new(node.id, wiring, pacing, CancelToken::new());
    Ok(PreparedRun {
        runner,
        ctx,
        pacing,
    })
}

impl PreparedRun {
    /// Stops the run when cancelled. Hand it to the lifecycle before calling
    /// [`run`](Self::run).
    pub fn cancel_token(&self) -> CancelToken {
        self.ctx.cancel.clone()
    }

    pub fn pacing(&self) -> Pacing {
        self.pacing
    }

    /// Runs the node on this thread until it finishes or is cancelled. A
    /// panic in the runner becomes an error.
    pub fn run(self) -> Result<(), String> {
        let Self { runner, ctx, .. } = self;
        catch_unwind(AssertUnwindSafe(|| runner.run(ctx)))
            .unwrap_or_else(|panic| Err(format!("Node crashed: {}", panic_message(&*panic))))
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> &str {
    if let Some(s) = panic.downcast_ref::<&str>() {
        s
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s
    } else {
        "unknown panic"
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use super::*;
    use crate::catalog::builtin_nodes;
    use crate::state::WorkflowState;
    use crate::workflow::{PortRef, Workflow};

    fn item(sequence: u64) -> StreamItem {
        StreamItem::new(sequence, sequence)
    }

    /// Reads its input until the stream ends or it is stopped.
    struct Drain;

    impl NodeRunner for Drain {
        fn run(self: Box<Self>, mut ctx: NodeContext) -> Result<(), String> {
            let input = ctx.take_input("packets")?;
            while input.recv().is_some() {}
            Ok(())
        }
    }

    /// Sends forever, until its output closes or it is stopped.
    struct Flood(Pacing);

    impl NodeRunner for Flood {
        fn pacing(&self) -> Pacing {
            self.0
        }

        fn run(self: Box<Self>, mut ctx: NodeContext) -> Result<(), String> {
            let output = ctx.take_output("packets")?;
            for i in 0.. {
                if output.send(item(i)).is_err() {
                    break;
                }
            }
            Ok(())
        }
    }

    struct Crash;

    impl NodeRunner for Crash {
        fn run(self: Box<Self>, _: NodeContext) -> Result<(), String> {
            panic!("boom")
        }
    }

    fn registry() -> NodeRegistry {
        let mut registry = NodeRegistry::new();
        registry
            .register("net.pcap_source", |_| Ok(Box::new(Flood(Pacing::Pausable))))
            .register("net.dns_parser", |_| Ok(Box::new(Drain)));
        registry
    }

    /// pcap source → DNS parser over a 4-slot `Block` edge. The pcap source
    /// needs an interface to pass preflight.
    fn pipeline() -> (Workflow, WorkflowState, NodeId, NodeId) {
        let nodes = builtin_nodes();
        let mut wf = Workflow::new("test");
        let source = wf.add_node(wf.root(), "Capture", &nodes[0]).unwrap();
        let parser = wf.add_node(wf.root(), "Parse", &nodes[1]).unwrap();
        wf.set_property(source, "interface", Some("en0".into()))
            .unwrap();
        wf.connect(
            PortRef::new(source, "packets"),
            PortRef::new(parser, "packets"),
            4,
        )
        .unwrap();
        let state = WorkflowState::new(&wf);
        (wf, state, source, parser)
    }

    fn prepared(
        registry: &NodeRegistry,
        wf: &Workflow,
        state: &WorkflowState,
        id: NodeId,
    ) -> PreparedRun {
        let wiring = state.wiring(wf, id).unwrap();
        prepare(registry, wf.node(id).unwrap(), wiring).unwrap()
    }

    #[test]
    fn source_feeds_sink_until_stopped() {
        let (wf, state, source, parser) = pipeline();
        let registry = registry();
        let producer = prepared(&registry, &wf, &state, source);
        let consumer = prepared(&registry, &wf, &state, parser);
        let stop = producer.cancel_token();
        let producer = thread::spawn(move || producer.run());
        let consumer = thread::spawn(move || consumer.run());

        thread::sleep(Duration::from_millis(20));
        assert!(!producer.is_finished());
        // Stopping the source ends its stream; the sink drains and finishes.
        stop.cancel();
        assert_eq!(producer.join().unwrap(), Ok(()));
        assert_eq!(consumer.join().unwrap(), Ok(()));
        let edge = state.edge_stats().next().unwrap().1;
        assert_eq!(edge.dropped, 0);
    }

    #[test]
    fn cancel_wakes_a_blocked_producer() {
        // No consumer: the pausable source fills the edge and waits.
        let (wf, state, source, _) = pipeline();
        let run = prepared(&registry(), &wf, &state, source);
        let stop = run.cancel_token();
        let producer = thread::spawn(move || run.run());
        thread::sleep(Duration::from_millis(20));
        assert!(!producer.is_finished());
        assert_eq!(state.edge_stats().next().unwrap().1.queue_depth, 4);

        stop.cancel();
        assert_eq!(producer.join().unwrap(), Ok(()));
    }

    #[test]
    fn cancel_wakes_a_blocked_consumer() {
        // No producer: the sink waits for data that never comes.
        let (wf, state, _, parser) = pipeline();
        let run = prepared(&registry(), &wf, &state, parser);
        let stop = run.cancel_token();
        let consumer = thread::spawn(move || run.run());
        thread::sleep(Duration::from_millis(20));
        assert!(!consumer.is_finished());

        stop.cancel();
        assert_eq!(consumer.join().unwrap(), Ok(()));
    }

    #[test]
    fn live_source_drops_instead_of_waiting() {
        let (wf, state, source, _) = pipeline();
        let mut registry = registry();
        registry.register("net.pcap_source", |_| Ok(Box::new(Flood(Pacing::Live))));
        let run = prepared(&registry, &wf, &state, source);
        assert_eq!(run.pacing(), Pacing::Live);
        let stop = run.cancel_token();
        let producer = thread::spawn(move || run.run());
        thread::sleep(Duration::from_millis(20));
        stop.cancel();
        assert_eq!(producer.join().unwrap(), Ok(()));

        let stats = state.edge_stats().next().unwrap().1;
        assert_eq!(stats.queue_depth, 4);
        assert!(stats.dropped > 0);
    }

    #[test]
    fn panics_become_errors() {
        let (wf, state, _, parser) = pipeline();
        let mut registry = registry();
        registry.register("net.dns_parser", |_| Ok(Box::new(Crash)));
        let run = prepared(&registry, &wf, &state, parser);
        assert_eq!(run.run(), Err("Node crashed: boom".into()));
    }

    #[test]
    fn prepare_checks_wiring_and_registry() {
        let (mut wf, state, source, parser) = pipeline();
        let registry = registry();
        let start = |wf: &Workflow, state: &WorkflowState, id| {
            prepare(
                &registry,
                wf.node(id).unwrap(),
                state.wiring(wf, id).unwrap(),
            )
            .map(drop)
        };

        // Preflight runs first.
        wf.set_property(source, "interface", None).unwrap();
        assert_eq!(
            start(&wf, &state, source),
            Err("Set interface before starting.".into())
        );

        let conn = wf.connections().next().unwrap().id;
        wf.disconnect(conn).unwrap();
        wf.set_property(source, "interface", Some("en0".into()))
            .unwrap();
        let state = WorkflowState::new(&wf);
        assert_eq!(
            start(&wf, &state, parser),
            Err("Connect packets before starting.".into())
        );
        assert_eq!(
            start(&wf, &state, source),
            Err("Connect an output before starting.".into())
        );

        let csv = builtin_nodes()
            .into_iter()
            .find(|d| d.type_id == "file.csv_source")
            .unwrap();
        let other = wf.add_node(wf.root(), "CSV", &csv).unwrap();
        assert_eq!(
            NodeRegistry::new().build(wf.node(other).unwrap()).map(drop),
            Err("file.csv_source nodes can't run yet.".into())
        );
    }

    #[test]
    fn restarted_nodes_reopen_their_edges() {
        let (wf, state, source, parser) = pipeline();
        let registry = registry();
        for _ in 0..2 {
            let producer = prepared(&registry, &wf, &state, source);
            let consumer = prepared(&registry, &wf, &state, parser);
            let stop = producer.cancel_token();
            let consumer = thread::spawn(move || consumer.run());
            let producer = thread::spawn(move || producer.run());
            thread::sleep(Duration::from_millis(10));
            stop.cancel();
            assert_eq!(producer.join().unwrap(), Ok(()));
            assert_eq!(consumer.join().unwrap(), Ok(()));
        }
    }
}
