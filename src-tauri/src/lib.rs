use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use parkourflow_core::{
    CancelToken, ConnectionId, DeviceOption, EdgeStats, GroupId, Node, NodeDescriptor, NodeId,
    NodeRegistry, NodeState, OverflowPolicy, PortRef, PreviewSnapshot, PropertyValue, Wiring,
    Workflow, WorkflowError, WorkflowState,
};
use serde::Serialize;
use tauri::{AppHandle, Emitter, Manager, State};

/// Event sent to the UI whenever a node's status changes.
const NODE_STATUS_EVENT: &str = "node-status";

/// Event carrying every connection's [`EdgeStats`], sent when they change.
const CONNECTION_STATS_EVENT: &str = "connection-stats";

/// Event carrying the previews that changed since the last sample, keyed by
/// node id.
const NODE_PREVIEWS_EVENT: &str = "node-previews";

/// How often edge telemetry and previews are sampled.
const STATS_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Serialize)]
struct NodeStatusEvent {
    id: NodeId,
    state: NodeState,
}

/// The open workflow and its runtime state. There is one concurrent user, so
/// a single lock around both is enough.
struct Session {
    workflow: Workflow,
    state: WorkflowState,
}

struct AppState {
    session: Mutex<Session>,
    /// The node types that can run.
    registry: NodeRegistry,
}

impl AppState {
    fn new(workflow: Workflow, registry: NodeRegistry) -> Self {
        let state = WorkflowState::new(&workflow);
        Self {
            session: Mutex::new(Session { workflow, state }),
            registry,
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Session>, String> {
        self.session.lock().map_err(|e| e.to_string())
    }

    fn read<T>(&self, f: impl FnOnce(&Session) -> T) -> Result<T, String> {
        Ok(f(&*self.lock()?))
    }

    /// Applies an edit to the workflow, then brings the runtime state in line.
    fn edit<T>(
        &self,
        f: impl FnOnce(&mut Workflow) -> Result<T, WorkflowError>,
    ) -> Result<T, String> {
        let mut session = self.lock()?;
        let Session { workflow, state } = &mut *session;
        let result = f(workflow).map_err(|e| e.to_string())?;
        state.reconcile(workflow);
        Ok(result)
    }

    /// Moves a node to `STARTING`. Returns a copy of its configuration and
    /// the edges it is wired to, for the startup work, which runs without
    /// holding the lock.
    fn begin_start(&self, id: NodeId) -> Result<(Node, Wiring, NodeStatusEvent), String> {
        let mut session = self.lock()?;
        let Session { workflow, state } = &mut *session;
        let node = workflow
            .node(id)
            .ok_or_else(|| WorkflowError::NodeNotFound(id).to_string())?
            .clone();
        let wiring = state.wiring(workflow, id).map_err(|e| e.to_string())?;
        let state = state.begin_start(id).map_err(|e| e.to_string())?.clone();
        Ok((node, wiring, NodeStatusEvent { id, state }))
    }

    /// Records the outcome of a start. `None` if the node was removed or
    /// stopped being `STARTING` in the meantime; then it must not run.
    fn finish_start(
        &self,
        id: NodeId,
        result: Result<CancelToken, String>,
    ) -> Option<NodeStatusEvent> {
        let mut session = self.session.lock().ok()?;
        let state = session.state.finish_start(id, result)?.clone();
        Some(NodeStatusEvent { id, state })
    }

    /// Moves a running node to `STOPPING` and cancels its runner.
    fn begin_stop(&self, id: NodeId) -> Result<NodeStatusEvent, String> {
        let mut session = self.lock()?;
        let state = session
            .state
            .begin_stop(id)
            .map_err(|e| e.to_string())?
            .clone();
        Ok(NodeStatusEvent { id, state })
    }

    /// Records that a node's runner returned.
    fn finish_run(&self, id: NodeId, result: Result<(), String>) -> Option<NodeStatusEvent> {
        let mut session = self.session.lock().ok()?;
        let state = session.state.finish_run(id, result)?.clone();
        Some(NodeStatusEvent { id, state })
    }
}

/// Where the lifecycle finds the session and sends status events: the Tauri
/// app, or a recorder in tests.
trait Host: Clone + Send + 'static {
    fn app_state(&self) -> &AppState;
    fn emit_status(&self, event: NodeStatusEvent);
}

impl Host for AppHandle {
    fn app_state(&self) -> &AppState {
        self.state::<AppState>().inner()
    }

    fn emit_status(&self, event: NodeStatusEvent) {
        if let Err(e) = self.emit(NODE_STATUS_EVENT, event) {
            eprintln!("failed to emit {NODE_STATUS_EVENT}: {e}");
        }
    }
}

/// Starts a node: `STARTING` now, then on the node's own thread: preflight
/// and build its runner (`RUNNING` or `ERROR`), run it, and record how it
/// ended (`STOPPED` or `ERROR`). Every transition is emitted.
fn start<H: Host>(host: H, id: NodeId) -> Result<(), String> {
    let (node, wiring, event) = host.app_state().begin_start(id)?;
    host.emit_status(event);

    std::thread::Builder::new()
        .name(format!("node-{id}"))
        .spawn(move || {
            let app = host.app_state();
            let prepared = parkourflow_core::prepare(&app.registry, &node, wiring);
            let (result, run) = match prepared {
                Ok(run) => (Ok(run.cancel_token()), Some(run)),
                Err(message) => (Err(message), None),
            };
            let Some(event) = app.finish_start(id, result) else {
                return;
            };
            host.emit_status(event);
            if let Some(run) = run {
                let result = run.run();
                if let Some(event) = app.finish_run(id, result) {
                    host.emit_status(event);
                }
            }
        })
        .map_err(|e| format!("can't start a thread for the node: {e}"))?;
    Ok(())
}

/// Stops a node: `STOPPING` now; `STOPPED` once its runner has returned.
fn stop<H: Host>(host: &H, id: NodeId) -> Result<(), String> {
    let event = host.app_state().begin_stop(id)?;
    host.emit_status(event);
    Ok(())
}

/// Samples telemetry: emits `connection-stats` whenever any edge's queue
/// depth, capacity, or drop count changed, and `node-previews` with the
/// previews that changed.
fn publish_telemetry(app: AppHandle) {
    let mut last_stats = BTreeMap::new();
    let mut last_previews = BTreeMap::new();
    loop {
        std::thread::sleep(STATS_INTERVAL);
        let Ok((stats, previews)) = app.state::<AppState>().read(|s| {
            (
                s.state.edge_stats().collect::<BTreeMap<_, _>>(),
                s.state.previews().collect::<BTreeMap<_, _>>(),
            )
        }) else {
            continue;
        };
        if stats != last_stats {
            if let Err(e) = app.emit(CONNECTION_STATS_EVENT, &stats) {
                eprintln!("failed to emit {CONNECTION_STATS_EVENT}: {e}");
            }
            last_stats = stats;
        }
        let changed = changed_previews(&last_previews, &previews);
        if !changed.is_empty() {
            if let Err(e) = app.emit(NODE_PREVIEWS_EVENT, &changed) {
                eprintln!("failed to emit {NODE_PREVIEWS_EVENT}: {e}");
            }
        }
        last_previews = previews;
    }
}

/// The previews in `now` that are new or differ from `before`.
fn changed_previews<'a>(
    before: &BTreeMap<NodeId, PreviewSnapshot>,
    now: &'a BTreeMap<NodeId, PreviewSnapshot>,
) -> BTreeMap<NodeId, &'a PreviewSnapshot> {
    now.iter()
        .filter(|(id, snapshot)| before.get(id) != Some(snapshot))
        .map(|(id, snapshot)| (*id, snapshot))
        .collect()
}

/// The latest preview of every node that has one, keyed by node id.
#[tauri::command]
fn node_previews(app: State<AppState>) -> Result<BTreeMap<NodeId, PreviewSnapshot>, String> {
    app.read(|s| s.state.previews().collect())
}

/// Control plane: returns the node types the runtime provides.
#[tauri::command]
fn list_node_types() -> Vec<NodeDescriptor> {
    parkourflow_core::builtin_nodes()
}

/// Control plane: returns the runtime version.
#[tauri::command]
fn runtime_version() -> &'static str {
    parkourflow_core::version()
}

#[tauri::command]
fn get_workflow(app: State<AppState>) -> Result<Workflow, String> {
    app.read(|s| s.workflow.clone())
}

/// Returns the runtime state of every node, keyed by node id.
#[tauri::command]
fn node_statuses(app: State<AppState>) -> Result<BTreeMap<NodeId, NodeState>, String> {
    app.read(|s| {
        s.workflow
            .nodes()
            .filter_map(|n| Some((n.id, s.state.node(n.id)?.clone())))
            .collect()
    })
}

/// Returns queue depth, capacity, and drop count of every connection, keyed
/// by connection id.
#[tauri::command]
fn connection_stats(app: State<AppState>) -> Result<BTreeMap<ConnectionId, EdgeStats>, String> {
    app.read(|s| s.state.edge_stats().collect())
}

/// Adds a node of type `type_id` to `group` (the root group if `None`).
#[tauri::command]
fn add_node(
    app: State<AppState>,
    group: Option<GroupId>,
    name: String,
    type_id: String,
) -> Result<NodeId, String> {
    let descriptor = parkourflow_core::builtin_nodes()
        .into_iter()
        .find(|d| d.type_id == type_id)
        .ok_or_else(|| format!("unknown node type `{type_id}`"))?;
    app.edit(|wf| wf.add_node(group.unwrap_or(wf.root()), name, &descriptor))
}

#[tauri::command]
fn remove_node(app: State<AppState>, id: NodeId) -> Result<(), String> {
    app.edit(|wf| wf.remove_node(id).map(drop))
}

#[tauri::command]
fn move_node(app: State<AppState>, id: NodeId, group: GroupId) -> Result<(), String> {
    app.edit(|wf| wf.move_node(id, group))
}

#[tauri::command]
fn add_group(app: State<AppState>, parent: GroupId, name: String) -> Result<GroupId, String> {
    app.edit(|wf| wf.add_group(parent, name))
}

#[tauri::command]
fn remove_group(app: State<AppState>, id: GroupId) -> Result<(), String> {
    app.edit(|wf| wf.remove_group(id))
}

/// Connects an output port to an input port. `buffer_capacity` defaults to
/// [`parkourflow_core::DEFAULT_BUFFER_CAPACITY`] and `overflow_policy` to
/// [`OverflowPolicy::Block`].
#[tauri::command]
fn connect(
    app: State<AppState>,
    from: PortRef,
    to: PortRef,
    buffer_capacity: Option<usize>,
    overflow_policy: Option<OverflowPolicy>,
) -> Result<ConnectionId, String> {
    let capacity = buffer_capacity.unwrap_or(parkourflow_core::DEFAULT_BUFFER_CAPACITY);
    app.edit(|wf| {
        let id = wf.connect(from, to, capacity)?;
        if let Some(policy) = overflow_policy {
            wf.set_overflow_policy(id, policy)?;
        }
        Ok(id)
    })
}

#[tauri::command]
fn disconnect(app: State<AppState>, id: ConnectionId) -> Result<(), String> {
    app.edit(|wf| wf.disconnect(id).map(drop))
}

#[tauri::command]
fn set_buffer_capacity(
    app: State<AppState>,
    id: ConnectionId,
    capacity: usize,
) -> Result<(), String> {
    app.edit(|wf| wf.set_buffer_capacity(id, capacity))
}

#[tauri::command]
fn set_overflow_policy(
    app: State<AppState>,
    id: ConnectionId,
    policy: OverflowPolicy,
) -> Result<(), String> {
    app.edit(|wf| wf.set_overflow_policy(id, policy))
}

/// Starts a `STOPPED` or `ERROR` node. Returns once the node is `STARTING`;
/// the outcome (`RUNNING` or `ERROR`, and later `STOPPED` or `ERROR` when it
/// finishes) arrives as `node-status` events.
#[tauri::command]
fn start_node(app: AppHandle, id: NodeId) -> Result<(), String> {
    start(app, id)
}

/// Stops a `RUNNING` node. Returns once it is `STOPPING`; `STOPPED` arrives
/// as a `node-status` event when its runner has finished.
#[tauri::command]
fn stop_node(app: AppHandle, id: NodeId) -> Result<(), String> {
    stop(&app, id)
}

/// Sets a node property. `value: null` resets it to its default, or clears
/// it if it has none.
#[tauri::command]
fn set_property(
    app: State<AppState>,
    id: NodeId,
    name: String,
    value: Option<PropertyValue>,
) -> Result<(), String> {
    app.edit(|wf| wf.set_property(id, &name, value))
}

/// Lists the devices currently available for a device property's class.
#[tauri::command]
fn list_device_options(app: State<AppState>, class: String) -> Result<Vec<DeviceOption>, String> {
    app.registry.device_options(&class)
}

/// The node types this build can run.
fn registry() -> NodeRegistry {
    let mut registry = NodeRegistry::new();
    parkourflow_debug::register(&mut registry);
    parkourflow_files::register(&mut registry);
    parkourflow_net::register(&mut registry);
    registry
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(AppState::new(Workflow::new("Untitled"), registry()))
        .setup(|app| {
            let handle = app.handle().clone();
            std::thread::Builder::new()
                .name("telemetry".into())
                .spawn(move || publish_telemetry(handle))?;
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            list_node_types,
            runtime_version,
            get_workflow,
            node_statuses,
            node_previews,
            connection_stats,
            add_node,
            remove_node,
            move_node,
            add_group,
            remove_group,
            connect,
            disconnect,
            set_buffer_capacity,
            set_overflow_policy,
            set_property,
            list_device_options,
            start_node,
            stop_node,
        ])
        .run(tauri::generate_context!())
        .expect("error while running ParkourFlow");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edits_reconcile_state() {
        let app = AppState::new(Workflow::new("test"), registry());
        let pcap = &parkourflow_core::builtin_nodes()[0];
        let id = app
            .edit(|wf| wf.add_node(wf.root(), "Capture", pcap))
            .unwrap();
        assert!(app.read(|s| s.state.node(id).is_some()).unwrap());

        app.edit(|wf| wf.remove_node(id).map(drop)).unwrap();
        assert!(app.read(|s| s.state.node(id).is_none()).unwrap());
    }

    #[test]
    fn policy_edits_reach_the_edge() {
        let app = AppState::new(Workflow::new("test"), registry());
        let nodes = parkourflow_core::builtin_nodes();
        let conn = app
            .edit(|wf| {
                let source = wf.add_node(wf.root(), "Capture", &nodes[0])?;
                let parser = wf.add_node(wf.root(), "Parse", &nodes[1])?;
                wf.connect(
                    PortRef::new(source, "packets"),
                    PortRef::new(parser, "packets"),
                    8,
                )
            })
            .unwrap();
        app.edit(|wf| wf.set_overflow_policy(conn, OverflowPolicy::KeepLatest))
            .unwrap();
        let stats = app
            .read(|s| s.state.edge_stats().collect::<BTreeMap<_, _>>())
            .unwrap();
        assert_eq!(stats[&conn].capacity, 1);
    }

    #[test]
    fn property_edits_are_validated() {
        let app = AppState::new(Workflow::new("test"), registry());
        let pcap = &parkourflow_core::builtin_nodes()[0];
        let id = app
            .edit(|wf| wf.add_node(wf.root(), "Capture", pcap))
            .unwrap();
        app.edit(|wf| wf.set_property(id, "snaplen", Some(1500.into())))
            .unwrap();
        let err = app
            .edit(|wf| wf.set_property(id, "snaplen", Some(1.into())))
            .unwrap_err();
        assert_eq!(err, "`snaplen` must be between 64 and 262144");
    }

    use std::sync::{Arc, Condvar};
    use std::time::Instant;

    use parkourflow_core::{NodeContext, NodeRunner, NodeStatus};

    /// Runs the lifecycle without Tauri and records the emitted events.
    #[derive(Clone)]
    struct TestHost(Arc<(AppState, Mutex<Vec<NodeStatusEvent>>, Condvar)>);

    impl Host for TestHost {
        fn app_state(&self) -> &AppState {
            &self.0 .0
        }

        fn emit_status(&self, event: NodeStatusEvent) {
            self.0 .1.lock().unwrap().push(event);
            self.0 .2.notify_all();
        }
    }

    impl TestHost {
        fn new(registry: NodeRegistry) -> Self {
            let app = AppState::new(Workflow::new("test"), registry);
            Self(Arc::new((app, Mutex::default(), Condvar::new())))
        }

        /// The statuses emitted for `id` so far.
        fn statuses(&self, id: NodeId) -> Vec<NodeStatus> {
            let events = self.0 .1.lock().unwrap();
            events
                .iter()
                .filter(|e| e.id == id)
                .map(|e| e.state.status)
                .collect()
        }

        /// Waits until `id` emits `status`, and returns its state then.
        fn wait_for(&self, id: NodeId, status: NodeStatus) -> NodeState {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut events = self.0 .1.lock().unwrap();
            loop {
                if let Some(e) = events
                    .iter()
                    .find(|e| e.id == id && e.state.status == status)
                {
                    return e.state.clone();
                }
                let left = deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or_else(|| panic!("node {id} never reached {status}"));
                events = self.0 .2.wait_timeout(events, left).unwrap().0;
            }
        }
    }

    /// Reads its input until the stream ends or the node stops.
    struct Drain;

    impl NodeRunner for Drain {
        fn run(self: Box<Self>, mut ctx: NodeContext) -> Result<(), String> {
            let input = ctx.take_input("packets")?;
            while input.recv().is_some() {}
            Ok(())
        }
    }

    /// Sends until the output closes or the node stops.
    struct Flood;

    impl NodeRunner for Flood {
        fn run(self: Box<Self>, mut ctx: NodeContext) -> Result<(), String> {
            let output = ctx.take_output("packets")?;
            let mut i = 0;
            while output.send(parkourflow_core::StreamItem::new(i, i)).is_ok() {
                i += 1;
            }
            Ok(())
        }
    }

    struct Fail;

    impl NodeRunner for Fail {
        fn run(self: Box<Self>, _: NodeContext) -> Result<(), String> {
            Err("bad data".into())
        }
    }

    fn test_registry() -> NodeRegistry {
        let mut registry = NodeRegistry::new();
        registry
            .register("net.pcap_source", |_| Ok(Box::new(Flood)))
            .register("net.dns_parser", |_| Ok(Box::new(Drain)));
        registry
    }

    /// pcap source → DNS parser, ready to start.
    fn pipeline(host: &TestHost) -> (NodeId, NodeId) {
        let nodes = parkourflow_core::builtin_nodes();
        host.app_state()
            .edit(|wf| {
                let source = wf.add_node(wf.root(), "Capture", &nodes[0])?;
                let parser = wf.add_node(wf.root(), "Parse", &nodes[1])?;
                wf.set_property(source, "interface", Some("en0".into()))?;
                wf.connect(
                    PortRef::new(source, "packets"),
                    PortRef::new(parser, "packets"),
                    4,
                )?;
                Ok((source, parser))
            })
            .unwrap()
    }

    #[test]
    fn start_and_stop_with_cancellation() {
        use NodeStatus::*;

        let host = TestHost::new(test_registry());
        let (_, parser) = pipeline(&host);

        // The parser waits for packets that never come, until stopped.
        start(host.clone(), parser).unwrap();
        host.wait_for(parser, Running);
        assert!(start(host.clone(), parser)
            .unwrap_err()
            .ends_with("while it is RUNNING"));
        stop(&host, parser).unwrap();
        host.wait_for(parser, Stopped);
        assert_eq!(
            host.statuses(parser),
            [Starting, Running, Stopping, Stopped]
        );
        assert!(stop(&host, parser).is_err());

        // It can run again.
        host.0 .1.lock().unwrap().clear();
        start(host.clone(), parser).unwrap();
        host.wait_for(parser, Running);
        stop(&host, parser).unwrap();
        assert_eq!(host.wait_for(parser, Stopped).error, None);
    }

    #[test]
    fn stopping_a_source_ends_its_stream() {
        use NodeStatus::*;

        let host = TestHost::new(test_registry());
        let (source, parser) = pipeline(&host);
        start(host.clone(), parser).unwrap();
        start(host.clone(), source).unwrap();
        host.wait_for(source, Running);
        host.wait_for(parser, Running);

        // Only the source is stopped; the parser finishes on its own once the
        // stream ends.
        stop(&host, source).unwrap();
        host.wait_for(source, Stopped);
        host.wait_for(parser, Stopped);
        assert_eq!(host.statuses(parser), [Starting, Running, Stopped]);
        let stats = host
            .app_state()
            .read(|s| s.state.edge_stats().next().unwrap().1)
            .unwrap();
        assert_eq!(stats.dropped, 0);
    }

    #[test]
    fn failures_move_the_node_to_error() {
        use NodeStatus::*;

        let mut registry = test_registry();
        registry.register("net.dns_parser", |_| Ok(Box::new(Fail)));
        let host = TestHost::new(registry);
        let (source, parser) = pipeline(&host);

        // The runner returns an error.
        start(host.clone(), parser).unwrap();
        let state = host.wait_for(parser, Error);
        assert_eq!(state.error.as_deref(), Some("bad data"));
        assert_eq!(host.statuses(parser), [Starting, Running, Error]);

        // Startup fails: preflight finds no interface, so it never runs.
        host.app_state()
            .edit(|wf| wf.set_property(source, "interface", None))
            .unwrap();
        start(host.clone(), source).unwrap();
        let state = host.wait_for(source, Error);
        assert_eq!(
            state.error.as_deref(),
            Some("Set interface before starting.")
        );
        assert_eq!(host.statuses(source), [Starting, Error]);
    }

    #[test]
    fn panics_move_the_node_to_error() {
        struct Crash;
        impl NodeRunner for Crash {
            fn run(self: Box<Self>, _: NodeContext) -> Result<(), String> {
                panic!("index out of bounds")
            }
        }

        let mut registry = test_registry();
        registry.register("net.dns_parser", |_| Ok(Box::new(Crash)));
        let host = TestHost::new(registry);
        let (_, parser) = pipeline(&host);
        start(host.clone(), parser).unwrap();
        let state = host.wait_for(parser, NodeStatus::Error);
        assert_eq!(
            state.error.as_deref(),
            Some("Node crashed: index out of bounds")
        );
    }

    #[test]
    fn copies_a_csv_file() {
        let dir = std::env::temp_dir().join(format!("parkourflow-app-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (input, output) = (dir.join("in.csv"), dir.join("out.csv"));
        std::fs::write(&input, "name,count\nalpha,1\n\"b, c\",2\n").unwrap();

        let host = TestHost::new(registry());
        let nodes = parkourflow_core::builtin_nodes();
        let find = |id: &str| nodes.iter().find(|d| d.type_id == id).unwrap();
        let path = |p: &std::path::Path| Some(p.to_str().unwrap().into());
        let (source, writer) = host
            .app_state()
            .edit(|wf| {
                let source = wf.add_node(wf.root(), "Read", find("file.csv_source"))?;
                let writer = wf.add_node(wf.root(), "Write", find("file.csv_writer"))?;
                wf.set_property(source, "path", path(&input))?;
                wf.set_property(writer, "path", path(&output))?;
                wf.connect(
                    PortRef::new(source, "records"),
                    PortRef::new(writer, "records"),
                    2,
                )?;
                Ok((source, writer))
            })
            .unwrap();

        start(host.clone(), writer).unwrap();
        start(host.clone(), source).unwrap();
        host.wait_for(source, NodeStatus::Stopped);
        host.wait_for(writer, NodeStatus::Stopped);
        assert_eq!(
            std::fs::read_to_string(&output).unwrap(),
            std::fs::read_to_string(&input).unwrap()
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn lists_network_interfaces() {
        // The app registry includes parkourflow-net's interface list, which
        // needs no capture privileges.
        let interfaces = registry().device_options("network_interface").unwrap();
        assert!(!interfaces.is_empty());
        assert_eq!(registry().device_options("camera"), Ok(vec![]));
    }

    #[test]
    fn only_changed_previews_are_sent() {
        use parkourflow_core::{Preview, PreviewSnapshot};

        let snapshot = |received| PreviewSnapshot {
            received,
            sequence: received - 1,
            latest: Preview::Unsupported,
        };
        let id = |n: u64| -> NodeId { serde_json::from_value(n.into()).unwrap() };
        let before = BTreeMap::from([(id(1), snapshot(5)), (id(2), snapshot(9))]);
        let now = BTreeMap::from([
            (id(1), snapshot(5)),
            (id(2), snapshot(12)),
            (id(3), snapshot(1)),
        ]);
        let changed = changed_previews(&before, &now);
        assert_eq!(
            changed.keys().copied().collect::<Vec<_>>(),
            vec![id(2), id(3)]
        );
        assert!(changed_previews(&now, &now).is_empty());
    }

    #[test]
    fn errors_become_strings() {
        let app = AppState::new(Workflow::new("test"), registry());
        let err = app.edit(|wf| wf.remove_group(wf.root())).unwrap_err();
        assert_eq!(err, "the root group cannot be removed");
    }
}
