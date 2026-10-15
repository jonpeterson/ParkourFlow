use std::collections::BTreeMap;
use std::sync::Mutex;

use parkourflow_core::{
    ConnectionId, DeviceOption, EdgeStats, GroupId, Node, NodeDescriptor, NodeId, NodeState,
    OverflowPolicy, PortRef, PropertyValue, Workflow, WorkflowError, WorkflowState,
};
use serde::Serialize;
use tauri::{AppHandle, Emitter, Manager, State};

/// Event sent to the UI whenever a node's status changes.
const NODE_STATUS_EVENT: &str = "node-status";

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

struct AppState(Mutex<Session>);

impl AppState {
    fn new(workflow: Workflow) -> Self {
        let state = WorkflowState::new(&workflow);
        Self(Mutex::new(Session { workflow, state }))
    }

    fn read<T>(&self, f: impl FnOnce(&Session) -> T) -> Result<T, String> {
        let session = self.0.lock().map_err(|e| e.to_string())?;
        Ok(f(&session))
    }

    /// Applies an edit to the workflow, then brings the runtime state in line.
    fn edit<T>(
        &self,
        f: impl FnOnce(&mut Workflow) -> Result<T, WorkflowError>,
    ) -> Result<T, String> {
        let mut session = self.0.lock().map_err(|e| e.to_string())?;
        let Session { workflow, state } = &mut *session;
        let result = f(workflow).map_err(|e| e.to_string())?;
        state.reconcile(workflow);
        Ok(result)
    }

    /// Moves a node to `STARTING`. Returns a copy of its configuration for
    /// the startup work, which runs without holding the lock.
    fn begin_start(&self, id: NodeId) -> Result<(Node, NodeStatusEvent), String> {
        let mut session = self.0.lock().map_err(|e| e.to_string())?;
        let Session { workflow, state } = &mut *session;
        let node = workflow
            .node(id)
            .ok_or_else(|| WorkflowError::NodeNotFound(id).to_string())?
            .clone();
        let state = state.begin_start(id).map_err(|e| e.to_string())?.clone();
        Ok((node, NodeStatusEvent { id, state }))
    }

    /// Records the outcome of a start. `None` if the node was removed or
    /// stopped being `STARTING` in the meantime.
    fn finish_start(&self, id: NodeId, result: Result<(), String>) -> Option<NodeStatusEvent> {
        let mut session = self.0.lock().ok()?;
        let state = session.state.finish_start(id, result)?.clone();
        Some(NodeStatusEvent { id, state })
    }

    fn stop(&self, id: NodeId) -> Result<NodeStatusEvent, String> {
        let mut session = self.0.lock().map_err(|e| e.to_string())?;
        let state = session.state.stop(id).map_err(|e| e.to_string())?.clone();
        Ok(NodeStatusEvent { id, state })
    }
}

fn emit_status(app: &AppHandle, event: NodeStatusEvent) {
    if let Err(e) = app.emit(NODE_STATUS_EVENT, event) {
        eprintln!("failed to emit {NODE_STATUS_EVENT}: {e}");
    }
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
/// the outcome (`RUNNING` or `ERROR`) arrives as a `node-status` event.
#[tauri::command]
fn start_node(app: AppHandle, id: NodeId) -> Result<(), String> {
    let (node, event) = app.state::<AppState>().begin_start(id)?;
    emit_status(&app, event);

    // Startup touches the filesystem (and later devices), so keep it off the
    // command thread.
    tauri::async_runtime::spawn_blocking(move || {
        let result = parkourflow_core::preflight(&node);
        if let Some(event) = app.state::<AppState>().finish_start(id, result) {
            emit_status(&app, event);
        }
    });
    Ok(())
}

/// Stops a `RUNNING` node. The new status also arrives as a `node-status`
/// event.
#[tauri::command]
fn stop_node(app: AppHandle, id: NodeId) -> Result<(), String> {
    let event = app.state::<AppState>().stop(id)?;
    emit_status(&app, event);
    Ok(())
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
fn list_device_options(class: String) -> Vec<DeviceOption> {
    parkourflow_core::device_options(&class)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(AppState::new(Workflow::new("Untitled")))
        .invoke_handler(tauri::generate_handler![
            list_node_types,
            runtime_version,
            get_workflow,
            node_statuses,
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
        let app = AppState::new(Workflow::new("test"));
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
        let app = AppState::new(Workflow::new("test"));
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
        let app = AppState::new(Workflow::new("test"));
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

    #[test]
    fn start_and_stop() {
        use parkourflow_core::NodeStatus;

        let app = AppState::new(Workflow::new("test"));
        let csv = parkourflow_core::builtin_nodes()
            .into_iter()
            .find(|d| d.type_id == "file.csv_writer")
            .unwrap();
        let id = app
            .edit(|wf| wf.add_node(wf.root(), "Write", &csv))
            .unwrap();

        // Missing `path`: the start fails with a message.
        let (node, event) = app.begin_start(id).unwrap();
        assert_eq!(event.state.status, NodeStatus::Starting);
        assert!(app
            .begin_start(id)
            .unwrap_err()
            .ends_with("while it is STARTING"));
        let event = app
            .finish_start(id, parkourflow_core::preflight(&node))
            .unwrap();
        assert_eq!(event.state.status, NodeStatus::Error);
        assert_eq!(
            event.state.error.as_deref(),
            Some("Set path before starting.")
        );

        // Retry with a valid path.
        let path = std::env::temp_dir().join("parkourflow-app-test.csv");
        app.edit(|wf| wf.set_property(id, "path", Some(path.to_str().unwrap().into())))
            .unwrap();
        let (node, _) = app.begin_start(id).unwrap();
        let event = app
            .finish_start(id, parkourflow_core::preflight(&node))
            .unwrap();
        assert_eq!(event.state.status, NodeStatus::Running);

        assert_eq!(app.stop(id).unwrap().state.status, NodeStatus::Stopped);
        assert!(app.stop(id).is_err());
    }

    #[test]
    fn errors_become_strings() {
        let app = AppState::new(Workflow::new("test"));
        let err = app.edit(|wf| wf.remove_group(wf.root())).unwrap_err();
        assert_eq!(err, "the root group cannot be removed");
    }
}
