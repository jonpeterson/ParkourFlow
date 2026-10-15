use std::collections::BTreeMap;
use std::sync::Mutex;

use parkourflow_core::{
    ConnectionId, GroupId, NodeDescriptor, NodeId, NodeState, PortRef, Workflow, WorkflowError,
    WorkflowState,
};
use tauri::State;

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
/// [`parkourflow_core::DEFAULT_BUFFER_CAPACITY`].
#[tauri::command]
fn connect(
    app: State<AppState>,
    from: PortRef,
    to: PortRef,
    buffer_capacity: Option<usize>,
) -> Result<ConnectionId, String> {
    let capacity = buffer_capacity.unwrap_or(parkourflow_core::DEFAULT_BUFFER_CAPACITY);
    app.edit(|wf| wf.connect(from, to, capacity))
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

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(AppState::new(Workflow::new("Untitled")))
        .invoke_handler(tauri::generate_handler![
            list_node_types,
            runtime_version,
            get_workflow,
            node_statuses,
            add_node,
            remove_node,
            move_node,
            add_group,
            remove_group,
            connect,
            disconnect,
            set_buffer_capacity,
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
    fn errors_become_strings() {
        let app = AppState::new(Workflow::new("test"));
        let err = app.edit(|wf| wf.remove_group(wf.root())).unwrap_err();
        assert_eq!(err, "the root group cannot be removed");
    }
}
