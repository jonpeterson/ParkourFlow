use parkourflow_core::NodeDescriptor;

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

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![list_node_types, runtime_version])
        .run(tauri::generate_context!())
        .expect("error while running ParkourFlow");
}
