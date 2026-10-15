//! Node startup checks.
//!
//! Starting a node begins with [`preflight`]: the checks that depend on the
//! outside world and so can't be made when a property is edited. The rest of
//! startup (wiring checks, building the runner) is
//! [`prepare`](crate::runner::prepare).

use std::path::Path;

use crate::property::{FileMode, PropertyType, PropertyValue};
use crate::workflow::Node;

/// Checks that `node` is ready to run: every required property has a value,
/// files to read exist, and folders to write into exist.
///
/// Returns a message for the user on failure. Device properties are not
/// checked yet, because no device backends exist.
pub fn preflight(node: &Node) -> Result<(), String> {
    let missing = node.missing_required();
    if !missing.is_empty() {
        return Err(format!("Set {} before starting.", missing.join(", ")));
    }

    for property in &node.properties {
        let Some(PropertyValue::String(path)) = node.value(&property.name) else {
            continue;
        };
        if path.is_empty() {
            continue;
        }
        let path = Path::new(path);
        let label = &property.label;
        match &property.ty {
            PropertyType::File {
                mode: FileMode::Open,
                ..
            } if !path.is_file() => {
                return Err(format!("{label}: file not found: {}", path.display()));
            }
            PropertyType::File {
                mode: FileMode::Save,
                ..
            } => {
                let parent = path.parent().filter(|p| !p.as_os_str().is_empty());
                if parent.is_some_and(|p| !p.is_dir()) {
                    return Err(format!(
                        "{label}: folder not found: {}",
                        parent.unwrap_or(path).display()
                    ));
                }
            }
            PropertyType::Directory if !path.is_dir() => {
                return Err(format!("{label}: folder not found: {}", path.display()));
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::builtin_nodes;
    use crate::workflow::{NodeId, Workflow};

    fn workflow_with(type_id: &str) -> (Workflow, NodeId) {
        let descriptor = builtin_nodes()
            .into_iter()
            .find(|d| d.type_id == type_id)
            .unwrap();
        let mut wf = Workflow::new("test");
        let id = wf.add_node(wf.root(), "Node", &descriptor).unwrap();
        (wf, id)
    }

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("parkourflow-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn requires_required_properties() {
        let (wf, id) = workflow_with("file.csv_source");
        assert_eq!(
            preflight(wf.node(id).unwrap()),
            Err("Set path before starting.".into())
        );
    }

    #[test]
    fn input_files_must_exist() {
        let (mut wf, id) = workflow_with("file.csv_source");
        let dir = temp_dir("open");
        let file = dir.join("in.csv");

        wf.set_property(id, "path", Some(file.to_str().unwrap().into()))
            .unwrap();
        let err = preflight(wf.node(id).unwrap()).unwrap_err();
        assert!(err.starts_with("Input file: file not found"), "{err}");

        std::fs::write(&file, "a,b\n1,2\n").unwrap();
        assert_eq!(preflight(wf.node(id).unwrap()), Ok(()));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn output_folders_must_exist() {
        let (mut wf, id) = workflow_with("file.csv_writer");
        let dir = temp_dir("save");

        let missing = dir.join("nope").join("out.csv");
        wf.set_property(id, "path", Some(missing.to_str().unwrap().into()))
            .unwrap();
        let err = preflight(wf.node(id).unwrap()).unwrap_err();
        assert!(err.starts_with("Output file: folder not found"), "{err}");

        let ok = dir.join("out.csv");
        wf.set_property(id, "path", Some(ok.to_str().unwrap().into()))
            .unwrap();
        assert_eq!(preflight(wf.node(id).unwrap()), Ok(()));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
