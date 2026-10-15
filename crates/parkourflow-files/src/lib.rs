//! File nodes for ParkourFlow: CSV Source and CSV Writer.
//!
//! The node types' descriptors live in `parkourflow_core::builtin_nodes`;
//! this crate provides the code they run. Call [`register`] to add it to a
//! [`NodeRegistry`].

pub mod csv;
pub mod format;

use parkourflow_core::NodeRegistry;

pub use csv::{CsvSource, CsvWriter};

/// Registers this crate's runnable node types.
pub fn register(registry: &mut NodeRegistry) {
    registry
        .register(csv::SOURCE_TYPE, CsvSource::build)
        .register(csv::WRITER_TYPE, CsvWriter::build);
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::thread;

    use parkourflow_core::{
        builtin_nodes, prepare, NodeId, OverflowPolicy, PortRef, PropertyValue, Workflow,
        WorkflowState,
    };

    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir()
                .join(format!("parkourflow-files-{name}-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn path(p: &Path) -> Option<PropertyValue> {
        Some(p.to_str().unwrap().into())
    }

    /// CSV Source → CSV Writer over one edge.
    fn copy_pipeline(
        input: &Path,
        output: &Path,
        capacity: usize,
    ) -> (Workflow, WorkflowState, NodeId, NodeId) {
        let nodes = builtin_nodes();
        let find = |id: &str| nodes.iter().find(|d| d.type_id == id).unwrap();
        let mut wf = Workflow::new("copy");
        let source = wf
            .add_node(wf.root(), "Read", find(csv::SOURCE_TYPE))
            .unwrap();
        let writer = wf
            .add_node(wf.root(), "Write", find(csv::WRITER_TYPE))
            .unwrap();
        wf.set_property(source, "path", path(input)).unwrap();
        wf.set_property(writer, "path", path(output)).unwrap();
        wf.connect(
            PortRef::new(source, "records"),
            PortRef::new(writer, "records"),
            capacity,
        )
        .unwrap();
        let state = WorkflowState::new(&wf);
        (wf, state, source, writer)
    }

    /// Starts both nodes on their own threads and waits for both to finish.
    fn run_both(
        wf: &Workflow,
        state: &WorkflowState,
        nodes: [NodeId; 2],
    ) -> [Result<(), String>; 2] {
        let mut registry = NodeRegistry::new();
        register(&mut registry);
        // Attach both before either runs, as starting both nodes would.
        let prepared = nodes.map(|id| {
            let wiring = state.wiring(wf, id).unwrap();
            prepare(&registry, wf.node(id).unwrap(), wiring).unwrap()
        });
        let runs = prepared.map(|run| thread::spawn(move || run.run()));
        runs.map(|run| run.join().unwrap())
    }

    #[test]
    fn copies_a_file_through_a_real_edge() {
        let dir = TempDir::new("copy");
        let (input, output) = (dir.0.join("in.csv"), dir.0.join("out.csv"));
        let mut text = String::from("id,name,note\n");
        for i in 0..1000 {
            text.push_str(&format!("{i},row {i},\"has, comma and \"\"quotes\"\"\"\n"));
        }
        std::fs::write(&input, &text).unwrap();

        // A tiny buffer, so the source waits for the writer many times.
        let (wf, state, source, writer) = copy_pipeline(&input, &output, 2);
        assert_eq!(run_both(&wf, &state, [source, writer]), [Ok(()), Ok(())]);
        assert_eq!(std::fs::read_to_string(&output).unwrap(), text);

        let stats = state.edge_stats().next().unwrap().1;
        assert_eq!((stats.queue_depth, stats.dropped), (0, 0));
    }

    #[test]
    fn delimiter_and_header_options() {
        let dir = TempDir::new("options");
        let (input, output) = (dir.0.join("in.csv"), dir.0.join("out.csv"));
        std::fs::write(&input, "a;b\n1;2\n").unwrap();
        let (mut wf, _, source, writer) = copy_pipeline(&input, &output, 8);
        wf.set_property(source, "delimiter", Some(";".into()))
            .unwrap();
        wf.set_property(source, "has_header", Some(false.into()))
            .unwrap();
        wf.set_property(writer, "delimiter", Some("tab".into()))
            .unwrap();
        let state = WorkflowState::new(&wf);

        // Without a header, every row is data and no header is written.
        assert_eq!(run_both(&wf, &state, [source, writer]), [Ok(()), Ok(())]);
        assert_eq!(std::fs::read_to_string(&output).unwrap(), "a\tb\n1\t2\n");
    }

    #[test]
    fn malformed_input_is_an_error() {
        let dir = TempDir::new("bad");
        let (input, output) = (dir.0.join("in.csv"), dir.0.join("out.csv"));
        std::fs::write(&input, b"a,b\n1,\xff\n").unwrap();
        let (wf, state, source, writer) = copy_pipeline(&input, &output, 8);
        let [read, write] = run_both(&wf, &state, [source, writer]);
        let err = read.unwrap_err();
        assert!(err.ends_with("line 2: not valid UTF-8 text"), "{err}");
        // The writer finishes cleanly; it got no records, so the file is
        // empty.
        assert_eq!(write, Ok(()));
        assert_eq!(std::fs::read_to_string(&output).unwrap(), "");
    }

    #[test]
    fn lossy_edges_count_drops() {
        let dir = TempDir::new("lossy");
        let (input, output) = (dir.0.join("in.csv"), dir.0.join("out.csv"));
        let text: String = (0..100).map(|i| format!("{i}\n")).collect();
        std::fs::write(&input, &text).unwrap();
        let (mut wf, _, source, _) = copy_pipeline(&input, &output, 4);
        wf.set_property(source, "has_header", Some(false.into()))
            .unwrap();
        let conn = wf.connections().next().unwrap().id;
        wf.set_overflow_policy(conn, OverflowPolicy::DropNewest)
            .unwrap();
        let state = WorkflowState::new(&wf);

        // Only the source runs; its edge keeps the first 4 rows and counts
        // the rest as dropped.
        let mut registry = NodeRegistry::new();
        register(&mut registry);
        let run = prepare(
            &registry,
            wf.node(source).unwrap(),
            state.wiring(&wf, source).unwrap(),
        )
        .unwrap();
        assert_eq!(run.run(), Ok(()));
        let stats = state.edge(conn).unwrap().stats();
        assert_eq!((stats.queue_depth, stats.dropped), (4, 96));
    }
}
