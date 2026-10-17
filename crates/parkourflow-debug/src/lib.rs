//! Debugging nodes for ParkourFlow.
//!
//! The node types' descriptors live in `parkourflow_core::builtin_nodes`;
//! this crate provides the code they run. Call [`register`] to add it to a
//! [`NodeRegistry`].

use parkourflow_core::{Node, NodeContext, NodeRegistry, NodeRunner};

pub const RECORD_PREVIEW_TYPE: &str = "debug.record_preview";

/// Registers this crate's runnable node types.
pub fn register(registry: &mut NodeRegistry) {
    registry.register(RECORD_PREVIEW_TYPE, RecordPreview::build);
}

/// Consumes `Record`s and keeps the latest one in the node's preview, which
/// the UI shows on the node.
///
/// It takes items as fast as they come, so it never holds up the stream it
/// is attached to. The preview is sampled for the UI a few times a second,
/// so on a fast stream the UI shows the latest record at each sample, not
/// every record.
pub struct RecordPreview;

impl RecordPreview {
    pub fn build(_node: &Node) -> Result<Box<dyn NodeRunner>, String> {
        Ok(Box::new(Self))
    }
}

impl NodeRunner for RecordPreview {
    fn run(self: Box<Self>, mut ctx: NodeContext) -> Result<(), String> {
        let input = ctx.take_input("records")?;
        while let Some(item) = input.recv() {
            ctx.preview().record(&item);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parkourflow_core::{
        builtin_nodes, prepare, PortRef, Preview, Record, StreamItem, Workflow, WorkflowState,
    };

    use super::*;

    #[test]
    fn shows_the_latest_record() {
        let nodes = builtin_nodes();
        let find = |id: &str| nodes.iter().find(|d| d.type_id == id).unwrap();
        let mut wf = Workflow::new("preview");
        // Any Record source will do for wiring; the test feeds the edge.
        let source = wf
            .add_node(wf.root(), "Read", find("file.csv_source"))
            .unwrap();
        let preview = wf
            .add_node(wf.root(), "Preview", find(RECORD_PREVIEW_TYPE))
            .unwrap();
        let conn = wf
            .connect(
                PortRef::new(source, "records"),
                PortRef::new(preview, "records"),
                4,
            )
            .unwrap();
        let state = WorkflowState::new(&wf);

        let mut registry = NodeRegistry::new();
        register(&mut registry);
        let run = prepare(
            &registry,
            wf.node(preview).unwrap(),
            state.wiring(&wf, preview).unwrap(),
        )
        .unwrap();
        let runner = std::thread::spawn(move || run.run());

        let sender = state.edge(conn).unwrap().attach_sender();
        let columns: Arc<[String]> = vec!["name".to_string(), "count".to_string()].into();
        for i in 0..100u64 {
            let record = Record::new(columns.clone(), vec![format!("row {i}"), i.to_string()]);
            sender.send_blocking(StreamItem::new(i, record)).unwrap();
        }
        // Ending the stream lets the node finish.
        drop(sender);
        assert_eq!(runner.join().unwrap(), Ok(()));

        let (id, snapshot) = state.previews().next().unwrap();
        assert_eq!(id, preview);
        assert_eq!((snapshot.received, snapshot.sequence), (100, 99));
        assert_eq!(
            snapshot.latest,
            Preview::Record {
                columns: vec!["name".into(), "count".into()],
                values: vec!["row 99".into(), "99".into()],
                omitted: 0,
            }
        );
    }
}
