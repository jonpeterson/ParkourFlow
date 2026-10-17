//! Network nodes for ParkourFlow: Read Network Interface (libpcap, Npcap on
//! Windows) and Packet Summary.
//!
//! The node types' descriptors live in `parkourflow_core::builtin_nodes`;
//! this crate provides the code they run and lists capture interfaces. Call
//! [`register`] to add it to a [`NodeRegistry`].

pub mod headers;
pub mod interface;
pub mod summary;

use parkourflow_core::NodeRegistry;

pub use interface::{list_interfaces, InterfaceSource};
pub use summary::PacketSummary;

/// Registers this crate's runnable node types and device lists.
pub fn register(registry: &mut NodeRegistry) {
    registry
        .register(interface::SOURCE_TYPE, InterfaceSource::build)
        .register(summary::SUMMARY_TYPE, PacketSummary::build)
        .register_devices(interface::DEVICE_CLASS, list_interfaces);
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::thread;

    use parkourflow_core::{
        builtin_nodes, prepare, NodeDescriptor, NodeId, PortRef, Workflow, WorkflowState,
    };
    use pcap::{Capture, Linktype, PacketHeader};

    use super::*;
    use crate::headers::tests::{ethernet_frame, ipv4_packet, ipv6_packet, ports};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("parkourflow-net-{name}-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn write_capture(path: &Path, frames: &[Vec<u8>]) {
        let mut file = Capture::dead(Linktype::ETHERNET)
            .unwrap()
            .savefile(path)
            .unwrap();
        for (i, frame) in frames.iter().enumerate() {
            let header = PacketHeader {
                ts: libc::timeval {
                    tv_sec: 1_700_000_000,
                    tv_usec: (i as i32 * 250_000) as _,
                },
                caplen: frame.len() as u32,
                len: frame.len() as u32 + 100,
            };
            file.write(&pcap::Packet::new(&header, frame));
        }
        file.flush().unwrap();
    }

    /// Read Network Interface → Packet Summary → CSV Writer. The source
    /// replays a capture file, since a real interface needs privileges.
    #[test]
    fn interface_to_summary_to_csv() {
        let dir = TempDir::new("e2e");
        let (capture, csv) = (dir.0.join("in.pcap"), dir.0.join("out.csv"));
        write_capture(
            &capture,
            &[
                ethernet_frame(
                    0x0800,
                    &ipv4_packet(17, [192, 168, 1, 5], [8, 8, 8, 8], &ports(53000, 53)),
                ),
                ethernet_frame(
                    0x86dd,
                    &ipv6_packet(
                        6,
                        "2001:db8::1".parse().unwrap(),
                        "2001:db8::2".parse().unwrap(),
                        &ports(443, 51000),
                    ),
                ),
                ethernet_frame(
                    0x0800,
                    &ipv4_packet(1, [10, 0, 0, 1], [10, 0, 0, 2], &[8, 0, 0, 0]),
                ),
            ],
        );

        let nodes = builtin_nodes();
        let find =
            |id: &str| -> &NodeDescriptor { nodes.iter().find(|d| d.type_id == id).unwrap() };
        let mut wf = Workflow::new("e2e");
        let root = wf.root();
        let source = wf
            .add_node(root, "Capture", find(interface::SOURCE_TYPE))
            .unwrap();
        let summary = wf
            .add_node(root, "Summarize", find(summary::SUMMARY_TYPE))
            .unwrap();
        let writer = wf.add_node(root, "Write", find("file.csv_writer")).unwrap();
        wf.set_property(source, "interface", Some(capture.to_str().unwrap().into()))
            .unwrap();
        wf.set_property(writer, "path", Some(csv.to_str().unwrap().into()))
            .unwrap();
        wf.connect(
            PortRef::new(source, "packets"),
            PortRef::new(summary, "packets"),
            64,
        )
        .unwrap();
        wf.connect(
            PortRef::new(summary, "records"),
            PortRef::new(writer, "records"),
            64,
        )
        .unwrap();
        let state = WorkflowState::new(&wf);

        let mut registry = NodeRegistry::new();
        register(&mut registry);
        parkourflow_files::register(&mut registry);
        // Replay the file in place of a live interface.
        registry.register(interface::SOURCE_TYPE, |node| {
            let path = match node.value("interface") {
                Some(parkourflow_core::PropertyValue::String(path)) => PathBuf::from(path),
                _ => return Err("no capture file".into()),
            };
            Ok(Box::new(InterfaceSource::new(interface::open_file(&path)?)))
        });

        // Attach every node before any runs, as starting them all would.
        let ids: [NodeId; 3] = [writer, summary, source];
        let runs = ids.map(|id| {
            prepare(
                &registry,
                wf.node(id).unwrap(),
                state.wiring(&wf, id).unwrap(),
            )
            .unwrap()
        });
        let results = runs
            .map(|run| thread::spawn(move || run.run()))
            .map(|t| t.join().unwrap());
        assert_eq!(results, [Ok(()), Ok(()), Ok(())]);

        assert_eq!(
            std::fs::read_to_string(&csv).unwrap(),
            "time,length,protocol,source,destination\n\
             2023-11-14T22:13:20.000000Z,142,UDP,192.168.1.5:53000,8.8.8.8:53\n\
             2023-11-14T22:13:20.250000Z,162,TCP,[2001:db8::1]:443,[2001:db8::2]:51000\n\
             2023-11-14T22:13:20.500000Z,138,ICMP,10.0.0.1,10.0.0.2\n"
        );
        for (_, stats) in state.edge_stats() {
            assert_eq!(stats.dropped, 0);
        }
    }
}
