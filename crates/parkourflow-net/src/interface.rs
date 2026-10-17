//! Read Network Interface: a live packet capture source.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use parkourflow_core::{DeviceOption, Node, NodeContext, NodeRunner, Pacing, Packet, StreamItem};
use pcap::{Activated, Capture};
use serde::Deserialize;

pub const SOURCE_TYPE: &str = "net.pcap_source";
/// The device class of the `interface` property.
pub const DEVICE_CLASS: &str = "network_interface";

/// How long a capture read waits before giving the runner a chance to see
/// that it was stopped.
const READ_TIMEOUT_MS: i32 = 100;

/// Lists the interfaces libpcap can capture on. Interfaces that are up come
/// first; the label adds the description and addresses where known.
pub fn list_interfaces() -> Result<Vec<DeviceOption>, String> {
    let mut devices = pcap::Device::list().map_err(|e| format!("Can't list interfaces: {e}"))?;
    // Up interfaces with addresses first, then up ones, then the rest.
    devices.sort_by_key(|d| (!d.flags.is_up(), d.addresses.is_empty()));
    Ok(devices
        .into_iter()
        .map(|d| {
            let mut label = d.name.clone();
            if let Some(desc) = d.desc.as_deref().filter(|s| !s.is_empty() && *s != d.name) {
                label.push_str(&format!(" ({desc})"));
            }
            let addrs: Vec<String> = d.addresses.iter().map(|a| a.addr.to_string()).collect();
            if !addrs.is_empty() {
                label.push_str(&format!(" — {}", addrs.join(", ")));
            }
            if !d.flags.is_up() {
                label.push_str(" [down]");
            }
            DeviceOption { id: d.name, label }
        })
        .collect())
}

#[derive(Debug, Deserialize)]
struct InterfaceConfig {
    interface: String,
    filter: String,
    promiscuous: bool,
    snaplen: i64,
}

/// What one read from a capture produced.
#[derive(Debug)]
pub enum Read {
    Packet(Packet),
    /// Nothing arrived before the read timeout.
    Idle,
    /// The capture has no more packets (only a capture file ends).
    End,
}

/// Where packets come from: an open libpcap capture in the app, or anything
/// else in tests.
pub trait PacketReader: Send {
    fn read(&mut self) -> Result<Read, String>;
}

/// A libpcap capture: a live interface, or a capture file.
struct PcapReader<T: Activated + Send> {
    capture: Capture<T>,
    source: Arc<str>,
    link_type: i32,
}

impl<T: Activated + Send> PcapReader<T> {
    fn new(capture: Capture<T>, source: &str) -> Self {
        let link_type = capture.get_datalink().0;
        Self {
            capture,
            source: source.into(),
            link_type,
        }
    }
}

impl<T: Activated + Send> PacketReader for PcapReader<T> {
    fn read(&mut self) -> Result<Read, String> {
        match self.capture.next_packet() {
            Ok(p) => {
                let ts = p.header.ts;
                let timestamp = SystemTime::UNIX_EPOCH
                    + Duration::new(
                        u64::try_from(ts.tv_sec).unwrap_or(0),
                        u32::try_from(ts.tv_usec).unwrap_or(0).saturating_mul(1000),
                    );
                Ok(Read::Packet(Packet {
                    timestamp,
                    source: self.source.clone(),
                    link_type: self.link_type,
                    original_len: p.header.len,
                    data: p.data.into(),
                }))
            }
            Err(pcap::Error::TimeoutExpired) => Ok(Read::Idle),
            Err(pcap::Error::NoMorePackets) => Ok(Read::End),
            Err(e) => Err(format!("{}: {e}", self.source)),
        }
    }
}

/// Opens a capture file as a packet reader. Used by tests; a PCAP File
/// Source node can reuse it.
pub fn open_file(path: &std::path::Path) -> Result<Box<dyn PacketReader>, String> {
    let capture =
        Capture::from_file(path).map_err(|e| format!("Can't open {}: {e}", path.display()))?;
    Ok(Box::new(PcapReader::new(
        capture,
        &path.display().to_string(),
    )))
}

/// Captures packets from a network interface and emits one [`Packet`] per
/// packet on `packets`.
///
/// It is a live source: the interface doesn't wait, so when a connection is
/// full the packet is dropped (and counted) by the connection's overflow
/// policy.
pub struct InterfaceSource {
    reader: Box<dyn PacketReader>,
}

impl InterfaceSource {
    pub fn new(reader: Box<dyn PacketReader>) -> Self {
        Self { reader }
    }

    /// Opens the configured interface, so a missing device, a bad filter or
    /// missing capture permission is a start error.
    pub fn build(node: &Node) -> Result<Box<dyn NodeRunner>, String> {
        let config: InterfaceConfig = node
            .config()
            .map_err(|e| format!("Invalid configuration: {e}"))?;
        let name = config.interface.as_str();
        let snaplen = i32::try_from(config.snaplen).map_err(|_| "Snapshot length is too large.")?;

        let mut capture = Capture::from_device(name)
            .and_then(|c| {
                c.promisc(config.promiscuous)
                    .snaplen(snaplen)
                    .timeout(READ_TIMEOUT_MS)
                    .immediate_mode(true)
                    .open()
            })
            .map_err(|e| open_error(name, &e))?;
        if !config.filter.trim().is_empty() {
            capture
                .filter(&config.filter, true)
                .map_err(|e| format!("Invalid capture filter \"{}\": {e}", config.filter))?;
        }
        Ok(Box::new(Self::new(Box::new(PcapReader::new(
            capture, name,
        )))))
    }
}

/// Explains the usual reasons an interface won't open.
fn open_error(name: &str, e: &pcap::Error) -> String {
    let text = e.to_string();
    let lower = text.to_lowercase();
    if lower.contains("permission") || lower.contains("not permitted") {
        let hint = if cfg!(target_os = "macos") {
            "Capturing needs read access to /dev/bpf*. Install Wireshark's ChmodBPF package, or run ParkourFlow as root."
        } else if cfg!(target_os = "linux") {
            "Capturing needs the CAP_NET_RAW and CAP_NET_ADMIN capabilities, or root."
        } else {
            "Capturing needs Npcap, installed with capture access for your user."
        };
        format!("Can't capture on {name}: {text}. {hint}")
    } else {
        format!("Can't capture on {name}: {text}")
    }
}

impl NodeRunner for InterfaceSource {
    fn pacing(&self) -> Pacing {
        Pacing::Live
    }

    fn run(mut self: Box<Self>, mut ctx: NodeContext) -> Result<(), String> {
        let output = ctx.take_output("packets")?;
        let mut sequence = 0;
        let mut first: Option<SystemTime> = None;

        while !ctx.is_cancelled() {
            let packet = match self.reader.read()? {
                Read::Packet(packet) => packet,
                Read::Idle => continue,
                Read::End => break,
            };
            // Item timestamps are relative to the first packet of the run.
            let start = *first.get_or_insert(packet.timestamp);
            let offset = packet.timestamp.duration_since(start).unwrap_or_default();
            let item = StreamItem::new(sequence, packet).with_timestamp(offset);
            if output.send(item).is_err() {
                // Stopped, or nobody downstream is listening any more.
                break;
            }
            sequence += 1;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::thread;
    use std::time::Instant;

    use parkourflow_core::{
        builtin_nodes, prepare, CancelToken, NodeId, NodeRegistry, OverflowPolicy, PortRef,
        Workflow, WorkflowState,
    };
    use pcap::{Linktype, PacketHeader};

    use super::*;

    struct TempFile(PathBuf);

    impl TempFile {
        fn new(name: &str) -> Self {
            Self(std::env::temp_dir().join(format!(
                "parkourflow-net-{name}-{}.pcap",
                std::process::id()
            )))
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    /// Writes `count` Ethernet frames, 10 ms apart, each 100 bytes on the wire
    /// with the first 64 captured.
    fn write_capture(path: &Path, count: u8) {
        let mut file = Capture::dead(Linktype::ETHERNET)
            .unwrap()
            .savefile(path)
            .unwrap();
        for i in 0..count {
            let header = PacketHeader {
                ts: libc::timeval {
                    tv_sec: 1_700_000_000,
                    tv_usec: (i32::from(i) * 10_000) as _,
                },
                caplen: 64,
                len: 100,
            };
            file.write(&pcap::Packet::new(&header, &[i; 64]));
        }
        file.flush().unwrap();
    }

    /// Capture → DNS Parser. In tests the `interface` value is a capture
    /// file to replay, since opening a real interface needs privileges.
    fn capture_workflow(file: &Path, capacity: usize) -> (Workflow, NodeId, NodeId) {
        let nodes = builtin_nodes();
        let find = |id: &str| nodes.iter().find(|d| d.type_id == id).unwrap();
        let mut wf = Workflow::new("capture");
        let source = wf
            .add_node(wf.root(), "Capture", find(SOURCE_TYPE))
            .unwrap();
        let parser = wf
            .add_node(wf.root(), "Parse", find("net.dns_parser"))
            .unwrap();
        wf.set_property(source, "interface", Some(file.to_str().unwrap().into()))
            .unwrap();
        wf.connect(
            PortRef::new(source, "packets"),
            PortRef::new(parser, "packets"),
            capacity,
        )
        .unwrap();
        (wf, source, parser)
    }

    fn file_registry() -> NodeRegistry {
        let mut registry = NodeRegistry::new();
        registry.register(SOURCE_TYPE, |node| {
            let config: InterfaceConfig = node.config().map_err(|e| e.to_string())?;
            Ok(Box::new(InterfaceSource::new(open_file(Path::new(
                &config.interface,
            ))?)))
        });
        registry
    }

    fn received(state: &WorkflowState, wf: &Workflow) -> Vec<StreamItem> {
        let conn = wf.connections().next().unwrap().id;
        let receiver = state.edge(conn).unwrap().attach_receiver();
        std::iter::from_fn(|| receiver.try_recv().ok()).collect()
    }

    #[test]
    fn emits_packets_with_metadata() {
        let file = TempFile::new("emit");
        write_capture(&file.0, 3);
        let (wf, source, _) = capture_workflow(&file.0, 8);
        let state = WorkflowState::new(&wf);

        let run = prepare(
            &file_registry(),
            wf.node(source).unwrap(),
            state.wiring(&wf, source).unwrap(),
        )
        .unwrap();
        assert_eq!(run.pacing(), Pacing::Live);
        assert_eq!(run.run(), Ok(()));

        let items = received(&state, &wf);
        assert_eq!(items.len(), 3);
        for (i, item) in items.iter().enumerate() {
            let packet = item.payload::<Packet>().unwrap();
            assert_eq!(item.sequence, i as u64);
            assert_eq!(item.timestamp, Some(Duration::from_millis(10 * i as u64)));
            assert_eq!(packet.link_type, Packet::ETHERNET);
            assert_eq!((packet.data.len(), packet.original_len), (64, 100));
            assert!(packet.is_truncated());
            assert_eq!(packet.data[0], i as u8);
            assert_eq!(&*packet.source, file.0.display().to_string());
            assert_eq!(
                packet.timestamp,
                SystemTime::UNIX_EPOCH
                    + Duration::from_secs(1_700_000_000)
                    + Duration::from_millis(10 * i as u64)
            );
        }
    }

    #[test]
    fn live_source_drops_instead_of_waiting() {
        let file = TempFile::new("drop");
        write_capture(&file.0, 10);
        // A `Block` edge with room for 4: a live source must not wait for
        // the (absent) consumer, so the other 6 are dropped and counted.
        let (wf, source, _) = capture_workflow(&file.0, 4);
        assert_eq!(
            wf.connections().next().unwrap().overflow_policy,
            OverflowPolicy::Block
        );
        let state = WorkflowState::new(&wf);

        let run = prepare(
            &file_registry(),
            wf.node(source).unwrap(),
            state.wiring(&wf, source).unwrap(),
        )
        .unwrap();
        assert_eq!(run.run(), Ok(()));
        let stats = state.edge_stats().next().unwrap().1;
        assert_eq!((stats.queue_depth, stats.dropped), (4, 6));
    }

    /// Never produces a packet, like a quiet interface.
    struct Quiet;

    impl PacketReader for Quiet {
        fn read(&mut self) -> Result<Read, String> {
            thread::sleep(Duration::from_millis(READ_TIMEOUT_MS as u64));
            Ok(Read::Idle)
        }
    }

    #[test]
    fn stops_promptly_on_a_quiet_interface() {
        let file = TempFile::new("quiet");
        write_capture(&file.0, 0);
        let (wf, source, _) = capture_workflow(&file.0, 4);
        let state = WorkflowState::new(&wf);

        let cancel = CancelToken::new();
        let ctx = NodeContext::new(
            source,
            state.wiring(&wf, source).unwrap(),
            Pacing::Live,
            cancel.clone(),
        );
        let runner = Box::new(InterfaceSource::new(Box::new(Quiet)));
        let run = thread::spawn(move || runner.run(ctx));

        thread::sleep(Duration::from_millis(250));
        let stopped = Instant::now();
        cancel.cancel();
        assert_eq!(run.join().unwrap(), Ok(()));
        assert!(
            stopped.elapsed() < Duration::from_millis(500),
            "{:?}",
            stopped.elapsed()
        );
    }

    /// Fails on the first read, like an interface that went away.
    struct Broken;

    impl PacketReader for Broken {
        fn read(&mut self) -> Result<Read, String> {
            Err("en9: The interface went down".into())
        }
    }

    #[test]
    fn capture_errors_fail_the_node() {
        let file = TempFile::new("broken");
        write_capture(&file.0, 0);
        let (wf, source, _) = capture_workflow(&file.0, 4);
        let state = WorkflowState::new(&wf);
        let ctx = NodeContext::new(
            source,
            state.wiring(&wf, source).unwrap(),
            Pacing::Live,
            CancelToken::new(),
        );
        let runner = Box::new(InterfaceSource::new(Box::new(Broken)));
        assert_eq!(runner.run(ctx), Err("en9: The interface went down".into()));
    }

    #[test]
    fn missing_interface_is_a_start_error() {
        let nodes = builtin_nodes();
        let descriptor = nodes.iter().find(|d| d.type_id == SOURCE_TYPE).unwrap();
        let mut wf = Workflow::new("missing");
        let id = wf.add_node(wf.root(), "Capture", descriptor).unwrap();
        wf.set_property(id, "interface", Some("parkourflow-no-such-if0".into()))
            .unwrap();
        let err = InterfaceSource::build(wf.node(id).unwrap()).err().unwrap();
        assert!(
            err.starts_with("Can't capture on parkourflow-no-such-if0"),
            "{err}"
        );
    }

    #[test]
    fn lists_interfaces() {
        // Listing needs no capture privileges, and every machine has a
        // loopback interface.
        let interfaces = list_interfaces().unwrap();
        assert!(!interfaces.is_empty());
        assert!(interfaces.iter().all(|d| d.label.starts_with(&d.id)));
    }
}
