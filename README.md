# ParkourFlow

ParkourFlow is an open-source visual dataflow tool for building and running streaming pipelines. You wire together **sources**, **processors**, and **sinks** on a node-based canvas, and they operate on continuous data such as video, audio, network packets, structured records, and GPU resources.

The project aims to stay flexible and low-level enough for experimentation while providing:

- a fast native runtime,
- explicit stream semantics, and
- easy integration with libraries like FFmpeg, libpcap, and GPU APIs.

> **Status:** Early design. A Tauri shell whose React Flow canvas edits a workflow held by the Rust runtime. Nodes start and stop individually; CSV Source and CSV Writer are the first nodes that process data.

---

## Example pipelines

```
                        typed streams

┌────────────┐       ┌──────────────┐       ┌────────────┐
│   Source   │──────▶│  Processor   │──────▶│    Sink    │
│            │       │              │       │            │
│ libpcap    │Packet │ DNS Parser   │Record │ CSV Writer │
└────────────┘       └──────────────┘       └────────────┘


┌────────────┐       ┌──────────────┐       ┌────────────┐
│ FFmpeg     │──────▶│ GPU Shader   │──────▶│ Display    │
│ Decoder    │ Frame │              │Texture│            │
└────────────┘       └──────────────┘       └────────────┘
```

---

## Architecture

ParkourFlow is a **typed streaming graph runtime**. A workflow is a directed graph of nodes connected by typed ports:

- **Source nodes** produce streams that may never end.
- **Processor nodes** consume one or more streams and emit transformed streams.
- **Sink nodes** end streams by writing, displaying, transmitting, or otherwise consuming the data.

> **Core principle:** the graph describes how data moves and is transformed. Each edge defines the contract for how that data is transported.

Because each edge carries its own contract, ParkourFlow can support ordinary records, realtime media, packet streams, and GPU resources without forcing them all through one generic queue.

### Key concepts

| Concept | Description |
| --- | --- |
| **Typed ports and edges** | Each connection carries a declared stream type such as `VideoFrame`, `AudioBuffer`, `Packet`, `DnsMessage`, `Record`, or `GpuTexture`. The editor rejects incompatible connections before execution. |
| **Streams, not messages** | An edge is a continuous sequence of values, not a series of RPC-style calls. A video edge is "a stream of frames," not repeated commands to process a frame. |
| **Native data plane** | High-volume payloads stay inside the Rust runtime. Frames, packet buffers, and GPU resources never cross into the Tauri/JavaScript UI, except as previews or aggregated telemetry. |
| **Demand-driven flow with explicit overflow** | Consumers grant producers capacity, so a slow consumer cannot cause unbounded memory growth. Each edge has an overflow policy (`Block`, `DropOldest`, `DropNewest`, `KeepLatest`) for producers that cannot wait. See [Flow control](#flow-control). |
| **Ownership-aware buffers** | Data moves between nodes by ownership transfer, borrowing, or shared reference where practical, rather than being copied at every node. GPU resources pass as handles between compatible nodes. |
| **Multiple execution domains** | Different streams use different scheduling. Network and record pipelines suit async tasks. DSP runs on fixed-size sample blocks. GPU and video pipelines use resource-aware scheduling with explicit synchronization. |
| **Metadata alongside payloads** | Stream items can carry timestamps, sequence numbers, formats, dimensions, sample rates, capture metadata, and other attributes needed downstream. |
| **Graph lifecycle management** | The runtime owns graph construction, validation, startup, shutdown, cancellation, node failures, and propagation of configuration changes. |
| **Separate control and data planes** | The UI sends operations (`start`, `stop`, `connect`, `set parameter`). The runtime reports status, errors, throughput, queue depth, latency, and previews. |
| **Composable node API** | A node declares its input/output ports and configuration schema. Its processing code is independent of the editor. |

### Control plane vs. data plane

```
┌──────────────────────────────────────────┐
│  UI  (TypeScript + React + React Flow)   │
│  graph editing · inspectors · controls   │
└───────────────┬──────────────▲───────────┘
   Tauri commands│              │Tauri events
 (create graph,  │              │(status, errors, stats,
  set param,     │              │ queue depth, previews)
  start/stop)    ▼              │
┌───────────────────────────────┴──────────┐
│  Runtime  (Rust)                         │
│  graph scheduling · lifecycle · nodes    │
│                                          │
│  data plane: frames, packets, audio      │
│  buffers, GPU resources — stays native   │
└──────────────────────────────────────────┘
```

### Workflow model

ParkourFlow has a single concurrent user, so a workflow and its runtime state are both held **in memory**. Saving and loading workflows will come later.

A workflow is split into two halves that are kept apart:

| | **Configuration** (`Workflow`) | **Runtime state** (`WorkflowState`) |
| --- | --- | --- |
| Holds | Groups, nodes, ports, connections, buffer sizes, overflow policies | Node status, connection edges (buffer contents, drop counters) |
| Changed by | Editor operations (add node, connect, …) | The runtime while the graph executes |
| Persisted | Yes, eventually (import/export) | No; rebuilt from the configuration |

State is keyed by the configuration's ids. After the configuration changes, `WorkflowState::reconcile` adds state for new nodes and connections, drops state for removed ones, and applies new buffer sizes and overflow policies.

```
Workflow
└── Group (root)
    ├── Node  "Capture"   source     out: packets <Packet>
    └── Group "Parsing"
        ├── Node "Parse"  processor  in: packets <Packet>   out: messages <Record>
        └── Node "Write"  sink       in: records <Record>

Connections (workflow level; may cross groups)
    Capture.packets ──▶ Parse.packets     buffer_capacity: 64
    Parse.messages  ──▶ Write.records     buffer_capacity: 8
```

- **Groups.** The top level of a workflow is a root group. A group holds nodes and child groups, so workflows form a tree.
- **Nodes** are one of three kinds, which decide their ports:

  | Kind | Input ports | Output ports |
  | --- | --- | --- |
  | Source | none | one or more |
  | Processor | one or more | one or more |
  | Sink | one or more | none |

- **Ports** have a name and a **data type** such as `Packet`, `Frame`, `Texture`, `Audio`, or `Record`. Node modules can define more data types.
- **Connections** join a node's output port, which emits data, to another node's input port, which receives it. A connection is only allowed when both ports carry the same data type. One output can feed many inputs, but each input accepts only one connection.
- **Buffers.** Each connection has a configurable `buffer_capacity` and `overflow_policy`. At runtime it gets an *edge*: a bounded FIFO of stream items. A full buffer never grows; the connection's overflow policy decides what happens next (see [Flow control](#flow-control)).
- **Node status** is `STOPPED`, `STARTING`, `RUNNING`, `STOPPING`, or `ERROR`. A node in `ERROR` carries an error message. See [Node lifecycle](#node-lifecycle).
- **Stream items** carry a sequence number, an optional timestamp, and a payload. The payload is shared by reference (`Arc`), not copied.

### Node lifecycle

Nodes are started and stopped individually.

| From | Action | To |
| --- | --- | --- |
| `STOPPED` | start | `STARTING` |
| `STARTING` | startup succeeds | `RUNNING` (its runner starts) |
| `STARTING` | startup fails | `ERROR` (with a message) |
| `RUNNING` | stop | `STOPPING` (its runner is cancelled) |
| `RUNNING`, `STOPPING` | runner returns | `STOPPED` |
| `RUNNING`, `STOPPING` | runner returns an error or panics | `ERROR` (with the message) |
| `ERROR` | start (retry) | `STARTING` |

Any other request, such as stopping a node that is `STARTING`, is rejected. The transition rules live in `WorkflowState` (`begin_start`, `finish_start`, `begin_stop`, `finish_run`).

**Startup** runs on the node's own thread, so `start_node` returns as soon as the node is `STARTING`. Startup means passing `preflight` (required properties are set, files to read exist, folders to write into exist; device properties are not checked yet), having every input connected (and, for a source, at least one output), and building the node's runner, which opens its files. The same thread then runs the node. See [Node execution](#node-execution).

**Stopping** cancels the runner and returns at once with the node `STOPPING`; it becomes `STOPPED` once the runner has actually returned. A node also stops on its own when its work is done: a CSV Source at the end of its file, a CSV Writer at the end of its input stream.

**Status changes are pushed, not polled.** The runtime emits a `node-status` event (`{ id, state }`) on every transition, and the UI updates the node from it.

**In the UI**, each node card and the inspector show the status and the actions it allows:

| Status | Controls |
| --- | --- |
| `STOPPED` | **Start** |
| `STARTING` | None |
| `RUNNING` | **Stop** |
| `STOPPING` | None |
| `ERROR` | **Details** (opens a dialog with the error message) and **Retry**. Hovering the status shows the message too. |

A button disables as soon as it is clicked and stays disabled until the runtime answers, so a double click sends only one request.

### Flow control

> **Status:** Implemented in `parkourflow-core` (`transport` module) and used by running nodes (see [Node execution](#node-execution)).

ParkourFlow uses a **hybrid** model: demand flows upstream as the default, and each connection has an **overflow policy** for producers that cannot wait.

Neither pure model fits every workload:

- **Pure push** (sources emit as fast as they can; full buffers drop data) silently loses data in offline work. Transcoding a file or replaying a PCAP would drop frames whenever the consumer is slow, even though the producer could have waited. It also wastes work on items that end up dropped.
- **Pure pull** (producers sit idle until asked) cannot pause live sources. A packet capture, camera, or microphone keeps producing whether anyone asks or not, so data piles up or is lost outside the runtime, where it cannot be seen or counted.

What matters is whether a producer can pause:

| Producer | Examples | Behavior |
| --- | --- | --- |
| **Pausable** | File reader, PCAP replay, decoder, most processors | Waits for demand. Lossless, with bounded memory. |
| **Live** | Packet capture, camera, microphone, network socket | Never waits. If downstream has no capacity, the item is dropped by policy and counted. |

#### Demand

Each connection grants its producer **credit**: the number of free slots in its buffer. A producer sends at most that many items, and credit returns as the consumer takes items. Credit is granted in batches, not item by item, so demand adds no per-item round trip.

#### Overflow policies

Each connection has a policy for when an item arrives and the buffer is full:

| Policy | Behavior | Typical use |
| --- | --- | --- |
| `Block` (default) | The producer waits for space. Backpressure propagates upstream. Nothing is lost. | File processing, recording, anything that must be complete |
| `DropOldest` | Evict the item at the head of the queue to make room. | Live streams where fresh data matters more than completeness |
| `DropNewest` | Discard the incoming item. | Live streams where already-queued data should be preserved |
| `KeepLatest` | The buffer holds a single item, and each new item replaces it. | Previews, displays, UI telemetry |

Rules:

- **Live sources never block.** A live source on a `Block` connection still cannot wait. It drops the item itself and counts the drop, rather than stalling its capture loop and letting the OS discard data invisibly.
- **Policies are per connection.** When one output feeds several inputs, each connection applies its own policy. With `Block` everywhere, the slowest consumer gates the producer. Making a less important branch, like a preview, lossy keeps the main path at full speed.
- **Drops are visible.** The runtime counts drops per connection and reports them with queue depth, so loss is never silent.
- **Independent of the scheduler.** A `Block` connection is a full queue the producer waits on, whether the producer is an async task, a dedicated thread, or a GPU work queue.

#### Implementation

`transport::Edge` is the runtime half of a connection. It wraps the connection's `ConnectionBuffer` and policy, and `WorkflowState` holds one per connection. Producers and consumers take `Sender` and `Receiver` handles from it:

| | Waits for credit on `Block` | Never waits |
| --- | --- | --- |
| Async task (any executor) | `send(item).await`, `recv().await` | `try_send`, `send_live`, `try_recv` |
| Dedicated thread | `send_blocking`, `recv_blocking` | `try_send`, `send_live`, `try_recv` |

- `Sender::credit()` is the number of free slots. A waiting `Block` producer is woken once at least half the buffer is free, which is the batched credit grant.
- `send_live` is for live sources: on a full `Block` edge it drops the item and counts it instead of waiting.
- `FanOut` sends one output to several edges. Lossy edges get the item first, then it waits on each full `Block` edge in turn.
- `KeepLatest` uses an effective capacity of 1, whatever `buffer_capacity` says.
- Shrinking a buffer or switching to `KeepLatest` drops the oldest items, and those drops are counted too.
- An edge closes when its last `Sender` or its `Receiver` drops, or when its connection is removed. A closed edge fails sends; the receiver gets the remaining items and then the end of the stream.
- The channel uses only `std` synchronization (`Mutex`, `Condvar`, `Waker`), so it doesn't depend on Tokio.
- Telemetry: `Edge::stats()` returns queue depth, effective capacity, and drop count. The shell samples every edge every 250 ms and emits a `connection-stats` event when anything changed; the UI also reads them with the `connection_stats` command when it redraws. Each edge on the canvas is labelled `queue depth/capacity`, plus `· N dropped` (in red) once it has dropped anything.

### Node execution

A running node runs a **runner**: code built from the node's configuration that moves data between its ports. The API lives in `parkourflow-core` (`runner` module); node implementations live in node-module crates such as `parkourflow-files`.

```rust
pub trait NodeRunner: Send + 'static {
    fn pacing(&self) -> Pacing { Pacing::Pausable }   // or Pacing::Live
    fn run(self: Box<Self>, ctx: NodeContext) -> Result<(), String>;
}
```

- **Building.** A node-module crate registers a builder per node type in a `NodeRegistry` (`registry.register("file.csv_source", CsvSource::build)`). The builder reads the node's typed config with `Node::config::<T>()` and opens what it needs, so a missing or unreadable file fails the start with a message. Starting a node type with no registered runner fails with an error.
- **Context.** `NodeContext` has an `Input` for each input port and an `Output` for each output port. An `Output` feeds every connection from its port (a `FanOut`). `Input::recv` waits for the next item and returns `None` at the end of the stream; `Output::send` waits for credit on full `Block` edges.
- **Live or pausable.** `NodeRunner::pacing` is asked once after the runner is built, so it can depend on properties (a real-time PCAP replay is live). A pausable node's `Output::send` waits for credit; a live node's never waits and drops (and counts) instead, as described under [Flow control](#flow-control).
- **Threads.** Each running node gets a dedicated OS thread. That suits file and record nodes; async tasks, DSP blocks, and GPU queues can get their own execution domains later behind the same trait. The session lock is never held while a runner works.
- **Cancellation.** Stopping a node cancels its `CancelToken`, which wakes every edge the node is attached to. `Input::recv` and `Output::send` return as soon as the node is cancelled, even mid-wait; other loops check `NodeContext::is_cancelled`.
- **End of stream.** When a runner returns, its edge handles drop and its edges close. Downstream nodes drain what is queued, see the end of the stream, and finish; upstream nodes see `Output::send` fail and finish. So stopping a source winds down the pipeline below it, and a CSV copy stops by itself when the file is done. A node that is started again reopens its idle edges and begins a new stream.
- **Failures.** A runner that returns `Err` or panics puts the node in `ERROR` with the message ("Node crashed: …" for a panic).
- **Connections changed while running** are picked up at the next start. Removing a connection closes its edge, which ends that stream for the running nodes on either side; removing a running node cancels it.

#### CSV nodes

`parkourflow-files` implements CSV Source and CSV Writer, so a CSV Source wired to a CSV Writer copies a file. Rows travel as `Record` items: `columns` (the header, shared by every row of a file through an `Arc`) and `values`. The file contents stay in Rust; only the paths cross into the UI.

- **CSV Source** reads `path` with `delimiter`; with `has_header` on, the first row becomes the column names. It is pausable, so it reads only as fast as downstream takes rows. A row that can't be read (such as text that isn't UTF-8) is an `ERROR` naming the file and line.
- **CSV Writer** creates (or truncates) `path` when it starts, writes the first record's column names as a header if `write_header` is on and there are any, then one row per record, and flushes when its input ends or it is stopped.
- Parsing and quoting use the [`csv`](https://crates.io/crates/csv) crate, behind a small `format.rs` interface: quoted fields can hold the delimiter, line breaks, and `""`; rows end in `\n` or `\r\n` (written as `\n`); blank lines are skipped; rows may differ in length.

#### Network nodes

`parkourflow-net` implements **Read Network Interface** (`net.pcap_source`), a live capture source built on libpcap through the [`pcap`](https://crates.io/crates/pcap) crate (Npcap on Windows).

- **Properties:** `interface` (a device picked from the interfaces libpcap finds), `filter` (a BPF expression such as `udp port 53`; empty captures everything), `promiscuous`, and `snaplen` (bytes kept per packet).
- **Opening the interface happens at start,** so a missing interface, an invalid filter, or missing capture permission is an `ERROR` with a message saying what to fix.
- **Output:** one `Packet` item per captured packet on `packets`. A `Packet` (`parkourflow-core/src/packet.rs`) carries the capture time, the interface name, the link type (`LINKTYPE_*`, 1 for Ethernet), the length on the wire, and the captured bytes. The item's `timestamp` is the time since the run's first packet. Packets stay in Rust.
- **It is live:** an interface can't be paused, so on a full connection the packet is dropped by the connection's overflow policy and counted, even on `Block` connections.
- **Stopping:** reads time out every 100 ms, so a node on a quiet interface still stops promptly.
- **Unbounded:** the node runs until it is stopped or capture fails.

Why libpcap rather than a pure-Rust capture library: it gives kernel-side BPF filters, capture files (for a future PCAP File Source) and interface descriptions on all three platforms. The pure-Rust alternative, `pnet`, still needs Npcap on Windows and the same `/dev/bpf` access on macOS.

**Capture permissions** are needed to open an interface, but not to list them:

| Platform | What's needed |
| --- | --- |
| macOS | Read access to `/dev/bpf*`, which is root-only by default. Install Wireshark's ChmodBPF package (it adds an `access_bpf` group), or run as root. |
| Linux | The `CAP_NET_RAW` and `CAP_NET_ADMIN` capabilities, or root. |
| Windows | [Npcap](https://npcap.com) installed. Unless you restrict it to administrators, normal users can capture. |

### Node properties

Each node type declares typed configuration fields, called **properties**, alongside its ports. The UI's inspector renders a form for the selected node from these declarations, so most node types need no frontend code.

There is no "file node" or "device node" category. A CSV source, a PCAP file replay, and a live capture are just different node types that declare different properties:

| Node type | Properties |
| --- | --- |
| CSV Source | `path` (file, open, `.csv`/`.tsv`, required), `delimiter` (enum), `has_header` (bool) |
| PCAP File Source | `path` (file, open, `.pcap`/`.pcapng`, required), `realtime` (bool), `speed` (float, live) |
| Read Network Interface | `interface` (device, required), `filter` (string), `promiscuous` (bool), `snaplen` (integer, 64–262144) |
| CSV Writer | `path` (file, save, `.csv`, required), `delimiter` (enum), `write_header` (bool) |

#### Property types

| Type | Inspector widget | Validation |
| --- | --- | --- |
| `bool` | Checkbox | |
| `integer { min, max }` | Number input | Whole number within bounds |
| `float { min, max, step }` | Number input | Finite number within bounds; integers are accepted |
| `string { multiline }` | Text box or text area | |
| `enum { options }` | Drop-down | One of the options |
| `file { mode: open \| save, extensions }` | Path + **Browse…** (OS file dialog) | Extension matches, if any are listed |
| `directory` | Path + **Browse…** (OS folder dialog) | |
| `device { class }` | Drop-down of devices the runtime finds right now, e.g. `network_interface` | |

Devices come and go while the app runs, so their options are not part of the node type. The UI asks the runtime with `list_device_options(class)` when the field is shown, and node modules register a lister per class (`NodeRegistry::register_devices`). `parkourflow-net` lists `network_interface` devices: interfaces that are up and have addresses come first, labeled with their description and addresses. A class with no lister returns an empty list, and the inspector falls back to a text box.

A file path is configuration, so it crosses the IPC boundary; the file's contents never do.

#### Each property also declares

- **`label`** and optional **`description`**, shown in the inspector.
- **`default`**, which a new node starts with. Clearing a value restores its default.
- **`required`**: the node cannot start until it has a value. The canvas and inspector flag missing required values.
- **`apply`**: `live` values take effect on a running node; `restart` values (the default) need the node restarted, and the inspector warns before changing them on a running node.

#### Values and validation

Values are configuration. They live on each node in the `Workflow` (`Node::values`) and will be saved with it once import/export exists.

- **On edit:** `set_property(node, name, value)` checks the value against the property's type and rejects it with a message, which the inspector shows under the field. `null` resets the value to its default.
- **On start:** problems that depend on the outside world, like a missing file or an unplugged device, put the node in `ERROR` with a message. They are not rejected at edit time, because the outside world can change between editing and running.
- **In node code:** a node reads its values as a typed struct with `Node::config::<T>()` (e.g. `struct CsvSourceConfig { path: PathBuf, has_header: bool }`) instead of handling raw JSON.

The schema is a small hand-written type rather than JSON Schema, because JSON Schema cannot express "show a file picker" or "ask the runtime for devices" without custom extensions.

#### Not yet decided

- **Ports that depend on configuration.** An FFmpeg demuxer's outputs depend on the streams in its file, and a CSV's columns depend on its header. Ports are fixed per node type for now; later a node type may recompute its ports when a property changes, and the workflow would drop connections that no longer fit.
- **Live vs. pausable at runtime.** Whether a source is live (see [Flow control](#flow-control)) can depend on its properties; a PCAP replay with `realtime` on behaves like a live source. The node will report this when it starts, not in its type.

### Node definitions

Nodes are **driven by metadata**. Each node declares its port types, properties, defaults, category, and configuration schema. The React UI renders most node types from this metadata alone, without custom frontend code.

At first, node modules are **statically linked**: built-in crates for media, networking, audio, GPU, files, and structured data. A dynamic plugin system may come later, once the node API is stable.

---

## Tech stack

| Layer | Technology | Role |
| --- | --- | --- |
| Desktop shell | **Tauri 2** | Native windowing, packaging, filesystem access, and IPC between UI and runtime. |
| Core engine | **Rust** | Graph scheduling, stream handling, buffer ownership, native-library integration, and performance-sensitive processing. |
| Async runtime | **Tokio** | I/O-heavy nodes, control-plane tasks, and networking. Not necessarily the execution model for every high-throughput stream. |
| UI | **TypeScript + React** | Graph editing, node configuration, inspectors, runtime controls, logs, and visualization. |
| Node canvas | **React Flow (`@xyflow/react`)** | Ports, edges, dragging, selection, zoom, pan, grouping, and other graph-editor interactions. |
| GPU | **wgpu** | Cross-platform compute shaders, texture processing, visualization, and GPU-backed nodes, without tying the project to OpenGL, Vulkan, or Metal directly. |
| Media | **FFmpeg** | Decoding, encoding, demuxing, muxing, resampling, and video/audio I/O. |
| Packet capture | **libpcap / Npcap** | Live network-stream sources and PCAP file processing. |
| Serialization | **Serde + JSON** | The first graph/workflow file format, and configuration metadata exchanged between Rust and TypeScript. |
| Control-plane IPC | **Tauri commands/events** | Create graphs, change parameters, start/stop execution, report errors, and publish statistics. |

---

## Roadmap (initial)

- [x] Core graph model: nodes, typed ports, edges, groups, validation
- [x] Edge transport: demand (credit) signaling, overflow policies, drop counters
- [x] Node lifecycle: start, stop, cancel, error propagation (per node)
- [ ] Graph lifecycle: start and stop a whole workflow
- [x] Node metadata schema: typed properties, validation, inspector
- [ ] JSON workflow import/export
- [ ] Tauri shell with React Flow editor driven by node metadata
- [x] Control-plane commands and telemetry events (node status, queue depth, drops)
- [x] First node module: files (CSV Source, CSV Writer) and structured records
- [ ] More node modules: libpcap, FFmpeg
- [ ] wgpu-backed nodes and GPU texture streams
- [ ] Audio / DSP execution domain
- [ ] Dynamic plugin system (after the node API stabilizes)

---

## Getting started

### Prerequisites

- [Rust](https://rustup.rs) (stable; `rust-toolchain.toml` selects it)
- Node.js and [pnpm](https://pnpm.io)
- Platform prerequisites for Tauri: see [Tauri's guide](https://v2.tauri.app/start/prerequisites/). On macOS that means the Xcode Command Line Tools.
- libpcap, for network capture nodes:
  - **macOS:** included with the Xcode Command Line Tools.
  - **Linux:** the development package, e.g. `sudo apt install libpcap-dev`.
  - **Windows:** [Npcap](https://npcap.com) and the Npcap SDK, with the SDK's `Lib/x64` folder on `LIB` (or in `LIBPCAP_LIBDIR`).
  - Capturing also needs permissions; see [Network nodes](#network-nodes).

### Run

```bash
pnpm install
pnpm tauri dev
```

The app opens a window with an empty workflow. Add nodes from the palette on the left, drag between ports to connect them, and select a node or edge and press Backspace/Delete to remove it. Every edit goes to the Rust runtime, which validates it; rejected edits (for example, mismatched stream types or a second connection into one input) show an error in the toolbar. Node positions are kept only in the UI for now.

To copy a CSV file: add a **CSV Source** and a **CSV Writer**, connect `records` to `records`, set each node's file in the inspector, then press **Start** on the writer and on the source. Both return to `STOPPED` when the copy is done. The edge label shows the queue depth while rows flow.

### Other commands

| Command | Purpose |
| --- | --- |
| `pnpm dev` | Run only the UI in a browser at `localhost:1420`. Runtime calls are unavailable there. |
| `pnpm build` | Typecheck and build the UI into `dist/` |
| `pnpm tauri build` | Build a release desktop bundle |
| `cargo test` | Run Rust tests |
| `cargo clippy --workspace` | Lint Rust code |

### Repository layout

```
crates/parkourflow-core/  Rust runtime (no Tauri dependency)
  src/workflow.rs         workflow configuration: groups, nodes, ports, connections
  src/state.rs            runtime state: node status, connection buffers
  src/buffer.rs           bounded connection buffer and stream items
  src/transport.rs        credit-based edge transport and overflow policies
  src/catalog.rs          node type descriptors
  src/property.rs         typed node properties: schema, values, validation
  src/lifecycle.rs        startup checks (preflight) for starting a node
  src/runner.rs           node execution: NodeRunner, NodeContext, NodeRegistry, cancellation
  src/record.rs           Record, the payload of Record streams
  src/packet.rs           Packet, the payload of Packet streams
crates/parkourflow-files/ file nodes: CSV Source and CSV Writer
  src/csv.rs              the CSV node runners
  src/format.rs           CSV row reader and writer (wraps the csv crate)
crates/parkourflow-net/   network nodes: Read Network Interface (libpcap)
  src/interface.rs        live capture source and interface listing
src-tauri/                Tauri app crate: control-plane commands that wrap the runtime
  src/lib.rs              holds the open Workflow + WorkflowState; registers commands
ui/                       React + TypeScript frontend (Vite root)
  src/runtime.ts          typed wrappers for Tauri commands
  src/App.tsx             canvas + node palette; edits call the runtime, then redraw
  src/MetadataNode.tsx    generic node renderer driven by node metadata
  src/Inspector.tsx       generic property form for the selected node
  src/NodeControls.tsx    start/stop controls and the error dialog
```

## Contributing

The project is in its design phase. See [AGENTS.md](AGENTS.md) for architectural rules and conventions that contributors (human and AI) should follow.

## License

[MIT](LICENSE)
