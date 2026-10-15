# ParkourFlow

ParkourFlow is an open-source visual dataflow tool for building and running streaming pipelines. You wire together **sources**, **processors**, and **sinks** on a node-based canvas, and they operate on continuous data such as video, audio, network packets, structured records, and GPU resources.

The project aims to stay flexible and low-level enough for experimentation while providing:

- a fast native runtime,
- explicit stream semantics, and
- easy integration with libraries like FFmpeg, libpcap, and GPU APIs.

> **Status:** Early design. The repo holds a hello-world scaffold: a Tauri shell whose React Flow canvas renders node types supplied by the Rust runtime.

---

## Example pipelines

```
                        typed streams

┌────────────┐       ┌──────────────┐       ┌────────────┐
│   Source   │──────▶│  Processor   │──────▶│    Sink    │
│            │       │              │       │            │
│ libpcap    │Packet │ DNS Parser   │DNS Msg│ CSV Writer │
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
        ├── Node "Parse"  processor  in: packets <Packet>   out: messages <DnsMessage>
        └── Node "Write"  sink       in: records <DnsMessage>

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
- **Node status** is `STOPPED`, `STARTING`, `RUNNING`, or `ERROR`. A node in `ERROR` carries an error message.
- **Stream items** carry a sequence number, an optional timestamp, and a payload. The payload is shared by reference (`Arc`), not copied.

### Flow control

> **Status:** Implemented in `parkourflow-core` (`transport` module). Nodes and graph execution don't use it yet.

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
- Telemetry: `Edge::stats()` returns queue depth, effective capacity, and drop count. The UI reads them with the `connection_stats` command.

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
- [ ] Graph lifecycle: build, start, stop, cancel, error propagation
- [ ] Node metadata schema and JSON workflow format
- [ ] Tauri shell with React Flow editor driven by node metadata
- [ ] Control-plane commands and telemetry events
- [ ] First node modules: files, structured records, libpcap, FFmpeg
- [ ] wgpu-backed nodes and GPU texture streams
- [ ] Audio / DSP execution domain
- [ ] Dynamic plugin system (after the node API stabilizes)

---

## Getting started

### Prerequisites

- [Rust](https://rustup.rs) (stable; `rust-toolchain.toml` selects it)
- Node.js and [pnpm](https://pnpm.io)
- Platform prerequisites for Tauri: see [Tauri's guide](https://v2.tauri.app/start/prerequisites/). On macOS that means the Xcode Command Line Tools.

### Run

```bash
pnpm install
pnpm tauri dev
```

The app opens a window with an empty workflow. Add nodes from the palette on the left, drag between ports to connect them, and select a node or edge and press Backspace/Delete to remove it. Every edit goes to the Rust runtime, which validates it; rejected edits (for example, mismatched stream types or a second connection into one input) show an error in the toolbar. Node positions are kept only in the UI for now.

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
  src/catalog.rs          node type descriptors
src-tauri/                Tauri app crate: control-plane commands that wrap the runtime
  src/lib.rs              holds the open Workflow + WorkflowState; registers commands
ui/                       React + TypeScript frontend (Vite root)
  src/runtime.ts          typed wrappers for Tauri commands
  src/App.tsx             canvas + node palette; edits call the runtime, then redraw
  src/MetadataNode.tsx    generic node renderer driven by node metadata
```

## Contributing

The project is in its design phase. See [AGENTS.md](AGENTS.md) for architectural rules and conventions that contributors (human and AI) should follow.

## License

[MIT](LICENSE)
