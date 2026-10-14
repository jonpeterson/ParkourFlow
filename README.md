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
| **Explicit buffering and backpressure** | Every edge has defined queueing behavior, so a slow consumer cannot cause unbounded memory growth. Policies include blocking, bounded buffering, dropping oldest/newest, and keeping only the latest item. |
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

- [ ] Core graph model: nodes, typed ports, edges, validation
- [ ] Edge transport with configurable buffering/backpressure policies
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

The app opens a window with a canvas showing the built-in demo nodes. You can only connect ports whose stream types match.

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
crates/parkourflow-core/  Rust runtime: graph model, node metadata (no Tauri dependency)
src-tauri/                Tauri app crate: control-plane commands that wrap the runtime
ui/                       React + TypeScript frontend (Vite root)
  src/runtime.ts          typed wrappers for Tauri commands
  src/MetadataNode.tsx    generic node renderer driven by NodeDescriptor
```

## Contributing

The project is in its design phase. See [AGENTS.md](AGENTS.md) for architectural rules and conventions that contributors (human and AI) should follow.

## License

[MIT](LICENSE)
