# AGENTS.md

Guidance for AI coding agents (and humans) working on ParkourFlow. Read [README.md](README.md) first for the project overview and architecture.

## Project state

Early scaffold. The core crate has the in-memory workflow model (configuration plus runtime state). The UI shows node metadata from the runtime but doesn't yet edit a backend workflow. There is no graph execution yet.

## Layout

- `crates/parkourflow-core/` — the runtime library. It must **not** depend on Tauri; the runtime has to be usable and testable headless.
- `crates/*` — add new runtime and node-module crates here (e.g. `parkourflow-media`, `parkourflow-net`). They join the Cargo workspace automatically.
- `src-tauri/` — thin Tauri shell. Commands here should be thin wrappers that call into the runtime crates.
- `ui/src/` — frontend. `runtime.ts` holds typed wrappers for every Tauri command; keep the TS types in sync with the Rust `Serialize` types.
- Tauri command handlers are registered in `src-tauri/src/lib.rs` (`generate_handler!`).

## Architectural invariants

Do not break these rules without an explicit design decision from the maintainer.

1. **Payloads never cross into JavaScript.** Video frames, packet buffers, audio buffers, and GPU resources stay in the Rust runtime. Only control operations, configuration, status, telemetry, and previews (downscaled, sampled, or aggregated) go over Tauri IPC.
2. **Control and data planes are separate.** Tauri commands and events are the control plane. Never use them to move stream data.
3. **Every edge is typed.** Ports declare a stream type. Reject incompatible connections at validation time, before execution starts, not at runtime.
4. **Every edge is bounded.** Every edge has an explicit buffering/backpressure policy (block, bounded queue, drop oldest, drop newest, latest-only). No unbounded queues in the data plane.
5. **Avoid copies.** Pass data by ownership transfer, borrowing, or shared reference (e.g. `Arc`) where practical. GPU resources pass as handles between compatible nodes.
6. **Don't force one scheduler.** Tokio is for I/O and control-plane work. Do not assume every high-throughput stream runs as an async task. DSP (fixed sample blocks) and GPU/video (resource-aware, explicitly synchronized) may need their own execution domains.
7. **Streams carry metadata.** Keep timestamps, sequence numbers, formats, dimensions, sample rates, and capture metadata with the payload. Do not drop them in transit.
8. **Configuration and runtime state stay separate.** `Workflow` (groups, nodes, ports, connections, buffer sizes) holds no runtime data. `WorkflowState` (node status, buffer contents) is keyed by workflow ids and synced with `WorkflowState::reconcile`. Don't put status, buffers, or other runtime data on config types.
9. **The runtime owns the lifecycle.** Graph construction, validation, startup, shutdown, cancellation, node failure handling, and config propagation live in Rust, not the UI.

## Node conventions

- A node declares its ports, port types, properties, defaults, category, and configuration schema as **metadata**. The UI renders nodes from this metadata.
- Prefer extending the metadata schema over writing node-specific React components. Add custom frontend code only when a node genuinely needs a bespoke inspector or visualization.
- Keep a node's processing code independent of the editor. A node must be runnable and testable without the UI.
- Node modules are **statically linked** crates grouped by domain (media, networking, audio, GPU, files, structured data). Do not build a dynamic plugin system until the node API is stable.

## Tech choices

| Area | Use | Don't |
| --- | --- | --- |
| Desktop shell | Tauri 2 | Electron |
| Runtime | Rust | Payload processing in TypeScript |
| Async | Tokio (I/O, control plane) | Tokio as the only scheduler |
| UI | TypeScript + React | |
| Canvas | `@xyflow/react` | A custom canvas from scratch |
| GPU | wgpu | Direct OpenGL/Vulkan/Metal calls |
| Media | FFmpeg | |
| Capture | libpcap / Npcap | |
| Serialization | Serde + JSON (for now) | Breaking the workflow format without a migration plan |

## Working style

- Keep changes small and focused. Match existing code style once it exists.
- Ask before introducing new major dependencies or changing anything in "Architectural invariants."
- When a design decision is made, record it here or in the README so later agents don't relitigate it.

## Commands

Package manager: **pnpm** (not npm/yarn).

```bash
pnpm install          # install JS deps
pnpm tauri dev        # run the desktop app (Vite dev server + Rust)
pnpm typecheck        # TypeScript check
pnpm build            # typecheck + build UI into dist/
cargo test            # Rust tests (workspace)
cargo clippy --workspace --all-targets
cargo fmt --all
```

Before finishing a change, run `pnpm typecheck`, `cargo test`, and `cargo clippy` for whatever you touched.
