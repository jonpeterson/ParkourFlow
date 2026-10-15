# AGENTS.md

Guidance for AI coding agents (and humans) working on ParkourFlow. Read [README.md](README.md) first for the project overview and architecture.

## Project state

Early scaffold. The core crate has the in-memory workflow model (configuration plus runtime state), the edge transport (credit-based bounded channels with overflow policies), and the node execution API (`runner.rs`). `parkourflow-files` holds the first runnable nodes: CSV Source and CSV Writer. The Tauri shell holds one `Workflow` and its `WorkflowState` in managed state, the canvas edits that workflow through Tauri commands, and nodes are started and stopped one at a time. There is no whole-graph start yet.

Current control-plane commands (`src-tauri/src/lib.rs`, wrapped in `ui/src/runtime.ts`): `list_node_types`, `runtime_version`, `get_workflow`, `node_statuses`, `connection_stats`, `add_node`, `remove_node`, `move_node`, `add_group`, `remove_group`, `connect` (optional `buffer_capacity` and `overflow_policy`), `disconnect`, `set_buffer_capacity`, `set_overflow_policy`, `set_property`, `list_device_options`, `start_node`, `stop_node`. Events (runtime → UI): `node-status` (`{ id, state }`), emitted on every node status change; `connection-stats` (every connection's `EdgeStats`, keyed by id), emitted when any of them changed since the last 250 ms sample.

Decisions so far:
- Edit commands go through `AppState::edit`, which applies the change to the `Workflow` and then calls `WorkflowState::reconcile`. Errors are returned to JS as strings (the `WorkflowError` message).
- The UI does not validate connections itself. Every edit is sent to the runtime and the canvas redraws from `get_workflow`, so the runtime stays the source of truth.
- Node positions are UI-only (not part of `Workflow`) until layout is added to the workflow format.
- Edge transport lives in `parkourflow-core/src/transport.rs` (see README → "Flow control" → "Implementation"). It uses only `std` sync primitives so the core stays scheduler-neutral; Tokio is a dev-dependency there, used only by tests. Drop counters live in `ConnectionBuffer` (runtime state), never on `Connection`.
- `OverflowPolicy` serializes as snake_case (`"block"`, `"drop_oldest"`, `"drop_newest"`, `"keep_latest"`).
- Node execution (README → "Node execution"): a node type that can run registers a `BuildRunner` in a `NodeRegistry`; the shell builds the registry in `registry()` (`src-tauri/src/lib.rs`). Each running node gets its own OS thread. `NodeRunner::run` receives a `NodeContext` of `Input`s and `Output`s that wait like `recv_blocking`/`send_blocking` but return once the node is cancelled, so runners never need their own cancel plumbing around edge waits. A runner declares `Pacing::Live` or `Pausable` (default) via `NodeRunner::pacing`, asked once after it is built.
- When a runner returns, its edges close: downstream drains and ends, upstream sends fail. Stopping a source therefore ends the pipeline below it, and a file pipeline stops on its own when the file is done. A restarted node reopens its idle edges (`Edge::attach_sender` / `attach_receiver`).
- `Record` (`parkourflow-core/src/record.rs`) is the payload of `Record` streams: shared `columns: Arc<[String]>` plus per-row `values`. The DNS Parser emits `Record`s, so CSV Writer takes `Record` input.
- CSV parsing is hand-written in `parkourflow-files/src/format.rs` (RFC 4180, `RowReader`/`RowWriter`) to avoid a dependency for now; the CSV nodes only use that interface, so a CSV crate can replace it in one file.

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
4. **Every edge is bounded, and demand flows upstream.** Consumers grant producers credit (free buffer slots); producers send no more than that. Each connection has an overflow policy (`Block` default, `DropOldest`, `DropNewest`, `KeepLatest`) for when a producer can't wait. No unbounded queues in the data plane. See README → "Flow control"; this design is decided, so don't propose pure push or pure pull.
   - Pausable producers (files, replay, decoders, most processors) wait for demand. Live sources (capture, camera, microphone, sockets) never block: when there is no capacity they drop by policy themselves.
   - Every drop is counted per connection and reported as telemetry. Never drop silently.
5. **Avoid copies.** Pass data by ownership transfer, borrowing, or shared reference (e.g. `Arc`) where practical. GPU resources pass as handles between compatible nodes.
6. **Don't force one scheduler.** Tokio is for I/O and control-plane work. Do not assume every high-throughput stream runs as an async task. DSP (fixed sample blocks) and GPU/video (resource-aware, explicitly synchronized) may need their own execution domains.
7. **Streams carry metadata.** Keep timestamps, sequence numbers, formats, dimensions, sample rates, and capture metadata with the payload. Do not drop them in transit.
8. **Configuration and runtime state stay separate.** `Workflow` (groups, nodes, ports, connections, buffer sizes) holds no runtime data. `WorkflowState` (node status, buffer contents) is keyed by workflow ids and synced with `WorkflowState::reconcile`. Don't put status, buffers, or other runtime data on config types.
9. **Status changes are pushed.** Node status transitions go through `WorkflowState::begin_start` / `finish_start` / `begin_stop` / `finish_run`, which enforce the allowed transitions, and every transition is emitted as a `node-status` event. The UI updates from events; don't add polling for status. Startup work and runners run off the command thread and must not hold the session lock.
10. **The runtime owns the lifecycle.** Graph construction, validation, startup, shutdown, cancellation, node failure handling, and config propagation live in Rust, not the UI.

## Node conventions

- A node declares its ports, port types, properties, defaults, category, and configuration schema as **metadata**. The UI renders nodes from this metadata.
- Prefer extending the metadata schema over writing node-specific React components. Add custom frontend code only when a node genuinely needs a bespoke inspector or visualization.
- **Configuration fields are typed properties** (`crates/parkourflow-core/src/property.rs`), declared on the `NodeDescriptor`. Use the most specific type: `file` (with `mode` and `extensions`), `directory`, or `device` instead of `string` for paths and devices, and `enum` for fixed choices. Mark `required` and `apply: live` where they apply. See README → "Node properties".
- Property values are configuration and live on `Node::values`. Validate type and range on edit (`Property::validate`); check outside-world conditions (file exists, device present) when the node starts, and report failures as `ERROR` status.
- Node code reads its values with `Node::config::<T>()` into its own typed struct. Don't pass raw `PropertyValue`s around.
- A new property type needs: a `PropertyType` variant, validation in `Property::validate`, a TS type in `ui/src/runtime.ts`, and a widget in `ui/src/Inspector.tsx`.
- The file picker uses the official `tauri-plugin-dialog` (`dialog:default` permission).
- Keep a node's processing code independent of the editor. A node must be runnable and testable without the UI.
- **A runnable node** implements `NodeRunner` (`crates/parkourflow-core/src/runner.rs`) and registers a builder with `NodeRegistry::register(type_id, build)` from its module crate's `register` function. The builder reads `Node::config::<T>()` and opens files or devices, so failures surface as start errors. `run` must return soon after cancellation: block only on `Input::recv`/`Output::send`, or check `NodeContext::is_cancelled` in other loops. Return `Ok` when the inputs end or `Output::send` fails (nobody is listening, or the node is stopping); return `Err` for real failures. Panics are caught and reported as `ERROR`.
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
