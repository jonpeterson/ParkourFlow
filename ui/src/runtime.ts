// Typed wrappers around the runtime's Tauri commands (control plane only).
// Types mirror the Rust `Serialize` output in `parkourflow-core`.
import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";

export type NodeKind = "source" | "processor" | "sink";

export interface Port {
  name: string;
  data_type: string;
}

export type FileMode = "open" | "save";

/** The kind of value a property holds; decides the inspector widget. */
export type PropertyType =
  | { kind: "bool" }
  | { kind: "integer"; min: number | null; max: number | null }
  | { kind: "float"; min: number | null; max: number | null; step: number | null }
  | { kind: "string"; multiline: boolean }
  | { kind: "enum"; options: string[] }
  | { kind: "file"; mode: FileMode; extensions: string[] }
  | { kind: "directory" }
  | { kind: "device"; class: string };

/** File, directory, device, and enum values are strings. */
export type PropertyValue = boolean | number | string;

/** A configuration field declared by a node type. */
export interface Property {
  name: string;
  label: string;
  description?: string;
  type: PropertyType;
  default: PropertyValue | null;
  required: boolean;
  /** `live`: applied while running. `restart`: the node must restart. */
  apply: "live" | "restart";
}

export interface DeviceOption {
  id: string;
  label: string;
}

export interface NodeDescriptor {
  type_id: string;
  label: string;
  kind: NodeKind;
  category: string;
  inputs: Port[];
  outputs: Port[];
  properties: Property[];
}

// Ids serialize as plain numbers, and as string keys in the workflow's maps.
export type GroupId = number;
export type NodeId = number;
export type ConnectionId = number;

export interface PortRef {
  node: NodeId;
  port: string;
}

export interface Group {
  id: GroupId;
  name: string;
  parent: GroupId | null;
  groups: GroupId[];
  nodes: NodeId[];
}

export interface WorkflowNode {
  id: NodeId;
  group: GroupId;
  name: string;
  type_id: string;
  kind: NodeKind;
  inputs: Port[];
  outputs: Port[];
  properties: Property[];
  /** Current values; properties with no value or default are absent. */
  values: Record<string, PropertyValue>;
}

/** Required properties with no value (or an empty string). Mirrors `Node::missing_required`. */
export const missingRequired = (node: WorkflowNode) =>
  node.properties.filter((p) => p.required && (node.values[p.name] ?? "") === "").map((p) => p.name);

/** What a connection does when an item arrives and its buffer is full. */
export type OverflowPolicy = "block" | "drop_oldest" | "drop_newest" | "keep_latest";

export interface Connection {
  id: ConnectionId;
  from: PortRef;
  to: PortRef;
  buffer_capacity: number;
  overflow_policy: OverflowPolicy;
}

export interface Workflow {
  name: string;
  root: GroupId;
  groups: Record<string, Group>;
  nodes: Record<string, WorkflowNode>;
  connections: Record<string, Connection>;
  next_id: number;
}

export type NodeStatus = "STOPPED" | "STARTING" | "RUNNING" | "STOPPING" | "ERROR";

export interface NodeState {
  status: NodeStatus;
  error: string | null;
}

/** Telemetry for one connection. `capacity` is 1 under `keep_latest`. */
export interface EdgeStats {
  queue_depth: number;
  capacity: number;
  dropped: number;
}

export const listNodeTypes = () => invoke<NodeDescriptor[]>("list_node_types");
export const runtimeVersion = () => invoke<string>("runtime_version");

export const getWorkflow = () => invoke<Workflow>("get_workflow");
export const nodeStatuses = () => invoke<Record<string, NodeState>>("node_statuses");
export const connectionStats = () => invoke<Record<string, EdgeStats>>("connection_stats");

/** Adds a node to `group`, or to the root group when `group` is null. */
export const addNode = (typeId: string, name: string, group: GroupId | null = null) =>
  invoke<NodeId>("add_node", { typeId, name, group });
export const removeNode = (id: NodeId) => invoke<void>("remove_node", { id });
export const moveNode = (id: NodeId, group: GroupId) => invoke<void>("move_node", { id, group });

export const addGroup = (parent: GroupId, name: string) => invoke<GroupId>("add_group", { parent, name });
export const removeGroup = (id: GroupId) => invoke<void>("remove_group", { id });

/**
 * Connects an output port to an input port. Omit `bufferCapacity` and
 * `overflowPolicy` for the runtime defaults (64, `block`).
 */
export const connect = (
  from: PortRef,
  to: PortRef,
  bufferCapacity: number | null = null,
  overflowPolicy: OverflowPolicy | null = null,
) => invoke<ConnectionId>("connect", { from, to, bufferCapacity, overflowPolicy });
export const disconnect = (id: ConnectionId) => invoke<void>("disconnect", { id });
export const setBufferCapacity = (id: ConnectionId, capacity: number) =>
  invoke<void>("set_buffer_capacity", { id, capacity });
export const setOverflowPolicy = (id: ConnectionId, policy: OverflowPolicy) =>
  invoke<void>("set_overflow_policy", { id, policy });

/** Sets a node property. `null` resets it to its default, or clears it if it has none. */
export const setProperty = (id: NodeId, name: string, value: PropertyValue | null) =>
  invoke<void>("set_property", { id, name, value });
/** Lists the devices currently available for a device property's class. */
export const listDeviceOptions = (deviceClass: string) =>
  invoke<DeviceOption[]>("list_device_options", { class: deviceClass });

/**
 * Starts a STOPPED or ERROR node. Resolves once it is STARTING; RUNNING or
 * ERROR, and STOPPED when the node finishes on its own, arrive via `onNodeStatus`.
 */
export const startNode = (id: NodeId) => invoke<void>("start_node", { id });
/** Stops a RUNNING node. Resolves once it is STOPPING; STOPPED arrives via `onNodeStatus`. */
export const stopNode = (id: NodeId) => invoke<void>("stop_node", { id });

export interface NodeStatusEvent {
  id: NodeId;
  state: NodeState;
}

/** Calls `handler` whenever a node's status changes. Resolves to an unsubscribe function. */
export const onNodeStatus = (handler: (event: NodeStatusEvent) => void): Promise<UnlistenFn> =>
  listen<NodeStatusEvent>("node-status", (e) => handler(e.payload));

/**
 * Calls `handler` with every connection's stats (keyed by connection id)
 * whenever any of them changes. Resolves to an unsubscribe function.
 */
export const onConnectionStats = (
  handler: (stats: Record<string, EdgeStats>) => void,
): Promise<UnlistenFn> => listen<Record<string, EdgeStats>>("connection-stats", (e) => handler(e.payload));
