// Typed wrappers around the runtime's Tauri commands (control plane only).
// Types mirror the Rust `Serialize` output in `parkourflow-core`.
import { invoke } from "@tauri-apps/api/core";

export type NodeKind = "source" | "processor" | "sink";

export interface Port {
  name: string;
  data_type: string;
}

export interface NodeDescriptor {
  type_id: string;
  label: string;
  kind: NodeKind;
  category: string;
  inputs: Port[];
  outputs: Port[];
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
}

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

export type NodeStatus = "STOPPED" | "STARTING" | "RUNNING" | "ERROR";

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
