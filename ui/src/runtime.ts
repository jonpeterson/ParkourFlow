// Typed wrappers around the runtime's Tauri commands (control plane only).
import { invoke } from "@tauri-apps/api/core";

export type NodeKind = "source" | "processor" | "sink";

export interface PortDescriptor {
  name: string;
  stream_type: string;
}

export interface NodeDescriptor {
  id: string;
  label: string;
  kind: NodeKind;
  category: string;
  inputs: PortDescriptor[];
  outputs: PortDescriptor[];
}

export const listNodeTypes = () => invoke<NodeDescriptor[]>("list_node_types");
export const runtimeVersion = () => invoke<string>("runtime_version");
