// Typed wrappers around the runtime's Tauri commands (control plane only).
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

export const listNodeTypes = () => invoke<NodeDescriptor[]>("list_node_types");
export const runtimeVersion = () => invoke<string>("runtime_version");
