import { useCallback, useEffect, useState } from "react";
import { isTauri } from "@tauri-apps/api/core";
import {
  Background,
  Controls,
  ReactFlow,
  addEdge,
  useEdgesState,
  useNodesState,
  type Connection,
  type Edge,
  type IsValidConnection,
} from "@xyflow/react";
import { MetadataNode, type MetadataNodeType } from "./MetadataNode";
import { listNodeTypes, runtimeVersion, type NodeDescriptor } from "./runtime";

const nodeTypes = { metadata: MetadataNode };

function portType(nodes: MetadataNodeType[], nodeId: string, handle: string | null, side: "inputs" | "outputs") {
  const node = nodes.find((n) => n.id === nodeId);
  return node?.data.descriptor[side].find((p) => p.name === handle)?.stream_type;
}

// Lay the built-in nodes out left to right as a demo graph.
function initialNodes(descriptors: NodeDescriptor[]): MetadataNodeType[] {
  return descriptors.map((descriptor, i) => ({
    id: `${descriptor.id}-${i}`,
    type: "metadata",
    position: { x: 80 + i * 300, y: 160 },
    data: { descriptor },
  }));
}

export default function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState<MetadataNodeType>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [version, setVersion] = useState<string>();
  const [error, setError] = useState<string>();

  useEffect(() => {
    if (!isTauri()) {
      setError("not running inside the Tauri shell (use `pnpm tauri dev`)");
      return;
    }
    Promise.all([listNodeTypes(), runtimeVersion()])
      .then(([descriptors, v]) => {
        setNodes(initialNodes(descriptors));
        setVersion(v);
      })
      .catch((e) => setError(String(e)));
  }, [setNodes]);

  // Typed ports: only allow output → input connections with matching stream types.
  const isValidConnection: IsValidConnection = useCallback(
    (c) => {
      const out = portType(nodes, c.source, c.sourceHandle ?? null, "outputs");
      const inp = portType(nodes, c.target, c.targetHandle ?? null, "inputs");
      return out !== undefined && out === inp;
    },
    [nodes],
  );

  const onConnect = useCallback((c: Connection) => setEdges((es) => addEdge(c, es)), [setEdges]);

  return (
    <div className="pf-app">
      <header className="pf-toolbar">
        <strong>ParkourFlow</strong>
        <span className="pf-status">
          {error ? `Runtime unavailable: ${error}` : version ? `runtime v${version}` : "connecting…"}
        </span>
      </header>
      <main className="pf-canvas">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          isValidConnection={isValidConnection}
          fitView
        >
          <Background />
          <Controls />
        </ReactFlow>
      </main>
    </div>
  );
}
