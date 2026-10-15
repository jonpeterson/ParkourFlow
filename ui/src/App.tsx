import { useCallback, useEffect, useRef, useState } from "react";
import { isTauri } from "@tauri-apps/api/core";
import {
  Background,
  Controls,
  ReactFlow,
  useEdgesState,
  useNodesState,
  type Connection,
  type Edge,
  type OnBeforeDelete,
  type XYPosition,
} from "@xyflow/react";
import { Inspector } from "./Inspector";
import { MetadataNode, type MetadataNodeType } from "./MetadataNode";
import * as runtime from "./runtime";
import type { NodeDescriptor, NodeState, Workflow } from "./runtime";

const nodeTypes = { metadata: MetadataNode };

// Node positions are UI-only for now; new nodes get a staggered spot.
function defaultPosition(index: number): XYPosition {
  return { x: 80 + (index % 4) * 300, y: 80 + Math.floor(index / 4) * 200 + (index % 4) * 30 };
}

function toFlowNodes(
  workflow: Workflow,
  statuses: Record<string, NodeState>,
  descriptors: NodeDescriptor[],
  previous: MetadataNodeType[],
): MetadataNodeType[] {
  const prev = new Map(previous.map((n) => [n.id, n]));
  return Object.values(workflow.nodes).map((node, i) => {
    const id = String(node.id);
    const old = prev.get(id);
    return {
      id,
      type: "metadata",
      position: old?.position ?? defaultPosition(i),
      selected: old?.selected,
      data: {
        node,
        category: descriptors.find((d) => d.type_id === node.type_id)?.category ?? "",
        state: statuses[id],
      },
    };
  });
}

function toFlowEdges(workflow: Workflow): Edge[] {
  return Object.values(workflow.connections).map((c) => ({
    id: String(c.id),
    source: String(c.from.node),
    sourceHandle: c.from.port,
    target: String(c.to.node),
    targetHandle: c.to.port,
    label: `buffer ${c.buffer_capacity}`,
  }));
}

export default function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState<MetadataNodeType>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [descriptors, setDescriptors] = useState<NodeDescriptor[]>([]);
  const [version, setVersion] = useState<string>();
  const [unavailable, setUnavailable] = useState<string>();
  const [error, setError] = useState<string>();
  const descriptorsRef = useRef<NodeDescriptor[]>([]);

  // Re-read the workflow and node status from the runtime and redraw.
  const refresh = useCallback(async () => {
    const [workflow, statuses] = await Promise.all([runtime.getWorkflow(), runtime.nodeStatuses()]);
    setNodes((prev) => toFlowNodes(workflow, statuses, descriptorsRef.current, prev));
    setEdges(toFlowEdges(workflow));
  }, [setNodes, setEdges]);

  // Run a backend edit, show its error if it fails, and redraw either way.
  const edit = useCallback(
    async (op: () => Promise<unknown>) => {
      try {
        await op();
        setError(undefined);
      } catch (e) {
        setError(String(e));
      }
      await refresh().catch((e) => setError(String(e)));
    },
    [refresh],
  );

  useEffect(() => {
    if (!isTauri()) {
      setUnavailable("not running inside the Tauri shell (use `pnpm tauri dev`)");
      return;
    }
    Promise.all([runtime.listNodeTypes(), runtime.runtimeVersion()])
      .then(([types, v]) => {
        descriptorsRef.current = types;
        setDescriptors(types);
        setVersion(v);
        return refresh();
      })
      .catch((e) => setUnavailable(String(e)));
  }, [refresh]);

  const selected = nodes.filter((n) => n.selected);
  const inspected = selected.length === 1 ? selected[0].data : undefined;
  const redraw = useCallback(() => refresh().catch((e) => setError(String(e))), [refresh]);

  const addNode = (d: NodeDescriptor) =>
    edit(() => runtime.addNode(d.type_id, `${d.label} ${nodes.filter((n) => n.data.node.type_id === d.type_id).length + 1}`));

  // The runtime validates connections (port types, one edge per input), so
  // every attempt goes to the backend and any rejection is shown.
  const onConnect = useCallback(
    (c: Connection) =>
      edit(() =>
        runtime.connect(
          { node: Number(c.source), port: c.sourceHandle ?? "" },
          { node: Number(c.target), port: c.targetHandle ?? "" },
        ),
      ),
    [edit],
  );

  // Deletions go through the runtime; the canvas redraws from its answer
  // instead of deleting locally. Edges first: removing a node also removes
  // its connections.
  const onBeforeDelete: OnBeforeDelete<MetadataNodeType, Edge> = useCallback(
    async ({ nodes: deadNodes, edges: deadEdges }) => {
      const doomed = new Set(deadNodes.map((n) => n.id));
      await edit(async () => {
        for (const e of deadEdges) {
          if (!doomed.has(e.source) && !doomed.has(e.target)) await runtime.disconnect(Number(e.id));
        }
        for (const n of deadNodes) await runtime.removeNode(Number(n.id));
      });
      return false;
    },
    [edit],
  );

  return (
    <div className="pf-app">
      <header className="pf-toolbar">
        <strong>ParkourFlow</strong>
        <span className="pf-status">
          {unavailable ? `Runtime unavailable: ${unavailable}` : version ? `runtime v${version}` : "connecting…"}
        </span>
        {error && (
          <span className="pf-error" role="alert">
            {error}
            <button type="button" onClick={() => setError(undefined)} aria-label="Dismiss">
              ×
            </button>
          </span>
        )}
      </header>
      <div className="pf-body">
        <aside className="pf-palette">
          <h2>Add node</h2>
          {descriptors.map((d) => (
            <button
              key={d.type_id}
              type="button"
              className={`pf-palette__item pf-node--${d.kind}`}
              onClick={() => addNode(d)}
            >
              <span>{d.label}</span>
              <span className="pf-node__category">{d.category}</span>
            </button>
          ))}
        </aside>
        <main className="pf-canvas">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onBeforeDelete={onBeforeDelete}
            fitView
          >
            <Background />
            <Controls />
          </ReactFlow>
        </main>
        <Inspector node={inspected?.node} state={inspected?.state} onChanged={redraw} />
      </div>
    </div>
  );
}
