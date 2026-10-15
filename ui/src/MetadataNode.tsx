// Generic node renderer: draws any node from its runtime metadata.
import { Handle, Position, type Node, type NodeProps } from "@xyflow/react";
import type { NodeState, WorkflowNode } from "./runtime";

export type MetadataNodeType = Node<
  { node: WorkflowNode; category: string; state?: NodeState },
  "metadata"
>;

export function MetadataNode({ data }: NodeProps<MetadataNodeType>) {
  const { node, category, state } = data;
  return (
    <div className={`pf-node pf-node--${node.kind}`}>
      <div className="pf-node__header">
        <span className="pf-node__label">{node.name}</span>
        <span className="pf-node__category">{category}</span>
      </div>
      <div className="pf-node__ports">
        <div>
          {node.inputs.map((p) => (
            <div key={p.name} className="pf-port pf-port--in">
              <Handle type="target" position={Position.Left} id={p.name} />
              {p.name} <em>{p.data_type}</em>
            </div>
          ))}
        </div>
        <div>
          {node.outputs.map((p) => (
            <div key={p.name} className="pf-port pf-port--out">
              {p.name} <em>{p.data_type}</em>
              <Handle type="source" position={Position.Right} id={p.name} />
            </div>
          ))}
        </div>
      </div>
      {state && (
        <div className="pf-node__status" title={state.error ?? undefined}>
          {state.status}
        </div>
      )}
    </div>
  );
}
