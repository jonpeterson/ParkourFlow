// Generic node renderer: draws any node type from its runtime metadata.
import { Handle, Position, type Node, type NodeProps } from "@xyflow/react";
import type { NodeDescriptor } from "./runtime";

export type MetadataNodeType = Node<{ descriptor: NodeDescriptor }, "metadata">;

export function MetadataNode({ data }: NodeProps<MetadataNodeType>) {
  const { descriptor } = data;
  return (
    <div className={`pf-node pf-node--${descriptor.kind}`}>
      <div className="pf-node__header">
        <span className="pf-node__label">{descriptor.label}</span>
        <span className="pf-node__category">{descriptor.category}</span>
      </div>
      <div className="pf-node__ports">
        <div>
          {descriptor.inputs.map((p) => (
            <div key={p.name} className="pf-port pf-port--in">
              <Handle type="target" position={Position.Left} id={p.name} />
              {p.name} <em>{p.stream_type}</em>
            </div>
          ))}
        </div>
        <div>
          {descriptor.outputs.map((p) => (
            <div key={p.name} className="pf-port pf-port--out">
              {p.name} <em>{p.stream_type}</em>
              <Handle type="source" position={Position.Right} id={p.name} />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
