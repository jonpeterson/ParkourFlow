// Shows a node's latest item (its preview) as a column/value table.
import type { PreviewSnapshot } from "./runtime";

export function PreviewView({ snapshot, maxRows }: { snapshot: PreviewSnapshot; maxRows?: number }) {
  const { latest, received, sequence } = snapshot;
  const meta = (
    <div className="pf-preview__meta">
      {received.toLocaleString()} received · #{sequence}
    </div>
  );
  if (latest.kind !== "record") {
    return (
      <div className="pf-preview nodrag">
        {meta}
        <p className="pf-preview__empty">No preview for this data type.</p>
      </div>
    );
  }

  const width = Math.max(latest.columns.length, latest.values.length);
  const rows = Array.from({ length: width }, (_, i) => [latest.columns[i] ?? String(i + 1), latest.values[i] ?? ""]);
  const shown = maxRows === undefined ? rows : rows.slice(0, maxRows);
  const hidden = rows.length - shown.length + latest.omitted;

  return (
    // `nodrag` lets you select and copy text without moving the node.
    <div className="pf-preview nodrag">
      {meta}
      <table>
        <tbody>
          {shown.map(([column, value], i) => (
            <tr key={i}>
              <th scope="row" title={column}>
                {column}
              </th>
              <td title={value}>{value}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {hidden > 0 && <div className="pf-preview__meta">+{hidden} more columns</div>}
    </div>
  );
}
