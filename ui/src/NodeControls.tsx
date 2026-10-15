// Start/stop controls for a node, shared by the canvas card and the inspector.
import { createContext, useContext, useEffect, useRef } from "react";
import type { NodeId, NodeState } from "./runtime";

export interface NodeActions {
  /** Nodes with a start/stop request in flight; their buttons are disabled. */
  pending: ReadonlySet<NodeId>;
  start: (id: NodeId) => void;
  stop: (id: NodeId) => void;
  showError: (id: NodeId) => void;
}

export const NodeActionsContext = createContext<NodeActions>({
  pending: new Set(),
  start: () => {},
  stop: () => {},
  showError: () => {},
});

export function NodeControls({ id, state }: { id: NodeId; state?: NodeState }) {
  const { pending, start, stop, showError } = useContext(NodeActionsContext);
  if (!state) return null;
  const busy = pending.has(id);
  // `nodrag` keeps React Flow from treating button clicks as drags.
  const button = "pf-control nodrag";

  return (
    <div className="pf-controls">
      <span className={`pf-badge pf-badge--${state.status.toLowerCase()}`} title={state.error ?? undefined}>
        {state.status}
      </span>
      {state.status === "STOPPED" && (
        <button type="button" className={button} disabled={busy} onClick={() => start(id)}>
          Start
        </button>
      )}
      {state.status === "RUNNING" && (
        <button type="button" className={button} disabled={busy} onClick={() => stop(id)}>
          Stop
        </button>
      )}
      {state.status === "ERROR" && (
        <>
          <button type="button" className={`${button} pf-control--error`} onClick={() => showError(id)}>
            Details
          </button>
          <button type="button" className={button} disabled={busy} onClick={() => start(id)}>
            Retry
          </button>
        </>
      )}
    </div>
  );
}

/** Modal showing why a node is in ERROR, with a retry. */
export function ErrorDialog({
  id,
  name,
  state,
  onClose,
}: {
  id: NodeId;
  name: string;
  state?: NodeState;
  onClose: () => void;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  const { pending, start } = useContext(NodeActionsContext);

  useEffect(() => {
    ref.current?.showModal();
  }, []);

  // Close once the node leaves ERROR (e.g. after a retry).
  useEffect(() => {
    if (state?.status !== "ERROR") onClose();
  }, [state?.status, onClose]);

  return (
    <dialog ref={ref} className="pf-dialog" onClose={onClose} aria-labelledby="pf-dialog-title">
      <h2 id="pf-dialog-title">{name} failed to start</h2>
      <p className="pf-dialog__message">{state?.error ?? "Unknown error."}</p>
      <div className="pf-dialog__actions">
        <button type="button" className="pf-control" onClick={onClose}>
          Close
        </button>
        <button type="button" className="pf-control pf-control--primary" disabled={pending.has(id)} onClick={() => start(id)}>
          Retry
        </button>
      </div>
    </dialog>
  );
}
