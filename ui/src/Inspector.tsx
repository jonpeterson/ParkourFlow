// Generic inspector: renders a form for any node from its property schema.
import { useEffect, useState, type KeyboardEvent } from "react";
import { open, save } from "@tauri-apps/plugin-dialog";
import * as runtime from "./runtime";
import type { DeviceOption, NodeState, Property, PropertyValue, WorkflowNode } from "./runtime";

interface InspectorProps {
  node?: WorkflowNode;
  state?: NodeState;
  /** Called after every edit attempt so the app can redraw from the runtime. */
  onChanged: () => Promise<void>;
}

export function Inspector({ node, state, onChanged }: InspectorProps) {
  if (!node) {
    return (
      <aside className="pf-inspector">
        <p className="pf-inspector__empty">Select a node to edit its properties.</p>
      </aside>
    );
  }

  const missing = runtime.missingRequired(node);
  const running = state?.status === "RUNNING" || state?.status === "STARTING";

  return (
    <aside className="pf-inspector">
      <header className="pf-inspector__header">
        <h2>{node.name}</h2>
        <span className="pf-node__category">{node.type_id}</span>
        {state && <span className={`pf-badge pf-badge--${state.status.toLowerCase()}`}>{state.status}</span>}
        {state?.error && <p className="pf-field__error">{state.error}</p>}
        {missing.length > 0 && <p className="pf-inspector__missing">Set {missing.join(", ")} before starting.</p>}
      </header>
      {node.properties.length === 0 && <p className="pf-inspector__empty">This node has no properties.</p>}
      {node.properties.map((p) => (
        <PropertyField
          key={`${node.id}:${p.name}`}
          nodeId={node.id}
          property={p}
          value={node.values[p.name]}
          running={running}
          onChanged={onChanged}
        />
      ))}
    </aside>
  );
}

interface FieldProps {
  nodeId: number;
  property: Property;
  value: PropertyValue | undefined;
  running: boolean;
  onChanged: () => Promise<void>;
}

function PropertyField({ nodeId, property, value, running, onChanged }: FieldProps) {
  const [error, setError] = useState<string>();

  // Send a value to the runtime, which validates it. `null` resets to default.
  const commit = async (next: PropertyValue | null) => {
    try {
      await runtime.setProperty(nodeId, property.name, next);
      setError(undefined);
    } catch (e) {
      setError(String(e));
    }
    await onChanged();
  };

  const isDefault = JSON.stringify(value ?? null) === JSON.stringify(property.default);
  const id = `pf-prop-${nodeId}-${property.name}`;

  return (
    <div className="pf-field">
      <div className="pf-field__label">
        <label htmlFor={id}>
          {property.label}
          {property.required && <span className="pf-field__required"> *</span>}
        </label>
        {!isDefault && (
          <button type="button" className="pf-link" onClick={() => commit(null)}>
            {property.default === null ? "Clear" : "Reset"}
          </button>
        )}
      </div>
      <PropertyInput id={id} property={property} value={value} commit={commit} />
      {property.description && <p className="pf-field__hint">{property.description}</p>}
      {running && property.apply === "restart" && <p className="pf-field__hint">Changing this restarts the node.</p>}
      {error && <p className="pf-field__error">{error}</p>}
    </div>
  );
}

interface InputProps {
  id: string;
  property: Property;
  value: PropertyValue | undefined;
  commit: (value: PropertyValue | null) => Promise<void>;
}

function PropertyInput({ id, property, value, commit }: InputProps) {
  const type = property.type;
  switch (type.kind) {
    case "bool":
      return <input id={id} type="checkbox" checked={value === true} onChange={(e) => commit(e.target.checked)} />;

    case "integer":
    case "float":
      return (
        <DraftInput
          id={id}
          value={value}
          inputType="number"
          min={type.min ?? undefined}
          max={type.max ?? undefined}
          step={type.kind === "float" ? (type.step ?? "any") : 1}
          // Empty resets; anything else goes to the runtime to validate.
          parse={(s) => (s.trim() === "" ? null : Number(s))}
          commit={commit}
        />
      );

    case "string":
      return <DraftInput id={id} value={value} multiline={type.multiline} parse={(s) => s} commit={commit} />;

    case "enum":
      return (
        <select id={id} value={typeof value === "string" ? value : ""} onChange={(e) => commit(e.target.value)}>
          {value === undefined && <option value="">Choose…</option>}
          {type.options.map((o) => (
            <option key={o} value={o}>
              {o}
            </option>
          ))}
        </select>
      );

    case "file":
    case "directory": {
      const browse = async () => {
        const filters = type.kind === "file" && type.extensions.length > 0
          ? [{ name: type.extensions.map((x) => `.${x}`).join(", "), extensions: type.extensions }]
          : undefined;
        const current = typeof value === "string" && value !== "" ? value : undefined;
        const picked =
          type.kind === "file" && type.mode === "save"
            ? await save({ defaultPath: current, filters })
            : await open({ defaultPath: current, filters, directory: type.kind === "directory", multiple: false });
        if (typeof picked === "string") await commit(picked);
      };
      return (
        <div className="pf-field__row">
          <DraftInput id={id} value={value} parse={(s) => s} commit={commit} placeholder="No file chosen" />
          <button type="button" onClick={browse}>
            Browse…
          </button>
        </div>
      );
    }

    case "device":
      return <DeviceInput id={id} deviceClass={type.class} value={value} commit={commit} />;
  }
}

function DeviceInput({
  id,
  deviceClass,
  value,
  commit,
}: {
  id: string;
  deviceClass: string;
  value: PropertyValue | undefined;
  commit: (value: PropertyValue | null) => Promise<void>;
}) {
  const [options, setOptions] = useState<DeviceOption[]>();
  const refresh = () => runtime.listDeviceOptions(deviceClass).then(setOptions, () => setOptions([]));
  useEffect(() => {
    runtime.listDeviceOptions(deviceClass).then(setOptions, () => setOptions([]));
  }, [deviceClass]);

  if (options === undefined) return <span className="pf-field__hint">Looking for devices…</span>;

  // No enumeration for this class (yet): fall back to typing an id.
  if (options.length === 0) {
    return (
      <>
        <DraftInput id={id} value={value} parse={(s) => s} commit={commit} placeholder="Device id" />
        <p className="pf-field__hint">
          No devices detected.{" "}
          <button type="button" className="pf-link" onClick={refresh}>
            Rescan
          </button>
        </p>
      </>
    );
  }

  return (
    <div className="pf-field__row">
      <select id={id} value={typeof value === "string" ? value : ""} onChange={(e) => commit(e.target.value)}>
        <option value="">Choose…</option>
        {options.map((o) => (
          <option key={o.id} value={o.id}>
            {o.label}
          </option>
        ))}
      </select>
      <button type="button" onClick={refresh}>
        Rescan
      </button>
    </div>
  );
}

/**
 * A text or number input that keeps a local draft and commits it on blur or
 * Enter, so the runtime isn't called on every keystroke.
 */
function DraftInput({
  id,
  value,
  parse,
  commit,
  inputType = "text",
  multiline = false,
  placeholder,
  min,
  max,
  step,
}: {
  id: string;
  value: PropertyValue | undefined;
  parse: (s: string) => PropertyValue | null;
  commit: (value: PropertyValue | null) => Promise<void>;
  inputType?: "text" | "number";
  multiline?: boolean;
  placeholder?: string;
  min?: number;
  max?: number;
  step?: number | "any";
}) {
  const stored = value === undefined ? "" : String(value);
  const [draft, setDraft] = useState(stored);
  useEffect(() => setDraft(stored), [stored]);

  const save = () => {
    if (draft !== stored) void commit(parse(draft));
  };
  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === "Enter" && !multiline) save();
    if (e.key === "Escape") setDraft(stored);
  };

  if (multiline) {
    return (
      <textarea
        id={id}
        value={draft}
        rows={3}
        placeholder={placeholder}
        onChange={(e) => setDraft(e.target.value)}
        onBlur={save}
        onKeyDown={onKeyDown}
      />
    );
  }
  return (
    <input
      id={id}
      type={inputType}
      value={draft}
      placeholder={placeholder}
      min={min}
      max={max}
      step={step}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={save}
      onKeyDown={onKeyDown}
    />
  );
}
