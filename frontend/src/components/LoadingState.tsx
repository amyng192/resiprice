import { PropertySlot } from "../types";

interface Props {
  slots: PropertySlot[];
}

export default function LoadingState({ slots }: Props) {
  return (
    <div className="loading-state">
      {slots.map((slot, i) => (
        <div key={i} className={`loading-card ${slot.status}`}>
          <div className="loading-indicator">
            {slot.status === "scraping" && <div className="spinner" />}
            {slot.status === "done" && <span className="check">&#10003;</span>}
            {slot.status === "error" && <span className="error-icon">!</span>}
          </div>
          <div className="loading-info">
            <div className="loading-url">{slot.url}</div>
            <div className="loading-status">
              {slot.status === "scraping" && "Scraping... this may take up to 60 seconds"}
              {slot.status === "done" &&
                `${slot.property!.name} — ${slot.property!.available_count} units found`}
              {slot.status === "error" && slot.error}
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
