import { useState } from "react";
import CommunityInput from "./CommunityInput";
import CsvImportButton from "./CsvImportButton";

interface CommunitySelection {
  url: string;
  displayName: string | null;
}

interface Props {
  onSubmit: (urls: string[]) => void;
  loading: boolean;
  token: string | null;
}

export default function UrlForm({ onSubmit, loading, token }: Props) {
  const [selections, setSelections] = useState<CommunitySelection[]>([
    { url: "", displayName: null },
  ]);

  const addRow = () => {
    if (selections.length < 4)
      setSelections([...selections, { url: "", displayName: null }]);
  };

  const removeRow = (i: number) => {
    setSelections(selections.filter((_, idx) => idx !== i));
  };

  const updateRow = (i: number, url: string, displayName: string | null) => {
    const next = [...selections];
    next[i] = { url, displayName };
    setSelections(next);
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const valid = selections
      .map((s) => s.url.trim())
      .filter((u) => u.length > 0);
    if (valid.length > 0) onSubmit(valid);
  };

  return (
    <form className="url-form" onSubmit={handleSubmit}>
      <div className="form-header">
        <div>
          <h2>Compare apartment communities</h2>
          <p className="form-subtitle">
            Search by name or paste a URL for up to 4 communities.
          </p>
        </div>
        <CsvImportButton token={token} disabled={loading} />
      </div>

      <div className="url-inputs">
        {selections.map((sel, i) => (
          <div key={i} className="url-row">
            <span className="url-label">#{i + 1}</span>
            <CommunityInput
              value={sel.url}
              displayName={sel.displayName}
              onChange={(url, name) => updateRow(i, url, name)}
              token={token}
              disabled={loading}
            />
            {selections.length > 1 && (
              <button
                type="button"
                className="btn-remove"
                onClick={() => removeRow(i)}
                disabled={loading}
              >
                x
              </button>
            )}
          </div>
        ))}
      </div>

      <div className="form-actions">
        {selections.length < 4 && (
          <button
            type="button"
            className="btn-secondary"
            onClick={addRow}
            disabled={loading}
          >
            + Add Community
          </button>
        )}
        <button type="submit" className="btn-primary" disabled={loading}>
          {loading ? "Scraping..." : "Compare Pricing"}
        </button>
      </div>
    </form>
  );
}
