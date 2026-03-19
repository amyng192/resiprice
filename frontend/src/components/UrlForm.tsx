import { useState } from "react";

interface Props {
  onSubmit: (urls: string[]) => void;
  loading: boolean;
}

export default function UrlForm({ onSubmit, loading }: Props) {
  const [urls, setUrls] = useState<string[]>([""]);

  const addUrl = () => {
    if (urls.length < 4) setUrls([...urls, ""]);
  };

  const removeUrl = (i: number) => {
    setUrls(urls.filter((_, idx) => idx !== i));
  };

  const updateUrl = (i: number, value: string) => {
    const next = [...urls];
    next[i] = value;
    setUrls(next);
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const valid = urls.filter((u) => u.trim().length > 0);
    if (valid.length > 0) onSubmit(valid);
  };

  return (
    <form className="url-form" onSubmit={handleSubmit}>
      <h2>Enter apartment community URLs</h2>
      <p className="form-subtitle">
        Paste the floor plans or availability page URL for up to 4 communities.
      </p>

      <div className="url-inputs">
        {urls.map((url, i) => (
          <div key={i} className="url-row">
            <span className="url-label">#{i + 1}</span>
            <input
              type="url"
              value={url}
              onChange={(e) => updateUrl(i, e.target.value)}
              placeholder="https://community-name.com/floor-plans/"
              required
              disabled={loading}
            />
            {urls.length > 1 && (
              <button
                type="button"
                className="btn-remove"
                onClick={() => removeUrl(i)}
                disabled={loading}
              >
                x
              </button>
            )}
          </div>
        ))}
      </div>

      <div className="form-actions">
        {urls.length < 4 && (
          <button
            type="button"
            className="btn-secondary"
            onClick={addUrl}
            disabled={loading}
          >
            + Add URL
          </button>
        )}
        <button type="submit" className="btn-primary" disabled={loading}>
          {loading ? "Scraping..." : "Compare Pricing"}
        </button>
      </div>
    </form>
  );
}
