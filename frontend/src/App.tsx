import { useState, useCallback } from "react";
import { fetchEventSource } from "@microsoft/fetch-event-source";
import { useNavigate } from "react-router-dom";
import { Property, PropertySlot } from "./types";
import { useAuth } from "./hooks/useAuth";
import UrlForm from "./components/UrlForm";
import LoadingState from "./components/LoadingState";
import ComparisonTable from "./components/ComparisonTable";

export default function App() {
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const [slots, setSlots] = useState<PropertySlot[]>([]);
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState<Property[]>([]);

  const handleSubmit = useCallback(async (urls: string[]) => {
    setLoading(true);
    setResults([]);

    const initialSlots: PropertySlot[] = urls.map((url) => ({
      url,
      status: "scraping",
      property: null,
      error: null,
    }));
    setSlots(initialSlots);

    const collected: (Property | null)[] = new Array(urls.length).fill(null);

    await fetchEventSource("/api/scrape", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ urls }),
      onmessage(ev) {
        if (ev.event === "property") {
          const data = JSON.parse(ev.data);
          const idx: number = data.index;
          const prop: Property = data.property;
          collected[idx] = prop;

          setSlots((prev) =>
            prev.map((s, i) =>
              i === idx ? { ...s, status: "done", property: prop } : s
            )
          );
        } else if (ev.event === "error") {
          const data = JSON.parse(ev.data);
          const idx: number = data.index;

          setSlots((prev) =>
            prev.map((s, i) =>
              i === idx ? { ...s, status: "error", error: data.error } : s
            )
          );
        } else if (ev.event === "done") {
          setResults(collected.filter((p): p is Property => p !== null));
          setLoading(false);
        }
      },
      onerror(err) {
        console.error("SSE error:", err);
        setLoading(false);
        throw err; // stop retrying
      },
    });
  }, []);

  return (
    <div className="app">
      <header className="app-header">
        <div className="app-header-top">
          <h1>ResiPrice</h1>
          <div className="app-header-user">
            <span className="app-username">{user?.username}</span>
            <button
              className="btn-secondary btn-sm"
              onClick={() => {
                logout();
                navigate("/");
              }}
            >
              Sign Out
            </button>
          </div>
        </div>
        <p>Compare apartment pricing across communities in real time.</p>
      </header>

      <main>
        <UrlForm onSubmit={handleSubmit} loading={loading} />

        {slots.length > 0 && <LoadingState slots={slots} />}

        {results.length > 0 && <ComparisonTable properties={results} />}
      </main>

      <footer>
        <p>ResiPrice scrapes publicly available pricing data from apartment community websites.</p>
      </footer>
    </div>
  );
}
