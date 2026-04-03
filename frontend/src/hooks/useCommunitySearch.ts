import { useState, useEffect, useRef } from "react";
import { Community } from "../types";

export function useCommunitySearch(query: string, token: string | null) {
  const [results, setResults] = useState<Community[]>([]);
  const [loading, setLoading] = useState(false);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    // Skip search for short queries or URLs
    if (!token || query.length < 2 || /^https?:\/\//i.test(query)) {
      setResults([]);
      setLoading(false);
      return;
    }

    const timer = setTimeout(async () => {
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      setLoading(true);
      try {
        const res = await fetch(
          `/api/communities/search?q=${encodeURIComponent(query)}`,
          {
            headers: { Authorization: `Bearer ${token}` },
            signal: controller.signal,
          }
        );
        if (res.ok) {
          const data = await res.json();
          setResults(data.communities);
        }
      } catch (e) {
        if (e instanceof DOMException && e.name === "AbortError") return;
      } finally {
        setLoading(false);
      }
    }, 300);

    return () => {
      clearTimeout(timer);
      abortRef.current?.abort();
    };
  }, [query, token]);

  return { results, loading };
}
