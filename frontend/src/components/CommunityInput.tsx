import { useState, useRef, useEffect } from "react";
import { useCommunitySearch } from "../hooks/useCommunitySearch";
import { Community } from "../types";

interface Props {
  value: string;
  displayName: string | null;
  onChange: (url: string, displayName: string | null) => void;
  token: string | null;
  disabled: boolean;
}

export default function CommunityInput({
  value,
  displayName,
  onChange,
  token,
  disabled,
}: Props) {
  const [inputText, setInputText] = useState(displayName || value);
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const listRef = useRef<HTMLUListElement>(null);

  const { results, loading } = useCommunitySearch(inputText, token);
  const showDropdown = open && results.length > 0;

  // Sync external value changes
  useEffect(() => {
    setInputText(displayName || value);
  }, [value, displayName]);

  // Close on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const handleSelect = (community: Community) => {
    onChange(community.url, community.name);
    setInputText(community.name);
    setOpen(false);
    setActiveIndex(-1);
  };

  const handleInputChange = (text: string) => {
    setInputText(text);
    setActiveIndex(-1);
    setOpen(true);

    // If it looks like a URL, pass it through directly
    if (/^https?:\/\//i.test(text)) {
      onChange(text, null);
    } else {
      // Clear the URL when typing a name (will be set when selecting)
      onChange("", text || null);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (!showDropdown) return;

    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActiveIndex((prev) => Math.min(prev + 1, results.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActiveIndex((prev) => Math.max(prev - 1, 0));
    } else if (e.key === "Enter" && activeIndex >= 0) {
      e.preventDefault();
      handleSelect(results[activeIndex]);
    } else if (e.key === "Escape") {
      setOpen(false);
    }
  };

  // Scroll active item into view
  useEffect(() => {
    if (activeIndex >= 0 && listRef.current) {
      const item = listRef.current.children[activeIndex] as HTMLElement;
      item?.scrollIntoView({ block: "nearest" });
    }
  }, [activeIndex]);

  return (
    <div className="community-input-wrapper" ref={wrapperRef}>
      <input
        type="text"
        value={inputText}
        onChange={(e) => handleInputChange(e.target.value)}
        onFocus={() => setOpen(true)}
        onKeyDown={handleKeyDown}
        placeholder="Community name or paste URL..."
        disabled={disabled}
        autoComplete="off"
      />
      {displayName && (
        <button
          type="button"
          className="community-clear"
          onClick={() => {
            onChange("", null);
            setInputText("");
            setOpen(false);
          }}
          disabled={disabled}
          title="Clear selection"
        >
          x
        </button>
      )}
      {showDropdown && (
        <ul className="community-dropdown" ref={listRef}>
          {results.map((c, i) => (
            <li
              key={c.id}
              className={i === activeIndex ? "active" : ""}
              onMouseDown={() => handleSelect(c)}
              onMouseEnter={() => setActiveIndex(i)}
            >
              <span className="community-dropdown-name">{c.name}</span>
              {c.platform && (
                <span className="community-dropdown-platform">{c.platform}</span>
              )}
            </li>
          ))}
        </ul>
      )}
      {open && loading && results.length === 0 && inputText.length >= 2 && (
        <ul className="community-dropdown">
          <li className="community-dropdown-loading">Searching...</li>
        </ul>
      )}
    </div>
  );
}
