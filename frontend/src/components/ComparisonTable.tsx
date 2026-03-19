import { useState } from "react";
import { Property, Unit } from "../types";

interface Props {
  properties: Property[];
}

type SortKey = "rent" | "sqft" | "beds" | "community";
type SortDir = "asc" | "desc";

interface FlatRow {
  community: string;
  communityIndex: number;
  unit: Unit;
}

const COLORS = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444"];

export default function ComparisonTable({ properties }: Props) {
  const [sortKey, setSortKey] = useState<SortKey>("rent");
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [filterBeds, setFilterBeds] = useState<string>("all");

  // Flatten all units with their community info
  const rows: FlatRow[] = properties.flatMap((p, i) =>
    p.units.map((u) => ({ community: p.name, communityIndex: i, unit: u }))
  );

  // Collect all bed types
  const allBedTypes = [...new Set(rows.map((r) => r.unit.unit_type).filter(Boolean))].sort();

  // Filter
  const filtered =
    filterBeds === "all"
      ? rows
      : rows.filter((r) => r.unit.unit_type === filterBeds);

  // Sort
  const sorted = [...filtered].sort((a, b) => {
    const dir = sortDir === "asc" ? 1 : -1;
    switch (sortKey) {
      case "rent":
        return ((a.unit.rent_min ?? 99999) - (b.unit.rent_min ?? 99999)) * dir;
      case "sqft":
        return ((a.unit.sqft ?? 0) - (b.unit.sqft ?? 0)) * dir;
      case "beds":
        return ((a.unit.bedrooms ?? 0) - (b.unit.bedrooms ?? 0)) * dir;
      case "community":
        return a.community.localeCompare(b.community) * dir;
      default:
        return 0;
    }
  });

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortKey(key);
      setSortDir("asc");
    }
  };

  const sortArrow = (key: SortKey) =>
    sortKey === key ? (sortDir === "asc" ? " ▲" : " ▼") : "";

  const fmt = (n: number | null) =>
    n != null ? `$${n.toLocaleString("en-US", { maximumFractionDigits: 0 })}` : "—";

  return (
    <div className="comparison">
      {/* Summary cards */}
      <div className="summary-cards">
        {properties.map((p, i) => (
          <div key={i} className="summary-card" style={{ borderTopColor: COLORS[i] }}>
            <h3>{p.name}</h3>
            {p.address && <p className="summary-address">{p.address}</p>}
            <div className="summary-stats">
              <span>{p.available_count} units available</span>
              {p.platform && <span className="badge">{p.platform}</span>}
            </div>
            {p.specials.length > 0 && (
              <div className="summary-specials">
                {p.specials.map((s, j) => (
                  <div key={j} className="special-tag">
                    {s.description.slice(0, 80)}
                  </div>
                ))}
              </div>
            )}
            <div className="unit-mix">
              {Object.entries(p.unit_mix).map(([type, count]) => (
                <span key={type} className="mix-tag">
                  {type}: {count}
                </span>
              ))}
            </div>
          </div>
        ))}
      </div>

      {/* Filters */}
      <div className="table-controls">
        <label>
          Filter by type:
          <select value={filterBeds} onChange={(e) => setFilterBeds(e.target.value)}>
            <option value="all">All ({filtered.length === rows.length ? rows.length : `${filtered.length}/${rows.length}`})</option>
            {allBedTypes.map((t) => (
              <option key={t!} value={t!}>
                {t} ({rows.filter((r) => r.unit.unit_type === t).length})
              </option>
            ))}
          </select>
        </label>
        <span className="result-count">{sorted.length} units</span>
      </div>

      {/* Table */}
      <div className="table-wrapper">
        <table>
          <thead>
            <tr>
              <th className="clickable" onClick={() => toggleSort("community")}>
                Community{sortArrow("community")}
              </th>
              <th>Unit</th>
              <th>Floor Plan</th>
              <th className="clickable" onClick={() => toggleSort("beds")}>
                Type{sortArrow("beds")}
              </th>
              <th className="clickable" onClick={() => toggleSort("sqft")}>
                Sq Ft{sortArrow("sqft")}
              </th>
              <th className="clickable" onClick={() => toggleSort("rent")}>
                Rent{sortArrow("rent")}
              </th>
              <th>Available</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((row, i) => (
              <tr key={i}>
                <td>
                  <span
                    className="community-dot"
                    style={{ backgroundColor: COLORS[row.communityIndex] }}
                  />
                  {row.community}
                </td>
                <td>{row.unit.unit_number}</td>
                <td>{row.unit.floor_plan_name ?? "—"}</td>
                <td>{row.unit.unit_type ?? "—"}</td>
                <td>{row.unit.sqft?.toLocaleString() ?? "—"}</td>
                <td className="rent-cell">
                  {fmt(row.unit.rent_min)}
                  {row.unit.rent_max && row.unit.rent_max !== row.unit.rent_min
                    ? ` – ${fmt(row.unit.rent_max)}`
                    : ""}
                </td>
                <td>{row.unit.available_date ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
