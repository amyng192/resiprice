import { Fragment, useState, useMemo } from "react";
import { Property, Unit } from "../types";
import type ExcelJSType from "exceljs";

interface Props {
  properties: Property[];
}

type SortKey = "rent" | "sqft" | "beds" | "community" | "rentPerSqft";
type SortDir = "asc" | "desc";

interface FlatRow {
  community: string;
  communityIndex: number;
  unit: Unit;
  rentPerSqft: number | null;
}

interface BedroomSummary {
  beds: number;
  label: string;
  communities: {
    name: string;
    colorIndex: number;
    count: number;
    avgSqft: number | null;
    avgRent: number | null;
    avgRentPerSqft: number | null;
  }[];
}

const COLORS = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444"];

function calcRentPerSqft(unit: Unit): number | null {
  if (unit.rent_min && unit.sqft && unit.sqft > 0) {
    return unit.rent_min / unit.sqft;
  }
  return null;
}

function weightedAvg(
  units: Unit[],
  getValue: (u: Unit) => number | null
): number | null {
  let sum = 0;
  let count = 0;
  for (const u of units) {
    const v = getValue(u);
    if (v != null) {
      sum += v;
      count++;
    }
  }
  return count > 0 ? sum / count : null;
}

function buildBedroomSummaries(properties: Property[]): BedroomSummary[] {
  const bedCounts = new Set<number>();
  for (const p of properties) {
    for (const u of p.units) {
      if (u.bedrooms != null) bedCounts.add(u.bedrooms);
    }
  }
  const sorted = [...bedCounts].sort((a, b) => a - b);

  return sorted.map((beds) => {
    const label =
      beds === 0 ? "Studio" : beds === 1 ? "1 Bed" : `${beds} Bed`;
    const communities = properties.map((p, i) => {
      const matching = p.units.filter((u) => u.bedrooms === beds);
      return {
        name: p.name,
        colorIndex: i,
        count: matching.length,
        avgSqft: weightedAvg(matching, (u) => u.sqft),
        avgRent: weightedAvg(matching, (u) => u.rent_min),
        avgRentPerSqft: weightedAvg(matching, calcRentPerSqft),
      };
    });
    return { beds, label, communities };
  });
}

async function downloadExcel(rows: FlatRow[]) {
  const [ExcelJS, { saveAs }] = await Promise.all([
    import("exceljs"),
    import("file-saver"),
  ]);
  const wb = new (ExcelJS.default as unknown as typeof ExcelJSType).Workbook();
  const ws = wb.addWorksheet("Units");

  ws.columns = [
    { header: "Community", key: "community", width: 28 },
    { header: "Unit", key: "unit", width: 12 },
    { header: "Floor Plan", key: "floorPlan", width: 18 },
    { header: "Type", key: "type", width: 10 },
    { header: "Sq Ft", key: "sqft", width: 10 },
    { header: "Rent", key: "rent", width: 12 },
    { header: "$/Sq Ft", key: "rentPerSqft", width: 10 },
    { header: "Available", key: "available", width: 14 },
  ];

  // Style header row
  ws.getRow(1).font = { bold: true };

  for (const row of rows) {
    ws.addRow({
      community: row.community,
      unit: row.unit.unit_number,
      floorPlan: row.unit.floor_plan_name ?? "",
      type: row.unit.unit_type ?? "",
      sqft: row.unit.sqft ?? "",
      rent: row.unit.rent_min ?? "",
      rentPerSqft: row.rentPerSqft != null ? Math.round(row.rentPerSqft * 100) / 100 : "",
      available: row.unit.available_date ?? "",
    });
  }

  const buf = await wb.xlsx.writeBuffer();
  saveAs(new Blob([buf]), "resiprice_units.xlsx");
}

export default function ComparisonTable({ properties }: Props) {
  const [sortKey, setSortKey] = useState<SortKey>("rent");
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [filterBeds, setFilterBeds] = useState<string>("all");

  // Flatten all units with their community info
  const rows: FlatRow[] = properties.flatMap((p, i) =>
    p.units.map((u) => ({
      community: p.name,
      communityIndex: i,
      unit: u,
      rentPerSqft: calcRentPerSqft(u),
    }))
  );

  // Bedroom summaries
  const bedroomSummaries = useMemo(
    () => buildBedroomSummaries(properties),
    [properties]
  );

  // Collect all bed types
  const allBedTypes = [
    ...new Set(rows.map((r) => r.unit.unit_type).filter(Boolean)),
  ].sort();

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
      case "rentPerSqft":
        return ((a.rentPerSqft ?? 99999) - (b.rentPerSqft ?? 99999)) * dir;
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
    n != null
      ? `$${n.toLocaleString("en-US", { maximumFractionDigits: 0 })}`
      : "—";

  const fmtDec = (n: number | null) =>
    n != null
      ? `$${n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
      : "—";

  return (
    <div className="comparison">
      {/* Summary cards */}
      <div className="summary-cards">
        {properties.map((p, i) => (
          <div
            key={i}
            className="summary-card"
            style={{ borderTopColor: COLORS[i] }}
          >
            <h3>{p.name}</h3>
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

      {/* Bedroom summary table */}
      {bedroomSummaries.length > 0 && (
        <div className="bedroom-summary">
          <h3 className="bedroom-summary-title">Average by Bedroom Type</h3>
          <div className="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th>Type</th>
                  {properties.map((p, i) => (
                    <th key={i} colSpan={3}>
                      <span
                        className="community-dot"
                        style={{ backgroundColor: COLORS[i] }}
                      />
                      {p.name}
                    </th>
                  ))}
                </tr>
                <tr>
                  <th></th>
                  {properties.map((_, i) => (
                    <Fragment key={i}>
                      <th className="sub-header">Avg Sq Ft</th>
                      <th className="sub-header">Avg Rent</th>
                      <th className="sub-header">$/Sq Ft</th>
                    </Fragment>
                  ))}
                </tr>
              </thead>
              <tbody>
                {bedroomSummaries.map((s) => (
                  <tr key={s.beds}>
                    <td className="bed-label">{s.label}</td>
                    {s.communities.map((c, i) => (
                      <Fragment key={i}>
                        <td>
                          {c.count > 0 && c.avgSqft != null
                            ? Math.round(c.avgSqft).toLocaleString()
                            : "—"}
                        </td>
                        <td className="rent-cell">
                          {c.count > 0 ? fmt(c.avgRent) : "—"}
                        </td>
                        <td>
                          {c.count > 0 && c.avgRentPerSqft != null
                            ? fmtDec(c.avgRentPerSqft)
                            : "—"}
                        </td>
                      </Fragment>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Filters + Download */}
      <div className="table-controls">
        <label>
          Filter by type:
          <select
            value={filterBeds}
            onChange={(e) => setFilterBeds(e.target.value)}
          >
            <option value="all">
              All (
              {filtered.length === rows.length
                ? rows.length
                : `${filtered.length}/${rows.length}`}
              )
            </option>
            {allBedTypes.map((t) => (
              <option key={t!} value={t!}>
                {t} ({rows.filter((r) => r.unit.unit_type === t).length})
              </option>
            ))}
          </select>
        </label>
        <div className="table-controls-right">
          <span className="result-count">{sorted.length} units</span>
          <button
            className="btn-secondary btn-sm"
            onClick={() => downloadExcel(sorted)}
          >
            Download Excel
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="table-wrapper">
        <table>
          <thead>
            <tr>
              <th
                className="clickable"
                onClick={() => toggleSort("community")}
              >
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
              <th
                className="clickable"
                onClick={() => toggleSort("rentPerSqft")}
              >
                $/Sq Ft{sortArrow("rentPerSqft")}
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
                  {row.unit.rent_max &&
                  row.unit.rent_max !== row.unit.rent_min
                    ? ` – ${fmt(row.unit.rent_max)}`
                    : ""}
                </td>
                <td>{row.rentPerSqft != null ? fmtDec(row.rentPerSqft) : "—"}</td>
                <td>{row.unit.available_date ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
