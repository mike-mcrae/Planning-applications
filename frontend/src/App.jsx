import { useEffect, useMemo, useRef, useState } from "react";
import L from "leaflet";
import {
  CircleMarker,
  GeoJSON,
  MapContainer,
  Pane,
  Popup,
  TileLayer,
  useMapEvents,
} from "react-leaflet";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

const DUBLIN_CENTER = [53.3498, -6.2603];
const DUBLIN_BOUNDS = [
  [53.24, -6.45],
  [53.43, -6.05],
];
const METRIC_OPTIONS = [
  { value: "letters_per_1000", label: "Letters / 1000 Residents" },
  { value: "total_applications", label: "Total Applications" },
  { value: "pct_with_objection", label: "% with Objection" },
  { value: "refusal_rate", label: "Refusal Rate" },
];

L.Icon.Default.mergeOptions({
  iconRetinaUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png",
  iconUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png",
  shadowUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png",
});

function MapBoundsWatcher({ onBoundsChange }) {
  useMapEvents({
    moveend: (event) => {
      const bounds = event.target.getBounds();
      onBoundsChange({
        min_lng: bounds.getWest(),
        min_lat: bounds.getSouth(),
        max_lng: bounds.getEast(),
        max_lat: bounds.getNorth(),
      });
    },
  });
  return null;
}

function MapClickWatcher({ onMapClick }) {
  useMapEvents({
    click: () => {
      onMapClick();
    },
  });
  return null;
}

function toQuery(filters, bbox, metric) {
  const params = new URLSearchParams();
  if (filters.yearMin) params.set("year_min", String(filters.yearMin));
  if (filters.yearMax) params.set("year_max", String(filters.yearMax));
  filters.development
    .filter((d) => d !== "all")
    .forEach((d) => params.append("development", d));
  if (filters.minSiteArea) params.set("min_site_area", String(filters.minSiteArea));
  if (filters.minUnits) params.set("min_units", String(filters.minUnits));
  if (filters.highDensity) params.set("high_density", "true");
  if (filters.hasObjection) params.set("has_objection", "true");
  if (filters.minLetters) params.set("min_letters", String(filters.minLetters));
  if (filters.topDecile) params.set("top_decile", "true");
  filters.outcomes.forEach((o) => params.append("outcomes", o));
  if (bbox) {
    params.set("min_lng", String(bbox.min_lng));
    params.set("min_lat", String(bbox.min_lat));
    params.set("max_lng", String(bbox.max_lng));
    params.set("max_lat", String(bbox.max_lat));
  }
  if (metric) params.set("metric", metric);
  return params.toString();
}

function formatNumber(value, decimals = 1) {
  if (value == null || Number.isNaN(value)) return "-";
  return Number(value).toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: decimals,
  });
}

function buildDefaultFilters(meta) {
  return {
    yearMin: meta?.year_min ?? "",
    yearMax: meta?.year_max ?? "",
    development: ["all"],
    minSiteArea: "",
    minUnits: "",
    highDensity: false,
    hasObjection: false,
    minLetters: "",
    topDecile: false,
    outcomes: [],
  };
}

export default function App() {
  const [meta, setMeta] = useState({ year_min: 2018, year_max: 2025 });
  const [bbox, setBbox] = useState(null);
  const [metric, setMetric] = useState("letters_per_1000");
  const [yearRange, setYearRange] = useState({ min: null, max: null });

  const [filters, setFilters] = useState(buildDefaultFilters({ year_min: "", year_max: "" }));

  const [layers, setLayers] = useState({
    smallAreas: true,
    electoralDivisions: false,
    points: true,
    heat: false,
  });

  const [applications, setApplications] = useState({ type: "FeatureCollection", features: [] });
  const [smallAreas, setSmallAreas] = useState({ type: "FeatureCollection", features: [] });
  const [electoralDivisions, setElectoralDivisions] = useState({ type: "FeatureCollection", features: [] });
  const [smallAreasCap, setSmallAreasCap] = useState(0);
  const [electoralDivisionsCap, setElectoralDivisionsCap] = useState(0);
  const [smallAreasVersion, setSmallAreasVersion] = useState(0);
  const [electoralDivisionsVersion, setElectoralDivisionsVersion] = useState(0);
  const [summary, setSummary] = useState({
    total_applications: 0,
    pct_with_objection: 0,
    median_letters: 0,
    letters_per_1000_residents: 0,
    refusal_rate: 0,
    appeal_rate: 0,
  });
  const [regionSummary, setRegionSummary] = useState(null);
  const applicationsRequestRef = useRef(0);
  const choroplethRequestRef = useRef(0);
  const summaryRequestRef = useRef(0);

  useEffect(() => {
    fetch(`${API_BASE}/meta`)
      .then((res) => res.json())
      .then((data) => {
        setMeta(data);
        setYearRange({ min: data.year_min, max: data.year_max });
        setFilters((prev) => ({ ...buildDefaultFilters(data), ...prev, yearMin: data.year_min, yearMax: data.year_max }));
      })
      .catch(() => null);
  }, []);

  const mapQuery = useMemo(() => toQuery(filters, bbox, null), [filters, bbox]);
  const choroplethQuery = useMemo(() => toQuery(filters, bbox, metric), [filters, bbox, metric]);
  const summaryQuery = useMemo(() => toQuery(filters, bbox, null), [filters, bbox]);

  useEffect(() => {
    const requestId = ++applicationsRequestRef.current;
    fetch(`${API_BASE}/applications?${mapQuery}`)
      .then((res) => res.json())
      .then((data) => {
        if (requestId !== applicationsRequestRef.current) return;
        setApplications(data);
      })
      .catch(() => setApplications({ type: "FeatureCollection", features: [] }));
  }, [mapQuery]);

  useEffect(() => {
    const requestId = ++choroplethRequestRef.current;
    fetch(`${API_BASE}/small_areas?${choroplethQuery}`)
      .then((res) => res.json())
      .then((data) => {
        if (requestId !== choroplethRequestRef.current) return;
        setSmallAreas({ type: data.type, features: data.features || [] });
        setSmallAreasCap(Number(data?.metadata?.cap_95 || 0));
        setSmallAreasVersion((v) => v + 1);
      })
      .catch(() => {
        if (requestId !== choroplethRequestRef.current) return;
        setSmallAreas({ type: "FeatureCollection", features: [] });
        setSmallAreasCap(0);
        setSmallAreasVersion((v) => v + 1);
      });

    fetch(`${API_BASE}/electoral_divisions?${choroplethQuery}`)
      .then((res) => res.json())
      .then((data) => {
        if (requestId !== choroplethRequestRef.current) return;
        setElectoralDivisions({ type: data.type, features: data.features || [] });
        setElectoralDivisionsCap(Number(data?.metadata?.cap_95 || 0));
        setElectoralDivisionsVersion((v) => v + 1);
      })
      .catch(() => {
        if (requestId !== choroplethRequestRef.current) return;
        setElectoralDivisions({ type: "FeatureCollection", features: [] });
        setElectoralDivisionsCap(0);
        setElectoralDivisionsVersion((v) => v + 1);
      });
  }, [choroplethQuery]);

  useEffect(() => {
    const requestId = ++summaryRequestRef.current;
    fetch(`${API_BASE}/summary?${summaryQuery}`)
      .then((res) => res.json())
      .then((data) => {
        if (requestId !== summaryRequestRef.current) return;
        setSummary(data);
      })
      .catch(() => null);
  }, [summaryQuery]);

  const getColor = (value, cap) => {
    if (!value) return "#f6f7f7";
    const ratio = cap > 0 ? Math.min(1, value / cap) : 0;
    if (ratio > 0.8) return "#8e1b14";
    if (ratio > 0.6) return "#bb3a17";
    if (ratio > 0.4) return "#d96519";
    if (ratio > 0.2) return "#f08f1e";
    return "#f7ba43";
  };

  const polygonStyle = (layerCap) => (feature) => {
    const props = feature.properties || {};
    const metricRaw = Number(props.choropleth_value ?? props[metric] ?? props.choropleth_raw ?? 0);
    const value = Number.isFinite(metricRaw) ? metricRaw : 0;
    const featureCap = Number(props.choropleth_cap || 0);
    const cap = Number.isFinite(layerCap) && layerCap > 0 ? layerCap : featureCap;
    return {
      color: "#2c403f",
      weight: 0.8,
      fillOpacity: 0.58,
      fillColor: getColor(value, cap),
    };
  };

  const updateToggle = (key, value) => {
    setLayers((prev) => {
      if (key === "smallAreas" && value) {
        return { ...prev, smallAreas: true, electoralDivisions: false };
      }
      if (key === "electoralDivisions" && value) {
        return { ...prev, smallAreas: false, electoralDivisions: true };
      }
      return { ...prev, [key]: value };
    });
  };

  const toggleSetItem = (key, value) => {
    if (key === "development") {
      setFilters((prev) => {
        const current = prev.development || [];
        if (value === "all") {
          return { ...prev, development: ["all"] };
        }
        const withoutAll = current.filter((v) => v !== "all");
        const exists = withoutAll.includes(value);
        const next = exists ? withoutAll.filter((v) => v !== value) : [...withoutAll, value];
        return { ...prev, development: next.length ? next : ["all"] };
      });
      return;
    }
    setFilters((prev) => {
      const arr = prev[key];
      const exists = arr.includes(value);
      return {
        ...prev,
        [key]: exists ? arr.filter((v) => v !== value) : [...arr, value],
      };
    });
  };

  const activeFilters = useMemo(() => {
    const items = [];
    if (filters.yearMin || filters.yearMax) items.push(`Years: ${filters.yearMin || "any"}-${filters.yearMax || "any"}`);
    filters.development
      .filter((d) => d !== "all")
      .forEach((d) => items.push(`Development: ${d}`));
    if (filters.minSiteArea) items.push(`Min site area: ${filters.minSiteArea}`);
    if (filters.minUnits) items.push(`Min units: ${filters.minUnits}`);
    if (filters.highDensity) items.push("High density");
    if (filters.hasObjection) items.push("Has objection");
    if (filters.minLetters) items.push(`Min letters: ${filters.minLetters}+`);
    if (filters.topDecile) items.push("Top decile by letters");
    filters.outcomes.forEach((o) => items.push(`Outcome: ${o}`));
    return items;
  }, [filters]);

  const hasTimeConflict = useMemo(() => {
    if (filters.yearMin && filters.yearMax && Number(filters.yearMin) > Number(filters.yearMax)) {
      return "Year min cannot be greater than year max.";
    }
    return "";
  }, [filters]);
  const metricLabel = METRIC_OPTIONS.find((m) => m.value === metric)?.label || metric;
  const activeRegionCap = layers.smallAreas
    ? smallAreasCap
    : layers.electoralDivisions
      ? electoralDivisionsCap
      : 0;
  const legendBins = useMemo(() => {
    if (!activeRegionCap || activeRegionCap <= 0) return [];
    const step = activeRegionCap / 4;
    return [
      { label: "0", color: "#f6f7f7" },
      { label: `0.1 - ${formatNumber(step)}`, color: "#f7ba43" },
      { label: `${formatNumber(step)} - ${formatNumber(step * 2)}`, color: "#f08f1e" },
      { label: `${formatNumber(step * 2)} - ${formatNumber(step * 3)}`, color: "#d96519" },
      { label: `${formatNumber(step * 3)} - ${formatNumber(step * 4)}`, color: "#8e1b14" },
    ];
  }, [activeRegionCap]);
  const developmentFilterLabel = useMemo(() => {
    const selected = (filters.development || []).filter((d) => d !== "all");
    if (!selected.length) return "All";
    return selected.join(" + ");
  }, [filters.development]);

  useEffect(() => {
    setRegionSummary(null);
  }, [filters, layers.smallAreas, layers.electoralDivisions]);

  const fetchRegionSummary = (regionType, regionId, regionLabel) => {
    const params = new URLSearchParams(toQuery(filters, null, null));
    params.set("region_type", regionType);
    params.set("region_id", regionId);
    fetch(`${API_BASE}/region_summary?${params.toString()}`)
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => {
        if (!data) return;
        setRegionSummary({ ...data, region_name: data.region_name || regionLabel || regionId });
      })
      .catch(() => null);
  };

  const sliderMin = meta.year_min ?? 2000;
  const sliderMax = meta.year_max ?? 2030;
  const effectiveMin = yearRange.min ?? sliderMin;
  const effectiveMax = yearRange.max ?? sliderMax;
  const sliderSpan = Math.max(1, sliderMax - sliderMin);
  const minPct = ((effectiveMin - sliderMin) / sliderSpan) * 100;
  const maxPct = ((effectiveMax - sliderMin) / sliderSpan) * 100;

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <h1>Planning Applications Explorer</h1>
        <p className="subtitle">Dublin City spatial analysis and filtering</p>
        <div className="toolbar-row">
          <button
            type="button"
            className="small-btn"
            onClick={() => {
              setFilters(buildDefaultFilters(meta));
              setYearRange({ min: meta.year_min, max: meta.year_max });
            }}
          >
            Reset filters
          </button>
          <span className="active-count">{activeFilters.length} active</span>
        </div>
        {activeFilters.length > 0 ? (
          <div className="chip-wrap">
            {activeFilters.map((label) => (
              <span key={label} className="chip">
                {label}
              </span>
            ))}
          </div>
        ) : null}
        {hasTimeConflict ? <div className="warning">{hasTimeConflict}</div> : null}

        <section>
          <h2>Layers</h2>
          <label className="checkbox">
            <input
              type="checkbox"
              checked={layers.smallAreas}
              onChange={(e) => updateToggle("smallAreas", e.target.checked)}
            />
            Small Areas
          </label>
          <label className="checkbox">
            <input
              type="checkbox"
              checked={layers.electoralDivisions}
              onChange={(e) => updateToggle("electoralDivisions", e.target.checked)}
            />
            Electoral Divisions
          </label>
          <label className="checkbox">
            <input
              type="checkbox"
              checked={layers.points}
              onChange={(e) => updateToggle("points", e.target.checked)}
            />
            Application points
          </label>
          <label className="checkbox">
            <input
              type="checkbox"
              checked={layers.heat}
              onChange={(e) => updateToggle("heat", e.target.checked)}
            />
            Objection heat
          </label>
          <label>
            Choropleth metric
            <select
              value={metric}
              disabled={!layers.smallAreas && !layers.electoralDivisions}
              onChange={(e) => setMetric(e.target.value)}
            >
              {METRIC_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
        </section>

        <section>
          <h2>Summary</h2>
          <div className="summary-grid">
            <div>
              <span>Total applications</span>
              <strong>{formatNumber(summary.total_applications, 0)}</strong>
            </div>
            <div>
              <span>% with objection</span>
              <strong>{formatNumber(summary.pct_with_objection)}%</strong>
            </div>
            <div>
              <span>Median letters</span>
              <strong>{formatNumber(summary.median_letters)}</strong>
            </div>
            <div>
              <span>Letters / 1000 residents</span>
              <strong>{formatNumber(summary.letters_per_1000_residents)}</strong>
            </div>
            <div>
              <span>Refusal rate</span>
              <strong>{formatNumber(summary.refusal_rate)}%</strong>
            </div>
            <div>
              <span>Appeal rate</span>
              <strong>{formatNumber(summary.appeal_rate)}%</strong>
            </div>
          </div>
        </section>


        <section>
          <h2>Time Filter</h2>
          <label>Year range</label>
          <div className="dual-range">
            <div className="dual-range-inner">
              <div className="dual-range-track" />
              <div
                className="dual-range-fill"
                style={{ left: `${minPct}%`, width: `${Math.max(0, maxPct - minPct)}%` }}
              />
              <input
                type="range"
                className="dual-thumb dual-thumb-left"
                min={sliderMin}
                max={sliderMax}
                value={effectiveMin}
                onChange={(e) => {
                  const nextMin = Number(e.target.value);
                  const nextMax = yearRange.max ?? sliderMax;
                  const clampedMin = Math.min(nextMin, nextMax);
                  setYearRange({ min: clampedMin, max: nextMax });
                  setFilters((p) => ({ ...p, yearMin: clampedMin, yearMax: nextMax }));
                }}
              />
              <input
                type="range"
                className="dual-thumb dual-thumb-right"
                min={sliderMin}
                max={sliderMax}
                value={effectiveMax}
                onChange={(e) => {
                  const nextMax = Number(e.target.value);
                  const nextMin = yearRange.min ?? sliderMin;
                  const clampedMax = Math.max(nextMax, nextMin);
                  setYearRange({ min: nextMin, max: clampedMax });
                  setFilters((p) => ({ ...p, yearMin: nextMin, yearMax: clampedMax }));
                }}
              />
            </div>
          </div>
          <div className="year-value">
            {yearRange.min ?? meta.year_min} - {yearRange.max ?? meta.year_max}
          </div>
        </section>

        <section>
          <h2>Development</h2>
          {[
            ["all", "All"],
            ["residential", "Residential only"],
            ["multi_unit", "Multi-unit only"],
            ["commercial", "Commercial"],
            ["extension", "Extensions"],
          ].map(([value, label]) => (
            <label className="checkbox" key={value}>
              <input
                type="checkbox"
                checked={filters.development.includes(value)}
                onChange={() => toggleSetItem("development", value)}
              />
              {label}
            </label>
          ))}
        </section>

        <section>
          <h2>Scale</h2>
          <label>
            Min site area
            <input
              type="number"
              value={filters.minSiteArea}
              onChange={(e) => setFilters((p) => ({ ...p, minSiteArea: e.target.value }))}
            />
          </label>
          <label>
            Min units
            <input
              type="number"
              value={filters.minUnits}
              onChange={(e) => setFilters((p) => ({ ...p, minUnits: e.target.value }))}
            />
          </label>
          <label className="checkbox">
            <input
              type="checkbox"
              checked={filters.highDensity}
              onChange={(e) => setFilters((p) => ({ ...p, highDensity: e.target.checked }))}
            />
            High density (&gt;10 units)
          </label>
        </section>

        <section>
          <h2>Engagement</h2>
          <label className="checkbox">
            <input
              type="checkbox"
              checked={filters.hasObjection}
              onChange={(e) => setFilters((p) => ({ ...p, hasObjection: e.target.checked }))}
            />
            Has objection
          </label>
          <label>
            Min letters
            <select
              value={filters.minLetters}
              onChange={(e) => setFilters((p) => ({ ...p, minLetters: e.target.value }))}
            >
              <option value="">Any</option>
              <option value="5">5+</option>
              <option value="10">10+</option>
            </select>
          </label>
          <label className="checkbox">
            <input
              type="checkbox"
              checked={filters.topDecile}
              onChange={(e) => setFilters((p) => ({ ...p, topDecile: e.target.checked }))}
            />
            Top decile by letters
          </label>
        </section>

        <section>
          <h2>Outcomes</h2>
          {[
            ["granted", "Granted"],
            ["refused", "Refused"],
            ["appealed", "Appealed"],
            ["overturned", "Overturned"],
          ].map(([value, label]) => (
            <label className="checkbox" key={value}>
              <input
                type="checkbox"
                checked={filters.outcomes.includes(value)}
                onChange={() => toggleSetItem("outcomes", value)}
              />
              {label}
            </label>
          ))}
        </section>

      </aside>

      <main className="map-wrap">
        <div className="map-legend">
          {(layers.smallAreas || layers.electoralDivisions) && (
            <div className="legend-group">
              <strong>
                {layers.smallAreas ? "Small Areas" : "Electoral Divisions"}: {metricLabel}
              </strong>
              {legendBins.length ? (
                legendBins.map((bin) => (
                  <div key={bin.label} className="legend-row">
                    <span className="legend-swatch" style={{ background: bin.color }} />
                    <span>{bin.label}</span>
                  </div>
                ))
              ) : (
                <div className="legend-row">
                  <span className="legend-swatch" style={{ background: "#f6f7f7" }} />
                  <span>No positive values</span>
                </div>
              )}
            </div>
          )}
          {layers.points && (
            <div className="legend-group">
              <strong>Application points</strong>
              <div className="legend-row">
                <span className="legend-dot" />
                <span>Application location</span>
              </div>
            </div>
          )}
          {layers.heat && (
            <div className="legend-group">
              <strong>Objection heat</strong>
              <div className="legend-row">
                <span className="legend-heat legend-heat-sm" />
                <span>Lower letters</span>
              </div>
              <div className="legend-row">
                <span className="legend-heat legend-heat-lg" />
                <span>Higher letters</span>
              </div>
            </div>
          )}
        </div>
        {regionSummary ? (
          <div className="region-flyout">
            <div className="flyout-head">
              <div>
                <div className="flyout-kicker">Selected Region</div>
                <div className="region-title">{regionSummary.region_name || regionSummary.region_id}</div>
                <div className="flyout-subtle">Development filter: {developmentFilterLabel}</div>
              </div>
              <button type="button" className="flyout-close" onClick={() => setRegionSummary(null)}>
                X
              </button>
            </div>
            <div className="summary-grid">
              <div>
                <span>Total applications</span>
                <strong>{formatNumber(regionSummary.total_applications, 0)}</strong>
              </div>
              <div>
                <span>% with objection</span>
                <strong>{formatNumber(regionSummary.pct_with_objection)}%</strong>
              </div>
              <div>
                <span>Median letters</span>
                <strong>{formatNumber(regionSummary.median_letters)}</strong>
              </div>
              <div>
                <span>Letters / 1000 residents</span>
                <strong>{formatNumber(regionSummary.letters_per_1000_residents)}</strong>
              </div>
            </div>
            <div className="breakdown">
              <strong>Development</strong>
              <div>Residential: {formatNumber(regionSummary.development_breakdown?.residential?.count, 0)}</div>
              <div>Multi-unit: {formatNumber(regionSummary.development_breakdown?.multi_unit?.count, 0)}</div>
              <div>Commercial: {formatNumber(regionSummary.development_breakdown?.commercial?.count, 0)}</div>
              <div>Extension: {formatNumber(regionSummary.development_breakdown?.extension?.count, 0)}</div>
            </div>
            <div className="breakdown">
              <strong>Outcomes</strong>
              <div>Granted: {formatNumber(regionSummary.outcomes_breakdown?.granted?.count, 0)}</div>
              <div>Refused: {formatNumber(regionSummary.outcomes_breakdown?.refused?.count, 0)}</div>
              <div>Appealed: {formatNumber(regionSummary.outcomes_breakdown?.appealed?.count, 0)}</div>
              <div>Overturned: {formatNumber(regionSummary.outcomes_breakdown?.overturned?.count, 0)}</div>
            </div>
          </div>
        ) : null}
        <MapContainer
          center={DUBLIN_CENTER}
          zoom={12}
          minZoom={11}
          maxZoom={18}
          maxBounds={DUBLIN_BOUNDS}
          maxBoundsViscosity={1.0}
          className="map"
        >
          <MapBoundsWatcher onBoundsChange={setBbox} />
          <MapClickWatcher onMapClick={() => setRegionSummary(null)} />
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />

          <Pane name="regions" style={{ zIndex: 350 }} />
          <Pane name="points" style={{ zIndex: 600 }} />
          <Pane name="heat" style={{ zIndex: 620 }} />

          {layers.smallAreas && (
            <GeoJSON
              key={`sa-${smallAreasVersion}-${metric}`}
              data={smallAreas}
              pane="regions"
              style={polygonStyle(smallAreasCap)}
              onEachFeature={(feature, layer) => {
                const id = feature?.properties?.SA_GUID_21;
                const label = feature?.properties?.SA_PUB2022;
                layer.on("click", (event) => {
                  L.DomEvent.stopPropagation(event);
                  if (id) fetchRegionSummary("sa", id, label);
                });
              }}
            />
          )}

          {layers.electoralDivisions && (
            <GeoJSON
              key={`ed-${electoralDivisionsVersion}-${metric}`}
              data={electoralDivisions}
              pane="regions"
              style={polygonStyle(electoralDivisionsCap)}
              onEachFeature={(feature, layer) => {
                const id = feature?.properties?.ED_GUID;
                const label = feature?.properties?.ed_name || feature?.properties?.ED_ENGLISH;
                layer.on("click", (event) => {
                  L.DomEvent.stopPropagation(event);
                  if (id) fetchRegionSummary("ed", id, label);
                });
              }}
            />
          )}

          {layers.points &&
            applications.features.map((feature) => {
              const [lng, lat] = feature.geometry.coordinates;
              const p = feature.properties || {};
              return (
                <CircleMarker
                  key={`${p.application_number}-${lat}-${lng}`}
                  center={[lat, lng]}
                  pane="points"
                  radius={4}
                  eventHandlers={{ click: () => setRegionSummary(null) }}
                  pathOptions={{ color: "#153331", fillColor: "#0f8a7d", fillOpacity: 0.78, weight: 1 }}
                >
                  <Popup>
                    <div className="popup">
                      <strong>{p.application_number || "Unknown"}</strong>
                      <div>{p.development_address || "No address"}</div>
                      <div>{p.development_description || "No description"}</div>
                      <div>Received: {p.received_date || "-"}</div>
                      <div>Decision date: {p.decision_date || "-"}</div>
                      <div>Units: {p.number_of_units ?? "-"}</div>
                      <div>Site area: {p.site_area ?? "-"}</div>
                      <div>Floor area: {p.floor_area ?? "-"}</div>
                      <div>Objection letters: {p.n_observation_letters ?? 0}</div>
                      <div>Decision: {p.decision || "-"}</div>
                      <div>Appeal status: {p.appeal_status || "-"}</div>
                      {p.portal_link ? (
                        <a href={p.portal_link} target="_blank" rel="noreferrer">
                          Open planning portal
                        </a>
                      ) : null}
                    </div>
                  </Popup>
                </CircleMarker>
              );
            })}

          {layers.heat &&
            applications.features.map((feature, idx) => {
              const [lng, lat] = feature.geometry.coordinates;
              const letters = Number(feature.properties?.n_observation_letters || 0);
              if (letters <= 0) return null;
              return (
                <CircleMarker
                  key={`heat-${idx}`}
                  center={[lat, lng]}
                  pane="heat"
                  radius={Math.min(24, 5 + letters * 0.5)}
                  eventHandlers={{ click: () => setRegionSummary(null) }}
                  pathOptions={{
                    color: "#cc4f15",
                    fillColor: "#e36b1d",
                    fillOpacity: 0.16,
                    weight: 0,
                  }}
                />
              );
            })}
        </MapContainer>
      </main>
    </div>
  );
}
