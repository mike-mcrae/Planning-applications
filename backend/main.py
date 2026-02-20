from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from shapely.geometry import box

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
LEGACY_DATA_DIR = BASE_DIR / "0. data"
LEGACY_SCRIPTS_DIR = BASE_DIR / "1. scripts"


def _pick_path(*candidates: Path) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


PLANNING_CSV = _pick_path(
    DATA_DIR / "IrishPlanningApplications_DublinCityCouncil.csv",
    LEGACY_DATA_DIR / "IrishPlanningApplications_DublinCityCouncil.csv",
)
GEOCODED_CSV = _pick_path(
    DATA_DIR / "DCC_all_applications_geocoded.csv",
    LEGACY_DATA_DIR / "DCC_all_applications_geocoded.csv",
)
MASTER_OBS_CSV = _pick_path(
    DATA_DIR / "applications_master_with_obs.csv",
    LEGACY_SCRIPTS_DIR / "outputs" / "applications_master_with_obs.csv",
)
SA_SHP = _pick_path(
    DATA_DIR / "small_areas" / "SMALL_AREA_2022.shp",
    LEGACY_DATA_DIR
    / "Small_Area_National_Statistical_Boundaries_2022_Ungeneralised_view_2205995009404967982"
    / "SMALL_AREA_2022.shp",
)
SA_POP_JSON = _pick_path(
    DATA_DIR / "cso" / "SAP2022T1T1ASA.20260219T220214.json",
    LEGACY_DATA_DIR / "cso" / "SAP2022T1T1ASA.20260219T220214.json",
)
ED_POP_CSV = _pick_path(
    DATA_DIR / "cso" / "CensusHub2022_T9_1_ED_7008835473864658512.csv",
    LEGACY_DATA_DIR / "cso" / "CensusHub2022_T9_1_ED_7008835473864658512.csv",
)


@dataclass(frozen=True)
class FilterSignature:
    date_from: str | None
    date_to: str | None
    year_min: int | None
    year_max: int | None
    development: tuple[str, ...]
    min_site_area: float | None
    min_units: int | None
    high_density: bool
    has_objection: bool
    min_letters: int | None
    top_decile: bool
    outcomes: tuple[str, ...]
    decisions: tuple[str, ...]


def _normalize_application_number(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.upper()
        .str.replace(" ", "", regex=False)
        .str.replace("-", "/", regex=False)
    )


def _read_sa_population() -> pd.DataFrame:
    with SA_POP_JSON.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    dims = payload["dimension"]
    sa_codes = dims["C04172V04943"]["category"]["index"]
    ages = dims["C03737V04485"]["category"]["index"]
    sexes = dims["C03738V04487"]["category"]["index"]
    values = payload["value"]

    age_idx = ages.index("AGET")
    sex_idx = sexes.index("B")
    n_age = len(ages)
    n_sex = len(sexes)

    records: list[dict[str, Any]] = []
    for sa_idx, sa_guid in enumerate(sa_codes):
        value_idx = (sa_idx * n_age + age_idx) * n_sex + sex_idx
        pop_value = values[value_idx] if value_idx < len(values) else None
        records.append({"SA_GUID_21": sa_guid, "population": pop_value})

    return pd.DataFrame(records)


def _read_ed_population() -> pd.DataFrame:
    df = pd.read_csv(ED_POP_CSV)
    mask = df["LOCAL_AUTHORITY"].astype(str).str.upper().str.contains("DUBLIN CITY")
    pop_col = "Total Population (Normalisation)"
    return (
        df.loc[mask, ["ED_GUID", pop_col]]
        .rename(columns={pop_col: "population"})
        .drop_duplicates(subset=["ED_GUID"])
    )


def _is_nonempty(value: Any) -> bool:
    if pd.isna(value):
        return False
    return str(value).strip() != ""


def _resolve_year_window(year: int | None, year_min: int | None, year_max: int | None) -> tuple[int | None, int | None]:
    if year is not None:
        return year, year
    return year_min, year_max


def _load_data() -> dict[str, Any]:
    planning = pd.read_csv(PLANNING_CSV)
    geocoded = pd.read_csv(GEOCODED_CSV)
    obs = pd.read_csv(MASTER_OBS_CSV)

    planning = planning.rename(
        columns={
            "Application Number": "application_number",
            "Development Description": "development_description",
            "Development Address": "development_address",
            "Received Date": "received_date",
            "Decision Date": "decision_date",
            "Number of Residential Units": "number_of_units",
            "Site Area": "site_area",
            "Floor Area": "floor_area",
            "Decision": "decision",
            "Appeal Status": "appeal_status",
            "Appeal Decision": "appeal_decision",
            "Appeal Reference Number": "appeal_reference_number",
            "Link Application Details": "portal_link",
            "One-Off House": "one_off_house",
            "Application Type": "application_type",
        }
    )

    geocoded["application_number_key"] = _normalize_application_number(geocoded["application_number"])
    planning["application_number_key"] = _normalize_application_number(planning["application_number"])
    obs["application_number_key"] = _normalize_application_number(obs["application_number"])

    obs_keep = obs[["application_number_key", "n_observation_letters", "has_observation"]].copy()
    obs_keep["has_observation"] = obs_keep["has_observation"].fillna(0).astype(int)

    merged = (
        planning.drop_duplicates(subset=["application_number_key"])
        .merge(
            geocoded[["application_number_key", "latitude", "longitude"]].drop_duplicates(
                subset=["application_number_key"]
            ),
            on="application_number_key",
            how="left",
        )
        .merge(obs_keep, on="application_number_key", how="left")
    )

    merged["n_observation_letters"] = merged["n_observation_letters"].fillna(0).astype(int)
    merged["has_observation"] = (
        (merged["has_observation"].fillna(0).astype(int) > 0)
        | (merged["n_observation_letters"] > 0)
    ).astype(int)

    for col in ["received_date", "decision_date"]:
        merged[col] = pd.to_datetime(merged[col], format="mixed", errors="coerce")
    for col in ["site_area", "number_of_units", "floor_area"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")

    text = merged["development_description"].fillna("").str.lower()
    one_off_text = merged["one_off_house"].fillna("").astype(str).str.lower()
    decision_text = merged["decision"].fillna("").astype(str).str.lower()
    appeal_status_text = merged["appeal_status"].fillna("").astype(str).str.lower()
    appeal_decision_text = merged["appeal_decision"].fillna("").astype(str).str.lower()

    merged["is_residential"] = text.str.contains(
        r"residential|apartment|dwelling|housing|house|unit", regex=True
    )
    merged["is_multi_unit"] = merged["number_of_units"].fillna(0) > 1
    merged["is_one_off"] = one_off_text.isin(["yes", "y", "true", "1"])
    merged["is_commercial"] = text.str.contains(
        r"office|retail|commercial|shop|industrial|warehouse|hotel|restaurant|cafe", regex=True
    )
    merged["is_extension"] = text.str.contains(
        r"extension|alteration|retention|refurbishment|attic|rear|front", regex=True
    )

    merged["is_granted"] = decision_text.str.contains("grant")
    merged["is_refused"] = decision_text.str.contains("refus")
    merged["is_appealed"] = (
        appeal_status_text.map(_is_nonempty)
        | merged["appeal_reference_number"].map(_is_nonempty)
        | appeal_decision_text.map(_is_nonempty)
    )
    merged["is_overturned"] = (
        (merged["is_refused"] & appeal_decision_text.str.contains("grant"))
        | (merged["is_granted"] & appeal_decision_text.str.contains("refus"))
    )

    point_df = merged.dropna(subset=["latitude", "longitude"]).copy()
    points_gdf = gpd.GeoDataFrame(
        point_df,
        geometry=gpd.points_from_xy(point_df["longitude"], point_df["latitude"]),
        crs="EPSG:4326",
    )

    sa_gdf = gpd.read_file(SA_SHP)
    sa_gdf = sa_gdf.loc[sa_gdf["COUNTY_ENG"].astype(str).str.upper() == "DUBLIN CITY"].copy()
    sa_gdf = sa_gdf.to_crs("EPSG:4326")

    points_joined = gpd.sjoin(
        points_gdf,
        sa_gdf[["SA_GUID_21", "SA_PUB2022", "ED_GUID", "ED_ENGLISH", "geometry"]],
        how="left",
        predicate="within",
    ).drop(columns=["index_right"], errors="ignore")

    sa_pop = _read_sa_population()
    ed_pop = _read_ed_population()

    sa_base = sa_gdf.merge(sa_pop, on="SA_GUID_21", how="left")
    ed_base = (
        sa_gdf.dissolve(by="ED_GUID", as_index=False, aggfunc="first")
        .merge(ed_pop, on="ED_GUID", how="left")
        .rename(columns={"ED_ENGLISH": "ed_name"})
    )

    years = points_joined["received_date"].dt.year.dropna().astype(int)

    return {
        "applications": points_joined,
        "sa_base": sa_base,
        "ed_base": ed_base,
        "year_min": int(years.min()) if not years.empty else 2000,
        "year_max": int(years.max()) if not years.empty else 2030,
    }


DATA = _load_data()


def _apply_filters(df: gpd.GeoDataFrame, sig: FilterSignature) -> gpd.GeoDataFrame:
    out = df

    if sig.date_from:
        out = out.loc[out["received_date"] >= pd.to_datetime(sig.date_from)]
    if sig.date_to:
        out = out.loc[out["received_date"] <= pd.to_datetime(sig.date_to)]

    years = out["received_date"].dt.year
    if sig.year_min is not None:
        out = out.loc[years >= sig.year_min]
    if sig.year_max is not None:
        out = out.loc[years <= sig.year_max]

    if sig.development:
        masks = []
        for item in sig.development:
            if item == "residential":
                masks.append(out["is_residential"])
            elif item == "multi_unit":
                masks.append(out["is_multi_unit"])
            elif item == "one_off":
                masks.append(out["is_one_off"])
            elif item == "commercial":
                masks.append(out["is_commercial"])
            elif item == "extension":
                masks.append(out["is_extension"])
        if masks:
            combined = masks[0]
            for m in masks[1:]:
                combined = combined | m
            out = out.loc[combined]

    if sig.min_site_area is not None:
        out = out.loc[out["site_area"].fillna(0) >= sig.min_site_area]
    if sig.min_units is not None:
        out = out.loc[out["number_of_units"].fillna(0) >= sig.min_units]
    if sig.high_density:
        out = out.loc[out["number_of_units"].fillna(0) > 10]

    if sig.has_objection:
        out = out.loc[out["n_observation_letters"] > 0]
    if sig.min_letters is not None:
        out = out.loc[out["n_observation_letters"] >= sig.min_letters]
    if sig.top_decile:
        positive_letters = df.loc[df["n_observation_letters"] > 0, "n_observation_letters"]
        if positive_letters.empty:
            return out.iloc[0:0]
        threshold = int(np.ceil(np.nanpercentile(positive_letters, 90)))
        out = out.loc[out["n_observation_letters"] >= threshold]

    if sig.outcomes:
        outcome_masks = []
        for item in sig.outcomes:
            if item == "granted":
                outcome_masks.append(out["is_granted"])
            elif item == "refused":
                outcome_masks.append(out["is_refused"])
            elif item == "appealed":
                outcome_masks.append(out["is_appealed"])
            elif item == "overturned":
                outcome_masks.append(out["is_overturned"])
        if outcome_masks:
            combined = outcome_masks[0]
            for m in outcome_masks[1:]:
                combined = combined | m
            out = out.loc[combined]

    if sig.decisions:
        decision_text = out["decision"].fillna("").astype(str).str.lower()
        decision_mask = pd.Series(False, index=out.index)
        for term in sig.decisions:
            decision_mask = decision_mask | decision_text.str.contains(term.lower(), regex=False)
        out = out.loc[decision_mask]

    return out


def _signature(
    date_from: str | None,
    date_to: str | None,
    year_min: int | None,
    year_max: int | None,
    development: list[str] | None,
    min_site_area: float | None,
    min_units: int | None,
    high_density: bool,
    has_objection: bool,
    min_letters: int | None,
    top_decile: bool,
    outcomes: list[str] | None,
    decisions: list[str] | None,
) -> FilterSignature:
    return FilterSignature(
        date_from=date_from,
        date_to=date_to,
        year_min=year_min,
        year_max=year_max,
        development=tuple(sorted(development or [])),
        min_site_area=min_site_area,
        min_units=min_units,
        high_density=high_density,
        has_objection=has_objection,
        min_letters=min_letters,
        top_decile=top_decile,
        outcomes=tuple(sorted(outcomes or [])),
        decisions=tuple(sorted(decisions or [])),
    )


def _aggregate(
    filtered: gpd.GeoDataFrame, base_gdf: gpd.GeoDataFrame, group_col: str, pop_col: str = "population"
) -> gpd.GeoDataFrame:
    if filtered.empty:
        empty = base_gdf.copy()
        empty["total_applications"] = 0
        empty["total_letters"] = 0
        empty["pct_with_objection"] = 0.0
        empty["median_letters"] = 0.0
        empty["refusal_rate"] = 0.0
        empty["appeal_rate"] = 0.0
        empty["letters_per_1000"] = 0.0
        return empty

    grouped = (
        filtered.groupby(group_col)
        .agg(
            total_applications=("application_number_key", "count"),
            total_letters=("n_observation_letters", "sum"),
            with_objection=("has_observation", "sum"),
            median_letters=("n_observation_letters", "median"),
            refusal_rate=("is_refused", "mean"),
            appeal_rate=("is_appealed", "mean"),
        )
        .reset_index()
    )
    grouped["pct_with_objection"] = np.where(
        grouped["total_applications"] > 0,
        grouped["with_objection"] / grouped["total_applications"] * 100,
        0,
    )

    merged = base_gdf.merge(grouped, on=group_col, how="left")
    fill_cols = [
        "total_applications",
        "total_letters",
        "with_objection",
        "pct_with_objection",
        "median_letters",
        "refusal_rate",
        "appeal_rate",
    ]
    for col in fill_cols:
        merged[col] = merged[col].fillna(0)

    merged["letters_per_1000"] = np.where(
        merged[pop_col].fillna(0) > 0,
        merged["total_letters"] / merged[pop_col] * 1000,
        0.0,
    )
    return merged


def _summary_from_filtered(filtered: gpd.GeoDataFrame, sa_pop_lookup: pd.DataFrame) -> dict[str, Any]:
    total = int(len(filtered))
    with_obs = int((filtered["n_observation_letters"] > 0).sum())
    total_letters = float(filtered["n_observation_letters"].sum())
    median_letters = float(filtered["n_observation_letters"].median()) if total else 0.0
    refusal_rate = float(filtered["is_refused"].mean() * 100) if total else 0.0
    appeal_rate = float(filtered["is_appealed"].mean() * 100) if total else 0.0

    pop = (
        filtered[["SA_GUID_21"]]
        .dropna()
        .drop_duplicates()
        .merge(sa_pop_lookup, on="SA_GUID_21", how="left")["population"]
        .fillna(0)
        .sum()
    )
    letters_per_1000 = float(total_letters / pop * 1000) if pop > 0 else 0.0

    def _count_pct(mask: pd.Series) -> dict[str, float]:
        count = int(mask.sum())
        pct = float(count / total * 100) if total else 0.0
        return {"count": count, "pct": pct}

    development_breakdown = {
        "residential": _count_pct(filtered["is_residential"]) if total else {"count": 0, "pct": 0.0},
        "multi_unit": _count_pct(filtered["is_multi_unit"]) if total else {"count": 0, "pct": 0.0},
        "one_off": _count_pct(filtered["is_one_off"]) if total else {"count": 0, "pct": 0.0},
        "commercial": _count_pct(filtered["is_commercial"]) if total else {"count": 0, "pct": 0.0},
        "extension": _count_pct(filtered["is_extension"]) if total else {"count": 0, "pct": 0.0},
    }

    outcomes_breakdown = {
        "granted": _count_pct(filtered["is_granted"]) if total else {"count": 0, "pct": 0.0},
        "refused": _count_pct(filtered["is_refused"]) if total else {"count": 0, "pct": 0.0},
        "appealed": _count_pct(filtered["is_appealed"]) if total else {"count": 0, "pct": 0.0},
        "overturned": _count_pct(filtered["is_overturned"]) if total else {"count": 0, "pct": 0.0},
    }

    return {
        "total_applications": total,
        "pct_with_objection": float(with_obs / total * 100) if total else 0.0,
        "median_letters": median_letters,
        "letters_per_1000_residents": letters_per_1000,
        "refusal_rate": refusal_rate,
        "appeal_rate": appeal_rate,
        "development_breakdown": development_breakdown,
        "outcomes_breakdown": outcomes_breakdown,
    }


@lru_cache(maxsize=128)
def _aggregate_bundle(sig: FilterSignature) -> dict[str, Any]:
    filtered = _apply_filters(DATA["applications"], sig)
    sa = _aggregate(filtered, DATA["sa_base"], "SA_GUID_21")
    ed = _aggregate(filtered, DATA["ed_base"], "ED_GUID")
    return {"filtered": filtered, "sa": sa, "ed": ed}


def _apply_scale(sa: gpd.GeoDataFrame, ed: gpd.GeoDataFrame, metric: str) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, float]:
    valid_metric = metric if metric in sa.columns else "letters_per_1000"
    combined = pd.concat([sa[valid_metric], ed[valid_metric]], ignore_index=True).fillna(0)
    positive = combined[combined > 0]
    cap = float(np.nanpercentile(positive, 95)) if not positive.empty else 0.0

    def _decorate(frame: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        out = frame.copy()
        raw = out[valid_metric].fillna(0).astype(float)
        out["choropleth_metric"] = valid_metric
        out["choropleth_raw"] = raw
        out["choropleth_cap"] = cap
        out["choropleth_value"] = np.where(raw == 0, 0, np.minimum(raw, cap if cap > 0 else raw))
        out["choropleth_bucket"] = np.where(raw == 0, "zero", "positive")
        return out

    return _decorate(sa), _decorate(ed), cap


def _filter_bbox(df: gpd.GeoDataFrame, min_lng: float | None, min_lat: float | None, max_lng: float | None, max_lat: float | None) -> gpd.GeoDataFrame:
    if None in {min_lng, min_lat, max_lng, max_lat}:
        return df
    bbox_geom = box(min_lng, min_lat, max_lng, max_lat)
    return df.loc[df.geometry.intersects(bbox_geom)]


def _to_geojson(gdf: gpd.GeoDataFrame, drop_cols: set[str] | None = None) -> dict[str, Any]:
    if drop_cols:
        keep = [c for c in gdf.columns if c not in drop_cols]
        gdf = gdf[keep]
    return json.loads(gdf.to_json())


app = FastAPI(title="Planning Applications Explorer API", version="0.1.0")
app.add_middleware(GZipMiddleware, minimum_size=1024)
frontend_origins = [o.strip() for o in os.getenv("FRONTEND_ORIGIN", "*").split(",") if o.strip()]
allow_credentials = frontend_origins != ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=frontend_origins if frontend_origins else ["*"],
    allow_credentials=allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/meta")
def meta() -> dict[str, Any]:
    return {
        "year_min": DATA["year_min"],
        "year_max": DATA["year_max"],
        "total_applications": int(len(DATA["applications"])),
    }


@app.get("/applications")
def applications(
    year: int | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    year_min: int | None = Query(default=None),
    year_max: int | None = Query(default=None),
    development: list[str] | None = Query(default=None),
    min_site_area: float | None = Query(default=None),
    min_units: int | None = Query(default=None),
    high_density: bool = Query(default=False),
    has_objection: bool = Query(default=False),
    min_letters: int | None = Query(default=None),
    top_decile: bool = Query(default=False),
    outcomes: list[str] | None = Query(default=None),
    decision: list[str] | None = Query(default=None),
    min_lng: float | None = Query(default=None),
    min_lat: float | None = Query(default=None),
    max_lng: float | None = Query(default=None),
    max_lat: float | None = Query(default=None),
) -> dict[str, Any]:
    year_min, year_max = _resolve_year_window(year, year_min, year_max)
    sig = _signature(
        date_from,
        date_to,
        year_min,
        year_max,
        development,
        min_site_area,
        min_units,
        high_density,
        has_objection,
        min_letters,
        top_decile,
        outcomes,
        decision,
    )
    filtered = _aggregate_bundle(sig)["filtered"]
    filtered = _filter_bbox(filtered, min_lng, min_lat, max_lng, max_lat)

    keep_cols = {
        "application_number",
        "development_address",
        "development_description",
        "received_date",
        "decision_date",
        "number_of_units",
        "site_area",
        "floor_area",
        "n_observation_letters",
        "decision",
        "appeal_status",
        "portal_link",
        "SA_GUID_21",
        "ED_GUID",
        "ED_ENGLISH",
        "geometry",
    }
    out = filtered[[c for c in filtered.columns if c in keep_cols]].copy()
    out["received_date"] = out["received_date"].dt.strftime("%Y-%m-%d")
    out["decision_date"] = out["decision_date"].dt.strftime("%Y-%m-%d")
    return _to_geojson(out)


@app.get("/small_areas")
def small_areas(
    metric: str = Query(default="letters_per_1000"),
    year: int | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    year_min: int | None = Query(default=None),
    year_max: int | None = Query(default=None),
    development: list[str] | None = Query(default=None),
    min_site_area: float | None = Query(default=None),
    min_units: int | None = Query(default=None),
    high_density: bool = Query(default=False),
    has_objection: bool = Query(default=False),
    min_letters: int | None = Query(default=None),
    top_decile: bool = Query(default=False),
    outcomes: list[str] | None = Query(default=None),
    decision: list[str] | None = Query(default=None),
    min_lng: float | None = Query(default=None),
    min_lat: float | None = Query(default=None),
    max_lng: float | None = Query(default=None),
    max_lat: float | None = Query(default=None),
) -> dict[str, Any]:
    year_min, year_max = _resolve_year_window(year, year_min, year_max)
    sig = _signature(
        date_from,
        date_to,
        year_min,
        year_max,
        development,
        min_site_area,
        min_units,
        high_density,
        has_objection,
        min_letters,
        top_decile,
        outcomes,
        decision,
    )
    filtered = _filter_bbox(_aggregate_bundle(sig)["filtered"], min_lng, min_lat, max_lng, max_lat)
    sa_agg = _aggregate(filtered, DATA["sa_base"], "SA_GUID_21")
    ed_agg = _aggregate(filtered, DATA["ed_base"], "ED_GUID")
    sa, _, cap = _apply_scale(sa_agg, ed_agg, metric)
    payload = _to_geojson(sa)
    payload["metadata"] = {"metric": metric, "cap_95": cap}
    return payload


@app.get("/electoral_divisions")
def electoral_divisions(
    metric: str = Query(default="letters_per_1000"),
    year: int | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    year_min: int | None = Query(default=None),
    year_max: int | None = Query(default=None),
    development: list[str] | None = Query(default=None),
    min_site_area: float | None = Query(default=None),
    min_units: int | None = Query(default=None),
    high_density: bool = Query(default=False),
    has_objection: bool = Query(default=False),
    min_letters: int | None = Query(default=None),
    top_decile: bool = Query(default=False),
    outcomes: list[str] | None = Query(default=None),
    decision: list[str] | None = Query(default=None),
    min_lng: float | None = Query(default=None),
    min_lat: float | None = Query(default=None),
    max_lng: float | None = Query(default=None),
    max_lat: float | None = Query(default=None),
) -> dict[str, Any]:
    year_min, year_max = _resolve_year_window(year, year_min, year_max)
    sig = _signature(
        date_from,
        date_to,
        year_min,
        year_max,
        development,
        min_site_area,
        min_units,
        high_density,
        has_objection,
        min_letters,
        top_decile,
        outcomes,
        decision,
    )
    filtered = _filter_bbox(_aggregate_bundle(sig)["filtered"], min_lng, min_lat, max_lng, max_lat)
    sa_agg = _aggregate(filtered, DATA["sa_base"], "SA_GUID_21")
    ed_agg = _aggregate(filtered, DATA["ed_base"], "ED_GUID")
    _, ed, cap = _apply_scale(sa_agg, ed_agg, metric)
    payload = _to_geojson(ed)
    payload["metadata"] = {"metric": metric, "cap_95": cap}
    return payload


@app.get("/summary")
def summary(
    year: int | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    year_min: int | None = Query(default=None),
    year_max: int | None = Query(default=None),
    development: list[str] | None = Query(default=None),
    min_site_area: float | None = Query(default=None),
    min_units: int | None = Query(default=None),
    high_density: bool = Query(default=False),
    has_objection: bool = Query(default=False),
    min_letters: int | None = Query(default=None),
    top_decile: bool = Query(default=False),
    outcomes: list[str] | None = Query(default=None),
    decision: list[str] | None = Query(default=None),
    min_lng: float | None = Query(default=None),
    min_lat: float | None = Query(default=None),
    max_lng: float | None = Query(default=None),
    max_lat: float | None = Query(default=None),
) -> dict[str, Any]:
    year_min, year_max = _resolve_year_window(year, year_min, year_max)
    sig = _signature(
        date_from,
        date_to,
        year_min,
        year_max,
        development,
        min_site_area,
        min_units,
        high_density,
        has_objection,
        min_letters,
        top_decile,
        outcomes,
        decision,
    )
    filtered = _filter_bbox(_aggregate_bundle(sig)["filtered"], min_lng, min_lat, max_lng, max_lat)

    sa_pop_lookup = DATA["sa_base"][["SA_GUID_21", "population"]].drop_duplicates()
    return _summary_from_filtered(filtered, sa_pop_lookup)


@app.get("/region_summary")
def region_summary(
    region_type: str = Query(..., pattern="^(sa|ed)$"),
    region_id: str = Query(...),
    year: int | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    year_min: int | None = Query(default=None),
    year_max: int | None = Query(default=None),
    development: list[str] | None = Query(default=None),
    min_site_area: float | None = Query(default=None),
    min_units: int | None = Query(default=None),
    high_density: bool = Query(default=False),
    has_objection: bool = Query(default=False),
    min_letters: int | None = Query(default=None),
    top_decile: bool = Query(default=False),
    outcomes: list[str] | None = Query(default=None),
    decision: list[str] | None = Query(default=None),
) -> dict[str, Any]:
    year_min, year_max = _resolve_year_window(year, year_min, year_max)
    sig = _signature(
        date_from,
        date_to,
        year_min,
        year_max,
        development,
        min_site_area,
        min_units,
        high_density,
        has_objection,
        min_letters,
        top_decile,
        outcomes,
        decision,
    )
    filtered = _aggregate_bundle(sig)["filtered"]

    if region_type == "sa":
        region_filtered = filtered.loc[filtered["SA_GUID_21"] == region_id]
        region_row = DATA["sa_base"].loc[DATA["sa_base"]["SA_GUID_21"] == region_id]
        region_name = None if region_row.empty else str(region_row.iloc[0].get("SA_PUB2022", region_id))
    else:
        region_filtered = filtered.loc[filtered["ED_GUID"] == region_id]
        region_row = DATA["ed_base"].loc[DATA["ed_base"]["ED_GUID"] == region_id]
        region_name = None if region_row.empty else str(region_row.iloc[0].get("ed_name", region_id))

    if region_row.empty:
        raise HTTPException(status_code=404, detail="Region not found")

    sa_pop_lookup = DATA["sa_base"][["SA_GUID_21", "population"]].drop_duplicates()
    summary_payload = _summary_from_filtered(region_filtered, sa_pop_lookup)
    summary_payload["region_type"] = region_type
    summary_payload["region_id"] = region_id
    summary_payload["region_name"] = region_name
    return summary_payload
