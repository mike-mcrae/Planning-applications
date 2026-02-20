import requests
import pandas as pd
import re

# =====================================================
# CONFIG
# =====================================================

# This is the endpoint used by CSO PxStat JSON-stat TOC
# (If CSO change it again, print the response.text[:500] and we adjust.)
TOC_URL_CANDIDATES = [
    # common PxStat TOC endpoints seen in the wild
    "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDatasetList",
    "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadCubeList",
    "https://ws.cso.ie/public/api.jsonstat/catalogue",  # may 404, harmless
]

SEARCH_TERMS = ["small area", "population"]  # adjust if you want
MAX_PRINT = 60

# =====================================================
# HELPERS
# =====================================================

def fetch_json(url: str):
    r = requests.get(url, timeout=60)
    if r.status_code != 200:
        return None, r.status_code, r.text[:300]
    try:
        return r.json(), r.status_code, None
    except Exception:
        return None, r.status_code, r.text[:300]

def normalize_toc(json_data):
    """
    Tries to normalize different CSO list schemas into a DataFrame
    with at least: table_id/code, title/label, updated/last_updated (optional)
    """
    if json_data is None:
        return pd.DataFrame()

    # Many CSO list endpoints return {"dataset":[{...}, ...]} or a list directly
    if isinstance(json_data, dict):
        # try common keys
        for k in ["dataset", "datasets", "data", "result", "Results", "items"]:
            if k in json_data and isinstance(json_data[k], list):
                records = json_data[k]
                break
        else:
            # maybe dict of dicts
            records = None
    else:
        records = json_data if isinstance(json_data, list) else None

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Harmonize columns
    colmap = {}
    # likely id/code fields
    for c in df.columns:
        lc = c.lower()
        if lc in ["code", "datasetcode", "table", "tablecode", "cube", "cubecode", "id", "identifier"]:
            colmap[c] = "table_id"
        if lc in ["label", "title", "name", "dataset", "description"]:
            colmap[c] = "table_name"
        if lc in ["updated", "lastupdated", "last_updated", "modified", "datetime"]:
            colmap[c] = "updated"

    df = df.rename(columns=colmap)

    # If we didn't catch table_id, try to infer from known fields
    if "table_id" not in df.columns:
        for candidate in ["DatasetCode", "datasetCode", "Dataset", "dataset", "Code", "code", "ID", "id"]:
            if candidate in df.columns:
                df["table_id"] = df[candidate].astype(str)
                break

    if "table_name" not in df.columns:
        for candidate in ["Title", "title", "Label", "label", "Name", "name", "Description", "description"]:
            if candidate in df.columns:
                df["table_name"] = df[candidate].astype(str)
                break

    if "updated" in df.columns:
        df["updated_parsed"] = pd.to_datetime(df["updated"], format="mixed", utc=True, errors="coerce")

    # keep only useful cols if they exist
    keep = [c for c in ["table_id", "table_name", "updated", "updated_parsed"] if c in df.columns]
    if keep:
        df = df[keep].drop_duplicates()

    return df

def search_df(df: pd.DataFrame, terms):
    if df.empty:
        return df
    s = df["table_name"].fillna("").str.lower()
    mask = pd.Series(True, index=df.index)
    for t in terms:
        mask &= s.str.contains(re.escape(t.lower()))
    return df.loc[mask].copy()

# =====================================================
# MAIN
# =====================================================

toc_df = pd.DataFrame()

for url in TOC_URL_CANDIDATES:
    json_data, status, err = fetch_json(url)
    if status == 200 and json_data is not None:
        toc_df = normalize_toc(json_data)
        if not toc_df.empty:
            print(f"Loaded TOC from: {url}")
            break
    else:
        print(f"TOC endpoint failed: {url} | status={status} | {err}")

if toc_df.empty:
    raise SystemExit("Could not load any TOC list from CSO endpoints. Paste the failing URLs + status codes and we’ll adjust.")

print("TOC rows:", len(toc_df))
print("TOC columns:", toc_df.columns.tolist())

hits = search_df(toc_df, SEARCH_TERMS).sort_values(["table_name"])
print(f"\nMatches for terms {SEARCH_TERMS}: {len(hits)}\n")

with pd.option_context("display.max_rows", MAX_PRINT, "display.max_colwidth", 120):
    print(hits.head(MAX_PRINT))

print("\nTIP: pick a table_id from the printed list, then run 7b.download_cso_table.py with that code.")
