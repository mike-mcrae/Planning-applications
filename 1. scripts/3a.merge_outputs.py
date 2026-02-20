#!/usr/bin/env python3
"""
Final merge script for Dublin City Council third-party observations.

Priority:
1. rerun_page_failures_results.csv (full override)
2. worker outputs
3. rerun_download_failures_results.csv (URL patch only)

Produces:
- third_party_obs_merged.csv
"""

from pathlib import Path
import pandas as pd

BASE = Path("/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/outputs")

WORKERS = [BASE / f"third_party_obs_worker_{i}.csv" for i in range(10)]
PAGE_FIX = BASE / "rerun_page_failures_results.csv"
DL_FIX = BASE / "rerun_download_failures_results.csv"

OUT = BASE / "third_party_obs_merged.csv"

# ---------------- LOAD BASE DATA ----------------

dfs = []
for f in WORKERS:
    if f.exists():
        dfs.append(pd.read_csv(f, low_memory=False))

base = pd.concat(dfs, ignore_index=True)
base = base.drop_duplicates(subset=["row_index"], keep="first")
base = base.set_index("row_index")

# ---------------- APPLY PAGE-LEVEL OVERRIDES ----------------

if PAGE_FIX.exists():
    page_fix = pd.read_csv(PAGE_FIX, low_memory=False)
    page_fix = page_fix.set_index("row_index")

    for idx in page_fix.index:
        base.loc[idx] = page_fix.loc[idx]

# ---------------- APPLY DOWNLOAD-LEVEL PATCHES ----------------

if DL_FIX.exists():
    dl_fix = pd.read_csv(DL_FIX, low_memory=False)
    dl_fix = dl_fix.set_index("row_index")

    for idx, row in dl_fix.iterrows():
        if idx not in base.index:
            continue
        base.loc[idx, "observation_urls"] = row["observation_urls"]

# ---------------- WRITE OUTPUT ----------------

base.reset_index().to_csv(OUT, index=False)
print(f"✅ Final merged dataset written → {OUT}")
print(f"Rows: {len(base)}")
