#!/usr/bin/env python3
"""
Merge round-2 parallel download retry results into third_party_obs_merged.csv.
"""

from pathlib import Path
import pandas as pd

BASE = Path("/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/outputs")

MERGED = BASE / "third_party_obs_merged.csv"
OUT = BASE / "third_party_obs_merged_v2.csv"

WORKER_FILES = [
    BASE / f"rerun_download_failures_round2_worker_{i}.csv"
    for i in range(8)
]

# Load current merged dataset
df = pd.read_csv(MERGED, low_memory=False)
df = df.set_index("row_index")

# Merge worker outputs
for path in WORKER_FILES:
    if not path.exists():
        continue

    fix = pd.read_csv(path, low_memory=False).set_index("row_index")

    for idx, row in fix.iterrows():
        if idx in df.index:
            df.loc[idx, "observation_urls"] = row["observation_urls"]

df.reset_index().to_csv(OUT, index=False)

print(f"✅ Round-2 results merged → {OUT}")
print(f"Rows: {len(df)}")
