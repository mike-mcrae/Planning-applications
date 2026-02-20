#!/usr/bin/env python3

from pathlib import Path
import pandas as pd

BASE = Path("/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/outputs")

MERGED = BASE / "third_party_obs_merged_v2.csv"
OUT = BASE / "rerun_download_failures_round3.csv"

df = pd.read_csv(MERGED, low_memory=False)

mask = df["observation_urls"].astype(str).str.contains("DOWNLOAD_FAILED", na=False)
remaining = df[mask]

remaining.to_csv(OUT, index=False)

print(f"Remaining failures after round 2: {len(remaining)}")
print(f"Wrote → {OUT}")
