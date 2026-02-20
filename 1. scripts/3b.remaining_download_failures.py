#!/usr/bin/env python3
"""
Identify remaining DOWNLOAD_FAILED entries after final merge.
"""

from pathlib import Path
import pandas as pd

BASE = Path("/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/outputs")

MERGED = BASE / "third_party_obs_merged.csv"
OUT = BASE / "rerun_download_failures_round2.csv"

df = pd.read_csv(MERGED, low_memory=False)

mask = df["observation_urls"].astype(str).str.contains("DOWNLOAD_FAILED", na=False)
failed = df[mask]

failed.to_csv(OUT, index=False)
print(f"Remaining download failures: {len(failed)}")
print(f"Wrote → {OUT}")
