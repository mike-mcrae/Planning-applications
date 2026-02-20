#!/usr/bin/env python3
"""
consolidate_and_extract_remaining_errors.py

Merges:
- all worker outputs
- rerun error outputs

Rules:
- Merge on row_index
- Rerun rows dominate worker rows
- Identify rows that STILL have errors after rerun
"""

from pathlib import Path
import pandas as pd

BASE_DIR = Path(
    "/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts"
)

OUTPUT_DIR = BASE_DIR / "outputs"

WORKER_FILES = [
    OUTPUT_DIR / f"third_party_obs_worker_{i}.csv"
    for i in range(10)
]

RERUN_FILE = OUTPUT_DIR / "third_party_obs_rerun_errors.csv"

CONSOLIDATED_OUT = OUTPUT_DIR / "third_party_obs_consolidated.csv"
STILL_ERRORS_OUT = OUTPUT_DIR / "third_party_obs_still_errors.csv"


def load_csvs(paths):
    dfs = []
    for p in paths:
        if p.exists():
            df = pd.read_csv(p, low_memory=False)
            dfs.append(df)
    return dfs


def main():
    print("Loading worker files...")
    worker_dfs = load_csvs(WORKER_FILES)

    if not worker_dfs:
        raise RuntimeError("No worker CSVs found")

    workers = pd.concat(worker_dfs, ignore_index=True)

    print(f"Loaded {len(workers)} worker rows")

    if RERUN_FILE.exists():
        print("Loading rerun file...")
        rerun = pd.read_csv(RERUN_FILE, low_memory=False)
        print(f"Loaded {len(rerun)} rerun rows")
    else:
        rerun = pd.DataFrame()
        print("No rerun file found")

    # --- Normalize columns ---
    for df in [workers, rerun]:
        if not df.empty:
            df["row_index"] = df["row_index"].astype(int)
            df["error"] = df["error"].fillna("").astype(str)

    # --- Merge logic ---
    # Start with workers, overwrite with rerun where present
    consolidated = workers.set_index("row_index")

    if not rerun.empty:
        rerun = rerun.set_index("row_index")
        consolidated.update(rerun)

    consolidated = consolidated.reset_index()

    # --- Write consolidated output ---
    consolidated.to_csv(CONSOLIDATED_OUT, index=False)
    print(f"Wrote consolidated file → {CONSOLIDATED_OUT}")

    # --- Identify remaining errors ---
    still_errors = consolidated[
        consolidated["error"].str.strip() != ""
    ].copy()

    still_errors.to_csv(STILL_ERRORS_OUT, index=False)

    print(f"Remaining errors: {len(still_errors)}")
    print(f"Wrote retry file → {STILL_ERRORS_OUT}")


if __name__ == "__main__":
    main()
