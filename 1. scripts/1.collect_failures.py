#!/usr/bin/env python3
"""
Collect and classify failures from completed worker outputs.

Produces:
1. rerun_page_failures.csv
2. rerun_download_failures.csv
"""

from pathlib import Path
import pandas as pd

OUT_DIR = Path(
    "/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/outputs"
)

WORKER_FILES = [OUT_DIR / f"third_party_obs_worker_{i}.csv" for i in range(10)]

PAGE_FAIL_OUT = OUT_DIR / "rerun_page_failures.csv"
DOWNLOAD_FAIL_OUT = OUT_DIR / "rerun_download_failures.csv"

page_fail_rows = []
download_fail_rows = []

for path in WORKER_FILES:
    if not path.exists():
        continue

    df = pd.read_csv(path)

    for _, row in df.iterrows():
        error = str(row.get("error", "")).strip()
        urls = str(row.get("observation_urls", "")).strip()

        # -------- PAGE-LEVEL FAILURE --------
        if error and "Page.goto" in error:
            page_fail_rows.append(row)
            continue

        # -------- DOWNLOAD-LEVEL FAILURE --------
        if "DOWNLOAD_FAILED" in urls:
            parts = urls.split(";")
            failed_positions = [
                i for i, p in enumerate(parts) if p == "DOWNLOAD_FAILED"
            ]

            download_fail_rows.append({
                "row_index": row["row_index"],
                "worker_id": row["worker_id"],
                "application_number": row["application_number"],
                "failed_positions": ",".join(map(str, failed_positions)),
                "observation_urls": urls,
            })

# -------- WRITE OUTPUTS --------

if page_fail_rows:
    pd.DataFrame(page_fail_rows).to_csv(PAGE_FAIL_OUT, index=False)
    print(f"Wrote {len(page_fail_rows)} page failures → {PAGE_FAIL_OUT}")

if download_fail_rows:
    pd.DataFrame(download_fail_rows).to_csv(DOWNLOAD_FAIL_OUT, index=False)
    print(f"Wrote {len(download_fail_rows)} download failures → {DOWNLOAD_FAIL_OUT}")

if not page_fail_rows and not download_fail_rows:
    print("No failures detected 🎉")
