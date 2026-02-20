#!/usr/bin/env python3
"""
Parallel retry of DOWNLOAD_FAILED observation documents.

Each worker:
- Reads the SAME input CSV
- Processes only its shard
- Writes incremental results
"""

from __future__ import annotations

import sys
import time
import urllib.parse
from pathlib import Path
from typing import List

import pandas as pd
import requests
from playwright.sync_api import sync_playwright, TimeoutError

# ---------------- ARGS ----------------

if len(sys.argv) != 3:
    print("Usage: python rerun_download_failures_parallel.py <worker_id> <n_workers>")
    sys.exit(1)

WORKER_ID = int(sys.argv[1])
N_WORKERS = int(sys.argv[2])

assert 0 <= WORKER_ID < N_WORKERS

# ---------------- PATHS ----------------

BASE = Path("/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/outputs")

INPUT = BASE / "rerun_download_failures_round2.csv"
OUT = BASE / f"rerun_download_failures_round2_worker_{WORKER_ID}.csv"

DOWNLOAD_DIR = BASE / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

# ---------------- SITE CONFIG ----------------

BASE_URL = (
    "https://webapps.dublincity.ie/PublicAccess_Live/"
    "SearchResult/RunThirdPartySearch"
)

OBS_SELECTOR = (
    'span[aria-label*="3rd Party Observation"], '
    'span[aria-label*="Third Party Observation"]'
)

POPUP_TIMEOUT_MS = 12_000
DOWNLOAD_TIMEOUT_MS = 12_000
HTTP_TIMEOUT_S = 90
ROW_SLEEP_S = 1.5

# ---------------- HELPERS ----------------


def build_url(app_no: str) -> str:
    return BASE_URL + "?" + urllib.parse.urlencode({
        "FileSystemId": "PL",
        "Folder1_Ref": app_no.strip()
    })


def download(url: str, out: Path) -> None:
    r = requests.get(url, timeout=HTTP_TIMEOUT_S)
    r.raise_for_status()
    out.write_bytes(r.content)


def append_row(row: dict) -> None:
    pd.DataFrame([row]).to_csv(
        OUT, mode="a", header=not OUT.exists(), index=False
    )


# ---------------- MAIN ----------------


def main() -> None:
    df = pd.read_csv(INPUT, low_memory=False)

    total = len(df)
    chunk = total // N_WORKERS
    start = WORKER_ID * chunk
    end = total if WORKER_ID == N_WORKERS - 1 else (WORKER_ID + 1) * chunk

    df_chunk = df.iloc[start:end]

    print(
        f"[Worker {WORKER_ID}] Processing rows {start}–{end - 1} "
        f"({len(df_chunk)} rows)"
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        for _, row in df_chunk.iterrows():
            record = row.to_dict()
            app_no = record["application_number"]
            urls = record["observation_urls"].split(";")

            print(f"[Worker {WORKER_ID}] {app_no}", flush=True)

            try:
                page.goto(build_url(app_no), timeout=45_000)
                page.wait_for_timeout(1200)

                icons = page.locator(OBS_SELECTOR)

                for i, u in enumerate(urls):
                    if u != "DOWNLOAD_FAILED":
                        continue

                    try:
                        icon = icons.nth(i)

                        with page.expect_popup(timeout=POPUP_TIMEOUT_MS) as pop:
                            icon.click(timeout=5_000)
                        popup = pop.value
                        popup.wait_for_load_state()
                        doc_url = popup.url
                        popup.close()

                        out_file = DOWNLOAD_DIR / f"{app_no.replace('/', '_')}_obs_{i+1}.pdf"
                        download(doc_url, out_file)
                        urls[i] = doc_url

                    except Exception:
                        # still failed → leave DOWNLOAD_FAILED
                        pass

            except Exception:
                pass

            record["observation_urls"] = ";".join(urls)
            append_row(record)

            time.sleep(ROW_SLEEP_S)

        browser.close()

    print(f"[Worker {WORKER_ID}] Finished")


if __name__ == "__main__":
    main()
