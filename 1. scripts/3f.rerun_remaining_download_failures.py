#!/usr/bin/env python3
"""
Round-3 parallel retry of remaining DOWNLOAD_FAILED entries.
"""

from __future__ import annotations
import sys
import time
import urllib.parse
from pathlib import Path
import pandas as pd
import requests
from playwright.sync_api import sync_playwright, TimeoutError

if len(sys.argv) != 3:
    print("Usage: python script.py <worker_id> <n_workers>")
    sys.exit(1)

WORKER_ID = int(sys.argv[1])
N_WORKERS = int(sys.argv[2])

BASE = Path("/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/outputs")

INPUT = BASE / "rerun_download_failures_round3.csv"
OUT = BASE / f"rerun_download_failures_round3_worker_{WORKER_ID}.csv"
DOWNLOAD_DIR = BASE / "downloads"

BASE_URL = (
    "https://webapps.dublincity.ie/PublicAccess_Live/"
    "SearchResult/RunThirdPartySearch"
)

OBS_SELECTOR = (
    'span[aria-label*="3rd Party Observation"], '
    'span[aria-label*="Third Party Observation"]'
)

def build_url(app_no: str):
    return BASE_URL + "?" + urllib.parse.urlencode({
        "FileSystemId": "PL",
        "Folder1_Ref": app_no.strip()
    })

def download(url, out):
    r = requests.get(url, timeout=90)
    r.raise_for_status()
    out.write_bytes(r.content)

def append(row):
    pd.DataFrame([row]).to_csv(
        OUT, mode="a", header=not OUT.exists(), index=False
    )

def main():
    df = pd.read_csv(INPUT, low_memory=False)

    total = len(df)
    chunk = total // N_WORKERS
    start = WORKER_ID * chunk
    end = total if WORKER_ID == N_WORKERS - 1 else (WORKER_ID + 1) * chunk
    df_chunk = df.iloc[start:end]

    print(f"[Worker {WORKER_ID}] Processing {len(df_chunk)} rows")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for _, row in df_chunk.iterrows():
            record = row.to_dict()
            app_no = record["application_number"]
            urls = record["observation_urls"].split(";")

            try:
                page.goto(build_url(app_no), timeout=45000)
                page.wait_for_timeout(1200)

                icons = page.locator(OBS_SELECTOR)

                for i, u in enumerate(urls):
                    if u != "DOWNLOAD_FAILED":
                        continue

                    try:
                        icon = icons.nth(i)
                        with page.expect_popup(timeout=12000) as pop:
                            icon.click(timeout=5000)
                        popup = pop.value
                        popup.wait_for_load_state()
                        doc_url = popup.url
                        popup.close()

                        out_file = DOWNLOAD_DIR / f"{app_no.replace('/', '_')}_obs_{i+1}.pdf"
                        download(doc_url, out_file)
                        urls[i] = doc_url
                    except Exception:
                        pass

            except Exception:
                pass

            record["observation_urls"] = ";".join(urls)
            append(record)
            time.sleep(1.2)

        browser.close()

if __name__ == "__main__":
    main()
