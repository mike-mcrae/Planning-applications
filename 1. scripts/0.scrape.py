#!/usr/bin/env python3
"""
scrape_observations_worker.py

Parallel worker for scraping Dublin City Council PublicAccess third-party
observation letters.

Enhancements:
- Handles pagination via "Next" button
- Counts observations even if download fails
- Matches both "3rd Party Observation" and "Third Party Observation"
"""

from __future__ import annotations

import sys
import time
import urllib.parse
from pathlib import Path
from typing import List, Set

import pandas as pd
import requests
from playwright.sync_api import sync_playwright, TimeoutError

# ---------------- CONFIG ----------------

INPUT_CSV = Path(
    "/Users/mikemcrae/Documents/GitHub/Planning applications/0. data/"
    "IrishPlanningApplications_2359694955245257726.csv"
)

OUT_DIR = Path("downloads")
OUT_DIR.mkdir(exist_ok=True)

OUT_CSV_DIR = Path("outputs")
OUT_CSV_DIR.mkdir(exist_ok=True)

OUT_LOG_DIR = Path("logs")
OUT_LOG_DIR.mkdir(exist_ok=True)

BASE_URL = (
    "https://webapps.dublincity.ie/PublicAccess_Live/"
    "SearchResult/RunThirdPartySearch"
)

RESULTS_LENGTH_SELECT = 'select[name="searchResult_length"]'
OBS_ICON_SELECTOR = (
    'span[aria-label*="3rd Party Observation"], '
    'span[aria-label*="Third Party Observation"]'
)
NEXT_BUTTON_SELECTOR = "#searchResult_next"
NO_RECORD_SELECTOR = "#searchResult_info"

# timeouts
GOTO_TIMEOUT_MS = 45_000
NETWORKIDLE_TIMEOUT_MS = 15_000
POPUP_TIMEOUT_MS = 10_000
DOWNLOAD_TIMEOUT_MS = 10_000
HTTP_TIMEOUT_S = 60

PER_ROW_SLEEP_S = 1.25
RESET_PAGE_EVERY = 50

# ---------------- ARGS ----------------

if len(sys.argv) != 3:
    print("Usage: python scrape_observations_worker.py <worker_id> <n_workers>")
    sys.exit(1)

WORKER_ID = int(sys.argv[1])
N_WORKERS = int(sys.argv[2])

OUT_CSV = OUT_CSV_DIR / f"third_party_obs_worker_{WORKER_ID}.csv"
OUT_LOG = OUT_LOG_DIR / f"third_party_obs_worker_{WORKER_ID}.log"

# ---------------- HELPERS ----------------


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with OUT_LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def build_search_url(app_number: str) -> str:
    params = {"FileSystemId": "PL", "Folder1_Ref": app_number.strip()}
    return BASE_URL + "?" + urllib.parse.urlencode(params)


def set_results_to_100(page) -> None:
    try:
        sel = page.locator(RESULTS_LENGTH_SELECT)
        if sel.count() > 0:
            sel.select_option("100")
            page.wait_for_timeout(800)
    except Exception:
        pass


def page_has_no_records(page) -> bool:
    try:
        info = page.locator(NO_RECORD_SELECTOR)
        if info.count() == 0:
            return False
        return "0 to 0 of 0" in info.first.inner_text(timeout=5_000)
    except Exception:
        return False


def download_from_url(url: str, out_path: Path) -> None:
    r = requests.get(url, timeout=HTTP_TIMEOUT_S)
    r.raise_for_status()
    out_path.write_bytes(r.content)


def extract_all_pages_observations(page, app_no: str) -> List[str]:
    urls: List[str] = []
    safe_app = app_no.replace("/", "_")

    while True:
        icons = page.locator(OBS_ICON_SELECTOR)
        n = icons.count()

        for i in range(n):
            out_file = OUT_DIR / f"{safe_app}_obs_{len(urls)+1}.pdf"
            icon = icons.nth(i)

            try:
                icon.scroll_into_view_if_needed(timeout=5_000)

                try:
                    with page.expect_popup(timeout=POPUP_TIMEOUT_MS) as pop:
                        icon.click(timeout=5_000)
                    popup = pop.value
                    popup.wait_for_load_state()
                    doc_url = popup.url
                    popup.close()
                except TimeoutError:
                    with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as dl:
                        icon.click(timeout=5_000)
                    doc_url = dl.value.url

                download_from_url(doc_url, out_file)
                urls.append(doc_url)

            except Exception:
                # count it anyway, but mark failure
                urls.append("DOWNLOAD_FAILED")

        # pagination
        next_btn = page.locator(NEXT_BUTTON_SELECTOR)
        if next_btn.count() == 0:
            break

        classes = next_btn.get_attribute("class") or ""
        if "disabled" in classes:
            break

        next_btn.scroll_into_view_if_needed()
        next_btn.click()
        page.wait_for_timeout(1200)

    return urls


def load_done_rows() -> Set[int]:
    if not OUT_CSV.exists():
        return set()
    return set(pd.read_csv(OUT_CSV, usecols=["row_index"])["row_index"].dropna())


def append_record(record: dict) -> None:
    pd.DataFrame([record]).to_csv(
        OUT_CSV, mode="a", header=not OUT_CSV.exists(), index=False
    )


# ---------------- MAIN ----------------


def main() -> None:
    log(f"[Worker {WORKER_ID}] Starting")

    df = pd.read_csv(INPUT_CSV, low_memory=False)
    df = df[df["Planning Authority"] == "Dublin City Council"]

    total = len(df)
    chunk = total // N_WORKERS
    start = WORKER_ID * chunk
    end = total if WORKER_ID == N_WORKERS - 1 else (WORKER_ID + 1) * chunk
    df_chunk = df.iloc[start:end]

    done_rows = load_done_rows()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        processed = 0

        for idx, row in df_chunk.iterrows():
            if idx in done_rows:
                continue

            app_no = str(row["Application Number"]).strip()
            record = {
                "worker_id": WORKER_ID,
                "row_index": idx,
                "application_number": app_no,
                "no_record_found": 0,
                "has_third_party_observation": False,
                "n_observation_letters": 0,
                "observation_urls": "",
                "error": "",
            }

            try:
                page.goto(build_search_url(app_no), timeout=GOTO_TIMEOUT_MS)
                page.wait_for_timeout(1500)
                set_results_to_100(page)

                if page_has_no_records(page):
                    record["no_record_found"] = 1
                else:
                    urls = extract_all_pages_observations(page, app_no)
                    record["n_observation_letters"] = len(urls)
                    record["has_third_party_observation"] = len(urls) > 0
                    record["observation_urls"] = ";".join(urls)

            except Exception as e:
                record["error"] = repr(e)

            append_record(record)
            processed += 1
            time.sleep(PER_ROW_SLEEP_S)

            if processed % RESET_PAGE_EVERY == 0:
                page.close()
                page = context.new_page()

        browser.close()

    log(f"[Worker {WORKER_ID}] Finished")


if __name__ == "__main__":
    main()
