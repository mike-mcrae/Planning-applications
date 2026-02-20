#!/usr/bin/env python3
"""
Retry-only incremental scraper for Dublin City Council
third-party observation letters.

This script:
- reads ONLY rows with errors
- retries them one-by-one
- WRITES EACH ROW IMMEDIATELY
- safe to stop/restart
"""

from __future__ import annotations

import time
import urllib.parse
from pathlib import Path
from typing import List

import pandas as pd
import requests
from playwright.sync_api import sync_playwright, TimeoutError

# ---------------- PATHS ----------------

INPUT_ERRORS = Path(
    "/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/outputs/"
    "third_party_obs_still_errors.csv"
)

OUT_RETRY = Path(
    "/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/outputs/"
    "third_party_obs_retry_pass.csv"
)

DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# ---------------- SITE CONFIG ----------------

BASE_URL = (
    "https://webapps.dublincity.ie/PublicAccess_Live/"
    "SearchResult/RunThirdPartySearch"
)

RESULTS_LENGTH_SELECT = 'select[name="searchResult_length"]'
OBS_ICON_SELECTOR = 'span[aria-label*="3rd Party Observation Letter"]'
NO_RECORD_SELECTOR = "#searchResult_info"

# ---------------- TIMEOUTS ----------------

GOTO_TIMEOUT_MS = 45_000
NETWORKIDLE_TIMEOUT_MS = 15_000
POPUP_TIMEOUT_MS = 15_000
DOWNLOAD_TIMEOUT_MS = 15_000
HTTP_TIMEOUT_S = 90

ROW_SLEEP_S = 2.0

# ---------------- HELPERS ----------------


def build_search_url(app_number: str) -> str:
    params = {
        "FileSystemId": "PL",
        "Folder1_Ref": app_number.strip(),
    }
    return BASE_URL + "?" + urllib.parse.urlencode(params)


def page_has_no_records(page) -> bool:
    try:
        info = page.locator(NO_RECORD_SELECTOR)
        if info.count() == 0:
            return False
        return "0 to 0 of 0" in info.first.inner_text(timeout=5_000)
    except Exception:
        return False


def set_results_to_100(page) -> None:
    try:
        sel = page.locator(RESULTS_LENGTH_SELECT)
        if sel.count() > 0:
            sel.select_option("100")
            page.wait_for_timeout(800)
    except Exception:
        pass


def download_from_url(url: str, out_path: Path) -> None:
    r = requests.get(url, timeout=HTTP_TIMEOUT_S)
    r.raise_for_status()
    out_path.write_bytes(r.content)


def extract_and_download_observations(page, app_no: str) -> List[str]:
    urls: List[str] = []
    icons = page.locator(OBS_ICON_SELECTOR)
    n = icons.count()

    if n == 0:
        return urls

    safe_app = app_no.replace("/", "_")

    for i in range(n):
        out_file = DOWNLOAD_DIR / f"{safe_app}_obs_{i+1}.pdf"

        if out_file.exists():
            urls.append("EXISTS")
            continue

        icon = icons.nth(i)
        icon.scroll_into_view_if_needed(timeout=10_000)

        try:
            with page.expect_popup(timeout=POPUP_TIMEOUT_MS) as pop:
                icon.click(timeout=10_000)
            popup = pop.value
            popup.wait_for_load_state()
            doc_url = popup.url
            popup.close()
        except TimeoutError:
            with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as dl:
                icon.click(timeout=10_000)
            doc_url = dl.value.url

        download_from_url(doc_url, out_file)
        urls.append(doc_url)

    return urls


def append_row(row: dict) -> None:
    pd.DataFrame([row]).to_csv(
        OUT_RETRY,
        mode="a",
        header=not OUT_RETRY.exists(),
        index=False,
    )


# ---------------- MAIN ----------------


def main() -> None:
    df = pd.read_csv(INPUT_ERRORS)

    # only rows that still have errors
    df = df[df["error"].notna() & (df["error"].str.strip() != "")].copy()

    if df.empty:
        print("No remaining errors to retry.")
        return

    print(f"Retrying {len(df)} failed rows")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context()
        page = context.new_page()

        for _, row in df.iterrows():
            record = row.to_dict()
            app_no = str(record["application_number"]).strip()

            print(f"Retrying row_index={record['row_index']} app={app_no}", flush=True)

            # reset fields
            record["error"] = ""
            record["no_record_found"] = 0
            record["has_third_party_observation"] = False
            record["n_observation_letters"] = 0
            record["observation_urls"] = ""

            try:
                page.goto(
                    build_search_url(app_no),
                    wait_until="domcontentloaded",
                    timeout=GOTO_TIMEOUT_MS,
                )

                try:
                    page.wait_for_load_state(
                        "networkidle", timeout=NETWORKIDLE_TIMEOUT_MS
                    )
                except Exception:
                    pass

                set_results_to_100(page)

                if page_has_no_records(page):
                    record["no_record_found"] = 1
                else:
                    urls = extract_and_download_observations(page, app_no)
                    record["n_observation_letters"] = len(urls)
                    record["has_third_party_observation"] = len(urls) > 0
                    record["observation_urls"] = ";".join(urls)

            except Exception as e:
                record["error"] = repr(e)

            # 🔹 WRITE IMMEDIATELY
            append_row(record)
            time.sleep(ROW_SLEEP_S)

        try:
            context.close()
            browser.close()
        except Exception:
            pass

    print(f"Retry pass finished → {OUT_RETRY}")


if __name__ == "__main__":
    main()
