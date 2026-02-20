#!/usr/bin/env python3
"""
rerun_page_failures.py

Re-run ONLY page-level failures (Page.goto timeouts) using the enhanced
scraping logic:
- pagination via Next button
- match both "3rd Party Observation" and "Third Party Observation"
- count observations even if download fails (DOWNLOAD_FAILED placeholder)
- writes incrementally, resume-safe by row_index
"""

from __future__ import annotations

import time
import urllib.parse
from pathlib import Path
from typing import List, Set

import pandas as pd
import requests
from playwright.sync_api import sync_playwright, TimeoutError

# ---------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------

BASE_DIR = Path("/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts")
OUT_DIR = BASE_DIR / "downloads"
OUT_DIR.mkdir(exist_ok=True)

IN_CSV = BASE_DIR / "outputs" / "rerun_page_failures.csv"
OUT_CSV = BASE_DIR / "outputs" / "rerun_page_failures_results.csv"
OUT_LOG = BASE_DIR / "outputs" / "rerun_page_failures_results.log"

# ---------------------------------------------------------------------
# SITE CONFIG
# ---------------------------------------------------------------------

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

# ---------------------------------------------------------------------
# TIMEOUTS
# ---------------------------------------------------------------------

GOTO_TIMEOUT_MS = 60_000
NETWORKIDLE_TIMEOUT_MS = 20_000
POPUP_TIMEOUT_MS = 15_000
DOWNLOAD_TIMEOUT_MS = 15_000
HTTP_TIMEOUT_S = 90

GOTO_ATTEMPTS = 3
BACKOFF_BASE_S = 6
ROW_SLEEP_S = 1.5

# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with OUT_LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def build_search_url(app_number: str) -> str:
    params = {"FileSystemId": "PL", "Folder1_Ref": app_number.strip()}
    return BASE_URL + "?" + urllib.parse.urlencode(params)


def safe_goto(page, url: str) -> None:
    last_err = None
    for attempt in range(1, GOTO_ATTEMPTS + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
            return
        except Exception as e:
            last_err = e
            if attempt == GOTO_ATTEMPTS:
                raise
            sleep_s = BACKOFF_BASE_S * attempt
            log(f"goto failed (attempt {attempt}/{GOTO_ATTEMPTS}); sleep {sleep_s}s")
            time.sleep(sleep_s)
    raise last_err


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


def next_is_disabled(page) -> bool:
    btn = page.locator(NEXT_BUTTON_SELECTOR)
    if btn.count() == 0:
        return True
    cls = btn.first.get_attribute("class") or ""
    return "disabled" in cls


def extract_all_pages_observations(page, app_no: str) -> List[str]:
    """
    Crawl all pages via Next, and attempt to download each observation.
    If a download step fails, still count it and record DOWNLOAD_FAILED.
    """
    urls: List[str] = []
    safe_app = app_no.replace("/", "_")

    while True:
        icons = page.locator(OBS_ICON_SELECTOR)
        n = icons.count()

        for i in range(n):
            # global index across pages
            obs_idx = len(urls) + 1
            out_file = OUT_DIR / f"{safe_app}_obs_{obs_idx}.pdf"
            icon = icons.nth(i)

            try:
                try:
                    icon.scroll_into_view_if_needed(timeout=7_000)
                except Exception:
                    pass

                # If already downloaded, don’t click again — still count
                if out_file.exists():
                    urls.append("EXISTS")
                    continue

                try:
                    with page.expect_popup(timeout=POPUP_TIMEOUT_MS) as pop:
                        icon.click(timeout=7_000)
                    popup = pop.value
                    popup.wait_for_load_state()
                    doc_url = popup.url
                    popup.close()
                except TimeoutError:
                    with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as dl:
                        icon.click(timeout=7_000)
                    doc_url = dl.value.url

                download_from_url(doc_url, out_file)
                urls.append(doc_url)

            except Exception:
                urls.append("DOWNLOAD_FAILED")

        if next_is_disabled(page):
            break

        btn = page.locator(NEXT_BUTTON_SELECTOR).first
        try:
            btn.scroll_into_view_if_needed(timeout=5_000)
        except Exception:
            pass

        btn.click(timeout=10_000)
        page.wait_for_timeout(1200)

    return urls


def load_done_row_indices() -> Set[int]:
    if not OUT_CSV.exists():
        return set()
    try:
        s = pd.read_csv(OUT_CSV, usecols=["row_index"])["row_index"].dropna()
        return set(int(x) for x in s.tolist())
    except Exception:
        return set()


def append_record(record: dict) -> None:
    pd.DataFrame([record]).to_csv(
        OUT_CSV, mode="a", header=not OUT_CSV.exists(), index=False
    )


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------


def main() -> None:
    if not IN_CSV.exists():
        log(f"Missing input: {IN_CSV}")
        return

    df = pd.read_csv(IN_CSV, low_memory=False)

    # Safety: only rows with Page.goto errors
    df["error"] = df["error"].astype(str)
    df = df[df["error"].str.contains("Page.goto", na=False)].copy()

    if df.empty:
        log("No Page.goto failures in rerun_page_failures.csv")
        return

    done = load_done_row_indices()
    if done:
        log(f"Resume: {len(done)} rows already written to {OUT_CSV.name}")

    log(f"Re-running {len(df)} page-failure rows")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context()
        page = context.new_page()

        for _, row in df.iterrows():
            row_index = int(row["row_index"])
            if row_index in done:
                continue

            app_no = str(row["application_number"]).strip()
            worker_id = row.get("worker_id", "")

            record = {
                "worker_id": worker_id,
                "row_index": row_index,
                "application_number": app_no,
                "no_record_found": 0,
                "has_third_party_observation": False,
                "n_observation_letters": 0,
                "observation_urls": "",
                "error": "",
            }

            log(f"Re-run page failure: row_index={row_index} app={app_no}")

            try:
                safe_goto(page, build_search_url(app_no))
                try:
                    page.wait_for_load_state("networkidle", timeout=NETWORKIDLE_TIMEOUT_MS)
                except Exception:
                    pass

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
            done.add(row_index)
            time.sleep(ROW_SLEEP_S)

        try:
            context.close()
            browser.close()
        except Exception:
            pass

    log(f"Done → {OUT_CSV}")


if __name__ == "__main__":
    main()
