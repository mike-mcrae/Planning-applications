#!/usr/bin/env python3
"""
tester_two_applications.py

Minimal tester for Dublin City Council PublicAccess scraping logic.
Tests two known application numbers:

- WEB1432/24  (known: record exists + 3rd party observation)
- 1710        (comparison case)

Uses the SAME logic as the full worker script.
"""

import time
import urllib.parse
from pathlib import Path
from typing import List

import requests
from playwright.sync_api import sync_playwright, TimeoutError

# ---------------- CONFIG ----------------

BASE_URL = (
    "https://webapps.dublincity.ie/PublicAccess_Live/"
    "SearchResult/RunThirdPartySearch"
)

RESULTS_LENGTH_SELECT = 'select[name="searchResult_length"]'
OBS_ICON_SELECTOR = 'span[aria-label*="3rd Party Observation Letter"]'
NO_RECORD_SELECTOR = '#searchResult_info'

GOTO_TIMEOUT_MS = 45_000
NETWORKIDLE_TIMEOUT_MS = 15_000
POPUP_TIMEOUT_MS = 10_000
DOWNLOAD_TIMEOUT_MS = 10_000
HTTP_TIMEOUT_S = 60

OUT_DIR = Path("downloads_test")
OUT_DIR.mkdir(exist_ok=True)

TEST_APPLICATIONS = [
    "WEB1432/24",
    "1710",
]

# ---------------- HELPERS ----------------


def build_search_url(app_number: str) -> str:
    params = {
        "FileSystemId": "PL",
        "Folder1_Ref": app_number.strip(),
    }
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
    """
    Detects:
    <div id="searchResult_info">Showing 0 to 0 of 0 entries</div>
    """
    try:
        info = page.locator(NO_RECORD_SELECTOR)
        if info.count() == 0:
            return False

        txt1 = info.first.inner_text(timeout=5_000).strip()
        page.wait_for_timeout(750)
        txt2 = info.first.inner_text(timeout=5_000).strip()

        if txt1 != txt2:
            return False

        return "0 to 0 of 0" in txt2
    except Exception:
        return False


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
        out_file = OUT_DIR / f"{safe_app}_obs_{i+1}.pdf"

        icon = icons.nth(i)
        try:
            icon.scroll_into_view_if_needed(timeout=10_000)
        except Exception:
            pass

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


# ---------------- MAIN TEST ----------------


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context()
        page = context.new_page()

        for app_no in TEST_APPLICATIONS:
            print("\n" + "=" * 70)
            print(f"Testing application number: {app_no}")

            page.goto(build_search_url(app_no), wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)

            try:
                page.wait_for_load_state("networkidle", timeout=NETWORKIDLE_TIMEOUT_MS)
            except Exception:
                pass

            set_results_to_100(page)

            if page_has_no_records(page):
                print("RESULT:")
                print("  no_record_found = 1")
                print("  has_third_party_observation = False")
                print("  n_observation_letters = 0")
            else:
                urls = extract_and_download_observations(page, app_no)
                print("RESULT:")
                print("  no_record_found = 0")
                print(f"  has_third_party_observation = {len(urls) > 0}")
                print(f"  n_observation_letters = {len(urls)}")
                if urls:
                    print("  observation_urls:")
                    for u in urls:
                        print(f"    - {u}")

            time.sleep(1.5)

        context.close()
        browser.close()


if __name__ == "__main__":
    main()
