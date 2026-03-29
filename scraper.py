"""
Scraper for LA Housing App — Property Activity Cases.

Stage 1: Playwright fetches the main table (JS-rendered, pagination → All).
Stage 2: aiohttp fetches all case detail pages in parallel (up to CONCURRENCY at once).
         Detail pages are publicly accessible — no session needed.

Run:
    python
     scraper.py
    python scraper.py --apn 2654002037
    python scraper.py --dry-run
"""

import argparse
import asyncio
import json
import logging
import sys
import time
from typing import List, Dict, Optional

import aiohttp
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from playwright.async_api import async_playwright

from models import Case
from storage import init_db, upsert_cases


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = (
    "https://housingapp.lacity.org/reportviolation/Pages/PropAtivityCases"
    "?APN={apn}&Source=ActivityReport"
)
DETAIL_URL = (
    "https://housingapp.lacity.org/reportviolation/Pages/PublicPropertyActivityReport"
    "?APN={apn}&CaseType={case_type_id}&CaseNo={case_no}"
)
WAIT_SELECTOR = "#divPropDetails, table, .table"
TABLE_KEYWORDS = ["case", "inspection", "activity", "violation"]
CONCURRENCY    = 15  # parallel aiohttp detail-page requests
PW_CONCURRENCY = 5   # parallel Playwright pages for JS-rendered fields


# ── Main table scraping ────────────────────────────────────────────────────────

def _extract_headers(table) -> List[str]:
    header_row = table.find("tr")
    if not header_row:
        return []
    return [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]


def _map_row(headers: List[str], cells: List[str]) -> Optional[Dict]:
    raw = dict(zip(headers, cells))

    def find(keys: List[str]) -> str:
        for key in keys:
            for h, v in raw.items():
                if key.lower() in h.lower():
                    return v.strip()
        return ""

    case_number = find(["case number", "case no", "case#", "caseno", "number"])
    if not case_number:
        return None

    return {
        "case_number": case_number,
        "case_type": find(["case type", "type", "category"]),
        "status": find(["status"]),
        "open_date": find(["open date", "opened", "start date", "filed"]),
        "close_date": find(["close date", "date closed", "closed", "end date", "resolved"]),
        "address": find(["address", "location", "property"]),
    }


def _extract_tables(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    results: List[Dict] = []

    for table in soup.find_all("table"):
        headers = _extract_headers(table)
        if not headers:
            continue
        header_text = " ".join(headers).lower()
        if not any(kw in header_text for kw in TABLE_KEYWORDS):
            continue
        log.info("Found relevant table with headers: %s", headers)

        for row in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or all(c == "" for c in cells):
                continue
            row_dict = _map_row(headers, cells)
            if not row_dict:
                continue

            # Grab case_type_id from the Action link's data-casetype attribute
            action_td = row.find("td")
            if action_td:
                link = action_td.find("a", attrs={"data-casetype": True})
                if link:
                    row_dict["case_type_id"] = link["data-casetype"]

            results.append(row_dict)

    return results


def _scrape_main_page(apn: str, timeout_ms: int = 30_000) -> List[Dict]:
    url = BASE_URL.format(apn=apn)
    log.info("Navigating to: %s", url)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        ).new_page()

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            try:
                page.wait_for_selector(WAIT_SELECTOR, timeout=timeout_ms)
            except PWTimeout:
                log.warning("Timed out waiting for selector — proceeding anyway.")
            time.sleep(2)

            # Expand pagination to show all entries
            try:
                page.select_option("select", "All")
                time.sleep(2)
                log.info("Pagination set to All.")
            except Exception:
                log.warning("Could not set pagination to All.")

            html = page.content()
            log.info("Main page loaded — %d bytes.", len(html))
        except Exception as exc:
            log.error("Failed to load main page: %s", exc)
            browser.close()
            return []

        browser.close()

    rows = _extract_tables(html)
    log.info("Extracted %d raw rows from main table.", len(rows))
    return rows


# ── Detail page enrichment (async / parallel) ─────────────────────────────────

def _parse_detail_page(html: str) -> dict:
    """
    Extract activity timeline from the detail page.
    Property info (inspector, address) is JS-rendered and not available via plain HTTP.
    We target the clean 2-column Date|Status table using non-recursive child traversal
    to avoid being tricked by nested tables.
    """
    soup = BeautifulSoup(html, "lxml")
    detail: dict = {"open_date": None, "current_status": "", "activity_count": 0}

    for table in soup.find_all("table"):
        # Only look at direct-child rows (avoids nested table confusion)
        rows = table.find_all("tr", recursive=False)
        if len(rows) < 2:
            continue
        header_cells = rows[0].find_all(["th", "td"], recursive=False)
        headers = [c.get_text(strip=True) for c in header_cells]

        # Target exactly the Date | Status table
        if headers != ["Date", "Status"]:
            continue

        data_rows = rows[1:]
        detail["activity_count"] = len(data_rows)

        if data_rows:
            # First row = most recent activity = current status
            first = [td.get_text(strip=True) for td in data_rows[0].find_all("td", recursive=False)]
            if len(first) >= 2:
                detail["current_status"] = first[1]

            # Last row = earliest activity = case open date
            last = [td.get_text(strip=True) for td in data_rows[-1].find_all("td", recursive=False)]
            if last:
                detail["open_date"] = last[0]

        break  # found the right table — no need to continue

    return detail


async def _fetch_one(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
) -> dict:
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                html = await resp.text()
                return _parse_detail_page(html)
        except Exception as exc:
            log.warning("Detail fetch failed %s: %s", url, exc)
            return {}


async def _enrich_all(raw_rows: List[Dict], apn: str) -> List[Dict]:
    semaphore = asyncio.Semaphore(CONCURRENCY)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [
            _fetch_one(
                session,
                DETAIL_URL.format(
                    apn=apn,
                    case_type_id=row.get("case_type_id", "1"),
                    case_no=row["case_number"],
                ),
                semaphore,
            )
            for row in raw_rows
        ]
        log.info("Fetching %d detail pages (concurrency=%d)...", len(tasks), CONCURRENCY)
        details = await asyncio.gather(*tasks)

    for row, detail in zip(raw_rows, details):
        for key, value in detail.items():
            if value:  # don't overwrite with empty
                row[key] = value

    return raw_rows


# ── Playwright async: JS-rendered fields ──────────────────────────────────────

async def _fetch_js_fields(page, url: str) -> dict:
    """Extract fields that require JavaScript: address, inspector, council district."""
    result = {"address": "", "inspector": "", "council_district": ""}
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_selector("#lnkbtnPropAddr", timeout=8000)
        for selector, key in [
            ("#lnkbtnPropAddr",  "address"),
            ("#lblInspectorName","inspector"),
            ("#lblCD",           "council_district"),
        ]:
            try:
                result[key] = (await page.locator(selector).inner_text()).strip()
            except Exception:
                pass
    except Exception as exc:
        log.warning("JS-field fetch failed %s: %s", url, exc)
    return result


async def _enrich_js_fields(raw_rows: List[Dict], apn: str) -> None:
    """Parallel Playwright pass to collect JS-rendered property fields."""
    semaphore = asyncio.Semaphore(PW_CONCURRENCY)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        async def fetch(row):
            async with semaphore:
                page = await browser.new_page()
                try:
                    url = DETAIL_URL.format(
                        apn=apn,
                        case_type_id=row.get("case_type_id", "1"),
                        case_no=row["case_number"],
                    )
                    fields = await _fetch_js_fields(page, url)
                    for k, v in fields.items():
                        if v:
                            row[k] = v
                finally:
                    await page.close()

        log.info("Fetching JS fields for %d cases (Playwright, concurrency=%d)...",
                 len(raw_rows), PW_CONCURRENCY)
        await asyncio.gather(*[fetch(row) for row in raw_rows])
        await browser.close()


# ── Public API ─────────────────────────────────────────────────────────────────

def scrape(apn: str) -> List[Case]:
    raw_rows = _scrape_main_page(apn)
    if not raw_rows:
        return []

    # Deduplicate by case_number before enrichment
    seen: Dict[str, Dict] = {}
    for row in raw_rows:
        seen[row["case_number"]] = row
    unique_rows = list(seen.values())
    log.info("%d unique cases after dedup.", len(unique_rows))

    # Stage 2: aiohttp — activity timeline (open_date, current_status, activity_count)
    enriched = asyncio.run(_enrich_all(unique_rows, apn))

    # Stage 3: Playwright async — JS-rendered fields (address, inspector, council_district)
    asyncio.run(_enrich_js_fields(enriched, apn))

    cases = [Case.from_dict(row, apn) for row in enriched]
    log.info("Done — %d cases ready.", len(cases))
    return cases


def main():
    parser = argparse.ArgumentParser(description="Scrape LA Housing property cases")
    parser.add_argument("--apn", default="2654002037")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    init_db()
    cases = scrape(apn=args.apn)

    if not cases:
        log.warning("No cases found.")
        sys.exit(1)

    if args.dry_run:
        print(json.dumps([c.to_dict() for c in cases], indent=2, ensure_ascii=False))
    else:
        saved = upsert_cases(cases)
        log.info("Saved/updated %d cases.", saved)
        print(f"Done - {saved} cases saved to database.")


if __name__ == "__main__":
    main()
