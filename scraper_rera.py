"""
scrapers/scraper_rera.py
MahaRERA scraper.

Key facts confirmed:
  - Registered Projects radio = value "0"
  - Revoked Projects radio = value "1"
  - Result cards class = div.row.shadow.p-3.mb-5.bg-body.rounded
  - Division: Amravati = "2", District: Akola = "501"
  - State: Maharashtra = "27"
"""

import json
import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Generator

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://maharera.maharashtra.gov.in"
SEARCH_URL = f"{BASE_URL}/projects-search-result"

DIVISION_CODES = {
    "Nashik": "1",
    "Amravati": "2",
    "Aurangabad": "3",
    "Nagpur": "4",
    "Pune": "5",
    "Konkan": "6",
}
DISTRICT_CODES = {
    "Akola": "501",
    "Amravati": "503",
    "Nagpur": "505",
    "Pune": "521",
    "Mumbai Suburban": "518",
    "Mumbai City": "519",
    "Nashik": "516",
    "Aurangabad": "515",
    "Washim": "502",
    "Buldana": "500",
    "Wardha": "504",
    "Yavatmal": "510",
}
DISTRICT_DIVISION = {
    "Akola": "Amravati",
    "Amravati": "Amravati",
    "Washim": "Amravati",
    "Buldana": "Amravati",
    "Wardha": "Amravati",
    "Yavatmal": "Amravati",
    "Nagpur": "Nagpur",
    "Pune": "Pune",
    "Mumbai Suburban": "Konkan",
    "Mumbai City": "Konkan",
    "Nashik": "Nashik",
    "Aurangabad": "Aurangabad",
}


class ScraperMahaRERA(BaseScraper):
    name = "maharera"
    city = "Akola"
    delay_min = 1.5
    delay_max = 3.0

    def __init__(self, district="Akola", max_pages=20):
        super().__init__()
        self.district = district
        self.city = district
        self.max_pages = max_pages
        self.division = DISTRICT_DIVISION.get(district, "Amravati")
        self.division_code = DIVISION_CODES.get(self.division, "2")
        self.district_code = DISTRICT_CODES.get(district, "501")
        self.playwright_debug = self._env_flag("PLAYWRIGHT_DEBUG")
        self.headless = self._env_flag("PLAYWRIGHT_HEADLESS")
        self.browser_channel = os.getenv("PLAYWRIGHT_BROWSER_CHANNEL") or None
        self.slow_mo = self._parse_int_env("PLAYWRIGHT_SLOW_MO", default=0)
        self.debug_dir = Path("debug_artifacts") / self.name

    def _get_table(self) -> str:
        return "rera_projects"

    @staticmethod
    def _env_flag(name: str, default: bool = False) -> bool:
        value = os.getenv(name)
        if value is None:
            return default
        return value.strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _parse_int_env(name: str, default: int = 0) -> int:
        value = os.getenv(name)
        if value is None:
            return default
        try:
            return int(value.strip())
        except ValueError:
            return default

    def _artifact_path(self, label: str, extension: str) -> Path:
        self.debug_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        safe_label = "".join(c if c.isalnum() or c in {
                             "_", "-"} else "_" for c in label)
        return self.debug_dir / f"{stamp}_{safe_label}.{extension}"

    def _attach_debug_listeners(self, page) -> None:
        if not self.playwright_debug:
            return

        def on_console(msg):
            self.logger.info(f"[browser:{msg.type}] {msg.text}")

        def on_page_error(exc):
            self.logger.error(f"[pageerror] {exc}")

        def on_request_failed(request):
            failure = request.failure or {}
            error_text = failure.get("errorText", "unknown error")
            self.logger.warning(
                f"[requestfailed] {request.method} {request.url} -> {error_text}"
            )

        def on_response(response):
            if response.status >= 400:
                self.logger.warning(
                    f"[response] {response.status} {response.request.method} {response.url}"
                )

        def on_frame_navigated(frame):
            if frame == page.main_frame:
                self.logger.info(f"[navigated] {frame.url}")

        page.on("console", on_console)
        page.on("pageerror", on_page_error)
        page.on("requestfailed", on_request_failed)
        page.on("response", on_response)
        page.on("framenavigated", on_frame_navigated)

    def _save_debug_snapshot(self, page, label: str) -> None:
        if not self.playwright_debug:
            return

        png_path = self._artifact_path(label, "png")
        html_path = self._artifact_path(label, "html")
        try:
            page.screenshot(path=str(png_path), full_page=True)
            html_path.write_text(page.content(), encoding="utf-8")
            self.logger.info(f"Saved debug snapshot: {png_path}")
            self.logger.info(f"Saved debug HTML: {html_path}")
        except Exception as exc:
            self.logger.warning(f"Debug snapshot failed at {label}: {exc}")

    def _wait_for_select_option(self, page, selector: str, value: str, timeout: int = 15000) -> None:
        page.wait_for_function(
            """([selector, value]) => {
                const el = document.querySelector(selector);
                return !!el && Array.from(el.options || []).some(opt => opt.value === value);
            }""",
            arg=[selector, value],
            timeout=timeout,
        )

    def _wait_for_select_value(self, page, selector: str, value: str, timeout: int = 10000) -> None:
        page.wait_for_function(
            """([selector, value]) => document.querySelector(selector)?.value === value""",
            arg=[selector, value],
            timeout=timeout,
        )

    def _log_dom_state(self, page, label: str) -> None:
        if not self.playwright_debug:
            return
        try:
            dom_state = page.evaluate(
                """() => ({
                    url: location.href,
                    title: document.title,
                    cards: document.querySelectorAll('div.row.shadow.p-3.mb-5.bg-body.rounded').length,
                    nextLinks: document.querySelectorAll("li.next a, a[title='Go to next page'], a[aria-label='Next page'], .pager__item--next a").length,
                    bodySample: (document.body?.innerText || '').slice(0, 500)
                })"""
            )
            self.logger.info(
                f"{label} DOM state: {json.dumps(dom_state, ensure_ascii=False)}")
        except Exception as exc:
            self.logger.warning(
                f"Could not capture DOM state at {label}: {exc}")

    def scrape(self) -> Generator[dict, None, None]:
        with sync_playwright() as p:
            launch_kwargs = {"headless": self.headless}
            if self.browser_channel:
                launch_kwargs["channel"] = self.browser_channel
            if self.slow_mo:
                launch_kwargs["slow_mo"] = self.slow_mo

            browser = p.chromium.launch(**launch_kwargs)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
            )
            if self.playwright_debug:
                context.tracing.start(
                    screenshots=True, snapshots=True, sources=True)
            page = context.new_page()

            try:
                self._attach_debug_listeners(page)
                self.logger.info(
                    f"Navigating to RERA search for {self.district} "
                    f"(division={self.division_code}, district={self.district_code})"
                )

                start_url = (
                    f"{SEARCH_URL}?project_state=27"
                    f"&project_division={self.division_code}"
                    f"&project_district={self.district_code}"
                    f"&page=1&op="
                )
                page.goto(start_url, wait_until="domcontentloaded",
                          timeout=30000)

                try:
                    page.wait_for_selector(
                        "div.row.shadow.p-3.mb-5.bg-body.rounded", timeout=15000)
                except PlaywrightTimeoutError:
                    self.logger.warning("No result cards found on first load")
                    self._save_debug_snapshot(page, "no_cards_first_load")
                    return

                page_num = 1
                total_yielded = 0

                while True:
                    self._log_dom_state(page, f"page_{page_num}_loaded")
                    projects = self._extract_cards(page)

                    if not projects:
                        self.logger.info(
                            f"No projects on page {page_num} - done")
                        break

                    self.logger.info(
                        f"Page {page_num} - {len(projects)} projects")
                    for proj in projects:
                        yield proj
                        total_yielded += 1

                    page_num += 1
                    if page_num > self.max_pages:
                        self.logger.info("Reached max_pages limit")
                        break
                    next_url = (
                        f"{SEARCH_URL}?project_state=27"
                        f"&project_division={self.division_code}"
                        f"&project_district={self.district_code}"
                        f"&page={page_num}&op="
                    )
                    self.logger.info(f"Navigating to page {page_num}...")
                    try:
                        page.goto(
                            next_url, wait_until="domcontentloaded", timeout=30000)
                        page.wait_for_selector(
                            "div.row.shadow.p-3.mb-5.bg-body.rounded", timeout=12000)
                    except PlaywrightTimeoutError:
                        self.logger.info("No more results pages")
                        break

                self.logger.info(f"Total projects scraped: {total_yielded}")
            except Exception:
                self._save_debug_snapshot(page, "fatal_scrape_error")
                self._log_dom_state(page, "fatal_scrape_error")
                raise
            finally:
                if self.playwright_debug:
                    trace_path = self._artifact_path("playwright_trace", "zip")
                    try:
                        context.tracing.stop(path=str(trace_path))
                        self.logger.info(
                            f"Saved Playwright trace: {trace_path}")
                    except Exception as exc:
                        self.logger.warning(
                            f"Could not save Playwright trace: {exc}")
                browser.close()

    def _extract_cards(self, page) -> list:
        """Extract project data from the repeated project result rows."""
        try:
            results = page.evaluate(
                """
                () => {
                    const projects = [];
                    const cards = document.querySelectorAll('div.row.shadow.p-3.mb-5.bg-body.rounded');

                    cards.forEach(card => {
                        try {
                            const text = card.innerText || '';
                            if (!text.trim()) return;

                            const reraMatch = text.match(/#\\s*(P[A-Z]*\\d+)/i);
                            const reraNo = reraMatch ? reraMatch[1] : '';

                            const nameEl = card.querySelector('h4, h4 strong, .title4, .title4 strong');
                            const projectName = nameEl ? nameEl.innerText.trim() : '';

                            const promoterEl = card.querySelector('p.darkBlue.bold');
                            const promoterName = promoterEl ? promoterEl.innerText.trim() : '';

                            const detailLink = Array.from(card.querySelectorAll('a')).find(
                                a => (a.textContent || '').trim() === 'View Details'
                            );
                            const detailUrl = detailLink ? detailLink.href : '';

                            const pinMatch = text.match(/Pincode[:\\s]+(\\d{6})/i);
                            const distMatch = text.match(/District[:\\s]+([\\w\\s]+?)(?:\\n|Last|$)/i);
                            const modifiedMatch = text.match(/Last Modified[:\\s]+(\\d{4}-\\d{2}-\\d{2})/i);

                            if (detailUrl && (reraNo || projectName)) {
                                projects.push({
                                    rera_registration: reraNo,
                                    project_name: projectName,
                                    promoter_name: promoterName,
                                    pin_code: pinMatch ? pinMatch[1] : '',
                                    district: distMatch ? distMatch[1].trim() : '',
                                    rera_status: 'registered',
                                    source_url: detailUrl,
                                    approval_date: modifiedMatch ? modifiedMatch[1] : '',
                                    raw_data: text.substring(0, 500),
                                });
                            }
                        } catch (e) {}
                    });
                    return projects;
                }
                """
            )
            return results or []
        except Exception as e:
            self.logger.error(f"Extraction error: {e}")
            return []

    def save(self, record: dict) -> str:
        from db.connection import insert_row, select_rows, update_rows

        table = self._get_table()  # always "rera_projects" — shared table + city_id

        if not record.get("project_name") or not record.get("source_url"):
            return "skipped"

        city_id = self._get_city_id(self.district)

        allowed = {
            "rera_registration", "project_name", "promoter_name",
            "promoter_type", "district", "address_raw", "pin_code",
            "project_type", "rera_status", "application_date",
            "approval_date", "proposed_completion", "revised_completion",
            "is_completed", "delay_months", "total_units", "units_sold",
            "units_available", "total_area_sqm", "land_area_sqm",
            "project_cost", "amount_collected", "escrow_balance",
            "loan_amount", "complaint_count", "is_flagged",
            "flag_reasons", "promoter_pan", "raw_data", "city_id",
        }

        raw_payload = record.get("raw_data")
        source_url = record.get("source_url")
        if source_url:
            if isinstance(raw_payload, dict):
                raw_payload = {**raw_payload, "source_url": source_url}
            else:
                raw_payload = {
                    "card_text": raw_payload,
                    "source_url": source_url,
                }
            record = {**record, "raw_data": raw_payload}

        record_clean = {"city_id": city_id}
        for key, value in record.items():
            if key in allowed and value is not None and value != "":
                record_clean[key] = value.isoformat(
                ) if isinstance(value, date) else value

        existing = []
        if record_clean.get("rera_registration"):
            existing = select_rows(
                table,
                {"rera_registration": record_clean["rera_registration"]},
            )

        if not existing:
            candidates = select_rows(table, {"city_id": city_id}, limit=500)
            source_url = record.get("source_url")
            if source_url:
                existing = [
                    row for row in candidates
                    if source_url in str(row.get("raw_data") or "")
                ][:1]

            if not existing and record_clean.get("rera_registration"):
                reg = record_clean["rera_registration"]
                existing = [
                    row for row in candidates
                    if not row.get("rera_registration")
                    and reg in str(row.get("raw_data") or "")
                ][:1]

        if existing:
            record_clean["scraped_at"] = datetime.utcnow().isoformat()
            update_rows(
                table,
                filters={"id": existing[0]["id"]},
                updates=record_clean,
            )
            return "updated"

        try:
            record_clean["scraped_at"] = datetime.utcnow().isoformat()
            insert_row(table, record_clean)
            return "inserted"
        except Exception as e:
            self.logger.error(f"Insert error: {e}")
            return "skipped"

    def _get_city_id(self, city_name):
        from db.connection import insert_row, select_rows

        cities = select_rows("cities", {"name": city_name})
        if cities:
            return cities[0]["id"]
        row = insert_row("cities", {"name": city_name, "state": "Maharashtra"})
        return row["id"]
