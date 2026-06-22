"""
scrapers/scraper_igr.py
IGR Maharashtra (Inspector General of Registration) scraper.

What this scrapes:
  - Actual registered sale transactions for a district/year range
  - Real transacted prices (consideration value from stamp duty records)
  - Buyer / seller names, property description, survey number, area
  - Village / locality, SRO office
  - Document type (sale deed, mortgage, gift deed, etc.)

Source:
  https://freesearchigrservice.maharashtra.gov.in/ — free search portal

Why this matters:
  - 99acres shows ASKING price. IGR shows ACTUAL transacted price.
  - Gap between asking vs actual = key anomaly signal.
  - Cross-referencing with RERA declared project cost = fraud signal.

── PORTAL STRUCTURE (confirmed via Chrome DevTools, April 2026) ─────────────
  The portal has THREE separate search mode buttons on the landing page:
    btnMumbaisearch        → Mumbai only (districts 30, 31)
    btnOtherdistrictSearch → Rest of Maharashtra ← WE USE THIS
    btnUrbansearch         → Urban areas

  Clicking btnOtherdistrictSearch loads a DIFFERENT set of form elements:
    ddlFromYear1   — year SELECT dropdown (not a text input)
    ddlDistrict1   — district SELECT (Akola=23, Amravati=21, Nagpur=25)
    ddltahsil      — tahsil/taluka SELECT (populated after district change)
    ddlvillage     — village SELECT (populated after tahsil change)

  Document search form (दस्त निहाय tab) uses:
    ddldistrictfordoc  — district
    ddlSROName         — SRO office (populated after district change)
    ddlYearForDoc      — year
    rblDocType         — registration type radio (1=eFiling,2=eReg,3=Regular,4=iSarita)
    txtDocumentNo      — document number (required)
    txtImg             — CAPTCHA answer

  CAPTCHA: image served by Handler.ashx — NO text version in DOM.
  Solved here with ddddocr (pure Python — pip install ddddocr, no system install needed).
  If OCR fails, install tesseract-ocr system package or swap in a
  paid solving service (2captcha/AntiCaptcha) in _solve_captcha().

── SCRAPING STRATEGY ──────────────────────────────────────────────────────────
  The free portal does NOT support listing all transactions for a taluka/year.
  Both forms require a specific property number OR document number.
  Document numbers ARE sequential integers within each SRO/year, so we
  enumerate them: 1, 2, 3, ... stopping on N consecutive misses.

  Flow per SRO x registration_type x year:
    1. Load wfSearch.aspx
    2. Click btnOtherdistrictSearch   <- CRITICAL: must happen before district select
    3. Switch to Document Number tab
    4. Select district -> wait for SRO dropdown to populate
    5. Select SRO, year, registration type
    6. For each doc_num in range:
         a. Fill txtDocumentNo
         b. Solve CAPTCHA image with ddddocr
         c. Submit -> parse result row
         d. On consecutive misses -> stop (end of that SRO/year)

── KNOWN SRO CODES (confirmed via DevTools) ───────────────────────────────────
  Akola district (code 23):
    237=Akola-1, 238=Akola-2, 361=Akola-3,
    242=Telhara, 243=Patur, 244=Balapur,
    245=Murtijapur, 247=Akot, 249=Barshi-Takali

  Amravati (21) and Nagpur (25): fetched at runtime from ddlSROName

DB table: igr_transactions (SQL below)
------------------------------------------------------
create table if not exists public.igr_transactions (
    id                  bigserial primary key,
    city_id             bigint references public.cities(id),
    district            text,
    taluka              text,
    sro_name            text,
    sro_code            text,
    doc_number          text,
    doc_year            integer,
    doc_type            text,
    party_1             text,
    party_2             text,
    consideration_value bigint,
    area_sqm            numeric,
    area_sqft           numeric,
    survey_number       text,
    property_description text,
    registration_date   date,
    source_url          text,
    raw_data            jsonb,
    scraped_at          timestamptz default now(),
    is_flagged          boolean default false,
    flag_reasons        text[] default '{}'
);
create unique index if not exists igr_transactions_dedup_idx
    on public.igr_transactions(doc_number, doc_year, sro_code);
create index if not exists igr_transactions_city_id_idx
    on public.igr_transactions(city_id);
create index if not exists igr_transactions_consideration_idx
    on public.igr_transactions(consideration_value);
------------------------------------------------------
"""

import json
import logging
import re
import time
import random
from datetime import datetime, date
from typing import Generator

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

IGR_BASE       = "https://freesearchigrservice.maharashtra.gov.in"
IGR_SEARCH_URL = f"{IGR_BASE}/wfSearch.aspx"

# District codes confirmed via DevTools, April 2026
DISTRICT_CODES = {
    "Akola":    "23",
    "Amravati": "21",
    "Nagpur":   "25",
    "Pune":     "1",
    "Nashik":   "10",
    "Mumbai":   "30",
}

# SRO codes for Akola confirmed via DevTools; others fetched at runtime
AKOLA_SRO_CODES = {
    "Akola-1":       "237",
    "Akola-2":       "238",
    "Akola-3":       "361",
    "Telhara":       "242",
    "Patur":         "243",
    "Balapur":       "244",
    "Murtijapur":    "245",
    "Akot":          "247",
    "Barshi-Takali": "249",
}

REG_TYPES = {
    "eFiling":       "1",
    "eRegistration": "2",
    "Regular":       "3",
    "iSarita":       "4",
}

SQM_TO_SQFT = 10.7639
MAX_CONSECUTIVE_MISSES = 20


class ScraperIGR(BaseScraper):
    """
    Scrapes IGR Maharashtra free search portal for property transaction records.

    Usage:
        scraper = ScraperIGR(district="Akola", years=[2024, 2025],
                             reg_types=["Regular", "eRegistration"])
        scraper.run()
    """

    name = "igr"
    delay_min = 2.0
    delay_max = 4.0

    def __init__(
        self,
        district="Akola",
        years=None,
        reg_types=None,
        max_records_per_sro=200,
    ):
        super().__init__()
        if district not in DISTRICT_CODES:
            supported = ", ".join(sorted(DISTRICT_CODES))
            raise ValueError(
                f"IGR district code not configured for '{district}'. "
                f"Supported IGR districts: {supported}"
            )

        self.district      = district
        self.city          = district
        self.years         = years or [datetime.now().year - 1, datetime.now().year]
        self.reg_types     = reg_types or list(REG_TYPES.keys())
        self.max_records   = max_records_per_sro
        self.district_code = DISTRICT_CODES[district]

    # ── Delay helper ───────────────────────────────────────────────────────────

    def _random_delay(self) -> None:
        duration = random.uniform(self.delay_min, self.delay_max)
        self.logger.debug(f"Sleeping {duration:.1f}s")
        time.sleep(duration)

    # ── CAPTCHA solver ─────────────────────────────────────────────────────────

    def _solve_captcha(self, page) -> str:
        """
        Solve the IGR portal CAPTCHA using ddddocr — pure Python, no system
        dependencies (no Tesseract install needed).

        Install once:
            pip install ddddocr

        Approach:
          1. Take a Playwright screenshot of the CAPTCHA <img> element directly.
             This avoids any cookie/session juggling since Playwright already
             holds the authenticated browser context.
          2. Feed the raw PNG bytes to ddddocr for classification.
          3. Strip non-alphanumeric characters from the result and uppercase it.

        If ddddocr accuracy is insufficient for a particular run, swap this
        method body for a paid service (2captcha, AntiCaptcha) — the interface
        is the same: return a string or "" on failure.
        """
        try:
            import ddddocr
        except ImportError:
            self.logger.warning(
                "ddddocr not installed — run: pip install ddddocr. "
                "Returning empty string; CAPTCHA submissions will fail."
            )
            return ""

        try:
            # Locate the CAPTCHA image element — id differs between property
            # and document search forms (imgCaptcha vs imgCaptcha1)
            captcha_el = page.locator("#imgCaptcha1, #imgCaptcha").first

            if captcha_el.count() == 0:
                self.logger.warning("CAPTCHA image element not found on page")
                return ""

            # Playwright screenshots the element using the existing browser
            # session — no separate HTTP request or cookie passing needed
            img_bytes = captcha_el.screenshot()

            ocr = ddddocr.DdddOcr(show_ad=False)
            raw = ocr.classification(img_bytes)

            # Keep only alphanumeric characters and uppercase
            # ddddocr occasionally outputs Chinese chars for ambiguous pixels
            import re as _re
            cleaned = _re.sub(r"[^A-Z0-9]", "", raw.upper())

            self.logger.debug(f"CAPTCHA: raw='{raw}' cleaned='{cleaned}'")
            return cleaned

        except Exception as exc:
            self.logger.warning(f"CAPTCHA solve error: {exc}")
            return ""

    # ── SRO loader ─────────────────────────────────────────────────────────────

    def _fetch_sro_codes(self, page) -> dict:
        """
        Return {sro_name: sro_code} for the currently selected district.
        Uses hardcoded values for Akola; fetches dynamically for other districts.
        """
        if self.district == "Akola":
            return AKOLA_SRO_CODES

        try:
            options = page.evaluate(
                """() => {
                    const sel = document.getElementById('ddlSROName');
                    if (!sel) return [];
                    return Array.from(sel.options)
                        .filter(o => o.value && o.value !== '0')
                        .map(o => ({ code: o.value, name: o.text.trim() }));
                }"""
            )
            return {o["name"]: o["code"] for o in options}
        except Exception as exc:
            self.logger.warning(f"Could not fetch SRO codes: {exc}")
            return {}

    # ── Navigation ─────────────────────────────────────────────────────────────

    def _navigate_to_doc_form_and_select_district(self, page) -> dict:
        """
        Correct navigation sequence (confirmed via DevTools):
          1. Load wfSearch.aspx
          2. Dismiss the Search Flow modal
          3. Click btnOtherdistrictSearch  <- ROOT CAUSE OF ORIGINAL ERROR
             Without this, ddlDistrict only shows Mumbai; Akola (23) doesn't exist there.
          4. Switch to the Document Number tab (mnuSearchType arg '3')
          5. Select district in ddldistrictfordoc
          6. Wait for ddlSROName to populate via ASP.NET UpdatePanel postback
        Returns {sro_name: sro_code}.
        """
        try:
            page.goto(IGR_SEARCH_URL, wait_until="domcontentloaded", timeout=30000)
        except PlaywrightTimeoutError:
            self.logger.error("Timeout loading IGR portal")
            return {}

        self._random_delay()

        # Dismiss modal
        try:
            page.evaluate(
                """() => {
                    const btns = Array.from(
                        document.querySelectorAll('button, input[type=button], a')
                    );
                    const close = btns.find(
                        b => (b.innerText || b.value || '').trim().toLowerCase() === 'close'
                    );
                    if (close) close.click();
                }"""
            )
            page.wait_for_timeout(500)
        except Exception:
            pass

        # --- CRITICAL FIX: click Rest of Maharashtra before selecting district ---
        # The portal defaults to Mumbai mode. In Mumbai mode ddlDistrict only
        # has values 30 and 31 (Mumbai/Mumbai Suburban). Akola is value 23 and
        # only appears after clicking btnOtherdistrictSearch. This was the root
        # cause of the Locator.select_option timeout error.
        try:
            page.wait_for_selector("#btnOtherdistrictSearch", timeout=10000)
            page.click("#btnOtherdistrictSearch")
            page.wait_for_selector("#ddlDistrict1", timeout=10000)
            self.logger.info("Switched to Rest of Maharashtra mode")
        except PlaywrightTimeoutError:
            self.logger.error(
                "Could not click btnOtherdistrictSearch — portal may have changed layout"
            )
            return {}

        self._random_delay()

        # Switch to Document Number tab
        try:
            page.evaluate(
                """() => {
                    const link = Array.from(document.querySelectorAll('a')).find(
                        a => a.href && a.href.includes("mnuSearchType','3")
                    );
                    if (link) link.click();
                }"""
            )
            page.wait_for_selector("#ddldistrictfordoc", timeout=10000)
            self.logger.info("Switched to Document Number tab")
        except PlaywrightTimeoutError:
            self.logger.error("Could not switch to Document Number tab")
            return {}

        self._random_delay()

        # Select district
        try:
            page.select_option("#ddldistrictfordoc", value=self.district_code)
            self.logger.info(f"Selected {self.district} (code={self.district_code})")
        except Exception as exc:
            self.logger.error(f"District select failed: {exc}")
            return {}

        # Wait for SRO dropdown to populate (ASP.NET UpdatePanel postback)
        try:
            page.wait_for_function(
                "() => { const s = document.getElementById('ddlSROName'); "
                "return s && s.options.length > 1; }",
                timeout=12000,
            )
        except PlaywrightTimeoutError:
            self.logger.warning("SRO dropdown did not populate — using static Akola codes")

        return self._fetch_sro_codes(page)

    # ── Core scrape loop ───────────────────────────────────────────────────────

    def scrape(self) -> Generator[dict, None, None]:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 768},
            )
            page = context.new_page()

            try:
                sro_codes = self._navigate_to_doc_form_and_select_district(page)
                if not sro_codes:
                    self.logger.error("No SRO codes loaded — aborting")
                    return

                self.logger.info(
                    f"{len(sro_codes)} SROs for {self.district}: {list(sro_codes.keys())}"
                )

                for sro_name, sro_code in sro_codes.items():
                    for reg_type in self.reg_types:
                        for year in self.years:
                            self.logger.info(
                                f"Enumerating SRO={sro_name} type={reg_type} year={year}"
                            )
                            try:
                                yield from self._enumerate_sro_year(
                                    page, sro_code, sro_name, reg_type, year
                                )
                            except Exception as exc:
                                self.logger.error(
                                    f"Failed SRO={sro_name} year={year}: {exc}"
                                )
                            self._random_delay()
            finally:
                browser.close()

    def _restore_doc_form(self, page, sro_code: str, year: int, reg_value: str) -> bool:
        """
        Re-navigate to the document search form and re-select all dropdowns.
        Called whenever the results panel has replaced the form after a successful lookup.
        Returns True if form was restored successfully.
        """
        try:
            page.goto(IGR_SEARCH_URL, wait_until="domcontentloaded", timeout=30000)
        except PlaywrightTimeoutError:
            return False

        self._random_delay()

        # Dismiss modal if it appears
        try:
            page.evaluate(
                """() => {
                    document.querySelectorAll('[class*="modal"]')
                        .forEach(el => el.style.display = 'none');
                    const c = Array.from(document.querySelectorAll('a,button,input'))
                        .find(b => (b.innerText||b.value||'').trim().toLowerCase()==='close');
                    if (c) c.click();
                }"""
            )
            page.wait_for_timeout(300)
        except Exception:
            pass

        # Click Rest of Maharashtra
        try:
            page.wait_for_selector("#btnOtherdistrictSearch", timeout=8000)
            page.click("#btnOtherdistrictSearch")
            page.wait_for_selector("#ddlDistrict1", timeout=8000)
        except PlaywrightTimeoutError:
            return False

        # Switch to Document Number tab
        try:
            page.evaluate(
                """() => {
                    Array.from(document.querySelectorAll('a')).find(
                        a => a.href && a.href.includes("mnuSearchType','3")
                    )?.click();
                }"""
            )
            page.wait_for_selector("#ddldistrictfordoc", timeout=8000)
        except PlaywrightTimeoutError:
            return False

        self._random_delay()

        # Re-select district
        try:
            page.select_option("#ddldistrictfordoc", value=self.district_code)
            page.wait_for_function(
                "() => { const s = document.getElementById('ddlSROName'); "
                "return s && s.options.length > 1; }",
                timeout=10000,
            )
        except Exception:
            return False

        # Re-select SRO, year, registration type
        try:
            page.select_option("#ddlSROName", value=sro_code)
            page.select_option("#ddlYearForDoc", value=str(year))
            page.evaluate(
                f"""() => {{
                    const r = document.querySelector(
                        'input[name="rblDocType"][value="{reg_value}"]'
                    );
                    if (r) r.checked = true;
                }}"""
            )
        except Exception:
            return False

        self.logger.debug("Doc search form restored after results panel")
        return True

    def _form_is_visible(self, page) -> bool:
        """Returns True if the document search form (btnSearchDoc) is present in the DOM."""
        return page.evaluate(
            "() => !!document.getElementById('btnSearchDoc')"
        )

    def _enumerate_sro_year(
        self, page, sro_code: str, sro_name: str, reg_type: str, year: int
    ) -> Generator[dict, None, None]:
        """
        Enumerate doc_num=1,2,3,... for this SRO/reg_type/year.
        Stops after MAX_CONSECUTIVE_MISSES misses.

        Form state after submit (confirmed via DevTools, April 2026):
          Wrong CAPTCHA  → "Entered Captcha is incorrect" → form stays, btnSearchDoc exists
          Correct CAPTCHA + doc not found → "no record" message → form stays
          Correct CAPTCHA + doc found     → RESULTS PANEL replaces form → btnSearchDoc is NULL

        The scraper detects which state we're in after each submit and calls
        _restore_doc_form() when the results panel has taken over, so the next
        iteration can fill and submit the form again.

        Element IDs confirmed via DevTools (document search form, April 2026):
          TextBox1     — CAPTCHA input  (not txtImg — that's the property form)
          btnSearchDoc — submit button  (not btnSearch — that's the property form)
          imgCaptcha1  — CAPTCHA image
        """
        if not sro_code or sro_code in ("0", "----Select SRO ------"):
            self.logger.warning(f"Skipping invalid SRO code: '{sro_code}'")
            return

        reg_value = REG_TYPES.get(reg_type, "3")
        misses = 0
        doc_num = 1
        yielded = 0

        while misses < MAX_CONSECUTIVE_MISSES and yielded < self.max_records:

            # If form was replaced by results panel on the previous iteration,
            # reload and navigate back to the doc search form
            try:
                if not self._form_is_visible(page):
                    self.logger.debug(
                        f"Results panel detected at doc={doc_num} — restoring form"
                    )
                    if not self._restore_doc_form(page, sro_code, year, reg_value):
                        self.logger.warning("Could not restore doc form — stopping SRO")
                        break
            except Exception:
                pass

            try:
                # Registration type radio — no AutoPostBack on this control
                page.evaluate(
                    f"""() => {{
                        const r = document.querySelector(
                            'input[name="rblDocType"][value="{reg_value}"]'
                        );
                        if (r) r.checked = true;
                    }}"""
                )
                page.select_option("#ddlSROName", value=sro_code)
                page.select_option("#ddlYearForDoc", value=str(year))
                page.fill("#txtDocumentNo", str(doc_num))

                captcha_src_before = page.evaluate(
                    "() => document.getElementById('imgCaptcha1')?.src || ''"
                )

                captcha = self._solve_captcha(page)
                if not captcha:
                    self.logger.debug(f"CAPTCHA empty at doc={doc_num}")
                    misses += 1
                    doc_num += 1
                    continue

                # TextBox1 = CAPTCHA input in the document form
                # (txtImg only exists in the property form — different ASP.NET panel)
                page.fill("#TextBox1", captcha)

                # JS .click() bypasses Playwright's pointer-event interactability
                # check. page.click("#btnSearchDoc") times out because div.linkdiv
                # sits above the button in the headless viewport's hit-test stack.
                page.evaluate("document.getElementById('btnSearchDoc').click()")

                # Wait for postback to settle. Two possible outcomes:
                #   1. Form stays (wrong CAPTCHA / no result) — imgCaptcha1 gets
                #      a new txt= token. Wait for src change.
                #   2. Results panel loads (success) — btnSearchDoc disappears.
                #      imgCaptcha1 also disappears, so the src-change wait would
                #      time out. Detect disappearance of btnSearchDoc as the signal.
                try:
                    page.wait_for_function(
                        f"""() => {{
                            const btn = document.getElementById('btnSearchDoc');
                            const img = document.getElementById('imgCaptcha1');
                            // Settled if: form gone (results loaded) OR captcha refreshed
                            const resultsLoaded = !btn;
                            const captchaRefreshed = img && img.src !== '{captcha_src_before}';
                            return resultsLoaded || captchaRefreshed;
                        }}""",
                        timeout=15000,
                    )
                except PlaywrightTimeoutError:
                    page.wait_for_timeout(4000)

            except Exception as exc:
                self.logger.warning(f"Form error at doc={doc_num}: {exc}")
                misses += 1
                doc_num += 1
                continue

            record = self._extract_result(page, sro_name, sro_code, doc_num, year)

            if record is None:
                misses += 1
            else:
                misses = 0
                yielded += 1
                yield record

            doc_num += 1
            self._random_delay()

        self.logger.info(
            f"SRO={sro_name} {year} {reg_type}: {yielded} records, stopped at doc={doc_num}"
        )

    # ── Result extraction ──────────────────────────────────────────────────────

    def _extract_result(
        self, page, sro_name: str, sro_code: str, doc_num: int, year: int
    ) -> dict | None:
        try:
            raw = page.evaluate(
                """() => {
                    const table = (
                        document.querySelector('table.GridStyle') ||
                        document.querySelector('table[id*="GridView"]') ||
                        document.querySelector('table.gvStyle') ||
                        document.querySelector('table[id*="gv"]')
                    );
                    if (!table) return null;

                    const rows = table.querySelectorAll('tr');
                    if (rows.length < 2) return null;

                    const fullText = table.innerText.toLowerCase();
                    if (fullText.includes('no record') ||
                        fullText.includes('invalid captcha') ||
                        fullText.includes('not found')) {
                        return null;
                    }

                    const dataRow = rows[1];
                    const cells = dataRow.querySelectorAll('td');
                    if (cells.length < 4) return null;

                    const getText = el => el ? el.innerText.trim() : '';
                    const linkEl = dataRow.querySelector('a');

                    return {
                        rawCells: Array.from(cells).map(c => getText(c)),
                        detailUrl: linkEl ? linkEl.href : '',
                        fullText: table.innerText.slice(0, 800),
                    };
                }"""
            )
        except Exception as exc:
            self.logger.debug(f"Extract error: {exc}")
            return None

        if not raw:
            return None

        return self._parse_result(raw, sro_name, sro_code, doc_num, year)

    def _parse_result(
        self, raw: dict, sro_name: str, sro_code: str, doc_num: int, year: int
    ) -> dict | None:
        cells = raw.get("rawCells", [])
        if not cells:
            return None

        full_text = raw.get("fullText", "")

        consideration = self._parse_amount(
            self._regex_extract(
                full_text,
                r"(?:consideration|amount|value)[:\s₹Rs.]*([\d,]+(?:\.\d+)?)",
                cells,
            )
        )
        reg_date = self._parse_date(
            self._regex_extract(full_text, r"(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})", cells)
        )
        doc_type = self._regex_extract(
            full_text,
            r"(sale deed|agreement for sale|conveyance|gift deed|mortgage)",
            cells,
            flags=re.I,
        ) or (cells[7] if len(cells) > 7 else "")
        area_sqm, area_sqft = self._parse_area(
            self._regex_extract(
                full_text,
                r"([\d.,]+\s*(?:sq\.?\s*m[tr]?|sqm|sq\.?\s*ft|sqft|hect))",
                cells,
                flags=re.I,
            )
        )

        return {
            "district":             self.district,
            "taluka":               sro_name,
            "sro_name":             sro_name,
            "sro_code":             sro_code,
            "doc_number":           str(doc_num),
            "doc_year":             year,
            "doc_type":             str(doc_type).strip(),
            "party_1":              self._clean_name(cells[1] if len(cells) > 1 else ""),
            "party_2":              self._clean_name(cells[2] if len(cells) > 2 else ""),
            "consideration_value":  consideration,
            "area_sqm":             area_sqm,
            "area_sqft":            area_sqft,
            "survey_number":        self._extract_survey(full_text),
            "property_description": " | ".join(c for c in cells if c)[:500],
            "registration_date":    reg_date,
            "source_url":           raw.get("detailUrl") or IGR_SEARCH_URL,
            "raw_data":             {"cells": cells, "text": full_text[:400]},
            "city":                 self.district,
            "scraped_at":           datetime.utcnow().isoformat(),
        }

    # ── Static parsing helpers ─────────────────────────────────────────────────

    @staticmethod
    def _regex_extract(text: str, pattern: str, cells: list, flags: int = 0) -> str:
        m = re.search(pattern, text, flags)
        if m:
            return m.group(1).strip()
        for cell in cells:
            m = re.search(pattern, cell, flags)
            if m:
                return m.group(1).strip()
        return ""

    @staticmethod
    def _parse_amount(raw: str) -> int | None:
        if not raw:
            return None
        cleaned = re.sub(r"[₹Rs.\s,]", "", str(raw)).strip()
        try:
            return int(float(cleaned))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_area(raw: str) -> tuple[float | None, float | None]:
        if not raw:
            return None, None
        text = str(raw).strip().lower()
        m = re.search(r"([\d,]+(?:\.\d+)?)\s*(sq\.?\s*mt[r]?|sq\.?\s*m\b|sqm)", text)
        if m:
            sqm = float(m.group(1).replace(",", ""))
            return round(sqm, 4), round(sqm * SQM_TO_SQFT, 2)
        m = re.search(r"([\d,]+(?:\.\d+)?)\s*(sq\.?\s*ft[.]?|sqft)", text)
        if m:
            sqft = float(m.group(1).replace(",", ""))
            return round(sqft / SQM_TO_SQFT, 4), round(sqft, 2)
        m = re.search(r"([\d,]+(?:\.\d+)?)\s*(hect|ha)\b", text)
        if m:
            sqm = float(m.group(1).replace(",", "")) * 10000
            return round(sqm, 4), round(sqm * SQM_TO_SQFT, 2)
        return None, None

    @staticmethod
    def _parse_date(raw: str) -> date | None:
        if not raw:
            return None
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(raw.strip(), fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _clean_name(raw: str) -> str:
        return re.sub(r"\s+", " ", str(raw or "").strip()).title()

    @staticmethod
    def _extract_survey(text: str) -> str:
        m = re.search(
            r"(?:survey|s\.no|cts|gat|plot)\s*[#no.:]*\s*([\w/\-]+)",
            text,
            flags=re.I,
        )
        return m.group(1).strip() if m else ""

    # ── DB save ────────────────────────────────────────────────────────────────

    def save(self, record: dict) -> str:
        from db.connection import insert_row, select_rows, update_rows

        table    = "igr_transactions"
        city_id  = self._get_city_id(self.district)
        doc_num  = record.get("doc_number", "")
        doc_year = record.get("doc_year")
        sro_code = record.get("sro_code", "")

        existing = []
        if doc_num and doc_year and sro_code:
            existing = select_rows(
                table,
                {"doc_number": doc_num, "doc_year": doc_year, "sro_code": sro_code},
            )

        reg_date = record.get("registration_date")
        row = {
            "city_id":              city_id,
            "district":             record.get("district"),
            "taluka":               record.get("taluka"),
            "sro_name":             record.get("sro_name"),
            "sro_code":             sro_code or None,
            "doc_number":           doc_num or None,
            "doc_year":             doc_year,
            "doc_type":             record.get("doc_type") or None,
            "party_1":              record.get("party_1") or None,
            "party_2":              record.get("party_2") or None,
            "consideration_value":  record.get("consideration_value"),
            "area_sqm":             record.get("area_sqm"),
            "area_sqft":            record.get("area_sqft"),
            "survey_number":        record.get("survey_number") or None,
            "property_description": record.get("property_description") or None,
            "registration_date":    (
                reg_date.isoformat() if isinstance(reg_date, date) else reg_date
            ),
            "source_url":           record.get("source_url"),
            "raw_data":             json.dumps(record.get("raw_data") or {}),
            "scraped_at":           record.get("scraped_at"),
        }
        row = {k: v for k, v in row.items() if v is not None}

        if existing:
            update_rows(
                table,
                filters={"id": existing[0]["id"]},
                updates={
                    "consideration_value": row.get("consideration_value"),
                    "area_sqm":            row.get("area_sqm"),
                    "area_sqft":           row.get("area_sqft"),
                    "party_1":             row.get("party_1"),
                    "party_2":             row.get("party_2"),
                    "raw_data":            row.get("raw_data"),
                    "scraped_at":          row.get("scraped_at"),
                },
            )
            return "updated"

        try:
            insert_row(table, row)
            return "inserted"
        except Exception as exc:
            self.logger.error(f"Insert error {doc_num}/{doc_year}: {exc}")
            return "skipped"

    def _get_city_id(self, city_name: str) -> int | None:
        from db.connection import insert_row, select_rows
        cities = select_rows("cities", {"name": city_name})
        if cities:
            return cities[0]["id"]
        row = insert_row("cities", {"name": city_name, "state": "Maharashtra"})
        return row["id"]

    # ── Run loop ───────────────────────────────────────────────────────────────

    def run(self):
        self.start_run()
        status = "success"
        try:
            for record in self.scrape():
                self.stats["fetched"] += 1
                try:
                    result = self.save(record)
                    if result == "inserted":
                        self.stats["inserted"] += 1
                    elif result == "updated":
                        self.stats["updated"] += 1
                    elif result == "skipped":
                        self.stats["skipped"] = self.stats.get("skipped", 0) + 1
                except Exception as exc:
                    self.logger.error(f"Save error: {exc} | {str(record)[:200]}")
                    self.stats["errors"].append(str(exc))
        except Exception as exc:
            self.logger.exception(f"Fatal scrape error: {exc}")
            self.stats["errors"].append(f"FATAL: {exc}")
            status = "failed"
        finally:
            self.finish_run(status)
