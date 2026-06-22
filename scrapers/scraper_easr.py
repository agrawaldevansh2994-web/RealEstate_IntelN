"""
scrapers/scraper_easr.py
Extracts annual Ready Reckoner (circle rate) data from the Maharashtra IGR
eASR 1.9 portal: https://easr.igrmaharashtra.gov.in/

── Portal mechanics (confirmed via DevTools, see project notes) ────────────
  - Pure ASP.NET WebForms. Every dropdown change fires __doPostBack and
    triggers a FULL PAGE POSTBACK — there is no JSON/XHR API to intercept.
    Confirmed via a live marker test: a window global set before a grid
    pagination click was gone afterward, proving the document genuinely
    reloads rather than updating via an AJAX UpdatePanel.
  - Entry URL: eASRCommon.aspx?hDistName={District}  (District pre-selected)
  - Cascade: Taluka dropdown is populated on initial page load.
             Village dropdown REQUIRES a taluka postback to populate.
  - The urban grid is itself paginated (GridView), 5 rows/page, via
    __doPostBack('...$grdUrbanSubZoneWiseRate','Page$N'). A single urban
    locality (e.g. Sitabardi, Nagpur) can have 50+ sub-zone rows across
    10+ pages -- one row per road/boundary-segment within that locality.
    Pages 11+ are reached via a "..." link, not a literal numbered link.

── Urban vs rural detection (CORRECTED — मौजा prefix is NOT universal) ───
  Originally assumed every district marks urban localities with a
  "मौजा : {name}" prefix in the village dropdown, with plain names being
  rural/agricultural. CONFIRMED FALSE for Pune: well-known Pune
  neighbourhoods (कोथरुड/Kothrud, औंध/Aundh, कात्रज/Katraj, कसबा पेठ/
  Kasba Peth) all appear as plain, unprefixed village names under the
  हवेली/Haveli taluka, and still correctly render the urban rate grid.
  Nagpur DOES use the मौजा prefix for its urban entries — so this is a
  district-specific convention, not a portal-wide rule.

  The only reliable signal is which grid renders after SELECTING a
  village: grdUrbanSubZoneWiseRate present -> urban (relevant), absent
  -> rural/agricultural (skip). This scraper now selects every village
  in the resolved taluka and lets that runtime check decide -- the
  मौजा-prefix text is retained only as a cheap heuristic for taluka
  auto-detection (see below) and is logged informationally, never used
  as a hard filter on which villages to scrape.

── Critical interaction rule ────────────────────────────────────────────
  Raw JS DOM dispatch (`el.value = X; el.dispatchEvent(new Event('change'))`)
  caused the page to go blank/crash twice during manual DevTools testing —
  almost certainly an ASP.NET EventValidation mismatch from firing events
  out of sync with server-rendered state. This scraper MUST use Playwright's
  native `page.select_option()` and `page.click()` exclusively. Never use
  page.evaluate() to mutate a <select>.value directly.

  Every dropdown's onchange fires __doPostBack wrapped in
  setTimeout(...,0), which defers the actual postback to a separate task
  -- a fixed wait_for_timeout() after select_option() is a guess that can
  run out before the real (deferred) postback even starts. All postback
  waits use page.expect_navigation() instead. Also: use
  wait_until="domcontentloaded", not "load" -- the page carries a
  floating chatbot widget and other slow auxiliary resources that delay
  the full 'load' event well past when the data we need has already
  rendered, which was the direct cause of repeated 20s grid-pagination
  timeouts in production.

── Aggregation strategy (Option B, confirmed) ───────────────────────────
  We do NOT store one row per sub-zone. Per (locality, property_type, year)
  we store min/max/avg across all sub-zone rows. This keeps row count sane
  (~1 row per locality per property type per year) while still letting
  fraud detection flag listings priced below a locality's circle-rate floor.

  Only 'flat' (निवासी सदनिका) and 'plot' (खुली जमीन) are populated.
  'house_villa' is deliberately left unpopulated — the govt grid has no
  separate bungalow rate; residential flat rate is the only built-structure
  figure available, and we don't want to silently imply it equals house
  rate without that being a deliberate downstream decision.

── City -> Taluka resolution (IMPORTANT — partially unverified) ────────
  Two cities are DevTools-confirmed:
      Nagpur -> taluka value "1" ("नागपूर (ग्रामीण)" / "Nagpur (Rural)")
      Pune   -> taluka value "5" ("हवेली" / Haveli)
  Both are non-obvious: Nagpur city's land-revenue boundary falls under
  the taluka literally named "Rural", and Pune city falls under "Haveli"
  -- there is no taluka literally named "Pune" at all.

  For Akola, Amravati, Nashik, Aurangabad, this scraper uses a two-signal
  auto-detect (see _resolve_taluka): मौजा-prefix counting first (works if
  the district follows Nagpur's convention), falling back to sampled
  urban-grid-type detection (works if it follows Pune's convention
  instead, with no text marker at all). This is still a HEURISTIC for
  4 of 6 cities. Before trusting output for those cities, spot-check a
  handful of locality names in circle_rates against known neighbourhoods.
"""

import logging
import re
import time
from datetime import datetime
from typing import Optional

from playwright.sync_api import sync_playwright, Page

from scrapers.base import BaseScraper
from scrapers.scraper_99acres import LOCALITY_ALIASES

logger = logging.getLogger(__name__)

# ── City -> District param (URL hDistName value) ────────────────────────
_CITY_TO_DISTRICT: dict[str, str] = {
    "akola":      "Akola",
    "amravati":   "Amravati",
    "nagpur":     "Nagpur",
    "pune":       "Pune",
    "nashik":     "Nashik",
    "aurangabad": "Aurangabad",   # fallback to Chhatrapati Sambhajinagar if empty
}

_AURANGABAD_FALLBACK = "Chhatrapati Sambhajinagar"

# Confirmed taluka value per city — ALL SIX now DevTools-verified.
# History: Nagpur and Pune were confirmed first. Akola, Amravati, and
# Aurangabad were confirmed 2026-06-21 via DevTools after auto-detect
# produced wrong results for all three (it selected the taluka with the
# most मौजा-style entries, which turned out to be a different town's
# taluka in every case — Murtizapur for Akola, Morshi for Amravati,
# and no match at all for Aurangabad). Root cause: the हिंदी-named cities'
# actual taluka is simply the taluka named after the city itself -- not
# the one with the most मौजा-prefixed village entries.
_CITY_TALUKA_HINT: dict[str, Optional[str]] = {
    "akola":      "14",   # CONFIRMED: "अकोला"
    "amravati":   "4",    # CONFIRMED: "अमरावती"
    "nagpur":     "1",    # CONFIRMED: "नागपूर (ग्रामीण)" -- urban entries use "मौजा :" prefix
    "pune":       "5",    # CONFIRMED: "हवेली" (Haveli) -- Pune city's actual revenue taluka,
                           # NOT a taluka literally named "Pune" (no such taluka exists).
                           # Urban entries here have NO मौजा prefix at all (कोथरुड/Kothrud,
                           # औंध/Aundh, कात्रज/Katraj all appear as plain village names) --
                           # confirms the मौजा-prefix convention is Nagpur/NMRDA-specific,
                           # not a universal eASR pattern. See _resolve_taluka and the
                           # village-loop in run() for how this is now handled.
    "nashik":     "1",    # CONFIRMED: "नाशिक" -- auto-detect correctly found this via
                           # grid-sampling (2/4 sampled villages rendered urban grid) on
                           # run #342 (2026-06-21), but hardcoded here to skip the
                           # 20-min taluka-scan overhead on future runs.
    "aurangabad": "10",   # CONFIRMED: "छत्रपती संभाजीनगर" -- portal returns talukas
                           # under the fallback district name "Chhatrapati Sambhajinagar";
                           # auto-detect failed entirely (no_taluka_found) because grid-
                           # sampling found zero urban-grid hits under the first few talukas.
}

MAUJA_PREFIX = "मौजा"

# Minimum मौजा-prefixed village count required before the run() village loop
# trusts the convention strongly enough to SKIP the postback entirely for
# non-मौजा villages (vs. just logging it informationally, the prior behavior).
# Deliberately conservative -- a single stray मौजा-prefixed entry in an
# otherwise Pune-style district should not be enough to start skipping real
# postbacks. Only Nagpur (106 मौजा entries / 218 villages) is currently known
# to clear this bar; see _resolve_taluka's district_uses_mauja_convention flag.
MAUJA_CONVENTION_MIN_COUNT = 10

# Grid-sampling fallback (for districts with no मौजा-style text marker, e.g. Pune).
TALUKA_SAMPLE_SIZE = 4            # villages to test-select per taluka
TALUKA_SAMPLE_CONFIDENT_HITS = 3  # early-exit threshold once this many hit

# Safety cap on sub-zone pagination per locality. Annual/low-frequency
# scrape, so completeness matters more than speed -- this is a runaway
# guard, not a real expected ceiling. Logged loudly if ever hit.
MAX_SUBZONE_PAGES_PER_LOCALITY = 30

# Sanity bounds for a single sub-zone rate (Rs/sqm). Anything outside this
# is almost certainly a parse error or a "0 = not applicable" placeholder.
MIN_PLAUSIBLE_RATE_SQM = 500
MAX_PLAUSIBLE_RATE_SQM = 2_000_000

SQM_TO_SQFT = 10.764

# DOM element IDs (confirmed via DevTools on Nagpur)
_ID_YEAR     = "ctl00_ContentPlaceHolder5_ddlYear"
_ID_DISTRICT = "ctl00_ContentPlaceHolder5_ddlDistrict"
_ID_TALUKA   = "ctl00_ContentPlaceHolder5_ddlTaluka"
_ID_VILLAGE  = "ctl00_ContentPlaceHolder5_ddlVillage"
_ID_GRID     = "ctl00_ContentPlaceHolder5_grdUrbanSubZoneWiseRate"

BASE_URL = "https://easr.igrmaharashtra.gov.in/eASRCommon.aspx"


class ScraperEASR(BaseScraper):
    name = "easr"

    def __init__(self, city: str, year: Optional[int] = None):
        super().__init__()
        self.city = city
        self.city_key = city.lower()
        self.year = year   # None -> use portal default (current FY)
        self.stats.setdefault("localities_scraped", 0)
        self.stats.setdefault("localities_skipped_rural", 0)
        self.stats.setdefault("pagination_cap_hits", 0)
        # Set by _resolve_taluka once a taluka is chosen. Only True when the
        # मौजा-prefix signal is strong enough (see MAUJA_CONVENTION_MIN_COUNT)
        # to trust skipping rural-village postbacks entirely in run(). Stays
        # False for Pune/Nashik-style districts with no text marker -- those
        # keep selecting every village, exactly as before.
        self.district_uses_mauja_convention = False

    def scrape(self):
        """
        Required override for BaseScraper's abstract interface, but unused.
        The District -> Taluka -> Village postback cascade and per-locality
        grid pagination don't map onto the "yield one record at a time"
        generator pattern other scrapers use -- run() below is fully
        self-contained and never calls this. Present only so the ABC
        allows instantiation.
        """
        return
        yield  # noqa: makes this a generator without ever producing one

    # ── Navigation helpers ────────────────────────────────────────────────

    def _district_url(self, district: str) -> str:
        return f"{BASE_URL}?hDistName={district}"

    def _load_district(self, page: Page) -> bool:
        """Navigate to the district page. Returns False if taluka list is empty
        (wrong district spelling) so caller can try a fallback name."""
        district = _CITY_TO_DISTRICT[self.city_key]
        page.goto(self._district_url(district), wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2000)

        taluka_count = page.eval_on_selector(
            f"#{_ID_TALUKA}", "el => el.options.length"
        )
        if taluka_count and taluka_count > 1:
            return True

        if self.city_key == "aurangabad":
            self.logger.info(
                f"No talukas for 'Aurangabad', retrying as '{_AURANGABAD_FALLBACK}'"
            )
            page.goto(
                self._district_url(_AURANGABAD_FALLBACK),
                wait_until="domcontentloaded",
                timeout=30000,
            )
            page.wait_for_timeout(2000)
            taluka_count = page.eval_on_selector(
                f"#{_ID_TALUKA}", "el => el.options.length"
            )
            return bool(taluka_count and taluka_count > 1)

        return False

    def _postback_select(
        self, page: Page, selector: str, value: str, timeout: int = 25000
    ) -> bool:
        """
        Select a dropdown option and properly wait for the resulting
        ASP.NET postback navigation to finish.

        page.select_option() alone is NOT sufficient here: every dropdown's
        onchange fires __doPostBack wrapped in setTimeout(...,0), which
        defers the actual postback to a separate task -- Playwright's
        select_option call returns before that deferred postback even
        starts, so a fixed wait_for_timeout() is just a guess that can
        fail under real network latency to a government server (this is
        exactly what crashed the Nashik run: "Execution context was
        destroyed, most likely because of a navigation" -- we were
        evaluating against the page WHILE the postback navigation was
        still in flight).

        Returns False (without raising) if the expected navigation never
        happens, so callers can retry/skip rather than crash the run.
        """
        try:
            with page.expect_navigation(wait_until="domcontentloaded", timeout=timeout):
                page.select_option(selector, value=value)
            page.wait_for_timeout(1200)  # settle buffer post-load, raised from 500ms --
            # gives ASP.NET's deferred secondary postback more room to finish before
            # the DOM is queried again. Note: this does NOT address ERR_ABORTED-type
            # failures seen in production logs, since those occur during the
            # expect_navigation wait itself, before this line is ever reached --
            # kept as cheap defensive hygiene, not a fix for those failures.
            return True
        except Exception as exc:
            self.logger.warning(
                f"Postback navigation wait failed for {selector}={value}: {exc}"
            )
            return False

    def _set_year(self, page: Page) -> None:
        if self.year is None:
            return  # use portal default (current FY), already selected
        options = page.eval_on_selector_all(
            f"#{_ID_YEAR} option", "els => els.map(e => e.value)"
        )
        target = next((o for o in options if o.startswith(str(self.year))), None)
        if target:
            self._postback_select(page, f"#{_ID_YEAR}", target)
        else:
            self.logger.warning(
                f"Year {self.year} not found in dropdown options {options}; "
                f"using portal default instead"
            )

    def _get_talukas(self, page: Page) -> list[dict]:
        return page.eval_on_selector_all(
            f"#{_ID_TALUKA} option",
            "els => els.map(e => ({value: e.value, text: e.textContent.trim()}))",
        )[1:]  # drop "- - Select Taluka - -"

    def _select_taluka(self, page: Page, value: str) -> bool:
        return self._postback_select(page, f"#{_ID_TALUKA}", value)

    def _get_village_options(self, page: Page) -> list[dict]:
        return page.eval_on_selector_all(
            f"#{_ID_VILLAGE} option",
            "els => els.map(e => ({value: e.value, text: e.textContent.trim()}))",
        )[1:]  # drop "- - Select Village/Zone - -"

    def _sample_urban_hit_count(
        self, page: Page, villages: list[dict], sample_size: int
    ) -> int:
        """
        Select up to `sample_size` villages from the currently-selected
        taluka and count how many render the urban rate grid. Structural
        fallback for districts (confirmed: Pune) where no village carries
        any text marker distinguishing urban from rural entries -- the
        only reliable signal there is which grid actually renders.
        """
        hits = 0
        for v in villages[:sample_size]:
            try:
                if not self._select_village(page, v["value"]):
                    continue
                if page.query_selector(f"#{_ID_GRID}"):
                    hits += 1
            except Exception:
                continue
        return hits

    def _resolve_taluka(self, page: Page) -> Optional[dict]:
        """
        Pick the taluka that contains this city's urban localities.
        Uses the verified hint if we have one. Otherwise scans every
        taluka with TWO signals, since the मौजा-prefix convention is
        confirmed NOT universal (Nagpur uses it, Pune does not):
          1. मौजा-prefixed village count (works for Nagpur-style districts)
          2. Sampled urban-grid hit rate (works for Pune-style districts
             with no text marker at all -- e.g. Haveli/कोथरुड)
        Whichever signal actually finds something wins; मौजा takes
        priority when both fire, since it's a cheaper/cleaner signal.
        Returns {"value": ..., "text": ...} or None.
        """
        hint = _CITY_TALUKA_HINT.get(self.city_key)
        talukas = self._get_talukas(page)
        if not talukas:
            return None

        if hint is not None:
            match = next((t for t in talukas if t["value"] == hint), None)
            if match:
                if not self._select_taluka(page, match["value"]):
                    self.logger.error(
                        f"{self.city}: postback failed selecting confirmed "
                        f"taluka '{match['text']}' -- falling through to auto-detect"
                    )
                else:
                    villages = self._get_village_options(page)
                    mauja_count = sum(1 for v in villages if v["text"].startswith(MAUJA_PREFIX))
                    self.district_uses_mauja_convention = mauja_count >= MAUJA_CONVENTION_MIN_COUNT
                    self.logger.info(
                        f"{self.city}: using CONFIRMED taluka '{match['text']}' "
                        f"({mauja_count} मौजा entries"
                        f"{', rural pre-filter ENABLED' if self.district_uses_mauja_convention else ''})"
                    )
                    return {**match, "mauja_count": mauja_count}
            else:
                self.logger.warning(
                    f"{self.city}: confirmed taluka hint '{hint}' not found in "
                    f"current taluka list -- falling through to auto-detect"
                )

        self.logger.warning(
            f"{self.city}: taluka NOT verified via DevTools -- using "
            f"auto-detect heuristic (मौजा-count, with grid-sampling fallback "
            f"for districts that don't use the मौजा prefix). Spot-check "
            f"output before trusting it for this city."
        )
        best_mauja = None
        best_sample = None
        for taluka in talukas:
            try:
                if not self._select_taluka(page, taluka["value"]):
                    self.logger.warning(
                        f"  taluka '{taluka['text']}' -> postback failed, skipping"
                    )
                    continue
                villages = self._get_village_options(page)
                mauja_count = sum(1 for v in villages if v["text"].startswith(MAUJA_PREFIX))
                self.logger.debug(
                    f"  taluka '{taluka['text']}' -> {mauja_count} मौजा entries"
                )
                if best_mauja is None or mauja_count > best_mauja["mauja_count"]:
                    best_mauja = {**taluka, "mauja_count": mauja_count}

                # Only bother with the expensive grid-sampling check when
                # the मौजा signal is weak -- avoids wasted work once a
                # clearly Nagpur-style district is already found.
                if mauja_count == 0 and villages:
                    sample_hits = self._sample_urban_hit_count(
                        page, villages, TALUKA_SAMPLE_SIZE
                    )
                    sample_total = min(len(villages), TALUKA_SAMPLE_SIZE)
                    self.logger.debug(
                        f"  taluka '{taluka['text']}' -> {sample_hits}/{sample_total} "
                        f"sampled villages render urban grid"
                    )
                    if best_sample is None or sample_hits > best_sample["sample_hits"]:
                        best_sample = {
                            **taluka,
                            "sample_hits": sample_hits,
                            "sample_total": sample_total,
                        }
                    if sample_hits >= TALUKA_SAMPLE_CONFIDENT_HITS:
                        self.logger.info(
                            f"  taluka '{taluka['text']}' -> {sample_hits}/{sample_total} "
                            f"sampled urban hits, confident match -- stopping scan early"
                        )
                        break
            except Exception as exc:
                self.logger.warning(
                    f"  taluka '{taluka['text']}' -> scan error, skipping: {exc}"
                )
                continue

        chosen = None
        reason = ""
        if best_mauja and best_mauja["mauja_count"] > 0:
            chosen = best_mauja
            reason = f"{chosen['mauja_count']} मौजा entries"
            self.district_uses_mauja_convention = (
                best_mauja["mauja_count"] >= MAUJA_CONVENTION_MIN_COUNT
            )
        elif best_sample and best_sample["sample_hits"] > 0:
            chosen = best_sample
            reason = (
                f"{chosen['sample_hits']}/{chosen['sample_total']} sampled "
                f"villages render urban grid (no मौजा-style text marker in this district)"
            )
            # Grid-sampling-based districts (Pune-style) never get the rural
            # pre-filter -- there is no text marker to filter on, by definition.
            self.district_uses_mauja_convention = False

        if chosen is None:
            self.logger.error(
                f"{self.city}: no taluka with urban data found "
                f"(tried both मौजा-prefix counting and grid-type sampling)"
            )
            return None

        if not self._select_taluka(page, chosen["value"]):  # leave it selected
            self.logger.error(
                f"{self.city}: postback failed re-selecting best taluka "
                f"'{chosen['text']}' after scan"
            )
            return None

        self.logger.info(
            f"{self.city}: auto-detected taluka '{chosen['text']}' ({reason})"
            f"{' -- rural pre-filter ENABLED' if self.district_uses_mauja_convention else ''}"
        )
        return chosen

    def _select_village(self, page: Page, value: str) -> bool:
        return self._postback_select(page, f"#{_ID_VILLAGE}", value)

    # ── Rate grid extraction ─────────────────────────────────────────────

    def _extract_grid_page(self, page: Page) -> list[dict]:
        """Extract sub-zone rows from the currently rendered grid page."""
        grid = page.query_selector(f"#{_ID_GRID}")
        if not grid:
            return []

        rows = grid.eval_on_selector_all(
            "tr",
            """
            trs => {
                const out = [];
                for (let i = 1; i < trs.length; i++) {
                    const cells = Array.from(trs[i].querySelectorAll('td'))
                        .map(td => td.textContent.trim());
                    // Pagination row has no <td>s matching the data shape; skip
                    if (cells.length < 8) continue;
                    out.push(cells);
                }
                return out;
            }
            """,
        )

        parsed = []
        for cells in rows:
            try:
                # Confirmed real column order (8 <td> cells per row):
                # [0] SurveyNo (link label, unused) [1] उपविभाग/sub_zone
                # [2] खुली जमीन/open_land [3] निवासी सदनिका/residential
                # [4] ऑफ़ीस/office (skip) [5] दुकाने/shop (skip)
                # [6] औद्योगिक/industrial (skip) [7] एकक/unit
                sub_zone    = cells[1]
                open_land   = cells[2]
                residential = cells[3]
                unit        = cells[7]
                parsed.append({
                    "sub_zone":     sub_zone,
                    "open_land":    open_land,
                    "residential":  residential,
                    "unit":         unit,
                })
            except (IndexError, TypeError):
                continue

        return parsed

    def _next_page_link(self, page: Page, current_page: int):
        """
        Returns the Playwright element handle for the next page, or None.
        GridView shows numbered links 1..10 plus a '...' link to advance
        to the next batch (11..20, etc). An exact-number match alone
        silently stops at every 10-page boundary -- this checks the
        ellipsis as a fallback before giving up.
        """
        target = str(current_page + 1)
        link = page.query_selector(f"#{_ID_GRID} a:text-is('{target}')")
        if link:
            return link
        return page.query_selector(f"#{_ID_GRID} a:text-is('...')")

    def _scrape_locality_rates(self, page: Page) -> list[dict]:
        """Paginate through the full sub-zone grid for the currently selected
        village and return all raw row dicts."""
        all_rows = []
        page_num = 1

        while page_num <= MAX_SUBZONE_PAGES_PER_LOCALITY:
            rows = self._extract_grid_page(page)
            all_rows.extend(rows)

            next_link = self._next_page_link(page, page_num)
            if not next_link:
                break

            try:
                with page.expect_navigation(wait_until="domcontentloaded", timeout=20000):
                    next_link.click()
                page.wait_for_timeout(500)
            except Exception as exc:
                self.logger.warning(
                    f"Grid pagination postback failed at page {page_num}, "
                    f"stopping with {len(all_rows)} rows collected so far: {exc}"
                )
                break

            page_num += 1

        if page_num >= MAX_SUBZONE_PAGES_PER_LOCALITY:
            self.logger.warning(
                f"Hit {MAX_SUBZONE_PAGES_PER_LOCALITY}-page safety cap -- "
                f"locality may have more sub-zone rows than captured"
            )
            self.stats["pagination_cap_hits"] += 1

        return all_rows

    # ── Parsing & aggregation ────────────────────────────────────────────

    @staticmethod
    def _parse_rate(raw: str) -> Optional[float]:
        if raw is None:
            return None
        cleaned = re.sub(r"[^\d.]", "", str(raw))
        if not cleaned:
            return None
        try:
            val = float(cleaned)
        except ValueError:
            return None
        # Treat 0 as "not applicable", not a real free rate
        if val <= 0:
            return None
        return val

    @staticmethod
    def _is_sqm_unit(unit_text: str) -> bool:
        # Confirmed unit text seen: "चौ. मीटर" (sqm). Defensive check for sqft.
        return "फूट" not in str(unit_text)

    def _aggregate(
        self, raw_rows: list[dict], field: str
    ) -> Optional[dict]:
        """Aggregate min/max/avg for one property-type field across all
        sub-zone rows. Filters out-of-bounds and zero/NA values."""
        values_sqm = []
        for row in raw_rows:
            rate = self._parse_rate(row.get(field))
            if rate is None:
                continue
            if not (MIN_PLAUSIBLE_RATE_SQM <= rate <= MAX_PLAUSIBLE_RATE_SQM):
                self.logger.debug(f"Dropping implausible rate {rate} for field={field}")
                continue
            if not self._is_sqm_unit(row.get("unit", "")):
                # Already sqft -- convert up to sqm for consistent aggregation
                rate = rate * SQM_TO_SQFT
            values_sqm.append(rate)

        if not values_sqm:
            return None

        return {
            "min_sqm": min(values_sqm),
            "max_sqm": max(values_sqm),
            "avg_sqm": round(sum(values_sqm) / len(values_sqm), 2),
            "count":   len(values_sqm),
        }

    # ── Locality canonicalization ────────────────────────────────────────

    def _canonicalize(self, mauja_text: str) -> str:
        """
        Strip "मौजा : " prefix and apply best-effort normalization.
        Government village names rarely match our canonical locality
        strings exactly -- this is a best-effort pass, not authoritative.
        Logs (does not block) when no alias match is found.
        """
        raw = mauja_text.replace(MAUJA_PREFIX, "").replace(":", "").strip()
        # Strip trailing parenthetical notes e.g. "(नागपूर महानगरपालिका)"
        raw = re.sub(r"\(.*?\)", "", raw).strip()

        normalized = raw.lower().strip()
        for suffix in (" ward", " layout", " colony", " nagar"):
            if normalized.endswith(suffix) and len(normalized) - len(suffix) >= 3:
                normalized = normalized[: -len(suffix)].strip()

        for alias, canonical in LOCALITY_ALIASES.items():
            if alias.lower() == normalized or alias.lower() == raw.lower():
                return canonical

        self.logger.debug(
            f"No alias match for village '{raw}' -- storing raw name as locality"
        )
        return raw

    # ── City <-> DB id resolution ────────────────────────────────────────

    def _resolve_city_id(self) -> Optional[int]:
        from db.connection import select_rows
        rows = select_rows("cities", {"name": self.city})
        return rows[0]["id"] if rows else None

    # ── Save ──────────────────────────────────────────────────────────────

    def save(self, record: dict) -> str:
        from db.connection import upsert_row
        upsert_row(
            "circle_rates",
            record,
            on_conflict="city_id,village,property_type,effective_year",
        )
        return "upserted"

    def _build_records(
        self,
        city_id: int,
        district: str,
        taluka_text: str,
        village_text: str,
        locality: str,
        raw_rows: list[dict],
        effective_year: int,
        source_url: str,
    ) -> list[dict]:
        records = []
        field_to_proptype = {
            "open_land":   "plot",
            "residential": "flat",
        }

        for field, prop_type in field_to_proptype.items():
            agg = self._aggregate(raw_rows, field)
            if agg is None:
                continue

            records.append({
                "city_id":            city_id,
                "district":           district,
                "taluka":             taluka_text,
                "village":            village_text,
                "locality":           locality,
                "property_type":      prop_type,
                "rate_per_sqm_min":   round(agg["min_sqm"], 2),
                "rate_per_sqm_max":   round(agg["max_sqm"], 2),
                "rate_per_sqm_avg":   agg["avg_sqm"],
                "rate_per_sqft_min":  round(agg["min_sqm"] / SQM_TO_SQFT, 2),
                "rate_per_sqft_max":  round(agg["max_sqm"] / SQM_TO_SQFT, 2),
                "rate_per_sqft_avg":  round(agg["avg_sqm"] / SQM_TO_SQFT, 2),
                "sub_zone_count":     agg["count"],
                "effective_year":     effective_year,
                "source_url":         source_url,
                "raw_data":           raw_rows,
                "scraped_at":         datetime.utcnow().isoformat(),
            })

        return records

    # ── Main entry point ─────────────────────────────────────────────────

    def _process_village(
        self,
        page: Page,
        village: dict,
        city_id: int,
        district: str,
        taluka: dict,
        effective_year: int,
    ) -> bool:
        """
        Select one village, check for the urban grid, scrape and save rate
        records if present. Extracted from run()'s main loop so the same
        logic can be reused for a retry pass over villages that failed the
        first time (see run()).

        Returns True for every "handled cleanly" outcome -- including a
        genuine rural skip or zero rate rows, neither of which are failures.
        Returns False only when the village postback itself failed (a real
        miss worth retrying). Lets unexpected exceptions propagate so the
        caller can apply its own recovery/retry policy, same as before.
        """
        if not self._select_village(page, village["value"]):
            self.logger.warning(
                f"Postback failed selecting village '{village['text']}' -- skipping"
            )
            return False

        if not page.query_selector(f"#{_ID_GRID}"):
            self.logger.debug(
                f"No urban grid rendered for '{village['text']}' "
                f"-- rural/agricultural entry, skipping"
            )
            self.stats["localities_skipped_rural"] += 1
            return True

        raw_rows = self._scrape_locality_rates(page)
        if not raw_rows:
            self.logger.debug(f"No rate rows for '{village['text']}'")
            return True

        locality = self._canonicalize(village["text"])
        records = self._build_records(
            city_id=city_id,
            district=district,
            taluka_text=taluka["text"],
            village_text=village["text"].replace(MAUJA_PREFIX, "").replace(":", "").strip(),
            locality=locality,
            raw_rows=raw_rows,
            effective_year=effective_year,
            source_url=page.url,
        )

        for record in records:
            self.save(record)
            self.stats["inserted"] = self.stats.get("inserted", 0) + 1

        self.stats["localities_scraped"] += 1
        self.logger.info(
            f"  '{village['text']}' -> {len(raw_rows)} sub-zone rows, "
            f"{len(records)} aggregated records saved"
        )
        return True

    def run(self) -> None:
        self.start_run()
        status = "success"
        city_id = self._resolve_city_id()
        if city_id is None:
            self.logger.error(f"Could not resolve city_id for '{self.city}' -- aborting")
            self.finish_run("failed")
            return

        try:
            with sync_playwright() as p:
                # headless=True intentionally -- a visible, unfocused window during
                # a multi-hour unattended run is suspected to trigger Chrome's
                # background-tab JS timer throttling, which would silently stall
                # the deferred __doPostBack (fired via setTimeout(...,0)) that every
                # dropdown interaction in this scraper depends on. Leading theory
                # for the unexplained 3h10m silent gap in the 2026-06-20 Nagpur run
                # (run #319: 223 min actual wall-clock). Headless Chrome is not
                # subject to the same focus-based throttling. If this scraper ever
                # needs a visible window for live debugging, override explicitly
                # rather than flipping this default back.
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"
                    ),
                    viewport={"width": 1366, "height": 900},
                )
                page = context.new_page()

                if not self._load_district(page):
                    self.logger.error(
                        f"Could not load district for {self.city} "
                        f"(district param '{_CITY_TO_DISTRICT[self.city_key]}' "
                        f"returned no talukas)"
                    )
                    self.stats["errors"].append("district_load_failed")
                    browser.close()
                    status = "failed"  # NOT self.finish_run("failed") here -- this is
                    # inside the try/finally below, and `return` does not skip
                    # finally. Calling finish_run() here AND letting finally call
                    # it again (with the stale "success" default) silently
                    # overwrote this correct status -- confirmed live on run #334
                    # (Aurangabad): errors=["no_taluka_found"] but status='success'
                    # in the DB. Setting the local var instead means the single
                    # finally-block call below is the only write, with the right value.
                    return

                self._set_year(page)

                taluka = self._resolve_taluka(page)
                if taluka is None:
                    self.logger.error(f"No usable taluka found for {self.city}")
                    self.stats["errors"].append("no_taluka_found")
                    browser.close()
                    status = "failed"  # see comment on the district_load_failed
                    # branch above -- same double-finish_run bug, same fix.
                    return

                district = _CITY_TO_DISTRICT[self.city_key]
                villages = self._get_village_options(page)

                # NOTE: the "मौजा :" prefix is NOT a universal eASR convention.
                # Confirmed via live testing: Nagpur district marks urban
                # entries with this prefix, but Pune district does not --
                # well-known Pune neighbourhoods (कोथरुड/Kothrud, औंध/Aundh,
                # कात्रज/Katraj) appear as plain village names with no prefix
                # at all, and still render the urban rate grid correctly.
                # A text-based pre-filter silently skips real city data on
                # any district that doesn't follow Nagpur's convention --
                # which is exactly what made the Pune run report "0 मौजा
                # entries" in every taluka.
                #
                # UPDATED: self.district_uses_mauja_convention (set by
                # _resolve_taluka) now gates a conditional pre-filter below.
                # It's only True when मौजा-prefixed villages clear
                # MAUJA_CONVENTION_MIN_COUNT for THIS taluka in THIS run --
                # i.e. only districts that behave like Nagpur, never inferred
                # from a single stray match. For every other district
                # (Pune-style, or unconfirmed with a weak/zero मौजा signal)
                # this stays False and every village is still selected and
                # let the runtime grid-check decide urban vs rural, exactly
                # as before -- no behavior change for those districts.
                mauja_count = sum(1 for v in villages if v["text"].startswith(MAUJA_PREFIX))
                self.logger.info(
                    f"{self.city}: {len(villages)} total villages to screen "
                    f"({mauja_count} carry the मौजा prefix"
                    f"{' -- rural pre-filter active, skipping non-मौजा postbacks' if self.district_uses_mauja_convention else ', informational only -- not used as a filter'})"
                )

                # Year for DB: read from selected dropdown text e.g. "20262027" -> 2026
                year_text = page.eval_on_selector(
                    f"#{_ID_YEAR}", "el => el.options[el.selectedIndex].value"
                )
                try:
                    effective_year = int(str(year_text)[:4])
                except (ValueError, TypeError):
                    effective_year = datetime.utcnow().year

                failed_villages = []   # villages eligible for a single retry pass after the main loop
                prefiltered_count = 0  # non-मौजा villages skipped without a postback

                for village in villages:
                    if (
                        self.district_uses_mauja_convention
                        and not village["text"].startswith(MAUJA_PREFIX)
                    ):
                        self.stats["localities_skipped_rural"] += 1
                        prefiltered_count += 1
                        continue

                    try:
                        ok = self._process_village(
                            page, village, city_id, district, taluka, effective_year
                        )
                        if not ok:
                            failed_villages.append(village)
                            # ── Page-state recovery on postback failure ───────────
                            # False means the village dropdown timed out (30s).
                            # Root cause: after a grid renders and is scraped, the
                            # page navigates to a results view where the district/
                            # taluka/village dropdowns are no longer in the DOM.
                            # Without recovery, every subsequent select_option()
                            # waits 30s for a missing selector — burning the entire
                            # remaining run (190 villages × 30s = ~95 min wasted).
                            # Recovery reloads the district page and restores the
                            # taluka so the dropdown is available for the next
                            # village. No wait needed here (unlike the exception
                            # path) — a postback timeout means no navigation is
                            # currently in flight.
                            self.logger.info(
                                f"  Postback failed for '{village['text']}' — "
                                f"resetting page state"
                            )
                            try:
                                self._load_district(page)
                                self._set_year(page)
                                self._select_taluka(page, taluka["value"])
                            except Exception as recover_exc:
                                self.logger.error(
                                    f"Recovery after postback failure: {recover_exc}"
                                )
                                break
                    except Exception as exc:
                        self.logger.error(
                            f"Error scraping village '{village.get('text')}': {exc}"
                        )
                        self.stats["errors"].append(f"{village.get('text')}: {exc}")
                        failed_villages.append(village)
                        # One failed locality should not kill the whole run --
                        # reload district page fresh and continue with next village
                        try:
                            page.wait_for_timeout(2500)  # let any in-flight
                            # navigation/postback settle before calling goto() --
                            # calling goto() while a previous postback is still
                            # resolving was the suspected cause of recovery
                            # itself throwing and hitting the break below
                            self._load_district(page)
                            self._set_year(page)
                            self._select_taluka(page, taluka["value"])
                        except Exception as recover_exc:
                            self.logger.error(f"Recovery navigation failed: {recover_exc}")
                            break

                if prefiltered_count:
                    self.logger.info(
                        f"{self.city}: {prefiltered_count} non-मौजा villages "
                        f"skipped without postback (rural pre-filter)"
                    )

                # Single retry pass -- villages that failed (postback failure or
                # exception) during the main loop are not guaranteed-lost anymore.
                # Each gets exactly one more attempt; permanent failures are
                # logged distinctly so they're easy to grep out of run history.
                if failed_villages:
                    self.logger.info(
                        f"{self.city}: retrying {len(failed_villages)} village(s) "
                        f"that failed in the main pass"
                    )
                    # Reset page to clean district/taluka state before retry pass —
                    # the main loop may have left the page in a broken state.
                    try:
                        self._load_district(page)
                        self._set_year(page)
                        self._select_taluka(page, taluka["value"])
                    except Exception as retry_setup_exc:
                        self.logger.error(
                            f"Could not restore page for retry pass: {retry_setup_exc} "
                            f"— skipping retry"
                        )
                        failed_villages = []  # prevent retry loop on broken page

                    still_failed = []
                    for village in failed_villages:
                        try:
                            ok = self._process_village(
                                page, village, city_id, district, taluka, effective_year
                            )
                            if not ok:
                                still_failed.append(village["text"])
                                try:
                                    self._load_district(page)
                                    self._set_year(page)
                                    self._select_taluka(page, taluka["value"])
                                except Exception:
                                    break
                        except Exception as exc:
                            self.logger.error(
                                f"Retry also failed for village "
                                f"'{village.get('text')}': {exc}"
                            )
                            self.stats["errors"].append(
                                f"RETRY FAILED {village.get('text')}: {exc}"
                            )
                            still_failed.append(village.get("text"))
                            try:
                                page.wait_for_timeout(2500)
                                self._load_district(page)
                                self._set_year(page)
                                self._select_taluka(page, taluka["value"])
                            except Exception:
                                break
                    if still_failed:
                        self.logger.warning(
                            f"{self.city}: {len(still_failed)} village(s) "
                            f"permanently failed after retry: {still_failed}"
                        )

                browser.close()

        except Exception as exc:
            self.logger.exception(f"Fatal eASR scrape error: {exc}")
            self.stats["errors"].append(f"FATAL: {exc}")
            status = "failed"
        finally:
            self.logger.info(
                f"── eASR summary [{self.city}] ──────────────────────────\n"
                f"  Localities scraped     : {self.stats.get('localities_scraped', 0)}\n"
                f"  Rural villages skipped : {self.stats.get('localities_skipped_rural', 0)}\n"
                f"  Records upserted       : {self.stats.get('inserted', 0)}\n"
                f"  Pagination cap hits    : {self.stats.get('pagination_cap_hits', 0)}\n"
                f"  Errors                 : {len(self.stats.get('errors', []))}\n"
                f"─────────────────────────────────────────────────────────"
            )
            self.finish_run(status)
