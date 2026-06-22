"""
scrapers/scraper_rera_detail.py
MahaRERA detail enrichment scraper — full field extraction.

Captures:
  - promoter_pan, promoter_type, promoter_name
  - project_cost, amount_collected, escrow_balance, loan_amount
  - total_units, units_sold, units_available
  - proposed_completion, revised_completion, actual_completion
  - total_area_sqm, land_area_sqm, total_built_up_sqm
  - delay_months, is_completed, rera_status, project_type
  - address_raw, pin_code, district
  - latitude, longitude, location (PostGIS)
  - is_flagged, flag_reasons

Debug tip:
  Set LOG_RAW_RESPONSES = True below to log the full API response
  for the first project. Paste here to identify new field paths.
"""

import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests
from playwright.sync_api import sync_playwright

from scrapers.scraper_rera import (
    DISTRICT_CODES,
    DISTRICT_DIVISION,
    DIVISION_CODES,
    SEARCH_URL,
)

logger = logging.getLogger(__name__)

# ── Toggle this to True for one run to inspect raw API responses ──────────────
LOG_RAW_RESPONSES = False   # set True temporarily when inspecting new field paths

BASE_API = "https://maharerait.maharashtra.gov.in/api"
AUTH_URL = f"{BASE_API}/maha-rera-login-service/login/authenticatePublic"
PROJ_API = (
    f"{BASE_API}/maha-rera-public-view-project-registration-service"
    f"/public/projectregistartion"
)
PROMOTER_API = (
    f"{BASE_API}/maha-rera-promoter-management-service/promoter"
)

PUBLIC_CREDS = {
    "userName": "U2FsdGVkX1+7cjm+qOyHkFn8VByR+Ql715axlxNG0zc3fel2a5+zqdxoZhBUqc18",
    "password":  "U2FsdGVkX18gx/z5FFiEqIVcHb4zVDbFWN3G5FbPJe8=",
}

ENDPOINTS = {
    "general":    "/getProjectGeneralDetailsByProjectId",
    "status":     "/getProjectCurrentStatus",
    "complaints": "/getComplaintDetailsByProjectId",
    "units":      "/getBuildingWingUnitSummary",
    "promoter":   "/getProjectAndAssociatedPromoterDetails",
    "address":    "/getProjectLandAddressDetails",
    "land_header": "/getProjectLandHeaderDetails",
    "land_cc": "/getProjectLandCCDetailsResponse",
    "litigation": "/getProjectLitigationDetails",
    "extensions": "/getProjectPreviousExtensionDetails",
    "cost_estimation": "/getBuildingWingsCostEstimation",
    "finance_bank": "/getProjectFinaceBankDetails",
    "means_finance": "/getProjectMeansOfFinance",
    "promoter_bank": "/getProjectPromoterBankDetails",
    "finance_inventory": "/getProjectFinanceInventoryDetails",
}

PROMOTER_ENDPOINTS = {
    "general": "/fetchPromoterGeneralDetails",
    "address": "/getPromoterAddressDetails",
    "contact": "/getPromoterContactDetails",
}

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "MahaRERA-IntelPlatform/1.0 (research)"}
PAN_RE = re.compile(r"^[A-Z]{5}[0-9]{4}[A-Z]$")
ZERO_ALLOWED_FIELDS = {
    "complaint_count",
    "total_units",
    "units_sold",
    "units_available",
    "amount_collected",
    "loan_amount",
}

# Canonical rera_status values: active | completed | lapsed
# Maps any raw statusName from the MahaRERA API to one of those three.
# "de-registered" / "cancelled" → lapsed so stalled_projects detection catches them.
# Keep this in sync with scraper_rera.py's normalisation.
_RERA_STATUS_MAP: dict[str, str] = {
    "active":          "active",
    "registered":      "active",      # list-page variant; safety net
    "new":             "active",
    "completed":       "completed",
    "lapsed":          "lapsed",
    "expired":         "lapsed",
    "de-registered":   "lapsed",      # MahaRERA active removal — treat as lapsed
    "deregistered":    "lapsed",      # alternate spelling
    "de registered":   "lapsed",      # space variant (no hyphen)
    "cancelled":       "lapsed",
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _first(*values):
    """Return first non-empty value from a list of candidates."""
    for v in values:
        if v not in (None, "", 0, [], {}):
            return v
    return None


def _first_present(*values):
    """Return first present value, allowing numeric 0."""
    for v in values:
        if v not in (None, "", [], {}):
            return v
    return None


def _safe_float(val, allow_zero: bool = False) -> float | None:
    if val is None or val == "":
        return None
    try:
        f = float(str(val).replace(",", "").strip())
        if allow_zero:
            return f if f >= 0 else None
        return f if f > 0 else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    if val is None or val == "":
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None


def _join_nonempty(*parts) -> str:
    return " ".join(str(p).strip() for p in parts if p and str(p).strip())


def _normalize_pan(val) -> str:
    pan = str(val or "").strip().upper()
    return pan if PAN_RE.fullmatch(pan) else ""


def _find_numeric_values(value, candidate_keys: set[str]) -> list[float]:
    values: list[float] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key in candidate_keys:
                num = _safe_float(item)
                if num is not None:
                    values.append(num)
            if isinstance(item, (dict, list)):
                values.extend(_find_numeric_values(item, candidate_keys))
    elif isinstance(value, list):
        for item in value:
            values.extend(_find_numeric_values(item, candidate_keys))
    return values


def _sum_finance_parts(obj: dict | None) -> float | None:
    """Sum secured + unsecured borrowed funds from a means-of-finance sub-object."""
    if not isinstance(obj, dict):
        return None
    secured = _safe_float(
        obj.get("totalBorrowedFundsSecured"),   allow_zero=True)
    unsecured = _safe_float(
        obj.get("totalBorrowedFundsUnsecured"), allow_zero=True)
    values = [v for v in (secured, unsecured) if v is not None]
    if not values:
        return None
    return round(sum(values), 2)


# ── Main class ─────────────────────────────────────────────────────────────────

class RERADetailScraper:

    def __init__(self):
        self.token = None
        self.token_time = 0
        self.session = requests.Session()
        self.url_cache:  dict[str, str] = {}
        self._geocode_cache: dict[str, tuple[float, float] | None] = {}
        self._last_geocode = 0.0
        self._logged_raw = False   # log raw response once per run
        self._token_lock = threading.Lock()
        self._playwright = None
        self._browser = None
        self._search_page = None
        self.stats = {"updated": 0, "skipped": 0, "errors": 0, "geocoded": 0}

        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept":        "application/json, text/plain, */*",
            "Origin":        "https://maharerait.maharashtra.gov.in",
            "Referer":       "https://maharerait.maharashtra.gov.in/",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        })

    # ── Playwright ─────────────────────────────────────────────────────────────

    def _ensure_search_page(self):
        if self._playwright is None:
            self._playwright = sync_playwright().start()
        if self._browser is None:
            self._browser = self._playwright.chromium.launch(headless=True)
        if self._search_page is None:
            self._search_page = self._browser.new_page(
                viewport={"width": 1366, "height": 768}
            )
            self._search_page.set_default_timeout(15000)
        return self._search_page

    def _close_search_page(self):
        for attr in ("_search_page", "_browser", "_playwright"):
            obj = getattr(self, attr)
            if obj is not None:
                obj.close() if attr != "_playwright" else obj.stop()
                setattr(self, attr, None)

    # ── Auth ───────────────────────────────────────────────────────────────────

    def authenticate(self):
        self.session.headers.pop("Authorization", None)
        try:
            resp = self.session.post(AUTH_URL, json=PUBLIC_CREDS, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            response_obj = data.get("responseObject") or {}
            token = ""
            if isinstance(response_obj, dict):
                token = response_obj.get("accessToken") or \
                    response_obj.get("token") or ""
            elif isinstance(response_obj, str):
                token = response_obj
            token = token or data.get("accessToken") or data.get("token") or ""
            if not token:
                raise ValueError("No access token in auth response")
            self.token = token
            self.token_time = time.time()
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            logger.info("MahaRERA JWT obtained")
            return token
        except Exception as e:
            self.token = None
            self.token_time = 0
            logger.error(f"Auth failed: {e}")
            raise

    def _ensure_token(self):
        with self._token_lock:
            if not self.token or (time.time() - self.token_time) > 3300:
                self.authenticate()

    # ── URL resolver ───────────────────────────────────────────────────────────

    def resolve_detail_url(self, registration: str, district: str) -> str:
        registration = (registration or "").strip()
        if not registration:
            return ""
        if registration in self.url_cache:
            return self.url_cache[registration]

        if self._search_page is not None:
            self._search_page.close()
            self._search_page = None
        page = self._ensure_search_page()
        division = DISTRICT_DIVISION.get(district or "", "Amravati")
        division_code = DIVISION_CODES.get(division, "2")
        district_code = DISTRICT_CODES.get(district or "", "501")

        try:
            page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=30000)
            page.evaluate("""() => {
                document.querySelectorAll('.modal-backdrop').forEach(el => el.remove());
                const modal = document.querySelector('#autoShowModal');
                if (modal) modal.remove();
                document.body.classList.remove('modal-open');
            }""")
            page.wait_for_function(
                """() => Array.from(document.querySelector("#edit-project-state")?.options || [])
                   .some(opt => opt.value === "27")"""
            )
            page.locator('label[for="edit-project-type-0"]').click()
            page.locator("#edit-project-state").select_option("27")
            page.wait_for_function(
                f"""() => Array.from(document.querySelector("#edit-project-division")?.options || [])
                   .some(opt => opt.value === "{division_code}")"""
            )
            page.locator("#edit-project-division").select_option(division_code)
            page.wait_for_function(
                f"""() => Array.from(document.querySelector("#edit-project-district")?.options || [])
                   .some(opt => opt.value === "{district_code}")"""
            )
            page.locator("#edit-project-district").select_option(district_code)
            page.locator("#edit-maharera-no").fill(registration)
            page.locator("#edit-submit-project-search-form").click()
            page.wait_for_load_state("networkidle", timeout=15000)

            for link in page.query_selector_all("a[href*='/view/']"):
                href = link.get_attribute("href") or ""
                if "/view/" in href:
                    full = href if href.startswith("http") else \
                        f"https://maharerait.maharashtra.gov.in{href}"
                    self.url_cache[registration] = full
                    return full
        except Exception as e:
            logger.warning(
                f"resolve_detail_url failed for {registration}: {e}")
        return ""

    def _resolve_api_project_id(self, url_id: int, detail_url: str) -> int | None:
        """
        Returns the working API projectId for this project.

        For cities like Akola/Amravati/Nagpur the URL view-ID is the real
        API projectId — confirmed with a quick probe (no extra cost).

        For Pune, the URL view-ID is a CMS node ID that doesn't match the
        API's internal projectId. We fall back to loading the detail page
        with Playwright and intercepting the real ID from the first XHR call
        the page makes to the MahaRERA project API.

        Results are cached per run (url_id → real_id) so each project is
        only probed/intercepted once even if the run loops encounter it again.
        """
        cache_key = f"pid_{url_id}"
        if cache_key in self.url_cache:
            cached = self.url_cache[cache_key]
            return int(cached) if cached else None

        # Quick probe — try the URL ID directly (fast path for all working cities)
        probe_url = f"{PROJ_API}{ENDPOINTS['general']}"
        try:
            self._ensure_token()
            resp = self.session.post(
                probe_url, json={"projectId": url_id}, timeout=10
            )
            if resp.status_code == 200:
                self.url_cache[cache_key] = str(url_id)
                return url_id
        except Exception:
            pass

        # URL ID doesn't work — intercept real ID from the detail page
        real_id = self._intercept_project_id_from_page(detail_url)
        self.url_cache[cache_key] = str(real_id) if real_id else ""
        return real_id

    def _intercept_project_id_from_page(self, detail_url: str) -> int | None:
        """
        Load the project detail page and intercept the projectId from the
        first XHR/fetch call the page makes to the MahaRERA project API.

        Used as fallback for cities (Pune) where the URL view-ID is a CMS
        node ID that does not match the API's internal projectId. The detail
        page JS resolves this mapping at runtime — we just listen to the
        outgoing request to read the real value.
        """
        if not detail_url or "http" not in detail_url:
            return None

        captured: list[int] = []
        page = self._ensure_search_page()

        def on_request(request) -> None:
            if captured:
                return
            if "projectregistartion" not in request.url:
                return
            try:
                post_data = request.post_data
                if not post_data:
                    return
                payload = json.loads(post_data)
                pid = payload.get("projectId")
                if pid is not None:
                    captured.append(int(pid))
            except Exception:
                pass

        page.on("request", on_request)
        try:
            page.goto(detail_url, wait_until="domcontentloaded", timeout=30000)
            # Poll until first API call fires (max 8 s)
            deadline = time.time() + 8
            while not captured and time.time() < deadline:
                page.wait_for_timeout(500)
        except Exception as e:
            logger.warning(
                f"_intercept_project_id_from_page failed ({detail_url}): {e}"
            )
        finally:
            page.remove_listener("request", on_request)

        if captured:
            logger.debug(
                f"Intercepted real projectId={captured[0]} from {detail_url}"
            )
            return captured[0]

        logger.warning(
            f"Could not intercept projectId from {detail_url}"
        )
        return None

    # ── API ────────────────────────────────────────────────────────────────────

    def _post_with_auth(self, url: str, payload: dict):
        self._ensure_token()
        resp = self.session.post(url, json=payload, timeout=30)
        if resp.status_code == 401:
            with self._token_lock:
                self.authenticate()
            resp = self.session.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def call_api(self, endpoint_key: str, project_id: int) -> dict | list:
        url = f"{PROJ_API}{ENDPOINTS[endpoint_key]}"
        try:
            data = self._post_with_auth(url, {"projectId": project_id})
            result = data.get("responseObject")
            if result is None:
                result = {}

            # Log raw response once per run for debugging
            if LOG_RAW_RESPONSES and not self._logged_raw:
                logger.info(
                    f"[RAW API RESPONSE] endpoint={endpoint_key} "
                    f"project_id={project_id}\n"
                    f"{json.dumps(result, indent=2, default=str)[:3000]}"
                )
            return result
        except Exception as e:
            logger.warning(
                f"API {endpoint_key} failed for project {project_id}: {e}")
            return {}

    def call_promoter_api(
        self,
        endpoint_key: str,
        user_profile_id: int,
        project_id: int,
    ) -> dict | list:
        url = f"{PROMOTER_API}{PROMOTER_ENDPOINTS[endpoint_key]}"
        payload = {"userProfileId": user_profile_id, "projectId": project_id}
        try:
            data = self._post_with_auth(url, payload)
            result = data.get("responseObject")
            return result if result is not None else {}
        except Exception as e:
            logger.warning(
                f"Promoter API {endpoint_key} failed for project "
                f"{project_id} / user {user_profile_id}: {e}"
            )
            return {}

    # ── Geocoding ──────────────────────────────────────────────────────────────

    def geocode(self, address_raw: str, pin_code: str, district: str) \
            -> tuple[float, float] | None:
        cache_key = f"{pin_code}_{district}"
        if cache_key in self._geocode_cache:
            return self._geocode_cache[cache_key]

        elapsed = time.time() - self._last_geocode
        if elapsed < 1.1:
            time.sleep(1.1 - elapsed)

        queries = []
        if address_raw and pin_code:
            queries.append(f"{address_raw}, Maharashtra, India")
        if pin_code and district:
            queries.append(f"{pin_code}, {district}, Maharashtra, India")
        if pin_code:
            queries.append(f"{pin_code}, Maharashtra, India")

        for query in queries:
            try:
                r = requests.get(
                    NOMINATIM_URL,
                    params={"q": query, "format": "json",
                            "limit": 1, "countrycodes": "in"},
                    headers=NOMINATIM_HEADERS,
                    timeout=10,
                )
                self._last_geocode = time.time()
                results = r.json()
                if results:
                    coords = (float(results[0]["lat"]),
                              float(results[0]["lon"]))
                    self._geocode_cache[cache_key] = coords
                    return coords
            except Exception as e:
                logger.debug(f"Geocode attempt failed '{query}': {e}")
            time.sleep(1.1)

        self._geocode_cache[cache_key] = None
        return None

    # ── Enrich ─────────────────────────────────────────────────────────────────

    def _should_skip(self, proj: dict) -> bool:
        """Return True if this project can be safely skipped this run.

        Rules:
        - Never skip if updated_at is NULL (never enriched before)
        - Never skip active projects — status, units_sold, complaints can change
        - Never skip if total_units is NULL (core data missing)
        - Skip completed/lapsed projects enriched within the last 30 days
        """
        updated_at = proj.get("updated_at")
        if not updated_at:
            return False  # never enriched

        rera_status = str(proj.get("rera_status") or "").lower().strip()
        if rera_status in ("active", ""):
            return False  # active or unknown — always re-enrich

        if proj.get("total_units") is None:
            return False  # missing core data

        try:
            updated = datetime.fromisoformat(
                str(updated_at).replace("Z", "+00:00"))
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - updated).days < 30
        except Exception:
            return False

    def enrich_project(self, db_id: str, rera_project_id: int) -> dict:
        time.sleep(0.1)  # reduced from 0.5s — network latency is the real throttle

        # ── Batch 1: all independent endpoints fired in parallel ───────────────
        _batch1 = [
            "general", "status", "complaints", "units", "promoter",
            "address", "land_header", "land_cc", "litigation", "extensions",
            "cost_estimation", "finance_bank", "means_finance",
        ]
        with ThreadPoolExecutor(max_workers=6) as executor:
            _futures = {
                key: executor.submit(self.call_api, key, rera_project_id)
                for key in _batch1
            }
            _results = {key: fut.result() for key, fut in _futures.items()}

        general         = _results["general"]
        status          = _results["status"]
        comp_raw        = _results["complaints"]
        units_list      = _results["units"]
        promoter        = _results["promoter"]
        address         = _results["address"]
        land_header     = _results["land_header"]
        land_cc         = _results["land_cc"]
        litigation      = _results["litigation"]
        extensions      = _results["extensions"]
        cost_estimation = _results["cost_estimation"]
        finance_bank    = _results["finance_bank"]
        means_finance   = _results["means_finance"]

        # Mark raw logged after first project
        if LOG_RAW_RESPONSES and not self._logged_raw:
            self._logged_raw = True

        # ── Complaints ────────────────────────────────────────────────────────
        complaints = comp_raw.get("complaintDetails") or []
        complaint_count = len(complaints) if isinstance(
            complaints, list) else 0

        # ── Units ─────────────────────────────────────────────────────────────
        total_units = 0
        units_sold = 0
        if isinstance(units_list, list):
            for wing in units_list:
                total_units += _safe_int(wing.get("totalUnitCount")) or 0
                units_sold += _safe_int(_first_present(
                    wing.get("bookedUnitCount"),
                    wing.get("soldUnitCount"),
                    wing.get("unitsSold"),
                )) or 0

        # Fallback to general if units API returned nothing
        if total_units == 0:
            total_units = _safe_int(
                _first(general.get("totalNumberOfUnits"),
                       general.get("noOfUnits"),
                       general.get("totalUnits"))
            ) or 0
        if units_sold == 0:
            units_sold = _safe_int(
                _first_present(
                    general.get("totalNumberOfSoldUnits"),
                    general.get("soldUnits"),
                    general.get("noOfSoldUnits"),
                    general.get("currentSaleCount"),
                )
            ) or 0

        # ── Promoter ──────────────────────────────────────────────────────────
        # The promoter endpoint can return the data in different shapes.
        # Log if empty so we can identify the actual field names.
        promoter_pan = ""
        promoter_type = ""
        promoter_name = ""

        promoter_details = {}
        raw_promoter = promoter.get("promoterDetails") if isinstance(
            promoter, dict) else {}
        if isinstance(raw_promoter, list) and raw_promoter:
            promoter_details = raw_promoter[0]
        elif isinstance(raw_promoter, dict):
            promoter_details = raw_promoter

        user_profile_id = _safe_int(_first(
            general.get("userProfileId"),
            promoter_details.get("userProfileId"),
        ))
        promoter_general = {}
        promoter_address = {}
        if user_profile_id:
            promoter_general = self.call_promoter_api(
                "general", user_profile_id, rera_project_id)
            promoter_address = self.call_promoter_api(
                "address", user_profile_id, rera_project_id)

        promoter_name = _first(
            promoter_general.get("organizationName"),
            promoter_general.get("businessName"),
            promoter_general.get("hufName"),
            _join_nonempty(
                promoter_general.get("firstName"),
                promoter_general.get("middleName"),
                promoter_general.get("lastName"),
            ),
            promoter_details.get("promoterName"),
            promoter_details.get("name"),
            # fallback: contact person
            promoter_details.get("contactPersonName"),
            general.get("promoterName"),
        ) or ""

        promoter_pan = _normalize_pan(_first_present(
            promoter_general.get("panNumber"),
            promoter_general.get("kartaPanCard"),
            promoter_details.get("panNumber"),
            promoter_details.get("pan"),
            promoter_details.get("panNo"),
            promoter_details.get("PAN"),
        ))

        promoter_type = _first(
            promoter_general.get("userProfileTypeName"),
            promoter_general.get("organizationType"),
            promoter_details.get("promoterType"),
            promoter_details.get("applicantType"),
            promoter_details.get("entityType"),
        ) or ""

        if not promoter_pan:
            logger.debug(
                f"[PROMOTER RAW] project_id={rera_project_id} "
                f"project_keys={list(promoter.keys()) if isinstance(promoter, dict) else type(promoter)} "
                f"general_keys={list(promoter_general.keys()) if isinstance(promoter_general, dict) else type(promoter_general)}"
            )

        # ── Financial fields ──────────────────────────────────────────────────
        status_obj = status.get("coreStatus") or {}
        fin_obj = status.get("financeDetails") or \
            status.get("projectFinance") or \
            general.get("financeDetails") or \
            general.get("projectFinance") or {}
        finance_bank_rows = finance_bank if isinstance(
            finance_bank, list) else []
        means_finance_obj = means_finance if isinstance(
            means_finance, dict) else {}
        means_actual = means_finance_obj.get("actual") \
            if isinstance(means_finance_obj, dict) else {}
        means_proposed = means_finance_obj.get("proposed") \
            if isinstance(means_finance_obj, dict) else {}
        means_estimated = means_finance_obj.get("estimated") \
            if isinstance(means_finance_obj, dict) else {}

        estimated_cost_values = _find_numeric_values(
            cost_estimation,
            {"totalEstimatedCostAsOnRegDate", "projectCost", "totalProjectCost"},
        )
        estimated_cost_total = round(sum(estimated_cost_values), 2) \
            if estimated_cost_values else None
        # Use estimated (full project budget) not actual (work done so far)
        means_estimated_cost = _safe_float(_first_present(
            means_estimated.get("totalEstimatedCostTableA")
            if isinstance(means_estimated, dict) else None,
            means_estimated.get("totalFundsForProject")
            if isinstance(means_estimated, dict) else None,
            means_proposed.get("totalEstimatedCostTableA")
            if isinstance(means_proposed, dict) else None,
            means_actual.get("totalEstimatedCostTableA")
            if isinstance(means_actual, dict) else None,
        ))

        loan_amount_values = _find_numeric_values(
            means_finance_obj,
            {"loanAmount", "bankLoanAmount", "loanSanctioned", "bankFinanceAmount"},
        )
        if not loan_amount_values:
            loan_amount_values = _find_numeric_values(
                finance_bank_rows,
                {"loanAmount", "bankLoanAmount", "sanctionedLoanAmount"},
            )
        loan_amount_from_means = _first_present(
            _sum_finance_parts(means_actual),
            _sum_finance_parts(means_proposed),
            _sum_finance_parts(means_estimated),
        )

        project_cost = _safe_float(_first_present(
            fin_obj.get("projectCost"),
            fin_obj.get("totalProjectCost"),
            general.get("projectCost"),
            general.get("totalProjectCost"),
            general.get("estimatedCost"),
            status.get("projectCost"),
            estimated_cost_total,
            means_estimated_cost,
        ))

        # customerReceipts from actual = money actually received from buyers so far
        amount_collected = _safe_float(_first_present(
            means_actual.get("customerReceipts")
            if isinstance(means_actual, dict) else None,
            fin_obj.get("amountCollected"),
            fin_obj.get("totalAmountCollected"),
            fin_obj.get("collectionAmount"),
            general.get("amountCollected"),
            status.get("amountCollected"),
            status_obj.get("amountCollected"),
        ), allow_zero=True)

        # escrow_balance not available via MahaRERA public API
        escrow_balance = None

        loan_amount = _safe_float(_first_present(
            fin_obj.get("loanAmount"),
            fin_obj.get("bankLoanAmount"),
            general.get("loanAmount"),
            general.get("bankLoanAmount"),
            status.get("loanAmount"),
            loan_amount_from_means,
            loan_amount_values[0] if loan_amount_values else None,
        ), allow_zero=True)

        if all(v is None for v in [project_cost, amount_collected]):
            logger.debug(
                f"[FINANCE RAW] project_id={rera_project_id} "
                f"status_keys={list(status.keys())} "
                f"general_keys={list(general.keys())} "
                f"cost_keys={list(cost_estimation.keys()) if isinstance(cost_estimation, dict) else type(cost_estimation)}"
            )

        # ── Area fields ───────────────────────────────────────────────────────
        total_area_sqm = _safe_float(_first(
            general.get("totalAreaOfLand"),
            general.get("totalProjectArea"),
            general.get("landAreaInSqMtrs"),
            land_header.get("aggregateArea") if isinstance(
                land_header, dict) else None,
            land_header.get("landAreaSqmts") if isinstance(
                land_header, dict) else None,
            address.get("totalArea") if isinstance(address, dict) else None,
        ))

        land_area_sqm = _safe_float(_first(
            land_header.get("landAreaSqmts") if isinstance(
                land_header, dict) else None,
            land_header.get("proposedLandAreaSqmts") if isinstance(
                land_header, dict) else None,
            address.get("landArea") if isinstance(address, dict) else None,
            general.get("landArea"),
            general.get("plotArea"),
        ))

        total_built_up_sqm = _safe_float(_first(
            general.get("totalBuiltUpArea"),
            general.get("builtUpArea"),
            general.get("totalConstructionArea"),
            land_header.get("projectProposedNotSanctionedBuildUpArea")
            if isinstance(land_header, dict) else None,
        ))

        # ── Address ───────────────────────────────────────────────────────────
        addr_dict = address if isinstance(address, dict) else {}

        # promoter endpoint also contains projectLegalLandAddressDetails
        # which sometimes has richer data than the address endpoint
        promo_land_addr = {}
        if isinstance(promoter, dict):
            promo_project = promoter.get("projectDetails") or {}
            promo_land_addr = promo_project.get(
                "projectLegalLandAddressDetails") or {}

        # Merge: address endpoint is primary, promoter land addr is fallback
        def _addr_field(key):
            return addr_dict.get(key) or promo_land_addr.get(key) or ""

        # Build address_raw preferring address endpoint
        address_raw = self._build_address(
            addr_dict) or self._build_address(promo_land_addr)
        pin_code = str(_addr_field("pinCode")).strip()
        district = str(_addr_field("districtName")).strip()

        # Final fallback: promoter_address (separate promoter API)
        if not pin_code and isinstance(promoter_address, dict):
            pin_code = str(promoter_address.get("pinCode", "")).strip()

        # ── Status + dates ────────────────────────────────────────────────────
        # Primary: coreStatus.statusName (works for Pune/Nashik/Aurangabad).
        # Phase 1 cities (Nagpur/Amravati) can return null coreStatus — try
        # progressively broader fallbacks before giving up.
        rera_status = (
            status_obj.get("statusName")
            or status.get("statusName")
            or status.get("projectStatus")
            or status.get("currentStatus")
            or general.get("statusName")
            or general.get("projectStatus")
            or general.get("status")
            or ""
        ).lower().strip()
        rera_status = _RERA_STATUS_MAP.get(rera_status, rera_status)
        if not rera_status:
            logger.warning(
                f"[STATUS MISSING] project_id={rera_project_id} — "
                f"status keys={list(status.keys()) if isinstance(status, dict) else type(status)}, "
                f"coreStatus keys={list(status_obj.keys()) if status_obj else 'empty'} — "
                f"DB value preserved"
            )
        extension_rows = extensions if isinstance(extensions, list) else []
        if isinstance(extensions, dict):
            extension_rows = extensions.get("extensionDetails") or []

        extension_revised_completion = None
        for row in reversed(extension_rows):
            extension_revised_completion = self._parse_date(_first_present(
                row.get("projectProposeRevisedCompletionDate"),
                row.get("revisedCompletionDate"),
            ))
            if extension_revised_completion:
                break

        proposed_completion = self._parse_date(_first(
            general.get("projectProposeComplitionDate"),
            general.get("proposedCompletionDate"),
            general.get("completionDate"),
        ))
        revised_completion = self._parse_date(_first_present(
            status_obj.get("revisedCompletionDate"),
            status.get("revisedCompletionDate"),
            general.get("revisedCompletionDate"),
            general.get("revisedProposedCompletionDate"),
            extension_revised_completion,
        ))
        actual_completion = self._parse_date(_first(
            status_obj.get("actualCompletionDate"),
            status.get("actualCompletionDate"),
            general.get("actualCompletionDate"),
            land_cc[0].get("ccIssuedDate")
            if isinstance(land_cc, list) and land_cc else None,
        ))
        application_date = self._parse_date(_first(
            general.get("projectApplicationDate"),
            general.get("applicationDate"),
        ))
        approval_date = self._parse_date(_first(
            general.get("reraRegistrationDate"),
            general.get("approvalDate"),
            general.get("registrationDate"),
        ))

        # ── Delay ─────────────────────────────────────────────────────────────
        delay_months = None
        if extension_rows:
            delay_months = len(extension_rows) * 6

        # ── Litigation ────────────────────────────────────────────────────────
        # MahaRERA's public litigation payload is often wrapped in a non-empty
        # object even when there are no actual case rows. Treating any non-empty
        # payload as litigation was causing nearly every project to be flagged.
        # Until concrete litigation rows are parsed reliably, do not auto-flag
        # projects from this endpoint alone.
        has_litigation = False

        # ── Build record ──────────────────────────────────────────────────────
        record: dict = {
            "rera_status":          rera_status,
            "project_type":         general.get("projectTypeName", ""),
            "proposed_completion":  proposed_completion,
            "revised_completion":   revised_completion,
            "actual_completion":    actual_completion,
            "application_date":     application_date,
            "approval_date":        approval_date,
            "is_completed":         rera_status == "completed",
            "total_units":          total_units,
            "units_sold":           units_sold,
            "units_available":      max(0, total_units - units_sold),
            "complaint_count":      complaint_count,
            "promoter_pan":         promoter_pan,
            "promoter_type":        promoter_type,
            "pin_code":             pin_code,
            "address_raw":          address_raw,
        }

        # Only write optional fields if non-null
        if promoter_name:
            record["promoter_name"] = promoter_name
        if project_cost is not None:
            record["project_cost"] = project_cost
        if amount_collected is not None:
            record["amount_collected"] = amount_collected
        # escrow_balance not available from MahaRERA public API
        if loan_amount is not None:
            record["loan_amount"] = loan_amount
        if total_area_sqm is not None:
            record["total_area_sqm"] = total_area_sqm
        if land_area_sqm is not None:
            record["land_area_sqm"] = land_area_sqm
        if total_built_up_sqm is not None:
            record["total_built_up_sqm"] = total_built_up_sqm
        if delay_months:
            record["delay_months"] = delay_months

        # ── Geocode ───────────────────────────────────────────────────────────
        if address_raw or pin_code:
            coords = self.geocode(address_raw, pin_code, district)
            if coords:
                lat, lon = coords
                record["location"] = f"SRID=4326;POINT({lon} {lat})"
                record["latitude"] = lat
                record["longitude"] = lon
                logger.debug(f"Geocoded: {lat}, {lon}")
                self.stats["geocoded"] += 1

        # Enrichment-only: downstream detectors own project risk.
        return record

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _parse_date(self, val) -> str | None:
        if not val:
            return None
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(str(val).strip()[:10], fmt).date().isoformat()
            except ValueError:
                continue
        return None

    def _build_address(self, addr: dict) -> str:
        if not addr:
            return ""
        parts = [
            addr.get("addressLine", ""),
            addr.get("street", ""),
            addr.get("locality", ""),
            addr.get("address", ""),
            addr.get("villageName", ""),
            addr.get("talukaName", ""),
            addr.get("districtName", ""),
            str(addr.get("pinCode", "")),
        ]
        return ", ".join(p for p in parts if p and str(p).strip())

    # ── Run ────────────────────────────────────────────────────────────────────

    def run(self, city: str = "Akola"):
        from db.connection import select_rows, update_rows

        city_rows = select_rows("cities", {"name": city})
        if not city_rows:
            logger.error(f"City '{city}' not found in DB")
            return

        city_id = city_rows[0]["id"]
        projects = select_rows(
            "rera_projects", {"city_id": city_id}, limit=1500)

        to_enrich = [p for p in projects if not self._should_skip(p)]
        skipped_smart = len(projects) - len(to_enrich)
        logger.info(
            f"{len(projects)} RERA projects for {city} — "
            f"enriching {len(to_enrich)}, "
            f"skipping {skipped_smart} (completed/lapsed, enriched <30d ago)"
        )

        try:
            self.authenticate()
        except Exception:
            logger.error("Unable to start enrichment without MahaRERA auth")
            return

        total = len(to_enrich)
        try:
            for idx, proj in enumerate(to_enrich, 1):
                raw_data = proj.get("raw_data")
                source_url = proj.get("source_url") or ""
                if not source_url and isinstance(raw_data, dict):
                    source_url = raw_data.get("source_url") or ""
                if not source_url:
                    source_url = raw_data or ""

                id_match = re.search(r"/view/(\d+)", str(source_url))

                if not id_match:
                    registration = str(proj.get("rera_registration") or "")
                    if re.match(r"^P[A-Z]*\d+$", registration):
                        resolved = self.resolve_detail_url(
                            registration,
                            str(proj.get("district") or city),
                        )
                        if resolved:
                            source_url = resolved
                            id_match = re.search(r"/view/(\d+)", resolved)
                            raw_payload = {
                                **(raw_data if isinstance(raw_data, dict) else {"card_text": raw_data}),
                                "source_url": resolved,
                            }
                            update_rows("rera_projects",
                                        filters={"id": str(proj["id"])},
                                        updates={
                                            "raw_data":   raw_payload,
                                            "source_url": resolved,
                                        })

                if not id_match:
                    id_match = re.search(r"/view/(\d+)", str(raw_data or ""))

                if not id_match:
                    self.stats["skipped"] += 1
                    continue

                rera_project_id = int(id_match.group(1))
                db_id = str(proj["id"])
                rera_no = proj.get("rera_registration", "")

                # Build full detail URL for potential Playwright interception
                detail_url = str(source_url or "")
                if detail_url and not detail_url.startswith("http"):
                    detail_url = (
                        "https://maharerait.maharashtra.gov.in" + detail_url
                    )

                # Resolve real API project ID (probe fast path, then intercept)
                url_id = rera_project_id
                rera_project_id = self._resolve_api_project_id(
                    url_id, detail_url)
                if rera_project_id is None:
                    logger.warning(
                        f"Could not resolve API project ID for {rera_no} "
                        f"(URL-ID: {url_id}), skipping"
                    )
                    self.stats["skipped"] += 1
                    continue

                if rera_project_id != url_id:
                    logger.info(
                        f"[{idx}/{total}] Enriching {rera_no} "
                        f"(URL-ID: {url_id} → API-ID: {rera_project_id})"
                    )
                else:
                    logger.info(
                        f"[{idx}/{total}] Enriching {rera_no} "
                        f"(ID: {rera_project_id})"
                    )

                try:
                    enriched = self.enrich_project(db_id, rera_project_id)
                    # Strip empty/zero values so we don't overwrite good data with blanks
                    enriched = {
                        k: v for k, v in enriched.items()
                        if v is not None and v != "" and v != []
                        and not (
                            isinstance(v, (int, float))
                            and v == 0
                            and k not in ZERO_ALLOWED_FIELDS
                        )
                    }

                    if enriched:
                        enriched["updated_at"] = datetime.now(
                            timezone.utc).isoformat()
                        update_rows("rera_projects",
                                    filters={"id": db_id},
                                    updates=enriched)
                        self.stats["updated"] += 1
                        logger.info(
                            f"  [ok] status={enriched.get('rera_status')} "
                            f"units={enriched.get('total_units')} "
                            f"complaints={enriched.get('complaint_count')} "
                            f"pan={'yes' if enriched.get('promoter_pan') else 'no'} "
                            f"cost={'yes' if enriched.get('project_cost') else 'no'} "
                            f"escrow={'yes' if enriched.get('escrow_balance') else 'no'} "
                            f"coords={'yes' if enriched.get('latitude') else 'no'}"
                        )

                except Exception as e:
                    logger.error(
                        f"  [error] Error enriching {rera_no}: {e}",
                        exc_info=True,
                    )
                    self.stats["errors"] += 1

        finally:
            self._close_search_page()

        logger.info(
            f"\nEnrichment complete — "
            f"updated={self.stats['updated']} "
            f"url_skipped={self.stats['skipped']} "
            f"smart_skipped={skipped_smart} "
            f"errors={self.stats['errors']} "
            f"geocoded={self.stats['geocoded']}"
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    scraper = RERADetailScraper()
    scraper.run("Akola")
