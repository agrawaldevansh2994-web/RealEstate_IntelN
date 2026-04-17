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
import time
from datetime import datetime

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

    # ── API ────────────────────────────────────────────────────────────────────

    def _post_with_auth(self, url: str, payload: dict):
        self._ensure_token()
        resp = self.session.post(url, json=payload, timeout=30)
        if resp.status_code == 401:
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

    def enrich_project(self, db_id: str, rera_project_id: int) -> dict:
        time.sleep(0.5)

        general = self.call_api("general", rera_project_id)
        status = self.call_api("status", rera_project_id)
        comp_raw = self.call_api("complaints", rera_project_id)
        units_list = self.call_api("units", rera_project_id)
        promoter = self.call_api("promoter", rera_project_id)
        address = self.call_api("address", rera_project_id)
        land_header = self.call_api("land_header", rera_project_id)
        land_cc = self.call_api("land_cc", rera_project_id)
        litigation = self.call_api("litigation", rera_project_id)
        extensions = self.call_api("extensions", rera_project_id)
        cost_estimation = self.call_api("cost_estimation", rera_project_id)
        finance_bank = self.call_api("finance_bank", rera_project_id)
        means_finance = self.call_api("means_finance", rera_project_id)

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

        def _sum_finance_parts(obj: dict | None) -> float | None:
            if not isinstance(obj, dict):
                return None
            secured = _safe_float(
                obj.get("totalBorrowedFundsSecured"),
                allow_zero=True,
            )
            unsecured = _safe_float(
                obj.get("totalBorrowedFundsUnsecured"),
                allow_zero=True,
            )
            values = [v for v in (secured, unsecured) if v is not None]
            if not values:
                return None
            return round(sum(values), 2)

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
        rera_status = (status_obj.get("statusName") or "").lower()
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

        # ── Flags ─────────────────────────────────────────────────────────────
        # escrow deficit flag removed — escrow data not available in public API


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
            "rera_projects", {"city_id": city_id}, limit=500)
        logger.info(f"Enriching {len(projects)} RERA projects for {city}")

        try:
            self.authenticate()
        except Exception:
            logger.error("Unable to start enrichment without MahaRERA auth")
            return

        try:
            for proj in projects:
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
                                        updates={"raw_data": raw_payload})

                if not id_match:
                    id_match = re.search(r"/view/(\d+)", str(raw_data or ""))

                if not id_match:
                    self.stats["skipped"] += 1
                    continue

                rera_project_id = int(id_match.group(1))
                db_id = str(proj["id"])
                rera_no = proj.get("rera_registration", "")

                logger.info(f"Enriching {rera_no} (ID: {rera_project_id})")

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
            f"skipped={self.stats['skipped']} "
            f"errors={self.stats['errors']} "
            f"geocoded={self.stats['geocoded']}"
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    scraper = RERADetailScraper()
    scraper.run("Akola")
