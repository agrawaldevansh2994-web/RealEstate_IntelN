"""
scrapers/scraper_99acres.py
Extracts listings directly from the SSR HTML page.
No API interception needed - data is in the DOM.

Geocoding:
  - New listings are geocoded by locality at insert time
  - Locality-level cache means same area is only geocoded once per run
  - Uses OSM Nominatim (free, no key) with progressive fallback queries
  - Updates are skipped for geocoding (coordinates do not change)
"""

import json
import logging
import random
import re
import time
from datetime import datetime
from typing import Generator

import requests
from playwright.sync_api import sync_playwright

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

LISTING_TYPE_MAP = {"buy": "sale", "rent": "rent"}
AREA_UNIT_TO_SQFT = {
    "sqft": 1.0,
    "sq ft": 1.0,
    "sq feet": 1.0,
    "square feet": 1.0,
    "sqyd": 9.0,
    "sq yd": 9.0,
    "sq yard": 9.0,
    "sq yards": 9.0,
    "square yard": 9.0,
    "square yards": 9.0,
    "sqm": 10.7639,
    "sq m": 10.7639,
    "sq meter": 10.7639,
    "sq meters": 10.7639,
    "sq metre": 10.7639,
    "sq metres": 10.7639,
}


def _content_match(a, b) -> bool:
    """Numeric-tolerant comparison for price/area/bedrooms dedup.
    None == None; floats within 0.5 are considered identical."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return abs(float(a) - float(b)) < 0.5
    except (TypeError, ValueError):
        return str(a).strip() == str(b).strip()


# Canonical locality name map — normalised (lowercase, stripped) key → Title Case value.
# Handles: spelling variants, BK/Budruk suffix, city-name appended without comma,
# sub-locality → parent locality, common misspellings seen in 99acres data.
# Keys are post-normalisation (lowercase, no city suffix, stripped).
#
# Sections: AKOLA | AMRAVATI | NAGPUR | PUNE | NASHIK | AURANGABAD
# When adding entries: normalise key to lowercase, strip city suffix, collapse spaces.
LOCALITY_ALIASES: dict[str, str] = {

    # ── AKOLA ────────────────────────────────────────────────────────────────

    # Khadki variants
    "khadki bk":                                    "Khadki",
    "khadki budruk":                                "Khadki",
    # Jatharpeth variants
    "jathar peth":                                  "Jatharpeth",
    "jatharpeth":                                   "Jatharpeth",
    # Mothi Umri variants
    "mothi umari":                                  "Mothi Umri",
    "mothi umri":                                   "Mothi Umri",
    "umari pr":                                     "Mothi Umri",
    "umri pragane balapur":                         "Mothi Umri",
    # Lokhande Layout sub-localities → Mothi Umri
    "lokhande layout mothi umari":                  "Mothi Umri",
    "lokhande layout datt mandir mothi umari":      "Mothi Umri",
    "new lokhande layout mothi umari":              "Mothi Umri",
    # Tapadia Nagar variants
    "tapdia nagar":                                 "Tapadia Nagar",
    "new tapdia nagar":                             "Tapadia Nagar",
    "tapadia nagar":                                "Tapadia Nagar",
    # Dabki Road sub-localities → parent
    "gajanan nagar dabaki road":                    "Dabki Road",
    "renuka nagar dabki road":                      "Dabki Road",
    "shriram nagar taplabad leyout dabki road":     "Dabki Road",
    # Malkapur sub-localities
    "yewata road makapur":                          "Malkapur",
    "suyog colony malkapur chowk":                  "Malkapur",
    # Ram Nagar
    "ram nagar aptewadi wadi":                      "Ram Nagar",
    # Hari Har Peth
    "hari har peth":                                "Hariharpeth",
    # Balaji Nagar
    "balaji nagar":                                 "Balaji Nagar",
    # Bhim Nagar
    "new bhim nagar":                               "Bhim Nagar",
    # Old City sub-localities
    "near narhari maharaj temple old city":         "Old City",
    # Akot Road
    "akot fail":                                    "Akot Road",

    # ── AMRAVATI ─────────────────────────────────────────────────────────────

    # Kathora — road suffix and city-appended variants → canonical Kathora
    "kathora road":                                 "Kathora",
    "kathora naka":                                 "Kathora",
    "kathora, amravati":                            "Kathora",
    # Badnera variants
    "badnera, amravati":                            "Badnera",
    "juni basti badnera":                           "Badnera",
    "rameshwar nagar badnera":                      "Badnera",
    # Rahatgaon road → Rahatgaon
    "rahatgaon road":                               "Rahatgaon",
    # Sai Nagar city-appended
    "sai nagar, amravati":                          "Sai Nagar",
    # Near Sardar Patel Nagar → Akoli (sub-locality)
    "near sardar patel nagar.akoli":                "Akoli",  # legacy key (pre dot-fix)
    "near sardar patel nagar":                      "Akoli",  # active key (post dot-fix)
    # Ganesh Colony city-appended
    "new ganesh colony, amravati":                  "New Ganesh Colony",
    # Murtizapur city-appended
    "murtizapur, amravati":                         "Murtizapur",
    # Loni city-appended
    "loni, amravati":                               "Loni",
    # kaza tollgate → outskirts, keep as Amravati Outskirts
    "kaza tollgate, amravati":                      "Amravati Outskirts",
    # Near Parvati Nagar → Parvati Nagar
    "near parvati nagar 3,, amravati":              "Parvati Nagar",
    # Ganediwal Layout Camp → Camp
    "ganediwal layout camp":                        "Camp",
    # Tahsil / Taluka — administrative unit leaking through → drop (return "")
    # Handled via JUNK_LOCALITIES set below rather than alias
    # MIDC Area variants → Amravati Midc
    "midc area":                                    "Amravati Midc",
    # Iim Road → outskirts label
    "iim road":                                     "Amravati Outskirts",
    # Nearby Airport → outskirts
    "nearby airport":                               "Amravati Outskirts",
    # V.M.V. Road → keep as-is (legitimate locality)
    "v.m.v. road":                                  "VMV Road",
    # Chandur and Chandur Bazar — keep separate (different towns)

    # ── NAGPUR ───────────────────────────────────────────────────────────────

    # Manish Nagar / New Manish Nagar — keep separate (distinct areas, both large)
    # Vrindavan sub-locality → Manish Nagar
    "vrindavan -2 manish nagar":                    "Manish Nagar",
    "manish layout":                                "Manish Nagar",
    # Wanadongri variants
    "wanadongri ct":                                "Wanadongri",
    "wanadongari mahajan":                          "Wanadongri",
    # Hudkeshwar Road → Hudkeshwar
    "hudkeshwar road":                              "Hudkeshwar",
    "hudkeshwar kh":                                "Hudkeshwar",
    "hudkeshwar road govind nagar":                 "Hudkeshwar",
    # Besa Pipla Road → Besa
    "besa pipla road":                              "Besa",
    # Kamptee Road → Kamptee
    "kamptee road":                                 "Kamptee",
    # Katol Road → Katol
    "katol road":                                   "Katol",
    # Godhani Road → Godhani
    "godhani road":                                 "Godhani",
    # Jaitala Road → Jaitala
    "jaitala road":                                 "Jaitala",
    # Koradi Naka → Koradi
    "koradi naka":                                  "Koradi",
    # Wardhamna / Waddhamna spelling variant
    "wardhamna":                                    "Waddhamna",
    # Mouza / Mouja prefix → strip to base locality
    "mouza shankarpur":                             "Shankarpur",
    "mouja- isasani":                               "Isasani",
    # Somalwada, Wardha Road → Somalwada
    "somalwada, wardha road":                       "Somalwada",
    # Kalmana / Kalamna spelling variant
    "kalmana":                                      "Kalamna",
    "kalmana market":                               "Kalamna",
    "old kamptee road kalamna":                     "Kalamna",
    # Pewatha / Peotha spelling variant → Peotha
    "pewatha":                                      "Peotha",
    # Narendra Nagar variants
    "new narendra nagar":                           "Narendra Nagar",
    "narendra square":                              "Narendra Nagar",
    # Dighori Square → Dighori
    "dighori square":                               "Dighori",
    # Manewada Square → Manewada
    "manewada square":                              "Manewada",
    # Panjari sub-localities → Panjari
    "panjari farm":                                 "Panjari",
    "panjari lodhi":                                "Panjari",
    # Samta Nagar variants
    "samta nagar nari":                             "Samta Nagar",
    # Near Buti Bori → Buti Bori
    "near buti bori":                               "Buti Bori",
    # Siddhivinayak Society Narsala → Narsala
    "siddhivinayak society narsala":                "Narsala",
    # Zingabai Takli sub-locality
    "zingabai takli bapu society":                  "Zingabai Takli",
    # Sai Nagar 1 → Sai Nagar (small variant)
    "sai nagar 1":                                  "Sai Nagar",
    # Sainagar (no space) → Sai Nagar
    "sainagar":                                     "Sai Nagar",
    # Chinchbhuwan spelling variant → Chinchbhavan
    "chinchbhuwan":                                 "Chinchbhavan",
    # Umred Road → Umred (road is the corridor, not a separate locality at this scale)
    "umred road":                                   "Umred",
    # Degma Kh → likely Degma village, keep
    # Ring Road / Outer Ring Road → generic, keep as-is
    # Junk / non-locality strings handled via JUNK_LOCALITIES below

    # ── PUNE ─────────────────────────────────────────────────────────────────

    # NIBM — acronym; title() would produce "Nibm"
    "nibm":                                        "NIBM",
    # Hinjewadi phase variants → Hinjewadi
    "phase 3 hinjewadi":                           "Hinjewadi",
    "hinjewadi phase 2":                           "Hinjewadi",
    # IT park / township brand → parent locality
    "eon free zone":                               "Kharadi",
    "amanora park town":                           "Hadapsar",
    # Road strings → parent area
    "pashan sus road":                             "Pashan",
    "khed shivapur road":                          "Khed Shivapur",
    "katraj kondhwa road":                         "Katraj",
    "saswad hadapsar road":                        "Hadapsar",
    # Budruk / Khurd sub-locality → parent
    "kondhwa budruk":                              "Kondhwa",
    "manjari khurd":                               "Manjari",
    "manjari budruk":                              "Manjari",
    # Society name → parent locality
    "mohan nagar co-op society":                   "Mohan Nagar",
    # Sub-locality → parent
    "moshi pradhikaran":                           "Moshi",
    "kharadi gaon":                                "Kharadi",
    "marunji village":                             "Marunji",
    "chinchwad gaon":                              "Chinchwad",
    "sector 29 ravet":                             "Ravet",

    # ── NASHIK ───────────────────────────────────────────────────────────────

    # Pathardi sub-locality variants → Pathardi
    "pathardi phata":                              "Pathardi",
    "pathardi shivar":                             "Pathardi",
    "pathardi gaon":                               "Pathardi",
    # Indira Nagar spelling variant
    "indiranagar":                                 "Indira Nagar",
    # Makhmalabad sub-locality variants
    "mankarmala makhmalabad":                      "Makhmalabad",
    "mankar mala":                                 "Makhmalabad",
    # Gangapur road string → Gangapur
    "gangapur road":                               "Gangapur",
    # Road string → parent area
    "dindori road":                                "Dindori",
    # Phata / junction variants → parent area
    "dhatrak phata":                               "Dhatrak",
    "sapte phata":                                 "Sapte",
    "tavli phata":                                 "Tavli",
    # Shivar / Gaon sub-village suffix → parent area
    "wadala shiwar":                               "Wadala",
    "wadala gaon":                                 "Wadala",
    # Ayodhya Nagar spelling variants → consolidated
    "ayodhya nagari":                              "Ayodhya Nagar",
    # Dhruv Nagar spelling variant
    "dhurav nagar":                                "Dhruv Nagar",
    # Dattanagar compound → Datta Nagar
    "dattanagar":                                  "Datta Nagar",
    # Vrindavan variants → Vrindavan Colony
    "vrindavan nagar":                             "Vrindavan Colony",

    # ── AURANGABAD ───────────────────────────────────────────────────────────

    # Garkheda area → Garkheda
    "garkheda area":                               "Garkheda",
    # Satara Parisar variants
    "satara deolai parisar":                       "Satara Parisar",
    "alok nagar deolai":                           "Deolai",
    # CIDCO sector variants → Cidco
    "cidco waluj mahanagar 1":                     "Cidco",
    "cidco waluj mahanagar 2":                     "Cidco",
    "n 2 cidco":                                   "Cidco",
    "n 6 cidco":                                   "Cidco",
    "n 7 cidco":                                   "Cidco",
    # Shendra variants → Shendra Midc
    "shendra":                                     "Shendra Midc",
    "shendra kamangar":                            "Shendra Midc",
    # Paithan Gate → Paithan Road corridor
    "paithan gate":                                "Paithan Road",
    # Dot-separator (pre-resolved in step 4); alias retained as fallback
    "peshve nagar.satara parisar":                 "Satara Parisar",
    "peshve nagar":                                "Satara Parisar",
    # Padegao sub-locality → Padegaon
    "padegao sainagar":                            "Padegaon",
    "sainagar padegao":                            "Padegaon",
    # Kokanwadi spelling → Konkanwadi
    "kokanwadi":                                   "Konkanwadi",
}

# Strings that are NOT locality names — administrative units, landmarks, generics.
# If _canonicalize_locality resolves to one of these, return "" (reject).
JUNK_LOCALITIES: set[str] = {
    "tahsil", "taluka", "land", "flat no", "plot no", "home",
    "mar", "hom", "university", "vyom", "yashoda",
    "behind golden leaf", "near balaji fabrication",
    "near gadgadeshwar mandir", "near narayan",
    "gadgadeshwar temple", "jaistambh chowk",
    "opposite nci", "opposite ycce college",
    "padole hospital square", "backside insara metro park",
    "near shani", "one acer land", "ram cooler",
    "gangotri resort", "dera green 1 yerla",
    "rachana nakshatra ashwini", "oasis vatika sirul",
    "anishka palace", "audhmber apartment",
    "chintamani nagari-2", "jaidurga housing society",
    "jai durga society 3", "shrikrishna apartment",
    "vishakha society nirmal nagar",
    "wcl colony, godhni railway",
    "banke bihari apartment", "apartment kamal",
    "sankalp heights", "rachana residency",
    "chirag city", "radha city", "shreeji park",
    "govind krushna sankul", "raghumohan residency rajhill nagar",
    "amravati venture", "amravati 444601",
    "v residency, amravati", "dahlia block",
    "ekvira vidyut colony", "vidyut nagar om colony",
    "matoshri ramabai ambedkar nagar",
    "guntur perecharla",   # wrong state entirely
    # ── Phase 2 additions ────────────────────────────────────────────────────
    # Aurangabad: landmarks, housing complex names, generic sector codes
    "near bharat gas godown",
    "behind mit college",
    "near skoda material gate",
    "mhada",
    "town center",
    "palm exotica chs",
    "a10",
    "sambhaji nagar",              # city's alternate name leaking as locality
    "sambhaji nagar gadhejal gaon",
    "bangalore ramnagar",          # wrong state (Karnataka)
    "sneha sadan",
    "shivkrupa apartment",
    "patel peace home",
    "kasliwal tarangan",
    "sai vyankateshwar",
    "midc",                        # too generic without city context; city-specific variants in aliases
    # Nashik: landmarks, institution names, road strings, project names
    "jatra hotel",
    "nandurkar hospital",
    "rto corner",
    "behind omsai industrial",
    "adgoan nashik jatra hotel",
    "serene meadows",
    "the aadress",
    "2a shrisharan society ramwadi",
    "shrisharan slociety ramwadi panchvati",
    "sai kutir apartment jailroad",
    "nashik pune road",
    "aurangabad road",
    "patidar bhavan road",
    # Pune: project names, garbled address fragments
    "hills and dales",
    "pune satra wai",
}

# Localities that are actually OTHER city names — reject entirely
KNOWN_OTHER_CITIES: set[str] = {
    "nashik", "pune", "mumbai", "nagpur", "aurangabad",
    "solapur", "kolhapur", "thane", "navi mumbai",
    "wardha", "yavatmal", "washim", "buldhana",
    # Nagpur-adjacent towns that leak into Nagpur scrape
    "ambernath", "paithan",
    # Phase 2 cross-city artefacts
    "ahmed nagar",     # Nashik scrape: Ahmednagar district HQ
    "shirdi",          # Nashik scrape: Ahmednagar district pilgrimage town
    "pandharpur",      # Aurangabad scrape: Solapur district
    "wai",             # Pune scrape: Satara district town
    # Generic non-place strings
    "land", "flat no", "plot no",
}

# Urban area threshold: plots larger than this are agricultural/rural land.
# Their price_per_sqft (e.g. ₹6/sqft on 20-acre agricultural land) is not
# comparable to urban residential pricing and should be nulled out.
MAX_URBAN_AREA_SQFT = 20_000  # ~1,850 sqm

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "MahaRERA-IntelPlatform/1.0 (research)"}


class Scraper99Acres(BaseScraper):
    name = "99acres"
    city = "Akola"
    delay_min = 1.0
    delay_max = 2.0

    # Cities that need gentler scraping (high listing volume → CAPTCHA risk)
    _HIGH_VOLUME_CITIES = {"nagpur", "pune", "nashik", "aurangabad"}

    def __init__(self, city="Akola", listing_types=None, max_pages=None):
        super().__init__()
        self.city = city
        city_lower = city.lower()
        is_high_volume = city_lower in self._HIGH_VOLUME_CITIES

        # City-aware defaults:
        # Nagpur/Pune have 7000+ listings — cap pages to avoid reCAPTCHA Enterprise quota.
        # Rent listings are irrelevant for fraud detection and the rent URL is
        # frequently broken for smaller cities (417 errors).
        if max_pages is None:
            self.max_pages = 15 if is_high_volume else 40
        else:
            self.max_pages = max_pages

        if listing_types is None:
            # Rent excluded for all cities — rent URL pattern is broken/unreliable
            # and rent listings are irrelevant for fraud detection.
            self.listing_types = ["buy"]
        else:
            self.listing_types = listing_types

        # Inter-page delay: longer for high-volume cities to stay under radar
        self.delay_min = 10.0 if is_high_volume else 4.0
        self.delay_max = 18.0 if is_high_volume else 8.0

        self._seen_listing_ids: dict[str, set[str]] = {
            LISTING_TYPE_MAP.get(listing_type, listing_type): set()
            for listing_type in self.listing_types
        }

        city_slug = city.lower().replace(" ", "-")
        self.search_urls = {
            "buy": f"https://www.99acres.com/resale-property-in-{city_slug}-ffid",
            "rent": f"https://www.99acres.com/rent-property-in-{city_slug}-ffid",
        }

        self._geocode_cache: dict[str, tuple[float, float] | None] = {}
        self._last_geocode_time = 0.0

    @staticmethod
    def _normalize_area_unit(unit: str) -> str:
        cleaned = re.sub(r"[^a-z\s]", " ", str(unit or "").lower())
        cleaned = cleaned.replace("square", "sq")
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned

    @classmethod
    def _parse_area_line(cls, line: str) -> tuple[float | None, str | None, float | None]:
        match = re.search(
            r"([\d,]+(?:\.\d+)?)\s*(sq(?:\s+|\.?)(?:ft|feet|yd|yard|yards|m|meter|meters|metre|metres)|sqft|sqyd|sqm)\b",
            str(line or ""),
            flags=re.I,
        )
        if not match:
            return None, None, None

        raw_value = float(match.group(1).replace(",", ""))
        raw_unit = cls._normalize_area_unit(match.group(2))
        factor = AREA_UNIT_TO_SQFT.get(raw_unit)
        if factor is None:
            return raw_value, raw_unit, None
        return raw_value, raw_unit, round(raw_value * factor, 2)

    @classmethod
    def _parse_area_from_url(cls, url: str) -> tuple[float | None, str | None, float | None]:
        match = re.search(
            r"-(\d[\d,]*(?:\.\d+)?)-sq-(ft|feet|yard|yards|yd|m|meter|meters|metre|metres)\b",
            str(url or "").lower(),
        )
        if not match:
            return None, None, None

        raw_value = float(match.group(1).replace(",", ""))
        raw_unit = cls._normalize_area_unit(f"sq {match.group(2)}")
        factor = AREA_UNIT_TO_SQFT.get(raw_unit)
        if factor is None:
            return raw_value, raw_unit, None
        return raw_value, raw_unit, round(raw_value * factor, 2)

    @classmethod
    def _extract_area_sqft(
        cls,
        lines: list[str],
        url: str,
    ) -> tuple[float | None, str | None, float | None]:
        for line in lines or []:
            raw_value, raw_unit, area_sqft = cls._parse_area_line(line)
            if raw_value is not None:
                return raw_value, raw_unit, area_sqft
        return cls._parse_area_from_url(url)

    @classmethod
    def _extract_price_per_sqft(
        cls,
        lines: list[str],
        listed_price: float | None,
        area_sqft: float | None,
    ) -> tuple[float | None, str | None]:
        for line in lines or []:
            match = re.search(
                r"₹\s*([\d,]+(?:\.\d+)?)\s*/\s*(sq(?:\s+|\.?)(?:ft|feet|yd|yard|yards|m|meter|meters|metre|metres)|sqft|sqyd|sqm)\b",
                str(line or ""),
                flags=re.I,
            )
            if not match:
                continue

            raw_rate = float(match.group(1).replace(",", ""))
            raw_unit = cls._normalize_area_unit(match.group(2))
            factor = AREA_UNIT_TO_SQFT.get(raw_unit)
            if factor is None:
                return None, raw_unit
            return round(raw_rate / factor, 2), raw_unit

        if listed_price and area_sqft and area_sqft > 0:
            return round(float(listed_price) / float(area_sqft), 2), "derived"
        return None, None

    @classmethod
    def _canonicalize_locality(cls, locality: str, city: str) -> str:
        """
        Normalize a raw locality string into a clean, canonical form.

        Steps applied in order:
        1. Strip the city name from the END (with or without comma).
           Handles "Dabki Road Akola", "Jathar Peth Akola", "Dabki Road, Akola".
        2. Strip slashes, brackets, and collapse whitespace.
        3. Strip the BK / Budruk suffix (village subdivision marker).
        4. If the result has a comma, take only the FIRST part.
           "Gajanan Nagar,Dabaki Road" → "Gajanan Nagar"
           "Kirti Nagar , Near Rto" → "Kirti Nagar"
        5. If the result has more than 5 words it is almost certainly a full
           address rather than a locality name. Apply LOCALITY_ALIASES first;
           if no alias matches, fall back to the first 2 words of the string.
        6. Reject strings that are themselves a known city name (wrong-city data).
        7. Look up in LOCALITY_ALIASES for known spelling variants.
        8. Title-case the final result.
        """
        value = str(locality or "").strip()
        city_name = str(city or "").strip()
        if not value:
            return ""

        # Step 1a: strip ", CityName" from end (with comma)
        value = re.sub(
            rf",\s*{re.escape(city_name)}\s*$",
            "",
            value,
            flags=re.I,
        )
        # Step 1b: strip " CityName" from end WITHOUT comma
        # Catches "Dabki Road Akola", "Jathar Peth Akola", "Balaji Nagar Akola"
        value = re.sub(
            rf"\s+{re.escape(city_name)}\s*$",
            "",
            value,
            flags=re.I,
        )

        # Step 2: clean punctuation
        value = value.replace("/", " ")
        value = re.sub(r"[()]", " ", value)
        value = re.sub(r"\s+", " ", value).strip(" ,.-")

        # Step 3: strip BK / Budruk suffix
        normalized = value.lower()
        normalized = re.sub(r"\b(bk|bk\.)\b$", "", normalized).strip(" ,.-")
        normalized = re.sub(r"\s+", " ", normalized).strip()

        if not normalized:
            return ""

        # Step 4: if comma or dot-separator present, take FIRST part only.
        # Replace dot used as word separator (e.g. "Peshve Nagar.Satara Parisar")
        # before splitting. Guard: only replace dots where BOTH sides have 2+
        # consecutive chars — avoids breaking abbreviations like "V.M.V. Road".
        normalized = re.sub(r"(?<=[a-z]{2})\.(?=[a-z])", ",", normalized)
        if "," in normalized:
            normalized = normalized.split(",")[0].strip(" .-")

        # Step 5: long string guard (full address leaked through)
        words = normalized.split()
        if len(words) >= 5:
            # Try alias table first on the full string
            if normalized in LOCALITY_ALIASES:
                return LOCALITY_ALIASES[normalized]
            # Try progressively shorter substrings from the start
            for n in range(min(len(words), 4), 1, -1):
                candidate = " ".join(words[:n])
                if candidate in LOCALITY_ALIASES:
                    return LOCALITY_ALIASES[candidate]
            # Last resort: take first 2 words
            normalized = " ".join(words[:2])

        # Step 6: reject if the result IS a city name (wrong-city scrape artefact)
        if normalized in KNOWN_OTHER_CITIES:
            return ""

        # Step 6b: reject administrative units, landmarks, and generic junk strings
        if normalized in JUNK_LOCALITIES:
            return ""

        # Step 7: alias lookup
        normalized = LOCALITY_ALIASES.get(normalized, normalized)

        # Step 7b: after alias resolution, check junk again (alias may resolve to "")
        if not normalized or normalized.lower() in JUNK_LOCALITIES:
            return ""

        # Step 8: title-case
        if not normalized:
            return ""
        return normalized.title() if isinstance(normalized, str) and not normalized[0].isupper() else normalized

    @staticmethod
    def _looks_like_project_name(locality: str) -> bool:
        value = str(locality or "").strip().lower()
        if not value:
            return False

        project_markers = (
            "apartment",
            "appartment",
            "residency",
            "residence",
            "plaza",
            "park",
            "heights",
            "height",
            "enclave",
            "villa",
            "villas",
            "duplex",
            "tower",
            "towers",
            "county",
            "palace",
            "meadows",   # project name suffix seen in Nashik (e.g. "Serene Meadows")
            "exotica",   # project name suffix seen in Aurangabad (e.g. "Palm Exotica CHS")
            "chs",       # co-operative housing society — not a locality name
        )
        return any(marker in value for marker in project_markers)

    @staticmethod
    def _clean_title_locality(candidate: str) -> str:
        value = str(candidate or "").strip(" ,-")
        value = re.sub(r"\s+", " ", value)
        return value.strip(" ,-")

    @classmethod
    def _extract_locality_from_title(cls, title: str, city: str) -> str:
        text = str(title or "").strip()
        city_name = str(city or "").strip()
        if not text:
            return ""

        match = re.search(r"\bin\s+(.+)$", text, flags=re.I)
        if not match:
            return ""

        candidate = cls._clean_title_locality(match.group(1))
        if not candidate:
            return ""

        if city_name and re.fullmatch(re.escape(city_name), candidate, flags=re.I):
            return ""

        candidate_parts = [part.strip()
                           for part in candidate.split(",") if part.strip()]
        if len(candidate_parts) == 1 and city_name and candidate_parts[0].lower() == city_name.lower():
            return ""

        return candidate

    @classmethod
    def _resolve_locality(cls, locality_raw: str, title: str, city: str) -> tuple[str, str]:
        title_locality_raw = cls._extract_locality_from_title(title, city)
        title_locality = cls._canonicalize_locality(title_locality_raw, city)
        raw_locality = cls._canonicalize_locality(locality_raw, city)

        if title_locality:
            return title_locality, title_locality_raw

        if raw_locality:
            return raw_locality, locality_raw

        if cls._looks_like_project_name(locality_raw) and title_locality_raw:
            return title_locality, title_locality_raw

        return "", locality_raw

    def _geocode(self, locality: str) -> tuple[float, float] | None:
        if not locality:
            return None

        if locality in self._geocode_cache:
            return self._geocode_cache[locality]

        parts = [p.strip() for p in locality.split(",") if p and p.strip()]
        if not any(p.lower() == self.city.lower() for p in parts):
            parts.append(self.city)

        queries = []
        for i in range(len(parts)):
            suffix = ", ".join(parts[i:])
            query = f"{suffix}, Maharashtra, India"
            if query not in queries:
                queries.append(query)

        result = None
        for query in queries:
            elapsed = time.time() - self._last_geocode_time
            if elapsed < 1.1:
                time.sleep(1.1 - elapsed)

            try:
                resp = requests.get(
                    NOMINATIM_URL,
                    params={
                        "q": query,
                        "format": "json",
                        "limit": 1,
                        "countrycodes": "in",
                    },
                    headers=NOMINATIM_HEADERS,
                    timeout=10,
                )
                self._last_geocode_time = time.time()
                data = resp.json()
                if data:
                    result = (float(data[0]["lat"]), float(data[0]["lon"]))
                    logger.debug(
                        f"Geocoded '{locality}' via '{query}' -> {result}")
                    break
            except Exception as exc:
                logger.debug(f"Nominatim error for '{query}': {exc}")

        if result is None:
            logger.debug(
                f"Could not geocode '{locality}' - all queries exhausted")

        self._geocode_cache[locality] = result
        return result

    def _navigate_and_extract(self, page, nav_url: str, listing_type: str, page_num: int) -> list:
        for attempt in range(1, 3):
            try:
                page.goto(nav_url, wait_until="domcontentloaded", timeout=45000)
            except Exception as exc:
                self.logger.warning(f"Load warning: {exc}")
                if page.is_closed():
                    return []

            try:
                page.wait_for_function(
                    """() => {
                        return document.querySelectorAll('a[href*="spid-"]').length > 0 ||
                               document.querySelectorAll('a[href*="npxid-"]').length > 0 ||
                               /results\\s*\\|/i.test(document.body?.innerText || '');
                    }""",
                    timeout=12000,
                )
            except Exception:
                pass

            page.wait_for_timeout(5500 if page_num == 1 else 2500)
            if page.is_closed():
                return []

            listings = self._extract_from_dom(page, listing_type)
            if listings:
                return listings

            if attempt == 1:
                self.logger.info(
                    f"Retrying page {page_num} for {self.city}/{listing_type} after empty extraction"
                )
                page.wait_for_timeout(4500)

        return []

    def scrape(self) -> Generator[dict, None, None]:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = browser.new_context(
                user_agent=(
                    # Keep in sync with current Chrome stable.
                    # An outdated UA string is a bot signal on sites that check it.
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 768},
                locale="en-IN",
            )
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            page = context.new_page()

            for listing_type in self.listing_types:
                self.logger.info(
                    f"Scraping {listing_type} listings for {self.city}")
                url = self.search_urls.get(
                    listing_type, self.search_urls["buy"])
                page_num = 1

                while page_num <= self.max_pages:
                    nav_url = url if page_num == 1 else f"{url}?page={page_num}"
                    self.logger.info(f"Loading page {page_num}: {nav_url}")
                    listings = self._navigate_and_extract(
                        page,
                        nav_url,
                        listing_type,
                        page_num,
                    )

                    if not listings:
                        self.logger.info(
                            f"No listings on page {page_num}, stopping.")
                        break

                    self.logger.info(
                        f"Page {page_num} - {len(listings)} listings extracted"
                    )
                    for listing in listings:
                        yield listing

                    page_num += 1

                    # Inter-page delay — randomised to appear human.
                    # High-volume cities (Nagpur) get a longer pause to stay
                    # under the reCAPTCHA Enterprise request quota on 99acres.
                    delay = random.uniform(self.delay_min, self.delay_max)
                    self.logger.debug(f"Inter-page delay: {delay:.1f}s")
                    time.sleep(delay)

            browser.close()

    def _extract_from_dom(self, page, listing_type) -> list:
        try:
            listings = page.evaluate("""
                () => {
                    const results = [];
                    const cards = document.querySelectorAll('a[href*="spid-"]');

                    cards.forEach(link => {
                        try {
                            let card = link;
                            for (let i = 0; i < 10; i++) {
                                card = card.parentElement;
                                if (!card) break;
                                const text = card.innerText || '';
                                if (text.includes('/sqft') && text.includes('sqft')) break;
                            }
                            if (!card) return;

                            const cardText = card.innerText || '';
                            const lines = cardText.split('\\n').map(l => l.trim()).filter(l => l);
                            const url = link.href || '';
                            const spidMatch = url.match(/spid-([A-Z0-9]+)/i);
                            const spid = spidMatch ? spidMatch[1] : '';
                            const title = link.innerText.trim();

                            let price = null;
                            for (const line of lines) {
                                if (line.startsWith('₹') && !line.includes('/sqft')) {
                                    const p = line.replace('₹', '').replace(',', '').trim();
                                    if (p.includes('Cr')) price = parseFloat(p) * 10000000;
                                    else if (p.includes('Lac')) price = parseFloat(p) * 100000;
                                    else price = parseFloat(p) || null;
                                }
                            }

                            let bedrooms = null;
                            for (const line of lines) {
                                const m = line.match(/(\\d+)\\s*BHK/i);
                                if (m) { bedrooms = parseInt(m[1]); break; }
                                if (line.includes('1 RK')) { bedrooms = 1; break; }
                            }

                            const locality = lines[0] || '';

                            let propertyType = 'flat';
                            const tl = title.toLowerCase();
                            if (tl.includes('plot') || tl.includes('land')) propertyType = 'plot';
                            else if (tl.includes('house') || tl.includes('villa')) propertyType = 'house_villa';

                            let listedBy = 'owner';
                            if (cardText.includes('Dealer')) listedBy = 'broker';
                            if (cardText.includes('Builder')) listedBy = 'builder';

                            results.push({
                                spid, title, url, price, bedrooms,
                                locality, propertyType, listedBy, lines,
                            });
                        } catch (e) {}
                    });
                    return results;
                }
            """)

            normalized = []
            for record in listings or []:
                if not record.get("spid"):
                    continue

                lines = record.get("lines") or []
                locality_raw = (record.get("locality") or "").strip()
                locality, locality_source_raw = self._resolve_locality(
                    locality_raw,
                    record.get("title", ""),
                    self.city,
                )

                # Reject listings where locality resolved to a different city name.
                # This happens when 99acres returns results for nearby cities.
                if locality.lower() in KNOWN_OTHER_CITIES:
                    self.logger.debug(
                        f"Skipping listing in wrong city: locality='{locality}' spid={record.get('spid')}"
                    )
                    continue

                area_value_raw, area_unit_raw, area_sqft = self._extract_area_sqft(
                    lines,
                    record.get("url", ""),
                )

                # Null impossible flat/house_villa areas — URL-parsing artefacts.
                # A flat at 348,000 sqft cannot exist. These come from sq-yard URL
                # slugs that inflate the sqft value (×9). area_value_raw is None
                # when the area came from the URL rather than the card text.
                property_type = record.get("propertyType", "flat")
                if (
                    area_sqft is not None
                    and area_sqft > 10_000
                    and property_type in ("flat", "house_villa")
                    and area_value_raw is None
                ):
                    self.logger.debug(
                        f"Nulling impossible {property_type} area: "
                        f"{area_sqft:.0f}sqft spid={record.get('spid')}"
                    )
                    area_sqft = None
                    area_value_raw = None
                    area_unit_raw = None
                # Null impossibly small areas for flat/house_villa.
                # Caused by sq-m values stored raw without unit conversion
                # (e.g. 9 sq m → stored as 9 sqft instead of 96.9 sqft).
                # A real flat cannot be under 50 sqft regardless of source.
                if (
                    area_sqft is not None
                    and area_sqft < 50
                    and property_type in ("flat", "house_villa")
                ):
                    self.logger.debug(
                        f"Nulling impossibly small {property_type} area: "
                        f"{area_sqft:.1f}sqft spid={record.get('spid')} "
                        f"— likely sq-m stored without conversion"
                    )
                    area_sqft = None
                    area_value_raw = None
                    area_unit_raw = None

                price_per_sqft, price_unit_raw = self._extract_price_per_sqft(
                    lines,
                    record.get("price"),
                    area_sqft,
                )

                # Null out price_per_sqft for agricultural/rural plots.
                # These have huge areas (>20,000 sqft / ~0.46 acres) and very low
                # per-sqft prices (₹6–500/sqft) that are not comparable to urban
                # residential pricing — including them corrupts locality medians
                # and triggers false outlier flags.
                if (
                    area_sqft is not None
                    and area_sqft > MAX_URBAN_AREA_SQFT
                    and price_per_sqft is not None
                    and price_per_sqft < 500
                ):
                    self.logger.debug(
                        f"Nulling rural price_per_sqft: area={area_sqft:.0f}sqft "
                        f"price={price_per_sqft}/sqft spid={record.get('spid')}"
                    )
                    price_per_sqft = None

                record["localityRaw"] = locality_raw
                record["localitySourceRaw"] = locality_source_raw
                record["localityNormalized"] = locality
                record["areaValueRaw"] = area_value_raw
                record["areaUnitRaw"] = area_unit_raw
                record["areaSqftNormalized"] = area_sqft
                record["pricePerSqftNormalized"] = price_per_sqft
                record["priceUnitRaw"] = price_unit_raw

                normalized.append({
                    "source": "99acres",
                    "source_listing_id": record["spid"],
                    "source_url": record.get("url", ""),
                    "city": self.city,
                    "listing_type": LISTING_TYPE_MAP.get(listing_type, listing_type),
                    "listing_status": "active",
                    "property_type": property_type,
                    "listed_price": record.get("price"),
                    "price_per_sqft": price_per_sqft,
                    "area_sqft": area_sqft,
                    "bedrooms": record.get("bedrooms"),
                    "locality": locality or locality_raw,
                    "address_raw": record.get("title", ""),
                    "listed_by": record.get("listedBy", "owner"),
                    "raw_data": json.dumps(record),
                    "scraped_at": datetime.utcnow().isoformat(),
                })
            return normalized

        except Exception as exc:
            self.logger.error(f"DOM extraction error: {exc}")
            return []

    def _retire_unseen_listings(self) -> None:
        from db.connection import select_rows, update_rows

        cities = select_rows("cities", {"name": self.city}, limit=5)
        if not cities:
            self.logger.warning(
                f"Could not resolve city_id for {self.city}; skipping stale listing retirement"
            )
            return

        city_id = cities[0]["id"]

        for listing_type, seen_ids in self._seen_listing_ids.items():
            active_rows = select_rows(
                "listings",
                filters={
                    "city_id": city_id,
                    "source": self.name,
                    "listing_type": listing_type,
                    "listing_status": "active",
                },
                limit=5000,
            )

            active_count = len(active_rows)
            seen_count = len(seen_ids)

            if active_count == 0 or seen_count == 0:
                self.logger.info(
                    f"Skipping stale cleanup for {self.city}/{listing_type}: "
                    f"active={active_count}, seen={seen_count}"
                )
                continue

            coverage = seen_count / active_count
            if coverage < 0.75:
                self.logger.warning(
                    f"Skipping stale cleanup for {self.city}/{listing_type}: "
                    f"coverage too low ({seen_count}/{active_count} = {coverage:.0%})"
                )
                continue

            retired = 0
            for row in active_rows:
                if row.get("source_listing_id") in seen_ids:
                    continue
                update_rows(
                    "listings",
                    filters={"id": row["id"]},
                    updates={"listing_status": "inactive"},
                )
                retired += 1

            self.logger.info(
                f"Retired {retired} stale {listing_type} listings for {self.city}"
            )

    def save(self, record) -> str:
        from db.connection import insert_row, select_rows, update_rows

        table = "listings"
        self._seen_listing_ids.setdefault(record["listing_type"], set()).add(
            record["source_listing_id"]
        )

        cities = select_rows("cities", {"name": record["city"]})
        city_id = cities[0]["id"] if cities else None

        zone_id = None
        if record.get("locality") and city_id:
            zones = select_rows("zones", {"city_id": city_id})
            for zone in zones:
                if record["locality"].lower() in zone["name"].lower():
                    zone_id = zone["id"]
                    break

        existing = select_rows(
            table,
            {
                "source": "99acres",
                "source_listing_id": record["source_listing_id"],
            },
        )

        if existing:
            updates = {
                "listed_price": record["listed_price"],
                "price_per_sqft": record["price_per_sqft"],
                "area_sqft": record["area_sqft"],
                "listing_status": record["listing_status"],
                "last_seen_at": record["scraped_at"],
                "property_type": record["property_type"],
                "bedrooms": record["bedrooms"],
                "locality": record["locality"],
                "address_raw": record["address_raw"],
                "listed_by": record["listed_by"],
                "raw_data": record["raw_data"],
            }
            if zone_id is not None:
                updates["zone_id"] = zone_id

            update_rows(
                table,
                filters={"id": existing[0]["id"]},
                updates=updates,
            )
            return "updated"

        lat, lon = None, None
        locality = (record.get("locality") or "").strip()

        # ── Content-hash dedup ────────────────────────────────────────────────
        # 99acres sometimes re-issues the same listing with a new spid across
        # scrape runs. Guard: if an active listing with identical
        # (city_id, locality, property_type, listed_price, area_sqft, bedrooms)
        # already exists, skip the insert and keep the original's spid alive
        # so it isn't retired by _retire_unseen_listings.
        if locality and record.get("listed_price"):
            locality_candidates = select_rows(
                table,
                {
                    "city_id":        city_id,
                    "locality":       record["locality"],
                    "property_type":  record["property_type"],
                    "listing_status": "active",
                },
                limit=30,
            )
            for match in locality_candidates:
                if (
                    _content_match(match.get("listed_price"), record.get("listed_price"))
                    and _content_match(match.get("area_sqft"),    record.get("area_sqft"))
                    and _content_match(match.get("bedrooms"),     record.get("bedrooms"))
                ):
                    # Keep the original listing alive in the seen-set
                    self._seen_listing_ids.setdefault(
                        record["listing_type"], set()
                    ).add(match.get("source_listing_id"))
                    logger.debug(
                        f"Content-hash dedup: skipping {record['source_listing_id']}"
                        f" — matches listing {match['id']} ({locality})"
                    )
                    return "duplicate"
        # ─────────────────────────────────────────────────────────────────────

        if locality:
            coords = self._geocode(locality)
            if coords:
                lat, lon = coords

        row = {
            "city_id": city_id,
            "zone_id": zone_id,
            "source": "99acres",
            "source_listing_id": record["source_listing_id"],
            "source_url": record["source_url"],
            "listing_type": record["listing_type"],
            "listing_status": record["listing_status"],
            "last_seen_at": record["scraped_at"],
            "property_type": record["property_type"],
            "listed_price": record["listed_price"],
            "price_per_sqft": record["price_per_sqft"],
            "area_sqft": record["area_sqft"],
            "bedrooms": record["bedrooms"],
            "locality": record["locality"],
            "address_raw": record["address_raw"],
            "listed_by": record["listed_by"],
            "raw_data": record["raw_data"],
            "latitude": lat,
            "longitude": lon,
        }
        row = {key: value for key, value in row.items() if value is not None}
        insert_row(table, row)
        return "inserted"

    def _quality_report(self) -> None:
        """
        Post-run quality check logged at INFO level.
        Queries the DB for the current city and logs counts of:
          - listings with low price_per_sqft (< 500) — possible bad data
          - listings with null area_sqft — area not parsed
          - listings with huge area (> 20,000 sqft) — agricultural/rural plots
          - listings with null price_per_sqft — price not parsed
          - duplicate source_listing_ids — dedup failures
          - new suspicious_flags created today — from anomaly/pattern detectors
        Printed after every run so issues are visible without querying Supabase.
        """
        from db.connection import select_rows
        try:
            cities = select_rows("cities", {"name": self.city}, limit=5)
            if not cities:
                return
            city_id = cities[0]["id"]

            rows = select_rows(
                "listings",
                filters={"city_id": city_id, "source": self.name,
                         "listing_status": "active"},
                limit=5000,
            )
            if not rows:
                return

            low_price = sum(1 for r in rows if r.get(
                "price_per_sqft") and float(r["price_per_sqft"]) < 500)
            null_area = sum(1 for r in rows if r.get("area_sqft") is None)
            huge_area = sum(1 for r in rows if r.get("area_sqft")
                            and float(r["area_sqft"]) > 20_000)
            null_psqft = sum(1 for r in rows if r.get(
                "price_per_sqft") is None)
            spids = [r["source_listing_id"]
                     for r in rows if r.get("source_listing_id")]
            dupes = len(spids) - len(set(spids))

            self.logger.info(
                f"── Quality report [{self.city}] ─────────────────\n"
                f"  Active listings      : {len(rows)}\n"
                f"  Null area_sqft       : {null_area}\n"
                f"  Null price_per_sqft  : {null_psqft}\n"
                f"  Low psqft (<500)     : {low_price}  ← check if agricultural\n"
                f"  Huge area (>20k sqft): {huge_area}  ← agricultural/rural plots\n"
                f"  Duplicate spids      : {dupes}      ← should be 0\n"
                f"────────────────────────────────────────────────"
            )
        except Exception as exc:
            self.logger.warning(f"Quality report failed: {exc}")

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
                except Exception as exc:
                    self.logger.error(
                        f"Save error: {exc} | record={str(record)[:200]}")
                    self.stats["errors"].append(str(exc))

            if not self.stats["errors"]:
                self._retire_unseen_listings()
        except Exception as exc:
            self.logger.exception(f"Fatal scrape error: {exc}")
            self.stats["errors"].append(f"FATAL: {exc}")
            status = "failed"
        finally:
            self._quality_report()
            self.finish_run(status)
