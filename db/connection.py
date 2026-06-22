"""
db/connection.py - Pure HTTP connection to Supabase REST API
Uses only 'requests' library - no supabase package needed
"""

import logging
import os

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()
logger = logging.getLogger(__name__)

CONNECT_TIMEOUT = 10
READ_TIMEOUT = 60
RETRY_TOTAL = 4

_session: requests.Session | None = None


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=RETRY_TOTAL,
        connect=RETRY_TOTAL,
        read=RETRY_TOTAL,
        status=RETRY_TOTAL,
        backoff_factor=1.5,
        status_forcelist=[408, 429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET", "POST", "PATCH", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = _build_session()
    return _session


def _require_supabase_config() -> tuple[str, str]:
    supabase_url = (os.getenv("SUPABASE_URL") or "").rstrip("/")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
        or ""
    )
    if not supabase_url or not supabase_key:
        missing = []
        if not supabase_url:
            missing.append("SUPABASE_URL")
        if not supabase_key:
            missing.append("SUPABASE_SERVICE_KEY")
        raise RuntimeError(
            "Missing Supabase configuration: "
            + ", ".join(missing)
            + ". Configure them via environment or MCP secrets."
        )
    return supabase_url, supabase_key


def _headers() -> dict[str, str]:
    _, supabase_key = _require_supabase_config()
    return {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def _request(method: str, path: str, **kwargs) -> requests.Response:
    supabase_url, _ = _require_supabase_config()
    url = f"{supabase_url}/rest/v1/{path}"
    timeout = kwargs.pop("timeout", (CONNECT_TIMEOUT, READ_TIMEOUT))
    session = _get_session()
    response = session.request(
        method=method,
        url=url,
        headers=_headers(),
        timeout=timeout,
        **kwargs,
    )
    response.raise_for_status()
    return response


def insert_row(table: str, data: dict) -> dict:
    resp = _request("POST", table, json=data)
    result = resp.json()
    return result[0] if result else {}


def upsert_row(table: str, data: dict, on_conflict: str | None = None) -> dict:
    """
    Upsert via PostgREST 'resolution=merge-duplicates'.
    Without on_conflict, PostgREST defaults to the table's primary key for
    conflict detection -- which is wrong for tables (like circle_rates)
    where dedup is on a separate unique index, not the PK. Pass
    on_conflict as a comma-separated column list matching that index,
    e.g. on_conflict="city_id,village,property_type,effective_year".
    Existing callers with just (table, data) are unaffected.
    """
    headers = _headers()
    headers["Prefer"] = "resolution=merge-duplicates,return=representation"
    params = {"on_conflict": on_conflict} if on_conflict else None
    resp = _get_session().request(
        method="POST",
        url=f"{_require_supabase_config()[0]}/rest/v1/{table}",
        headers=headers,
        params=params,
        json=data,
        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
    )
    resp.raise_for_status()
    result = resp.json()
    return result[0] if result else {}


def select_rows(table: str, filters: dict = None, limit: int = 100) -> list:
    params = {"limit": limit, "select": "*"}
    if filters:
        for col, val in filters.items():
            params[col] = f"eq.{val}"
    resp = _request("GET", table, params=params)
    return resp.json()


def update_rows(table: str, filters: dict, updates: dict) -> list:
    params = {}
    for col, val in filters.items():
        params[col] = f"eq.{val}"
    resp = _request("PATCH", table, params=params, json=updates)
    return resp.json()


def count_rows(table: str) -> int:
    headers = _headers()
    headers["Prefer"] = "count=exact"
    headers["Range-Unit"] = "items"
    resp = _get_session().request(
        method="HEAD",
        url=f"{_require_supabase_config()[0]}/rest/v1/{table}",
        headers=headers,
        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
    )
    resp.raise_for_status()
    content_range = resp.headers.get("Content-Range", "0/0")
    return int(content_range.split("/")[-1])
