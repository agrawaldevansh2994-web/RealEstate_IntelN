"""
scrapers/base.py — Base scraper all others inherit from
"""

import time
import random
import logging
import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Generator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]


class BaseScraper(ABC):

    name: str = "base"
    city: str = "Akola"
    delay_min: float = 1.5
    delay_max: float = 4.0
    max_retries: int = 3

    def __init__(self):
        self.session = self._build_session()
        self.run_id = None
        self.stats = {"fetched": 0, "inserted": 0, "updated": 0, "errors": []}
        self.logger = logging.getLogger(f"scraper.{self.name}")

    def _build_session(self):
        s = requests.Session()
        retry = Retry(
            total=self.max_retries,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        s.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-IN,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        return s

    def _sleep(self):
        t = random.uniform(self.delay_min, self.delay_max)
        self.logger.debug(f"Sleeping {t:.1f}s")
        time.sleep(t)

    def get(self, url, **kwargs):
        self._sleep()
        self.session.headers["User-Agent"] = random.choice(USER_AGENTS)
        resp = self.session.get(url, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp

    def post(self, url, **kwargs):
        self._sleep()
        self.session.headers["User-Agent"] = random.choice(USER_AGENTS)
        resp = self.session.post(url, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp

    def start_run(self):
        from db.connection import insert_row
        row = insert_row("scraper_runs", {
            "scraper_name": self.name,
            "city": self.city,
            "status": "running",
            "config": json.dumps({"delay_min": self.delay_min}),
        })
        self.run_id = row.get("id")
        self.logger.info(
            f"Run #{self.run_id} started — {self.name} / {self.city}")
        return self.run_id

    def finish_run(self, status="success"):
        from db.connection import update_rows
        update_rows(
            "scraper_runs",
            filters={"id": self.run_id},
            updates={
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "status": status,
                "records_fetched": self.stats["fetched"],
                "records_inserted": self.stats["inserted"],
                "records_updated": self.stats["updated"],
                "errors": json.dumps(self.stats["errors"][:50]),
            }
        )
        self.logger.info(
            f"Run #{self.run_id} {status} — "
            f"fetched={self.stats['fetched']} "
            f"inserted={self.stats['inserted']} "
            f"errors={len(self.stats['errors'])}"
        )

    @abstractmethod
    def scrape(self) -> Generator[dict, None, None]:
        ...

    @abstractmethod
    def save(self, record: dict) -> str:
        ...

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
                except Exception as e:
                    self.logger.error(
                        f"Save error: {e} | record={str(record)[:200]}")
                    self.stats["errors"].append(str(e))
        except Exception as e:
            self.logger.exception(f"Fatal scrape error: {e}")
            self.stats["errors"].append(f"FATAL: {e}")
            status = "failed"
        finally:
            self.finish_run(status)
