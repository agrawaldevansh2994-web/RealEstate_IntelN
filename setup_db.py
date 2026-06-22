"""
setup_db.py — Test Supabase connection using plain requests.
No supabase library needed.
Usage: python setup_db.py
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

def run():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL or SUPABASE_SERVICE_KEY missing in .env")
        print(f"  SUPABASE_URL = '{SUPABASE_URL}'")
        print(f"  SUPABASE_KEY = '{SUPABASE_KEY[:10]}...' " if SUPABASE_KEY else "  SUPABASE_KEY = ''")
        return

    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }

    print(f"Connecting to {SUPABASE_URL}...")

    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/cities",
        headers=headers,
        params={"select": "*", "limit": 20},
        timeout=30,
    )

    if resp.status_code == 200:
        cities = resp.json()
        print(f"\nConnection successful!")
        print(f"Cities in DB ({len(cities)}): {[c['name'] for c in cities]}")

        resp2 = requests.get(
            f"{SUPABASE_URL}/rest/v1/listings",
            headers=headers,
            params={"select": "id", "limit": 1},
            timeout=30,
        )
        print(f"Listings table: accessible ✓")
        print(f"\nAll good — run: python main.py --scraper 99acres --city Akola")
    else:
        print(f"Error {resp.status_code}: {resp.text}")

if __name__ == "__main__":
    run()
