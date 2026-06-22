"""
test_api.py — Hit the real 99acres API and print the JSON structure.
Run this BEFORE the full scraper to confirm field names.

Usage: python test_api.py
"""

import json
import requests
import sys

API_URL = "https://www.99acres.com/api-aggregator/srp/search"

PARAMS = {
    "city":                  417,        # Akola — confirmed
    "res_com":               "R",
    "preference":            "S",        # S = Sale
    "page":                  1,
    "page_size":             5,          # just 5 for testing
    "platform":              "DESKTOP",
    "moduleName":            "GRAILS_SRP",
    "workflow":              "GRAILS_SRP",
    "search_type":           "QS",
    "reraType":              "RERA",
    "rera_type":             "RERA",
    "seoUrlType":            "DEFAULT",
    "recomGroupType":        "VSP",
    "pageName":              "SRP",
    "groupByConfigurations": "true",
    "lazy":                  "true",
}

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer":         "https://www.99acres.com/",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
    "platform":        "DESKTOP",
}


def explore_keys(obj, prefix="", depth=0, max_depth=4):
    """Recursively print JSON keys and sample values."""
    if depth > max_depth:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else k
            if isinstance(v, (dict, list)):
                print(f"  {'  '*depth}{path}:")
                explore_keys(v, path, depth+1, max_depth)
            else:
                sample = str(v)[:80] if v is not None else "null"
                print(f"  {'  '*depth}{path} = {sample}")
    elif isinstance(obj, list) and obj:
        print(f"  {'  '*depth}[list of {len(obj)}]")
        explore_keys(obj[0], prefix + "[0]", depth+1, max_depth)


def main():
    print("=" * 60)
    print("Testing 99acres API — Akola listings")
    print("=" * 60)

    try:
        resp = requests.get(API_URL, params=PARAMS, headers=HEADERS, timeout=30)
        print(f"\nStatus: {resp.status_code}")
        print(f"URL hit: {resp.url}\n")

        if resp.status_code != 200:
            print(f"ERROR: Got {resp.status_code}")
            print(resp.text[:500])
            sys.exit(1)

        data = resp.json()

        # ---- Top-level keys ----
        print("TOP LEVEL KEYS:", list(data.keys()))
        print()

        # ---- Explore structure ----
        print("FULL STRUCTURE:")
        explore_keys(data)
        print()

        # ---- Find where properties actually live ----
        print("=" * 60)
        print("SEARCHING FOR PROPERTY ARRAY...")
        print("=" * 60)

        def find_lists(obj, path=""):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    find_lists(v, f"{path}.{k}")
            elif isinstance(obj, list) and len(obj) > 2:
                # Could be our property array
                print(f"\n  Found list at: {path} (len={len(obj)})")
                if obj and isinstance(obj[0], dict):
                    print(f"  First item keys: {list(obj[0].keys())}")

        find_lists(data)

        # ---- Print first full property ----
        print("\n" + "=" * 60)
        print("SAVING FULL RAW RESPONSE TO: raw_response.json")
        print("=" * 60)
        with open("raw_response.json", "w") as f:
            json.dump(data, f, indent=2)

        print("\nOpen raw_response.json to see the complete structure.")
        print("Share the 'property array path' and first property fields here.")

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        print("\nThis may mean 99acres is blocking the request.")
        print("Solution: Copy the full request as cURL from DevTools and share it.")
        sys.exit(1)
    except json.JSONDecodeError:
        print("Response is not JSON:")
        print(resp.text[:1000])
        sys.exit(1)


if __name__ == "__main__":
    main()
