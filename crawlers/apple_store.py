import json
import os
import random
import requests
import threading
import time

from concurrent.futures import ThreadPoolExecutor, as_completed

from db.queries import insert_developer, insert_app, insert_app_version
from crawlers.search_terms import generate_search_terms

# ──────────────────────────────────────────────
# Proxy — each container routes through its own Tor exit node
# giving Apple a different IP per container
# ──────────────────────────────────────────────
_proxy_host = os.environ.get("PROXY_HOST")
_proxy_port = os.environ.get("PROXY_PORT", "8118")
PROXIES = (
    {
        "http":  f"http://{_proxy_host}:{_proxy_port}",
        "https": f"http://{_proxy_host}:{_proxy_port}",
    }
    if _proxy_host else None
)

STORE = "app_store"
MAX_APPS_PER_CATEGORY = 200
BATCH_SIZE = 200

# 4 containers × 2 workers = 8 threads total
# 3-5s sleep per request = ~1.5 req/s globally — safe for Apple
WAIT_MIN = 3.0
WAIT_MAX = 5.0
CATEGORY_WAIT_MIN = 5.0
CATEGORY_WAIT_MAX = 8.0
COUNTRY_WORKERS = 2

_countries_file = os.environ.get("COUNTRIES_FILE", "listings/countries.json")
with open(_countries_file, "r") as f:
    COUNTRIES: list[str] = json.load(f)

with open("listings/apple-appstore-categories.json", "r") as f:
    CATEGORIES_DATA: list[dict] = json.load(f)

print(f"[CONTAINER] Loaded {len(COUNTRIES)} countries from {_countries_file}")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

COLLECTIONS = [
    "topfreeapplications",
    "toppaidapplications",
    "topgrossingapplications",
    "newapplications",
    "newfreeapplications",
    "newpaidapplications",
]

_seen_lock = threading.Lock()
_seen_app_ids: set[str] = set()


# ──────────────────────────────────────────────
# iTunes API helpers
# ──────────────────────────────────────────────

def _search_by_keyword(keyword: str, country: str, limit: int = 200) -> list[dict]:
    time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))
    try:
        url = (
            f"https://itunes.apple.com/search"
            f"?term={requests.utils.quote(keyword)}"
            f"&entity=software&country={country}&limit={limit}"
        )
        resp = requests.get(url, headers=HEADERS, timeout=10, proxies=PROXIES)
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as e:
        print(f"[WARN] keyword '{keyword}' [{country}] failed: {e}")
        return []


def _search_category(category_id: str, country: str, limit: int = MAX_APPS_PER_CATEGORY) -> list[str]:
    capped = min(limit, 200)
    seen: set[str] = set()
    app_ids: list[str] = []

    for collection in COLLECTIONS:
        url = (
            f"https://itunes.apple.com/{country}/rss/{collection}"
            f"/limit={capped}/genre={category_id}/json"
        )
        try:
            time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))
            resp = requests.get(url, headers=HEADERS, timeout=10, proxies=PROXIES)
            resp.raise_for_status()
            entries = resp.json().get("feed", {}).get("entry", [])
            for entry in entries:
                try:
                    aid = entry["id"]["attributes"]["im:id"]
                    if aid not in seen:
                        seen.add(aid)
                        app_ids.append(aid)
                except (KeyError, TypeError):
                    continue
        except Exception as e:
            print(f"[WARN] {collection} failed for {category_id}/{country}: {e}")
            continue

    return app_ids


def _fetch_batch(app_ids: list[str], country: str, retries: int = 3) -> list[dict]:
    for attempt in range(retries):
        try:
            time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))
            url = f"https://itunes.apple.com/lookup?id={','.join(app_ids)}&country={country}"
            resp = requests.get(url, headers=HEADERS, timeout=20, proxies=PROXIES)
            resp.raise_for_status()
            return resp.json().get("results", [])
        except requests.HTTPError as e:
            status = e.response.status_code if e.response else None
            if status in (403, 429) and attempt < retries - 1:
                wait = 30 + random.uniform(0, 10)  # flat 30-40s backoff on rate limit
                print(f"[RATE LIMIT] [{country}] {status}, retrying in {wait:.1f}s...")
                time.sleep(wait)
            else:
                raise
    return []


# ──────────────────────────────────────────────
# DB insert
# ──────────────────────────────────────────────

def _insert_app_info(app_info: dict, country: str) -> bool:
    try:
        category = app_info.get("primaryGenreName") or "Unknown"
        dev_id = insert_developer(
            name=app_info.get("artistName") or app_info.get("sellerName"),
            email=None,
            website=app_info.get("sellerUrl"),
        )
        app_db_id = insert_app(
            developer_id=dev_id,
            store=STORE,
            app_id=str(app_info.get("trackId")),
            app_name=app_info.get("trackName"),
            category=category,
            country=country,
        )
        version = app_info.get("version")
        if version:
            insert_app_version(app_db_id, version)
        return True
    except Exception as e:
        print(f"[ERROR] insert {app_info.get('trackId')}: {e}")
        return False


# ──────────────────────────────────────────────
# Per-country worker
# ──────────────────────────────────────────────

def _crawl_country(country: str) -> None:
    print(f"[{country.upper()}] starting")

    # Category RSS pass
    for cat_entry in CATEGORIES_DATA:
        category_id   = cat_entry["category"]
        category_desc = cat_entry.get("category_description", category_id)

        try:
            app_ids = _search_category(category_id, country)
        except Exception as e:
            print(f"[ERROR] [{country}] {category_id}: {e}")
            continue

        with _seen_lock:
            new_ids = [aid for aid in app_ids if aid not in _seen_app_ids]
            _seen_app_ids.update(new_ids)

        if not new_ids:
            continue

        inserted = 0
        for i in range(0, len(new_ids), BATCH_SIZE):
            chunk = new_ids[i: i + BATCH_SIZE]
            try:
                apps = _fetch_batch(chunk, country)
            except Exception as e:
                print(f"[ERROR] [{country}] batch: {e}")
                continue
            for app_info in apps:
                if _insert_app_info(app_info, country):
                    inserted += 1

        print(f"[{country}/{category_desc}] {inserted} inserted")
        time.sleep(random.uniform(CATEGORY_WAIT_MIN, CATEGORY_WAIT_MAX))

    # Keyword sweep
    print(f"[{country}] keyword sweep starting")
    for term in generate_search_terms(country):
        results = _search_by_keyword(term, country)

        with _seen_lock:
            new_results = [r for r in results if str(r.get("trackId")) not in _seen_app_ids]
            for r in new_results:
                _seen_app_ids.add(str(r.get("trackId")))

        inserted = sum(_insert_app_info(r, country) for r in new_results)
        if inserted:
            print(f"  [{country}] '{term}' → {inserted} new")

    print(f"[{country.upper()}] DONE")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def crawl_app_store() -> None:
    while True:
        print(f"[RUN START] {len(COUNTRIES)} countries, {COUNTRY_WORKERS} workers")
        with ThreadPoolExecutor(max_workers=COUNTRY_WORKERS) as executor:
            futures = {executor.submit(_crawl_country, c): c for c in COUNTRIES}
            for future in as_completed(futures):
                country = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"[ERROR] {country} crashed: {e}")
        print("[RUN COMPLETE] restarting...")


if __name__ == "__main__":
    crawl_app_store()