import json
import random
import requests
import time

from db.queries import insert_developer, insert_app, insert_app_version
from crawlers.search_terms import generate_search_terms

STORE = "app_store"
MAX_APPS_PER_CATEGORY = 200
BATCH_SIZE = 200

with open("listings/countries.json", "r") as f:
    COUNTRIES = json.load(f)

# Match the original script's wait times
WAIT_MIN = 0.2
WAIT_MAX = 0.4
CATEGORY_WAIT_MIN = 0.5
CATEGORY_WAIT_MAX = 1.0

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


# ──────────────────────────────────────────────
# iTunes API helpers
# ──────────────────────────────────────────────

def _search_by_keyword(keyword: str, country: str, limit: int = 200) -> list[dict]:
    """
    Search iTunes — returns full result dicts directly.
    No follow-up lookup needed; search results include all fields we store.
    limit=200 is Apple's hard cap.
    """
    try:
        url = (
            f"https://itunes.apple.com/search"
            f"?term={requests.utils.quote(keyword)}"
            f"&entity=software&country={country}&limit={limit}"
        )
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as e:
        print(f"[WARN] keyword search '{keyword}' failed: {e}")
        return []


def _search_category(category_id: str, country: str, limit: int = MAX_APPS_PER_CATEGORY) -> list[str]:
    """
    Fetch app IDs from iTunes RSS feeds for a category.
    RSS feeds return IDs only so _fetch_batch is still needed for this pass.
    """
    capped = min(limit, 200)
    seen: set[str] = set()
    app_ids: list[str] = []

    for collection in COLLECTIONS:
        url = (
            f"https://itunes.apple.com/{country}/rss/{collection}"
            f"/limit={capped}/genre={category_id}/json"
        )
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
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
            print(f"[WARN] collection {collection} failed for {category_id}/{country}: {e}")
            continue

    return app_ids


def _fetch_batch(app_ids: list[str], country: str, retries: int = 3) -> list[dict]:
    """
    Fetch full metadata for up to 200 apps via iTunes Lookup.
    Only called for the category RSS pass — keyword pass inserts from search results directly.
    """
    for attempt in range(retries):
        try:
            url = f"https://itunes.apple.com/lookup?id={','.join(app_ids)}&country={country}"
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            return resp.json().get("results", [])
        except requests.HTTPError as e:
            status = e.response.status_code if e.response else None
            if status in (403, 429) and attempt < retries - 1:
                wait = (5 ** attempt) + random.uniform(0, 2)
                print(f"[RATE LIMIT] batch → {status}, retrying in {wait:.1f}s...")
                time.sleep(wait)
            else:
                raise
    return []


# ──────────────────────────────────────────────
# DB insert
# ──────────────────────────────────────────────

def _insert_app_info(app_info: dict, category_desc: str, country: str) -> bool:
    """
    Insert a single app from an iTunes result dict.
    Handles both search results and lookup results — field names are identical.
    """
    try:
        dev_id = insert_developer(
            name=app_info.get("artistName") or app_info.get("sellerName"),
            email=None,  # Apple never exposes developer email publicly
            website=app_info.get("sellerUrl"),
        )
        app_db_id = insert_app(
            developer_id=dev_id,
            store=STORE,
            app_id=str(app_info.get("trackId")),
            app_name=app_info.get("trackName"),
            category=category_desc,
            country=country,
        )
        version = app_info.get("version")
        if version:
            insert_app_version(app_db_id, version)
        return True
    except Exception as e:
        print(f"[ERROR] insert failed for {app_info.get('trackId')}: {e}")
        return False


# ──────────────────────────────────────────────
# Category pass — RSS → batch lookup
# ──────────────────────────────────────────────

def _process_category(category_id: str, category_desc: str, app_ids: list[str], country: str) -> int:
    inserted = 0
    for i in range(0, len(app_ids), BATCH_SIZE):
        chunk = app_ids[i: i + BATCH_SIZE]
        try:
            apps = _fetch_batch(chunk, country)
        except Exception as e:
            print(f"[ERROR] batch fetch failed: {e}")
            continue

        for app_info in apps:
            if _insert_app_info(app_info, category_desc, country):
                inserted += 1
                print(f"[{category_id}] {inserted}/{len(app_ids)} → {app_info.get('trackName')}")

        time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))

    return inserted


# ──────────────────────────────────────────────
# Keyword sweep — inserts directly from search results, no _fetch_batch
# ──────────────────────────────────────────────

def _run_keyword_sweep(country: str, seen_app_ids: set[str]) -> None:
    search_terms = generate_search_terms(country)
    total = len(search_terms)
    print(f"\n--- [{country}] Keyword sweep ({total} terms) ---")

    for i, term in enumerate(search_terms):
        results = _search_by_keyword(term, country)
        new_results = [r for r in results if str(r.get("trackId")) not in seen_app_ids]

        inserted = 0
        for app_info in new_results:
            if _insert_app_info(app_info, term, country):
                inserted += 1
                seen_app_ids.add(str(app_info.get("trackId")))

        if inserted:
            print(f"  [{i+1}/{total}] '{term}' → {inserted} new")
        else:
            print(f"  [{i+1}/{total}] '{term}' → 0 new (all dupes)")

        time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))


# ──────────────────────────────────────────────
# Main crawler
# ──────────────────────────────────────────────

def crawl_app_store():
    with open("listings/apple-appstore-categories.json", "r") as f:
        categories_data = json.load(f)

    while True:
        seen_app_ids: set[str] = set()

        for country in COUNTRIES:
            print(f"\n{'='*50}")
            print(f"  Country: {country.upper()}")
            print(f"{'='*50}")

            for cat_entry in categories_data:
                category_id   = cat_entry["category"]
                category_desc = cat_entry.get("category_description", category_id)

                print(f"\n=== [{country}] {category_id} ({category_desc}) ===")

                try:
                    app_ids = _search_category(category_id, country)
                except Exception as e:
                    print(f"[ERROR] search failed: {e}")
                    continue

                new_ids = [aid for aid in app_ids if aid not in seen_app_ids]
                if not new_ids:
                    print("[SKIP] all apps already seen")
                    continue

                count = _process_category(category_id, category_desc, new_ids, country)
                seen_app_ids.update(new_ids)
                print(f"[{country}/{category_id}] done — {count} inserted ({len(app_ids) - len(new_ids)} dupes skipped)")

                time.sleep(random.uniform(CATEGORY_WAIT_MIN, CATEGORY_WAIT_MAX))

            _run_keyword_sweep(country, seen_app_ids)


if __name__ == "__main__":
    crawl_app_store()