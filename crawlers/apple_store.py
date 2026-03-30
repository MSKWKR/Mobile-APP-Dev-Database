import json
import random
import requests
import time

from db.queries import (
    insert_developer,
    insert_app,
    insert_app_version,
)

STORE = "app_store"
MAX_APPS_PER_CATEGORY = 200
BATCH_SIZE = 200

# Major markets — covers ~90% of the global App Store catalog
COUNTRIES = ["us", "gb", "cn", "jp", "kr", "de", "fr", "br", "in", "au", "tw"]          # iTunes lookup accepts up to 200 IDs per request

WAIT_MIN = 0.5
WAIT_MAX = 1.0
CATEGORY_WAIT_MIN = 1.0
CATEGORY_WAIT_MAX = 2.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

COLLECTIONS = [
    "topfreeapplications",
    "topgrossingapplications",
    "newapplications",
]

# ──────────────────────────────────────────────
# iTunes API helpers
# ──────────────────────────────────────────────

def _search_category(category_id: str, country: str, limit: int = MAX_APPS_PER_CATEGORY) -> list[str]:
    """
    Fetch app IDs across all collections for a category via the iTunes RSS feed.
    Deduplicates across collections. Returns a list of trackId strings.
    """
    capped = min(limit, 200)
    seen = set()
    app_ids = []

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
    Fetch full metadata for up to 200 apps in a single iTunes Lookup request.
    Returns a list of app detail dicts.
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
                wait = (5 ** attempt) + random.uniform(0, 2)  # 5s, 25s, 125s
                print(f"[RATE LIMIT] batch → {status}, retrying in {wait:.1f}s...")
                time.sleep(wait)
            else:
                raise

    return []


# ──────────────────────────────────────────────
# Core worker
# ──────────────────────────────────────────────

def process_category(category_id: str, category_desc: str, app_ids: list[str], country: str) -> int:
    """
    Fetch all app details for a category in batches, persist to DB.
    Returns the count of successfully inserted apps.
    """
    inserted = 0

    for i in range(0, len(app_ids), BATCH_SIZE):
        chunk = app_ids[i : i + BATCH_SIZE]

        try:
            apps = _fetch_batch(chunk, country)
        except Exception as e:
            print(f"[ERROR] batch fetch failed: {e}")
            continue

        for app_info in apps:
            try:
                dev_id = insert_developer(
                    name=app_info.get("artistName"),
                    email=None,
                    website=app_info.get("sellerUrl"),
                )

                app_db_id = insert_app(
                    developer_id=dev_id,
                    store=STORE,
                    app_id=str(app_info.get("trackId")),
                    app_name=app_info.get("trackName"),
                    category=category_desc,
                )

                version = app_info.get("version")
                if version:
                    insert_app_version(app_db_id, version)

                inserted += 1
                print(f"[{category_id}] {inserted}/{len(app_ids)} → {app_info.get('trackName')}")

            except Exception as e:
                print(f"[ERROR] insert failed for {app_info.get('trackId')}: {e}")

        # Be polite between batches
        time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))

    return inserted


# ──────────────────────────────────────────────
# Main crawler
# ──────────────────────────────────────────────

def crawl_app_store():
    with open("categories/apple-appstore-categories.json", "r") as f:
        categories_data = json.load(f)

    while True:
        seen_app_ids: set[str] = set()  # reset deduplication each full run

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
                    print(f"[SKIP] all apps already seen")
                    continue

                count = process_category(category_id, category_desc, new_ids, country)
                seen_app_ids.update(new_ids)
                print(f"[{country}/{category_id}] done — {count} new apps inserted ({len(app_ids) - len(new_ids)} dupes skipped)")

                cooldown = random.uniform(CATEGORY_WAIT_MIN, CATEGORY_WAIT_MAX)
                print(f"[COOLDOWN] {cooldown:.1f}s...")
                time.sleep(cooldown)



if __name__ == "__main__":
    crawl_app_store()