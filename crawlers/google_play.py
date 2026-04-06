import json
import random
import re
import requests
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from google_play_scraper import app, search

from db.queries import insert_developer, insert_app, insert_app_version

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────

STORE = "google_play"
MAX_WORKERS = 8

WAIT_MIN = 0.05
WAIT_MAX = 0.25
COLLECTION_WAIT_MIN = 0.5
COLLECTION_WAIT_MAX = 1.0
CATEGORY_WAIT_MIN = 1.0
CATEGORY_WAIT_MAX = 2.0
KEYWORD_WAIT_MIN = 0.5
KEYWORD_WAIT_MAX = 1.0

COUNTRIES = ["us", "gb", "in", "br", "jp", "kr", "de", "fr", "au", "tw"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Mirror of App Store RSS collections — Google Play equivalents
CHART_COLLECTIONS = [
    "topselling_free",       # Top Free
    "topselling_paid",       # Top Paid
    "topgrossing",           # Top Grossing
    "topselling_new_free",   # New Free
    "topselling_new_paid",   # New Paid
]

# All Google Play categories
CATEGORIES = [
    "APPLICATION",
    "ANDROID_WEAR",
    "ART_AND_DESIGN",
    "AUTO_AND_VEHICLES",
    "BEAUTY",
    "BOOKS_AND_REFERENCE",
    "BUSINESS",
    "COMICS",
    "COMMUNICATION",
    "DATING",
    "EDUCATION",
    "ENTERTAINMENT",
    "EVENTS",
    "FINANCE",
    "FOOD_AND_DRINK",
    "HEALTH_AND_FITNESS",
    "HOUSE_AND_HOME",
    "LIBRARIES_AND_DEMO",
    "LIFESTYLE",
    "MAPS_AND_NAVIGATION",
    "MEDICAL",
    "MUSIC_AND_AUDIO",
    "NEWS_AND_MAGAZINES",
    "PARENTING",
    "PERSONALIZATION",
    "PHOTOGRAPHY",
    "PRODUCTIVITY",
    "SHOPPING",
    "SOCIAL",
    "SPORTS",
    "TOOLS",
    "TRAVEL_AND_LOCAL",
    "VIDEO_PLAYERS",
    "WEATHER",
    "GAME",
    "GAME_ACTION",
    "GAME_ADVENTURE",
    "GAME_ARCADE",
    "GAME_BOARD",
    "GAME_CARD",
    "GAME_CASINO",
    "GAME_CASUAL",
    "GAME_EDUCATIONAL",
    "GAME_MUSIC",
    "GAME_PUZZLE",
    "GAME_RACING",
    "GAME_ROLE_PLAYING",
    "GAME_SIMULATION",
    "GAME_SPORTS",
    "GAME_STRATEGY",
    "GAME_TRIVIA",
    "GAME_WORD",
]

# Per-category keyword terms for the keyword sweep pass
CATEGORY_SEARCH_TERMS = {
    "GAME_ACTION":            ["action games android", "fighting games", "shooter games mobile"],
    "GAME_ADVENTURE":         ["adventure games android", "rpg adventure", "quest games"],
    "GAME_ARCADE":            ["arcade games android", "classic arcade mobile", "retro games"],
    "GAME_CASUAL":            ["casual games android", "hyper casual", "idle clicker games"],
    "GAME_PUZZLE":            ["puzzle games android", "brain games", "logic puzzle mobile"],
    "GAME_RACING":            ["racing games android", "car racing mobile", "driving games"],
    "GAME_SPORTS":            ["sports games android", "football game mobile", "basketball game"],
    "GAME_STRATEGY":          ["strategy games android", "tower defense", "war strategy games"],
    "GAME_SIMULATION":        ["simulation games android", "city builder mobile", "life simulator"],
    "GAME_ROLE_PLAYING":      ["rpg android", "role playing game mobile", "fantasy rpg"],
    "GAME_BOARD":             ["board games android", "chess mobile", "tabletop games"],
    "GAME_CARD":              ["card games android", "solitaire mobile", "poker game"],
    "GAME_CASINO":            ["casino games android", "slots mobile", "blackjack app"],
    "GAME_EDUCATIONAL":       ["educational games kids", "learning games android", "kids quiz"],
    "GAME_MUSIC":             ["music games android", "rhythm game mobile", "piano game"],
    "GAME_TRIVIA":            ["trivia games android", "quiz game mobile", "general knowledge game"],
    "GAME_WORD":              ["word games android", "crossword mobile", "word puzzle app"],
    "FINANCE":                ["banking app android", "budget tracker", "investment app", "stock trading mobile"],
    "HEALTH_AND_FITNESS":     ["fitness app android", "workout tracker", "running app", "meditation app"],
    "PRODUCTIVITY":           ["productivity app android", "notes app", "task manager", "to do list app"],
    "SOCIAL":                 ["social media android", "chat app", "messaging app"],
    "PHOTOGRAPHY":            ["photo editor android", "camera app", "photo filter app"],
    "MUSIC_AND_AUDIO":        ["music player android", "streaming music app", "podcast app"],
    "VIDEO_PLAYERS":          ["video player android", "video editor mobile", "streaming app"],
    "EDUCATION":              ["learning app android", "language learning", "kids education app"],
    "TRAVEL_AND_LOCAL":       ["travel app android", "hotel booking", "flight tracker app"],
    "FOOD_AND_DRINK":         ["food delivery android", "restaurant finder", "recipe app"],
    "SHOPPING":               ["shopping app android", "online store app", "deals app"],
    "NEWS_AND_MAGAZINES":     ["news app android", "newspaper app", "magazine reader"],
    "TOOLS":                  ["utility app android", "system tools", "file manager android"],
    "COMMUNICATION":          ["messaging app android", "video call app", "email client"],
    "MAPS_AND_NAVIGATION":    ["navigation app android", "maps app", "gps app"],
    "WEATHER":                ["weather app android", "weather forecast", "radar weather"],
    "BUSINESS":               ["business app android", "crm mobile", "project management app"],
    "MEDICAL":                ["medical app android", "health tracker", "symptom checker app"],
    "SPORTS":                 ["sports scores android", "live sports app", "fantasy sports"],
    "LIFESTYLE":              ["lifestyle app android", "daily planner", "self improvement app"],
    "PERSONALIZATION":        ["launcher android", "wallpaper app", "icon pack android"],
    "DATING":                 ["dating app android", "meet people app", "relationship app"],
    "PARENTING":              ["parenting app android", "kids tracker", "baby monitor app"],
    "BEAUTY":                 ["makeup app android", "beauty tips app", "skincare tracker"],
    "HOUSE_AND_HOME":         ["home design app android", "interior design", "smart home app"],
    "EVENTS":                 ["event app android", "ticket booking", "concert finder"],
    "COMICS":                 ["comics app android", "manga reader", "webtoon app"],
    "BOOKS_AND_REFERENCE":    ["ebook reader android", "book app", "dictionary app"],
    "AUTO_AND_VEHICLES":      ["car app android", "vehicle tracker", "parking app"],
    "ART_AND_DESIGN":         ["drawing app android", "art app", "design tools mobile"],
    "ENTERTAINMENT":          ["entertainment app android", "streaming video", "funny videos app"],
}

# ──────────────────────────────────────────────
# Chart scraper (replaces missing collection())
# Hits Google Play top chart pages directly,
# same as App Store crawler hits iTunes RSS
# ──────────────────────────────────────────────

def _scrape_chart(collection: str, category: str, country: str, count: int = 100, retries: int = 3) -> list[str]:
    """
    Fetch app IDs from Google Play's internal batchexecute API.
    This is the actual endpoint the Play Store web UI uses for top charts.
    Replaces the dead /collection/<name>/category/<cat> URL format.
    """
    url = f"https://play.google.com/_/PlayStoreUi/data/batchexecute?hl=en&gl={country.upper()}"

    # The nested array payload for rpcids=vyAe2 (top charts endpoint)
    # Format: [[category, collection, null, null, null, count]]
    payload_inner = json.dumps([[category, collection, None, None, None, count]])
    payload = f"f.req={requests.utils.quote(json.dumps([[['vyAe2', payload_inner, None, 'generic']]])) }"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded;charset=utf-8",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    }

    for attempt in range(retries):
        try:
            resp = requests.post(url, data=payload, headers=headers, timeout=20)
            resp.raise_for_status()

            # Response is garbled first line + JSON; strip the first line
            text = resp.text
            # Find the first '[' which starts the actual JSON payload
            json_start = text.find("[[")
            if json_start == -1:
                return []

            raw = text[json_start:]

            # App IDs appear as the package name pattern in the response
            app_ids = re.findall(r'"((?:[a-zA-Z][a-zA-Z0-9_]*\.)+[a-zA-Z][a-zA-Z0-9_]*)"', raw)

            # Filter to valid-looking package names and deduplicate
            seen = set()
            unique = []
            for aid in app_ids:
                # Must have at least 2 segments and not look like a URL/domain
                parts = aid.split(".")
                if len(parts) >= 2 and aid not in seen and len(aid) < 100:
                    seen.add(aid)
                    unique.append(aid)

            return unique

        except requests.HTTPError as e:
            status = e.response.status_code if e.response else None
            if status in (429, 403) and attempt < retries - 1:
                wait = (5 ** attempt) + random.uniform(0, 2)
                print(f"[RATE LIMIT] chart {collection}/{category}/{country} → retrying in {wait:.1f}s...")
                time.sleep(wait)
            else:
                print(f"[WARN] chart failed {collection}/{category}/{country}: {e}")
                return []
        except Exception as e:
            print(f"[WARN] chart failed {collection}/{category}/{country}: {e}")
            return []

    return []


def fetch_app_ids_for_category(category_id: str, country: str) -> list[str]:
    """
    Pull app IDs from all chart collections for this category + country.
    Mirrors App Store's _search_category() which loops all RSS collections.
    """
    seen = set()
    app_ids = []

    for collection in CHART_COLLECTIONS:
        ids = _scrape_chart(collection, category_id, country)

        for aid in ids:
            if aid not in seen:
                seen.add(aid)
                app_ids.append(aid)

        print(f"  [chart] {collection}/{category_id}/{country} → {len(ids)} apps")
        time.sleep(random.uniform(COLLECTION_WAIT_MIN, COLLECTION_WAIT_MAX))

    return app_ids


# ──────────────────────────────────────────────
# Keyword sweep (mirrors App Store Pass 2)
# ──────────────────────────────────────────────

def _search_by_keyword(term: str, country: str, n_hits: int = 100) -> list[str]:
    """Search Google Play by keyword, return app IDs."""
    try:
        results = search(term, n_hits=n_hits, lang="en", country=country)
        return [r["appId"] for r in results if r.get("appId")]
    except Exception as e:
        print(f"[WARN] search '{term}' for {country}: {e}")
        return []


# ──────────────────────────────────────────────
# App detail fetch with backoff
# ──────────────────────────────────────────────

def fetch_app_details(app_id: str, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            return app(app_id, lang="en", country="us")
        except Exception as e:
            msg = str(e).lower()
            if ("429" in msg or "too many" in msg) and attempt < retries - 1:
                wait = (5 ** attempt) + random.uniform(0, 2)
                print(f"[RATE LIMIT] {app_id} → retrying in {wait:.1f}s...")
                time.sleep(wait)
            else:
                return None
    return None


# ──────────────────────────────────────────────
# DB worker
# ──────────────────────────────────────────────

def process_app(app_id: str, category_desc: str) -> str | None:
    try:
        app_info = fetch_app_details(app_id)
        if not app_info:
            return None

        dev_id = insert_developer(
            name=app_info.get("developer"),
            email=app_info.get("developerEmail"),
            website=app_info.get("developerWebsite"),
        )
        app_db_id = insert_app(
            developer_id=dev_id,
            store=STORE,
            app_id=app_id,
            app_name=app_info.get("title"),
            category=category_desc,
        )
        version = app_info.get("version")
        if version:
            insert_app_version(app_db_id, version)

        time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))
        return app_info.get("title")

    except Exception as e:
        print(f"[ERROR] {app_id}: {e}")
        return None


def _bulk_insert(app_ids: list[str], category_desc: str) -> int:
    """Insert a list of app IDs into DB using thread pool. Returns inserted count."""
    inserted = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_app, aid, category_desc): aid
            for aid in app_ids
        }
        for future in as_completed(futures):
            title = future.result()
            if title:
                inserted += 1
                print(f"  [{category_desc}] {inserted}/{len(app_ids)} → {title}")
    return inserted


# ──────────────────────────────────────────────
# Main crawler
# ──────────────────────────────────────────────

def crawl_google_play():
    # Load categories JSON or fall back to hardcoded list
    try:
        with open("categories/google-play-apps-categories.json", "r") as f:
            categories_data = json.load(f)
        category_pairs = [
            (c["category"], c.get("category_description", c["category"]))
            for c in categories_data
        ]
    except FileNotFoundError:
        print("[INFO] No categories JSON found — using built-in category list")
        category_pairs = [(cat, cat) for cat in CATEGORIES]

    while True:
        seen_app_ids: set[str] = set()  # global dedup for the full run

        for country in COUNTRIES:
            print(f"\n{'='*55}")
            print(f"  Country: {country.upper()}")
            print(f"{'='*55}")

            # ── Pass 1: Top chart sweep (mirrors App Store RSS pass) ──
            for category_id, category_desc in category_pairs:
                print(f"\n=== [{country}] CHARTS {category_id} ({category_desc}) ===")

                app_ids = fetch_app_ids_for_category(category_id, country)
                new_ids = [aid for aid in app_ids if aid not in seen_app_ids]

                if not new_ids:
                    print("[SKIP] all already seen")
                    continue

                print(f"  → {len(new_ids)} new  |  {len(app_ids) - len(new_ids)} dupes skipped")

                count = _bulk_insert(new_ids, category_desc)
                seen_app_ids.update(new_ids)
                print(f"  done — {count} inserted")

                time.sleep(random.uniform(CATEGORY_WAIT_MIN, CATEGORY_WAIT_MAX))

            # ── Pass 2: Keyword sweep (mirrors App Store keyword pass) ──
            print(f"\n--- [{country}] Keyword sweep ---")

            for category_id, category_desc in category_pairs:
                terms = CATEGORY_SEARCH_TERMS.get(
                    category_id,
                    [category_id.lower().replace("_", " ")]
                )

                for term in terms:
                    print(f"\n=== [{country}] keyword: '{term}' ===")

                    kw_ids = _search_by_keyword(term, country)
                    new_kw_ids = [aid for aid in kw_ids if aid not in seen_app_ids]

                    if not new_kw_ids:
                        print("[SKIP] all already seen")
                        time.sleep(random.uniform(KEYWORD_WAIT_MIN, KEYWORD_WAIT_MAX))
                        continue

                    print(f"  → {len(new_kw_ids)} new  |  {len(kw_ids) - len(new_kw_ids)} dupes skipped")

                    count = _bulk_insert(new_kw_ids, f"{category_desc}::{term}")
                    seen_app_ids.update(new_kw_ids)
                    print(f"  done — {count} inserted")

                    time.sleep(random.uniform(KEYWORD_WAIT_MIN, KEYWORD_WAIT_MAX))

        print("\n[FULL RUN COMPLETE] Sleeping 24h before next run...")
        time.sleep(60 * 60 * 24)


if __name__ == "__main__":
    crawl_google_play()