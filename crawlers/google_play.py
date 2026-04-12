import json
import random
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from google_play_scraper import app as gplay_app, search

from db.queries import insert_developer, insert_app, insert_app_version
from crawlers.search_terms import generate_search_terms, get_country_lang, ALL_LANGS, VERTICALS

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────

STORE = "google_play"
MAX_WORKERS = 8
N_HITS = 30  # Google Play's hard cap per search

# Match the original script's wait times
WAIT_MIN = 0.05
WAIT_MAX = 0.25
CATEGORY_WAIT_MIN = 0.5
CATEGORY_WAIT_MAX = 1.0
KEYWORD_WAIT_MIN = 0.2
KEYWORD_WAIT_MAX = 0.4
LANG_WAIT_MIN = 0.2
LANG_WAIT_MAX = 0.4

with open("listings/countries.json", "r") as f:
    _all_countries = json.load(f)

with open("listings/google-play-apps-categories.json", "r") as f:
    _categories_data = json.load(f)

GPLAY_SUPPORTED_COUNTRIES = {
    "ae", "ar", "at", "au", "az", "be", "bg", "bh", "bo", "br",
    "by", "ca", "ch", "cl", "co", "cr", "cz", "de", "dk", "do",
    "dz", "ec", "eg", "es", "et", "fi", "fr", "gb", "ge", "gh",
    "gr", "gt", "hk", "hn", "hr", "hu", "id", "ie", "il", "in",
    "iq", "it", "jm", "jo", "jp", "ke", "kw", "kz", "lb", "lk",
    "lt", "lv", "ma", "mk", "mx", "my", "ng", "ni", "nl", "no",
    "nz", "om", "pa", "pe", "pg", "ph", "pk", "pl", "pr", "pt",
    "py", "qa", "ro", "rs", "ru", "sa", "se", "sg", "si", "sk",
    "sv", "th", "tn", "tr", "tw", "tz", "ua", "ug", "us", "uy",
    "uz", "ve", "vn", "ye", "za", "zw",
}

COUNTRIES = [c for c in _all_countries if c.lower() in GPLAY_SUPPORTED_COUNTRIES]

CATEGORY_PAIRS: list[tuple[str, str]] = [
    (c["category"], c.get("category_description", c["category"]))
    for c in _categories_data
]


# ──────────────────────────────────────────────
# Search
# ──────────────────────────────────────────────

def _search_by_term(term: str, country: str, lang: str = "en") -> list[dict]:
    """Search Google Play, return full result dicts (appId, title, developer, genre)."""
    retries = 3
    for attempt in range(retries):
        try:
            return search(term, lang=lang, country=country, n_hits=N_HITS)
        except Exception as e:
            msg = str(e).lower()
            if "429" in msg and attempt < retries - 1:
                wait = (5 ** attempt) + random.uniform(0, 2)
                print(f"[RATE LIMIT] '{term}' lang={lang} ({country}) → retrying in {wait:.1f}s...")
                time.sleep(wait)
            else:
                return []
    return []


# ──────────────────────────────────────────────
# App detail fetch — called in parallel to get developerEmail
# ──────────────────────────────────────────────

def _fetch_app_details(app_id: str, country: str, retries: int = 3) -> dict | None:
    """
    Fetch full app details including developerEmail and developerWebsite.
    Run in parallel via ThreadPoolExecutor to match original script's throughput.
    """
    for attempt in range(retries):
        try:
            return gplay_app(app_id, lang="en", country=country)
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
# DB insert
# ──────────────────────────────────────────────

def _insert_app_info(app_info: dict, category_desc: str, country: str) -> str | None:
    """Insert a single app from a gplay_app() result dict. Returns title on success."""
    try:
        dev_id = insert_developer(
            name=app_info.get("developer"),
            email=app_info.get("developerEmail"),
            website=app_info.get("developerWebsite"),
        )
        app_db_id = insert_app(
            developer_id=dev_id,
            store=STORE,
            app_id=app_info.get("appId"),
            app_name=app_info.get("title"),
            category=category_desc,
            country=country,
        )
        version = app_info.get("version")
        if version:
            insert_app_version(app_db_id, version)
        return app_info.get("title")
    except Exception as e:
        print(f"[ERROR] {app_info.get('appId')}: {e}")
        return None


def _bulk_fetch_and_insert(app_ids: list[str], category_desc: str, country: str) -> int:
    """
    Fetch full app details in parallel then insert.
    Parallelism here is what made the original script fast despite calling app() per result.
    """
    inserted = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_app_details, aid, country): aid
            for aid in app_ids
        }
        for future in as_completed(futures):
            app_info = future.result()
            if app_info:
                title = _insert_app_info(app_info, category_desc, country)
                if title:
                    inserted += 1
                    print(f"  [{category_desc}] {inserted}/{len(app_ids)} → {title}")
    return inserted


# ──────────────────────────────────────────────
# Sweeps
# ──────────────────────────────────────────────

def _collect_new_ids(results: list[dict], seen_app_ids: set[str]) -> list[str]:
    new_ids = []
    for r in results:
        aid = r.get("appId")
        if aid and aid not in seen_app_ids:
            seen_app_ids.add(aid)
            new_ids.append(aid)
    return new_ids


def _run_category_sweep(country: str, seen_app_ids: set[str]) -> None:
    total = len(CATEGORY_PAIRS)
    print(f"\n--- [{country}] Category sweep ({total} categories) ---")

    for i, (category_id, category_desc) in enumerate(CATEGORY_PAIRS):
        term = category_desc.lower().replace(" apps", "").replace(" games", "").strip()
        print(f"\n=== [{country}] [{i+1}/{total}] {category_id} → '{term}' ===")

        langs = ["en"]
        local_lang = get_country_lang(country)
        if local_lang != "en":
            langs.append(local_lang)

        new_ids = []
        for lang in langs:
            results = _search_by_term(term, country, lang)
            new_ids.extend(_collect_new_ids(results, seen_app_ids))

        if new_ids:
            count = _bulk_fetch_and_insert(new_ids, category_desc, country)
            print(f"  done — {count} inserted")

        time.sleep(random.uniform(CATEGORY_WAIT_MIN, CATEGORY_WAIT_MAX))


def _run_keyword_sweep(country: str, seen_app_ids: set[str]) -> None:
    search_terms = list(generate_search_terms(country))
    total = len(search_terms)
    print(f"\n--- [{country}] Keyword sweep ({total} terms) ---")

    langs = ["en"]
    local_lang = get_country_lang(country)
    if local_lang != "en":
        langs.append(local_lang)

    for i, term in enumerate(search_terms):
        new_ids = []
        for lang in langs:
            results = _search_by_term(term, country, lang)
            new_ids.extend(_collect_new_ids(results, seen_app_ids))

        if new_ids:
            count = _bulk_fetch_and_insert(new_ids, term, country)
            print(f"  [{i+1}/{total}] '{term}' → {count} new")
        else:
            print(f"  [{i+1}/{total}] '{term}' → 0 new (all dupes)")

        time.sleep(random.uniform(KEYWORD_WAIT_MIN, KEYWORD_WAIT_MAX))


def _run_language_sweep(country: str, seen_app_ids: set[str]) -> None:
    total_langs = len(ALL_LANGS)
    total_terms = len(VERTICALS)
    print(f"\n--- [{country}] Language sweep ({total_langs} langs × {total_terms} verticals) ---")

    for li, lang in enumerate(ALL_LANGS):
        for term in VERTICALS:
            results = _search_by_term(term, country, lang)
            new_ids = _collect_new_ids(results, seen_app_ids)

            if new_ids:
                count = _bulk_fetch_and_insert(new_ids, term, country)
                print(f"  [lang={lang}] '{term}' → {count} new")

            time.sleep(random.uniform(LANG_WAIT_MIN, LANG_WAIT_MAX))

        print(f"  [lang {li+1}/{total_langs}] '{lang}' done")


# ──────────────────────────────────────────────
# Main crawler
# ──────────────────────────────────────────────

def crawl_google_play():
    while True:
        seen_app_ids: set[str] = set()

        for country in COUNTRIES:
            print(f"\n{'='*55}")
            print(f"  Country: {country.upper()}")
            print(f"{'='*55}")

            _run_category_sweep(country, seen_app_ids)
            _run_keyword_sweep(country, seen_app_ids)
            _run_language_sweep(country, seen_app_ids)


if __name__ == "__main__":
    crawl_google_play()