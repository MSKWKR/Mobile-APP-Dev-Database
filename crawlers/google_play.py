import os
import time
import random
import requests

from google_play_scraper import app as gplay_app, search

from db.queries import insert_developer, insert_app, insert_app_version
from db.crawl_tasks import fetch_task, mark_done

from crawlers.search_terms import (
    generate_search_terms,
    get_country_lang,
    ALL_LANGS,
    VERTICALS,
)

# ──────────────────────────────────────────────
# Proxy
# ──────────────────────────────────────────────
_proxy_host = os.environ.get("PROXY_HOST")
_proxy_port = os.environ.get("PROXY_PORT", "8118")

PROXIES = (
    {
        "http": f"http://{_proxy_host}:{_proxy_port}",
        "https": f"http://{_proxy_host}:{_proxy_port}",
    }
    if _proxy_host else None
)

STORE = "google_play"

WAIT_MIN = 0.5
WAIT_MAX = 1.0
N_HITS = 30

# ──────────────────────────────────────────────
# API wrappers
# ──────────────────────────────────────────────

def _search_by_term(term: str, country: str, lang: str = "en"):
    for attempt in range(5):
        try:
            time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))
            return search(term, lang=lang, country=country, n_hits=N_HITS)
        except Exception as e:
            msg = str(e).lower()
            if ("429" in msg or "too many" in msg) and attempt < 4:
                time.sleep(10 + random.random() * 5)
                continue
            return []
    return []


def _fetch_app(app_id: str, country: str):
    for attempt in range(5):
        try:
            time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))
            return gplay_app(app_id, lang="en", country=country)
        except Exception as e:
            msg = str(e).lower()
            if ("429" in msg or "too many" in msg) and attempt < 4:
                time.sleep(10 + random.random() * 5)
                continue
            return None
    return None


# ──────────────────────────────────────────────
# DB insert
# ──────────────────────────────────────────────

def _insert_app(app_info: dict, country: str):
    try:
        dev_name = app_info.get("developer")
        if not dev_name:
            return

        dev_id = insert_developer(
            name=dev_name,
            email=app_info.get("developerEmail"),
            website=app_info.get("developerWebsite"),
        )

        app_db_id = insert_app(
            developer_id=dev_id,
            store=STORE,
            app_id=app_info.get("appId"),
            app_name=app_info.get("title"),
            category=app_info.get("genre") or "Unknown",
            country=country,
        )

        version = app_info.get("version")
        if version:
            insert_app_version(app_db_id, version)

    except Exception as e:
        print(f"[DB ERROR] {app_info.get('appId')}: {e}")


# ──────────────────────────────────────────────
# Task processors
# ──────────────────────────────────────────────

def process_category(country: str, category_id: str):
    langs = ["en"]
    local_lang = get_country_lang(country)
    if local_lang != "en":
        langs.append(local_lang)

    all_ids = set()

    for lang in langs:
        results = _search_by_term(category_id, country, lang)
        for r in results:
            aid = r.get("appId")
            if aid:
                all_ids.add(aid)

    for app_id in all_ids:
        app_info = _fetch_app(app_id, country)
        if app_info:
            _insert_app(app_info, country)


def process_keyword(country: str, keyword: str):
    results = _search_by_term(keyword, country, "en")

    for r in results:
        app_id = r.get("appId")
        if not app_id:
            continue

        app_info = _fetch_app(app_id, country)
        if app_info:
            _insert_app(app_info, country)


def process_language(country: str, lang: str, term: str):
    results = _search_by_term(term, country, lang)

    for r in results:
        app_id = r.get("appId")
        if not app_id:
            continue

        app_info = _fetch_app(app_id, country)
        if app_info:
            _insert_app(app_info, country)


# ──────────────────────────────────────────────
# Worker loop
# ──────────────────────────────────────────────

def worker():
    print("[GOOGLE PLAY WORKER] started")

    while True:
        task = fetch_task()

        if not task:
            time.sleep(2)
            continue

        task_id, source, country, task_type, payload = task

        try:
            if source != "google_play":
                mark_done(task_id)
                continue

            # ── category tasks ─────────────────────
            if task_type == "category":
                process_category(country, payload)

            # ── keyword tasks ──────────────────────
            elif task_type == "keyword":
                process_keyword(country, payload)

            # ── language tasks ──────────────────────
            elif task_type == "language":
                lang, term = payload.split("::", 1)
                process_language(country, lang, term)

            mark_done(task_id)

            print(f"[DONE] {country} | {task_type} | {payload}")

        except Exception as e:
            print(f"[ERROR] task {task_id}: {e}")
            # leave task for retry

# ──────────────────────────────────────────────
# ENTRY
# ──────────────────────────────────────────────

if __name__ == "__main__":
    worker()