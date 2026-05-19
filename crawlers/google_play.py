import os
import time
import random
import threading

import requests
from google_play_scraper import app as gplay_app, search

from db.queries import insert_developer, insert_app, insert_app_version
from db.crawl_tasks import fetch_task, mark_done, mark_failed

from crawlers.search_terms import get_country_lang

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG  (all tunable via env vars — no redeploy needed)
# ──────────────────────────────────────────────────────────────────────────────

STORE        = "google_play"
REGION       = os.environ.get("REGION", "default")
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", 4))

WAIT_TASK = (
    float(os.environ.get("WAIT_TASK_MIN", 0.5)),
    float(os.environ.get("WAIT_TASK_MAX", 1.0)),
)

N_HITS = 30

_proxy_host = os.environ.get("PROXY_HOST")
_proxy_port = os.environ.get("PROXY_PORT", "8118")
PROXIES = (
    {
        "http":  f"http://{_proxy_host}:{_proxy_port}",
        "https": f"http://{_proxy_host}:{_proxy_port}",
    }
    if _proxy_host else None
)

# ──────────────────────────────────────────────────────────────────────────────
# PER-THREAD SESSION
# ──────────────────────────────────────────────────────────────────────────────

_local = threading.local()


def _session() -> requests.Session:
    if not hasattr(_local, "session"):
        s = requests.Session()
        if PROXIES:
            s.proxies.update(PROXIES)
        _local.session = s
    return _local.session


def _sleep(range_: tuple[float, float] = WAIT_TASK):
    time.sleep(random.uniform(*range_))


# ──────────────────────────────────────────────────────────────────────────────
# API WRAPPERS  (retry on 429, per-thread session)
# ──────────────────────────────────────────────────────────────────────────────

def _search_by_term(term: str, country: str, lang: str = "en") -> list[dict]:
    for attempt in range(5):
        try:
            _sleep()
            return search(
                term,
                lang=lang,
                country=country,
                n_hits=N_HITS,
            )
        except Exception as e:
            msg = str(e).lower()
            if ("429" in msg or "too many" in msg) and attempt < 4:
                time.sleep(10 + random.random() * 5)
                continue
            print(f"[WARN] search '{term}' ({country}/{lang}): {e}")
            return []
    return []


def _fetch_app(app_id: str, country: str) -> dict | None:
    for attempt in range(5):
        try:
            _sleep()
            return gplay_app(app_id, lang="en", country=country)
        except Exception as e:
            msg = str(e).lower()
            if ("429" in msg or "too many" in msg) and attempt < 4:
                time.sleep(10 + random.random() * 5)
                continue
            print(f"[WARN] fetch app {app_id} ({country}): {e}")
            return None
    return None


# ──────────────────────────────────────────────────────────────────────────────
# DB INSERT
# ──────────────────────────────────────────────────────────────────────────────

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
        if version := app_info.get("version"):
            insert_app_version(app_db_id, version)

    except Exception as e:
        print(f"[DB ERROR] {app_info.get('appId')}: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# TASK PROCESSORS
# ──────────────────────────────────────────────────────────────────────────────

def process_category(country: str, category_id: str):
    langs = ["en"]
    local_lang = get_country_lang(country)
    if local_lang != "en":
        langs.append(local_lang)

    all_ids: set[str] = set()
    for lang in langs:
        for r in _search_by_term(category_id, country, lang):
            if aid := r.get("appId"):
                all_ids.add(aid)

    for app_id in all_ids:
        if app_info := _fetch_app(app_id, country):
            _insert_app(app_info, country)


def process_keyword(country: str, keyword: str):
    for r in _search_by_term(keyword, country, "en"):
        if app_id := r.get("appId"):
            if app_info := _fetch_app(app_id, country):
                _insert_app(app_info, country)


def process_language(country: str, lang: str, term: str):
    for r in _search_by_term(term, country, lang):
        if app_id := r.get("appId"):
            if app_info := _fetch_app(app_id, country):
                _insert_app(app_info, country)


# ──────────────────────────────────────────────────────────────────────────────
# SINGLE WORKER LOOP
# ──────────────────────────────────────────────────────────────────────────────

def _worker(worker_id: int):
    print(f"[GP WORKER-{worker_id}] started  region={REGION}", flush=True)

    while True:
        task = fetch_task(REGION)

        if not task:
            time.sleep(2)
            continue

        task_id, source, country, task_type, payload, _region = task

        # Safety check — skip tasks that don't belong to this crawler
        if source != STORE:
            mark_done(task_id)
            continue

        try:
            if task_type == "category":
                process_category(country, payload)

            elif task_type == "keyword":
                process_keyword(country, payload)

            elif task_type == "language":
                lang, term = payload.split("::", 1)
                process_language(country, lang, term)

            else:
                print(f"[GP WORKER-{worker_id}] unknown task_type '{task_type}', skipping")

            mark_done(task_id)
            print(f"[GP WORKER-{worker_id}][DONE] {country} | {task_type} | {payload}", flush=True)

        except Exception as e:
            print(f"[GP WORKER-{worker_id}][ERROR] task {task_id}: {e}", flush=True)
            mark_failed(task_id, error=str(e))


# ──────────────────────────────────────────────────────────────────────────────
# ENTRYPOINT  (called from main.py)
# ──────────────────────────────────────────────────────────────────────────────

def worker():
    """Spawns WORKER_COUNT threads, each running an independent worker loop."""
    threads = [
        threading.Thread(target=_worker, args=(i,), daemon=True)
        for i in range(WORKER_COUNT)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()