import os
import time
import random
import threading

import requests

from db.queries import insert_developer, insert_app, insert_app_version
from db.crawl_tasks import fetch_task, mark_done, mark_failed

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG  (all tunable via env vars — no redeploy needed)
# ──────────────────────────────────────────────────────────────────────────────

STORE        = "app_store"
REGION       = os.environ.get("REGION", "default")
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", 3))

# Delay between RSS collection requests (same host, reuse session)
WAIT_COLLECTION = (
    float(os.environ.get("WAIT_COL_MIN", 1.0)),
    float(os.environ.get("WAIT_COL_MAX", 2.0)),
)
# Delay between task-level requests (search, lookup)
WAIT_TASK = (
    float(os.environ.get("WAIT_TASK_MIN", 0.5)),
    float(os.environ.get("WAIT_TASK_MAX", 1.5)),
)

_proxy_host = os.environ.get("PROXY_HOST")
_proxy_port = os.environ.get("PROXY_PORT", "8118")
PROXIES = (
    {
        "http":  f"http://{_proxy_host}:{_proxy_port}",
        "https": f"http://{_proxy_host}:{_proxy_port}",
    }
    if _proxy_host else None
)

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

# ──────────────────────────────────────────────────────────────────────────────
# PER-THREAD SESSION  (connection reuse, no lock contention)
# ──────────────────────────────────────────────────────────────────────────────

_local = threading.local()


def _session() -> requests.Session:
    """Returns a requests.Session local to the calling thread."""
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        if PROXIES:
            s.proxies.update(PROXIES)
        _local.session = s
    return _local.session


def _sleep(range_: tuple[float, float]):
    time.sleep(random.uniform(*range_))


# ──────────────────────────────────────────────────────────────────────────────
# iTunes / RSS HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _search_category(category_id: str, country: str) -> list[str]:
    seen: set[str] = set()
    app_ids: list[str] = []

    for collection in COLLECTIONS:
        url = (
            f"https://itunes.apple.com/{country}/rss/{collection}"
            f"/limit=200/genre={category_id}/json"
        )
        try:
            _sleep(WAIT_COLLECTION)
            resp = _session().get(url, timeout=15)
            resp.raise_for_status()

            for entry in resp.json().get("feed", {}).get("entry", []):
                try:
                    aid = entry["id"]["attributes"]["im:id"]
                    if aid not in seen:
                        seen.add(aid)
                        app_ids.append(aid)
                except (KeyError, TypeError):
                    continue

        except Exception as e:
            print(f"[WARN] category {category_id}/{collection} ({country}): {e}")

    return app_ids


def _fetch_batch(app_ids: list[str], country: str) -> list[dict]:
    try:
        _sleep(WAIT_TASK)
        url = (
            "https://itunes.apple.com/lookup"
            f"?id={','.join(app_ids)}&country={country}"
        )
        resp = _session().get(url, timeout=20)
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as e:
        print(f"[WARN] batch fetch failed ({country}): {e}")
        return []


# ──────────────────────────────────────────────────────────────────────────────
# DB INSERT
# ──────────────────────────────────────────────────────────────────────────────

def _insert_app(app_info: dict, country: str):
    try:
        name = app_info.get("artistName") or app_info.get("sellerName")
        if not name:
            return

        dev_id = insert_developer(
            name=name,
            email=None,
            website=app_info.get("sellerUrl"),
        )
        app_db_id = insert_app(
            developer_id=dev_id,
            store=STORE,
            app_id=str(app_info["trackId"]),
            app_name=app_info.get("trackName"),
            category=app_info.get("primaryGenreName") or "Unknown",
            country=country,
        )
        if version := app_info.get("version"):
            insert_app_version(app_db_id, version)

    except Exception as e:
        print(f"[DB ERROR] trackId={app_info.get('trackId')}: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# TASK PROCESSORS
# ──────────────────────────────────────────────────────────────────────────────

def process_category(country: str, category_id: str):
    app_ids = _search_category(category_id, country)
    if not app_ids:
        return

    for i in range(0, len(app_ids), 200):
        batch = app_ids[i : i + 200]
        for app in _fetch_batch(batch, country):
            _insert_app(app, country)


def process_keyword(country: str, keyword: str):
    try:
        _sleep(WAIT_TASK)
        url = (
            "https://itunes.apple.com/search"
            f"?term={requests.utils.quote(keyword)}"
            f"&entity=software&country={country}&limit=200"
        )
        resp = _session().get(url, timeout=15)
        resp.raise_for_status()

        for r in resp.json().get("results", []):
            _insert_app(r, country)

    except Exception as e:
        print(f"[WARN] keyword '{keyword}' ({country}): {e}")


# ──────────────────────────────────────────────────────────────────────────────
# SINGLE WORKER LOOP
# ──────────────────────────────────────────────────────────────────────────────

def _worker(worker_id: int):
    print(f"[AS WORKER-{worker_id}] started  region={REGION}", flush=True)

    while True:
        task = fetch_task(REGION, STORE)

        if not task:
            time.sleep(2)
            continue

        task_id, _, country, task_type, payload, _ = task

        try:
            if task_type == "category":
                process_category(country, payload)
            elif task_type == "keyword":
                process_keyword(country, payload)
            else:
                print(f"[AS WORKER-{worker_id}] unknown task_type '{task_type}', skipping")

            mark_done(task_id)
            print(f"[AS WORKER-{worker_id}][DONE] {country} | {task_type} | {payload}", flush=True)

        except Exception as e:
            print(f"[AS WORKER-{worker_id}][ERROR] task {task_id}: {e}", flush=True)
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