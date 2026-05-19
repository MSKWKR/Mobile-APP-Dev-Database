import os
import time
import random
import requests

from db.queries import insert_developer, insert_app, insert_app_version
from db.crawl_tasks import fetch_task, mark_done

# ──────────────────────────────────────────────
# Proxy setup (unchanged)
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

STORE = "app_store"

WAIT_MIN = 3.0
WAIT_MAX = 5.0

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
# iTunes helpers
# ──────────────────────────────────────────────

def _search_category(category_id: str, country: str) -> list[str]:
    seen = set()
    app_ids = []

    for collection in COLLECTIONS:
        url = (
            f"https://itunes.apple.com/{country}/rss/{collection}"
            f"/limit=200/genre={category_id}/json"
        )

        try:
            time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))
            resp = requests.get(url, headers=HEADERS, timeout=15, proxies=PROXIES)
            resp.raise_for_status()

            entries = resp.json().get("feed", {}).get("entry", [])

            for entry in entries:
                try:
                    aid = entry["id"]["attributes"]["im:id"]
                    if aid not in seen:
                        seen.add(aid)
                        app_ids.append(aid)
                except:
                    continue

        except Exception as e:
            print(f"[WARN] category {category_id} failed: {e}")

    return app_ids


def _fetch_batch(app_ids: list[str], country: str):
    try:
        time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))

        url = (
            "https://itunes.apple.com/lookup"
            f"?id={','.join(app_ids)}&country={country}"
        )

        resp = requests.get(url, headers=HEADERS, timeout=20, proxies=PROXIES)
        resp.raise_for_status()

        return resp.json().get("results", [])

    except Exception as e:
        print(f"[WARN] batch fetch failed: {e}")
        return []


# ──────────────────────────────────────────────
# DB insert
# ──────────────────────────────────────────────

def _insert_app(app_info: dict, country: str) -> None:
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
            app_id=str(app_info.get("trackId")),
            app_name=app_info.get("trackName"),
            category=app_info.get("primaryGenreName") or "Unknown",
            country=country,
        )

        version = app_info.get("version")
        if version:
            insert_app_version(app_db_id, version)

    except Exception as e:
        print(f"[DB ERROR] {app_info.get('trackId')}: {e}")


# ──────────────────────────────────────────────
# Task processors
# ──────────────────────────────────────────────

def process_category(country: str, category_id: str):
    app_ids = _search_category(category_id, country)

    if not app_ids:
        return

    for i in range(0, len(app_ids), 200):
        batch = app_ids[i:i+200]
        apps = _fetch_batch(batch, country)

        for app in apps:
            _insert_app(app, country)


def process_keyword(country: str, keyword: str):
    try:
        time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))

        url = (
            "https://itunes.apple.com/search"
            f"?term={requests.utils.quote(keyword)}"
            f"&entity=software&country={country}&limit=200"
        )

        resp = requests.get(url, headers=HEADERS, timeout=15, proxies=PROXIES)
        resp.raise_for_status()

        results = resp.json().get("results", [])

        for r in results:
            _insert_app(r, country)

    except Exception as e:
        print(f"[WARN] keyword '{keyword}' failed: {e}")


# ──────────────────────────────────────────────
# Worker loop
# ──────────────────────────────────────────────

def worker():
    print("[APP STORE WORKER] started")

    while True:
        task = fetch_task()

        if not task:
            time.sleep(2)
            continue

        task_id, source, country, task_type, payload = task

        try:
            if source != "app_store":
                mark_done(task_id)
                continue

            if task_type == "category":
                process_category(country, payload)

            elif task_type == "keyword":
                process_keyword(country, payload)

            mark_done(task_id)

            print(f"[DONE] {country} | {task_type} | {payload}")

        except Exception as e:
            print(f"[ERROR] task {task_id}: {e}")
            # optional: you can leave it unmarked for retry


# ──────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────

if __name__ == "__main__":
    worker()