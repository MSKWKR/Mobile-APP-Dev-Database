import json
import random
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from google_play_scraper import search, app

from db.queries import (
    insert_developer,
    insert_app,
    insert_app_version
)

STORE = "google_play"
MAX_APPS_PER_CATEGORY = 100
MAX_WORKERS = 8

WAIT_MIN = 0.05
WAIT_MAX = 0.25


def process_app(app_id, category):

    try:
        app_info = app(app_id)

        dev_id = insert_developer(
            name=app_info.get("developer"),
            email=app_info.get("developerEmail"),
            website=app_info.get("developerWebsite")
        )

        app_db_id = insert_app(
            developer_id=dev_id,
            store=STORE,
            app_id=app_id,
            app_name=app_info.get("title"),
            category=category
        )

        version = app_info.get("version")

        if version:
            insert_app_version(app_db_id, version)

        time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))

        return app_info.get("title")

    except Exception as e:
        print(f"[ERROR] {app_id}: {e}")
        return None


def crawl_google_play():

    with open("google-play-apps-categories.json", "r") as f:
        categories_data = json.load(f)

    for cat_entry in categories_data:

        category_id = cat_entry["category"]
        category_desc = cat_entry.get("category_description", category_id)

        print(f"\n=== Scraping {category_id} ({category_desc}) ===")

        try:
            results = search(category_id, n_hits=MAX_APPS_PER_CATEGORY)
        except Exception as e:
            print(f"[ERROR] search failed: {e}")
            continue

        app_ids = [r["appId"] for r in results]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

            futures = [
                executor.submit(process_app, app_id, category_id)
                for app_id in app_ids
            ]

            for idx, future in enumerate(as_completed(futures), 1):

                title = future.result()

                if title:
                    print(f"[{category_id}] {idx}/{len(app_ids)} → {title}")