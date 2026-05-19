import json

print("[SEED] started", flush=True)

from db.crawl_tasks import add_tasks_bulk
from crawlers.search_terms import generate_search_terms, ALL_LANGS, VERTICALS


# ──────────────────────────────────────────────
# LOAD DATA
# ──────────────────────────────────────────────
with open("listings/countries.json") as f:
    COUNTRIES = json.load(f)

with open("listings/apple-appstore-categories.json") as f:
    APPLE_CATEGORIES = json.load(f)

with open("listings/google-play-apps-categories.json") as f:
    GP_CATEGORIES = json.load(f)

print(f"[SEED] countries loaded: {len(COUNTRIES)}", flush=True)


# ──────────────────────────────────────────────
# APP STORE TASKS
# ──────────────────────────────────────────────
print("[SEED] building app_store tasks...", flush=True)

app_tasks = []

for country in COUNTRIES:
    for cat in APPLE_CATEGORIES:
        app_tasks.append(
            ("app_store", country, "category", cat["category"])
        )

    for term in generate_search_terms(country):
        app_tasks.append(
            ("app_store", country, "keyword", term)
        )

print(f"[SEED] app_store tasks built: {len(app_tasks)}", flush=True)

inserted = add_tasks_bulk(app_tasks)
print(f"[SEED] app_store inserted: {inserted}", flush=True)


# ──────────────────────────────────────────────
# GOOGLE PLAY TASKS
# ──────────────────────────────────────────────
print("[SEED] building google_play tasks...", flush=True)

gp_tasks = []

for country in COUNTRIES:
    for cat in GP_CATEGORIES:
        gp_tasks.append(
            ("google_play", country, "category", cat["category"])
        )

    for term in generate_search_terms(country):
        gp_tasks.append(
            ("google_play", country, "keyword", term)
        )

    for lang in ALL_LANGS:
        for term in VERTICALS:
            gp_tasks.append(
                ("google_play", country, "language", f"{lang}::{term}")
            )

print(f"[SEED] google_play tasks built: {len(gp_tasks)}", flush=True)

inserted = add_tasks_bulk(gp_tasks)
print(f"[SEED] google_play inserted: {inserted}", flush=True)


print("[SEED] done", flush=True)