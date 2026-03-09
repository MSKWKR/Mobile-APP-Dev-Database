import json
import random
import time
import pandas as pd
from google_play_scraper import search, app

# Configuration
COUNTRY = "tw"
LANG = "zh"
MAX_APPS_PER_CATEGORY = 50
WAIT_MIN = 0.05
WAIT_MAX = 0.25

# Load categories from JSON
with open("google-play-apps-categories.json", "r") as f:
    categories_data = json.load(f)

data = []

for cat_entry in categories_data:
    category_id = cat_entry["category"]
    category_desc = cat_entry.get("category_description", category_id)
    
    print(f"\n=== Scraping category: {category_id} ({category_desc}) ===")
    try:
        results = search(category_id, lang=LANG, country=COUNTRY, n_hits=MAX_APPS_PER_CATEGORY)
    except Exception as e:
        print(f"[ERROR] Failed to search category {category_id}: {e}")
        continue

    for idx, result in enumerate(results):
        app_id = result["appId"]
        try:
            app_info = app(app_id, lang=LANG, country=COUNTRY)

            data.append({
                "category": category_id,
                "category_description": category_desc,
                "app_id": app_id,
                "title": app_info.get("title"),
                "developer_name": app_info.get("developer"),
                "developer_email": app_info.get("developerEmail"),
                "developer_website": app_info.get("developerWebsite"),
                "installs": app_info.get("installs"),
                "score": app_info.get("score")
            })

            print(f"[{category_id}] {idx+1}/{len(results)} → {app_info.get('title')}")

            # Polite wait with jitter
            time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))

        except Exception as e:
            print(f"[ERROR] {app_id}: {e}")

# Save results to CSV
df = pd.DataFrame(data)
df.to_csv("google_play_devs.csv", index=False)
print("\nDone! Saved to google_play_devs.csv")
