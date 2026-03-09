import requests
import pandas as pd
import time
import random

COUNTRY = "us"
LIMIT = 50
WAIT_MIN = 0.2
WAIT_MAX = 0.4

SEARCH_TERMS = [
    "bank",
    "finance",
    "wallet",
    "payment",
    "invoice"
]

data = []

for term in SEARCH_TERMS:
    print(f"\n=== Searching for: {term} ===")
    
    url = "https://itunes.apple.com/search"
    params = {
        "term": term,
        "country": COUNTRY,
        "entity": "software",
        "limit": LIMIT
    }

    try:
        response = requests.get(url, params=params)
        results = response.json().get("results", [])

        for idx, app in enumerate(results):
            data.append({
                "search_term": term,
                "app_name": app.get("trackName"),
                "bundle_id": app.get("bundleId"),
                "developer_name": app.get("sellerName"),
                "developer_url": app.get("sellerUrl"),
                "support_url": app.get("supportUrl"),
                "genre": app.get("primaryGenreName"),
                "rating": app.get("averageUserRating"),
                "track_view_url": app.get("trackViewUrl")
            })

            print(f"[{term}] {idx+1}/{len(results)} → {app.get('trackName')}")

        time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))

    except Exception as e:
        print(f"[ERROR] {term}: {e}")

df = pd.DataFrame(data)
df.to_csv("ios_app_metadata.csv", index=False)

print("\nDone. Saved to ios_app_metadata.csv")