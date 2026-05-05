import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from db.schema import create_tables

CRAWLER_TYPE = os.environ.get("CRAWLER_TYPE")  # "app_store" | "google_play" | None (local)


def main():
    create_tables()

    if CRAWLER_TYPE == "app_store":
        from crawlers.apple_store import crawl_app_store
        print("[CONTAINER] Running App Store crawler")
        crawl_app_store()

    elif CRAWLER_TYPE == "google_play":
        from crawlers.google_play import crawl_google_play
        print("[CONTAINER] Running Google Play crawler")
        crawl_google_play()

    else:
        # Local run — both in parallel
        from crawlers.apple_store import crawl_app_store
        from crawlers.google_play import crawl_google_play

        print("[LOCAL] Running both crawlers in parallel")
        crawlers = {
            "google_play": crawl_google_play,
            "app_store":   crawl_app_store,
        }
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(fn): name for name, fn in crawlers.items()}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    future.result()
                    print(f"\n[DONE] {name} finished")
                except Exception as e:
                    print(f"\n[ERROR] {name} crashed: {e}")


if __name__ == "__main__":
    main()