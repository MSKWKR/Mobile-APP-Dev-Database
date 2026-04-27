import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from db.schema import create_tables
from crawlers.apple_store import crawl_app_store

IN_DOCKER = os.environ.get("COUNTRIES_FILE") is not None
CRAWLER = os.environ.get("CRAWLER", "app_store")  # "app_store" | "google_play"


def main():
    create_tables()

    if IN_DOCKER:
        if CRAWLER == "google_play":
            from crawlers.google_play import crawl_google_play
            print("[CONTAINER] Running Google Play crawler only")
            crawl_google_play()
        else:
            print("[CONTAINER] Running App Store crawler only")
            crawl_app_store()

    else:
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