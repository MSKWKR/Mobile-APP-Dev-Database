from concurrent.futures import ThreadPoolExecutor, as_completed

from db.schema import create_tables
from crawlers.google_play import crawl_google_play
from crawlers.apple_store import crawl_app_store


def main():
    create_tables()

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