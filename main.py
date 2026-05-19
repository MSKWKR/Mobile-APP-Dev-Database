import os

from db.schema import create_tables

CRAWLER_TYPE = os.environ.get("CRAWLER_TYPE")  # app_store | google_play


def main():
    create_tables()

    if CRAWLER_TYPE == "app_store":
        from crawlers.apple_store import worker
        print("[CONTAINER] Starting App Store worker")
        worker()

    elif CRAWLER_TYPE == "google_play":
        from crawlers.google_play import worker
        print("[CONTAINER] Starting Google Play worker")
        worker()

    else:
        # Local debug mode — run both workers in threads
        from crawlers.apple_store import worker as as_worker
        from crawlers.google_play import worker as gp_worker

        import threading

        print("[LOCAL] Running both workers")

        t1 = threading.Thread(target=as_worker)
        t2 = threading.Thread(target=gp_worker)

        t1.start()
        t2.start()

        t1.join()
        t2.join()


if __name__ == "__main__":
    main()