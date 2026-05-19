import os
import threading

from db.schema import create_tables
from db.crawl_tasks import reset_stuck_tasks

CRAWLER_TYPE = os.environ.get("CRAWLER_TYPE")   # app_store | google_play
REGION       = os.environ.get("REGION", "default")


def main():
    create_tables()
    reset_stuck_tasks()

    if CRAWLER_TYPE == "app_store":
        from crawlers.apple_store import worker
        print(f"[CONTAINER] Starting App Store worker  region={REGION}", flush=True)
        worker()

    elif CRAWLER_TYPE == "google_play":
        from crawlers.google_play import worker
        print(f"[CONTAINER] Starting Google Play worker  region={REGION}", flush=True)
        worker()

    else:
        # ── Local debug mode — run both crawlers in-process ──────────────────
        from crawlers.apple_store import worker as as_worker
        from crawlers.google_play import worker as gp_worker

        print("[LOCAL] Running both workers", flush=True)

        t1 = threading.Thread(target=as_worker, name="app-store",   daemon=True)
        t2 = threading.Thread(target=gp_worker, name="google-play", daemon=True)

        t1.start()
        t2.start()

        t1.join()
        t2.join()


if __name__ == "__main__":
    main()