from db.queries import insert_developer

def crawl_developer():

    # Example scraped data
    name = "Example Studio"
    store = "google_play"
    url = "https://play.google.com/store/apps/dev?id=123"

    dev_id = insert_developer(name, store, url)

    print("Inserted developer id:", dev_id)