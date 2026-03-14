from db.schema import create_tables
from crawlers.google_play import crawl_google_play

def main():

    create_tables()
    crawl_google_play()

if __name__ == "__main__":
    main()