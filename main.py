from db.schema import create_tables
from crawlers.google_play import crawl_developer

def main():

    create_tables()
    crawl_developer()

if __name__ == "__main__":
    main()