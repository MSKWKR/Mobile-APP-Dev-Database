import os
import psycopg2

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "localhost"),
    "port":     int(os.environ.get("DB_PORT", 5433)),
    "database": os.environ.get("DB_NAME", "appcrawler"),
    "user":     os.environ.get("DB_USER", "crawler"),
    "password": os.environ.get("DB_PASS", "crawlerpass"),
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)