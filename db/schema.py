from db.connection import get_connection


def create_tables():

    conn = get_connection()
    cur = conn.cursor()

    # developers
    cur.execute("""
    CREATE TABLE IF NOT EXISTS developers (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT,
        website TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(name)
    )
    """)

    # apps
    cur.execute("""
    CREATE TABLE IF NOT EXISTS apps (
        id SERIAL PRIMARY KEY,
        developer_id INTEGER REFERENCES developers(id),
        store TEXT NOT NULL,
        app_id TEXT NOT NULL,
        app_name TEXT NOT NULL,
        category TEXT,
        country TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(store, app_id, country)
    )
    """)

    # app versions
    cur.execute("""
    CREATE TABLE IF NOT EXISTS app_versions (
        id SERIAL PRIMARY KEY,
        app_db_id INTEGER REFERENCES apps(id),
        version TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    cur.close()
    conn.close()