from db.connection import get_connection


def create_tables():
    conn = get_connection()
    cur = conn.cursor()

    # Advisory lock — only one container runs CREATE TABLE at a time
    # Other containers will wait, then skip since IF NOT EXISTS handles it
    cur.execute("SELECT pg_advisory_lock(1)")

    try:
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
            UNIQUE(store, app_id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS app_versions (
            id SERIAL PRIMARY KEY,
            app_db_id INTEGER REFERENCES apps(id),
            version TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(app_db_id, version)
        )
        """)

        conn.commit()

    finally:
        cur.execute("SELECT pg_advisory_unlock(1)")
        cur.close()
        conn.close()