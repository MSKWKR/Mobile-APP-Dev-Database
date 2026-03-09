from db.connection import get_connection

def create_tables():

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS developers (
        id SERIAL PRIMARY KEY,
        name TEXT,
        store TEXT,
        developer_url TEXT,
        email TEXT,
        website TEXT,
        country TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS apps (
        id SERIAL PRIMARY KEY,
        developer_id INTEGER REFERENCES developers(id),
        app_name TEXT,
        store TEXT,
        rating FLOAT,
        downloads TEXT,
        category TEXT
    )
    """)

    conn.commit()

    cur.close()
    conn.close()