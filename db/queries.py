from db.connection import get_connection

def insert_developer(name, store, url):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO developers (name, store, developer_url)
    VALUES (%s, %s, %s)
    RETURNING id
    """, (name, store, url))

    developer_id = cur.fetchone()[0]

    conn.commit()
    cur.close()
    conn.close()

    return developer_id