from db.connection import get_connection


def insert_developer(name, email=None, website=None):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO developers (name, email, website)
        VALUES (%s, %s, %s)
        ON CONFLICT (name)
        DO UPDATE SET
            email = COALESCE(EXCLUDED.email, developers.email)
        RETURNING id
    """, (name, email, website))

    dev_id = cur.fetchone()[0]

    conn.commit()
    cur.close()
    conn.close()

    return dev_id

def insert_app(developer_id, store, app_id, app_name, category):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO apps
        (developer_id, store, app_id, app_name, category)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (store, app_id)
        DO UPDATE SET app_name = EXCLUDED.app_name
        RETURNING id
    """, (developer_id, store, app_id, app_name, category))

    app_id = cur.fetchone()[0]

    conn.commit()
    cur.close()
    conn.close()

    return app_id

def insert_app_version(app_db_id, version):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO app_versions (app_db_id, version)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
    """, (app_db_id, version))

    conn.commit()
    cur.close()
    conn.close()