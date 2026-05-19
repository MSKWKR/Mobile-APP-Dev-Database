from db.connection import get_connection
from psycopg2.extras import execute_values


# ──────────────────────────────────────────────
# SINGLE INSERT
# ──────────────────────────────────────────────
def add_task(source, country, task_type, payload, region):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO crawl_tasks (source, country, task_type, payload, region)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (source, country, task_type, payload, region))

    conn.commit()
    cur.close()
    conn.close()


# ──────────────────────────────────────────────
# BULK INSERT (USED BY SEED)
# ──────────────────────────────────────────────
def add_tasks_bulk(tasks):
    """
    tasks = [
        (source, country, task_type, payload, region),
        ...
    ]
    """

    if not tasks:
        return 0

    conn = get_connection()
    cur = conn.cursor()

    execute_values(
        cur,
        """
        INSERT INTO crawl_tasks (source, country, task_type, payload, region)
        VALUES %s
        ON CONFLICT DO NOTHING
        """,
        tasks
    )

    conn.commit()
    cur.close()
    conn.close()

    return len(tasks)


# ──────────────────────────────────────────────
# FETCH TASK (REGION-AWARE QUEUE)
# ──────────────────────────────────────────────
def fetch_task(region: str):
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE crawl_tasks
            SET status = 'running',
                locked_at = NOW()
            WHERE id = (
                SELECT id
                FROM crawl_tasks
                WHERE status = 'pending'
                AND region = %s
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, source, country, task_type, payload, region
        """, (region,))

        task = cur.fetchone()
        conn.commit()

        return task

    except Exception as e:
        conn.rollback()
        print(f"[QUEUE ERROR] fetch_task: {e}")
        return None

    finally:
        cur.close()
        conn.close()


# ──────────────────────────────────────────────
# MARK DONE
# ──────────────────────────────────────────────
def mark_done(task_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        UPDATE crawl_tasks
        SET status = 'done',
            updated_at = NOW()
        WHERE id = %s
    """, (task_id,))

    conn.commit()
    cur.close()
    conn.close()


# ──────────────────────────────────────────────
# RESET STUCK TASKS (OPTIONAL)
# ──────────────────────────────────────────────
def reset_stuck_tasks(timeout_minutes=30):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        UPDATE crawl_tasks
        SET status = 'pending'
        WHERE status = 'running'
        AND locked_at < NOW() - INTERVAL '%s minutes'
    """ % timeout_minutes)

    conn.commit()
    cur.close()
    conn.close()