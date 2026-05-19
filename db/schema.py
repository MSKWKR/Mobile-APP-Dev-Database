from db.connection import get_connection


def create_tables():
    with get_connection() as conn:
        with conn.cursor() as cur:

            # Advisory lock — only one container runs CREATE TABLE at a time.
            # Others wait, then skip because of IF NOT EXISTS.
            cur.execute("SELECT pg_advisory_lock(1)")

            try:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS developers (
                        id         SERIAL PRIMARY KEY,
                        name       TEXT NOT NULL,
                        email      TEXT,
                        website    TEXT,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE (name)
                    )
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS apps (
                        id           SERIAL PRIMARY KEY,
                        developer_id INTEGER REFERENCES developers(id),
                        store        TEXT NOT NULL,
                        app_id       TEXT NOT NULL,
                        app_name     TEXT NOT NULL,
                        category     TEXT,
                        country      TEXT,
                        created_at   TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE (store, app_id)
                    )
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS app_versions (
                        id         SERIAL PRIMARY KEY,
                        app_db_id  INTEGER REFERENCES apps(id),
                        version    TEXT NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE (app_db_id, version)
                    )
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS crawl_tasks (
                        id         SERIAL PRIMARY KEY,
                        source     TEXT        NOT NULL,           -- app_store / google_play
                        country    TEXT        NOT NULL,
                        task_type  TEXT        NOT NULL,           -- category / keyword / language
                        payload    TEXT,                           -- category_id / keyword / lang::term
                        region     TEXT        NOT NULL DEFAULT 'default',
                        status     TEXT        NOT NULL DEFAULT 'pending',  -- pending / running / done / dead
                        retries    INTEGER     NOT NULL DEFAULT 0,
                        last_error TEXT,
                        locked_at  TIMESTAMPTZ,
                        updated_at TIMESTAMPTZ DEFAULT NOW(),
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE (source, country, task_type, payload)
                    )
                """)

                # Migrate existing tables that predate the region/retries columns.
                # ADD COLUMN IF NOT EXISTS is a no-op when the column already exists,
                # so this is safe to run on every startup.
                cur.execute("""
                    ALTER TABLE crawl_tasks
                        ADD COLUMN IF NOT EXISTS region     TEXT        NOT NULL DEFAULT 'default',
                        ADD COLUMN IF NOT EXISTS retries    INTEGER     NOT NULL DEFAULT 0,
                        ADD COLUMN IF NOT EXISTS last_error TEXT,
                        ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()
                """)

                # Partial index — only covers pending rows, stays small as tasks are consumed
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_crawl_tasks_queue
                        ON crawl_tasks (region, status, id)
                        WHERE status = 'pending'
                """)

                conn.commit()

            finally:
                cur.execute("SELECT pg_advisory_unlock(1)")