import os
from contextlib import contextmanager
from psycopg2 import pool

_pool: pool.ThreadedConnectionPool | None = None


def _get_pool() -> pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=int(os.environ.get("DB_POOL_SIZE", 10)),
            host=os.environ["DB_HOST"],
            port=int(os.environ.get("DB_PORT", 5432)),
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASS"],
        )
    return _pool


@contextmanager
def get_connection():
    """
    Yields a psycopg2 connection from the thread-safe pool.
    Always use as:
        with get_connection() as conn:
            ...
    The connection is returned to the pool on exit (not closed).
    """
    p = _get_pool()
    conn = p.getconn()
    try:
        yield conn
    finally:
        p.putconn(conn)