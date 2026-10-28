"""SQLite persistence layer: schema, claim, complete."""

import sqlite3
import threading
import time
from typing import Any, Dict, Optional

DEFAULT_DB_PATH = "jobs.db"
_local = threading.local()


def get_connection(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Returns a thread-local SQLite connection configured with WAL mode and busy timeout."""
    if not hasattr(_local, "connections"):
        _local.connections = {}
    if db_path not in _local.connections:
        # isolation_level=None enables autocommit mode, required for explicit BEGIN IMMEDIATE transactions.
        conn = sqlite3.connect(db_path, timeout=10.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        _local.connections[db_path] = conn
    return _local.connections[db_path]


def close_thread_connection(db_path: Optional[str] = None) -> None:
    """Closes thread-local SQLite connection(s) for clean teardown."""
    if not hasattr(_local, "connections"):
        return
    if db_path is not None:
        conn = _local.connections.pop(db_path, None)
        if conn:
            conn.close()
    else:
        for conn in _local.connections.values():
            conn.close()
        _local.connections.clear()


def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    """Initializes the jobs table and indexes if they do not exist."""
    conn = get_connection(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            idem_key TEXT UNIQUE NOT NULL,
            type TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued', 'running', 'completed', 'failed')),
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            lease_owner TEXT,
            lease_expires_at REAL,
            not_before REAL NOT NULL DEFAULT 0,
            error TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_jobs_claim
        ON jobs (status, not_before, id);
        """
    )


def create_job(
    idem_key: str,
    type: str,
    payload: str,
    db_path: str = DEFAULT_DB_PATH,
) -> int:
    """Inserts a job. If idem_key exists, returns the existing job id."""
    conn = get_connection(db_path)
    init_db(db_path)
    now = time.time()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO jobs (idem_key, type, payload, status, created_at, updated_at, not_before)
        VALUES (?, ?, ?, 'queued', ?, ?, ?)
        ON CONFLICT(idem_key) DO NOTHING;
        """,
        (idem_key, type, payload, now, now, now),
    )
    if cursor.rowcount > 0:
        return cursor.lastrowid

    cursor.execute("SELECT id FROM jobs WHERE idem_key = ?;", (idem_key,))
    row = cursor.fetchone()
    return row["id"]


def claim_job(
    worker_id: str,
    lease_secs: float,
    db_path: str = DEFAULT_DB_PATH,
) -> Optional[Dict[str, Any]]:
    """Claims the oldest queued job in a single atomic transaction.

    WHY CLAIM IS A SINGLE TRANSACTION (BEGIN IMMEDIATE):
    In a concurrent environment with multiple workers polling for tasks,
    selecting the oldest queued job and updating its status to 'running'
    must happen atomically.

    Using BEGIN IMMEDIATE acquires a reserved write lock immediately at
    the start of the transaction before any rows are read. This prevents
    race conditions where two workers could execute the SELECT simultaneously,
    receive the same queued job, and both attempt to claim it. With BEGIN
    IMMEDIATE, only one worker can enter the claim block at any time;
    concurrent callers wait via SQLite's busy handler and will see the updated
    state once unblocked, ensuring each job is claimed exactly once.
    """
    conn = get_connection(db_path)
    init_db(db_path)

    conn.execute("BEGIN IMMEDIATE;")
    try:
        now = time.time()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM jobs
            WHERE status = 'queued' AND not_before <= ?
            ORDER BY id ASC
            LIMIT 1;
            """,
            (now,),
        )
        row = cursor.fetchone()
        if row is None:
            conn.execute("COMMIT;")
            return None

        job_id = row["id"]
        lease_expires_at = now + lease_secs
        cursor.execute(
            """
            UPDATE jobs
            SET status = 'running',
                attempts = attempts + 1,
                lease_owner = ?,
                lease_expires_at = ?,
                updated_at = ?
            WHERE id = ?;
            """,
            (worker_id, lease_expires_at, now, job_id),
        )

        cursor.execute("SELECT * FROM jobs WHERE id = ?;", (job_id,))
        updated_row = cursor.fetchone()
        conn.execute("COMMIT;")
        return dict(updated_row)
    except Exception:
        conn.execute("ROLLBACK;")
        raise


def complete_job(
    job_id: int,
    worker_id: Optional[str] = None,
    attempt: Optional[int] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> bool:
    """Marks a job as completed if it is currently queued or running.

    Returns True if a row was updated, False otherwise (e.g. if already completed).
    """
    conn = get_connection(db_path)
    init_db(db_path)
    now = time.time()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE jobs
        SET status = 'completed',
            updated_at = ?
        WHERE id = ? AND status IN ('queued', 'running');
        """,
        (now, job_id),
    )
    return cursor.rowcount > 0
