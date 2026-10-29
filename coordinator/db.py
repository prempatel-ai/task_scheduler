"""SQLite persistence layer: schema, atomic claim, complete, and sweeper."""

import os
import sqlite3
import threading
import time
from typing import Any, Dict, Optional, Tuple

DEFAULT_DB_PATH = "jobs.db"
_local = threading.local()
_initialized_dbs = set()


def get_connection(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Returns a thread-local SQLite connection with PRAGMAs configured once."""
    if not hasattr(_local, "connections"):
        _local.connections = {}
    if db_path not in _local.connections:
        # isolation_level=None enables autocommit mode, required for explicit BEGIN IMMEDIATE transactions.
        conn = sqlite3.connect(db_path, timeout=10.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        sync_mode = os.getenv("SQLITE_SYNCHRONOUS", "NORMAL")
        conn.execute(f"PRAGMA synchronous = {sync_mode};")
        _local.connections[db_path] = conn

    # Auto-initialize schema once per db_path
    if db_path not in _initialized_dbs:
        init_db(db_path)
    return _local.connections[db_path]


def close_thread_connection(db_path: Optional[str] = None) -> None:
    """Closes thread-local SQLite connection(s) for clean teardown."""
    if not hasattr(_local, "connections"):
        return
    if db_path is not None:
        conn = _local.connections.pop(db_path, None)
        if conn:
            conn.close()
        _initialized_dbs.discard(db_path)
    else:
        for conn in _local.connections.values():
            conn.close()
        _local.connections.clear()
        _initialized_dbs.clear()


def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    """Initializes schema and partial indexes."""
    if not hasattr(_local, "connections") or db_path not in _local.connections:
        conn = sqlite3.connect(db_path, timeout=10.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        sync_mode = os.getenv("SQLITE_SYNCHRONOUS", "NORMAL")
        conn.execute(f"PRAGMA synchronous = {sync_mode};")
        if not hasattr(_local, "connections"):
            _local.connections = {}
        _local.connections[db_path] = conn
    else:
        conn = _local.connections[db_path]

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
            result TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        """
    )
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(jobs);")
    cols = [r["name"] for r in cursor.fetchall()]
    if "result" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN result TEXT;")

    # Partial indexes tailored to query patterns
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_jobs_claim_partial
        ON jobs (not_before, id)
        WHERE status = 'queued';
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_jobs_running_lease
        ON jobs (lease_expires_at)
        WHERE status = 'running';
        """
    )
    _initialized_dbs.add(db_path)


def create_job(
    idem_key: str,
    type: str,
    payload: str,
    now: Optional[float] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> Tuple[int, bool]:
    """Inserts a job. Returns (job_id, created)."""
    conn = get_connection(db_path)
    if now is None:
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
        return cursor.lastrowid, True

    cursor.execute("SELECT id FROM jobs WHERE idem_key = ?;", (idem_key,))
    row = cursor.fetchone()
    return row["id"], False


def get_job(job_id: int, db_path: str = DEFAULT_DB_PATH) -> Optional[Dict[str, Any]]:
    """Fetches a single job row by ID."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs WHERE id = ?;", (job_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


def get_job_by_idem_key(idem_key: str, db_path: str = DEFAULT_DB_PATH) -> Optional[Dict[str, Any]]:
    """Fetches a single job row by idempotency key."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs WHERE idem_key = ?;", (idem_key,))
    row = cursor.fetchone()
    return dict(row) if row else None


def get_queue_depth(db_path: str = DEFAULT_DB_PATH) -> int:
    """Returns number of queued jobs using partial index."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) AS cnt FROM jobs WHERE status = 'queued';")
    return cursor.fetchone()["cnt"]


def claim_job(
    worker_id: str,
    lease_secs: float,
    now: Optional[float] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> Optional[Dict[str, Any]]:
    """Claims the oldest queued job in a single atomic transaction.

    WHY CLAIM IS A SINGLE TRANSACTION (BEGIN IMMEDIATE):
    Selecting the oldest queued job and updating status to 'running' must be atomic.
    BEGIN IMMEDIATE acquires a write lock before any read, preventing two workers
    from claiming the same row. SQLite >= 3.35 executes this in a single UPDATE RETURNING statement.
    Attempts are incremented on claim, not on failure.
    """
    conn = get_connection(db_path)
    if now is None:
        now = time.time()
    lease_expires_at = now + lease_secs

    conn.execute("BEGIN IMMEDIATE;")
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE jobs
            SET status = 'running',
                attempts = attempts + 1,
                lease_owner = ?,
                lease_expires_at = ?,
                updated_at = ?
            WHERE id = (
                SELECT id FROM jobs
                WHERE status = 'queued' AND not_before <= ?
                ORDER BY id ASC
                LIMIT 1
            )
            RETURNING id, idem_key, type, payload, status, attempts, max_attempts,
                      lease_owner, lease_expires_at, not_before, error, created_at, updated_at;
            """,
            (worker_id, lease_expires_at, now, now),
        )
        row = cursor.fetchone()
        conn.execute("COMMIT;")
        return dict(row) if row else None
    except Exception:
        conn.execute("ROLLBACK;")
        raise


def complete_job(
    job_id: int,
    worker_id: Optional[str] = None,
    attempt: Optional[int] = None,
    result: Optional[str] = None,
    now: Optional[float] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> bool:
    """Marks job completed with result.

    First completion wins: WHERE status IN ('queued', 'running') ensures late completions are rejected.
    """
    conn = get_connection(db_path)
    if now is None:
        now = time.time()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE jobs
        SET status = 'completed',
            result = ?,
            updated_at = ?
        WHERE id = ? AND status IN ('queued', 'running');
        """,
        (result, now, job_id),
    )
    return cursor.rowcount > 0


def heartbeat(
    job_id: int,
    worker_id: str,
    attempt: int,
    lease_secs: float,
    now: Optional[float] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> bool:
    """Extends lease only if worker_id, attempt, and running status match."""
    conn = get_connection(db_path)
    if now is None:
        now = time.time()
    new_expires = now + lease_secs
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE jobs
        SET lease_expires_at = ?,
            updated_at = ?
        WHERE id = ?
          AND status = 'running'
          AND lease_owner = ?
          AND attempts = ?;
        """,
        (new_expires, now, job_id, worker_id, attempt),
    )
    return cursor.rowcount > 0


def requeue_expired(
    now: Optional[float] = None,
    base_backoff: Optional[float] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> int:
    """Sweeps expired running jobs in two SQL statements without Python loops."""
    conn = get_connection(db_path)
    if now is None:
        now = time.time()
    if base_backoff is None:
        base_backoff = float(os.getenv("BASE_BACKOFF", "2.0"))

    conn.execute("BEGIN IMMEDIATE;")
    try:
        # Statement 1: Mark expired jobs at max_attempts as failed
        failed_cnt = conn.execute(
            """
            UPDATE jobs
            SET status = 'failed',
                error = 'lease expired, max attempts',
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = ?
            WHERE status = 'running'
              AND lease_expires_at < ?
              AND attempts >= max_attempts;
            """,
            (now, now),
        ).rowcount

        # Statement 2: Requeue remaining expired jobs with backoff and jitter computed in SQL
        requeued_cnt = conn.execute(
            """
            UPDATE jobs
            SET status = 'queued',
                lease_owner = NULL,
                lease_expires_at = NULL,
                not_before = ? + (? * (1 << attempts)) + (abs(random() % 500) / 1000.0),
                updated_at = ?
            WHERE status = 'running'
              AND lease_expires_at < ?;
            """,
            (now, base_backoff, now, now),
        ).rowcount

        conn.execute("COMMIT;")
        return failed_cnt + requeued_cnt
    except Exception:
        conn.execute("ROLLBACK;")
        raise


def fail_job(
    job_id: int,
    worker_id: str,
    attempt: int,
    error: str,
    now: Optional[float] = None,
    base_backoff: Optional[float] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> bool:
    """Fails or requeues a running job using the shared retry/fail path."""
    conn = get_connection(db_path)
    if now is None:
        now = time.time()
    if base_backoff is None:
        base_backoff = float(os.getenv("BASE_BACKOFF", "2.0"))

    conn.execute("BEGIN IMMEDIATE;")
    try:
        failed = conn.execute(
            """
            UPDATE jobs
            SET status = 'failed',
                error = ?,
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = ?
            WHERE id = ? AND status = 'running' AND attempts = ? AND lease_owner = ?
              AND attempts >= max_attempts;
            """,
            (error, now, job_id, attempt, worker_id),
        ).rowcount

        requeued = 0
        if failed == 0:
            requeued = conn.execute(
                """
                UPDATE jobs
                SET status = 'queued',
                    error = ?,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    not_before = ? + (? * (1 << attempts)) + (abs(random() % 500) / 1000.0),
                    updated_at = ?
                WHERE id = ? AND status = 'running' AND attempts = ? AND lease_owner = ?;
                """,
                (error, now, base_backoff, now, job_id, attempt, worker_id),
            ).rowcount

        conn.execute("COMMIT;")
        return (failed + requeued) > 0
    except Exception:
        conn.execute("ROLLBACK;")
        raise
