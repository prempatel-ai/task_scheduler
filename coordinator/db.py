"""SQLite persistence layer: schema, claim, complete."""

import os
import random
import sqlite3
import threading
import time
from typing import Any, Dict, Optional, Tuple

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
    now: Optional[float] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> Tuple[int, bool]:
    """Inserts a job.

    Returns (job_id, created) where created is True if newly inserted,
    or False if idem_key already existed.
    """
    conn = get_connection(db_path)
    init_db(db_path)
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
    """Fetches a single job row by ID, or None if not found."""
    conn = get_connection(db_path)
    init_db(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs WHERE id = ?;", (job_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


def claim_job(
    worker_id: str,
    lease_secs: float,
    now: Optional[float] = None,
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
        if now is None:
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
    now: Optional[float] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> bool:
    """Marks a job as completed if it is currently queued or running.

    Returns True if a row was updated, False otherwise (e.g. if already completed).
    """
    conn = get_connection(db_path)
    init_db(db_path)
    if now is None:
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


def heartbeat(
    job_id: int,
    worker_id: str,
    attempt: int,
    lease_secs: float,
    now: Optional[float] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> bool:
    """Extends lease_expires_at only if status is running, lease_owner matches,

    and attempts equals attempt. Returns True if extended, False otherwise.
    """
    conn = get_connection(db_path)
    init_db(db_path)
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
    base_backoff: float = 2.0,
    db_path: str = DEFAULT_DB_PATH,
) -> int:
    """Requeues or fails expired running jobs in a single atomic transaction.

    For each running job with lease_expires_at < now:
    - If attempts >= max_attempts: set status='failed' and error='lease expired, max attempts'.
    - Otherwise: set status='queued', clear lease fields, and set
      not_before = now + base_backoff * 2^attempts + random jitter.
    """
    conn = get_connection(db_path)
    init_db(db_path)
    if now is None:
        now = time.time()

    conn.execute("BEGIN IMMEDIATE;")
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, attempts, max_attempts
            FROM jobs
            WHERE status = 'running' AND lease_expires_at < ?;
            """,
            (now,),
        )
        expired_jobs = cursor.fetchall()
        for job in expired_jobs:
            job_id = job["id"]
            attempts = job["attempts"]
            max_attempts = job["max_attempts"]
            if attempts >= max_attempts:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = 'failed',
                        error = 'lease expired, max attempts',
                        lease_owner = NULL,
                        lease_expires_at = NULL,
                        updated_at = ?
                    WHERE id = ?;
                    """,
                    (now, job_id),
                )
            else:
                backoff = base_backoff * (2 ** attempts) + random.uniform(0.0, 0.5)
                not_before = now + backoff
                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = 'queued',
                        lease_owner = NULL,
                        lease_expires_at = NULL,
                        not_before = ?,
                        updated_at = ?
                    WHERE id = ?;
                    """,
                    (not_before, now, job_id),
                )
        conn.execute("COMMIT;")
        return len(expired_jobs)
    except Exception:
        conn.execute("ROLLBACK;")
        raise


def fail_job(
    job_id: int,
    worker_id: str,
    attempt: int,
    error: str,
    now: Optional[float] = None,
    base_backoff: float = 2.0,
    db_path: str = DEFAULT_DB_PATH,
) -> bool:
    """Handles explicit job failure reported by a worker.

    Only updates if status is running and attempts equals attempt.
    Applies the same retry/fail path as lease expiration.
    """
    conn = get_connection(db_path)
    init_db(db_path)
    if now is None:
        now = time.time()

    conn.execute("BEGIN IMMEDIATE;")
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, attempts, max_attempts
            FROM jobs
            WHERE id = ? AND status = 'running' AND attempts = ? AND lease_owner = ?;
            """,
            (job_id, attempt, worker_id),
        )
        row = cursor.fetchone()
        if not row:
            conn.execute("COMMIT;")
            return False

        attempts = row["attempts"]
        max_attempts = row["max_attempts"]
        if attempts >= max_attempts:
            cursor.execute(
                """
                UPDATE jobs
                SET status = 'failed',
                    error = ?,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = ?
                WHERE id = ?;
                """,
                (error, now, job_id),
            )
        else:
            backoff = base_backoff * (2 ** attempts) + random.uniform(0.0, 0.5)
            not_before = now + backoff
            cursor.execute(
                """
                UPDATE jobs
                SET status = 'queued',
                    error = ?,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    not_before = ?,
                    updated_at = ?
                WHERE id = ?;
                """,
                (error, not_before, now, job_id),
            )
        conn.execute("COMMIT;")
        return True
    except Exception:
        conn.execute("ROLLBACK;")
        raise
