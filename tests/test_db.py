"""Unit tests for coordinator database layer."""

import os
import sys
import tempfile
import threading
import unittest

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from coordinator.db import (
    claim_job,
    close_thread_connection,
    complete_job,
    create_job,
    get_connection,
    init_db,
)


class TestDatabaseLayer(unittest.TestCase):

    def setUp(self):
        # Create a fresh temporary database file for each test
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        init_db(self.db_path)

    def tearDown(self):
        close_thread_connection(self.db_path)
        for ext in ("", "-wal", "-shm"):
            file_to_remove = self.db_path + ext
            if os.path.exists(file_to_remove):
                try:
                    os.remove(file_to_remove)
                except OSError:
                    pass

    def test_concurrent_claims_exact_once(self):
        """10 jobs, 4 threads claiming at once. Each job is claimed exactly once."""
        num_jobs = 10
        num_threads = 4

        # Enqueue 10 distinct jobs
        job_ids = [
            create_job(f"key-{i}", "count_words", f"payload-{i}", db_path=self.db_path)[0]
            for i in range(num_jobs)
        ]
        self.assertEqual(len(job_ids), num_jobs)

        claimed_ids = []
        lock = threading.Lock()

        def worker_loop(worker_id: str):
            while True:
                job = claim_job(worker_id=worker_id, lease_secs=10.0, db_path=self.db_path)
                if job is None:
                    break
                with lock:
                    claimed_ids.append(job["id"])
            close_thread_connection(self.db_path)

        threads = [
            threading.Thread(target=worker_loop, args=(f"worker-{i}",))
            for i in range(num_threads)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Each job must be claimed exactly once
        self.assertEqual(len(claimed_ids), num_jobs)
        self.assertEqual(sorted(claimed_ids), sorted(job_ids))

    def test_create_job_idempotency(self):
        """create_job twice with the same key returns one id and one row."""
        key = "idem-key-duplicate-check"
        id1, created1 = create_job(key, "count_words", "sample payload 1", db_path=self.db_path)
        id2, created2 = create_job(key, "count_words", "sample payload 2", db_path=self.db_path)

        self.assertEqual(id1, id2)
        self.assertTrue(created1)
        self.assertFalse(created2)

        conn = get_connection(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS cnt FROM jobs WHERE idem_key = ?;", (key,))
        count = cursor.fetchone()["cnt"]
        self.assertEqual(count, 1)

    def test_complete_job_twice(self):
        """complete_job called twice on the same job. The second call changes nothing."""
        job_id, _ = create_job("complete-test-key", "count_words", "payload", db_path=self.db_path)
        claimed = claim_job("worker-test", lease_secs=30.0, db_path=self.db_path)
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["id"], job_id)

        # First completion: status changes from running to completed -> returns True
        first_call = complete_job(job_id, worker_id="worker-test", attempt=1, db_path=self.db_path)
        self.assertTrue(first_call)

        # Second completion: status is already completed -> returns False
        second_call = complete_job(job_id, worker_id="worker-test", attempt=1, db_path=self.db_path)
        self.assertFalse(second_call)

        # Verify final status remains completed
        conn = get_connection(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM jobs WHERE id = ?;", (job_id,))
        self.assertEqual(cursor.fetchone()["status"], "completed")

    def test_claim_increments_attempts(self):
        """claim_job increments attempts by 1."""
        job_id, _ = create_job("attempt-test-key", "count_words", "payload", db_path=self.db_path)

        conn = get_connection(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT attempts, status FROM jobs WHERE id = ?;", (job_id,))
        initial = cursor.fetchone()
        self.assertEqual(initial["attempts"], 0)
        self.assertEqual(initial["status"], "queued")

        claimed = claim_job("worker-attempt-test", lease_secs=15.0, db_path=self.db_path)
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["id"], job_id)
        self.assertEqual(claimed["attempts"], 1)
        self.assertEqual(claimed["status"], "running")

        cursor.execute("SELECT attempts, status FROM jobs WHERE id = ?;", (job_id,))
        updated = cursor.fetchone()
        self.assertEqual(updated["attempts"], 1)
        self.assertEqual(updated["status"], "running")


if __name__ == "__main__":
    unittest.main()
