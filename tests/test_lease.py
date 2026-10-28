"""Tests for lease, heartbeat, sweeper, and crash recovery."""

import os
import random
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from fastapi.testclient import TestClient

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from coordinator.db import (
    claim_job,
    close_thread_connection,
    complete_job,
    create_job,
    get_connection,
    get_job,
    heartbeat,
    init_db,
    requeue_expired,
)
from coordinator.main import app


def get_free_port() -> int:
    """Finds an available TCP port for real server testing."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class TestLeaseAndFaults(unittest.TestCase):

    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        os.environ["DB_PATH"] = self.db_path
        init_db(self.db_path)
        self.client = TestClient(app)

    def tearDown(self):
        close_thread_connection(self.db_path)
        for ext in ("", "-wal", "-shm"):
            file_to_remove = self.db_path + ext
            if os.path.exists(file_to_remove):
                try:
                    os.remove(file_to_remove)
                except OSError:
                    pass

    def test_claim_expire_requeue(self):
        """Claim, no heartbeat, now moves past the lease, then requeue_expired:

        status queued, attempts 1, not_before in the future.
        """
        job_id, _ = create_job("lease-job-1", "count_words", "sample", now=1000.0, db_path=self.db_path)

        # Claim with 10s lease at now=1000.0
        job = claim_job("worker-1", lease_secs=10.0, now=1000.0, db_path=self.db_path)
        self.assertIsNotNone(job)
        self.assertEqual(job["id"], job_id)
        self.assertEqual(job["status"], "running")
        self.assertEqual(job["attempts"], 1)
        self.assertEqual(job["lease_expires_at"], 1010.0)

        # Move clock past lease (now=1015.0) and trigger requeue_expired
        requeued_count = requeue_expired(now=1015.0, base_backoff=2.0, db_path=self.db_path)
        self.assertEqual(requeued_count, 1)

        row = get_job(job_id, db_path=self.db_path)
        self.assertEqual(row["status"], "queued")
        self.assertEqual(row["attempts"], 1)
        self.assertIsNone(row["lease_owner"])
        self.assertIsNone(row["lease_expires_at"])
        # not_before is in the future (> 1015.0)
        self.assertGreater(row["not_before"], 1015.0)

    def test_heartbeat_extends_lease(self):
        """Heartbeat before expiry extends the lease, and requeue_expired leaves the job alone."""
        job_id, _ = create_job("lease-job-2", "count_words", "sample", now=1000.0, db_path=self.db_path)
        claim_job("worker-1", lease_secs=10.0, now=1000.0, db_path=self.db_path)

        # Heartbeat at now=1005.0 extends lease by 10s -> new expires at 1015.0
        extended = heartbeat(job_id, "worker-1", attempt=1, lease_secs=10.0, now=1005.0, db_path=self.db_path)
        self.assertTrue(extended)

        # At now=1012.0, original lease (1010.0) has passed, but extended lease (1015.0) is active
        requeued_count = requeue_expired(now=1012.0, db_path=self.db_path)
        self.assertEqual(requeued_count, 0)

        row = get_job(job_id, db_path=self.db_path)
        self.assertEqual(row["status"], "running")
        self.assertEqual(row["lease_expires_at"], 1015.0)

    def test_heartbeat_wrong_worker_or_attempt(self):
        """Heartbeat with wrong worker_id or attempt returns extended: false."""
        job_id, _ = create_job("lease-job-3", "count_words", "sample", now=1000.0, db_path=self.db_path)
        claim_job("worker-1", lease_secs=10.0, now=1000.0, db_path=self.db_path)

        # Wrong worker_id
        res_wrong_worker = self.client.post(
            f"/jobs/{job_id}/heartbeat",
            json={"worker_id": "wrong-worker", "attempt": 1},
        )
        self.assertEqual(res_wrong_worker.status_code, 200)
        self.assertEqual(res_wrong_worker.json(), {"extended": False})

        # Wrong attempt
        res_wrong_attempt = self.client.post(
            f"/jobs/{job_id}/heartbeat",
            json={"worker_id": "worker-1", "attempt": 2},
        )
        self.assertEqual(res_wrong_attempt.status_code, 200)
        self.assertEqual(res_wrong_attempt.json(), {"extended": False})

    def test_max_attempts_expires_to_failed(self):
        """A job that expires max_attempts times ends failed with an error set."""
        job_id, _ = create_job("lease-job-4", "count_words", "sample", now=1000.0, db_path=self.db_path)

        # Default max_attempts = 3
        current_time = 1000.0
        for attempt_num in range(1, 4):
            # Claim job
            claimed = claim_job("worker-1", lease_secs=10.0, now=current_time, db_path=self.db_path)
            self.assertIsNotNone(claimed)
            self.assertEqual(claimed["attempts"], attempt_num)

            # Expire job past lease
            current_time += 15.0
            requeue_expired(now=current_time, base_backoff=2.0, db_path=self.db_path)

            # If not yet at max, advance time past not_before to make it claimable again
            if attempt_num < 3:
                row = get_job(job_id, db_path=self.db_path)
                self.assertEqual(row["status"], "queued")
                current_time = row["not_before"] + 1.0

        # After 3rd expiration, status should be failed
        final_row = get_job(job_id, db_path=self.db_path)
        self.assertEqual(final_row["status"], "failed")
        self.assertEqual(final_row["attempts"], 3)
        self.assertIn("lease expired, max attempts", final_row["error"])

    def test_queued_job_with_future_not_before_not_claimable(self):
        """A queued job with not_before in the future is not claimable."""
        job_id, _ = create_job("lease-job-5", "count_words", "sample", now=1000.0, db_path=self.db_path)

        # Manually set not_before to 2000.0
        conn = get_connection(self.db_path)
        conn.execute("UPDATE jobs SET not_before = 2000.0 WHERE id = ?;", (job_id,))

        # At now=1500.0, job should NOT be claimable
        claimed_early = claim_job("worker-1", lease_secs=10.0, now=1500.0, db_path=self.db_path)
        self.assertIsNone(claimed_early)

        # At now=2000.0, job becomes claimable
        claimed_ready = claim_job("worker-1", lease_secs=10.0, now=2000.0, db_path=self.db_path)
        self.assertIsNotNone(claimed_ready)
        self.assertEqual(claimed_ready["id"], job_id)

    def test_slow_worker_completion_race(self):
        """Slow worker: job expires and is requeued, the old worker then calls complete.

        Status becomes completed, and the later complete from the second worker returns updated: false.
        """
        job_id, _ = create_job("lease-job-6", "count_words", "sample", now=1000.0, db_path=self.db_path)

        # Worker 1 claims at now=1000.0
        claim_job("worker-1", lease_secs=10.0, now=1000.0, db_path=self.db_path)

        # Lease expires at now=1015.0 and gets requeued
        requeue_expired(now=1015.0, db_path=self.db_path)
        row = get_job(job_id, db_path=self.db_path)
        self.assertEqual(row["status"], "queued")

        # Worker 2 claims job at now=1050.0 (past not_before)
        claim_job("worker-2", lease_secs=10.0, now=row["not_before"] + 1.0, db_path=self.db_path)
        row = get_job(job_id, db_path=self.db_path)
        self.assertEqual(row["status"], "running")
        self.assertEqual(row["attempts"], 2)

        # Worker 1 (slow worker) finishes late and calls complete
        first_complete = complete_job(job_id, worker_id="worker-1", attempt=1, db_path=self.db_path)
        self.assertTrue(first_complete)
        row = get_job(job_id, db_path=self.db_path)
        self.assertEqual(row["status"], "completed")

        # Worker 2 later finishes and calls complete -> returns False
        second_complete = complete_job(job_id, worker_id="worker-2", attempt=2, db_path=self.db_path)
        self.assertFalse(second_complete)
        row = get_job(job_id, db_path=self.db_path)
        self.assertEqual(row["status"], "completed")

    def test_real_kill_worker_and_recover(self):
        """Real kill test: run worker, kill -9 it during sleep, start another worker.

        The job ends completed, and GET /jobs/{id} shows attempts 2.
        """
        port = get_free_port()
        env = os.environ.copy()
        env["DB_PATH"] = self.db_path
        env["LEASE_SECS"] = "2"
        env["SWEEP_SECS"] = "0.5"

        # Start coordinator server in a separate process
        coord_proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "coordinator.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--log-level",
                "error",
            ],
            env=env,
        )

        try:
            # Wait for coordinator to start listening
            time.sleep(1.2)
            import httpx

            # Submit a job that sleeps for 4 seconds
            submit_resp = httpx.post(
                f"http://127.0.0.1:{port}/jobs",
                json={"type": "sleep", "payload": "4"},
                headers={"Idempotency-Key": "kill-test-job-key"},
                timeout=5.0,
            )
            self.assertEqual(submit_resp.status_code, 201)
            job_id = submit_resp.json()["job_id"]

            # Launch Worker 1 in a separate process
            worker1 = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "worker.worker",
                    "--coordinator-url",
                    f"http://127.0.0.1:{port}",
                    "--worker-id",
                    "worker-kill-1",
                    "--run-once",
                ],
                env=env,
            )

            # Wait until Worker 1 has claimed the job
            claimed = False
            for _ in range(30):
                resp = httpx.get(f"http://127.0.0.1:{port}/jobs/{job_id}", timeout=5.0)
                if resp.status_code == 200 and resp.json().get("status") == "running":
                    claimed = True
                    break
                time.sleep(0.1)
            self.assertTrue(claimed, "Worker 1 did not claim the job in time")

            # Kill Worker 1 mid-execution (SIGKILL on Unix / TerminateProcess on Windows)
            worker1.kill()
            worker1.wait()

            # Wait for the sweeper to detect lease expiration and requeue the job
            requeued = False
            for _ in range(40):
                resp = httpx.get(f"http://127.0.0.1:{port}/jobs/{job_id}", timeout=5.0)
                if resp.status_code == 200 and resp.json().get("status") == "queued":
                    requeued = True
                    break
                time.sleep(0.2)
            self.assertTrue(requeued, "Coordinator sweeper did not requeue expired job")

            # Wait for not_before delay so job is ready to be claimed by worker 2
            for _ in range(40):
                resp = httpx.get(f"http://127.0.0.1:{port}/jobs/{job_id}", timeout=5.0)
                if resp.status_code == 200:
                    not_before = resp.json().get("not_before", 0)
                    if time.time() >= not_before:
                        break
                time.sleep(0.2)

            # Launch Worker 2 to pick up the requeued job
            worker2 = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "worker.worker",
                    "--coordinator-url",
                    f"http://127.0.0.1:{port}",
                    "--worker-id",
                    "worker-kill-2",
                    "--run-once",
                ],
                env=env,
            )

            # Wait for Worker 2 to complete the job
            worker2.wait(timeout=15.0)

            # Verify final state: completed and attempts == 2
            resp = httpx.get(f"http://127.0.0.1:{port}/jobs/{job_id}", timeout=5.0)
            self.assertEqual(resp.status_code, 200)
            data = resp.json()
            self.assertEqual(data["status"], "completed")
            self.assertEqual(data["attempts"], 2)

            # Result file exists in outputs/
            result_file = os.path.join("outputs", f"{job_id}.txt")
            self.assertTrue(os.path.exists(result_file))
        finally:
            coord_proc.terminate()
            coord_proc.wait()


if __name__ == "__main__":
    unittest.main()
