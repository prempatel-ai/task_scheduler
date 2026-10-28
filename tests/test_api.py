"""Integration tests for coordinator FastAPI endpoints using TestClient."""

import os
import sys
import tempfile
import unittest
from fastapi.testclient import TestClient

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from coordinator.db import close_thread_connection, get_connection
from coordinator.main import app


class TestCoordinatorAPI(unittest.TestCase):

    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        os.environ["DB_PATH"] = self.db_path

        # TestClient triggers the FastAPI lifespan event, initializing the DB schema
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

    def test_same_idempotency_key_twice(self):
        """Same Idempotency-Key twice: same job_id, one row in table, first call 201, second 200."""
        headers = {"Idempotency-Key": "test-idem-key-1"}
        payload = {"type": "count_words", "payload": "sample.txt"}

        # First call creates the job -> 201 Created
        res1 = self.client.post("/jobs", json=payload, headers=headers)
        self.assertEqual(res1.status_code, 201)
        job_id_1 = res1.json()["job_id"]

        # Second call with same key -> 200 OK with same job_id
        res2 = self.client.post("/jobs", json=payload, headers=headers)
        self.assertEqual(res2.status_code, 200)
        job_id_2 = res2.json()["job_id"]

        self.assertEqual(job_id_1, job_id_2)

        # Verify only 1 row exists in the database
        conn = get_connection(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS cnt FROM jobs WHERE idem_key = ?;", ("test-idem-key-1",))
        self.assertEqual(cursor.fetchone()["cnt"], 1)

    def test_missing_idempotency_key_header_returns_400(self):
        """Missing or empty Idempotency-Key header returns 400 Bad Request."""
        payload = {"type": "count_words", "payload": "sample.txt"}

        # Completely missing header
        res_missing = self.client.post("/jobs", json=payload)
        self.assertEqual(res_missing.status_code, 400)

        # Empty header
        res_empty = self.client.post("/jobs", json=payload, headers={"Idempotency-Key": "   "})
        self.assertEqual(res_empty.status_code, 400)

    def test_claim_on_empty_queue_returns_204(self):
        """POST /claim on an empty queue returns 204 No Content."""
        res = self.client.post("/claim", json={"worker_id": "worker-1"})
        self.assertEqual(res.status_code, 204)
        self.assertEqual(res.text, "")

    def test_two_claims_with_two_queued_jobs_returns_two_different_jobs(self):
        """Two claims with 2 queued jobs return two different jobs."""
        res1 = self.client.post(
            "/jobs",
            json={"type": "count_words", "payload": "doc1.txt"},
            headers={"Idempotency-Key": "job-claim-key-1"},
        )
        res2 = self.client.post(
            "/jobs",
            json={"type": "count_words", "payload": "doc2.txt"},
            headers={"Idempotency-Key": "job-claim-key-2"},
        )
        job1_id = res1.json()["job_id"]
        job2_id = res2.json()["job_id"]

        # Worker 1 claims
        claim1 = self.client.post("/claim", json={"worker_id": "worker-1"})
        self.assertEqual(claim1.status_code, 200)
        claimed_job1 = claim1.json()

        # Worker 2 claims
        claim2 = self.client.post("/claim", json={"worker_id": "worker-2"})
        self.assertEqual(claim2.status_code, 200)
        claimed_job2 = claim2.json()

        self.assertNotEqual(claimed_job1["id"], claimed_job2["id"])
        self.assertEqual({claimed_job1["id"], claimed_job2["id"]}, {job1_id, job2_id})
        self.assertEqual(claimed_job1["status"], "running")
        self.assertEqual(claimed_job2["status"], "running")
        self.assertIn("attempt", claimed_job1)

    def test_complete_twice_on_one_job(self):
        """complete twice on one job: first returns updated: true, second returns updated: false."""
        res = self.client.post(
            "/jobs",
            json={"type": "count_words", "payload": "data.txt"},
            headers={"Idempotency-Key": "complete-job-key"},
        )
        job_id = res.json()["job_id"]

        claim = self.client.post("/claim", json={"worker_id": "worker-1"})
        self.assertEqual(claim.status_code, 200)

        # First complete call -> updated: true
        comp1 = self.client.post(f"/jobs/{job_id}/complete", json={"worker_id": "worker-1", "attempt": 1})
        self.assertEqual(comp1.status_code, 200)
        self.assertEqual(comp1.json(), {"updated": True})

        # Second complete call -> updated: false (not an error)
        comp2 = self.client.post(f"/jobs/{job_id}/complete", json={"worker_id": "worker-1", "attempt": 1})
        self.assertEqual(comp2.status_code, 200)
        self.assertEqual(comp2.json(), {"updated": False})

    def test_get_unknown_id_returns_404(self):
        """GET /jobs/{id} on an unknown id returns 404 Not Found."""
        res = self.client.get("/jobs/999999")
        self.assertEqual(res.status_code, 404)

        # POST complete on unknown id also returns 404
        comp_res = self.client.post("/jobs/999999/complete", json={"worker_id": "worker-1", "attempt": 1})
        self.assertEqual(comp_res.status_code, 404)


if __name__ == "__main__":
    unittest.main()
