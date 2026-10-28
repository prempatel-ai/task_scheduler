"""Worker client: poll loop, heartbeat thread, and job execution."""

import argparse
import os
import sys
import threading
import time
from typing import Any, Dict, Optional

import httpx

from worker.handlers import get_handler


class HeartbeatThread:
    """Background thread that sends periodic heartbeats during job execution."""

    def __init__(
        self,
        coordinator_url: str,
        job_id: int,
        worker_id: str,
        attempt: int,
        interval: float,
    ):
        self.coordinator_url = coordinator_url
        self.job_id = job_id
        self.worker_id = worker_id
        self.attempt = attempt
        self.interval = interval
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=1.0)

    def _run(self):
        while not self.stop_event.is_set():
            if self.stop_event.wait(self.interval):
                break
            try:
                resp = httpx.post(
                    f"{self.coordinator_url}/jobs/{self.job_id}/heartbeat",
                    json={"worker_id": self.worker_id, "attempt": self.attempt},
                    timeout=5.0,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    # If extended is false, stop heartbeating but worker finishes anyway
                    if not data.get("extended", False):
                        break
            except Exception:
                pass


def process_one_job(
    coordinator_url: str,
    worker_id: str,
    client: Optional[httpx.Client] = None,
) -> Optional[bool]:
    """Claims and executes a single job. Returns True/False if a job was processed, None if queue was empty."""
    http = client or httpx.Client()
    should_close = client is None

    try:
        claim_resp = http.post(
            f"{coordinator_url}/claim",
            json={"worker_id": worker_id},
            timeout=5.0,
        )
        if claim_resp.status_code == 204 or not claim_resp.content:
            return None

        job: Dict[str, Any] = claim_resp.json()
        job_id = job["id"]
        attempt = job.get("attempt", job.get("attempts", 1))

        # Heartbeat every LEASE_SECS / 3
        lease_secs = float(os.getenv("LEASE_SECS", "30"))
        heartbeat_interval = max(0.1, lease_secs / 3.0)

        hb = HeartbeatThread(
            coordinator_url=coordinator_url,
            job_id=job_id,
            worker_id=worker_id,
            attempt=attempt,
            interval=heartbeat_interval,
        )
        hb.start()

        try:
            handler = get_handler(job["type"])
            handler(job["payload"], job_id)
        except Exception as exc:
            hb.stop()
            http.post(
                f"{coordinator_url}/jobs/{job_id}/fail",
                json={
                    "worker_id": worker_id,
                    "attempt": attempt,
                    "error": str(exc),
                },
                timeout=5.0,
            )
            return False
        else:
            hb.stop()
            http.post(
                f"{coordinator_url}/jobs/{job_id}/complete",
                json={"worker_id": worker_id, "attempt": attempt},
                timeout=5.0,
            )
            return True
    finally:
        if should_close:
            http.close()


def run_worker_loop(
    coordinator_url: str,
    worker_id: str,
    run_once: bool = False,
    poll_interval: float = 0.5,
    stop_event: Optional[threading.Event] = None,
):
    """Worker polling loop."""
    event = stop_event or threading.Event()
    with httpx.Client() as client:
        while not event.is_set():
            processed = process_one_job(coordinator_url, worker_id, client=client)
            if run_once:
                break
            if processed is None:
                if event.wait(poll_interval):
                    break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Task Queue Worker")
    parser.add_argument("--coordinator-url", default="http://127.0.0.1:8000")
    parser.add_argument("--worker-id", default=f"worker-{os.getpid()}")
    parser.add_argument("--run-once", action="store_true")
    parser.add_argument("--poll-interval", type=float, default=0.5)
    args = parser.parse_args()

    run_worker_loop(
        coordinator_url=args.coordinator_url,
        worker_id=args.worker_id,
        run_once=args.run_once,
        poll_interval=args.poll_interval,
    )
