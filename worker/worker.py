"""Worker client: poll loop, heartbeat thread, fault injection, HTTPS, and auth."""

import argparse
import json
import os
import threading
import time
from typing import Any, Dict, Optional

import httpx

from worker.handlers import get_handler


def get_http_client(ca_cert: Optional[str] = None) -> httpx.Client:
    """Builds an HTTP client configured with TLS certificate verification."""
    cert_path = ca_cert or os.getenv("CA_CERT")
    # Verify server certificate against CA_CERT path if provided, else system CAs. Never verify=False.
    verify_target = cert_path if cert_path else True
    return httpx.Client(verify=verify_target, timeout=5.0)


class HeartbeatThread:
    """Background thread that sends periodic heartbeats during job execution."""

    def __init__(
        self,
        coordinator_url: str,
        job_id: int,
        worker_id: str,
        attempt: int,
        interval: float,
        client: httpx.Client,
        worker_token: Optional[str] = None,
    ):
        self.coordinator_url = coordinator_url
        self.job_id = job_id
        self.worker_id = worker_id
        self.attempt = attempt
        self.interval = interval
        self.client = client
        self.worker_token = worker_token
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=1.0)

    def _run(self):
        headers = {}
        if self.worker_token:
            headers["Authorization"] = f"Bearer {self.worker_token}"

        while not self.stop_event.is_set():
            if self.stop_event.wait(self.interval):
                break
            try:
                resp = self.client.post(
                    f"{self.coordinator_url}/jobs/{self.job_id}/heartbeat",
                    json={"worker_id": self.worker_id, "attempt": self.attempt},
                    headers=headers,
                )
                if resp.status_code == 200 and not resp.json().get("extended", False):
                    # Stale lease: stop heartbeating but finish job anyway (first completion wins)
                    break
            except httpx.RequestError as exc:
                print(f"Worker {self.worker_id} heartbeat network error: {exc}", flush=True)


def send_complete_with_retries(
    coordinator_url: str,
    job_id: int,
    worker_id: str,
    attempt: int,
    result: Optional[str],
    client: httpx.Client,
    worker_token: Optional[str] = None,
    max_retries: int = 5,
) -> Optional[bool]:
    """Retries complete endpoint across connection failures and logs response."""
    headers = {}
    if worker_token:
        headers["Authorization"] = f"Bearer {worker_token}"

    for _ in range(max_retries):
        try:
            resp = client.post(
                f"{coordinator_url}/jobs/{job_id}/complete",
                json={
                    "worker_id": worker_id,
                    "attempt": attempt,
                    "result": result,
                },
                headers=headers,
            )
            if resp.status_code == 200:
                data = resp.json()
                updated = data.get("updated", False)
                print(
                    f"Worker {worker_id} complete response for job {job_id}: updated={updated}",
                    flush=True,
                )
                return updated
            elif resp.status_code == 413:
                print(f"Worker {worker_id} complete rejected: result too large (413)", flush=True)
                return False
        except httpx.RequestError as exc:
            print(f"Worker {worker_id} complete connection error: {exc}", flush=True)
            time.sleep(0.5)
    return None


def process_one_job(
    coordinator_url: str,
    worker_id: str,
    worker_token: Optional[str] = None,
    ca_cert: Optional[str] = None,
    client: Optional[httpx.Client] = None,
) -> Optional[bool]:
    """Claims and executes a single job using a shared keep-alive HTTP session."""
    http = client or get_http_client(ca_cert)
    should_close = client is None

    headers = {}
    if worker_token:
        headers["Authorization"] = f"Bearer {worker_token}"

    try:
        t0 = time.perf_counter()
        try:
            claim_resp = http.post(
                f"{coordinator_url}/claim",
                json={"worker_id": worker_id},
                headers=headers,
            )
            claim_log = os.getenv("CLAIM_LOG")
            if claim_log and claim_resp.status_code == 200:
                lat_ms = (time.perf_counter() - t0) * 1000.0
                try:
                    with open(claim_log, "a", encoding="utf-8") as f:
                        f.write(f"{lat_ms}\n")
                except OSError:
                    pass
        except httpx.RequestError as exc:
            print(f"Worker {worker_id} claim connection error: {exc}", flush=True)
            time.sleep(0.5)
            return None

        if claim_resp.status_code == 204 or not claim_resp.content:
            return None

        if claim_resp.status_code in (401, 403):
            print(f"Worker {worker_id} claim unauthorized ({claim_resp.status_code})", flush=True)
            return None

        job: Dict[str, Any] = claim_resp.json()
        job_id = job["id"]
        attempt = job.get("attempt", job.get("attempts", 1))

        # Fault Injection Hook: after_claim
        if os.getenv("CRASH_AT") == "after_claim":
            os._exit(1)

        hb: Optional[HeartbeatThread] = None
        if os.getenv("HEARTBEAT", "on").lower() != "off":
            lease_secs = float(os.getenv("LEASE_SECS", "30"))
            hb = HeartbeatThread(
                coordinator_url=coordinator_url,
                job_id=job_id,
                worker_id=worker_id,
                attempt=attempt,
                interval=max(0.1, lease_secs / 3.0),
                client=http,
                worker_token=worker_token,
            )
            hb.start()

        computed_result: Optional[str] = None
        try:
            handler = get_handler(job["type"])
            raw_output = handler(job["payload"], job_id)
            if isinstance(raw_output, (dict, list)):
                computed_result = json.dumps(raw_output)
            else:
                computed_result = str(raw_output)
        except Exception as exc:
            if hb:
                hb.stop()
            try:
                http.post(
                    f"{coordinator_url}/jobs/{job_id}/fail",
                    json={
                        "worker_id": worker_id,
                        "attempt": attempt,
                        "error": str(exc),
                    },
                    headers=headers,
                )
            except httpx.RequestError:
                pass
            return False
        else:
            if hb:
                hb.stop()

            # Fault Injection Hook: after_compute_before_ack
            if os.getenv("CRASH_AT") in ("after_compute_before_ack", "after_write_before_ack"):
                os._exit(1)

            send_complete_with_retries(
                coordinator_url=coordinator_url,
                job_id=job_id,
                worker_id=worker_id,
                attempt=attempt,
                result=computed_result,
                client=http,
                worker_token=worker_token,
            )
            return True
    finally:
        if should_close:
            http.close()


def run_worker_loop(
    coordinator_url: str,
    worker_id: str,
    worker_token: Optional[str] = None,
    ca_cert: Optional[str] = None,
    run_once: bool = False,
    poll_interval: float = 0.2,
    stop_event: Optional[threading.Event] = None,
):
    """Worker polling loop with session reuse and exponential poll backoff."""
    event = stop_event or threading.Event()
    current_poll = poll_interval
    with get_http_client(ca_cert) as client:
        while not event.is_set():
            processed = process_one_job(
                coordinator_url=coordinator_url,
                worker_id=worker_id,
                worker_token=worker_token,
                ca_cert=ca_cert,
                client=client,
            )
            if run_once:
                break
            if processed is None:
                # Empty queue: back off poll up to 2.0s
                if event.wait(current_poll):
                    break
                current_poll = min(2.0, current_poll * 2.0)
            else:
                # Reset poll interval on job processed
                current_poll = poll_interval


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Task Queue Worker")
    parser.add_argument(
        "--coordinator-url",
        default=os.getenv("COORDINATOR_URL", "http://127.0.0.1:8000"),
    )
    parser.add_argument(
        "--worker-id",
        default=os.getenv("WORKER_ID", f"worker-{os.getpid()}"),
    )
    parser.add_argument(
        "--worker-token",
        default=os.getenv("WORKER_TOKEN"),
    )
    parser.add_argument(
        "--ca-cert",
        default=os.getenv("CA_CERT"),
    )
    parser.add_argument("--run-once", action="store_true")
    parser.add_argument("--poll-interval", type=float, default=0.2)
    args = parser.parse_args()

    run_worker_loop(
        coordinator_url=args.coordinator_url,
        worker_id=args.worker_id,
        worker_token=args.worker_token,
        ca_cert=args.ca_cert,
        run_once=args.run_once,
        poll_interval=args.poll_interval,
    )
