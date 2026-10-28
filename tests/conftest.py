"""Pytest test harness: coordinator and worker fixtures with subprocess lifecycle."""

import os
import socket
import subprocess
import sys
import time
from typing import Dict, List, Optional
import httpx
import pytest

# Ensure project root is at the very beginning of sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def get_free_port() -> int:
    """Finds an available TCP port."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class CoordinatorController:
    """Manages the coordinator server process for fault testing."""

    def __init__(self, tmp_path):
        self.tmp_path = tmp_path
        self.db_path = str(tmp_path / "coordinator_test.db")
        self.outputs_dir = str(tmp_path / "outputs")
        os.makedirs(self.outputs_dir, exist_ok=True)
        self.port = get_free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.proc: Optional[subprocess.Popen] = None
        self.lease_secs = "1.5"
        self.sweep_secs = "0.3"
        self.base_backoff = "0.2"
        self.max_queue = "100"

    def start(
        self,
        lease_secs: str = "1.5",
        sweep_secs: str = "0.3",
        base_backoff: str = "0.2",
        max_queue: str = "100",
    ):
        self.lease_secs = lease_secs
        self.sweep_secs = sweep_secs
        self.base_backoff = base_backoff
        self.max_queue = max_queue

        env = os.environ.copy()
        env["DB_PATH"] = self.db_path
        env["LEASE_SECS"] = self.lease_secs
        env["SWEEP_SECS"] = self.sweep_secs
        env["BASE_BACKOFF"] = self.base_backoff
        env["MAX_QUEUE"] = self.max_queue

        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "coordinator.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--log-level",
                "error",
            ],
            env=env,
        )

        # Wait until server responds to HTTP requests
        deadline = time.time() + 6.0
        while time.time() < deadline:
            try:
                resp = httpx.get(f"{self.url}/claim", timeout=1.0)
                if resp.status_code in (200, 204):
                    return
            except httpx.RequestError:
                pass
            time.sleep(0.1)

    def kill(self):
        """Kills coordinator immediately via SIGKILL / TerminateProcess."""
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()

    def restart(self):
        """Restarts coordinator against the exact same database."""
        self.kill()
        self.start(
            lease_secs=self.lease_secs,
            sweep_secs=self.sweep_secs,
            base_backoff=self.base_backoff,
            max_queue=self.max_queue,
        )


@pytest.fixture
def wait_for_terminal():
    """Helper fixture to poll until job reaches a terminal state."""
    def _wait(coordinator_url: str, job_id: int, timeout: float = 12.0, poll_interval: float = 0.1) -> Dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                resp = httpx.get(f"{coordinator_url}/jobs/{job_id}", timeout=2.0)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("status") in ("completed", "failed"):
                        return data
            except httpx.RequestError:
                pass
            time.sleep(poll_interval)
        raise TimeoutError(f"Job {job_id} did not reach terminal state within {timeout}s")

    return _wait


@pytest.fixture
def wait_for_queued_ready():
    """Helper fixture to poll until a requeued job's not_before delay has elapsed."""
    def _wait(coordinator_url: str, job_id: int, timeout: float = 8.0, poll_interval: float = 0.1) -> Dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                resp = httpx.get(f"{coordinator_url}/jobs/{job_id}", timeout=2.0)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("status") == "queued" and time.time() >= data.get("not_before", 0):
                        return data
            except httpx.RequestError:
                pass
            time.sleep(poll_interval)
        raise TimeoutError(f"Job {job_id} was not ready in queue within {timeout}s")

    return _wait


@pytest.fixture
def coordinator(tmp_path):
    """Fixture providing a running coordinator instance, cleanly torn down after test."""
    ctrl = CoordinatorController(tmp_path)
    ctrl.start()
    yield ctrl
    ctrl.kill()


@pytest.fixture
def coordinator_factory(tmp_path):
    """Factory fixture to create coordinators with custom settings."""
    controllers: List[CoordinatorController] = []

    def _create(
        max_queue: str = "100",
        lease_secs: str = "1.5",
        sweep_secs: str = "0.3",
        base_backoff: str = "0.2",
    ) -> CoordinatorController:
        ctrl = CoordinatorController(tmp_path)
        ctrl.start(
            lease_secs=lease_secs,
            sweep_secs=sweep_secs,
            base_backoff=base_backoff,
            max_queue=max_queue,
        )
        controllers.append(ctrl)
        return ctrl

    yield _create

    for c in controllers:
        c.kill()


@pytest.fixture
def worker_launcher(coordinator):
    """Fixture to launch and automatically track/kill worker subprocesses."""
    procs: List[subprocess.Popen] = []

    def _launch(
        worker_id: str,
        env_overrides: Optional[Dict[str, str]] = None,
        run_once: bool = False,
        coordinator_url: Optional[str] = None,
        outputs_dir: Optional[str] = None,
    ) -> subprocess.Popen:
        target_url = coordinator_url or coordinator.url
        target_outputs = outputs_dir or coordinator.outputs_dir

        env = os.environ.copy()
        env["OUTPUTS_DIR"] = target_outputs
        env["LEASE_SECS"] = coordinator.lease_secs
        if env_overrides:
            env.update(env_overrides)

        cmd = [
            sys.executable,
            "-m",
            "worker.worker",
            "--coordinator-url",
            target_url,
            "--worker-id",
            worker_id,
        ]
        if run_once:
            cmd.append("--run-once")

        proc = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        procs.append(proc)
        return proc

    yield _launch

    # Teardown: ensure all worker subprocesses are killed
    for p in procs:
        if p.poll() is None:
            p.kill()
            try:
                p.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                pass
