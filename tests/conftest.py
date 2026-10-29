"""Pytest test harness: HTTPS coordinator, tokens, and worker fixtures."""

import datetime
import ipaddress
import json
import os
import socket
import subprocess
import sys
import time
from typing import Dict, List, Optional
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
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


def generate_self_signed_cert(cert_path: str, key_path: str):
    """Generates an X.509 self-signed certificate with SAN for IP:127.0.0.1 and DNS:localhost."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([
        x509.NameAttribute(x509.NameOID.COMMON_NAME, "127.0.0.1"),
    ])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1))
        .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=365))
        .add_extension(
            x509.SubjectAlternativeName([
                x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
                x509.DNSName("localhost"),
            ]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    with open(key_path, "wb") as f:
        f.write(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
    with open(cert_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))


class CoordinatorController:
    """Manages the HTTPS coordinator server process with bearer token authentication."""

    def __init__(self, tmp_path, use_https: bool = True):
        self.tmp_path = tmp_path
        self.use_https = use_https
        self.db_path = str(tmp_path / "coordinator_test.db")
        self.outputs_dir = str(tmp_path / "outputs")
        os.makedirs(self.outputs_dir, exist_ok=True)

        self.cert_path = str(tmp_path / "cert.pem")
        self.key_path = str(tmp_path / "key.pem")
        if self.use_https:
            generate_self_signed_cert(self.cert_path, self.key_path)

        self.tokens_file = str(tmp_path / "tokens.json")
        self.tokens: Dict[str, str] = {
            "w1": "secret-token-w1",
            "w2": "secret-token-w2",
            "worker-1": "secret-token-1",
            "worker-2": "secret-token-2",
            "worker-clean-1": "secret-token-clean-1",
            "worker-clean-2": "secret-token-clean-2",
            "worker-mid-crash": "secret-token-mid",
            "worker-ack-crash": "secret-token-ack",
            "worker-slow-A": "secret-token-slow-a",
            "worker-fast-B": "secret-token-fast-b",
            "worker-coord-kill": "secret-token-coord",
            "worker-crash-1": "secret-token-c1",
            "worker-crash-2": "secret-token-c2",
            "worker-crash-3": "secret-token-c3",
            "drain-worker": "secret-token-drain",
            "remote-worker": "secret-token-remote",
            "local-worker": "secret-token-local",
        }
        with open(self.tokens_file, "w", encoding="utf-8") as f:
            json.dump(self.tokens, f)

        self.client_token = "secret-client-token"
        self.port = get_free_port()
        self.scheme = "https" if self.use_https else "http"
        self.url = f"{self.scheme}://127.0.0.1:{self.port}"
        self.proc: Optional[subprocess.Popen] = None
        self.lease_secs = "1.5"
        self.sweep_secs = "0.3"
        self.base_backoff = "0.2"
        self.max_queue = "100"

    def register_worker(self, worker_id: str, token: str):
        self.tokens[worker_id] = token
        with open(self.tokens_file, "w", encoding="utf-8") as f:
            json.dump(self.tokens, f)

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
        env["TOKENS_FILE"] = self.tokens_file
        env["CLIENT_TOKEN"] = self.client_token

        cmd = [
            sys.executable,
            "-m",
            "uvicorn",
            "coordinator.main:app",
            "--host",
            "0.0.0.0",
            "--port",
            str(self.port),
            "--log-level",
            "error",
        ]
        if self.use_https:
            cmd.extend([
                "--ssl-keyfile",
                self.key_path,
                "--ssl-certfile",
                self.cert_path,
            ])

        self.proc = subprocess.Popen(cmd, env=env)

        # Wait until server responds to HTTPS requests
        deadline = time.time() + 6.0
        while time.time() < deadline:
            try:
                resp = httpx.get(
                    f"{self.url}/jobs/1",
                    headers={"Authorization": f"Bearer {self.client_token}"},
                    verify=self.cert_path if self.use_https else True,
                    timeout=1.0,
                )
                if resp.status_code in (200, 404):
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
def cert_generator():
    """Fixture providing self-signed certificate generator."""
    return generate_self_signed_cert


@pytest.fixture
def wait_for_terminal():
    """Helper fixture to poll until job reaches a terminal state."""
    def _wait(
        coordinator_url: str,
        job_id: int,
        client_token: Optional[str] = "secret-client-token",
        ca_cert: Optional[str] = None,
        timeout: float = 12.0,
        poll_interval: float = 0.1,
    ) -> Dict:
        deadline = time.time() + timeout
        headers = {}
        if client_token:
            headers["Authorization"] = f"Bearer {client_token}"

        while time.time() < deadline:
            try:
                resp = httpx.get(
                    f"{coordinator_url}/jobs/{job_id}",
                    headers=headers,
                    verify=ca_cert if ca_cert else True,
                    timeout=2.0,
                )
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
    def _wait(
        coordinator_url: str,
        job_id: int,
        client_token: Optional[str] = "secret-client-token",
        ca_cert: Optional[str] = None,
        timeout: float = 8.0,
        poll_interval: float = 0.1,
    ) -> Dict:
        deadline = time.time() + timeout
        headers = {}
        if client_token:
            headers["Authorization"] = f"Bearer {client_token}"

        while time.time() < deadline:
            try:
                resp = httpx.get(
                    f"{coordinator_url}/jobs/{job_id}",
                    headers=headers,
                    verify=ca_cert if ca_cert else True,
                    timeout=2.0,
                )
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
    """Fixture providing an HTTPS coordinator instance with auth."""
    ctrl = CoordinatorController(tmp_path, use_https=True)
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
        use_https: bool = True,
    ) -> CoordinatorController:
        ctrl = CoordinatorController(tmp_path, use_https=use_https)
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
    """Fixture to launch and automatically track/kill worker subprocesses with TLS & tokens."""
    procs: List[subprocess.Popen] = []

    def _launch(
        worker_id: str,
        env_overrides: Optional[Dict[str, str]] = None,
        run_once: bool = False,
        coordinator_url: Optional[str] = None,
        outputs_dir: Optional[str] = None,
        ca_cert: Optional[str] = None,
        worker_token: Optional[str] = None,
    ) -> subprocess.Popen:
        target_url = coordinator_url or coordinator.url
        target_outputs = outputs_dir or coordinator.outputs_dir
        cert_target = ca_cert or (coordinator.cert_path if coordinator.use_https else None)
        token_target = worker_token or coordinator.tokens.get(worker_id, f"secret-token-{worker_id}")

        env = os.environ.copy()
        env["COORDINATOR_URL"] = target_url
        env["WORKER_ID"] = worker_id
        env["WORKER_TOKEN"] = token_target
        if cert_target:
            env["CA_CERT"] = cert_target
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
            "--worker-token",
            token_target,
        ]
        if cert_target:
            cmd.extend(["--ca-cert", cert_target])
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
