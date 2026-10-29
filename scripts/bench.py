"""End-to-end benchmark script for task queue throughput and claim latency."""

import argparse
import concurrent.futures
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from typing import List

import httpx

import datetime
import ipaddress
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa

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



def get_free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def run_benchmark(num_jobs: int = 500, num_workers: int = 2, profile: bool = False):
    bench_dir = tempfile.mkdtemp(prefix="queue_bench_")
    db_path = os.path.join(bench_dir, "bench.db")
    cert_path = os.path.join(bench_dir, "cert.pem")
    key_path = os.path.join(bench_dir, "key.pem")
    tokens_path = os.path.join(bench_dir, "tokens.json")
    claim_log = os.path.join(bench_dir, "claim_latencies.txt")
    prof_path = os.path.join(bench_dir, "coordinator.prof")

    port = get_free_port()
    generate_self_signed_cert(cert_path, key_path)

    tokens = {f"worker-{i}": f"token-w-{i}" for i in range(1, num_workers + 1)}
    with open(tokens_path, "w", encoding="utf-8") as f:
        json.dump(tokens, f)

    client_token = "bench-client-token"
    coord_url = f"https://127.0.0.1:{port}"

    env = os.environ.copy()
    env["DB_PATH"] = db_path
    env["LEASE_SECS"] = "30"
    env["SWEEP_SECS"] = "2"
    env["TOKENS_FILE"] = tokens_path
    env["CLIENT_TOKEN"] = client_token
    env["MAX_QUEUE"] = str(num_jobs + 100)

    # Launch coordinator process (optionally with cProfile)
    if profile:
        cmd = [
            sys.executable,
            "-m",
            "cProfile",
            "-o",
            prof_path,
            "-m",
            "uvicorn",
            "coordinator.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--ssl-keyfile",
            key_path,
            "--ssl-certfile",
            cert_path,
            "--log-level",
            "warning",
        ]
    else:
        cmd = [
            sys.executable,
            "-m",
            "uvicorn",
            "coordinator.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--ssl-keyfile",
            key_path,
            "--ssl-certfile",
            cert_path,
            "--log-level",
            "warning",
        ]

    coord_proc = subprocess.Popen(cmd, env=env)

    # Wait for coordinator startup
    deadline = time.time() + 8.0
    while time.time() < deadline:
        try:
            resp = httpx.get(
                f"{coord_url}/jobs/1",
                headers={"Authorization": f"Bearer {client_token}"},
                verify=cert_path,
                timeout=1.0,
            )
            if resp.status_code in (200, 404):
                break
        except httpx.RequestError:
            pass
        time.sleep(0.1)

    workers: List[subprocess.Popen] = []
    worker_env = os.environ.copy()
    worker_env["JOB_SLEEP"] = "0"
    worker_env["CLAIM_LOG"] = claim_log
    worker_env["CA_CERT"] = cert_path

    # Start workers
    for i in range(1, num_workers + 1):
        w_id = f"worker-{i}"
        w_token = tokens[w_id]
        w_env = worker_env.copy()
        w_env["WORKER_ID"] = w_id
        w_env["WORKER_TOKEN"] = w_token
        p = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "worker.worker",
                "--coordinator-url",
                coord_url,
                "--worker-id",
                w_id,
                "--worker-token",
                w_token,
                "--ca-cert",
                cert_path,
                "--poll-interval",
                "0.05",
            ],
            env=w_env,
        )
        workers.append(p)

    start_time = time.perf_counter()

    # Submit N jobs concurrently
    headers = {
        "Authorization": f"Bearer {client_token}",
        "Content-Type": "application/json",
    }

    def submit_job(idx: int):
        h = {**headers, "Idempotency-Key": f"bench-job-{idx}"}
        with httpx.Client(verify=cert_path, timeout=10.0) as client:
            client.post(
                f"{coord_url}/jobs",
                json={"type": "count_words", "payload": "apple banana orange cherry grape"},
                headers=h,
            )

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(submit_job, range(num_jobs)))

    # Wait until all N jobs reach 'completed' in SQLite
    while True:
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM jobs WHERE status = 'completed';")
            completed = cursor.fetchone()[0]
            conn.close()
            if completed >= num_jobs:
                break
        except sqlite3.OperationalError:
            pass
        time.sleep(0.05)

    end_time = time.perf_counter()
    duration = end_time - start_time
    jobs_per_sec = num_jobs / duration if duration > 0 else 0.0

    # Stop workers
    for p in workers:
        p.kill()
        p.wait()

    # Stop coordinator
    coord_proc.terminate()
    try:
        coord_proc.wait(timeout=3.0)
    except subprocess.TimeoutExpired:
        coord_proc.kill()
        coord_proc.wait()

    # Read claim latencies
    latencies = []
    if os.path.exists(claim_log):
        with open(claim_log, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        latencies.append(float(line))
                    except ValueError:
                        pass

    latencies.sort()
    if latencies:
        p50 = latencies[len(latencies) // 2]
        p95 = latencies[int(len(latencies) * 0.95)]
    else:
        p50, p95 = 0.0, 0.0

    print(f"Benchmark Results (N={num_jobs}, W={num_workers}):")
    print(f"  Duration:          {duration:.2f} s")
    print(f"  Throughput:        {jobs_per_sec:.1f} jobs/s")
    print(f"  Claim Latency p50: {p50:.2f} ms")
    print(f"  Claim Latency p95: {p95:.2f} ms")

    if profile and os.path.exists(prof_path):
        import pstats
        print("\n--- Coordinator Top Cumulative Costs ---")
        p = pstats.Stats(prof_path)
        p.strip_dirs().sort_stats("cumulative").print_stats(15)

    shutil.rmtree(bench_dir, ignore_errors=True)
    return {
        "jobs_per_sec": jobs_per_sec,
        "p50_ms": p50,
        "p95_ms": p95,
        "duration": duration,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--jobs", type=int, default=500)
    parser.add_argument("-w", "--workers", type=int, default=2)
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()

    run_benchmark(num_jobs=args.jobs, num_workers=args.workers, profile=args.profile)
