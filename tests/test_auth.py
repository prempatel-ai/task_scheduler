"""Tests for HTTPS, bearer token authentication, and multi-machine worker isolation."""

import time
import httpx
import pytest


def test_no_token_or_bad_token_on_worker_endpoint_returns_401(coordinator):
    """No token or a bad token on a worker endpoint: 401."""
    # No token
    res_no_token = httpx.post(
        f"{coordinator.url}/claim",
        json={"worker_id": "w1"},
        verify=coordinator.cert_path,
        timeout=5.0,
    )
    assert res_no_token.status_code == 401

    # Bad token
    res_bad_token = httpx.post(
        f"{coordinator.url}/claim",
        json={"worker_id": "w1"},
        headers={"Authorization": "Bearer completely-invalid-token"},
        verify=coordinator.cert_path,
        timeout=5.0,
    )
    assert res_bad_token.status_code == 401


def test_valid_token_wrong_worker_id_returns_403(coordinator):
    """Valid token for w1 with worker_id w2: 403."""
    # Token for w1 used with worker_id w2
    token_w1 = coordinator.tokens["w1"]
    res = httpx.post(
        f"{coordinator.url}/claim",
        json={"worker_id": "w2"},
        headers={"Authorization": f"Bearer {token_w1}"},
        verify=coordinator.cert_path,
        timeout=5.0,
    )
    assert res.status_code == 403


def test_worker_token_on_client_endpoints_rejected(coordinator):
    """A worker token on POST /jobs or GET /jobs/{id} is rejected (401)."""
    token_w1 = coordinator.tokens["w1"]

    # POST /jobs with worker token
    submit_res = httpx.post(
        f"{coordinator.url}/jobs",
        json={"type": "count_words", "payload": "sample"},
        headers={
            "Idempotency-Key": "auth-check-key",
            "Authorization": f"Bearer {token_w1}",
        },
        verify=coordinator.cert_path,
        timeout=5.0,
    )
    assert submit_res.status_code == 401

    # GET /jobs/1 with worker token
    get_res = httpx.get(
        f"{coordinator.url}/jobs/1",
        headers={"Authorization": f"Bearer {token_w1}"},
        verify=coordinator.cert_path,
        timeout=5.0,
    )
    assert get_res.status_code == 401


def test_plain_http_to_https_port_fails(coordinator):
    """Plain http:// request to the HTTPS port fails."""
    plain_url = f"http://127.0.0.1:{coordinator.port}/jobs"
    with pytest.raises(httpx.RequestError):
        httpx.get(plain_url, timeout=2.0)


def test_wrong_ca_cert_worker_does_not_crash(coordinator, worker_launcher, cert_generator, tmp_path):
    """Worker given wrong CA_CERT cannot connect, and does not crash (retries and logs)."""
    wrong_cert = str(tmp_path / "wrong_cert.pem")
    wrong_key = str(tmp_path / "wrong_key.pem")
    cert_generator(wrong_cert, wrong_key)

    worker = worker_launcher(
        "worker-tls-fail",
        ca_cert=wrong_cert,
        run_once=False,
    )

    # Let the worker attempt to connect over TLS
    time.sleep(1.5)

    # Verify worker did not crash despite SSL validation failures
    assert worker.poll() is None, "Worker crashed unexpectedly on TLS error"

    worker.kill()
    worker.wait(timeout=2.0)


def test_remote_worker_finishes_five_jobs(coordinator, worker_launcher, wait_for_terminal):
    """Worker on the second machine finishes 5 jobs. All 5 show completed with a result."""
    job_ids = []
    headers = {
        "Authorization": f"Bearer {coordinator.client_token}",
    }

    # Submit 5 distinct jobs
    for i in range(5):
        h = {**headers, "Idempotency-Key": f"remote-multi-job-{i}"}
        res = httpx.post(
            f"{coordinator.url}/jobs",
            json={"type": "count_words", "payload": f"word count test number {i}"},
            headers=h,
            verify=coordinator.cert_path,
            timeout=5.0,
        )
        assert res.status_code == 201
        job_ids.append(res.json()["job_id"])

    # Launch remote worker over HTTPS with its own token
    remote_worker = worker_launcher(
        "remote-worker",
        worker_token=coordinator.tokens["remote-worker"],
        run_once=False,
    )

    # All 5 jobs reach completed with results stored directly in the database
    for jid in job_ids:
        data = wait_for_terminal(
            coordinator.url,
            jid,
            client_token=coordinator.client_token,
            ca_cert=coordinator.cert_path,
            timeout=12.0,
        )
        assert data["status"] == "completed"
        assert data["result"] == "5"

    remote_worker.kill()
    remote_worker.wait(timeout=2.0)


def test_kill_remote_worker_local_worker_recovers(
    coordinator,
    worker_launcher,
    wait_for_terminal,
    wait_for_queued_ready,
):
    """Kill remote worker mid-job. A local worker finishes the job: one result, attempts 2."""
    submit = httpx.post(
        f"{coordinator.url}/jobs",
        json={"type": "count_words", "payload": "one two three four"},
        headers={
            "Idempotency-Key": "kill-remote-worker-key",
            "Authorization": f"Bearer {coordinator.client_token}",
        },
        verify=coordinator.cert_path,
        timeout=5.0,
    )
    assert submit.status_code == 201
    job_id = submit.json()["job_id"]

    # Remote worker claims and crashes mid-job
    remote_worker = worker_launcher(
        "remote-worker",
        worker_token=coordinator.tokens["remote-worker"],
        env_overrides={"CRASH_AT": "mid_job"},
        run_once=True,
    )
    remote_worker.wait(timeout=5.0)
    assert remote_worker.returncode == 1

    # Wait for coordinator sweeper to requeue
    wait_for_queued_ready(
        coordinator.url,
        job_id,
        client_token=coordinator.client_token,
        ca_cert=coordinator.cert_path,
    )

    # Local worker with its own distinct token picks up the requeued job
    worker_launcher(
        "local-worker",
        worker_token=coordinator.tokens["local-worker"],
        run_once=True,
    )

    job_data = wait_for_terminal(
        coordinator.url,
        job_id,
        client_token=coordinator.client_token,
        ca_cert=coordinator.cert_path,
        timeout=12.0,
    )
    assert job_data["status"] == "completed"
    assert job_data["attempts"] == 2
    assert job_data["result"] == "4"
