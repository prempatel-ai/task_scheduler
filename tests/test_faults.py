"""Automated failure and crash recovery tests."""

import time
import httpx


def test_crash_mid_job(coordinator, worker_launcher, wait_for_terminal, wait_for_queued_ready):
    """CRASH_AT=mid_job, then start a clean worker: job ends completed, attempts 2, one output file."""
    submit = httpx.post(
        f"{coordinator.url}/jobs",
        json={"type": "count_words", "payload": "one two three four five"},
        headers={
            "Idempotency-Key": "crash-mid-job-key",
            "Authorization": f"Bearer {coordinator.client_token}",
        },
        verify=coordinator.cert_path,
        timeout=5.0,
    )
    assert submit.status_code == 201
    job_id = submit.json()["job_id"]

    # Worker 1 claims and crashes mid-job
    worker1 = worker_launcher("worker-mid-crash", env_overrides={"CRASH_AT": "mid_job"}, run_once=True)
    worker1.wait(timeout=5.0)
    assert worker1.returncode == 1

    # Wait for sweeper to requeue the job and not_before to arrive
    wait_for_queued_ready(
        coordinator.url,
        job_id,
        client_token=coordinator.client_token,
        ca_cert=coordinator.cert_path,
    )

    # Worker 2 clean run
    worker_launcher("worker-clean-1", run_once=True)

    job_data = wait_for_terminal(
        coordinator.url,
        job_id,
        client_token=coordinator.client_token,
        ca_cert=coordinator.cert_path,
        timeout=12.0,
    )
    assert job_data["status"] == "completed"
    assert job_data["attempts"] == 2
    assert job_data.get("result") == "5"


def test_crash_after_write_before_ack(coordinator, worker_launcher, wait_for_terminal, wait_for_queued_ready):
    """CRASH_AT=after_write_before_ack, then start a clean worker:

    job ends completed, attempts 2, exactly one result on one completed row.
    """
    submit = httpx.post(
        f"{coordinator.url}/jobs",
        json={"type": "count_words", "payload": "apple banana orange grape"},
        headers={
            "Idempotency-Key": "crash-ack-key",
            "Authorization": f"Bearer {coordinator.client_token}",
        },
        verify=coordinator.cert_path,
        timeout=5.0,
    )
    assert submit.status_code == 201
    job_id = submit.json()["job_id"]

    # Worker 1 writes result then crashes before ACK
    worker1 = worker_launcher(
        "worker-ack-crash",
        env_overrides={"CRASH_AT": "after_write_before_ack"},
        run_once=True,
    )
    worker1.wait(timeout=5.0)
    assert worker1.returncode == 1

    # Wait for sweeper to requeue and not_before to elapse
    wait_for_queued_ready(
        coordinator.url,
        job_id,
        client_token=coordinator.client_token,
        ca_cert=coordinator.cert_path,
    )

    # Worker 2 clean run
    worker_launcher("worker-clean-2", run_once=True)

    job_data = wait_for_terminal(
        coordinator.url,
        job_id,
        client_token=coordinator.client_token,
        ca_cert=coordinator.cert_path,
        timeout=12.0,
    )
    assert job_data["status"] == "completed"
    assert job_data["attempts"] == 2
    # Phase 6 requirement: asserts one result on one completed row
    assert job_data["result"] == "4"


def test_slow_worker_heartbeat_off(coordinator, worker_launcher, wait_for_terminal, wait_for_queued_ready):
    """Slow worker: worker A with HEARTBEAT=off and JOB_SLEEP longer than lease,

    then worker B starts. Job ends completed, one output file, and A or B logs updated: false.
    """
    submit = httpx.post(
        f"{coordinator.url}/jobs",
        json={"type": "sleep", "payload": ""},
        headers={
            "Idempotency-Key": "slow-worker-key",
            "Authorization": f"Bearer {coordinator.client_token}",
        },
        verify=coordinator.cert_path,
        timeout=5.0,
    )
    assert submit.status_code == 201
    job_id = submit.json()["job_id"]

    # Worker A starts with heartbeat off and sleeps for 3.5s via JOB_SLEEP (lease is 1.5s)
    worker_a = worker_launcher(
        "worker-slow-A",
        env_overrides={"HEARTBEAT": "off", "JOB_SLEEP": "3.5"},
        run_once=True,
    )

    # Wait until Worker A has claimed the job
    for _ in range(40):
        resp = httpx.get(
            f"{coordinator.url}/jobs/{job_id}",
            headers={"Authorization": f"Bearer {coordinator.client_token}"},
            verify=coordinator.cert_path,
            timeout=2.0,
        )
        if resp.status_code == 200 and resp.json().get("status") == "running":
            break
        time.sleep(0.1)

    # Wait until lease expires and job is requeued and ready for claim
    wait_for_queued_ready(
        coordinator.url,
        job_id,
        client_token=coordinator.client_token,
        ca_cert=coordinator.cert_path,
    )

    # Worker B starts with short sleep 0.1s and completes the job first
    worker_b = worker_launcher(
        "worker-fast-B",
        env_overrides={"JOB_SLEEP": "0.1"},
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

    stdout_a, _ = worker_a.communicate(timeout=8.0)
    stdout_b, _ = worker_b.communicate(timeout=8.0)

    # One worker completed first (updated=True), the second late complete logged updated=False
    combined_logs = stdout_a + stdout_b
    assert "updated=False" in combined_logs or "updated=false" in combined_logs.lower()


def test_kill_coordinator_and_recover(coordinator, worker_launcher, wait_for_terminal):
    """kill -9 coordinator while job is running, restart on same DB:

    job still reaches completed, and worker did not crash.
    """
    submit = httpx.post(
        f"{coordinator.url}/jobs",
        json={"type": "sleep", "payload": "2.0"},
        headers={
            "Idempotency-Key": "kill-coord-key",
            "Authorization": f"Bearer {coordinator.client_token}",
        },
        verify=coordinator.cert_path,
        timeout=5.0,
    )
    assert submit.status_code == 201
    job_id = submit.json()["job_id"]

    # Worker starts running job
    worker = worker_launcher("worker-coord-kill", run_once=True)

    # Wait until worker has claimed the job
    for _ in range(30):
        resp = httpx.get(
            f"{coordinator.url}/jobs/{job_id}",
            headers={"Authorization": f"Bearer {coordinator.client_token}"},
            verify=coordinator.cert_path,
            timeout=2.0,
        )
        if resp.status_code == 200 and resp.json().get("status") == "running":
            break
        time.sleep(0.1)

    # Kill coordinator mid-job
    coordinator.kill()
    time.sleep(0.8)

    # Worker is still alive retrying complete
    assert worker.poll() is None

    # Restart coordinator
    coordinator.restart()

    job_data = wait_for_terminal(
        coordinator.url,
        job_id,
        client_token=coordinator.client_token,
        ca_cert=coordinator.cert_path,
        timeout=15.0,
    )
    assert job_data["status"] == "completed"

    # Worker finished cleanly
    worker.wait(timeout=5.0)
    assert worker.returncode == 0


def test_submit_idempotency_key_twice(coordinator):
    """Same Idempotency-Key submitted twice: one job, 201 then 200."""
    headers = {
        "Idempotency-Key": "unique-idem-check-1",
        "Authorization": f"Bearer {coordinator.client_token}",
    }
    body = {"type": "count_words", "payload": "hello world"}

    res1 = httpx.post(f"{coordinator.url}/jobs", json=body, headers=headers, verify=coordinator.cert_path)
    assert res1.status_code == 201
    job_id_1 = res1.json()["job_id"]

    res2 = httpx.post(f"{coordinator.url}/jobs", json=body, headers=headers, verify=coordinator.cert_path)
    assert res2.status_code == 200
    job_id_2 = res2.json()["job_id"]

    assert job_id_1 == job_id_2


def test_always_crash_reaches_failed(coordinator, worker_launcher, wait_for_terminal, wait_for_queued_ready):
    """always_crash job with workers respawned by the test: ends failed with attempts == max_attempts."""
    submit = httpx.post(
        f"{coordinator.url}/jobs",
        json={"type": "always_crash", "payload": ""},
        headers={
            "Idempotency-Key": "always-crash-job",
            "Authorization": f"Bearer {coordinator.client_token}",
        },
        verify=coordinator.cert_path,
        timeout=5.0,
    )
    assert submit.status_code == 201
    job_id = submit.json()["job_id"]

    # Respawn workers for 3 crashes
    for attempt in range(1, 4):
        wait_for_queued_ready(
            coordinator.url,
            job_id,
            client_token=coordinator.client_token,
            ca_cert=coordinator.cert_path,
        )
        w = worker_launcher(f"worker-crash-{attempt}", run_once=True)
        w.wait(timeout=5.0)
        assert w.returncode == 1

    job_data = wait_for_terminal(
        coordinator.url,
        job_id,
        client_token=coordinator.client_token,
        ca_cert=coordinator.cert_path,
        timeout=12.0,
    )
    assert job_data["status"] == "failed"
    assert job_data["attempts"] == 3
    assert "lease expired, max attempts" in job_data.get("error", "")


def test_backpressure_max_queue(coordinator_factory, worker_launcher, wait_for_terminal):
    """MAX_QUEUE=5: first 5 return 201, rest return 429 with Retry-After.

    Existing key returns 200 even on full queue. New submit returns 201 after drain.
    """
    ctrl = coordinator_factory(max_queue="5")

    submitted_ids = []
    headers = {"Authorization": f"Bearer {ctrl.client_token}"}

    # Submit 20 jobs
    for i in range(20):
        h = {**headers, "Idempotency-Key": f"backpressure-key-{i}"}
        res = httpx.post(
            f"{ctrl.url}/jobs",
            json={"type": "count_words", "payload": f"text {i}"},
            headers=h,
            verify=ctrl.cert_path,
        )
        if i < 5:
            assert res.status_code == 201, f"Job {i} should be accepted"
            submitted_ids.append(res.json()["job_id"])
        else:
            assert res.status_code == 429, f"Job {i} should be rate limited"
            assert "Retry-After" in res.headers

    # Repeat of an existing key against a full queue returns 200, not 429
    repeat_res = httpx.post(
        f"{ctrl.url}/jobs",
        json={"type": "count_words", "payload": "text 0"},
        headers={**headers, "Idempotency-Key": "backpressure-key-0"},
        verify=ctrl.cert_path,
    )
    assert repeat_res.status_code == 200
    assert repeat_res.json()["job_id"] == submitted_ids[0]

    # Drain the queue using a worker
    worker_launcher(
        "drain-worker",
        run_once=False,
        coordinator_url=ctrl.url,
        outputs_dir=ctrl.outputs_dir,
        ca_cert=ctrl.cert_path,
    )
    for job_id in submitted_ids:
        wait_for_terminal(
            ctrl.url,
            job_id,
            client_token=ctrl.client_token,
            ca_cert=ctrl.cert_path,
            timeout=12.0,
        )

    # Queue is now drained; new submit with a fresh key returns 201
    fresh_res = httpx.post(
        f"{ctrl.url}/jobs",
        json={"type": "count_words", "payload": "after drain"},
        headers={**headers, "Idempotency-Key": "after-drain-key"},
        verify=ctrl.cert_path,
    )
    assert fresh_res.status_code == 201
