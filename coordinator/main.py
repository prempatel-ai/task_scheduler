"""Coordinator service: API + background sweeper."""

import os
import threading
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from coordinator.db import (
    claim_job,
    complete_job,
    create_job,
    fail_job,
    get_job,
    get_job_by_idem_key,
    get_queue_depth,
    heartbeat,
    init_db,
    requeue_expired,
)


def get_db_path() -> str:
    """Returns database path configured via environment variable."""
    return os.getenv("DB_PATH", "jobs.db")


def sweeper_loop(stop_event: threading.Event, sweep_secs: float, db_path: str):
    """Background thread loop that periodically sweeps and requeues expired jobs."""
    while not stop_event.is_set():
        if stop_event.wait(sweep_secs):
            break
        try:
            requeue_expired(db_path=db_path)
        except Exception:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initializes schema and starts sweeper background thread."""
    db_path = get_db_path()
    init_db(db_path)

    sweep_secs = float(os.getenv("SWEEP_SECS", "2"))
    stop_event = threading.Event()
    thread = threading.Thread(
        target=sweeper_loop,
        args=(stop_event, sweep_secs, db_path),
        daemon=True,
    )
    thread.start()
    yield
    stop_event.set()
    thread.join(timeout=1.0)


app = FastAPI(title="Task Coordinator", lifespan=lifespan)


class CreateJobRequest(BaseModel):
    type: str
    payload: str


class ClaimRequest(BaseModel):
    worker_id: str


class CompleteJobRequest(BaseModel):
    worker_id: Optional[str] = None
    attempt: Optional[int] = None


class HeartbeatRequest(BaseModel):
    worker_id: str
    attempt: int


class FailJobRequest(BaseModel):
    worker_id: str
    attempt: int
    error: str


@app.post("/jobs")
def create_job_endpoint(
    body: CreateJobRequest,
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
):
    """Submits a new job or returns an existing one for duplicate idempotency keys.

    If queue depth >= MAX_QUEUE for a new key, returns 429 with Retry-After.
    """
    if not idempotency_key or not idempotency_key.strip():
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")

    db_path = get_db_path()

    # 1. Known key returns 200 even if the queue is full (client retry is never rejected)
    existing = get_job_by_idem_key(idempotency_key, db_path=db_path)
    if existing:
        return JSONResponse(status_code=200, content={"job_id": existing["id"]})

    # 2. Check queue depth against MAX_QUEUE (queued only, not running)
    max_queue = int(os.getenv("MAX_QUEUE", "100"))
    if get_queue_depth(db_path=db_path) >= max_queue:
        return JSONResponse(
            status_code=429,
            headers={"Retry-After": "5"},
            content={"detail": "Queue is full, try later"},
        )

    # 3. Insert new job
    job_id, created = create_job(
        idem_key=idempotency_key,
        type=body.type,
        payload=body.payload,
        db_path=db_path,
    )
    status_code = 201 if created else 200
    return JSONResponse(status_code=status_code, content={"job_id": job_id})


@app.get("/jobs/{id}")
def get_job_endpoint(id: int):
    """Returns full job row by ID, or 404 if missing."""
    job = get_job(id, db_path=get_db_path())
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.post("/claim")
def claim_endpoint(body: ClaimRequest):
    """Claims the oldest queued job. Returns 204 if queue is empty."""
    lease_secs = float(os.getenv("LEASE_SECS", "30"))
    job = claim_job(worker_id=body.worker_id, lease_secs=lease_secs, db_path=get_db_path())
    if job is None:
        return Response(status_code=204)
    job["attempt"] = job.get("attempts", 1)
    return job


@app.post("/jobs/{id}/complete")
def complete_endpoint(id: int, body: CompleteJobRequest):
    """Marks a job completed. Returns 404 if missing, or {updated: bool}."""
    job = get_job(id, db_path=get_db_path())
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    updated = complete_job(
        job_id=id,
        worker_id=body.worker_id,
        attempt=body.attempt,
        db_path=get_db_path(),
    )
    return {"updated": updated}


@app.post("/jobs/{id}/heartbeat")
def heartbeat_endpoint(id: int, body: HeartbeatRequest):
    """Extends the lease of a running job."""
    lease_secs = float(os.getenv("LEASE_SECS", "30"))
    extended = heartbeat(
        job_id=id,
        worker_id=body.worker_id,
        attempt=body.attempt,
        lease_secs=lease_secs,
        db_path=get_db_path(),
    )
    return {"extended": extended}


@app.post("/jobs/{id}/fail")
def fail_endpoint(id: int, body: FailJobRequest):
    """Reports a job failure from a worker."""
    failed = fail_job(
        job_id=id,
        worker_id=body.worker_id,
        attempt=body.attempt,
        error=body.error,
        db_path=get_db_path(),
    )
    return {"failed": failed}
