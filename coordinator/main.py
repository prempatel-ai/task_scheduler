"""Coordinator service: API + background sweeper + auth + HTTPS."""

import hmac
import json
import os
import threading
from contextlib import asynccontextmanager
from typing import Dict, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Response
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
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

security = HTTPBearer(auto_error=False)


def get_db_path() -> str:
    """Returns database path configured via environment variable."""
    return os.getenv("DB_PATH", "jobs.db")


def get_tokens() -> Dict[str, str]:
    """Loads worker token mappings from TOKENS_FILE."""
    tokens_path = os.getenv("TOKENS_FILE")
    if tokens_path and os.path.exists(tokens_path):
        with open(tokens_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def verify_client_auth(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
):
    """Enforces client bearer authentication on job submit and read endpoints."""
    expected = os.getenv("CLIENT_TOKEN")
    if not expected:
        return
    if not credentials or not credentials.credentials:
        raise HTTPException(status_code=401, detail="Missing client token")
    if not hmac.compare_digest(credentials.credentials, expected):
        raise HTTPException(status_code=401, detail="Invalid client token")


def verify_worker_auth(
    worker_id: str,
    credentials: Optional[HTTPAuthorizationCredentials],
):
    """Enforces worker bearer authentication and matches token owner to worker_id."""
    tokens = get_tokens()
    if not tokens and not os.getenv("TOKENS_FILE"):
        return

    if not credentials or not credentials.credentials:
        raise HTTPException(status_code=401, detail="Missing worker token")

    token = credentials.credentials
    matching_worker = None
    for w_id, sec in tokens.items():
        if hmac.compare_digest(sec, token):
            matching_worker = w_id
            break

    if matching_worker is None:
        raise HTTPException(status_code=401, detail="Invalid worker token")

    if matching_worker != worker_id:
        raise HTTPException(status_code=403, detail="Token does not belong to specified worker_id")


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
    result: Optional[str] = None


class HeartbeatRequest(BaseModel):
    worker_id: str
    attempt: int


class FailJobRequest(BaseModel):
    worker_id: str
    attempt: int
    error: str


@app.post("/jobs", dependencies=[Depends(verify_client_auth)])
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


@app.get("/jobs/{id}", dependencies=[Depends(verify_client_auth)])
def get_job_endpoint(id: int):
    """Returns full job row by ID, or 404 if missing."""
    job = get_job(id, db_path=get_db_path())
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.post("/claim")
def claim_endpoint(
    body: ClaimRequest,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
):
    """Claims the oldest queued job. Returns 204 if queue is empty."""
    verify_worker_auth(body.worker_id, credentials)

    lease_secs = float(os.getenv("LEASE_SECS", "30"))
    job = claim_job(worker_id=body.worker_id, lease_secs=lease_secs, db_path=get_db_path())
    if job is None:
        return Response(status_code=204)
    job["attempt"] = job.get("attempts", 1)
    return job


@app.post("/jobs/{id}/complete")
def complete_endpoint(
    id: int,
    body: CompleteJobRequest,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
):
    """Marks a job completed with its result. Returns 404 if missing, 413 if result is too large."""
    if body.worker_id:
        verify_worker_auth(body.worker_id, credentials)

    max_result_bytes = int(os.getenv("MAX_RESULT_BYTES", "65536"))
    if body.result is not None and len(body.result.encode("utf-8")) > max_result_bytes:
        raise HTTPException(status_code=413, detail="Result payload exceeds maximum size")

    job = get_job(id, db_path=get_db_path())
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    updated = complete_job(
        job_id=id,
        worker_id=body.worker_id,
        attempt=body.attempt,
        result=body.result,
        db_path=get_db_path(),
    )
    return {"updated": updated}


@app.post("/jobs/{id}/heartbeat")
def heartbeat_endpoint(
    id: int,
    body: HeartbeatRequest,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
):
    """Extends the lease of a running job."""
    verify_worker_auth(body.worker_id, credentials)

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
def fail_endpoint(
    id: int,
    body: FailJobRequest,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
):
    """Reports a job failure from a worker."""
    verify_worker_auth(body.worker_id, credentials)

    failed = fail_job(
        job_id=id,
        worker_id=body.worker_id,
        attempt=body.attempt,
        error=body.error,
        db_path=get_db_path(),
    )
    return {"failed": failed}
