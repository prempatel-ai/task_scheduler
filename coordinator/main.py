"""Coordinator service: FastAPI endpoints over database layer."""

import os
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from coordinator.db import claim_job, complete_job, create_job, get_job, init_db


def get_db_path() -> str:
    """Returns database path configured via environment variable."""
    return os.getenv("DB_PATH", "jobs.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initializes database schema on startup."""
    init_db(get_db_path())
    yield


app = FastAPI(title="Task Coordinator", lifespan=lifespan)


class CreateJobRequest(BaseModel):
    type: str
    payload: str


class ClaimRequest(BaseModel):
    worker_id: str


class CompleteJobRequest(BaseModel):
    worker_id: Optional[str] = None
    attempt: Optional[int] = None


@app.post("/jobs")
def create_job_endpoint(
    body: CreateJobRequest,
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
):
    """Submits a new job or returns an existing one for duplicate idempotency keys."""
    if not idempotency_key or not idempotency_key.strip():
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")

    job_id, created = create_job(
        idem_key=idempotency_key,
        type=body.type,
        payload=body.payload,
        db_path=get_db_path(),
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
