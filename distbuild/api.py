from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Optional
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from sqlmodel import Session, select
from sqlalchemy import desc, update

from distbuild import config
from distbuild.db import init_db, session
from distbuild.models import Consumer, Job, JobLogChunk, JobStatus, SandboxType
from distbuild.quota import QuotaError, enforce_claim_quota, enforce_submit_quota
from distbuild.security import verify_api_key


app = FastAPI(title="distbuild")


@app.on_event("startup")
def _startup() -> None:
    init_db()


def _db() -> Session:
    with session() as s:
        yield s


def _require_consumer(
    db: Session = Depends(_db),
    consumer_key: Optional[str] = Header(default=None, alias="X-Consumer-Key"),
) -> Consumer:
    if not consumer_key:
        raise HTTPException(status_code=401, detail="missing X-Consumer-Key")

    # key_id is the prefix before the first '.' if present; else use the full token.
    key_id = consumer_key.split(".", 1)[0]
    consumer = db.exec(select(Consumer).where(Consumer.key_id == key_id)).first()
    if consumer is None:
        raise HTTPException(status_code=401, detail="invalid consumer key")
    if not verify_api_key(
        consumer_key,
        salt_b64=consumer.key_salt_b64,
        digest_b64=consumer.key_digest_b64,
    ):
        raise HTTPException(status_code=401, detail="invalid consumer key")
    if not consumer.active:
        raise HTTPException(status_code=403, detail="consumer disabled")
    return consumer


def _require_worker(
    worker_token: Optional[str] = Header(default=None, alias="X-Worker-Token"),
) -> str:
    if not config.WORKER_SHARED_TOKEN:
        # Misconfiguration; treat as temporarily unavailable.
        raise HTTPException(status_code=503, detail="server missing DISTBUILD_WORKER_SHARED_TOKEN")
    if not worker_token or worker_token != config.WORKER_SHARED_TOKEN:
        raise HTTPException(status_code=401, detail="invalid worker token")
    return worker_token


class JobCreate(BaseModel):
    command: str = Field(min_length=1, max_length=20_000)
    timeout_seconds: int = Field(default=config.DEFAULT_TIMEOUT_SECONDS, ge=1, le=86_400)
    sandbox: SandboxType = Field(default=SandboxType.local)
    image: Optional[str] = Field(default=None, max_length=200)


class JobOut(BaseModel):
    id: UUID
    consumer_id: UUID
    status: JobStatus
    created_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    sandbox: SandboxType
    image: Optional[str]
    command: str
    timeout_seconds: int
    worker_id: Optional[str]
    exit_code: Optional[int]
    error: Optional[str]


@app.get("/v1/jobs")
def list_jobs(
    limit: int = 20,
    offset: int = 0,
    consumer: Consumer = Depends(_require_consumer),
    db: Session = Depends(_db),
):
    limit = max(1, min(limit, 200))
    offset = max(0, offset)
    rows = db.exec(
        select(Job)
        .where(Job.consumer_id == consumer.id)
        .order_by(desc(Job.created_at))
        .offset(offset)
        .limit(limit)
    ).all()
    return {
        "limit": limit,
        "offset": offset,
        "jobs": [JobOut(**j.model_dump()) for j in rows],
    }


@app.post("/v1/jobs", response_model=JobOut)
def create_job(payload: JobCreate, consumer: Consumer = Depends(_require_consumer), db: Session = Depends(_db)):
    if payload.sandbox == SandboxType.local and not config.ALLOW_LOCAL_SANDBOX:
        raise HTTPException(status_code=400, detail="local sandbox disabled on this server")
    try:
        enforce_submit_quota(db, consumer=consumer)
    except QuotaError as e:
        raise HTTPException(status_code=e.status_code, detail=str(e))

    job = Job(
        consumer_id=consumer.id,
        command=payload.command,
        timeout_seconds=payload.timeout_seconds,
        sandbox=payload.sandbox,
        image=payload.image,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return JobOut(**job.model_dump())


@app.get("/v1/jobs/{job_id}", response_model=JobOut)
def get_job(job_id: UUID, consumer: Consumer = Depends(_require_consumer), db: Session = Depends(_db)):
    job = db.get(Job, job_id)
    if job is None or job.consumer_id != consumer.id:
        raise HTTPException(status_code=404, detail="job not found")
    return JobOut(**job.model_dump())


class LogChunkOut(BaseModel):
    seq: int
    ts: datetime
    stream: str
    text: str


@app.get("/v1/jobs/{job_id}/logs")
def get_job_logs(
    job_id: UUID,
    offset_seq: int = 0,
    limit: int = 500,
    consumer: Consumer = Depends(_require_consumer),
    db: Session = Depends(_db),
):
    job = db.get(Job, job_id)
    if job is None or job.consumer_id != consumer.id:
        raise HTTPException(status_code=404, detail="job not found")

    limit = max(1, min(limit, 2000))
    chunks = db.exec(
        select(JobLogChunk)
        .where(JobLogChunk.job_id == job_id)
        .where(JobLogChunk.seq >= offset_seq)
        .order_by(JobLogChunk.seq)
        .limit(limit)
    ).all()
    return {
        "job_id": str(job_id),
        "next_offset_seq": (chunks[-1].seq + 1) if chunks else offset_seq,
        "chunks": [LogChunkOut.model_validate(c.model_dump()) for c in chunks],
    }


@app.websocket("/v1/jobs/{job_id}/logs/ws")
async def ws_job_logs(ws: WebSocket, job_id: UUID):
    await ws.accept()

    consumer_key = ws.headers.get("x-consumer-key")
    if not consumer_key:
        await ws.close(code=4401)
        return

    # Authenticate consumer and ensure job ownership.
    with session() as db:
        key_id = consumer_key.split(".", 1)[0]
        consumer = db.exec(select(Consumer).where(Consumer.key_id == key_id)).first()
        if consumer is None or not verify_api_key(
            consumer_key, salt_b64=consumer.key_salt_b64, digest_b64=consumer.key_digest_b64
        ):
            await ws.close(code=4401)
            return
        job = db.get(Job, job_id)
        if job is None or job.consumer_id != consumer.id:
            await ws.close(code=4404)
            return

    offset = 0
    try:
        while True:
            # Allow the client to update the offset opportunistically.
            try:
                msg = await asyncio.wait_for(ws.receive_text(), timeout=0.5)
                if msg.startswith("offset="):
                    offset = max(0, int(msg.split("=", 1)[1]))
            except asyncio.TimeoutError:
                pass

            with session() as db:
                chunks = db.exec(
                    select(JobLogChunk)
                    .where(JobLogChunk.job_id == job_id)
                    .where(JobLogChunk.seq >= offset)
                    .order_by(JobLogChunk.seq)
                    .limit(200)
                ).all()
            if chunks:
                await ws.send_json(
                    {
                        "next_offset_seq": chunks[-1].seq + 1,
                        "chunks": [
                            {
                                "seq": c.seq,
                                "ts": c.ts.isoformat() + "Z",
                                "stream": c.stream,
                                "text": c.text,
                            }
                            for c in chunks
                        ],
                    }
                )
                offset = chunks[-1].seq + 1
    except WebSocketDisconnect:
        return


class ClaimResponse(BaseModel):
    job: Optional[JobOut] = None


@app.post("/v1/worker/claim", response_model=ClaimResponse)
def worker_claim(
    worker_token: str = Depends(_require_worker),
    worker_id: Optional[str] = Header(default=None, alias="X-Worker-Id"),
    db: Session = Depends(_db),
):
    for _ in range(10):
        job = db.exec(
            select(Job).where(Job.status == JobStatus.queued).order_by(Job.created_at).limit(1)
        ).first()
        if job is None:
            return ClaimResponse(job=None)

        try:
            enforce_claim_quota(db, consumer_id=job.consumer_id)
        except QuotaError:
            return ClaimResponse(job=None)

        now = datetime.utcnow()
        stmt = (
            update(Job)
            .where(Job.id == job.id)
            .where(Job.status == JobStatus.queued)
            .values(status=JobStatus.running, started_at=now, worker_id=(worker_id or "worker"))
        )
        res = db.exec(stmt)
        db.commit()
        if getattr(res, "rowcount", 0) == 1:
            claimed = db.get(Job, job.id)
            if claimed is None:
                return ClaimResponse(job=None)
            return ClaimResponse(job=JobOut(**claimed.model_dump()))
    return ClaimResponse(job=None)


class AppendLogsIn(BaseModel):
    chunks: list[LogChunkOut]


@app.post("/v1/worker/jobs/{job_id}/logs")
def worker_append_logs(
    job_id: UUID,
    payload: AppendLogsIn,
    worker_token: str = Depends(_require_worker),
    db: Session = Depends(_db),
):
    job = db.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if job.status not in (JobStatus.running, JobStatus.queued):
        return {"ok": True}

    # Determine next seq.
    last = db.exec(
        select(JobLogChunk).where(JobLogChunk.job_id == job_id).order_by(desc(JobLogChunk.seq)).limit(1)
    ).first()
    seq = (last.seq + 1) if last else 0

    for c in payload.chunks:
        text = c.text
        if len(text) > config.MAX_LOG_CHARS:
            text = text[: config.MAX_LOG_CHARS] + "\n[truncated]\n"
        db.add(JobLogChunk(job_id=job_id, seq=seq, stream=c.stream, text=text))
        seq += 1
    db.commit()
    return {"ok": True}


class FinishJobIn(BaseModel):
    status: JobStatus
    exit_code: Optional[int] = None
    error: Optional[str] = None


@app.post("/v1/worker/jobs/{job_id}/finish")
def worker_finish(
    job_id: UUID,
    payload: FinishJobIn,
    worker_token: str = Depends(_require_worker),
    db: Session = Depends(_db),
):
    job = db.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")

    job.status = payload.status
    job.exit_code = payload.exit_code
    job.error = payload.error
    job.finished_at = datetime.utcnow()
    db.add(job)
    db.commit()
    return {"ok": True}
