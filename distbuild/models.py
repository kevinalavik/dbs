from __future__ import annotations

import enum
from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel


def utcnow() -> datetime:
    return datetime.utcnow()


class JobStatus(str, enum.Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


class SandboxType(str, enum.Enum):
    local = "local"
    docker = "docker"


class Consumer(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    # Unique consumer name to prevent duplicates.
    name: str = Field(index=True, unique=True)
    active: bool = Field(default=True, index=True)

    key_id: str = Field(index=True, unique=True)
    key_salt_b64: str
    key_digest_b64: str

    max_concurrent_jobs: int = Field(default=1)
    max_jobs_per_day: int = Field(default=100)

    created_at: datetime = Field(default_factory=utcnow, index=True)


class Job(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    consumer_id: UUID = Field(index=True)

    status: JobStatus = Field(default=JobStatus.queued, index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    started_at: Optional[datetime] = Field(default=None, index=True)
    finished_at: Optional[datetime] = Field(default=None, index=True)

    sandbox: SandboxType = Field(default=SandboxType.local, index=True)
    image: Optional[str] = Field(default=None)

    command: str
    timeout_seconds: int

    worker_id: Optional[str] = Field(default=None, index=True)

    exit_code: Optional[int] = Field(default=None)
    error: Optional[str] = Field(default=None)


class JobLogChunk(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: UUID = Field(index=True)
    seq: int = Field(index=True)
    ts: datetime = Field(default_factory=utcnow, index=True)
    stream: str = Field(index=True)  # stdout|stderr|system
    text: str
