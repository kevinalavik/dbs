from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

from sqlmodel import Session, select

from distbuild.models import Consumer, Job, JobStatus


class QuotaError(Exception):
    def __init__(self, message: str, *, status_code: int = 429):
        super().__init__(message)
        self.status_code = status_code


def enforce_submit_quota(db: Session, *, consumer: Consumer) -> None:
    if not consumer.active:
        raise QuotaError("consumer is disabled", status_code=403)

    running = db.exec(
        select(Job)
        .where(Job.consumer_id == consumer.id)
        .where(Job.status == JobStatus.running)
    ).all()
    if len(running) >= consumer.max_concurrent_jobs:
        raise QuotaError("concurrent job limit reached")

    since = datetime.utcnow() - timedelta(days=1)
    recent = db.exec(
        select(Job)
        .where(Job.consumer_id == consumer.id)
        .where(Job.created_at >= since)
    ).all()
    if len(recent) >= consumer.max_jobs_per_day:
        raise QuotaError("daily job limit reached")


def enforce_claim_quota(db: Session, *, consumer_id: UUID) -> None:
    consumer = db.get(Consumer, consumer_id)
    if consumer is None or not consumer.active:
        raise QuotaError("consumer is disabled", status_code=403)

    running = db.exec(
        select(Job)
        .where(Job.consumer_id == consumer.id)
        .where(Job.status == JobStatus.running)
    ).all()
    if len(running) >= consumer.max_concurrent_jobs:
        raise QuotaError("concurrent job limit reached")
