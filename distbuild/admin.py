from __future__ import annotations

import argparse
import os
import secrets
from typing import Optional
from uuid import UUID

from sqlalchemy import delete
from sqlalchemy.exc import IntegrityError

from distbuild import config
from distbuild.db import init_db, session
from distbuild.models import Consumer, Job, JobLogChunk
from distbuild.security import generate_api_key, hash_api_key
from sqlmodel import select


def _key_id() -> str:
    return "kid_" + secrets.token_urlsafe(10)


def create_consumer(name: str, max_concurrent: int, max_per_day: int) -> None:
    init_db()
    with session() as db:
        existing = db.exec(select(Consumer).where(Consumer.name == name)).first()
        if existing is not None:
            raise SystemExit(f"consumer name already exists: {name}")

    kid = _key_id()
    api_key = generate_api_key()
    token = f"{kid}.{api_key}"
    kh = hash_api_key(token)

    consumer = Consumer(
        name=name,
        key_id=kid,
        key_salt_b64=kh.salt_b64,
        key_digest_b64=kh.digest_b64,
        max_concurrent_jobs=max_concurrent,
        max_jobs_per_day=max_per_day,
    )
    with session() as db:
        db.add(consumer)
        try:
            db.commit()
        except IntegrityError as e:
            db.rollback()
            raise SystemExit(f"failed to create consumer: {e}")
        db.refresh(consumer)

    # Prepend key_id so we can look up without scanning.
    print("consumer_id:", consumer.id)
    print("consumer_name:", consumer.name)
    print("consumer_key:", token)


def list_consumers() -> None:
    init_db()
    with session() as db:
        rows = db.exec(select(Consumer).order_by(Consumer.created_at)).all()

    if not rows:
        print("(no consumers)")
        return

    for c in rows:
        print(
            "id=", c.id,
            " name=", c.name,
            " active=", c.active,
            " max_concurrent=", c.max_concurrent_jobs,
            " max_per_day=", c.max_jobs_per_day,
            sep="",
        )


def set_active(*, consumer_id: Optional[str], name: Optional[str], active: bool) -> None:
    init_db()
    with session() as db:
        if consumer_id is not None:
            c = db.get(Consumer, UUID(consumer_id))
        else:
            c = db.exec(select(Consumer).where(Consumer.name == name)).first()
        if c is None:
            raise SystemExit("consumer not found")
        c.active = active
        db.add(c)
        db.commit()
        print("ok")


def set_quota(
    *,
    consumer_id: Optional[str],
    name: Optional[str],
    max_concurrent: Optional[int],
    max_per_day: Optional[int],
) -> None:
    init_db()
    with session() as db:
        if consumer_id is not None:
            c = db.get(Consumer, UUID(consumer_id))
        else:
            c = db.exec(select(Consumer).where(Consumer.name == name)).first()
        if c is None:
            raise SystemExit("consumer not found")
        if max_concurrent is not None:
            c.max_concurrent_jobs = max_concurrent
        if max_per_day is not None:
            c.max_jobs_per_day = max_per_day
        db.add(c)
        db.commit()
        print("ok")


def rotate_key(*, consumer_id: Optional[str], name: Optional[str]) -> None:
    init_db()
    with session() as db:
        if consumer_id is not None:
            c = db.get(Consumer, UUID(consumer_id))
        else:
            c = db.exec(select(Consumer).where(Consumer.name == name)).first()
        if c is None:
            raise SystemExit("consumer not found")

        kid = _key_id()
        api_key = generate_api_key()
        token = f"{kid}.{api_key}"
        kh = hash_api_key(token)

        c.key_id = kid
        c.key_salt_b64 = kh.salt_b64
        c.key_digest_b64 = kh.digest_b64
        db.add(c)
        db.commit()

    print("consumer_id:", c.id)
    print("consumer_name:", c.name)
    print("consumer_key:", token)


def delete_consumer(*, consumer_id: Optional[str], name: Optional[str], yes: bool) -> None:
    if not yes:
        raise SystemExit("refusing to delete without --yes")

    init_db()
    with session() as db:
        if consumer_id is not None:
            c = db.get(Consumer, UUID(consumer_id))
        else:
            c = db.exec(select(Consumer).where(Consumer.name == name)).first()
        if c is None:
            raise SystemExit("consumer not found")

        jobs = db.exec(select(Job).where(Job.consumer_id == c.id)).all()
        job_ids = [j.id for j in jobs]
        if job_ids:
            db.exec(delete(JobLogChunk).where(JobLogChunk.job_id.in_(job_ids)))
            db.exec(delete(Job).where(Job.id.in_(job_ids)))
        db.exec(delete(Consumer).where(Consumer.id == c.id))
        db.commit()
        print("ok")


def reset_db(*, yes: bool) -> None:
    if not yes:
        raise SystemExit("refusing to reset without --yes")

    url = config.DATABASE_URL
    if not (url.startswith("sqlite:///") or url.startswith("sqlite:////")):
        raise SystemExit("reset-db only supports sqlite database urls")

    if url.startswith("sqlite:////"):
        path = "/" + url.removeprefix("sqlite:////").lstrip("/")
    else:
        path = url.removeprefix("sqlite:///")

    if path in (":memory:", ""):
        raise SystemExit("refusing to reset in-memory sqlite")

    path = os.path.abspath(path)
    if os.path.exists(path):
        os.remove(path)
        print("deleted:", path)
    else:
        print("missing:", path)

    init_db()
    print("initialized fresh database")


def db_info() -> None:
    url = config.DATABASE_URL
    print("database_url:", url)
    if url.startswith("sqlite:////"):
        path = url.removeprefix("sqlite:////")
        path = "/" + path.lstrip("/")
        print("sqlite_path:", path)
        print("exists:", os.path.exists(path))
    elif url.startswith("sqlite:///"):
        print("note: sqlite relative paths are fragile; prefer sqlite:////absolute/path")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="distbuild-admin")
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("create-consumer")
    c.add_argument("--name", required=True)
    c.add_argument("--max-concurrent", type=int, default=1)
    c.add_argument("--max-per-day", type=int, default=100)

    sub.add_parser("list-consumers")

    en = sub.add_parser("enable-consumer")
    en.add_argument("--id")
    en.add_argument("--name")

    dis = sub.add_parser("disable-consumer")
    dis.add_argument("--id")
    dis.add_argument("--name")

    q = sub.add_parser("set-quota")
    q.add_argument("--id")
    q.add_argument("--name")
    q.add_argument("--max-concurrent", type=int)
    q.add_argument("--max-per-day", type=int)

    rk = sub.add_parser("rotate-key")
    rk.add_argument("--id")
    rk.add_argument("--name")

    dc = sub.add_parser("delete-consumer")
    dc.add_argument("--id")
    dc.add_argument("--name")
    dc.add_argument("--yes", action="store_true")

    rd = sub.add_parser("reset-db")
    rd.add_argument("--yes", action="store_true")

    sub.add_parser("db-info")

    args = ap.parse_args(argv)
    if args.cmd == "create-consumer":
        create_consumer(args.name, args.max_concurrent, args.max_per_day)
        return 0
    if args.cmd == "list-consumers":
        list_consumers()
        return 0
    if args.cmd == "enable-consumer":
        set_active(consumer_id=args.id, name=args.name, active=True)
        return 0
    if args.cmd == "disable-consumer":
        set_active(consumer_id=args.id, name=args.name, active=False)
        return 0
    if args.cmd == "set-quota":
        set_quota(
            consumer_id=args.id,
            name=args.name,
            max_concurrent=args.max_concurrent,
            max_per_day=args.max_per_day,
        )
        return 0
    if args.cmd == "rotate-key":
        rotate_key(consumer_id=args.id, name=args.name)
        return 0
    if args.cmd == "delete-consumer":
        delete_consumer(consumer_id=args.id, name=args.name, yes=args.yes)
        return 0
    if args.cmd == "reset-db":
        reset_db(yes=args.yes)
        return 0
    if args.cmd == "db-info":
        db_info()
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
