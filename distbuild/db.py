from __future__ import annotations

import os
from pathlib import Path

from sqlmodel import Session, SQLModel, create_engine

from distbuild.config import DATABASE_URL


engine = create_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)


def _ensure_sqlite_parent_dir() -> None:
    # sqlite:////absolute/path or sqlite:///relative/path
    url = DATABASE_URL
    if url.startswith("sqlite:////"):
        p = Path("/" + url.removeprefix("sqlite:////").lstrip("/"))
        p.parent.mkdir(parents=True, exist_ok=True)
    elif url.startswith("sqlite:///"):
        rel = url.removeprefix("sqlite:///")
        if rel not in (":memory:", ""):
            p = Path(rel)
            p.parent.mkdir(parents=True, exist_ok=True)


def init_db() -> None:
    _ensure_sqlite_parent_dir()
    SQLModel.metadata.create_all(engine)


def session() -> Session:
    return Session(engine)
