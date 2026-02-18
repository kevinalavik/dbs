from __future__ import annotations

import os
from pathlib import Path


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if (v is None or v == "") else v


def _default_data_dir() -> Path:
    xdg = os.getenv("XDG_DATA_HOME")
    base = Path(xdg) if xdg else (Path.home() / ".local" / "share")
    return base / "distbuild"


def _default_database_url() -> str:
    # Use an absolute sqlite path by default so api/worker/admin agree
    # even when started from different working directories.
    db_path = (_default_data_dir() / "distbuild.db").resolve()
    # For an absolute path, SQLAlchemy expects sqlite:////absolute/path
    return "sqlite:////" + db_path.as_posix().lstrip("/")


DATABASE_URL = _env("DISTBUILD_DATABASE_URL", _default_database_url())

SERVER_HOST = _env("DISTBUILD_SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(_env("DISTBUILD_SERVER_PORT", "8080"))

WORKER_SHARED_TOKEN = _env("DISTBUILD_WORKER_SHARED_TOKEN", "")

DEFAULT_TIMEOUT_SECONDS = int(_env("DISTBUILD_DEFAULT_TIMEOUT_SECONDS", "600"))


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    v = v.strip().lower()
    return v in ("1", "true", "yes", "y", "on")


# If false, the API rejects jobs submitted with sandbox=local.
ALLOW_LOCAL_SANDBOX = _env_bool("DISTBUILD_ALLOW_LOCAL_SANDBOX", True)

# Maximum characters per log chunk stored in DB.
MAX_LOG_CHARS = int(_env("DISTBUILD_MAX_LOG_CHARS", "4000"))


# Docker sandbox defaults. These aim to support "build" style jobs where the
# container can fetch dependencies from the internet and run as root, while
# still applying basic resource limits in the worker.
#
# Notes:
# - Network isolation here means "not shared with other jobs" by default.
#   It is not a firewall against the Docker host.


DOCKER_DEFAULT_IMAGE = _env("DISTBUILD_DOCKER_DEFAULT_IMAGE", "debian:stable")


# Network mode for job containers:
# - "job": create a fresh user-defined bridge network per job (default)
# - "bridge": use Docker's default bridge
# - "none": disable networking
# - any other string: treated as an existing Docker network name
DOCKER_NETWORK_MODE = _env("DISTBUILD_DOCKER_NETWORK_MODE", "job")


# User for job containers:
# - "root" (default)
# - "nobody"
# - "UID:GID" (e.g. "1000:1000")
DOCKER_RUN_AS = _env("DISTBUILD_DOCKER_RUN_AS", "root")


# Capabilities to add back after dropping ALL.
# Comma-separated, e.g. "CHOWN,SETUID,SETGID".
# Default is "build-friendly" so common tools like apt and ping work.
DOCKER_CAP_ADD = _env(
    "DISTBUILD_DOCKER_CAP_ADD",
    "CHOWN,DAC_OVERRIDE,FOWNER,SETUID,SETGID,NET_RAW",
)


# If true, run containers with a read-only root filesystem.
# This is more restrictive and may break package installs.
DOCKER_READ_ONLY_ROOTFS = _env_bool("DISTBUILD_DOCKER_READ_ONLY_ROOTFS", False)
