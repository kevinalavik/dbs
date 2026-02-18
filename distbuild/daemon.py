from __future__ import annotations

import os
import signal
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class DaemonSpec:
    argv: list[str]
    env: dict[str, str]
    pidfile: Path
    logfile: Path
    chdir: Path | None = None


def _write_pid(pidfile: Path, pid: int, *, force: bool) -> None:
    pidfile.parent.mkdir(parents=True, exist_ok=True)
    if pidfile.exists() and not force:
        raise SystemExit(f"pidfile exists: {pidfile} (use --force)")
    pidfile.write_text(str(pid) + "\n", encoding="utf-8")


def spawn_daemon(spec: DaemonSpec, *, force: bool = False) -> int:
    spec.logfile.parent.mkdir(parents=True, exist_ok=True)
    out = open(spec.logfile, "a", encoding="utf-8")
    try:
        p = subprocess.Popen(
            spec.argv,
            env=spec.env,
            cwd=str(spec.chdir) if spec.chdir else None,
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=out,
            start_new_session=True,
        )
        _write_pid(spec.pidfile, p.pid, force=force)
        return p.pid
    finally:
        try:
            out.flush()
        except Exception:
            pass
        try:
            out.close()
        except Exception:
            pass


def _read_pid(pidfile: Path) -> int:
    if not pidfile.exists():
        raise SystemExit(f"pidfile not found: {pidfile}")
    txt = pidfile.read_text(encoding="utf-8").strip()
    if not txt.isdigit():
        raise SystemExit(f"invalid pidfile: {pidfile}")
    return int(txt)


def stop_daemon(pidfile: Path, *, sig: str = "TERM") -> None:
    pid = _read_pid(pidfile)
    signum = {
        "TERM": signal.SIGTERM,
        "INT": signal.SIGINT,
        "KILL": signal.SIGKILL,
    }.get(sig.upper())
    if signum is None:
        raise SystemExit("invalid --signal (use TERM|INT|KILL)")

    try:
        os.kill(pid, signum)
    except ProcessLookupError:
        pass
    try:
        pidfile.unlink()
    except Exception:
        pass


def status_daemon(pidfile: Path) -> int:
    pid = _read_pid(pidfile)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return 1
    except PermissionError:
        return 2
    return 0


def tail_text_file(path: Path, *, max_bytes: int = 16_384) -> str:
    try:
        with open(path, "rb") as f:
            try:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                start = max(0, size - max_bytes)
                f.seek(start)
            except Exception:
                pass
            data = f.read()
        text = data.decode("utf-8", errors="replace")
        return text
    except FileNotFoundError:
        return ""


def default_runtime_dir() -> Path:
    xdg = os.getenv("XDG_RUNTIME_DIR")
    if xdg:
        return Path(xdg)
    # Fallback (still per-user).
    return Path.home() / ".cache" / "distbuild" / "run"
