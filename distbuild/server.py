from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import uvicorn

from distbuild.daemon import (
    DaemonSpec,
    default_runtime_dir,
    spawn_daemon,
    status_daemon,
    stop_daemon,
    tail_text_file,
)
from distbuild.utils import parse_toml


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="distbuild-api")
    ap.add_argument("--config-file", default=os.getenv("DISTBUILD_API_CONFIG", ""))
    ap.add_argument("--host", default=os.getenv("DISTBUILD_SERVER_HOST", "0.0.0.0"))
    ap.add_argument("--port", type=int, default=int(os.getenv("DISTBUILD_SERVER_PORT", "8080")))
    ap.add_argument("--db-url", default=os.getenv("DISTBUILD_DATABASE_URL", ""))
    ap.add_argument("--worker-token", default=os.getenv("DISTBUILD_WORKER_SHARED_TOKEN", ""))
    ap.add_argument(
        "--allow-local-sandbox",
        action="store_true",
        default=(os.getenv("DISTBUILD_ALLOW_LOCAL_SANDBOX", "").strip().lower() in ("1", "true", "yes", "on")),
    )
    ap.add_argument("--log-level", default=os.getenv("DISTBUILD_LOG_LEVEL", "info"))
    ap.add_argument("--access-log", action="store_true", default=True)
    ap.add_argument("--no-access-log", action="store_false", dest="access_log")

    ap.add_argument("-d", "--daemon", action="store_true")
    ap.add_argument("--start", action="store_true", help="alias for --daemon")
    ap.add_argument("--restart", action="store_true", help="stop then start (daemon)")
    ap.add_argument("--pidfile", default="")
    ap.add_argument("--log-file", default="")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--stop", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--signal", default="TERM")
    args = ap.parse_args(argv)

    if args.config_file:
        p = Path(args.config_file)
        cfg = parse_toml(p.read_text(encoding="utf-8"))
        # Only fill in values the user didn't explicitly set.
        if "host" in cfg and args.host == os.getenv("DISTBUILD_SERVER_HOST", "0.0.0.0"):
            args.host = str(cfg["host"])
        if "port" in cfg and args.port == int(os.getenv("DISTBUILD_SERVER_PORT", "8080")):
            args.port = int(cfg["port"])
        if "db_url" in cfg and not args.db_url:
            args.db_url = str(cfg["db_url"])
        if "worker_token" in cfg and not args.worker_token:
            args.worker_token = str(cfg["worker_token"])
        if "allow_local_sandbox" in cfg:
            # If config explicitly sets it, honor it.
            args.allow_local_sandbox = bool(cfg["allow_local_sandbox"])
        if "log_level" in cfg and args.log_level == os.getenv("DISTBUILD_LOG_LEVEL", "info"):
            args.log_level = str(cfg["log_level"])
        if "access_log" in cfg:
            args.access_log = bool(cfg["access_log"])

    run_dir = default_runtime_dir() / "api"
    pidfile = Path(args.pidfile) if args.pidfile else (run_dir / f"distbuild-api.{args.port}.pid")
    logfile = Path(args.log_file) if args.log_file else (run_dir / f"distbuild-api.{args.port}.log")

    if args.start:
        args.daemon = True

    if args.stop:
        stop_daemon(pidfile, sig=args.signal)
        print("stopped")
        return 0
    if args.status:
        rc = status_daemon(pidfile)
        if rc == 0:
            print("running")
        elif rc == 1:
            print("not running")
            tail = tail_text_file(logfile)
            if tail.strip():
                print("log_tail:")
                print(tail.rstrip())
        else:
            print("unknown")
        return rc

    env = os.environ.copy()
    if args.db_url:
        env["DISTBUILD_DATABASE_URL"] = args.db_url
    if args.worker_token:
        env["DISTBUILD_WORKER_SHARED_TOKEN"] = args.worker_token
    env["DISTBUILD_ALLOW_LOCAL_SANDBOX"] = "true" if args.allow_local_sandbox else "false"

    if args.restart:
        try:
            stop_daemon(pidfile, sig=args.signal)
        except Exception:
            pass
        args.daemon = True

    if args.daemon:
        # Re-exec in the background as `python -m distbuild.server ...`.
        argv2 = [
            sys.executable,
            "-m",
            "distbuild.server",
            "--host",
            str(args.host),
            "--port",
            str(args.port),
            "--log-level",
            str(args.log_level),
        ]
        if not args.access_log:
            argv2.append("--no-access-log")
        if args.db_url:
            argv2 += ["--db-url", args.db_url]
        if args.worker_token:
            argv2 += ["--worker-token", args.worker_token]
        if args.allow_local_sandbox:
            argv2.append("--allow-local-sandbox")
        if args.config_file:
            argv2 += ["--config-file", args.config_file]

        pid = spawn_daemon(
            DaemonSpec(argv=argv2, env=env, pidfile=pidfile, logfile=logfile),
            force=args.force,
        )
        # Quick health check.
        import time as _time
        import os as _os

        _time.sleep(0.2)
        try:
            _os.kill(pid, 0)
        except Exception:
            tail = tail_text_file(logfile)
            if tail.strip():
                print(tail.rstrip())
            raise SystemExit(f"daemon exited immediately; see logfile: {logfile}")
        print("pid:", pid)
        print("pidfile:", str(pidfile))
        print("logfile:", str(logfile))
        return 0

    # Foreground server.
    os.environ.update(env)
    uvicorn.run(
        "distbuild.api:app",
        host=str(args.host),
        port=int(args.port),
        log_level=str(args.log_level),
        access_log=bool(args.access_log),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
