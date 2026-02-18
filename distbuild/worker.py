from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx

from distbuild.sandbox import SandboxLimits, run_sandbox
from distbuild.daemon import (
    DaemonSpec,
    default_runtime_dir,
    spawn_daemon,
    status_daemon,
    stop_daemon,
    tail_text_file,
)
from distbuild.utils import parse_toml


def _now() -> str:
    return datetime.utcnow().isoformat() + "Z"


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="distbuild-worker")
    ap.add_argument("--config-file", default=os.getenv("DISTBUILD_WORKER_CONFIG", ""))
    ap.add_argument("--server", default="http://127.0.0.1:8080")
    ap.add_argument("--worker-token", default=os.getenv("DISTBUILD_WORKER_SHARED_TOKEN", ""))
    ap.add_argument("--worker-id", default="worker")
    ap.add_argument("--poll-interval", type=float, default=1.0)
    ap.add_argument("--log-level", default=os.getenv("DISTBUILD_LOG_LEVEL", "info"))

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
        if "server" in cfg and args.server == "http://127.0.0.1:8080":
            args.server = str(cfg["server"])
        if "worker_token" in cfg and not args.worker_token:
            args.worker_token = str(cfg["worker_token"])
        if "worker_id" in cfg and args.worker_id == "worker":
            args.worker_id = str(cfg["worker_id"])
        if "poll_interval" in cfg and args.poll_interval == 1.0:
            args.poll_interval = float(cfg["poll_interval"])
        if "log_level" in cfg and args.log_level == os.getenv("DISTBUILD_LOG_LEVEL", "info"):
            args.log_level = str(cfg["log_level"])

    import logging

    logging.basicConfig(
        level=str(args.log_level).upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger("distbuild.worker")

    run_dir = default_runtime_dir() / "worker"
    safe_id = "".join([c for c in args.worker_id if c.isalnum() or c in ("-", "_", ".")]) or "worker"
    pidfile = os.path.expanduser(args.pidfile) if args.pidfile else str(run_dir / f"distbuild-worker.{safe_id}.pid")
    logfile = os.path.expanduser(args.log_file) if args.log_file else str(run_dir / f"distbuild-worker.{safe_id}.log")

    if args.start:
        args.daemon = True

    if args.stop:
        stop_daemon(Path(pidfile), sig=args.signal)
        print("stopped")
        return 0
    if args.status:
        rc = status_daemon(Path(pidfile))
        if rc == 0:
            print("running")
        elif rc == 1:
            print("not running")
            tail = tail_text_file(Path(logfile))
            if tail.strip():
                print("log_tail:")
                print(tail.rstrip())
        else:
            print("unknown")
        return rc

    if not args.worker_token:
        raise SystemExit("missing --worker-token (or DISTBUILD_WORKER_SHARED_TOKEN)")

    if args.restart:
        try:
            stop_daemon(Path(pidfile), sig=args.signal)
        except Exception:
            pass
        args.daemon = True

    if args.daemon:
        env = os.environ.copy()
        env["DISTBUILD_WORKER_SHARED_TOKEN"] = args.worker_token
        argv2 = [
            sys.executable,
            "-m",
            "distbuild.worker",
            "--server",
            str(args.server),
            "--worker-token",
            str(args.worker_token),
            "--worker-id",
            str(args.worker_id),
            "--poll-interval",
            str(args.poll_interval),
            "--log-level",
            str(args.log_level),
        ]
        if args.config_file:
            argv2 += ["--config-file", str(args.config_file)]
        pid = spawn_daemon(
            DaemonSpec(argv=argv2, env=env, pidfile=Path(pidfile), logfile=Path(logfile)),
            force=args.force,
        )
        import time as _time
        import os as _os

        _time.sleep(0.2)
        try:
            _os.kill(pid, 0)
        except Exception:
            print(f"daemon exited immediately; see logfile: {logfile}")
            return 1
        print("pid:", pid)
        print("pidfile:", pidfile)
        print("logfile:", logfile)
        return 0

    headers = {
        "X-Worker-Token": args.worker_token,
        "X-Worker-Id": args.worker_id,
    }

    log.info("starting worker_id=%s server=%s", args.worker_id, args.server)
    with httpx.Client(base_url=args.server, headers=headers, timeout=30.0) as client:
        try:
            while True:
                try:
                    r = client.post("/v1/worker/claim")
                    r.raise_for_status()
                    data = r.json()
                except httpx.HTTPStatusError as e:
                    status = e.response.status_code
                    body = None
                    try:
                        body = e.response.json()
                    except Exception:
                        body = e.response.text
                    log.warning("claim failed status=%s body=%s", status, body)
                    if status >= 500:
                        log.warning(
                            "server error while claiming; common cause: API missing DISTBUILD_WORKER_SHARED_TOKEN"
                        )
                    time.sleep(max(args.poll_interval, 1.0))
                    continue
                except Exception as e:
                    log.warning("claim request failed: %r", e)
                    time.sleep(max(args.poll_interval, 1.0))
                    continue

                job = data.get("job")
                if not job:
                    time.sleep(args.poll_interval)
                    continue

                job_id = job["id"]
                command = job["command"]
                timeout_seconds = int(job["timeout_seconds"])
                sandbox = job["sandbox"]
                image = job.get("image")

                seq_buf: list[dict] = []

                def flush():
                    nonlocal seq_buf
                    if not seq_buf:
                        return
                    client.post(f"/v1/worker/jobs/{job_id}/logs", json={"chunks": seq_buf}).raise_for_status()
                    seq_buf = []

                def on_log(stream: str, text: str) -> None:
                    seq_buf.append({"seq": 0, "ts": _now(), "stream": stream, "text": text})
                    if len(seq_buf) >= 50:
                        flush()

                on_log("system", f"claimed job {job_id} at {_now()}\n")
                flush()

                exit_code = 1
                err: Optional[str] = None
                try:
                    exit_code = run_sandbox(
                        sandbox=sandbox,
                        command=command,
                        timeout_seconds=timeout_seconds,
                        image=image,
                        limits=SandboxLimits(),
                        on_log=on_log,
                    )
                except Exception as e:
                    err = f"worker exception: {e!r}"
                    on_log("system", err + "\n")
                finally:
                    flush()

                status = "succeeded" if (err is None and exit_code == 0) else "failed"
                client.post(
                    f"/v1/worker/jobs/{job_id}/finish",
                    json={"status": status, "exit_code": exit_code, "error": err},
                ).raise_for_status()
        except KeyboardInterrupt:
            log.info("shutdown requested")
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
