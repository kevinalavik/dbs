#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import secrets
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Optional

import httpx


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name)
    if v is None or v == "":
        if default is None:
            raise SystemExit(f"missing env var: {name}")
        return default
    return v


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = int(s.getsockname()[1])
    s.close()
    return port


def _pump(prefix: str, pipe) -> None:
    try:
        for line in iter(pipe.readline, ""):
            sys.stdout.write(f"[{prefix}] {line}")
    finally:
        try:
            pipe.close()
        except Exception:
            pass


class _Color:
    def __init__(self, enabled: bool):
        self.enabled = enabled

    def _c(self, s: str, code: str) -> str:
        if not self.enabled:
            return s
        return f"\x1b[{code}m{s}\x1b[0m"

    def dim(self, s: str) -> str:
        return self._c(s, "2")

    def red(self, s: str) -> str:
        return self._c(s, "31")

    def green(self, s: str) -> str:
        return self._c(s, "32")

    def yellow(self, s: str) -> str:
        return self._c(s, "33")

    def blue(self, s: str) -> str:
        return self._c(s, "34")

    def magenta(self, s: str) -> str:
        return self._c(s, "35")

    def cyan(self, s: str) -> str:
        return self._c(s, "36")


def _colors_enabled(flag: str) -> bool:
    # flag: auto|always|never
    if flag == "never":
        return False
    if flag == "always":
        return True
    if os.getenv("NO_COLOR") is not None:
        return False
    return sys.stdout.isatty()


def _prefixed_pump(prefix: str, pipe, *, color: _Color, prefix_color: str, emit: bool) -> None:
    try:
        pfx = color._c(prefix, prefix_color) if color.enabled else prefix
        for line in iter(pipe.readline, ""):
            if emit:
                sys.stdout.write(f"[{pfx}] {line}")
    finally:
        try:
            pipe.close()
        except Exception:
            pass


@dataclass(frozen=True)
class LogConfig:
    api: bool = True
    api_body: bool = False
    admin: bool = True
    server: bool = True
    worker: bool = True
    compose: bool = True
    job_stdout: bool = True
    job_stderr: bool = True
    job_system: bool = True


def _parse_csv_set(s: str) -> set[str]:
    return {p.strip().lower() for p in s.split(",") if p.strip()}


def _logs_from_args(*, logs: str, job_streams: str, api_body: bool) -> LogConfig:
    cats = _parse_csv_set(logs)
    if "all" in cats:
        cats = {"api", "admin", "server", "worker", "compose", "job"}
    if "none" in cats:
        cats = set()

    streams = _parse_csv_set(job_streams)
    if "all" in streams:
        streams = {"stdout", "stderr", "system"}
    if "none" in streams:
        streams = set()

    job_enabled = "job" in cats
    return LogConfig(
        api=("api" in cats),
        api_body=api_body,
        admin=("admin" in cats),
        server=("server" in cats),
        worker=("worker" in cats),
        compose=("compose" in cats),
        job_stdout=(job_enabled and "stdout" in streams),
        job_stderr=(job_enabled and "stderr" in streams),
        job_system=(job_enabled and "system" in streams),
    )


def _start_server(*, port: int, worker_token: str, db_url: Optional[str], color: _Color, logs: LogConfig) -> subprocess.Popen:
    env = os.environ.copy()
    env["DISTBUILD_WORKER_SHARED_TOKEN"] = worker_token
    if db_url:
        env["DISTBUILD_DATABASE_URL"] = db_url

    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "distbuild.api:app",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--log-level",
        "info",
    ]
    p = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    threading.Thread(
        target=_prefixed_pump,
        args=("server", p.stdout),
        kwargs={"color": color, "prefix_color": "32", "emit": logs.server},
        daemon=True,
    ).start()
    threading.Thread(
        target=_prefixed_pump,
        args=("server", p.stderr),
        kwargs={"color": color, "prefix_color": "32", "emit": logs.server},
        daemon=True,
    ).start()
    return p


def _start_worker(*, server: str, worker_token: str, worker_id: str, color: _Color, logs: LogConfig) -> subprocess.Popen:
    cmd = [
        "distbuild-worker",
        "--server",
        server,
        "--worker-token",
        worker_token,
        "--worker-id",
        worker_id,
        "--poll-interval",
        "0.2",
    ]
    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    threading.Thread(
        target=_prefixed_pump,
        args=("worker", p.stdout),
        kwargs={"color": color, "prefix_color": "35", "emit": logs.worker},
        daemon=True,
    ).start()
    threading.Thread(
        target=_prefixed_pump,
        args=("worker", p.stderr),
        kwargs={"color": color, "prefix_color": "35", "emit": logs.worker},
        daemon=True,
    ).start()
    return p


def _docker_compose_available() -> bool:
    try:
        subprocess.run(["docker", "compose", "version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return True
    except Exception:
        return False


def _docker_usable() -> tuple[bool, str]:
    try:
        r = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5,
            check=False,
        )
        if r.returncode == 0:
            return True, ""
        return False, (r.stderr or "").strip()
    except FileNotFoundError:
        return False, "docker not found"
    except Exception as e:
        return False, repr(e)


def _compose_up(*, color: _Color, logs: LogConfig) -> None:
    if not _docker_compose_available():
        raise SystemExit("docker compose not available")
    if logs.compose:
        print(f"[{color.blue('compose')}] up --build -d")
    subprocess.run(["docker", "compose", "up", "--build", "-d"], check=True)


def _compose_down(*, color: _Color, logs: LogConfig) -> None:
    if not _docker_compose_available():
        return
    if logs.compose:
        print(f"[{color.blue('compose')}] down")
    subprocess.run(["docker", "compose", "down", "--remove-orphans"], check=True)


def _compose_logs_follow(*, color: _Color, logs: LogConfig) -> subprocess.Popen:
    # Stream compose logs in the background.
    p = subprocess.Popen(
        ["docker", "compose", "logs", "-f", "--no-color"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    threading.Thread(
        target=_prefixed_pump,
        args=("compose", p.stdout),
        kwargs={"color": color, "prefix_color": "34", "emit": logs.compose},
        daemon=True,
    ).start()
    threading.Thread(
        target=_prefixed_pump,
        args=("compose", p.stderr),
        kwargs={"color": color, "prefix_color": "34", "emit": logs.compose},
        daemon=True,
    ).start()
    return p


def _wait_http_ready(server: str, *, timeout_s: float = 20.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = httpx.get(server + "/openapi.json", timeout=1.0)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.2)
    raise SystemExit(f"server not ready after {timeout_s:.1f}s: {server}")


def _api(
    client: httpx.Client,
    method: str,
    path: str,
    *,
    color: _Color,
    logs: LogConfig,
    show_body: bool,
    **kwargs,
) -> httpx.Response:
    url = str(client.base_url) + path
    if logs.api:
        print(f"[{color.cyan('api')}] {color.dim(method)} {url}")
    r = client.request(method, path, **kwargs)
    code = str(r.status_code)
    if r.status_code >= 500:
        code = color.red(code)
    elif r.status_code >= 400:
        code = color.yellow(code)
    else:
        code = color.green(code)
    if logs.api:
        print(f"[{color.cyan('api')}] -> {code}")
    if (show_body or r.status_code >= 400) and (logs.api_body or r.status_code >= 400):
        ct = r.headers.get("content-type", "")
        if "application/json" in ct:
            try:
                print(f"[{color.cyan('api')}] body:", r.json())
            except Exception:
                print(f"[{color.cyan('api')}] body: <invalid json>")
        else:
            txt = r.text
            if len(txt) > 2000:
                txt = txt[:2000] + "..."
            if txt:
                print(f"[{color.cyan('api')}] body:", txt)
    return r


def _parse_consumer_key(output: str) -> str:
    for line in output.splitlines():
        if line.startswith("consumer_key:"):
            return line.split(":", 1)[1].strip()
    return ""


def _create_consumer_key(
    *,
    name: str,
    max_concurrent: int,
    max_per_day: int,
    db_url: Optional[str],
    color: _Color,
    logs: LogConfig,
) -> str:
    env = os.environ.copy()
    if db_url:
        env["DISTBUILD_DATABASE_URL"] = db_url
    cmd = [
        "distbuild-admin",
        "create-consumer",
        "--name",
        name,
        "--max-concurrent",
        str(max_concurrent),
        "--max-per-day",
        str(max_per_day),
    ]
    if logs.admin:
        print(f"[{color.magenta('admin')}] {' '.join(cmd)}")
    p = subprocess.run(cmd, env=env, text=True, capture_output=True)
    out = (p.stdout or "") + (p.stderr or "")
    if p.returncode != 0:
        print(f"[{color.red('error')}] distbuild-admin failed")
        if out.strip():
            print(out.rstrip())
        raise SystemExit(2)
    if logs.admin and out.strip():
        print(out.rstrip())
    key = _parse_consumer_key(out)
    if not key:
        print(f"[{color.red('error')}] failed to parse consumer_key from distbuild-admin output")
        print(out.rstrip())
        raise SystemExit(2)
    return key


def _reset_db(*, db_url: Optional[str], color: _Color, logs: LogConfig) -> None:
    env = os.environ.copy()
    if db_url:
        env["DISTBUILD_DATABASE_URL"] = db_url
    cmd = ["distbuild-admin", "reset-db", "--yes"]
    if logs.admin:
        print(f"[{color.magenta('admin')}] {' '.join(cmd)}")
    p = subprocess.run(cmd, env=env, text=True, capture_output=True)
    out = (p.stdout or "") + (p.stderr or "")
    if logs.admin and out.strip():
        print(out.rstrip())
    if p.returncode != 0:
        if out.strip() and not logs.admin:
            print(out.rstrip())
        raise SystemExit(2)


def _stop_process(p: subprocess.Popen) -> None:
    try:
        p.terminate()  # SIGTERM (cleaner than SIGINT; avoids KeyboardInterrupt stacktraces)
    except Exception:
        return
    try:
        p.wait(timeout=5)
    except Exception:
        try:
            p.kill()
        except Exception:
            pass


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="test_job.py")
    ap.add_argument("--consumer-key", default=os.getenv("DISTBUILD_CONSUMER_KEY"), required=False)
    ap.add_argument(
        "--create-consumer",
        default=None,
        help="create a consumer in the local DB and use its key (proc orchestrator only)",
    )
    ap.add_argument("--create-max-concurrent", type=int, default=2)
    ap.add_argument("--create-max-per-day", type=int, default=50)
    ap.add_argument("--command", default=os.getenv("DISTBUILD_TEST_COMMAND", "echo hello-from-distbuild && uname -a"))
    ap.add_argument("--sandbox", default=os.getenv("DISTBUILD_TEST_SANDBOX", "local"), choices=["local", "docker"])
    ap.add_argument("--image", default=os.getenv("DISTBUILD_TEST_IMAGE", ""), help="docker image (only for --sandbox docker)")
    ap.add_argument("--timeout", type=int, default=int(os.getenv("DISTBUILD_TEST_TIMEOUT", "60")))

    ap.add_argument("--suite", default="single", choices=["single", "big"])
    ap.add_argument("--quota-test", action="store_true", help="in big suite, also test max_jobs_per_day enforcement")

    ap.add_argument("--colors", default="auto", choices=["auto", "always", "never"])
    ap.add_argument("--api-body", action="store_true", help="print JSON responses")

    ap.add_argument(
        "--logs",
        default=os.getenv("DISTBUILD_TEST_LOGS", "all"),
        help="comma list: all,none,api,admin,server,worker,compose,job",
    )
    ap.add_argument(
        "--job-streams",
        default=os.getenv("DISTBUILD_TEST_JOB_STREAMS", "all"),
        help="comma list: all,none,stdout,stderr,system (only affects printed output)",
    )

    ap.add_argument("--reset-db", action="store_true", help="clear and re-init the local sqlite db (proc orchestrator only)")

    ap.add_argument(
        "--orchestrator",
        default="proc",
        choices=["proc", "compose"],
        help="proc=spawn uvicorn/worker locally; compose=docker compose up",
    )
    ap.add_argument("--server", default=os.getenv("DISTBUILD_SERVER", ""), help="base url (used for --orchestrator compose)")
    ap.add_argument("--keep-compose", action="store_true", help="do not docker compose down on exit")
    ap.add_argument("--compose-logs", action="store_true", help="follow docker compose logs")

    ap.add_argument("--db-url", default=os.getenv("DISTBUILD_DATABASE_URL"))
    ap.add_argument("--port", type=int, default=0, help="0 = choose a free local port")
    ap.add_argument("--no-server", action="store_true", help="do not start uvicorn")
    ap.add_argument("--no-worker", action="store_true", help="do not start a worker")
    ap.add_argument("--worker-token", default=os.getenv("DISTBUILD_WORKER_SHARED_TOKEN", ""))
    ap.add_argument("--worker-id", default="test-worker")
    args = ap.parse_args(argv)

    color = _Color(_colors_enabled(args.colors))
    logs = _logs_from_args(logs=args.logs, job_streams=args.job_streams, api_body=args.api_body)

    if args.sandbox == "docker" and args.orchestrator == "proc" and not args.no_worker:
        ok, err = _docker_usable()
        if not ok:
            msg = "docker sandbox requested but docker is not usable for this user"
            print(f"[{color.red('error')}] {msg}")
            if err:
                print(f"[{color.red('error')}] {err}")
            if "permission denied" in err.lower() and "/var/run/docker.sock" in err:
                print(
                    f"[{color.yellow('hint')}] run `sudo usermod -aG docker $USER` then re-login, or run `newgrp docker`"
                )
            return 2

    if args.create_consumer and args.orchestrator == "compose":
        raise SystemExit("--create-consumer is not supported with --orchestrator compose")

    if args.reset_db and args.orchestrator == "compose":
        raise SystemExit("--reset-db is not supported with --orchestrator compose")

    if not args.consumer_key and not args.create_consumer:
        raise SystemExit("missing --consumer-key (or DISTBUILD_CONSUMER_KEY), or use --create-consumer")

    worker_token = args.worker_token or ("test_" + secrets.token_urlsafe(16))
    if args.orchestrator == "compose":
        server = args.server or "http://127.0.0.1:8080"
        port = 0
    else:
        port = int(args.port) if args.port else _free_port()
        server = args.server or f"http://127.0.0.1:{port}"

    server_p: Optional[subprocess.Popen] = None
    worker_p: Optional[subprocess.Popen] = None
    compose_logs_p: Optional[subprocess.Popen] = None
    try:
        if args.orchestrator == "compose":
            _compose_up(color=color, logs=logs)
            if args.compose_logs:
                compose_logs_p = _compose_logs_follow(color=color, logs=logs)
            _wait_http_ready(server)
        else:
            if args.reset_db:
                _reset_db(db_url=args.db_url, color=color, logs=logs)

            if not args.no_server:
                server_p = _start_server(port=port, worker_token=worker_token, db_url=args.db_url, color=color, logs=logs)
                _wait_http_ready(server)

            if not args.no_worker:
                worker_p = _start_worker(server=server, worker_token=worker_token, worker_id=args.worker_id, color=color, logs=logs)

        def get_consumer_key() -> str:
            if args.consumer_key:
                return args.consumer_key
            assert args.create_consumer is not None
            key = _create_consumer_key(
                name=args.create_consumer,
                max_concurrent=args.create_max_concurrent,
                max_per_day=args.create_max_per_day,
                db_url=args.db_url,
                color=color,
                logs=logs,
            )
            print("consumer_key:", key)
            return key

        consumer_key = get_consumer_key()
        headers = {"X-Consumer-Key": consumer_key}

        def print_job_line(job_prefix: str, stream: str, text: str) -> None:
            if stream == "stdout" and not logs.job_stdout:
                return
            if stream == "stderr" and not logs.job_stderr:
                return
            if stream == "system" and not logs.job_system:
                return
            if stream == "stderr":
                pfx = color.red(f"{job_prefix}:{stream}")
            elif stream == "system":
                pfx = color.yellow(f"{job_prefix}:{stream}")
            else:
                pfx = color.blue(f"{job_prefix}:{stream}")
            sys.stdout.write(f"[{pfx}] {text}")

        want_job_logs = logs.job_stdout or logs.job_stderr or logs.job_system

        with httpx.Client(base_url=server, headers=headers, timeout=30.0) as client:
            def submit_job(*, name: str, command: str, sandbox: str, timeout_s: int, image: str | None = None) -> dict:
                body = {"command": command, "sandbox": sandbox, "timeout_seconds": timeout_s}
                if sandbox == "docker" and image:
                    body["image"] = image
                r = _api(client, "POST", "/v1/jobs", color=color, logs=logs, show_body=args.api_body, json=body)
                if r.status_code != 200:
                    if r.status_code == 401:
                        print(
                            f"[{color.yellow('hint')}] 401 usually means the consumer key isn't in the current DB. If you ran reset-db, create a new consumer key."
                        )
                    raise SystemExit(1)
                j = r.json()
                print("submitted job:", name, j["id"])
                return j

            if args.suite == "single":
                job = submit_job(
                    name="single",
                    command=args.command,
                    sandbox=args.sandbox,
                    timeout_s=args.timeout,
                    image=args.image or None,
                )
                job_id = job["id"]
                job_prefix = f"job:{str(job_id)[:8]}"
                offset = 0
                last_status = None
                while True:
                    jr = _api(client, "GET", f"/v1/jobs/{job_id}", color=color, logs=logs, show_body=args.api_body)
                    jr.raise_for_status()
                    st = jr.json()
                    status = st["status"]
                    if status != last_status:
                        if status == "succeeded":
                            status_s = color.green(status)
                        elif status in ("failed", "cancelled"):
                            status_s = color.red(status)
                        elif status == "running":
                            status_s = color.yellow(status)
                        else:
                            status_s = color.dim(status)
                        print("status:", status_s)
                        last_status = status

                    if want_job_logs:
                        lr = _api(
                            client,
                            "GET",
                            f"/v1/jobs/{job_id}/logs",
                            color=color,
                            logs=logs,
                            show_body=False,
                            params={"offset_seq": offset, "limit": 500},
                        )
                        lr.raise_for_status()
                        payload = lr.json()
                        for c in payload.get("chunks", []):
                            print_job_line(job_prefix, c.get("stream", "?"), c.get("text", ""))
                        offset = int(payload.get("next_offset_seq", offset))

                    if status in ("succeeded", "failed", "cancelled"):
                        if want_job_logs:
                            for _ in range(5):
                                lr2 = _api(
                                    client,
                                    "GET",
                                    f"/v1/jobs/{job_id}/logs",
                                    color=color,
                                    logs=logs,
                                    show_body=False,
                                    params={"offset_seq": offset, "limit": 500},
                                )
                                lr2.raise_for_status()
                                payload2 = lr2.json()
                                chunks2 = payload2.get("chunks", [])
                                if not chunks2:
                                    break
                                for c in chunks2:
                                    print_job_line(job_prefix, c.get("stream", "?"), c.get("text", ""))
                                offset = int(payload2.get("next_offset_seq", offset))
                                time.sleep(0.2)

                        print("exit_code:", st.get("exit_code"))
                        if st.get("error"):
                            print("error:", st.get("error"))
                        return 0 if status == "succeeded" else 1
                    time.sleep(0.5)

            # Big suite
            tests: list[dict] = []
            tests.append(
                {
                    "name": "local-ok",
                    "sandbox": "local",
                    "timeout": 30,
                    "command": "echo local-ok && python -V",
                    "expect": "succeeded",
                }
            )
            tests.append(
                {
                    "name": "local-stderr",
                    "sandbox": "local",
                    "timeout": 30,
                    "command": "echo out && echo err 1>&2 && python -c \"print('py-out')\"",
                    "expect": "succeeded",
                }
            )
            tests.append(
                {
                    "name": "local-fail",
                    "sandbox": "local",
                    "timeout": 30,
                    "command": "echo about-to-fail && exit 7",
                    "expect": "failed",
                }
            )

            net_check = (
                "python -c \"import socket; s=socket.socket(); s.settimeout(1); "
                "\ntry: s.connect(('1.1.1.1', 53)); print('network reachable'); raise SystemExit(1) "
                "\nexcept Exception as e: print('network blocked:', type(e).__name__, e); raise SystemExit(0)\""
            )
            tests.append(
                {
                    "name": "docker-ok",
                    "sandbox": "docker",
                    "timeout": 60,
                    "command": "echo docker-ok && python -V",
                    "image": args.image or None,
                    "expect": "succeeded",
                }
            )
            tests.append(
                {
                    "name": "docker-no-net",
                    "sandbox": "docker",
                    "timeout": 60,
                    "command": net_check,
                    "image": args.image or None,
                    "expect": "succeeded",
                }
            )
            tests.append(
                {
                    "name": "docker-timeout",
                    "sandbox": "docker",
                    "timeout": 1,
                    "command": "python -c \"import time; time.sleep(3); print('done')\"",
                    "image": args.image or None,
                    "expect": "failed",
                }
            )
            tests.append(
                {
                    "name": "docker-ident",
                    "sandbox": "docker",
                    "timeout": 60,
                    "command": "id && hostname && cat /etc/os-release | head -n 3",
                    "image": args.image or None,
                    "expect": "succeeded",
                }
            )
            tests.append(
                {
                    "name": "docker-output",
                    "sandbox": "docker",
                    "timeout": 60,
                    "command": "python -c \"[print('line', i) for i in range(200)]\"",
                    "image": args.image or None,
                    "expect": "succeeded",
                }
            )

            # Submit all tests.
            jobs: list[dict] = []
            for t in tests:
                jobs.append(
                    {
                        "test": t,
                        "job": submit_job(
                            name=t["name"],
                            command=t["command"],
                            sandbox=t["sandbox"],
                            timeout_s=int(t["timeout"]),
                            image=t.get("image"),
                        ),
                        "offset": 0,
                        "done": False,
                        "final": None,
                    }
                )

            pending = len(jobs)
            print(f"running suite: {pending} jobs")
            while pending:
                for rec in jobs:
                    if rec["done"]:
                        continue
                    job_id = rec["job"]["id"]
                    test = rec["test"]
                    job_prefix = f"{test['name']}:{str(job_id)[:8]}"

                    jr = _api(client, "GET", f"/v1/jobs/{job_id}", color=color, logs=logs, show_body=False)
                    jr.raise_for_status()
                    st = jr.json()
                    status = st["status"]

                    if want_job_logs:
                        lr = _api(
                            client,
                            "GET",
                            f"/v1/jobs/{job_id}/logs",
                            color=color,
                            logs=logs,
                            show_body=False,
                            params={"offset_seq": rec["offset"], "limit": 500},
                        )
                        lr.raise_for_status()
                        payload = lr.json()
                        for c in payload.get("chunks", []):
                            print_job_line(job_prefix, c.get("stream", "?"), c.get("text", ""))
                        rec["offset"] = int(payload.get("next_offset_seq", rec["offset"]))

                    if status in ("succeeded", "failed", "cancelled"):
                        # Final drain.
                        if want_job_logs:
                            for _ in range(5):
                                lr2 = _api(
                                    client,
                                    "GET",
                                    f"/v1/jobs/{job_id}/logs",
                                    color=color,
                                    logs=logs,
                                    show_body=False,
                                    params={"offset_seq": rec["offset"], "limit": 500},
                                )
                                lr2.raise_for_status()
                                payload2 = lr2.json()
                                chunks2 = payload2.get("chunks", [])
                                if not chunks2:
                                    break
                                for c in chunks2:
                                    print_job_line(job_prefix, c.get("stream", "?"), c.get("text", ""))
                                rec["offset"] = int(payload2.get("next_offset_seq", rec["offset"]))
                                time.sleep(0.2)

                        rec["done"] = True
                        rec["final"] = st
                        pending -= 1
                time.sleep(0.3)

            # Optional quota test (daily limit).
            if args.quota_test and args.orchestrator == "proc":
                qname = "quota_" + secrets.token_urlsafe(6)
                qkey = _create_consumer_key(
                    name=qname,
                    max_concurrent=1,
                    max_per_day=3,
                    db_url=args.db_url,
                    color=color,
                    logs=logs,
                )
                qheaders = {"X-Consumer-Key": qkey}
                with httpx.Client(base_url=server, headers=qheaders, timeout=30.0) as qclient:
                    for i in range(4):
                        r = _api(
                            qclient,
                            "POST",
                            "/v1/jobs",
                            color=color,
                            logs=logs,
                            show_body=True,
                            json={"command": f"echo quota-{i}", "sandbox": "local", "timeout_seconds": 10},
                        )
                        if i < 3 and r.status_code != 200:
                            raise SystemExit(1)
                        if i == 3 and r.status_code != 429:
                            print(f"[{color.red('error')}] expected 429 on 4th submit, got {r.status_code}")
                            raise SystemExit(1)

            # Summary.
            ok = 0
            bad = 0
            print("suite results:")
            for rec in jobs:
                test = rec["test"]
                st = rec["final"] or {}
                status = st.get("status")
                expect = test.get("expect")
                passed = status == expect
                line = f"- {test['name']}: {status} (expect {expect}) exit={st.get('exit_code')}"
                if passed:
                    print(color.green(line))
                    ok += 1
                else:
                    print(color.red(line))
                    bad += 1
            print(f"passed: {ok} failed: {bad}")
            return 0 if bad == 0 else 1
    finally:
        for p in (compose_logs_p, worker_p, server_p):
            if p is None:
                continue
            _stop_process(p)

        if args.orchestrator == "compose" and not args.keep_compose:
            try:
                _compose_down(color=color, logs=logs)
            except Exception as e:
                print(f"[{color.blue('compose')}] down failed: {e!r}")


if __name__ == "__main__":
    raise SystemExit(main())
