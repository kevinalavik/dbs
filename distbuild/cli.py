from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Optional

import httpx

from distbuild.client import ClientConfig, DistBuildClient, JobFuture
from distbuild.utils import (
    atomic_write_text,
    default_config_path,
    dumps_config_profiles,
    parse_toml,
    redact_key,
)


def _print_http_error(e: httpx.HTTPStatusError) -> None:
    r = e.response
    try:
        detail = r.json()
    except Exception:
        detail = r.text
    print("http error:", r.status_code, str(r.request.method), str(r.request.url))
    if detail:
        print("detail:", detail)
    if r.status_code == 401:
        print("hint: use the admin-generated consumer_key (looks like `kid_...db_...`), not consumer_id")


def _pick_profile(data: dict[str, Any], *, override: Optional[str]) -> tuple[str, dict[str, Any]]:
    # Back-compat: flat config means a single implicit profile.
    if "profiles" not in data and "server" in data and "consumer_key" in data:
        prof = {
            "server": data.get("server"),
            "consumer_key": data.get("consumer_key"),
            "default_sandbox": data.get("default_sandbox", "local"),
            "default_timeout_seconds": data.get("default_timeout_seconds", 600),
            "default_image": data.get("default_image", ""),
        }
        return "default", prof

    profiles = data.get("profiles") or {}
    if not isinstance(profiles, dict) or not profiles:
        raise SystemExit("config has no profiles")

    active = str(data.get("active_profile", "default"))
    name = override or active
    prof = profiles.get(name)
    if not isinstance(prof, dict):
        raise SystemExit(f"profile not found: {name}")
    return name, prof


def _load_client_config(config_path: Path, *, profile: Optional[str]) -> ClientConfig:
    if not config_path.exists():
        raise SystemExit(f"config not found: {config_path} (run `distbuild init`)")
    data = parse_toml(config_path.read_text(encoding="utf-8"))
    _, prof = _pick_profile(data, override=profile)

    server = str(prof.get("server", "")).strip()
    consumer_key = str(prof.get("consumer_key", "")).strip()
    if not server:
        raise SystemExit("missing 'server' in profile")
    if not consumer_key:
        raise SystemExit("missing 'consumer_key' in profile")

    return ClientConfig(
        server=server,
        consumer_key=consumer_key,
        default_sandbox=str(prof.get("default_sandbox", "local")),
        default_timeout_seconds=int(prof.get("default_timeout_seconds", 600)),
        default_image=str(prof.get("default_image", "")),
    )


def _load_raw_config(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        return {}
    return parse_toml(config_path.read_text(encoding="utf-8"))


def _save_raw_config(config_path: Path, data: dict[str, Any]) -> None:
    atomic_write_text(config_path, dumps_config_profiles(data))


def cmd_init(args: argparse.Namespace) -> int:
    path = Path(args.config)
    data = _load_raw_config(path)
    prof_name = args.profile

    # Normalize existing config.
    if data and "profiles" not in data and "server" in data and "consumer_key" in data:
        data = {
            "active_profile": "default",
            "profiles": {
                "default": {
                    "server": data.get("server"),
                    "consumer_key": data.get("consumer_key"),
                    "default_sandbox": data.get("default_sandbox", "local"),
                    "default_timeout_seconds": data.get("default_timeout_seconds", 600),
                    "default_image": data.get("default_image", ""),
                }
            },
        }

    if not data:
        data = {"active_profile": prof_name, "profiles": {}}
    if "profiles" not in data or not isinstance(data.get("profiles"), dict):
        data["profiles"] = {}

    profiles: dict[str, Any] = data["profiles"]  # type: ignore[assignment]
    if prof_name in profiles and not args.force:
        raise SystemExit(f"profile already exists: {prof_name} (use --force to replace)")

    if "." not in args.consumer_key:
        raise SystemExit(
            "consumer_key looks wrong (expected `kid_...db_...`). "
            "Run `distbuild-admin create-consumer ...` and copy the printed consumer_key."
        )

    profiles[prof_name] = {
        "server": args.server,
        "consumer_key": args.consumer_key,
        "default_sandbox": args.default_sandbox,
        "default_timeout_seconds": int(args.default_timeout_seconds),
        "default_image": args.default_image or "",
    }
    if not args.no_set_active:
        data["active_profile"] = prof_name

    _save_raw_config(path, data)
    print("wrote:", str(path))
    print("profile:", prof_name)
    return 0


def cmd_profiles(args: argparse.Namespace) -> int:
    path = Path(args.config)
    data = _load_raw_config(path)
    if not data:
        raise SystemExit(f"config not found: {path}")

    # Back-compat
    if "profiles" not in data and "server" in data and "consumer_key" in data:
        print("* default")
        return 0

    active = str(data.get("active_profile", "default"))
    profiles = data.get("profiles") or {}
    if not isinstance(profiles, dict):
        raise SystemExit("config profiles is not a dict")

    for name in sorted(profiles.keys()):
        mark = "*" if name == active else " "
        print(f"{mark} {name}")
    return 0


def cmd_use(args: argparse.Namespace) -> int:
    path = Path(args.config)
    data = _load_raw_config(path)
    if not data:
        raise SystemExit(f"config not found: {path}")

    if "profiles" not in data:
        raise SystemExit("cannot `use` with legacy flat config; re-run `distbuild init --force`")
    profiles = data.get("profiles") or {}
    if args.profile_name not in profiles:
        raise SystemExit(f"profile not found: {args.profile_name}")

    data["active_profile"] = args.profile_name
    _save_raw_config(path, data)
    print("active_profile:", args.profile_name)
    return 0


def cmd_config_show(args: argparse.Namespace) -> int:
    path = Path(args.config)
    data = _load_raw_config(path)
    if not data:
        raise SystemExit(f"config not found: {path}")

    prof_name, prof = _pick_profile(data, override=args.profile)

    print("config:", str(path))
    print("profile:", prof_name)
    print("server:", str(prof.get("server", "")))
    print("consumer_key:", redact_key(str(prof.get("consumer_key", ""))))
    print("default_sandbox:", str(prof.get("default_sandbox", "local")))
    print("default_timeout_seconds:", int(prof.get("default_timeout_seconds", 600)))
    img = str(prof.get("default_image", ""))
    if img:
        print("default_image:", img)
    return 0


def cmd_ping(args: argparse.Namespace) -> int:
    cfg = _load_client_config(Path(args.config), profile=args.profile)
    with DistBuildClient(cfg) as client:
        try:
            payload = client.list_jobs(limit=1, offset=0)
        except httpx.HTTPStatusError as e:
            _print_http_error(e)
            return 1
        print("ok")
        print("server:", cfg.server)
        print("jobs_visible:", len(payload.get("jobs", [])))
    return 0


def _read_command(args: argparse.Namespace) -> str:
    if args.command and (args.command_file or args.stdin):
        raise SystemExit("use only one of --command/--command-file/--stdin")
    if args.command_file and args.stdin:
        raise SystemExit("use only one of --command-file/--stdin")

    if args.stdin:
        return sys.stdin.read()
    if args.command_file:
        return Path(args.command_file).read_text(encoding="utf-8")
    if args.command:
        return args.command
    raise SystemExit("missing command (use --command or --command-file or --stdin)")


def cmd_submit(args: argparse.Namespace) -> int:
    cfg = _load_client_config(Path(args.config), profile=args.profile)
    command = _read_command(args)

    with DistBuildClient(cfg) as client:
        if args.repeat < 1:
            raise SystemExit("--repeat must be >= 1")

        futs: list[JobFuture] = []
        for _ in range(args.repeat):
            try:
                futs.append(
                    client.submit_future(
                        command=command,
                        sandbox=args.sandbox,
                        timeout_seconds=args.timeout_seconds,
                        image=args.image,
                    )
                )
            except httpx.HTTPStatusError as e:
                _print_http_error(e)
                return 1

        job_ids = [f.job_id for f in futs]
        if args.json:
            print(json.dumps({"job_ids": job_ids}))
        else:
            for jid in job_ids:
                print(jid)

        if not (args.wait or args.follow):
            return 0

        # Wait sequentially; simple and reliable.
        rc = 0
        for fut in futs:
            final = fut.wait(print_logs=args.follow, poll_seconds=float(args.poll))
            st = final.get("status")
            if not args.json:
                print("job:", fut.job_id, "status:", st, "exit_code:", final.get("exit_code"))
            if st != "succeeded":
                rc = 1
        return rc


def cmd_wait(args: argparse.Namespace) -> int:
    cfg = _load_client_config(Path(args.config), profile=args.profile)
    with DistBuildClient(cfg) as client:
        fut = JobFuture(client, args.job_id)
        try:
            final = fut.wait(print_logs=args.follow, poll_seconds=float(args.poll))
        except httpx.HTTPStatusError as e:
            _print_http_error(e)
            return 1
        if args.json:
            print(json.dumps(final))
        else:
            print("status:", final.get("status"))
            if final.get("exit_code") is not None:
                print("exit_code:", final.get("exit_code"))
            if final.get("error"):
                print("error:", final.get("error"))
        return 0 if final.get("status") == "succeeded" else 1


def cmd_status(args: argparse.Namespace) -> int:
    cfg = _load_client_config(Path(args.config), profile=args.profile)
    with DistBuildClient(cfg) as client:
        try:
            job = client.get_job(args.job_id)
        except httpx.HTTPStatusError as e:
            _print_http_error(e)
            return 1
        if args.json:
            print(json.dumps(job))
            return 0

        print("id:", job.get("id"))
        print("status:", job.get("status"))
        if job.get("worker_id"):
            print("worker:", job.get("worker_id"))
        if job.get("exit_code") is not None:
            print("exit_code:", job.get("exit_code"))
        if job.get("error"):
            print("error:", job.get("error"))
    return 0


def cmd_logs(args: argparse.Namespace) -> int:
    cfg = _load_client_config(Path(args.config), profile=args.profile)
    streams = {s.strip().lower() for s in (args.streams or "").split(",") if s.strip()}
    if not streams:
        streams = {"stdout", "stderr", "system"}
    with DistBuildClient(cfg) as client:
        try:
            payload = client.get_logs(args.job_id, offset_seq=int(args.offset_seq), limit=int(args.limit))
        except httpx.HTTPStatusError as e:
            _print_http_error(e)
            return 1
        if args.json:
            print(json.dumps(payload))
            return 0
        for c in payload.get("chunks", []):
            stream = str(c.get("stream", "?"))
            if stream not in streams:
                continue
            text = str(c.get("text", ""))
            sys.stdout.write(f"[{stream}] {text}")
        return 0


def cmd_tail(args: argparse.Namespace) -> int:
    cfg = _load_client_config(Path(args.config), profile=args.profile)
    streams = {s.strip().lower() for s in (args.streams or "").split(",") if s.strip()}
    if not streams:
        streams = {"stdout", "stderr", "system"}
    with DistBuildClient(cfg) as client:
        fut = JobFuture(client, args.job_id)
        while True:
            try:
                for c in fut.iter_logs():
                    stream = str(c.get("stream", "?"))
                    if stream not in streams:
                        continue
                    text = str(c.get("text", ""))
                    sys.stdout.write(f"[{stream}] {text}")
                j = client.get_job(args.job_id)
            except httpx.HTTPStatusError as e:
                _print_http_error(e)
                return 1
            st = j.get("status")
            if st in ("succeeded", "failed", "cancelled"):
                return 0 if st == "succeeded" else 1
            import time

            time.sleep(float(args.poll))


def cmd_list(args: argparse.Namespace) -> int:
    cfg = _load_client_config(Path(args.config), profile=args.profile)
    with DistBuildClient(cfg) as client:
        try:
            payload = client.list_jobs(limit=int(args.limit), offset=int(args.offset))
        except httpx.HTTPStatusError as e:
            _print_http_error(e)
            return 1
        if args.json:
            print(json.dumps(payload))
            return 0
        jobs = payload.get("jobs", [])
        for j in jobs:
            jid = str(j.get("id"))
            st = str(j.get("status"))
            cmd = str(j.get("command", ""))
            if len(cmd) > 80:
                cmd = cmd[:77] + "..."
            print(jid, st, cmd)
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="distbuild")
    ap.add_argument(
        "--config",
        default=os.getenv("DISTBUILD_CONFIG", str(default_config_path())),
        help="path to config.toml (or DISTBUILD_CONFIG)",
    )
    ap.add_argument(
        "--profile",
        default=os.getenv("DISTBUILD_PROFILE"),
        help="profile name (or DISTBUILD_PROFILE)",
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    init = sub.add_parser("init", help="write a config file")
    init.add_argument("--profile", default="default")
    init.add_argument("--server", default="http://127.0.0.1:8080")
    init.add_argument("--consumer-key", required=True)
    init.add_argument("--default-sandbox", default="local", choices=["local", "docker"])
    init.add_argument("--default-timeout-seconds", type=int, default=600)
    init.add_argument("--default-image", default="")
    init.add_argument("--force", action="store_true")
    init.add_argument("--no-set-active", action="store_true", help="do not change active_profile")
    init.set_defaults(fn=cmd_init)

    pr = sub.add_parser("profiles", help="list config profiles")
    pr.set_defaults(fn=cmd_profiles)

    use = sub.add_parser("use", help="set active profile")
    use.add_argument("profile_name")
    use.set_defaults(fn=cmd_use)

    cs = sub.add_parser("config", help="show effective config")
    cs.set_defaults(fn=cmd_config_show)

    ping = sub.add_parser("ping", help="check server/auth")
    ping.set_defaults(fn=cmd_ping)

    submit = sub.add_parser("submit", help="submit a job")
    submit.add_argument("--command")
    submit.add_argument("--command-file")
    submit.add_argument("--stdin", action="store_true")
    submit.add_argument("--sandbox", choices=["local", "docker"], default=None)
    submit.add_argument("--timeout-seconds", type=int, default=None)
    submit.add_argument("--image", default=None)
    submit.add_argument("--repeat", type=int, default=1)
    submit.add_argument("--wait", action="store_true")
    submit.add_argument("--follow", action="store_true")
    submit.add_argument("--poll", type=float, default=0.5)
    submit.add_argument("--json", action="store_true")
    submit.set_defaults(fn=cmd_submit)

    wait = sub.add_parser("wait", help="wait for job completion")
    wait.add_argument("job_id")
    wait.add_argument("--follow", action="store_true")
    wait.add_argument("--poll", type=float, default=0.5)
    wait.add_argument("--json", action="store_true")
    wait.set_defaults(fn=cmd_wait)

    st = sub.add_parser("status", help="show job status")
    st.add_argument("job_id")
    st.add_argument("--json", action="store_true")
    st.set_defaults(fn=cmd_status)

    lg = sub.add_parser("logs", help="fetch job logs")
    lg.add_argument("job_id")
    lg.add_argument("--offset-seq", type=int, default=0)
    lg.add_argument("--limit", type=int, default=500)
    lg.add_argument("--streams", default="stdout,stderr,system")
    lg.add_argument("--json", action="store_true")
    lg.set_defaults(fn=cmd_logs)

    tl = sub.add_parser("tail", help="follow logs (no wait summary)")
    tl.add_argument("job_id")
    tl.add_argument("--poll", type=float, default=0.5)
    tl.add_argument("--streams", default="stdout,stderr,system")
    tl.set_defaults(fn=cmd_tail)

    ls = sub.add_parser("list", help="list your jobs")
    ls.add_argument("--limit", type=int, default=20)
    ls.add_argument("--offset", type=int, default=0)
    ls.add_argument("--json", action="store_true")
    ls.set_defaults(fn=cmd_list)

    args = ap.parse_args(argv)
    return int(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(main())
