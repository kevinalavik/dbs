from __future__ import annotations

import os
import resource
import shutil
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass(frozen=True)
class SandboxLimits:
    cpu_seconds: int = 300
    memory_bytes: int = 1024 * 1024 * 1024
    pids: int = 256


def _set_limits(lim: SandboxLimits) -> None:
    resource.setrlimit(resource.RLIMIT_CPU, (lim.cpu_seconds, lim.cpu_seconds))
    resource.setrlimit(resource.RLIMIT_AS, (lim.memory_bytes, lim.memory_bytes))
    resource.setrlimit(resource.RLIMIT_NPROC, (lim.pids, lim.pids))
    resource.setrlimit(resource.RLIMIT_NOFILE, (256, 256))


def run_local(
    command: str,
    *,
    timeout_seconds: int,
    limits: SandboxLimits,
    on_log: Callable[[str, str], None],
) -> int:
    with tempfile.TemporaryDirectory(prefix="distbuild_job_") as td:
        env = {
            "PATH": os.getenv("PATH", "/usr/bin:/bin"),
            "HOME": td,
        }

        p = subprocess.Popen(
            command,
            cwd=td,
            env=env,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            preexec_fn=lambda: _set_limits(limits),
        )

        def pump(stream_name: str, f):
            try:
                for line in iter(f.readline, ""):
                    on_log(stream_name, line)
            finally:
                try:
                    f.close()
                except Exception:
                    pass

        t1 = threading.Thread(target=pump, args=("stdout", p.stdout), daemon=True)
        t2 = threading.Thread(target=pump, args=("stderr", p.stderr), daemon=True)
        t1.start()
        t2.start()

        try:
            rc = p.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            on_log("system", f"timeout after {timeout_seconds}s\n")
            p.kill()
            rc = 124

        # Ensure pump threads drain remaining output.
        try:
            t1.join(timeout=1.0)
            t2.join(timeout=1.0)
        except Exception:
            pass
        return rc


def run_docker(
    command: str,
    *,
    timeout_seconds: int,
    image: str,
    limits: SandboxLimits,
    on_log: Callable[[str, str], None],
) -> int:
    if shutil.which("docker") is None:
        on_log("system", "docker not found; falling back to local\n")
        return run_local(command, timeout_seconds=timeout_seconds, limits=limits, on_log=on_log)

    try:
        pre = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5,
            check=False,
        )
        if pre.returncode != 0:
            err = (pre.stderr or "").strip()
            on_log("system", "docker is not usable on this worker\n")
            if err:
                on_log("system", err + "\n")
            if "permission denied" in err.lower() and "/var/run/docker.sock" in err:
                on_log(
                    "system",
                    "hint: add your user to the docker group and re-login (or run `newgrp docker`)\n",
                )
            return 126
    except Exception as e:
        on_log("system", f"docker preflight failed: {e!r}\n")
        return 126

    # Hardening defaults:
    # - No network access
    # - No additional Linux capabilities
    # - No new privileges (blocks setuid escalation)
    # - Read-only root filesystem; writable tmpfs for /tmp and /work
    # - Run as "nobody" inside container
    cpu = str(max(0.1, min(4.0, limits.cpu_seconds / 300)))
    docker_cmd = [
        "docker",
        "run",
        "-i",
        "--rm",
        "--network",
        "none",
        "--ipc",
        "none",
        "--read-only",
        "--security-opt",
        "no-new-privileges",
        "--cap-drop",
        "ALL",
        "--pids-limit",
        str(limits.pids),
        "--memory",
        str(limits.memory_bytes),
        "--cpus",
        cpu,
        "--ulimit",
        "nofile=256:256",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,size=256m",
        "--tmpfs",
        "/work:rw,nosuid,nodev,size=1024m",
        "--workdir",
        "/work",
        "--user",
        "65534:65534",
        image,
        "/bin/sh",
        "-s",
    ]

    # Provide the user command over stdin to avoid bind-mounting a host directory.
    script = "set -eu\n" + command + "\n"

    p = subprocess.Popen(
        docker_cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    try:
        assert p.stdin is not None
        p.stdin.write(script)
        p.stdin.close()
    except Exception:
        try:
            p.kill()
        except Exception:
            pass
        raise

    def pump(stream_name: str, f):
        try:
            for line in iter(f.readline, ""):
                on_log(stream_name, line)
        finally:
            try:
                f.close()
            except Exception:
                pass

    t1 = threading.Thread(target=pump, args=("stdout", p.stdout), daemon=True)
    t2 = threading.Thread(target=pump, args=("stderr", p.stderr), daemon=True)
    t1.start()
    t2.start()

    try:
        rc = p.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        on_log("system", f"timeout after {timeout_seconds}s\n")
        p.kill()
        rc = 124

    # Ensure pump threads drain remaining output.
    try:
        t1.join(timeout=1.0)
        t2.join(timeout=1.0)
    except Exception:
        pass
    return rc


def run_sandbox(
    *,
    sandbox: str,
    command: str,
    timeout_seconds: int,
    image: Optional[str],
    limits: SandboxLimits,
    on_log: Callable[[str, str], None],
) -> int:
    if sandbox == "docker":
        return run_docker(
            command,
            timeout_seconds=timeout_seconds,
            image=image or "python:3.12-slim",
            limits=limits,
            on_log=on_log,
        )
    return run_local(command, timeout_seconds=timeout_seconds, limits=limits, on_log=on_log)
