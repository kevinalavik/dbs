from __future__ import annotations

import os
import resource
import shutil
import subprocess
import tempfile
import threading
import uuid
from dataclasses import dataclass
from typing import Callable, Optional

from distbuild import config


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

    # Docker sandbox defaults (tunable via DISTBUILD_DOCKER_* env vars):
    # - Per-job network by default (internet access, not shared with other jobs)
    # - Drop all Linux capabilities
    # - no-new-privileges (blocks setuid escalation)
    # - Optional read-only rootfs
    # - Default user is root (build-style jobs often need it)
    cpu = str(max(0.1, min(4.0, limits.cpu_seconds / 300)))

    def _parse_caps(s: str) -> list[str]:
        out: list[str] = []
        for part in (s or "").split(","):
            cap = part.strip().upper()
            if not cap:
                continue
            if cap.startswith("CAP_"):
                cap = cap[len("CAP_") :]
            out.append(cap)
        # Keep deterministic ordering.
        return sorted(set(out))

    docker_user = (config.DOCKER_RUN_AS or "").strip()
    docker_user_l = docker_user.lower()
    if docker_user_l == "root":
        docker_user = "0:0"
    elif docker_user_l == "nobody":
        docker_user = "65534:65534"

    net_mode = (config.DOCKER_NETWORK_MODE or "job").strip() or "job"
    net_name = net_mode
    created_network = False
    if net_mode == "job":
        net_name = f"distbuild-job-{uuid.uuid4().hex[:12]}"
        try:
            subprocess.run(
                ["docker", "network", "create", "--driver", "bridge", net_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                timeout=10,
                check=True,
            )
            created_network = True
        except Exception as e:
            on_log("system", f"failed to create job network; falling back to bridge: {e!r}\n")
            net_name = "bridge"
    elif net_mode in ("bridge", "none"):
        net_name = net_mode

    docker_cmd = [
        "docker",
        "run",
        "-i",
        "--rm",
        "--network",
        net_name,
        "--ipc",
        "none",
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
        "--workdir",
        "/work",
    ]

    for cap in _parse_caps(getattr(config, "DOCKER_CAP_ADD", "")):
        docker_cmd += ["--cap-add", cap]

    if config.DOCKER_READ_ONLY_ROOTFS:
        docker_cmd += ["--read-only", "--tmpfs", "/work:rw,nosuid,nodev,size=1024m"]

    if docker_user:
        docker_cmd += ["--user", docker_user]

    docker_cmd += [
        image,
        "/bin/sh",
        "-s",
    ]

    # Provide the user command over stdin to avoid bind-mounting a host directory.
    script = "set -eu\nmkdir -p /work\n" + command + "\n"

    try:
        p = subprocess.Popen(
            docker_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
    except Exception:
        if created_network:
            try:
                subprocess.run(["docker", "network", "rm", net_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception:
                pass
        raise

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

    if created_network:
        try:
            subprocess.run(["docker", "network", "rm", net_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            on_log("system", f"warning: failed to remove job network {net_name}: {e!r}\n")
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
            image=image or config.DOCKER_DEFAULT_IMAGE,
            limits=limits,
            on_log=on_log,
        )
    return run_local(command, timeout_seconds=timeout_seconds, limits=limits, on_log=on_log)
