from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Optional

import httpx


@dataclass(frozen=True)
class ClientConfig:
    server: str
    consumer_key: str
    default_sandbox: str = "local"
    default_timeout_seconds: int = 600
    default_image: str = ""


class DistBuildClient:
    def __init__(self, cfg: ClientConfig):
        self.cfg = cfg
        self._client = httpx.Client(
            base_url=cfg.server,
            headers={"X-Consumer-Key": cfg.consumer_key},
            timeout=30.0,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "DistBuildClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def submit_job(
        self,
        *,
        command: str,
        sandbox: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        image: Optional[str] = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "command": command,
            "sandbox": sandbox or self.cfg.default_sandbox,
            "timeout_seconds": int(timeout_seconds or self.cfg.default_timeout_seconds),
        }
        img = image
        if img is None:
            img = self.cfg.default_image
        if body["sandbox"] == "docker" and img:
            body["image"] = img

        r = self._client.post("/v1/jobs", json=body)
        r.raise_for_status()
        return r.json()

    def submit_future(
        self,
        *,
        command: str,
        sandbox: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        image: Optional[str] = None,
    ) -> "JobFuture":
        job = self.submit_job(
            command=command,
            sandbox=sandbox,
            timeout_seconds=timeout_seconds,
            image=image,
        )
        return JobFuture(self, str(job["id"]))

    def submit_many(
        self,
        commands: list[str],
        *,
        sandbox: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        image: Optional[str] = None,
    ) -> list["JobFuture"]:
        futs: list[JobFuture] = []
        for cmd in commands:
            futs.append(
                self.submit_future(
                    command=cmd,
                    sandbox=sandbox,
                    timeout_seconds=timeout_seconds,
                    image=image,
                )
            )
        return futs

    def get_job(self, job_id: str) -> dict[str, Any]:
        r = self._client.get(f"/v1/jobs/{job_id}")
        r.raise_for_status()
        return r.json()

    def list_jobs(self, *, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        r = self._client.get("/v1/jobs", params={"limit": limit, "offset": offset})
        r.raise_for_status()
        return r.json()

    def get_logs(self, job_id: str, *, offset_seq: int = 0, limit: int = 500) -> dict[str, Any]:
        r = self._client.get(
            f"/v1/jobs/{job_id}/logs",
            params={"offset_seq": offset_seq, "limit": limit},
        )
        r.raise_for_status()
        return r.json()


class JobFuture:
    def __init__(self, client: DistBuildClient, job_id: str):
        self.client = client
        self.job_id = job_id
        self._offset = 0

    def iter_logs(self, *, limit: int = 500) -> Iterator[dict[str, Any]]:
        payload = self.client.get_logs(self.job_id, offset_seq=self._offset, limit=limit)
        chunks = payload.get("chunks", [])
        self._offset = int(payload.get("next_offset_seq", self._offset))
        yield from chunks

    def wait(self, *, poll_seconds: float = 0.5, print_logs: bool = False) -> dict[str, Any]:
        import time

        start = time.time()
        hinted = False
        sleep_s = max(0.1, float(poll_seconds))

        while True:
            if print_logs:
                for c in self.iter_logs():
                    stream = c.get("stream", "?")
                    text = c.get("text", "")
                    print(f"[{stream}] {text}", end="")

            j = self.client.get_job(self.job_id)
            st = j.get("status")

            if st == "queued":
                # Back off a bit while queued.
                sleep_s = min(max(sleep_s, float(poll_seconds)), 2.0)
                if not hinted and (time.time() - start) > 5.0:
                    hinted = True
                    print(
                        "[system] still queued; ensure a worker is running and the server has DISTBUILD_WORKER_SHARED_TOKEN set\n",
                        end="",
                    )
            else:
                sleep_s = float(poll_seconds)

            if st in ("succeeded", "failed", "cancelled"):
                # Drain remaining logs.
                if print_logs:
                    for _ in range(5):
                        any_new = False
                        for c in self.iter_logs():
                            any_new = True
                            stream = c.get("stream", "?")
                            text = c.get("text", "")
                            print(f"[{stream}] {text}", end="")
                        if not any_new:
                            break
                        time.sleep(0.2)
                return j

            time.sleep(sleep_s)
