from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def default_config_path() -> Path:
    # Prefer XDG, fallback to ~/.config.
    xdg = os.getenv("XDG_CONFIG_HOME")
    base = Path(xdg) if xdg else (Path.home() / ".config")
    return base / "distbuild" / "config.toml"


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def atomic_write_text(path: Path, text: str) -> None:
    ensure_parent_dir(path)
    tmp = None
    try:
        fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
        tmp = Path(tmp_name)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, path)
    finally:
        try:
            if tmp is not None and tmp.exists():
                tmp.unlink()
        except Exception:
            pass


def parse_toml(text: str) -> dict[str, Any]:
    # Python 3.11+.
    import tomllib

    return tomllib.loads(text)


def dumps_toml_simple(d: dict[str, Any]) -> str:
    # Minimal TOML writer for our tiny config shape.
    # Only supports top-level str/int/bool keys.
    lines: list[str] = []
    for k in sorted(d.keys()):
        v = d[k]
        if isinstance(v, bool):
            lines.append(f"{k} = {'true' if v else 'false'}")
        elif isinstance(v, int):
            lines.append(f"{k} = {v}")
        elif isinstance(v, str):
            esc = v.replace("\\", "\\\\").replace('"', '\\"')
            lines.append(f"{k} = \"{esc}\"")
        elif v is None:
            continue
        else:
            raise TypeError(f"unsupported type for {k}: {type(v)}")
    return "\n".join(lines) + "\n"


def redact_key(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    # Keep key_id prefix, redact the secret body.
    if "." in s:
        kid, rest = s.split(".", 1)
        if len(rest) <= 8:
            return kid + "." + "*" * len(rest)
        return kid + "." + rest[:4] + "..." + rest[-4:]
    if len(s) <= 8:
        return "*" * len(s)
    return s[:4] + "..." + s[-4:]


def dumps_config_profiles(cfg: dict[str, Any]) -> str:
    # Expected shape:
    # {
    #   "active_profile": "default",
    #   "profiles": {
    #       "default": {"server": ..., "consumer_key": ..., ...}
    #   }
    # }
    lines: list[str] = []
    active = str(cfg.get("active_profile", "default"))
    lines.append(f"active_profile = \"{active}\"")
    lines.append("")

    profiles = cfg.get("profiles") or {}
    if not isinstance(profiles, dict):
        raise TypeError("profiles must be a dict")

    for name in sorted(profiles.keys()):
        p = profiles[name]
        if not isinstance(p, dict):
            continue
        lines.append(f"[profiles.{name}]")
        # Write known keys first.
        for k in ("server", "consumer_key", "default_sandbox", "default_timeout_seconds", "default_image"):
            if k not in p:
                continue
            v = p[k]
            if isinstance(v, int):
                lines.append(f"{k} = {v}")
            else:
                esc = str(v).replace("\\", "\\\\").replace('"', '\\"')
                lines.append(f"{k} = \"{esc}\"")
        # Unknown keys.
        for k in sorted(p.keys()):
            if k in ("server", "consumer_key", "default_sandbox", "default_timeout_seconds", "default_image"):
                continue
            v = p[k]
            if isinstance(v, bool):
                lines.append(f"{k} = {'true' if v else 'false'}")
            elif isinstance(v, int):
                lines.append(f"{k} = {v}")
            else:
                esc = str(v).replace("\\", "\\\\").replace('"', '\\"')
                lines.append(f"{k} = \"{esc}\"")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"
