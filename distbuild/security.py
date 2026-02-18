from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
from dataclasses import dataclass


_PBKDF2_ITERS = 210_000


def generate_api_key() -> str:
    # URL-safe; keep it reasonably long.
    return "db_" + secrets.token_urlsafe(32)


def _b64e(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("ascii").rstrip("=")


def _b64d(s: str) -> bytes:
    pad = "=" * ((4 - (len(s) % 4)) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii"))


@dataclass(frozen=True)
class KeyHash:
    salt_b64: str
    digest_b64: str


def hash_api_key(api_key: str) -> KeyHash:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", api_key.encode("utf-8"), salt, _PBKDF2_ITERS)
    return KeyHash(salt_b64=_b64e(salt), digest_b64=_b64e(digest))


def verify_api_key(api_key: str, *, salt_b64: str, digest_b64: str) -> bool:
    salt = _b64d(salt_b64)
    expected = _b64d(digest_b64)
    actual = hashlib.pbkdf2_hmac("sha256", api_key.encode("utf-8"), salt, _PBKDF2_ITERS)
    return hmac.compare_digest(actual, expected)
