import base64
import hashlib
import hmac
import json
import logging
import secrets
import time
import os
from collections import defaultdict
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

log = logging.getLogger("resiprice.auth")

router = APIRouter(prefix="/api/auth")

# Secret key for signing tokens — MUST be set in production via env var.
# A random fallback is generated per-process for local dev (tokens won't survive restarts).
SECRET_KEY = os.environ.get("RESIPRICE_SECRET", "")
if not SECRET_KEY:
    SECRET_KEY = secrets.token_hex(32)
    log.warning("RESIPRICE_SECRET not set — using ephemeral key (tokens reset on restart)")

# Admin password hash — set RESIPRICE_ADMIN_HASH in production.
# Generate with: python -c "import hashlib; print(hashlib.sha256(b'YOUR_PASSWORD').hexdigest())"
_ADMIN_HASH = os.environ.get("RESIPRICE_ADMIN_HASH", "")
if not _ADMIN_HASH:
    log.warning("RESIPRICE_ADMIN_HASH not set — admin login is disabled")

USERS = {
    "admin": {
        "password_hash": _ADMIN_HASH,
        "role": "admin",
    },
}


class LoginRequest(BaseModel):
    username: str
    password: str


def _sign_token(payload: dict) -> str:
    """Create a simple signed token (HMAC-SHA256)."""
    payload_bytes = json.dumps(payload, sort_keys=True).encode()
    sig = hmac.new(SECRET_KEY.encode(), payload_bytes, hashlib.sha256).hexdigest()
    token = base64.urlsafe_b64encode(payload_bytes).decode() + "." + sig
    return token


def verify_token(token: str) -> Optional[dict]:
    """Verify and decode a signed token. Returns payload or None."""
    try:
        parts = token.split(".")
        if len(parts) != 2:
            return None
        payload_bytes = base64.urlsafe_b64decode(parts[0])
        expected_sig = hmac.new(SECRET_KEY.encode(), payload_bytes, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected_sig, parts[1]):
            return None
        payload = json.loads(payload_bytes)
        if payload.get("exp", 0) < time.time():
            return None
        return payload
    except (json.JSONDecodeError, ValueError, Exception):
        return None


# --------------- Rate limiting ---------------
_MAX_LOGIN_ATTEMPTS = 5
_LOCKOUT_SECONDS = 300  # 5 minutes
_login_attempts: dict[str, list[float]] = defaultdict(list)


def _check_rate_limit(key: str) -> None:
    """Raise 429 if too many recent login attempts from this key."""
    now = time.time()
    window_start = now - _LOCKOUT_SECONDS
    # Prune old attempts
    _login_attempts[key] = [t for t in _login_attempts[key] if t > window_start]
    if len(_login_attempts[key]) >= _MAX_LOGIN_ATTEMPTS:
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")
    _login_attempts[key].append(now)


@router.post("/login")
async def login(req: LoginRequest, request: Request):
    # Rate limit by IP
    client_ip = request.client.host if request.client else "unknown"
    _check_rate_limit(client_ip)

    user = USERS.get(req.username)
    if not user or not user["password_hash"]:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    password_hash = hashlib.sha256(req.password.encode()).hexdigest()
    if not hmac.compare_digest(password_hash, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = _sign_token({
        "sub": req.username,
        "role": user["role"],
        "exp": time.time() + 86400 * 7,  # 7 days
    })

    return {"token": token, "username": req.username, "role": user["role"]}


@router.get("/me")
async def me(request: Request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")

    payload = verify_token(auth[7:])
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    return {"username": payload["sub"], "role": payload["role"]}
