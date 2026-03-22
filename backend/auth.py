import hashlib
import hmac
import json
import time
import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

router = APIRouter(prefix="/api/auth")

# Secret key for signing tokens (generated once, persisted in env or fallback)
SECRET_KEY = os.environ.get("RESIPRICE_SECRET", "rp-secret-k3y-ch4ng3-1n-pr0d")

# Admin credentials — change password before deploying
USERS = {
    "admin": {
        "password_hash": hashlib.sha256("ResiPrice2024!".encode()).hexdigest(),
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
    import base64
    token = base64.urlsafe_b64encode(payload_bytes).decode() + "." + sig
    return token


def verify_token(token: str) -> Optional[dict]:
    """Verify and decode a signed token. Returns payload or None."""
    try:
        import base64
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
    except Exception:
        return None


@router.post("/login")
async def login(req: LoginRequest):
    user = USERS.get(req.username)
    if not user:
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
