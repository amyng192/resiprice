"""Local dev launcher — sets env vars before importing the app."""
import os

os.environ.setdefault(
    "RESIPRICE_SECRET", "dev-testing-secret-key-1234567890abcdef"
)
os.environ.setdefault(
    "RESIPRICE_ADMIN_HASH",
    "cc992d8f4610208eba6b00e37f21c24b901ff9918d4f9fa39f8a50cd39e15018",
)

import uvicorn  # noqa: E402

from backend.main import app  # noqa: E402

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
