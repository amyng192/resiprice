import csv
import io
import logging

from fastapi import APIRouter, HTTPException, Request, UploadFile, File

from backend.auth import verify_token
from backend.database import (
    search_communities,
    get_all_communities,
    import_communities,
    delete_community,
)

log = logging.getLogger("resiprice.communities")

router = APIRouter(prefix="/api/communities")


def _require_auth(request: Request) -> dict:
    """Verify Bearer token and return the payload, or raise 401."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    payload = verify_token(auth[7:])
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return payload


@router.get("/search")
async def search(request: Request, q: str = ""):
    _require_auth(request)
    results = search_communities(q, limit=10)
    return {"communities": results}


@router.get("")
async def list_all(request: Request):
    _require_auth(request)
    return {"communities": get_all_communities()}


@router.post("/import")
async def import_csv(request: Request, file: UploadFile = File(...)):
    _require_auth(request)

    if not file.filename or not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    content = await file.read()
    try:
        text = content.decode("utf-8-sig")  # handles BOM from Excel
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File must be UTF-8 encoded")

    reader = csv.DictReader(io.StringIO(text))

    # Validate required columns
    fieldnames = reader.fieldnames or []
    lower_fields = [f.lower().strip() for f in fieldnames]
    if "name" not in lower_fields or "url" not in lower_fields:
        raise HTTPException(
            status_code=400,
            detail='CSV must have "name" and "url" columns',
        )

    # Normalize column names to lowercase
    rows = []
    for row in reader:
        normalized = {k.lower().strip(): v for k, v in row.items()}
        rows.append(normalized)

    count = import_communities(rows)
    log.info(f"Imported {count} communities from CSV")
    return {"imported": count, "total_rows": len(rows)}


@router.delete("/{community_id}")
async def delete(community_id: int, request: Request):
    _require_auth(request)
    if not delete_community(community_id):
        raise HTTPException(status_code=404, detail="Community not found")
    return {"deleted": True}
