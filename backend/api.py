import sys
import os
import json
import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, HTTPException, Request
from sse_starlette.sse import EventSourceResponse

from backend.auth import verify_token
from backend.schemas import ScrapeRequest

# Ensure the project root is on the path so we can import apartment_scraper
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from apartment_scraper import PlaywrightScraper

log = logging.getLogger("resiprice.api")
router = APIRouter(prefix="/api")

# Single-worker pool — we run one browser at a time to avoid thrashing.
executor = ThreadPoolExecutor(max_workers=1)

# Per-URL timeout in seconds.  Each URL gets its own full window.
PER_URL_TIMEOUT = 240


def scrape_one(url: str, cancel_event: threading.Event) -> dict:
    """Run a single scrape, checking the cancel event periodically."""
    scraper = PlaywrightScraper(headless=True)
    result = scraper.scrape(
        url,
        tab_labels=["0", "1", "2", "3", "4", "5"],
        tab_type="floor",
        cancel_event=cancel_event,
    )
    return result.to_dict()


def scrape_one_safe(url: str, cancel_event: threading.Event) -> dict:
    """Wrapper that ensures browser cleanup even on cancellation."""
    try:
        return scrape_one(url, cancel_event)
    except Exception:
        if cancel_event.is_set():
            raise TimeoutError("Scrape cancelled")
        raise


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.post("/scrape")
async def scrape(request: ScrapeRequest, raw_request: Request):
    # Require valid auth token
    auth = raw_request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    payload = verify_token(auth[7:])
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    urls = request.urls
    loop = asyncio.get_event_loop()

    async def event_generator():
        # Process URLs sequentially so each one gets the full timeout window.
        # With max_workers=1, parallel submission would just queue and starve.
        for idx, url in enumerate(urls):
            cancel_event = threading.Event()
            try:
                result = await asyncio.wait_for(
                    loop.run_in_executor(
                        executor, scrape_one_safe, url, cancel_event
                    ),
                    timeout=PER_URL_TIMEOUT,
                )
                yield {
                    "event": "property",
                    "data": json.dumps({
                        "index": idx,
                        "property": result,
                    }),
                }
            except asyncio.TimeoutError:
                # Signal the thread to stop, so it doesn't block the worker
                cancel_event.set()
                log.error(f"Scrape timed out for {url}")
                yield {
                    "event": "error",
                    "data": json.dumps({
                        "index": idx,
                        "url": url,
                        "error": "Scrape timed out — the site took too long to respond.",
                    }),
                }
                # Give the thread a moment to notice the cancel and release the worker
                await asyncio.sleep(2)
            except Exception as e:
                log.error(f"Scrape failed for {url}: {e}")
                yield {
                    "event": "error",
                    "data": json.dumps({
                        "index": idx,
                        "url": url,
                        "error": "Scrape failed — an unexpected error occurred.",
                    }),
                }

        yield {"event": "done", "data": json.dumps({"done": True})}

    return EventSourceResponse(event_generator())
