import sys
import os
import json
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter
from sse_starlette.sse import EventSourceResponse

from backend.schemas import ScrapeRequest

# Ensure the project root is on the path so we can import apartment_scraper
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from apartment_scraper import PlaywrightScraper

log = logging.getLogger("resiprice.api")
router = APIRouter(prefix="/api")

executor = ThreadPoolExecutor(max_workers=4)


def scrape_one(url: str) -> dict:
    scraper = PlaywrightScraper(headless=True)
    # Single scrape — try explicit floor tabs first, auto-detect as fallback
    # (both strategies run inside a single browser session now)
    result = scraper.scrape(url, tab_labels=["0", "1", "2", "3", "4", "5"], tab_type="floor")
    return result.to_dict()


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.post("/scrape")
async def scrape(request: ScrapeRequest):
    urls = request.urls
    loop = asyncio.get_event_loop()

    # Per-URL timeout: 90 seconds max so the frontend never spins forever
    per_url_timeout = 90

    futures = {
        i: loop.run_in_executor(executor, scrape_one, url)
        for i, url in enumerate(urls)
    }

    async def event_generator():
        pending = {asyncio.ensure_future(f): i for f, i in
                   {futures[i]: i for i in futures}.items()}

        while pending:
            done, still_pending = await asyncio.wait(
                pending.keys(), return_when=asyncio.FIRST_COMPLETED,
                timeout=per_url_timeout,
            )

            # If nothing completed within the timeout, cancel remaining
            if not done:
                for task in still_pending:
                    idx = pending[task]
                    task.cancel()
                    log.error(f"Scrape timed out for {urls[idx]}")
                    yield {
                        "event": "error",
                        "data": json.dumps({
                            "index": idx,
                            "url": urls[idx],
                            "error": "Scrape timed out — this site may be too complex to scrape automatically.",
                        }),
                    }
                break

            for task in done:
                idx = pending.pop(task)
                try:
                    result = task.result()
                    yield {
                        "event": "property",
                        "data": json.dumps({
                            "index": idx,
                            "property": result,
                        }),
                    }
                except Exception as e:
                    log.error(f"Scrape failed for {urls[idx]}: {e}")
                    yield {
                        "event": "error",
                        "data": json.dumps({
                            "index": idx,
                            "url": urls[idx],
                            "error": str(e),
                        }),
                    }

        yield {"event": "done", "data": json.dumps({"done": True})}

    return EventSourceResponse(event_generator())
