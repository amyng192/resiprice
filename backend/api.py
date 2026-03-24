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

# Limit to 1 concurrent browser — parallel Chromium instances thrash
# CPU/RAM and cause all scrapes to time out on typical hardware.
# URLs are queued and streamed back one at a time as they complete.
executor = ThreadPoolExecutor(max_workers=1)


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

    # Per-URL timeout — each URL gets its own independent deadline
    per_url_timeout = 180

    async def scrape_with_timeout(idx: int, url: str) -> tuple[int, dict | None, str | None]:
        """Run a single scrape with its own timeout. Returns (index, result, error)."""
        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(executor, scrape_one, url),
                timeout=per_url_timeout,
            )
            return (idx, result, None)
        except asyncio.TimeoutError:
            log.error(f"Scrape timed out for {url}")
            return (idx, None, "Scrape timed out — this site may be too complex to scrape automatically.")
        except Exception as e:
            log.error(f"Scrape failed for {url}: {e}")
            return (idx, None, str(e))

    async def event_generator():
        tasks = {
            asyncio.ensure_future(scrape_with_timeout(i, url)): i
            for i, url in enumerate(urls)
        }

        while tasks:
            done, _ = await asyncio.wait(
                tasks.keys(), return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:
                tasks.pop(task)
                idx, result, error = task.result()
                if result is not None:
                    yield {
                        "event": "property",
                        "data": json.dumps({
                            "index": idx,
                            "property": result,
                        }),
                    }
                else:
                    yield {
                        "event": "error",
                        "data": json.dumps({
                            "index": idx,
                            "url": urls[idx],
                            "error": error,
                        }),
                    }

        yield {"event": "done", "data": json.dumps({"done": True})}

    return EventSourceResponse(event_generator())
