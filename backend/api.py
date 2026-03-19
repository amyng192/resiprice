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
    # Try with common floor tab labels first — most Entrata/property sites
    # use numbered floor tabs. Without these, the scraper picks up nav menus.
    result = scraper.scrape(url, tab_labels=["0", "1", "2", "3", "4", "5"], tab_type="floor")
    if not result.units:
        # Fallback: try auto-detect if floor tabs found nothing
        result = scraper.scrape(url)
    return result.to_dict()


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.post("/scrape")
async def scrape(request: ScrapeRequest):
    urls = request.urls
    loop = asyncio.get_event_loop()

    futures = {
        i: loop.run_in_executor(executor, scrape_one, url)
        for i, url in enumerate(urls)
    }

    async def event_generator():
        pending = set(futures.values())
        index_by_future = {f: i for i, f in futures.items()}

        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for future in done:
                idx = index_by_future[future]
                try:
                    result = future.result()
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
