import asyncio
import httpx

DEFAULT_HEADERS = {
    "User-Agent": "webharvest/0.1 ",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


async def fetch_text_retry(url: str, tries: int = 3, backoff: float = 1.6) -> tuple[int, str]:
    """
    Fetch URL with simple exponential backoff on transient errors.
    Retries on network errors and 429/5xx.
    """
    delay = 0.5
    async with httpx.AsyncClient(
        headers=DEFAULT_HEADERS, http2=True, follow_redirects=True, timeout=15.0
    ) as client:
        last_exc = None
        for attempt in range(1, tries + 1):
            try:
                r = await client.get(url)
                if r.status_code in (429, 500, 502, 503, 504):
                    raise httpx.HTTPStatusError("server busy", request=r.request, response=r)
                return r.status_code, r.text
            except Exception as exc:
                last_exc = exc
                if attempt == tries:
                    raise
                await asyncio.sleep(delay)
                delay *= backoff
    # Should never reach here, but keeps type checkers happy
    raise RuntimeError(f"Failed to fetch {url}") from last_exc
