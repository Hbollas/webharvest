import asyncio
import httpx
from typing import Sequence, Dict, Tuple

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


async def _fetch_with_client(
    client: httpx.AsyncClient, url: str, tries: int = 3, backoff: float = 1.6
) -> Tuple[int, str]:
    delay = 0.5
    for attempt in range(1, tries + 1):
        try:
            r = await client.get(url)
            if r.status_code in (429, 500, 502, 503, 504):
                raise httpx.HTTPStatusError("server busy", request=r.request, response=r)
            return r.status_code, r.text
        except Exception:
            if attempt == tries:
                # For bulk mode we don't raise; return a sentinel instead
                return 0, ""
            await asyncio.sleep(delay)
            delay *= backoff
    return 0, ""


async def fetch_many(
    urls: Sequence[str],
    tries: int = 3,
    backoff: float = 1.6,
    concurrency: int = 5,
    delay: float = 0.0,
) -> Dict[str, Tuple[int, str]]:
    """
    Fetch many URLs concurrently with a shared client.
    - concurrency: max in-flight requests
    - delay: optional per-request pause inside each worker
    Returns: {url: (status, text)}
    """
    results: Dict[str, Tuple[int, str]] = {}
    sem = asyncio.Semaphore(max(1, concurrency))
    async with httpx.AsyncClient(
        headers=DEFAULT_HEADERS, http2=True, follow_redirects=True, timeout=15.0
    ) as client:

        async def worker(u: str):
            async with sem:
                if delay:
                    await asyncio.sleep(delay)
                status, text = await _fetch_with_client(client, u, tries=tries, backoff=backoff)
                results[u] = (status, text)

        await asyncio.gather(*(worker(u) for u in urls))
    return results
