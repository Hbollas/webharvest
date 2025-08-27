from urllib.parse import urlparse, urljoin
import httpx

def robots_url(base: str) -> str:
    p = urlparse(base)
    origin = f"{p.scheme}://{p.netloc}"
    return urljoin(origin, "/robots.txt")

def fetch_disallows(base: str, user_agent: str = "*") -> list[str]:
    """
    Parse a very small subset of robots.txt for User-agent: * Disallow: rules.
    Good enough for demos; use a real parser for production.
    """
    url = robots_url(base)
    try:
        r = httpx.get(url, timeout=10.0, follow_redirects=True)
        if r.status_code != 200:
            return []
    except Exception:
        return []

    disallows: list[str] = []
    ua_block = False
    for raw in r.text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("user-agent:"):
            ua = line.split(":", 1)[1].strip()
            ua_block = (ua == "*" or ua.lower() == user_agent.lower())
        elif ua_block and line.lower().startswith("disallow:"):
            path = line.split(":", 1)[1].strip()
            if path:
                disallows.append(path)
    return disallows

def is_allowed(url: str, disallows: list[str]) -> bool:
    path = urlparse(url).path or "/"
    for rule in disallows:
        if path.startswith(rule):
            return False
    return True
