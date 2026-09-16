"""One-off script to inject starting URLs into the Redis Queue.
Usage: python seed.py https://example.com/ https://example.org/start

This is also the ONLY place new domains enter the system: for every domain among the
given URLs that isn't already in the DomainsWhitelist table, robots.txt is fetched and parsed
here, and the domain is whitelisted (a row is inserted) only if it's permitted.
Crawlers load the DomainsWhitelist table once at startup and never recheck it - if a crawler is
already running, re-running this script will NOT make it see a newly whitelisted
domain. Stop the crawler, seed, then start it again if you need that.
"""

import asyncio
import os
import sys
import logging
from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl
from urllib.robotparser import RobotFileParser

import aiohttp
import asyncpg
from redis.asyncio import Redis

_LOG_LEVEL_NAME = os.environ.get('LOG_LEVEL', 'INFO').upper()
LOG_LEVEL = getattr(logging, _LOG_LEVEL_NAME, logging.INFO)
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
if not isinstance(getattr(logging, _LOG_LEVEL_NAME, None), int):
    logging.warning(f"Unknown LOG_LEVEL={_LOG_LEVEL_NAME!r}, falling back to INFO")

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_CRAWL_STREAM=os.environ.get('REDIS_CRAWL_STREAM', 'crawl_stream')


DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = int(os.environ.get('DB_PORT', '5432'))
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'postgres')
DB_NAME = os.environ.get('DB_NAME', 'scraped_data')

USER_AGENT = os.environ.get('CRAWLER_USER_AGENT', '*')


def _warn_new_domains(new_domains: list[str]) -> None:
    # Warning message: If someone runs seed without stopping the scrapers things will break.
    lines = [
        "  WARNING: whitelisting domain(s) never seeded before:",
        *(f"    - {d}" for d in new_domains),
        "",
        "  Crawlers load the domain whitelist ONCE at startup and never recheck it.",
        "  If a crawler is ALREADY RUNNING, it will NOT see these new domains, and",
        "  links discovered on them will be silently dropped.",
        "",
        "  If this crawl needs these domains, stop the crawler first:",
        "    docker compose stop crawler",
        "    docker compose exec dispatcher python seed.py <urls...>",
        "    docker compose up -d crawler",
    ]
    logging.warning("\n".join(lines))


async def _fetch_robots(session: aiohttp.ClientSession, domain: str) -> tuple[bool, str | None]:
    """Fetch and evaluate robots.txt for a domain. Returns (permitted, robots_txt)."""
    robots_text = None
    no_robots_file = False
    access_denied = False
    for scheme in ("https", "http"):
        robots_url = f"{scheme}://{domain}/robots.txt"
        try:
            async with session.get(robots_url) as response:
                if response.status == 200:
                    robots_text = await response.text()
                    break
                if response.status in (401, 403):
                    # Access to robots.txt itself refused -> treat the domain as fully disallowed
                    logging.warning(f"robots.txt at {robots_url} returned HTTP {response.status} (access denied)")
                    access_denied = True
                    break
                if 400 <= response.status < 500:
                    # Any other 4xx (404, 410, ...) -> no robots.txt in effect, no restrictions apply
                    logging.info(f"robots.txt at {robots_url} returned HTTP {response.status}; treating as absent")
                    no_robots_file = True
                    break
                logging.warning(f"robots.txt at {robots_url} returned HTTP {response.status}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logging.warning(f"Failed to fetch {robots_url}: {e}")

    if access_denied or (robots_text is None and not no_robots_file):
        # Either access was explicitly refused, or its absence/presence couldn't be confirmed
        logging.warning(f"Could not confirm crawl permission for {domain!r}; not whitelisting.")
        return False, None

    parser = RobotFileParser()
    parser.parse(robots_text.splitlines() if robots_text is not None else [])
    if not parser.can_fetch(USER_AGENT, f"https://{domain}/"):
        logging.warning(f"robots.txt for {domain!r} disallows {USER_AGENT!r} entirely; not whitelisting.")
        return False, None

    logging.info(f"Domain resolved for crawling: {domain!r} ({'no robots.txt found' if robots_text is None else 'robots.txt parsed'})")
    return True, robots_text


async def _resolve_domain_whitelist(pg_pool: asyncpg.Pool, domains: set[str]) -> dict[str, str | None]:
    """Ensure every given domain is present in the DomainsWhitelist table with its robots.txt.
    Returns {domain: robots_txt} for the PERMITTED domains.
    """
    if not domains:
        return {}

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT domain, robots_txt FROM DomainsWhitelist WHERE domain = ANY($1::text[]);", list(domains))
    known = {row["domain"]: row["robots_txt"] for row in rows}

    new_domains = sorted(domains - known.keys())
    if new_domains:
        _warn_new_domains(new_domains)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            for domain in new_domains:
                permitted, robots_text = await _fetch_robots(session, domain)
                if not permitted:
                    continue
                async with pg_pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO DomainsWhitelist (domain, robots_txt) VALUES ($1, $2) ON CONFLICT (domain) DO NOTHING;",
                        domain, robots_text,
                    )
                known[domain] = robots_text

    return known


async def seed(urls: list[str]) -> None:
    redis_client = Redis(
        host=REDIS_HOST,
        decode_responses=True
    )

    pg_pool = await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        min_size=1, max_size=1,
    )

    try:
        url_domains = {}
        for url in urls:
            hostname = urlparse(url).hostname
            url_domains[url] = hostname.lower() if hostname else None

        domains = {d for d in url_domains.values() if d}
        resolved = await _resolve_domain_whitelist(pg_pool, domains)

        permitted_urls = []
        for url in urls:
            domain = url_domains[url]
            if domain is None:
                logging.warning(f"Skipping url with no hostname: {url!r}")
                continue
            if domain not in resolved:
                logging.warning(f"Skipping url on non-permitted domain: {url!r}")
                continue
            parser = RobotFileParser()
            parser.parse((resolved[domain] or "").splitlines())
            if not parser.can_fetch(USER_AGENT, url):
                logging.warning(f"Skipping url disallowed by robots.txt: {url!r}")
                continue
            permitted_urls.append(url)

        if not permitted_urls:
            logging.warning("No URLs left to seed after robots.txt checks.")
            return

        async with pg_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                INSERT INTO RawData (link)
                SELECT unnest($1::text[])
                ON CONFLICT (link) DO UPDATE SET link = EXCLUDED.link
                RETURNING id, link, (xmax = 0) AS is_new;
                """,
                permitted_urls,
            )

        new_rows = [row for row in rows if row["is_new"]]
        if new_rows:
            async with redis_client.pipeline(transaction=True) as pipe:
                for row in new_rows:
                    pipe.xadd(REDIS_CRAWL_STREAM, {"id": str(row["id"]), "url": row["link"]})
                await pipe.execute()

        logging.info(
            f"Seeded {len(new_rows)} new URLs "
            f"(of {len(permitted_urls)} permitted, {len(urls)} given, {len(rows) - len(new_rows)} already known)."
        )
    finally:
        await redis_client.aclose()
        await pg_pool.close()


def _canonicalize(url: str) -> str:
    p = urlparse(url)
    path = p.path.rstrip("/") or "/"
    query = urlencode(sorted(parse_qsl(p.query)))
    return urlunparse((p.scheme.lower(), p.netloc.lower(), path, "", query, ""))


if __name__ == "__main__":
    urls = sys.argv[1:]
    if not urls:
        print("Usage: python seed.py <url1> <url2> ...")
        sys.exit(1)
    urls = [_canonicalize(u if urlparse(u).scheme else f"http://{u}") for u in urls]
    asyncio.run(seed(urls))