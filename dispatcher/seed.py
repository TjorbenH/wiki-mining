"""One-off script to inject starting URLs into the Redis Queue.
Usage: python seed.py https://example.com/ https://example.org/start
"""

import asyncio
import os
import sys
import logging
from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl

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
        async with pg_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                INSERT INTO RawData (link)
                SELECT unnest($1::text[])
                ON CONFLICT (link) DO UPDATE SET link = EXCLUDED.link
                RETURNING id, link, (xmax = 0) AS is_new;
                """,
                urls,
            )

        new_rows = [row for row in rows if row["is_new"]]
        if new_rows:
            async with redis_client.pipeline(transaction=True) as pipe:
                for row in new_rows:
                    pipe.xadd(REDIS_CRAWL_STREAM, {"id": str(row["id"]), "url": row["link"]})
                await pipe.execute()

        logging.info(f"Seeded {len(new_rows)} new URLs (of {len(urls)} given, {len(rows) - len(new_rows)} already known).")
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