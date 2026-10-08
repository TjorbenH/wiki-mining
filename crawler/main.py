import asyncio
import os
import logging
import signal
import asyncpg
from redis.asyncio import Redis
from minio import Minio

from MediaWikiScraperWorker import MediaWikiScraperWorker

# environment variables
S3_ENDPOINT = os.environ.get('S3_ENDPOINT') or 'localhost:8333'
S3_ACCESS = os.environ.get('S3_ACCESS_KEY') or 'admin'
S3_SECRET = os.environ.get('S3_SECRET_KEY') or 'admin'
S3_BUCKET = os.environ.get('S3_BUCKET') or 'raw-html'

CONTAINER_NAME = os.environ.get('HOSTNAME', 'unknown')

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_CRAWL_STREAM=os.environ.get('REDIS_CRAWL_STREAM', 'crawl_stream')
REDIS_CRAWL_GROUP=os.environ.get('REDIS_CRAWL_GROUP', 'crawlers')
REDIS_DISPATCHER_STREAM=os.environ.get('REDIS_DISPATCHER_STREAM', 'unprocessed_links')
REDIS_DISPATCHER_GROUP=os.environ.get('REDIS_DISPATCHER_GROUP', 'dispatchers')

DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = int(os.environ.get('DB_PORT', '5432'))
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'postgres')
DB_NAME = os.environ.get('DB_NAME', 'scraped_data')

# `or` so an empty value (compose forwards a key missing from .env as "") falls back to the default too
CONCURRENCY_LIMIT = int(os.environ.get('CONCURRENCY_LIMIT') or '4')
RATE_LIMIT = float(os.environ.get('RATE_LIMIT') or '0.5')

CRAWLER_USER_AGENT = os.environ.get('CRAWLER_USER_AGENT', '*')

_LOG_LEVEL_NAME = os.environ.get('LOG_LEVEL', 'INFO').upper()
LOG_LEVEL = getattr(logging, _LOG_LEVEL_NAME, logging.INFO)


async def main():
    logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
    if not isinstance(getattr(logging, _LOG_LEVEL_NAME, None), int):
        logging.warning(f"Unknown LOG_LEVEL={_LOG_LEVEL_NAME!r}, falling back to INFO")
    logging.info(
        f"Config: DB={DB_HOST}:{DB_PORT}/{DB_NAME} user={DB_USER} | "
        f"Redis={REDIS_HOST} crawl_stream={REDIS_CRAWL_STREAM} crawl_group={REDIS_CRAWL_GROUP} dispatcher_stream={REDIS_DISPATCHER_STREAM} dispatcher_group={REDIS_DISPATCHER_GROUP} | "
        f"S3={S3_ENDPOINT} bucket={S3_BUCKET} | "
        f"concurrency={CONCURRENCY_LIMIT} rate_limit={RATE_LIMIT}s user_agent={CRAWLER_USER_AGENT!r}"
    )

    redis_client = Redis(
        host=REDIS_HOST,
        decode_responses=True
    )
    
    s3_client = Minio(
        S3_ENDPOINT,
        access_key=S3_ACCESS,
        secret_key=S3_SECRET,
        secure=False
    )

    if not s3_client.bucket_exists(S3_BUCKET):
        s3_client.make_bucket(S3_BUCKET)
        logging.info(f"Created bucket: {S3_BUCKET}")
        
    pg_pool = await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        min_size=CONCURRENCY_LIMIT,
        max_size=CONCURRENCY_LIMIT,
    )

    scraper = MediaWikiScraperWorker(
        s3_client=s3_client,
        s3_bucket=S3_BUCKET,
        redis_client=redis_client,
        crawl_stream=REDIS_CRAWL_STREAM,
        crawl_group=REDIS_CRAWL_GROUP,
        dispatcher_stream=REDIS_DISPATCHER_STREAM,
        dispatcher_group=REDIS_DISPATCHER_GROUP,
        pg_pool=pg_pool,
        hostname=CONTAINER_NAME,
        headers={'User-Agent': CRAWLER_USER_AGENT},
        max_connections_per_host=1,
    )

    scraper_task = asyncio.create_task(scraper.run(concurrency_limit=CONCURRENCY_LIMIT, rate_limit=RATE_LIMIT))
        
    def _handle_shutdown_signal():
        logging.info("Shutdown signal received, stopping scraper...")
        scraper.stop()
    
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_shutdown_signal)
    
    try:
        await scraper_task
    finally:
        await redis_client.aclose()
        await pg_pool.close()

if __name__ == "__main__":
    asyncio.run(main())