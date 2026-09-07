import os
import logging
import signal
import asyncpg
import asyncio
from redis.asyncio import Redis

from QueueManager import QueueManager


# environment variables
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

BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '50'))

DB_POOL_MIN_SIZE = 1
DB_POOL_MAX_SIZE = 5

_LOG_LEVEL_NAME = os.environ.get('LOG_LEVEL', 'INFO').upper()
LOG_LEVEL = getattr(logging, _LOG_LEVEL_NAME, logging.INFO)


async def main():
    logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
    if not isinstance(getattr(logging, _LOG_LEVEL_NAME, None), int):
        logging.warning(f"Unknown LOG_LEVEL={_LOG_LEVEL_NAME!r}, falling back to INFO")
    logging.info(
        f"Config: DB={DB_HOST}:{DB_PORT}/{DB_NAME} user={DB_USER} | "
        f"Redis={REDIS_HOST} crawl_stream={REDIS_CRAWL_STREAM} crawl_group={REDIS_CRAWL_GROUP} dispatcher_stream={REDIS_DISPATCHER_STREAM} dispatcher_group={REDIS_DISPATCHER_GROUP} | "
        f"batch_size={BATCH_SIZE}"
    )

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
        min_size=DB_POOL_MIN_SIZE,
        max_size=DB_POOL_MAX_SIZE,
    )
    
    queue_manager = QueueManager(
        redis_client=redis_client,
        crawl_stream=REDIS_CRAWL_STREAM,
        crawl_group=REDIS_CRAWL_GROUP,
        dispatcher_stream=REDIS_DISPATCHER_STREAM,
        dispatcher_group=REDIS_DISPATCHER_GROUP,
        pg_pool=pg_pool,
        hostname=CONTAINER_NAME,
        batch_size=BATCH_SIZE
    )
    
    queue_task = asyncio.create_task(queue_manager.run())
    
    def _handle_shutdown_signal():
        logging.info("Shutdown signal received, stopping scraper...")
        queue_manager.stop()
    
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_shutdown_signal)
    
    try:
        await queue_task
    finally:
        await redis_client.aclose()
        await pg_pool.close()


if __name__ == "__main__":
    asyncio.run(main())