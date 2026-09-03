import asyncio
import os
import logging

import asyncpg
from redis.asyncio import Redis
from minio import Minio

from WikiScraper import WikiScraper 

# environment variables
MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS = os.environ.get('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET = os.environ.get('MINIO_SECRET_KEY', 'admin')
MINIO_BUCKET = os.environ.get('MINIO_BUCKET', 'raw-html')

CONTAINER_NAME = os.environ.get('HOSTNAME', 'unknown')

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_CRAWL_STREAM=os.environ.get('REDIS_CRAWL_STREAM', 'crawl_stream')
REDIS_CRAWL_GROUP=os.environ.get('REDIS_CRAWL_GROUP', 'crawlers')
REDIS_LINKS_QUEUE=os.environ.get('REDIS_LINKS_QUEUE', 'unprocessed_links')


DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = int(os.environ.get('DB_PORT', '5432'))
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'postgres')
DB_NAME = os.environ.get('DB_NAME', 'scraped_data')

CONCURRENCY_LIMIT = 1
RATE_LIMIT = 1



# initialize MinIO client
s3_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS,
    secret_key=MINIO_SECRET,
    secure=False
)

async def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    redis_client = Redis(host=REDIS_HOST)
    
    if not s3_client.bucket_exists(MINIO_BUCKET):
        logging.error(f"MinIO bucket does not exist: {MINIO_BUCKET}")
        exit()
        
    pg_pool = await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        min_size=CONCURRENCY_LIMIT,
        max_size=CONCURRENCY_LIMIT,
    )


    scraper = WikiScraper(
        minio_client=s3_client,
        minio_bucket=MINIO_BUCKET,
        redis_client=redis_client,
        pg_pool = pg_pool,
        hostname=CONTAINER_NAME,
        crawl_stream=REDIS_CRAWL_STREAM,
        crawl_group=REDIS_CRAWL_GROUP,
        links_queue=REDIS_LINKS_QUEUE,
        domain="books.toscrape.com"
    )
    
    scraper_task = asyncio.create_task(scraper.run(concurrency_limit=CONCURRENCY_LIMIT, rate_limit=RATE_LIMIT))
    
    try:
        await scraper_task
    except asyncio.CancelledError:
        logging.info("Main task cancelled, issuing stop command...")
        scraper.stop()
        await scraper_task
    finally:
        await redis_client.aclose()
        await pg_pool.close()

if __name__ == "__main__":
    asyncio.run(main())