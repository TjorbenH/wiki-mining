import asyncio
import os
import logging

from redis.asyncio import Redis
from minio import Minio

from WikiScaper import WikiScraper 

# environment variables
REDIS_HOST = os.environ.get('QUEUE_HOST', 'localhost')
MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS = os.environ.get('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET = os.environ.get('MINIO_SECRET_KEY', 'admin')
MINIO_BUCKET = os.environ.get('MINIO_BUCKET', 'raw-html')

# Wikipedia requires a descriptive user agent with contact information
USER_AGENT = "WikiScraperBot/1.0 (bot@yourdomain.com)"

# initialize MinIO client
s3_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS,
    secret_key=MINIO_SECRET,
    secure=False
)

async def main():
    redis_client = Redis(host=REDIS_HOST)
    
    if not s3_client.bucket_exists(MINIO_BUCKET):
        logging.error(f"MinIO bucket does not exist: {MINIO_BUCKET}")
        exit()

    headers = {
        "User-Agent": USER_AGENT
    }

    scraper = WikiScraper(
        minio_client=s3_client,
        minio_bucket=MINIO_BUCKET,
        redis_client=redis_client,
        domain="books.toscrape.com",
        
    )

    # Seed the queue for testing
    await redis_client.lpush('crawl_queue', 'https://books.toscrape.com/')
    
    # Run the scraper in an asyncio task so we can catch interrupts
    scraper_task = asyncio.create_task(scraper.run(concurrency_limit=1, rate_limit=1))
    
    try:
        await scraper_task
    except asyncio.CancelledError:
        logging.info("Main task cancelled, issuing stop command...")
        scraper.stop()
        await scraper_task
    finally:
        await redis_client.aclose()

if __name__ == "__main__":
    asyncio.run(main())