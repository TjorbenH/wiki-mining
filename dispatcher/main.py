import os
import logging

from QueueManager import QueueManager


# environment variables
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



async def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')        
    pass


if __name__ == "__main__":
    pass