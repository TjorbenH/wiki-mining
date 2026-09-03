import asyncio
import io
import logging

from urllib.parse import urljoin, urlparse

import aiohttp
import bs4
from redis.asyncio import Redis
from minio import Minio


logger = logging.getLogger(__name__)

class ScraperWorker:
    """ ScaperWorker is at its core a resource handle for a number of IO-threads.
    These threads fetches URLs out of the Redis Queue, process the html and run an extraction scheme to find and store relevant new URLs to crawl.
    Start / Stop using the run() and stop() method.
    Subclass and override _process_url() / _filter_urls() to customize per-site behavior. 
    """
    
    def __init__(
        self, 
        minio_client: Minio, 
        minio_bucket: str, 
        redis_client: Redis, 
        headers: dict = None
    ):     
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
        self.minio_client = minio_client
        self.minio_bucket = minio_bucket
        self.redis_client = redis_client
        self._headers = headers
        
        # Event to manage the worker threads
        self.stop_event = asyncio.Event()
        # Mutex to prevent calling run multiple times
        self.run_mutex = asyncio.Lock()
        
        # Session with custom header if a website requires identification for automated scapeing (wikipedia)
        self.session = None
    
    async def run(self, concurrency_limit: int = 10, rate_limit: int = 1) -> None:
        if self.run_mutex.locked():
            logger.error(f"Scapers are already running. No more run() calls permitted!")
            return
        
        async with self.run_mutex:
            self.stop_event.clear()
            self.session = aiohttp.ClientSession(headers=self._headers)
            logger.info(f"Starting {concurrency_limit} many worker threads ...")
            try:
                async with asyncio.TaskGroup() as tg:
                    for worker_id in range(concurrency_limit):
                        tg.create_task(self._worker_loop(worker_id, rate_limit))
            finally:
                await self.session.close()
                self.session = None
    
    def stop(self) -> None:
        if not self.run_mutex.locked():
            logger.error(f"Received stop signal without any scrapers running!")
            return
        logger.info(f"Recieved stop signal ...")
        self.stop_event.set()
        
    async def _worker_loop(self, worker_id: int, rate_limit: int) -> None:
        logger.info(f"Worker {worker_id} started...")
        
        while not self.stop_event.is_set():
            try:
                url = await self._pop_url()
                if url is None:
                    continue
                logger.info(f"Worker {worker_id} processing: {url}")
                await self._process_url(url)
            except Exception as e:
                logger.error(f"Worker {worker_id} failed on {url}: {e}")
                continue
            await asyncio.sleep(rate_limit) # basic per item rate limiting
    
    async def _pop_url(self) -> str | None:
        # timeout is needed so the stop signal is checked periodically even if the worker blocks on an empty queue
        to_scrape = await self.redis_client.blpop("crawl_queue", timeout=1)
        if not to_scrape:
            return None
                        
        _, url_bytes = to_scrape
        return url_bytes.decode("utf-8")
    
    async def _process_url(self, url) -> None:
        """ Interface: Overwrite to specify a processing scheme. 
        This default simply stores all the raw html in the redis bucket and runs the extraction pipline to find new urls to scape.
        """
        async with self.session.get(url) as response:
            if response.status == 200:
                # store the raw html
                html = await response.text()
                await self._save_to_minio(url, html)
                                
                # url extraciton pipeline
                raw_links = await asyncio.to_thread(self._extract_urls, html, url)
                new_links = await self._filter_urls(raw_links)
                
                if new_links:
                    await self.redis_client.rpush("unprocessed_links", *new_links)
                                    
            else:
                logger.error(f"Error {response.status} fetching {url}") 
        
    async def _save_to_minio(self, url: str, html_content: str) -> None:
        parsed = urlparse(url)
        safe_filename = f"{parsed.netloc}{parsed.path}".strip("/").replace("/", "_") + ".html"
        
        html_bytes = html_content.encode("utf-8")
        data_stream = io.BytesIO(html_bytes)
        
        def _upload():
            self.minio_client.put_object(
                bucket_name=self.minio_bucket,
                object_name=safe_filename,
                data=data_stream,
                length=len(html_bytes),
                content_type="text/html"
            )
        
        await asyncio.to_thread(_upload)
        logger.info(f"Saved in MinIO: {safe_filename}")
        
    def _extract_urls(self, html: str, base_url: str) -> set[str]:
        """ Parse HTML and extract all hyperlink URLs."""    
        soup = bs4.BeautifulSoup(html, "lxml")
        links = set()
        
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            # resolve relative URLs
            if base_url:
                href = urljoin(base_url, href)
            links.add(href)
             
        return links
    
    async def _filter_urls(self, urls: set[str]) ->set[str]:
        """ Interface: Overwrite to set specific crawl rules (e.g. certain top level domains, whitelist, blacklist ...)"""
        return urls